"""
Worker Implementation

This module provides the worker for executing tasks concurrently.
"""

import threading
import time
import logging
from typing import Callable, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor

from .task import Task, TaskStatus
from .queue import TaskQueue
from .retry import RetryPolicy, RetryError
from .metrics import MetricsCollector


logger = logging.getLogger(__name__)


class Worker:
    """
    A worker that executes tasks from a task queue.
    
    The worker runs in its own thread, continuously pulling tasks from the queue
    and executing them using registered handler functions. It supports proper
    lifecycle management (start/stop), task status tracking, error handling,
    and retry logic integration.
    
    Attributes:
        worker_id: Unique identifier for this worker
        queue: The task queue to pull tasks from
        metrics: Optional metrics collector for tracking statistics
        running: Whether the worker is currently running
        thread: The worker thread
    """
    
    def __init__(
        self,
        worker_id: str,
        queue: TaskQueue,
        handlers: Optional[Dict[str, Callable[[Task], Any]]] = None,
        metrics: Optional[MetricsCollector] = None,
        retry_policy: Optional[RetryPolicy] = None,
        poll_interval: float = 0.1
    ):
        """
        Initialize a new Worker.
        
        Args:
            worker_id: Unique identifier for this worker
            queue: The task queue to pull tasks from
            handlers: Dictionary mapping task names to handler functions
            metrics: Optional metrics collector for tracking statistics
            retry_policy: Optional retry policy for handling task failures
            poll_interval: Time to wait between queue polls when empty (seconds)
        """
        self.worker_id = worker_id
        self.queue = queue
        self.handlers = handlers or {}
        self.metrics = metrics
        self.retry_policy = retry_policy or RetryPolicy()
        self.poll_interval = poll_interval
        
        self._running: bool = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        
        logger.debug(f"Worker {self.worker_id} initialized")
    
    def register_handler(self, task_name: str, handler: Callable[[Task], Any]) -> None:
        """
        Register a handler function for a specific task name.
        
        Args:
            task_name: The name of the task this handler processes
            handler: The function to call when processing the task
        """
        with self._lock:
            self.handlers[task_name] = handler
        logger.debug(f"Worker {self.worker_id} registered handler for '{task_name}'")
    
    def start(self) -> None:
        """
        Start the worker in a new thread.
        
        The worker will begin pulling tasks from the queue and executing them.
        This method returns immediately; the worker runs in the background.
        """
        if self.is_running():
            logger.warning(f"Worker {self.worker_id} is already running")
            return
        
        with self._lock:
            self._running = True
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name=f"Worker-{self.worker_id}",
                daemon=True
            )
            self._thread.start()
        
        logger.info(f"Worker {self.worker_id} started")
    
    def stop(self, timeout: Optional[float] = None) -> None:
        """
        Stop the worker gracefully.
        
        Args:
            timeout: Maximum time to wait for the worker to stop (seconds).
                     If None, waits indefinitely.
        """
        if not self.is_running():
            logger.debug(f"Worker {self.worker_id} is not running")
            return
        
        logger.info(f"Worker {self.worker_id} stopping...")
        
        with self._lock:
            self._running = False
            self._stop_event.set()
        
        # Wait for the worker thread to finish
        if self._thread:
            self._thread.join(timeout=timeout)
        
        logger.info(f"Worker {self.worker_id} stopped")
    
    def is_running(self) -> bool:
        """
        Check if the worker is currently running.
        
        Returns:
            True if the worker is running, False otherwise
        """
        with self._lock:
            return self._running and (self._thread is not None and self._thread.is_alive())
    
    def _run_loop(self) -> None:
        """
        Main worker loop that continuously pulls and executes tasks.
        
        This method runs in the worker thread until stop() is called.
        """
        logger.debug(f"Worker {self.worker_id} entering run loop")
        
        try:
            while self._running and not self._stop_event.is_set():
                try:
                    # Try to get a task with a timeout
                    task = self.queue.get(timeout=self.poll_interval)
                    
                    if task is None:
                        # Queue is empty, continue polling
                        continue
                    
                    # Execute the task
                    self._execute_task(task)
                    
                except Exception as e:
                    # Log but don't exit the loop on queue errors
                    logger.error(
                        f"Worker {self.worker_id} error in run loop: {e}",
                        exc_info=True
                    )
        
        finally:
            logger.debug(f"Worker {self.worker_id} exiting run loop")
    
    def _execute_task(self, task: Task) -> None:
        """
        Execute a single task with proper status tracking and error handling.
        
        Args:
            task: The task to execute
        """
        start_time = time.time()
        duration = None
        
        try:
            # Get the handler for this task
            handler = self._get_handler(task)
            
            if handler is None:
                raise ValueError(f"No handler registered for task '{task.name}'")
            
            # Update task status to running
            task.status = TaskStatus.RUNNING
            logger.debug(
                f"Worker {self.worker_id} executing task {task.id} "
                f"(name: {task.name}, priority: {task.priority})"
            )
            
            # Execute the handler with retry logic
            result = self._execute_with_retry(task, handler)
            
            # Update task status to completed
            task.status = TaskStatus.COMPLETED
            duration = time.time() - start_time
            
            # Update metrics
            if self.metrics:
                self.metrics.increment_completed(duration=duration)
            
            logger.debug(
                f"Worker {self.worker_id} completed task {task.id} "
                f"in {duration:.3f}s"
            )
        
        except RetryError as e:
            # All retries exhausted
            task.status = TaskStatus.FAILED
            task.retry_count = task.max_retries
            duration = time.time() - start_time
            
            if self.metrics:
                self.metrics.increment_failed(duration=duration)
            
            logger.error(
                f"Worker {self.worker_id} task {task.id} failed after "
                f"{task.max_retries} retries: {e}",
                exc_info=True
            )
        
        except Exception as e:
            # Other exception (e.g., no handler registered)
            task.status = TaskStatus.FAILED
            duration = time.time() - start_time
            
            if self.metrics:
                self.metrics.increment_failed(duration=duration)
            
            logger.error(
                f"Worker {self.worker_id} task {task.id} failed: {e}",
                exc_info=True
            )
    
    def _get_handler(self, task: Task) -> Optional[Callable[[Task], Any]]:
        """
        Get the handler for a task.
        
        Args:
            task: The task to get the handler for
            
        Returns:
            The handler function, or None if not found
        """
        with self._lock:
            return self.handlers.get(task.name)
    
    def _execute_with_retry(
        self,
        task: Task,
        handler: Callable[[Task], Any]
    ) -> Any:
        """
        Execute a task handler with retry logic.
        
        Args:
            task: The task to execute
            handler: The handler function to call
            
        Returns:
            The result of the handler function
            
        Raises:
            RetryError: If all retry attempts are exhausted
            Exception: Re-raises if the error is not retryable
        """
        last_error = None
        
        # Calculate remaining retries
        remaining_attempts = task.max_retries - task.retry_count
        
        for attempt in range(remaining_attempts):
            try:
                result = handler(task)
                return result
            
            except Exception as e:
                last_error = e
                
                # Increment retry count
                task.retry_count += 1
                
                # Update task status back to pending for retry
                task.status = TaskStatus.PENDING
                
                # Check if we should retry
                if task.retry_count < task.max_retries:
                    # Add task back to queue for retry
                    self.queue.put(task)
                    
                    # Calculate delay using retry policy
                    delay = self.retry_policy.calculate_delay(task.retry_count - 1)
                    
                    logger.warning(
                        f"Worker {self.worker_id} task {task.id} failed "
                        f"(attempt {task.retry_count}/{task.max_retries}), "
                        f"retrying in {delay:.2f}s: {e}"
                    )
                    
                    # Sleep before allowing the next polling cycle
                    time.sleep(delay)
                    
                    # Break to let the task be picked up again
                    break
                else:
                    # All retries exhausted
                    logger.error(
                        f"Worker {self.worker_id} task {task.id} exhausted "
                        f"retries ({task.retry_count}/{task.max_retries})"
                    )
                    raise RetryError(
                        f"Task {task.id} failed after {task.max_retries} attempts"
                    ) from e
        
        # If we get here, the task was requeued
        raise last_error if last_error else Exception("Task execution failed")


class WorkerPool:
    """
    A pool of worker threads for concurrent task execution.
    
    Manages multiple Worker instances and provides a simple interface
    for controlling the entire pool.
    
    Attributes:
        worker_id_prefix: Prefix for worker IDs
        num_workers: Number of workers in the pool
        queue: The task queue shared by all workers
        workers: List of Worker instances
    """
    
    def __init__(
        self,
        num_workers: int = 4,
        queue: Optional[TaskQueue] = None,
        handlers: Optional[Dict[str, Callable[[Task], Any]]] = None,
        metrics: Optional[MetricsCollector] = None,
        retry_policy: Optional[RetryPolicy] = None,
        poll_interval: float = 0.1
    ):
        """
        Initialize a new WorkerPool.
        
        Args:
            num_workers: Number of worker threads to create
            queue: The task queue to share (creates a new one if None)
            handlers: Dictionary mapping task names to handler functions
            metrics: Optional metrics collector for tracking statistics
            retry_policy: Optional retry policy for handling task failures
            poll_interval: Time to wait between queue polls when empty (seconds)
        """
        self.num_workers = num_workers
        self.queue = queue or TaskQueue()
        self.metrics = metrics
        self.retry_policy = retry_policy
        self.handlers = handlers or {}
        self.poll_interval = poll_interval
        
        # Create workers
        self.workers: list[Worker] = []
        for i in range(num_workers):
            worker = Worker(
                worker_id=f"pool-{i}",
                queue=self.queue,
                handlers=self.handlers.copy(),
                metrics=self.metrics,
                retry_policy=self.retry_policy,
                poll_interval=self.poll_interval
            )
            self.workers.append(worker)
        
        logger.info(f"WorkerPool initialized with {num_workers} workers")
    
    def register_handler(self, task_name: str, handler: Callable[[Task], Any]) -> None:
        """
        Register a handler function across all workers.
        
        Args:
            task_name: The name of the task this handler processes
            handler: The function to call when processing the task
        """
        for worker in self.workers:
            worker.register_handler(task_name, handler)
        logger.debug(f"WorkerPool registered handler for '{task_name}' on all workers")
    
    def start(self) -> None:
        """Start all workers in the pool."""
        for worker in self.workers:
            worker.start()
        logger.info(f"WorkerPool started {len(self.workers)} workers")
    
    def stop(self, timeout: Optional[float] = None) -> None:
        """
        Stop all workers in the pool.
        
        Args:
            timeout: Maximum time to wait for each worker to stop (seconds)
        """
        for worker in self.workers:
            worker.stop(timeout=timeout)
        logger.info(f"WorkerPool stopped {len(self.workers)} workers")
    
    def is_running(self) -> bool:
        """
        Check if all workers in the pool are running.
        
        Returns:
            True if all workers are running, False otherwise
        """
        return all(worker.is_running() for worker in self.workers)
    
    def submit_task(self, task: Task) -> None:
        """
        Submit a task to the pool's queue.
        
        Args:
            task: The task to submit
        """
        self.queue.put(task)
        
        # Update metrics
        if self.metrics:
            self.metrics.increment_submitted()
        
        logger.debug(f"WorkerPool submitted task {task.id}")
    
    def get_queue_size(self) -> int:
        """
        Get the current size of the task queue.
        
        Returns:
            Number of tasks in the queue
        """
        return self.queue.size()