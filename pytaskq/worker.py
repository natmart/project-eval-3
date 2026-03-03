"""
Worker and Worker Pool Implementation

This module provides the Worker and WorkerPool classes for executing tasks concurrently.
"""

import threading
import time
from typing import Callable, Optional, List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime

from .task import Task, TaskStatus
from .queue import TaskQueue
from .retry import RetryPolicy
from .metrics import MetricsCollector


@dataclass
class WorkerMetrics:
    """Metrics for a single worker."""
    id: str
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_duration: float = 0.0
    is_running: bool = False


class Worker:
    """
    A worker that continuously pulls and executes tasks from a task queue.
    
    The worker runs in a separate thread and continuously polls the queue for
    tasks to execute. It handles task execution, status updates, and retry logic.
    
    Attributes:
        id: Unique identifier for the worker
        queue: The task queue to pull tasks from
        handler: Handler function to execute tasks
        retry_policy: Retry policy for failed tasks
        metrics_collector: Optional metrics collector for tracking task execution
        polling_interval: Seconds to wait between queue polls
        stop_event: Threading event to signal graceful shutdown
        _thread: The worker thread
        _tasks_completed: Count of completed tasks by this worker
        _tasks_failed: Count of failed tasks by this worker
    """
    
    def __init__(
        self,
        worker_id: str,
        queue: TaskQueue,
        handler: Callable[[Task], Any],
        retry_policy: Optional[RetryPolicy] = None,
        metrics_collector: Optional[MetricsCollector] = None,
        polling_interval: float = 0.1
    ):
        """
        Initialize a new Worker instance.
        
        Args:
            worker_id: Unique identifier for this worker
            queue: TaskQueue to pull tasks from
            handler: Handler function to execute tasks
            retry_policy: Optional retry policy for failed tasks
            metrics_collector: Optional metrics collector
            polling_interval: Seconds to wait between queue polls
        """
        self.id = worker_id
        self.queue = queue
        self.handler = handler
        self.retry_policy = retry_policy or RetryPolicy()
        self.metrics_collector = metrics_collector
        self.polling_interval = polling_interval
        
        self.stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._is_running: bool = False
        self._lock = threading.Lock()
        
    def start(self) -> None:
        """Start the worker thread."""
        with self._lock:
            if self._is_running:
                return
            
            self._is_running = True
            self.stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name=f"Worker-{self.id}",
                daemon=True
            )
            self._thread.start()
    
    def stop(self) -> None:
        """Signal the worker to stop and wait for it to finish."""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False
            self.stop_event.set()
        
        if self._thread:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                # Thread didn't stop gracefully
                pass
    
    def _run_loop(self) -> None:
        """Main worker loop that pulls and executes tasks."""
        while not self.stop_event.is_set():
            try:
                task = self.queue.dequeue()
                
                if task is None:
                    # No tasks available, wait and continue
                    self.stop_event.wait(self.polling_interval)
                    continue
                
                # Execute the task
                self._execute_task(task)
                
            except Exception as e:
                # Unexpected error in worker loop
                pass
    
    def _execute_task(self, task: Task) -> None:
        """
        Execute a single task with retry logic.
        
        Args:
            task: The task to execute
        """
        start_time = datetime.utcnow()
        last_error = None
        
        for attempt in range(task.max_retries + 1):
            if self.stop_event.is_set():
                # Worker is stopping, put task back if possible
                task.retry_count = attempt
                task.status = TaskStatus.PENDING
                self.queue.enqueue(task)
                return
            
            try:
                # Update task status to running
                task.status = TaskStatus.RUNNING
                task.retry_count = attempt
                
                # Execute the handler
                result = self.handler(task)
                
                # Task completed successfully
                task.status = TaskStatus.COMPLETED
                duration = (datetime.utcnow() - start_time).total_seconds()
                
                with self._lock:
                    self._tasks_completed += 1
                
                if self.metrics_collector:
                    self.metrics_collector.increment_completed(duration=duration)
                
                return
                
            except Exception as e:
                last_error = e
                task.retry_count = attempt + 1
                
                # Check if we should retry
                if attempt < task.max_retries and self.retry_policy.should_retry(attempt, e):
                    delay = self.retry_policy.calculate_delay(attempt)
                    # Wait before retry, checking for stop event
                    if self.stop_event.wait(delay):
                        # Stop event was set, re-enqueue task and exit
                        task.status = TaskStatus.PENDING
                        self.queue.enqueue(task)
                        return
                else:
                    # No more retries or should not retry
                    break
        
        # Task failed
        task.status = TaskStatus.FAILED
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        with self._lock:
            self._tasks_failed += 1
        
        if self.metrics_collector:
            self.metrics_collector.increment_failed(duration=duration)
    
    @property
    def is_running(self) -> bool:
        """Check if the worker is currently running."""
        with self._lock:
            return self._is_running
    
    @property
    def tasks_completed(self) -> int:
        """Get the number of tasks completed by this worker."""
        with self._lock:
            return self._tasks_completed
    
    @property
    def tasks_failed(self) -> int:
        """Get the number of tasks failed by this worker."""
        with self._lock:
            return self._tasks_failed
    
    def get_metrics(self) -> WorkerMetrics:
        """Get current metrics for this worker."""
        with self._lock:
            return WorkerMetrics(
                id=self.id,
                tasks_completed=self._tasks_completed,
                tasks_failed=self._tasks_failed,
                total_duration=0.0,
                is_running=self._is_running
            )


class WorkerPool:
    """
    Manages a pool of workers for concurrent task execution.
    
    The WorkerPool manages multiple worker instances, providing methods to
    start/stop the pool, dynamically scale up/down, and track metrics across
    all workers.
    
    Attributes:
        name: Name标识符 for the pool
        queue: The task queue shared by all workers
        handler: Handler function to execute tasks
        initial_workers: Initial number of workers to create
        retry_policy: Optional retry policy for failed tasks
        metrics_collector: Optional metrics collector for tracking
        polling_interval: Seconds between queue polls for workers
    """
    
    def __init__(
        self,
        name: str,
        queue: TaskQueue,
        handler: Callable[[Task], Any],
        initial_workers: int = 4,
        retry_policy: Optional[RetryPolicy] = None,
        metrics_collector: Optional[MetricsCollector] = None,
        polling_interval: float = 0.1
    ):
        """
        Initialize a new WorkerPool.
        
        Args:
            name: Name identifier for the pool
            queue: TaskQueue shared by all workers
            handler: Handler function to execute tasks
            initial_workers: Initial number of workers to create
            retry_policy: Optional retry policy for failed tasks
            metrics_collector: Optional metrics collector
            polling_interval: Seconds between queue polls for workers
        """
        self.name = name
        self.queue = queue
        self.handler = handler
        self.initial_workers = initial_workers
        self.retry_policy = retry_policy
        self.metrics_collector = metrics_collector
        self.polling_interval = polling_interval
        
        self._workers: Dict[str, Worker] = {}
        self._worker_counter: int = 0
        self._lock = threading.Lock()
        self._is_running: bool = False
    
    def start_all(self) -> None:
        """Start all workers in the pool."""
        with self._lock:
            if self._is_running:
                return
            
            self._is_running = True
            
            # Create initial workers
            with self._lock:
                for _ in range(self.initial_workers):
                    worker_id = self._generate_worker_id()
                    worker = Worker(
                        worker_id=worker_id,
                        queue=self.queue,
                        handler=self.handler,
                        retry_policy=self.retry_policy,
                        metrics_collector=self.metrics_collector,
                        polling_interval=self.polling_interval
                    )
                    self._workers[worker_id] = worker
            
            # Start all workers
            for worker in self._workers.values():
                worker.start()
    
    def stop_all(self) -> None:
        """Stop all workers in the pool cleanly."""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False
        
        # Stop all workers
        workers_to_stop = list(self._workers.values())
        for worker in workers_to_stop:
            worker.stop()
        
        # Clear workers dictionary after stopping
        with self._lock:
            self._workers.clear()
    
    def scale_up(self, n: int) -> int:
        """
        Scale up the pool by adding n workers.
        
        Args:
            n: Number of workers to add
            
        Returns:
            New total number of active workers
        """
        if n <= 0:
            return self.active_workers_count
        
        with self._lock:
            if not self._is_running:
                return 0
            
            new_workers = []
            for _ in range(n):
                worker_id = self._generate_worker_id()
                worker = Worker(
                    worker_id=worker_id,
                    queue=self.queue,
                    handler=self.handler,
                    retry_policy=self.retry_policy,
                    metrics_collector=self.metrics_collector,
                    polling_interval=self.polling_interval
                )
                self._workers[worker_id] = worker
                new_workers.append(worker)
            
            # Start new workers outside the lock
            new_total = len(self._workers)
        
        for worker in new_workers:
            worker.start()
        
        return new_total
    
    def scale_down(self, n: int) -> int:
        """
        Scale down the pool by removing n workers.
        
        Args:
            n: Number of workers to remove
            
        Returns:
            New total number of active workers
        """
        if n <= 0:
            return self.active_workers_count
        
        with self._lock:
            if not self._is_running:
                return 0
            
            # Remove up to n workers
            workers_to_remove = []
            worker_ids = list(self._workers.keys())[:n]
            
            for worker_id in worker_ids:
                if worker_id in self._workers:
                    workers_to_remove.append(self._workers.pop(worker_id))
            
            remaining = len(self._workers)
        
        # Stop removed workers outside the lock
        for worker in workers_to_remove:
            worker.stop()
        
        return remaining
    
    def _generate_worker_id(self) -> str:
        """Generate a unique worker ID."""
        with self._lock:
            self._worker_counter += 1
            return f"{self.name}-worker-{self._worker_counter}"
    
    @property
    def active_workers_count(self) -> int:
        """Get the number of active workers in the pool."""
        with self._lock:
            return len(self._workers)
    
    @property
    def completed_tasks_count(self) -> int:
        """Get the total number of tasks completed across all workers."""
        with self._lock:
            return sum(w.tasks_completed for w in self._workers.values())
    
    @property
    def failed_tasks_count(self) -> int:
        """Get the total number of tasks failed across all workers."""
        with self._lock:
            return sum(w.tasks_failed for w in self._workers.values())
    
    @property
    def is_running(self) -> bool:
        """Check if the pool is currently running."""
        with self._lock:
            return self._is_running
    
    def get_worker_metrics(self) -> List[WorkerMetrics]:
        """
        Get metrics for all workers in the pool.
        
        Returns:
            List of WorkerMetrics for each worker
        """
        with self._lock:
            return [worker.get_metrics() for worker in self._workers.values()]
    
    def get_pool_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the pool status and metrics.
        
        Returns:
            Dictionary containing pool summary information
        """
        with self._lock:
            completed = sum(w.tasks_completed for w in self._workers.values())
            failed = sum(w.tasks_failed for w in self._workers.values())
            
            return {
                "name": self.name,
                "is_running": self._is_running,
                "active_workers": len(self._workers),
                "completed_tasks": completed,
                "failed_tasks": failed,
                "total_tasks": completed + failed
            }