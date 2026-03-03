"""
Worker Implementation

This module provides the worker pool for executing tasks concurrently.
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pytaskq.queue import TaskQueue
from pytaskq.retry import RetryPolicy
from pytaskq.task import Task, TaskStatus


@dataclass
class WorkerMetrics:
    """Metrics for tracking worker performance."""
    tasks_processed: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_execution_time: float = 0.0
    start_time: Optional[datetime] = None
    is_active: bool = False


class Worker:
    """
    Worker for executing tasks from a task queue.

    The worker continuously polls the queue for tasks, executes them using
    registered handler functions, updates task status, and handles retries.

    Attributes:
        queue: The task queue to pull tasks from
        name: Human-readable name for this worker
        poll_interval: Seconds to wait between queue polls
        metrics: Metrics tracking worker performance
        retry_policy: Policy for handling failed tasks
    """

    def __init__(
        self,
        queue: TaskQueue,
        name: str = "Worker",
        poll_interval: float = 0.1,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        """
        Initialize a new Worker.

        Args:
            queue: The task queue to pull tasks from
            name: Human-readable name for this worker
            poll_interval: Seconds to wait between queue polls (default: 0.1)
            retry_policy: Policy for handling failed tasks (default: None)
        """
        self.queue = queue
        self.name = name
        self.poll_interval = poll_interval
        self.retry_policy = retry_policy or RetryPolicy()

        self._handlers: Dict[str, Callable[[Task], Any]] = {}
        self._default_handler: Optional[Callable[[Task], Any]] = None

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self.metrics = WorkerMetrics()

    def register_handler(self, task_name: str, handler: Callable[[Task], Any]) -> None:
        """
        Register a handler function for a specific task name.

        Args:
            task_name: The name of the task this handler processes
            handler: Function to call when processing the task (receives Task, returns any)
        """
        with self._lock:
            self._handlers[task_name] = handler

    def register_default_handler(self, handler: Callable[[Task], Any]) -> None:
        """
        Register a default handler for tasks without specific handlers.

        Args:
            handler: Function to call when processing unspecificed tasks
        """
        with self._lock:
            self._default_handler = handler

    def _get_handler(self, task: Task) -> Optional[Callable[[Task], Any]]:
        """Get the appropriate handler for a task."""
        with self._lock:
            return self._handlers.get(task.name, self._default_handler)

    def start(self) -> None:
        """Start the worker in a separate thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self.metrics.start_time = datetime.utcnow()
            self.metrics.is_active = True

        self._thread = threading.Thread(target=self._run_loop, name=self.name, daemon=True)
        self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        """
        Stop the worker gracefully.

        Args:
            timeout: Maximum seconds to wait for worker to stop (None = wait indefinitely)
        """
        with self._lock:
            if not self._running:
                return
            self._running = False

        if self._thread:
            self._thread.join(timeout=timeout)

        with self._lock:
            self.metrics.is_active = False

    def is_running(self) -> bool:
        """Check if the worker is currently running."""
        with self._lock:
            return self._running

    def _run_loop(self) -> None:
        """Main worker loop that pulls and executes tasks."""
        while self._running:
            task = self.queue.dequeue()
            if task:
                self._execute_task(task)
            else:
                time.sleep(self.poll_interval)

    def _execute_task(self, task: Task) -> None:
        """
        Execute a single task with status updates and retry handling.

        Args:
            task: The task to execute
        """
        handler = self._get_handler(task)
        if handler is None:
            # No handler available, mark task as failed
            task.status = TaskStatus.FAILED
            task.retry_count += 1
            with self._lock:
                self.metrics.tasks_processed += 1
                self.metrics.tasks_failed += 1
            return

        # Mark task as running
        task.status = TaskStatus.RUNNING
        start_time = time.time()

        try:
            # Execute the handler
            result = handler(task)

            # Check if handler returned a result indicating failure
            if result is False:
                raise Exception("Handler returned False, indicating failure")

            # Task completed successfully
            task.status = TaskStatus.COMPLETED
            task.retry_count = 0

            execution_time = time.time() - start_time
            with self._lock:
                self.metrics.tasks_processed += 1
                self.metrics.tasks_completed += 1
                self.metrics.total_execution_time += execution_time

        except Exception as e:
            # Task execution failed
            execution_time = time.time() - start_time
            task.retry_count += 1

            # Check if we should retry
            should_retry = (
                task.retry_count < task.max_retries
                and task.retry_count < self.retry_policy.max_attempts
            )

            if should_retry:
                # Calculate backoff delay
                delay = self.retry_policy.calculate_delay(task.retry_count - 1)
                time.sleep(delay)

                # Mark task as pending for retry
                task.status = TaskStatus.PENDING
                self.queue.enqueue(task)

                with self._lock:
                    self.metrics.tasks_processed += 1

            else:
                # Max retries exceeded, mark as failed
                task.status = TaskStatus.FAILED
                with self._lock:
                    self.metrics.tasks_processed += 1
                    self.metrics.tasks_failed += 1
                    self.metrics.total_execution_time += execution_time


class WorkerPool:
    """
    Pool for managing multiple worker instances.

    The WorkerPool allows scaling the number of workers and provides
    collective control over starting, stopping, and monitoring workers.

    Attributes:
        queue: The shared task queue for all workers
        worker_count: Number of workers in the pool
        poll_interval: Poll interval for each worker
        retry_policy: Retry policy for all workers
    """

    def __init__(
        self,
        queue: TaskQueue,
        worker_count: int = 4,
        poll_interval: float = 0.1,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        """
        Initialize a new WorkerPool.

        Args:
            queue: The shared task queue for all workers
            worker_count: Number of workers to create (default: 4)
            poll_interval: Poll interval for each worker (default: 0.1)
            retry_policy: Retry policy for all workers (default: None)
        """
        self.queue = queue
        self.worker_count = worker_count
        self.poll_interval = poll_interval
        self.retry_policy = retry_policy or RetryPolicy()

        self._workers: List[Worker] = []
        self._lock = threading.Lock()

        # Create workers
        for i in range(worker_count):
            worker = Worker(
                queue=self.queue,
                name=f"Worker-{i}",
                poll_interval=self.poll_interval,
                retry_policy=self.retry_policy,
            )
            self._workers.append(worker)

    def register_handler(self, task_name: str, handler: Callable[[Task], Any]) -> None:
        """
        Register a handler function for a specific task name on all workers.

        Args:
            task_name: The name of the task this handler processes
            handler: Function to call when processing the task
        """
        for worker in self._workers:
            worker.register_handler(task_name, handler)

    def register_default_handler(self, handler: Callable[[Task], Any]) -> None:
        """
        Register a default handler for all workers.

        Args:
            handler: Function to call when processing unspecified tasks
        """
        for worker in self._workers:
            worker.register_default_handler(handler)

    def start_all(self) -> None:
        """Start all workers in the pool."""
        for worker in self._workers:
            worker.start()

    def stop_all(self, timeout: Optional[float] = None) -> None:
        """
        Stop all workers in the pool.

        Args:
            timeout: Maximum seconds to wait for each worker to stop
        """
        for worker in self._workers:
            worker.stop(timeout=timeout)

    def scale_up(self, n: int) -> None:
        """
        Add n new workers to the pool.

        Args:
            n: Number of workers to add
        """
        with self._lock:
            current_count = len(self._workers)
            for i in range(n):
                worker = Worker(
                    queue=self.queue,
                    name=f"Worker-{current_count + i}",
                    poll_interval=self.poll_interval,
                    retry_policy=self.retry_policy,
                )
                # Register existing handlers
                if self._workers:
                    self._copy_handlers(self._workers[0], worker)
                self._workers.append(worker)

    def scale_down(self, n: int) -> None:
        """
        Remove n workers from the pool.

        Args:
            n: Number of workers to remove
        """
        with self._lock:
            n = min(n, len(self._workers))
            for _ in range(n):
                if self._workers:
                    worker = self._workers.pop()
                    worker.stop()

    @property
    def active_workers_count(self) -> int:
        """Get the number of active (running) workers."""
        return sum(1 for worker in self._workers if worker.is_running())

    @property
    def completed_tasks_count(self) -> int:
        """Get the total number of completed tasks across all workers."""
        return sum(worker.metrics.tasks_completed for worker in self._workers)

    @property
    def failed_tasks_count(self) -> int:
        """Get the total number of failed tasks across all workers."""
        return sum(worker.metrics.tasks_failed for worker in self._workers)

    def get_worker_metrics(self) -> List[WorkerMetrics]:
        """Get metrics for all workers in the pool."""
        return [worker.metrics for worker in self._workers]

    def get_pool_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of the pool status.

        Returns:
            Dictionary containing pool metrics and status
        """
        return {
            "total_workers": len(self._workers),
            "active_workers": self.active_workers_count,
            "completed_tasks": self.completed_tasks_count,
            "failed_tasks": self.failed_tasks_count,
            "total_processed": sum(
                worker.metrics.tasks_processed for worker in self._workers
            ),
            "total_execution_time": sum(
                worker.metrics.total_execution_time for worker in self._workers
            ),
        }

    def _copy_handlers(self, source: Worker, target: Worker) -> None:
        """Copy handlers from source worker to target worker."""
        with source._lock:
            for task_name, handler in source._handlers.items():
                target.register_handler(task_name, handler)
            if source._default_handler:
                target.register_default_handler(source._default_handler)