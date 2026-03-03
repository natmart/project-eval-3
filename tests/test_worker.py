"""
Tests for Worker

Tests for the Worker class covering execution lifecycle, start/stop behavior,
handler function invocation, error handling, and retry integration.
"""

import time
import threading
from datetime import datetime
from typing import Any

import pytest

from pytaskq import Task, TaskQueue, TaskStatus, Worker, WorkerPool, WorkerMetrics
from pytaskq.retry import RetryPolicy


class TestWorkerInitialization:
    """Test Worker initialization and basic setup."""

    def test_worker_initialization_defaults(self) -> None:
        """Test worker initialization with default values."""
        queue = TaskQueue()
        worker = Worker(queue)

        assert worker.queue == queue
        assert worker.name == "Worker"
        assert worker.poll_interval == 0.1
        assert worker.retry_policy is not None
        assert worker.retry_policy.max_attempts == 3
        assert worker.is_running() is False
        assert worker.metrics.tasks_processed == 0
        assert worker.metrics.tasks_completed == 0
        assert worker.metrics.tasks_failed == 0

    def test_worker_initialization_custom_values(self) -> None:
        """Test worker initialization with custom values."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=5, base_delay=2.0)
        worker = Worker(
            queue=queue,
            name="CustomWorker",
            poll_interval=0.5,
            retry_policy=retry_policy,
        )

        assert worker.name == "CustomWorker"
        assert worker.poll_interval == 0.5
        assert worker.retry_policy.max_attempts == 5
        assert worker.retry_policy.base_delay == 2.0

    def test_worker_metrics_initial_state(self) -> None:
        """Test that worker metrics start at zero."""
        queue = TaskQueue()
        worker = Worker(queue)

        assert worker.metrics.tasks_processed == 0
        assert worker.metrics.tasks_completed == 0
        assert worker.metrics.tasks_failed == 0
        assert worker.metrics.total_execution_time == 0.0
        assert worker.metrics.is_active is False
        assert worker.metrics.start_time is None


class TestWorkerLifecycle:
    """Test worker start/stop lifecycle."""

    def test_worker_start_stop(self) -> None:
        """Test that worker can be started and stopped."""
        queue = TaskQueue()
        worker = Worker(queue)

        assert worker.is_running() is False

        worker.start()
        time.sleep(0.2)  # Give thread time to start
        assert worker.is_running() is True

        worker.stop(timeout=1.0)
        assert worker.is_running() is False

    def test_worker_start_sets_metrics_is_active(self) -> None:
        """Test that starting worker sets metrics.is_active to True."""
        queue = TaskQueue()
        worker = Worker(queue)

        assert worker.metrics.is_active is False

        worker.start()
        time.sleep(0.2)  # Give thread time to start

        assert worker.metrics.is_active is True
        assert worker.metrics.start_time is not None

        worker.stop(timeout=1.0)

    def test_worker_stop_clears_metrics_is_active(self) -> None:
        """Test that stopping worker sets metrics.is_active to False."""
        queue = TaskQueue()
        worker = Worker(queue)

        worker.start()
        time.sleep(0.2)
        assert worker.metrics.is_active is True

        worker.stop(timeout=1.0)
        assert worker.metrics.is_active is False

    def test_worker_multiple_starts_idempotent(self) -> None:
        """Test that starting an already running worker is idempotent."""
        queue = TaskQueue()
        worker = Worker(queue)

        worker.start()
        time.sleep(0.2)
        initial_thread = worker._thread

        # Start again - should not create new thread
        worker.start()
        time.sleep(0.2)

        assert worker._thread == initial_thread
        assert worker.is_running() is True

        worker.stop(timeout=1.0)

    def test_worker_multiple_stops_idempotent(self) -> None:
        """Test that stopping an already stopped worker is safe."""
        queue = TaskQueue()
        worker = Worker(queue)

        worker.start()
        time.sleep(0.2)
        worker.stop(timeout=1.0)

        # Stop again - should not raise error
        worker.stop(timeout=1.0)
        assert worker.is_running() is False

    def test_worker_stop_with_timeout(self) -> None:
        """Test that stop respects timeout parameter."""
        queue = TaskQueue()
        worker = Worker(queue)

        worker.start()
        time.sleep(0.1)

        start = time.time()
        worker.stop(timeout=0.5)
        elapsed = time.time() - start

        assert elapsed < 1.0  # Should return quickly


class TestWorkerTaskExecution:
    """Test worker task execution and status updates."""

    def test_worker_executes_task_with_handler(self) -> None:
        """Test that worker executes task with registered handler."""
        queue = TaskQueue()
        worker = Worker(queue)

        # Track execution
        executed_tasks = []

        def handler(task: Task) -> Any:
            executed_tasks.append(task.id)
            return True

        worker.register_handler("TestTask", handler)

        # Add task
        task = Task(name="TestTask", payload={"data": "test"})
        queue.enqueue(task)

        # Start worker
        worker.start()
        time.sleep(0.5)  # Give worker time to process
        worker.stop(timeout=1.0)

        assert len(executed_tasks) == 1
        assert executed_tasks[0] == task.id
        assert task.status == TaskStatus.COMPLETED
        assert worker.metrics.tasks_completed == 1

    def test_worker_does_not_execute_task_without_handler(self) -> None:
        """Test that task without handler is marked as failed."""
        queue = TaskQueue()
        worker = Worker(queue)

        # Add task without registering handler
        task = Task(name="TestTask", payload={"data": "test"})
        queue.enqueue(task)

        # Start worker
        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.FAILED
        assert worker.metrics.tasks_failed == 1

    def test_worker_executes_task_with_default_handler(self) -> None:
        """Test that worker uses default handler when no specific handler."""
        queue = TaskQueue()
        worker = Worker(queue)

        executed_tasks = []

        def default_handler(task: Task) -> Any:
            executed_tasks.append(task.id)
            return True

        worker.register_default_handler(default_handler)

        # Add task without specific handler
        task1 = Task(name="Task1", payload={})
        task2 = Task(name="Task2", payload={})
        queue.enqueue(task1)
        queue.enqueue(task2)

        # Start worker
        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert len(executed_tasks) == 2
        assert task1.status == TaskStatus.COMPLETED
        assert task2.status == TaskStatus.COMPLETED

    def test_worker_specific_handler_overrides_default(self) -> None:
        """Test that specific handler takes precedence over default handler."""
        queue = TaskQueue()
        worker = Worker(queue)

        executed_by = []

        def default_handler(task: Task) -> Any:
            executed_by.append("default")
            return True

        def specific_handler(task: Task) -> Any:
            executed_by.append("specific")
            return True

        worker.register_default_handler(default_handler)
        worker.register_handler("SpecialTask", specific_handler)

        # Add tasks
        task1 = Task(name="RegularTask", payload={})
        task2 = Task(name="SpecialTask", payload={})
        queue.enqueue(task1)
        queue.enqueue(task2)

        # Start worker
        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert "default" in executed_by
        assert "specific" in executed_by

    def test_worker_updates_task_status_during_execution(self) -> None:
        """Test that worker updates task status throughout lifecycle."""
        queue = TaskQueue()
        worker = Worker(queue)

        status_changes = []

        def handler(task: Task) -> Any:
            status_changes.append(task.status)
            time.sleep(0.01)  # Small delay to ensure status transitions
            status_changes.append(task.status)
            return True

        worker.register_handler("TestTask", handler)

        task = Task(name="TestTask", payload={})
        assert task.status == TaskStatus.PENDING

        queue.enqueue(task)
        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.COMPLETED
        assert TaskStatus.RUNNING in status_changes

    def test_worker_processes_multiple_tasks(self) -> None:
        """Test that worker processes all tasks in queue."""
        queue = TaskQueue()
        worker = Worker(queue)

        executed_count = [0]

        def handler(task: Task) -> Any:
            executed_count[0] += 1
            return True

        worker.register_handler("TestTask", handler)

        # Add multiple tasks
        for i in range(5):
            task = Task(name="TestTask", payload={"index": i})
            queue.enqueue(task)

        # Start worker
        worker.start()
        time.sleep(1.0)  # Give time to process all
        worker.stop(timeout=1.0)

        assert executed_count[0] == 5
        assert worker.metrics.tasks_completed == 5
        assert queue.is_empty() is True


class TestWorkerErrorHandling:
    """Test worker error handling and exception management."""

    def test_worker_handles_handler_exception(self) -> None:
        """Test that worker handles exceptions raised by handler."""
        queue = TaskQueue()
        worker = Worker(queue)

        def failing_handler(task: Task) -> Any:
            raise ValueError("Handler failed")

        worker.register_handler("TestTask", failing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 0  # No retries to simplify test
        queue.enqueue(task)

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.FAILED
        assert worker.metrics.tasks_failed == 1

    def test_worker_handles_handler_returning_false(self) -> None:
        """Test that worker treats handler returning False as failure."""
        queue = TaskQueue()
        worker = Worker(queue)

        def failing_handler(task: Task) -> Any:
            return False

        worker.register_handler("TestTask", failing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 0
        queue.enqueue(task)

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.FAILED
        assert worker.metrics.tasks_failed == 1

    def test_worker_increments_retry_count_on_failure(self) -> None:
        """Test that retry count increments on task failure."""
        queue = TaskQueue()
        worker = Worker(queue)

        def failing_handler(task: Task) -> Any:
            raise Exception("Always fails")

        worker.register_handler("TestTask", failing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 0
        queue.enqueue(task)

        assert task.retry_count == 0

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert task.retry_count > 0

    def test_worker_continues_after_task_failure(self) -> None:
        """Test that worker continues processing after a task failure."""
        queue = TaskQueue()
        worker = Worker(queue)

        executed = []

        def failing_handler(task: Task) -> Any:
            if task.payload.get("should_fail"):
                raise Exception("Task failed")
            executed.append(task.id)
            return True

        worker.register_handler("TestTask", failing_handler)

        # Add tasks - first fails, second succeeds
        task1 = Task(name="TestTask", payload={"should_fail": True})
        task1.max_retries = 0
        task2 = Task(name="TestTask", payload={"should_fail": False})

        queue.enqueue(task1)
        queue.enqueue(task2)

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert task1.status == TaskStatus.FAILED
        assert task2.status == TaskStatus.COMPLETED
        assert len(executed) == 1
        assert executed[0] == task2.id


class TestWorkerRetryIntegration:
    """Test worker retry policy integration."""

    def test_worker_retries_failed_task(self) -> None:
        """Test that worker retries failed task according to policy."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.01)
        worker = Worker(queue, retry_policy=retry_policy)

        attempt_count = [0]

        def flaky_handler(task: Task) -> Any:
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise Exception("Not yet")
            return True

        worker.register_handler("TestTask", flaky_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 5
        queue.enqueue(task)

        worker.start()
        time.sleep(1.0)  # Give time for retries
        worker.stop(timeout=1.0)

        assert attempt_count[0] >= 3  # At least retries + initial
        assert task.status == TaskStatus.COMPLETED
        assert worker.metrics.tasks_completed == 1

    def test_worker_fails_task_after_max_retries(self) -> None:
        """Test that worker marks task as failed after exceeding max retries."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=2, base_delay=0.01)
        worker = Worker(queue, retry_policy=retry_policy)

        def always_failing_handler(task: Task) -> Any:
            raise Exception("Always fails")

        worker.register_handler("TestTask", always_failing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 2
        queue.enqueue(task)

        worker.start()
        time.sleep(1.0)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.FAILED
        assert worker.metrics.tasks_failed == 1
        assert task.retry_count >= 2

    def test_worker_respects_task_max_retries(self) -> None:
        """Test that worker respects task.max_retries over retry_policy."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=10, base_delay=0.01)
        worker = Worker(queue, retry_policy=retry_policy)

        def always_failing_handler(task: Task) -> Any:
            raise Exception("Always fails")

        worker.register_handler("TestTask", always_failing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 2  # Lower than retry_policy
        queue.enqueue(task)

        worker.start()
        time.sleep(1.0)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.FAILED
        # Should fail after task.max_retries, not retry_policy.max_attempts
        assert task.retry_count == 3  # initial + 2 retries

    def test_worker_respects_retry_policy_max_attempts(self) -> None:
        """Test that worker respects retry_policy.max_attempts over task.max_retries."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=2, base_delay=0.01)
        worker = Worker(queue, retry_policy=retry_policy)

        def always_failing_handler(task: Task) -> Any:
            raise Exception("Always fails")

        worker.register_handler("TestTask", always_failing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 10  # Higher than retry_policy
        queue.enqueue(task)

        worker.start()
        time.sleep(1.0)
        worker.stop(timeout=1.0)

        assert task.status == TaskStatus.FAILED
        # Should fail after retry_policy.max_attempts
        assert task.retry_count == 2  # limited by retry_policy

    def test_worker_uses_backoff_delays(self) -> None:
        """Test that worker uses backoff delays between retries."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        worker = Worker(queue, retry_policy=retry_policy)

        attempt_times = []

        def timing_handler(task: Task) -> Any:
            attempt_times.append(time.time())
            raise Exception("Retry required")

        worker.register_handler("TestTask", timing_handler)

        task = Task(name="TestTask", payload={})
        task.max_retries = 2
        queue.enqueue(task)

        start_time = time.time()
        worker.start()
        time.sleep(0.8)  # Wait for retries
        worker.stop(timeout=1.0)

        # Should have at least 2 attempts
        assert len(attempt_times) >= 2
        # Check delay between attempts
        if len(attempt_times) >= 2:
            delay = attempt_times[1] - attempt_times[0]
            # Should have at least the base_delay
            assert delay >= 0.05  # Allow some tolerance


class TestWorkerMetrics:
    """Test worker metrics collection."""

    def test_worker_tracks_completed_tasks(self) -> None:
        """Test that worker increments completed tasks counter."""
        queue = TaskQueue()
        worker = Worker(queue)

        def handler(task: Task) -> Any:
            return True

        worker.register_handler("TestTask", handler)

        for _ in range(5):
            task = Task(name="TestTask", payload={})
            queue.enqueue(task)

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert worker.metrics.tasks_completed == 5

    def test_worker_tracks_failed_tasks(self) -> None:
        """Test that worker increments failed tasks counter."""
        queue = TaskQueue()
        worker = Worker(queue)

        def failing_handler(task: Task) -> Any:
            raise Exception("Failed")

        worker.register_handler("TestTask", failing_handler)

        for i in range(3):
            task = Task(name="TestTask", payload={})
            task.max_retries = 0
            queue.enqueue(task)

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert worker.metrics.tasks_failed == 3

    def test_worker_tracks_total_processed(self) -> None:
        """Test that worker tracks total tasks processed."""
        queue = TaskQueue()
        worker = Worker(queue)

        def handler(task: Task) -> Any:
            return True

        def failing_handler(task: Task) -> Any:
            raise Exception("Failed")

        worker.register_handler("SuccessTask", handler)
        worker.register_handler("FailureTask", failing_handler)

        # Add mix of successful and failing tasks
        for _ in range(3):
            task = Task(name="SuccessTask", payload={})
            queue.enqueue(task)

        for _ in range(2):
            task = Task(name="FailureTask", payload={})
            task.max_retries = 0
            queue.enqueue(task)

        worker.start()
        time.sleep(0.5)
        worker.stop(timeout=1.0)

        assert worker.metrics.tasks_processed == 5
        assert worker.metrics.tasks_completed == 3
        assert worker.metrics.tasks_failed == 2

    def test_worker_tracks_execution_time(self) -> None:
        """Test that worker tracks total execution time."""
        queue = TaskQueue()
        worker = Worker(queue)

        def slow_handler(task: Task) -> Any:
            time.sleep(0.1)
            return True

        worker.register_handler("TestTask", slow_handler)

        for _ in range(3):
            task = Task(name="TestTask", payload={})
            queue.enqueue(task)

        worker.start()
        time.sleep(1.0)
        worker.stop(timeout=1.0)

        # Should have measured execution time for 3 tasks
        assert worker.metrics.total_execution_time >= 0.2  # Allow some tolerance

    def test_worker_start_time_recorded(self) -> None:
        """Test that worker records start time when started."""
        queue = TaskQueue()
        worker = Worker(queue)

        before_start = datetime.utcnow()
        worker.start()
        time.sleep(0.1)
        after_start = datetime.utcnow()
        worker.stop(timeout=1.0)

        assert worker.metrics.start_time is not None
        assert before_start <= worker.metrics.start_time <= after_start


class TestWorkerPoolInitialization:
    """Test WorkerPool initialization."""

    def test_worker_pool_initialization_defaults(self) -> None:
        """Test worker pool initialization with default values."""
        queue = TaskQueue()
        pool = WorkerPool(queue)

        assert pool.queue == queue
        assert pool.worker_count == 4
        assert pool.poll_interval == 0.1
        assert len(pool._workers) == 4
        assert pool.active_workers_count == 0

    def test_worker_pool_initialization_custom(self) -> None:
        """Test worker pool initialization with custom values."""
        queue = TaskQueue()
        retry_policy = RetryPolicy(max_attempts=5)
        pool = WorkerPool(queue, worker_count=2, poll_interval=0.5, retry_policy=retry_policy)

        assert len(pool._workers) == 2
        assert pool.poll_interval == 0.5
        for worker in pool._workers:
            assert worker.retry_policy.max_attempts == 5


class TestWorkerPoolLifecycle:
    """Test WorkerPool lifecycle operations."""

    def test_worker_pool_start_all(self) -> None:
        """Test that pool can start all workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=3)

        pool.start_all()
        time.sleep(0.2)

        assert pool.active_workers_count == 3

        pool.stop_all()

    def test_worker_pool_stop_all(self) -> None:
        """Test that pool can stop all workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=3)

        pool.start_all()
        time.sleep(0.2)

        assert pool.active_workers_count == 3

        pool.stop_all()
        time.sleep(0.1)

        assert pool.active_workers_count == 0

    def test_worker_pool_scale_up(self) -> None:
        """Test that pool can scale up by adding workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=2)

        initial_count = len(pool._workers)
        pool.scale_up(2)

        assert len(pool._workers) == initial_count + 2

    def test_worker_pool_scale_down(self) -> None:
        """Test that pool can scale down by removing workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=5)

        initial_count = len(pool._workers)
        pool.scale_down(2)

        assert len(pool._workers) == initial_count - 2

    def test_worker_pool_scale_down_stops_workers(self) -> None:
        """Test that scaling down stops removed workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=3)

        pool.start_all()
        time.sleep(0.2)
        assert pool.active_workers_count == 3

        pool.scale_down(2)
        time.sleep(0.1)
        assert pool.active_workers_count <= 1

        pool.stop_all()


class TestWorkerPoolHandlers:
    """Test WorkerPool handler registration."""

    def test_worker_pool_register_handler(self) -> None:
        """Test that pool registers handler on all workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=3)

        executed = []

        def handler(task: Task) -> Any:
            executed.append(task.id)
            return True

        pool.register_handler("TestTask", handler)

        # Verify all workers have the handler
        for worker in pool._workers:
            assert worker._get_handler(Task(name="TestTask")) is not None

    def test_worker_pool_register_default_handler(self) -> None:
        """Test that pool registers default handler on all workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=3)

        executed = []

        def default_handler(task: Task) -> Any:
            executed.append(task.id)
            return True

        pool.register_default_handler(default_handler)

        # Verify all workers have the default handler
        for worker in pool._workers:
            assert worker._default_handler is not None


class TestWorkerPoolMetrics:
    """Test WorkerPool metrics collection."""

    def test_worker_pool_completed_tasks_count(self) -> None:
        """Test that pool tracks completed tasks across workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=2)

        def handler(task: Task) -> Any:
            return True

        pool.register_handler("TestTask", handler)

        # Add tasks
        for _ in range(5):
            task = Task(name="TestTask", payload={})
            queue.enqueue(task)

        pool.start_all()
        time.sleep(0.5)
        pool.stop_all()

        assert pool.completed_tasks_count == 5

    def test_worker_pool_failed_tasks_count(self) -> None:
        """Test that pool tracks failed tasks across workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=2)

        def failing_handler(task: Task) -> Any:
            raise Exception("Failed")

        pool.register_handler("TestTask", failing_handler)

        # Add tasks
        for _ in range(3):
            task = Task(name="TestTask", payload={})
            task.max_retries = 0
            queue.enqueue(task)

        pool.start_all()
        time.sleep(0.5)
        pool.stop_all()

        assert pool.failed_tasks_count == 3

    def test_worker_pool_get_worker_metrics(self) -> None:
        """Test that pool returns metrics for all workers."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=3)

        metrics = pool.get_worker_metrics()
        assert len(metrics) == 3
        for metric in metrics:
            assert isinstance(metric, WorkerMetrics)

    def test_worker_pool_get_pool_summary(self) -> None:
        """Test that pool returns comprehensive summary."""
        queue = TaskQueue()
        pool = WorkerPool(queue, worker_count=2)

        def handler(task: Task) -> Any:
            return True

        pool.register_handler("TestTask", handler)

        # Add a task
        task = Task(name="TestTask", payload={})
        queue.enqueue(task)

        pool.start_all()
        time.sleep(0.3)
        pool.stop_all()

        summary = pool.get_pool_summary()

        assert "total_workers" in summary
        assert "active_workers" in summary
        assert "completed_tasks" in summary
        assert "failed_tasks" in summary
        assert "total_processed" in summary
        assert "total_execution_time" in summary

        assert summary["total_workers"] == 2
        assert summary["active_workers"] == 0