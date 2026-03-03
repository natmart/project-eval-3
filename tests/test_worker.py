"""
Tests for Worker
"""

import time
import threading
import pytest
from unittest.mock import Mock, patch, MagicMock

from pytaskq import Worker, WorkerPool, Task, TaskStatus, TaskQueue, MetricsCollector, RetryPolicy, RetryError


class TestWorkerInitialization:
    """Tests for Worker initialization."""
    
    def test_worker_initialization(self):
        """Test that a worker can be initialized with required parameters."""
        queue = TaskQueue()
        worker = Worker(
            worker_id="test-worker",
            queue=queue
        )
        
        assert worker.worker_id == "test-worker"
        assert worker.queue == queue
        assert not worker.is_running()
        assert worker.handlers == {}
    
    def test_worker_initialization_with_handlers(self):
        """Test worker initialization with handler functions."""
        def handler(task):
            return "result"
        
        queue = TaskQueue()
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handlers={"task1": handler}
        )
        
        assert worker.handlers == {"task1": handler}
    
    def test_worker_initialization_with_metrics(self):
        """Test worker initialization with metrics collector."""
        queue = TaskQueue()
        metrics = MetricsCollector()
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            metrics=metrics
        )
        
        assert worker.metrics == metrics
    
    def test_worker_initialization_with_retry_policy(self):
        """Test worker initialization with custom retry policy."""
        queue = TaskQueue()
        policy = RetryPolicy(max_attempts=5, base_delay=2.0)
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            retry_policy=policy
        )
        
        assert worker.retry_policy == policy
        assert worker.retry_policy.max_attempts == 5
        assert worker.retry_policy.base_delay == 2.0


class TestWorkerStartStop:
    """Tests for worker start/stop lifecycle."""
    
    def test_start_worker(self):
        """Test that a worker can be started."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        assert not worker.is_running()
        
        worker.start()
        
        # Give the worker time to start
        time.sleep(0.1)
        
        assert worker.is_running()
        
        # Clean up
        worker.stop(timeout=2.0)
    
    def test_stop_worker(self):
        """Test that a worker can be stopped."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        worker.start()
        time.sleep(0.1)
        
        assert worker.is_running()
        
        worker.stop(timeout=2.0)
        time.sleep(0.1)
        
        assert not worker.is_running()
    
    def test_restart_worker(self):
        """Test that a worker can be restarted."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        # First run
        worker.start()
        time.sleep(0.1)
        assert worker.is_running()
        worker.stop(timeout=2.0)
        time.sleep(0.1)
        assert not worker.is_running()
        
        # Second run
        worker.start()
        time.sleep(0.1)
        assert worker.is_running()
        worker.stop(timeout=2.0)
    
    def test_stop_when_not_running(self):
        """Test stopping a worker that is not running."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        # Should not raise an exception
        worker.stop()
    
    def test_start_when_already_running(self):
        """Test starting a worker that is already running."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        worker.start()
        time.sleep(0.1)
        
        # Should not raise an exception
        worker.start()
        worker.stop(timeout=2.0)


class TestWorkerHandlerRegistration:
    """Tests for handler registration."""
    
    def test_register_handler(self):
        """Test registering a handler for a task."""
        def handler(task):
            return "result"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        worker.register_handler("task1", handler)
        
        assert worker.handlers["task1"] == handler
    
    def test_register_multiple_handlers(self):
        """Test registering multiple handlers."""
        def handler1(task):
            return "result1"
        
        def handler2(task):
            return "result2"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        worker.register_handler("task1", handler1)
        worker.register_handler("task2", handler2)
        
        assert len(worker.handlers) == 2
        assert worker.handlers["task1"] == handler1
        assert worker.handlers["task2"] == handler2
    
    def test_replace_handler(self):
        """Test replacing an existing handler."""
        def handler1(task):
            return "result1"
        
        def handler2(task):
            return "result2"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        worker.register_handler("task1", handler1)
        worker.register_handler("task1", handler2)
        
        assert worker.handlers["task1"] == handler2


class TestWorkerTaskExecution:
    """Tests for task execution."""
    
    def test_execute_task_success(self):
        """Test successful task execution."""
        results = []
        
        def handler(task):
            results.append(task.id)
            return "success"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        task = Task(name="test", payload={"data": "test"})
        
        worker.start()
        queue.put(task)
        
        # Wait for task to be processed
        time.sleep(0.5)
        
        worker.stop(timeout=2.0)
        
        assert len(results) == 1
        assert results[0] == task.id
        assert task.status == TaskStatus.COMPLETED
    
    def test_execute_task_with_handler_result(self):
        """Test that handler returns correct result."""
        def handler(task):
            return task.payload * 2
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        task = Task(name="test", payload=21)
        
        worker.start()
        queue.put(task)
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        assert task.status == TaskStatus.COMPLETED
    
    def test_execute_task_without_handler(self):
        """Test executing a task with no registered handler."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        
        task = Task(name="unknown_task")
        
        worker.start()
        queue.put(task)
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        assert task.status == TaskStatus.FAILED
    
    def test_execute_multiple_tasks(self):
        """Test executing multiple tasks."""
        results = []
        
        def handler(task):
            results.append(task.id)
            return "success"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        tasks = [Task(name="test") for _ in range(5)]
        for task in tasks:
            queue.put(task)
        
        worker.start()
        
        # Wait for all tasks to be processed
        time.sleep(1.0)
        
        worker.stop(timeout=2.0)
        
        assert len(results) == 5
        assert all(task.status == TaskStatus.COMPLETED for task in tasks)
    
    def test_execute_task_with_exception(self):
        """Test handling exception in task handler."""
        def handler(task):
            raise ValueError("Test exception")
        
        queue = TaskQueue()
        metrics = MetricsCollector()
        worker = Worker(worker_id="test-worker", queue=queue, metrics=metrics)
        worker.register_handler("test", handler)
        
        task = Task(name="test")
        
        worker.start()
        queue.put(task)
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        assert task.status == TaskStatus.FAILED
        assert metrics.tasks_failed == 1
    
    def test_task_status_transitions(self):
        """Test that task status transitions correctly."""
        status_transitions = []
        
        def handler(task):
            status_transitions.append(("handler_start", task.status))
            return "success"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        task = Task(name="test")
        
        assert task.status == TaskStatus.PENDING
        
        worker.start()
        queue.put(task)
        
        time.sleep(0.5)
        
        # After processing
        assert task.status == TaskStatus.COMPLETED
        
        worker.stop(timeout=2.0)


class TestWorkerRetryLogic:
    """Tests for retry logic integration."""
    
    def test_retry_on_failure(self):
        """Test that failed tasks are retried."""
        call_count = [0]
        
        def handler(task):
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("Not yet")
            return "success"
        
        queue = TaskQueue()
        policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        worker = Worker(worker_id="test-worker", queue=queue, retry_policy=policy)
        worker.register_handler("test", handler)
        
        task = Task(name="test", max_retries=2)
        
        worker.start()
        queue.put(task)
        
        # Wait for retries
        time.sleep(1.0)
        
        worker.stop(timeout=2.0)
        
        # Should have been called multiple times
        assert call_count[0] >= 2
        assert task.retry_count >= 1
    
    def test_exhaust_retries(self):
        """Test that tasks fail after exhausting retries."""
        def handler(task):
            raise ValueError("Always fails")
        
        queue = TaskQueue()
        policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        metrics = MetricsCollector()
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            retry_policy=policy,
            metrics=metrics
        )
        worker.register_handler("test", handler)
        
        task = Task(name="test", max_retries=2)
        
        worker.start()
        queue.put(task)
        
        # Wait for retries to exhaust
        time.sleep(1.0)
        
        worker.stop(timeout=2.0)
        
        assert task.status == TaskStatus.FAILED
        assert task.retry_count == task.max_retries
        assert metrics.tasks_failed == 1
    
    def test_no_retry_on_special_exceptions(self):
        """Test that certain exceptions don't cause retry."""
        class CriticalError(Exception):
            pass
        
        call_count = [0]
        
        def handler(task):
            call_count[0] += 1
            raise CriticalError("Critical failure")
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        task = Task(name="test", max_retries=3)
        
        worker.start()
        queue.put(task)
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        # Should fail immediately without retry
        assert call_count[0] == 1
        assert task.status == TaskStatus.FAILED


class TestWorkerMetrics:
    """Tests for metrics tracking."""
    
    def test_metrics_completed(self):
        """Test that completed tasks are counted."""
        def handler(task):
            return "success"
        
        queue = TaskQueue()
        metrics = MetricsCollector()
        worker = Worker(worker_id="test-worker", queue=queue, metrics=metrics)
        worker.register_handler("test", handler)
        
        tasks = [Task(name="test") for _ in range(3)]
        for task in tasks:
            queue.put(task)
        
        worker.start()
        
        time.sleep(1.0)
        worker.stop(timeout=2.0)
        
        assert metrics.tasks_completed == 3
    
    def test_metrics_failed(self):
        """Test that failed tasks are counted."""
        def handler(task):
            raise ValueError("Test error")
        
        queue = TaskQueue()
        metrics = MetricsCollector()
        worker = Worker(worker_id="test-worker", queue=queue, metrics=metrics)
        worker.register_handler("test", handler)
        
        task = Task(name="test", max_retries=0)
        queue.put(task)
        
        worker.start()
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        assert metrics.tasks_failed == 1
    
    def test_metrics_duration(self):
        """Test that task duration is tracked."""
        def handler(task):
            time.sleep(0.1)
            return "success"
        
        queue = TaskQueue()
        metrics = MetricsCollector()
        worker = Worker(worker_id="test-worker", queue=queue, metrics=metrics)
        worker.register_handler("test", handler)
        
        task = Task(name="test")
        queue.put(task)
        
        worker.start()
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        assert metrics.average_duration > 0
        assert metrics.duration_count >= 1


class TestWorkerPriority:
    """Tests for priority-based task execution."""
    
    def test_priority_order(self):
        """Test that higher priority tasks are executed first."""
        execution_order = []
        
        def handler(task):
            execution_order.append(task.priority)
            return "success"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        # Add tasks with different priorities
        tasks = [
            Task(name="test", priority=5),
            Task(name="test", priority=1),
            Task(name="test", priority=3),
            Task(name="test", priority=2),
            Task(name="test", priority=4),
        ]
        
        for task in tasks:
            queue.put(task)
        
        worker.start()
        
        time.sleep(1.0)
        worker.stop(timeout=2.0)
        
        # Tasks should be executed in priority order (1, 2, 3, 4, 5)
        assert execution_order == [1, 2, 3, 4, 5]


class TestWorkerPool:
    """Tests for WorkerPool."""
    
    def test_worker_pool_initialization(self):
        """Test that a worker pool can be initialized."""
        queue = TaskQueue()
        pool = WorkerPool(num_workers=3, queue=queue)
        
        assert pool.num_workers == 3
        assert len(pool.workers) == 3
        assert pool.queue == queue
    
    def test_worker_pool_start_stop(self):
        """Test starting and stopping a worker pool."""
        queue = TaskQueue()
        pool = WorkerPool(num_workers=2, queue=queue)
        
        assert not pool.is_running()
        
        pool.start()
        time.sleep(0.1)
        
        assert pool.is_running()
        
        pool.stop(timeout=2.0)
        time.sleep(0.1)
        
        assert not pool.is_running()
    
    def test_worker_pool_task_distribution(self):
        """Test that tasks are distributed among workers."""
        results = []
        
        def handler(task):
            results.append(task.id)
            return "success"
        
        pool = WorkerPool(num_workers=3)
        pool.register_handler("test", handler)
        
        tasks = [Task(name="test") for _ in range(6)]
        for task in tasks:
            pool.submit_task(task)
        
        pool.start()
        
        time.sleep(1.0)
        pool.stop(timeout=2.0)
        
        assert len(results) == 6
        assert all(task.status == TaskStatus.COMPLETED for task in tasks)
    
    def test_worker_pool_register_handler(self):
        """Test registering handlers on all workers."""
        def handler(task):
            return "success"
        
        pool = WorkerPool(num_workers=3)
        pool.register_handler("test", handler)
        
        # All workers should have the handler
        for worker in pool.workers:
            assert "test" in worker.handlers
            assert worker.handlers["test"] == handler
    
    def test_worker_pool_metrics(self):
        """Test that metrics work with worker pool."""
        def handler(task):
            return "success"
        
        metrics = MetricsCollector()
        pool = WorkerPool(num_workers=2, metrics=metrics)
        pool.register_handler("test", handler)
        
        tasks = [Task(name="test") for _ in range(4)]
        for task in tasks:
            pool.submit_task(task)
        
        pool.start()
        
        time.sleep(1.0)
        pool.stop(timeout=2.0)
        
        assert metrics.tasks_submitted == 4
        assert metrics.tasks_completed == 4
    
    def test_worker_pool_queue_size(self):
        """Test getting queue size from worker pool."""
        pool = WorkerPool(num_workers=2)
        
        assert pool.get_queue_size() == 0
        
        pool.submit_task(Task(name="test"))
        pool.submit_task(Task(name="test"))
        pool.submit_task(Task(name="test"))
        
        assert pool.get_queue_size() == 3


class TestWorkerThreadSafety:
    """Tests for thread-safety of worker operations."""
    
    def test_concurrent_handler_registration(self):
        """Test that handlers can be registered concurrently."""
        worker = Worker(worker_id="test-worker", queue=TaskQueue())
        
        def handler(task):
            return "success"
        
        def register_handler(i):
            worker.register_handler(f"task{i}", handler)
        
        threads = [threading.Thread(target=register_handler, args=(i,)) for i in range(10)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All handlers should be registered without errors
        assert len(worker.handlers) == 10
    
    def test_concurrent_task_submission(self):
        """Test that tasks can be submitted concurrently."""
        pool = WorkerPool(num_workers=3)
        pool.register_handler("test", lambda task: "success")
        
        def submit_task(i):
            pool.submit_task(Task(name="test", payload=i))
        
        threads = [threading.Thread(target=submit_task, args=(i,)) for i in range(20)]
        
        pool.start()
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        time.sleep(1.0)
        pool.stop(timeout=2.0)
        
        assert pool.metrics.tasks_submitted >= 20

class TestWorkerEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_empty_queue_poll(self):
        """Test that worker waits when queue is empty."""
        worker = Worker(worker_id="test-worker", queue=TaskQueue())
        worker.poll_interval = 0.01
        
        worker.start()
        
        # Worker should stay alive even with empty queue
        for _ in range(5):
            assert worker.is_running()
            time.sleep(0.02)
        
        worker.stop(timeout=2.0)
    
    def test_worker_id_uniqueness(self):
        """Test that workers have unique IDs."""
        queue = TaskQueue()
        pool = WorkerPool(num_workers=5, queue=queue)
        
        worker_ids = [worker.worker_id for worker in pool.workers]
        
        assert len(worker_ids) == len(set(worker_ids))
    
    def test_task_with_none_payload(self):
        """Test handling tasks with None payload."""
        def handler(task):
            assert task.payload is None
            return "success"
        
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue)
        worker.register_handler("test", handler)
        
        task = Task(name="test", payload=None)
        
        worker.start()
        queue.put(task)
        
        time.sleep(0.5)
        worker.stop(timeout=2.0)
        
        assert task.status == TaskStatus.COMPLETED
    
    def test_worker_with_custom_poll_interval(self):
        """Test worker with custom poll interval."""
        queue = TaskQueue()
        worker = Worker(worker_id="test-worker", queue=queue, poll_interval=0.5)
        
        assert worker.poll_interval == 0.5
        
        worker.start()
        worker.stop(timeout=2.0)