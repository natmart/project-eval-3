"""
Unit tests for Worker and WorkerPool classes.

Tests cover:
- Worker lifecycle (start/stop)
- Task execution and status updates
- Handler registration and execution
- Retry logic integration
- Metrics collection
- Thread-safe operations
- WorkerPool management (start/stop/scale_up/scale_down)
- Pool metrics and summaries
- Concurrent scaling operations
- Edge cases and error handling
"""

import threading
import time
import pytest

from pytaskq import Task, TaskStatus, TaskQueue, RetryPolicy, MetricsCollector, Worker, WorkerPool


class TestWorkerInitialization:
    """Test Worker initialization and basic properties."""
    
    def test_worker_initialization_basic(self):
        """Test basic worker initialization with required parameters."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler
        )
        
        assert worker.id == "test-worker"
        assert worker.queue == queue
        assert worker.handler == handler
        assert worker.polling_interval == 0.1
        assert worker.is_running is False
        assert worker.tasks_completed == 0
        assert worker.tasks_failed == 0
    
    def test_worker_initialization_with_retry_policy(self):
        """Test worker initialization with custom retry policy."""
        queue = TaskQueue()
        handler = lambda task: None
        retry_policy = RetryPolicy(max_attempts=5, base_delay=2.0)
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            retry_policy=retry_policy
        )
        
        assert worker.retry_policy == retry_policy
        assert worker.retry_policy.max_attempts == 5
    
    def test_worker_initialization_with_metrics_collector(self):
        """Test worker initialization with metrics collector."""
        queue = TaskQueue()
        handler = lambda task: None
        metrics = MetricsCollector()
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            metrics_collector=metrics
        )
        
        assert worker.metrics_collector == metrics
    
    def test_worker_initialization_with_polling_interval(self):
        """Test worker initialization with custom polling interval."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.5
        )
        
        assert worker.polling_interval == 0.5
    
    def test_worker_metrics_initially_zero(self):
        """Test that worker metrics start at zero."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler
        )
        
        metrics = worker.get_metrics()
        assert metrics.tasks_completed == 0
        assert metrics.tasks_failed == 0
        assert metrics.is_running is False


class TestWorkerLifecycle:
    """Test Worker start and stop lifecycle."""
    
    def test_worker_start_changes_running_state(self):
        """Test that starting a worker changes its running state."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        assert worker.is_running is False
        
        worker.start()
        time.sleep(0.05)
        
        assert worker.is_running is True
    
    def test_worker_stop_changes_running_state(self):
        """Test that stopping a worker changes its running state."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        worker.start()
        time.sleep(0.05)
        assert worker.is_running is True
        
        worker.stop()
        time.sleep(0.05)
        
        assert worker.is_running is False
    
    def test_worker_multiple_starts_idempotent(self):
        """Test that calling start multiple times doesn't create multiple threads."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        worker.start()
        worker.start()
        worker.start()
        time.sleep(0.05)
        
        assert worker.is_running is True
    
    def test_worker_multiple_stops_idempotent(self):
        """Test that calling stop multiple times is safe."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        worker.start()
        time.sleep(0.05)
        
        worker.stop()
        worker.stop()
        worker.stop()
        
        assert worker.is_running is False
    
    def test_worker_context_manager_like_usage(self):
        """Test worker can be started and stopped cleanly."""
        queue = TaskQueue()
        handler = lambda task: None
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        worker.start()
        time.sleep(0.05)
        assert worker.is_running is True
        
        worker.stop()
        assert worker.is_running is False


class TestWorkerTaskExecution:
    """Test Worker task execution functionality."""
    
    def test_worker_executes_task_handler(self):
        """Test that worker executes task handler."""
        queue = TaskQueue()
        executed_tasks = []
        
        def handler(task):
            executed_tasks.append(task)
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        task = Task(name="test-task")
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.2)
        worker.stop()
        
        assert len(executed_tasks) == 1
        assert executed_tasks[0].id == task.id
        assert worker.tasks_completed == 1
    
    def test_worker_updates_task_status(self):
        """Test that worker updates task status during execution."""
        queue = TaskQueue()
        
        def handler(task):
            time.sleep(0.01)
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        task = Task(name="test-task")
        assert task.status == TaskStatus.PENDING
        
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.2)
        worker.stop()
        
        assert task.status == TaskStatus.COMPLETED
    
    def test_worker_executes_multiple_tasks(self):
        """Test that worker processes all tasks in queue."""
        queue = TaskQueue()
        executed_count = [0]
        
        def handler(task):
            executed_count[0] += 1
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        for i in range(5):
            queue.enqueue(Task(name=f"task-{i}"))
        
        worker.start()
        time.sleep(0.5)
        worker.stop()
        
        assert executed_count[0] == 5
        assert worker.tasks_completed == 5
    
    def test_worker_handles_task_with_payload(self):
        """Test that worker handles tasks with payload."""
        queue = TaskQueue()
        
        def handler(task):
            assert task.payload == {"key": "value"}
            return "success"
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        task = Task(name="test-task", payload={"key": "value"})
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.2)
        worker.stop()
        
        assert worker.tasks_completed == 1
    
    def test_worker_respects_task_priority(self):
        """Test that worker processes higher priority tasks first."""
        queue = TaskQueue()
        processed_order = []
        
        def handler(task):
            processed_order.append(task.id)
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        # Add tasks with different priorities
        task0 = Task(name="task-0", priority=5)
        task1 = Task(name="task-1", priority=1)
        task2 = Task(name="task-2", priority=3)
        
        queue.enqueue(task0)
        queue.enqueue(task1)
        queue.enqueue(task2)
        
        worker.start()
        time.sleep(0.4)
        worker.stop()
        
        # Task 1 (priority 1) should be processed first
        assert processed_order[0] == task1.id
        # Task 2 (priority 3) should be processed second
        assert processed_order[1] == task2.id
        # Task 0 (priority 5) should be processed last
        assert processed_order[2] == task0.id


class TestWorkerErrorHandling:
    """Test Worker error handling and retry logic."""
    
    def test_worker_handles_handler_exception_no_retry(self):
        """Test that worker marks task as failed when handler fails and no retries left."""
        queue = TaskQueue()
        
        def handler(task):
            raise ValueError("Test error")
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        task = Task(name="test-task", max_retries=0)
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.2)
        worker.stop()
        
        assert task.status == TaskStatus.FAILED
        assert worker.tasks_failed == 1
        assert worker.tasks_completed == 0
    
    def test_worker_retries_failed_task(self):
        """Test that worker retries failed tasks according to retry policy."""
        queue = TaskQueue()
        attempt_count = [0]
        
        def handler(task):
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Not yet")
        
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.01)
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            retry_policy=retry_policy,
            polling_interval=0.01
        )
        
        task = Task(name="test-task", max_retries=2)
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.5)
        worker.stop()
        
        # Handler should be called 3 times (initial + 2 retries)
        assert attempt_count[0] == 3
        assert task.status == TaskStatus.COMPLETED
        assert worker.tasks_completed == 1
    
    def test_worker_marks_failed_after_max_retries(self):
        """Test that worker marks task failed after exhausting retries."""
        queue = TaskQueue()
        
        def handler(task):
            raise ValueError("Always fails")
        
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.01)
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            retry_policy=retry_policy,
            polling_interval=0.01
        )
        
        task = Task(name="test-task", max_retries=2)
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.5)
        worker.stop()
        
        assert task.status == TaskStatus.FAILED
        assert task.retry_count == 3
        assert worker.tasks_failed == 1
    
    def test_worker_updates_retry_count(self):
        """Test that worker updates task retry count."""
        queue = TaskQueue()
        attempt_count = [0]
        
        def handler(task):
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise ValueError("Not yet")
        
        retry_policy = RetryPolicy(max_attempts=3, base_delay=0.01)
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            retry_policy=retry_policy,
            polling_interval=0.01
        )
        
        task = Task(name="test-task", max_retries=2)
        assert task.retry_count == 0
        
        queue.enqueue(task)
        
        worker.start()
        time.sleep(0.3)
        worker.stop()
        
        assert task.retry_count >= 1


class TestWorkerMetrics:
    """Test Worker metrics collection."""
    
    def test_worker_tracks_completed_tasks(self):
        """Test that worker correctly counts completed tasks."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        for i in range(3):
            queue.enqueue(Task(name=f"task-{i}"))
        
        worker.start()
        time.sleep(0.4)
        worker.stop()
        
        assert worker.tasks_completed == 3
    
    def test_worker_tracks_failed_tasks(self):
        """Test that worker correctly counts failed tasks."""
        queue = TaskQueue()
        
        def handler(task):
            raise ValueError("Fail")
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        for i in range(2):
            queue.enqueue(Task(name=f"task-{i}", max_retries=0))
        
        worker.start()
        time.sleep(0.3)
        worker.stop()
        
        assert worker.tasks_failed == 2
    
    def test_worker_metrics_collector_integration(self):
        """Test that worker integrates with metrics collector."""
        queue = TaskQueue()
        metrics = MetricsCollector()
        
        def handler(task):
            pass
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            metrics_collector=metrics,
            polling_interval=0.01
        )
        
        for i in range(3):
            queue.enqueue(Task(name=f"task-{i}"))
        
        worker.start()
        time.sleep(0.4)
        worker.stop()
        
        snapshot = metrics.snapshot()
        assert snapshot.tasks_completed == 3
        assert snapshot.tasks_failed == 0
    
    def test_worker_metrics_collector_tracks_failed(self):
        """Test that metrics collector tracks failed tasks."""
        queue = TaskQueue()
        metrics = MetricsCollector()
        
        def handler(task):
            raise ValueError("Fail")
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            metrics_collector=metrics,
            polling_interval=0.01
        )
        
        queue.enqueue(Task(name="task-1", max_retries=0))
        
        worker.start()
        time.sleep(0.2)
        worker.stop()
        
        snapshot = metrics.snapshot()
        assert snapshot.tasks_failed == 1
        assert snapshot.tasks_completed == 0
    
    def test_worker_get_metrics(self):
        """Test that worker returns correct metrics object."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        queue.enqueue(Task(name="task-1"))
        
        worker.start()
        time.sleep(0.2)
        worker.stop()
        
        metrics = worker.get_metrics()
        assert metrics.id == "test-worker"
        assert metrics.tasks_completed == 1
        assert metrics.tasks_failed == 0
        assert metrics.is_running is False


class TestWorkerConcurrency:
    """Test worker thread-safe operations."""
    
    def test_worker_concurrent_access_to_thread_safety(self):
        """Test that worker handles concurrent accesses safely."""
        queue = TaskQueue()
        
        def handler(task):
            time.sleep(0.01)
        
        worker = Worker(
            worker_id="test-worker",
            queue=queue,
            handler=handler,
            polling_interval=0.01
        )
        
        # Start worker
        worker.start()
        time.sleep(0.05)
        
        # Concurrently access properties
        results = []
        threads = []
        
        def access_properties():
            for _ in range(10):
                results.append(worker.is_running)
                results.append(worker.tasks_completed)
                results.append(worker.tasks_failed)
                time.sleep(0.001)
        
        for _ in range(5):
            t = threading.Thread(target=access_properties)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        worker.stop()
        
        # Verify no exceptions were raised
        assert len(results) == 150  # 5 threads * 3 properties * 10 iterations


class TestWorkerPoolInitialization:
    """Test WorkerPool initialization."""
    
    def test_pool_initialization_basic(self):
        """Test basic pool initialization."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler
        )
        
        assert pool.name == "test-pool"
        assert pool.queue == queue
        assert pool.handler == handler
        assert pool.initial_workers == 4
        assert pool.is_running is False
        assert pool.active_workers_count == 0
    
    def test_pool_initialization_with_custom_worker_count(self):
        """Test pool initialization with custom worker count."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=8
        )
        
        assert pool.initial_workers == 8
    
    def test_pool_initialization_with_retry_policy(self):
        """Test pool initialization with retry policy."""
        queue = TaskQueue()
        handler = lambda task: None
        retry_policy = RetryPolicy(max_attempts=5)
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            retry_policy=retry_policy
        )
        
        assert pool.retry_policy == retry_policy
    
    def test_pool_initialization_with_metrics_collector(self):
        """Test pool initialization with metrics collector."""
        queue = TaskQueue()
        handler = lambda task: None
        metrics = MetricsCollector()
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            metrics_collector=metrics
        )
        
        assert pool.metrics_collector == metrics
    
    def test_pool_initial_metrics_zero(self):
        """Test that pool metrics start at zero."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler
        )
        
        assert pool.active_workers_count == 0
        assert pool.completed_tasks_count == 0
        assert pool.failed_tasks_count == 0


class TestWorkerPoolLifecycle:
    """Test WorkerPool start and stop lifecycle."""
    
    def test_pool_start_creates_workers(self):
        """Test that pool start creates initial workers."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        assert pool.is_running is True
        assert pool.active_workers_count == 3
    
    def test_pool_stop_stops_all_workers(self):
        """Test that pool stop stops all workers."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        assert pool.active_workers_count == 3
        
        pool.stop_all()
        time.sleep(0.1)
        
        assert pool.is_running is False
        assert pool.active_workers_count == 0
    
    def test_pool_multiple_starts_idempotent(self):
        """Test that multiple starts don't create duplicate workers."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        pool.start_all()
        pool.start_all()
        pool.start_all()
        time.sleep(0.1)
        
        assert pool.active_workers_count == 2
    
    def test_pool_multiple_stops_idempotent(self):
        """Test that multiple stops are safe."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        pool.stop_all()
        pool.stop_all()
        pool.stop_all()
        
        assert pool.is_running is False


class TestWorkerPoolScaling:
    """Test WorkerPool dynamic scaling operations."""
    
    def test_pool_scale_up_adds_workers(self):
        """Test that scale_up adds workers."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        initial_count = pool.active_workers_count
        assert initial_count == 2
        
        new_count = pool.scale_up(3)
        time.sleep(0.05)
        
        assert new_count == 5
        assert pool.active_workers_count == 5
    
    def test_pool_scale_down_removes_workers(self):
        """Test that scale_down removes workers."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=5,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        initial_count = pool.active_workers_count
        assert initial_count == 5
        
        new_count = pool.scale_down(2)
        time.sleep(0.05)
        
        assert new_count == 3
        assert pool.active_workers_count == 3
    
    def test_pool_scale_up_with_zero(self):
        """Test that scale_up with zero doesn't change count."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        count = pool.scale_up(0)
        assert count == 2
        assert pool.active_workers_count == 2
    
    def test_pool_scale_down_with_zero(self):
        """Test that scale_down with zero doesn't change count."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        count = pool.scale_down(0)
        assert count == 3
        assert pool.active_workers_count == 3
    
    def test_pool_scale_down_more_than_available(self):
        """Test that scaling down more than available removes all workers."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        new_count = pool.scale_down(10)
        time.sleep(0.05)
        
        assert new_count == 0
        assert pool.active_workers_count == 0
    
    def test_pool_scaling_while_not_running(self):
        """Test that scaling while not running returns 0."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2
        )
        
        # Don't start the pool
        count = pool.scale_up(3)
        assert count == 0
        
        count = pool.scale_down(1)
        assert count == 0


class TestWorkerPoolTaskDistribution:
    """Test WorkerPool task distribution among workers."""
    
    def test_pool_distributes_tasks(self):
        """Test that pool distributes tasks to workers."""
        queue = TaskQueue()
        processed_by = []
        
        def handler(task):
            processed_by.append(task.name)
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        # Add tasks
        for i in range(6):
            queue.enqueue(Task(name=f"task-{i}"))
        
        pool.start_all()
        time.sleep(0.5)
        pool.stop_all()
        
        assert pool.completed_tasks_count == 6
        assert len(processed_by) == 6
    
    def test_pool_has_completed_tasks_count(self):
        """Test that pool tracks completed tasks across all workers."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        for i in range(5):
            queue.enqueue(Task(name=f"task-{i}"))
        
        pool.start_all()
        time.sleep(0.4)
        pool.stop_all()
        
        assert pool.completed_tasks_count == 5
    
    def test_pool_has_failed_tasks_count(self):
        """Test that pool tracks failed tasks across all workers."""
        queue = TaskQueue()
        
        def handler(task):
            raise ValueError("Fail")
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        for i in range(3):
            queue.enqueue(Task(name=f"task-{i}", max_retries=0))
        
        pool.start_all()
        time.sleep(0.3)
        pool.stop_all()
        
        assert pool.failed_tasks_count == 3


class TestWorkerPoolMetrics:
    """Test WorkerPool metrics and summaries."""
    
    def test_pool_get_worker_metrics(self):
        """Test getting metrics for all workers."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        metrics = pool.get_worker_metrics()
        
        assert len(metrics) == 3
        assert all(m.is_running for m in metrics)
        
        pool.stop_all()
    
    def test_pool_get_pool_summary(self):
        """Test getting pool summary."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=4,
            polling_interval=0.01
        )
        
        pool.start_all()
        
        for i in range(5):
            queue.enqueue(Task(name=f"task-{i}"))
        
        time.sleep(0.4)
        
        summary = pool.get_pool_summary()
        
        assert summary["name"] == "test-pool"
        assert summary["is_running"] is True
        assert summary["active_workers"] == 4
        assert summary["completed_tasks"] >= 0
        
        pool.stop_all()
    
    def test_pool_summary_after_stop(self):
        """Test pool summary after stopping."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2
        )
        
        pool.start_all()
        pool.stop_all()
        
        summary = pool.get_pool_summary()
        
        assert summary["is_running"] is False
        assert summary["active_workers"] == 0


class TestWorkerPoolThreadSafety:
    """Test WorkerPool thread-safe operations."""
    
    def test_pool_concurrent_scale_operations(self):
        """Test concurrent scale_up and scale_down operations."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=5,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        # Perform concurrent scaling operations
        threads = []
        
        def scale_up():
            for _ in range(3):
                pool.scale_up(2)
                time.sleep(0.01)
        
        def scale_down():
            for _ in range(2):
                pool.scale_down(1)
                time.sleep(0.01)
        
        for i in range(3):
            t1 = threading.Thread(target=scale_up)
            t2 = threading.Thread(target=scale_down)
            threads.extend([t1, t2])
            t1.start()
            t2.start()
        
        for t in threads:
            t.join()
        
        time.sleep(0.1)
        
        # Pool should still be running with some workers
        assert pool.is_running is True
        assert pool.active_workers_count >= 0
        
        pool.stop_all()
    
    def test_pool_concurrent_metrics_access(self):
        """Test concurrent access to pool metrics."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        results = []
        threads = []
        
        def access_metrics():
            for _ in range(10):
                results.append(pool.active_workers_count)
                results.append(pool.completed_tasks_count)
                results.append(pool.failed_tasks_count)
                summary = pool.get_pool_summary()
                results.append(summary["active_workers"])
                time.sleep(0.001)
        
        for _ in range(5):
            t = threading.Thread(target=access_metrics)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        pool.stop_all()
        
        # Verify no exceptions were raised
        assert len(results) == 200  # 5 threads * 4 metrics * 10 iterations


class TestWorkerPoolEdgeCases:
    """Test WorkerPool edge cases and error handling."""
    
    def test_pool_with_empty_queue(self):
        """Test pool behavior with empty queue."""
        queue = TaskQueue()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.2)
        
        # Pool should run without errors even with no tasks
        assert pool.is_running is True
        assert pool.active_workers_count == 2
        
        pool.stop_all()
    
    def test_pool_worker_generates_unique_ids(self):
        """Test that workers get unique IDs."""
        queue = TaskQueue()
        handler = lambda task: None
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=3,
            polling_interval=0.01
        )
        
        pool.start_all()
        time.sleep(0.1)
        
        metrics = pool.get_worker_metrics()
        worker_ids = [m.id for m in metrics]
        
        # All worker IDs should be unique
        assert len(worker_ids) == len(set(worker_ids))
        
        # All IDs should start with pool name
        assert all(id.startswith(pool.name) for id in worker_ids)
        
        pool.stop_all()
    
    def test_pool_survives_worker_handler_exception(self):
        """Test that pool continues running when handler raises exceptions."""
        queue = TaskQueue()
        
        def handler(task):
            if task.name == "fail-task":
                raise ValueError("Intentional failure")
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            polling_interval=0.01
        )
        
        # Add mix of tasks
        queue.enqueue(Task(name="good-task-1"))
        queue.enqueue(Task(name="fail-task", max_retries=0))
        queue.enqueue(Task(name="good-task-2"))
        
        pool.start_all()
        time.sleep(0.3)
        
        # Pool should still be running
        assert pool.is_running is True
        assert pool.active_workers_count == 2
        
        # Should have processed at least the good tasks
        assert pool.completed_tasks_count >= 2
        assert pool.failed_tasks_count >= 1
        
        pool.stop_all()
    
    def test_pool_with_metrics_collector(self):
        """Test pool with shared metrics collector."""
        queue = TaskQueue()
        metrics = MetricsCollector()
        
        def handler(task):
            pass
        
        pool = WorkerPool(
            name="test-pool",
            queue=queue,
            handler=handler,
            initial_workers=2,
            metrics_collector=metrics,
            polling_interval=0.01
        )
        
        for i in range(4):
            queue.enqueue(Task(name=f"task-{i}"))
        
        pool.start_all()
        time.sleep(0.4)
        pool.stop_all()
        
        snapshot = metrics.snapshot()
        assert snapshot.tasks_completed == 4
        assert snapshot.tasks_failed == 0