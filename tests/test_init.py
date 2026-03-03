"""
Tests for package initialization
"""

import pytest

from pytaskq import (
    # Task and Status
    Task,
    TaskStatus,
    # Queue implementations
    TaskQueue,
    PriorityQueue,
    # Storage
    StorageBackend,
    SQLiteBackend,
    # Workers
    Worker,
    WorkerPool,
    WorkerMetrics,
    # Retry logic
    RetryPolicy,
    with_retry,
    RetryError,
    # Scheduler
    Scheduler,
    # Metrics
    MetricsCollector,
    MetricsSnapshot,
    # Configuration
    Config,
    # Convenience functions
    create_queue,
    # Package metadata
    __version__,
)


class TestPackageExports:
    """Test cases for package exports."""

    def test_version_is_accessible(self):
        """Test that __version__ is accessible and is a string."""
        assert isinstance(__version__, str)
        assert len(__version__) > 0
        assert "." in __version__  # Should be a version like "1.0.0"

    def test_version_format(self):
        """Test that __version__ follows semantic versioning format."""
        parts = __version__.split(".")
        assert len(parts) >= 3  # Should have at least major.minor.patch
        for part in parts:
            assert part.isdigit() or part.replace(".", "").isdigit()

    def test_all_defined(self):
        """Test that __all__ is defined and contains expected items."""
        import pytaskq

        assert hasattr(pytaskq, "__all__")
        assert isinstance(pytaskq.__all__, list)
        assert len(pytaskq.__all__) > 0

    def test_all_contains_required_exports(self):
        """Test that __all__ contains all required exports."""
        import pytaskq

        expected_exports = [
            # Task and Status
            "Task",
            "TaskStatus",
            # Queue implementations
            "TaskQueue",
            "PriorityQueue",
            # Storage
            "StorageBackend",
            "SQLiteBackend",
            # Workers
            "Worker",
            "WorkerPool",
            "WorkerMetrics",
            # Retry logic
            "RetryPolicy",
            "with_retry",
            "RetryError",
            # Scheduler
            "Scheduler",
            # Metrics
            "MetricsCollector",
            "MetricsSnapshot",
            # Configuration
            "Config",
            # Convenience functions
            "create_queue",
            # Package metadata
            "__version__",
        ]

        for export in expected_exports:
            assert export in pytaskq.__all__, f"Expected export '{export}' not in __all__"

    def test_exports_are_accessible(self):
        """Test that all exports in __all__ are accessible."""
        import pytaskq

        for export in pytaskq.__all__:
            assert hasattr(
                pytaskq, export
            ), f"Export '{export}' in __all__ but not accessible as attribute"

    def test_task_classes_exported(self):
        """Test that task-related classes are exported."""
        import pytaskq

        assert "Task" in pytaskq.__all__
        assert "TaskStatus" in pytaskq.__all__
        assert hasattr(pytaskq, "Task")
        assert hasattr(pytaskq, "TaskStatus")

    def test_queue_classes_exported(self):
        """Test that queue-related classes are exported."""
        import pytaskq

        assert "TaskQueue" in pytaskq.__all__
        assert "PriorityQueue" in pytaskq.__all__
        assert hasattr(pytaskq, "TaskQueue")
        assert hasattr(pytaskq, "PriorityQueue")

    def test_storage_classes_exported(self):
        """Test that storage-related classes are exported."""
        import pytaskq

        assert "StorageBackend" in pytaskq.__all__
        assert "SQLiteBackend" in pytaskq.__all__
        assert hasattr(pytaskq, "StorageBackend")
        assert hasattr(pytaskq, "SQLiteBackend")

    def test_worker_classes_exported(self):
        """Test that worker-related classes are exported."""
        import pytaskq

        assert "Worker" in pytaskq.__all__
        assert "WorkerPool" in pytaskq.__all__
        assert "WorkerMetrics" in pytaskq.__all__
        assert hasattr(pytaskq, "Worker")
        assert hasattr(pytaskq, "WorkerPool")
        assert hasattr(pytaskq, "WorkerMetrics")

    def test_retry_classes_exported(self):
        """Test that retry-related classes are exported."""
        import pytaskq

        assert "RetryPolicy" in pytaskq.__all__
        assert "with_retry" in pytaskq.__all__
        assert "RetryError" in pytaskq.__all__
        assert hasattr(pytaskq, "RetryPolicy")
        assert hasattr(pytaskq, "with_retry")
        assert hasattr(pytaskq, "RetryError")

    def test_scheduler_exported(self):
        """Test that scheduler is exported."""
        import pytaskq

        assert "Scheduler" in pytaskq.__all__
        assert hasattr(pytaskq, "Scheduler")

    def test_metrics_classes_exported(self):
        """Test that metrics-related classes are exported."""
        import pytaskq

        assert "MetricsCollector" in pytaskq.__all__
        assert "MetricsSnapshot" in pytaskq.__all__
        assert hasattr(pytaskq, "MetricsCollector")
        assert hasattr(pytaskq, "MetricsSnapshot")

    def test_config_exported(self):
        """Test that config is exported."""
        import pytaskq

        assert "Config" in pytaskq.__all__
        assert hasattr(pytaskq, "Config")


class TestCreateQueueFactory:
    """Test cases for create_queue() factory function."""

    def test_create_queue_exists(self):
        """Test that create_queue function exists and is callable."""
        import pytaskq

        assert hasattr(pytaskq, "create_queue")
        assert callable(pytaskq.create_queue)

    def test_create_queue_in_exports(self):
        """Test that create_queue is in __all__."""
        import pytaskq

        assert "create_queue" in pytaskq.__all__

    def test_create_queue_returns_priority_queue(self):
        """Test that create_queue returns a PriorityQueue instance."""
        queue = create_queue()
        assert isinstance(queue, PriorityQueue)

    def test_create_queue_default_params(self):
        """Test that create_queue works with default parameters."""
        queue = create_queue()
        assert queue is not None
        assert isinstance(queue, PriorityQueue)

    def test_create_queue_with_max_size(self):
        """Test that create_queue accepts max_size parameter."""
        queue = create_queue(max_size=100)
        assert queue is not None
        assert isinstance(queue, PriorityQueue)

    def test_create_queue_with_none_max_size(self):
        """Test that create_queue accepts None for max_size."""
        queue = create_queue(max_size=None)
        assert queue is not None
        assert isinstance(queue, PriorityQueue)

    def test_create_queue_is_usable(self):
        """Test that queue created by create_queue is usable."""
        queue = create_queue()
        task = Task(name="test", priority=1)
        queue.enqueue(task)

        assert queue.size() == 1
        retrieved_task = queue.get()
        assert retrieved_task.id == task.id

    def test_create_queue_is_different_instance(self):
        """Test that each call to create_queue returns a new instance."""
        queue1 = create_queue()
        queue2 = create_queue()

        assert queue1 is not queue2
        assert isinstance(queue1, PriorityQueue)
        assert isinstance(queue2, PriorityQueue)

    def test_create_queue_has_docstring(self):
        """Test that create_queue has proper documentation."""
        assert create_queue.__doc__ is not None
        assert len(create_queue.__doc__) > 0


class TestImports:
    """Test cases for import functionality."""

    def test_import_from_package(self):
        """Test that main classes can be imported from package."""
        from pytaskq import (
            Task,
            TaskStatus,
            TaskQueue,
            PriorityQueue,
            StorageBackend,
            SQLiteBackend,
            Worker,
            WorkerPool,
            WorkerMetrics,
            RetryPolicy,
            with_retry,
            RetryError,
            Scheduler,
            MetricsCollector,
            MetricsSnapshot,
            Config,
            create_queue,
            __version__,
        )

        assert Task is not None
        assert TaskStatus is not None
        assert TaskQueue is not None
        assert PriorityQueue is not None
        assert StorageBackend is not None
        assert SQLiteBackend is not None
        assert Worker is not None
        assert WorkerPool is not None
        assert WorkerMetrics is not None
        assert RetryPolicy is not None
        assert with_retry is not None
        assert RetryError is not None
        assert Scheduler is not None
        assert MetricsCollector is not None
        assert MetricsSnapshot is not None
        assert Config is not None
        assert create_queue is not None
        assert __version__ is not None

    def test_import_classes_are_correct_types(self):
        """Test that imported classes are the correct types (classes/functions)."""
        from pytaskq import (
            Task,
            TaskStatus,
            TaskQueue,
            PriorityQueue,
            StorageBackend,
            SQLiteBackend,
            Worker,
            WorkerPool,
            RetryPolicy,
            Scheduler,
            MetricsCollector,
            Config,
            __version__,
        )

        # Classes should be types
        assert isinstance(Task, type)
        assert isinstance(TaskStatus, type)
        assert isinstance(TaskQueue, type)
        assert isinstance(PriorityQueue, type)
        assert isinstance(StorageBackend, type)
        assert isinstance(SQLiteBackend, type)
        assert isinstance(Worker, type)
        assert isinstance(WorkerPool, type)
        assert isinstance(RetryPolicy, type)
        assert isinstance(Scheduler, type)
        assert isinstance(MetricsCollector, type)
        assert isinstance(Config, type)

        # __version__ should be a string
        assert isinstance(__version__, str)

    def test_import_functions_are_callable(self):
        """Test that imported functions are callable."""
        from pytaskq import create_queue, with_retry

        assert callable(create_queue)
        assert callable(with_retry)

    def test_import_exceptions_are_exceptions(self):
        """Test that imported exceptions are exception classes."""
        from pytaskq import RetryError

        assert issubclass(RetryError, Exception)

    def test_task_status_enum_values(self):
        """Test that TaskStatus enum has expected values."""
        from pytaskq import TaskStatus

        # Check that enum has common status values
        # The actual implementation may vary, but should have at least some statuses
        assert hasattr(TaskStatus, "__members__")
        assert len(TaskStatus.__members__) > 0

    def test_star_import(self):
        """Test that star import works correctly."""
        # This tests that __all__ is properly configured
        exec("from pytaskq import *")
        # If no exception is raised, star import works
        import pytaskq

        # Verify that all __all__ items are imported
        for item in pytaskq.__all__:
            assert item in dir(), f"Item '{item}' from __all__ not found in namespace"