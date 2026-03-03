"""
PyTaskQ - A Python Task Queue Library v2

A lightweight, thread-safe task queue implementation using heapq for priority-based
task scheduling and threading for concurrent execution.
"""

__version__ = "2.0.0"
__author__ = "PyTaskQ Team"

from .storage import StorageBackend, SQLiteBackend
from .queue import TaskQueue, PriorityQueue
from .task import Task, TaskStatus
from .metrics import MetricsCollector, MetricsSnapshot
from .retry import RetryPolicy, with_retry, RetryError
from .scheduler import Scheduler
from .worker import Worker, WorkerPool, WorkerMetrics
from .config import Config


def create_queue(max_size: int = 1000) -> PriorityQueue:
    """
    Convenience factory function to create a new priority queue.

    This function provides a simple way to create a priority queue with
    optional size constraints. The queue is thread-safe and ready to use.

    Args:
        max_size: Maximum number of tasks allowed in the queue (default: 1000).
                  Use None for unlimited size. Currently, this parameter is
                  informational as the TaskQueue implementation doesn't enforce
                  size limits.

    Returns:
        A new PriorityQueue instance configured with the specified settings.

    Example:
        >>> from pytaskq import create_queue
        >>> queue = create_queue()
        >>> task = Task(name="example", priority=1)
        >>> queue.enqueue(task)
    """
    return PriorityQueue()


__all__ = [
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