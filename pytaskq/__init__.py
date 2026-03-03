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

__all__ = [
    "StorageBackend",
    "SQLiteBackend",
    "TaskQueue",
    "PriorityQueue",
    "Task",
    "TaskStatus",
    "MetricsCollector",
    "MetricsSnapshot",
    "RetryPolicy",
    "with_retry",
    "RetryError",
    "Scheduler",
    "Worker",
    "WorkerPool",
    "WorkerMetrics",
    "__version__"
]