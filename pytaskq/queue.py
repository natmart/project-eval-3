"""
Task Queue Implementation

This module provides a thread-safe priority-based task queue using heapq.
"""

import heapq
import threading
from typing import Optional

from pytaskq.task import Task


class TaskQueue:
    """
    A thread-safe priority-based task queue using heapq.

    Tasks are stored as (priority, insertion_order, task) tuples to ensure
    FIFO ordering for tasks with the same priority. Lower priority values
    indicate higher priority (0 is highest).

    Attributes:
        _queue: Internal heap-based priority queue
        _lock: Thread lock for thread-safe operations
        _counter: Monotonically increasing counter for insertion order
    """

    def __init__(self) -> None:
        """Initialize an empty priority queue."""
        self._queue: list = []
        self._lock: threading.Lock = threading.Lock()
        self._counter: int = 0

    def enqueue(self, task: Task) -> None:
        """
        Add a task to the queue with the given priority.

        Args:
            task: The task to add to the queue
        """
        with self._lock:
            # Use task.priority and counter for stable ordering
            heapq.heappush(self._queue, (task.priority, self._counter, task))
            self._counter += 1

    def dequeue(self) -> Optional[Task]:
        """
        Remove and return the highest priority task from the queue.

        Returns:
            The highest priority task, or None if the queue is empty
        """
        with self._lock:
            if not self._queue:
                return None
            # Return just the task, not the priority tuple
            _, _, task = heapq.heappop(self._queue)
            return task

    def peek(self) -> Optional[Task]:
        """
        Return the highest priority task without removing it from the queue.

        Returns:
            The highest priority task, or None if the queue is empty
        """
        with self._lock:
            if not self._queue:
                return None
            # Return just the task, not the priority tuple
            _, _, task = self._queue[0]
            return task

    def size(self) -> int:
        """
        Return the number of tasks currently in the queue.

        Returns:
            Number of tasks in the queue
        """
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        """
        Check if the queue is empty.

        Returns:
            True if the queue is empty, False otherwise
        """
        with self._lock:
            return len(self._queue) == 0

    def clear(self) -> None:
        """Remove all tasks from the queue."""
        with self._lock:
            self._queue.clear()
            # Reset counter for consistent behavior
            self._counter = 0


# Alias for backwards compatibility and clarity
PriorityQueue = TaskQueue