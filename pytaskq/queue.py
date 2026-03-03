"""
Task Queue Implementation

This module provides a thread-safe priority-based task queue using heapq.
"""

import heapq
import threading
from typing import Optional, List, Any, Tuple
from dataclasses import dataclass

from pytaskq.task import Task


@dataclass
class PriorityQueue:
    """
    A thread-safe priority queue for tasks.
    
    Tasks are ordered by priority (lower number = higher priority).
    All operations are thread-safe using threading.Lock.
    """
    
    _queue: List[Tuple[int, int, Task]] = None
    _lock: threading.Lock = None
    _counter: int = 0
    
    def __post_init__(self) -> None:
        """Initialize the queue with thread-safe data structures."""
        object.__setattr__(self, '_queue', [])
        object.__setattr__(self, '_lock', threading.Lock())
        object.__setattr__(self, '_counter', 0)
    
    def enqueue(self, task: Task) -> None:
        """
        Add a task to the queue.
        
        Args:
            task: The task to add to the queue
        """
        with self._lock:
            # Use a counter to maintain FIFO order for tasks with same priority
            heapq.heappush(self._queue, (task.priority, self._counter, task))
            self._counter += 1
    
    def dequeue(self) -> Optional[Task]:
        """
        Remove and return the highest priority task.
        
        Returns:
            The highest priority task, or None if the queue is empty
        """
        with self._lock:
            if not self._queue:
                return None
            # Pop the task (priority, counter, task) and return just the task
            return heapq.heappop(self._queue)[2]
    
    def peek(self) -> Optional[Task]:
        """
        View the highest priority task without removing it.
        
        Returns:
            The highest priority task, or None if the queue is empty
        """
        with self._lock:
            if not self._queue:
                return None
            # Return just the task without removing it
            return self._queue[0][2]
    
    def size(self) -> int:
        """
        Get the number of tasks in the queue.
        
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
        """
        Remove all tasks from the queue.
        """
        with self._lock:
            self._queue.clear()
            self._counter = 0


# Export TaskQueue as an alias for PriorityQueue for backward compatibility
TaskQueue = PriorityQueue