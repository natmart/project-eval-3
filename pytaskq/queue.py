"""
Task Queue Implementation

This module provides a thread-safe priority-based task queue using heapq.
"""

import heapq
import threading
from typing import Callable, Optional, List, Any
from dataclasses import dataclass, field
from datetime import datetime

from .task import Task


@dataclass(order=True)
class PrioritizedTask:
    """A task with priority for heap ordering."""
    priority: int = field(compare=True)
    created_at: datetime = field(compare=True)
    task: Task = field(compare=False)


class TaskQueue:
    """
    A thread-safe priority-based task queue.
    
    Tasks are ordered by priority (lower number = higher priority) and
    then by creation time (FIFO for same priority).
    """
    
    def __init__(self):
        """Initialize an empty task queue."""
        self._queue: List[PrioritizedTask] = []
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
    
    def put(self, task: Task) -> None:
        """
        Add a task to the queue.
        
        Args:
            task: The task to add to the queue
        """
        with self._lock:
            prioritized = PrioritizedTask(
                priority=task.priority,
                created_at=task.created_at,
                task=task
            )
            heapq.heappush(self._queue, prioritized)
            self._not_empty.notify()
    
    def get(self, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Get the next task from the queue.
        
        Args:
            timeout: Maximum time to wait in seconds. None means wait indefinitely.
            
        Returns:
            The next task, or None if timeout expires
        """
        with self._not_empty:
            while not self._queue:
                if not self._not_empty.wait(timeout):
                    return None
            
            prioritized = heapq.heappop(self._queue)
            return prioritized.task
    
    def task_done(self) -> None:
        """Mark a task as done (for queue tracking)."""
        pass
    
    def qsize(self) -> int:
        """
        Get the number of tasks in the queue.
        
        Returns:
            Number of tasks currently in the queue
        """
        with self._lock:
            return len(self._queue)
    
    def empty(self) -> bool:
        """
        Check if the queue is empty.
        
        Returns:
            True if queue is empty, False otherwise
        """
        with self._lock:
            return len(self._queue) == 0
    
    def clear(self) -> None:
        """Clear all tasks from the queue."""
        with self._lock:
            self._queue.clear()
            self._not_empty.notify_all()