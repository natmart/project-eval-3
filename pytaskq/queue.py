"""
Task Queue Implementation

This module provides a thread-safe priority-based task queue using heapq.
"""

import heapq
import threading
import logging
from typing import Optional
from datetime import datetime

from .task import Task, TaskStatus


logger = logging.getLogger(__name__)


class TaskQueue:
    """
    A thread-safe priority-based task queue.
    
    Tasks are ordered by priority (lower number = higher priority) and
    then by creation time (older tasks first). This ensures that the
    highest priority tasks are executed first.
    """
    
    def __init__(self):
        """Initialize a new empty task queue."""
        self._queue = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        
        logger.debug("TaskQueue initialized")
    
    def put(self, task: Task) -> None:
        """
        Add a task to the queue.
        
        Args:
            task: The task to add to the queue
        """
        with self._condition:
            # Priority is based on task priority, then creation time
            priority = (
                task.priority,
                task.created_at.timestamp() if isinstance(task.created_at, datetime) else 0,
                task.id
            )
            heapq.heappush(self._queue, (priority, task))
            self._condition.notify()
        
        logger.debug(f"Task {task.id} added to queue (priority: {task.priority})")
    
    def get(self, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Get the next task from the queue.
        
        Args:
            timeout: Maximum time to wait for a task (seconds).
                     If None and queue is empty, returns immediately.
                     If > 0 and queue is empty, waits up to timeout seconds.
        
        Returns:
            The next task, or None if queue is empty and timeout expired
        """
        with self._condition:
            if not self._queue:
                if timeout is None:
                    # No timeout, return None immediately
                    return None
                elif timeout <= 0:
                    # Non-blocking return
                    return None
                else:
                    # Wait for a task or timeout
                    if not self._condition.wait(timeout=timeout):
                        # Timeout expired
                        return None
            
            # Get the highest priority task
            priority, task = heapq.heappop(self._queue)
            return task
    
    def size(self) -> int:
        """
        Get the current number of tasks in the queue.
        
        Returns:
            Number of tasks in the queue
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
        """Remove all tasks from the queue."""
        with self._lock:
            self._queue.clear()
        
        logger.debug("TaskQueue cleared")
    
    def peek(self) -> Optional[Task]:
        """
        Peek at the next task without removing it from the queue.
        
        Returns:
            The next task, or None if queue is empty
        """
        with self._lock:
            if self._queue:
                _, task = self._queue[0]
                return task
            return None