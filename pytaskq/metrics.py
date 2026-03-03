"""
Metrics Implementation

This module provides the MetricsCollector class for tracking task queue metrics
in a thread-safe manner.
"""

import threading
from dataclasses import dataclass
from typing import Optional


@dataclass
class MetricsSnapshot:
    """Immutable snapshot of current metrics."""
    tasks_submitted: int
    tasks_completed: int
    tasks_failed: int
    average_duration: float
    
    @property
    def total_tasks(self) -> int:
        """Total number of tasks processed (completed + failed)."""
        return self.tasks_completed + self.tasks_failed


class MetricsCollector:
    """
    Thread-safe metrics collector for tracking task queue statistics.
    
    Tracks the following metrics:
    - tasks_submitted: Total number of tasks submitted to the queue
    - tasks_completed: Total number of tasks that completed successfully
    - tasks_failed: Total number of tasks that failed
    - average_duration: Average execution duration of completed and failed tasks
    
    All metric updates are thread-safe using threading.Lock.
    """
    
    def __init__(self) -> None:
        """Initialize a new MetricsCollector with all counters set to zero."""
        self._lock = threading.Lock()
        self._tasks_submitted: int = 0
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._total_duration: float = 0.0
        self._total_duration_count: int = 0
    
    def increment_submitted(self, count: int = 1) -> None:
        """
        Increment the tasks submitted counter.
        
        Args:
            count: Number of tasks to add (default: 1)
        """
        if count <= 0:
            return
        
        with self._lock:
            self._tasks_submitted += count
    
    def increment_completed(self, duration: Optional[float] = None) -> None:
        """
        Increment the tasks completed counter and record duration.
        
        Args:
            duration: Execution duration in seconds. If provided, contributes to
                     average_duration calculation. If None, no duration is recorded.
        """
        with self._lock:
            self._tasks_completed += 1
            if duration is not None and duration >= 0:
                self._total_duration += duration
                self._total_duration_count += 1
    
    def increment_failed(self, duration: Optional[float] = None) -> None:
        """
        Increment the tasks failed counter and record duration.
        
        Args:
            duration: Execution duration in seconds. If provided, contributes to
                     average_duration calculation. If None, no duration is recorded.
        """
        with self._lock:
            self._tasks_failed += 1
            if duration is not None and duration >= 0:
                self._total_duration += duration
                self._total_duration_count += 1
    
    @property
    def tasks_submitted(self) -> int:
        """Get the number of tasks submitted (thread-safe)."""
        with self._lock:
            return self._tasks_submitted
    
    @property
    def tasks_completed(self) -> int:
        """Get the number of tasks completed (thread-safe)."""
        with self._lock:
            return self._tasks_completed
    
    @property
    def tasks_failed(self) -> int:
        """Get the number of tasks failed (thread-safe)."""
        with self._lock:
            return self._tasks_failed
    
    @property
    def average_duration(self) -> float:
        """
        Get the average execution duration (thread-safe).
        
        Returns:
            Average duration in seconds, or 0.0 if no durations have been recorded.
        """
        with self._lock:
            if self._total_duration_count == 0:
                return 0.0
            return self._total_duration / self._total_duration_count
    
    @property
    def total_tasks_processed(self) -> int:
        """Get the total number of tasks processed (completed + failed)."""
        with self._lock:
            return self._tasks_completed + self._tasks_failed
    
    def reset(self) -> None:
        """Reset all metrics to their initial state (zero). Thread-safe."""
        with self._lock:
            self._tasks_submitted = 0
            self._tasks_completed = 0
            self._tasks_failed = 0
            self._total_duration = 0.0
            self._total_duration_count = 0
    
    def snapshot(self) -> MetricsSnapshot:
        """
        Create an immutable snapshot of current metrics.
        
        Returns:
            MetricsSnapshot containing current values of all metrics.
        """
        with self._lock:
            avg_duration = (self._total_duration / self._total_duration_count 
                          if self._total_duration_count > 0 else 0.0)
            return MetricsSnapshot(
                tasks_submitted=self._tasks_submitted,
                tasks_completed=self._tasks_completed,
                tasks_failed=self._tasks_failed,
                average_duration=avg_duration
            )
    
    def __repr__(self) -> str:
        """String representation of current metrics."""
        snapshot = self.snapshot()
        return (
            f"MetricsCollector(submitted={snapshot.tasks_submitted}, "
            f"completed={snapshot.tasks_completed}, "
            f"failed={snapshot.tasks_failed}, "
            f"avg_duration={snapshot.average_duration:.4f}s)"
        )