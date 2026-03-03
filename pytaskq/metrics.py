"""
Thread-safe metrics collector for task queue statistics.

This module provides a MetricsCollector class that tracks various
task execution metrics in a thread-safe manner.
"""

import threading
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class MetricsSnapshot:
    """Immutable snapshot of current metrics."""
    tasks_submitted: int
    tasks_completed: int
    tasks_failed: int
    average_duration: float
    total_duration: float
    duration_count: int


class MetricsCollector:
    """
    Thread-safe collector for task queue metrics.
    
    Tracks:
    - Number of tasks submitted
    - Number of tasks completed
    - Number of tasks failed
    - Average task execution duration
    """
    
    def __init__(self) -> None:
        """Initialize a new MetricsCollector with all counters at zero."""
        self._lock = threading.Lock()
        self._tasks_submitted: int = 0
        self._tasks_completed: int = 0
        self._tasks_failed: int = 0
        self._total_duration: float = 0.0
        self._duration_count: int = 0
    
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
    
    def increment_completed(
        self, 
        duration: Optional[float] = None
    ) -> None:
        """
        Increment the tasks completed counter and optionally record duration.
        
        Args:
            duration: Optional duration in seconds. If provided, it's included
                     in the average duration calculation.
        """
        with self._lock:
            self._tasks_completed += 1
            if duration is not None and duration >= 0:
                self._total_duration += duration
                self._duration_count += 1
    
    def increment_failed(
        self,
        duration: Optional[float] = None
    ) -> None:
        """
        Increment the tasks failed counter and optionally record duration.
        
        Args:
            duration: Optional duration in seconds. If provided, it's included
                     in the average duration calculation.
        """
        with self._lock:
            self._tasks_failed += 1
            if duration is not None and duration >= 0:
                self._total_duration += duration
                self._duration_count += 1
    
    def reset(self) -> None:
        """Reset all metrics counters to zero."""
        with self._lock:
            self._tasks_submitted = 0
            self._tasks_completed = 0
            self._tasks_failed = 0
            self._total_duration = 0.0
            self._duration_count = 0
    
    def snapshot(self) -> MetricsSnapshot:
        """
        Get an immutable snapshot of current metrics.
        
        Returns:
            MetricsSnapshot containing current metrics
        """
        with self._lock:
            avg_duration = (
                self._total_duration / self._duration_count
                if self._duration_count > 0
                else 0.0
            )
            return MetricsSnapshot(
                tasks_submitted=self._tasks_submitted,
                tasks_completed=self._tasks_completed,
                tasks_failed=self._tasks_failed,
                average_duration=avg_duration,
                total_duration=self._total_duration,
                duration_count=self._duration_count
            )
    
    @property
    def tasks_submitted(self) -> int:
        """Get the number of tasks submitted."""
        with self._lock:
            return self._tasks_submitted
    
    @property
    def tasks_completed(self) -> int:
        """Get the number of tasks completed."""
        with self._lock:
            return self._tasks_completed
    
    @property
    def tasks_failed(self) -> int:
        """Get the number of tasks failed."""
        with self._lock:
            return self._tasks_failed
    
    @property
    def average_duration(self) -> float:
        """Get the average task execution duration."""
        with self._lock:
            return (
                self._total_duration / self._duration_count
                if self._duration_count > 0
                else 0.0
            )
    
    @property
    def total_duration(self) -> float:
        """Get the total duration of all recorded tasks."""
        with self._lock:
            return self._total_duration
    
    @property
    def duration_count(self) -> int:
        """Get the count of durations recorded."""
        with self._lock:
            return self._duration_count