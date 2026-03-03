"""
Scheduler Implementation

This module provides a task scheduler supporting delayed execution,
recurring tasks with intervals, and cron-like scheduling.
"""

import threading
import time
import heapq
from datetime import datetime, timedelta
from typing import Optional, Callable, Any, Dict, List
from dataclasses import dataclass, field

from pytaskq.task import Task, TaskStatus
from pytaskq.queue import TaskQueue


@dataclass
class ScheduledTask:
    """
    Represents a scheduled task with execution timing information.
    
    Attributes:
        task: The task to be executed
        execute_at: When to execute the task
        interval: Seconds between executions (None for one-shot)
        last_run: Last execution time
        times_run: Number of times executed
        max_runs: Maximum number of executions (None for unlimited)
        enabled: Whether the scheduling is active
        id: Unique identifier for the scheduled task
    """
    task: Task
    execute_at: datetime
    interval: Optional[float] = None
    last_run: Optional[datetime] = None
    times_run: int = 0
    max_runs: Optional[int] = None
    enabled: bool = True
    id: str = field(default_factory=lambda: str(id(object())))
    
    def reschedule(self) -> Optional['ScheduledTask']:
        """
        Create a new scheduled task for the next execution interval.
        
        Returns:
            New ScheduledTask if recurring and still enabled, None otherwise
        """
        if not self.enabled:
            return None
        
        if self.max_runs is not None and self.times_run >= self.max_runs:
            return None
        
        if self.interval is None or self.interval <= 0:
            return None
        
        return ScheduledTask(
            task=self.task,
            execute_at=self.execute_at + timedelta(seconds=self.interval),
            interval=self.interval,
            last_run=None,
            times_run=0,
            max_runs=self.max_runs,
            enabled=True,
        )
    
    def __lt__(self, other: 'ScheduledTask') -> bool:
        """Compare tasks by execution time for heap ordering."""
        return self.execute_at < other.execute_at


class Scheduler:
    """
    Task scheduler supporting delayed and recurring tasks.
    
    The scheduler runs a daemon thread that periodically checks for tasks
    that are due for execution and submits them to the task queue.
    
    Attributes:
        queue: The task queue to submit tasks to
        _scheduled_tasks: Priority queue of scheduled tasks
        _scheduled_tasks_map: Dict mapping task IDs to scheduled tasks
        _lock: Thread lock for thread-safe operations
        _running: Whether the scheduler is running
        _daemon_thread: Background thread for checking tasks
        _check_interval: Seconds between scheduler checks
    """
    
    def __init__(self, queue: TaskQueue, check_interval: float = 1.0) -> None:
        """
        Initialize the scheduler.
        
        Args:
            queue: Task queue to submit executed tasks to
            check_interval: Seconds between scheduler checks (default 1.0)
        """
        self.queue = queue
        self._scheduled_tasks: List[ScheduledTask] = []
        self._scheduled_tasks_map: Dict[str, ScheduledTask] = {}
        self._lock = threading.Lock()
        self._running = False
        self._daemon_thread: Optional[threading.Thread] = None
        self._check_interval = check_interval
    
    def start(self) -> None:
        """Start the scheduler daemon thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._daemon_thread = threading.Thread(
                target=self._run_scheduler,
                daemon=True,
                name="SchedulerThread"
            )
            self._daemon_thread.start()
    
    def stop(self) -> None:
        """Stop the scheduler daemon thread."""
        with self._lock:
            self._running = False
            if self._daemon_thread:
                self._daemon_thread.join(timeout=2.0)
    
    def _run_scheduler(self) -> None:
        """Main scheduler loop that checks for due tasks."""
        while self._running:
            self._process_due_tasks()
            time.sleep(self._check_interval)
    
    def _process_due_tasks(self) -> None:
        """Check for and process tasks that are due for execution."""
        now = datetime.utcnow()
        tasks_to_submit = []
        tasks_to_reschedule = []
        
        with self._lock:
            # Process all tasks that are due
            while self._scheduled_tasks and self._scheduled_tasks[0].execute_at <= now:
                scheduled = heapq.heappop(self._scheduled_tasks)
                
                if scheduled.id in self._scheduled_tasks_map:
                    del self._scheduled_tasks_map[scheduled.id]
                
                if not scheduled.enabled:
                    continue
                
                # Submit the task to queue
                tasks_to_submit.append(scheduled.task)
                
                # Handle recurring tasks
                next_run = scheduled.reschedule()
                if next_run:
                    tasks_to_reschedule.append(next_run)
            
            # Add rescheduled tasks back to heap
            for task in tasks_to_reschedule:
                heapq.heappush(self._scheduled_tasks, task)
                self._scheduled_tasks_map[task.id] = task
        
        # Submit tasks outside the lock to avoid holding lock during queue operations
        for task in tasks_to_submit:
            try:
                self.queue.enqueue(task)
            except Exception:
                # If queue enqueue fails, we still continue with other tasks
                pass
    
    def schedule_delayed(
        self,
        task: Task,
        delay_seconds: float
    ) -> str:
        """
        Schedule a one-shot task to execute after a delay.
        
        Args:
            task: The task to execute
            delay_seconds: Number of seconds to delay before execution
            
        Returns:
            Unique identifier for the scheduled task
        """
        execute_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=None,
            enabled=True
        )
        
        with self._lock:
            heapq.heappush(self._scheduled_tasks, scheduled)
            self._scheduled_tasks_map[scheduled.id] = scheduled
        
        return scheduled.id
    
    def schedule_recurring(
        self,
        task: Task,
        interval_seconds: float,
        first_run_seconds: float = 0.0,
        max_runs: Optional[int] = None
    ) -> str:
        """
        Schedule a recurring task to execute at fixed intervals.
        
        Args:
            task: The task to execute
            interval_seconds: Seconds between executions
            first_run_seconds: Delay before first execution (default 0)
            max_runs: Maximum number of executions (None for unlimited)
            
        Returns:
            Unique identifier for the scheduled task
        """
        execute_at = datetime.utcnow() + timedelta(seconds=first_run_seconds)
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=interval_seconds,
            max_runs=max_runs,
            enabled=True
        )
        
        with self._lock:
            heapq.heappush(self._scheduled_tasks, scheduled)
            self._scheduled_tasks_map[scheduled.id] = scheduled
        
        return scheduled.id
    
    def cancel(self, task_id: str) -> bool:
        """
        Cancel a scheduled task.
        
        Args:
            task_id: Unique identifier of the scheduled task
            
        Returns:
            True if task was found and cancelled, False otherwise
        """
        with self._lock:
            if task_id not in self._scheduled_tasks_map:
                return False
            
            scheduled = self._scheduled_tasks_map[task_id]
            scheduled.enabled = False
            del self._scheduled_tasks_map[task_id]
            
            # Note: Task remains in heap but will be skipped when processed
            # We could rebuild heap to remove it, but that's expensive
            return True
    
    def get_scheduled_count(self) -> int:
        """
        Get the number of currently scheduled tasks.
        
        Returns:
            Number of enabled scheduled tasks
        """
        with self._lock:
            return len(self._scheduled_tasks_map)
    
    def clear(self) -> None:
        """Clear all scheduled tasks."""
        with self._lock:
            self._scheduled_tasks.clear()
            self._scheduled_tasks_map.clear()
    
    def is_running(self) -> bool:
        """
        Check if the scheduler is running.
        
        Returns:
            True if scheduler is running, False otherwise
        """
        with self._lock:
            return self._running
    
    def wait_until_empty(self, timeout: float = 10.0) -> bool:
        """
        Wait until all scheduled tasks have been processed.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if all tasks processed, False if timeout occurred
        """
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if not self._scheduled_tasks_map:
                    return True
            time.sleep(0.1)
        return False