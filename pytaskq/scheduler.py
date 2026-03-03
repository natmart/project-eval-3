"""
Task Scheduler Implementation

This module provides a task scheduler that supports scheduling tasks with
cron-like expressions or intervals. It supports one-shot delayed tasks and
recurring tasks using threading.Timer for delayed execution.
"""

import threading
import time
from datetime import datetime, timedelta
from typing import Callable, Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
import re
import heapq

from .task import Task
from .queue import TaskQueue


@dataclass(order=True)
class ScheduledTask:
    """A task scheduled for future execution."""
    next_run: datetime = field(compare=True)
    task: Task = field(compare=False)
    recurring: bool = field(compare=False)
    interval: Optional[float] = field(compare=False, default=None)
    cron_expression: Optional[str] = field(compare=False, default=None)
    callback: Optional[Callable[[Task], None]] = field(compare=False, default=None)
    job_id: str = field(compare=False, default=None)


class CronValidationError(Exception):
    """Raised when a cron expression is invalid."""
    pass


class CronParser:
    """
    Parser for cron-like expressions.
    
    Supports simplified cron expressions with five fields:
    minute hour day month day_of_week
    
    Examples:
        "5 * * * *" - Every hour at 5 minutes past the hour
        "0 9 * * 1-5" - Every weekday at 9 AM
        "*/15 * * * *" - Every 15 minutes
        "0 0 1 * *" - On the first day of every month at midnight
    """
    
    FIELD_RANGES = {
        'minute': (0, 59),
        'hour': (0, 23),
        'day': (1, 31),
        'month': (1, 12),
        'day_of_week': (0, 6)  # 0 = Sunday, 6 = Saturday
    }
    
    FIELD_NAMES = ['minute', 'hour', 'day', 'month', 'day_of_week']
    
    @classmethod
    def parse(cls, expression: str) -> Dict[str, List[int]]:
        """
        Parse a cron expression into a dictionary of allowed values.
        
        Args:
            expression: Cron expression string (5 fields)
            
        Returns:
            Dictionary mapping field names to lists of allowed values
            
        Raises:
            CronValidationError: If the expression is invalid
        """
        # Split expression into fields
        parts = expression.strip().split()
        if len(parts) != 5:
            raise CronValidationError(
                f"Invalid cron expression: expected 5 fields, got {len(parts)}"
            )
        
        result = {}
        for field_name, part in zip(cls.FIELD_NAMES, parts):
            result[field_name] = cls._parse_field(field_name, part)
        
        return result
    
    @classmethod
    def _parse_field(cls, field_name: str, part: str) -> List[int]:
        """
        Parse a single cron field.
        
        Args:
            field_name: Name of the field
            part: The field value string
            
        Returns:
            List of allowed values for this field
        """
        min_val, max_val = cls.FIELD_RANGES[field_name]
        values = set()
        
        # Handle comma-separated values
        for segment in part.split(','):
            # Handle ranges (e.g., 1-5)
            if '-' in segment:
                range_parts = segment.split('-')
                if len(range_parts) != 2:
                    raise CronValidationError(
                        f"Invalid range in field '{field_name}': {segment}"
                    )
                
                start = cls._parse_value(field_name, range_parts[0])
                end = cls._parse_value(field_name, range_parts[1])
                
                if start > end:
                    raise CronValidationError(
                        f"Invalid range in field '{field_name}': {start} > {end}"
                    )
                
                for val in range(start, end + 1):
                    if not (min_val <= val <= max_val):
                        raise CronValidationError(
                            f"Value {val} out of range for field '{field_name}'"
                        )
                    values.add(val)
            
            # Handle steps (e.g., */5 or 1-10/2)
            elif '/' in segment:
                step_parts = segment.split('/')
                if len(step_parts) != 2:
                    raise CronValidationError(
                        f"Invalid step in field '{field_name}': {segment}"
                    )
                
                base = step_parts[0]
                step = cls._parse_value(field_name, step_parts[1])
                
                if base == '*':
                    start, end = min_val, max_val
                elif '-' in base:
                    range_parts = base.split('-')
                    start = cls._parse_value(field_name, range_parts[0])
                    end = cls._parse_value(field_name, range_parts[1])
                else:
                    start = end = cls._parse_value(field_name, base)
                
                for val in range(start, end + 1, step):
                    if not (min_val <= val <= max_val):
                        raise CronValidationError(
                            f"Value {val} out of range for field '{field_name}'"
                        )
                    values.add(val)
            
            # Handle wildcard
            elif segment == '*':
                for val in range(min_val, max_val + 1):
                    values.add(val)
            
            # Handle single value
            else:
                val = cls._parse_value(field_name, segment)
                if not (min_val <= val <= max_val):
                    raise CronValidationError(
                        f"Value {val} out of range for field '{field_name}'"
                    )
                values.add(val)
        
        return sorted(values)
    
    @classmethod
    def _parse_value(cls, field_name: str, value_str: str) -> int:
        """Parse a single numeric value."""
        try:
            return int(value_str)
        except ValueError:
            raise CronValidationError(
                f"Invalid numeric value in field '{field_name}': {value_str}"
            )
    
    @classmethod
    def next_run_time(cls, expression: str, after: Optional[datetime] = None) -> datetime:
        """
        Calculate the next run time for a cron expression.
        
        Args:
            expression: Cron expression string
            after: Starting time (defaults to current time)
            
        Returns:
            Next datetime when the task should run
        """
        if after is None:
            after = datetime.now()
        
        parsed = cls.parse(expression)
        
        # Start checking from the next minute
        check_time = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        # Check up to 4 years ahead (to handle leap years and month boundaries)
        max_iterations = 365 * 4 * 24 * 60
        iterations = 0
        
        while iterations < max_iterations:
            iterations += 1
            
            # Check if this datetime matches the expression
            if cls._matches_cron(check_time, parsed):
                return check_time
            
            # Move to next minute
            try:
                check_time += timedelta(minutes=1)
            except OverflowError:
                # Handle very large dates by wrapping around
                check_time = datetime(9999, 12, 31, 23, 59)
        
        raise CronValidationError(
            f"Could not calculate next run time for cron expression: {expression}"
        )
    
    @classmethod
    def _matches_cron(cls, dt: datetime, parsed: Dict[str, List[int]]) -> bool:
        """Check if a datetime matches the parsed cron expression."""
        return (
            dt.minute in parsed['minute'] and
            dt.hour in parsed['hour'] and
            dt.day in parsed['day'] and
            dt.month in parsed['month'] and
            dt.weekday() in parsed['day_of_week']
        )


class Scheduler:
    """
    Task scheduler supporting delayed and recurring tasks.
    
    Features:
    - One-shot delayed tasks (execute after a delay)
    - Recurring tasks (execute at regular intervals)
    - Cron-like expressions for complex schedules
    - Thread-safe using internal locking
    - Integration with TaskQueue for task submission
    """
    
    def __init__(self, queue: Optional[TaskQueue] = None):
        """
        Initialize the scheduler.
        
        Args:
            queue: TaskQueue to submit tasks to (creates a new one if None)
        """
        self._queue = queue if queue is not None else TaskQueue()
        self._scheduled_tasks: List[ScheduledTask] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._job_counter = 0
        self._timers: Dict[str, threading.Timer] = {}
        self._job_counter_lock = threading.Lock()
    
    def schedule_delayed(
        self,
        task: Task,
        delay: float,
        callback: Optional[Callable[[Task], None]] = None
    ) -> str:
        """
        Schedule a one-shot delayed task.
        
        Args:
            task: The task to execute
            delay: Delay in seconds before execution
            callback: Optional callback to invoke when task is submitted
            
        Returns:
            Job ID for this scheduled task
        """
        job_id = self._generate_job_id()
        next_run = datetime.now() + timedelta(seconds=delay)
        
        scheduled = ScheduledTask(
            next_run=next_run,
            task=task,
            recurring=False,
            interval=delay,
            job_id=job_id,
            callback=callback
        )
        
        with self._lock:
            heapq.heappush(self._scheduled_tasks, scheduled)
            self._condition.notify()
        
        return job_id
    
    def schedule_recurring(
        self,
        task: Task,
        interval: float,
        callback: Optional[Callable[[Task], None]] = None
    ) -> str:
        """
        Schedule a recurring task with fixed interval.
        
        Args:
            task: The task to execute
            interval: Interval in seconds between executions
            callback: Optional callback to invoke when each task is submitted
            
        Returns:
            Job ID for this scheduled task
        """
        job_id = self._generate_job_id()
        next_run = datetime.now() + timedelta(seconds=interval)
        
        scheduled = ScheduledTask(
            next_run=next_run,
            task=task,
            recurring=True,
            interval=interval,
            job_id=job_id,
            callback=callback
        )
        
        with self._lock:
            heapq.heappush(self._scheduled_tasks, scheduled)
            self._condition.notify()
        
        return job_id
    
    def schedule_cron(
        self,
        task: Task,
        cron_expression: str,
        callback: Optional[Callable[[Task], None]] = None
    ) -> str:
        """
        Schedule a task using a cron-like expression.
        
        Args:
            task: The task to execute
            cron_expression: Cron expression (5 fields)
            callback: Optional callback to invoke when each task is submitted
            
        Returns:
            Job ID for this scheduled task
        """
        # Validate the cron expression
        next_run = CronParser.next_run_time(cron_expression)
        
        job_id = self._generate_job_id()
        
        scheduled = ScheduledTask(
            next_run=next_run,
            task=task,
            recurring=True,
            cron_expression=cron_expression,
            job_id=job_id,
            callback=callback
        )
        
        with self._lock:
            heapq.heappush(self._scheduled_tasks, scheduled)
            self._condition.notify()
        
        return job_id
    
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a scheduled job.
        
        Args:
            job_id: The job ID to cancel
            
        Returns:
            True if job was found and cancelled, False otherwise
        """
        with self._lock:
            # Cancel any active timer for this job
            if job_id in self._timers:
                self._timers[job_id].cancel()
                del self._timers[job_id]
            
            # Remove from scheduled tasks
            for i, scheduled in enumerate(self._scheduled_tasks):
                if scheduled.job_id == job_id:
                    self._scheduled_tasks.pop(i)
                    heapq.heapify(self._scheduled_tasks)
                    self._condition.notify()
                    return True
        
        return False
    
    def get_queue(self) -> TaskQueue:
        """
        Get the task queue used by the scheduler.
        
        Returns:
            The TaskQueue instance
        """
        return self._queue
    
    def start(self) -> None:
        """Start the scheduler in a background thread."""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._scheduler_thread = threading.Thread(
                target=self._run_scheduler,
                name="TaskScheduler",
                daemon=True
            )
            self._scheduler_thread.start()
    
    def stop(self) -> None:
        """Stop the scheduler."""
        with self._lock:
            self._running = False
            self._condition.notify_all()
        
        # Wait for scheduler thread to finish
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)
        
        # Cancel all active timers
        with self._lock:
            for timer in self._timers.values():
                timer.cancel()
            self._timers.clear()
    
    def _run_scheduler(self) -> None:
        """Main scheduler loop running in background thread."""
        while True:
            with self._lock:
                if not self._running:
                    break
                
                # Get the next scheduled task
                if not self._scheduled_tasks:
                    # No tasks, wait indefinitely
                    self._condition.wait()
                    continue
                
                next_scheduled = self._scheduled_tasks[0]
                now = datetime.now()
                
                wait_time = (next_scheduled.next_run - now).total_seconds()
                
                if wait_time <= 0:
                    # Time to run this task
                    scheduled = heapq.heappop(self._scheduled_tasks)
                    
                    # Create a timer to run the task outside the lock
                    timer = threading.Timer(
                        0,
                        self._execute_scheduled_task,
                        args=[scheduled]
                    )
                    timer.start()
                    self._timers[scheduled.job_id] = timer
                    
                    # Reschedule if recurring
                    if scheduled.recurring:
                        self._reschedule_task(scheduled)
                    
                    continue
                
                # Wait until next task is due
                self._condition.wait(min(wait_time, 60))  # Max 60s wait
    
    def _execute_scheduled_task(self, scheduled: ScheduledTask) -> None:
        """
        Execute a scheduled task.
        
        Args:
            scheduled: The scheduled task to execute
        """
        try:
            # Submit task to queue
            self._queue.put(scheduled.task)
            
            # Invoke callback if provided
            if scheduled.callback:
                scheduled.callback(scheduled.task)
        finally:
            # Clean up timer reference
            with self._lock:
                if scheduled.job_id in self._timers:
                    del self._timers[scheduled.job_id]
    
    def _reschedule_task(self, scheduled: ScheduledTask) -> None:
        """
        Reschedule a recurring task.
        
        Args:
            scheduled: The scheduled task to reschedule
        """
        if scheduled.interval is not None:
            # Fixed interval scheduling
            next_run = scheduled.next_run + timedelta(seconds=scheduled.interval)
        elif scheduled.cron_expression is not None:
            # Cron-based scheduling
            next_run = CronParser.next_run_time(
                scheduled.cron_expression,
                after=scheduled.next_run
            )
        else:
            return
        
        with self._lock:
            new_scheduled = ScheduledTask(
                next_run=next_run,
                task=scheduled.task,  # Note: This reuses the same task
                recurring=scheduled.recurring,
                interval=scheduled.interval,
                cron_expression=scheduled.cron_expression,
                callback=scheduled.callback,
                job_id=scheduled.job_id
            )
            heapq.heappush(self._scheduled_tasks, new_scheduled)
            self._condition.notify()
    
    def _generate_job_id(self) -> str:
        """Generate a unique job ID."""
        with self._job_counter_lock:
            self._job_counter += 1
            return f"job-{self._job_counter}-{int(time.time())}"
    
    @property
    def running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running
    
    def pending_jobs_count(self) -> int:
        """
        Get the number of pending scheduled jobs.
        
        Returns:
            Number of jobs waiting to be executed
        """
        with self._lock:
            return len(self._scheduled_tasks)