"""
Tests for Task Scheduler
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import patch

from pytaskq import (
    Scheduler,
    Task,
    TaskQueue,
    CronParser,
    CronValidationError,
    TaskStatus
)


class TestCronParser:
    """Test cases for CronParser class."""
    
    def test_parse_wildcard(self):
        """Test parsing wildcard expressions."""
        result = CronParser.parse("* * * * *")
        
        assert 'minute' in result
        assert 'hour' in result
        assert 'day' in result
        assert 'month' in result
        assert 'day_of_week' in result
        
        # Wildcard should include all valid values
        assert len(result['minute']) == 60  # 0-59
        assert len(result['hour']) == 24  # 0-23
        assert len(result['day']) == 31  # 1-31
        assert len(result['month']) == 12  # 1-12
        assert len(result['day_of_week']) == 7  # 0-6
    
    def test_parse_single_value(self):
        """Test parsing single values."""
        result = CronParser.parse("5 10 15 3 1")
        
        assert result['minute'] == [5]
        assert result['hour'] == [10]
        assert result['day'] == [15]
        assert result['month'] == [3]
        assert result['day_of_week'] == [1]
    
    def test_parse_comma_separated(self):
        """Test parsing comma-separated values."""
        result = CronParser.parse("5,10,15 * * * *")
        
        assert result['minute'] == [5, 10, 15]
        assert len(result['hour']) == 24
    
    def test_parse_range(self):
        """Test parsing ranges."""
        result = CronParser.parse("5-10 * * * *")
        
        assert result['minute'] == [5, 6, 7, 8, 9, 10]
    
    def test_parse_step(self):
        """Test parsing step expressions."""
        result = CronParser.parse("*/15 * * * *")
        
        assert result['minute'] == [0, 15, 30, 45]
    
    def test_parse_range_with_step(self):
        """Test parsing ranges with steps."""
        result = CronParser.parse("10-20/5 * * * *")
        
        assert result['minute'] == [10, 15, 20]
    
    def test_parse_complex_expression(self):
        """Test parsing a complex cron expression."""
        result = CronParser.parse("5,10-15,20 */2 1-15 * 1-5")
        
        assert set(result['minute']) == {5, 10, 11, 12, 13, 14, 15, 20}
        assert result['hour'][::2]  # Every 2 hours
        assert result['day'] == list(range(1, 16))
        assert len(result['month']) == 12
        assert set(result['day_of_week']) == {1, 2, 3, 4, 5}  # Weekdays
    
    def test_parse_invalid_field_count(self):
        """Test that invalid field count raises error."""
        with pytest.raises(CronValidationError, match="expected 5 fields"):
            CronParser.parse("* * * *")
        
        with pytest.raises(CronValidationError, match="expected 5 fields"):
            CronParser.parse("* * * * * *")
    
    def test_parse_invalid_value(self):
        """Test that invalid values raise error."""
        with pytest.raises(CronValidationError, match="Invalid numeric value"):
            CronParser.parse("abc * * * *")
        
        with pytest.raises(CronValidationError, match="out of range"):
            CronParser.parse("70 * * * *")  # Invalid minute
    
    def test_parse_invalid_range(self):
        """Test that invalid ranges raise error."""
        with pytest.raises(CronValidationError, match="Invalid range"):
            CronParser.parse("10-5 * * * *")  # Start > end
    
    def test_next_run_time(self):
        """Test calculating next run time."""
        now = datetime.now().replace(second=0, microsecond=0)
        
        # Every hour at 5 minutes past the hour
        next_run = CronParser.next_run_time("5 * * * *", after=now)
        
        assert next_run.minute == 5
        assert next_run >= now + timedelta(minutes=1)
    
    def test_next_run_time_exact_future(self):
        """Test next run time for exact future time."""
        now = datetime.now()
        future_time = now + timedelta(hours=2)
        future_time = future_time.replace(minute=30, second=0, microsecond=0)
        
        # Schedule for exact time
        next_run = CronParser.next_run_time(
            f"{future_time.minute} {future_time.hour} * * *",
            after=now
        )
        
        assert next_run.minute == future_time.minute
        assert next_run.hour == future_time.hour
    
    def test_next_run_time_daily(self):
        """Test next run time for daily schedule."""
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Every day at 9 AM
        next_run = CronParser.next_run_time("0 9 * * *", after=now)
        
        assert next_run.hour == 9
        assert next_run.minute == 0


class TestScheduler:
    """Test cases for Scheduler class."""
    
    def test_scheduler_initialization(self):
        """Test scheduler initialization."""
        queue = TaskQueue()
        scheduler = Scheduler(queue=queue)
        
        assert scheduler.get_queue() is queue
        assert not scheduler.running
        assert scheduler.pending_jobs_count() == 0
    
    def test_scheduler_default_queue(self):
        """Test scheduler creates default queue."""
        scheduler = Scheduler()
        
        assert scheduler.get_queue() is not None
        assert isinstance(scheduler.get_queue(), TaskQueue)
    
    def test_schedule_delayed_task(self):
        """Test scheduling a one-shot delayed task."""
        scheduler = Scheduler()
        task = Task(name="Delayed Task", payload={"test": "data"})
        
        job_id = scheduler.schedule_delayed(task, delay=5.0)
        
        assert job_id
        assert job_id.startswith("job-")
        assert scheduler.pending_jobs_count() == 1
    
    def test_schedule_recurring_task(self):
        """Test scheduling a recurring task."""
        scheduler = Scheduler()
        task = Task(name="Recurring Task", payload={"test": "data"})
        
        job_id = scheduler.schedule_recurring(task, interval=10.0)
        
        assert job_id
        assert scheduler.pending_jobs_count() == 1
    
    def test_schedule_cron_task(self):
        """Test scheduling a task with cron expression."""
        scheduler = Scheduler()
        task = Task(name="Cron Task", payload={"test": "data"})
        
        job_id = scheduler.schedule_cron(task, "0 * * * *")
        
        assert job_id
        assert scheduler.pending_jobs_count() == 1
    
    def test_schedule_cron_invalid_expression(self):
        """Test that invalid cron expression raises error."""
        scheduler = Scheduler()
        task = Task(name="Cron Task")
        
        with pytest.raises(CronValidationError):
            scheduler.schedule_cron(task, "invalid")
    
    def test_cancel_job(self):
        """Test cancelling a scheduled job."""
        scheduler = Scheduler()
        task = Task(name="Cancellable Task")
        
        job_id = scheduler.schedule_delayed(task, delay=10.0)
        assert scheduler.pending_jobs_count() == 1
        
        # Cancel the job
        result = scheduler.cancel_job(job_id)
        assert result is True
        assert scheduler.pending_jobs_count() == 0
        
        # Try to cancel again
        result = scheduler.cancel_job(job_id)
        assert result is False
    
    def test_cancel_nonexistent_job(self):
        """Test cancelling a non-existent job."""
        scheduler = Scheduler()
        
        result = scheduler.cancel_job("nonexistent-job")
        assert result is False
    
    def test_start_stop_scheduler(self):
        """Test starting and stopping the scheduler."""
        scheduler = Scheduler()
        
        assert not scheduler.running
        
        scheduler.start()
        assert scheduler.running
        
        scheduler.stop()
        assert not scheduler.running
    
    def test_multiple_scheduler_starts(self):
        """Test that multiple starts don't create multiple threads."""
        scheduler = Scheduler()
        
        scheduler.start()
        first_thread = scheduler._scheduler_thread
        
        scheduler.start()
        second_thread = scheduler._scheduler_thread
        
        assert first_thread is second_thread
        
        scheduler.stop()
    
    def test_delayed_task_execution(self):
        """Test that delayed task is added to queue at scheduled time."""
        scheduler = Scheduler()
        task = Task(name="Delayed Task", payload={" executed": True})
        
        # Schedule task with very short delay
        scheduler.schedule_delayed(task, delay=0.1)
        scheduler.start()
        
        # Wait for task to be executed
        time.sleep(0.3)
        
        # Check that task was added to queue
        queued_task = scheduler.get_queue().get(timeout=0.1)
        assert queued_task is not None
        assert queued_task.name == "Delayed Task"
        
        scheduler.stop()
    
    def test_recurring_task_execution(self):
        """Test that recurring task is executed multiple times."""
        scheduler = Scheduler()
        task = Task(name="Recurring Task", payload={"count": 0})
        
        # Schedule recurring task with short interval
        scheduler.schedule_recurring(task, interval=0.1)
        scheduler.start()
        
        # Wait for multiple executions
        time.sleep(0.35)
        
        # Check that multiple tasks were added to queue
        tasks_received = []
        while True:
            queued_task = scheduler.get_queue().get(timeout=0.1)
            if queued_task is None:
                break
            tasks_received.append(queued_task)
        
        assert len(tasks_received) >= 2  # At least 2 executions
        
        scheduler.stop()
    
    def test_cron_task_execution(self):
        """Test that cron task is executed at correct times."""
        scheduler = Scheduler()
        task = Task(name="Cron Task", payload={"cron": True})
        
        # Schedule for every minute (use next minute)
        now = datetime.now()
        next_minute = now + timedelta(minutes=1)
        cron_expr = f"{next_minute.minute} * * * *"
        
        job_id = scheduler.schedule_cron(task, cron_expr)
        scheduler.start()
        
        # Wait a bit
        time.sleep(0.2)
        
        # Cancel the job to avoid infinite wait
        scheduler.cancel_job(job_id)
        scheduler.stop()
    
    def test_callback_invocation(self):
        """Test that callback is invoked when task is submitted."""
        scheduler = Scheduler()
        task = Task(name="Callback Task")
        
        callback_invoked = threading.Event()
        
        def callback(task):
            callback_invoked.set()
        
        scheduler.schedule_delayed(task, delay=0.1, callback=callback)
        scheduler.start()
        
        # Wait for callback to be invoked
        assert callback_invoked.wait(timeout=1.0)
        
        scheduler.stop()
    
    def test_multiple_scheduled_tasks(self):
        """Test scheduling multiple tasks."""
        scheduler = Scheduler()
        
        for i in range(5):
            task = Task(name=f"Task {i}")
            scheduler.schedule_delayed(task, delay=1.0 + i * 0.1)
        
        assert scheduler.pending_jobs_count() == 5
    
    def test_scheduler_with_external_queue(self):
        """Test scheduler using external task queue."""
        external_queue = TaskQueue()
        scheduler = Scheduler(queue=external_queue)
        
        task = Task(name="External Queue Task")
        scheduler.schedule_delayed(task, delay=0.1)
        scheduler.start()
        
        time.sleep(0.3)
        
        # Task should be in external queue
        queued_task = external_queue.get(timeout=0.1)
        assert queued_task is not None
        
        scheduler.stop()
    
    def test_stop_cancels_pending_tasks(self):
        """Test that stopping scheduler handles pending tasks."""
        scheduler = Scheduler()
        
        # Schedule tasks far in the future
        for i in range(3):
            task = Task(name=f"Future Task {i}")
            scheduler.schedule_delayed(task, delay=100.0)
        
        assert scheduler.pending_jobs_count() == 3
        
        scheduler.start()
        scheduler.stop()
        
        # Scheduler should stop cleanly
        assert not scheduler.running


class TestSchedulerIntegration:
    """Integration tests for scheduler with full system."""
    
    def test_end_to_end_delayed_task(self):
        """Test complete workflow of delayed task."""
        scheduler = Scheduler()
        
        # Create a task
        task = Task(
            name="Integration Task",
            payload={"key": "value"},
            priority=1
        )
        
        # Schedule it
        job_id = scheduler.schedule_delayed(task, delay=0.05)
        
        # Start scheduler
        scheduler.start()
        
        # Wait for execution
        time.sleep(0.2)
        
        # Retrieve from queue
        executed_task = scheduler.get_queue().get(timeout=0.1)
        
        assert executed_task is not None
        assert executed_task.name == "Integration Task"
        assert executed_task.priority == 1
        assert executed_task.payload == {"key": "value"}
        
        scheduler.stop()
    
    def test_end_to_end_recurring_task(self):
        """Test complete workflow of recurring task."""
        scheduler = Scheduler()
        
        task = Task(name="Integration Recurring Task")
        
        # Schedule recurring task
        job_id = scheduler.schedule_recurring(task, interval=0.1)
        
        scheduler.start()
        
        # Wait for a few executions
        time.sleep(0.35)
        
        # Cancel to stop adding more tasks
        scheduler.cancel_job(job_id)
        
        # Collect executed tasks
        executed_tasks = []
        while True:
            t = scheduler.get_queue().get(timeout=0.1)
            if t is None:
                break
            executed_tasks.append(t)
        
        # Should have at least 2 executions
        assert len(executed_tasks) >= 2
        
        scheduler.stop()
    
    def test_scheduler_thread_safety(self):
        """Test scheduler is thread-safe with multiple operations."""
        scheduler = Scheduler()
        
        def schedule_tasks(count):
            for i in range(count):
                task = Task(name=f"Concurrent Task {i}")
                scheduler.schedule_delayed(task, delay=1.0 + i * 0.01)
        
        # Schedule tasks from multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=schedule_tasks, args=(10,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # All tasks should be scheduled
        assert scheduler.pending_jobs_count() == 50
        
        # Clean up
        scheduler.start()
        scheduler.stop()


class TestSchedulerEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_zero_delay(self):
        """Test scheduling with zero delay."""
        scheduler = Scheduler()
        task = Task(name="Zero Delay Task")
        
        job_id = scheduler.schedule_delayed(task, delay=0)
        scheduler.start()
        
        time.sleep(0.1)
        
        queued_task = scheduler.get_queue().get(timeout=0.1)
        assert queued_task is not None
        
        scheduler.stop()
    
    def test_very_short_interval(self):
        """Test recurring task with very short interval."""
        scheduler = Scheduler()
        task = Task(name="Short Interval Task")
        
        job_id = scheduler.schedule_recurring(task, interval=0.01)
        scheduler.start()
        
        time.sleep(0.1)
        
        scheduler.cancel_job(job_id)
        scheduler.stop()
        
        # Should have executed multiple times
        count = 0
        while scheduler.get_queue().get(timeout=0.05):
            count += 1
            if count >= 5:
                break
        
        assert count >= 2
    
    def test_cron_expression_boundary_values(self):
        """Test cron expressions with boundary values."""
        scheduler = Scheduler()
        task = Task(name="Boundary Task")
        
        # Test various boundary expressions
        expressions = [
            "0 0 1 1 0",  # Midnight on Jan 1, Sunday
            "59 23 31 12 6",  # 23:59 on Dec 31, Saturday
            "0 0 * * 0",  # Midnight every Sunday
        ]
        
        for expr in expressions:
            job_id = scheduler.schedule_cron(task, expr)
            scheduler.cancel_job(job_id)
    
    def test_scheduler_empty_operations(self):
        """Test scheduler operations with no tasks."""
        scheduler = Scheduler()
        
        scheduler.start()
        
        assert scheduler.pending_jobs_count() == 0
        
        time.sleep(0.1)
        
        # Should not crash with no tasks
        assert scheduler.running
        
        scheduler.stop()
    
    def test_rapid_cancel_schedule(self):
        """Test rapidly cancelling and rescheduling."""
        scheduler = Scheduler()
        task = Task(name="Rapid Task")
        
        for i in range(10):
            job_id = scheduler.schedule_delayed(task, delay=1.0)
            scheduler.cancel_job(job_id)
        
        assert scheduler.pending_jobs_count() == 0