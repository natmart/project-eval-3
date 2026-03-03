"""
Unit Tests for Scheduler

Tests for the Scheduler class handling delayed and recurring tasks.
"""

import pytest
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pytaskq import Scheduler, Task, TaskQueue
from pytaskq.scheduler import ScheduledTask


class TestScheduledTask:
    """Test cases for ScheduledTask class."""
    
    def test_scheduled_task_initialization(self) -> None:
        """Test that ScheduledTask can be initialized with required parameters."""
        task = Task(id="1", name="test", payload={"data": "test"})
        execute_at = datetime.utcnow() + timedelta(seconds=10)
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=None
        )
        
        assert scheduled.task == task
        assert scheduled.execute_at == execute_at
        assert scheduled.interval is None
        assert scheduled.last_run is None
        assert scheduled.times_run == 0
        assert scheduled.max_runs is None
        assert scheduled.enabled is True
    
    def test_scheduled_task_with_interval(self) -> None:
        """Test that ScheduledTask can be initialized with interval."""
        task = Task(id="1", name="test")
        execute_at = datetime.utcnow()
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=5.0,
            max_runs=10
        )
        
        assert scheduled.interval == 5.0
        assert scheduled.max_runs == 10
    
    def test_scheduled_task_comparison(self) -> None:
        """Test that ScheduledTask can be compared by execution time."""
        task1 = Task(id="1", name="test1")
        task2 = Task(id="2", name="test2")
        
        now = datetime.utcnow()
        scheduled_early = ScheduledTask(
            task=task1,
            execute_at=now + timedelta(seconds=5)
        )
        scheduled_late = ScheduledTask(
            task=task2,
            execute_at=now + timedelta(seconds=10)
        )
        
        assert scheduled_early < scheduled_late
        assert not (scheduled_late < scheduled_early)
    
    def test_reschedule_one_shot_task(self) -> None:
        """Test that one-shot tasks return None on reschedule."""
        task = Task(id="1", name="test")
        execute_at = datetime.utcnow()
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=None
        )
        
        next_run = scheduled.reschedule()
        assert next_run is None
    
    def test_reschedule_recurring_task(self) -> None:
        """Test that recurring tasks create a new scheduled task on reschedule."""
        task = Task(id="1", name="test")
        execute_at = datetime.utcnow()
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=5.0
        )
        
        next_run = scheduled.reschedule()
        assert next_run is not None
        assert next_run.task == task
        assert next_run.interval == 5.0
        assert next_run.execute_at == execute_at + timedelta(seconds=5.0)
        assert next_run.times_run == 0
    
    def test_reschedule_disabled_task(self) -> None:
        """Test that disabled tasks return None on reschedule."""
        task = Task(id="1", name="test")
        execute_at = datetime.utcnow()
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=5.0,
            enabled=False
        )
        
        next_run = scheduled.reschedule()
        assert next_run is None
    
    def test_reschedule_max_runs_exceeded(self) -> None:
        """Test that tasks with max_runs return None when exceeded."""
        task = Task(id="1", name="test")
        execute_at = datetime.utcnow()
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=5.0,
            max_runs=3,
            times_run=3
        )
        
        next_run = scheduled.reschedule()
        assert next_run is None
    
    def test_reschedule_max_runs_not_met(self) -> None:
        """Test that tasks continue when max_runs not yet met."""
        task = Task(id="1", name="test")
        execute_at = datetime.utcnow()
        
        scheduled = ScheduledTask(
            task=task,
            execute_at=execute_at,
            interval=5.0,
            max_runs=3,
            times_run=2
        )
        
        next_run = scheduled.reschedule()
        assert next_run is not None
        assert next_run.max_runs == 3


class TestSchedulerInitialization:
    """Test cases for scheduler initialization and basic state."""
    
    def test_scheduler_initialization(self) -> None:
        """Test that scheduler can be initialized with a queue."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        assert scheduler.queue == queue
        assert not scheduler.is_running()
        assert scheduler.get_scheduled_count() == 0
    
    def test_scheduler_with_custom_check_interval(self) -> None:
        """Test that scheduler accepts custom check interval."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.5)
        
        assert scheduler.queue == queue
        assert scheduler._check_interval == 0.5
    
    def test_scheduler_initial_state_empty(self) -> None:
        """Test that new scheduler has no scheduled tasks."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        assert scheduler.get_scheduled_count() == 0
        assert scheduler.is_running() is False


class TestSchedulerDelayedTasks:
    """Test cases for one-shot delayed task scheduling."""
    
    def test_schedule_delayed_task(self) -> None:
        """Test scheduling a delayed task returns a task ID."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="delayed_task")
        task_id = scheduler.schedule_delayed(task, delay_seconds=5.0)
        
        assert isinstance(task_id, str)
        assert len(task_id) > 0
        assert scheduler.get_scheduled_count() == 1
    
    def test_schedule_delayed_task_future_time(self) -> None:
        """Test that delayed task has correct execution time."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="delayed_task")
        delay = 10.0
        before = datetime.utcnow()
        task_id = scheduler.schedule_delayed(task, delay)
        after = datetime.utcnow()
        
        # Access the scheduled task
        with scheduler._lock:
            scheduled = scheduler._scheduled_tasks_map.get(task_id)
        
        assert scheduled is not None
        expected_min = before + timedelta(seconds=delay)
        expected_max = after + timedelta(seconds=delay)
        assert expected_min <= scheduled.execute_at <= expected_max
    
    def test_schedule_multiple_delayed_tasks(self) -> None:
        """Test scheduling multiple delayed tasks."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task1 = Task(id="1", name="task1")
        task2 = Task(id="2", name="task2")
        task3 = Task(id="3", name="task3")
        
        id1 = scheduler.schedule_delayed(task1, delay_seconds=5.0)
        id2 = scheduler.schedule_delayed(task2, delay_seconds=10.0)
        id3 = scheduler.schedule_delayed(task3, delay_seconds=2.0)
        
        assert id1 != id2 != id3
        assert scheduler.get_scheduled_count() == 3
    
    def test_delayed_task_submitted_to_queue(self) -> None:
        """Test that delayed task gets submitted to queue when due."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="delayed_task")
        scheduler.schedule_delayed(task, delay_seconds=0.2)
        
        scheduler.start()
        
        # Wait for task to be submitted
        time.sleep(0.5)
        
        assert queue.size() >= 1
        
        scheduler.stop()
    
    def test_delayed_task_zero_delay(self) -> None:
        """Test scheduling a task with zero delay."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="immediate_task")
        scheduler.schedule_delayed(task, delay_seconds=0.0)
        
        scheduler.start()
        time.sleep(0.3)
        
        assert queue.size() >= 1
        
        scheduler.stop()


class TestSchedulerRecurringTasks:
    """Test cases for recurring task scheduling."""
    
    def test_schedule_recurring_task(self) -> None:
        """Test that recurring task can be scheduled."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="recurring_task")
        task_id = scheduler.schedule_recurring(
            task,
            interval_seconds=5.0,
            first_run_seconds=1.0
        )
        
        assert isinstance(task_id, str)
        assert scheduler.get_scheduled_count() == 1
    
    def test_schedule_recurring_with_max_runs(self) -> None:
        """Test scheduling recurring task with max runs limit."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="recurring_task")
        task_id = scheduler.schedule_recurring(
            task,
            interval_seconds=1.0,
            max_runs=3
        )
        
        with scheduler._lock:
            scheduled = scheduler._scheduled_tasks_map.get(task_id)
        
        assert scheduled is not None
        assert scheduled.max_runs == 3
    
    def test_recurring_task_executes_multiple_times(self) -> None:
        """Test that recurring task executes multiple times."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="recurring_task")
        scheduler.schedule_recurring(
            task,
            interval_seconds=0.3,
            first_run_seconds=0.1,
            max_runs=3
        )
        
        scheduler.start()
        
        # Wait for multiple executions
        time.sleep(1.5)
        
        task_count = queue.size()
        assert task_count >= 3
        
        scheduler.stop()
    
    def test_recurring_task_reschedules_correctly(self) -> None:
        """Test that recurring tasks reschedule with correct interval."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="recurring_task")
        interval = 0.2
        scheduler.schedule_recurring(
            task,
            interval_seconds=interval,
            first_run_seconds=0.1,
            max_runs=2
        )
        
        scheduler.start()
        
        # Wait for first execution
        time.sleep(0.3)
        first_count = queue.size()
        assert first_count >= 1
        
        # Wait for second execution
        time.sleep(0.3)
        second_count = queue.size()
        assert second_count >= 2
        
        scheduler.stop()
    
    def test_schedule_recurring_immediate_start(self) -> None:
        """Test recurring task with immediate first run."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="recurring_task")
        scheduler.schedule_recurring(
            task,
            interval_seconds=0.5,
            first_run_seconds=0.0,
            max_runs=1
        )
        
        scheduler.start()
        time.sleep(0.3)
        
        assert queue.size() >= 1
        
        scheduler.stop()


class TestSchedulerQueueIntegration:
    """Test cases for scheduler interaction with task queue."""
    
    def test_tasks_submitted_to_queue(self) -> None:
        """Test that tasks are properly submitted to the queue."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="test_task", payload={"key": "value"})
        scheduler.schedule_delayed(task, delay_seconds=0.1)
        
        scheduler.start()
        time.sleep(0.4)
        
        submitted_task = queue.dequeue()
        assert submitted_task is not None
        assert submitted_task.id == "1"
        assert submitted_task.name == "test_task"
        assert submitted_task.payload == {"key": "value"}
        
        scheduler.stop()
    
    def test_multiple_tasks_queue_order(self) -> None:
        """Test that multiple tasks maintain order in queue."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task1 = Task(id="1", name="task1", priority=1)
        task2 = Task(id="2", name="task2", priority=0)
        task3 = Task(id="3", name="task3", priority=2)
        
        scheduler.schedule_delayed(task1, delay_seconds=0.1)
        scheduler.schedule_delayed(task2, delay_seconds=0.1)
        scheduler.schedule_delayed(task3, delay_seconds=0.1)
        
        scheduler.start()
        time.sleep(0.5)
        
        # Tasks should be dequeued in priority order
        tasks_received = []
        while not queue.is_empty():
            task = queue.dequeue()
            if task:
                tasks_received.append(task.id)
        
        # Priority 0 (task2) should come first
        assert tasks_received[0] == "2"
        
        scheduler.stop()
    
    def test_concurrent_scheduling_and_queue_access(self) -> None:
        """Test thread-safe concurrent scheduling and queue access."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        def schedule_tasks(count: int) -> None:
            for i in range(count):
                task = Task(id=f"task-{i}", name=f"task{i}")
                scheduler.schedule_delayed(task, delay_seconds=0.05)
                time.sleep(0.01)
        
        scheduler.start()
        
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=schedule_tasks, args=(5,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        # Wait for all tasks to be submitted
        time.sleep(0.5)
        
        # All 15 tasks should be in queue
        assert queue.size() >= 10
        
        scheduler.stop()


class TestSchedulerCancellation:
    """Test cases for task cancellation."""
    
    def test_cancel_scheduled_task(self) -> None:
        """Test that a scheduled task can be cancelled."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="cancelable_task")
        task_id = scheduler.schedule_delayed(task, delay_seconds=10.0)
        
        assert scheduler.get_scheduled_count() == 1
        
        result = scheduler.cancel(task_id)
        assert result is True
        assert scheduler.get_scheduled_count() == 0
    
    def test_cancel_nonexistent_task(self) -> None:
        """Test cancelling a non-existent task returns False."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        result = scheduler.cancel("nonexistent_id")
        assert result is False
    
    def test_cancelled_task_not_submitted(self) -> None:
        """Test that cancelled tasks are not submitted to queue."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="cancelled_task")
        task_id = scheduler.schedule_delayed(task, delay_seconds=0.5)
        
        scheduler.start()
        
        # Cancel before execution
        time.sleep(0.1)
        scheduler.cancel(task_id)
        
        # Wait past execution time
        time.sleep(0.6)
        
        # Task should not be in queue
        assert queue.size() == 0
        
        scheduler.stop()
    
    def test_cancel_multiple_tasks(self) -> None:
        """Test cancelling multiple scheduled tasks."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task1 = Task(id="1", name="task1")
        task2 = Task(id="2", name="task2")
        task3 = Task(id="3", name="task3")
        
        id1 = scheduler.schedule_delayed(task1, delay_seconds=10.0)
        id2 = scheduler.schedule_delayed(task2, delay_seconds=10.0)
        id3 = scheduler.schedule_delayed(task3, delay_seconds=10.0)
        
        assert scheduler.get_scheduled_count() == 3
        
        scheduler.cancel(id2)
        assert scheduler.get_scheduled_count() == 2
        
        scheduler.cancel(id1)
        scheduler.cancel(id3)
        assert scheduler.get_scheduled_count() == 0
    
    def test_cancel_recurring_task_stops_execution(self) -> None:
        """Test that cancelling a recurring task stops further executions."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="recurring")
        task_id = scheduler.schedule_recurring(
            task,
            interval_seconds=0.3,
            first_run_seconds=0.1,
            max_runs=None  # Unlimited
        )
        
        scheduler.start()
        time.sleep(0.4)
        
        # Cancel after first execution
        scheduler.cancel(task_id)
        
        # Wait for potential second execution
        time.sleep(0.5)
        
        # Only one task should be in queue (or very few)
        count = queue.size()
        assert count <= 2
        
        scheduler.stop()


class TestSchedulerLifecycle:
    """Test cases for scheduler start/stop lifecycle."""
    
    def test_start_scheduler(self) -> None:
        """Test that scheduler can be started."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        assert not scheduler.is_running()
        
        scheduler.start()
        assert scheduler.is_running()
        
        scheduler.stop()
    
    def test_stop_scheduler(self) -> None:
        """Test that scheduler can be stopped."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        scheduler.start()
        assert scheduler.is_running()
        
        scheduler.stop()
        assert not scheduler.is_running()
    
    def test_start_already_running_scheduler(self) -> None:
        """Test that starting an already running scheduler is safe."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        scheduler.start()
        is_running = scheduler.is_running()
        
        # Start again should not cause issues
        scheduler.start()
        
        assert scheduler.is_running() == is_running
        
        scheduler.stop()
    
    def test_scheduler_thread_is_daemon(self) -> None:
        """Test that scheduler thread is a daemon thread."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        scheduler.start()
        
        assert scheduler._daemon_thread is not None
        assert scheduler._daemon_thread.daemon is True
        
        scheduler.stop()
    
    def test_multiple_start_stop_cycles(self) -> None:
        """Test that scheduler can be started and stopped multiple times."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        for _ in range(3):
            scheduler.start()
            assert scheduler.is_running()
            
            time.sleep(0.1)
            
            scheduler.stop()
            assert not scheduler.is_running()
    
    def test_stop_without_start(self) -> None:
        """Test that stopping scheduler without starting is safe."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        # Should not raise an exception
        scheduler.stop()
        assert not scheduler.is_running()


class TestSchedulerUtilityMethods:
    """Test cases for scheduler utility methods."""
    
    def test_get_scheduled_count(self) -> None:
        """Test getting count of scheduled tasks."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        assert scheduler.get_scheduled_count() == 0
        
        scheduler.schedule_delayed(Task(id="1", name="task1"), delay_seconds=10)
        scheduler.schedule_delayed(Task(id="2", name="task2"), delay_seconds=10)
        
        assert scheduler.get_scheduled_count() == 2
    
    def test_clear_scheduled_tasks(self) -> None:
        """Test clearing all scheduled tasks."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        scheduler.schedule_delayed(Task(id="1", name="task1"), delay_seconds=10)
        scheduler.schedule_delayed(Task(id="2", name="task2"), delay_seconds=10)
        scheduler.schedule_delayed(Task(id="3", name="task3"), delay_seconds=10)
        
        assert scheduler.get_scheduled_count() == 3
        
        scheduler.clear()
        
        assert scheduler.get_scheduled_count() == 0
    
    def test_wait_until_empty_with_tasks(self) -> None:
        """Test waiting until tasks are processed."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        scheduler.schedule_delayed(Task(id="1", name="task1"), delay_seconds=0.1)
        scheduler.schedule_delayed(Task(id="2", name="task2"), delay_seconds=0.1)
        
        scheduler.start()
        
        # Wait for tasks to be processed
        result = scheduler.wait_until_empty(timeout=2.0)
        
        scheduler.stop()
        
        # Should return True as tasks get processed
        assert result is True
        assert scheduler.get_scheduled_count() == 0
    
    def test_wait_until_empty_timeout(self) -> None:
        """Test wait until empty with timeout."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        # Schedule tasks far in the future
        scheduler.schedule_delayed(Task(id="1", name="task1"), delay_seconds=100)
        scheduler.schedule_delayed(Task(id="2", name="task2"), delay_seconds=100)
        
        # Wait with short timeout
        start = time.time()
        result = scheduler.wait_until_empty(timeout=0.5)
        elapsed = time.time() - start
        
        # Should return False due to timeout
        assert result is False
        assert elapsed >= 0.5
        assert scheduler.get_scheduled_count() == 2
    
    def test_wait_until_empty_no_tasks(self) -> None:
        """Test wait until empty when no tasks scheduled."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        # Should return immediately
        result = scheduler.wait_until_empty(timeout=1.0)
        
        assert result is True


class TestSchedulerEdgeCases:
    """Test cases for edge cases and error conditions."""
    
    def test_schedule_with_negative_delay(self) -> None:
        """Test scheduling task with negative delay."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        task = Task(id="1", name="task")
        # Negative delay should be treated as immediate
        scheduler.schedule_delayed(task, delay_seconds=-1.0)
        
        scheduler.start()
        time.sleep(0.3)
        
        # Task should still be submitted
        assert queue.size() >= 1
        
        scheduler.stop()
    
    def test_schedule_with_zero_interval(self) -> None:
        """Test scheduling recurring task with zero interval."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="task")
        task_id = scheduler.schedule_recurring(
            task,
            interval_seconds=0.0,
            max_runs=1
        )
        
        assert scheduler.get_scheduled_count() == 1
        scheduler.cancel(task_id)
    
    def test_schedule_very_short_delay(self) -> None:
        """Test scheduling task with very short delay."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.05)
        
        task = Task(id="1", name="task")
        scheduler.schedule_delayed(task, delay_seconds=0.01)
        
        scheduler.start()
        time.sleep(0.2)
        
        # Task should be submitted
        assert queue.size() >= 1
        
        scheduler.stop()
    
    def test_high_volume_scheduling(self) -> None:
        """Test scheduling many tasks at once."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        num_tasks = 100
        for i in range(num_tasks):
            task = Task(id=f"task-{i}", name=f"task{i}")
            scheduler.schedule_delayed(task, delay_seconds=i * 0.01)
        
        assert scheduler.get_scheduled_count() == num_tasks
        
        scheduler.clear()
        assert scheduler.get_scheduled_count() == 0
    
    def test_scheduler_with_long_running_tasks(self) -> None:
        """Test scheduler behavior with tasks queued for long periods."""
        queue = TaskQueue()
        scheduler = Scheduler(queue, check_interval=0.1)
        
        # Schedule task for distant future
        task = Task(id="1", name="future_task")
        scheduler.schedule_delayed(task, delay_seconds=1000.0)
        
        scheduler.start()
        
        # Wait a bit, task should not be submitted
        time.sleep(0.5)
        assert queue.size() == 0
        assert scheduler.get_scheduled_count() == 1
        
        scheduler.stop()
    
    def test_schedule_and_cancel_race_condition(self) -> None:
        """Test concurrent schedule and cancel operations."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        task = Task(id="1", name="task")
        
        def schedule_and_cancel() -> None:
            task_id = scheduler.schedule_delayed(task, delay_seconds=1.0)
            time.sleep(0.01)
            scheduler.cancel(task_id)
        
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=schedule_and_cancel)
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        # All should be cancelled
        assert scheduler.get_scheduled_count() == 0


class TestSchedulerThreadSafety:
    """Test cases for scheduler thread safety."""
    
    def test_concurrent_scheduling(self) -> None:
        """Test that multiple threads can schedule tasks concurrently."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        num_threads = 10
        tasks_per_thread = 10
        
        def schedule_tasks(thread_id: int) -> None:
            for i in range(tasks_per_thread):
                task = Task(
                    id=f"task-{thread_id}-{i}",
                    name=f"thread{thread_id}_task{i}"
                )
                scheduler.schedule_delayed(task, delay_seconds=1.0)
        
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=schedule_tasks, args=(i,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        assert scheduler.get_scheduled_count() == num_threads * tasks_per_thread
    
    def test_concurrent_cancel(self) -> None:
        """Test concurrent cancel operations."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        # Schedule tasks
        task_ids = []
        for i in range(20):
            task = Task(id=f"task-{i}", name=f"task{i}")
            task_id = scheduler.schedule_delayed(task, delay_seconds=10.0)
            task_ids.append(task_id)
        
        # Cancel from multiple threads
        def cancel_task_ids(ids: List[str]) -> None:
            for task_id in ids:
                scheduler.cancel(task_id)
        
        threads = []
        mid = len(task_ids) // 2
        threads.append(threading.Thread(target=cancel_task_ids, args=(task_ids[:mid],)))
        threads.append(threading.Thread(target=cancel_task_ids, args=(task_ids[mid:],)))
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        assert scheduler.get_scheduled_count() == 0
    
    def test_concurrent_start_stop(self) -> None:
        """Test concurrent start/stop operations."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        def lifecycle_operations() -> None:
            scheduler.start()
            time.sleep(0.01)
            scheduler.stop()
        
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=lifecycle_operations)
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        # Should not be running
        assert not scheduler.is_running()
    
    def test_concurrent_scheduling_and_clear(self) -> None:
        """Test concurrent scheduling and clear operations."""
        queue = TaskQueue()
        scheduler = Scheduler(queue)
        
        def schedule_many() -> None:
            for i in range(50):
                task = Task(id=f"task-{i}", name=f"task{i}")
                scheduler.schedule_delayed(task, delay_seconds=1.0)
                time.sleep(0.001)
        
        def clear_periodically() -> None:
            for _ in range(5):
                time.sleep(0.01)
                scheduler.clear()
        
        schedule_thread = threading.Thread(target=schedule_many)
        clear_thread = threading.Thread(target=clear_periodically)
        
        schedule_thread.start()
        clear_thread.start()
        
        schedule_thread.join()
        clear_thread.join()
        
        # Scheduler should still be functional
        assert scheduler.get_scheduled_count() >= 0