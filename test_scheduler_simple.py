#!/usr/bin/env python3
"""
Simple test to verify scheduler implementation works.
"""

import time
import sys
import os

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from pytaskq import Scheduler, Task, TaskQueue, CronParser, CronValidationError
    
    print("✓ Successfully imported scheduler components")
    
    # Test 1: CronParser basic parsing
    print("\nTest 1: CronParser basic parsing")
    result = CronParser.parse("* * * * *")
    print(f"  Parsed wildcard expression: {len(result['minute'])} minutes")
    assert len(result['minute']) == 60
    print("  ✓ Passed")
    
    # Test 2: CronParser single value
    print("\nTest 2: CronParser single value")
    result = CronParser.parse("5 10 15 3 1")
    assert result['minute'] == [5]
    assert result['hour'] == [10]
    print("  ✓ Passed")
    
    # Test 3: CronParser range
    print("\nTest 3: CronParser range")
    result = CronParser.parse("5-10 * * * *")
    assert result['minute'] == [5, 6, 7, 8, 9, 10]
    print("  ✓ Passed")
    
    # Test 4: CronParser step
    print("\nTest 4: CronParser step")
    result = CronParser.parse("*/15 * * * *")
    assert result['minute'] == [0, 15, 30, 45]
    print("  ✓ Passed")
    
    # Test 5: CronParser invalid expression
    print("\nTest 5: CronParser invalid expression")
    try:
        CronParser.parse("* * * *")
        print("  ✗ Should have raised error")
        sys.exit(1)
    except CronValidationError:
        print("  ✓ Correctly raised error")
    
    # Test 6: Scheduler initialization
    print("\nTest 6: Scheduler initialization")
    scheduler = Scheduler()
    assert not scheduler.running
    assert scheduler.pending_jobs_count() == 0
    print("  ✓ Passed")
    
    # Test 7: Schedule delayed task
    print("\nTest 7: Schedule delayed task")
    task = Task(name="Test Task", payload={"key": "value"})
    job_id = scheduler.schedule_delayed(task, delay=5.0)
    assert job_id.startswith("job-")
    assert scheduler.pending_jobs_count() == 1
    print(f"  ✓ Scheduled job: {job_id}")
    
    # Test 8: Schedule recurring task
    print("\nTest 8: Schedule recurring task")
    task2 = Task(name="Recurring Task")
    job_id2 = scheduler.schedule_recurring(task2, interval=10.0)
    assert scheduler.pending_jobs_count() == 2
    print(f"  ✓ Scheduled recurring job: {job_id2}")
    
    # Test 9: Schedule cron task
    print("\nTest 9: Schedule cron task")
    task3 = Task(name="Cron Task")
    job_id3 = scheduler.schedule_cron(task3, "0 * * * *")
    assert scheduler.pending_jobs_count() == 3
    print(f"  ✓ Scheduled cron job: {job_id3}")
    
    # Test 10: Cancel job
    print("\nTest 10: Cancel job")
    cancelled = scheduler.cancel_job(job_id)
    assert cancelled is True
    assert scheduler.pending_jobs_count() == 2
    print("  ✓ Successfully cancelled job")
    
    # Test 11: Cancel non-existent job
    print("\nTest 11: Cancel non-existent job")
    cancelled = scheduler.cancel_job("nonexistent")
    assert cancelled is False
    print("  ✓ Correctly returned False")
    
    # Test 12: Start and stop scheduler
    print("\nTest 12: Start and stop scheduler")
    scheduler.start()
    assert scheduler.running
    print("  ✓ Scheduler started")
    time.sleep(0.1)
    scheduler.stop()
    assert not scheduler.running
    print("  ✓ Scheduler stopped")
    
    print("\n" + "="*50)
    print("All tests passed! ✓")
    print("="*50)
    
except ImportError as e:
    print(f"✗ Import error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"✗ Test failed with error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)