#!/usr/bin/env python3
"""Simple test to check if the Task module imports correctly."""

try:
    import sys
    sys.path.insert(0, '.')
    from pytaskq.task import Task, TaskStatus
    print("✓ Import successful")

    # Test basic instantiation
    task = Task()
    print(f"✓ Task created with ID: {task.id}")

    # Test instantiation with values
    task2 = Task(name="Test Task", priority=1)
    print(f"✓ Task with values created: {task2.name}")

    # Test serialization
    task_dict = task.to_dict()
    print(f"✓ to_dict works: {list(task_dict.keys())}")

    # Test deserialization
    task3 = Task.from_dict(task_dict)
    print(f"✓ from_dict works: {task3.name}")

    print("\nAll basic tests passed!")
    sys.exit(0)
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)