#!/usr/bin/env python
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("Testing storage backend imports...")

try:
    from pytaskq.task import Task, TaskStatus
    print("✓ Task imports successful")
except Exception as e:
    print(f"✗ Task import failed: {e}")
    sys.exit(1)

try:
    from pytaskq.storage import StorageBackend, SQLiteBackend
    print("✓ Storage imports successful")
except Exception as e:
    print(f"✗ Storage import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nTesting basic functionality...")

try:
    # Test in-memory database
    backend = SQLiteBackend(db_path=":memory:")
    print("✓ SQLiteBackend initialized")

    # Test save_task
    task = Task(id="test-1", name="Test Task", payload={"key": "value"}, priority=5)
    backend.save_task(task)
    print("✓ Task saved")

    # Test get_task
    retrieved = backend.get_task("test-1")
    assert retrieved is not None
    assert retrieved.name == "Test Task"
    assert retrieved.payload == {"key": "value"}
    assert retrieved.priority == 5
    print("✓ Task retrieved successfully")

    # Test list_tasks
    tasks = backend.list_tasks()
    assert len(tasks) == 1
    print("✓ List tasks successful")

    # Test update_task_status
    success = backend.update_task_status("test-1", TaskStatus.RUNNING)
    assert success is True
    print("✓ Task status updated")

    # Verify status update
    updated = backend.get_task("test-1")
    assert updated.status == TaskStatus.RUNNING
    print("✓ Status verified")

    # Test error handling - nonexistent task
    assert backend.get_task("nonexistent") is None
    print("✓ Get nonexistent task returns None")

    # Test error handling - update nonexistent task
    assert backend.update_task_status("nonexistent", TaskStatus.COMPLETED) is False
    print("✓ Update nonexistent task returns False")

    backend.close()
    print("✓ Backend closed")

    # Test persistence
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    try:
        # First session
        backend1 = SQLiteBackend(db_path=db_path)
        task1 = Task(id="persist-1", name="Persistent Task")
        backend1.save_task(task1)
        backend1.close()

        # Second session
        backend2 = SQLiteBackend(db_path=db_path)
        persisted = backend2.get_task("persist-1")
        assert persisted is not None
        assert persisted.name == "Persistent Task"
        backend2.close()
        print("✓ Persistence test passed")
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

    print("\n✅ All tests passed!")
    sys.exit(0)

except Exception as e:
    print(f"\n✗ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)