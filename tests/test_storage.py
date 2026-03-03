"""
Tests for Storage Backend

This module contains comprehensive tests for StorageBackend and SQLiteBackend implementations.
"""

import os
import sqlite3
import tempfile
from unittest.mock import patch, MagicMock

import pytest
from datetime import datetime

from pytaskq import Task, TaskStatus
from pytaskq.storage import StorageBackend, SQLiteBackend


class TestStorageBackendAbstract:
    """Test cases for the abstract StorageBackend class."""
    
    def test_storage_backend_is_abstract(self):
        """Test that StorageBackend cannot be instantiated directly."""
        with pytest.raises(TypeError):
            StorageBackend()
    
    def test_storage_backend_has_required_methods(self):
        """Test that StorageBackend defines all required abstract methods."""
        abstract_methods = StorageBackend.__abstractmethods__
        assert 'save_task' in abstract_methods
        assert 'get_task' in abstract_methods
        assert 'list_tasks' in abstract_methods
        assert 'update_task_status' in abstract_methods
        assert 'close' in abstract_methods


class TestSQLiteBackendInitialization:
    """Test cases for SQLiteBackend initialization."""
    
    def test_initialization_in_memory(self):
        """Test initializing SQLiteBackend with in-memory database."""
        backend = SQLiteBackend(db_path=":memory:")
        assert backend.db_path == ":memory:"
        backend.close()
    
    def test_initialization_file_path(self):
        """Test initializing SQLiteBackend with file path."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            backend = SQLiteBackend(db_path=db_path)
            assert backend.db_path == db_path
            backend.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_database_schema_created(self):
        """Test that database schema is created on initialization."""
        backend = SQLiteBackend(db_path=":memory:")
        
        conn = backend._get_connection()
        cursor = conn.cursor()
        
        # Check that tasks table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='tasks'
        """)
        result = cursor.fetchone()
        assert result is not None
        
        # Check that indexes exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND tbl_name='tasks'
        """)
        indexes = cursor.fetchall()
        index_names = [idx[0] for idx in indexes]
        assert 'idx_tasks_status' in index_names
        assert 'idx_tasks_priority' in index_names
        
        backend.close()
    
    def test_context_manager(self):
        """Test using SQLiteBackend as a context manager."""
        with SQLiteBackend(db_path=":memory:") as backend:
            task = Task(name="Test Task")
            backend.save_task(task)
        
        # Connection should be closed after exiting context


class TestSQLLiteBackendCRUD:
    """Test cases for CRUD operations."""
    
    def test_save_task_and_retrieve(self):
        """Test saving a task and retrieving it."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(
            id="task-1",
            name="Test Task",
            payload={"key": "value"},
            priority=5,
            status=TaskStatus.PENDING,
            retry_count=0,
            max_retries=3,
        )
        
        backend.save_task(task)
        retrieved = backend.get_task("task-1")
        
        assert retrieved is not None
        assert retrieved.id == "task-1"
        assert retrieved.name == "Test Task"
        assert retrieved.payload == {"key": "value"}
        assert retrieved.priority == 5
        assert retrieved.status == TaskStatus.PENDING
        assert retrieved.retry_count == 0
        assert retrieved.max_retries == 3
        
        backend.close()
    
    def test_save_task_with_defaults(self):
        """Test saving a task with default values."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Default Task")
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        
        assert retrieved is not None
        assert retrieved.name == "Default Task"
        assert retrieved.payload is None
        assert retrieved.priority == 0
        assert retrieved.status == TaskStatus.PENDING
        assert retrieved.retry_count == 0
        assert retrieved.max_retries == 3
        assert isinstance(retrieved.created_at, datetime)
        
        backend.close()
    
    def test_get_nonexistent_task(self):
        """Test retrieving a task that doesn't exist."""
        backend = SQLiteBackend(db_path=":memory:")
        
        retrieved = backend.get_task("nonexistent-id")
        assert retrieved is None
        
        backend.close()
    
    def test_list_tasks_empty(self):
        """Test listing tasks when database is empty."""
        backend = SQLiteBackend(db_path=":memory:")
        
        tasks = backend.list_tasks()
        assert tasks == []
        
        backend.close()
    
    def test_list_tasks_single(self):
        """Test listing a single task."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Single Task", priority=1)
        backend.save_task(task)
        
        tasks = backend.list_tasks()
        assert len(tasks) == 1
        assert tasks[0].id == task.id
        
        backend.close()
    
    def test_list_tasks_multiple(self):
        """Test listing multiple tasks."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task1 = Task(name="Task 1", priority=2)
        task2 = Task(name="Task 2", priority=1)
        task3 = Task(name="Task 3", priority=3)
        
        backend.save_task(task1)
        backend.save_task(task2)
        backend.save_task(task3)
        
        tasks = backend.list_tasks()
        assert len(tasks) == 3
        
        # Should be ordered by priority (ascending)
        assert tasks[0].priority == 1
        assert tasks[1].priority == 2
        assert tasks[2].priority == 3
        
        backend.close()
    
    def test_update_task_status(self):
        """Test updating task status."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Status Task", status=TaskStatus.PENDING)
        backend.save_task(task)
        
        # Update status to RUNNING
        success = backend.update_task_status(task.id, TaskStatus.RUNNING)
        assert success is True
        
        # Verify the update
        updated = backend.get_task(task.id)
        assert updated.status == TaskStatus.RUNNING
        
        backend.close()
    
    def test_update_task_status_all_states(self):
        """Test updating task status through all states."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="State Transition Task")
        backend.save_task(task)
        
        states = [
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
        ]
        
        for state in states:
            success = backend.update_task_status(task.id, state)
            assert success is True
            
            updated = backend.get_task(task.id)
            assert updated.status == state
        
        backend.close()
    
    def test_update_nonexistent_task_status(self):
        """Test updating status of a task that doesn't exist."""
        backend = SQLiteBackend(db_path=":memory:")
        
        success = backend.update_task_status("nonexistent-id", TaskStatus.RUNNING)
        assert success is False
        
        backend.close()
    
    def test_save_task_replace_existing(self):
        """Test that saving a task with existing ID replaces it."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(id="replace-me", name="Original", priority=1)
        backend.save_task(task)
        
        # Save new task with same ID
        updated_task = Task(id="replace-me", name="Updated", priority=2, status=TaskStatus.RUNNING)
        backend.save_task(updated_task)
        
        # Verify the task was replaced
        retrieved = backend.get_task("replace-me")
        assert retrieved.name == "Updated"
        assert retrieved.priority == 2
        assert retrieved.status == TaskStatus.RUNNING
        
        # Should still be only one task
        tasks = backend.list_tasks()
        assert len(tasks) == 1
        
        backend.close()


class TestSQLiteBackendPersistence:
    """Test cases for persistence across sessions."""
    
    def test_persistence_across_sessions(self):
        """Test that data persists across multiple backend instances."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # First session: save a task
            backend1 = SQLiteBackend(db_path=db_path)
            task1 = Task(id="persistent-task", name="Persistent Task", payload={"data": 123})
            backend1.save_task(task1)
            backend1.close()
            
            # Second session: retrieve the task
            backend2 = SQLiteBackend(db_path=db_path)
            retrieved = backend2.get_task("persistent-task")
            
            assert retrieved is not None
            assert retrieved.id == "persistent-task"
            assert retrieved.name == "Persistent Task"
            assert retrieved.payload == {"data": 123}
            
            backend2.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_persistence_with_status_updates(self):
        """Test that status updates persist across sessions."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # First session: create and update task
            backend1 = SQLiteBackend(db_path=db_path)
            task = Task(id="status-persist", name="Status Test")
            backend1.save_task(task)
            
            # Update status in first session
            backend1.update_task_status("status-persist", TaskStatus.RUNNING)
            backend1.close()
            
            # Second session: verify status persisted
            backend2 = SQLiteBackend(db_path=db_path)
            retrieved = backend2.get_task("status-persist")
            assert retrieved.status == TaskStatus.RUNNING
            
            # Update in second session
            backend2.update_task_status("status-persist", TaskStatus.COMPLETED)
            backend2.close()
            
            # Third session: verify final status
            backend3 = SQLiteBackend(db_path=db_path)
            final = backend3.get_task("status-persist")
            assert final.status == TaskStatus.COMPLETED
            backend3.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_multiple_tasks_persist(self):
        """Test that multiple tasks persist across sessions."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # First session: save multiple tasks
            backend1 = SQLiteBackend(db_path=db_path)
            tasks = [Task(name=f"Task {i}", priority=i) for i in range(5)]
            for task in tasks:
                backend1.save_task(task)
            backend1.close()
            
            # Second session: retrieve all tasks
            backend2 = SQLiteBackend(db_path=db_path)
            persisted = backend2.list_tasks()
            
            assert len(persisted) == 5
            for i, task in enumerate(sorted(persisted, key=lambda t: t.priority)):
                assert task.name == f"Task {i}"
                assert task.priority == i
            
            backend2.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_task_count_persists(self):
        """Test that task count is accurate after persistence."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # First session: add tasks
            backend1 = SQLiteBackend(db_path=db_path)
            for i in range(10):
                backend1.save_task(Task(name=f"Task {i}"))
            
            count1 = len(backend1.list_tasks())
            assert count1 == 10
            backend1.close()
            
            # Second session: verify count
            backend2 = SQLiteBackend(db_path=db_path)
            count2 = len(backend2.list_tasks())
            assert count2 == 10
            backend2.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestSQLiteBackendErrorHandling:
    """Test cases for error handling and edge cases."""
    
    def test_get_task_with_invalid_id(self):
        """Test retrieving task with invalid ID format."""
        backend = SQLiteBackend(db_path=":memory:")
        
        # Empty string
        assert backend.get_task("") is None
        
        # Special characters
        assert backend.get_task("'; DROP TABLE tasks; --") is None
        
        # Very long ID
        long_id = "x" * 10000
        assert backend.get_task(long_id) is None
        
        backend.close()
    
    def test_save_task_with_none_payload(self):
        """Test saving task with None payload."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="No Payload", payload=None)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved is not None
        assert retrieved.payload is None
        
        backend.close()
    
    def test_save_task_with_complex_payload(self):
        """Test saving task with complex nested payload."""
        backend = SQLiteBackend(db_path=":memory:")
        
        complex_payload = {
            "nested": {
                "deeply": {
                    "nested": "value",
                    "numbers": [1, 2, 3],
                    "boolean": True,
                    "null": None,
                }
            },
            "array": [{"a": 1}, {"b": 2}],
        }
        
        task = Task(name="Complex Payload", payload=complex_payload)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved is not None
        assert retrieved.payload == complex_payload
        
        backend.close()
    
    def test_save_task_with_list_payload(self):
        """Test saving task with list payload."""
        backend = SQLiteBackend(db_path=":memory:")
        
        list_payload = [1, 2, 3, "four", {"five": 5}]
        task = Task(name="List Payload", payload=list_payload)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved is not None
        assert retrieved.payload == list_payload
        
        backend.close()
    
    def test_update_status_invalid_task_id(self):
        """Test update_status with non-existent task IDs."""
        backend = SQLiteBackend(db_path=":memory:")
        
        # Non-existent UUID
        result = backend.update_task_status(
            "00000000-0000-0000-0000-000000000000",
            TaskStatus.RUNNING
        )
        assert result is False
        
        # Empty string
        result = backend.update_task_status("", TaskStatus.RUNNING)
        assert result is False
        
        # SQL injection attempt (should be safe)
        result = backend.update_task_status("'; DELETE FROM tasks; --", TaskStatus.RUNNING)
        assert result is False
        
        # Verify no tasks were deleted
        count = len(backend.list_tasks())
        assert count == 0
        
        backend.close()
    
    def test_close_multiple_times(self):
        """Test that close can be called multiple times safely."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Test")
        backend.save_task(task)
        
        # Close multiple times - should not raise an error
        backend.close()
        backend.close()
        backend.close()
    
    def test_save_task_after_close(self):
        """Test that operations after close fail gracefully."""
        backend = SQLiteBackend(db_path=":memory:")
        backend.close()
        
        # After close, behavior depends on SQLite implementation
        # It should create a new connection when needed
        task = Task(name="After Close")
        backend.save_task(task)
        
        # If it fails, it should raise an appropriate error
        # If it succeeds (creates new connection), that's also acceptable
    
    def test_task_with_special_characters_in_name(self):
        """Test saving task with special characters in name."""
        backend = SQLiteBackend(db_path=":memory:")
        
        special_names = [
            "Task with 'quotes'",
            'Task with "double quotes"',
            "Task with\nnewline",
            "Tab\tcharacter",
            "Emoji \u2600 test",
            "日本語",
            "العربية",
        ]
        
        for name in special_names:
            task = Task(name=name)
            backend.save_task(task)
            retrieved = backend.get_task(task.id)
            assert retrieved.name == name
        
        backend.close()
    
    def test_negative_priority(self):
        """Test saving task with negative priority."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Negative Priority", priority=-10)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved.priority == -10
        
        backend.close()
    
    def test_high_priority_value(self):
        """Test saving task with very high priority value."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="High Priority", priority=1000000)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved.priority == 1000000
        
        backend.close()
    
    def test_zero_retry_count(self):
        """Test task with zero retry count."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="No Retries", retry_count=0, max_retries=0)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved.retry_count == 0
        assert retrieved.max_retries == 0
        
        backend.close()
    
    def test_large_retry_count(self):
        """Test task with large retry count."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Many Retries", retry_count=100, max_retries=1000)
        backend.save_task(task)
        
        retrieved = backend.get_task(task.id)
        assert retrieved.retry_count == 100
        assert retrieved.max_retries == 1000
        
        backend.close()


class TestSQLiteBackendSerialization:
    """Test cases for task serialization/deserialization."""
    
    def test_task_datetime_serialization(self):
        """Test that datetime is serialized and deserialized correctly."""
        backend = SQLiteBackend(db_path=":memory:")
        
        # Create task with specific time
        specific_time = datetime(2024, 1, 15, 10, 30, 45)
        task = Task(id="datetime-test", name="Datetime Test", created_at=specific_time)
        backend.save_task(task)
        
        # Retrieve and verify
        retrieved = backend.get_task("datetime-test")
        assert retrieved is not None
        assert isinstance(retrieved.created_at, datetime)
        assert retrieved.created_at.year == 2024
        assert retrieved.created_at.month == 1
        assert retrieved.created_at.day == 15
        assert retrieved.created_at.hour == 10
        assert retrieved.created_at.minute == 30
        assert retrieved.created_at.second == 45
        
        backend.close()
    
    def test_task_status_serialization_all_values(self):
        """Test that all TaskStatus values are serialized correctly."""
        backend = SQLiteBackend(db_path=":memory:")
        
        statuses = [
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
        ]
        
        for status in statuses:
            task = Task(name=f"Status {status.value}", status=status)
            backend.save_task(task)
            
            retrieved = backend.get_task(task.id)
            assert retrieved.status == status
        
        backend.close()
    
    def test_roundtrip_with_all_fields(self):
        """Test complete roundtrip with all task fields."""
        backend = SQLiteBackend(db_path=":memory:")
        
        original_task = Task(
            id="roundtrip-test",
            name="Roundtrip Test",
            payload={"key1": "value1", "key2": 42, "key3": [1, 2, 3]},
            priority=5,
            status=TaskStatus.RUNNING,
            retry_count=2,
            max_retries=5,
        )
        
        backend.save_task(original_task)
        retrieved = backend.get_task("roundtrip-test")
        
        assert retrieved.id == original_task.id
        assert retrieved.name == original_task.name
        assert retrieved.payload == original_task.payload
        assert retrieved.priority == original_task.priority
        assert retrieved.status == original_task.status
        assert retrieved.retry_count == original_task.retry_count
        assert retrieved.max_retries == original_task.max_retries
        # Note: created_at might have microsecond differences
        
        backend.close()


class TestSQLiteBackendConcurrency:
    """Test cases for concurrent access and thread safety."""
    
    def test_concurrent_save_from_multiple_threads(self):
        """Test saving tasks from multiple threads concurrently."""
        import threading
        
        backend = SQLiteBackend(db_path=":memory:")
        num_tasks = 100
        errors = []
        
        def save_task(index):
            try:
                task = Task(name=f"Concurrent Task {index}", priority=index)
                backend.save_task(task)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=save_task, args=(i,)) for i in range(num_tasks)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify no errors occurred
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # Verify all tasks were saved
        tasks = backend.list_tasks()
        assert len(tasks) == num_tasks
        
        backend.close()
    
    def test_concurrent_read_from_multiple_threads(self):
        """Test reading tasks from multiple threads concurrently."""
        import threading
        
        backend = SQLiteBackend(db_path=":memory:")
        
        # Save a task first
        task = Task(name="Shared Task", payload={"data": "shared"})
        backend.save_task(task)
        
        num_reads = 50
        errors = []
        results = []
        
        def read_task():
            try:
                retrieved = backend.get_task(task.id)
                results.append(retrieved)
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=read_task) for _ in range(num_reads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # Verify all reads returned the correct task
        assert len(results) == num_reads
        for result in results:
            assert result is not None
            assert result.id == task.id
            assert result.name == "Shared Task"
            assert result.payload == {"data": "shared"}
        
        backend.close()
    
    def test_concurrent_update_status(self):
        """Test updating status from multiple threads concurrently."""
        import threading
        
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(name="Concurrent Update")
        backend.save_task(task)
        
        num_updates = 20
        errors = []
        
        def update_status(status_value):
            try:
                backend.update_task_status(task.id, status_value)
            except Exception as e:
                errors.append(e)
        
        # Use different statuses for different threads
        statuses = [TaskStatus.RUNNING, TaskStatus.COMPLETED, TaskStatus.FAILED]
        threads = [
            threading.Thread(target=update_status, args=(statuses[i % len(statuses)],))
            for i in range(num_updates)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Verify no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # Verify the task still exists and has a valid status
        final = backend.get_task(task.id)
        assert final is not None
        assert final.status in statuses
        
        backend.close()


class TestSQLiteBackendEdgeCases:
    """Test cases for edge cases and boundary conditions."""
    
    def test_empty_database_operations(self):
        """Test operations on an empty database."""
        backend = SQLiteBackend(db_path=":memory:")
        
        assert backend.get_task("any-id") is None
        assert len(backend.list_tasks()) == 0
        
        # Update status should fail gracefully
        result = backend.update_task_status("any-id", TaskStatus.RUNNING)
        assert result is False
        
        backend.close()
    
    def test_single_character_id(self):
        """Test task with single character ID."""
        backend = SQLiteBackend(db_path=":memory:")
        
        task = Task(id="a", name="Single Char ID")
        backend.save_task(task)
        
        retrieved = backend.get_task("a")
        assert retrieved is not None
        assert retrieved.name == "Single Char ID"
        
        backend.close()
    
    def test_task_with_unicode_in_fields(self):
        """Test task with unicode characters in all fields."""
        backend = SQLiteBackend(db_path=":memory:")
        
        unicode_id = "id-тест-测试"
        unicode_name = "Täsk with Ünicode 🎉"
        unicode_payload = {"msg": "Hëllö Wörld 世界"}
        
        task = Task(id=unicode_id, name=unicode_name, payload=unicode_payload)
        backend.save_task(task)
        
        retrieved = backend.get_task(unicode_id)
        assert retrieved is not None
        assert retrieved.name == unicode_name
        assert retrieved.payload == unicode_payload
        
        backend.close()
    
    def test_many_tasks_performance(self):
        """Test handling a large number of tasks."""
        import time
        
        backend = SQLiteBackend(db_path=":memory:")
        
        num_tasks = 1000
        
        # Measure time to save
        start = time.time()
        for i in range(num_tasks):
            task = Task(name=f"Perf Task {i}", priority=i % 100)
            backend.save_task(task)
        save_time = time.time() - start
        
        # Measure time to list
        start = time.time()
        tasks = backend.list_tasks()
        list_time = time.time() - start
        
        assert len(tasks) == num_tasks
        
        # These should be reasonably fast (adjust thresholds as needed)
        assert save_time < 5.0, f"Save too slow: {save_time}s for {num_tasks} tasks"
        assert list_time < 1.0, f"List too slow: {list_time}s for {num_tasks} tasks"
        
        backend.close()
    
    def test_list_tasks_ordering_by_priority(self):
        """Test that list_tasks returns tasks ordered by priority."""
        backend = SQLiteBackend(db_path=":memory:")
        
        # Create tasks with varying priorities
        priorities = [5, 1, 3, 2, 4]
        tasks = []
        for i, priority in enumerate(priorities):
            task = Task(name=f"Priority {i}", priority=priority)
            backend.save_task(task)
        
        # Add tasks with same priority
        mid_time = datetime.utcnow()
        backend.save_task(Task(name="Same Priority 1", priority=2))
        import time
        time.sleep(0.001)  # Small delay to ensure different timestamps
        backend.save_task(Task(name="Same Priority 2", priority=2))
        
        # Retrieve and check ordering
        retrieved = backend.list_tasks()
        task_priorities = [t.priority for t in retrieved]
        
        # Should be sorted by priority (ascending)
        for i in range(len(task_priorities) - 1):
            assert task_priorities[i] <= task_priorities[i+1]
        
        backend.close()
    
    def test_database_file_permissions(self):
        """Test that database file is created with appropriate permissions."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Remove the temporary file so backend can create it
            os.unlink(db_path)
            
            backend = SQLiteBackend(db_path=db_path)
            task = Task(name="Permissions Test")
            backend.save_task(task)
            backend.close()
            
            # Check file exists and is readable
            assert os.path.exists(db_path)
            assert os.access(db_path, os.R_OK)
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)