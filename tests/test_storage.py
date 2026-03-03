"""
Tests for Storage Backend Implementation
"""

import os
import sqlite3
import tempfile
import threading
import time
from datetime import datetime

import pytest

from pytaskq.storage import SQLiteBackend, StorageBackend
from pytaskq.task import Task, TaskStatus


class TestStorageBackend:
    """Test the abstract StorageBackend class."""
    
    def test_abstract_class(self):
        """Verify StorageBackend is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            StorageBackend()


class TestSQLiteBackend:
    """Test the SQLiteBackend implementation."""
    
    @pytest.fixture
    def in_memory_backend(self):
        """Create an in-memory SQLite backend for testing."""
        backend = SQLiteBackend(":memory:")
        yield backend
        backend.close()
    
    @pytest.fixture
    def file_backend(self):
        """Create a file-based SQLite backend for testing."""
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        
        backend = SQLiteBackend(db_path)
        yield backend
        backend.close()
        
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.remove(db_path)
    
    @pytest.fixture
    def sample_task(self):
        """Create a sample task for testing."""
        return Task(
            name="test_task",
            payload={"key": "value"},
            priority=1,
            max_retries=5,
        )
    
    def test_initialization_memory_db(self):
        """Test initializing with in-memory database."""
        backend = SQLiteBackend(":memory:")
        assert backend.db_path == ":memory:"
        backend.close()
    
    def test_initialization_file_db(self):
        """Test initializing with file-based database."""
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        
        try:
            backend = SQLiteBackend(db_path)
            assert backend.db_path == db_path
            backend.close()
            
            # Verify file exists
            assert os.path.exists(db_path)
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)
    
    def test_save_task(self, in_memory_backend, sample_task):
        """Test saving a task to the database."""
        in_memory_backend.save_task(sample_task)
        
        # Verify task was saved
        retrieved = in_memory_backend.get_task(sample_task.id)
        assert retrieved is not None
        assert retrieved.id == sample_task.id
        assert retrieved.name == sample_task.name
        assert retrieved.payload == sample_task.payload
        assert retrieved.priority == sample_task.priority
        assert retrieved.status == sample_task.status
        assert retrieved.max_retries == sample_task.max_retries
    
    def test_save_task_overwrite(self, in_memory_backend, sample_task):
        """Test that saving a task with the same ID overwrites it."""
        in_memory_backend.save_task(sample_task)
        
        # Update the task
        sample_task.name = "updated_name"
        sample_task.status = TaskStatus.RUNNING
        in_memory_backend.save_task(sample_task)
        
        # Verify the task was updated
        retrieved = in_memory_backend.get_task(sample_task.id)
        assert retrieved is not None
        assert retrieved.name == "updated_name"
        assert retrieved.status == TaskStatus.RUNNING
    
    def test_get_task_not_found(self, in_memory_backend):
        """Test getting a task that doesn't exist."""
        result = in_memory_backend.get_task("non_existent_id")
        assert result is None
    
    def test_get_task_found(self, in_memory_backend, sample_task):
        """Test getting a task that exists."""
        in_memory_backend.save_task(sample_task)
        
        result = in_memory_backend.get_task(sample_task.id)
        assert result is not None
        assert result.id == sample_task.id
    
    def test_list_tasks_empty(self, in_memory_backend):
        """Test listing tasks when database is empty."""
        tasks = in_memory_backend.list_tasks()
        assert tasks == []
    
    def test_list_tasks_single(self, in_memory_backend, sample_task):
        """Test listing tasks with one task."""
        in_memory_backend.save_task(sample_task)
        
        tasks = in_memory_backend.list_tasks()
        assert len(tasks) == 1
        assert tasks[0].id == sample_task.id
    
    def test_list_tasks_multiple(self, in_memory_backend):
        """Test listing tasks with multiple tasks."""
        task1 = Task(name="task1", priority=2)
        task2 = Task(name="task2", priority=1)
        task3 = Task(name="task3", priority=3)
        
        in_memory_backend.save_task(task1)
        in_memory_backend.save_task(task2)
        in_memory_backend.save_task(task3)
        
        tasks = in_memory_backend.list_tasks()
        assert len(tasks) == 3
        
        # Verify all tasks are present
        task_ids = {t.id for t in tasks}
        assert task1.id in task_ids
        assert task2.id in task_ids
        assert task3.id in task_ids
    
    def test_update_task_status_success(self, in_memory_backend, sample_task):
        """Test updating task status successfully."""
        in_memory_backend.save_task(sample_task)
        
        result = in_memory_backend.update_task_status(sample_task.id, TaskStatus.RUNNING)
        assert result is True
        
        # Verify the status was updated
        task = in_memory_backend.get_task(sample_task.id)
        assert task.status == TaskStatus.RUNNING
    
    def test_update_task_status_not_found(self, in_memory_backend):
        """Test updating status of non-existent task."""
        result = in_memory_backend.update_task_status("non_existent", TaskStatus.RUNNING)
        assert result is False
    
    def test_update_task_status_multiple_updates(self, in_memory_backend, sample_task):
        """Test updating task status multiple times."""
        in_memory_backend.save_task(sample_task)
        
        # Update through various statuses
        statuses = [
            TaskStatus.RUNNING,
            TaskStatus.FAILED,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
        ]
        
        for status in statuses:
            result = in_memory_backend.update_task_status(sample_task.id, status)
            assert result is True
            
            task = in_memory_backend.get_task(sample_task.id)
            assert task.status == status
    
    def test_task_persistence_across_sessions(self, file_backend, sample_task):
        """Test that tasks persist across backend sessions."""
        # Save a task in the first session
        file_backend.save_task(sample_task)
        file_backend.close()
        
        # Create a new backend with the same file
        new_backend = SQLiteBackend(file_backend.db_path)
        
        # Verify the task is still there
        retrieved = new_backend.get_task(sample_task.id)
        assert retrieved is not None
        assert retrieved.id == sample_task.id
        assert retrieved.name == sample_task.name
 
        # close file backend after retrieval
        new_backend.close()
    
    def test_payload_serialization_dict(self, in_memory_backend):
        """Test serializing dictionary payload."""
        task = Task(name="dict_payload", payload={"key": "value", "number": 42})
        in_memory_backend.save_task(task)
        
        retrieved = in_memory_backend.get_task(task.id)
        assert retrieved.payload == {"key": "value", "number": 42}
    
    def test_payload_serialization_list(self, in_memory_backend):
        """Test serializing list payload."""
        task = Task(name="list_payload", payload=[1, 2, 3, "four"])
        in_memory_backend.save_task(task)
        
        retrieved = in_memory_backend.get_task(task.id)
        assert retrieved.payload == [1, 2, 3, "four"]
    
    def test_payload_serialization_string(self, in_memory_backend):
        """Test serializing string payload."""
        task = Task(name="string_payload", payload="simple string")
        in_memory_backend.save_task(task)
        
        retrieved = in_memory_backend.get_task(task.id)
        assert retrieved.payload == "simple string"
    
    def test_payload_serialization_none(self, in_memory_backend):
        """Test serializing None payload."""
        task = Task(name="none_payload", payload=None)
        in_memory_backend.save_task(task)
        
        retrieved = in_memory_backend.get_task(task.id)
        assert retrieved.payload is None
    
    def test_created_at_serialization(self, in_memory_backend):
        """Test serializing datetime."""
        now = datetime.utcnow()
        task = Task(name="timestamp_test", created_at=now)
        in_memory_backend.save_task(task)
        
        retrieved = in_memory_backend.get_task(task.id)
        # Check the time is approximately the same (within 1 second to account for any truncation)
        assert abs((retrieved.created_at - now).total_seconds()) < 1
    
    def test_retry_count_and_max_retries(self, in_memory_backend):
        """Test retry count and max_retries are persisted."""
        task = Task(name="retry_test", retry_count=2, max_retries=10)
        in_memory_backend.save_task(task)
        
        retrieved = in_memory_backend.get_task(task.id)
        assert retrieved.retry_count == 2
        assert retrieved.max_retries == 10
    
    def test_priority_ordering(self, in_memory_backend):
        """Test that tasks are listed in priority order."""
        task1 = Task(name="low_priority", priority=10)
        task2 = Task(name="high_priority", priority=1)
        task3 = Task(name="medium_priority", priority=5)
        
        in_memory_backend.save_task(task1)
        in_memory_backend.save_task(task2)
        in_memory_backend.save_task(task3)
        
        tasks = in_memory_backend.list_tasks()
        assert len(tasks) == 3
        
        # Tasks should be ordered by priority (lower first), then by created_at
        assert tasks[0].id == task2.id  # priority 1
        assert tasks[1].id == task3.id  # priority 5
        assert tasks[2].id == task1.id  # priority 10
    
    def test_context_manager(self):
        """Test using backend as context manager."""
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        
        try:
            with SQLiteBackend(db_path) as backend:
                task = Task(name="context_test")
                backend.save_task(task)
            
            # Backend should be closed now
            # Create a new backend to verify persistence
            backend2 = SQLiteBackend(db_path)
            retrieved = backend2.get_task(task.id)
            assert retrieved is not None
            backend2.close()
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)
    
    def test_thread_safety(self):
        """Test that backend is thread-safe for concurrent operations."""
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        
        try:
            backend = SQLiteBackend(db_path)
            
            errors = []
            task_ids = []
            
            def save_tasks(thread_id, count):
                """Save tasks from a thread."""
                try:
                    for i in range(count):
                        task = Task(name=f"thread_{thread_id}_task_{i}")
                        backend.save_task(task)
                        task_ids.append(task.id)
                except Exception as e:
                    errors.append(e)
            
            # Create multiple threads
            threads = []
            for i in range(10):
                t = threading.Thread(target=save_tasks, args=(i, 10))
                threads.append(t)
                t.start()
            
            # Wait for all threads to complete
            for t in threads:
                t.join()
            
            # Check for errors
            assert len(errors) == 0, f"Thread errors: {errors}"
            
            # Verify all tasks were saved
            tasks = backend.list_tasks()
            assert len(tasks) == 100  # 10 threads * 10 tasks each
            assert len(set(t.id for t in tasks)) == 100  # All unique IDs
            
            backend.close()
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)
    
    def test_database_schema_created(self, in_memory_backend):
        """Test that database schema is created correctly."""
        conn = in_memory_backend._get_connection()
        cursor = conn.cursor()
        
        # Check that tasks table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='tasks'
        """)
        result = cursor.fetchone()
        assert result is not None
        
        # Check the table structure
        cursor.execute("PRAGMA table_info(tasks)")
        columns = {row['name'] for row in cursor.fetchall()}
        
        expected_columns = {
            'id', 'name', 'payload', 'priority', 
            'status', 'created_at', 'retry_count', 'max_retries'
        }
        assert columns == expected_columns
    
    def test_database_indexes_created(self, in_memory_backend):
        """Test that database indexes are created correctly."""
        conn = in_memory_backend._get_connection()
        cursor = conn.cursor()
        
        # Check that indexes exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='index' AND tbl_name='tasks'
        """)
        indexes = {row['name'] for row in cursor.fetchall()}
        
        assert 'idx_tasks_status' in indexes
        assert 'idx_tasks_priority' in indexes