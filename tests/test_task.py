"""
Tests for Task
"""

import pytest
from datetime import datetime
from pytaskq import Task, TaskStatus


class TestTask:
    """Test cases for Task class."""
    
    def test_task_creation_defaults(self):
        """Test creating a task with default values."""
        task = Task()
        
        assert task.id
        assert task.name == ""
        assert task.payload is None
        assert task.priority == 0
        assert task.status == TaskStatus.PENDING
        assert isinstance(task.created_at, datetime)
        assert task.retry_count == 0
        assert task.max_retries == 3
    
    def test_task_creation_with_values(self):
        """Test creating a task with specific values."""
        task_id = "test-id-123"
        task = Task(
            id=task_id,
            name="Test Task",
            payload={"data": "test"},
            priority=5,
            status=TaskStatus.RUNNING,
            retry_count=1,
            max_retries=5,
        )
        
        assert task.id == task_id
        assert task.name == "Test Task"
        assert task.payload == {"data": "test"}
        assert task.priority == 5
        assert task.status == TaskStatus.RUNNING
        assert task.retry_count == 1
        assert task.max_retries == 5
    
    def test_to_dict(self):
        """Test serializing a task to dictionary."""
        task = Task(
            name="Test Task",
            payload={"key": "value"},
            priority=2,
            status=TaskStatus.COMPLETED,
            retry_count=1,
        )
        
        task_dict = task.to_dict()
        
        assert isinstance(task_dict, dict)
        assert task_dict["id"] == task.id
        assert task_dict["name"] == "Test Task"
        assert task_dict["payload"] == {"key": "value"}
        assert task_dict["priority"] == 2
        assert task_dict["status"] == "completed"
        assert "created_at" in task_dict
        assert task_dict["retry_count"] == 1
        assert task_dict["max_retries"] == 3
    
    def test_from_dict(self):
        """Test deserializing a task from dictionary."""
        task_data = {
            "id": "test-id-456",
            "name": "Deserialized Task",
            "payload": {"data": 123},
            "priority": 3,
            "status": "running",
            "created_at": "2024-01-15T10:30:00",
            "retry_count": 2,
            "max_retries": 4,
        }
        
        task = Task.from_dict(task_data)
        
        assert task.id == "test-id-456"
        assert task.name == "Deserialized Task"
        assert task.payload == {"data": 123}
        assert task.priority == 3
        assert task.status == TaskStatus.RUNNING
        assert task.retry_count == 2
        assert task.max_retries == 4
        assert isinstance(task.created_at, datetime)
    
    def test_roundtrip_serialization(self):
        """Test that to_dict and from_dict are inverses."""
        original_task = Task(
            name="Roundtrip Test",
            payload={"test": True},
            priority=1,
            status=TaskStatus.FAILED,
            retry_count=2,
            max_retries=5,
        )
        
        task_dict = original_task.to_dict()
        restored_task = Task.from_dict(task_dict)
        
        assert restored_task.id == original_task.id
        assert restored_task.name == original_task.name
        assert restored_task.payload == original_task.payload
        assert restored_task.priority == original_task.priority
        assert restored_task.status == original_task.status
        assert restored_task.retry_count == original_task.retry_count
        assert restored_task.max_retries == original_task.max_retries
        # Note: created_at may differ slightly due to serialization
    
    def test_from_dict_with_missing_fields(self):
        """Test deserializing a task with missing fields uses defaults."""
        task_data = {"name": "Minimal Task"}
        
        task = Task.from_dict(task_data)
        
        assert task.id  # Should generate a UUID
        assert task.name == "Minimal Task"
        assert task.payload is None
        assert task.priority == 0
        assert task.status == TaskStatus.PENDING
        assert task.retry_count == 0
        assert task.max_retries == 3
    
    def test_from_dict_with_invalid_status(self):
        """Test deserializing with invalid status falls back to PENDING."""
        task_data = {
            "name": "Invalid Status Task",
            "status": "invalid_status_value",
        }
        
        task = Task.from_dict(task_data)
        
        assert task.status == TaskStatus.PENDING
    
    def test_status_enum_values(self):
        """Test that TaskStatus enum has correct values."""
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
    
    def test_task_immutability_of_id(self):
        """Test that task IDs are generated uniquely."""
        task1 = Task()
        task2 = Task()
        
        assert task1.id != task2.id
    
    def test_task_with_different_statuses(self):
        """Test tasks with different status values."""
        statuses = [
            TaskStatus.PENDING,
            TaskStatus.RUNNING,
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
        ]
        
        for status in statuses:
            task = Task(name=f"Task {status.value}", status=status)
            assert task.status == status
            
            # Test serialization
            task_dict = task.to_dict()
            assert task_dict["status"] == status.value
            
            # Test deserialization
            restored = Task.from_dict(task_dict)
            assert restored.status == status