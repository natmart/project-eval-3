"""
Task Implementation

This module provides the Task class for defining unit of work in the queue.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
import uuid


class TaskStatus(Enum):
    """Enumeration for task status values."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Task:
    """
    Represents a unit of work in the task queue.
    
    Attributes:
        id: Unique identifier for the task
        name: Human-readable name for the task
        payload: The data to be processed by the task
        priority: Task priority (lower number = higher priority)
        status: Current status of the task
        created_at: Timestamp when the task was created
        retry_count: Number of times the task has been retried
        max_retries: Maximum number of retry attempts
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    payload: Optional[Any] = None
    priority: int = 0
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0
    max_retries: int = 3
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the Task instance to a dictionary.
        
        Returns:
            Dictionary representation of the task
        """
        return {
            "id": self.id,
            "name": self.name,
            "payload": self.payload,
            "priority": self.priority,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """
        Deserialize a dictionary to a Task instance.
        
        Args:
            data: Dictionary containing task data
            
        Returns:
            Task instance created from the dictionary data
        """
        # Parse created_at from ISO format string
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        
        # Parse status from string to enum
        status_value = data.get("status", "pending")
        if isinstance(status_value, str):
            try:
                status = TaskStatus(status_value)
            except ValueError:
                status = TaskStatus.PENDING
        else:
            status = TaskStatus.PENDING
        
        return cls(
            id=data.get("id", str(uuid.uuid4())),
            name=data.get("name", ""),
            payload=data.get("payload"),
            priority=data.get("priority", 0),
            status=status,
            created_at=created_at,
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
        )