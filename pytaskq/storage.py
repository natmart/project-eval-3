"""
Storage Backend Implementation

This module provides abstract base class and SQLite implementation for persisting tasks.
"""

import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .task import Task, TaskStatus


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    
    Defines the interface that all storage backends must implement.
    """
    
    @abstractmethod
    def save_task(self, task: Task) -> None:
        """
        Save a task to storage.
        
        Args:
            task: The Task instance to save
        """
        pass
    
    @abstractmethod
    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Retrieve a task by its ID.
        
        Args:
            task_id: The unique identifier of the task
            
        Returns:
            The Task instance if found, None otherwise
        """
        pass
    
    @abstractmethod
    def list_tasks(self) -> List[Task]:
        """
        List all tasks in storage.
        
        Returns:
            List of all Task instances
        """
        pass
    
    @abstractmethod
    def update_task_status(self, task_id: str, status: TaskStatus) -> bool:
        """
        Update the status of a task.
        
        Args:
            task_id: The unique identifier of the task
            status: The new status to set
            
        Returns:
            True if the task was found and updated, False otherwise
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close the storage backend and release resources.
        """
        pass


class SQLiteBackend(StorageBackend):
    """
    SQLite implementation of the storage backend.
    
    Provides persistent storage of tasks using SQLite database.
    Thread-safe for concurrent access.
    """
    
    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize the SQLite backend.
        
        Args:
            db_path: Path to the SQLite database file. Use ":memory:" for in-memory database
        """
        self.db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        
        # Initialize database schema
        self._initialize_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get a thread-local SQLite connection.
        
        Returns:
            SQLite connection for the current thread
        """
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            # Enable foreign keys
            self._local.connection.execute("PRAGMA foreign_keys = ON")
            # Set row factory to dictionary-like rows
            self._local.connection.row_factory = sqlite3.Row
        
        return self._local.connection
    
    def _initialize_db(self) -> None:
        """
        Initialize the database schema.
        
        Creates the tasks table if it doesn't exist.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                payload TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3
            )
        """)
        
        # Create index on status for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_status 
            ON tasks(status)
        """)
        
        # Create index on priority for priority-based queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_priority 
            ON tasks(priority)
        """)
        
        conn.commit()
    
    def _serialize_task(self, task: Task) -> tuple:
        """
        Serialize a Task instance to a tuple for database storage.
        
        Args:
            task: The Task instance to serialize
            
        Returns:
            Tuple of values suitable for database insertion
        """
        return (
            task.id,
            task.name,
            json.dumps(task.payload) if task.payload is not None else None,
            task.priority,
            task.status.value,
            task.created_at.isoformat(),
            task.retry_count,
            task.max_retries,
        )
    
    def _deserialize_task(self, row: sqlite3.Row) -> Task:
        """
        Deserialize a database row to a Task instance.
        
        Args:
            row: SQLite row containing task data
            
        Returns:
            Task instance
        """
        # Parse payload from JSON
        payload = None
        if row['payload'] is not None:
            try:
                payload = json.loads(row['payload'])
            except json.JSONDecodeError:
                payload = row['payload']
        
        # Parse created_at from ISO format
        created_at = None
        if row['created_at'] is not None:
            try:
                created_at = datetime.fromisoformat(row['created_at'])
            except (ValueError, TypeError):
                created_at = datetime.utcnow()
        
        # Parse status from string to enum
        status = TaskStatus.PENDING
        try:
            status = TaskStatus(row['status'])
        except (ValueError, TypeError):
            status = TaskStatus.PENDING
        
        return Task(
            id=row['id'],
            name=row['name'],
            payload=payload,
            priority=row['priority'],
            status=status,
            created_at=created_at,
            retry_count=row['retry_count'],
            max_retries=row['max_retries'],
        )
    
    def save_task(self, task: Task) -> None:
        """
        Save a task to the database.
        
        If a task with the same ID already exists, it will be replaced.
        
        Args:
            task: The Task instance to save
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Use INSERT OR REPLACE to handle existing tasks
        cursor.execute("""
            INSERT OR REPLACE INTO tasks 
            (id, name, payload, priority, status, created_at, retry_count, max_retries)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, self._serialize_task(task))
        
        conn.commit()
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """
        Retrieve a task by its ID.
        
        Args:
            task_id: The unique identifier of the task
            
        Returns:
            The Task instance if found, None otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, name, payload, priority, status, created_at, retry_count, max_retries
            FROM tasks
            WHERE id = ?
        """, (task_id,))
        
        row = cursor.fetchone()
        
        if row is None:
            return None
        
        return self._deserialize_task(row)
    
    def list_tasks(self) -> List[Task]:
        """
        List all tasks in the database.
        
        Returns:
            List of all Task instances
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, name, payload, priority, status, created_at, retry_count, max_retries
            FROM tasks
            ORDER BY priority ASC, created_at ASC
        """)
        
        rows = cursor.fetchall()
        return [self._deserialize_task(row) for row in rows]
    
    def update_task_status(self, task_id: str, status: TaskStatus) -> bool:
        """
        Update the status of a task.
        
        Args:
            task_id: The unique identifier of the task
            status: The new status to set
            
        Returns:
            True if the task was found and updated, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE tasks
            SET status = ?
            WHERE id = ?
        """, (status.value, task_id))
        
        conn.commit()
        
        # Return True if any row was modified
        return cursor.rowcount > 0
    
    def close(self) -> None:
        """
        Close the database connection and release resources.
        """
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            delattr(self._local, 'connection')
    
    def __enter__(self):
        """
        Context manager entry.
        
        Returns:
            Self
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit.
        
        Args:
            exc_type: Exception type if an exception was raised
            exc_val: Exception value if an exception was raised
            exc_tb: Exception traceback if an exception was raised
        """
        self.close()