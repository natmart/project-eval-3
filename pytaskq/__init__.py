"""
PyTaskQ - A Python Task Queue Library v2

A lightweight, thread-safe task queue implementation using heapq for priority-based
task scheduling and threading for concurrent execution.
"""

__version__ = "2.0.0"
__author__ = "PyTaskQ Team"

from .queue import TaskQueue
from .task import Task

__all__ = ["TaskQueue", "Task", "__version__"]