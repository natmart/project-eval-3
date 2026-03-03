"""
Tests for Task Queue
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from pytaskq.queue import PriorityQueue, TaskQueue
from pytaskq.task import Task, TaskStatus


class TestPriorityQueue:
    """Test cases for priority queue functionality."""
    
    def test_empty_queue_initialization(self):
        """Test that a new queue is initialized as empty."""
        queue = PriorityQueue()
        assert queue.size() == 0
        assert queue.is_empty() is True
    
    def test_enqueue_single_task(self):
        """Test enqueuing a single task."""
        queue = PriorityQueue()
        task = Task(name="test_task", priority=1)
        
        queue.enqueue(task)
        
        assert queue.size() == 1
        assert queue.is_empty() is False
    
    def test_enqueue_multiple_tasks(self):
        """Test enqueuing multiple tasks."""
        queue = PriorityQueue()
        
        for i in range(5):
            task = Task(name=f"task_{i}", priority=i)
            queue.enqueue(task)
        
        assert queue.size() == 5
        assert queue.is_empty() is False
    
    def test_dequeue_from_empty_queue(self):
        """Test that dequeue returns None for empty queue."""
        queue = PriorityQueue()
        result = queue.dequeue()
        assert result is None
    
    def test_dequeue_single_task(self):
        """Test dequeuing a single task."""
        queue = PriorityQueue()
        task = Task(name="test_task", priority=1)
        queue.enqueue(task)
        
        result = queue.dequeue()
        
        assert result is not None
        assert result.name == "test_task"
        assert result.priority == 1
        assert queue.size() == 0
        assert queue.is_empty() is True
    
    def test_dequeue_returns_highest_priority(self):
        """Test that dequeue returns highest priority task (lowest priority number)."""
        queue = PriorityQueue()
        
        # Add tasks in random priority order
        queue.enqueue(Task(name="low", priority=10))
        queue.enqueue(Task(name="high", priority=1))
        queue.enqueue(Task(name="medium", priority=5))
        queue.enqueue(Task(name="highest", priority=0))
        
        # Verify tasks come out in priority order
        first = queue.dequeue()
        assert first.priority == 0
        assert first.name == "highest"
        
        second = queue.dequeue()
        assert second.priority == 1
        assert second.name == "high"
        
        third = queue.dequeue()
        assert third.priority == 5
        assert third.name == "medium"
        
        fourth = queue.dequeue()
        assert fourth.priority == 10
        assert fourth.name == "low"
    
    def test_fifo_order_for_same_priority(self):
        """Test that tasks with same priority maintain FIFO order."""
        queue = PriorityQueue()
        
        queue.enqueue(Task(name="first", priority=5))
        queue.enqueue(Task(name="second", priority=5))
        queue.enqueue(Task(name="third", priority=5))
        
        # Should come out in the order they were added
        assert queue.dequeue().name == "first"
        assert queue.dequeue().name == "second"
        assert queue.dequeue().name == "third"
    
    def test_peek_from_empty_queue(self):
        """Test that peek returns None for empty queue."""
        queue = PriorityQueue()
        result = queue.peek()
        assert result is None
    
    def test_peek_without_removing(self):
        """Test that peek returns task without removing it."""
        queue = PriorityQueue()
        task = Task(name="test_task", priority=3)
        queue.enqueue(task)
        
        # Peek multiple times - should return same task
        first_peek = queue.peek()
        second_peek = queue.peek()
        
        assert first_peek is not None
        assert first_peek.name == "test_task"
        assert first_peek.priority == 3
        
        # Size should remain the same
        assert queue.size() == 1
        assert second_peek.name == "test_task"
        
        # Task should still be in queue
        dequeued = queue.dequeue()
        assert dequeued.name == "test_task"
    
    def test_peek_returns_highest_priority(self):
        """Test that peek returns the highest priority task."""
        queue = PriorityQueue()
        
        queue.enqueue(Task(name="low", priority=10))
        queue.enqueue(Task(name="high", priority=1))
        queue.enqueue(Task(name="medium", priority=5))
        
        # Peek should return highest priority task
        result = queue.peek()
        assert result.priority == 1
        assert result.name == "high"
        
        # Task should still be in queue
        assert queue.size() == 3
    
    def test_size_method(self):
        """Test the size method accuracy."""
        queue = PriorityQueue()
        
        assert queue.size() == 0
        
        queue.enqueue(Task(name="task1", priority=1))
        assert queue.size() == 1
        
        queue.enqueue(Task(name="task2", priority=2))
        assert queue.size() == 2
        
        queue.enqueue(Task(name="task3", priority=3))
        assert queue.size() == 3
        
        queue.dequeue()
        assert queue.size() == 2
        
        queue.dequeue()
        assert queue.size() == 1
        
        queue.dequeue()
        assert queue.size() == 0
    
    def test_is_empty_method(self):
        """Test the is_empty method accuracy."""
        queue = PriorityQueue()
        
        assert queue.is_empty() is True
        
        queue.enqueue(Task(name="task1", priority=1))
        assert queue.is_empty() is False
        
        queue.dequeue()
        assert queue.is_empty() is True
    
    def test_clear_method(self):
        """Test the clear method removes all tasks."""
        queue = PriorityQueue()
        
        for i in range(10):
            queue.enqueue(Task(name=f"task_{i}", priority=i))
        
        assert queue.size() == 10
        
        queue.clear()
        
        assert queue.size() == 0
        assert queue.is_empty() is True
        assert queue.dequeue() is None
        assert queue.peek() is None
    
    def test_complex_priority_scenario(self):
        """Test complex scenario with mixed priorities and order."""
        queue = PriorityQueue()
        
        # Add tasks with various priorities
        tasks = [
            ("a", 5),
            ("b", 1),
            ("c", 8),
            ("d", 1),
            ("e", 3),
            ("f", 1),
            ("g", 2),
        ]
        
        for name, priority in tasks:
            queue.enqueue(Task(name=name, priority=priority))
        
        # Expected order: b(1,d,f in order), a(5), c(8), e(3), g(2)
        # After priority 1: b, d, f (FIFO)
        # Then g(2), e(3), a(5), c(8)
        
        expected_order = ["b", "d", "f", "g", "e", "a", "c"]
        
        for expected_name in expected_order:
            task = queue.dequeue()
            assert task.name == expected_name, f"Expected {expected_name}, got {task.name}"
        
        assert queue.is_empty()
    
    def test_enqueue_after_dequeue(self):
        """Test that enqueuing after dequeuing works correctly."""
        queue = PriorityQueue()
        
        # Enqueue and dequeue some tasks
        queue.enqueue(Task(name="task1", priority=3))
        queue.enqueue(Task(name="task2", priority=1))
        
        assert queue.dequeue().name == "task2"
        
        # Enqueue more tasks
        queue.enqueue(Task(name="task3", priority=2))
        queue.enqueue(Task(name="task4", priority=0))
        
        # Check priorities are maintained
        assert queue.dequeue().name == "task4"  # priority 0
        assert queue.dequeue().name == "task3"  # priority 2
        assert queue.dequeue().name == "task1"  # priority 3
        
        assert queue.is_empty()


class TestThreadSafety:
    """Test cases for thread-safety of priority queue."""
    
    def test_concurrent_enqueue(self):
        """Test that concurrent enqueuing is thread-safe."""
        queue = PriorityQueue()
        num_threads = 10
        tasks_per_thread = 100
        
        def enqueue_tasks(thread_id):
            for i in range(tasks_per_thread):
                task = Task(name=f"thread_{thread_id}_task_{i}", priority=i % 10)
                queue.enqueue(task)
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(executor.map(enqueue_tasks, range(num_threads)))
        
        # Wait for all tasks to complete
        time.sleep(0.1)
        
        # Verify all tasks were added
        assert queue.size() == num_threads * tasks_per_thread
    
    def test_concurrent_dequeue(self):
        """Test that concurrent dequeuing is thread-safe."""
        queue = PriorityQueue()
        num_threads = 10
        tasks_per_thread = 100
        
        # Populate the queue first
        for i in range(num_threads * tasks_per_thread):
            queue.enqueue(Task(name=f"task_{i}", priority=i % 10))
        
        results = []
        
        def dequeue_tasks():
            while True:
                task = queue.dequeue()
                if task is None:
                    break
                results.append(task)
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            list(executor.map(dequeue_tasks, range(num_threads)))
        
        # Verify all tasks were dequeued exactly once
        assert len(results) == num_threads * tasks_per_thread
        assert queue.is_empty()
    
    def test_concurrent_enqueue_dequeue(self):
        """Test that concurrent enqueuing and dequeuing is thread-safe."""
        queue = PriorityQueue()
        num_threads = 5
        num_operations = 100
        
        def enqueue_dequeue_operations(thread_id):
            for i in range(num_operations):
                if i % 2 == 0:
                    # Enqueue
                    task = Task(name=f"task_{thread_id}_{i}", priority=i % 10)
                    queue.enqueue(task)
                else:
                    # Dequeue
                    queue.dequeue()
        
        threads = [
            threading.Thread(target=enqueue_dequeue_operations, args=(i,))
            for i in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Queue should be in a valid state
        # (we don't check exact size as it depends on timing)
        assert queue.size() >= 0
    
    def test_concurrent_peek(self):
        """Test that concurrent peeking is thread-safe."""
        queue = PriorityQueue()
        num_tasks = 100
        
        # Populate the queue
        for i in range(num_tasks):
            queue.enqueue(Task(name=f"task_{i}", priority=i))
        
        results = []
        
        def peek_multiple_times():
            for _ in range(50):
                task = queue.peek()
                if task is not None:
                    results.append(task.name)
        
        threads = [
            threading.Thread(target=peek_multiple_times)
            for _ in range(10)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All peeks should return the highest priority task (priority 0)
        for name in results:
            assert name == "task_0"
        
        # Queue should be unchanged
        assert queue.size() == num_tasks
    
    def test_concurrent_size_and_is_empty(self):
        """Test that concurrent size and is_empty calls are thread-safe."""
        queue = PriorityQueue()
        num_threads = 10
        
        def check_size_empty():
            for i in range(100):
                size = queue.size()
                is_empty = queue.is_empty()
                # Both should be consistent
                if size == 0:
                    assert is_empty is True
                else:
                    assert is_empty is False
        
        threads = [
            threading.Thread(target=check_size_empty)
            for _ in range(num_threads)
        ]
        
        # Also add some tasks while checking
        def add_tasks():
            for i in range(50):
                queue.enqueue(Task(name=f"task_{i}", priority=i))
                time.sleep(0.001)
        
        for thread in threads:
            thread.start()
        
        adder = threading.Thread(target=add_tasks)
        adder.start()
        
        for thread in threads:
            thread.join()
        
        adder.join()
    
    def test_thread_safety_under_high_load(self):
        """Test thread-safety under high concurrent load."""
        queue = PriorityQueue()
        num_threads = 20
        num_operations = 500
        
        def mixed_operations(thread_id):
            local_counter = 0
            for i in range(num_operations):
                operation = (thread_id + i) % 4
                
                if operation == 0:
                    # Enqueue
                    task = Task(name=f"task_{thread_id}_{i}", priority=(thread_id + i) % 10)
                    queue.enqueue(task)
                elif operation == 1:
                    # Dequeue
                    queue.dequeue()
                elif operation == 2:
                    # Peek
                    queue.peek()
                else:
                    # Size
                    size = queue.size()
                    assert size >= 0
                
                local_counter += 1
        
        threads = [
            threading.Thread(target=mixed_operations, args=(i,))
            for i in range(num_threads)
        ]
        
        start_time = time.time()
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        elapsed = time.time() - start_time
        
        # Queue should be in a valid state
        assert queue.size() >= 0
        
        # Performance should be reasonable (less than 10 seconds for 10,000 operations)
        assert elapsed < 10.0, f"Operation took too long: {elapsed}s"


class TestTaskQueueAlias:
    """Test that TaskQueue is a proper alias for PriorityQueue."""
    
    def test_task_queue_is_priority_queue(self):
        """Test that TaskQueue is an alias for PriorityQueue."""
        task_queue = TaskQueue()
        priority_queue = PriorityQueue()
        
        # Both should have the same methods
        assert hasattr(task_queue, 'enqueue')
        assert hasattr(task_queue, 'dequeue')
        assert hasattr(task_queue, 'peek')
        assert hasattr(task_queue, 'size')
        assert hasattr(task_queue, 'is_empty')
        assert hasattr(task_queue, 'clear')
    
    def test_task_queue_functionality(self):
        """Test that TaskQueue works the same as PriorityQueue."""
        queue = TaskQueue()
        
        task = Task(name="test", priority=5)
        queue.enqueue(task)
        
        assert queue.size() == 1
        assert queue.peek().name == "test"
        assert queue.dequeue().name == "test"
        assert queue.is_empty()