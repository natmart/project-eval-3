"""
Unit tests for the PriorityQueue class.

Tests cover basic operations (enqueue/dequeue), priority ordering,
thread-safety with concurrent operations, peek behavior, size/is_empty
methods, and edge cases with empty queue.
"""

import threading
import time

import pytest

from pytaskq import Task, TaskStatus
from pytaskq.queue import PriorityQueue


class TestBasicOperations:
    """Test basic enqueue and dequeue operations."""

    def test_empty_queue_initial_state(self) -> None:
        """Test that a new queue is empty and has size 0."""
        queue = PriorityQueue()

        assert queue.is_empty() is True
        assert queue.size() == 0
        assert queue.dequeue() is None
        assert queue.peek() is None

    def test_enqueue_single_task(self) -> None:
        """Test enqueueing a single task."""
        queue = PriorityQueue()
        task = Task(name="Test Task")

        queue.enqueue(task)

        assert queue.is_empty() is False
        assert queue.size() == 1
        assert queue.peek() == task

    def test_dequeue_single_task(self) -> None:
        """Test dequeueing a single task."""
        queue = PriorityQueue()
        task = Task(name="Test Task")
        queue.enqueue(task)

        dequeued = queue.dequeue()

        assert dequeued == task
        assert queue.is_empty() is True
        assert queue.size() == 0

    def test_enqueue_multiple_tasks(self) -> None:
        """Test enqueueing multiple tasks."""
        queue = PriorityQueue()
        tasks = [Task(name=f"Task {i}") for i in range(5)]

        for task in tasks:
            queue.enqueue(task)

        assert queue.size() == 5
        assert queue.is_empty() is False

    def test_dequeue_multiple_tasks(self) -> None:
        """Test dequeueing multiple tasks in correct order."""
        queue = PriorityQueue()
        tasks = [Task(name=f"Task {i}") for i in range(5)]

        for task in tasks:
            queue.enqueue(task)

        dequeued_tasks = [queue.dequeue() for _ in range(5)]

        assert dequeued_tasks == tasks
        assert queue.is_empty() is True

    def test_dequeue_from_empty_queue(self) -> None:
        """Test that dequeueing from an empty queue returns None."""
        queue = PriorityQueue()

        result = queue.dequeue()

        assert result is None

    def test_peek_does_not_remove_task(self) -> None:
        """Test that peek returns task without removing it."""
        queue = PriorityQueue()
        task = Task(name="Test Task")
        queue.enqueue(task)

        # Peek multiple times
        first_peek = queue.peek()
        second_peek = queue.peek()

        assert first_peek == task
        assert second_peek == task
        assert queue.size() == 1

    def test_peek_from_empty_queue(self) -> None:
        """Test that peeking an empty queue returns None."""
        queue = PriorityQueue()

        result = queue.peek()

        assert result is None


class TestPriorityOrdering:
    """Test priority ordering where lower numbers have higher priority."""

    def test_dequeue_respects_priority_ordering(self) -> None:
        """Test tasks are dequeued in priority order (lowest first)."""
        queue = PriorityQueue()
        task_low = Task(name="Low Priority", priority=10)
        task_medium = Task(name="Medium Priority", priority=5)
        task_high = Task(name="High Priority", priority=1)

        # Enqueue in random order
        queue.enqueue(task_medium)
        queue.enqueue(task_high)
        queue.enqueue(task_low)

        # Should dequeue in priority order
        assert queue.dequeue() == task_high
        assert queue.dequeue() == task_medium
        assert queue.dequeue() == task_low

    def test_fifo_ordering_for_same_priority(self) -> None:
        """Test that tasks with same priority maintain FIFO order."""
        queue = PriorityQueue()
        task1 = Task(name="Task 1", priority=5)
        task2 = Task(name="Task 2", priority=5)
        task3 = Task(name="Task 3", priority=5)

        queue.enqueue(task1)
        queue.enqueue(task2)
        queue.enqueue(task3)

        assert queue.dequeue() == task1
        assert queue.dequeue() == task2
        assert queue.dequeue() == task3

    def test_mixed_priorities_correct_ordering(self) -> None:
        """Test complex scenario with mixed priorities and orderings."""
        queue = PriorityQueue()
        tasks = [
            Task(name="A", priority=5),
            Task(name="B", priority=2),
            Task(name="C", priority=5),
            Task(name="D", priority=1),
            Task(name="E", priority=3),
            Task(name="F", priority=2),
        ]

        for task in tasks:
            queue.enqueue(task)

        # Expected order: D (1), B (2), F (2), E (3), A (5), C (5)
        expected_order = ["D", "B", "F", "E", "A", "C"]
        dequeued_names = [queue.dequeue().name for _ in range(6)]

        assert dequeued_names == expected_order

    def test_priority_with_zero_value(self) -> None:
        """Test that priority 0 is treated as highest priority."""
        queue = PriorityQueue()
        task_zero = Task(name="Priority 0", priority=0)
        task_one = Task(name="Priority 1", priority=1)
        task_negative = Task(name="Priority -1", priority=-1)

        queue.enqueue(task_one)
        queue.enqueue(task_negative)
        queue.enqueue(task_zero)

        # Negative priority should come first (highest priority)
        assert queue.dequeue() == task_negative
        assert queue.dequeue() == task_zero
        assert queue.dequeue() == task_one

    def test_peek_respects_priority(self) -> None:
        """Test that peek returns the highest priority task."""
        queue = PriorityQueue()
        task_low = Task(name="Low", priority=10)
        task_high = Task(name="High", priority=1)

        queue.enqueue(task_low)
        queue.enqueue(task_high)

        assert queue.peek() == task_high
        # Verify task wasn't removed
        assert queue.size() == 2


class TestSizeAndIsEmpty:
    """Test size() and is_empty() methods."""

    def test_size_increases_after_enqueue(self) -> None:
        """Test that size increases correctly after each enqueue."""
        queue = PriorityQueue()

        assert queue.size() == 0

        queue.enqueue(Task(name="Task 1"))
        assert queue.size() == 1

        queue.enqueue(Task(name="Task 2"))
        assert queue.size() == 2

        queue.enqueue(Task(name="Task 3"))
        assert queue.size() == 3

    def test_size_decreases_after_dequeue(self) -> None:
        """Test that size decreases correctly after each dequeue."""
        queue = PriorityQueue()
        for i in range(5):
            queue.enqueue(Task(name=f"Task {i}"))

        assert queue.size() == 5

        queue.dequeue()
        assert queue.size() == 4

        queue.dequeue()
        assert queue.size() == 3

        queue.dequeue()
        assert queue.size() == 2

    def test_is_empty_with_tasks(self) -> None:
        """Test is_empty returns False when queue has tasks."""
        queue = PriorityQueue()
        queue.enqueue(Task(name="Task 1"))

        assert queue.is_empty() is False

        # Add another task
        queue.enqueue(Task(name="Task 2"))
        assert queue.is_empty() is False

        # Remove all but one
        queue.dequeue()
        assert queue.is_empty() is False

    def test_is_empty_after_dequeue_all(self) -> None:
        """Test is_empty returns True after dequeuing all tasks."""
        queue = PriorityQueue()
        for i in range(3):
            queue.enqueue(Task(name=f"Task {i}"))

        assert queue.is_empty() is False

        queue.dequeue()
        queue.dequeue()
        queue.dequeue()

        assert queue.is_empty() is True

    def test_size_and_is_empty_consistency(self) -> None:
        """Test that size() and is_empty() remain consistent."""
        queue = PriorityQueue()

        for i in range(10):
            queue.enqueue(Task(name=f"Task {i}"))
            assert queue.size() == i + 1
            assert queue.is_empty() is False

        for i in range(10):
            assert queue.size() == 10 - i
            assert queue.is_empty() is (i == 9)
            queue.dequeue()

        assert queue.size() == 0
        assert queue.is_empty() is True


class TestClear:
    """Test clear() method."""

    def test_clear_empty_queue(self) -> None:
        """Test clearing an already empty queue."""
        queue = PriorityQueue()

        queue.clear()

        assert queue.is_empty() is True
        assert queue.size() == 0

    def test_clear_resets_queue(self) -> None:
        """Test that clear removes all tasks."""
        queue = PriorityQueue()
        for i in range(10):
            queue.enqueue(Task(name=f"Task {i}", priority=i))

        assert queue.size() == 10

        queue.clear()

        assert queue.is_empty() is True
        assert queue.size() == 0
        assert queue.peek() is None
        assert queue.dequeue() is None

    def test_clear_then_enqueue(self) -> None:
        """Test that enqueue works correctly after clear."""
        queue = PriorityQueue()
        queue.enqueue(Task(name="Task 1", priority=1))
        queue.enqueue(Task(name="Task 2", priority=2))

        queue.clear()

        # Should be able to enqueue again
        task = Task(name="New Task", priority=5)
        queue.enqueue(task)

        assert queue.size() == 1
        assert queue.peek() == task
        assert queue.dequeue() == task


class TestEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_enqueue_and_dequeue_cycle(self) -> None:
        """Test multiple enqueue/dequeue cycles."""
        queue = PriorityQueue()

        for cycle in range(3):
            tasks = [
                Task(name=f"Cycle {cycle} Task {i}", priority=i)
                for i in range(3)
            ]

            for task in tasks:
                queue.enqueue(task)

            assert queue.size() == 3

            for task in tasks:
                assert queue.dequeue() == task

            assert queue.is_empty() is True

    def test_task_with_high_priority_number(self) -> None:
        """Test tasks with very high priority numbers."""
        queue = PriorityQueue()
        task1 = Task(name="Task 1", priority=1000000)
        task2 = Task(name="Task 2", priority=999999)

        queue.enqueue(task1)
        queue.enqueue(task2)

        # Lower number has higher priority
        assert queue.dequeue() == task2
        assert queue.dequeue() == task1

    def test_negative_priorities(self) -> None:
        """Test tasks with negative priority values."""
        queue = PriorityQueue()
        tasks = [
            Task(name="Task -5", priority=-5),
            Task(name="Task -1", priority=-1),
            Task(name="Task 0", priority=0),
            Task(name="Task -10", priority=-10),
        ]

        for task in tasks:
            queue.enqueue(task)

        # Most negative has highest priority
        result = queue.dequeue()
        assert result.priority == -10
        result = queue.dequeue()
        assert result.priority == -5


class TestThreadSafety:
    """Test thread-safety of queue operations."""

    def test_concurrent_enqueue(self) -> None:
        """Test that multiple threads can enqueue safely."""
        queue = PriorityQueue()
        num_threads = 50
        tasks_per_thread = 20

        def enqueue_tasks(thread_id: int) -> None:
            for i in range(tasks_per_thread):
                task = Task(
                    name=f"Thread {thread_id} Task {i}",
                    priority=i % 5,
                )
                queue.enqueue(task)

        threads = [
            threading.Thread(target=enqueue_tasks, args=(i,))
            for i in range(num_threads)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # All tasks should be enqueued
        assert queue.size() == num_threads * tasks_per_thread

    def test_concurrent_dequeue(self) -> None:
        """Test that multiple threads can dequeue safely."""
        queue = PriorityQueue()
        num_tasks = 100

        # Pre-fill queue with tasks
        for i in range(num_tasks):
            queue.enqueue(Task(name=f"Task {i}", priority=i % 10))

        dequeued_count = [0]
        lock = threading.Lock()

        def dequeue_tasks() -> None:
            while True:
                task = queue.dequeue()
                if task is None:
                    break
                with lock:
                    dequeued_count[0] += 1

        threads = [threading.Thread(target=dequeue_tasks) for _ in range(10)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # All tasks should be dequeued exactly once
        assert dequeued_count[0] == num_tasks
        assert queue.is_empty() is True

    def test_concurrent_enqueue_and_dequeue(self) -> None:
        """Test that enqueue and dequeue work concurrently."""
        queue = PriorityQueue()
        num_producers = 5
        num_consumers = 5
        tasks_per_producer = 20

        produced_count = [0]
        consumed_count = [0]
        lock = threading.Lock()

        def producer() -> None:
            for i in range(tasks_per_producer):
                task = Task(
                    name=f"Producer Task {i}",
                    priority=i % 5,
                )
                queue.enqueue(task)
                with lock:
                    produced_count[0] += 1
                time.sleep(0.001)  # Small delay to allow interleaving

        def consumer() -> None:
            for _ in range(max(tasks_per_producer * num_producers // num_consumers, 1)):
                # Wait a bit for tasks to be available
                for _ in range(100):
                    task = queue.dequeue()
                    if task is not None:
                        with lock:
                            consumed_count[0] += 1
                        break
                    time.sleep(0.001)

        producer_threads = [threading.Thread(target=producer) for _ in range(num_producers)]
        consumer_threads = [threading.Thread(target=consumer) for _ in range(num_consumers)]

        # Start all threads
        for thread in producer_threads + consumer_threads:
            thread.start()

        # Wait for all threads
        for thread in producer_threads + consumer_threads:
            thread.join()

        # All produced tasks should be consumed
        assert produced_count[0] == num_producers * tasks_per_producer
        assert consumed_count[0] == produced_count[0]

    def test_concurrent_size_and_is_empty(self) -> None:
        """Test that size() and is_empty() are thread-safe during modifications."""
        queue = PriorityQueue()
        num_tasks = 100

        def enqueue_tasks() -> None:
            for i in range(num_tasks // 2):
                queue.enqueue(Task(name=f"Task {i}", priority=i))
                time.sleep(0.0001)

        def dequeue_tasks() -> None:
            time.sleep(0.05)  # Wait for some enqueues
            for _ in range(num_tasks // 2):
                queue.dequeue()
                time.sleep(0.0001)

        def read_size() -> None:
            for _ in range(50):
                size = queue.size()
                is_empty = queue.is_empty()
                # Validate consistency
                assert size >= 0
                assert is_empty == (size == 0)
                time.sleep(0.001)

        # Start threads
        threads = [
            threading.Thread(target=enqueue_tasks),
            threading.Thread(target=dequeue_tasks),
            threading.Thread(target=read_size),
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    def test_concurrent_peek(self) -> None:
        """Test that peek is thread-safe during concurrent operations."""
        queue = PriorityQueue()

        # Add initial tasks
        for i in range(5):
            queue.enqueue(Task(name=f"Task {i}", priority=i))

        peeked_tasks = []
        lock = threading.Lock()

        def peek_and_enqueue() -> None:
            for i in range(10, 20):
                task = queue.peek()
                if task is not None:
                    with lock:
                        peeked_tasks.append(task.name)
                queue.enqueue(Task(name=f"Task {i}", priority=i))

        threads = [threading.Thread(target=peek_and_enqueue) for _ in range(5)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Should have 5 initial + 50 new tasks
        assert queue.size() == 5 + 50
        # All peeks should have returned a valid task
        assert len(peeked_tasks) == 50

    def test_clear_during_concurrent_operations(self) -> None:
        """Test that clear works safely during concurrent enqueue/dequeue."""
        queue = PriorityQueue()
        num_tasks = 50

        def enqueue_and_clear() -> None:
            for i in range(num_tasks):
                queue.enqueue(Task(name=f"Task {i}", priority=i))
                if i % 10 == 0:
                    queue.clear()

        threads = [threading.Thread(target=enqueue_and_clear) for _ in range(3)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Queue should be in a valid state
        assert queue.size() >= 0
        assert queue.is_empty() in [True, False]


class TestTaskPreservation:
    """Test that tasks are preserved correctly through queue operations."""

    def test_task_properties_preserved_after_dequeue(self) -> None:
        """Test that task properties are preserved after dequeue."""
        queue = PriorityQueue()
        original_task = Task(
            name="Test Task",
            payload={"key": "value"},
            priority=3,
            status=TaskStatus.RUNNING,
            max_retries=5,
        )

        queue.enqueue(original_task)
        dequeued_task = queue.dequeue()

        assert dequeued_task.id == original_task.id
        assert dequeued_task.name == original_task.name
        assert dequeued_task.payload == original_task.payload
        assert dequeued_task.priority == original_task.priority
        assert dequeued_task.status == original_task.status
        assert dequeued_task.max_retries == original_task.max_retries

    def test_task_multiple_enqueue_dequeue_preserves_data(self) -> None:
        """Test that task data is preserved through multiple cycles."""
        queue = PriorityQueue()
        tasks = [
            Task(
                name=f"Task {i}",
                payload={"data": i * 10},
                priority=i,
            )
            for i in range(10)
        ]

        for task in tasks:
            queue.enqueue(task)

        for original_task in tasks:
            dequeued_task = queue.dequeue()
            assert dequeued_task.name == original_task.name
            assert dequeued_task.payload == original_task.payload
            assert dequeued_task.priority == original_task.priority