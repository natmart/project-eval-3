"""
Tests for MetricsCollector
"""

import threading
import time
import unittest

from pytaskq.metrics import MetricsCollector, MetricsSnapshot


class TestMetricsCollector(unittest.TestCase):
    """Test cases for MetricsCollector class."""
    
    def test_initial_state(self) -> None:
        """Test that MetricsCollector starts with all zeros."""
        collector = MetricsCollector()
        self.assertEqual(collector.tasks_submitted, 0)
        self.assertEqual(collector.tasks_completed, 0)
        self.assertEqual(collector.tasks_failed, 0)
        self.assertEqual(collector.average_duration, 0.0)
        self.assertEqual(collector.total_tasks_processed, 0)
    
    def test_increment_submitted(self) -> None:
        """Test incrementing tasks submitted."""
        collector = MetricsCollector()
        collector.increment_submitted()
        self.assertEqual(collector.tasks_submitted, 1)
        
        collector.increment_submitted(5)
        self.assertEqual(collector.tasks_submitted, 6)
    
    def test_increment_submitted_zero_or_negative(self) -> None:
        """Test that incrementing with zero or negative values doesn't change counter."""
        collector = MetricsCollector()
        collector.increment_submitted(5)
        self.assertEqual(collector.tasks_submitted, 5)
        
        collector.increment_submitted(0)
        self.assertEqual(collector.tasks_submitted, 5)
        
        collector.increment_submitted(-3)
        self.assertEqual(collector.tasks_submitted, 5)
    
    def test_increment_completed(self) -> None:
        """Test incrementing tasks completed."""
        collector = MetricsCollector()
        collector.increment_completed()
        self.assertEqual(collector.tasks_completed, 1)
        
        collector.increment_completed(1.5)
        self.assertEqual(collector.tasks_completed, 2)
    
    def test_increment_completed_with_duration(self) -> None:
        """Test that duration contributes to average calculation."""
        collector = MetricsCollector()
        collector.increment_completed(1.0)
        collector.increment_completed(2.0)
        collector.increment_completed(3.0)
        
        self.assertEqual(collector.tasks_completed, 3)
        self.assertAlmostEqual(collector.average_duration, 2.0)
    
    def test_increment_completed_without_duration(self) -> None:
        """Test incrementing without duration doesn't affect average."""
        collector = MetricsCollector()
        collector.increment_completed()
        self.assertEqual(collector.tasks_completed, 1)
        self.assertEqual(collector.average_duration, 0.0)
    
    def test_increment_completed_negative_duration(self) -> None:
        """Test that negative durations are ignored."""
        collector = MetricsCollector()
        collector.increment_completed(-1.0)
        self.assertEqual(collector.tasks_completed, 1)
        self.assertEqual(collector.average_duration, 0.0)
    
    def test_increment_failed(self) -> None:
        """Test incrementing tasks failed."""
        collector = MetricsCollector()
        collector.increment_failed()
        self.assertEqual(collector.tasks_failed, 1)
        
        collector.increment_failed(0.5)
        self.assertEqual(collector.tasks_failed, 2)
    
    def test_increment_failed_with_duration(self) -> None:
        """Test that failed task durations contribute to average calculation."""
        collector = MetricsCollector()
        collector.increment_failed(1.0)
        collector.increment_failed(2.0)
        
        self.assertEqual(collector.tasks_failed, 2)
        self.assertAlmostEqual(collector.average_duration, 1.5)
    
    def test_increment_failed_without_duration(self) -> None:
        """Test incrementing failed without duration doesn't affect average."""
        collector = MetricsCollector()
        collector.increment_failed()
        self.assertEqual(collector.tasks_failed, 1)
        self.assertEqual(collector.average_duration, 0.0)
    
    def test_mixed_completed_and_failed(self) -> None:
        """Test average calculated from both completed and failed tasks."""
        collector = MetricsCollector()
        collector.increment_completed(2.0)
        collector.increment_completed(4.0)
        collector.increment_failed(6.0)
        
        self.assertEqual(collector.tasks_completed, 2)
        self.assertEqual(collector.tasks_failed, 1)
        # Average: (2.0 + 4.0 + 6.0) / 3 = 4.0
        self.assertAlmostEqual(collector.average_duration, 4.0)
    
    def test_total_tasks_processed(self) -> None:
        """Test total tasks processed property."""
        collector = MetricsCollector()
        self.assertEqual(collector.total_tasks_processed, 0)
        
        collector.increment_completed()
        self.assertEqual(collector.total_tasks_processed, 1)
        
        collector.increment_completed()
        collector.increment_failed()
        self.assertEqual(collector.total_tasks_processed, 3)
    
    def test_reset(self) -> None:
        """Test resetting all metrics."""
        collector = MetricsCollector()
        collector.increment_submitted(10)
        collector.increment_completed(1.0)
        collector.increment_completed(2.0)
        collector.increment_failed(0.5)
        
        self.assertEqual(collector.tasks_submitted, 10)
        self.assertEqual(collector.tasks_completed, 2)
        self.assertEqual(collector.tasks_failed, 1)
        self.assertAlmostEqual(collector.average_duration, 1.1666667)
        
        collector.reset()
        
        self.assertEqual(collector.tasks_submitted, 0)
        self.assertEqual(collector.tasks_completed, 0)
        self.assertEqual(collector.tasks_failed, 0)
        self.assertEqual(collector.average_duration, 0.0)
    
    def test_snapshot(self) -> None:
        """Test creating a snapshot of current metrics."""
        collector = MetricsCollector()
        collector.increment_submitted(5)
        collector.increment_completed(1.0)
        collector.increment_completed(2.0)
        collector.increment_failed(3.0)
        
        snapshot = collector.snapshot()
        
        self.assertIsInstance(snapshot, MetricsSnapshot)
        self.assertEqual(snapshot.tasks_submitted, 5)
        self.assertEqual(snapshot.tasks_completed, 2)
        self.assertEqual(snapshot.tasks_failed, 1)
        self.assertAlmostEqual(snapshot.average_duration, 2.0)
    
    def test_snapshot_total_tasks(self) -> None:
        """Test snapshot total_tasks property."""
        collector = MetricsCollector()
        collector.increment_completed()
        collector.increment_completed()
        collector.increment_failed()
        
        snapshot = collector.snapshot()
        self.assertEqual(snapshot.total_tasks, 3)
    
    def test_snapshot_immutability(self) -> None:
        """Test that snapshot values don't change after collector is modified."""
        collector = MetricsCollector()
        collector.increment_submitted(5)
        collector.increment_completed(1.0)
        
        snapshot = collector.snapshot()
        self.assertEqual(snapshot.tasks_submitted, 5)
        self.assertEqual(snapshot.tasks_completed, 1)
        
        # Modify collector
        collector.increment_submitted(10)
        collector.increment_completed(2.0)
        
        # Snapshot should have original values
        self.assertEqual(snapshot.tasks_submitted, 5)
        self.assertEqual(snapshot.tasks_completed, 1)
        
        # New snapshot has updated values
        new_snapshot = collector.snapshot()
        self.assertEqual(new_snapshot.tasks_submitted, 15)
        self.assertEqual(new_snapshot.tasks_completed, 2)
    
    def test_repr(self) -> None:
        """Test string representation."""
        collector = MetricsCollector()
        collector.increment_submitted(10)
        collector.increment_completed(5.0)
        collector.increment_failed(3.0)
        
        repr_str = repr(collector)
        self.assertIn("MetricsCollector", repr_str)
        self.assertIn("submitted=10", repr_str)
        self.assertIn("completed=1", repr_str)
        self.assertIn("failed=1", repr_str)
    
    def test_thread_safety_counters(self) -> None:
        """Test that counters are thread-safe under concurrent access."""
        collector = MetricsCollector()
        num_threads = 10
        increments_per_thread = 100
        
        def increment_submitted() -> None:
            for _ in range(increments_per_thread):
                collector.increment_submitted()
        
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=increment_submitted)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have num_threads * increments_per_thread
        self.assertEqual(collector.tasks_submitted, num_threads * increments_per_thread)
    
    def test_thread_safety_completed_with_duration(self) -> None:
        """Test thread-safety when incrementing completed with durations."""
        collector = MetricsCollector()
        num_threads = 10
        increments_per_thread = 50
        duration_per_task = 1.0
        
        def increment_completed() -> None:
            for _ in range(increments_per_thread):
                collector.increment_completed(duration_per_task)
        
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=increment_completed)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        expected_count = num_threads * increments_per_thread
        self.assertEqual(collector.tasks_completed, expected_count)
        self.assertAlmostEqual(collector.average_duration, duration_per_task)
    
    def test_thread_safety_failed_with_duration(self) -> None:
        """Test thread-safety when incrementing failed with durations."""
        collector = MetricsCollector()
        num_threads = 10
        increments_per_thread = 50
        duration_per_task = 0.5
        
        def increment_failed() -> None:
            for _ in range(increments_per_thread):
                collector.increment_failed(duration_per_task)
        
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=increment_failed)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        expected_count = num_threads * increments_per_thread
        self.assertEqual(collector.tasks_failed, expected_count)
        self.assertAlmostEqual(collector.average_duration, duration_per_task)
    
    def test_thread_safety_mixed_operations(self) -> None:
        """Test thread-safety with mixed operations on all counters."""
        collector = MetricsCollector()
        num_threads = 5
        operations_per_thread = 20
        
        def mixed_operations() -> None:
            for i in range(operations_per_thread):
                collector.increment_submitted()
                if i % 2 == 0:
                    collector.increment_completed(1.0)
                else:
                    collector.increment_failed(1.0)
        
        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=mixed_operations)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        expected_submitted = num_threads * operations_per_thread
        expected_processed = num_threads * operations_per_thread  # All tasks either completed or failed
        
        self.assertEqual(collector.tasks_submitted, expected_submitted)
        self.assertEqual(collector.total_tasks_processed, expected_processed)
        self.assertAlmostEqual(collector.average_duration, 1.0)
    
    def test_thread_safety_snapshot(self) -> None:
        """Test that snapshot is safe under concurrent modifications."""
        collector = MetricsCollector()
        snapshots = []
        
        def collect_snapshots() -> None:
            for _ in range(50):
                snapshots.append(collector.snapshot())
        
        def modify_metrics() -> None:
            for i in range(100):
                collector.increment_submitted()
                collector.increment_completed(1.0)
                time.sleep(0.001)
        
        threads = [
            threading.Thread(target=collect_snapshots),
            threading.Thread(target=modify_metrics),
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All snapshots should be valid
        for snapshot in snapshots:
            self.assertIsInstance(snapshot, MetricsSnapshot)
            self.assertGreaterEqual(snapshot.tasks_submitted, 0)
            self.assertGreaterEqual(snapshot.tasks_completed, 0)
            self.assertGreaterEqual(snapshot.average_duration, 0.0)
    
    def test_average_duration_zero_division(self) -> None:
        """Test that average_duration handles zero count gracefully."""
        collector = MetricsCollector()
        self.assertEqual(collector.average_duration, 0.0)
        
        # Add durations through completed tasks
        collector.increment_completed(0)
        self.assertAlmostEqual(collector.average_duration, 0.0)
    
    def test_average_duration_precision(self) -> None:
        """Test that average_duration maintains precision."""
        collector = MetricsCollector()
        
        # Add durations that result in a fractional average
        collector.increment_completed(1.0)
        collector.increment_completed(2.0)
        collector.increment_completed(2.0)
        
        # Average = (1 + 2 + 2) / 3 = 1.666...
        avg = collector.average_duration
        self.assertAlmostEqual(avg, 1.6666667, places=6)


if __name__ == '__main__':
    unittest.main()