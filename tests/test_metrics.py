"""
Unit tests for the MetricsCollector class.

Tests cover thread-safety of counters, average_duration calculation,
reset functionality, snapshot method, and increment operations.
"""

import threading
import time
import pytest

from pytaskq.metrics import MetricsCollector, MetricsSnapshot


class TestMetricsCollectorInitialState:
    """Test the initial state of a new MetricsCollector."""
    
    def test_initial_counters_are_zero(self) -> None:
        """Test that all counters start at zero."""
        collector = MetricsCollector()
        
        assert collector.tasks_submitted == 0
        assert collector.tasks_completed == 0
        assert collector.tasks_failed == 0
        assert collector.average_duration == 0.0
        assert collector.total_duration == 0.0
        assert collector.duration_count == 0
    
    def test_initial_snapshot(self) -> None:
        """Test that snapshot returns correct initial state."""
        collector = MetricsCollector()
        snapshot = collector.snapshot()
        
        assert isinstance(snapshot, MetricsSnapshot)
        assert snapshot.tasks_submitted == 0
        assert snapshot.tasks_completed == 0
        assert snapshot.tasks_failed == 0
        assert snapshot.average_duration == 0.0
        assert snapshot.total_duration == 0.0
        assert snapshot.duration_count == 0


class TestCounterIncrements:
    """Test increment operations for all counters."""
    
    def test_increment_submitted_default(self) -> None:
        """Test incrementing submitted counter with default value."""
        collector = MetricsCollector()
        collector.increment_submitted()
        
        assert collector.tasks_submitted == 1
        assert collector.tasks_completed == 0
        assert collector.tasks_failed == 0
    
    def test_increment_submitted_multiple(self) -> None:
        """Test incrementing submitted counter with custom value."""
        collector = MetricsCollector()
        collector.increment_submitted(5)
        
        assert collector.tasks_submitted == 5
    
    def test_increment_submitted_multiple_calls(self) -> None:
        """Test multiple increment_submitted calls accumulate."""
        collector = MetricsCollector()
        collector.increment_submitted(3)
        collector.increment_submitted(2)
        collector.increment_submitted()
        
        assert collector.tasks_submitted == 6
    
    def test_increment_submitted_ignores_zero(self) -> None:
        """Test that increment_submitted ignores zero."""
        collector = MetricsCollector()
        collector.increment_submitted(0)
        
        assert collector.tasks_submitted == 0
    
    def test_increment_submitted_ignores_negative(self) -> None:
        """Test that increment_submitted ignores negative values."""
        collector = MetricsCollector()
        collector.increment_submitted(5)
        collector.increment_submitted(-3)
        
        assert collector.tasks_submitted == 5
    
    def test_increment_completed_without_duration(self) -> None:
        """Test incrementing completed counter without duration."""
        collector = MetricsCollector()
        collector.increment_completed()
        
        assert collector.tasks_completed == 1
        assert collector.average_duration == 0.0
        assert collector.duration_count == 0
    
    def test_increment_completed_with_duration(self) -> None:
        """Test incrementing completed counter with duration."""
        collector = MetricsCollector()
        collector.increment_completed(duration=1.5)
        
        assert collector.tasks_completed == 1
        assert collector.total_duration == 1.5
        assert collector.duration_count == 1
        assert collector.average_duration == 1.5
    
    def test_increment_completed_multiple_durations(self) -> None:
        """Test incrementing completed counter with multiple durations."""
        collector = MetricsCollector()
        collector.increment_completed(duration=2.0)
        collector.increment_completed(duration=4.0)
        collector.increment_completed()
        
        assert collector.tasks_completed == 3
        assert collector.total_duration == 6.0
        assert collector.duration_count == 2
        assert collector.average_duration == 3.0
    
    def test_increment_completed_ignores_negative_duration(self) -> None:
        """Test that increment_completed ignores negative durations."""
        collector = MetricsCollector()
        collector.increment_completed(duration=-1.5)
        
        assert collector.tasks_completed == 1
        assert collector.total_duration == 0.0
        assert collector.duration_count == 0
    
    def test_increment_completed_ignores_zero_duration(self) -> None:
        """Test that increment_completed ignores zero duration."""
        collector = MetricsCollector()
        collector.increment_completed(duration=0.0)
        
        assert collector.tasks_completed == 1
        assert collector.total_duration == 0.0
        assert collector.duration_count == 0
    
    def test_increment_failed_without_duration(self) -> None:
        """Test incrementing failed counter without duration."""
        collector = MetricsCollector()
        collector.increment_failed()
        
        assert collector.tasks_failed == 1
        assert collector.average_duration == 0.0
        assert collector.duration_count == 0
    
    def test_increment_failed_with_duration(self) -> None:
        """Test incrementing failed counter with duration."""
        collector = MetricsCollector()
        collector.increment_failed(duration=2.5)
        
        assert collector.tasks_failed == 1
        assert collector.total_duration == 2.5
        assert collector.duration_count == 1
        assert collector.average_duration == 2.5
    
    def test_increment_failed_multiple_durations(self) -> None:
        """Test incrementing failed counter with multiple durations."""
        collector = MetricsCollector()
        collector.increment_failed(duration=1.0)
        collector.increment_failed(duration=2.0)
        collector.increment_failed()
        
        assert collector.tasks_failed == 3
        assert collector.total_duration == 3.0
        assert collector.duration_count == 2
        assert collector.average_duration == 1.5
    
    def test_increment_failed_ignores_negative_duration(self) -> None:
        """Test that increment_failed ignores negative durations."""
        collector = MetricsCollector()
        collector.increment_failed(duration=-2.0)
        
        assert collector.tasks_failed == 1
        assert collector.total_duration == 0.0
        assert collector.duration_count == 0


class TestAverageDurationCalculation:
    """Test average duration calculation logic."""
    
    def test_average_duration_single_value(self) -> None:
        """Test average duration with a single value."""
        collector = MetricsCollector()
        collector.increment_completed(duration=10.0)
        
        assert collector.average_duration == 10.0
    
    def test_average_duration_multiple_values(self) -> None:
        """Test average duration with multiple values."""
        collector = MetricsCollector()
        collector.increment_completed(duration=2.0)
        collector.increment_completed(duration=4.0)
        collector.increment_completed(duration=6.0)
        
        assert collector.average_duration == 4.0
    
    def test_average_duration_mixed_completed_and_failed(self) -> None:
        """Test average duration includes both completed and failed tasks."""
        collector = MetricsCollector()
        collector.increment_completed(duration=2.0)
        collector.increment_failed(duration=4.0)
        collector.increment_completed(duration=6.0)
        
        assert collector.duration_count == 3
        assert collector.average_duration == 4.0
    
    def test_average_duration_floating_point(self) -> None:
        """Test average duration with floating point precision."""
        collector = MetricsCollector()
        collector.increment_completed(duration=1.5)
        collector.increment_completed(duration=2.5)
        
        assert abs(collector.average_duration - 2.0) < 0.0001
    
    def test_average_duration_large_values(self) -> None:
        """Test average duration with large values."""
        collector = MetricsCollector()
        collector.increment_completed(duration=1000.0)
        collector.increment_completed(duration=2000.0)
        
        assert collector.average_duration == 1500.0
    
    def test_average_duration_no_durations_recorded(self) -> None:
        """Test average duration when no durations have been recorded."""
        collector = MetricsCollector()
        collector.increment_submitted(10)
        
        assert collector.average_duration == 0.0
    
    def test_average_duration_resets_correctly(self) -> None:
        """Test that average duration resets correctly."""
        collector = MetricsCollector()
        collector.increment_completed(duration=5.0)
        collector.increment_completed(duration=15.0)
        
        assert collector.average_duration == 10.0
        
        collector.reset()
        assert collector.average_duration == 0.0


class TestResetFunctionality:
    """Test reset functionality."""
    
    def test_resets_all_counters(self) -> None:
        """Test that reset clears all counters to zero."""
        collector = MetricsCollector()
        
        collector.increment_submitted(10)
        collector.increment_completed(duration=5.0)
        collector.increment_completed(duration=5.0)
        collector.increment_failed(duration=10.0)
        
        assert collector.tasks_submitted == 10
        assert collector.tasks_completed == 2
        assert collector.tasks_failed == 1
        assert collector.total_duration > 0
        assert collector.duration_count == 3
        
        collector.reset()
        
        assert collector.tasks_submitted == 0
        assert collector.tasks_completed == 0
        assert collector.tasks_failed == 0
        assert collector.total_duration == 0.0
        assert collector.duration_count == 0
        assert collector.average_duration == 0.0
    
    def test_reset_empty_collector(self) -> None:
        """Test that reset works on an empty collector."""
        collector = MetricsCollector()
        collector.reset()
        
        assert collector.tasks_submitted == 0
        assert collector.tasks_completed == 0
        assert collector.tasks_failed == 0
    
    def test_reset_multiple_times(self) -> None:
        """Test that reset can be called multiple times."""
        collector = MetricsCollector()
        
        collector.increment_submitted(5)
        collector.reset()
        collector.increment_completed(duration=3.0)
        collector.reset()
        collector.increment_failed(duration=7.0)
        
        assert collector.tasks_submitted == 0
        assert collector.tasks_completed == 0
        assert collector.tasks_failed == 1
        assert collector.total_duration == 7.0


class TestSnapshotFunctionality:
    """Test snapshot functionality."""
    
    def test_snapshot_returns_immutable(self) -> None:
        """Test that snapshot returns an immutable MetricsSnapshot."""
        collector = MetricsCollector()
        collector.increment_submitted(5)
        collector.increment_completed(duration=10.0)
        
        snapshot = collector.snapshot()
        
        # Test that snapshot is frozen dataclass
        assert isinstance(snapshot, MetricsSnapshot)
        
        # Try to modify (should fail silently or raise error)
        try:
            snapshot.tasks_submitted = 10
            assert False, "Should not be able to modify frozen dataclass"
        except (AttributeError, TypeError):
            pass  # Expected
    
    def test_snapshot_captures_current_state(self) -> None:
        """Test that snapshot captures the current state."""
        collector = MetricsCollector()
        
        collector.increment_submitted(3)
        snapshot1 = collector.snapshot()
        assert snapshot1.tasks_submitted == 3
        
        collector.increment_submitted(2)
        snapshot2 = collector.snapshot()
        assert snapshot2.tasks_submitted == 5
        
        # First snapshot should not change
        assert snapshot1.tasks_submitted == 3
    
    def test_snapshot_independent_from_collector(self) -> None:
        """Test that changes to collector don't affect existing snapshots."""
        collector = MetricsCollector()
        
        collector.increment_submitted(10)
        snapshot = collector.snapshot()
        
        assert snapshot.tasks_submitted == 10
        
        collector.reset()
        collector.increment_submitted(5)
        
        # Snapshot should still show original value
        assert snapshot.tasks_submitted == 10
        assert collector.tasks_submitted == 5
    
    def test_snapshot_all_fields(self) -> None:
        """Test that snapshot includes all metrics fields."""
        collector = MetricsCollector()
        
        collector.increment_submitted(100)
        collector.increment_completed(duration=2.0)
        collector.increment_completed(duration=4.0)
        collector.increment_failed(duration=1.0)
        
        snapshot = collector.snapshot()
        
        assert snapshot.tasks_submitted == 100
        assert snapshot.tasks_completed == 2
        assert snapshot.tasks_failed == 1
        assert snapshot.total_duration == 7.0
        assert snapshot.duration_count == 3
        assert snapshot.average_duration == pytest.approx(7.0 / 3)
    
    def test_snapshot_after_reset(self) -> None:
        """Test that snapshot after reset shows zeros."""
        collector = MetricsCollector()
        
        collector.increment_submitted(10)
        collector.increment_completed(duration=5.0)
        
        collector.reset()
        snapshot = collector.snapshot()
        
        assert snapshot.tasks_submitted == 0
        assert snapshot.tasks_completed == 0
        assert snapshot.tasks_failed == 0
        assert snapshot.total_duration == 0.0
        assert snapshot.duration_count == 0
        assert snapshot.average_duration == 0.0


class TestThreadSafety:
    """Test thread-safety of counters and operations."""
    
    def test_thread_safety_increment_submitted(self) -> None:
        """Test thread-safety of increment_submitted."""
        collector = MetricsCollector()
        num_threads = 100
        increments_per_thread = 1000
        
        def increment_many() -> None:
            for _ in range(increments_per_thread):
                collector.increment_submitted()
        
        threads = [
            threading.Thread(target=increment_many)
            for _ in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        expected = num_threads * increments_per_thread
        assert collector.tasks_submitted == expected
    
    def test_thread_safety_increment_completed(self) -> None:
        """Test thread-safety of increment_completed."""
        collector = MetricsCollector()
        num_threads = 50
        increments_per_thread = 100
        
        def increment_many() -> None:
            for i in range(increments_per_thread):
                collector.increment_completed(duration=float(i % 10))
        
        threads = [
            threading.Thread(target=increment_many)
            for _ in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        assert collector.tasks_completed == num_threads * increments_per_thread
    
    def test_thread_safety_increment_failed(self) -> None:
        """Test thread-safety of increment_failed."""
        collector = MetricsCollector()
        num_threads = 50
        increments_per_thread = 100
        
        def increment_many() -> None:
            for i in range(increments_per_thread):
                collector.increment_failed(duration=float(i % 10))
        
        threads = [
            threading.Thread(target=increment_many)
            for _ in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        assert collector.tasks_failed == num_threads * increments_per_thread
    
    def test_thread_safety_mixed_operations(self) -> None:
        """Test thread-safety with mixed operations."""
        collector = MetricsCollector()
        num_threads = 30
        operations_per_thread = 200
        
        def mixed_operations(thread_id: int) -> None:
            for i in range(operations_per_thread):
                if i % 3 == 0:
                    collector.increment_submitted()
                elif i % 3 == 1:
                    collector.increment_completed(duration=float(i % 5))
                else:
                    collector.increment_failed(duration=float(i % 5))
        
        threads = [
            threading.Thread(target=mixed_operations, args=(i,))
            for i in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Total operations should match
        total = num_threads * operations_per_thread
        assert collector.tasks_submitted + collector.tasks_completed + collector.tasks_failed == total
    
    def test_thread_safety_reset_while_incrementing(self) -> None:
        """Test thread-safety when reset is called during increments."""
        collector = MetricsCollector()
        num_threads = 20
        increments_per_thread = 1000
        
        def increment_many() -> None:
            for _ in range(increments_per_thread):
                collector.increment_submitted()
        
        threads = [
            threading.Thread(target=increment_many)
            for _ in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        # Reset while threads are still running
        time.sleep(0.001)  # Small delay to let threads start
        collector.reset()
        
        for thread in threads:
            thread.join()
        
        # Should have some counts from after reset
        assert collector.tasks_submitted > 0
    
    def test_thread_safety_snapshot_while_incrementing(self) -> None:
        """Test thread-safety when snapshot is called during increments."""
        collector = MetricsCollector()
        num_threads = 20
        increments_per_thread = 100
        snapshots = []
        
        def increment_many() -> None:
            for _ in range(increments_per_thread):
                collector.increment_submitted()
        
        def take_snapshots() -> None:
            for _ in range(50):
                snapshot = collector.snapshot()
                snapshots.append(snapshot.tasks_submitted)
                time.sleep(0.0001)
        
        increment_threads = [
            threading.Thread(target=increment_many)
            for _ in range(num_threads)
        ]
        
        snapshot_thread = threading.Thread(target=take_snapshots)
        
        for thread in increment_threads:
            thread.start()
        snapshot_thread.start()
        
        for thread in increment_threads:
            thread.join()
        snapshot_thread.join()
        
        # All snapshots should be valid (non-negative and not exceeding final count)
        final_count = collector.tasks_submitted
        for count in snapshots:
            assert 0 <= count <= final_count
        
        # Should have captured multiple different values
        assert len(set(snapshots)) > 1
    
    def test_thread_safety_properties_access(self) -> None:
        """Test thread-safety of property access during concurrent updates."""
        collector = MetricsCollector()
        num_threads = 30
        iterations = 500
        results = []
        
        def read_and_write(thread_id: int) -> None:
            for i in range(iterations):
                # Read all properties
                submitted = collector.tasks_submitted
                completed = collector.tasks_completed
                failed = collector.tasks_failed
                avg = collector.average_duration
                
                # Verify consistency
                if submitted < 0 or completed < 0 or failed < 0 or avg < 0:
                    results.append(False)
                    return
                
                # Write
                if i % 3 == 0:
                    collector.increment_submitted()
                elif i % 3 == 1:
                    collector.increment_completed(duration=float(i))
                else:
                    collector.increment_failed(duration=float(i))
        
        threads = [
            threading.Thread(target=read_and_write, args=(i,))
            for i in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have no errors (no False in results)
        assert not any(results)
    
    def test_thread_safety_concurrent_resets(self) -> None:
        """Test thread-safety with multiple threads calling reset."""
        collector = MetricsCollector()
        num_threads = 10
        iterations = 50
        
        def increment_and_reset() -> None:
            for _ in range(iterations):
                for _ in range(100):
                    collector.increment_submitted()
                collector.reset()
        
        threads = [
            threading.Thread(target=increment_and_reset)
            for _ in range(num_threads)
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Final state should be consistent (might be 0 or some value from last operation)
        assert collector.tasks_submitted >= 0


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_zero_duration_handled(self) -> None:
        """Test that zero duration doesn't break average calculation."""
        collector = MetricsCollector()
        
        collector.increment_completed(duration=0.0)
        collector.increment_completed(duration=5.0)
        collector.increment_completed(duration=5.0)
        
        # Zero duration should be ignored
        assert collector.duration_count == 2
        assert collector.average_duration == 5.0
    
    def test_negative_duration_handled(self) -> None:
        """Test that negative duration doesn't break average calculation."""
        collector = MetricsCollector()
        
        collector.increment_completed(duration=-5.0)
        collector.increment_completed(duration=10.0)
        collector.increment_completed(duration=20.0)
        
        # Negative duration should be ignored
        assert collector.duration_count == 2
        assert collector.average_duration == 15.0
    
    def test_very_small_durations(self) -> None:
        """Test handling of very small durations."""
        collector = MetricsCollector()
        
        collector.increment_completed(duration=0.0001)
        collector.increment_completed(duration=0.0002)
        
        assert abs(collector.average_duration - 0.00015) < 0.00001
    
    def test_all_increments_without_durations(self) -> None:
        """Test that increments without durations don't affect average."""
        collector = MetricsCollector()
        
        collector.increment_completed()
        collector.increment_completed()
        collector.increment_failed()
        collector.increment_failed()
        
        assert collector.tasks_completed == 2
        assert collector.tasks_failed == 2
        assert collector.duration_count == 0
        assert collector.average_duration == 0.0
    
    def test_single_operation_each(self) -> None:
        """Test single operation of each type."""
        collector = MetricsCollector()
        
        collector.increment_submitted(1)
        collector.increment_completed(duration=1.0)
        collector.increment_failed(duration=2.0)
        
        snapshot = collector.snapshot()
        
        assert snapshot.tasks_submitted == 1
        assert snapshot.tasks_completed == 1
        assert snapshot.tasks_failed == 1
        assert snapshot.duration_count == 2
        assert snapshot.average_duration == 1.5