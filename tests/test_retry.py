"""
Unit tests for retry logic

Tests for RetryPolicy, exponential backoff calculation, 
max delay enforcement, and @with_retry decorator behavior.
"""

import time
import pytest
from unittest.mock import patch

from pytaskq.retry import RetryPolicy, with_retry, RetryError


class TestRetryPolicyInitialization:
    """Test RetryPolicy initialization and validation."""
    
    def test_default_retry_policy(self):
        """Test RetryPolicy with default values."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 60.0
        assert policy.exponential_base == 2.0
        assert policy.jitter is False
    
    def test_custom_retry_policy(self):
        """Test RetryPolicy with custom values."""
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=2.0,
            max_delay=30.0,
            exponential_base=3.0,
            jitter=True
        )
        assert policy.max_attempts == 5
        assert policy.base_delay == 2.0
        assert policy.max_delay == 30.0
        assert policy.exponential_base == 3.0
        assert policy.jitter is True
    
    def test_invalid_max_attempts(self):
        """Test RetryPolicy with invalid max_attempts."""
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            RetryPolicy(max_attempts=0)
        
        with pytest.raises(ValueError, match="max_attempts must be at least 1"):
            RetryPolicy(max_attempts=-1)
    
    def test_invalid_base_delay(self):
        """Test RetryPolicy with invalid base_delay."""
        with pytest.raises(ValueError, match="base_delay must be non-negative"):
            RetryPolicy(base_delay=-1.0)
    
    def test_invalid_max_delay(self):
        """Test RetryPolicy with invalid max_delay."""
        with pytest.raises(ValueError, match="max_delay must be non-negative"):
            RetryPolicy(max_delay=-1.0)
    
    def test_invalid_exponential_base(self):
        """Test RetryPolicy with invalid exponential_base."""
        with pytest.raises(ValueError, match="exponential_base must be at least 1"):
            RetryPolicy(exponential_base=0.5)
        
        with pytest.raises(ValueError, match="exponential_base must be at least 1"):
            RetryPolicy(exponential_base=0)


class TestBackoffCalculation:
    """Test exponential backoff calculation."""
    
    def test_backoff_calculation_default_policy(self):
        """Test backoff calculation with default policy."""
        policy = RetryPolicy(base_delay=1.0, exponential_base=2.0)
        
        # Attempt 0: 1.0 * (2^0) = 1.0
        assert policy.calculate_delay(0) == 1.0
        
        # Attempt 1: 1.0 * (2^1) = 2.0
        assert policy.calculate_delay(1) == 2.0
        
        # Attempt 2: 1.0 * (2^2) = 4.0
        assert policy.calculate_delay(2) == 4.0
        
        # Attempt 3: 1.0 * (2^3) = 8.0
        assert policy.calculate_delay(3) == 8.0
    
    def test_backoff_calculation_custom_base(self):
        """Test backoff calculation with custom base delay."""
        policy = RetryPolicy(base_delay=2.0, exponential_base=2.0)
        
        # Attempt 0: 2.0 * (2^0) = 2.0
        assert policy.calculate_delay(0) == 2.0
        
        # Attempt 1: 2.0 * (2^1) = 4.0
        assert policy.calculate_delay(1) == 4.0
        
        # Attempt 2: 2.0 * (2^2) = 8.0
        assert policy.calculate_delay(2) == 8.0
    
    def test_backoff_calculation_different_base(self):
        """Test backoff calculation with different exponential base."""
        policy = RetryPolicy(base_delay=1.0, exponential_base=3.0)
        
        # Attempt 0: 1.0 * (3^0) = 1.0
        assert policy.calculate_delay(0) == 1.0
        
        # Attempt 1: 1.0 * (3^1) = 3.0
        assert policy.calculate_delay(1) == 3.0
        
        # Attempt 2: 1.0 * (3^2) = 9.0
        assert policy.calculate_delay(2) == 9.0
    
    def test_backoff_calculation_zero_delay(self):
        """Test backoff calculation with zero base delay."""
        policy = RetryPolicy(base_delay=0.0, exponential_base=2.0)
        
        # All attempts should have zero delay
        assert policy.calculate_delay(0) == 0.0
        assert policy.calculate_delay(1) == 0.0
        assert policy.calculate_delay(5) == 0.0
    
    def test_backoff_calculation_large_attempts(self):
        """Test backoff calculation with large attempt numbers."""
        policy = RetryPolicy(base_delay=1.0, exponential_base=2.0)
        
        # Attempt 10: 1.0 * (2^10) = 1024.0
        delay = policy.calculate_delay(10)
        assert delay == 1024.0
    
    def test_backoff_invalid_attempt(self):
        """Test backoff calculation with invalid attempt number."""
        policy = RetryPolicy()
        
        with pytest.raises(ValueError, match="attempt must be non-negative"):
            policy.calculate_delay(-1)
    
    def test_backoff_fractional_delay(self):
        """Test backoff calculation with fractional base delay."""
        policy = RetryPolicy(base_delay=0.5, exponential_base=2.0)
        
        # Attempt 0: 0.5 * (2^0) = 0.5
        assert policy.calculate_delay(0) == 0.5
        
        # Attempt 1: 0.5 * (2^1) = 1.0
        assert policy.calculate_delay(1) == 1.0
    
    def test_backoff_with_jitter(self):
        """Test that jitter adds randomness to delay."""
        policy = RetryPolicy(base_delay=10.0, jitter=True)
        
        # With jitter, delay should be in range [10.0, 12.5]
        # (base delay + up to 25% random jitter)
        delays = [policy.calculate_delay(0) for _ in range(50)]
        
        for delay in delays:
            assert 10.0 <= delay <= 12.5
        
        # At least some variation should occur
        assert len(set(delays)) > 1
    
    def test_backoff_without_jitter(self):
        """Test that without jitter, delays are deterministic."""
        policy = RetryPolicy(base_delay=1.0, jitter=False)
        
        # Without jitter, delay should be the same each time
        delay1 = policy.calculate_delay(0)
        delay2 = policy.calculate_delay(0)
        delay3 = policy.calculate_delay(0)
        
        assert delay1 == delay2 == delay3


class TestMaxDelayEnforcement:
    """Test max delay enforcement in exponential backoff."""
    
    def test_max_delay_enforcement(self):
        """Test that delays are capped at max_delay."""
        policy = RetryPolicy(base_delay=1.0, max_delay=10.0, exponential_base=2.0)
        
        # Attempt 0: 1.0 * (2^0) = 1.0 (under max)
        assert policy.calculate_delay(0) == 1.0
        
        # Attempt 1: 1.0 * (2^1) = 2.0 (under max)
        assert policy.calculate_delay(1) == 2.0
        
        # Attempt 2: 1.0 * (2^2) = 4.0 (under max)
        assert policy.calculate_delay(2) == 4.0
        
        # Attempt 3: 1.0 * (2^3) = 8.0 (under max)
        assert policy.calculate_delay(3) == 8.0
        
        # Attempt 4: 1.0 * (2^4) = 16.0 (over max, should cap at 10.0)
        assert policy.calculate_delay(4) == 10.0
        
        # Attempt 10: would be 1024.0 (way over max, should cap at 10.0)
        assert policy.calculate_delay(10) == 10.0
    
    def test_max_delay_equal_to_base_delay(self):
        """Test that max_delay works when equal to base_delay."""
        policy = RetryPolicy(base_delay=5.0, max_delay=5.0, exponential_base=2.0)
        
        # All attempts should be capped at 5.0
        assert policy.calculate_delay(0) == 5.0
        assert policy.calculate_delay(1) == 5.0
        assert policy.calculate_delay(5) == 5.0
    
    def test_max_delay_zero(self):
        """Test that max_delay=0 results in zero delay."""
        policy = RetryPolicy(base_delay=1.0, max_delay=0.0, exponential_base=2.0)
        
        # All attempts should be capped at 0.0
        assert policy.calculate_delay(0) == 0.0
        assert policy.calculate_delay(1) == 0.0
        assert policy.calculate_delay(10) == 0.0
    
    def test_max_delay_with_jitter(self):
        """Test that max_delay enforcement works with jitter."""
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0, jitter=True)
        
        # For attempt 4, exponential would be 16.0, but maxed at 5.0
        # With jitter, should be in range [5.0, 6.25]
        delays = [policy.calculate_delay(4) for _ in range(50)]
        
        for delay in delays:
            assert 5.0 <= delay <= 6.25


class TestShouldRetry:
    """Test should_retry logic."""
    
    def test_should_retry_within_limit(self):
        """Test should_retry returns True within attempt limit."""
        policy = RetryPolicy(max_attempts=3)
        
        # Attempt 0: first try, has 2 retries left
        assert policy.should_retry(0) is True
        
        # Attempt 1: first retry, has 1 retry left
        assert policy.should_retry(1) is True
    
    def test_should_retry_at_limit(self):
        """Test should_retry returns False at attempt limit."""
        policy = RetryPolicy(max_attempts=3)
        
        # Attempt 2: second retry (final attempt), no retries left
        assert policy.should_retry(2) is False
        
        # Attempt 3: beyond max attempts
        assert policy.should_retry(3) is False
    
    def test_should_retry_single_attempt(self):
        """Test should_retry with max_attempts=1."""
        policy = RetryPolicy(max_attempts=1)
        
        # Single attempt, never retry
        assert policy.should_retry(0) is False
        assert policy.should_retry(1) is False
    
    def test_should_retry_many_attempts(self):
        """Test should_retry with high max_attempts."""
        policy = RetryPolicy(max_attempts=10)
        
        # Should retry for attempts 0-8
        for attempt in range(9):
            assert policy.should_retry(attempt) is True
        
        # Should not retry at attempt 9
        assert policy.should_retry(9) is False
    
    def test_should_retry_invalid_attempt(self):
        """Test should_retry with invalid attempt number."""
        policy = RetryPolicy()
        
        with pytest.raises(ValueError, match="attempt must be non-negative"):
            policy.should_retry(-1)


class TestWithRetryDecorator:
    """Test @with_retry decorator behavior."""
    
    def test_decorator_successful_function(self):
        """Test decorator with function that succeeds on first try."""
        @with_retry(max_attempts=3)
        def successful_func():
            return "success"
        
        result = successful_func()
        assert result == "success"
    
    def test_decorator_fails_then_succeeds(self):
        """Test decorator with function that fails then succeeds."""
        attempts = [0]
        
        @with_retry(max_attempts=3, base_delay=0.01)
        def fails_twice_func():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ValueError("Temporary failure")
            return "success"
        
        result = fails_twice_func()
        assert result == "success"
        assert attempts[0] == 3
    
    def test_decorator_all_attempts_fail(self):
        """Test decorator when all retry attempts fail."""
        @with_retry(max_attempts=3, base_delay=0.01)
        def always_fail_func():
            raise ValueError("Permanent failure")
        
        with pytest.raises(RetryError, match="failed after 3 attempts"):
            always_fail_func()
    
    def test_decorator_single_attempt_no_retry(self):
        """Test decorator with max_attempts=1 (no retries)."""
        @with_retry(max_attempts=1)
        def fail_once_func():
            raise ValueError("Failed")
        
        with pytest.raises(RetryError, match="failed after 1 attempts"):
            fail_once_func()
    
    def test_decorator_custom_policy(self):
        """Test decorator with custom RetryPolicy."""
        policy = RetryPolicy(max_attempts=5, base_delay=0.01)
        
        @with_retry(policy=policy)
        def custom_retry_func():
            raise ValueError("Fail")
        
        with pytest.raises(RetryError, match="failed after 5 attempts"):
            custom_retry_func()
    
    def test_decorator_specific_exceptions(self):
        """Test decorator that only retries on specific exceptions."""
        attempts = [0]
        
        @with_retry(max_attempts=3, base_delay=0.01, exceptions=(ValueError,))
        def selective_retry_func(raise_type):
            attempts[0] += 1
            if raise_type == "value_error":
                raise ValueError("Value error")
            elif raise_type == "type_error":
                raise TypeError("Type error")
        
        # Should retry on ValueError
        attempts[0] = 0
        with pytest.raises(RetryError):
            selective_retry_func("value_error")
        assert attempts[0] == 3
        
        # Should not retry on TypeError
        attempts[0] = 0
        with pytest.raises(TypeError):
            selective_retry_func("type_error")
        assert attempts[0] == 1
    
    def test_decorator_on_retry_callback(self):
        """Test decorator with on_retry callback."""
        retry_log = []
        
        def on_retry_callback(error, attempt):
            retry_log.append((type(error).__name__, attempt))
        
        @with_retry(max_attempts=3, base_delay=0.01, on_retry=on_retry_callback)
        def failing_func():
            raise ValueError("Test error")
        
        with pytest.raises(RetryError):
            failing_func()
        
        # Callback should be called twice (after attempts 0 and 1)
        assert len(retry_log) == 2
        assert retry_log[0] == ("ValueError", 1)
        assert retry_log[1] == ("ValueError", 2)
    
    def test_decorator_preserves_function_name(self):
        """Test that decorator preserves original function name."""
        @with_retry(max_attempts=3)
        def my_function():
            return "result"
        
        assert my_function.__name__ == "my_function"
    
    def test_decorator_preserves_function_docstring(self):
        """Test that decorator preserves original function docstring."""
        @with_retry(max_attempts=3)
        def documented_function():
            """This is a documented function."""
            return "result"
        
        assert documented_function.__doc__ == "This is a documented function."
    
    def test_decorator_with_arguments(self):
        """Test decorator with function that takes arguments."""
        @with_retry(max_attempts=3, base_delay=0.01)
        def func_with_args(x, y):
            if x + y < 10:
                raise ValueError("Too small")
            return x + y
        
        result = func_with_args(5, 10)
        assert result == 15
    
    def test_decorator_with_kwargs(self):
        """Test decorator with function that takes keyword arguments."""
        @with_retry(max_attempts=3, base_delay=0.01)
        def func_with_kwargs(a, b=None):
            if b is None:
                raise ValueError("b is required")
            return a + b
        
        result = func_with_kwargs(5, b=10)
        assert result == 15
    
    def test_decorator_zero_delay_fast_retries(self):
        """Test that zero base_delay allows fast retries."""
        attempts = [0]
        
        @with_retry(base_delay=0.0, max_attempts=5)
        def fast_retry_func():
            attempts[0] += 1
            if attempts[0] < 5:
                raise ValueError("Not yet")
            return "done"
        
        start = time.time()
        result = fast_retry_func()
        elapsed = time.time() - start
        
        assert result == "done"
        assert attempts[0] == 5
        # Should complete very quickly (< 1 second) with zero delay
        assert elapsed < 1.0
    
    def test_decorator_exception_chain(self):
        """Test that original exception is chained in RetryError."""
        original_error = ValueError("Original error")
        
        @with_retry(max_attempts=2, base_delay=0.01)
        def chain_func():
            raise original_error
        
        try:
            chain_func()
            pytest.fail("Should have raised RetryError")
        except RetryError as e:
            assert e.__cause__ is original_error
            assert "chain_func" in str(e)
    
    def test_decorator_with_jitter(self):
        """Test decorator with jitter enabled."""
        attempts = [0]
        delays = []
        
        def on_retry_with_timing(error, attempt):
            delays.append(attempt)
        
        @with_retry(
            policy=RetryPolicy(max_attempts=3, base_delay=0.1, jitter=True),
            on_retry=on_retry_with_timing
        )
        def jitter_func():
            attempts[0] += 1
            if attempts[0] < 3:
                raise ValueError("Fail")
            return "success"
        
        result = jitter_func()
        assert result == "success"
        assert len(delays) == 2
    
    def test_decorator_multiple_exception_types(self):
        """Test decorator with multiple exception types."""
        attempts = [0]
        
        @with_retry(
            max_attempts=3,
            base_delay=0.01,
            exceptions=(ValueError, TypeError, KeyError)
        )
        def multi_exception_func(error_type):
            attempts[0] += 1
            if error_type == "value":
                raise ValueError("Value error")
            elif error_type == "type":
                raise TypeError("Type error")
            elif error_type == "key":
                raise KeyError("Key error")
        
        # Should retry on all specified exception types
        for error_type in ["value", "type", "key"]:
            attempts[0] = 0
            with pytest.raises(RetryError):
                multi_exception_func(error_type)
            assert attempts[0] == 3  # Max attempts reached


class TestRetryError:
    """Test RetryError exception."""
    
    def test_retry_error_creation(self):
        """Test RetryError can be created with message."""
        error = RetryError("Test error message")
        assert str(error) == "Test error message"
    
    def test_retry_error_with_cause(self):
        """Test RetryError can chain with cause."""
        original = ValueError("Original")
        error = RetryError("Wrapped error", original)
        
        assert error.__cause__ is original
        assert "Wrapped error" in str(error)


class TestIntegrationScenarios:
    """Integration tests combining multiple retry features."""
    
    def test_backoff_sequence_integration(self):
        """Test complete backoff sequence with delays."""
        policy = RetryPolicy(
            max_attempts=4,
            base_delay=0.01,
            max_delay=0.1,
            exponential_base=2.0,
            jitter=False
        )
        
        delays = [policy.calculate_delay(i) for i in range(4)]
        
        # Should follow exponential backoff:
        # 0.01, 0.02, 0.04, 0.08 (all under max_delay of 0.1)
        assert delays == [0.01, 0.02, 0.04, 0.08]
    
    def test_full_retry_scenario_with_callback(self):
        """Test complete retry scenario with tracking."""
        attempt_results = []
        
        def track_retry(error, attempt):
            attempt_results.append(("retry", attempt, str(error)))
        
        attempts = [0]
        
        @with_retry(
            policy=RetryPolicy(max_attempts=4, base_delay=0.01, jitter=False),
            on_retry=track_retry
        )
        def scenario_func():
            attempts[0] += 1
            if attempts[0] == 1:
                raise ValueError("First error")
            elif attempts[0] == 2:
                raise TypeError("Second error")
            elif attempts[0] == 3:
                raise RuntimeError("Third error")
            return "success"
        
        result = scenario_func()
        
        assert result == "success"
        assert len(attempt_results) == 3
        assert attempt_results[0] == ("retry", 1, "First error")
        assert attempt_results[1] == ("retry", 2, "Second error")
        assert attempt_results[2] == ("retry", 3, "Third error")
    
    def test_decorator_with_large_max_attempts_and_max_delay(self):
        """Test decorator with high max_attempts and max_delay cap."""
        @with_retry(
            base_delay=1.0,
            max_delay=5.0,
            max_attempts=20,
            exceptions=(ValueError,)
        )
        def high_retry_func():
            raise ValueError("Always fails")
        
        with pytest.raises(RetryError, match="failed after 20 attempts"):
            high_retry_func()