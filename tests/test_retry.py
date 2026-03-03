"""
Tests for Retry Policy
"""

import time
import pytest

from pytaskq import RetryPolicy, with_retry, with_retry_error, RetryError


class TestRetryPolicy:
    """Test cases for RetryPolicy class."""
    
    def test_default_initialization(self) -> None:
        """Test that RetryPolicy initializes with correct defaults."""
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.backoff_factor == 2.0
        assert policy.max_delay == 60.0
        assert policy.initial_delay == 1.0
        assert policy.retriable_exceptions == (Exception,)
    
    def test_custom_initialization(self) -> None:
        """Test that RetryPolicy accepts custom values."""
        policy = RetryPolicy(
            max_retries=5,
            backoff_factor=1.5,
            max_delay=120.0,
            initial_delay=2.0,
            retriable_exceptions=(ValueError, KeyError),
        )
        assert policy.max_retries == 5
        assert policy.backoff_factor == 1.5
        assert policy.max_delay == 120.0
        assert policy.initial_delay == 2.0
        assert policy.retriable_exceptions == (ValueError, KeyError)
    
    def test_invalid_max_retries(self) -> None:
        """Test that negative max_retries raises ValueError."""
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            RetryPolicy(max_retries=-1)
    
    def test_invalid_backoff_factor(self) -> None:
        """Test that backoff_factor < 1.0 raises ValueError."""
        with pytest.raises(ValueError, match="backoff_factor must be >= 1.0"):
            RetryPolicy(backoff_factor=0.5)
    
    def test_invalid_max_delay(self) -> None:
        """Test that non-positive max_delay raises ValueError."""
        with pytest.raises(ValueError, match="max_delay must be positive"):
            RetryPolicy(max_delay=0)
        with pytest.raises(ValueError, match="max_delay must be positive"):
            RetryPolicy(max_delay=-10)
    
    def test_invalid_initial_delay(self) -> None:
        """Test that negative initial_delay raises ValueError."""
        with pytest.raises(ValueError, match="initial_delay must be non-negative"):
            RetryPolicy(initial_delay=-1)
    
    def test_invalid_retriable_exceptions(self) -> None:
        """Test that empty retriable_exceptions raises ValueError."""
        with pytest.raises(ValueError, match="retriable_exceptions cannot be empty"):
            RetryPolicy(retriable_exceptions=())
    
    def test_calculate_delay_first_attempt(self) -> None:
        """Test delay calculation for first retry attempt."""
        policy = RetryPolicy()
        assert policy.calculate_delay(0) == 1.0
    
    def test_calculate_delay_exponential_growth(self) -> None:
        """Test that delay grows exponentially with attempts."""
        policy = RetryPolicy()
        assert policy.calculate_delay(0) == 1.0
        assert policy.calculate_delay(1) == 2.0
        assert policy.calculate_delay(2) == 4.0
        assert policy.calculate_delay(3) == 8.0
        assert policy.calculate_delay(4) == 16.0
    
    def test_calculate_delay_with_custom_backoff(self) -> None:
        """Test delay calculation with custom backoff factor."""
        policy = RetryPolicy(backoff_factor=3.0)
        assert policy.calculate_delay(0) == 1.0
        assert policy.calculate_delay(1) == 3.0
        assert policy.calculate_delay(2) == 9.0
        assert policy.calculate_delay(3) == 27.0
    
    def test_calculate_delay_with_custom_initial_delay(self) -> None:
        """Test delay calculation with custom initial delay."""
        policy = RetryPolicy(initial_delay=2.0)
        assert policy.calculate_delay(0) == 2.0
        assert policy.calculate_delay(1) == 4.0
        assert policy.calculate_delay(2) == 8.0
    
    def test_calculate_delay_max_delay_cap(self) -> None:
        """Test that delay is capped at max_delay."""
        policy = RetryPolicy(max_delay=10.0)
        assert policy.calculate_delay(0) == 1.0
        assert policy.calculate_delay(1) == 2.0
        assert policy.calculate_delay(2) == 4.0
        assert policy.calculate_delay(3) == 8.0
        assert policy.calculate_delay(4) == 10.0  # Capped
        assert policy.calculate_delay(5) == 10.0  # Still capped
    
    def test_calculate_delay_negative_attempt(self) -> None:
        """Test that negative attempt raises ValueError."""
        policy = RetryPolicy()
        with pytest.raises(ValueError, match="attempt must be non-negative"):
            policy.calculate_delay(-1)
    
    def test_should_retry_within_limit(self) -> None:
        """Test that should_retry returns True when under max_retries."""
        policy = RetryPolicy(max_retries=3)
        assert policy.should_retry(ValueError("test"), 0) is True
        assert policy.should_retry(ValueError("test"), 1) is True
        assert policy.should_retry(ValueError("test"), 2) is True
    
    def test_should_retry_exceeds_limit(self) -> None:
        """Test that should_retry returns False when at or over max_retries."""
        policy = RetryPolicy(max_retries=3)
        assert policy.should_retry(ValueError("test"), 3) is False
        assert policy.should_retry(ValueError("test"), 4) is False
    
    def test_should_retry_retriable_exception(self) -> None:
        """Test that retriable exceptions return True."""
        policy = RetryPolicy(retriable_exceptions=(ValueError, KeyError))
        assert policy.should_retry(ValueError("test"), 0) is True
        assert policy.should_retry(KeyError("test"), 0) is True
    
    def test_should_retry_non_retriable_exception(self) -> None:
        """Test that non-retriable exceptions return False."""
        policy = RetryPolicy(retriable_exceptions=(ValueError, KeyError))
        assert policy.should_retry(RuntimeError("test"), 0) is False
        assert policy.should_retry(TypeError("test"), 0) is False


class TestWithRetry:
    """Test cases for @with_retry decorator."""
    
    def test_successful_execution_no_retry(self) -> None:
        """Test that successful function executes without retry."""
        call_count = 0
        
        @with_retry()
        def successful_func() -> int:
            nonlocal call_count
            call_count += 1
            return 42
        
        result = successful_func()
        assert result == 42
        assert call_count == 1
    
    def test_retry_on_failure(self) -> None:
        """Test that function is retried on failure."""
        call_count = 0
        
        @with_retry(RetryPolicy(max_retries=3, initial_delay=0.01))
        def failing_func() -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return 42
        
        result = failing_func()
        assert result == 42
        assert call_count == 3
    
    def test_exhaust_retries(self) -> None:
        """Test that function raises exception after exhausting retries."""
        call_count = 0
        
        @with_retry(RetryPolicy(max_retries=2, initial_delay=0.01))
        def always_failing_func() -> int:
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError, match="Always fails"):
            always_failing_func()
        
        # Should have initial attempt + 2 retries = 3 total calls
        assert call_count == 3
    
    def test_non_retriable_exception(self) -> None:
        """Test that non-retriable exceptions are not retried."""
        call_count = 0
        
        @with_retry(
            RetryPolicy(
                max_retries=3,
                initial_delay=0.01,
                retriable_exceptions=(ValueError,),
            )
        )
        def mixed_failure_func() -> int:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TypeError("Non-retriable")
            raise ValueError("Retriable")
        
        with pytest.raises(TypeError, match="Non-retriable"):
            mixed_failure_func()
        
        # Should only be called once (no retries for TypeError)
        assert call_count == 1
    
    def test_on_retry_callback(self) -> None:
        """Test that on_retry callback is called."""
        call_count = 0
        retry_count = 0
        
        def retry_callback(exc: Exception, attempt: int) -> None:
            nonlocal retry_count
            retry_count += 1
            assert isinstance(exc, ValueError)
            assert attempt in [0, 1]
        
        @with_retry(
            RetryPolicy(max_retries=3, initial_delay=0.01),
            on_retry=retry_callback,
        )
        def failing_func() -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return 42
        
        result = failing_func()
        assert result == 42
        assert call_count == 3
        assert retry_count == 2  # Callback called before each retry
    
    def test_preserves_function_metadata(self) -> None:
        """Test that decorator preserves function metadata."""
        @with_retry()
        def sample_func(x: int, y: int) -> int:
            """Sample function docstring."""
            return x + y
        
        assert sample_func.__name__ == "sample_func"
        assert sample_func.__doc__ == "Sample function docstring."
        assert sample_func(1, 2) == 3
    
    def test_exponential_backoff_timing(self) -> None:
        """Test that exponential backoff delays are correct."""
        call_times = []
        
        @with_retry(
            RetryPolicy(
                max_retries=3,
                backoff_factor=2.0,
                initial_delay=0.1,
            ),
        )
        def failing_func() -> None:
            call_times.append(time.time())
            if len(call_times) < 4:
                raise ValueError("Fail")
        
        with pytest.raises(ValueError):
            failing_func()
        
        # Check delays between calls
        assert len(call_times) == 4
        delays = [
            call_times[i + 1] - call_times[i]
            for i in range(len(call_times) - 1)
        ]
        
        # Allow some tolerance for timing
        assert delays[0] >= pytest.approx(0.1, abs=0.05)  # ~100ms
        assert delays[1] >= pytest.approx(0.2, abs=0.05)  # ~200ms
        assert delays[2] >= pytest.approx(0.4, abs=0.05)  # ~400ms


class TestWithRetryError:
    """Test cases for @with_retry_error decorator."""
    
    def test_successful_execution(self) -> None:
        """Test that successful function executes normally."""
        @with_retry_error()
        def successful_func() -> int:
            return 42
        
        result = successful_func()
        assert result == 42
    
    def test_raises_retry_error_on_exhaustion(self) -> None:
        """Test that RetryError is raised when retries are exhausted."""
        call_count = 0
        
        @with_retry_error(
            RetryPolicy(max_retries=2, initial_delay=0.01),
        )
        def always_failing_func() -> int:
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")
        
        with pytest.raises(RetryError) as exc_info:
            always_failing_func()
        
        assert call_count == 3  # Initial + 2 retries
        error = exc_info.value
        assert isinstance(error.original_exception, ValueError)
        assert error.attempt == 3  # Total attempts made
    
    def test_retry_error_message(self) -> None:
        """Test that RetryError has informative message."""
        @with_retry_error(RetryPolicy(max_retries=1, initial_delay=0.01))
        def failing_func() -> None:
            raise RuntimeError("Test error")
        
        with pytest.raises(RetryError) as exc_info:
            failing_func()
        
        error_str = str(exc_info.value)
        assert "failing_func" in error_str
        assert "RuntimeError" in error_str
        assert "Test error" in error_str
        assert "Attempts made" in error_str
    
    def test_on_retry_callback(self) -> None:
        """Test that on_retry callback works with RetryError variant."""
        retry_count = 0
        
        def retry_callback(exc: Exception, attempt: int) -> None:
            nonlocal retry_count
            retry_count += 1
        
        @with_retry_error(
            RetryPolicy(max_retries=2, initial_delay=0.01),
            on_retry=retry_callback,
        )
        def always_failing_func() -> None:
            raise ValueError("Fail")
        
        with pytest.raises(RetryError):
            always_failing_func()
        
        assert retry_count == 2  # Called before each retry


class TestRetryError:
    """Test cases for RetryError class."""
    
    def test_retry_error_creation(self) -> None:
        """Test that RetryError can be created with proper arguments."""
        original_exc = ValueError("Original error")
        error = RetryError("Failed after retries", original_exc, 3)
        
        assert str(error) == "Failed after retries"
        assert error.original_exception is original_exc
        assert error.attempt == 3
    
    def test_retry_error_string_representation(self) -> None:
        """Test that RetryError string representation is informative."""
        original_exc = RuntimeError("Something went wrong")
        error = RetryError("Operation failed", original_exc, 5)
        
        error_str = str(error)
        assert "Operation failed" in error_str
        assert "RuntimeError" in error_str
        assert "Something went wrong" in error_str
        assert "Attempts made: 5" in error_str
    
    def test_retry_error_is_exception(self) -> None:
        """Test that RetryError is an Exception subclass."""
        error = RetryError("Test", ValueError(), 1)
        assert isinstance(error, Exception)
        assert isinstance(error, RetryError)