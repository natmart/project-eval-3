"""
Retry Policy Implementation

This module provides retry logic with exponential backoff for task handlers.
"""

import time
import functools
import inspect
from typing import Callable, Type, Tuple, Any, Optional, TypeVar
from dataclasses import dataclass


T = TypeVar('T')


@dataclass
class RetryPolicy:
    """
    Configuration for retry behavior with exponential backoff.
    
    Attributes:
        max_retries: Maximum number of retry attempts (default: 3)
        backoff_factor: Multiplier for exponential backoff (default: 2.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        initial_delay: Initial delay before first retry in seconds (default: 1.0)
        retriable_exceptions: Tuple of exception types that should trigger retries
                            (default: all Exception types)
    """
    
    max_retries: int = 3
    backoff_factor: float = 2.0
    max_delay: float = 60.0
    initial_delay: float = 1.0
    retriable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    
    def __post_init__(self) -> None:
        """Validate the retry policy parameters."""
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.backoff_factor < 1.0:
            raise ValueError("backoff_factor must be >= 1.0")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be positive")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if not self.retriable_exceptions:
            raise ValueError("retriable_exceptions cannot be empty")
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay for a given retry attempt using exponential backoff.
        
        The formula is: min(initial_delay * (backoff_factor ** attempt), max_delay)
        
        Args:
            attempt: The current retry attempt number (0-indexed)
            
        Returns:
            The calculated delay in seconds
            
        Examples:
            >>> policy = RetryPolicy()
            >>> policy.calculate_delay(0)  # First retry
            1.0
            >>> policy.calculate_delay(1)  # Second retry
            2.0
            >>> policy.calculate_delay(2)  # Third retry
            4.0
        """
        if attempt < 0:
            raise ValueError("attempt must be non-negative")
        
        # Calculate exponential backoff: initial_delay * (backoff_factor ** attempt)
        delay = self.initial_delay * (self.backoff_factor ** attempt)
        
        # Cap the delay at max_delay
        return min(delay, self.max_delay)
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """
        Determine if a failed operation should be retried.
        
        Args:
            exception: The exception that was raised
            attempt: The current retry attempt number (0-indexed)
            
        Returns:
            True if the operation should be retried, False otherwise
        """
        # Check if we've exceeded max retries
        if attempt >= self.max_retries:
            return False
        
        # Check if the exception is retriable
        return isinstance(exception, self.retriable_exceptions)


def with_retry(
    policy: Optional[RetryPolicy] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that wraps a function with retry logic using exponential backoff.
    
    Args:
        policy: The retry policy to use. If None, uses default RetryPolicy().
        on_retry: Optional callback function called before each retry attempt.
                 Receives the exception and attempt number as arguments.
    
    Returns:
        A decorator function that wraps the original function with retry logic
    
    Examples:
        >>> @with_retry()
        ... def fetch_data(url):
        ...     # May fail and will be retried
        ...     return requests.get(url)
        
        >>> custom_policy = RetryPolicy(max_retries=5, backoff_factor=1.5)
        >>> @with_retry(policy=custom_policy)
        ... def process_item(item):
        ...     # Custom retry configuration
        ...     return complex_operation(item)
        
        >>> def log_retry(exc, attempt):
        ...     print(f"Attempt {attempt + 1} failed: {exc}, retrying...")
        >>> @with_retry(on_retry=log_retry)
        ... def unstable_task():
        ...     # Will log each retry attempt
        ...     return risky_operation()
    """
    
    if policy is None:
        policy = RetryPolicy()
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            last_exception = None
            
            while True:
                try:
                    # Try to execute the function
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if not policy.should_retry(e, attempt):
                        # We've exhausted retries or exception is not retriable
                        break
                    
                    # Calculate delay before next retry
                    delay = policy.calculate_delay(attempt)
                    
                    # Call the retry callback if provided
                    if on_retry is not None:
                        on_retry(e, attempt)
                    
                    # Wait before retrying
                    time.sleep(delay)
                    
                    # Increment attempt counter
                    attempt += 1
            
            # If we get here, all retries have been exhausted
            # Re-raise the last exception
            assert last_exception is not None  # For type checker
            raise last_exception
        
        return wrapper
    
    return decorator


class RetryError(Exception):
    """
    Exception raised when all retry attempts have been exhausted.
    
    This exception wraps the original exception that caused the failure
    and provides context about the retry attempts.
    """
    
    def __init__(
        self,
        message: str,
        original_exception: Exception,
        attempt: int,
    ) -> None:
        """
        Initialize a RetryError.
        
        Args:
            message: Error message
            original_exception: The original exception that caused the failure
            attempt: The number of retry attempts that were made
        """
        super().__init__(message)
        self.original_exception = original_exception
        self.attempt = attempt
    
    def __str__(self) -> str:
        return (
            f"{super().__str__()}\n"
            f"  Original exception: {type(self.original_exception).__name__}: {self.original_exception}\n"
            f"  Attempts made: {self.attempt}"
        )


def with_retry_error(
    policy: Optional[RetryPolicy] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator similar to @with_retry, but raises RetryError on exhaustion.
    
    This variant wraps the final exception in a RetryError for better error
    handling and context preservation.
    
    Args:
        policy: The retry policy to use. If None, uses default RetryPolicy().
        on_retry: Optional callback function called before each retry attempt.
    
    Returns:
        A decorator function that wraps the original function with retry logic
    
    Examples:
        >>> @with_retry_error()
        ... def unreliable_operation():
        ...     return risky_call()
        
        >>> try:
        ...     unreliable_operation()
        ... except RetryError as e:
        ...     print(f"Failed after {e.attempt} attempts")
        ...     print(f"Original error: {e.original_exception}")
    """
    
    if policy is None:
        policy = RetryPolicy()
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            last_exception = None
            
            while True:
                try:
                    # Try to execute the function
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    
                    # Check if we should retry
                    if not policy.should_retry(e, attempt):
                        # We've exhausted retries or exception is not retriable
                        break
                    
                    # Calculate delay before next retry
                    delay = policy.calculate_delay(attempt)
                    
                    # Call the retry callback if provided
                    if on_retry is not None:
                        on_retry(e, attempt)
                    
                    # Wait before retrying
                    time.sleep(delay)
                    
                    # Increment attempt counter
                    attempt += 1
            
            # If we get here, all retries have been exhausted
            # Raise RetryError with context
            assert last_exception is not None  # For type checker
            raise RetryError(
                message=f"Function '{func.__name__}' failed after {attempt} attempts",
                original_exception=last_exception,
                attempt=attempt,
            )
        
        return wrapper
    
    return decorator