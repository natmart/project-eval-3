"""
Retry Policy Implementation

This module provides retry logic with exponential backoff for retrying failed operations.
"""

import time
import functools
import math
from typing import Callable, Optional, Type, Any, Tuple
from dataclasses import dataclass


class RetryError(Exception):
    """Exception raised when all retry attempts are exhausted."""
    pass


@dataclass
class RetryPolicy:
    """
    Configuration for retry behavior with exponential backoff.
    
    Attributes:
        max_attempts: Maximum number of retry attempts (including initial attempt)
        base_delay: Base delay in seconds between retries
        max_delay: Maximum delay in seconds (caps exponential backoff)
        exponential_base: Base for exponential backoff calculation (default: 2)
        jitter: Whether to add random jitter to delays (default: False)
    """
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = False
    
    def __post_init__(self):
        """Validate retry policy parameters."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be non-negative")
        if self.max_delay < 0:
            raise ValueError("max_delay must be non-negative")
        if self.exponential_base < 1:
            raise ValueError("exponential_base must be at least 1")
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given retry attempt using exponential backoff.
        
        Args:
            attempt: The attempt number (0-indexed, where 0 is first retry)
            
        Returns:
            Delay in seconds, capped at max_delay
        """
        if attempt < 0:
            raise ValueError("attempt must be non-negative")
        
        # Calculate exponential backoff: base_delay * (exponential_base ^ attempt)
        exponential_delay = self.base_delay * (self.exponential_base ** attempt)
        
        # Cap at max_delay
        delay = min(exponential_delay, self.max_delay)
        
        # Add jitter if enabled
        if self.jitter:
            # Add random jitter up to 25% of the delay
            import random
            jitter_amount = delay * 0.25 * random.random()
            delay += jitter_amount
        
        return delay
    
    def should_retry(self, attempt: int, error: Optional[Exception] = None) -> bool:
        """
        Determine if another retry should be attempted.
        
        Args:
            attempt: The current attempt number (0-indexed, 0 is first attempt)
            error: The exception that caused the failure (optional)
            
        Returns:
            True if should retry, False otherwise
        """
        if attempt < 0:
            raise ValueError("attempt must be non-negative")
        return attempt < self.max_attempts - 1


def with_retry(
    policy: Optional[RetryPolicy] = None,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """
    Decorator that adds retry logic with exponential backoff to a function.
    
    Args:
        policy: RetryPolicy instance (default: RetryPolicy with defaults)
        exceptions: Tuple of exception types to catch and retry on (default: all Exceptions)
        on_retry: Optional callback called before each retry with (exception, attempt_number)
        
    Returns:
        Decorated function with retry logic
        
    Example:
        @with_retry(max_attempts=3, base_delay=1.0)
        def fetch_data():
            # Some operation that might fail
            pass
    """
    if policy is None:
        policy = RetryPolicy()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            
            for attempt in range(policy.max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    
                    # Check if we should retry
                    if not policy.should_retry(attempt, e):
                        raise RetryError(
                            f"Function {func.__name__} failed after {attempt + 1} attempts"
                        ) from e
                    
                    # Calculate delay and wait before retry
                    delay = policy.calculate_delay(attempt)
                    
                    # Call on_retry callback if provided
                    if on_retry:
                        on_retry(e, attempt + 1)
                    
                    time.sleep(delay)
            
            # This should not be reached, but just in case
            raise RetryError(
                f"Function {func.__name__} failed after {policy.max_attempts} attempts"
            ) from last_error
        
        return wrapper
    
    return decorator