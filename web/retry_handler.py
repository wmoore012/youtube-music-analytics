#!/usr / bin / env python3
"""
Retry Handler with Exponential Backoff

This module provides robust retry mechanisms for database operations and API calls
with intelligent backoff strategies and comprehensive error handling.
"""

import functools
import logging
import random
import time
from typing import Any, Callable, List, Optional, Type, TypeVar

from sqlalchemy.exc import (
    DisconnectionError,
    OperationalError,
    SQLAlchemyError,
)
from sqlalchemy.exc import TimeoutError as SQLTimeoutError

from .error_handling import (
    DatabaseError,
    ErrorContext,
    ErrorSeverity,
    ETLError,
)

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or [
            OperationalError,
            DisconnectionError,
            SQLTimeoutError,
            ConnectionError,
            TimeoutError,
        ]


class RetryHandler:
    """Handles retry logic with exponential backoff and jitter."""

    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for the given attempt number.

        Args:
            attempt: Current attempt number (0-based)

        Returns:
            Delay in seconds
        """
        # Exponential backoff: base_delay * (exponential_base ^ attempt)
        delay = self.config.base_delay * (self.config.exponential_base**attempt)

        # Cap at max_delay
        delay = min(delay, self.config.max_delay)

        # Add jitter to prevent thundering herd
        if self.config.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    def is_retryable_exception(self, exception: Exception) -> bool:
        """
        Check if an exception is retryable.

        Args:
            exception: Exception to check

        Returns:
            True if exception should be retried
        """
        return any(isinstance(exception, exc_type) for exc_type in self.config.retryable_exceptions)

    def retry_operation(
        self,
        operation: Callable[..., Any],
        operation_name: str,
        context: Optional[ErrorContext] = None,
        *args,
        **kwargs,
    ) -> Any:
        """
        Execute operation with retry logic.

        Args:
            operation: Function to execute
            operation_name: Name of operation for logging
            context: Error context for debugging
            *args: Arguments to pass to operation
            **kwargs: Keyword arguments to pass to operation

        Returns:
            Result of successful operation

        Raises:
            ETLError: If all retry attempts fail
        """
        if not context:
            context = ErrorContext(
                component="RetryHandler", operation=operation_name, user_data={"max_attempts": self.config.max_attempts}
            )

        last_exception = None

        for attempt in range(self.config.max_attempts):
            try:
                self.logger.debug(f"Attempting {operation_name} (attempt {attempt + 1}/{self.config.max_attempts})")
                result = operation(*args, **kwargs)

                if attempt > 0:
                    self.logger.info(f"Operation {operation_name} succeeded on attempt {attempt + 1}")

                return result

            except Exception as e:
                last_exception = e

                # Check if this is the last attempt
                if attempt == self.config.max_attempts-1:
                    break

                # Check if exception is retryable
                if not self.is_retryable_exception(e):
                    self.logger.error(f"Non-retryable exception in {operation_name}: {str(e)}")
                    break

                # Calculate delay and wait
                delay = self.calculate_delay(attempt)
                self.logger.warning(
                    f"Attempt {attempt + 1} of {operation_name} failed: {str(e)}. "
                    f"Retrying in {delay:.2f} seconds..."
                )
                time.sleep(delay)

        # All attempts failed
        if isinstance(last_exception, SQLAlchemyError):
            raise DatabaseError(
                f"Database operation '{operation_name}' failed after {self.config.max_attempts} attempts",
                context=context,
                original_error=last_exception,
                severity=ErrorSeverity.HIGH,
            )
        else:
            raise ETLError(
                f"Operation '{operation_name}' failed after {self.config.max_attempts} attempts",
                context=context,
                original_error=last_exception,
                severity=ErrorSeverity.HIGH,
            )


def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Optional[List[Type[Exception]]] = None,
) -> Callable[[F], F]:
    """
    Decorator for adding retry logic with exponential backoff to functions.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        retryable_exceptions: List of exception types that should trigger retries

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(max_attempts=5, base_delay=2.0)
        def fetch_data_from_api():
            # This function will be retried up to 5 times with exponential backoff
            return api_client.get_data()
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
        jitter=jitter,
        retryable_exceptions=retryable_exceptions,
    )

    retry_handler = RetryHandler(config)

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            context = ErrorContext(
                component=func.__module__,
                operation=func.__name__,
                user_data={"function": f"{func.__module__}.{func.__name__}"},
            )

            return retry_handler.retry_operation(func, func.__name__, context, *args, **kwargs)

        return wrapper

    return decorator


def retry_database_operation(
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> Callable[[F], F]:
    """
    Specialized decorator for database operations with appropriate retry settings.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds

    Returns:
        Decorated function with database-specific retry logic
    """
    return retry_with_backoff(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=30.0,  # Shorter max delay for database operations
        retryable_exceptions=[
            OperationalError,
            DisconnectionError,
            SQLTimeoutError,
            ConnectionError,
        ],
    )


def retry_api_operation(
    max_attempts: int = 5,
    base_delay: float = 2.0,
) -> Callable[[F], F]:
    """
    Specialized decorator for API operations with appropriate retry settings.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds

    Returns:
        Decorated function with API-specific retry logic
    """
    return retry_with_backoff(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=120.0,  # Longer max delay for API operations
        retryable_exceptions=[
            ConnectionError,
            TimeoutError,
            # Add HTTP-specific exceptions if using requests
        ],
    )


# Progress tracking for long-running operations
class ProgressTracker:
    """Tracks progress of long-running operations with logging."""

    def __init__(self, operation_name: str, total_items: int, log_interval: int = 100):
        self.operation_name = operation_name
        self.total_items = total_items
        self.log_interval = log_interval
        self.processed_items = 0
        self.start_time = time.time()
        self.logger = logging.getLogger(f"{__name__}.ProgressTracker")

    def update(self, items_processed: int = 1) -> None:
        """
        Update progress and log if necessary.

        Args:
            items_processed: Number of items processed in this update
        """
        self.processed_items += items_processed

        # Log progress at intervals
        if self.processed_items % self.log_interval == 0 or self.processed_items == self.total_items:
            elapsed_time = time.time() - self.start_time
            percentage = (self.processed_items / self.total_items) * 100
            rate = self.processed_items / elapsed_time if elapsed_time > 0 else 0

            if self.processed_items < self.total_items:
                # Estimate remaining time
                remaining_items = self.total_items-self.processed_items
                eta_seconds = remaining_items / rate if rate > 0 else 0
                eta_str = f", ETA: {eta_seconds:.0f}s" if eta_seconds > 0 else ""

                self.logger.info(
                    f"{self.operation_name}: {self.processed_items:,}/{self.total_items:,} "
                    f"({percentage:.1f}%) processed at {rate:.1f} items / sec{eta_str}"
                )
            else:
                self.logger.info(
                    f"{self.operation_name}: Completed {self.total_items:,} items "
                    f"in {elapsed_time:.1f}s ({rate:.1f} items / sec)"
                )

    def __enter__(self):
        self.logger.info(f"Starting {self.operation_name}: {self.total_items:,} items to process")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            elapsed_time = time.time() - self.start_time
            rate = self.processed_items / elapsed_time if elapsed_time > 0 else 0
            self.logger.info(
                f"Completed {self.operation_name}: {self.processed_items:,} items "
                f"in {elapsed_time:.1f}s ({rate:.1f} items / sec)"
            )
        else:
            self.logger.error(f"Failed {self.operation_name} after processing {self.processed_items:,} items")
