#!/usr / bin / env python3
"""
Robust Error Handling Framework for YouTube ETL Pipeline

This module provides:
- Custom exception classes with clear error messages
- Centralized error handling with fail-fast principles
- Retry mechanisms with exponential backoff
- Comprehensive error logging and context tracking
- Pydantic-based error models for structured error handling

Design Principles:
- Fail loudly with clear, actionable error messages
- Provide full context for debugging
- Use natural keys and meaningful error codes
- Never silently ignore errors
"""

import functools
import logging
import time
import traceback
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from pydantic import BaseModel, Field, validator
from sqlalchemy.exc import SQLAlchemyError

# Type variable for retry decorator
F = TypeVar("F", bound=Callable[..., Any])


class ErrorSeverity(str, Enum):
    """Error severity levels for classification and handling."""

    CRITICAL = "CRITICAL"  # Stop pipeline execution immediately
    HIGH = "HIGH"  # Retry with backoff, then fail
    MEDIUM = "MEDIUM"  # Log and continue with degraded functionality
    LOW = "LOW"  # Log for monitoring, continue normally


class ErrorCategory(str, Enum):
    """Error categories for better classification and handling."""

    DATABASE = "DATABASE"
    API = "API"
    VALIDATION = "VALIDATION"
    CONFIGURATION = "CONFIGURATION"
    DATA_QUALITY = "DATA_QUALITY"
    PROCESSING = "PROCESSING"
    NETWORK = "NETWORK"
    TIMEOUT = "TIMEOUT"


class ErrorContext(BaseModel):
    """Structured error context for debugging and monitoring."""

    timestamp: datetime = Field(default_factory=datetime.utcnow)
    component: str = Field(..., description="Component where error occurred")
    operation: str = Field(..., description="Operation being performed")
    user_data: Dict[str, Any] = Field(default_factory=dict, description="Relevant data context")
    system_state: Dict[str, Any] = Field(default_factory=dict, description="System state at error time")

    @validator("component")
    def validate_component(cls, v):
        if not v or not v.strip():
            raise ValueError("Component name cannot be empty")
        return v.strip()

    @validator("operation")
    def validate_operation(cls, v):
        if not v or not v.strip():
            raise ValueError("Operation name cannot be empty")
        return v.strip()


class ETLError(Exception):
    """Base exception class for all ETL-related errors."""

    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        category: ErrorCategory = ErrorCategory.PROCESSING,
        context: Optional[ErrorContext] = None,
        original_error: Optional[Exception] = None,
    ):
        self.message = message
        self.severity = severity
        self.category = category
        self.context = context or ErrorContext(component="unknown", operation="unknown")
        self.original_error = original_error
        self.error_id = f"{category.value}_{int(time.time())}"

        # Create comprehensive error message
        full_message = f"[{self.error_id}] {severity.value}: {message}"
        if context:
            full_message += f" (Component: {context.component}, Operation: {context.operation})"
        if original_error:
            full_message += f" | Original: {str(original_error)}"

        super().__init__(full_message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging and monitoring."""
        return {
            "error_id": self.error_id,
            "message": self.message,
            "severity": self.severity.value,
            "category": self.category.value,
            "context": self.context.dict() if self.context else None,
            "original_error": str(self.original_error) if self.original_error else None,
            "traceback": traceback.format_exc(),
        }


class DatabaseError(ETLError):
    """Database-related errors with connection and query context."""

    def __init__(self, message: str, query: Optional[str] = None, **kwargs):
        self.query = query
        context = kwargs.get("context")
        if context and query:
            context.user_data["query"] = query[:500]  # Truncate long queries
        super().__init__(message, category=ErrorCategory.DATABASE, **kwargs)


class APIError(ETLError):
    """API-related errors with request / response context."""

    def __init__(self, message: str, status_code: Optional[int] = None, endpoint: Optional[str] = None, **kwargs):
        self.status_code = status_code
        self.endpoint = endpoint
        context = kwargs.get("context")
        if context:
            if status_code:
                context.user_data["status_code"] = status_code
            if endpoint:
                context.user_data["endpoint"] = endpoint
        super().__init__(message, category=ErrorCategory.API, **kwargs)


class ValidationError(ETLError):
    """Data validation errors with field-level details."""

    def __init__(self, message: str, field: Optional[str] = None, value: Any = None, **kwargs):
        self.field = field
        self.value = value
        context = kwargs.get("context")
        if context:
            if field:
                context.user_data["field"] = field
            if value is not None:
                context.user_data["invalid_value"] = str(value)[:200]  # Truncate long values
        super().__init__(message, category=ErrorCategory.VALIDATION, **kwargs)


class ConfigurationError(ETLError):
    """Configuration-related errors."""

    def __init__(self, message: str, config_key: Optional[str] = None, **kwargs):
        self.config_key = config_key
        context = kwargs.get("context")
        if context and config_key:
            context.user_data["config_key"] = config_key
        super().__init__(message, severity=ErrorSeverity.CRITICAL, category=ErrorCategory.CONFIGURATION, **kwargs)


class DataQualityError(ETLError):
    """Data quality issues that may not stop processing."""

    def __init__(
        self, message: str, affected_records: int = 0, severity: ErrorSeverity = ErrorSeverity.MEDIUM, **kwargs
    ):
        self.affected_records = affected_records
        context = kwargs.get("context")
        if context:
            context.user_data["affected_records"] = affected_records
        super().__init__(message, severity=severity, category=ErrorCategory.DATA_QUALITY, **kwargs)


class ErrorHandler:
    """Centralized error handling with logging and retry mechanisms."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.error_counts: Dict[str, int] = {}

    def handle_error(
        self, error: Union[Exception, ETLError], context: Optional[ErrorContext] = None, should_raise: bool = True
    ) -> None:
        """
        Handle an error with appropriate logging and actions.

        Args:
            error: The error to handle
            context: Additional context for the error
            should_raise: Whether to re-raise the error after handling
        """
        # Convert to ETLError if needed
        if not isinstance(error, ETLError):
            etl_error = ETLError(message=str(error), context=context, original_error=error)
        else:
            etl_error = error
            if context and not etl_error.context:
                etl_error.context = context

        # Log the error (avoid 'message' key conflict with logging)
        error_dict = etl_error.to_dict()
        # Remove 'message' key to avoid logging conflict
        log_extra = {k: v for k, v in error_dict.items() if k != "message"}

        if etl_error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(f"CRITICAL ERROR: {etl_error.message}", extra=log_extra)
        elif etl_error.severity == ErrorSeverity.HIGH:
            self.logger.error(f"HIGH SEVERITY ERROR: {etl_error.message}", extra=log_extra)
        elif etl_error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(f"MEDIUM SEVERITY ERROR: {etl_error.message}", extra=log_extra)
        else:
            self.logger.info(f"LOW SEVERITY ERROR: {etl_error.message}", extra=log_extra)

        # Track error counts for monitoring
        error_key = f"{etl_error.category.value}_{etl_error.severity.value}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1

        # Re-raise if requested and severity warrants it
        if should_raise and etl_error.severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH]:
            raise etl_error

    def get_error_summary(self) -> Dict[str, int]:
        """Get summary of error counts for monitoring."""
        return self.error_counts.copy()

    def reset_error_counts(self) -> None:
        """Reset error counts (useful for testing)."""
        self.error_counts.clear()


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Multiplier for delay after each retry
        exceptions: Tuple of exception types to retry on
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = base_delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        # Final attempt failed, raise with context
                        context = ErrorContext(
                            component=func.__module__,
                            operation=func.__name__,
                            user_data={
                                "attempt": attempt + 1,
                                "max_retries": max_retries,
                                "args": str(args)[:200],
                                "kwargs": str(kwargs)[:200],
                            },
                        )

                        if isinstance(e, SQLAlchemyError):
                            raise DatabaseError(
                                f"Database operation failed after {max_retries} retries: {str(e)}",
                                context=context,
                                original_error=e,
                            )
                        else:
                            raise ETLError(
                                f"Operation failed after {max_retries} retries: {str(e)}",
                                context=context,
                                original_error=e,
                            )

                    # Wait before retry
                    time.sleep(min(delay, max_delay))
                    delay *= backoff_factor

                    logging.getLogger(__name__).warning(
                        f"Retry attempt {attempt + 1}/{max_retries} for {func.__name__}: {str(e)}"
                    )

            # This should never be reached, but just in case
            raise last_exception

        return wrapper

    return decorator


def validate_required_config(config_dict: Dict[str, Any], required_keys: List[str]) -> None:
    """
    Validate that required configuration keys are present and not empty.

    Args:
        config_dict: Configuration dictionary to validate
        required_keys: List of required configuration keys

    Raises:
        ConfigurationError: If any required keys are missing or empty
    """
    missing_keys = []
    empty_keys = []

    for key in required_keys:
        if key not in config_dict:
            missing_keys.append(key)
        elif not config_dict[key] or (isinstance(config_dict[key], str) and not config_dict[key].strip()):
            empty_keys.append(key)

    if missing_keys or empty_keys:
        error_parts = []
        if missing_keys:
            error_parts.append(f"Missing keys: {', '.join(missing_keys)}")
        if empty_keys:
            error_parts.append(f"Empty keys: {', '.join(empty_keys)}")

        raise ConfigurationError(
            f"Configuration validation failed: {'; '.join(error_parts)}",
            context=ErrorContext(
                component="configuration",
                operation="validate_required_config",
                user_data={"missing_keys": missing_keys, "empty_keys": empty_keys, "required_keys": required_keys},
            ),
        )


# Global error handler instance
_global_error_handler = ErrorHandler()


def get_error_handler() -> ErrorHandler:
    """Get the global error handler instance."""
    return _global_error_handler


def setup_error_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Set up structured logging for error handling.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("etl_error_handler")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Create formatter for structured logging
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


class PerformanceLogger:
    """Logger for performance metrics and system monitoring."""

    def __init__(self, logger_name: str = "etl_performance"):
        self.logger = logging.getLogger(logger_name)
        self.metrics: Dict[str, List[float]] = {}

    def log_operation_time(self, operation: str, duration_seconds: float, context: Optional[Dict[str, Any]] = None):
        """Log operation timing with context."""
        if operation not in self.metrics:
            self.metrics[operation] = []
        self.metrics[operation].append(duration_seconds)

        context_str = f" | Context: {context}" if context else ""
        self.logger.info(f"⏱️ {operation}: {duration_seconds:.3f}s{context_str}")

    def log_memory_usage(self, operation: str, memory_mb: float):
        """Log memory usage for operations."""
        self.logger.info(f"🧠 {operation}: {memory_mb:.1f} MB")

    def log_throughput(self, operation: str, items_processed: int, duration_seconds: float):
        """Log throughput metrics."""
        rate = items_processed / duration_seconds if duration_seconds > 0 else 0
        self.logger.info(
            f"📊 {operation}: {items_processed:,} items in {duration_seconds:.1f}s ({rate:.1f} items / sec)"
        )

    def get_performance_summary(self) -> Dict[str, Dict[str, float]]:
        """Get performance summary statistics."""
        summary = {}
        for operation, times in self.metrics.items():
            if times:
                summary[operation] = {
                    "count": len(times),
                    "total_time": sum(times),
                    "avg_time": sum(times) / len(times),
                    "min_time": min(times),
                    "max_time": max(times),
                }
        return summary


class StructuredLogger:
    """Enhanced structured logging with JSON output and context tracking."""

    def __init__(self, component: str, enable_json: bool = False):
        self.component = component
        self.logger = logging.getLogger(f"etl.{component}")
        self.enable_json = enable_json

    def log_with_context(
        self,
        level: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        performance_data: Optional[Dict[str, Any]] = None,
    ):
        """Log message with structured context and performance data."""
        log_data = {"component": self.component, "timestamp": datetime.utcnow().isoformat(), "message": message}

        if context:
            log_data["context"] = context

        if performance_data:
            log_data["performance"] = performance_data

        if self.enable_json:
            import json

            log_message = json.dumps(log_data)
        else:
            # Human-readable format
            context_str = f" | Context: {context}" if context else ""
            perf_str = f" | Performance: {performance_data}" if performance_data else ""
            log_message = f"[{self.component}] {message}{context_str}{perf_str}"

        getattr(self.logger, level.lower())(log_message)

    def info(self, message: str, **kwargs):
        """Log info message with context."""
        self.log_with_context("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with context."""
        self.log_with_context("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with context."""
        self.log_with_context("ERROR", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with context."""
        self.log_with_context("DEBUG", message, **kwargs)
