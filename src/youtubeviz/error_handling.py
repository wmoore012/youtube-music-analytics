"""Robust error handling framework for ETL pipeline operations."""

import logging
import random
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional


class ErrorSeverity(Enum):
    """Error severity levels for classification."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ErrorCategory(Enum):
    """Error categories for classification."""

    DATABASE = "database"
    NETWORK = "network"
    DATA_VALIDATION = "data_validation"
    PROCESSING = "processing"
    CONFIGURATION = "configuration"
    EXTERNAL_API = "external_api"
    SYSTEM = "system"


class ErrorAction(Enum):
    """Actions to take when errors occur."""

    STOP_PIPELINE = "stop_pipeline"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    LOG_AND_CONTINUE = "log_and_continue"
    SKIP_RECORD = "skip_record"
    FALLBACK_PROCESSING = "fallback_processing"


@dataclass
class ErrorContext:
    """Context information for error handling."""

    operation: str
    component: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None

    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to error context."""
        self.metadata[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """Convert error context to dictionary."""
        return {
            "operation": self.operation,
            "component": self.component,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "stack_trace": self.stack_trace,
        }


@dataclass
class ErrorRecord:
    """Record of an error occurrence."""

    error_type: str
    message: str
    severity: ErrorSeverity
    category: ErrorCategory
    action_taken: ErrorAction
    context: ErrorContext
    resolved: bool = False
    resolution_notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert error record to dictionary."""
        return {
            "error_type": self.error_type,
            "message": self.message,
            "severity": self.severity.value,
            "category": self.category.value,
            "action_taken": self.action_taken.value,
            "context": self.context.to_dict(),
            "resolved": self.resolved,
            "resolution_notes": self.resolution_notes,
        }


class ETLError(Exception):
    """Base exception for ETL operations."""

    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.PROCESSING,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[ErrorContext] = None,
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.context = context or ErrorContext(operation="unknown", component="unknown")


class CriticalError(ETLError):
    """Critical error that should stop pipeline execution."""

    def __init__(
        self, message: str, category: ErrorCategory = ErrorCategory.SYSTEM, context: Optional[ErrorContext] = None
    ):
        super().__init__(message, category, ErrorSeverity.CRITICAL, context)


class RecoverableError(ETLError):
    """Recoverable error that can be retried."""

    def __init__(
        self, message: str, category: ErrorCategory = ErrorCategory.NETWORK, context: Optional[ErrorContext] = None
    ):
        super().__init__(message, category, ErrorSeverity.HIGH, context)


class DataError(ETLError):
    """Data-related error that can be logged and processing continued."""

    def __init__(
        self,
        message: str,
        category: ErrorCategory = ErrorCategory.DATA_VALIDATION,
        context: Optional[ErrorContext] = None,
    ):
        super().__init__(message, category, ErrorSeverity.LOW, context)


class ErrorHandler:
    """Centralized error handling system with error classification and retry logic."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        """Initialize error handler.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff (seconds)
            max_delay: Maximum delay between retries (seconds)
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.error_records: List[ErrorRecord] = []
        self.logger = logging.getLogger(__name__)

        # Configure structured logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up structured logging for error handling."""
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def classify_error(self, error: Exception, context: Optional[ErrorContext] = None) -> ErrorAction:
        """Classify error and determine appropriate action.

        Args:
            error: The exception that occurred
            context: Additional context about the error

        Returns:
            ErrorAction: The action to take for this error
        """
        # Handle ETL-specific errors
        if isinstance(error, CriticalError):
            return ErrorAction.STOP_PIPELINE
        elif isinstance(error, RecoverableError):
            return ErrorAction.RETRY_WITH_BACKOFF
        elif isinstance(error, DataError):
            return ErrorAction.LOG_AND_CONTINUE

        # Handle standard Python exceptions
        elif isinstance(error, (ConnectionError, TimeoutError)):
            return ErrorAction.RETRY_WITH_BACKOFF
        elif isinstance(error, (ValueError, TypeError)) and context and "data" in context.operation.lower():
            return ErrorAction.SKIP_RECORD
        elif isinstance(error, (MemoryError, SystemError)):
            return ErrorAction.STOP_PIPELINE
        elif isinstance(error, (KeyError, AttributeError, IndexError)):
            return ErrorAction.LOG_AND_CONTINUE
        else:
            # Default action for unknown errors
            return ErrorAction.LOG_AND_CONTINUE

    def handle_error(self, error: Exception, context: Optional[ErrorContext] = None) -> ErrorAction:
        """Handle an error occurrence.

        Args:
            error: The exception that occurred
            context: Additional context about the error

        Returns:
            ErrorAction: The action taken for this error
        """
        # Enhance context with stack trace
        if context:
            context.stack_trace = traceback.format_exc()
        else:
            context = ErrorContext(operation="unknown", component="unknown", stack_trace=traceback.format_exc())

        # Classify error and determine action
        action = self.classify_error(error, context)

        # Determine error properties
        if isinstance(error, ETLError):
            severity = error.severity
            category = error.category
        else:
            severity = self._infer_severity(error, action)
            category = self._infer_category(error, context)

        # Create error record
        error_record = ErrorRecord(
            error_type=type(error).__name__,
            message=str(error),
            severity=severity,
            category=category,
            action_taken=action,
            context=context,
        )

        # Store error record
        self.error_records.append(error_record)

        # Log error with appropriate level
        self._log_error(error_record)

        return action

    def _infer_severity(self, error: Exception, action: ErrorAction) -> ErrorSeverity:
        """Infer error severity based on error type and action."""
        if action == ErrorAction.STOP_PIPELINE:
            return ErrorSeverity.CRITICAL
        elif action == ErrorAction.RETRY_WITH_BACKOFF:
            return ErrorSeverity.HIGH
        elif isinstance(error, (ValueError, TypeError, KeyError)):
            return ErrorSeverity.MEDIUM
        else:
            return ErrorSeverity.LOW

    def _infer_category(self, error: Exception, context: ErrorContext) -> ErrorCategory:
        """Infer error category based on error type and context."""
        if isinstance(error, (ConnectionError, TimeoutError)):
            return ErrorCategory.NETWORK
        elif "database" in context.operation.lower() or "db" in context.operation.lower():
            return ErrorCategory.DATABASE
        elif "api" in context.operation.lower():
            return ErrorCategory.EXTERNAL_API
        elif isinstance(error, (ValueError, TypeError)):
            return ErrorCategory.DATA_VALIDATION
        elif isinstance(error, (MemoryError, SystemError)):
            return ErrorCategory.SYSTEM
        else:
            return ErrorCategory.PROCESSING

    def _log_error(self, error_record: ErrorRecord) -> None:
        """Log error with appropriate level and structured information."""
        log_data = {
            "error_type": error_record.error_type,
            "message": error_record.message,
            "severity": error_record.severity.value,
            "category": error_record.category.value,
            "action": error_record.action_taken.value,
            "operation": error_record.context.operation,
            "component": error_record.context.component,
            "metadata": error_record.context.metadata,
        }

        log_message = f"[{error_record.severity.value.upper()}] {error_record.message}"

        if error_record.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(log_message, extra=log_data)
        elif error_record.severity == ErrorSeverity.HIGH:
            self.logger.error(log_message, extra=log_data)
        elif error_record.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(log_message, extra=log_data)
        else:
            self.logger.info(log_message, extra=log_data)

        # Log stack trace for critical and high severity errors
        if error_record.severity in [ErrorSeverity.CRITICAL, ErrorSeverity.HIGH] and error_record.context.stack_trace:
            self.logger.debug(f"Stack trace: {error_record.context.stack_trace}")

    def retry_with_backoff(
        self, func: Callable, *args, context: Optional[ErrorContext] = None, max_retries: Optional[int] = None, **kwargs
    ) -> Any:
        """Execute function with exponential backoff retry logic.

        Args:
            func: Function to execute
            *args: Positional arguments for function
            context: Error context for logging
            max_retries: Override default max retries
            **kwargs: Keyword arguments for function

        Returns:
            Result of successful function execution

        Raises:
            Exception: The last exception if all retries fail
        """
        max_attempts = max_retries or self.max_retries
        last_exception = None

        for attempt in range(max_attempts + 1):
            try:
                result = func(*args, **kwargs)

                # Log successful retry if this wasn't the first attempt
                if attempt > 0:
                    self.logger.info(f"Function {func.__name__} succeeded on attempt {attempt + 1}")

                return result

            except Exception as e:
                last_exception = e

                # Don't retry on the last attempt
                if attempt == max_attempts:
                    break

                # Calculate delay with exponential backoff and jitter
                delay = min(self.base_delay * (2**attempt) + random.uniform(0, 1), self.max_delay)

                # Log retry attempt
                retry_context = context or ErrorContext(operation=f"retry_{func.__name__}", component="error_handler")
                retry_context.add_metadata("attempt", attempt + 1)
                retry_context.add_metadata("max_attempts", max_attempts + 1)
                retry_context.add_metadata("delay_seconds", delay)

                self.logger.warning(
                    f"Attempt {attempt + 1}/{max_attempts + 1} failed for {func.__name__}: {e}. "
                    f"Retrying in {delay:.2f} seconds..."
                )

                time.sleep(delay)

        # All retries failed, handle the final error
        if last_exception:
            final_context = context or ErrorContext(
                operation=f"retry_failed_{func.__name__}", component="error_handler"
            )
            final_context.add_metadata("total_attempts", max_attempts + 1)

            action = self.handle_error(last_exception, final_context)

            # Re-raise the exception for critical errors
            if action == ErrorAction.STOP_PIPELINE:
                raise last_exception

        return None

    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors encountered.

        Returns:
            Dictionary containing error statistics and details
        """
        if not self.error_records:
            return {"total_errors": 0, "summary": "No errors recorded"}

        # Count errors by severity and category
        severity_counts = {}
        category_counts = {}
        action_counts = {}

        for record in self.error_records:
            severity_counts[record.severity.value] = severity_counts.get(record.severity.value, 0) + 1
            category_counts[record.category.value] = category_counts.get(record.category.value, 0) + 1
            action_counts[record.action_taken.value] = action_counts.get(record.action_taken.value, 0) + 1

        # Get recent errors (last 10)
        recent_errors = [record.to_dict() for record in self.error_records[-10:]]

        return {
            "total_errors": len(self.error_records),
            "severity_breakdown": severity_counts,
            "category_breakdown": category_counts,
            "action_breakdown": action_counts,
            "recent_errors": recent_errors,
            "critical_errors": [
                record.to_dict() for record in self.error_records if record.severity == ErrorSeverity.CRITICAL
            ],
        }

    def clear_errors(self) -> None:
        """Clear all recorded errors."""
        self.error_records.clear()
        self.logger.info("Error records cleared")

    def mark_error_resolved(self, error_index: int, resolution_notes: str) -> None:
        """Mark an error as resolved.

        Args:
            error_index: Index of error in error_records list
            resolution_notes: Notes about how the error was resolved
        """
        if 0 <= error_index < len(self.error_records):
            self.error_records[error_index].resolved = True
            self.error_records[error_index].resolution_notes = resolution_notes
            self.logger.info(f"Error {error_index} marked as resolved: {resolution_notes}")


def with_error_handling(component: str, operation: str, error_handler: Optional[ErrorHandler] = None):
    """Decorator for automatic error handling in functions.

    Args:
        component: Name of the component (e.g., 'etl_pipeline', 'sentiment_analyzer')
        operation: Name of the operation (e.g., 'process_batch', 'validate_data')
        error_handler: Optional error handler instance (creates default if None)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            handler = error_handler or ErrorHandler()
            context = ErrorContext(operation=operation, component=component)

            # Add function arguments to context metadata
            context.add_metadata("function", func.__name__)
            context.add_metadata("args_count", len(args))
            context.add_metadata("kwargs_keys", list(kwargs.keys()))

            try:
                return func(*args, **kwargs)
            except Exception as e:
                action = handler.handle_error(e, context)

                # Re-raise for critical errors
                if action == ErrorAction.STOP_PIPELINE:
                    raise

                # Return None for other actions (caller should handle)
                return None

        return wrapper

    return decorator


# Global error handler instance
_global_error_handler = ErrorHandler()


def get_global_error_handler() -> ErrorHandler:
    """Get the global error handler instance."""
    return _global_error_handler


def set_global_error_handler(handler: ErrorHandler) -> None:
    """Set the global error handler instance."""
    global _global_error_handler
    _global_error_handler = handler
