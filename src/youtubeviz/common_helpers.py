"""
Common Helper Functions - YouTube Analytics Platform

This module contains commonly used helper functions extracted from across the codebase
to reduce duplication and improve maintainability. These functions follow the single
responsibility principle and are designed to be reusable.

Categories:
- Database operation helpers
- Data validation helpers
- Error handling helpers
- Formatting / output helpers
- File operation helpers
"""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

# =============================================================================
# DATABASE OPERATION HELPERS
# =============================================================================


def execute_query_safely(conn: Connection, query: str, params: Optional[Dict] = None) -> Any:
    """
    Execute a database query with error handling and logging.

    Args:
        conn: Database connection
        query: SQL query string
        params: Optional query parameters

    Returns:
        Query result

    Raises:
        Exception: If query execution fails
    """
    try:
        if params:
            result = conn.execute(text(query), params)
        else:
            result = conn.execute(text(query))
        return result
    except Exception as e:
        logging.error(f"Database query failed: {query[:100]}... Error: {e}")
        raise


def get_table_row_count(conn: Connection, table_name: str) -> int:
    """
    Get the number of rows in a database table.

    Args:
        conn: Database connection
        table_name: Name of the table

    Returns:
        Number of rows in the table
    """
    try:
        result = execute_query_safely(conn, f"SELECT COUNT(*) FROM {table_name}")
        return result.scalar() or 0
    except Exception:
        logging.warning(f"Could not get row count for table {table_name}")
        return 0


def check_table_exists(conn: Connection, table_name: str) -> bool:
    """
    Check if a database table exists.

    Args:
        conn: Database connection
        table_name: Name of the table to check

    Returns:
        True if table exists, False otherwise
    """
    try:
        query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = %s AND table_schema = DATABASE()
        """
        result = execute_query_safely(conn, query, {"table_name": table_name})
        return result.scalar() > 0
    except Exception:
        return False


def batch_insert_records(conn: Connection, table_name: str, records: List[Dict], batch_size: int = 1000) -> int:
    """
    Insert records in batches for better performance.

    Args:
        conn: Database connection
        table_name: Target table name
        records: List of record dictionaries
        batch_size: Number of records per batch

    Returns:
        Total number of records inserted
    """
    if not records:
        return 0

    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        # Build INSERT query
        columns = list(batch[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            result = conn.execute(text(query), batch)
            total_inserted += len(batch)
        except Exception as e:
            logging.error(f"Batch insert failed for {table_name}: {e}")
            raise

    return total_inserted


# =============================================================================
# DATA VALIDATION HELPERS
# =============================================================================


def validate_required_fields(data: Dict, required_fields: List[str]) -> List[str]:
    """
    Validate that required fields are present and not empty.

    Args:
        data: Dictionary to validate
        required_fields: List of required field names

    Returns:
        List of missing or empty field names
    """
    missing_fields = []

    for field in required_fields:
        if field not in data or data[field] is None or str(data[field]).strip() == "":
            missing_fields.append(field)

    return missing_fields


def validate_data_types(data: Dict, type_specs: Dict[str, type]) -> List[str]:
    """
    Validate data types for specified fields.

    Args:
        data: Dictionary to validate
        type_specs: Dictionary mapping field names to expected types

    Returns:
        List of fields with incorrect types
    """
    type_errors = []

    for field, expected_type in type_specs.items():
        if field in data and data[field] is not None:
            if not isinstance(data[field], expected_type):
                type_errors.append(f"{field} should be {expected_type.__name__}, got {type(data[field]).__name__}")

    return type_errors


def clean_text_field(text: Optional[str], max_length: Optional[int] = None) -> Optional[str]:
    """
    Clean and normalize text field.

    Args:
        text: Text to clean
        max_length: Maximum allowed length

    Returns:
        Cleaned text or None if input was empty
    """
    if not text or not isinstance(text, str):
        return None

    # Remove extra whitespace
    cleaned = re.sub(r"\s+", " ", text.strip())

    # Truncate if necessary
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip()

    return cleaned if cleaned else None


def validate_youtube_id(youtube_id: str, id_type: str = "video") -> bool:
    """
    Validate YouTube ID format.

    Args:
        youtube_id: YouTube ID to validate
        id_type: Type of ID ("video", "channel", "playlist")

    Returns:
        True if ID format is valid
    """
    if not youtube_id or not isinstance(youtube_id, str):
        return False

    patterns = {
        "video": r"^[a - zA - Z0 - 9_-]{11}$",
        "channel": r"^UC[a - zA - Z0 - 9_-]{22}$",
        "playlist": r"^[a - zA - Z0 - 9_-]{34}$",
    }

    pattern = patterns.get(id_type, patterns["video"])
    return bool(re.match(pattern, youtube_id))


# =============================================================================
# ERROR HANDLING HELPERS
# =============================================================================


def log_error_with_context(error: Exception, context: Dict[str, Any], logger: Optional[logging.Logger] = None) -> None:
    """
    Log error with additional context information.

    Args:
        error: Exception that occurred
        context: Dictionary with context information
        logger: Optional logger instance
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
    logger.error(f"Error: {str(error)} | Context: {context_str}")


def retry_operation(operation, max_retries: int = 3, delay: float = 1.0, backoff_factor: float = 2.0) -> Any:
    """
    Retry an operation with exponential backoff.

    Args:
        operation: Function to retry
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries
        backoff_factor: Factor to multiply delay by after each retry

    Returns:
        Result of successful operation

    Raises:
        Exception: If all retries fail
    """
    import time

    last_exception = None
    current_delay = delay

    for attempt in range(max_retries + 1):
        try:
            return operation()
        except Exception as e:
            last_exception = e

            if attempt < max_retries:
                logging.warning(f"Operation failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                time.sleep(current_delay)
                current_delay *= backoff_factor
            else:
                logging.error(f"Operation failed after {max_retries + 1} attempts")

    raise last_exception


def safe_divide(
    numerator: Union[int, float], denominator: Union[int, float], default: Union[int, float] = 0
) -> Union[int, float]:
    """
    Safely divide two numbers, returning default if division by zero.

    Args:
        numerator: Number to divide
        denominator: Number to divide by
        default: Value to return if denominator is zero

    Returns:
        Result of division or default value
    """
    try:
        if denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default


# =============================================================================
# FORMATTING / OUTPUT HELPERS
# =============================================================================


def format_number(number: Union[int, float], precision: int = 1) -> str:
    """
    Format number with appropriate units (K, M, B).

    Args:
        number: Number to format
        precision: Decimal places to show

    Returns:
        Formatted number string
    """
    if not isinstance(number, (int, float)) or number == 0:
        return "0"

    abs_number = abs(number)
    sign = "-" if number < 0 else ""

    if abs_number >= 1_000_000_000:
        formatted = f"{abs_number / 1_000_000_000:.{precision}f}B"
    elif abs_number >= 1_000_000:
        formatted = f"{abs_number / 1_000_000:.{precision}f}M"
    elif abs_number >= 1_000:
        formatted = f"{abs_number / 1_000:.{precision}f}K"
    else:
        formatted = f"{abs_number:.{precision}f}" if precision > 0 else str(int(abs_number))

    return f"{sign}{formatted}"


def format_duration(seconds: Union[int, float]) -> str:
    """
    Format duration in seconds to human - readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string (e.g., "2h 30m 45s")
    """
    if not isinstance(seconds, (int, float)) or seconds < 0:
        return "0s"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def format_percentage(value: Union[int, float], total: Union[int, float], precision: int = 1) -> str:
    """
    Format value as percentage of total.

    Args:
        value: Numerator value
        total: Denominator value
        precision: Decimal places to show

    Returns:
        Formatted percentage string
    """
    if total == 0:
        return "0.0%"

    percentage = (value / total) * 100
    return f"{percentage:.{precision}f}%"


def create_progress_bar(current: int, total: int, width: int = 50) -> str:
    """
    Create a text - based progress bar.

    Args:
        current: Current progress value
        total: Total / maximum value
        width: Width of progress bar in characters

    Returns:
        Progress bar string
    """
    if total == 0:
        return "[" + " " * width + "] 0%"

    progress = min(current / total, 1.0)
    filled = int(width * progress)
    bar = "█" * filled + "░" * (width - filled)
    percentage = progress * 100

    return f"[{bar}] {percentage:.1f}%"


# =============================================================================
# FILE OPERATION HELPERS
# =============================================================================


def ensure_directory_exists(directory_path: Union[str, Path]) -> Path:
    """
    Ensure directory exists, creating it if necessary.

    Args:
        directory_path: Path to directory

    Returns:
        Path object for the directory
    """
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_json_file(file_path: Union[str, Path], default: Any = None) -> Any:
    """
    Safely read JSON file with error handling.

    Args:
        file_path: Path to JSON file
        default: Default value if file cannot be read

    Returns:
        Parsed JSON data or default value
    """
    try:
        with open(file_path, "r", encoding="utf - 8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.warning(f"Could not read JSON file {file_path}: {e}")
        return default


def write_json_file(file_path: Union[str, Path], data: Any, indent: int = 2) -> bool:
    """
    Safely write data to JSON file.

    Args:
        file_path: Path to JSON file
        data: Data to write
        indent: JSON indentation

    Returns:
        True if successful, False otherwise
    """
    try:
        ensure_directory_exists(Path(file_path).parent)
        with open(file_path, "w", encoding="utf - 8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        return True
    except Exception as e:
        logging.error(f"Could not write JSON file {file_path}: {e}")
        return False


def get_file_size_mb(file_path: Union[str, Path]) -> float:
    """
    Get file size in megabytes.

    Args:
        file_path: Path to file

    Returns:
        File size in MB, or 0 if file doesn't exist
    """
    try:
        size_bytes = Path(file_path).stat().st_size
        return size_bytes / (1024 * 1024)
    except (FileNotFoundError, OSError):
        return 0.0


# =============================================================================
# DATE / TIME HELPERS
# =============================================================================


def get_current_timestamp() -> datetime:
    """Get current timestamp in UTC."""
    return datetime.now(timezone.utc)


def format_timestamp(timestamp: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format timestamp to string.

    Args:
        timestamp: Datetime object
        format_str: Format string

    Returns:
        Formatted timestamp string
    """
    try:
        return timestamp.strftime(format_str)
    except (AttributeError, ValueError):
        return "Invalid timestamp"


def parse_youtube_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    Parse YouTube API timestamp format.

    Args:
        timestamp_str: YouTube timestamp string (e.g., "2023 - 01 - 01T12:00:00Z")

    Returns:
        Parsed datetime object or None if parsing fails
    """
    try:
        # Handle YouTube's ISO format
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"
        return datetime.fromisoformat(timestamp_str)
    except (ValueError, AttributeError):
        return None


# =============================================================================
# PANDAS HELPERS
# =============================================================================


def safe_dataframe_operation(df: pd.DataFrame, operation, default_value=None) -> Any:
    """
    Safely perform operation on DataFrame with error handling.

    Args:
        df: DataFrame to operate on
        operation: Function to apply to DataFrame
        default_value: Value to return if operation fails

    Returns:
        Result of operation or default value
    """
    try:
        return operation(df)
    except Exception as e:
        logging.warning(f"DataFrame operation failed: {e}")
        return default_value


def clean_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame column names (lowercase, underscores).

    Args:
        df: DataFrame to clean

    Returns:
        DataFrame with cleaned column names
    """
    df = df.copy()
    df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in df.columns]
    return df


def remove_empty_rows(df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Remove rows that are completely empty or have empty required columns.

    Args:
        df: DataFrame to clean
        subset: List of columns to check for emptiness

    Returns:
        DataFrame with empty rows removed
    """
    if subset:
        return df.dropna(subset=subset, how="all")
    else:
        return df.dropna(how="all")
