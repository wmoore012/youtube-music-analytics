"""Bulletproof chart execution with timeouts and data validation.

This module provides guard rails for chart functions to prevent hangs,
handle missing data gracefully, and provide clear error messages.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
import functools
import logging
from typing import Any, Callable, List, Optional, TypeVar, Union

import pandas as pd
import plotly.graph_objects as go

# Type variables for generic function wrapping
F = TypeVar("F", bound=Callable[..., Any])


# Configure logging to be notebook - safe
def _setup_logger(name: str) -> logging.Logger:
    """Set up a logger that won't duplicate handlers in notebooks."""
    logger = logging.getLogger(name)

    # Clear existing handlers to prevent duplicates
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    # Add a single clean handler
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent propagation to avoid duplicates

    return logger


logger = _setup_logger("musicscope.charts")


def check_data_quality(df: pd.DataFrame, required_columns: List[str]) -> Optional[str]:
    """Check data quality and return error message if issues found.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present

    Returns:
        None if data is valid, error message string if issues found
    """
    if df is None or df.empty:
        return "DataFrame is None or empty"

    # Check for required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        return f"Missing required columns: {missing_cols}"

    # Check null rates and warn (but don't fail)
    for col in required_columns:
        null_rate = df[col].isnull().sum() / len(df)
        if null_rate > 0.5:
            logger.warning(f"Column '{col}' has high null rate: {null_rate:.1%}")
        elif null_rate > 0:
            logger.info(f"Column '{col}' has some nulls: {null_rate:.1%}")

    return None


def bulletproof_chart(
    chart_name: str, required_columns: List[str], timeout_sec: float = 5.0
) -> Callable[[Callable], Callable]:
    """Decorator to add timeout and data validation to chart functions.

    Args:
        chart_name: Name of the chart for logging
        required_columns: List of column names that must be present in DataFrame
        timeout_sec: Maximum execution time in seconds

    Returns:
        Decorated function that returns None on error / timeout
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs) -> Optional[go.Figure]:
            # Data quality check
            error_msg = check_data_quality(df, required_columns)
            if error_msg:
                logger.error(f"Chart '{chart_name}' failed data validation: {error_msg}")
                return None

            # Execute with timeout
            try:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, df, *args, **kwargs)
                    result = future.result(timeout=timeout_sec)

                    # Validate result is a proper Plotly figure
                    if hasattr(result, "to_dict"):
                        return result
                    else:
                        logger.error(f"Chart '{chart_name}' returned invalid figure type: {type(result)}")
                        return None

            except FutureTimeoutError:
                logger.error(f"Chart '{chart_name}' timed out after {timeout_sec}s")
                return None
            except Exception as e:
                logger.error(f"Chart '{chart_name}' failed with error: {e}", exc_info=True)
                return None

        return wrapper

    return decorator


def safe_chart_execution(
    chart_func: Callable, df: pd.DataFrame, chart_name: str = "unknown", timeout_sec: float = 5.0, **kwargs
) -> Optional[go.Figure]:
    """Execute a chart function safely with timeout and error handling.

    This is a functional alternative to the decorator for one - off usage.

    Args:
        chart_func: Chart function to execute
        df: DataFrame to pass to chart function
        chart_name: Name for logging
        timeout_sec: Timeout in seconds
        **kwargs: Additional arguments to pass to chart function

    Returns:
        Plotly Figure or None if execution failed
    """
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(chart_func, df, **kwargs)
            result = future.result(timeout=timeout_sec)

            if hasattr(result, "to_dict"):
                return result
            else:
                logger.error(f"Chart '{chart_name}' returned invalid figure type: {type(result)}")
                return None

    except FutureTimeoutError:
        logger.error(f"Chart '{chart_name}' timed out after {timeout_sec}s")
        return None
    except Exception as e:
        logger.error(f"Chart '{chart_name}' failed with error: {e}", exc_info=True)
        return None
