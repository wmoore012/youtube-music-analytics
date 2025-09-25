"""
Comprehensive error handling and logging system for MusicScope™ charts.

This module provides bulletproof error handling, data validation warnings,
fallback chart options, and performance monitoring for chart generation.
"""

from datetime import datetime
from functools import wraps
import logging
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("musicscope.charts")


class ChartError(Exception):
    """Custom exception for chart generation errors."""

    pass


class DataQualityWarning(UserWarning):
    """Warning for data quality issues that don't prevent chart generation."""

    pass


class ChartErrorHandler:
    """Comprehensive error handling system for chart generation."""

    def __init__(self):
        self.error_counts = {}
        self.performance_metrics = {}
        self.data_quality_issues = []

    def log_chart_error(self, chart_name: str, error: Exception, data_info: Dict[str, Any]) -> None:
        """Log detailed chart error information."""

        error_info = {
            "chart_name": chart_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat(),
            "data_shape": data_info.get("shape", "unknown"),
            "data_columns": data_info.get("columns", []),
            "traceback": traceback.format_exc(),
        }

        logger.error(f"Chart generation failed: {chart_name}")
        logger.error(f"Error: {error_info['error_message']}")
        logger.error(f"Data info: {data_info}")

        # Track error counts
        if chart_name not in self.error_counts:
            self.error_counts[chart_name] = 0
        self.error_counts[chart_name] += 1

    def log_performance_metric(self, chart_name: str, execution_time: float, data_size: int) -> None:
        """Log chart performance metrics."""

        if chart_name not in self.performance_metrics:
            self.performance_metrics[chart_name] = []

        self.performance_metrics[chart_name].append(
            {"execution_time": execution_time, "data_size": data_size, "timestamp": datetime.now().isoformat()}
        )

        logger.info(f"Chart performance: {chart_name} - {execution_time:.2f}s for {data_size} rows")

    def validate_data_quality(self, df: pd.DataFrame, chart_name: str, required_columns: List[str]) -> List[str]:
        """Validate data quality and return list of issues."""

        issues = []

        if df is None:
            issues.append("Data is None")
            return issues

        if df.empty:
            issues.append("Data is empty")
            return issues

        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")

        # Check data volume
        if len(df) < 3:
            issues.append(f"Insufficient data volume: {len(df)} rows (minimum 3 recommended)")

        # Check for null values in required columns
        for col in required_columns:
            if col in df.columns:
                null_pct = (df[col].isnull().sum() / len(df)) * 100
                if null_pct > 50:
                    issues.append(f"High null percentage in {col}: {null_pct:.1f}%")
                elif null_pct > 10:
                    issues.append(f"Moderate null percentage in {col}: {null_pct:.1f}%")

        # Log data quality issues
        if issues:
            self.data_quality_issues.extend(
                [{"chart_name": chart_name, "issues": issues, "timestamp": datetime.now().isoformat()}]
            )

            for issue in issues:
                logger.warning(f"Data quality issue in {chart_name}: {issue}")

        return issues

    def should_skip_chart(self, df: pd.DataFrame, chart_name: str, required_columns: List[str]) -> Tuple[bool, str]:
        """Determine if chart should be skipped due to data issues."""

        if df is None:
            return True, "Data is None"

        if df.empty:
            return True, "Data is empty"

        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return True, f"Missing required columns: {missing_columns}"

        # Check if we have any data in required columns
        for col in required_columns:
            if col in df.columns and df[col].notna().sum() == 0:
                return True, f"No valid data in required column: {col}"

        return False, "Data validation passed"

    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors and performance metrics."""

        return {
            "error_counts": self.error_counts,
            "total_errors": sum(self.error_counts.values()),
            "charts_with_errors": len(self.error_counts),
            "data_quality_issues": len(self.data_quality_issues),
            "performance_metrics": {
                chart: {
                    "avg_time": sum(m["execution_time"] for m in metrics) / len(metrics),
                    "max_time": max(m["execution_time"] for m in metrics),
                    "total_executions": len(metrics),
                }
                for chart, metrics in self.performance_metrics.items()
            },
        }


# Global error handler instance
error_handler = ChartErrorHandler()


def bulletproof_chart(chart_name: str, required_columns: List[str]):
    """
    Decorator for bulletproof chart generation with comprehensive error handling.

    Args:
        chart_name: Name of the chart for logging
        required_columns: List of required columns for the chart
    """

    def decorator(chart_function: Callable) -> Callable:
        @wraps(chart_function)
        def wrapper(df: pd.DataFrame, *args, **kwargs) -> Optional[go.Figure]:

            start_time = time.time()

            try:
                # Get data info for logging
                data_info = {
                    "shape": df.shape if df is not None else None,
                    "columns": df.columns.tolist() if df is not None and hasattr(df, "columns") else [],
                    "memory_usage": (
                        df.memory_usage(deep=True).sum() if df is not None and hasattr(df, "memory_usage") else 0
                    ),
                }

                # Check if chart should be skipped
                should_skip, skip_reason = error_handler.should_skip_chart(df, chart_name, required_columns)

                if should_skip:
                    logger.warning(f"Skipping chart {chart_name}: {skip_reason}")
                    return None

                # Validate data quality (warnings only)
                data_issues = error_handler.validate_data_quality(df, chart_name, required_columns)

                # Execute chart function
                logger.info(f"Generating chart: {chart_name}")
                result = chart_function(df, *args, **kwargs)

                # Log performance
                execution_time = time.time() - start_time
                data_size = len(df) if df is not None else 0
                error_handler.log_performance_metric(chart_name, execution_time, data_size)

                # Validate result
                if result is None:
                    logger.warning(f"Chart function {chart_name} returned None")
                    return None

                logger.info(f"Successfully generated chart: {chart_name}")
                return result

            except Exception as e:
                # Log error
                execution_time = time.time() - start_time
                data_info = {
                    "shape": df.shape if df is not None else None,
                    "columns": df.columns.tolist() if df is not None and hasattr(df, "columns") else [],
                }

                error_handler.log_chart_error(chart_name, e, data_info)
                logger.error(f"Chart {chart_name} failed: {str(e)}")

                # Return None instead of fallback chart
                return None

        return wrapper

    return decorator


def monitor_chart_performance(df: pd.DataFrame, chart_functions: Dict[str, Callable]) -> Dict[str, Any]:
    """
    Monitor performance of multiple chart functions.

    Args:
        df: Data to test with
        chart_functions: Dictionary of chart_name -> function

    Returns:
        Performance monitoring results
    """

    results = {"successful_charts": [], "failed_charts": [], "performance_metrics": {}, "data_quality_score": 0.0}

    for chart_name, chart_func in chart_functions.items():
        try:
            start_time = time.time()

            # Execute chart
            fig = chart_func(df)

            execution_time = time.time() - start_time

            if fig is not None:
                results["successful_charts"].append(chart_name)
                results["performance_metrics"][chart_name] = {"execution_time": execution_time, "success": True}
            else:
                results["failed_charts"].append(chart_name)
                results["performance_metrics"][chart_name] = {
                    "execution_time": execution_time,
                    "success": False,
                    "error": "Function returned None",
                }

        except Exception as e:
            execution_time = time.time() - start_time
            results["failed_charts"].append(chart_name)
            results["performance_metrics"][chart_name] = {
                "execution_time": execution_time,
                "success": False,
                "error": str(e),
            }

    # Calculate data quality score
    total_charts = len(chart_functions)
    successful_charts = len(results["successful_charts"])
    results["data_quality_score"] = successful_charts / total_charts if total_charts > 0 else 0.0

    return results


def create_error_dashboard() -> go.Figure:
    """Create dashboard showing error summary and performance metrics."""

    summary = error_handler.get_error_summary()

    fig = go.Figure()

    # Create error summary text
    error_text = (
        f"🔍 Chart Error Dashboard<br><br>"
        + f"📊 Total Errors: {summary['total_errors']}<br>"
        + f"📈 Charts with Errors: {summary['charts_with_errors']}<br>"
        + f"⚠️  Data Quality Issues: {summary['data_quality_issues']}<br><br>"
    )

    if summary["error_counts"]:
        error_text += "🚨 Most Problematic Charts:<br>"
        sorted_errors = sorted(summary["error_counts"].items(), key=lambda x: x[1], reverse=True)
        for chart, count in sorted_errors[:5]:
            error_text += f"• {chart}: {count} errors<br>"

    fig.add_annotation(
        text=error_text,
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=12, color="#333"),
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="#ddd",
        borderwidth=1,
    )

    fig.update_layout(title="MusicScope™ Chart Error Dashboard", height=500, showlegend=False)

    return fig


# Example usage functions with bulletproof decorator
@bulletproof_chart("Example Chart", ["artist_name", "view_count"])
def example_bulletproof_chart(df: pd.DataFrame) -> go.Figure:
    """Example of a bulletproof chart function."""

    fig = go.Figure()

    if "artist_name" in df.columns and "view_count" in df.columns:
        artist_views = df.groupby("artist_name")["view_count"].sum().reset_index()

        fig.add_trace(go.Bar(x=artist_views["artist_name"], y=artist_views["view_count"], name="Views"))

        fig.update_layout(title="Artist Views - Real Data", xaxis_title="Artist", yaxis_title="Total Views")

    return fig
