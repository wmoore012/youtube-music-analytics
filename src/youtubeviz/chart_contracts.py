"""
Chart contracts and bulletproof decorators for MusicScope™

This module provides proper validation, timeouts, and error handling
for chart generation without hiding real errors.
"""

import concurrent.futures as cf
import functools
import logging
import time
from typing import Any, Callable, List, Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


class ChartSpec(BaseModel):
    """Chart specification with validation requirements."""

    name: str
    required_columns: List[str]
    max_rows: int = Field(default=2_000_000, description="Max rows to prevent kernel lock")
    min_rows: int = Field(default=1, description="Minimum rows required")
    timeout_sec: int = Field(default=10, description="Chart generation timeout")


def _validate_df(df: pd.DataFrame, spec: ChartSpec) -> None:
    """Validate DataFrame against chart specification."""
    # Check if DataFrame exists and is not None
    if df is None:
        raise ValueError(f"[{spec.name}] DataFrame is None")

    # Check required columns
    missing = [c for c in spec.required_columns if c not in df.columns]
    if missing:
        available = list(df.columns)
        raise ValueError(f"[{spec.name}] Missing required columns: {missing}\n" f"Available columns: {available}")

    # Check row count constraints
    if len(df) < spec.min_rows:
        raise ValueError(f"[{spec.name}] Not enough data: {len(df)} rows < {spec.min_rows} required")

    if len(df) > spec.max_rows:
        raise ValueError(f"[{spec.name}] Too many rows: {len(df):,} > {spec.max_rows:,} (may lock kernel)")

    # Check for completely empty required columns
    empty_cols = []
    for col in spec.required_columns:
        if df[col].isna().all():
            empty_cols.append(col)

    if empty_cols:
        raise ValueError(f"[{spec.name}] Required columns are completely empty: {empty_cols}")


def bulletproof_chart(spec: ChartSpec, timeout_sec: Optional[int] = None):
    """
    Decorator for bulletproof chart generation.

    - Validates input data upfront
    - Applies timeout to prevent hanging
    - Provides clear error messages
    - Does NOT hide real errors
    - Supports both ChartSpec objects and direct parameters
    """
    # Handle both ChartSpec objects and direct parameters for backward compatibility
    if isinstance(spec, str):
        # Legacy usage: bulletproof_chart("ChartName", ["col1", "col2"])
        name = spec
        required_columns = timeout_sec if isinstance(timeout_sec, list) else []
        actual_spec = ChartSpec(name=name, required_columns=required_columns)
        actual_timeout = 10  # default
    else:
        actual_spec = spec
        actual_timeout = timeout_sec or spec.timeout_sec

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(df: pd.DataFrame, *args, **kwargs) -> Any:
            start_time = time.time()

            try:
                # Validate input data first
                _validate_df(df, actual_spec)

                # Execute with timeout
                with cf.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(fn, df, *args, **kwargs)
                    try:
                        result = future.result(timeout=actual_timeout)

                        # Validate result is a proper chart object
                        if result is None:
                            raise ValueError(f"[{actual_spec.name}] Chart function returned None")

                        # Log successful execution
                        elapsed = time.time() - start_time
                        logger.info(f"[{actual_spec.name}] Generated successfully in {elapsed:.2f}s")

                        return result

                    except cf.TimeoutError:
                        raise TimeoutError(
                            f"[{actual_spec.name}] Chart generation timed out after {actual_timeout}s\n"
                            f"Consider reducing data size or increasing timeout"
                        )

            except ValidationError as e:
                # Pydantic validation errors-these are data issues
                elapsed = time.time() - start_time
                logger.error(f"[{actual_spec.name}] Data validation failed after {elapsed:.2f}s: {e}")
                raise ValueError(f"[{actual_spec.name}] Data validation error: {e}") from e

            except Exception as e:
                # Log the error but re-raise it (don't hide it)
                elapsed = time.time() - start_time
                logger.error(f"[{actual_spec.name}] Failed after {elapsed:.2f}s: {e}")

                # Re-raise with enhanced context but preserve original exception type
                raise type(e)(f"[{actual_spec.name}] {str(e)}") from e

        return wrapper

    return decorator


def create_interactive_plotly_config() -> dict:
    """Create standard Plotly configuration for interactive charts."""
    return {
        "displayModeBar": True,
        "displaylogo": False,
        "modeBarButtonsToRemove": ["pan2d", "lasso2d"],
        "toImageButtonOptions": {
            "format": "png",
            "filename": "musicscope_chart",
            "height": 600,
            "width": 1000,
            "scale": 2,
        },
    }


def setup_plotly_animation(
    fig, autoplay: bool = False, frame_duration: int = 500, transition_duration: int = 200
) -> None:
    """
    Setup Plotly animation controls with sensible defaults and axis pinning.

    Prevents jitter by fixing axis ranges and optimizing frame transitions.
    """
    if not hasattr(fig, "layout"):
        return

    # Pin axis ranges to prevent rescaling jitter during animation
    if hasattr(fig, "data") and fig.data:
        # Calculate stable axis ranges from all data
        all_x_vals, all_y_vals = [], []

        for trace in fig.data:
            if hasattr(trace, "x") and trace.x is not None:
                all_x_vals.extend([x for x in trace.x if x is not None])
            if hasattr(trace, "y") and trace.y is not None:
                all_y_vals.extend([y for y in trace.y if y is not None])

        # Set stable ranges with 5% padding
        if all_x_vals:
            x_min, x_max = min(all_x_vals), max(all_x_vals)
            x_padding = (x_max - x_min) * 0.05
            fig.update_xaxes(range=[x_min - x_padding, x_max + x_padding])

        if all_y_vals:
            y_min, y_max = min(all_y_vals), max(all_y_vals)
            y_padding = (y_max - y_min) * 0.05
            fig.update_yaxes(range=[y_min - y_padding, y_max + y_padding])

    # Configure animation controls
    if hasattr(fig.layout, "updatemenus") and fig.layout.updatemenus:
        for button in fig.layout.updatemenus[0].buttons:
            if hasattr(button, "args") and len(button.args) > 1:
                # Set frame duration and transition
                if "frame" in button.args[1]:
                    button.args[1]["frame"]["duration"] = frame_duration
                    button.args[1]["frame"]["redraw"] = False  # Reduce redraws for smoother animation
                if "transition" in button.args[1]:
                    button.args[1]["transition"]["duration"] = transition_duration
                    button.args[1]["transition"]["easing"] = "cubic-in-out"

        # Set autoplay behavior
        if not autoplay:
            fig.layout.updatemenus[0].active = -1


def create_altair_brush_selection():
    """Create standard Altair brush selection for linked interactions."""
    try:
        import altair as alt

        return alt.selection_interval(encodings=["x"])
    except ImportError:
        return None


def create_altair_click_selection():
    """Create standard Altair click selection for categorical filtering."""
    try:
        import altair as alt

        return alt.selection_multi(fields=["artist_name"])
    except ImportError:
        return None


def apply_plotly_hover_enhancements(fig, hover_data: Optional[List[str]] = None):
    """
    Apply consistent hover enhancements to Plotly charts.

    Args:
        fig: Plotly figure
        hover_data: Additional fields to include in hover (e.g., ['isrc', 'dsp'])
    """
    if not hasattr(fig, "update_traces"):
        return fig

    # Standard hover template with music industry context
    base_template = "<b>%{fullData.name}</b><br>"

    # Add hover data fields if provided
    if hover_data:
        for field in hover_data:
            base_template += f"{field.upper()}: %{{customdata[{hover_data.index(field)}]}}<br>"

    base_template += "%{x}<br>%{y:,.0f}<extra></extra>"

    fig.update_traces(
        hovertemplate=base_template,
        hoverlabel=dict(bgcolor="white", bordercolor="black", font_size=12, font_family="Arial"),
    )

    # Enable unified hover mode for time series
    fig.update_layout(hovermode="x unified")

    return fig


# Export all functions for easy importing
__all__ = [
    "ChartSpec",
    "bulletproof_chart",
    "create_interactive_plotly_config",
    "setup_plotly_animation",
    "create_altair_brush_selection",
    "create_altair_click_selection",
    "apply_plotly_hover_enhancements",
    "CHART_SPECS",
]


# Example chart specifications for common MusicScope™ charts
CHART_SPECS = {
    "views_over_time": ChartSpec(
        name="ViewsOverTime", required_columns=["date", "views", "artist_name"], max_rows=100_000, timeout_sec=8
    ),
    "sentiment_analysis": ChartSpec(
        name="SentimentAnalysis",
        required_columns=["artist_name", "sentiment_score", "comment_text"],
        max_rows=50_000,
        timeout_sec=12,
    ),
    "engagement_metrics": ChartSpec(
        name="EngagementMetrics",
        required_columns=["artist_name", "likes", "comments", "views"],
        max_rows=200_000,
        timeout_sec=6,
    ),
    "content_analysis": ChartSpec(
        name="ContentAnalysis",
        required_columns=["artist_name", "content_type", "views"],
        max_rows=150_000,
        timeout_sec=8,
    ),
}
