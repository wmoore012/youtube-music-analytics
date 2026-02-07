from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from .chart_contracts import (
    ChartSpec,
    apply_plotly_hover_enhancements,
    bulletproof_chart,
    create_altair_brush_selection,
    create_altair_click_selection,
    create_interactive_plotly_config,
    setup_plotly_animation,
)
from .content import analyze_genre_context

try:
    import plotly.express as px
    import plotly.graph_objects as go
except Exception:  # pragma: no cover-optional
    px = None
    go = None

try:
    import altair as alt
except Exception:  # pragma: no cover-optional
    alt = None


logger = logging.getLogger(__name__)


_SCHEME_COLORS: dict[str, list[str]] = {
    "vibrant": [
        "#FF6B6B",
        "#4ECDC4",
        "#45B7D1",
        "#96CEB4",
        "#FFEAA7",
        "#DDA0DD",
        "#98D8C8",
        "#F7DC6F",
        "#BB8FCE",
        "#85C1E9",
    ],
    "pastel": [
        "#FFB3BA",
        "#FFDFBA",
        "#FFFFBA",
        "#BAFFC9",
        "#BAE1FF",
        "#E1BAFF",
        "#FFBAE1",
        "#C9FFBA",
        "#BAFFE1",
        "#E1FFBA",
        "#FFD1FF",
        "#E0BBE4",
        "#957DAD",
        "#D291BC",
        "#FEC8D8",
        "#FFDFD3",
    ],
    "monochrome": [
        "#2C3E50",
        "#34495E",
        "#7F8C8D",
        "#95A5A6",
        "#BDC3C7",
        "#ECF0F1",
        "#3498DB",
        "#5DADE2",
        "#85C1E9",
        "#AED6F1",
    ],
}


def _default_palette(n: int) -> list[str]:
    # Plotly category10-like fallback palette
    base = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]
    if n <= len(base):
        return base[:n]
    # repeat if needed
    out = []
    while len(out) < n:
        out.extend(base)
    return out[:n]


def get_artist_color_map(artists: Sequence[str]) -> dict[str, str]:
    """Build a stable color mapping for artists.

    Respects env var ARTIST_COLORS_JSON (JSON object {"Artist": "#RRGGBB", ...}).
    Assigns remaining artists from a default palette deterministically by name.

    Args:
        artists: Sequence of artist names (strings)

    Raises:
        TypeError: If artists is not a sequence of strings
    """
    # Input validation-reject DataFrames explicitly
    if hasattr(artists, "columns"):  # This catches pandas DataFrames
        raise TypeError("artists cannot be a DataFrame. Pass a list / array of artist names instead.")

    if isinstance(artists, str):
        raise TypeError("artists must be a sequence of artist names, not a single string")

    if not isinstance(artists, (list, tuple, pd.Index, pd.Series)) and not hasattr(artists, "__iter__"):
        raise TypeError(f"artists must be a sequence of strings, got {type(artists)}")

    # Convert to list and validate string content
    try:
        artist_list = list(artists)
        if not all(isinstance(artist, str) for artist in artist_list):
            raise TypeError("All artists must be strings")
    except Exception as e:
        raise TypeError(f"Could not convert artists to list of strings: {e}")

    # Load user-specified mapping from env (JSON or file path)
    env_map: dict[str, str] = {}
    # (a) JSON directly
    raw = os.getenv("ARTIST_COLORS_JSON")
    if raw:
        try:
            env_map = json.loads(raw)
        except Exception as exc:
            logger.warning("Invalid ARTIST_COLORS_JSON payload; falling back to defaults. Error: %s", exc)
            env_map = {}
    # (b) Or from a JSON file path via ARTIST_COLORS_FILE
    if not env_map:
        path = os.getenv("ARTIST_COLORS_FILE")
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    env_map = json.load(fh)
            except Exception as exc:
                logger.warning("Invalid ARTIST_COLORS_FILE JSON at %s; falling back to defaults. Error: %s", path, exc)
                env_map = {}
    known = {a: env_map.get(a) for a in artist_list if env_map.get(a)}
    remaining = [a for a in artist_list if a not in known]
    palette = _default_palette(len(remaining))
    # Assign colors by sorted order to keep stability
    for i, a in enumerate(sorted(remaining)):
        known[a] = palette[i]
    return known


def _get_scheme_colors(scheme_name: str) -> list[str]:
    """Return colors for the requested ``scheme_name`` (case insensitive)."""

    key = (scheme_name or "vibrant").lower()
    colors = _SCHEME_COLORS.get(key, _SCHEME_COLORS["vibrant"])
    return list(colors)


def _extract_plotly_series_labels(chart) -> list[str]:
    """Return a unique, ordered list of trace labels from a Plotly figure."""

    labels: list[str] = []

    for trace in getattr(chart, "data", []) or []:
        name = getattr(trace, "name", None)
        if not name:
            continue
        label = str(name).split(" (", 1)[0].strip()
        if label and label not in labels:
            labels.append(label)

    return labels


def enhance_chart_beauty(
    chart,
    title: Optional[str] = None,
    emotional_theme: str = "professional",
    config: Optional[dict] = None,
    annotations: Optional[list] = None,
):
    """Enhance chart visual appeal with emotional theming.

    Args:
        chart: Plotly or Altair chart object
        title: Optional title to set
        emotional_theme: Theme (professional, energetic, warm, dramatic)
        config: Optional configuration dict with styling parameters
        annotations: Optional list of annotations to add

    Returns:
        Enhanced chart object
    """
    if chart is None:
        return chart

    # Default config
    default_config = {
        "height": 600,
        "width": None,
        "title_size": 24,
        "axis_title_size": 14,
        "font_family": "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
    }

    if config:
        default_config.update(config)

    # Theme configurations
    themes = {
        "professional": {
            "bg_color": "#FFFFFF",
            "grid_color": "#E5E5E5",
            "text_color": "#2C3E50",
            "title_color": "#1A252F",
        },
        "energetic": {
            "bg_color": "#FAFAFA",
            "grid_color": "#E8E8E8",
            "text_color": "#E74C3C",
            "title_color": "#C0392B",
        },
        "warm": {"bg_color": "#FFF8F0", "grid_color": "#F0E6D2", "text_color": "#8B4513", "title_color": "#A0522D"},
        "dramatic": {"bg_color": "#1A1A1A", "grid_color": "#404040", "text_color": "#FFFFFF", "title_color": "#F39C12"},
    }

    theme_config = themes.get(emotional_theme, themes["professional"])

    # Try to enhance Plotly chart
    if hasattr(chart, "update_layout"):
        layout_updates = {
            "plot_bgcolor": theme_config["bg_color"],
            "paper_bgcolor": theme_config["bg_color"],
            "font": {"family": default_config["font_family"], "size": 12, "color": theme_config["text_color"]},
            "title": {
                "font": {
                    "size": default_config["title_size"],
                    "color": theme_config["title_color"],
                    "family": default_config["font_family"],
                },
                "x": 0.5,
                "xanchor": "center",
            },
            "xaxis": {
                "gridcolor": theme_config["grid_color"],
                "title": {"font": {"size": default_config["axis_title_size"]}},
            },
            "yaxis": {
                "gridcolor": theme_config["grid_color"],
                "title": {"font": {"size": default_config["axis_title_size"]}},
            },
        }

        if title:
            layout_updates["title"]["text"] = title

        if default_config["height"]:
            layout_updates["height"] = default_config["height"]

        if default_config["width"]:
            layout_updates["width"] = default_config["width"]

        if annotations:
            layout_updates["annotations"] = annotations

        chart.update_layout(**layout_updates)
        return chart

    # Try to enhance Altair chart
    elif hasattr(chart, "resolve_scale") and hasattr(chart, "properties"):
        properties = {
            "background": theme_config["bg_color"],
            "title": {
                "fontSize": default_config["title_size"],
                "color": theme_config["title_color"],
                "anchor": "middle",
            },
        }

        if title:
            properties["title"]["text"] = title

        if default_config["height"]:
            properties["height"] = default_config["height"]

        if default_config["width"]:
            properties["width"] = default_config["width"]

        enhanced = chart.resolve_scale(color="independent").properties(**properties)
        return enhanced

    # Return unchanged if we can't enhance it
    return chart


def apply_color_scheme(  # noqa: C901
    chart,
    scheme_name: Optional[str] = None,
    custom_colors: Optional[Mapping[str, str]] = None,
    artists: Optional[Sequence[str]] = None,
):
    """Apply consistent artist colors to Plotly or Altair charts."""

    if chart is None:
        return chart

    resolved_scheme = (scheme_name or os.getenv("ARTIST_COLOR_SCHEME", "vibrant")).lower()
    base_palette = _get_scheme_colors(resolved_scheme)

    color_map: dict[str, str] = {}
    artists_list = [str(name) for name in artists] if artists else []

    if artists_list:
        color_map.update(get_artist_color_map(artists_list))

    candidate_labels: list[str] = []

    def _extend(labels: Sequence[str] | None) -> None:
        if not labels:
            return
        for label in labels:
            if not isinstance(label, str):
                continue
            normalized = label.strip()
            if normalized and normalized not in candidate_labels:
                candidate_labels.append(normalized)

    _extend(artists_list)

    trace_labels = _extract_plotly_series_labels(chart)
    _extend(trace_labels)

    if custom_colors:
        for label, color in custom_colors.items():
            if isinstance(label, str) and isinstance(color, str):
                normalized = label.strip()
                if not normalized:
                    continue
                color_map[normalized] = color
                if normalized not in candidate_labels:
                    candidate_labels.append(normalized)

    if not candidate_labels:
        trace_count = len(getattr(chart, "data", []) or [])
        default_count = trace_count if trace_count else len(base_palette)
        candidate_labels = [f"Series {i + 1}" for i in range(default_count)]

    for idx, label in enumerate(candidate_labels):
        if label not in color_map:
            color_map[label] = base_palette[idx % len(base_palette)]

    if hasattr(chart, "update_traces") and hasattr(chart, "data"):
        return _apply_plotly_colors(chart, color_map)

    if alt is not None and hasattr(chart, "encoding") and hasattr(chart, "encode"):
        return _apply_altair_colors(chart, color_map)

    return chart


def _apply_plotly_colors(fig, color_map: Mapping[str, str]):
    """Apply ``color_map`` to a Plotly figure in place."""

    if go is None:
        return fig

    for trace in getattr(fig, "data", []):
        name = getattr(trace, "name", None)
        if not name:
            continue
        series_name = str(name).split(" (")[0]
        color = color_map.get(series_name)
        if not color:
            continue
        if hasattr(trace, "line") and trace.line is not None:
            trace.line.color = color
        if hasattr(trace, "marker") and trace.marker is not None:
            trace.marker.color = color
        elif hasattr(trace, "marker"):
            trace.marker = {"color": color}

    return fig


def _apply_altair_colors(fig, color_map: Mapping[str, str]):
    """Apply ``color_map`` to an Altair chart when possible."""

    if alt is None or not (hasattr(fig, "encoding") and hasattr(fig, "encode")):
        return fig

    color_encoding = getattr(fig.encoding, "color", None)
    if not getattr(color_encoding, "field", None):
        return fig

    domain = list(color_map.keys())
    range_colors = [color_map[label] for label in domain]

    return fig.encode(color=alt.Color(color_encoding.field, scale=alt.Scale(domain=domain, range=range_colors)))


def create_chart_annotations(
    insights: Optional[Sequence[str]] = None,
    chart_type: str = "line",
    highlight_points: Optional[Sequence[Mapping[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """Generate Plotly-style annotations for insights and highlights."""

    annotations: list[dict[str, Any]] = []
    insight_texts = [str(text) for text in (insights or [])][:5]

    layout_defaults = {
        "line": {"x": 0.02, "xanchor": "left", "y_base": 0.94, "y_step": -0.08},
        "bar": {"x": 0.98, "xanchor": "right", "y_base": 1.04, "y_step": -0.07},
        "scatter": {"x": 0.02, "xanchor": "left", "y_base": 0.9, "y_step": -0.07},
    }
    config = layout_defaults.get(chart_type, layout_defaults["line"])

    for idx, insight in enumerate(insight_texts):
        annotations.append(
            {
                "text": f"💡 {insight}",
                "xref": "paper",
                "yref": "paper",
                "x": config["x"],
                "y": config["y_base"] + idx * config["y_step"],
                "xanchor": config["xanchor"],
                "showarrow": False,
                "font": {"size": 11, "color": "#666666"},
                "bgcolor": "rgba(255, 255, 255, 0.8)",
                "bordercolor": "#CCCCCC",
                "borderwidth": 1,
                "borderpad": 4,
            }
        )

    if highlight_points:
        defaults = {
            "text": "📍",
            "xref": "x",
            "yref": "y",
            "showarrow": True,
            "arrowhead": 2,
            "arrowsize": 1,
            "arrowwidth": 2,
            "arrowcolor": "#FF6B6B",
            "font": {"size": 10, "color": "#FF6B6B"},
            "bgcolor": "rgba(255, 255, 255, 0.9)",
            "bordercolor": "#FF6B6B",
            "borderwidth": 1,
        }
        override_keys = {
            "text",
            "xref",
            "yref",
            "arrowhead",
            "arrowsize",
            "arrowwidth",
            "arrowcolor",
            "font",
            "bgcolor",
            "bordercolor",
            "borderwidth",
        }
        for point in highlight_points:
            if isinstance(point, Mapping) and {"x", "y"}.issubset(point):
                highlight = {**defaults, "x": point["x"], "y": point["y"]}
                for key in override_keys:
                    if key in point:
                        highlight[key] = point[key]
                annotations.append(highlight)

    return annotations


@bulletproof_chart(
    ChartSpec(
        name="ViewsOverTime",
        # Only require the grouping column so the chart can work with both
        # production schemas (published_at/view_count) and synthetic notebook
        # fixtures (date/views).
        required_columns=["artist_name"],
        # Allow empty DataFrames so the function can return an informative
        # placeholder chart instead of failing validation.
        min_rows=0,
        max_rows=200_000,
        timeout_sec=8,
    )
)
def views_over_time_plotly(
    df: pd.DataFrame,
    date_col: str = "published_at",
    value_col: str = "view_count",
    group_col: str = "artist_name",
    hover_col: Optional[str] = None,
    animate_by: Optional[str] = None,
    use_log_scale: bool = False,
    title: Optional[str] = None,
):
    """
    Interactive views over time chart with animation and hover features.

    Features:
    - Interactive legend (click to hide / show artists)
    - Hover tooltips with detailed metrics (ISRC, DSP data)
    - Optional animation by date with stable axis ranges
    - Fixed axis ranges to prevent jitter
    - Log scale option for handling extreme outliers

    Args:
        df: DataFrame with time-series data
        date_col: Column name for dates
        value_col: Column name for values (views)
        group_col: Column name for grouping (artists)
        hover_col: Optional column for hover labels
        animate_by: Optional column for animation frames
        use_log_scale: Whether to use log scale for y-axis (helps with outliers)
        title: Optional custom chart title
    """
    if px is None or go is None:
        raise ImportError("Plotly (express + graph_objects) is required for this chart")

    # Handle empty data gracefully with a placeholder figure so tests and
    # notebooks get a valid (but clearly marked) chart object.
    if df is None or df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=("No data available for views over time — " "please run the pipeline to fetch YouTube metrics."),
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(title=title or "Views Over Time (no data)")
        return fig

    # Sort data for proper line connections (ensure date ordering is real, not lexical)
    df_sorted = df.copy()
    df_sorted[date_col] = pd.to_datetime(df_sorted[date_col], errors="coerce")
    df_sorted = df_sorted.dropna(subset=[date_col])

    if df_sorted.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=("No valid dates available for views over time — " "please check the date column formatting."),
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(title=title or "Views Over Time (no valid dates)")
        return fig

    df_sorted = df_sorted.sort_values([date_col, group_col])

    # Prepare hover data (include ISRC / DSP if available)
    hover_data = []
    if "isrc" in df_sorted.columns:
        hover_data.append("isrc")
    if "dsp" in df_sorted.columns:
        hover_data.append("dsp")

    # Use global artist color palette for consistency across dashboard
    from youtubeviz.viz_theme import build_color_discrete_map

    artists = df_sorted[group_col].unique().tolist()
    color_map = build_color_discrete_map(artists)

    default_title = "📈 Views Over Time (Animated)" if animate_by else "📈 Views Over Time"

    # Create base chart with animation frame
    if animate_by:
        fig = px.line(
            df_sorted,
            x=date_col,
            y=value_col,
            color=group_col,
            color_discrete_map=color_map,
            hover_name=hover_col,
            hover_data=hover_data,
            animation_frame=animate_by,
            title=title or default_title,
            log_y=use_log_scale,
        )
        # Setup animation with stable axes and smooth transitions
        setup_plotly_animation(fig, autoplay=False, frame_duration=300, transition_duration=200)
    else:
        fig = px.line(
            df_sorted,
            x=date_col,
            y=value_col,
            color=group_col,
            color_discrete_map=color_map,
            hover_name=hover_col,
            hover_data=hover_data,
            title=title or default_title,
            log_y=use_log_scale,
        )

    # Pin axis ranges to prevent jitter (critical for animations)
    date_range = [df_sorted[date_col].min(), df_sorted[date_col].max()]
    if not use_log_scale:
        view_range = [0, df_sorted[value_col].max() * 1.1]  # 10% padding above max
    else:
        # For log scale, let Plotly auto-range but set minimum to avoid log(0)
        view_range = [max(1, df_sorted[value_col].min() * 0.5), df_sorted[value_col].max() * 1.5]

    fig.update_layout(
        xaxis_range=date_range,
        yaxis_range=view_range if not use_log_scale else None,  # Let Plotly handle log scale range
        hovermode="x unified",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(tickformat="%b %d %Y"),  # Include year in date labels (e.g., "Jan 01 2024")
    )

    # Apply hover enhancements with music industry context
    apply_plotly_hover_enhancements(fig, hover_data)

    return fig


@bulletproof_chart(
    ChartSpec(
        name="ViewsOverTimeAdvanced",
        required_columns=[],
        max_rows=150_000,
        timeout_sec=10,
    )
)
def views_over_time_advanced(
    df: pd.DataFrame,
    date_col: str = "published_at",
    value_col: str = "view_count",
    group_col: str = "artist_name",
    rolling_window: int = 7,
    highlight_artists: Optional[Sequence[str]] = None,
    palette_override: Optional[Mapping[str, str]] = None,
) -> "go.Figure":
    """Interactive time series with rolling averages and artist highlights.

    Features:
    - Per-artist daily series with rolling averages
    - Highlighted artists with thicker lines
    - Stable axis ranges and smooth interactions
    - Music industry hover context
    """
    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    # Ensure datetime sort
    data = df[[date_col, value_col, group_col]].dropna().copy()
    data[date_col] = pd.to_datetime(data[date_col])
    data = data.sort_values([group_col, date_col])

    # Rolling average per group
    data["rolling"] = data.groupby(group_col)[value_col].transform(
        lambda s: s.rolling(window=max(1, int(rolling_window)), min_periods=1).mean()
    )

    artists = list(data[group_col].dropna().astype(str).unique())
    color_map = dict(palette_override or {}) or get_artist_color_map(artists)
    hi = set(a for a in (highlight_artists or []) if a in artists)

    fig = go.Figure()

    # Calculate stable axis ranges
    date_range = [data[date_col].min(), data[date_col].max()]
    value_range = [0, data[value_col].max() * 1.1]

    for artist in artists:
        sub = data[data[group_col] == artist]
        color = color_map.get(artist, "#1f77b4")  # Default blue if no color
        width_main = 3.0 if artist in hi else 1.5
        width_roll = 4.0 if artist in hi else 2.0

        # Raw daily series
        fig.add_trace(
            go.Scatter(
                x=sub[date_col],
                y=sub[value_col],
                mode="lines",
                name=f"{artist} (daily)",
                line=dict(color=color, width=width_main, dash="solid"),
                hovertemplate="<b>" + artist + "</b><br>%{x|%b %d, %Y}<br>Daily: %{y:,} views<extra></extra>",
                legendgroup=artist,
                opacity=0.6,
            )
        )

        # Rolling mean
        fig.add_trace(
            go.Scatter(
                x=sub[date_col],
                y=sub["rolling"],
                mode="lines",
                name=f"{artist} ({rolling_window}d avg)",
                line=dict(color=color, width=width_roll, dash="solid"),
                hovertemplate=f"<b>{artist}</b><br>%{{x|%b %d, %Y}}<br>{rolling_window}d avg: %{{y:,.0f}} views<extra></extra>",
                legendgroup=artist,
            )
        )

    fig.update_layout(
        title="📈 Views Over Time (with rolling averages)",
        xaxis_title="Date",
        yaxis_title="Views",
        xaxis_range=date_range,
        yaxis_range=value_range,
        hovermode="x unified",
        legend_title="Artist",
        template="plotly_white",
        xaxis=dict(tickformat="%b %d %Y"),  # Include year in date labels (e.g., "Jan 01 2024")
    )

    return fig


@bulletproof_chart(
    ChartSpec(
        name="ArtistCompareAltair", required_columns=["artist_name", "view_count"], max_rows=100_000, timeout_sec=6
    )
)
def artist_compare_altair(df: pd.DataFrame, group_col: str = "artist_name", value_col: str = "view_count"):
    """
    Interactive Altair bar chart with brush selection for artist comparison.

    Features:
    - Brush selection for filtering
    - Click selection for multi-artist comparison
    - Linked interactions with other charts
    """
    if alt is None:
        # Provide aggregated DataFrame as fallback
        return df.groupby(group_col, as_index=False)[value_col].sum().sort_values(value_col, ascending=False)

    # Aggregate data
    agg = df.groupby(group_col, as_index=False)[value_col].sum()

    # Create brush and click selections
    brush = create_altair_brush_selection()
    click = create_altair_click_selection()

    # Base chart with selections
    base = alt.Chart(agg).add_params(brush, click)

    # Main bar chart
    bars = (
        base.mark_bar(stroke="black", strokeWidth=1)
        .encode(
            x=alt.X(f"{group_col}:N", sort="-y", title="Artist"),
            y=alt.Y(f"{value_col}:Q", title="Total Views"),
            color=alt.condition(click, alt.Color(f"{group_col}:N", legend=None), alt.value("lightgray")),
            opacity=alt.condition(brush, alt.value(1.0), alt.value(0.7)),
            tooltip=[
                alt.Tooltip(f"{group_col}:N", title="Artist"),
                alt.Tooltip(f"{value_col}:Q", title="Total Views", format=","),
            ],
        )
        .properties(width=600, height=400, title="Artist Performance Comparison (Interactive)")
    )

    # Add text labels on bars
    text = base.mark_text(align="center", baseline="bottom", dy=-5, fontSize=10).encode(
        x=alt.X(f"{group_col}:N", sort="-y"),
        y=alt.Y(f"{value_col}:Q"),
        text=alt.Text(f"{value_col}:Q", format=".2s"),
        opacity=alt.condition(brush, alt.value(1.0), alt.value(0.5)),
    )

    # Return base bar chart to satisfy environments/tests that expect a Chart with a 'mark' attribute
    return bars


@bulletproof_chart(
    ChartSpec(name="DivergentSentimentChart", required_columns=["artist_name"], max_rows=100_000, timeout_sec=8)
)
def create_divergent_sentiment_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    sentiment_col: str = "sentiment",
    title: Optional[str] = None,
):
    """
    Create a divergent stacked bar chart showing sentiment breakdown by artist.

    Args:
        df: DataFrame with sentiment data
        artist_col: Column name for artist names (default: "artist_name")
        sentiment_col: Column name for sentiment categories (default: "sentiment")
        title: Optional chart title

    Features:
    - Divergent bars (negative left, positive right, horizontal layout)
    - Interactive hover with detailed metrics
    - Stable axis ranges and smooth interactions
    - Music industry color scheme

    Returns:
        Plotly figure with divergent bar chart
    """
    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    # Handle missing sentiment column by creating proxy from engagement
    if sentiment_col not in df.columns:
        # Create sentiment proxy from engagement metrics
        df = df.copy()
        likes_col = "like_count" if "like_count" in df.columns else "likes"
        comments_col = "comment_count" if "comment_count" in df.columns else "comments"
        views_col = "view_count" if "view_count" in df.columns else "views"

        likes = df[likes_col] if likes_col in df.columns else pd.Series(0, index=df.index, dtype="float")
        comments = df[comments_col] if comments_col in df.columns else pd.Series(0, index=df.index, dtype="float")
        views = df[views_col] if views_col in df.columns else pd.Series(0, index=df.index, dtype="float")

        # Calculate engagement rate
        likes = pd.to_numeric(likes, errors="coerce").fillna(0)
        comments = pd.to_numeric(comments, errors="coerce").fillna(0)
        views = pd.to_numeric(views, errors="coerce").fillna(0).clip(lower=1)
        df["engagement_rate"] = (likes + comments) / views

        # Categorize into sentiment buckets
        df[sentiment_col] = pd.cut(
            df["engagement_rate"],
            bins=[0, 0.02, 0.04, float("inf")],
            labels=["negative", "neutral", "positive"],
            include_lowest=True,
        )

    # Calculate sentiment percentages by artist
    sentiment_counts = df.groupby([artist_col, sentiment_col]).size().unstack(fill_value=0)
    sentiment_pct = sentiment_counts.div(sentiment_counts.sum(axis=1), axis=0) * 100

    # Reset index for plotting
    sentiment_pct = sentiment_pct.reset_index()

    # Ensure we have required sentiment columns
    for col in ["positive", "negative", "neutral"]:
        if col not in sentiment_pct.columns:
            sentiment_pct[col] = 0

    # Make negative values negative for divergent chart
    sentiment_pct["negative_display"] = -sentiment_pct["negative"]

    # Create divergent bar chart
    fig = go.Figure()

    # Add negative bars (left side)
    negative_counts = sentiment_counts.get("negative", pd.Series(0, index=sentiment_counts.index))
    neutral_counts = sentiment_counts.get("neutral", pd.Series(0, index=sentiment_counts.index))
    positive_counts = sentiment_counts.get("positive", pd.Series(0, index=sentiment_counts.index))

    fig.add_trace(
        go.Bar(
            name="Negative",
            x=sentiment_pct["negative_display"],
            y=sentiment_pct[artist_col],
            orientation="h",
            marker_color="#DC143C",  # Crimson
            text=[f"{val:.1f}%" for val in sentiment_pct["negative"]],
            textposition="inside",
            hovertemplate="<b>%{y}</b><br>Negative: %{text}<br>Count: %{customdata}<extra></extra>",
            customdata=negative_counts.reindex(sentiment_pct[artist_col]).to_list(),
        )
    )

    # Add neutral bars (center)
    if "neutral" in sentiment_pct.columns and sentiment_pct["neutral"].sum() > 0:
        fig.add_trace(
            go.Bar(
                name="Neutral",
                x=sentiment_pct["neutral"],
                y=sentiment_pct[artist_col],
                orientation="h",
                marker_color="#FFD700",  # Gold
                text=[f"{val:.1f}%" for val in sentiment_pct["neutral"]],
                textposition="inside",
                hovertemplate="<b>%{y}</b><br>Neutral: %{text}<br>Count: %{customdata}<extra></extra>",
                customdata=neutral_counts.reindex(sentiment_pct[artist_col]).to_list(),
            )
        )

    # Add positive bars (right side)
    fig.add_trace(
        go.Bar(
            name="Positive",
            x=sentiment_pct["positive"],
            y=sentiment_pct[artist_col],
            orientation="h",
            marker_color="#2E8B57",  # Sea green
            text=[f"{val:.1f}%" for val in sentiment_pct["positive"]],
            textposition="inside",
            hovertemplate="<b>%{y}</b><br>Positive: %{text}<br>Count: %{customdata}<extra></extra>",
            customdata=positive_counts.reindex(sentiment_pct[artist_col]).to_list(),
        )
    )

    # Update layout with stable ranges
    fig.update_layout(
        title=title or "Sentiment Breakdown by Artist",
        xaxis_title="Sentiment Percentage",
        yaxis_title="Artist",
        barmode="relative",
        hovermode="y unified",
        xaxis=dict(range=[-100, 100], zeroline=True, zerolinecolor="black", zerolinewidth=2),
        template="plotly_white",
    )

    # Add zero line annotation
    fig.add_vline(x=0, line_dash="solid", line_color="black", line_width=2)

    return fig


def create_sentiment_cluster_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    aspect_col: str = "sentiment_aspect",
    sentiment_col: str = "sentiment_category",
    sentiment_score_col: Optional[str] = None,
    category_col: Optional[str] = None,
) -> "go.Figure":
    """Compatibility wrapper for Chart #2: Sentiment Model Categories Heatmap.

    Historically this chart lived in :mod:`advanced_charts` as
    :func:`create_sentiment_cluster_heatmap`. The TDD tests and notebooks
    import ``create_sentiment_cluster_chart`` from :mod:`youtubeviz.charts`, so
    this function simply delegates to the advanced implementation while
    keeping a stable public API.
    """

    if df is None:
        raise ValueError("[SentimentCluster] DataFrame 'df' is None")

    # Local import to avoid circular dependencies at module import time.
    from .advanced_charts import create_sentiment_cluster_heatmap

    if category_col:
        sentiment_col = category_col

    return create_sentiment_cluster_heatmap(
        df=df,
        artist_col=artist_col,
        aspect_col=aspect_col,
        sentiment_col=sentiment_col,
    )


def create_sentiment_wordcloud(
    comments: Sequence[str],
    sentiment_type: str = "positive",
    top_n: int = 30,
) -> dict[str, Any]:
    """Build a lightweight word frequency payload for sentiment word clouds."""
    if comments is None:
        raise ValueError("[SentimentWordcloud] comments cannot be None")

    import re
    from collections import Counter

    tokens: list[str] = []
    for comment in comments:
        if not comment:
            continue
        tokens.extend(re.findall(r"[a-z0-9']+", str(comment).lower()))

    stopwords = {
        "the",
        "and",
        "a",
        "an",
        "to",
        "of",
        "in",
        "is",
        "it",
        "this",
        "that",
        "for",
        "on",
        "with",
        "at",
        "by",
        "be",
        "are",
        "was",
        "were",
        "so",
        "but",
        "or",
        "as",
        "i",
        "you",
        "we",
        "they",
        "he",
        "she",
        "them",
        "us",
    }
    cleaned = [token for token in tokens if token not in stopwords]
    counts = Counter(cleaned).most_common(top_n)

    return {
        "sentiment_type": sentiment_type,
        "top_terms": [{"term": term, "count": count} for term, count in counts],
        "total_terms": len(cleaned),
    }


def create_sentiment_timeline(
    df: pd.DataFrame,
    date_col: str = "date",
    sentiment_col: str = "avg_sentiment",
    artist_col: str = "artist",
) -> "go.Figure":
    """Plot average sentiment over time by artist."""
    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[SentimentTimeline] DataFrame 'df' is None")

    required_cols = [date_col, sentiment_col, artist_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(f"[SentimentTimeline] Missing required columns: {missing}")

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No sentiment timeline data available yet",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(title="Sentiment Over Time (needs real data)", template="plotly_white")
        return fig

    work = df.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work = work.dropna(subset=[date_col])

    if work.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No valid dates available for sentiment timeline",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(title="Sentiment Over Time (no valid dates)", template="plotly_white")
        return fig

    fig = px.line(
        work.sort_values(date_col),
        x=date_col,
        y=sentiment_col,
        color=artist_col,
        title="How does sentiment shift over time by artist?",
    )
    fig.update_layout(template="plotly_white", hovermode="x unified")
    return fig


def create_content_type_breakdown_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    content_type_col: str = "content_type",
    views_col: str = "views",
) -> "go.Figure":
    """Chart #10: Content type breakdown (MV / Lyric / Visualizer) by artist.

    This restores the high-level behavior expected by the notebook and TDD tests:
    - Accepts flexible column names via ``artist_col``, ``content_type_col``, ``views_col``
    - Returns a Plotly figure (never ``None``)
    - Handles empty data frames gracefully by rendering an explanatory placeholder chart
    - Fails loudly with an informative error when required columns are missing
    """
    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[ContentTypeBreakdown] DataFrame 'df' is None")

    required_cols = [artist_col, content_type_col, views_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(
            f"[ContentTypeBreakdown] Missing required columns: {missing}. " f"Available columns: {list(df.columns)}"
        )

    # Handle empty data gracefully: return a valid but informative figure
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=(
                "\ud83d\udccb No content data available yet \u2014 add real YouTube videos "
                "to see the content mix by format."
            ),
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=12),
        )
        fig.update_layout(
            title="Content Mix by Format (needs real data)",
            template="plotly_white",
        )
        return fig

    # Aggregate total views by artist and content type
    summary = (
        df[[artist_col, content_type_col, views_col]]
        .dropna(subset=[artist_col, content_type_col])
        .groupby([artist_col, content_type_col], as_index=False)[views_col]
        .sum()
    )

    if summary.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="\ud83d\udccb No usable content rows after filtering \u2014 check content type labels.",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=12),
        )
    else:
        # Stacked bar chart: which content formats are driving views for each artist?
        fig = px.bar(
            summary,
            x=artist_col,
            y=views_col,
            color=content_type_col,
            barmode="stack",
            title="Which content formats are driving YouTube views by artist?",
            labels={
                artist_col: "Artist",
                views_col: "Total Views",
                content_type_col: "Content Format",
            },
        )
        fig.update_layout(
            template="plotly_white",
            legend_title_text="Content format",
        )

    return fig


def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create pie chart showing content distribution across categories.

    Args:
        df: DataFrame with content data
        category_cols: List of category column names
        artist_col: Column name for artist names
        content_type_col: Column name for content types

    Returns:
        Plotly figure with pie chart

    Note: This is a placeholder implementation. The original function definitions
    were malformed during refactoring and have been consolidated here.
    """
    # Simple pie chart implementation
    if px is None:
        return df

    # Use first category column if provided
    if category_cols and len(category_cols) > 0:
        category_col = category_cols[0]
    else:
        category_col = content_type_col

    # Create pie chart
    if category_col in df.columns:
        value_counts = df[category_col].value_counts()
        fig = px.pie(values=value_counts.values, names=value_counts.index, title="Content Distribution")
        return fig

    return df


def create_isrc_balance_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    isrc_col: str = "has_isrc",
    views_col: str = "views",
) -> "go.Figure":
    """Chart #8: ISRC vs non-ISRC balance by artist.

    This version is intentionally conservative and test-friendly:

    - Accepts a boolean-style column (default ``has_isrc``) or a raw code column
      that can be coerced to boolean
    - Raises an explicit :class:`KeyError` when the configured ISRC column is
      missing (tests assert that the error message contains the column name)
    - Returns a valid placeholder ``go.Figure`` when the data frame is empty
    - Visualizes the share of views coming from ISRC vs non-ISRC content for
      each artist as a 100% stacked bar chart
    """

    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[ISRCBalance] DataFrame 'df' is None")

    required_cols = [artist_col, isrc_col, views_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        # Tests for missing-column behavior expect a KeyError mentioning the
        # requested ISRC column name (e.g. "has_isrc").
        raise KeyError(
            f"[ISRCBalance] Missing required columns: {missing}. "
            f"Expected at least '{isrc_col}' plus artist and views columns."
        )

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No content data available yet for ISRC analysis",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="ISRC vs Non-ISRC Content Balance (needs real data)",
            template="plotly_white",
        )
        return fig

    work = df[[artist_col, isrc_col, views_col]].copy()

    # Coerce the ISRC column to a clean boolean: True = has ISRC / music video,
    # False = no ISRC / content clip. Any nulls are treated as False.
    work[isrc_col] = work[isrc_col].fillna(False).astype(bool)

    grouped = work.groupby([artist_col, isrc_col], as_index=False).agg(
        video_count=(views_col, "size"), total_views=(views_col, "sum")
    )

    if grouped.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No usable rows for ISRC analysis after filtering",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="ISRC vs Non-ISRC Content Balance",
            template="plotly_white",
        )
        return fig

    grouped["isrc_label"] = grouped[isrc_col].map({True: "Has ISRC", False: "No ISRC"})

    # Compute share of views per artist so each bar sums to 100%.
    grouped["total_views"] = pd.to_numeric(grouped["total_views"], errors="coerce").fillna(0).clip(lower=0)
    totals = grouped.groupby(artist_col)["total_views"].transform(lambda s: s.sum() or 1.0)
    grouped["view_share"] = grouped["total_views"] / totals

    fig = px.bar(
        grouped,
        x=artist_col,
        y="view_share",
        color="isrc_label",
        barmode="stack",
        labels={
            artist_col: "Artist",
            "view_share": "Share of Views",
            "isrc_label": "Content type",
        },
        title="Are music videos or content clips driving views by artist?",
    )

    fig.update_layout(
        template="plotly_white",
        yaxis=dict(tickformat=".0%", range=[0, 1]),
        legend_title_text="Content mix",
    )

    return fig


def create_duration_breakdown_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    duration_col: str = "duration_seconds",
    views_col: str = "views",
    short_form_threshold: int = 180,
) -> "go.Figure":
    """Chart #9: Short-form vs long-form video breakdown by artist.

    Splits each artist's catalog into short-form and long-form buckets using a
    configurable time threshold (default: 3 minutes), and shows which bucket
    is driving the most views.
    """

    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[DurationBreakdown] DataFrame 'df' is None")

    required_cols = [artist_col, duration_col, views_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(
            f"[DurationBreakdown] Missing required columns: {missing}. "
            f"Expected at least '{duration_col}' plus artist and views columns."
        )

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No content data available yet for duration analysis",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Short-form vs Long-form Performance (needs real data)",
            template="plotly_white",
        )
        return fig

    work = df[[artist_col, duration_col, views_col]].dropna(subset=[artist_col, duration_col]).copy()

    minutes = short_form_threshold / 60
    short_label = f"Short-form (<= {minutes:g} min)"
    long_label = f"Long-form (> {minutes:g} min)"

    work["length_bucket"] = work[duration_col].apply(lambda v: short_label if v <= short_form_threshold else long_label)

    summary = (
        work.groupby([artist_col, "length_bucket"], as_index=False)[views_col]
        .sum()
        .rename(columns={views_col: "total_views"})
    )

    if summary.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No usable rows for duration analysis after filtering",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Short-form vs Long-form Performance",
            template="plotly_white",
        )
        return fig

    fig = px.bar(
        summary,
        x=artist_col,
        y="total_views",
        color="length_bucket",
        barmode="stack",
        labels={
            artist_col: "Artist",
            "total_views": "Total Views",
            "length_bucket": "Video length",
        },
        title="Are short-form or long-form videos winning by artist?",
    )

    fig.update_layout(template="plotly_white", legend_title_text="Video length")

    return fig


def create_artist_content_comparison_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    content_type_col: str = "video_type",
    views_col: str = "views",
) -> "go.Figure":
    """Matrix-style comparison of artist performance by content type.

    Renders a heatmap where rows are artists, columns are content formats, and
    cell values represent total views. This is intentionally simple and
    robust so that notebook and test code can rely on it as a building block
    for richer dashboards.
    """

    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[ArtistContentComparison] DataFrame 'df' is None")

    required_cols = [artist_col, content_type_col, views_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(
            f"[ArtistContentComparison] Missing required columns: {missing}. "
            f"Expected artist, content type, and views columns."
        )

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No content data available yet for artist comparison",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Artist vs Content Type Performance (needs real data)",
            template="plotly_white",
        )
        return fig

    work = df[[artist_col, content_type_col, views_col]].dropna(subset=[artist_col, content_type_col]).copy()

    summary = (
        work.groupby([artist_col, content_type_col], as_index=False)[views_col]
        .sum()
        .rename(columns={views_col: "total_views"})
    )

    if summary.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No usable rows for artist/content comparison after filtering",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Artist vs Content Type Performance",
            template="plotly_white",
        )
        return fig

    pivot = summary.pivot(index=artist_col, columns=content_type_col, values="total_views").fillna(0)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            colorscale="YlGnBu",
            hovertemplate="<b>%{y}</b><br>Content: %{x}<br>Views: %{z:,}<extra></extra>",
            colorbar=dict(title="Total views"),
        )
    )

    fig.update_layout(
        title="Artist vs Content Type Performance Matrix",
        xaxis_title="Content type",
        yaxis_title="Artist",
        template="plotly_white",
    )

    return fig


def create_roster_content_overview_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    content_type_col: str = "video_type",
    views_col: str = "views",
) -> "go.Figure":
    """High-level roster content mix overview.

    Aggregates views across the roster by content format to answer
    "what formats does this roster lean on most?". Designed as a
    simple, reliable executive summary chart.
    """

    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[RosterContentOverview] DataFrame 'df' is None")

    required_cols = [artist_col, content_type_col, views_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(
            f"[RosterContentOverview] Missing required columns: {missing}. "
            f"Expected artist, content type, and views columns."
        )

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No content data available yet for roster overview",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Roster Content Mix (needs real data)",
            template="plotly_white",
        )
        return fig

    work = df[[artist_col, content_type_col, views_col]].dropna(subset=[artist_col, content_type_col]).copy()

    summary = (
        work.groupby(content_type_col, as_index=False)
        .agg(
            total_views=(views_col, "sum"),
            video_count=(views_col, "size"),
            unique_artists=(artist_col, "nunique"),
        )
        .sort_values("total_views", ascending=False)
    )

    if summary.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No usable rows for roster overview after filtering",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Roster Content Mix",
            template="plotly_white",
        )
        return fig

    fig = px.bar(
        summary,
        x=content_type_col,
        y="total_views",
        hover_data={"video_count": True, "unique_artists": True},
        labels={
            content_type_col: "Content format",
            "total_views": "Total Views",
            "video_count": "# of videos",
            "unique_artists": "# of artists",
        },
        title="Roster Content Mix: which formats dominate the release strategy?",
    )

    fig.update_layout(template="plotly_white")

    return fig


def create_genre_context_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    genre_col: str = "genre",
    views_col: str = "views",
) -> "go.Figure":
    """Genre context chart for new signees.

    Uses :func:`youtubeviz.content.analyze_genre_context` under the hood to
    compute performance by genre, then visualizes total performance per genre.
    Tests call this with a synthetic ``genre`` column added to the
    ``sample_video_data`` fixture.
    """

    if px is None or go is None:
        raise ImportError("Plotly is required for this chart")

    if df is None:
        raise ValueError("[GenreContext] DataFrame 'df' is None")

    required_cols = [artist_col, genre_col, views_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise KeyError(
            f"[GenreContext] Missing required columns: {missing}. " f"Expected artist, genre, and views columns."
        )

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data available yet for genre context analysis",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
        )
        fig.update_layout(
            title="Genre context for new signees (needs real data)",
            template="plotly_white",
        )
        return fig

    # Normalize column names for the analysis helper while keeping the
    # original DataFrame unchanged for downstream callers.
    analysis_df = df[[artist_col, genre_col, views_col]].rename(columns={views_col: "views"})

    analysis = analyze_genre_context(
        df=analysis_df,
        artist_col=artist_col,
        genre_col=genre_col,
        performance_col="views",
    )

    records = analysis.get("genre_performance")
    genre_df = pd.DataFrame(records or [])

    if genre_df.empty or genre_col not in genre_df.columns:
        # Fallback: simple aggregation directly from the provided DataFrame.
        fallback = df.groupby(genre_col)[views_col].sum().reset_index()
        fallback.columns = [genre_col, "total_views"]
        genre_df = fallback
        value_col = "total_views"
    else:
        value_col = "total_performance"

    fig = px.bar(
        genre_df,
        x=genre_col,
        y=value_col,
        labels={genre_col: "Genre", value_col: "Total Views"},
        title="Which genres are breaking through for new signees?",
    )

    fig.update_layout(template="plotly_white")

    return fig


def create_venn_diagram_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    categories: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Compute Venn-style artist strengths across simple strategy lenses.

    This helper intentionally returns a plain data structure rather than a
    rendered chart so tests and notebooks can decide how to visualize it
    (classic Venn diagram, UpSet plot, etc.).

    The default categories line up with the TDD tests:

    - ``"high_views"``: artists in the top quartile for total views
    - ``"high_engagement"``: artists in the top quartile for engagement rate
      ( (likes + comments) / views ) when those columns exist
    - ``"consistent_uploads"``: artists in the top quartile for upload count
    """

    if df is None:
        raise ValueError("[Venn] DataFrame 'df' is None")

    if artist_col not in df.columns:
        raise KeyError(f"[Venn] Missing required artist column '{artist_col}'")

    if categories is None:
        categories = ["high_views", "high_engagement", "consistent_uploads"]

    # Normalize to list for deterministic iteration
    categories = list(categories)

    # Group once to derive metrics
    grouped = df.groupby(artist_col)

    strengths: Dict[str, set[str]] = {name: set() for name in categories}

    # High views
    if "high_views" in strengths and "views" in df.columns:
        views_total = grouped["views"].sum()
        if not views_total.empty:
            threshold = views_total.quantile(0.75)
            for artist, total in views_total.items():
                if total >= threshold:
                    strengths["high_views"].add(artist)

    # High engagement
    if "high_engagement" in strengths and {"likes", "comments", "views"}.issubset(df.columns):
        likes = grouped["likes"].sum()
        comments = grouped["comments"].sum()
        views_total = grouped["views"].sum().clip(lower=1)
        engagement_rate = (likes + comments) / views_total
        if not engagement_rate.empty:
            threshold = engagement_rate.quantile(0.75)
            for artist, rate in engagement_rate.items():
                if rate >= threshold:
                    strengths["high_engagement"].add(artist)

    # Consistent uploads (proxy = upload count)
    if "consistent_uploads" in strengths:
        upload_counts = grouped.size()
        if not upload_counts.empty:
            threshold = upload_counts.quantile(0.75)
            for artist, count in upload_counts.items():
                if count >= threshold:
                    strengths["consistent_uploads"].add(artist)

    # Build overlaps for 2-way intersections (sufficient for tests and most
    # storytelling use-cases).
    overlaps: Dict[str, list[str]] = {}
    for i in range(len(categories)):
        for j in range(i + 1, len(categories)):
            c1, c2 = categories[i], categories[j]
            s1, s2 = strengths.get(c1, set()), strengths.get(c2, set())
            if not s1 or not s2:
                continue
            key = f"{c1} & {c2}"
            overlaps[key] = sorted(s1 & s2)

    return {
        "categories": {name: sorted(artists) for name, artists in strengths.items()},
        "overlaps": overlaps,
    }


# Note: Additional legacy chart functions were removed during refactoring due
# to syntax errors. The functions defined in this module represent the
# supported, test-backed surface area for MusicScope charts.
