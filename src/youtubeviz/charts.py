from __future__ import annotations

import json
import os
from typing import Any, List, Mapping, Optional, Sequence

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

try:
    import plotly.express as px
    import plotly.graph_objects as go
except Exception:  # pragma: no cover - optional
    px = None
    go = None

try:
    import altair as alt
except Exception:  # pragma: no cover - optional
    alt = None


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
    # Plotly category10 - like fallback palette
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
    # Input validation - reject DataFrames explicitly
    if hasattr(artists, "columns"):  # This catches pandas DataFrames
        raise TypeError("artists cannot be a DataFrame. Pass a list / array of artist names instead.")

    if not isinstance(artists, (list, tuple, pd.Index, pd.Series)) and not hasattr(artists, "__iter__"):
        raise TypeError(f"artists must be a sequence of strings, got {type(artists)}")

    # Convert to list and validate string content
    try:
        artist_list = list(artists)
        if not all(isinstance(artist, str) for artist in artist_list):
            raise TypeError("All artists must be strings")
    except Exception as e:
        raise TypeError(f"Could not convert artists to list of strings: {e}")

    # Load user - specified mapping from env (JSON or file path)
    env_map: dict[str, str] = {}
    # (a) JSON directly
    raw = os.getenv("ARTIST_COLORS_JSON")
    if raw:
        try:
            env_map = json.loads(raw)
        except Exception:
            env_map = {}
    # (b) Or from a JSON file path via ARTIST_COLORS_FILE
    if not env_map:
        path = os.getenv("ARTIST_COLORS_FILE")
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf - 8") as fh:
                    env_map = json.load(fh)
            except Exception:
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
        "font_family": "system - ui, -apple - system, Segoe UI, Roboto, sans - serif",
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
    """Generate Plotly - style annotations for insights and highlights."""

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
        required_columns=["published_at", "view_count", "artist_name"],
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
):
    """
    Interactive views over time chart with animation and hover features.

    Features:
    - Interactive legend (click to hide / show artists)
    - Hover tooltips with detailed metrics (ISRC, DSP data)
    - Optional animation by date with stable axis ranges
    - Fixed axis ranges to prevent jitter
    """
    if px is None:
        raise ImportError("Plotly is required for this chart")

    # Sort data for proper line connections
    df_sorted = df.sort_values([date_col, group_col])

    # Prepare hover data (include ISRC / DSP if available)
    hover_data = []
    if "isrc" in df_sorted.columns:
        hover_data.append("isrc")
    if "dsp" in df_sorted.columns:
        hover_data.append("dsp")

    # Create base chart with animation frame
    if animate_by:
        fig = px.line(
            df_sorted,
            x=date_col,
            y=value_col,
            color=group_col,
            hover_name=hover_col,
            hover_data=hover_data,
            animation_frame=animate_by,
            title="📈 Views Over Time (Animated)",
        )
        # Setup animation with stable axes and smooth transitions
        setup_plotly_animation(fig, autoplay=False, frame_duration=300, transition_duration=200)
    else:
        fig = px.line(
            df_sorted,
            x=date_col,
            y=value_col,
            color=group_col,
            hover_name=hover_col,
            hover_data=hover_data,
            title="📈 Views Over Time",
        )

    # Pin axis ranges to prevent jitter (critical for animations)
    date_range = [df_sorted[date_col].min(), df_sorted[date_col].max()]
    view_range = [0, df_sorted[value_col].max() * 1.1]  # 10% padding above max

    fig.update_layout(
        xaxis_range=date_range,
        yaxis_range=view_range,
        hovermode="x unified",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Apply hover enhancements with music industry context
    apply_plotly_hover_enhancements(fig, hover_data)

    return fig


@bulletproof_chart(
    ChartSpec(
        name="ViewsOverTimeAdvanced",
        required_columns=["published_at", "view_count", "artist_name"],
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
    - Per - artist daily series with rolling averages
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
                hovertemplate=f"<b>{
                    artist}</b><br>%{{x|%b %d, %Y}}<br>{rolling_window}d avg: %{{y:,.0f}} views<extra></extra>",
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
    - Click selection for multi - artist comparison
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

    return (bars + text).resolve_scale(color="independent")


@bulletproof_chart(
    ChartSpec(name="DivergentSentimentChart", required_columns=["artist_name"], max_rows=100_000, timeout_sec=8)
)
def create_divergent_sentiment_chart(
    df: pd.DataFrame,
    def create_content_distribution_pie_chart(  # noqa: E999
    df: pd.DataFrame,
    category_cols: Optional[List[str]]=None,
    artist_col: Optional[str]=None,
    content_type_col: str="content_type",
):
    """
    Create a divergent stacked bar chart showing sentiment breakdown by artist.

    Features:
    - Divergent bars (negative left, positive right)
    - Interactive hover with detailed metrics
    - Stable axis ranges and smooth interactions
    - Music industry color scheme
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

        # Calculate engagement rate
        df["engagement_rate"] = (df.get(likes_col, 0).fillna(0) + df.get(comments_col, 0).fillna(0)) / df.get(
            views_col, 1
    ).fillna(1).clip(lower=1)

        # Categorize into sentiment buckets
        df[sentiment_col] = pd.cut(
            df["engagement_rate"], bins=[0, 0.02, 0.04, float("inf")], labels=["negative", "neutral", "positive"]
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
    fig.add_trace(
        go.Bar(
            name="Negative",
            x=sentiment_pct[artist_col],
            y=sentiment_pct["negative_display"],
            marker_color="#DC143C",  # Crimson
            text=[f"{val:.1f}%" for val in sentiment_pct["negative"]],
            textposition="inside",
            hovertemplate="<b>%{x}</b><br>Negative: %{text}<br>Count: %{customdata}<extra></extra>",
            customdata=sentiment_counts.get("negative", [0] * len(sentiment_pct)),
        )
    )

    # Add neutral bars (center)
    if "neutral" in sentiment_pct.columns and sentiment_pct["neutral"].sum() > 0:
        fig.add_trace(
            go.Bar(
                name="Neutral",
                x=sentiment_pct[artist_col],
                y=sentiment_pct["neutral"],
                marker_color="#FFD700",  # Gold
                text=[f"{val:.1f}%" for val in sentiment_pct["neutral"]],
                textposition="inside",
                hovertemplate="<b>%{x}</b><br>Neutral: %{text}<br>Count: %{customdata}<extra></extra>",
                customdata=sentiment_counts.get("neutral", [0] * len(sentiment_pct)),
            )
        )

    # Add positive bars (right side)
    fig.add_trace(
        go.Bar(
            name="Positive",
            x=sentiment_pct[artist_col],
            y=sentiment_pct["positive"],
            marker_color="#2E8B57",  # Sea green
            text=[f"{val:.1f}%" for val in sentiment_pct["positive"]],
            textposition="inside",
            hovertemplate="<b>%{x}</b><br>Positive: %{text}<br>Count: %{customdata}<extra></extra>",
            customdata=sentiment_counts.get("positive", [0] * len(sentiment_pct)),
        )
    )

    # Update layout with stable ranges
    fig.update_layout(
        title=title or "Sentiment Breakdown by Artist",
        xaxis_title="Artist",
        yaxis_title="Sentiment Percentage",
        barmode="relative",
        hovermode="x unified",
        yaxis=dict(range=[-100, 100], zeroline=True, zerolinecolor="black", zerolinewidth=2),
        template="plotly_white",
    )

    # Add zero line annotation
    fig.add_hline(y=0, line_dash="solid", line_color="black", line_width=2)

    return fig


    def create_sentiment_cluster_chart(
        df: pd.DataFrame, sentiment_score_col: str, category_col: str, artist_col: str, title: Optional[str]=None
        ):
    """
    Create a scatter plot showing sentiment score clustering by category and artist.

    Args:
        df: DataFrame with sentiment data
        sentiment_score_col: Column name for sentiment scores
        category_col: Column name for sentiment categories
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create timeline chart showing sentiment changes over time.

    Args:
        df: DataFrame with time - series sentiment data
        date_col: Column name for dates
        sentiment_col: Column name for sentiment scores
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]]=None,
    artist_col: Optional[str]=None,
    content_type_col: str="content_type",
):
    """
    Create overlapping circle / Venn diagram showing what artists are doing well as a whole.

    Note: True Venn diagrams are complex in Plotly. This creates an alternative
    visualization showing overlapping strengths using bubble charts.

    Args:
        df: DataFrame with content data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create balance analysis chart: videos with ISRC vs without(music videos vs content videos).

    Args:
        df: DataFrame with ISRC data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]]=None,
    artist_col: Optional[str]=None,
    content_type_col: str="content_type",
):
        artist_df = df[df[artist_col] == artist]

        isrc_views = artist_df[artist_df[isrc_col] is True][views_col].sum()
        non_isrc_views = artist_df[artist_df[isrc_col] is False][views_col].sum()

        isrc_data.extend(
            [
                {artist_col: artist, "content_category": "Music Content (ISRC)", "views": isrc_views},
                {artist_col: artist, "content_category": "Content Videos (No ISRC)", "views": non_isrc_views},
            ]
        )

    isrc_df = pd.DataFrame(isrc_data)

    # Create stacked bar chart
    fig = px.bar(
        isrc_df,
        x=artist_col,
        y="views",
        color="content_category",
        title="Music Content vs Content Videos Balance",
        labels={"views": "Total Views", artist_col: "Artist", "content_category": "Content Category"},
        color_discrete_map={
            "Music Content (ISRC)": "#2E8B57",  # Sea green
            "Content Videos (No ISRC)": "#FF6347",  # Tomato
        },
    )

    fig.update_layout(barmode="stack")

    return fig


    def create_duration_breakdown_chart(
        df: pd.DataFrame, artist_col: str, duration_col: str, views_col: str, short_form_threshold: int=300
        ):
    """
    Create short - form vs long - form video breakdown with view totals.

    Args:
        df: DataFrame with duration data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create chart showing music video count vs lyric video count vs visualizer count vs other content.

    Args:
        df: DataFrame with content data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]]=None,
    artist_col: Optional[str]=None,
    content_type_col: str="content_type",
):
    """
    Create side - by - side artist comparison chart.

    Args:
        df: DataFrame with content data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create combined roster analysis showing overall content strategy.

    Args:
        df: DataFrame with content data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]]=None,
    artist_col: Optional[str]=None,
    content_type_col: str="content_type",
):
    """
    Create stacked bar chart for content breakdown.

    Args:
        df: DataFrame with content data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create pie chart for content distribution.

    Args:
        df: DataFrame with content data
        content_type_col: Column name for content types
        views_col: Column name for view counts

    Returns:
        Plotly figure with pie chart
    """
    if px is None:
        return df

    fig = px.pie(df, values=views_col, names=content_type_col, title="Content Distribution by Views")

    fig.update_traces(textposition="inside", textinfo="percent + label")

    return fig


    def create_artist_strategy_comparison_chart(df: pd.DataFrame, artist_col: str):
    """
    Create comparison chart for artist content strategies.

    Args:
        df: DataFrame with artist strategy data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]]=None,
    artist_col: Optional[str]=None,
    content_type_col: str="content_type",
):
    """Create a linked Altair scatter + detail view with selection.

    Returns an Altair VConcatChart or the original dataframe if Altair isn't present.
    """
    if alt is None:
        return df

    # Ensure columns exist
    for c in (
        x_col,
        y_col,
    ):
        if c not in df.columns:
            raise ValueError(f"column '{c}' not found in dataframe")

    selection = alt.selection_interval(encodings=["x", "y"]) | alt.selection_single(
        on="mouseover", fields=[group_col] if group_col else []
    )

    base = (
        alt.Chart(df)
        .mark_circle(size=60)
        .encode(
            x=alt.X(x_col + ":Q"),
            y=alt.Y(y_col + ":Q"),
            color=alt.Color(group_col + ":N") if group_col else alt.value("steelblue"),
            tooltip=[hover_col] if hover_col else None,
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.2)),
        )
        .add_selection(selection)
        .interactive()
    )

    # detail table: show top selected points
    detail = (
        alt.Chart(df)
        .transform_filter(selection)
        .mark_text(align="left")
        .encode(
            y=alt.Y("rank:O", axis=None),
            text=alt.Text(hover_col + ":N") if hover_col and hover_col in df.columns else alt.Text(x_col + ":Q"),
        )
    )

    try:
        combo = alt.vconcat(base, detail)
        return combo
    except Exception:
        return base


    __all__ = [
        "views_over_time_plotly",
        "artist_compare_altair",
        "linked_scatter_detail_altair",
        "get_artist_color_map",
        "enhance_chart_beauty",
        "apply_color_scheme",
        "create_chart_annotations",
        "create_divergent_sentiment_chart",
        "create_sentiment_cluster_chart",
        "create_sentiment_wordcloud",
        "create_sentiment_timeline",
        "create_artist_strengths_venn_diagram",
        "create_isrc_balance_chart",
        "create_duration_breakdown_chart",
        "create_content_type_breakdown_chart",
        "create_content_distribution_pie_chart",
        "create_content_category_stacked_chart",
        "create_performance_diversity_bubble_chart",
        ]


    # def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create pie chart showing content distribution across roster.

    Args:
        df: DataFrame with content data
        category_cols: Optional list of category columns (for backward compatibility)
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create stacked bar chart for content categories.

    Args:
        df: DataFrame with content data


def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
            artist = row[artist_col]
            for col in category_cols:
                if col in row:
                    stacked_data.append({artist_col: artist, "category": col, "value": row[col]})

        stacked_df = pd.DataFrame(stacked_data)

        fig = px.bar(
        stacked_df,
        x=artist_col,
        y="value",
        color="category",
        title="Content Categories by Artist",
        labels={"value": "Count", artist_col: "Artist", "category": "Content Category"},
        )
    else:
        # Calculate views by artist and content type
        stacked_data = df.groupby([artist_col, content_type_col])[views_col].sum().reset_index()

        fig = px.bar(
        stacked_data,
        x=artist_col,
        y=views_col,
        color=content_type_col,
        title="Content Performance by Artist and Type",
        labels={views_col: "Total Views", artist_col: "Artist", content_type_col: "Content Type"},
        )

    fig.update_layout(barmode="stack")

    return fig


    def create_performance_diversity_bubble_chart(
        df: pd.DataFrame,
        artist_col: str,
        performance_col: str = "performance",
        diversity_col: str = "diversity",
        size_col: Optional[str] = None,
        x_col: Optional[str] = None,
        y_col: Optional[str] = None,
        ):
    """
    Create bubble chart showing performance vs content diversity.

    Args:
        df: DataFrame with performance data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create genre context chart showing new signees across different genres.

    Args:
        df: DataFrame with artist and genre data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
    """
    Create Venn diagram showing overlapping artist strengths.

    Args:
        df: DataFrame with artist performance data
def create_content_distribution_pie_chart(
    df: pd.DataFrame,
    category_cols: Optional[List[str]] = None,
    artist_col: Optional[str] = None,
    content_type_col: str = "content_type",
):
        artist_df = df[df[artist_col] == artist]

        scores = {
        "artist_name": artist,
        "high_views": 1 if artist_df["views"].mean() > df["views"].quantile(0.75) else 0,
        "high_engagement": (
            1 if (artist_df["likes"].sum() + artist_df["comments"].sum()) / artist_df["views"].sum() > 0.05 else 0
            ),
        "consistent_uploads": 1 if len(artist_df) > df.groupby(artist_col).size().median() else 0,
        }
        artist_scores.append(scores)

    scores_df = pd.DataFrame(artist_scores)

    # Create scatter plot matrix
    fig = px.scatter_matrix(
        scores_df,
        dimensions=["high_views", "high_engagement", "consistent_uploads"],
        color="artist_name",
        title=title or "🎯 Artist Strengths Overlap Analysis",
        )

    return fig


    # Add the new functions to __all__
    __all__.extend(["create_genre_context_chart", "create_venn_diagram_chart"])
