from __future__ import annotations

import json
import os
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

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
    # Input validation - reject DataFrames explicitly
    if hasattr(artists, "columns"):  # This catches pandas DataFrames
        raise TypeError("artists cannot be a DataFrame. Pass a list/array of artist names instead.")

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
        except Exception:
            env_map = {}
    # (b) Or from a JSON file path via ARTIST_COLORS_FILE
    if not env_map:
        path = os.getenv("ARTIST_COLORS_FILE")
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
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


def apply_color_scheme(
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


def views_over_time_plotly(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_col: str,
    hover_col: Optional[str] = None,
    animate_by: Optional[str] = None,
):
    """Small wrapper that returns a Plotly figure (or df) for views over time.

    If Plotly isn't available, return the input dataframe so notebooks can still inspect data.
    """
    if px is None:
        return df
    fig = px.line(
        df.sort_values(date_col),
        x=date_col,
        y=value_col,
        color=group_col,
        hover_name=hover_col,
    )
    return fig


def views_over_time_advanced(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_col: str,
    rolling_window: int = 7,
    highlight_artists: Optional[Sequence[str]] = None,
    palette_override: Optional[Mapping[str, str]] = None,
) -> Optional["go.Figure"]:
    """Interactive time series with optional rolling averages and highlights.

    - Draws per-artist daily series
    - Adds a rolling mean trace per artist
    - Highlights selected artists with thicker lines

    Returns a Plotly figure, or None if Plotly is unavailable.
    """
    if px is None or go is None:
        return None

    if df.empty:
        return go.Figure()

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
    for artist in artists:
        sub = data[data[group_col] == artist]
        color = color_map.get(artist, None)
        width_main = 2.0 if artist in hi else 1.2
        width_roll = 3.0 if artist in hi else 1.6

        # Raw daily series
        fig.add_trace(
            go.Scatter(
                x=sub[date_col],
                y=sub[value_col],
                mode="lines",
                name=f"{artist} (daily)",
                line=dict(color=color, width=width_main, dash="solid"),
                hovertemplate="%{x|%b %d, %Y}<br>%{y:,} views<extra>" + artist + "</extra>",
                legendgroup=artist,
                opacity=0.5,
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
                hovertemplate=f"%{{x|%b %d, %Y}}<br>{rolling_window}d avg: %{{y:,.0f}}<extra>" + artist + "</extra>",
                legendgroup=artist,
            )
        )

    fig.update_layout(
        title="📈 Views Over Time (with rolling average)",
        xaxis_title="Date",
        yaxis_title="Views",
        hovermode="x unified",
        legend_title="Artist",
        template="plotly_white",
    )
    return fig


def artist_compare_altair(df: pd.DataFrame, group_col: str, value_col: str):
    """Return a simple Altair bar chart comparing artists by an aggregated value.

    If Altair isn't available, return a grouped dataframe as fallback.
    """
    if alt is None:
        # provide a simple aggregated df as fallback
        return df.groupby(group_col, as_index=False)[value_col].sum().sort_values(value_col, ascending=False)

    agg = df.groupby(group_col, as_index=False)[value_col].sum()
    chart = (
        alt.Chart(agg)
        .mark_bar()
        .encode(
            x=alt.X(group_col + ":N", sort="-y"),
            y=value_col + ":Q",
            tooltip=[group_col, value_col],
        )
    )
    return chart


def linked_scatter_detail_altair(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    group_col: Optional[str] = None,
    hover_col: Optional[str] = None,
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
]
