from __future__ import annotations

import json
import os
from typing import Mapping, Optional, Sequence

import pandas as pd

try:
    import plotly.express as px
except Exception:  # pragma: no cover - optional
    px = None

try:
    import altair as alt
    from vega_datasets import data as vega_data  # type: ignore
except Exception:  # pragma: no cover - optional
    alt = None


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
    """
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
    known = {a: env_map.get(a) for a in artists if env_map.get(a)}
    remaining = [a for a in artists if a not in known]
    palette = _default_palette(len(remaining))
    # Assign colors by sorted order to keep stability
    for i, a in enumerate(sorted(remaining)):
        known[a] = palette[i]
    return known


def views_over_time_plotly(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_col: str,
    hover_col: Optional[str] = None,
    animate_by: Optional[str] = None,
    color_map: Optional[Mapping[str, str]] = None,
):
    """Small wrapper that returns a Plotly figure (or df) for views over time.

    If Plotly isn't available, return the input dataframe so notebooks can still inspect data.
    """
    if px is None:
        return df
    if color_map is None and group_col in df.columns:
        color_map = get_artist_color_map(list(pd.unique(df[group_col].dropna().astype(str))))
    fig = px.line(
        df.sort_values(date_col),
        x=date_col,
        y=value_col,
        color=group_col,
        hover_name=hover_col,
        color_discrete_map=dict(color_map) if color_map else None,
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


def box_whisker_plotly(
    df: pd.DataFrame,
    value_col: str,
    group_col: str,
    hover_cols: Optional[list[str]] = None,
    color_map: Optional[Mapping[str, str]] = None,
):
    """Interactive box-and-whisker by group with outlier points.

    If Plotly isn't available, return a small grouped summary (Q1/median/Q3) instead.
    """
    if value_col not in df.columns or group_col not in df.columns:
        raise ValueError("missing required columns for box_whisker_plotly")

    if px is None:
        stats = (
            df.groupby(group_col)[value_col]
            .agg(q1=lambda s: s.quantile(0.25), median="median", q3=lambda s: s.quantile(0.75))
            .reset_index()
        )
        return stats

    if color_map is None and group_col in df.columns:
        color_map = get_artist_color_map(list(pd.unique(df[group_col].dropna().astype(str))))
    fig = px.box(
        df,
        x=group_col,
        y=value_col,
        points="suspectedoutliers",
        hover_data=hover_cols or [],
        color=group_col,
        color_discrete_map=dict(color_map) if color_map else None,
    )
    # Compact layout
    fig.update_layout(boxmode="group", legend_title_text=group_col, margin=dict(l=10, r=10, t=10, b=10))
    return fig


__all__ = [
    "views_over_time_plotly",
    "artist_compare_altair",
    "linked_scatter_detail_altair",
    "box_whisker_plotly",
]


def scatter_plotly(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: Optional[str] = None,
    hover_cols: Optional[list[str]] = None,
    color_map: Optional[Mapping[str, str]] = None,
):
    """Simple interactive scatter with optional color and hover fields.

    Returns a Plotly fig when available, else the input dataframe.
    """
    if px is None:
        return df
    if color and color_map is None and color in df.columns:
        color_map = get_artist_color_map(list(pd.unique(df[color].dropna().astype(str))))
    fig = px.scatter(
        df,
        x=x,
        y=y,
        color=color,
        hover_data=hover_cols or [],
        color_discrete_map=dict(color_map) if color_map else None,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
    return fig


def sentiment_timeline_plotly(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_col: Optional[str] = None,
    color_map: Optional[Mapping[str, str]] = None,
):
    """Interactive sentiment timeline by date (optionally grouped).

    If Plotly isn't present, return df sorted by date.
    """
    if date_col not in df.columns or value_col not in df.columns:
        raise ValueError("missing required columns for timeline plot")
    data = df.sort_values(date_col)
    if px is None:
        return data
    if group_col and color_map is None and group_col in data.columns:
        color_map = get_artist_color_map(list(pd.unique(data[group_col].dropna().astype(str))))
    fig = px.line(
        data,
        x=date_col,
        y=value_col,
        color=group_col,
        markers=True,
        color_discrete_map=dict(color_map) if color_map else None,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
    return fig


def yoy_bars_plotly(
    df: pd.DataFrame,
    year_col: str,
    value_col: str,
    group_col: str,
    color_map: Optional[Mapping[str, str]] = None,
):
    """Stacked or grouped bar chart for year-over-year values by group (e.g., artist).

    Falls back to returning the aggregated dataframe if Plotly isn't present.
    """
    if year_col not in df.columns or value_col not in df.columns or group_col not in df.columns:
        raise ValueError("missing required columns for yoy_bars_plotly")
    agg = (
        df.groupby([group_col, year_col], as_index=False)[value_col]
        .sum()
        .sort_values([year_col, value_col], ascending=[True, False])
    )
    if px is None:
        return agg
    if color_map is None and group_col in agg.columns:
        color_map = get_artist_color_map(list(pd.unique(agg[group_col].dropna().astype(str))))
    fig = px.bar(
        agg,
        x=year_col,
        y=value_col,
        color=group_col,
        barmode="group",
        text_auto=True,
        color_discrete_map=dict(color_map) if color_map else None,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
    return fig


def histogram_plotly(
    df: pd.DataFrame,
    value_col: str,
    color: Optional[str] = None,
    nbins: int = 30,
    opacity: float = 0.75,
):
    """Interactive histogram (optionally colored by group). Returns df if Plotly missing."""
    if value_col not in df.columns:
        raise ValueError("missing required columns for histogram_plotly")
    if px is None:
        return df[[value_col] + ([color] if color and color in df.columns else [])]
    fig = px.histogram(df, x=value_col, color=color, nbins=nbins, opacity=opacity)
    fig.update_layout(margin=dict(l=10, r=10, t=20, b=10))
    return fig


__all__ += [
    "scatter_plotly",
    "sentiment_timeline_plotly",
    "yoy_bars_plotly",
    "histogram_plotly",
]
