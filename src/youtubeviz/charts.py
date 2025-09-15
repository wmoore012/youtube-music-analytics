from __future__ import annotations

import json
import os
from typing import Mapping, Optional, Sequence

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


__all__ = ["views_over_time_plotly", "artist_compare_altair", "linked_scatter_detail_altair", "get_artist_color_map"]
