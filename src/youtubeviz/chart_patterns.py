"""Chart patterns with bulletproof execution.

This module provides examples of properly wrapped chart functions
with explicit required columns and timeout protection.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from .bulletproof import bulletproof_chart

# Artist Views Bar Chart
ARTIST_VIEWS_REQUIRED = ["artist_name", "view_count"]


def _artist_views_bar(df: pd.DataFrame) -> go.Figure:
    """Create a bar chart of total views by artist."""
    # Group by artist and sum views
    grouped = df.groupby("artist_name", dropna=False)["view_count"].sum().reset_index()

    # Create bar chart
    fig = go.Figure()
    fig.add_trace(go.Bar(x=grouped["artist_name"], y=grouped["view_count"], name="Total Views"))

    fig.update_layout(title="Total Views by Artist", xaxis_title="Artist", yaxis_title="Total Views", showlegend=False)

    return fig


# Wrap with bulletproof decorator
safe_artist_views_bar = bulletproof_chart("artist_views_bar", ARTIST_VIEWS_REQUIRED, timeout_sec=5.0)(_artist_views_bar)


# Content Type Sentiment Chart
CONTENT_TYPE_SENTIMENT_REQUIRED = ["content_type", "sentiment_score", "comment_text"]


def _content_type_sentiment(df: pd.DataFrame) -> go.Figure:
    """Create a box plot of sentiment scores by content type."""
    fig = go.Figure()

    # Get unique content types
    content_types = df["content_type"].dropna().unique()

    for content_type in content_types:
        subset = df[df["content_type"] == content_type]
        fig.add_trace(go.Box(y=subset["sentiment_score"], name=content_type, boxpoints="outliers"))

    fig.update_layout(
        title="Sentiment Distribution by Content Type",
        xaxis_title="Content Type",
        yaxis_title="Sentiment Score",
        yaxis=dict(range=[-1, 1]),
    )

    return fig


# Wrap with bulletproof decorator
safe_content_type_sentiment = bulletproof_chart(
    "content_type_sentiment", CONTENT_TYPE_SENTIMENT_REQUIRED, timeout_sec=5.0
)(_content_type_sentiment)
