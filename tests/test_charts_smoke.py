"""Smoke tests (pytest-friendly) for MusicScope™ charts.

These verify: correct imports, minimal inputs, figure return, and no crashes.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

import src.youtubeviz.advanced_charts as ac
from src.youtubeviz.bulletproof import bulletproof_chart
from src.youtubeviz.chart_patterns import (
    safe_artist_views_bar,
    safe_content_type_sentiment,
)


def _df_diverging_ok():
    return pd.DataFrame(
        {
            "content_type": ["Short", "Short", "Long", "Long"],
            "sentiment_score": [0.9, 0.8, 0.2, 0.4],
            "comment_text": ["fire!!", "lowkey slaps", "mid", "trash"],
            "like_count": [10, 12, 3, 4],
            "comment_id": ["a", "b", "c", "d"],
        }
    )


def _df_artist_views_ok():
    return pd.DataFrame(
        {
            "artist_name": ["A", "B", "A"],
            "view_count": [100, 50, 40],
        }
    )


def _df_content_type_dots_ok():
    # add 'artist_name' because advanced_charts.create_content_type_dots expects it
    return pd.DataFrame(
        {
            "content_type": ["Short", "Long", "Short"],
            "artist_name": ["A", "B", "C"],
            "upload_count": [10, 5, 3],
            "like_count": [100, 50, 40],
            "comment_count": [20, 9, 6],
        }
    )


def test_chart_patterns_artist_views():
    fig = safe_artist_views_bar(_df_artist_views_ok())
    assert isinstance(fig, go.Figure)


def test_chart_patterns_content_type_sentiment():
    fig = safe_content_type_sentiment(_df_diverging_ok())
    assert isinstance(fig, go.Figure)


def test_advanced_diverging_sentiment_bars():
    safe = bulletproof_chart("diverging_sentiment", ["content_type", "sentiment_score", "comment_text"])(
        ac.create_diverging_sentiment_bars
    )
    fig = safe(_df_diverging_ok())
    assert isinstance(fig, go.Figure)


def test_advanced_content_type_dots():
    safe = bulletproof_chart("content_type_dots", ["content_type", "artist_name"])(ac.create_content_type_dots)
    fig = safe(_df_content_type_dots_ok())
    assert isinstance(fig, go.Figure)
