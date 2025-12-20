from __future__ import annotations

import pandas as pd

from youtubeviz.chart_titles import (
    format_date_axis_label,
    generate_growth_signal_title,
    generate_heatmap_title,
    generate_standout_videos_title,
)


def test_format_date_axis_label_parses_dates():
    assert format_date_axis_label("2024-03-15") == "Mar 15 2024"
    assert format_date_axis_label("2024-03-15", include_year=False) == "Mar 15"


def test_format_date_axis_label_returns_input_on_failure():
    assert format_date_axis_label("not-a-date") == "not-a-date"


def test_generate_heatmap_title_avoids_double_counting():
    df = pd.DataFrame(
        {
            "artist_name": ["Artist A"],
            "engagement_rate": [0.02],
            "like_rate": [0.02],
            "comment_rate": [0.02],
        }
    )

    title = generate_heatmap_title(df)
    assert "2.0%" in title
    assert "6.0%" not in title


def test_generate_standout_videos_title_handles_no_variance():
    df = pd.DataFrame(
        {
            "view_count": [1000] * 12,
            "positive_sentiment_rate": [0.5] * 12,
        }
    )

    title = generate_standout_videos_title(df)
    assert "insufficient variance" in title.lower()


def test_generate_growth_signal_title_sorts_dates():
    df = pd.DataFrame(
        {
            "artist_name": ["Artist A", "Artist A"],
            "published_at": ["2024-2-01", "2024-12-01"],
            "engagement_rate": [1.0, 3.0],
        }
    )

    title = generate_growth_signal_title(df)
    assert "surges 3x" in title
