import pandas as pd
import pytest

from streamlit_app import (
    build_artist_content_action_rows,
    build_delta_signal_rows,
    compute_pct_delta,
    format_delta_value,
)


def test_compute_pct_delta_hides_invalid_or_tiny_change() -> None:
    assert compute_pct_delta(100, 0) is None
    assert compute_pct_delta(100.05, 100.0) is None
    assert compute_pct_delta(110, 100) == pytest.approx(10.0)


def test_format_delta_value() -> None:
    assert format_delta_value(None) is None
    assert format_delta_value(12.34) == "+12.3%"
    assert format_delta_value(-3.21) == "-3.2%"


def test_build_delta_signal_rows_only_shows_visible_deltas() -> None:
    rows = build_delta_signal_rows(
        views_per_artist=110,
        roster_views_per_artist=100,
        videos_per_artist=10,
        roster_videos_per_artist=10,  # hidden
        likes_per_artist=50,
        roster_likes_per_artist=40,
        comments_per_artist=8,
        roster_comments_per_artist=8,  # hidden
        avg_engagement=5.5,
        roster_avg_engagement=5.0,
        revenue_per_artist=220.0,
        roster_revenue_per_artist=200.0,
    )

    assert not rows.empty
    assert "Total views" in rows["KPI"].tolist()
    assert "Total likes" in rows["KPI"].tolist()
    assert "Avg engagement rate" in rows["KPI"].tolist()
    assert "Est. revenue (USD)" in rows["KPI"].tolist()
    assert "Videos analyzed" not in rows["KPI"].tolist()
    assert "Total comments" not in rows["KPI"].tolist()
    assert rows["Arithmetic"].str.contains("x 100", regex=False).all()


def test_build_artist_content_action_rows_returns_actionable_rows() -> None:
    df = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "video_type": "Short",
                "video_id": "a1",
                "view_count": 1000,
                "views_per_day": 200.0,
                "engagement_rate": 4.0,
            },
            {
                "artist_name": "Artist A",
                "video_type": "Official Music Video",
                "video_id": "a2",
                "view_count": 3000,
                "views_per_day": 120.0,
                "engagement_rate": 6.0,
            },
            {
                "artist_name": "Artist B",
                "video_type": "Official Music Video",
                "video_id": "b1",
                "view_count": 4000,
                "views_per_day": 80.0,
                "engagement_rate": 2.5,
            },
        ]
    )

    rows = build_artist_content_action_rows(df)
    assert len(rows) == 2
    assert set(rows.columns) == {"Artist", "Best Reach Format", "Best Engagement Format", "Action Plan"}
    assert rows["Action Plan"].str.len().min() > 20
