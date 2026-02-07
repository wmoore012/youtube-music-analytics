import pandas as pd
import pytest
from streamlit_app import (
    build_artist_content_action_rows,
    build_delta_signal_rows,
    build_kpi_context,
    compute_pct_delta,
    format_delta_value,
)


def test_compute_pct_delta_hides_invalid_or_tiny_change() -> None:
    assert compute_pct_delta(100, 0) is None
    assert compute_pct_delta(100.05, 100.0) is None
    assert compute_pct_delta(float("nan"), 100.0) is None
    assert compute_pct_delta(100.0, float("inf")) is None
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


def test_build_kpi_context_uses_window_scoped_video_rows() -> None:
    summary = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "total_views": 1000,
                "total_videos": 10,
                "total_likes": 100,
                "total_comments": 20,
                "total_est_revenue_usd": 5.0,
                "avg_engagement_rate": 5.0,
            },
            {
                "artist_name": "Artist B",
                "total_views": 900,
                "total_videos": 9,
                "total_likes": 90,
                "total_comments": 18,
                "total_est_revenue_usd": 4.5,
                "avg_engagement_rate": 4.0,
            },
        ]
    )

    selected_rows = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "video_id": "a1",
                "view_count": 120,
                "like_count": 12,
                "comment_count": 4,
                "est_revenue_usd": 0.6,
                "engagement_rate": 13.0,
            },
            {
                "artist_name": "Artist A",
                "video_id": "a2",
                "view_count": 80,
                "like_count": 8,
                "comment_count": 2,
                "est_revenue_usd": 0.4,
                "engagement_rate": 12.0,
            },
        ]
    )

    roster_rows = pd.concat(
        [
            selected_rows,
            pd.DataFrame(
                [
                    {
                        "artist_name": "Artist B",
                        "video_id": "b1",
                        "view_count": 200,
                        "like_count": 20,
                        "comment_count": 5,
                        "est_revenue_usd": 1.0,
                        "engagement_rate": 8.0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    context = build_kpi_context(
        summary,
        artists=["Artist A"],
        videos=selected_rows,
        roster_videos=roster_rows,
    )

    assert context["total_views"] == 200
    assert context["total_videos"] == 2
    assert context["total_likes"] == 20
    assert context["total_comments"] == 6
    assert context["total_revenue"] == pytest.approx(1.0)
    assert context["avg_engagement"] == pytest.approx(12.5)
    assert context["selected_artist_count"] == 1
    assert context["roster_views_per_artist"] == pytest.approx(200.0)
    assert context["roster_videos_per_artist"] == pytest.approx(1.5)
    assert context["roster_likes_per_artist"] == pytest.approx(20.0)
    assert context["roster_comments_per_artist"] == pytest.approx(5.5)
    assert context["roster_revenue_per_artist"] == pytest.approx(1.0)
    assert context["roster_avg_engagement"] == pytest.approx(10.25)
