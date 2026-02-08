import pandas as pd

from streamlit_app import build_artist_summary_from_metrics, build_kpi_context


def test_artist_summary_excludes_estimated_revenue_fields() -> None:
    metrics = pd.DataFrame(
        [
            {
                "video_id": "vid-1",
                "artist_name": "Artist A",
                "metrics_date": pd.Timestamp("2026-02-08"),
                "view_count": 1000,
                "like_count": 50,
                "comment_count": 10,
                "engagement_rate": 6.0,
            }
        ]
    )

    summary = build_artist_summary_from_metrics(metrics)
    assert "total_est_revenue_usd" not in summary.columns


def test_kpi_context_excludes_estimated_revenue_keys() -> None:
    summary = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "total_views": 1000,
                "total_videos": 2,
                "total_likes": 100,
                "total_comments": 20,
                "avg_engagement_rate": 12.0,
            }
        ]
    )
    videos = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "video_id": "a1",
                "view_count": 600,
                "like_count": 60,
                "comment_count": 12,
                "engagement_rate": 12.0,
            },
            {
                "artist_name": "Artist A",
                "video_id": "a2",
                "view_count": 400,
                "like_count": 40,
                "comment_count": 8,
                "engagement_rate": 12.0,
            },
        ]
    )

    context = build_kpi_context(summary=summary, artists=["Artist A"], videos=videos, roster_videos=videos)
    assert "total_revenue" not in context
    assert "roster_revenue_per_artist" not in context
