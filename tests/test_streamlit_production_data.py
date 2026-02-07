import pandas as pd

from streamlit_app import (
    _classify_video_type_from_duration,
    _parse_iso8601_duration_seconds,
    build_artist_summary_from_metrics,
)


def test_parse_iso8601_duration_seconds() -> None:
    assert _parse_iso8601_duration_seconds("PT59S") == 59
    assert _parse_iso8601_duration_seconds("PT1M") == 60
    assert _parse_iso8601_duration_seconds("PT1H2M3S") == 3723
    assert _parse_iso8601_duration_seconds(None) is None
    assert _parse_iso8601_duration_seconds("bad") is None


def test_classify_video_type_from_duration() -> None:
    assert _classify_video_type_from_duration("PT59S") == "Short"
    assert _classify_video_type_from_duration("PT1M") == "Short"
    assert _classify_video_type_from_duration("PT1M1S") == "Official Music Video"
    assert _classify_video_type_from_duration(None) == "Video"


def test_build_artist_summary_uses_latest_snapshot() -> None:
    df = pd.DataFrame(
        [
            {
                "video_id": "v1",
                "artist_name": "A",
                "metrics_date": pd.Timestamp("2026-02-05"),
                "view_count": 100,
                "like_count": 10,
                "comment_count": 2,
                "est_revenue_usd": 1.0,
                "engagement_rate": 12.0,
            },
            {
                "video_id": "v1",
                "artist_name": "A",
                "metrics_date": pd.Timestamp("2026-02-06"),
                "view_count": 120,
                "like_count": 12,
                "comment_count": 3,
                "est_revenue_usd": 1.2,
                "engagement_rate": 12.5,
            },
            {
                "video_id": "v2",
                "artist_name": "A",
                "metrics_date": pd.Timestamp("2026-02-06"),
                "view_count": 80,
                "like_count": 8,
                "comment_count": 1,
                "est_revenue_usd": 0.8,
                "engagement_rate": 11.25,
            },
        ]
    )

    summary = build_artist_summary_from_metrics(df)
    row = summary.loc[summary["artist_name"] == "A"].iloc[0]

    assert int(row["total_videos"]) == 2
    assert int(row["total_views"]) == 200
    assert int(row["total_likes"]) == 20
    assert int(row["total_comments"]) == 4
    assert float(row["total_est_revenue_usd"]) == 2.0

