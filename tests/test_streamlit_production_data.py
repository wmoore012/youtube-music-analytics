import pandas as pd
import pytest

from streamlit_app import (
    _classify_video_type_from_duration,
    _parse_iso8601_duration_seconds,
    build_artist_summary_from_metrics,
    load_artist_summary_from_demo,
    load_normalized_videos_from_demo,
    resolve_metrics_date_window,
)


def test_parse_iso8601_duration_seconds() -> None:
    assert _parse_iso8601_duration_seconds("PT59S") == 59
    assert _parse_iso8601_duration_seconds("PT1M") == 60
    assert _parse_iso8601_duration_seconds("PT1H2M3S") == 3723
    assert _parse_iso8601_duration_seconds(None) is None
    assert _parse_iso8601_duration_seconds("bad") is None


def test_classify_video_type_from_duration() -> None:
    assert _classify_video_type_from_duration("PT59S") == "Short video (<60s)"
    assert _classify_video_type_from_duration("PT1M") == "Short video (<60s)"
    assert _classify_video_type_from_duration("PT1M1S") == "Other Content"
    assert _classify_video_type_from_duration(None) == "Other Content"


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
                "engagement_rate": 12.0,
            },
            {
                "video_id": "v1",
                "artist_name": "A",
                "metrics_date": pd.Timestamp("2026-02-06"),
                "view_count": 120,
                "like_count": 12,
                "comment_count": 3,
                "engagement_rate": 12.5,
            },
            {
                "video_id": "v2",
                "artist_name": "A",
                "metrics_date": pd.Timestamp("2026-02-06"),
                "view_count": 80,
                "like_count": 8,
                "comment_count": 1,
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


def test_demo_loaders_handle_malformed_and_dict_indexed_records(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "last_updated": "2026-02-07T12:34:56+00:00",
        "artists": {
            0: {
                "name": "BiC Fizzle",
                "metrics": {
                    "total_views": 1_500,
                    "total_videos": 2,
                    "engagement_rate": 1.5,
                },
                "videos": {
                    0: {
                        "video_id": "vid_1",
                        "title": "Test Video",
                        "published_at": "2026-02-07T00:00:00",
                        "view_count": 1_500,
                        "views_per_day": 250.0,
                        "engagement_rate": 1.5,
                        "like_rate": 1.2,
                        "comment_rate": 0.3,
                        "video_type": "Official Music Video",
                    },
                    1: 999,  # malformed row should be skipped, not crash
                },
            },
            1: 123,  # malformed artist should be skipped, not crash
        },
    }

    monkeypatch.setattr("streamlit_app._load_demo_cohort", lambda: payload)

    artist_summary = load_artist_summary_from_demo()
    normalized_videos = load_normalized_videos_from_demo()

    assert artist_summary["artist_name"].tolist() == ["BiC Fizzle"]
    assert int(artist_summary.loc[0, "total_views"]) == 1_500
    assert int(artist_summary.loc[0, "total_videos"]) == 1
    assert int(artist_summary.loc[0, "total_likes"]) == 18
    assert int(artist_summary.loc[0, "total_comments"]) == 4

    assert normalized_videos["artist_name"].tolist() == ["BiC Fizzle"]
    assert normalized_videos["video_id"].tolist() == ["vid_1"]
    assert normalized_videos["title"].tolist() == ["Test Video"]
    assert normalized_videos["metrics_date"].dt.date.iloc[0].isoformat() == "2026-02-07"


def test_demo_loader_uses_duration_only_for_short_video_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "last_updated": "2026-02-07T00:00:00+00:00",
        "artists": [
            {
                "name": "Flyana Boss",
                "videos": [
                    {
                        "video_id": "f1",
                        "title": "quick run clip #shorts",
                        "published_at": "2025-01-01T00:00:00",
                        "view_count": 1000,
                        "duration": "PT45S",
                        "like_rate": 2.0,
                        "comment_rate": 1.0,
                        "video_type": "Music Content",
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr("streamlit_app._load_demo_cohort", lambda: payload)

    normalized = load_normalized_videos_from_demo()
    assert normalized["video_type"].tolist() == ["Short video (<60s)"]
    assert int(normalized["like_count"].iloc[0]) == 20
    assert int(normalized["comment_count"].iloc[0]) == 10


def test_demo_loader_sets_unknown_age_to_sentinel_and_zero_velocity(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "last_updated": "2026-02-07T00:00:00+00:00",
        "artists": [
            {
                "name": "BiC Fizzle",
                "videos": [
                    {
                        "video_id": "b1",
                        "title": "missing publish date",
                        "view_count": 5000,
                        "views_per_day": 9999.0,
                        "video_type": "Official Music Video",
                    }
                ],
            }
        ],
    }

    monkeypatch.setattr("streamlit_app._load_demo_cohort", lambda: payload)

    normalized = load_normalized_videos_from_demo()
    assert int(normalized["age_days"].iloc[0]) == -1
    assert float(normalized["views_per_day"].iloc[0]) == 0.0


def test_resolve_metrics_date_window_requires_column() -> None:
    with pytest.raises(ValueError, match="metrics_date column"):
        resolve_metrics_date_window(pd.DataFrame([{"view_count": 1}]))


def test_resolve_metrics_date_window_rejects_all_invalid_dates() -> None:
    df = pd.DataFrame([{"metrics_date": "bad-date"}, {"metrics_date": None}])
    with pytest.raises(ValueError, match="missing or invalid"):
        resolve_metrics_date_window(df)


def test_resolve_metrics_date_window_returns_min_max() -> None:
    df = pd.DataFrame(
        [
            {"metrics_date": "2026-02-07T00:00:00"},
            {"metrics_date": "2026-02-05T00:00:00"},
            {"metrics_date": "2026-02-06T00:00:00"},
        ]
    )

    min_date, max_date = resolve_metrics_date_window(df)
    assert min_date.isoformat() == "2026-02-05"
    assert max_date.isoformat() == "2026-02-07"
