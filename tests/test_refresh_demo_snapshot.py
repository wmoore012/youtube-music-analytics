import pandas as pd
import pytest

import scripts.refresh_demo_snapshot as snapshot


def test_build_curated_cohort_canonicalizes_artist_names_and_filters_untracked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(snapshot, "DATA_DIR", tmp_path / "music_analysis_tables")
    monkeypatch.setattr(snapshot, "DEMO_DATA_PATH", tmp_path / "demo_data" / "curated_cohort.json")
    monkeypatch.setattr(snapshot, "get_engine", lambda: object())
    monkeypatch.setattr(snapshot, "_load_expected_artists", lambda: ["COBRAH", "Corook"])
    monkeypatch.setattr(
        snapshot,
        "_load_artist_aliases",
        lambda: {
            "cobrah": "COBRAH",
            "hicorook": "Corook",
        },
    )
    monkeypatch.setattr(
        snapshot,
        "create_music_videos_table",
        lambda: pd.DataFrame(
            [
                {
                    "artist_name": "Cobrah",
                    "video_id": "v1",
                    "title": "Official One",
                    "published_at": pd.Timestamp("2026-01-01"),
                    "view_count": 1000,
                    "views_per_day": 100.0,
                    "engagement_rate": 5.0,
                    "like_rate": 4.0,
                    "comment_rate": 1.0,
                    "video_type": "Official Music Video",
                },
                {
                    "artist_name": "COBRAH",
                    "video_id": "v2",
                    "title": "Official Two",
                    "published_at": pd.Timestamp("2026-01-03"),
                    "view_count": 2000,
                    "views_per_day": 120.0,
                    "engagement_rate": 6.0,
                    "like_rate": 5.0,
                    "comment_rate": 1.0,
                    "video_type": "Official Audio",
                },
                {
                    "artist_name": "hicorook",
                    "video_id": "v3",
                    "title": "Corook clip",
                    "published_at": pd.Timestamp("2026-01-05"),
                    "view_count": 1500,
                    "views_per_day": 90.0,
                    "engagement_rate": 4.5,
                    "like_rate": 3.8,
                    "comment_rate": 0.7,
                    "video_type": "Other Content",
                },
                {
                    "artist_name": "No loses",
                    "video_id": "v4",
                    "title": "Should be filtered",
                    "published_at": pd.Timestamp("2026-01-07"),
                    "view_count": 9999,
                    "views_per_day": 999.0,
                    "engagement_rate": 9.0,
                    "like_rate": 7.5,
                    "comment_rate": 1.5,
                    "video_type": "Other Content",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        snapshot,
        "create_music_summary_by_artist",
        lambda: pd.DataFrame(
            [
                {"artist_name": "Cobrah", "total_views": 1000, "total_videos": 1, "avg_engagement_rate": 5.0},
                {"artist_name": "COBRAH", "total_views": 2000, "total_videos": 1, "avg_engagement_rate": 6.0},
                {"artist_name": "hicorook", "total_views": 1500, "total_videos": 1, "avg_engagement_rate": 4.5},
                {"artist_name": "No loses", "total_views": 9999, "total_videos": 1, "avg_engagement_rate": 9.0},
            ]
        ),
    )

    cohort = snapshot.build_curated_cohort(top_artists=8, top_videos_per_artist=50)
    names = sorted(artist["name"] for artist in cohort["artists"])

    assert names == ["COBRAH", "Corook"]
    cobrah_metrics = next(artist["metrics"] for artist in cohort["artists"] if artist["name"] == "COBRAH")
    assert cobrah_metrics["total_views"] == 3000
    assert cobrah_metrics["total_videos"] == 2


def test_build_curated_cohort_raises_if_filtering_removes_everything(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(snapshot, "DATA_DIR", tmp_path / "music_analysis_tables")
    monkeypatch.setattr(snapshot, "DEMO_DATA_PATH", tmp_path / "demo_data" / "curated_cohort.json")
    monkeypatch.setattr(snapshot, "get_engine", lambda: object())
    monkeypatch.setattr(snapshot, "_load_expected_artists", lambda: ["COBRAH"])
    monkeypatch.setattr(snapshot, "_load_artist_aliases", lambda: {})
    monkeypatch.setattr(
        snapshot,
        "create_music_videos_table",
        lambda: pd.DataFrame(
            [
                {
                    "artist_name": "Unknown Artist",
                    "video_id": "v1",
                    "title": "Unknown",
                    "published_at": pd.Timestamp("2026-01-01"),
                    "view_count": 1,
                    "views_per_day": 1.0,
                    "engagement_rate": 1.0,
                    "like_rate": 1.0,
                    "comment_rate": 0.0,
                    "video_type": "Other Content",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        snapshot,
        "create_music_summary_by_artist",
        lambda: pd.DataFrame(
            [{"artist_name": "Unknown Artist", "total_views": 1, "total_videos": 1, "avg_engagement_rate": 1.0}]
        ),
    )

    with pytest.raises(RuntimeError, match="No eligible artists remained"):
        snapshot.build_curated_cohort(top_artists=8, top_videos_per_artist=50)
