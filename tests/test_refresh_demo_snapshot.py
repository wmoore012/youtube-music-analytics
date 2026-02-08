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


def test_build_curated_cohort_drops_nan_like_artist_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(snapshot, "DATA_DIR", tmp_path / "music_analysis_tables")
    monkeypatch.setattr(snapshot, "DEMO_DATA_PATH", tmp_path / "demo_data" / "curated_cohort.json")
    monkeypatch.setattr(snapshot, "get_engine", lambda: object())
    monkeypatch.setattr(snapshot, "_load_expected_artists", lambda: ["COBRAH"])
    monkeypatch.setattr(snapshot, "_load_artist_aliases", lambda: {"cobrah": "COBRAH"})
    monkeypatch.setattr(
        snapshot,
        "create_music_videos_table",
        lambda: pd.DataFrame(
            [
                {
                    "artist_name": pd.NA,
                    "video_id": "v0",
                    "title": "missing artist",
                    "published_at": pd.Timestamp("2026-01-01"),
                    "view_count": 1,
                    "views_per_day": 1.0,
                    "engagement_rate": 1.0,
                    "like_rate": 0.5,
                    "comment_rate": 0.5,
                    "video_type": "Other Content",
                },
                {
                    "artist_name": "Cobrah",
                    "video_id": "v1",
                    "title": "real track",
                    "published_at": pd.Timestamp("2026-01-02"),
                    "view_count": 100,
                    "views_per_day": 25.0,
                    "engagement_rate": 2.0,
                    "like_rate": 1.5,
                    "comment_rate": 0.5,
                    "video_type": "Official Music Video",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        snapshot,
        "create_music_summary_by_artist",
        lambda: pd.DataFrame(
            [
                {"artist_name": float("nan"), "total_views": 1, "total_videos": 1, "avg_engagement_rate": 1.0},
                {"artist_name": "Cobrah", "total_views": 100, "total_videos": 1, "avg_engagement_rate": 2.0},
            ]
        ),
    )

    cohort = snapshot.build_curated_cohort(top_artists=8, top_videos_per_artist=50)
    assert [artist["name"] for artist in cohort["artists"]] == ["COBRAH"]


def test_build_curated_cohort_exports_csv_without_pii_or_finance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(snapshot, "DATA_DIR", tmp_path / "music_analysis_tables")
    monkeypatch.setattr(snapshot, "DEMO_DATA_PATH", tmp_path / "demo_data" / "curated_cohort.json")
    monkeypatch.setattr(snapshot, "get_engine", lambda: object())
    monkeypatch.setattr(snapshot, "_load_expected_artists", lambda: ["COBRAH"])
    monkeypatch.setattr(snapshot, "_load_artist_aliases", lambda: {"cobrah": "COBRAH"})
    monkeypatch.setattr(
        snapshot,
        "create_music_videos_table",
        lambda: pd.DataFrame(
            [
                {
                    "artist_name": "Cobrah",
                    "video_id": "v1",
                    "title": "Official One",
                    "song_title": "Official One",
                    "video_type": "Official Music Video",
                    "isrc": "US-XXX-26-00001",
                    "has_isrc": True,
                    "published_at": pd.Timestamp("2026-02-01"),
                    "view_count": 1200,
                    "like_count": 45,
                    "comment_count": 9,
                    "like_rate": 3.75,
                    "comment_rate": 0.75,
                    "engagement_rate": 4.5,
                    "days_since_publish": 6,
                    "views_per_day": 200.0,
                    "metrics_date": pd.Timestamp("2026-02-07"),
                    "fetched_at": pd.Timestamp("2026-02-07T04:00:00"),
                    "est_revenue_usd": 999.0,
                    "comment_text": "fan comment",
                    "author_name": "fan_user",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        snapshot,
        "create_music_summary_by_artist",
        lambda: pd.DataFrame(
            [
                {"artist_name": "Cobrah", "total_views": 1200, "total_videos": 1, "avg_engagement_rate": 4.5},
            ]
        ),
    )

    snapshot.build_curated_cohort(top_artists=8, top_videos_per_artist=50)
    exported = pd.read_csv(tmp_path / "music_analysis_tables" / "normalized_music_videos.csv")

    assert "est_revenue_usd" not in exported.columns
    assert "comment_text" not in exported.columns
    assert "author_name" not in exported.columns
    assert "has_isrc" not in exported.columns
    assert "has_isrc_code" in exported.columns
    assert set(exported["has_isrc_code"].astype(int).unique().tolist()) <= {0, 1}
    assert "bool" not in {str(dtype) for dtype in exported.dtypes}
