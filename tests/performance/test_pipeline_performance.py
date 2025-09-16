from sqlalchemy import create_engine

from src.youtubeviz import data as data_module


def _prepare_engine(rows: int) -> object:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE youtube_videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                channel_title TEXT,
                published_at TEXT,
                isrc TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            CREATE TABLE youtube_metrics (
                video_id TEXT,
                metrics_date TEXT,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER
            )
            """
        )
        for idx in range(rows):
            vid = f"v{idx}"
            conn.exec_driver_sql(
                "INSERT INTO youtube_videos (video_id, title, channel_title, published_at, isrc) VALUES (?, ?, ?, ?, ?)",
                (vid, f"Song {idx}", "Artist", "2020-01-01", f"ISRC{idx}"),
            )
            conn.exec_driver_sql(
                "INSERT INTO youtube_metrics (video_id, metrics_date, view_count, like_count, comment_count) VALUES (?, ?, ?, ?, ?)",
                (vid, "2020-01-02", 1000 + idx, 10, 1),
            )
    return engine


def test_benchmark_pipeline_detects_regressions(monkeypatch):
    engine = _prepare_engine(150)
    monkeypatch.delenv("ARTIST_ALIASES_JSON", raising=False)

    summary = data_module.benchmark_run_artist_metrics_pipeline(engine=engine, iterations=3)

    assert summary["rows"] == 150
    assert summary["iterations"] == 3
    assert summary["duration_sec"] < 0.5
