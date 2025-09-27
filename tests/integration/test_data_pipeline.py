import pytest
from sqlalchemy import create_engine

from src.youtubeviz import data as data_module


def _setup_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn: conn.exec_driver_sql("""
            CREATE TABLE youtube_videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                channel_title TEXT,
                published_at TEXT,
                isrc TEXT ) """
                              ) conn.exec_driver_sql("""
            CREATE TABLE youtube_metrics (
                video_id TEXT,
                metrics_date TEXT,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER ) """
                              ) conn.exec_driver_sql("""
            CREATE TABLE artist_aliases (
                alias TEXT,
                canonical_name TEXT ) """) conn.exec_driver_sql("INSERT INTO artist_aliases (alias, canonical_name) VALUES ('THE WEEKND', 'The Weeknd')") conn.exec_driver_sql("INSERT INTO youtube_videos (video_id, " "title, " "channel_title, " "published_at, " "isrc) VALUES ('v1', " "'Blinding Lights', " "'The weeknd', " "'2020-01-01', " "'ISRC1')"
                              ) conn.exec_driver_sql( "INSERT INTO youtube_metrics (video_id,  # noqa: E999
                metrics_date,
                view_count,
                like_count,
                comment_count) VALUES ('v1',
                '2020 - 01 - 02',
                1000,
                50,
                10)"
        )
    return engine


def test_load_artist_daily_metrics_applies_alias_mapping(monkeypatch): engine = _setup_engine() monkeypatch.delenv("ARTIST_ALIASES_JSON", raising=False)

    df = data_module.load_artist_daily_metrics(engine=engine)
 assert not df.empty assert df.loc[0, "artist_name"] == "The Weeknd" assert df.loc[0, "views"] == 1000 assert df.loc[0, "likes"] == 50


def test_run_artist_metrics_pipeline_returns_revenue(monkeypatch): engine = _setup_engine() monkeypatch.delenv("ARTIST_ALIASES_JSON", raising=False)

    result = data_module.run_artist_metrics_pipeline(engine=engine) assert set(result.keys()) == {"daily_metrics", "revenue"} daily = result["daily_metrics"] revenue = result["revenue"]
    assert not daily.empty assert not revenue.empty assert revenue.loc[0, "artist_name"] == "The Weeknd" assert revenue.loc[0, "est_revenue_usd"] == pytest.approx(3.0)
