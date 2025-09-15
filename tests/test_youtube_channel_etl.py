import json
import os
import types
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pymysql
import pytest
from dotenv import load_dotenv

from web.youtube_channel_etl import YouTubeChannelETL

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)

# Normalize DB_* env from DATABASE_URL when present (helps local mismatches)
_dburl = os.getenv("DATABASE_URL")
if _dburl:
    from urllib.parse import urlparse

    p = urlparse(_dburl)
    auth, hostport = p.netloc.split("@", 1) if "@" in p.netloc else ("", p.netloc)
    if ":" in auth:
        user, pw = auth.split(":", 1)
    else:
        user, pw = os.getenv("DB_USER", ""), os.getenv("DB_PASS", "")
    if ":" in hostport:
        host, port = hostport.split(":", 1)
    else:
        host, port = hostport, os.getenv("DB_PORT", "3306")
    db_name = p.path.lstrip("/") or os.getenv("DB_NAME")
    os.environ["DB_HOST"] = host
    os.environ["DB_PORT"] = str(port)
    os.environ["DB_USER"] = user
    os.environ["DB_PASS"] = pw
    if db_name:
        os.environ["DB_NAME"] = db_name

REQUIRED_ENV = [
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASS",
    "DB_NAME",
    "YOUTUBE_API_KEY",
]


def _env_ok():
    return all(os.getenv(k) for k in REQUIRED_ENV)


pytestmark = pytest.mark.skipif(not _env_ok(), reason="Database/YouTube env vars not set for integration tests")


def _from_database_url():
    url = os.getenv("DATABASE_URL")
    if not url:
        return None
    p = urlparse(url)
    # p.netloc -> user:pass@host:port
    auth_host = p.netloc
    if "@" in auth_host:
        auth, hostport = auth_host.split("@", 1)
        if ":" in auth:
            user, pw = auth.split(":", 1)
        else:
            user, pw = auth, ""
    else:
        hostport = auth_host
        user = os.getenv("DB_USER")
        pw = os.getenv("DB_PASS")
    if ":" in hostport:
        host, port = hostport.split(":", 1)
    else:
        host, port = hostport, os.getenv("DB_PORT", "3306")
    db_name = p.path.lstrip("/")
    return {
        "host": host or os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(port or os.getenv("DB_PORT", 3306)),
        "user": user or os.getenv("DB_USER"),
        "password": pw or os.getenv("DB_PASS"),
        "db": db_name or os.getenv("DB_NAME"),
    }


def make_etl():
    url_cfg = _from_database_url() or {}
    return YouTubeChannelETL(
        api_key=os.getenv("YOUTUBE_API_KEY") or "dummy",
        db_host=url_cfg.get("host", os.getenv("DB_HOST", "127.0.0.1")),
        db_port=int(url_cfg.get("port", os.getenv("DB_PORT", 3306))),
        db_user=url_cfg.get("user", os.getenv("DB_USER")),
        db_pass=url_cfg.get("password", os.getenv("DB_PASS")),
        db_name=url_cfg.get("db", os.getenv("DB_NAME")),
    )


def test_coerce_counts_nulls():
    etl = make_etl()
    assert etl._coerce_counts(None) == (0, 0, 0)
    assert etl._coerce_counts({}) == (0, 0, 0)
    assert etl._coerce_counts({"viewCount": None, "likeCount": None, "commentCount": None}) == (0, 0, 0)
    assert etl._coerce_counts({"viewCount": "10", "likeCount": "5", "commentCount": "2"}) == (10, 5, 2)


def test_batch_upsert_raw_and_metrics_smoke(monkeypatch):
    etl = make_etl()
    cfg = _from_database_url()
    if cfg:
        monkeypatch.setattr(
            etl,
            "_connect",
            lambda: pymysql.connect(
                host=cfg["host"],
                port=int(cfg["port"]),
                user=cfg["user"],
                password=cfg["password"],
                db=cfg["db"],
                cursorclass=pymysql.cursors.DictCursor,
            ),
        )

        # Clear any existing ETL run locks for test channel
        conn = etl._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM youtube_etl_runs WHERE channel_id = %s", ("UC_TEST_CHANNEL",))
            conn.commit()
        finally:
            conn.close()

    # Monkeypatch API methods to avoid network
    ch_id = "UC_TEST_CHANNEL"
    uploads = "UU_TEST_UPLOADS"
    vids = [f"vid{i}" for i in range(1, 4)]

    monkeypatch.setattr(etl, "resolve_channel_id", lambda url: ch_id)
    monkeypatch.setattr(etl, "get_uploads_playlist", lambda cid: uploads)

    # Yield 3 playlist items
    def _iter_items(pid):
        for v in vids:
            yield {"contentDetails": {"videoId": v}}

    monkeypatch.setattr(etl, "iter_playlist_items", _iter_items)

    # Return details for those ids; include a None stat to test null safety
    def _details(ids):
        return {
            "items": [
                {"id": ids[0], "statistics": {"viewCount": "10", "likeCount": "2", "commentCount": "1"}},
                {"id": ids[1], "statistics": {"viewCount": None, "likeCount": None, "commentCount": None}},
                {"id": ids[2], "statistics": {}},
            ]
        }

    monkeypatch.setattr(etl, "get_videos_details", _details)

    summary = etl.run_for_channel("https://www.youtube.com/@dummy", limit=None)
    assert summary.channel_id == ch_id
    assert summary.uploads_playlist_id == uploads
    assert summary.videos_seen == 3
    assert summary.errors == []

    # Verify DB contents for raw table and metrics
    cfg = _from_database_url() or {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "db": os.getenv("DB_NAME"),
    }
    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["db"],
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(vids))
            cur.execute(f"SELECT COUNT(*) AS n FROM youtube_videos_raw WHERE video_id IN ({placeholders})", vids)
            assert cur.fetchone()["n"] == 3

            # metrics should have today's date entries
            placeholders = ",".join(["%s"] * len(vids))
            cur.execute(
                f"SELECT COUNT(*) AS n FROM youtube_metrics WHERE video_id IN ({placeholders}) AND metrics_date = CURDATE()",
                vids,
            )
            assert cur.fetchone()["n"] == 3
    finally:
        conn.close()


def test_daily_max_semantics(monkeypatch):
    etl = make_etl()
    cfg = _from_database_url()
    if cfg:
        monkeypatch.setattr(
            etl,
            "_connect",
            lambda: pymysql.connect(
                host=cfg["host"],
                port=int(cfg["port"]),
                user=cfg["user"],
                password=cfg["password"],
                db=cfg["db"],
                cursorclass=pymysql.cursors.DictCursor,
            ),
        )

        # Clear any existing ETL run locks for test channel
        conn = etl._connect()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM youtube_etl_runs WHERE channel_id = %s", ("UC_TEST2",))
                # Also clean up any existing test data
                cur.execute("DELETE FROM youtube_videos WHERE video_id = %s", ("vidX",))
                cur.execute("DELETE FROM youtube_metrics WHERE video_id = %s", ("vidX",))
            conn.commit()
        finally:
            conn.close()
    ch_id = "UC_TEST2"
    uploads = "UU_TEST2"
    v = "vidX"
    monkeypatch.setattr(etl, "resolve_channel_id", lambda url: ch_id)
    monkeypatch.setattr(etl, "get_uploads_playlist", lambda cid: uploads)

    # First run: 100 views
    def _iter_once(pid):
        yield {"contentDetails": {"videoId": v}}

    monkeypatch.setattr(etl, "iter_playlist_items", _iter_once)
    monkeypatch.setattr(
        etl,
        "get_videos_details",
        lambda ids: {"items": [{"id": v, "statistics": {"viewCount": "100", "likeCount": "1", "commentCount": "1"}}]},
    )
    etl.run_for_channel("https://www.youtube.com/@dummy2")

    # Second run: lower counts (should not decrease)
    monkeypatch.setattr(etl, "iter_playlist_items", _iter_once)
    monkeypatch.setattr(
        etl,
        "get_videos_details",
        lambda ids: {"items": [{"id": v, "statistics": {"viewCount": "50", "likeCount": "0", "commentCount": "0"}}]},
    )
    etl.run_for_channel("https://www.youtube.com/@dummy2")

    # Verify metrics stayed at 100 for today
    cfg = _from_database_url() or {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "db": os.getenv("DB_NAME"),
    }
    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["db"],
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT view_count, like_count, comment_count FROM youtube_metrics WHERE video_id=%s AND metrics_date = CURDATE()",
                (v,),
            )
            row = cur.fetchone()
            assert row is not None, f"No metrics found for video {v} on current date"
            assert row["view_count"] >= 100
    finally:
        conn.close()
