#!/usr/bin/env python3
"""
Delete known dummy video_ids from MySQL tables.

Targets:
- youtube_videos
- youtube_videos_raw
- youtube_metrics (optional via flag)

Reads DB config from either DATABASE_URL or DB_* env vars.
Supports --dry-run to preview deletions and --ids to override default list.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any, List, Tuple, TypedDict
from urllib.parse import urlparse

import pymysql
from dotenv import load_dotenv

DEFAULT_IDS = ["vid1", "vid2", "vid3", "vidX"]


def load_env():
    # Attempt to load .env at repo root
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(here, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path, override=False)


class DBConfig(TypedDict):
    host: str
    port: int
    user: str
    password: str
    db: str


def cfg_from_env() -> DBConfig:
    url = os.getenv("DATABASE_URL")
    if url:
        p = urlparse(url)
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
            "host": (host or os.getenv("DB_HOST", "127.0.0.1")) or "127.0.0.1",
            "port": int(port or os.getenv("DB_PORT", 3306) or 3306),
            "user": (user or os.getenv("DB_USER") or ""),
            "password": (pw or os.getenv("DB_PASS") or ""),
            "db": (db_name or os.getenv("DB_NAME") or ""),
        }
    # Fallback to discrete env vars
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1") or "127.0.0.1",
        "port": int(os.getenv("DB_PORT", 3306) or 3306),
        "user": os.getenv("DB_USER") or "",
        "password": os.getenv("DB_PASS") or "",
        "db": os.getenv("DB_NAME") or "",
    }


def connect(cfg: DBConfig) -> Any:
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["db"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def delete_ids(conn: Any, ids: List[str], include_metrics: bool) -> Tuple[int, int, int]:
    """Return counts deleted from (videos, raw, metrics)."""
    v = r = m = 0
    placeholders = ",".join(["%s"] * len(ids))
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM youtube_videos WHERE video_id IN ({placeholders})", ids)
        v = cur.rowcount
        cur.execute(f"DELETE FROM youtube_videos_raw WHERE video_id IN ({placeholders})", ids)
        r = cur.rowcount
        if include_metrics:
            cur.execute(f"DELETE FROM youtube_metrics WHERE video_id IN ({placeholders})", ids)
            m = cur.rowcount
    return v, r, m


def count_ids(conn: Any, ids: List[str]) -> Tuple[int, int, int]:
    v = r = m = 0
    placeholders = ",".join(["%s"] * len(ids))
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM youtube_videos WHERE video_id IN ({placeholders})", ids)
        v = cur.fetchone()["n"]
        cur.execute(f"SELECT COUNT(*) AS n FROM youtube_videos_raw WHERE video_id IN ({placeholders})", ids)
        r = cur.fetchone()["n"]
        cur.execute(f"SELECT COUNT(*) AS n FROM youtube_metrics WHERE video_id IN ({placeholders})", ids)
        m = cur.fetchone()["n"]
    return v, r, m


def main(argv: List[str]) -> int:
    load_env()
    parser = argparse.ArgumentParser(description="Delete dummy video ids from DB")
    parser.add_argument(
        "--ids",
        nargs="+",
        default=DEFAULT_IDS,
        help=f"Space-separated list of video_ids to delete (default: {', '.join(DEFAULT_IDS)})",
    )
    parser.add_argument("--include-metrics", action="store_true", help="Also delete from youtube_metrics")
    parser.add_argument("--dry-run", action="store_true", help="Preview counts only; no deletion")
    args = parser.parse_args(argv)

    ids = list(dict.fromkeys([s.strip() for s in args.ids if s.strip()]))
    if not ids:
        print("No ids provided.")
        return 1

    cfg = cfg_from_env()
    if not all(cfg.get(k) for k in ("host", "port", "user", "db")):
        print("Missing DB configuration. Ensure DATABASE_URL or DB_* env vars are set.")
        return 2

    conn = connect(cfg)
    try:
        before = count_ids(conn, ids)
        print(f"Existing rows for ids={ids}: videos={before[0]}, raw={before[1]}, metrics={before[2]}")

        if args.dry_run:
            print("Dry run: not deleting.")
            return 0

        v, r, m = delete_ids(conn, ids, include_metrics=args.include_metrics)
        conn.commit()
        print(f"Deleted rows: videos={v}, raw={r}, metrics={m if args.include_metrics else 'skipped'}")

        after = count_ids(conn, ids)
        print(f"Remaining rows: videos={after[0]}, raw={after[1]}, metrics={after[2]}")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        return 3
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
