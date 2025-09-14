#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

import pymysql
from dotenv import load_dotenv

ISRC_RE = re.compile(r"ISRC:?\s*([A-Z]{2}[A-Z0-9]{10})", re.IGNORECASE)


def main(limit: Optional[int] = None) -> int:
    # Load .env explicitly
    env = Path(__file__).resolve().parents[1] / ".env"
    if env.exists():
        load_dotenv(dotenv_path=env, override=True)

    conn = pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER") or "",
        password=os.getenv("DB_PASS") or "",
        db=os.getenv("DB_NAME_PRIVATE") or os.getenv("DB_NAME") or "",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                (
                    "SELECT v.video_id, JSON_EXTRACT(r.raw_data, '$.snippet.description') as descr "
                    "FROM youtube_videos v JOIN youtube_videos_raw r ON v.video_id=r.video_id "
                    "WHERE v.isrc IS NULL"
                )
            )
            rows = cur.fetchall()
        updates = []
        for r in rows:
            descr = r.get("descr")
            if isinstance(descr, (bytes, bytearray)):
                try:
                    descr = descr.decode("utf-8", errors="ignore")
                except Exception:
                    descr = None
            if descr and isinstance(descr, str):
                m = ISRC_RE.search(descr)
                if m:
                    isrc = m.group(1).upper()
                    updates.append((isrc, r["video_id"]))
            if limit and len(updates) >= limit:
                break
        if updates:
            with conn.cursor() as cur:
                cur.executemany("UPDATE youtube_videos SET isrc=%s WHERE video_id=%s", updates)
            conn.commit()
        return len(updates)
    finally:
        conn.close()


if __name__ == "__main__":
    n = main()
    print({"updated": n})
