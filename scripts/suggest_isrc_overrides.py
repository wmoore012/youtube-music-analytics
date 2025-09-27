#!/usr / bin / env python3
"""
Generate a CSV of videos missing ISRC to help create config / video_isrc_overrides.json.

Outputs: video_id,title,channel_title,published_at
Usage: python scripts / suggest_isrc_overrides.py > missing_isrc_candidates.csv
Then fill config / video_isrc_overrides.json with mappings: { "<video_id>": "<ISRC>" }
"""
from __future__ import annotations

import csv
import sys

from dotenv import load_dotenv
from sqlalchemy import text

from web.db_guard import get_engine


def main() -> int:
    load_dotenv()
    eng = get_engine()
    sql = text(
        """
        SELECT video_id, title, channel_title, published_at
        FROM youtube_videos
        WHERE isrc IS NULL OR TRIM(COALESCE(isrc,'')) = ''
        ORDER BY published_at DESC
        LIMIT 200
        """
    )
    with eng.connect() as c:
        rows = c.execute(sql).fetchall()
    w = csv.writer(sys.stdout)
    w.writerow(["video_id", "title", "channel_title", "published_at"])  # header
    for r in rows:
        w.writerow([r[0], r[1], r[2], r[3]])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
