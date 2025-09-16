#!/usr/bin/env python3
from __future__ import annotations

from dotenv import load_dotenv
from sqlalchemy import text

from web.db_guard import get_engine


def main() -> int:
    load_dotenv()
    eng = get_engine()
    tables = [
        "youtube_videos",
        "youtube_videos_raw",
        "youtube_metrics",
        "music_videos_normalized",
        "video_recording_link",
        "isrc_recordings",
        "songs",
    ]
    with eng.connect() as c:
        for t in tables:
            try:
                n = c.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                print(f"{t:26} {n}")
            except Exception as e:
                print(f"{t:26} ERR {e}")
        present = c.execute(
            text("SELECT COUNT(*) FROM youtube_videos WHERE isrc IS NOT NULL AND TRIM(COALESCE(isrc,'')) <> ''")
        ).scalar()
        print(f"youtube_videos.isrc present: {present}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
