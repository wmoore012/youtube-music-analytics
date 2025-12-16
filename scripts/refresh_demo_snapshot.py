#!/usr/bin/env python3
"""Refresh the curated demo cohort for the MusicScope Streamlit app.

This is a **maintainer-only** utility that regenerates:

- demo_data/curated_cohort.json
- music_analysis_tables/normalized_music_videos.csv
- music_analysis_tables/artist_music_summary.csv

It reads from the local MySQL analytics warehouse using the same helpers and
ETL logic used in production. You should run this script only when:

- The underlying warehouse has been refreshed with new data, and
- You want the checked-in demo snapshot to reflect that updated state.

Requirements
------------
- Local MySQL instance reachable via DB_* environment variables or .env
- The `web.etl_helpers.get_engine` function must be able to connect

Example usage (from repo root):

    python -m scripts.refresh_demo_snapshot

This script is **not** intended for general users or evaluators; they should
use the built-in demo mode in `streamlit_app.py` which reads the committed
JSON/CSV snapshot without touching any database.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from tools.specialized.analytics.create_music_videos_table import (
    create_music_summary_by_artist,
    create_music_videos_table,
)
from web.etl_helpers import get_engine


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "music_analysis_tables"
DEMO_DATA_PATH = BASE_DIR / "demo_data" / "curated_cohort.json"


def _ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEMO_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)


def build_curated_cohort(
    *,
    top_artists: int = 5,
    top_videos_per_artist: int = 10,
) -> Dict[str, Any]:
    """Build the curated cohort payload from the live warehouse.

    The structure matches what `streamlit_app.py` expects in demo mode.
    """

    # Ensure we can connect; this will fail fast if DB is misconfigured.
    _ = get_engine()

    music_videos = create_music_videos_table()
    artist_summary = create_music_summary_by_artist()

    if artist_summary.empty or music_videos.empty:
        raise RuntimeError(
            "Artist summary or music videos are empty. Ensure the ETL has "
            "populated the analytics tables before refreshing the demo snapshot."
        )

    # Persist full tables used by the Streamlit app in production mode.
    _ensure_dirs()
    music_videos.to_csv(DATA_DIR / "normalized_music_videos.csv", index=False)
    artist_summary.to_csv(DATA_DIR / "artist_music_summary.csv", index=False)

    # Choose top artists by total views for the curated cohort.
    top_artists_df = artist_summary.sort_values("total_views", ascending=False).head(top_artists).reset_index(drop=True)

    artists: List[Dict[str, Any]] = []

    for _, row in top_artists_df.iterrows():
        artist_name = str(row["artist_name"])

        # Pick the top N videos per artist by view_count.
        artist_videos = (
            music_videos[music_videos["artist_name"] == artist_name]
            .sort_values("view_count", ascending=False)
            .head(top_videos_per_artist)
        )

        videos_payload: List[Dict[str, Any]] = []
        for _, v in artist_videos.iterrows():
            videos_payload.append(
                {
                    "video_id": str(v["video_id"]),
                    "title": str(v["title"]),
                    "published_at": pd.to_datetime(v["published_at"]).isoformat(),
                    "view_count": int(v["view_count"]),
                    "views_per_day": float(v["views_per_day"]),
                    "engagement_rate": float(v["engagement_rate"]),
                    "like_rate": float(v["like_rate"]),
                    "comment_rate": float(v["comment_rate"]),
                    "video_type": str(v["video_type"]),
                }
            )

        artists.append(
            {
                "name": artist_name,
                # Channel ID and primary_color are optional for the current
                # Streamlit demo; they can be filled in later if desired.
                "channel_id": None,
                "primary_color": None,
                "metrics": {
                    "total_views": int(row["total_views"]),
                    "total_videos": int(row["total_videos"]),
                    "views_30d": None,
                    "views_7d": None,
                    "engagement_rate": float(row["avg_engagement_rate"]),
                    "momentum_score": None,
                },
                "videos": videos_payload,
            }
        )

    return {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "artists": artists,
    }


def main() -> None:
    _ensure_dirs()
    payload = build_curated_cohort()
    DEMO_DATA_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Updated demo cohort at {DEMO_DATA_PATH}")


if __name__ == "__main__":  # pragma: no cover
    main()
