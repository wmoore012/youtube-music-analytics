#!/usr/bin/env python3
"""
Normalization helpers to populate music_videos_normalized and reduce nulls without full ETL.

- Reads youtube_videos + youtube_metrics (latest per video)
- Applies artist alias normalization from config/artist_aliases.json
- Links ISRC when present; otherwise attempts a conservative parse using web.youtube_version_parser
- Writes/updates rows in music_videos_normalized

This module is intentionally light-weight to support quick iterative TDD cycles.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from web.db_guard import get_engine


@dataclass
class NormalizedVideo:
    video_id: str
    artist_name: str
    title: Optional[str]
    isrc: Optional[str]
    published_at: Optional[datetime]
    total_views: int
    total_likes: int
    total_comments: int
    est_revenue_usd: float


def load_alias_map(path: str = "config/artist_aliases.json") -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k.strip(): v.strip() for k, v in data.items() if k and v}
    except FileNotFoundError:
        return {}


def canonicalize_artist(name: str, aliases: Dict[str, str]) -> str:
    if not name:
        return name
    return aliases.get(name, name)


def load_latest_metrics_df(engine: Engine) -> pd.DataFrame:
    # Latest metrics per video
    sql = """
    SELECT ym.video_id,
           ym.view_count,
           ym.like_count,
           ym.comment_count,
           ym.metrics_date,
           ROW_NUMBER() OVER (PARTITION BY ym.video_id ORDER BY ym.metrics_date DESC) AS rn
    FROM youtube_metrics ym
    """
    df = pd.read_sql(sql, engine)
    return df[df["rn"] == 1].drop(columns=["rn"]).rename(
        columns={"view_count": "total_views", "like_count": "total_likes", "comment_count": "total_comments"}
    )


def compute_estimated_revenue(views: int, rpm_usd: float = 2.5) -> float:
    return round((views / 1000.0) * rpm_usd, 2)


def build_normalized_rows(engine: Engine) -> Iterable[NormalizedVideo]:
    aliases = load_alias_map()

    videos = pd.read_sql(
        "SELECT video_id, isrc, title, channel_title AS artist_name, published_at FROM youtube_videos",
        engine,
    )
    latest = load_latest_metrics_df(engine)

    df = videos.merge(latest, on="video_id", how="left")

    for _, row in df.iterrows():
        raw_artist = row.get("artist_name") or ""
        artist = canonicalize_artist(str(raw_artist), aliases)
        views = int(row.get("total_views") or 0)
        likes = int(row.get("total_likes") or 0)
        comments = int(row.get("total_comments") or 0)
        rev = compute_estimated_revenue(views)
        yield NormalizedVideo(
            video_id=row["video_id"],
            artist_name=artist or "",
            title=row.get("title"),
            isrc=row.get("isrc"),
            published_at=row.get("published_at"),
            total_views=views,
            total_likes=likes,
            total_comments=comments,
            est_revenue_usd=rev,
        )


def upsert_normalized(engine: Engine, rows: Iterable[NormalizedVideo]) -> int:
    sql = text(
        """
        INSERT INTO music_videos_normalized
            (video_id, artist_name, title, isrc, published_at, total_views, total_likes, total_comments, est_revenue_usd)
        VALUES
            (:video_id, :artist_name, :title, :isrc, :published_at, :total_views, :total_likes, :total_comments, :est_revenue_usd)
        ON DUPLICATE KEY UPDATE
            artist_name=VALUES(artist_name),
            title=VALUES(title),
            isrc=VALUES(isrc),
            published_at=VALUES(published_at),
            total_views=VALUES(total_views),
            total_likes=VALUES(total_likes),
            total_comments=VALUES(total_comments),
            est_revenue_usd=VALUES(est_revenue_usd)
        """
    )
    count = 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(
                sql,
                {
                    "video_id": r.video_id,
                    "artist_name": r.artist_name,
                    "title": r.title,
                    "isrc": r.isrc,
                    "published_at": r.published_at,
                    "total_views": r.total_views,
                    "total_likes": r.total_likes,
                    "total_comments": r.total_comments,
                    "est_revenue_usd": r.est_revenue_usd,
                },
            )
            count += 1
    return count


def run_normalization() -> int:
    engine: Engine = get_engine()
    rows = list(build_normalized_rows(engine))
    return upsert_normalized(engine, rows)


if __name__ == "__main__":
    inserted = run_normalization()
    print(f"✅ Normalized rows upserted: {inserted}")
