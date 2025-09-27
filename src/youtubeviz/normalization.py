#!/usr / bin / env python3
"""
Normalization helpers to populate music_videos_normalized and reduce nulls without full ETL.

- Reads youtube_videos + youtube_metrics (latest per video)
- Applies artist alias normalization from config / artist_aliases.json
- Links ISRC when present; otherwise attempts a conservative parse using web.youtube_version_parser
- Writes / updates rows in music_videos_normalized

This module is intentionally light - weight to support quick iterative TDD cycles.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import os
from typing import Dict, Iterable, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from web.db_guard import get_engine
from web.youtube_version_parser import extract_artists_from_title


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


def load_alias_map(path: str = "config / artist_aliases.json") -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf - 8") as f:
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
    return (
        df[df["rn"] == 1]
        .drop(columns=["rn"])
        .rename(columns={"view_count": "total_views", "like_count": "total_likes", "comment_count": "total_comments"})
    )


def compute_estimated_revenue(views: int, rpm_usd: float = 2.5) -> float:
    return round((views / 1000.0) * rpm_usd, 2)


def _load_vrl_map(engine: Engine) -> Dict[str, str]:
    """Load video_id -> isrc mapping from video_recording_link and optional overrides file.

    Precedence: overrides file > DB links.
    """
    mapping: Dict[str, str] = {}
    # DB links
    try:
        df = pd.read_sql("SELECT video_id, isrc FROM video_recording_link", engine)
        mapping.update({row["video_id"]: row["isrc"] for _, row in df.iterrows()})
    except Exception:
        pass
    # Optional overrides file
    try:
        with open("config / video_isrc_overrides.json", "r", encoding="utf - 8") as f:
            overrides = json.load(f) or {}
        # Normalize values
        for vid, isrc in overrides.items():
            if not vid or not isrc:
                continue
            mapping[str(vid).strip()] = str(isrc).strip().upper()
    except FileNotFoundError:
        pass
    except Exception:
        # Ignore malformed file
        pass
    return mapping


def build_normalized_rows(engine: Engine) -> Iterable[NormalizedVideo]:
    aliases = load_alias_map()
    vrl_map = _load_vrl_map(engine)

    videos = pd.read_sql(
        "SELECT video_id, isrc, title, channel_title AS artist_name, published_at FROM youtube_videos",
        engine,
    )
    # Fill missing ISRC from video_recording_link if available
    if not videos.empty and vrl_map:
        from pandas import Series as PdSeries  # type: ignore

        def _fill_isrc(row: PdSeries) -> Optional[str]:
            val = row.get("isrc")
            # Treat NaN / None / blank as missing
            if pd.isna(val) or (str(val).strip() == ""):
                return vrl_map.get(row["video_id"])
            return str(val).strip()

        # type: ignore[assignment]
        videos["isrc"] = videos.apply(_fill_isrc, axis=1)
    latest = load_latest_metrics_df(engine)

    df = videos.merge(latest, on="video_id", how="left")

    for _, row in df.iterrows():
        # Title normalization: convert NaN to None and strip
        raw_title = row.get("title")
        title_val = None
        if pd.notna(raw_title) and str(raw_title).strip():
            title_val = str(raw_title).strip()

        # Artist normalization: prefer channel_title; if empty, parse from title
        raw_artist = row.get("artist_name") or ""
        artist = canonicalize_artist(str(raw_artist).strip(), aliases) if raw_artist else ""
        if not artist and title_val:
            artists, _clean_title = extract_artists_from_title(title_val, "")
            if artists:
                artist = canonicalize_artist(artists[0], aliases)

        # Published_at: coerce pandas NaT to None and to native datetime
        pub = row.get("published_at")
        if pd.isna(pub):
            pub_val = None
        else:
            try:
                # pandas.Timestamp -> datetime
                pub_val = pub.to_pydatetime() if hasattr(pub, "to_pydatetime") else pub
            except Exception:
                pub_val = None

        views = int(row.get("total_views") or 0)
        likes = int(row.get("total_likes") or 0)
        comments = int(row.get("total_comments") or 0)
        rev = compute_estimated_revenue(views)
        # Normalize ISRC to None if blank / NaN
        isrc_raw = row.get("isrc")
        isrc_val = None
        if pd.notna(isrc_raw) and str(isrc_raw).strip():
            isrc_val = str(isrc_raw).strip()

        yield NormalizedVideo(
            video_id=row["video_id"],
            artist_name=artist or "",
            title=title_val,
            isrc=isrc_val,
            published_at=pub_val,
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
