from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .sentiment_job import YouTubeCommentSentimentJob, seed_version_types
from .youtube_channel_etl import ETLSummary, YouTubeChannelETL


def run_channel_etl(channel_url: str, limit: Optional[int] = None) -> ETLSummary:
    """Module-level entrypoint to run ETL for a single channel.

    Construct the ETL from environment variables and execute run_for_channel.
    Kept simple and picklable for use with multiprocessing.
    """
    # Reload .env from repo root so the latest values (e.g., DB_PORT) apply
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)

    etl = YouTubeChannelETL(
        api_key=os.getenv("YOUTUBE_API_KEY") or "",
        db_host=os.getenv("DB_HOST", "127.0.0.1"),
        db_port=int(os.getenv("DB_PORT", "3306")),
        db_user=os.getenv("DB_USER") or "",
        db_pass=os.getenv("DB_PASS") or "",
        db_name=os.getenv("DB_NAME_PRIVATE") or os.getenv("DB_NAME") or "",
    )
    return etl.run_for_channel(channel_url, limit=limit)


def run_sentiment_scoring(
    *,
    batch_size: int = 500,
    loop: bool = True,
    update_summary: bool = True,
    snapshot_daily: bool = False,
    max_passes: int | None = None,
    max_seconds: int | None = None,
) -> dict:
    """Run sentiment scoring over youtube_comments and upsert summaries.

    - batch_size: process this many unrated comments per pass
    - loop: if True, keep processing until no more comments are left
    - update_summary: if True, refresh youtube_sentiment_summary at the end
    - snapshot_daily: if True, upsert daily snapshot rows in youtube_sentiment

    Returns simple stats dict.
    """
    # Reload .env to ensure DB env is present
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)

    import time

    start = time.time()
    job = YouTubeCommentSentimentJob()
    total_processed = 0
    total_updated = 0
    passes = 0
    while True:
        passes += 1
        stats = job.score_batch(limit=batch_size)
        total_processed += stats.processed
        total_updated += stats.updated
        # Exit conditions: loop disabled, nothing processed, passes/time budget exceeded
        if not loop or stats.processed == 0:
            break
        if max_passes is not None and passes >= max_passes:
            break
        if max_seconds is not None and (time.time() - start) >= max_seconds:
            break

    summary_upserts = 0
    snapshot_inserts = 0
    if update_summary:
        summary_upserts = job.refresh_summary()
    if snapshot_daily:
        snapshot_inserts = job.snapshot_daily_sentiment()

    return {
        "passes": passes,
        "processed": total_processed,
        "updated": total_updated,
        "summary_upserts": summary_upserts,
        "snapshot_inserts": snapshot_inserts,
        "elapsed_sec": round(time.time() - start, 2),
    }


def seed_version_types_defaults() -> int:
    """Seed the version_types table with common values (idempotent)."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
    return seed_version_types()
