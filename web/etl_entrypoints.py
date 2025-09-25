from __future__ import annotations

import os
from pathlib import Path
import time
from typing import Optional, Tuple

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
        db_name=os.getenv("DB_NAME") or "",
    )
    return etl.run_for_channel(channel_url, limit=limit)


def _setup_sentiment_environment() -> None:
    """Reload environment variables to ensure database configuration is available."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)


def _should_continue_processing(
    loop: bool, stats_processed: int, passes: int, start_time: float, max_passes: int | None, max_seconds: int | None
) -> bool:
    """Determine if sentiment processing should continue based on exit conditions."""
    if not loop or stats_processed == 0:
        return False
    if max_passes is not None and passes >= max_passes:
        return False
    if max_seconds is not None and (time.time() - start_time) >= max_seconds:
        return False
    return True


def _process_sentiment_batches(
    job: "YouTubeCommentSentimentJob",
    batch_size: int,
    loop: bool,
    max_passes: int | None,
    max_seconds: int | None,
    start_time: float,
) -> Tuple[int, int, int]:
    """Process sentiment scoring batches and return statistics."""
    total_processed = 0
    total_updated = 0
    passes = 0

    while True:
        passes += 1
        stats = job.score_batch(limit=batch_size)
        total_processed += stats.processed
        total_updated += stats.updated

        if not _should_continue_processing(loop, stats.processed, passes, start_time, max_passes, max_seconds):
            break

    return passes, total_processed, total_updated


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

    _setup_sentiment_environment()
    start_time = time.time()
    job = YouTubeCommentSentimentJob()

    # Process sentiment batches
    passes, total_processed, total_updated = _process_sentiment_batches(
        job, batch_size, loop, max_passes, max_seconds, start_time
    )

    # Handle post-processing tasks
    summary_upserts = job.refresh_summary() if update_summary else 0
    snapshot_inserts = job.snapshot_daily_sentiment() if snapshot_daily else 0

    return {
        "passes": passes,
        "processed": total_processed,
        "updated": total_updated,
        "summary_upserts": summary_upserts,
        "snapshot_inserts": snapshot_inserts,
        "elapsed_sec": round(time.time() - start_time, 2),
    }


def seed_version_types_defaults() -> int:
    """Seed the version_types table with common values (idempotent)."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
    return seed_version_types()
