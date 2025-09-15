#!/usr/bin/env python3
"""
Daily Raw Data Cleanup

Cleans up raw YouTube data immediately after ETL ingestion to remove:
- Videos with NULL/empty channel titles
- Videos from unauthorized channels (not in .env)
- Orphaned records and data quality issues

This should run as part of daily cron jobs after ETL.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_configured_artists():
    """Get list of configured artists from .env file."""
    configured_artists = []

    # Read .env file
    env_path = ".env"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("YT_") and "_YT=" in line:
                    # Extract artist name from YT_ARTISTNAME_YT=URL format
                    artist_key = line.split("=")[0]
                    artist_name = artist_key.replace("YT_", "").replace("_YT", "")

                    # Convert to proper artist name format
                    if artist_name == "BICFIZZLE":
                        configured_artists.append("BiC Fizzle")
                    elif artist_name == "COBRAH":
                        configured_artists.append("COBRAH")
                    elif artist_name == "COROOK":
                        configured_artists.append("Corook")
                    elif artist_name == "RAICHE":
                        configured_artists.append("Raiche")
                    elif artist_name == "RE6CE":
                        configured_artists.append("re6ce")
                    elif artist_name == "FLYANABOSS":
                        configured_artists.append("Flyana Boss")

    logger.info(f"✅ Found {len(configured_artists)} configured artists: {configured_artists}")
    return configured_artists


def cleanup_raw_videos():
    """Clean up raw videos table."""
    engine = get_engine()

    with engine.connect() as conn:
        logger.info("🧹 Starting raw videos cleanup...")

        # 1. Remove processed raw entries older than 7 days
        result = conn.execute(
            text(
                """
            DELETE FROM youtube_videos_raw
            WHERE processed = 1 AND fetched_at < DATE_SUB(NOW(), INTERVAL 7 DAY)
        """
            )
        )
        old_processed = result.rowcount
        logger.info(f"   - Deleted {old_processed} old processed raw entries")

        # 2. Remove duplicate raw entries (same video_id, keep latest)
        result = conn.execute(
            text(
                """
            DELETE r1 FROM youtube_videos_raw r1
            INNER JOIN youtube_videos_raw r2
            WHERE r1.fetched_at < r2.fetched_at AND r1.video_id = r2.video_id
        """
            )
        )
        duplicates_deleted = result.rowcount
        logger.info(f"   - Deleted {duplicates_deleted} duplicate raw entries")

        conn.commit()

        total_deleted = old_processed + duplicates_deleted
        logger.info(f"✅ Raw videos cleanup complete: {total_deleted} records removed")

        return total_deleted


def cleanup_processed_videos():
    """Clean up processed videos table."""
    engine = get_engine()
    configured_artists = get_configured_artists()

    with engine.connect() as conn:
        logger.info("🧹 Starting processed videos cleanup...")

        # 1. Remove videos with NULL/empty channel titles
        result = conn.execute(
            text(
                """
            DELETE FROM youtube_videos
            WHERE channel_title IS NULL OR channel_title = '' OR TRIM(channel_title) = ''
        """
            )
        )
        null_deleted = result.rowcount
        logger.info(f"   - Deleted {null_deleted} videos with NULL/empty channel titles")

        # 2. Remove videos from unauthorized channels
        if configured_artists:
            placeholders = ", ".join([f"'{artist}'" for artist in configured_artists])
            unauthorized_query = f"""
                DELETE FROM youtube_videos
                WHERE channel_title NOT IN ({placeholders})
            """
            result = conn.execute(text(unauthorized_query))
            unauthorized_deleted = result.rowcount
            logger.info(f"   - Deleted {unauthorized_deleted} videos from unauthorized channels")

        conn.commit()

        total_deleted = null_deleted + unauthorized_deleted
        logger.info(f"✅ Processed videos cleanup complete: {total_deleted} records removed")

        return total_deleted


def cleanup_orphaned_data():
    """Clean up orphaned metrics and comments."""
    engine = get_engine()

    with engine.connect() as conn:
        logger.info("🧹 Starting orphaned data cleanup...")

        # 1. Remove orphaned metrics (no corresponding video)
        result = conn.execute(
            text(
                """
            DELETE ym FROM youtube_metrics ym
            LEFT JOIN youtube_videos yv ON ym.video_id = yv.video_id
            WHERE yv.video_id IS NULL
        """
            )
        )
        orphaned_metrics = result.rowcount
        logger.info(f"   - Deleted {orphaned_metrics} orphaned metrics")

        # 2. Remove orphaned comments (no corresponding video)
        result = conn.execute(
            text(
                """
            DELETE yc FROM youtube_comments yc
            LEFT JOIN youtube_videos yv ON yc.video_id = yv.video_id
            WHERE yv.video_id IS NULL
        """
            )
        )
        orphaned_comments = result.rowcount
        logger.info(f"   - Deleted {orphaned_comments} orphaned comments")

        # 3. Remove orphaned sentiment records (no corresponding comment)
        result = conn.execute(
            text(
                """
            DELETE cs FROM comment_sentiment cs
            LEFT JOIN youtube_comments yc ON cs.comment_id = yc.comment_id
            WHERE yc.comment_id IS NULL
        """
            )
        )
        orphaned_sentiment = result.rowcount
        logger.info(f"   - Deleted {orphaned_sentiment} orphaned sentiment records")

        conn.commit()

        total_deleted = orphaned_metrics + orphaned_comments + orphaned_sentiment
        logger.info(f"✅ Orphaned data cleanup complete: {total_deleted} records removed")

        return total_deleted


def validate_data_integrity():
    """Validate data integrity after cleanup."""
    engine = get_engine()
    configured_artists = get_configured_artists()

    with engine.connect() as conn:
        logger.info("🔍 Validating data integrity...")

        # Check for remaining NULL channel titles
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) FROM youtube_videos
            WHERE channel_title IS NULL OR channel_title = ''
        """
            )
        ).fetchone()
        null_count = result[0]

        # Check for unauthorized artists
        if configured_artists:
            placeholders = ", ".join([f"'{artist}'" for artist in configured_artists])
            unauthorized_query = f"""
                SELECT COUNT(*) FROM youtube_videos
                WHERE channel_title NOT IN ({placeholders})
            """
            result = conn.execute(text(unauthorized_query)).fetchone()
            unauthorized_count = result[0]
        else:
            unauthorized_count = 0

        # Check for orphaned records
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) FROM youtube_metrics ym
            LEFT JOIN youtube_videos yv ON ym.video_id = yv.video_id
            WHERE yv.video_id IS NULL
        """
            )
        ).fetchone()
        orphaned_metrics = result[0]

        # Generate validation report
        issues = []
        if null_count > 0:
            issues.append(f"{null_count} videos with NULL channel titles")
        if unauthorized_count > 0:
            issues.append(f"{unauthorized_count} videos from unauthorized channels")
        if orphaned_metrics > 0:
            issues.append(f"{orphaned_metrics} orphaned metrics")

        if issues:
            logger.warning(f"⚠️  Data integrity issues found: {', '.join(issues)}")
            return False
        else:
            logger.info("✅ Data integrity validation passed")
            return True


def generate_cleanup_report():
    """Generate cleanup summary report."""
    engine = get_engine()

    with engine.connect() as conn:
        # Get current data stats
        result = conn.execute(
            text(
                """
            SELECT
                COUNT(DISTINCT channel_title) as artists,
                COUNT(DISTINCT video_id) as videos,
                COUNT(*) as total_records
            FROM youtube_videos
            WHERE channel_title IS NOT NULL
        """
            )
        ).fetchone()

        artists, videos, total_records = result

        # Get comment stats
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) FROM youtube_comments
        """
            )
        ).fetchone()
        comments = result[0]

        # Get sentiment coverage
        result = conn.execute(
            text(
                """
            SELECT COUNT(DISTINCT comment_id) FROM comment_sentiment
        """
            )
        ).fetchone()
        sentiment_coverage = result[0]

        report = {
            "timestamp": datetime.now().isoformat(),
            "cleanup_type": "daily_raw_cleanup",
            "final_stats": {
                "artists": artists,
                "videos": videos,
                "total_records": total_records,
                "comments": comments,
                "sentiment_coverage": sentiment_coverage,
                "sentiment_coverage_percent": (sentiment_coverage / comments * 100) if comments > 0 else 0,
            },
        }

        logger.info(f"📊 Final stats: {artists} artists, {videos} videos, {comments} comments")
        logger.info(
            f"📊 Sentiment coverage: {sentiment_coverage}/{comments} ({report['final_stats']['sentiment_coverage_percent']:.1f}%)"
        )

        return report


def main():
    """Main cleanup function."""
    logger.info("🚀 Starting daily raw data cleanup...")

    try:
        # Run cleanup operations
        raw_deleted = cleanup_raw_videos()
        processed_deleted = cleanup_processed_videos()
        orphaned_deleted = cleanup_orphaned_data()

        # Validate integrity
        integrity_ok = validate_data_integrity()

        # Generate report
        report = generate_cleanup_report()

        total_deleted = raw_deleted + processed_deleted + orphaned_deleted

        if integrity_ok:
            logger.info(f"🎉 Daily cleanup completed successfully!")
            logger.info(f"📊 Total records removed: {total_deleted}")
            logger.info(f"✅ Data integrity validated")
            return True
        else:
            logger.error("❌ Data integrity validation failed")
            return False

    except Exception as e:
        logger.error(f"❌ Daily cleanup failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
