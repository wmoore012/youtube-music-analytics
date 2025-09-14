"""
YouTube API Data Retention Compliance Module

This module ensures compliance with YouTube API Terms of Service regarding data retention.
Implements automatic cleanup of YouTube data based on configurable retention periods.

IMPORTANT: Always review current YouTube API Terms of Service for your use case:
- Academic/Research: 30 days typical
- Educational: Up to 180 days may be acceptable
- Commercial: Check current ToS requirements

References:
- YouTube API Terms of Service: https://developers.google.com/youtube/terms/api-services-terms-of-service
- YouTube Data API Policy: https://developers.google.com/youtube/terms/developer-policies
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import pymysql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class YouTubeDataRetentionManager:
    """Manages YouTube data retention compliance."""

    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_user: str,
        db_pass: str,
        db_name: str,
        data_retention_days: Optional[int] = None,
        comment_retention_days: Optional[int] = None,
    ):
        self.db_config = {
            "host": db_host,
            "port": db_port,
            "user": db_user,
            "password": db_pass,
            "database": db_name,
            "charset": "utf8mb4",
        }

        # Load retention periods from environment or use defaults
        self.data_retention_days = data_retention_days or int(os.getenv("YOUTUBE_DATA_RETENTION_DAYS", "30"))
        self.comment_retention_days = comment_retention_days or int(os.getenv("YOUTUBE_COMMENT_RETENTION_DAYS", "30"))

        logger.info(f"YouTube data retention: {self.data_retention_days} days")
        logger.info(f"Comment data retention: {self.comment_retention_days} days")

    def _connect(self):
        """Create database connection."""
        return pymysql.connect(**self.db_config, cursorclass=pymysql.cursors.DictCursor)

    def cleanup_expired_data(self, dry_run: bool = True) -> dict:
        """
        Clean up expired YouTube data according to retention policy.

        Args:
            dry_run: If True, only report what would be deleted without actually deleting

        Returns:
            Dictionary with cleanup statistics
        """
        stats = {
            "videos_raw_deleted": 0,
            "videos_deleted": 0,
            "comments_deleted": 0,
            "metrics_deleted": 0,
            "sentiment_deleted": 0,
            "dry_run": dry_run,
        }

        conn = self._connect()
        try:
            # Calculate cutoff dates
            data_cutoff = datetime.now() - timedelta(days=self.data_retention_days)
            comment_cutoff = datetime.now() - timedelta(days=self.comment_retention_days)

            with conn.cursor() as cur:
                # 1. Clean up comments (user-generated content - most strict)
                if dry_run:
                    cur.execute(
                        "SELECT COUNT(*) as count FROM youtube_comments WHERE published_at < %s",
                        (comment_cutoff,),
                    )
                    stats["comments_deleted"] = cur.fetchone()["count"]
                else:
                    cur.execute(
                        "DELETE FROM youtube_comments WHERE published_at < %s",
                        (comment_cutoff,),
                    )
                    stats["comments_deleted"] = cur.rowcount

                # 2. Clean up sentiment data tied to deleted comments
                if dry_run:
                    cur.execute(
                        """SELECT COUNT(*) as count FROM youtube_sentiment s
                           WHERE NOT EXISTS (
                               SELECT 1 FROM youtube_comments c WHERE c.video_id = s.video_id
                           )"""
                    )
                    stats["sentiment_deleted"] = cur.fetchone()["count"]
                else:
                    cur.execute(
                        """DELETE s FROM youtube_sentiment s
                           WHERE NOT EXISTS (
                               SELECT 1 FROM youtube_comments c WHERE c.video_id = s.video_id
                           )"""
                    )
                    stats["sentiment_deleted"] = cur.rowcount

                # 3. Clean up old metrics data
                if dry_run:
                    cur.execute(
                        "SELECT COUNT(*) as count FROM youtube_metrics WHERE fetched_at < %s",
                        (data_cutoff,),
                    )
                    stats["metrics_deleted"] = cur.fetchone()["count"]
                else:
                    cur.execute(
                        "DELETE FROM youtube_metrics WHERE fetched_at < %s",
                        (data_cutoff,),
                    )
                    stats["metrics_deleted"] = cur.rowcount

                # 4. Clean up raw video data
                if dry_run:
                    cur.execute(
                        "SELECT COUNT(*) as count FROM youtube_videos_raw WHERE fetched_at < %s",
                        (data_cutoff,),
                    )
                    stats["videos_raw_deleted"] = cur.fetchone()["count"]
                else:
                    cur.execute(
                        "DELETE FROM youtube_videos_raw WHERE fetched_at < %s",
                        (data_cutoff,),
                    )
                    stats["videos_raw_deleted"] = cur.rowcount

                # 5. Clean up processed video data for videos with no raw data
                if dry_run:
                    cur.execute(
                        """SELECT COUNT(*) as count FROM youtube_videos v
                           WHERE NOT EXISTS (
                               SELECT 1 FROM youtube_videos_raw r WHERE r.video_id = v.video_id
                           )"""
                    )
                    stats["videos_deleted"] = cur.fetchone()["count"]
                else:
                    cur.execute(
                        """DELETE v FROM youtube_videos v
                           WHERE NOT EXISTS (
                               SELECT 1 FROM youtube_videos_raw r WHERE r.video_id = v.video_id
                           )"""
                    )
                    stats["videos_deleted"] = cur.rowcount

                if not dry_run:
                    conn.commit()

        except Exception as e:
            if not dry_run:
                conn.rollback()
            logger.error(f"Data retention cleanup failed: {e}")
            raise
        finally:
            conn.close()

        return stats

    def get_retention_status(self) -> dict:
        """Get current data retention status."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                # Get oldest data in each table
                status = {}

                tables_queries = {
                    "youtube_comments": "SELECT MIN(published_at) as oldest, COUNT(*) as total FROM youtube_comments",
                    "youtube_videos_raw": "SELECT MIN(fetched_at) as oldest, COUNT(*) as total FROM youtube_videos_raw",
                    "youtube_metrics": "SELECT MIN(fetched_at) as oldest, COUNT(*) as total FROM youtube_metrics",
                    "youtube_videos": "SELECT MIN(published_at) as oldest, COUNT(*) as total FROM youtube_videos",
                }

                for table, query in tables_queries.items():
                    try:
                        cur.execute(query)
                        result = cur.fetchone()
                        oldest = result["oldest"]
                        total = result["total"]

                        if oldest:
                            age_days = (datetime.now() - oldest).days
                            status[table] = {
                                "oldest_record": oldest.isoformat() if oldest else None,
                                "age_days": age_days,
                                "total_records": total,
                                "exceeds_retention": age_days > self.data_retention_days,
                            }
                        else:
                            status[table] = {
                                "oldest_record": None,
                                "age_days": 0,
                                "total_records": total,
                                "exceeds_retention": False,
                            }
                    except Exception as e:
                        logger.warning(f"Could not query {table}: {e}")
                        status[table] = {"error": str(e)}

                return status

        finally:
            conn.close()


def create_retention_job():
    """Factory function to create retention manager from environment variables."""
    return YouTubeDataRetentionManager(
        db_host=os.getenv("DB_HOST", "127.0.0.1"),
        db_port=int(os.getenv("DB_PORT", "3306")),
        db_user=os.getenv("DB_USER", ""),
        db_pass=os.getenv("DB_PASS", ""),
        db_name=os.getenv("DB_NAME", ""),
    )


if __name__ == "__main__":
    # Example usage
    manager = create_retention_job()

    # Check current status
    print("=== YouTube Data Retention Status ===")
    status = manager.get_retention_status()
    for table, info in status.items():
        if "error" in info:
            print(f"{table}: Error - {info['error']}")
        else:
            print(f"{table}: {info['total_records']} records, oldest: {info['age_days']} days")
            if info["exceeds_retention"]:
                print("  ⚠️  WARNING: Exceeds retention policy!")

    # Dry run cleanup
    print("\n=== Dry Run Cleanup ===")
    cleanup_stats = manager.cleanup_expired_data(dry_run=True)
    for key, value in cleanup_stats.items():
        if key != "dry_run" and value > 0:
            print(f"Would delete {value} records from {key}")

    print("\nTo actually perform cleanup, set dry_run=False")
    print(
        f"Current retention policy: {manager.data_retention_days} days (data), {manager.comment_retention_days} days (comments)"
    )
