"""
Safe Data Retention Manager for YouTube Analytics Platform

This module provides controlled data deletion with dependency checking to ensure
data integrity while respecting YouTube API Terms of Service retention policies.

Key Features:
- Dependency checking before deletion to prevent orphaned data
- Configurable retention periods via environment variables
- Detailed deletion reports with dependency information
- Dry-run mode for safe testing
- UTC timestamp consistency across all operations
- Explicit confirmation required for destructive operations

Usage:
    from web.data_retention_manager import DataRetentionManager

    manager = DataRetentionManager(engine, retention_days=30)

    # Check what would be deleted (safe)
    report = manager.check_deletion_safety()
    print(f"Videos eligible for deletion: {len(report.eligible_videos)}")

    # Perform actual deletion (requires confirmation)
    result = manager.delete_expired_videos(dry_run=False)
    print(f"Deleted {result.videos_deleted} videos")
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import os
from typing import Dict, List, Optional, Set

from sqlalchemy import Engine, delete, select, text
from sqlalchemy.engine import Connection

# Constants
DEFAULT_RETENTION_DAYS = 30
DEFAULT_BATCH_SIZE = 500
CONNECTION_TIMEOUT = 180

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class DependencyInfo:
    """Information about dependencies that prevent deletion."""

    table_name: str
    count: int
    sample_ids: List[str]


@dataclass
class VideoInfo:
    """Information about a video and its dependencies."""

    video_id: str
    title: str
    published_at: datetime
    age_days: int
    dependencies: List[DependencyInfo]

    @property
    def has_dependencies(self) -> bool:
        """Check if video has any dependencies that prevent deletion."""
        return len(self.dependencies) > 0


@dataclass
class DeletionReport:
    """Report of videos eligible for deletion and their dependencies."""

    eligible_videos: List[VideoInfo]
    blocked_videos: List[VideoInfo]
    total_videos_checked: int
    retention_cutoff_date: datetime

    @property
    def videos_safe_to_delete(self) -> List[VideoInfo]:
        """Videos that can be safely deleted (no dependencies)."""
        return [v for v in self.eligible_videos if not v.has_dependencies]

    @property
    def videos_with_dependencies(self) -> List[VideoInfo]:
        """Videos that cannot be deleted due to dependencies."""
        return [v for v in self.eligible_videos if v.has_dependencies] + self.blocked_videos


@dataclass
class DeletionResult:
    """Result of a deletion operation."""

    videos_deleted: int
    related_data_deleted: Dict[str, int]
    errors: List[str]
    deletion_timestamp: datetime


class DataRetentionManager:
    """
    Safe data retention manager with dependency checking.

    This class provides controlled deletion of expired YouTube data while ensuring
    referential integrity and providing detailed reporting of all operations.
    """

    def __init__(self, engine: Engine, retention_days: Optional[int] = None):
        """
        Initialize the data retention manager.

        Args:
            engine: SQLAlchemy engine for database operations
            retention_days: Number of days to retain data (defaults to env var or 30)
        """
        self.engine = engine
        self.retention_days = retention_days or int(os.getenv("YOUTUBE_DATA_RETENTION_DAYS", DEFAULT_RETENTION_DAYS))

        # Calculate cutoff date (UTC)
        self.cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.retention_days)

        logger.info(f"DataRetentionManager initialized with {self.retention_days} day retention")
        logger.info(f"Cutoff date: {self.cutoff_date.isoformat()}")

    def check_deletion_safety(self, video_ids: Optional[List[str]] = None) -> DeletionReport:
        """
        Check which videos can be safely deleted and identify dependencies.

        Args:
            video_ids: Specific video IDs to check (if None, checks all expired videos)

        Returns:
            DeletionReport with detailed information about eligible videos
        """
        logger.info("Starting deletion safety check")

        with self.engine.connect() as conn:
            # Get videos older than retention period
            if video_ids:
                videos = self._get_specific_videos(conn, video_ids)
            else:
                videos = self._get_expired_videos(conn)

            logger.info(f"Found {len(videos)} videos to check")

            eligible_videos = []
            blocked_videos = []

            for video in videos:
                dependencies = self._check_video_dependencies(conn, video["video_id"])

                # Handle timezone-naive datetimes from database
                published_at = video["published_at"]
                if published_at.tzinfo is None:
                    published_at = published_at.replace(tzinfo=timezone.utc)

                video_info = VideoInfo(
                    video_id=video["video_id"],
                    title=video["title"] or "Unknown Title",
                    published_at=published_at,
                    age_days=(datetime.now(timezone.utc) - published_at).days,
                    dependencies=dependencies,
                )

                if video_info.age_days >= self.retention_days:
                    eligible_videos.append(video_info)
                else:
                    blocked_videos.append(video_info)

        report = DeletionReport(
            eligible_videos=eligible_videos,
            blocked_videos=blocked_videos,
            total_videos_checked=len(videos),
            retention_cutoff_date=self.cutoff_date,
        )

        logger.info(
            f"Safety check complete: {len(report.videos_safe_to_delete)} safe to delete, "
            f"{len(report.videos_with_dependencies)} have dependencies"
        )

        return report

    def delete_expired_videos(self, dry_run: bool = True, confirm_deletion: bool = False) -> DeletionResult:
        """
        Delete expired videos that have no dependencies.

        Args:
            dry_run: If True, only report what would be deleted without actual deletion
            confirm_deletion: Must be True for actual deletion (safety check)

        Returns:
            DeletionResult with details of the deletion operation
        """
        if not dry_run and not confirm_deletion:
            raise ValueError("confirm_deletion must be True for actual deletion operations")

        logger.info(f"Starting deletion operation (dry_run={dry_run})")

        # Get deletion report
        report = self.check_deletion_safety()
        safe_videos = report.videos_safe_to_delete

        if not safe_videos:
            logger.info("No videos eligible for deletion")
            return DeletionResult(
                videos_deleted=0, related_data_deleted={}, errors=[], deletion_timestamp=datetime.now(timezone.utc)
            )

        if dry_run:
            logger.info(f"DRY RUN: Would delete {len(safe_videos)} videos")
            for video in safe_videos[:5]:  # Show first 5 as sample
                logger.info(f"  - {video.video_id}: {video.title[:50]}... (age: {video.age_days} days)")
            if len(safe_videos) > 5:
                logger.info(f"  ... and {len(safe_videos) - 5} more videos")

            return DeletionResult(
                videos_deleted=len(safe_videos),
                related_data_deleted={},
                errors=[],
                deletion_timestamp=datetime.now(timezone.utc),
            )

        # Perform actual deletion
        return self._perform_deletion(safe_videos)

    def cleanup_orphaned_data(self, dry_run: bool = True) -> DeletionResult:
        """
        Clean up orphaned data that references non-existent videos.

        Args:
            dry_run: If True, only report what would be cleaned without actual deletion

        Returns:
            DeletionResult with details of the cleanup operation
        """
        logger.info(f"Starting orphaned data cleanup (dry_run={dry_run})")

        orphaned_counts = {}
        errors = []

        with self.engine.connect() as conn:
            # Check for orphaned metrics
            orphaned_metrics = self._find_orphaned_metrics(conn)
            orphaned_counts["youtube_metrics"] = len(orphaned_metrics)

            # Check for orphaned comments
            orphaned_comments = self._find_orphaned_comments(conn)
            orphaned_counts["youtube_comments"] = len(orphaned_comments)

            # Check for orphaned ISRC links
            orphaned_links = self._find_orphaned_isrc_links(conn)
            orphaned_counts["video_recording_link"] = len(orphaned_links)

            if not dry_run:
                # Delete orphaned data
                try:
                    if orphaned_metrics:
                        placeholders = ",".join([f":metric_id_{i}" for i in range(len(orphaned_metrics))])
                        params = {f"metric_id_{i}": vid for i, vid in enumerate(orphaned_metrics)}
                        conn.execute(
                            text(
                                f"""
                            DELETE FROM youtube_metrics WHERE video_id IN ({placeholders})
                        """
                            ),
                            params,
                        )

                    if orphaned_comments:
                        placeholders = ",".join([f":comment_id_{i}" for i in range(len(orphaned_comments))])
                        params = {f"comment_id_{i}": vid for i, vid in enumerate(orphaned_comments)}
                        conn.execute(
                            text(
                                f"""
                            DELETE FROM youtube_comments WHERE video_id IN ({placeholders})
                        """
                            ),
                            params,
                        )

                    if orphaned_links:
                        placeholders = ",".join([f":link_id_{i}" for i in range(len(orphaned_links))])
                        params = {f"link_id_{i}": vid for i, vid in enumerate(orphaned_links)}
                        conn.execute(
                            text(
                                f"""
                            DELETE FROM video_recording_link WHERE video_id IN ({placeholders})
                        """
                            ),
                            params,
                        )

                    conn.commit()
                    logger.info("Orphaned data cleanup completed successfully")

                except Exception as e:
                    conn.rollback()
                    error_msg = f"Error during orphaned data cleanup: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)

        total_cleaned = sum(orphaned_counts.values())
        logger.info(f"Cleanup complete: {total_cleaned} orphaned records {'would be' if dry_run else ''} removed")

        return DeletionResult(
            videos_deleted=0,
            related_data_deleted=orphaned_counts,
            errors=errors,
            deletion_timestamp=datetime.now(timezone.utc),
        )

    def _get_expired_videos(self, conn: Connection) -> List[Dict]:
        """Get videos older than the retention period."""
        query = text(
            """
            SELECT video_id, title, published_at, fetched_at
            FROM youtube_videos
            WHERE published_at < :cutoff_date
            ORDER BY published_at ASC
        """
        )

        result = conn.execute(query, {"cutoff_date": self.cutoff_date})
        return [dict(row._mapping) if hasattr(row, "_mapping") else dict(row) for row in result]

    def _get_specific_videos(self, conn: Connection, video_ids: List[str]) -> List[Dict]:
        """Get specific videos by their IDs."""
        placeholders = ",".join([f":video_id_{i}" for i in range(len(video_ids))])
        query = text(
            f"""
            SELECT video_id, title, published_at, fetched_at
            FROM youtube_videos
            WHERE video_id IN ({placeholders})
            ORDER BY published_at ASC
        """
        )

        params = {f"video_id_{i}": vid for i, vid in enumerate(video_ids)}
        result = conn.execute(query, params)
        return [dict(row._mapping) if hasattr(row, "_mapping") else dict(row) for row in result]

    def _check_video_dependencies(self, conn: Connection, video_id: str) -> List[DependencyInfo]:
        """Check for dependencies that prevent video deletion."""
        dependencies = []

        # Check youtube_metrics
        result = conn.execute(
            text("SELECT COUNT(*) FROM youtube_metrics WHERE video_id = :video_id"), {"video_id": video_id}
        )
        metrics_count = result.scalar() if hasattr(result, "scalar") else result

        if metrics_count > 0:
            # Get sample metric dates
            sample_result = conn.execute(
                text(
                    "SELECT metrics_date FROM youtube_metrics WHERE video_id = :video_id "
                    "ORDER BY metrics_date DESC LIMIT 3"
                ),
                {"video_id": video_id},
            )
            sample_dates = sample_result.fetchall() if hasattr(sample_result, "fetchall") else sample_result

            dependencies.append(
                DependencyInfo(
                    table_name="youtube_metrics", count=metrics_count, sample_ids=[str(row[0]) for row in sample_dates]
                )
            )

        # Check youtube_comments
        result = conn.execute(
            text("SELECT COUNT(*) FROM youtube_comments WHERE video_id = :video_id"), {"video_id": video_id}
        )
        comments_count = result.scalar() if hasattr(result, "scalar") else result

        if comments_count > 0:
            # Get sample comment IDs
            sample_result = conn.execute(
                text(
                    "SELECT comment_id FROM youtube_comments WHERE video_id = :video_id "
                    "ORDER BY published_at DESC LIMIT 3"
                ),
                {"video_id": video_id},
            )
            sample_comments = sample_result.fetchall() if hasattr(sample_result, "fetchall") else sample_result

            dependencies.append(
                DependencyInfo(
                    table_name="youtube_comments",
                    count=comments_count,
                    sample_ids=[str(row[0]) for row in sample_comments],
                )
            )

        # Check video_recording_link
        result = conn.execute(
            text("SELECT COUNT(*) FROM video_recording_link WHERE video_id = :video_id"), {"video_id": video_id}
        )
        isrc_links_count = result.scalar() if hasattr(result, "scalar") else result

        if isrc_links_count > 0:
            # Get sample ISRCs
            sample_result = conn.execute(
                text("SELECT isrc FROM video_recording_link WHERE video_id = :video_id LIMIT 3"), {"video_id": video_id}
            )
            sample_isrcs = sample_result.fetchall() if hasattr(sample_result, "fetchall") else sample_result

            dependencies.append(
                DependencyInfo(
                    table_name="video_recording_link",
                    count=isrc_links_count,
                    sample_ids=[str(row[0]) for row in sample_isrcs],
                )
            )

        return dependencies

    def _perform_deletion(self, videos: List[VideoInfo]) -> DeletionResult:
        """Perform the actual deletion of videos and log the operation."""
        deleted_count = 0
        related_data_deleted = {}
        errors = []

        logger.info(f"Starting deletion of {len(videos)} videos")

        with self.engine.connect() as conn:
            try:
                # Delete videos in batches
                for i in range(0, len(videos), DEFAULT_BATCH_SIZE):
                    batch = videos[i : i + DEFAULT_BATCH_SIZE]
                    video_ids = [v.video_id for v in batch]

                    # Delete the videos
                    placeholders = ",".join([f":video_id_{j}" for j in range(len(video_ids))])
                    params = {f"video_id_{j}": vid for j, vid in enumerate(video_ids)}

                    result = conn.execute(
                        text(
                            f"""
                        DELETE FROM youtube_videos WHERE video_id IN ({placeholders})
                    """
                        ),
                        params,
                    )

                    batch_deleted = result.rowcount
                    deleted_count += batch_deleted

                    logger.info(f"Deleted batch {i//DEFAULT_BATCH_SIZE + 1}: {batch_deleted} videos")

                conn.commit()
                logger.info(f"Successfully deleted {deleted_count} videos")

            except Exception as e:
                conn.rollback()
                error_msg = f"Error during video deletion: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

        return DeletionResult(
            videos_deleted=deleted_count,
            related_data_deleted=related_data_deleted,
            errors=errors,
            deletion_timestamp=datetime.now(timezone.utc),
        )

    def _find_orphaned_metrics(self, conn: Connection) -> List[str]:
        """Find metrics records that reference non-existent videos."""
        query = text(
            """
            SELECT DISTINCT m.video_id
            FROM youtube_metrics m
            LEFT JOIN youtube_videos v ON m.video_id = v.video_id
            WHERE v.video_id IS NULL
        """
        )

        result = conn.execute(query)
        return [row[0] for row in result]

    def _find_orphaned_comments(self, conn: Connection) -> List[str]:
        """Find comment records that reference non-existent videos."""
        query = text(
            """
            SELECT DISTINCT c.video_id
            FROM youtube_comments c
            LEFT JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE v.video_id IS NULL
        """
        )

        result = conn.execute(query)
        return [row[0] for row in result]

    def _find_orphaned_isrc_links(self, conn: Connection) -> List[str]:
        """Find ISRC link records that reference non-existent videos."""
        query = text(
            """
            SELECT DISTINCT l.video_id
            FROM video_recording_link l
            LEFT JOIN youtube_videos v ON l.video_id = v.video_id
            WHERE v.video_id IS NULL
        """
        )

        result = conn.execute(query)
        return [row[0] for row in result]
