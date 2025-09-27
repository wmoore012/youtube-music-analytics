# SPDX - License - Identifier: GPL - 3.0 - or - later
"""
YouTube metrics helper functions for the iCatalog ETL pipeline.

This module provides helper functions for working with YouTube metrics,
including functions for upserting metrics data with daily snapshots.
"""
from datetime import datetime, timezone
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from web.etl_helpers import get_table, read_sql_safe

logger = logging.getLogger(__name__)


def upsert_metrics(
    engine: Engine,
    video_id: str,
    views: int,
    likes: int,
    comments: int,
    subscriber_count: int = 0,
) -> None:
    """
    Insert today's metrics snapshot or update if it already exists.

    This function ensures that only one entry per day is stored for each
    video_id, preventing data redundancy. Uses actual youtube_metrics schema.

    Args:
        engine (Engine): SQLAlchemy engine
        video_id (str): YouTube video ID
        views (int): View count
        likes (int): Like count
        comments (int): Comment count
        subscriber_count (int): Subscriber count (optional, defaults to 0)
    """
    # Get current date for daily granularity
    now = datetime.now(tz=timezone.utc)

    with engine.begin() as conn:
        # Check if we already have an entry for today
        today_entry = conn.execute(
            text(
                """
                SELECT video_id, metrics_date
                FROM youtube_metrics
                WHERE video_id = :video_id
                AND metrics_date = CURDATE()
            """
            ),
            {"video_id": video_id},
        ).fetchone()

        if today_entry:
            # Update existing entry for today with maximum values
            conn.execute(
                text(
                    """
                    UPDATE youtube_metrics
                    SET view_count = GREATEST(view_count, :views),
                        like_count = GREATEST(like_count, :likes),
                        comment_count = GREATEST(comment_count, :comments),
                        subscriber_count = GREATEST(COALESCE(subscriber_count, 0), :subscriber_count),
                        fetched_at = :now
                    WHERE video_id = :video_id
                    AND metrics_date = CURDATE()
                """
                ),
                {
                    "video_id": video_id,
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "subscriber_count": subscriber_count,
                    "now": now,
                },
            )
            logger.debug(f"Updated today's metrics for video {video_id}")
        else:
            # Insert new entry for today
            conn.execute(
                text(
                    """
                    INSERT INTO youtube_metrics
                    (video_id, view_count, like_count, dislike_count, comment_count,
                     subscriber_count, metrics_date, fetched_at)
                    VALUES (:video_id, :views, :likes, 0, :comments, :subscriber_count, CURDATE(), :now)
                """
                ),
                {
                    "video_id": video_id,
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "subscriber_count": subscriber_count,
                    "now": now,
                },
            )
            logger.debug(f"Inserted new metrics for video {video_id}")


def get_playlist_count(conn: Connection, video_id: str) -> int:
    """
    Get the number of playlists a video appears in.

    This is a placeholder function that would normally query the YouTube API
    to get the number of playlists a video appears in. Since this requires
    broader API access, we're returning 0 for now.

    Args:
        conn (Connection): SQLAlchemy connection
        video_id (str): YouTube video ID

    Returns:
        int: Number of playlists the video appears in
    """
    # This would normally query the YouTube API to get the number of playlists
    # a video appears in. Since this requires broader API access, we're
    # returning 0 for now.
    return 0


def get_latest_metrics(engine: Engine, video_id: str) -> Optional[dict]:
    """
    Get the latest metrics for a video.

    Args:
        engine (Engine): SQLAlchemy engine
        video_id (str): YouTube video ID

    Returns:
        Optional[dict]: Latest metrics for the video, or None if not found
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT
                    video_id, metrics_date, fetched_at,
                    view_count, like_count, dislike_count, comment_count, subscriber_count
                FROM youtube_metrics
                WHERE video_id = :video_id
                ORDER BY metrics_date DESC, fetched_at DESC
                LIMIT 1
            """
            ),
            {"video_id": video_id},
        ).fetchone()

        if result:
            return {
                "video_id": result.video_id,
                "metrics_date": result.metrics_date,
                "fetched_at": result.fetched_at,
                "view_count": result.view_count,
                "like_count": result.like_count,
                "dislike_count": result.dislike_count,
                "comment_count": result.comment_count,
                "subscriber_count": result.subscriber_count,
            }
        return None


def get_top_viewcount_increases(engine: Engine, limit: int = 10) -> pd.DataFrame:
    """
    Get the top videos with the biggest YouTube view count increases.

    This function finds videos with the greatest growth in view counts from
    the earliest to the latest record for each video_id.

    Args:
        engine (Engine): SQLAlchemy engine
        limit (int): Number of videos to return

    Returns:
        pd.DataFrame: DataFrame with the top videos
    """
    logger.info(f"Getting top {limit} videos with biggest YouTube view count increases")

    # Complex SQL query with CTEs to calculate view count growth
    # CTE 1: Get first recorded view count for each video
    # CTE 2: Get latest recorded view count for each video
    # CTE 3: Calculate increases and join with video metadata
    query = """
        WITH first_counts AS (
            -- Get the earliest view count record for each video
            SELECT
                t.video_id,
                t.metrics_date AS first_date,
                t.view_count AS first_count
            FROM (
                SELECT
                    ym.*,
                    ROW_NUMBER() OVER (PARTITION BY ym.video_id
                                       ORDER BY ym.metrics_date ASC, ym.fetched_at ASC) AS rn
                FROM youtube_metrics ym
                WHERE ym.view_count > 0
            ) AS t
            WHERE t.rn = 1
        ),
        last_counts AS (
            -- Get the latest view count record for each video
            SELECT
                t.video_id,
                t.metrics_date AS last_date,
                t.view_count AS last_count
            FROM (
                SELECT
                    ym.*,
                    ROW_NUMBER() OVER (PARTITION BY ym.video_id
                                       ORDER BY ym.metrics_date DESC, ym.fetched_at DESC) AS rn
                FROM youtube_metrics ym
            ) AS t
            WHERE t.rn = 1
        ),
        increases AS (
            -- Calculate view count increases and join with video metadata
            SELECT
                fc.video_id,
                yv.title AS video_title,
                yv.channel_title,
                vrl.isrc,
                ir.title AS recording_title,
                ir.artist_primary,
                fc.first_date,
                lc.last_date,
                fc.first_count,
                lc.last_count,
                (lc.last_count - fc.first_count) AS increase,
                ROUND((lc.last_count - fc.first_count) /
                      NULLIF(fc.first_count, 0) * 100, 2) AS percent_increase
            FROM first_counts fc
            JOIN last_counts lc ON fc.video_id = lc.video_id
            JOIN youtube_videos yv ON yv.video_id = fc.video_id
            LEFT JOIN video_recording_link vrl ON vrl.video_id = fc.video_id
            LEFT JOIN isrc_recordings ir ON ir.isrc = vrl.isrc
            WHERE
                fc.first_count > 0 AND
                lc.last_count > fc.first_count AND
                fc.first_date < lc.last_date
            ORDER BY increase DESC
            LIMIT %s
        )
        SELECT * FROM increases
    """

    # Execute query and return results
    df = read_sql_safe(query, engine, params=[limit])

    if not df.empty:
        logger.info(f"Found {len(df)} videos with view count increases")
    else:
        logger.warning("No videos found with view count increases")

    return df


def analyze_viewcount_changes(engine: Engine) -> pd.DataFrame:
    """
    Analyze YouTube view count changes over time and return a DataFrame with the results.

    Args:
        engine (Engine): SQLAlchemy engine

    Returns:
        pd.DataFrame: DataFrame with view count changes
    """
    logger.info("Analyzing YouTube view count changes over time")

    # SQL query to get view count changes over time
    query = """
        WITH view_counts AS (
            SELECT
                ym.video_id,
                yv.title AS video_title,
                yv.channel_title,
                vrl.isrc,
                ir.title AS recording_title,
                ir.artist_primary,
                ym.metrics_date,
                ym.fetched_at,
                ym.view_count,
                LAG(ym.view_count) OVER (PARTITION BY ym.video_id ORDER BY ym.metrics_date, ym.fetched_at) AS prev_count
            FROM youtube_metrics ym
            JOIN youtube_videos yv ON ym.video_id = yv.video_id
            LEFT JOIN video_recording_link vrl ON vrl.video_id = ym.video_id
            LEFT JOIN isrc_recordings ir ON ir.isrc = vrl.isrc
            ORDER BY ym.video_id, ym.metrics_date, ym.fetched_at
        )
        SELECT
            video_id,
            video_title,
            channel_title,
            isrc,
            recording_title,
            artist_primary,
            metrics_date,
            fetched_at,
            view_count,
            prev_count,
            (view_count - COALESCE(prev_count, 0)) AS view_count_change,
            CASE
                WHEN prev_count > 0 THEN ROUND((view_count - prev_count) / prev_count * 100, 2)
                ELSE 0
            END AS percent_change
        FROM view_counts
        WHERE prev_count IS NOT NULL
        ORDER BY metrics_date DESC, fetched_at DESC, view_count_change DESC
        LIMIT 300
    """

    # Use read_sql_safe to execute the query and return a DataFrame
    df = read_sql_safe(query, engine)

    if not df.empty:
        logger.info(f"Found {len(df)} view count changes")
    else:
        logger.warning("No view count changes found")

    return df
