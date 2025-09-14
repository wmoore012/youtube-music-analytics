# SPDX-License-Identifier: GPL-3.0-or-later
"""
YouTube metrics helper functions for the iCatalog ETL pipeline.

This module provides helper functions for working with YouTube metrics,
including functions for upserting metrics data with daily snapshots.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

from web.etl_helpers import get_table, read_sql_safe

logger = logging.getLogger(__name__)


def upsert_metrics(
    engine: Engine,
    isrc: str,
    video_id: str,
    views: int,
    likes: int,
    faves: int,
    comments: int,
) -> None:
    """
    Insert today's snapshot or update it if it already exists.

    This function ensures that only one entry per day is stored for each
    video_id and isrc combination, preventing data redundancy.

    Args:
        engine (Engine): SQLAlchemy engine
        isrc (str): International Standard Recording Code
        video_id (str): YouTube video ID
        views (int): View count
        likes (int): Like count
        faves (int): Favorite count or playlist count
        comments (int): Comment count
    """
    # Get current date with time set to midnight (00:00:00) for daily granularity
    now = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Get the youtube_metrics table
    metrics = get_table("youtube_metrics")

    with engine.begin() as conn:
        # Check if we already have an entry for today
        today_entry = conn.execute(
            text(
                """
                SELECT isrc, video_id, fetch_datetime
                FROM youtube_metrics
                WHERE isrc = :isrc
                AND video_id = :video_id
                AND DATE(fetch_datetime) = DATE(:now)
            """
            ),
            {"isrc": isrc, "video_id": video_id, "now": now},
        ).fetchone()

        if today_entry:
            # Update existing entry for today
            conn.execute(
                text(
                    """
                    UPDATE youtube_metrics
                    SET view_count = :views,
                        like_count = :likes,
                        favorite_count = :faves,
                        comment_count = :comments,
                        fetch_datetime = :now
                    WHERE isrc = :isrc
                    AND video_id = :video_id
                    AND DATE(fetch_datetime) = DATE(:now)
                """
                ),
                {
                    "isrc": isrc,
                    "video_id": video_id,
                    "views": views,
                    "likes": likes,
                    "faves": faves,
                    "comments": comments,
                    "now": now,
                },
            )
            logger.debug(f"Updated today's metrics for video {video_id} (ISRC: {isrc})")
        else:
            # Insert new entry for today
            # Create the insert statement
            stmt = mysql_insert(metrics).values(
                isrc=isrc,
                video_id=video_id,
                fetch_datetime=now,
                view_count=views,
                like_count=likes,
                favorite_count=faves,
                comment_count=comments,
            )

            # Execute the statement
            conn.execute(stmt)
            logger.debug(f"Inserted new metrics for video {video_id} (ISRC: {isrc})")


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
                    isrc, video_id, fetch_datetime,
                    view_count, like_count, favorite_count, comment_count
                FROM youtube_metrics
                WHERE video_id = :video_id
                ORDER BY fetch_datetime DESC
                LIMIT 1
            """
            ),
            {"video_id": video_id},
        ).fetchone()

        if result:
            return {
                "isrc": result.isrc,
                "video_id": result.video_id,
                "fetch_datetime": result.fetch_datetime,
                "view_count": result.view_count,
                "like_count": result.like_count,
                "favorite_count": result.favorite_count,
                "comment_count": result.comment_count,
            }
        return None


def get_top_viewcount_increases(engine: Engine, limit: int = 10) -> pd.DataFrame:
    """
    Get the top songs with the biggest YouTube view count increases.

    This function finds songs with the greatest growth in view counts from
    the earliest to the latest record for each unique ISRC and video_id.

    Args:
        engine (Engine): SQLAlchemy engine
        limit (int): Number of songs to return

    Returns:
        pd.DataFrame: DataFrame with the top songs
    """
    logger.info(f"Getting top {limit} songs with biggest YouTube view count increases")

    # SQL query to find songs with the greatest growth in view counts
    query = """
        WITH first_counts AS (
            SELECT
                t.isrc,
                t.video_id,
                t.fetch_datetime AS first_date,
                t.view_count AS first_count
            FROM (
                SELECT
                    ym.*,
                    ROW_NUMBER() OVER (PARTITION BY ym.isrc, ym.video_id
                                       ORDER BY ym.fetch_datetime ASC) AS rn
                FROM youtube_metrics ym
                WHERE ym.view_count > 0
            ) AS t
            WHERE t.rn = 1
        ),
        last_counts AS (
            SELECT
                t.isrc,
                t.video_id,
                t.fetch_datetime AS last_date,
                t.view_count AS last_count
            FROM (
                SELECT
                    ym.*,
                    ROW_NUMBER() OVER (PARTITION BY ym.isrc, ym.video_id
                                       ORDER BY ym.fetch_datetime DESC) AS rn
                FROM youtube_metrics ym
            ) AS t
            WHERE t.rn = 1
        ),
        increases AS (
            SELECT
                fc.isrc,
                fc.video_id,
                s.song_title,
                yv.video_title,
                GROUP_CONCAT(DISTINCT a.artist_name ORDER BY a.artist_name SEPARATOR ', ') AS artists,
                fc.first_date,
                lc.last_date,
                fc.first_count,
                lc.last_count,
                (lc.last_count - fc.first_count) AS increase,
                ROUND((lc.last_count - fc.first_count) /
                      NULLIF(fc.first_count, 0) * 100, 2) AS percent_increase
            FROM first_counts fc
            JOIN last_counts lc ON fc.isrc = lc.isrc AND fc.video_id = lc.video_id
            JOIN songs s ON s.ISRC = fc.isrc
            JOIN youtube_videos yv ON yv.video_id = fc.video_id
            LEFT JOIN song_artist_roles sar ON s.ISRC = sar.ISRC
            LEFT JOIN artists a ON sar.ArtistID = a.ArtistID
            WHERE
                fc.first_count > 0 AND
                lc.last_count > fc.first_count AND
                fc.first_date < lc.last_date
            GROUP BY fc.isrc, fc.video_id, s.song_title, yv.video_title, fc.first_date, fc.first_count, lc.last_date, lc.last_count
            ORDER BY increase DESC
            LIMIT %s
        )
        SELECT * FROM increases
    """

    # Use read_sql_safe to execute the query and return a DataFrame
    df = read_sql_safe(query, engine, params=[limit])

    if not df.empty:
        logger.info(f"Found {len(df)} songs with view count increases")
    else:
        logger.warning("No songs found with view count increases")

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
                ym.isrc,
                ym.video_id,
                s.song_title,
                yv.video_title,
                ym.fetch_datetime,
                ym.view_count,
                LAG(ym.view_count) OVER (PARTITION BY ym.isrc, ym.video_id ORDER BY ym.fetch_datetime) AS prev_count
            FROM youtube_metrics ym
            JOIN songs s ON ym.isrc = s.ISRC
            JOIN youtube_videos yv ON ym.video_id = yv.video_id
            ORDER BY ym.isrc, ym.video_id, ym.fetch_datetime
        )
        SELECT
            isrc,
            video_id,
            song_title,
            video_title,
            fetch_datetime,
            view_count,
            prev_count,
            (view_count - COALESCE(prev_count, 0)) AS view_count_change,
            CASE
                WHEN prev_count > 0 THEN ROUND((view_count - prev_count) / prev_count * 100, 2)
                ELSE 0
            END AS percent_change
        FROM view_counts
        WHERE prev_count IS NOT NULL
        ORDER BY fetch_datetime DESC, view_count_change DESC
        LIMIT 300
    """

    # Use read_sql_safe to execute the query and return a DataFrame
    df = read_sql_safe(query, engine)

    if not df.empty:
        logger.info(f"Found {len(df)} view count changes")
    else:
        logger.warning("No view count changes found")

    return df
