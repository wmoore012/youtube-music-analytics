"""
Dynamic Data Discovery System for MusicScope™

Discovers actual database structure, artists, and data without hardcoding.
Provides dynamic configuration for notebooks and charts.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseDiscovery:
    """Discovers database structure and content dynamically."""

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize with database connection."""
        if connection_string is None:
            # Try to build from environment variables
            connection_string = self._build_connection_from_env()

        self.engine = create_engine(connection_string)
        self.inspector = inspect(self.engine)

    def _build_connection_from_env(self) -> str:
        """Build connection string from environment variables."""
        # Check if DATABASE_URL is provided first
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return database_url

        # Otherwise build from individual components
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "3306")
        user = os.getenv("DB_USER", "root")
        # Try both DB_PASSWORD and DB_PASS for compatibility
        password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS", "")
        database = os.getenv("DB_NAME", "yt_proj")

        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    def discover_tables(self) -> Dict[str, Any]:
        """Discover all tables and their structure."""
        tables = self.inspector.get_table_names()

        table_info = {}
        for table in tables:
            columns = self.inspector.get_columns(table)
            table_info[table] = {
                "columns": [col["name"] for col in columns],
                "column_types": {col["name"]: str(col["type"]) for col in columns},
            }

        return {
            "total_tables": len(tables),
            "tables": table_info,
            "youtube_tables": [t for t in tables if "youtube" in t.lower()],
            "sentiment_tables": [t for t in tables if "sentiment" in t.lower()],
            "isrc_tables": [t for t in tables if "isrc" in t.lower()],
        }

    def discover_artists(self, min_videos: int = 5) -> List[str]:
        """Discover actual artists from the database with sufficient data."""

        # Try multiple sources for artist data
        artist_queries = [
            # From youtube_videos table
            """
            SELECT channel_title as artist_name, COUNT(*) as video_count
            FROM youtube_videos
            WHERE channel_title IS NOT NULL
            GROUP BY channel_title
            HAVING COUNT(*) >= :min_videos
            ORDER BY COUNT(*) DESC
            """,
            # From music_videos_normalized table
            """
            SELECT artist_name, COUNT(*) as video_count
            FROM music_videos_normalized
            WHERE artist_name IS NOT NULL
            GROUP BY artist_name
            HAVING COUNT(*) >= :min_videos
            ORDER BY COUNT(*) DESC
            """,
            # From artist_performance_summary
            """
            SELECT artist_name, total_videos as video_count
            FROM artist_performance_summary
            WHERE total_videos >= :min_videos
            ORDER BY total_videos DESC
            """,
        ]

        artists = []
        for query in artist_queries:
            try:
                df = pd.read_sql(text(query), self.engine, params={"min_videos": min_videos})
                if not df.empty:
                    artists.extend(df["artist_name"].tolist())
                    logger.info(f"Found {len(df)} artists from query")
                    break  # Use first successful query
            except Exception as e:
                logger.warning(f"Query failed: {e}")
                continue

        # Remove duplicates and return top artists
        unique_artists = list(dict.fromkeys(artists))  # Preserves order
        return unique_artists[:10]  # Return top 10 artists

    def get_data_summary(self) -> Dict[str, Any]:
        """Get comprehensive data summary."""

        summary = {
            "discovery_time": datetime.now().isoformat(),
            "total_videos": 0,
            "total_comments": 0,
            "date_range": None,
            "artists": [],
            "has_sentiment": False,
            "has_isrc": False,
        }

        try:
            # Count videos
            video_count_query = """
            SELECT COUNT(*) as count FROM youtube_videos
            """
            result = pd.read_sql(text(video_count_query), self.engine)
            summary["total_videos"] = int(result["count"].iloc[0])

            # Count comments
            comment_count_query = """
            SELECT COUNT(*) as count FROM youtube_comments
            """
            result = pd.read_sql(text(comment_count_query), self.engine)
            summary["total_comments"] = int(result["count"].iloc[0])

            # Get date range
            date_range_query = """
            SELECT
                MIN(published_at) as min_date,
                MAX(published_at) as max_date
            FROM youtube_videos
            WHERE published_at IS NOT NULL
            """
            result = pd.read_sql(text(date_range_query), self.engine)
            if not result.empty and result["min_date"].iloc[0] is not None:
                summary["date_range"] = {
                    "start": result["min_date"].iloc[0].isoformat(),
                    "end": result["max_date"].iloc[0].isoformat(),
                }

            # Check for sentiment data
            sentiment_check_query = """
            SELECT COUNT(*) as count FROM comment_sentiment LIMIT 1
            """
            try:
                result = pd.read_sql(text(sentiment_check_query), self.engine)
                summary["has_sentiment"] = int(result["count"].iloc[0]) > 0
            except:
                summary["has_sentiment"] = False

            # Check for ISRC data
            isrc_check_query = """
            SELECT COUNT(*) as count FROM isrc_recordings LIMIT 1
            """
            try:
                result = pd.read_sql(text(isrc_check_query), self.engine)
                summary["has_isrc"] = int(result["count"].iloc[0]) > 0
            except:
                summary["has_isrc"] = False

            # Get artists
            summary["artists"] = self.discover_artists()

        except Exception as e:
            logger.error(f"Error getting data summary: {e}")

        return summary


def load_dynamic_data(engine, artists: List[str], limit_per_artist: int = 1000) -> Dict[str, pd.DataFrame]:
    """Load actual data for the discovered artists."""

    data = {}

    try:
        # Load videos data with ALL required columns for charts
        if artists:
            artist_list = "', '".join(artists)
            videos_query = f"""
            SELECT
                video_id,
                title,
                channel_title as artist_name,
                published_at,
                view_count,
                like_count,
                comment_count,
                duration,
                -- Calculate missing columns that charts need
                COALESCE(like_count, 0) as likes,
                COALESCE(comment_count, 0) as comments,
                COALESCE(view_count, 1) as views,
                CASE
                    WHEN view_count > 0 THEN (COALESCE(like_count, 0) + COALESCE(comment_count, 0)) / view_count
                    ELSE 0
                END as engagement_rate,
                CASE
                    WHEN published_at IS NOT NULL THEN view_count / GREATEST(DATEDIFF(NOW(), published_at), 1)
                    ELSE view_count
                END as daily_views,
                -- Add content type classification
                CASE
                    WHEN title LIKE '%live%' OR title LIKE '%Live%' THEN 'Live Performance'
                    WHEN title LIKE '%music video%' OR title LIKE '%Music Video%' THEN 'Music Video'
                    WHEN title LIKE '%behind%' OR title LIKE '%Behind%' THEN 'Behind the Scenes'
                    WHEN title LIKE '%interview%' OR title LIKE '%Interview%' THEN 'Interview'
                    ELSE 'Other'
                END as content_type,
                -- Add genre classification (simplified)
                CASE
                    WHEN channel_title LIKE '%hip%' OR channel_title LIKE '%rap%' THEN 'Hip-Hop'
                    WHEN channel_title LIKE '%pop%' OR channel_title LIKE '%Pop%' THEN 'Pop'
                    WHEN channel_title LIKE '%rock%' OR channel_title LIKE '%Rock%' THEN 'Rock'
                    ELSE 'Alternative'
                END as genre
            FROM youtube_videos
            WHERE channel_title IN ('{artist_list}')
            ORDER BY published_at DESC
            LIMIT {limit_per_artist * len(artists)}
            """

            data["videos"] = pd.read_sql(text(videos_query), engine)
            logger.info(f"Loaded {len(data['videos'])} videos")

        # Load comments data
        if "videos" in data and not data["videos"].empty:
            video_ids = data["videos"]["video_id"].tolist()[:100]  # Limit for performance
            video_id_list = "', '".join(video_ids)

            comments_query = f"""
            SELECT
                c.comment_id,
                c.video_id,
                c.comment_text,
                c.author_name,
                c.like_count,
                c.published_at,
                c.sentiment_score,
                v.channel_title as artist_name,
                CASE
                    WHEN c.sentiment_score > 0.1 THEN 'positive'
                    WHEN c.sentiment_score < -0.1 THEN 'negative'
                    ELSE 'neutral'
                END as sentiment_category
            FROM youtube_comments c
            JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE c.video_id IN ('{video_id_list}')
            AND c.comment_text IS NOT NULL
            ORDER BY c.published_at DESC
            LIMIT 5000
            """

            data["comments"] = pd.read_sql(text(comments_query), engine)
            logger.info(f"Loaded {len(data['comments'])} comments")

        # Load sentiment summary if available
        try:
            sentiment_query = """
            SELECT
                video_id,
                avg_sentiment,
                comment_count
            FROM youtube_sentiment_summary
            LIMIT 1000
            """
            data["sentiment_summary"] = pd.read_sql(text(sentiment_query), engine)
            logger.info(f"Loaded {len(data['sentiment_summary'])} sentiment summaries")
        except:
            logger.info("No sentiment summary data available")

        # Load metrics data
        try:
            if "videos" in data and not data["videos"].empty:
                video_ids = data["videos"]["video_id"].tolist()[:50]
                video_id_list = "', '".join(video_ids)

                metrics_query = f"""
                SELECT
                    video_id,
                    view_count,
                    like_count,
                    comment_count,
                    metrics_date
                FROM youtube_metrics
                WHERE video_id IN ('{video_id_list}')
                ORDER BY metrics_date DESC
                LIMIT 1000
                """

                data["metrics"] = pd.read_sql(text(metrics_query), engine)
                logger.info(f"Loaded {len(data['metrics'])} metrics records")
        except:
            logger.info("No metrics data available")

    except Exception as e:
        logger.error(f"Error loading dynamic data: {e}")

    return data


def create_chart_config(artists: List[str], data_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Create dynamic chart configuration based on discovered data."""

    config = {
        "artists": artists,
        "chart_count": 20,
        "data_summary": data_summary,
        "chart_types": [
            "artist_performance_overview",
            "sentiment_distribution",
            "engagement_trends",
            "content_type_analysis",
            "viral_potential_scatter",
            "artist_comparison_bars",
            "comment_sentiment_heatmap",
            "view_count_distribution",
            "upload_frequency_timeline",
            "top_performing_videos",
            "engagement_rate_comparison",
            "sentiment_over_time",
            "artist_growth_trends",
            "content_length_analysis",
            "comment_volume_trends",
            "like_to_view_ratios",
            "artist_collaboration_network",
            "seasonal_performance_patterns",
            "audience_engagement_quality",
            "performance_anomaly_detection",
        ],
    }

    # Adjust chart types based on available data
    if not data_summary.get("has_sentiment", False):
        # Remove sentiment-dependent charts
        config["chart_types"] = [ct for ct in config["chart_types"] if "sentiment" not in ct.lower()]

    if not data_summary.get("has_isrc", False):
        # Remove ISRC-dependent charts
        config["chart_types"] = [ct for ct in config["chart_types"] if "isrc" not in ct.lower()]

    return config


def get_dynamic_notebook_config() -> Dict[str, Any]:
    """Get complete dynamic configuration for notebook generation.

    FAILS LOUDLY if database connection issues - NO FAKE DATA EVER.
    """

    # Initialize discovery - FAIL LOUDLY if this doesn't work
    discovery = DatabaseDiscovery()

    # Discover database structure - FAIL LOUDLY if no tables
    db_summary = discovery.discover_tables()
    if db_summary["total_tables"] == 0:
        raise RuntimeError(
            "🚨 CRITICAL: No database tables found! "
            "Check your database connection and ensure yt_proj database exists with data."
        )

    # Discover artists - FAIL LOUDLY if no artists
    artists = discovery.discover_artists(min_videos=3)
    if len(artists) == 0:
        raise RuntimeError(
            "🚨 CRITICAL: No artists found in database! "
            "Ensure youtube_videos or music_videos_normalized tables have data with artist names."
        )

    # Get data summary - FAIL LOUDLY if no data
    data_summary = discovery.get_data_summary()
    if data_summary["total_videos"] == 0:
        raise RuntimeError("🚨 CRITICAL: No video data found! " "Ensure youtube_videos table has data.")

    # Create chart configuration
    chart_config = create_chart_config(artists, data_summary)

    config = {
        "database": db_summary,
        "artists": artists,
        "data_summary": data_summary,
        "charts": chart_config,
        "notebook_title": f"MusicScope™ Real Data Dashboard - {len(artists)} Artists",
        "generation_time": datetime.now().isoformat(),
    }

    logger.info(f"✅ REAL DATA CONFIG: {len(artists)} artists, {db_summary['total_tables']} tables")
    logger.info(f"📊 DATA VOLUME: {data_summary['total_videos']:,} videos, {data_summary['total_comments']:,} comments")

    return config
