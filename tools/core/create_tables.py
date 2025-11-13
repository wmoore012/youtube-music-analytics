#!/usr/bin/env python3
"""
Create tables script for YouTube ETL pipeline.

This script creates all the required tables for the YouTube ETL pipeline
based on the schema provided in the issue description.
"""

import logging
import os
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pymysql
from dotenv import load_dotenv
from sqlalchemy import text

from web.etl_helpers import get_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def create_youtube_tables() -> bool:
    """Create all required YouTube tables if they don't exist."""
    logger.info("Creating YouTube tables...")

    # Ensure target database exists
    try:
        host = os.getenv("DB_HOST", "127.0.0.1")
        port = int(os.getenv("DB_PORT", "3306"))
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS") or ""
        db_name = os.getenv("DB_NAME", "yt_proj")
        conn = pymysql.connect(host=host, port=port, user=user, password=password)
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        conn.close()
        logger.info(f"Ensured database exists: {db_name}")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Could not ensure database exists (continuing): {e}")
        # Proceed to table creation; database may already exist in CI environment

    # Get database engine
    try:
        engine = get_engine()
        logger.info("Connected to database")
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to connect to database: {e}")
        return False

    table_statements = [
        """
        CREATE TABLE IF NOT EXISTS `youtube_comments` (
          `id` int NOT NULL AUTO_INCREMENT,
          `video_id` varchar(50) DEFAULT NULL,
          `comment_id` varchar(100) DEFAULT NULL,
          `comment_text` text,
          `author_name` varchar(255) DEFAULT NULL,
          `like_count` int DEFAULT '0',
          `published_at` timestamp NULL DEFAULT NULL,
          `sentiment_score` decimal(3,2) DEFAULT NULL,
          `beat_appreciation` tinyint(1) DEFAULT '0',
          `is_bot_suspected` tinyint(1) DEFAULT '0',
          `ged_mention` tinyint(1) DEFAULT '0',
          `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `comment_id` (`comment_id`),
          KEY `idx_video_id` (`video_id`),
          KEY `idx_sentiment` (`sentiment_score`),
          KEY `idx_beat_appreciation` (`beat_appreciation`),
          KEY `idx_ged_mention` (`ged_mention`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_metrics` (
          `video_id` varchar(50) NOT NULL,
          `view_count` bigint DEFAULT NULL,
          `like_count` bigint DEFAULT NULL,
          `dislike_count` bigint DEFAULT NULL,
          `comment_count` bigint DEFAULT NULL,
          `subscriber_count` bigint DEFAULT NULL,
          `metrics_date` date NOT NULL,
          `fetched_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`video_id`,`metrics_date`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_playlists_raw` (
          `playlist_id` varchar(50) NOT NULL,
          `raw_data` json DEFAULT NULL,
          `fetched_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `processed` smallint NOT NULL DEFAULT '0',
          `error` text,
          `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`playlist_id`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_videos` (
          `video_id` varchar(50) NOT NULL,
          `isrc` char(12) DEFAULT NULL,
          `title` varchar(255) DEFAULT NULL,
          `channel_title` varchar(255) DEFAULT NULL,
          `published_at` datetime DEFAULT NULL,
          `duration` varchar(20) DEFAULT NULL,
          `view_count` bigint DEFAULT NULL,
          `like_count` int DEFAULT NULL,
          `comment_count` int DEFAULT NULL,
          `dsp_name` varchar(50) DEFAULT 'YouTube',
          `fetched_at` datetime DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`video_id`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_videos_raw` (
          `video_id` varchar(50) NOT NULL,
          `playlist_id` varchar(100) DEFAULT NULL,
          `raw_data` json DEFAULT NULL,
          `fetched_at` datetime DEFAULT CURRENT_TIMESTAMP,
          `processed` tinyint(1) DEFAULT '0',
          PRIMARY KEY (`video_id`),
          KEY `idx_yvraw_playlist` (`playlist_id`),
          KEY `idx_yvraw_processed` (`processed`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `project_benchmarks` (
          `benchmark_id` varchar(50) NOT NULL,
          `benchmark_date` datetime NOT NULL,
          `total_records` int DEFAULT NULL,
          `unique_videos` int DEFAULT NULL,
          `unique_artists` int DEFAULT NULL,
          `unique_channels` int DEFAULT NULL,
          `date_range_days` int DEFAULT NULL,
          `date_range_years` decimal(4,2) DEFAULT NULL,
          `load_time_seconds` decimal(8,4) DEFAULT NULL,
          `throughput_rows_per_sec` decimal(10,2) DEFAULT NULL,
          `null_percentage` decimal(5,2) DEFAULT NULL,
          `comment_count` bigint DEFAULT NULL,
          `test_coverage` decimal(5,2) DEFAULT NULL,
          `duplicate_functions` int DEFAULT NULL,
          `lines_of_code` int DEFAULT NULL,
          `sentiment_available` enum('available','not_available') NOT NULL DEFAULT 'not_available',
          `sentiment_avg_time` decimal(8,6) DEFAULT NULL,
          `sentiment_p95_time` decimal(8,6) DEFAULT NULL,
          `sentiment_throughput` decimal(10,2) DEFAULT NULL,
          `sentiment_comments_tested` int DEFAULT NULL,
          `bot_detection_available` enum('available','not_available') NOT NULL DEFAULT 'not_available',
          `bot_detection_avg_time` decimal(8,6) DEFAULT NULL,
          `bot_detection_throughput` decimal(10,2) DEFAULT NULL,
          `bot_detection_precision` decimal(5,4) DEFAULT NULL,
          `existing_model_benchmarks` json DEFAULT NULL,
          `notes` text DEFAULT NULL,
          `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`benchmark_id`),
          KEY `idx_benchmark_date` (`benchmark_date`),
          KEY `idx_created_desc` (`created_at` DESC, `benchmark_date` DESC),
          KEY `idx_sentiment_available` (`sentiment_available`),
          KEY `idx_bot_detection_available` (`bot_detection_available`),
          CONSTRAINT `chk_pct_coverage` CHECK (`test_coverage` BETWEEN 0 AND 100),
          CONSTRAINT `chk_pct_nulls` CHECK (`null_percentage` BETWEEN 0 AND 100),
          CONSTRAINT `chk_bot_precision` CHECK (`bot_detection_precision` BETWEEN 0 AND 1),
          CONSTRAINT `chk_throughput_nonneg` CHECK (`throughput_rows_per_sec` >= 0),
          CONSTRAINT `chk_sent_throughput` CHECK (`sentiment_throughput` >= 0),
          CONSTRAINT `chk_bot_throughput` CHECK (`bot_detection_throughput` >= 0),
          CONSTRAINT `chk_years_nonneg` CHECK (`date_range_years` >= 0),
          CONSTRAINT `chk_load_time_nonneg` CHECK (`load_time_seconds` >= 0)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `project_benchmark_models` (
          `benchmark_id` varchar(50) NOT NULL,
          `model_name` varchar(100) NOT NULL,
          `accuracy_pct` decimal(5,2) NOT NULL,
          `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`benchmark_id`, `model_name`),
          KEY `idx_model_accuracy` (`model_name`, `accuracy_pct` DESC),
          CONSTRAINT `chk_model_accuracy` CHECK (`accuracy_pct` BETWEEN 0 AND 100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `operational_health_log` (
          `id` bigint NOT NULL AUTO_INCREMENT,
          `recorded_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `source` varchar(100) NOT NULL,
          `lookback_days` int NOT NULL,
          `data_freshness_hours` decimal(8,2) NOT NULL,
          `coverage_ratio` decimal(5,2) NOT NULL,
          `average_daily_views` decimal(12,2) NOT NULL,
          `engagement_rate` decimal(5,2) NOT NULL,
          `reliability_score` decimal(5,2) NOT NULL,
          `stale_channels` json NOT NULL,
          `notes` json NOT NULL,
          PRIMARY KEY (`id`),
          KEY `idx_recorded_at` (`recorded_at` DESC),
          KEY `idx_source` (`source`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `artist_aliases` (
          `canonical_name` varchar(255) NOT NULL,
          `alias` varchar(255) NOT NULL DEFAULT '',
          `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
          `updated_at` datetime DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`canonical_name`, `alias`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `artist_performance_summary` (
          `artist_name` varchar(255) NOT NULL,
          `total_videos` int DEFAULT '0',
          `total_views` bigint DEFAULT '0',
          `total_comments` int DEFAULT '0',
          `avg_sentiment` decimal(5,3) DEFAULT '0.000',
          `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`artist_name`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `comment_bot_analysis` (
          `comment_id` text,
          `video_id` text,
          `author_name` text,
          `comment_text` text,
          `bot_score` double DEFAULT NULL,
          `bot_risk_level` text,
          `duplicate_count_local` bigint DEFAULT NULL,
          `duplicate_count_global` bigint DEFAULT NULL,
          `burst_score` double DEFAULT NULL,
          `author_repetition_score` double DEFAULT NULL,
          `engagement_score` double DEFAULT NULL,
          `emoji_count` bigint DEFAULT NULL,
          `is_whitelisted` tinyint(1) DEFAULT NULL,
          `analyzed_at` timestamp NULL DEFAULT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `comment_sentiment` (
          `comment_id` varchar(255) NOT NULL,
          `video_id` varchar(255) NOT NULL,
          `comment_text` text,
          `sentiment_score` decimal(5,3) DEFAULT NULL,
          `confidence_score` decimal(5,3) DEFAULT NULL,
          `created_at` datetime DEFAULT NULL,
          `processed_at` datetime DEFAULT CURRENT_TIMESTAMP,
          `confidence` decimal(5,3) DEFAULT '0.000',
          `beat_appreciation` tinyint(1) DEFAULT '0',
          PRIMARY KEY (`comment_id`),
          KEY `idx_video_id` (`video_id`),
          KEY `idx_sentiment_score` (`sentiment_score`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `comment_sentiment_backup` (
          `comment_id` varchar(255) NOT NULL,
          `video_id` varchar(255) NOT NULL,
          `comment_text` text,
          `sentiment_score` decimal(5,3) DEFAULT NULL,
          `confidence_score` decimal(5,3) DEFAULT NULL,
          `created_at` datetime DEFAULT NULL,
          `processed_at` datetime DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `isrc_recordings` (
          `isrc` char(12) NOT NULL,
          `title` varchar(300) NOT NULL,
          `artist_primary` varchar(255) NOT NULL,
          `cc` char(2) GENERATED ALWAYS AS (substr(`isrc`,1,2)) STORED,
          `registrant` char(3) GENERATED ALWAYS AS (substr(`isrc`,3,3)) STORED,
          `year_code` char(2) GENERATED ALWAYS AS (substr(`isrc`,6,2)) STORED,
          `designation` char(5) GENERATED ALWAYS AS (substr(`isrc`,8,5)) STORED,
          `release_date` date DEFAULT NULL,
          `source` varchar(100) DEFAULT NULL,
          `metadata` json DEFAULT NULL,
          `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
          `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`isrc`),
          CONSTRAINT `isrc_format_chk` CHECK (regexp_like(`isrc`,_utf8mb4'^[A-Z]{2}[A-Z0-9]{3}[0-9]{2}[0-9]{5}$'))  # noqa: E501
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `isrc_artists` (
          `isrc` char(12) NOT NULL,
          `artist_name` varchar(255) NOT NULL,
          `role` enum('primary','feature','remixer','producer') NOT NULL DEFAULT 'primary',
          PRIMARY KEY (`isrc`,`artist_name`,`role`),
          CONSTRAINT `fk_isrc_artists_isrc` FOREIGN KEY (`isrc`) REFERENCES `isrc_recordings` (`isrc`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `music_videos_normalized` (
          `video_id` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
          `artist_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          `title` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `isrc` char(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `published_at` datetime DEFAULT NULL,
          `total_views` bigint DEFAULT '0',
          `total_likes` int DEFAULT '0',
          `total_comments` int DEFAULT '0',
          `est_revenue_usd` decimal(10,2) DEFAULT '0.00',
          `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`video_id`),
          KEY `idx_artist` (`artist_name`),
          KEY `idx_isrc` (`isrc`),
          KEY `idx_published` (`published_at`),
          KEY `idx_revenue` (`est_revenue_usd`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `songs` (
          `isrc` char(12) NOT NULL,
          `title` varchar(255) DEFAULT NULL,
          `artist` varchar(255) DEFAULT NULL,
          PRIMARY KEY (`isrc`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `video_recording_link` (
          `video_id` varchar(50) NOT NULL,
          `isrc` char(12) NOT NULL,
          `match_method` enum('explicit_isrc','title_parse','fingerprint','catalog_api','manual') NOT NULL,
          `confidence` decimal(4,3) NOT NULL DEFAULT '0.000',
          `matched_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`video_id`,`isrc`),
          KEY `idx_isrc` (`isrc`),
          KEY `idx_confidence` (`confidence`),
          CONSTRAINT `fk_vrl_isrc` FOREIGN KEY (`isrc`) REFERENCES `isrc_recordings` (`isrc`),
          CONSTRAINT `fk_vrl_video` FOREIGN KEY (`video_id`) REFERENCES `youtube_videos` (`video_id`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_etl_runs` (
          `channel_id` varchar(255) NOT NULL,
          `run_date` date NOT NULL,
          `started_at` datetime DEFAULT NULL,
          `finished_at` datetime DEFAULT NULL,
          `status` varchar(32) DEFAULT NULL,
          `run_type` varchar(10) DEFAULT 'manual',
          `reason` text,
          `error_message` text,
          `videos_processed` int DEFAULT '0',
          `metrics_collected` int DEFAULT '0',
          PRIMARY KEY (`channel_id`,`run_date`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_sentiment` (
          `isrc` char(12) NOT NULL,
          `video_id` varchar(50) NOT NULL,
          `fetch_datetime` datetime NOT NULL,
          `sentiment_score` float DEFAULT NULL,
          `sentiment_magnitude` float DEFAULT NULL,
          `compound_score` float DEFAULT NULL,
          `positive_score` float DEFAULT NULL,
          `neutral_score` float DEFAULT NULL,
          `negative_score` float DEFAULT NULL,
          `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`isrc`,`video_id`,`fetch_datetime`),
          KEY `idx_ysent_video` (`video_id`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_sentiment_by_video` (
          `video_id` varchar(50) NOT NULL,
          `fetch_datetime` datetime NOT NULL,
          `sentiment_score` float DEFAULT NULL,
          `compound_score` float DEFAULT NULL,
          `comment_count` int DEFAULT NULL,
          `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`video_id`,`fetch_datetime`)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `youtube_sentiment_summary` (
          `video_id` varchar(50) NOT NULL,
          `avg_sentiment` float NOT NULL,
          `comment_count` int NOT NULL,
          `last_updated` datetime NOT NULL,
          PRIMARY KEY (`video_id`)
        )
        """,
    ]

    try:
        with engine.begin() as conn:
            for statement in table_statements:
                conn.execute(text(statement))
                table_name = statement.split("`")[1]
                logger.info(f"Created or verified table: {table_name}")
    except Exception as e:  # noqa: BLE001
        logger.error(f"Failed to create tables: {e}")
        return False

    logger.info("✅ All YouTube tables created successfully!")
    return True


if __name__ == "__main__":
    create_youtube_tables()
