#!/usr/bin/env python3
"""
Pytest Configuration and Fixtures for ETL Testing

This module provides comprehensive testing infrastructure including:
- Isolated test database setup and cleanup
- Test data factories using Pydantic models
- Common fixtures for ETL components
- Database transaction management for tests
"""

import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Generator, List, Optional
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from web.error_handling import ErrorHandler, setup_error_logging
from web.models import ETLConfig, YouTubeComment, YouTubeVideo


class TestDatabaseManager:
    """Manages isolated test database for testing."""

    def __init__(self):
        self.engine: Optional[Engine] = None
        self.test_db_name = f"test_yt_proj_{int(datetime.now().timestamp())}"

    def create_test_database(self) -> Engine:
        """Create isolated test database."""
        # Use SQLite for testing to avoid MySQL dependency
        db_path = tempfile.mktemp(suffix=".db")
        connection_string = f"sqlite:///{db_path}"

        self.engine = create_engine(connection_string, echo=False)

        # Create basic tables for testing
        self._create_test_tables()

        return self.engine

    def _create_test_tables(self):
        """Create minimal test tables."""
        with self.engine.connect() as conn:
            # YouTube videos table
            conn.execute(
                text(
                    """
                CREATE TABLE youtube_videos (
                    video_id VARCHAR(50) PRIMARY KEY,
                    title VARCHAR(500),
                    channel_id VARCHAR(50),
                    channel_title VARCHAR(255),
                    published_at DATETIME,
                    duration VARCHAR(20),
                    view_count INTEGER DEFAULT 0,
                    like_count INTEGER DEFAULT 0,
                    comment_count INTEGER DEFAULT 0,
                    isrc VARCHAR(12),
                    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
            )

            # YouTube comments table
            conn.execute(
                text(
                    """
                CREATE TABLE youtube_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id VARCHAR(50),
                    comment_id VARCHAR(100) UNIQUE,
                    comment_text TEXT,
                    author_name VARCHAR(255),
                    like_count INTEGER DEFAULT 0,
                    published_at DATETIME,
                    sentiment_score DECIMAL(3,2),
                    beat_appreciation BOOLEAN DEFAULT 0,
                    is_bot_suspected BOOLEAN DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
            )

            # Comment sentiment table
            conn.execute(
                text(
                    """
                CREATE TABLE comment_sentiment (
                    comment_id VARCHAR(255) PRIMARY KEY,
                    video_id VARCHAR(255),
                    comment_text TEXT,
                    sentiment_score DECIMAL(5,3),
                    confidence_score DECIMAL(5,3),
                    created_at DATETIME,
                    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    confidence DECIMAL(5,3) DEFAULT 0.000,
                    beat_appreciation BOOLEAN DEFAULT 0
                )
            """
                )
            )

            # YouTube metrics table
            conn.execute(
                text(
                    """
                CREATE TABLE youtube_metrics (
                    video_id VARCHAR(50),
                    view_count BIGINT,
                    like_count BIGINT,
                    comment_count BIGINT,
                    metrics_date DATE,
                    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (video_id, metrics_date)
                )
            """
                )
            )

            # Music videos normalized table
            conn.execute(
                text(
                    """
                CREATE TABLE music_videos_normalized (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id VARCHAR(50) UNIQUE,
                    artist_name VARCHAR(255),
                    title VARCHAR(500),
                    isrc VARCHAR(12),
                    published_at DATETIME,
                    total_views BIGINT DEFAULT 0,
                    total_likes INTEGER DEFAULT 0,
                    total_comments INTEGER DEFAULT 0,
                    est_revenue_usd DECIMAL(10,2) DEFAULT 0.00,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
                )
            )

            conn.commit()

    def cleanup_test_database(self):
        """Clean up test database."""
        if self.engine:
            self.engine.dispose()
            # SQLite file will be cleaned up automatically since we use temp files


class TestDataFactory:
    """Factory for creating test data using Pydantic models."""

    @staticmethod
    def create_youtube_video(
        video_id: str = "dQw4w9WgXcQ",
        title: str = "Test Video",
        channel_id: str = "UCuAXFkgsw1L7xaCfnd5JJOw",
        channel_title: str = "Test Channel",
        published_at: Optional[datetime] = None,
        duration: str = "PT3M33S",
        view_count: int = 1000,
        like_count: int = 100,
        comment_count: int = 50,
        isrc: Optional[str] = None,
        **kwargs,
    ) -> YouTubeVideo:
        """Create a test YouTube video with valid data."""
        if published_at is None:
            published_at = datetime.now() - timedelta(days=30)

        return YouTubeVideo(
            video_id=video_id,
            title=title,
            channel_id=channel_id,
            channel_title=channel_title,
            published_at=published_at,
            duration=duration,
            view_count=view_count,
            like_count=like_count,
            comment_count=comment_count,
            isrc=isrc,
            **kwargs,
        )

    @staticmethod
    def create_youtube_comment(
        comment_id: str = "test_comment_123",
        video_id: str = "dQw4w9WgXcQ",
        author_name: str = "Test User",
        comment_text: str = "This is a test comment",
        like_count: int = 5,
        published_at: Optional[datetime] = None,
        parent_id: Optional[str] = None,
        **kwargs,
    ) -> YouTubeComment:
        """Create a test YouTube comment with valid data."""
        if published_at is None:
            published_at = datetime.now() - timedelta(hours=1)

        return YouTubeComment(
            comment_id=comment_id,
            video_id=video_id,
            author_name=author_name,
            comment_text=comment_text,
            like_count=like_count,
            published_at=published_at,
            parent_id=parent_id,
            **kwargs,
        )

    @staticmethod
    def create_test_videos_batch(count: int = 5) -> List[YouTubeVideo]:
        """Create a batch of test videos with different characteristics."""
        videos = []

        # Valid video IDs for testing
        video_ids = [
            "dQw4w9WgXcQ",
            "oHg5SJYRHA0",
            "kJQP7kiw5Fk",
            "jNQXAC9IVRw",
            "ScMzIvxBSi4",
            "9bZkp7q19f0",
            "fC7oUOUEEi4",
            "YQHsXMglC9A",
            "3tmd-ClpJxA",
            "dQw4w9WgXcR",
        ]

        channel_ids = ["UCuAXFkgsw1L7xaCfnd5JJOw", "UC-9-kyTW8ZkZNDHQJ6FgpwQ", "UCsT0YIqwnpJCM-mx7-gSA4Q"]

        for i in range(min(count, len(video_ids))):
            video = TestDataFactory.create_youtube_video(
                video_id=video_ids[i],
                title=f"Test Video {i+1}",
                channel_id=channel_ids[i % len(channel_ids)],
                channel_title=f"Test Channel {i+1}",
                published_at=datetime.now() - timedelta(days=i + 1),
                view_count=1000 * (i + 1),
                like_count=100 * (i + 1),
                comment_count=10 * (i + 1),
            )
            videos.append(video)

        return videos

    @staticmethod
    def create_test_comments_batch(video_id: str, count: int = 10) -> List[YouTubeComment]:
        """Create a batch of test comments for a video."""
        comments = []

        comment_texts = [
            "Great video!",
            "Love this song 🔥",
            "Amazing performance",
            "This is fire 🌊",
            "So good!",
            "Perfect music",
            "Incredible talent",
            "Best song ever",
            "This slaps hard",
            "Absolutely beautiful",
        ]

        for i in range(count):
            comment = TestDataFactory.create_youtube_comment(
                comment_id=f"test_comment_{video_id}_{i+1}",
                video_id=video_id,
                author_name=f"TestUser{i+1}",
                comment_text=comment_texts[i % len(comment_texts)],
                like_count=i + 1,
                published_at=datetime.now() - timedelta(minutes=i * 10),
            )
            comments.append(comment)

        return comments

    @staticmethod
    def create_etl_config(
        database_url: str = "sqlite:///test.db", youtube_api_key: str = "test_api_key_123456789", **kwargs
    ) -> ETLConfig:
        """Create test ETL configuration."""
        config_data = {
            "database_url": database_url,
            "youtube_api_key": youtube_api_key,
            "comments_per_video": 50,
            "batch_size": 100,
            "max_retries": 2,
            "timeout_seconds": 60,
            "enable_bot_detection": True,
            "enable_sentiment_analysis": True,
            "enable_data_quality_checks": True,
            "quality_threshold": 75.0,
            "sentiment_confidence_threshold": 0.6,
            "bot_detection_threshold": 0.8,
            "log_level": "DEBUG",
        }

        config_data.update(kwargs)
        return ETLConfig(**config_data)


# Pytest Fixtures


@pytest.fixture(scope="session")
def test_db_manager():
    """Session-scoped test database manager."""
    manager = TestDatabaseManager()
    yield manager
    manager.cleanup_test_database()


@pytest.fixture
def test_engine(test_db_manager):
    """Test database engine with clean state for each test."""
    engine = test_db_manager.create_test_database()
    yield engine

    # Clean up data after each test
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM comment_sentiment"))
        conn.execute(text("DELETE FROM youtube_comments"))
        conn.execute(text("DELETE FROM youtube_metrics"))
        conn.execute(text("DELETE FROM music_videos_normalized"))
        conn.execute(text("DELETE FROM youtube_videos"))
        conn.commit()


@pytest.fixture
def test_data_factory():
    """Test data factory for creating test objects."""
    return TestDataFactory()


@pytest.fixture
def sample_videos(test_data_factory):
    """Sample YouTube videos for testing."""
    return test_data_factory.create_test_videos_batch(5)


@pytest.fixture
def sample_comments(test_data_factory):
    """Sample YouTube comments for testing."""
    return test_data_factory.create_test_comments_batch("dQw4w9WgXcQ", 10)


@pytest.fixture
def test_config(test_engine):
    """Test ETL configuration."""
    return TestDataFactory.create_etl_config(database_url=str(test_engine.url))


@pytest.fixture
def error_handler():
    """Test error handler with debug logging."""
    logger = setup_error_logging("DEBUG")
    handler = ErrorHandler(logger)
    yield handler
    handler.reset_error_counts()


@pytest.fixture
def mock_youtube_api():
    """Mock YouTube API client for testing."""
    mock_api = Mock()

    # Mock video response
    mock_api.videos().list().execute.return_value = {
        "items": [
            {
                "id": "dQw4w9WgXcQ",
                "snippet": {
                    "title": "Test Video",
                    "channelId": "UCuAXFkgsw1L7xaCfnd5JJOw",
                    "channelTitle": "Test Channel",
                    "publishedAt": "2023-01-01T00:00:00Z",
                    "description": "Test video description",
                },
                "contentDetails": {"duration": "PT3M33S"},
                "statistics": {"viewCount": "1000", "likeCount": "100", "commentCount": "50"},
            }
        ]
    }

    # Mock comments response
    mock_api.commentThreads().list().execute.return_value = {
        "items": [
            {
                "id": "test_comment_1",
                "snippet": {
                    "topLevelComment": {
                        "id": "test_comment_1",
                        "snippet": {
                            "textDisplay": "Great video!",
                            "authorDisplayName": "Test User",
                            "likeCount": 5,
                            "publishedAt": "2023-01-01T01:00:00Z",
                        },
                    }
                },
            }
        ]
    }

    return mock_api


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    original_env = os.environ.copy()

    # Set test environment variables
    test_env = {
        "YOUTUBE_API_KEY": "test_api_key_123456789",
        "DB_HOST": "localhost",
        "DB_PORT": "3306",
        "DB_USER": "test_user",
        "DB_PASS": "test_pass",
        "DB_NAME": "test_db",
        "ETL_LOG_LEVEL": "DEBUG",
        "PERSONAL_ISSUE_VIDEO_IDS": "dQw4w9WgXcQ,fC7oUOUEEi4",
        "BLOCKED_TITLE_PATTERNS": "spam.*content|test.*blocked",
        "MIN_VIDEO_DURATION_SECONDS": "30",
        "MAX_VIDEO_DURATION_SECONDS": "3600",
    }

    os.environ.update(test_env)

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


# Test utilities


def insert_test_video(engine: Engine, video: YouTubeVideo):
    """Insert a test video into the database."""
    with engine.connect() as conn:
        conn.execute(
            text(
                """
            INSERT INTO youtube_videos
            (video_id, title, channel_id, channel_title, published_at, duration,
             view_count, like_count, comment_count, isrc)
            VALUES
            (:video_id, :title, :channel_id, :channel_title, :published_at, :duration,
             :view_count, :like_count, :comment_count, :isrc)
        """
            ),
            {
                "video_id": video.video_id,
                "title": video.title,
                "channel_id": video.channel_id,
                "channel_title": video.channel_title,
                "published_at": video.published_at,
                "duration": video.duration,
                "view_count": video.view_count,
                "like_count": video.like_count,
                "comment_count": video.comment_count,
                "isrc": video.isrc,
            },
        )
        conn.commit()


def insert_test_comment(engine: Engine, comment: YouTubeComment):
    """Insert a test comment into the database."""
    with engine.connect() as conn:
        conn.execute(
            text(
                """
            INSERT INTO youtube_comments
            (comment_id, video_id, comment_text, author_name, like_count, published_at)
            VALUES
            (:comment_id, :video_id, :comment_text, :author_name, :like_count, :published_at)
        """
            ),
            {
                "comment_id": comment.comment_id,
                "video_id": comment.video_id,
                "comment_text": comment.comment_text,
                "author_name": comment.author_name,
                "like_count": comment.like_count,
                "published_at": comment.published_at,
            },
        )
        conn.commit()


def get_table_count(engine: Engine, table_name: str) -> int:
    """Get count of records in a table."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()


def assert_video_in_database(engine: Engine, video_id: str) -> bool:
    """Assert that a video exists in the database."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM youtube_videos WHERE video_id = :video_id"), {"video_id": video_id}
        )
        return result.scalar() > 0


def assert_comment_in_database(engine: Engine, comment_id: str) -> bool:
    """Assert that a comment exists in the database."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM youtube_comments WHERE comment_id = :comment_id"), {"comment_id": comment_id}
        )
        return result.scalar() > 0
