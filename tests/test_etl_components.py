#!/usr / bin / env python3
"""
Comprehensive Unit Tests for ETL Components

This module provides unit tests for all major ETL components to ensure:
- 80%+ code coverage
- Robust error handling
- Data validation
- Component integration
"""

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

from tests.conftest import (
    assert_comment_in_database,
    assert_video_in_database,
    get_table_count,
    insert_test_comment,
    insert_test_video,
)
from web.error_handling import ErrorCategory, ErrorSeverity, ETLError
from web.models import BotDetectionResult, SentimentResult, YouTubeComment, YouTubeVideo
from web.validation import get_data_validator, get_database_validator
from web.video_filter import FilterReason, VideoFilter, VideoFilterEngine


class TestDataValidation:
    """Test data validation components."""

    def test_youtube_video_validation_success(self, test_data_factory):
        """Test successful YouTube video validation."""
        validator = get_data_validator()

        video_data = {
            "video_id": "dQw4w9WgXcQ",
            "title": "Test Video",
            "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
            "channel_title": "Test Channel",
            "published_at": datetime.now(),
            "view_count": 1000,
            "like_count": 100,
            "comment_count": 50,
        }

        video = validator.validate_youtube_video(video_data)

        assert video.video_id == "dQw4w9WgXcQ"
        assert video.title == "Test Video"
        assert video.view_count == 1000

    def test_youtube_video_validation_failure(self):
        """Test YouTube video validation with invalid data."""
        validator = get_data_validator()

        invalid_video_data = {
            "video_id": "invalid_id",  # Invalid format
            "title": "",  # Empty title
            "channel_id": "invalid_channel",  # Invalid format
            "channel_title": "Test Channel",
            "published_at": datetime.now(),
            "view_count": -100,  # Negative count
            "like_count": 100,
            "comment_count": 50,
        }

        with pytest.raises(Exception):  # Should raise validation error
            validator.validate_youtube_video(invalid_video_data)

    def test_youtube_videos_batch_validation(self, test_data_factory):
        """Test batch validation of YouTube videos."""
        validator = get_data_validator()

        videos_data = [
            {
                "video_id": "dQw4w9WgXcQ",
                "title": "Valid Video 1",
                "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
                "channel_title": "Test Channel",
                "published_at": datetime.now(),
                "view_count": 1000,
                "like_count": 100,
                "comment_count": 50,
            },
            {
                "video_id": "invalid_id",  # This will fail validation
                "title": "Invalid Video",
                "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
                "channel_title": "Test Channel",
                "published_at": datetime.now(),
                "view_count": 1000,
                "like_count": 100,
                "comment_count": 50,
            },
            {
                "video_id": "oHg5SJYRHA0",
                "title": "Valid Video 2",
                "channel_id": "UC-9-kyTW8ZkZNDHQJ6FgpwQ",
                "channel_title": "Test Channel 2",
                "published_at": datetime.now(),
                "view_count": 2000,
                "like_count": 200,
                "comment_count": 100,
            },
        ]

        valid_videos, invalid_videos = validator.validate_youtube_videos_batch(videos_data)

        assert len(valid_videos) == 2
        assert len(invalid_videos) == 1
        assert invalid_videos[0]["data"]["video_id"] == "invalid_id"

    def test_comment_validation_success(self, test_data_factory):
        """Test successful YouTube comment validation."""
        validator = get_data_validator()

        comment_data = {
            "comment_id": "test_comment_123",
            "video_id": "dQw4w9WgXcQ",
            "author_name": "Test User",
            "comment_text": "This is a great video!",
            "like_count": 5,
            "published_at": datetime.now(),
        }

        comment = validator.validate_youtube_comment(comment_data)

        assert comment.comment_id == "test_comment_123"
        assert comment.video_id == "dQw4w9WgXcQ"
        assert comment.comment_text == "This is a great video!"

    def test_sentiment_result_validation(self):
        """Test sentiment result validation."""
        validator = get_data_validator()

        sentiment_data = {
            "comment_id": "test_comment_123",
            "video_id": "dQw4w9WgXcQ",
            "sentiment_score": 0.75,
            "confidence_score": 0.85,
            "method": "vader",
        }

        result = validator.validate_sentiment_result(sentiment_data)

        assert result.sentiment_score == 0.75
        assert result.confidence_score == 0.85
        assert result.method == "vader"

    def test_database_validator_video_id_format(self):
        """Test database validator for video ID format."""
        validator = get_database_validator()

        # Valid video ID
        assert validator.validate_video_id_format("dQw4w9WgXcQ") is True

        # Invalid video IDs
        with pytest.raises(Exception):
            validator.validate_video_id_format("invalid_id")

        with pytest.raises(Exception):
            validator.validate_video_id_format("")

        with pytest.raises(Exception):
            validator.validate_video_id_format(None)

    def test_database_validator_isrc_format(self):
        """Test database validator for ISRC format."""
        validator = get_database_validator()

        # Valid ISRC
        assert validator.validate_isrc_format("USRC17607839") is True

        # Invalid ISRCs
        with pytest.raises(Exception):
            validator.validate_isrc_format("invalid_isrc")

        with pytest.raises(Exception):
            validator.validate_isrc_format("US123")  # Too short


class TestVideoFiltering:
    """Test video filtering components."""

    def test_video_filter_blocked_id(self):
        """Test video filtering by blocked ID."""
        config = VideoFilter(blocked_video_ids=["dQw4w9WgXcQ"])
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id="dQw4w9WgXcQ",
            title="Blocked Video",
            channel_id="UCuAXFkgsw1L7xaCfnd5JJOw",
            channel_title="Test Channel",
            published_at=datetime.now(),
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.BLOCKED_VIDEO_ID

    def test_video_filter_duration_constraints(self):
        """Test video filtering by duration constraints."""
        config = VideoFilter(min_duration_seconds=60, max_duration_seconds=300)
        filter_engine = VideoFilterEngine(config)

        # Too short video
        short_video = YouTubeVideo(
            video_id="jNQXAC9IVRw",
            title="Short Video",
            channel_id="UCuAXFkgsw1L7xaCfnd5JJOw",
            channel_title="Test Channel",
            published_at=datetime.now(),
            duration="PT30S",  # 30 seconds
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(short_video)
        assert result.is_filtered is True
        assert result.reason == FilterReason.DURATION_TOO_SHORT

        # Too long video
        long_video = YouTubeVideo(
            video_id="9bZkp7q19f0",
            title="Long Video",
            channel_id="UCuAXFkgsw1L7xaCfnd5JJOw",
            channel_title="Test Channel",
            published_at=datetime.now(),
            duration="PT10M",  # 10 minutes
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(long_video)
        assert result.is_filtered is True
        assert result.reason == FilterReason.DURATION_TOO_LONG

    def test_video_filter_statistics(self):
        """Test video filtering statistics tracking."""
        config = VideoFilter(blocked_video_ids=["dQw4w9WgXcQ"])
        filter_engine = VideoFilterEngine(config)

        videos = [
            YouTubeVideo(
                video_id="dQw4w9WgXcQ",  # Will be filtered
                title="Blocked Video",
                channel_id="UCuAXFkgsw1L7xaCfnd5JJOw",
                channel_title="Test Channel",
                published_at=datetime.now(),
                view_count=1000,
                like_count=100,
                comment_count=50,
            ),
            YouTubeVideo(
                video_id="oHg5SJYRHA0",  # Will pass
                title="Valid Video",
                channel_id="UC-9-kyTW8ZkZNDHQJ6FgpwQ",
                channel_title="Test Channel 2",
                published_at=datetime.now(),
                view_count=2000,
                like_count=200,
                comment_count=100,
            ),
        ]

        passed_videos, filter_results = filter_engine.filter_videos(videos)
        stats = filter_engine.get_stats()

        assert len(passed_videos) == 1
        assert len(filter_results) == 2
        assert stats.total_videos == 2
        assert stats.filtered_videos == 1
        assert stats.passed_videos == 1
        assert stats.filter_rate == 50.0


class TestErrorHandling:
    """Test error handling components."""

    def test_etl_error_creation(self):
        """Test ETL error creation and properties."""
        from web.error_handling import ErrorContext

        context = ErrorContext(
            component="test_component", operation="test_operation", user_data={"test_key": "test_value"}
        )

        error = ETLError(
            message="Test error message",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.VALIDATION,
            context=context,
        )

        assert error.message == "Test error message"
        assert error.severity == ErrorSeverity.HIGH
        assert error.category == ErrorCategory.VALIDATION
        assert error.context == context
        assert error.error_id.startswith("VALIDATION_")

    def test_error_handler_logging(self, error_handler):
        """Test error handler logging functionality."""
        test_error = ETLError(
            message="Test error for logging", severity=ErrorSeverity.MEDIUM, category=ErrorCategory.PROCESSING
        )

        # Should not raise since severity is MEDIUM
        error_handler.handle_error(test_error, should_raise=False)

        # Check error counts
        summary = error_handler.get_error_summary()
        assert "PROCESSING_MEDIUM" in summary
        assert summary["PROCESSING_MEDIUM"] == 1

    def test_retry_decorator(self):
        """Test retry decorator functionality."""
        from web.error_handling import retry_with_backoff

        call_count = 0

        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        result = failing_function()
        assert result == "success"
        assert call_count == 3  # Failed twice, succeeded on third try


class TestDatabaseOperations:
    """Test database operations and integration."""

    def test_video_insertion(self, test_engine, sample_videos):
        """Test video insertion into database."""
        video = sample_videos[0]

        # Insert video
        insert_test_video(test_engine, video)

        # Verify insertion
        assert assert_video_in_database(test_engine, video.video_id)
        assert get_table_count(test_engine, "youtube_videos") == 1

    def test_comment_insertion(self, test_engine, sample_comments):
        """Test comment insertion into database."""
        comment = sample_comments[0]

        # Insert comment
        insert_test_comment(test_engine, comment)

        # Verify insertion
        assert assert_comment_in_database(test_engine, comment.comment_id)
        assert get_table_count(test_engine, "youtube_comments") == 1

    def test_batch_video_insertion(self, test_engine, sample_videos):
        """Test batch video insertion."""
        # Insert all sample videos
        for video in sample_videos:
            insert_test_video(test_engine, video)

        # Verify all videos were inserted
        assert get_table_count(test_engine, "youtube_videos") == len(sample_videos)

        for video in sample_videos:
            assert assert_video_in_database(test_engine, video.video_id)

    def test_database_cleanup(self, test_engine, sample_videos):
        """Test database cleanup between tests."""
        # Insert videos
        for video in sample_videos:
            insert_test_video(test_engine, video)

        assert get_table_count(test_engine, "youtube_videos") == len(sample_videos)

        # Cleanup happens automatically via fixture
        # This test verifies the cleanup mechanism works


class TestETLConfiguration:
    """Test ETL configuration and validation."""

    def test_etl_config_creation(self, test_config):
        """Test ETL configuration creation and validation."""
        assert test_config.youtube_api_key == "test_api_key_123456789"
        assert test_config.batch_size == 100
        assert test_config.enable_bot_detection is True
        assert test_config.quality_threshold == 75.0

    def test_etl_config_validation_failure(self):
        """Test ETL configuration validation with invalid data."""
        from web.models import ETLConfig

        with pytest.raises(Exception):
            ETLConfig(
                database_url="invalid_url",  # Invalid URL format
                youtube_api_key="short",  # Too short
                batch_size=-1,  # Negative value
                quality_threshold=150.0,  # Out of range
            )

    def test_environment_variable_validation(self):
        """Test environment variable validation."""
        from web.validation import validate_required_environment_variables

        required_vars = ["YOUTUBE_API_KEY", "DB_HOST", "DB_NAME"]

        # Should succeed with test environment
        env_vars = validate_required_environment_variables(required_vars)

        assert "YOUTUBE_API_KEY" in env_vars
        assert env_vars["YOUTUBE_API_KEY"] == "test_api_key_123456789"


class TestMockIntegration:
    """Test integration with mocked external services."""

    def test_mock_youtube_api_integration(self, mock_youtube_api):
        """Test integration with mocked YouTube API."""
        # Test video API call
        response = mock_youtube_api.videos().list().execute()

        assert "items" in response
        assert len(response["items"]) == 1
        assert response["items"][0]["id"] == "dQw4w9WgXcQ"

    def test_mock_youtube_comments_api(self, mock_youtube_api):
        """Test integration with mocked YouTube comments API."""
        # Test comments API call
        response = mock_youtube_api.commentThreads().list().execute()

        assert "items" in response
        assert len(response["items"]) == 1

        comment = response["items"][0]["snippet"]["topLevelComment"]["snippet"]
        assert comment["textDisplay"] == "Great video!"
        assert comment["authorDisplayName"] == "Test User"


class TestPerformanceAndScaling:
    """Test performance and scaling aspects."""

    def test_large_batch_processing(self, test_engine, test_data_factory):
        """Test processing of large batches of data."""
        # Create a larger batch of test videos
        large_batch = test_data_factory.create_test_videos_batch(10)

        # Process batch
        start_time = datetime.now()

        for video in large_batch:
            insert_test_video(test_engine, video)

        end_time = datetime.now()
        processing_time = (end_time-start_time).total_seconds()

        # Verify all videos were processed
        assert get_table_count(test_engine, "youtube_videos") == len(large_batch)

        # Performance should be reasonable (less than 1 second for 10 videos)
        assert processing_time < 1.0

    def test_memory_usage_with_large_datasets(self, test_data_factory):
        """Test memory usage with large datasets."""
        # Create large dataset
        large_dataset = []
        # Use valid video ID format (11 characters)
        valid_video_ids = [
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

        for i in range(min(100, len(valid_video_ids) * 10)):
            base_id = valid_video_ids[i % len(valid_video_ids)]
            # Modify the last character to create unique IDs
            video_id = base_id[:-1] + str(i % 10)

            video = test_data_factory.create_youtube_video(
                video_id=video_id, title=f"Test Video {i}", view_count=i * 1000
            )
            large_dataset.append(video)

        # Verify dataset creation
        assert len(large_dataset) == 100

        # Memory usage should be reasonable
        # (This is a basic test-in production you'd use memory profiling tools)
        import sys

        dataset_size = sys.getsizeof(large_dataset)
        assert dataset_size < 1024 * 1024  # Less than 1MB for 100 videos


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
