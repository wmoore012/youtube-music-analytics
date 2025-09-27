#!/usr / bin / env python3
"""
Tests for Video Filtering System

This module tests the video filtering functionality to ensure:
- Problematic videos are filtered at API level
- Configuration - driven filtering works correctly
- Filtering decisions are logged properly
- Statistics are tracked accurately
"""

from datetime import datetime
import json
import os
import tempfile
from unittest.mock import patch

import pytest

from web.error_handling import ValidationError
from web.models import VideoFilter, YouTubeVideo
from web.video_filter import (
    FilterReason,
    FilterResult,
    VideoFilterEngine,
    filter_videos_at_api_level,
    load_filter_config,
)

# Valid test IDs for testing
VALID_VIDEO_IDS = {
    "blocked": "dQw4w9WgXcQ",
    "valid1": "oHg5SJYRHA0",
    "valid2": "kJQP7kiw5Fk",
    "personal": "fC7oUOUEEi4",
    "short": "jNQXAC9IVRw",
    "long": "9bZkp7q19f0",
    "test": "ScMzIvxBSi4",
}

VALID_CHANNEL_IDS = {
    "blocked": "UCuAXFkgsw1L7xaCfnd5JJOw",
    "valid": "UC - 9-kyTW8ZkZNDHQJ6FgpwQ",
    "test": "UCsT0YIqwnpJCM - mx7 - gSA4Q",
}


class TestVideoFilter:
    """Test video filtering functionality."""

    def test_filter_blocked_video_id(self):
        """Test filtering of blocked video IDs."""
        config = VideoFilter(blocked_video_ids=[VALID_VIDEO_IDS["blocked"]])
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["blocked"],
            title="Test Video",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Test Channel",
            published_at=datetime.now(),
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.BLOCKED_VIDEO_ID
        assert VALID_VIDEO_IDS["blocked"] in result.details
        assert result.filter_rule == "blocked_video_ids"

    def test_filter_blocked_channel_id(self):
        """Test filtering of blocked channel IDs."""
        config = VideoFilter(blocked_channel_ids=[VALID_CHANNEL_IDS["blocked"]])
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["test"],
            title="Blocked Video",
            channel_id=VALID_CHANNEL_IDS["blocked"],
            channel_title="Blocked Channel",
            published_at=datetime.now(),
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.BLOCKED_CHANNEL_ID
        assert VALID_CHANNEL_IDS["blocked"] in result.details
        assert result.filter_rule == "blocked_channel_ids"

    def test_filter_blocked_title_pattern(self):
        """Test filtering based on title patterns."""
        config = VideoFilter(blocked_title_patterns=["spam.*content", "clickbait"])
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["test"],
            title="This is spam content you should avoid",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Test Channel",
            published_at=datetime.now(),
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.BLOCKED_TITLE_PATTERN
        assert "spam.*content" in result.details
        assert result.filter_rule == "blocked_title_pattern_0"

    def test_filter_duration_too_short(self):
        """Test filtering videos that are too short."""
        config = VideoFilter(min_duration_seconds=60)  # 1 minute minimum
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["short"],
            title="Short Video",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Test Channel",
            published_at=datetime.now(),
            duration="PT30S",  # 30 seconds
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.DURATION_TOO_SHORT
        assert "30s is less than minimum 60s" in result.details
        assert result.filter_rule == "min_duration_seconds"

    def test_filter_duration_too_long(self):
        """Test filtering videos that are too long."""
        config = VideoFilter(max_duration_seconds=300)  # 5 minutes maximum
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["long"],
            title="Long Video",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Test Channel",
            published_at=datetime.now(),
            duration="PT10M",  # 10 minutes
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.DURATION_TOO_LONG
        assert "600s exceeds maximum 300s" in result.details
        assert result.filter_rule == "max_duration_seconds"

    def test_filter_missing_isrc_required(self):
        """Test filtering when ISRC is required but missing."""
        config = VideoFilter(require_isrc=True)
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["test"],
            title="Video Without ISRC",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Test Channel",
            published_at=datetime.now(),
            view_count=1000,
            like_count=100,
            comment_count=50,
            # No ISRC provided
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.MISSING_ISRC_REQUIRED
        assert "ISRC is required but not present" in result.details
        assert result.filter_rule == "require_isrc"

    def test_video_passes_all_filters(self):
        """Test that valid video passes all filters."""
        config = VideoFilter(
            blocked_video_ids=[VALID_VIDEO_IDS["blocked"]],
            blocked_channel_ids=[VALID_CHANNEL_IDS["blocked"]],
            blocked_title_patterns=["spam"],
            min_duration_seconds=30,
            max_duration_seconds=600,
            require_isrc=False,
        )
        filter_engine = VideoFilterEngine(config)

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["valid1"],
            title="Valid Music Video",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Valid Channel",
            published_at=datetime.now(),
            duration="PT4M13S",  # 4 minutes 13 seconds
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is False
        assert result.reason is None
        assert "passed all filtering rules" in result.details
        assert result.filter_rule is None

    def test_personal_issue_videos_filtering(self):
        """Test that personal issue videos are filtered."""
        config = VideoFilter()
        filter_engine = VideoFilterEngine(config)

        # Mock the personal issue videos
        filter_engine.personal_issue_videos = {VALID_VIDEO_IDS["personal"]}

        video = YouTubeVideo(
            video_id=VALID_VIDEO_IDS["personal"],
            title="Personal Issue Video",
            channel_id=VALID_CHANNEL_IDS["valid"],
            channel_title="Test Channel",
            published_at=datetime.now(),
            view_count=1000,
            like_count=100,
            comment_count=50,
        )

        result = filter_engine.filter_video(video)

        assert result.is_filtered is True
        assert result.reason == FilterReason.PERSONAL_ISSUE
        assert "personal issue list" in result.details
        assert result.filter_rule == "personal_issue_videos"

    def test_filter_multiple_videos(self):
        """Test filtering multiple videos and statistics tracking."""
        config = VideoFilter(blocked_video_ids=[VALID_VIDEO_IDS["blocked"], VALID_VIDEO_IDS["personal"]])
        filter_engine = VideoFilterEngine(config)

        videos = [
            YouTubeVideo(
                video_id=VALID_VIDEO_IDS["valid1"],
                title="Valid Video 1",
                channel_id=VALID_CHANNEL_IDS["valid"],
                channel_title="Test Channel",
                published_at=datetime.now(),
                view_count=1000,
                like_count=100,
                comment_count=50,
            ),
            YouTubeVideo(
                video_id=VALID_VIDEO_IDS["blocked"],
                title="Blocked Video 1",
                channel_id=VALID_CHANNEL_IDS["valid"],
                channel_title="Test Channel",
                published_at=datetime.now(),
                view_count=1000,
                like_count=100,
                comment_count=50,
            ),
            YouTubeVideo(
                video_id=VALID_VIDEO_IDS["valid2"],
                title="Valid Video 2",
                channel_id=VALID_CHANNEL_IDS["valid"],
                channel_title="Test Channel",
                published_at=datetime.now(),
                view_count=1000,
                like_count=100,
                comment_count=50,
            ),
            YouTubeVideo(
                video_id=VALID_VIDEO_IDS["personal"],
                title="Blocked Video 2",
                channel_id=VALID_CHANNEL_IDS["valid"],
                channel_title="Test Channel",
                published_at=datetime.now(),
                view_count=1000,
                like_count=100,
                comment_count=50,
            ),
        ]

        passed_videos, filter_results = filter_engine.filter_videos(videos)

        # Check results
        assert len(passed_videos) == 2
        assert len(filter_results) == 4

        # Check statistics
        stats = filter_engine.get_stats()
        assert stats.total_videos == 4
        assert stats.passed_videos == 2
        assert stats.filtered_videos == 2
        assert stats.filter_rate == 50.0
        assert stats.filter_reasons[FilterReason.BLOCKED_VIDEO_ID] == 2

        # Check that correct videos passed
        passed_ids = {video.video_id for video in passed_videos}
        assert passed_ids == {VALID_VIDEO_IDS["valid1"], VALID_VIDEO_IDS["valid2"]}

    def test_duration_parsing(self):
        """Test ISO 8601 duration parsing."""
        config = VideoFilter()
        filter_engine = VideoFilterEngine(config)

        # Test various duration formats
        assert filter_engine._parse_duration("PT4M13S") == 253  # 4:13
        assert filter_engine._parse_duration("PT1H30M45S") == 5445  # 1:30:45
        assert filter_engine._parse_duration("PT30S") == 30  # 0:30
        assert filter_engine._parse_duration("PT5M") == 300  # 5:00
        assert filter_engine._parse_duration("PT2H") == 7200  # 2:00:00
        assert filter_engine._parse_duration("") is None
        assert filter_engine._parse_duration("invalid") is None

    def test_filter_videos_at_api_level(self):
        """Test the main API - level filtering function."""
        videos = [
            YouTubeVideo(
                video_id=VALID_VIDEO_IDS["valid1"],
                title="Valid Video",
                channel_id=VALID_CHANNEL_IDS["valid"],
                channel_title="Test Channel",
                published_at=datetime.now(),
                view_count=1000,
                like_count=100,
                comment_count=50,
            )
        ]

        passed_videos, filter_results = filter_videos_at_api_level(videos)

        assert len(passed_videos) == 1
        assert len(filter_results) == 1
        assert filter_results[0].is_filtered is False
        assert passed_videos[0].video_id == VALID_VIDEO_IDS["valid1"]


if __name__ == "__main__":
    pytest.main([__file__])
