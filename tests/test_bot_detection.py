"""
Test-Driven Development for Bot Detection System

This module contains comprehensive tests for the bot detection functionality.
Following TDD principles: Red -> Green -> Refactor
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.icatalogviz.bot_detection import (
    BotDetectionConfig,
    BotDetector,
    _clamp_01,
    _count_emojis,
    _normalize_text,
    _strip_emojis,
    analyze_bot_patterns,
    load_recent_comments,
)


class TestTextProcessingUtilities:
    """Test text processing utility functions."""

    def test_normalize_text_basic(self):
        """Test basic text normalization."""
        assert _normalize_text("Hello World") == "hello world"
        assert _normalize_text("  HELLO   WORLD  ") == "hello world"
        assert _normalize_text("") == ""
        assert _normalize_text(None) == ""

    def test_normalize_text_unicode(self):
        """Test Unicode normalization."""
        assert _normalize_text("café") == "café"
        assert _normalize_text("naïve") == "naïve"
        assert _normalize_text("CAFÉ") == "café"

    def test_strip_emojis(self):
        """Test emoji removal."""
        assert _strip_emojis("Hello 🔥 World") == "Hello  World"
        assert _strip_emojis("🔥🔥🔥") == ""
        assert _strip_emojis("No emojis here") == "No emojis here"

    def test_count_emojis(self):
        """Test emoji counting."""
        assert _count_emojis("Hello 🔥 World") == 1
        assert _count_emojis("🔥🔥🔥") == 3
        assert _count_emojis("No emojis here") == 0
        assert _count_emojis("Love this! ❤️😍🔥") == 3

    def test_clamp_01(self):
        """Test value clamping to [0, 1] range."""
        assert _clamp_01(0.5) == 0.5
        assert _clamp_01(-0.5) == 0.0
        assert _clamp_01(1.5) == 1.0
        assert _clamp_01(0.0) == 0.0
        assert _clamp_01(1.0) == 1.0


class TestBotDetectionConfig:
    """Test bot detection configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = BotDetectionConfig()

        assert config.near_dupe_threshold == 0.90
        assert config.min_dupe_cluster == 3
        assert config.burst_window_seconds == 30
        assert config.emoji_max_weight == 0.15
        assert "love this" in config.whitelist_phrases
        assert "fire" in config.whitelist_phrases

    def test_custom_config(self):
        """Test custom configuration."""
        config = BotDetectionConfig(
            near_dupe_threshold=0.85, min_dupe_cluster=5, whitelist_phrases=frozenset({"test", "custom"})
        )

        assert config.near_dupe_threshold == 0.85
        assert config.min_dupe_cluster == 5
        assert "test" in config.whitelist_phrases
        assert "custom" in config.whitelist_phrases

    def test_config_validation(self):
        """Test configuration validation."""
        # Valid threshold
        config = BotDetectionConfig(near_dupe_threshold=0.75)
        assert config.near_dupe_threshold == 0.75

        # Invalid threshold should raise error
        with pytest.raises(ValueError):
            BotDetectionConfig(near_dupe_threshold=0.4)

        with pytest.raises(ValueError):
            BotDetectionConfig(near_dupe_threshold=1.0)


class TestBotDetector:
    """Test the main BotDetector class."""

    @pytest.fixture
    def sample_comments_df(self):
        """Create sample comments DataFrame for testing."""
        return pd.DataFrame(
            {
                "comment_id": ["c1", "c2", "c3", "c4", "c5"],
                "video_id": ["v1", "v1", "v2", "v2", "v1"],
                "comment_text": [
                    "This is amazing!",
                    "This is amazing!",  # Duplicate
                    "Love this song 🔥",
                    "Terrible music",
                    "fire fire fire",  # Repetitive
                ],
                "author_name": ["user1", "user2", "user3", "user4", "user1"],
                "like_count": [5, 0, 10, 2, 1],
                "published_at": [
                    datetime.now() - timedelta(hours=1),
                    datetime.now() - timedelta(minutes=30),
                    datetime.now() - timedelta(hours=2),
                    datetime.now() - timedelta(hours=3),
                    datetime.now() - timedelta(minutes=5),
                ],
            }
        )

    @pytest.fixture
    def bot_detector(self):
        """Create BotDetector instance for testing."""
        config = BotDetectionConfig(whitelist_phrases=frozenset({"love this", "fire", "amazing"}))
        return BotDetector(config=config)

    def test_analyze_comments_basic(self, bot_detector, sample_comments_df):
        """Test basic comment analysis."""
        results = bot_detector.analyze_comments(sample_comments_df)

        # Check structure
        assert len(results) == 5
        assert "bot_score" in results.columns
        assert "bot_risk_level" in results.columns
        assert "duplicate_count_local" in results.columns

        # Check score range
        assert all(0 <= score <= 100 for score in results["bot_score"])

        # Check risk levels
        assert all(level in ["Low", "Medium", "High"] for level in results["bot_risk_level"])

    def test_analyze_comments_missing_columns(self, bot_detector):
        """Test error handling for missing columns."""
        incomplete_df = pd.DataFrame(
            {
                "comment_id": ["c1"],
                "comment_text": ["test"],
                # Missing required columns
            }
        )

        with pytest.raises(ValueError, match="Missing required columns"):
            bot_detector.analyze_comments(incomplete_df)

    def test_analyze_comments_empty_dataframe(self, bot_detector):
        """Test error handling for empty DataFrame."""
        empty_df = pd.DataFrame(
            columns=["comment_id", "video_id", "comment_text", "author_name", "like_count", "published_at"]
        )

        with pytest.raises(ValueError, match="Input DataFrame is empty"):
            bot_detector.analyze_comments(empty_df)

    def test_duplicate_detection(self, bot_detector, sample_comments_df):
        """Test duplicate comment detection."""
        results = bot_detector.analyze_comments(sample_comments_df)

        # Comments with same text should have higher duplicate counts
        duplicate_comments = results[results["comment_text"] == "This is amazing!"]
        assert len(duplicate_comments) == 2

        # Both should have duplicate count > 1
        assert all(count >= 2 for count in duplicate_comments["duplicate_count_local"])

    def test_whitelist_handling(self, bot_detector, sample_comments_df):
        """Test whitelist phrase handling."""
        results = bot_detector.analyze_comments(sample_comments_df)

        # Comments with whitelisted phrases should be marked
        whitelisted = results[results["is_whitelisted"]]
        assert len(whitelisted) > 0

        # Whitelisted comments should generally have lower scores
        non_whitelisted = results[~results["is_whitelisted"]]
        if len(whitelisted) > 0 and len(non_whitelisted) > 0:
            avg_whitelisted_score = whitelisted["bot_score"].mean()
            avg_non_whitelisted_score = non_whitelisted["bot_score"].mean()
            # This is a general trend, not absolute
            assert avg_whitelisted_score <= avg_non_whitelisted_score + 10  # Allow some variance

    def test_author_repetition_detection(self, bot_detector, sample_comments_df):
        """Test author repetition scoring."""
        results = bot_detector.analyze_comments(sample_comments_df)

        # user1 appears twice, should have higher repetition score
        user1_comments = results[results["author_name"] == "user1"]
        user3_comments = results[results["author_name"] == "user3"]

        if len(user1_comments) > 0 and len(user3_comments) > 0:
            user1_avg_repetition = user1_comments["author_repetition_score"].mean()
            user3_avg_repetition = user3_comments["author_repetition_score"].mean()
            assert user1_avg_repetition >= user3_avg_repetition

    def test_emoji_bonus(self, bot_detector, sample_comments_df):
        """Test emoji bonus calculation."""
        results = bot_detector.analyze_comments(sample_comments_df)

        # Comments with emojis should have emoji count > 0
        emoji_comments = results[results["emoji_count"] > 0]
        assert len(emoji_comments) > 0

        # Check that emoji count is correctly calculated
        fire_comment = results[results["comment_text"].str.contains("🔥", na=False)]
        if len(fire_comment) > 0:
            assert fire_comment.iloc[0]["emoji_count"] >= 1


class TestDatabaseIntegration:
    """Test database integration functions."""

    @patch("src.icatalogviz.bot_detection.pd.read_sql")
    def test_load_recent_comments(self, mock_read_sql):
        """Test loading recent comments from database."""
        # Mock database response
        mock_read_sql.return_value = pd.DataFrame(
            {
                "comment_id": ["c1", "c2"],
                "video_id": ["v1", "v2"],
                "comment_text": ["test1", "test2"],
                "author_name": ["user1", "user2"],
                "like_count": [1, 2],
                "published_at": [datetime.now(), datetime.now()],
                "video_title": ["title1", "title2"],
                "channel_title": ["channel1", "channel2"],
            }
        )

        mock_engine = Mock()
        result = load_recent_comments(mock_engine, days=7)

        assert len(result) == 2
        assert "comment_id" in result.columns
        assert "comment_text" in result.columns
        mock_read_sql.assert_called_once()

    @patch("src.icatalogviz.bot_detection.load_recent_comments")
    def test_analyze_bot_patterns(self, mock_load_comments):
        """Test the convenience function for bot pattern analysis."""
        # Mock comment data
        mock_load_comments.return_value = pd.DataFrame(
            {
                "comment_id": ["c1", "c2"],
                "video_id": ["v1", "v2"],
                "comment_text": ["test comment 1", "test comment 2"],
                "author_name": ["user1", "user2"],
                "like_count": [1, 2],
                "published_at": [datetime.now(), datetime.now()],
            }
        )

        mock_engine = Mock()
        config = BotDetectionConfig()

        results = analyze_bot_patterns(mock_engine, config=config, days=30)

        assert len(results) == 2
        assert "bot_score" in results.columns
        mock_load_comments.assert_called_once_with(mock_engine, days=30)

    @patch("src.icatalogviz.bot_detection.load_recent_comments")
    def test_analyze_bot_patterns_no_comments(self, mock_load_comments):
        """Test error handling when no comments are found."""
        # Mock empty response
        mock_load_comments.return_value = pd.DataFrame()

        mock_engine = Mock()

        with pytest.raises(ValueError, match="No comments found"):
            analyze_bot_patterns(mock_engine, days=30)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_single_comment(self):
        """Test analysis with single comment."""
        config = BotDetectionConfig()
        detector = BotDetector(config=config)

        single_comment_df = pd.DataFrame(
            {
                "comment_id": ["c1"],
                "video_id": ["v1"],
                "comment_text": ["Single comment"],
                "author_name": ["user1"],
                "like_count": [1],
                "published_at": [datetime.now()],
            }
        )

        results = detector.analyze_comments(single_comment_df)

        assert len(results) == 1
        assert 0 <= results.iloc[0]["bot_score"] <= 100

    def test_empty_comments(self):
        """Test handling of empty comment text."""
        config = BotDetectionConfig()
        detector = BotDetector(config=config)

        empty_comments_df = pd.DataFrame(
            {
                "comment_id": ["c1", "c2"],
                "video_id": ["v1", "v1"],
                "comment_text": ["", None],
                "author_name": ["user1", "user2"],
                "like_count": [0, 0],
                "published_at": [datetime.now(), datetime.now()],
            }
        )

        results = detector.analyze_comments(empty_comments_df)

        # Should handle empty comments gracefully
        assert len(results) == 2
        assert all(0 <= score <= 100 for score in results["bot_score"])

    def test_special_characters(self):
        """Test handling of special characters and Unicode."""
        config = BotDetectionConfig()
        detector = BotDetector(config=config)

        special_chars_df = pd.DataFrame(
            {
                "comment_id": ["c1", "c2", "c3"],
                "video_id": ["v1", "v1", "v1"],
                "comment_text": ["Comment with émojis 🔥🎵", "Special chars: @#$%^&*()", "Unicode: café naïve résumé"],
                "author_name": ["user1", "user2", "user3"],
                "like_count": [1, 2, 3],
                "published_at": [datetime.now(), datetime.now(), datetime.now()],
            }
        )

        results = detector.analyze_comments(special_chars_df)

        # Should handle special characters without errors
        assert len(results) == 3
        assert all(0 <= score <= 100 for score in results["bot_score"])


class TestPerformance:
    """Test performance characteristics."""

    def test_large_dataset_handling(self):
        """Test handling of larger datasets."""
        config = BotDetectionConfig()
        detector = BotDetector(config=config)

        # Create larger test dataset
        n_comments = 100
        large_df = pd.DataFrame(
            {
                "comment_id": [f"c{i}" for i in range(n_comments)],
                "video_id": [f"v{i // 10}" for i in range(n_comments)],  # 10 comments per video
                "comment_text": [f"Comment number {i}" for i in range(n_comments)],
                "author_name": [f"user{i // 5}" for i in range(n_comments)],  # 5 comments per user
                "like_count": [i % 10 for i in range(n_comments)],
                "published_at": [datetime.now() - timedelta(minutes=i) for i in range(n_comments)],
            }
        )

        results = detector.analyze_comments(large_df)

        assert len(results) == n_comments
        assert all(0 <= score <= 100 for score in results["bot_score"])

        # Should complete in reasonable time (this is implicit - if it hangs, test fails)

    def test_duplicate_heavy_dataset(self):
        """Test dataset with many duplicates."""
        config = BotDetectionConfig()
        detector = BotDetector(config=config)

        # Create dataset with many duplicates
        duplicate_df = pd.DataFrame(
            {
                "comment_id": [f"c{i}" for i in range(20)],
                "video_id": ["v1"] * 20,
                "comment_text": ["Duplicate comment"] * 15 + ["Unique comment"] * 5,
                "author_name": [f"user{i}" for i in range(20)],
                "like_count": [1] * 20,
                "published_at": [datetime.now() - timedelta(minutes=i) for i in range(20)],
            }
        )

        results = detector.analyze_comments(duplicate_df)

        assert len(results) == 20

        # Duplicate comments should have higher scores
        duplicate_comments = results[results["comment_text"] == "Duplicate comment"]
        unique_comments = results[results["comment_text"] == "Unique comment"]

        if len(duplicate_comments) > 0 and len(unique_comments) > 0:
            avg_duplicate_score = duplicate_comments["bot_score"].mean()
            avg_unique_score = unique_comments["bot_score"].mean()
            assert avg_duplicate_score >= avg_unique_score


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
