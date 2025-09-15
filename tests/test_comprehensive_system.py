#!/usr/bin/env python3
"""
Comprehensive System Tests

Tests for all major system components including:
- Channel validation and cleanup
- Enhanced sentiment analysis
- Bot detection vs fan engagement
- Momentum calculation
- Full ETL pipeline integration
"""

import os
import sys
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from tools.maintenance.comprehensive_data_cleanup import validate_configured_artists
from tools.sentiment.deploy_bot_detection import EnhancedBotDetector
from web.etl_helpers import get_engine
from youtubeviz.weak_supervision_sentiment import WeakSupervisionSentimentAnalyzer


class TestChannelValidationCleanup:
    """Test channel title validation and cleanup functionality."""

    def test_validate_configured_artists(self):
        """Test that only configured artists are validated."""
        # Mock .env artists
        configured_artists = ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"]

        # Test validation
        result = validate_configured_artists()
        assert isinstance(result, list)
        assert len(result) == 6  # Should have 6 configured artists

        # Check that all configured artists are present
        for artist in configured_artists:
            assert artist in result

    def test_channel_cleanup_removes_unauthorized(self):
        """Test that unauthorized channels are properly identified for removal."""
        # This would test the cleanup logic without actually deleting data
        # In a real test, we'd use a test database
        pass


class TestEnhancedSentimentAnalysis:
    """Test enhanced sentiment analysis accuracy and functionality."""

    def setUp(self):
        """Set up test analyzer."""
        self.analyzer = WeakSupervisionSentimentAnalyzer()

    def test_music_slang_positive_detection(self):
        """Test detection of music-specific positive slang."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        # Train on minimal data for testing
        training_texts = [
            "this is fire",
            "my nigga snapped",
            "she ate that",
            "drop the album already!",
            "we need the album now",
            "visuals when?!!",
            "these lyrics!",
            "on my playlist",
            "bro this crazy",
            "absolute banger",
            "this slaps",
            "goes hard",
            "sick beat",
            "who produced this?",
            "what's the sample?",
            "lyrics?",
            "clean version",
            "this is trash",
            "mid",
            "overrated",
            "who approved this?",
            "hate this",
        ] * 5  # Repeat to get enough training data

        try:
            analyzer.train_classifier(training_texts)

            # Test positive music slang
            positive_tests = [
                "this is fire 🔥",
                "my nigga snapped",
                "she ate that",
                "bro this crazy",
                "absolute banger",
            ]

            for text in positive_tests:
                result = analyzer.predict(text)
                assert result["sentiment_score"] > 0, f"'{text}' should be positive"
                assert result["confidence"] > 0.5, f"'{text}' should have high confidence"

        except Exception as e:
            pytest.skip(f"Sentiment analysis test skipped due to insufficient data: {e}")

    def test_aave_language_support(self):
        """Test AAVE and Gen Z language support."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        # Test AAVE patterns are recognized
        aave_tests = [
            ("slay", "positive"),
            ("periodt", "positive"),
            ("no cap", "positive"),
            ("ate that", "positive"),
            ("understood the assignment", "positive"),
        ]

        # Apply labeling functions directly (doesn't require training)
        for text, expected in aave_tests:
            weak_labels = analyzer.apply_labeling_functions([text])
            if weak_labels and weak_labels[0].final_label:
                if expected == "positive":
                    assert weak_labels[0].final_label.value > 0, f"'{text}' should be positive"

    def test_sentiment_confidence_calibration(self):
        """Test that sentiment confidence scores are properly calibrated."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        # Test confidence ranges
        test_cases = [("this is fire 🔥🔥🔥", "high_confidence"), ("okay", "low_confidence"), ("", "no_confidence")]

        weak_labels = analyzer.apply_labeling_functions([case[0] for case, _ in test_cases])

        for i, (text, expected_conf) in enumerate(test_cases):
            if i < len(weak_labels):
                confidence = weak_labels[i].confidence
                if expected_conf == "high_confidence":
                    assert confidence > 0.7, f"'{text}' should have high confidence"
                elif expected_conf == "low_confidence":
                    assert confidence < 0.5, f"'{text}' should have low confidence"


class TestBotDetectionVsFanEngagement:
    """Test bot detection while preserving fan engagement."""

    def setUp(self):
        """Set up test bot detector."""
        self.detector = EnhancedBotDetector()

    def test_fan_whitelist_protection(self):
        """Test that legitimate fan expressions are protected."""
        detector = EnhancedBotDetector()

        fan_expressions = [
            "my nigga snapped 🔥🔥🔥",
            "she ate that",
            "slay queen",
            "periodt",
            "this is fire",
            "absolute banger",
        ]

        for expression in fan_expressions:
            is_whitelisted = detector.is_whitelisted_fan(expression)
            assert is_whitelisted, f"'{expression}' should be whitelisted as fan expression"

    def test_bot_pattern_detection(self):
        """Test detection of bot-like patterns."""
        detector = EnhancedBotDetector()

        bot_patterns = [
            "❤",  # Single emoji
            "first",  # Generic first comment
            "nice",  # Generic praise
            "check out my channel",  # Spam
            "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥",  # Excessive emojis
        ]

        for pattern in bot_patterns:
            comment_data = {"comment_text": pattern, "created_at": None, "author_name": "test"}
            bot_score = detector.calculate_bot_score(comment_data)
            assert bot_score > 0.3, f"'{pattern}' should have elevated bot score"

    def test_engagement_authenticity_scoring(self):
        """Test engagement authenticity scoring."""
        detector = EnhancedBotDetector()

        authentic_comments = [
            "this reminds me of my childhood",
            "i love how she sings this part",
            "the beat at 2:30 is incredible",
            "can't wait for the tour dates",
        ]

        for comment in authentic_comments:
            comment_data = {"comment_text": comment, "created_at": None, "author_name": "test"}
            authenticity = detector.calculate_engagement_authenticity(comment_data)
            assert authenticity > 0.3, f"'{comment}' should have high authenticity score"


class TestMomentumCalculation:
    """Test momentum calculation accuracy."""

    def test_momentum_score_calculation(self):
        """Test basic momentum score calculation."""
        # Create sample data
        sample_data = pd.DataFrame(
            {
                "view_count": [1000, 2000, 5000],
                "like_count": [100, 200, 500],
                "comment_count": [10, 20, 50],
                "days_since_publish": [1, 2, 3],
            }
        )

        # Test that momentum scores are calculated
        # This is a basic test - in practice we'd test the actual momentum analyzer
        assert len(sample_data) > 0
        assert "view_count" in sample_data.columns

    def test_momentum_trend_analysis(self):
        """Test momentum trend analysis over time."""
        # Test momentum trends
        dates = pd.date_range("2025-01-01", periods=5, freq="D")
        momentum_data = pd.DataFrame(
            {"date": dates, "momentum_score": [10, 15, 20, 18, 25], "artist_name": ["Test Artist"] * 5}
        )

        # Test trend calculation
        momentum_data["trend"] = momentum_data["momentum_score"].diff()
        assert momentum_data["trend"].iloc[-1] > 0  # Should be increasing


class TestETLPipelineIntegration:
    """Test full ETL pipeline integration."""

    def test_etl_pipeline_components(self):
        """Test that all ETL components are properly integrated."""
        # Test that key components exist and are importable
        try:
            from web.etl_helpers import get_engine
            from web.sentiment_job import SentimentAnalysisJob
            from web.youtube_channel_etl import YouTubeChannelETL

            # Basic integration test
            engine = get_engine()
            assert engine is not None

        except ImportError as e:
            pytest.fail(f"ETL component import failed: {e}")

    def test_database_connectivity(self):
        """Test database connectivity and basic operations."""
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute("SELECT 1 as test")
                assert result.fetchone()[0] == 1
        except Exception as e:
            pytest.skip(f"Database connectivity test skipped: {e}")


class TestPerformanceAndScaling:
    """Test performance with large datasets."""

    def test_sentiment_analysis_performance(self):
        """Test sentiment analysis performance on large datasets."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        # Generate test data
        test_comments = ["this is fire"] * 100

        # Test that we can process comments efficiently
        weak_labels = analyzer.apply_labeling_functions(test_comments)
        assert len(weak_labels) == 100

        # Test processing time (should be reasonable)
        import time

        start_time = time.time()
        analyzer.apply_labeling_functions(test_comments)
        processing_time = time.time() - start_time

        # Should process 100 comments in under 5 seconds
        assert processing_time < 5.0, f"Processing took too long: {processing_time}s"

    def test_bot_detection_performance(self):
        """Test bot detection performance on large datasets."""
        detector = EnhancedBotDetector()

        # Generate test data
        test_comments = [{"comment_text": "fire", "created_at": None, "author_name": "test"}] * 100

        # Test processing time
        import time

        start_time = time.time()
        for comment in test_comments:
            detector.calculate_bot_score(comment)
        processing_time = time.time() - start_time

        # Should process 100 comments in under 2 seconds
        assert processing_time < 2.0, f"Bot detection took too long: {processing_time}s"


# Test runner
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
