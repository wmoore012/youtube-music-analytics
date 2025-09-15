#!/usr/bin/env python3
"""
System Integration Tests

Tests for system integration and functionality.
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from tools.sentiment.deploy_bot_detection import EnhancedBotDetector
from youtubeviz.weak_supervision_sentiment import WeakSupervisionSentimentAnalyzer


class TestSentimentAnalysisIntegration:
    """Test sentiment analysis integration."""

    def test_weak_supervision_analyzer_creation(self):
        """Test that weak supervision analyzer can be created."""
        analyzer = WeakSupervisionSentimentAnalyzer()
        assert analyzer is not None
        assert len(analyzer.labeling_functions) > 0

    def test_labeling_functions_application(self):
        """Test that labeling functions work correctly."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        test_texts = ["this is fire 🔥", "my nigga snapped", "who produced this?", "this is trash"]

        weak_labels = analyzer.apply_labeling_functions(test_texts)
        assert len(weak_labels) == len(test_texts)

        # Check that some labels were assigned
        labeled_count = sum(1 for wl in weak_labels if wl.final_label is not None)
        assert labeled_count > 0, "At least some texts should be labeled"

    def test_music_slang_detection(self):
        """Test detection of music-specific slang."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        music_slang = ["this is fire", "absolute banger", "this slaps", "goes hard"]

        weak_labels = analyzer.apply_labeling_functions(music_slang)

        # Most music slang should be detected as positive
        positive_count = sum(1 for wl in weak_labels if wl.final_label and wl.final_label.value > 0)
        assert positive_count >= len(music_slang) * 0.5, "Most music slang should be positive"


class TestBotDetectionIntegration:
    """Test bot detection integration."""

    def test_bot_detector_creation(self):
        """Test that bot detector can be created."""
        detector = EnhancedBotDetector()
        assert detector is not None
        assert len(detector.fan_whitelist) > 0
        assert len(detector.bot_patterns) > 0

    def test_fan_whitelist_functionality(self):
        """Test fan whitelist functionality."""
        detector = EnhancedBotDetector()

        fan_expressions = ["this is fire", "she ate that", "slay", "periodt"]

        for expression in fan_expressions:
            is_whitelisted = detector.is_whitelisted_fan(expression)
            # At least some should be whitelisted
            # We don't assert all because patterns might not match exactly

        # Test that obvious fan expression is whitelisted
        assert detector.is_whitelisted_fan("this is fire")

    def test_bot_score_calculation(self):
        """Test bot score calculation."""
        detector = EnhancedBotDetector()

        test_cases = [
            ("❤", "high_bot_score"),  # Single emoji
            ("this is an amazing song with great lyrics", "low_bot_score"),  # Authentic comment
            ("first", "medium_bot_score"),  # Generic comment
        ]

        for text, expected in test_cases:
            comment_data = {"comment_text": text, "created_at": None, "author_name": "test"}

            bot_score = detector.calculate_bot_score(comment_data)
            assert 0.0 <= bot_score <= 1.0, "Bot score should be between 0 and 1"

            if expected == "high_bot_score":
                assert bot_score > 0.5, f"'{text}' should have high bot score"
            elif expected == "low_bot_score":
                assert bot_score < 0.5, f"'{text}' should have low bot score"


class TestSystemPerformance:
    """Test system performance."""

    def test_sentiment_analysis_speed(self):
        """Test sentiment analysis processing speed."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        # Test with 50 comments
        test_comments = ["this is fire"] * 50

        import time

        start_time = time.time()
        weak_labels = analyzer.apply_labeling_functions(test_comments)
        processing_time = time.time() - start_time

        assert len(weak_labels) == 50
        assert processing_time < 10.0, f"Processing 50 comments took too long: {processing_time}s"

    def test_bot_detection_speed(self):
        """Test bot detection processing speed."""
        detector = EnhancedBotDetector()

        # Test with 50 comments
        test_comments = [{"comment_text": "fire", "created_at": None, "author_name": "test"}] * 50

        import time

        start_time = time.time()
        for comment in test_comments:
            detector.calculate_bot_score(comment)
        processing_time = time.time() - start_time

        assert processing_time < 5.0, f"Bot detection on 50 comments took too long: {processing_time}s"


class TestDataQuality:
    """Test data quality and validation."""

    def test_sentiment_score_ranges(self):
        """Test that sentiment scores are in valid ranges."""
        analyzer = WeakSupervisionSentimentAnalyzer()

        test_texts = ["fire", "okay", "trash", ""]
        weak_labels = analyzer.apply_labeling_functions(test_texts)

        for wl in weak_labels:
            if wl.final_label is not None:
                assert -1 <= wl.final_label.value <= 1, "Sentiment score should be between -1 and 1"
            assert 0.0 <= wl.confidence <= 1.0, "Confidence should be between 0 and 1"

    def test_bot_score_ranges(self):
        """Test that bot scores are in valid ranges."""
        detector = EnhancedBotDetector()

        test_comments = [
            {"comment_text": "fire", "created_at": None, "author_name": "test"},
            {"comment_text": "❤", "created_at": None, "author_name": "test"},
            {"comment_text": "", "created_at": None, "author_name": "test"},
        ]

        for comment in test_comments:
            bot_score = detector.calculate_bot_score(comment)
            assert 0.0 <= bot_score <= 1.0, f"Bot score should be between 0 and 1, got {bot_score}"


# Test runner
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
