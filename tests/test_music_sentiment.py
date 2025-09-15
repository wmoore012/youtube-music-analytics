#!/usr/bin/env python3
"""
Tests for enhanced music industry sentiment analysis
"""

import pandas as pd
import pytest

from src.youtubeviz.music_sentiment import (
    MusicIndustrySentimentAnalyzer,
    MusicSentimentConfig,
    update_comment_sentiment_table,
)


class TestMusicSentimentConfig:
    """Test music sentiment configuration."""

    def test_config_initialization(self):
        """Test that config initializes with expected patterns."""
        config = MusicSentimentConfig()

        # Check that positive patterns exist
        assert len(config.positive_patterns) > 0
        assert r"\bthis is sick\b" in config.positive_patterns
        assert r"\bfucking queen\b" in config.positive_patterns

        # Check emoji sentiment mapping
        assert len(config.emoji_sentiment) > 0
        assert "😍" in config.emoji_sentiment
        assert "🔥" in config.emoji_sentiment

        print("✅ Music sentiment config initialized correctly")


class TestMusicIndustrySentimentAnalyzer:
    """Test enhanced sentiment analyzer for music industry."""

    @pytest.fixture
    def analyzer(self):
        """Create analyzer instance for testing."""
        return MusicIndustrySentimentAnalyzer()

    def test_positive_music_slang(self, analyzer):
        """Test recognition of positive music industry slang."""
        test_cases = [
            ("this is sick!", True, "sick as positive"),
            ("fucking queen!", True, "queen as positive"),
            ("go off king", True, "go off as encouragement"),
            ("oh my yes!", True, "excitement"),
            ("fuck it up", True, "encouragement"),
            ("I need the lyrics", True, "engagement"),
            ("yessir!", True, "affirmation"),
            ("10/10", True, "rating"),
            ("100/10", True, "high rating"),
            ("queen", True, "royalty reference"),
            ("hot bish", True, "compliment"),
            ("bad bish", True, "compliment"),
            ("YES MOTHER!", True, "excitement"),
            ("Bitchhh!", True, "excitement"),
            ("bitch, it's givinnnng!", True, "approval"),
            ("please come to atlanta", True, "tour request"),
        ]

        for comment, should_be_positive, description in test_cases:
            result = analyzer.analyze_comment(comment)
            sentiment_score = result["sentiment_score"]

            if should_be_positive:
                assert sentiment_score > 0, f"Failed: {description} - '{comment}' got {sentiment_score}"
            else:
                assert sentiment_score <= 0, f"Failed: {description} - '{comment}' got {sentiment_score}"

        print("✅ Positive music slang recognition working correctly")

    def test_emoji_sentiment(self, analyzer):
        """Test emoji sentiment analysis."""
        test_cases = [
            ("This song 😍", True),
            ("Fire track 🔥", True),
            ("Perfect 💯", True),
            ("Queen 👑", True),
            ("Love this ❤️", True),
        ]

        for comment, should_be_positive in test_cases:
            result = analyzer.analyze_comment(comment)
            sentiment_score = result["sentiment_score"]

            if should_be_positive:
                assert sentiment_score > 0, f"Emoji sentiment failed for: {comment}"

        print("✅ Emoji sentiment analysis working correctly")

    def test_beat_appreciation_detection(self, analyzer):
        """Test detection of beat appreciation comments."""
        test_cases = [
            ("this beat is fire", True),
            ("the beat goes hard", True),
            ("sick production", True),
            ("crazy instrumental", True),
            ("drums are insane", True),
            ("bass is sick", True),
            ("just a regular comment", False),
            ("I like the lyrics", False),
        ]

        for comment, should_detect_beat in test_cases:
            result = analyzer.analyze_comment(comment)
            beat_appreciation = result["beat_appreciation"]

            assert (
                beat_appreciation == should_detect_beat
            ), f"Beat detection failed for: {comment} (expected {should_detect_beat}, got {beat_appreciation})"

        print("✅ Beat appreciation detection working correctly")

    def test_confidence_scoring(self, analyzer):
        """Test confidence scoring mechanism."""
        test_cases = [
            ("", 0.0),  # Empty comment should have 0 confidence
            ("ok", 0.0),  # Very short, no patterns
            ("this is sick and fire 🔥", 0.5),  # Multiple patterns should increase confidence
        ]

        for comment, min_confidence in test_cases:
            result = analyzer.analyze_comment(comment)
            confidence = result["confidence"]

            assert (
                confidence >= min_confidence
            ), f"Confidence too low for: {comment} (got {confidence}, expected >= {min_confidence})"

        print("✅ Confidence scoring working correctly")

    def test_batch_analysis(self, analyzer):
        """Test batch processing of comments."""
        comments = ["this is sick!", "fucking queen!", "regular comment", "fire track 🔥", "the beat is crazy"]

        results_df = analyzer.analyze_batch(comments)

        assert len(results_df) == len(comments)
        assert "sentiment_score" in results_df.columns
        assert "confidence" in results_df.columns
        assert "beat_appreciation" in results_df.columns

        # Check that positive comments got positive scores
        assert results_df.iloc[0]["sentiment_score"] > 0  # "this is sick!"
        assert results_df.iloc[1]["sentiment_score"] > 0  # "fucking queen!"
        assert results_df.iloc[3]["sentiment_score"] > 0  # "fire track 🔥"

        # Check beat appreciation detection
        assert results_df.iloc[4]["beat_appreciation"] == True  # "the beat is crazy"

        print("✅ Batch analysis working correctly")

    def test_edge_cases(self, analyzer):
        """Test edge cases and error handling."""
        edge_cases = [
            None,
            "",
            "   ",  # Whitespace only
            pd.NA,
            123,  # Non-string input
        ]

        for case in edge_cases:
            try:
                result = analyzer.analyze_comment(case)
                # Should return default values for invalid input
                assert result["sentiment_score"] == 0.0
                assert result["confidence"] == 0.0
                assert result["beat_appreciation"] == False
            except Exception as e:
                pytest.fail(f"Analyzer failed on edge case {case}: {e}")

        print("✅ Edge case handling working correctly")


class TestSentimentAccuracy:
    """Test sentiment analysis accuracy on real music industry comments."""

    def test_music_industry_context_accuracy(self):
        """Test accuracy on music industry specific contexts."""
        analyzer = MusicIndustrySentimentAnalyzer()

        # Test cases with expected sentiment (positive=True, negative=False, neutral=None)
        test_cases = [
            # Positive cases that might be misunderstood without context
            ("this is sick", True, "sick = good in music context"),
            ("this slaps", True, "slaps = good music"),
            ("absolute banger", True, "banger = good song"),
            ("fucking queen", True, "queen = compliment"),
            ("go off king", True, "encouragement"),
            ("bad bish", True, "bad = good in this context"),
            ("it's giving", True, "modern slang for approval"),
            # Engagement indicators (should be positive)
            ("I need the lyrics", True, "engagement request"),
            ("please come to my city", True, "tour request"),
            ("when is this dropping", True, "anticipation"),
            # Neutral cases
            ("when does this come out", None, "neutral question"),
            ("what's the name of this song", None, "neutral question"),
            # Negative cases (should still be negative)
            ("this is trash", False, "clearly negative"),
            ("I hate this", False, "clearly negative"),
        ]

        correct_predictions = 0
        total_predictions = 0

        for comment, expected_positive, description in test_cases:
            result = analyzer.analyze_comment(comment)
            sentiment_score = result["sentiment_score"]

            if expected_positive is True:
                predicted_positive = sentiment_score > 0.1
                correct = predicted_positive
            elif expected_positive is False:
                predicted_positive = sentiment_score < -0.1
                correct = predicted_positive
            else:  # None (neutral)
                predicted_neutral = -0.1 <= sentiment_score <= 0.1
                correct = predicted_neutral

            if correct:
                correct_predictions += 1
            else:
                print(f"❌ Incorrect: {description} - '{comment}' got {sentiment_score}")

            total_predictions += 1

        accuracy = correct_predictions / total_predictions
        print(f"✅ Music industry sentiment accuracy: {accuracy:.2%} ({correct_predictions}/{total_predictions})")

        # Require at least 80% accuracy
        assert accuracy >= 0.8, f"Sentiment accuracy too low: {accuracy:.2%}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
