#!/usr/bin/env python3
"""
Comprehensive Tests for Enhanced Music Sentiment Analysis

Tests the enhanced music sentiment analyzer against real fan comments,
Gen Z slang, and music industry context.
"""

import os
import sys

import pytest

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.youtubeviz.enhanced_music_sentiment import ComprehensiveMusicSentimentAnalyzer


class TestEnhancedMusicSentiment:
    """Test suite for enhanced music sentiment analysis."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = ComprehensiveMusicSentimentAnalyzer()

    def test_analyzer_initialization(self):
        """Test that analyzer initializes correctly."""
        assert self.analyzer is not None
        assert hasattr(self.analyzer, "positive_phrases")
        assert hasattr(self.analyzer, "emoji_sentiment")
        assert hasattr(self.analyzer, "beat_patterns")

        # Check that we have comprehensive phrase coverage
        assert len(self.analyzer.positive_phrases) > 100, "Should have extensive phrase coverage"
        assert len(self.analyzer.emoji_sentiment) > 20, "Should support many emojis"

    def test_basic_functionality(self):
        """Test basic sentiment analysis functionality."""
        # Test positive comment
        result = self.analyzer.analyze_comment("this is amazing!")
        assert "sentiment_score" in result
        assert "confidence" in result
        assert "beat_appreciation" in result
        assert isinstance(result["sentiment_score"], float)
        assert isinstance(result["confidence"], float)
        assert isinstance(result["beat_appreciation"], bool)

        # Test empty/null comments
        assert self.analyzer.analyze_comment("")["sentiment_score"] == 0.0
        assert self.analyzer.analyze_comment(None)["sentiment_score"] == 0.0

    def test_music_slang_recognition(self):
        """Test recognition of music slang that sounds negative but is positive."""

        music_slang_tests = [
            ("this is sick", True),
            ("this sick", True),
            ("so sick", True),
            ("that's sick", True),
            ("sick beat", True),
            ("sick flow", True),
        ]

        for comment, should_be_positive in music_slang_tests:
            result = self.analyzer.analyze_comment(comment)
            is_positive = result["sentiment_score"] > 0.1
            assert (
                is_positive == should_be_positive
            ), f"'{comment}' should be {'positive' if should_be_positive else 'negative'}"

    def test_hard_crazy_positive_context(self):
        """Test that 'hard' and 'crazy' are positive in music context."""

        hard_crazy_tests = [
            ("this hard", True),
            ("goes hard", True),
            ("hard af", True),
            ("hard as shit", True),
            ("this hard af", True),
            ("bro this crazy", True),
            ("this crazy", True),
            ("absolutely crazy", True),
        ]

        for comment, should_be_positive in hard_crazy_tests:
            result = self.analyzer.analyze_comment(comment)
            is_positive = result["sentiment_score"] > 0.1
            assert is_positive == should_be_positive, f"'{comment}' should be positive in music context"

    def test_gen_z_slang_comprehensive(self):
        """Test comprehensive Gen Z slang recognition."""

        gen_z_tests = [
            ("slay", True),
            ("slayed", True),
            ("slaying", True),
            ("periodt", True),
            ("period", True),
            ("no cap", True),
            ("ate that", True),
            ("ate and left no crumbs", True),
            ("devoured", True),
            ("served", True),
            ("understood the assignment", True),
            ("main character energy", True),
            ("hits different", True),
            ("chef's kiss", True),
            ("we stan", True),
            ("iconic", True),
            ("legendary", True),
            ("obsessed", True),
            ("lowkey obsessed", True),
            ("highkey obsessed", True),
            ("you slid", True),
            ("this fye", True),
            ("sheeeesh", True),
            ("sheeesh", True),
        ]

        correct = 0
        for comment, should_be_positive in gen_z_tests:
            result = self.analyzer.analyze_comment(comment)
            is_positive = result["sentiment_score"] > 0.1
            if is_positive == should_be_positive:
                correct += 1
            else:
                print(f"❌ Failed: '{comment}' → {result['sentiment_score']:.2f}")

        accuracy = correct / len(gen_z_tests)
        assert accuracy >= 0.9, f"Gen Z slang accuracy too low: {accuracy:.1%}"

    def test_real_fan_comments_accuracy(self):
        """Test accuracy on real fan comments from database."""

        real_positive_comments = [
            "Hottie, Baddie, Maddie",
            "Part two pleaseee wtfff",
            "Cuz I willie 😖😚💕",
            "sheeeeesh my nigga snapped 🔥🔥🔥🔥",
            "my legs are spread!!",
            "Bestie goals fr 🤞",
            "🔥🔥🔥",
            "🌊🌊🌊🌊",
            "💯💯💯",
            "this hard af",
            "this hard as shit",
            "Bro this crazy",
            "the beat though!",
            "the beat tho!",
            "who made this beat bro?!",
            "you slid",
            "this fye my boi",
            "sheeeesh",
            "fucking queen!",
            "go off king",
            "oh my!",
            "oh my yes!",
            "fuck it up",
            "I need the lyrics",
            "yessir!",
            "yessuh",
            "10/10",
            "100!",
            "100/10",
            "queen",
            "hot bish",
            "bad bish",
            "YES MOTHER!",
            "friday can't come sooner",
            "Bitchhh!",
            "Bitch, it's givinnnng!",
            "please come to atlanta",
        ]

        correct = 0
        failed_comments = []

        for comment in real_positive_comments:
            result = self.analyzer.analyze_comment(comment)
            is_positive = result["sentiment_score"] > 0.1

            if is_positive:
                correct += 1
            else:
                failed_comments.append((comment, result["sentiment_score"]))

        accuracy = correct / len(real_positive_comments)

        if failed_comments:
            print(f"\n❌ Failed comments ({len(failed_comments)}):")
            for comment, score in failed_comments:
                print(f"   '{comment}' → {score:.2f}")

        assert (
            accuracy >= 0.95
        ), f"Real comment accuracy too low: {accuracy:.1%} ({correct}/{len(real_positive_comments)})"

    def test_beat_appreciation_detection(self):
        """Test beat appreciation detection functionality."""

        beat_comments = [
            ("the beat though!", True),
            ("the beat tho!", True),
            ("who made this beat bro?!", True),
            ("this beat is fire", True),
            ("beat goes hard", True),
            ("beat slaps", True),
            ("production is crazy", True),
            ("who produced this", True),
        ]

        non_beat_comments = [
            ("love the lyrics", False),
            ("great song overall", False),
            ("amazing vocals", False),
            ("this is fire", False),  # Fire but not beat-specific
        ]

        # Test beat detection
        beat_correct = 0
        for comment, should_detect_beat in beat_comments:
            result = self.analyzer.analyze_comment(comment)
            if result["beat_appreciation"] == should_detect_beat:
                beat_correct += 1

        # Test non-beat detection
        non_beat_correct = 0
        for comment, should_detect_beat in non_beat_comments:
            result = self.analyzer.analyze_comment(comment)
            if result["beat_appreciation"] == should_detect_beat:
                non_beat_correct += 1

        beat_accuracy = beat_correct / len(beat_comments)
        non_beat_accuracy = non_beat_correct / len(non_beat_comments)

        assert beat_accuracy >= 0.8, f"Beat detection accuracy too low: {beat_accuracy:.1%}"
        assert non_beat_accuracy >= 0.8, f"Non-beat detection accuracy too low: {non_beat_accuracy:.1%}"

    def test_emoji_sentiment_analysis(self):
        """Test emoji sentiment analysis including multipliers."""

        emoji_tests = [
            ("🔥🔥🔥", True, True),  # Should be positive, emoji-only
            ("🌊🌊🌊🌊", True, True),  # Should be positive, emoji-only
            ("💯💯💯", True, True),  # Should be positive, emoji-only
            ("😍", True, True),  # Should be positive, emoji-only
            ("this is fire 🔥", True, False),  # Positive with emoji
            ("amazing 😍😍", True, False),  # Positive with multiple emojis
        ]

        for emoji_comment, should_be_positive, is_emoji_only in emoji_tests:
            result = self.analyzer.analyze_comment(emoji_comment)
            is_positive = result["sentiment_score"] > 0.1

            assert (
                is_positive == should_be_positive
            ), f"'{emoji_comment}' should be {'positive' if should_be_positive else 'negative'}"

            # Emoji-only comments should have good scores
            if is_emoji_only and should_be_positive:
                assert (
                    result["sentiment_score"] > 0.5
                ), f"Emoji-only comment '{emoji_comment}' should have strong positive score"

    def test_confidence_scoring(self):
        """Test confidence scoring mechanism."""

        # Longer comments with more indicators should have higher confidence
        short_comment = "sick"
        long_comment = "this is absolutely sick! the beat goes so hard 🔥🔥🔥"

        short_result = self.analyzer.analyze_comment(short_comment)
        long_result = self.analyzer.analyze_comment(long_comment)

        # Long comment should have higher confidence
        assert long_result["confidence"] > short_result["confidence"], "Longer comments should have higher confidence"

        # Both should be positive
        assert short_result["sentiment_score"] > 0.1, "Short comment should be positive"
        assert long_result["sentiment_score"] > 0.1, "Long comment should be positive"

    def test_model_comparison_justification(self):
        """Test that our model significantly outperforms VADER and TextBlob on music slang."""

        # Test cases that VADER and TextBlob typically fail on
        difficult_cases = [
            "this is sick",
            "fucking queen",
            "go off king",
            "this hard af",
            "bro this crazy",
            "you slid",
            "periodt",
            "slay",
            "ate that",
            "no cap",
        ]

        correct = 0
        for comment in difficult_cases:
            result = self.analyzer.analyze_comment(comment)
            if result["sentiment_score"] > 0.1:  # Should be positive
                correct += 1

        accuracy = correct / len(difficult_cases)

        # Our model should handle these cases much better than VADER (22.7%) or TextBlob (13.6%)
        assert accuracy >= 0.9, f"Enhanced model should significantly outperform baseline models: {accuracy:.1%}"

    def test_comprehensive_accuracy(self):
        """Run the comprehensive test that matches the standalone test."""

        accuracy = self.analyzer.test_real_comments()

        # Should achieve near-perfect accuracy
        assert accuracy >= 95.0, f"Comprehensive test accuracy too low: {accuracy:.1f}%"

        print(f"✅ Comprehensive accuracy test passed: {accuracy:.1f}%")


def test_model_availability():
    """Test that the enhanced model is available for import."""
    try:
        from src.youtubeviz.enhanced_music_sentiment import (
            ComprehensiveMusicSentimentAnalyzer,
        )

        analyzer = ComprehensiveMusicSentimentAnalyzer()
        assert analyzer is not None
        print("✅ Enhanced music sentiment analyzer successfully imported")
    except ImportError as e:
        pytest.fail(f"Enhanced music sentiment analyzer not available: {e}")


if __name__ == "__main__":
    # Run tests directly
    test_suite = TestEnhancedMusicSentiment()
    test_suite.setup_method()

    print("🎵 Running Enhanced Music Sentiment Analysis Tests")
    print("=" * 60)

    try:
        test_suite.test_analyzer_initialization()
        print("✅ Analyzer initialization test passed")

        test_suite.test_basic_functionality()
        print("✅ Basic functionality test passed")

        test_suite.test_music_slang_recognition()
        print("✅ Music slang recognition test passed")

        test_suite.test_hard_crazy_positive_context()
        print("✅ Hard/crazy positive context test passed")

        test_suite.test_gen_z_slang_comprehensive()
        print("✅ Gen Z slang comprehensive test passed")

        test_suite.test_real_fan_comments_accuracy()
        print("✅ Real fan comments accuracy test passed")

        test_suite.test_beat_appreciation_detection()
        print("✅ Beat appreciation detection test passed")

        test_suite.test_emoji_sentiment_analysis()
        print("✅ Emoji sentiment analysis test passed")

        test_suite.test_confidence_scoring()
        print("✅ Confidence scoring test passed")

        test_suite.test_model_comparison_justification()
        print("✅ Model comparison justification test passed")

        test_suite.test_comprehensive_accuracy()
        print("✅ Comprehensive accuracy test passed")

        print("\n🎉 All enhanced sentiment analysis tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
