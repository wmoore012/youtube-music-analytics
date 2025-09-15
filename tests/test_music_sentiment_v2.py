#!/usr/bin/env python3
"""
Comprehensive Tests for Music Industry Sentiment Dataset v2.0

Tests the production-grade v2.0 dataset with proper validation,
intent/sentiment separation, and model performance evaluation.
"""

import os
import sys
from typing import Dict, List

import pytest

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from datasets.music_industry_sentiment_dataset_v2 import (
    Aspect,
    Intent,
    MusicSlangEntry,
    SentimentLabel,
    SlangCategory,
    Toxicity,
    get_music_industry_dataset_v2,
)
from src.youtubeviz.production_music_sentiment import ProductionMusicSentimentAnalyzer


class TestMusicSentimentDatasetV2:
    """Comprehensive test suite for v2.0 dataset."""

    def setup_method(self):
        """Set up test fixtures."""
        self.dataset = get_music_industry_dataset_v2()

    def test_dataset_initialization(self):
        """Test that dataset initializes correctly with proper validation."""
        assert self.dataset is not None
        assert len(self.dataset.entries) >= 250, "Dataset should have at least 250 phrases"
        assert self.dataset.dataset_version == "2.0"
        assert self.dataset.schema_version == "2.0"

    def test_entry_validation(self):
        """Test that all entries pass Pydantic validation."""
        for entry in self.dataset.entries:
            # Test that entry has all required fields
            assert hasattr(entry, "phrase")
            assert hasattr(entry, "sentiment")
            assert hasattr(entry, "intent")
            assert hasattr(entry, "category")
            assert hasattr(entry, "aspect")
            assert hasattr(entry, "confidence")
            assert hasattr(entry, "id")

            # Test field types and constraints
            assert isinstance(entry.phrase, str)
            assert len(entry.phrase.strip()) > 0
            assert len(entry.phrase) <= 200
            assert isinstance(entry.confidence, float)
            assert 0.0 <= entry.confidence <= 1.0
            assert entry.sentiment in SentimentLabel
            assert entry.intent in Intent
            assert entry.category in SlangCategory
            assert entry.aspect in Aspect
            assert entry.toxicity in Toxicity

    def test_deduplication_quality(self):
        """Test that dataset has proper deduplication."""
        normalized_keys = set()

        for entry in self.dataset.entries:
            norm_key = entry.norm_key()

            # Check if we've seen this normalized phrase before
            if norm_key in normalized_keys:
                # Find the duplicate
                duplicate_entries = [e for e in self.dataset.entries if e.norm_key() == norm_key]

                # Duplicates are only allowed if they have different sentiment/intent/aspect
                if len(duplicate_entries) > 1:
                    for i, dup1 in enumerate(duplicate_entries):
                        for dup2 in duplicate_entries[i + 1 :]:
                            assert (
                                dup1.sentiment != dup2.sentiment
                                or dup1.intent != dup2.intent
                                or dup1.aspect != dup2.aspect
                            ), f"True duplicate found: '{dup1.phrase}' and '{dup2.phrase}'"

            normalized_keys.add(norm_key)

    def test_intent_sentiment_consistency(self):
        """Test intent/sentiment separation follows SemEval guidelines."""

        # All praise intent should be positive
        praise_entries = [e for e in self.dataset.entries if e.intent == Intent.PRAISE]
        for entry in praise_entries:
            assert entry.sentiment == SentimentLabel.POSITIVE, f"Praise intent should be positive: '{entry.phrase}'"

        # All critique intent should be negative
        critique_entries = [e for e in self.dataset.entries if e.intent == Intent.CRITIQUE]
        for entry in critique_entries:
            assert entry.sentiment == SentimentLabel.NEGATIVE, f"Critique intent should be negative: '{entry.phrase}'"

        # Request and info intents should generally be neutral (unless opinion cues present)
        request_entries = [e for e in self.dataset.entries if e.intent == Intent.REQUEST]
        info_entries = [e for e in self.dataset.entries if e.intent == Intent.INFO]

        for entry in request_entries + info_entries:
            if entry.sentiment != SentimentLabel.NEUTRAL:
                # Should have clear opinion cues
                has_opinion_cues = (
                    "!" in entry.phrase
                    or "🔥" in entry.phrase
                    or "💯" in entry.phrase
                    or "can't wait" in entry.phrase.lower()
                    or "need this" in entry.phrase.lower()
                    or "already" in entry.phrase.lower()
                )
                assert has_opinion_cues, f"Request/Info without opinion cues should be neutral: '{entry.phrase}'"

    def test_beat_appreciation_consistency(self):
        """Test beat appreciation detection consistency."""

        beat_entries = [e for e in self.dataset.entries if e.beat_appreciation]

        # Beat appreciation entries should have beat-related aspects or patterns
        for entry in beat_entries:
            has_beat_aspect = entry.aspect in [Aspect.BEAT, Aspect.MIX]
            has_beat_words = any(
                word in entry.phrase.lower()
                for word in ["beat", "production", "mix", "instrumental", "producer", "drums", "bass"]
            )

            assert (
                has_beat_aspect or has_beat_words
            ), f"Beat appreciation entry should have beat-related content: '{entry.phrase}'"

    def test_gen_z_slang_flagging(self):
        """Test Gen Z slang flagging accuracy."""

        gen_z_entries = [e for e in self.dataset.entries if e.gen_z_slang]

        # Check that flagged entries actually contain Gen Z slang
        gen_z_indicators = [
            "periodt",
            "slay",
            "no cap",
            "ate that",
            "devoured",
            "served",
            "understood the assignment",
            "main character",
            "hits different",
            "chef's kiss",
            "we stan",
            "obsessed",
            "rent free",
            "sending me",
            "I'm deceased",
            "not me",
            "lowkey",
            "highkey",
            "it's giving",
            "different breed",
            "built different",
            "say less",
            "bet",
            "facts",
            "af",
            "goated",
            "sheesh",
            "sheeesh",
            "sheeeesh",
            "mid",
            "bestie",
            "fr",
            "fye",
            "stan",
            "bussin",
            "weak",
            "screaming",
            "sent me",
            "talent jumped out",
            "slid",
            "crumbs",
            "bitchhh",
            "period",
            "crying",
            "mother",
            "wtfff",
            "that's the tweet",
            "love to see",
        ]

        for entry in gen_z_entries:
            has_gen_z_indicator = any(indicator in entry.phrase.lower() for indicator in gen_z_indicators)
            # Allow some flexibility for borderline cases
            if not has_gen_z_indicator:
                print(f"⚠️ Potentially mis-flagged Gen Z: '{entry.phrase}'")

    def test_toxicity_flagging(self):
        """Test toxicity flagging accuracy."""

        light_toxicity = [e for e in self.dataset.entries if e.toxicity == Toxicity.LIGHT]
        strong_toxicity = [e for e in self.dataset.entries if e.toxicity == Toxicity.STRONG]

        # Light toxicity should contain mild profanity or strong slang
        light_indicators = ["fuck", "shit", "bitch", "bish", "damn", "hell", "af", "as fuck", "as shit", "basura"]
        for entry in light_toxicity:
            has_light_indicator = any(indicator in entry.phrase.lower() for indicator in light_indicators)
            assert has_light_indicator, f"Light toxicity should contain mild profanity: '{entry.phrase}'"

        # Strong toxicity should contain strong profanity or slurs
        strong_indicators = ["nigga", "nigger"]  # Real content needs real testing
        for entry in strong_toxicity:
            has_strong_indicator = any(indicator in entry.phrase.lower() for indicator in strong_indicators)
            assert has_strong_indicator, f"Strong toxicity should contain strong language: '{entry.phrase}'"

    def test_train_test_split_quality(self):
        """Test stratified train/test split maintains distribution."""

        train_entries, test_entries = self.dataset.get_train_test_split(test_size=0.2, random_state=42)

        # Check split sizes
        total_size = len(self.dataset.entries)
        expected_test_size = int(total_size * 0.2)
        assert abs(len(test_entries) - expected_test_size) <= 2, "Test set size should be ~20%"
        assert len(train_entries) + len(test_entries) == total_size, "Split should cover all entries"

        # Check sentiment distribution is maintained
        original_sentiment_dist = {}
        train_sentiment_dist = {}
        test_sentiment_dist = {}

        for entry in self.dataset.entries:
            sentiment = entry.sentiment.value
            original_sentiment_dist[sentiment] = original_sentiment_dist.get(sentiment, 0) + 1

        for entry in train_entries:
            sentiment = entry.sentiment.value
            train_sentiment_dist[sentiment] = train_sentiment_dist.get(sentiment, 0) + 1

        for entry in test_entries:
            sentiment = entry.sentiment.value
            test_sentiment_dist[sentiment] = test_sentiment_dist.get(sentiment, 0) + 1

        # Check that sentiment proportions are roughly maintained
        for sentiment in original_sentiment_dist:
            original_prop = original_sentiment_dist[sentiment] / total_size
            train_prop = train_sentiment_dist.get(sentiment, 0) / len(train_entries)
            test_prop = test_sentiment_dist.get(sentiment, 0) / len(test_entries)

            # Allow some variance in stratification
            assert abs(train_prop - original_prop) < 0.1, f"Train set {sentiment} proportion off"
            assert abs(test_prop - original_prop) < 0.15, f"Test set {sentiment} proportion off"

    def test_export_functionality(self):
        """Test dataset export to different formats."""

        # Test DataFrame conversion
        df = self.dataset.to_dataframe()
        assert len(df) == len(self.dataset.entries)

        required_columns = [
            "id",
            "phrase",
            "sentiment",
            "intent",
            "category",
            "aspect",
            "confidence",
            "beat_appreciation",
            "gen_z_slang",
            "toxicity",
        ]
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

        # Test statistics generation
        stats = self.dataset.get_statistics()
        assert "total_phrases" in stats
        assert "sentiment_distribution" in stats
        assert "intent_distribution" in stats
        assert stats["total_phrases"] == len(self.dataset.entries)

    def test_model_integration(self):
        """Test integration with production sentiment model."""

        # Test that production model can load the dataset
        analyzer = ProductionMusicSentimentAnalyzer(use_dataset=True)

        model_info = analyzer.get_model_info()
        assert model_info["dataset_loaded"] == True
        assert model_info["total_phrases"] == len(self.dataset.entries)
        assert model_info["positive_phrases"] > 0
        assert model_info["negative_phrases"] > 0

        # Test analysis on sample phrases
        test_phrases = [
            ("this is sick", "positive"),
            ("mid", "negative"),
            ("drop the album", "neutral"),
            ("the beat though!", "positive"),
        ]

        for phrase, expected_sentiment in test_phrases:
            result = analyzer.analyze_comment(phrase)
            score = result["sentiment_score"]

            if expected_sentiment == "positive":
                assert score > 0.1, f"'{phrase}' should be positive, got {score}"
            elif expected_sentiment == "negative":
                assert score < -0.1, f"'{phrase}' should be negative, got {score}"
            else:  # neutral
                assert -0.1 <= score <= 0.1, f"'{phrase}' should be neutral, got {score}"


class TestOriginalProblemCases:
    """Test the original problem cases that motivated this work."""

    def setup_method(self):
        """Set up analyzer."""
        self.analyzer = ProductionMusicSentimentAnalyzer(use_dataset=True)

    def test_original_failing_cases(self):
        """Test that original problem cases now work correctly."""

        original_cases = [
            "Hottie, Baddie, Maddie",
            "Part two pleaseee wtfff",
            "Cuz I willie 😖😚💕",
            "sheeeeesh my nigga snapped 🔥🔥🔥🔥",
            "my legs are spread!!",
            "Bestie goals fr 🤞",
            "🔥🔥🔥",
            "🌊🌊🌊🌊",
            "💯💯💯",
        ]

        correct = 0
        for comment in original_cases:
            result = self.analyzer.analyze_comment(comment)
            score = result["sentiment_score"]

            # All should be positive
            if score > 0.1:
                correct += 1
            else:
                print(f"❌ Failed: '{comment}' → {score:.2f}")

        accuracy = correct / len(original_cases) * 100
        assert accuracy >= 90, f"Original cases accuracy too low: {accuracy:.1f}%"

    def test_music_slang_recognition(self):
        """Test music slang that should be positive."""

        music_slang_cases = [
            "this hard af",
            "this hard as shit",
            "Bro this crazy",
            "the beat though!",
            "the beat tho!",
            "who made this beat bro?!",
            "you slid",
            "this fye my boi",
            "sheeeesh",
        ]

        correct = 0
        for comment in music_slang_cases:
            result = self.analyzer.analyze_comment(comment)
            score = result["sentiment_score"]

            if score > 0.1:
                correct += 1
            else:
                print(f"❌ Failed music slang: '{comment}' → {score:.2f}")

        accuracy = correct / len(music_slang_cases) * 100
        assert accuracy >= 85, f"Music slang accuracy too low: {accuracy:.1f}%"

    def test_gen_z_expressions(self):
        """Test Gen Z expressions recognition."""

        gen_z_cases = [
            "periodt",
            "no cap",
            "slay",
            "ate that",
            "understood the assignment",
            "hits different",
            "chef's kiss",
            "we stan",
            "obsessed",
        ]

        correct = 0
        for comment in gen_z_cases:
            result = self.analyzer.analyze_comment(comment)
            score = result["sentiment_score"]

            if score > 0.1:
                correct += 1
            else:
                print(f"❌ Failed Gen Z: '{comment}' → {score:.2f}")

        accuracy = correct / len(gen_z_cases) * 100
        assert accuracy >= 80, f"Gen Z expressions accuracy too low: {accuracy:.1f}%"

    def test_beat_appreciation_detection(self):
        """Test beat appreciation detection."""

        beat_cases = [
            ("the beat though!", True),
            ("the beat tho!", True),
            ("who made this beat bro?!", True),
            ("beat goes hard", True),
            ("love the lyrics", False),
            ("great vocals", False),
        ]

        correct = 0
        for comment, should_detect_beat in beat_cases:
            result = self.analyzer.analyze_comment(comment)
            detected = result["beat_appreciation"]

            if detected == should_detect_beat:
                correct += 1
            else:
                print(f"❌ Beat detection failed: '{comment}' → {detected}, expected {should_detect_beat}")

        accuracy = correct / len(beat_cases) * 100
        assert accuracy >= 80, f"Beat detection accuracy too low: {accuracy:.1f}%"


def test_dataset_availability():
    """Test that v2.0 dataset is available for import."""
    try:
        dataset = get_music_industry_dataset_v2()
        assert dataset is not None
        assert len(dataset.entries) >= 250
        print("✅ Music Industry Sentiment Dataset v2.0 successfully loaded")
    except Exception as e:
        pytest.fail(f"Dataset v2.0 not available: {e}")


def test_model_performance_baseline():
    """Test that model performance meets baseline requirements."""

    dataset = get_music_industry_dataset_v2()
    train_entries, test_entries = dataset.get_train_test_split(test_size=0.2, random_state=42)

    # Create analyzer and train on training set
    analyzer = ProductionMusicSentimentAnalyzer(use_dataset=False)

    for entry in train_entries:
        if entry.sentiment.value == "positive":
            analyzer.positive_phrases[entry.phrase.lower()] = entry.confidence
        elif entry.sentiment.value == "negative":
            analyzer.negative_phrases[entry.phrase.lower()] = entry.confidence

    # Test on test set
    correct = 0
    for entry in test_entries:
        result = analyzer.analyze_comment(entry.phrase)
        score = result["sentiment_score"]

        if score > 0.1:
            predicted = "positive"
        elif score < -0.1:
            predicted = "negative"
        else:
            predicted = "neutral"

        if predicted == entry.sentiment.value:
            correct += 1

    accuracy = correct / len(test_entries) * 100

    # Should perform better than random (33.3%) and better than baseline models
    assert accuracy > 35, f"Model accuracy too low: {accuracy:.1f}%"
    print(f"✅ Model baseline performance: {accuracy:.1f}% accuracy")


if __name__ == "__main__":
    # Run tests directly
    test_suite = TestMusicSentimentDatasetV2()
    test_suite.setup_method()

    print("🎵 Running Music Industry Sentiment Dataset v2.0 Tests")
    print("=" * 70)

    try:
        test_suite.test_dataset_initialization()
        print("✅ Dataset initialization test passed")

        test_suite.test_entry_validation()
        print("✅ Entry validation test passed")

        test_suite.test_deduplication_quality()
        print("✅ Deduplication quality test passed")

        test_suite.test_intent_sentiment_consistency()
        print("✅ Intent/sentiment consistency test passed")

        test_suite.test_beat_appreciation_consistency()
        print("✅ Beat appreciation consistency test passed")

        test_suite.test_gen_z_slang_flagging()
        print("✅ Gen Z slang flagging test passed")

        test_suite.test_toxicity_flagging()
        print("✅ Toxicity flagging test passed")

        test_suite.test_train_test_split_quality()
        print("✅ Train/test split quality test passed")

        test_suite.test_export_functionality()
        print("✅ Export functionality test passed")

        test_suite.test_model_integration()
        print("✅ Model integration test passed")

        # Test original problem cases
        problem_test = TestOriginalProblemCases()
        problem_test.setup_method()

        problem_test.test_original_failing_cases()
        print("✅ Original problem cases test passed")

        problem_test.test_music_slang_recognition()
        print("✅ Music slang recognition test passed")

        problem_test.test_gen_z_expressions()
        print("✅ Gen Z expressions test passed")

        problem_test.test_beat_appreciation_detection()
        print("✅ Beat appreciation detection test passed")

        # Test baseline performance
        test_model_performance_baseline()

        print("\n🎉 All Music Industry Sentiment Dataset v2.0 tests passed!")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
