#!/usr/bin/env python3
"""
Production Music Industry Sentiment Analyzer

A clean, production-ready sentiment analyzer that uses the centralized
music industry sentiment dataset for training and evaluation.

This analyzer is designed for:
- Production deployment in music analytics systems
- Consistent, reproducible sentiment analysis
- Easy model updates via dataset changes
- Comprehensive music industry language understanding

Key Features:
- Uses centralized dataset (no hardcoded phrases)
- Handles Gen Z slang and music industry terminology
- Beat appreciation detection
- Emoji sentiment analysis with multipliers
- Confidence scoring
- Extensible architecture for model improvements
"""

import os
import re
import sys
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add datasets path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from datasets.music_industry_sentiment_dataset_v2 import (
    SentimentLabel,
    get_music_industry_dataset_v2,
)


class ProductionMusicSentimentAnalyzer:
    """
    Production-ready music industry sentiment analyzer.

    Uses the centralized music industry sentiment dataset for consistent,
    reproducible sentiment analysis optimized for music industry language.
    """

    def __init__(self, use_dataset: bool = True):
        """
        Initialize the production sentiment analyzer.

        Args:
            use_dataset: Whether to load phrases from centralized dataset
        """
        self.dataset = get_music_industry_dataset_v2() if use_dataset else None
        self.positive_phrases = {}
        self.negative_phrases = {}
        self.beat_patterns = []

        if use_dataset:
            self._load_phrases_from_dataset()

        # Emoji sentiment mapping
        self.emoji_sentiment = {
            "😍": 0.8,
            "🔥": 0.7,
            "💯": 0.8,
            "🌊": 0.6,
            "👑": 0.7,
            "💖": 0.8,
            "❤️": 0.7,
            "🎵": 0.6,
            "🎶": 0.6,
            "🙌": 0.7,
            "👏": 0.6,
            "😖": 0.5,
            "😚": 0.7,
            "💕": 0.8,
            "🤞": 0.6,
            "🎤": 0.6,
            "🎧": 0.6,
            "🔊": 0.6,
            "💃": 0.7,
            "🕺": 0.7,
            "🎉": 0.8,
            "✨": 0.7,
            "💫": 0.7,
            "🤩": 0.8,
            "😭": 0.6,
            "🥺": 0.6,
        }

    def _load_phrases_from_dataset(self):
        """Load sentiment phrases from the centralized dataset."""
        if not self.dataset:
            return

        # Load positive phrases
        for entry in self.dataset.entries:
            if entry.sentiment == SentimentLabel.POSITIVE:
                self.positive_phrases[entry.phrase.lower()] = entry.confidence
            elif entry.sentiment == SentimentLabel.NEGATIVE:
                self.negative_phrases[entry.phrase.lower()] = entry.confidence

            # Collect beat appreciation patterns
            if entry.beat_appreciation:
                # Extract key words for beat pattern matching
                words = entry.phrase.lower().split()
                for word in ["beat", "production", "mix", "instrumental", "producer"]:
                    if word in words:
                        pattern = rf"\b{word}\b"
                        if pattern not in self.beat_patterns:
                            self.beat_patterns.append(pattern)

        # Add additional beat patterns
        self.beat_patterns.extend([r"\bwho made\b", r"\bwho produced\b", r"\bcar test\b", r"\bdrums?\b", r"\bbass\b"])

    def analyze_comment(self, comment_text: str) -> Dict[str, float]:
        """
        Analyze sentiment of a music industry comment.

        Args:
            comment_text: The comment to analyze

        Returns:
            Dict with sentiment_score, confidence, beat_appreciation
        """
        if not comment_text or pd.isna(comment_text):
            return {"sentiment_score": 0.0, "confidence": 0.0, "beat_appreciation": False}

        comment_lower = comment_text.lower().strip()
        original_comment = comment_text.strip()

        # Initialize scores
        sentiment_score = 0.0
        phrase_matches = 0

        # Check for positive phrases
        for phrase, confidence in self.positive_phrases.items():
            if phrase in comment_lower:
                sentiment_score += confidence
                phrase_matches += 1

        # Check for negative phrases
        for phrase, confidence in self.negative_phrases.items():
            if phrase in comment_lower:
                sentiment_score -= confidence  # Subtract for negative
                phrase_matches += 1

        # Check emoji sentiment
        emoji_score = 0.0
        emoji_count = 0

        for emoji, score in self.emoji_sentiment.items():
            count = original_comment.count(emoji)
            if count > 0:
                emoji_count += count
                # Multiple same emojis = more positive
                emoji_score += score * min(count, 3)  # Cap at 3x multiplier

        # Calculate total indicators for confidence
        total_indicators = phrase_matches + min(emoji_count, 3)

        # Check if this is an emoji-only comment
        is_emoji_only = len(original_comment.strip()) <= 10 and emoji_count > 0 and phrase_matches == 0

        if is_emoji_only:
            # Special scoring for emoji-only comments
            if "🔥" in original_comment:
                sentiment_score = 0.8
            elif "🌊" in original_comment:
                wave_count = original_comment.count("🌊")
                sentiment_score = 0.5 + (wave_count * 0.1)
            elif "💯" in original_comment:
                sentiment_score = 0.9
            elif "😍" in original_comment:
                sentiment_score = 0.8
            else:
                sentiment_score = emoji_score / emoji_count if emoji_count > 0 else 0
        else:
            # Combine phrase and emoji scores for mixed content
            if total_indicators > 0:
                if phrase_matches > 0 and emoji_count > 0:
                    # Both phrases and emojis - weighted average
                    phrase_weight = phrase_matches / (phrase_matches + emoji_count)
                    emoji_weight = 1 - phrase_weight

                    avg_phrase_score = sentiment_score / phrase_matches if phrase_matches > 0 else 0
                    avg_emoji_score = emoji_score / emoji_count if emoji_count > 0 else 0

                    sentiment_score = (avg_phrase_score * phrase_weight) + (avg_emoji_score * emoji_weight)

                elif phrase_matches > 0:
                    # Only phrases
                    sentiment_score = sentiment_score / phrase_matches

                elif emoji_count > 0:
                    # Only emojis (but not emoji-only due to length)
                    sentiment_score = emoji_score / emoji_count

        # Normalize to [-1, 1] range
        sentiment_score = max(-1.0, min(1.0, sentiment_score))

        # Calculate confidence based on indicators and comment length
        confidence = min(1.0, total_indicators * 0.3 + len(comment_text) * 0.002)

        # Detect beat appreciation
        beat_appreciation = any(re.search(pattern, comment_lower) for pattern in self.beat_patterns)

        return {
            "sentiment_score": round(sentiment_score, 3),
            "confidence": round(confidence, 3),
            "beat_appreciation": beat_appreciation,
        }

    def analyze_batch(self, comments: List[str]) -> pd.DataFrame:
        """Analyze a batch of comments and return results as DataFrame."""
        results = []

        for comment in comments:
            analysis = self.analyze_comment(comment)
            analysis["comment"] = comment
            results.append(analysis)

        return pd.DataFrame(results)

    def get_model_info(self) -> Dict:
        """Get information about the loaded model."""
        if not self.dataset:
            return {
                "dataset_loaded": False,
                "total_phrases": 0,
                "positive_phrases": 0,
                "negative_phrases": 0,
                "beat_patterns": len(self.beat_patterns),
                "emoji_count": len(self.emoji_sentiment),
            }

        return {
            "dataset_loaded": True,
            "dataset_version": self.dataset.dataset_version,
            "total_phrases": len(self.dataset.entries),
            "positive_phrases": len(self.positive_phrases),
            "negative_phrases": len(self.negative_phrases),
            "beat_patterns": len(self.beat_patterns),
            "emoji_count": len(self.emoji_sentiment),
            "dataset_statistics": self.dataset.get_statistics(),
        }

    def test_on_dataset(self, test_size: float = 0.2, random_state: int = 42) -> Dict:
        """
        Test the analyzer on a random split of the dataset.

        Args:
            test_size: Proportion for test set
            random_state: Random seed for reproducibility

        Returns:
            Dict with test results
        """
        if not self.dataset:
            return {"error": "No dataset loaded"}

        # Get train/test split
        train_entries, test_entries = self.dataset.get_train_test_split(test_size, random_state)

        # Test on test set
        correct = 0
        total = len(test_entries)
        failed_cases = []

        for entry in test_entries:
            result = self.analyze_comment(entry.phrase)
            predicted_score = result["sentiment_score"]

            # Convert to sentiment labels
            if predicted_score > 0.1:
                predicted = "positive"
            elif predicted_score < -0.1:
                predicted = "negative"
            else:
                predicted = "neutral"

            expected = entry.sentiment.value

            if predicted == expected:
                correct += 1
            else:
                failed_cases.append(
                    {
                        "phrase": entry.phrase,
                        "expected": expected,
                        "predicted": predicted,
                        "score": predicted_score,
                        "category": entry.category.value,
                    }
                )

        accuracy = correct / total * 100 if total > 0 else 0

        return {
            "total_test_cases": total,
            "correct_predictions": correct,
            "accuracy": accuracy,
            "failed_cases": failed_cases,
            "train_set_size": len(train_entries),
            "test_set_size": len(test_entries),
        }


def get_production_analyzer() -> ProductionMusicSentimentAnalyzer:
    """Get a production-ready music sentiment analyzer."""
    return ProductionMusicSentimentAnalyzer()


if __name__ == "__main__":
    # Demonstrate production analyzer
    analyzer = get_production_analyzer()

    # Show model info
    info = analyzer.get_model_info()
    print("🎵 Production Music Sentiment Analyzer")
    print("=" * 50)
    print(f"Dataset loaded: {info['dataset_loaded']}")
    print(f"Total phrases: {info['total_phrases']}")
    print(f"Positive phrases: {info['positive_phrases']}")
    print(f"Negative phrases: {info['negative_phrases']}")
    print(f"Beat patterns: {info['beat_patterns']}")
    print(f"Emoji mappings: {info['emoji_count']}")

    # Test on dataset
    print(f"\n🧪 Testing on Dataset:")
    test_results = analyzer.test_on_dataset()
    print(f"Test accuracy: {test_results['accuracy']:.1f}%")
    print(f"Correct: {test_results['correct_predictions']}/{test_results['total_test_cases']}")

    # Test some examples
    print(f"\n📝 Example Analysis:")
    examples = ["this is sick!", "mid", "the beat though!", "🔥🔥🔥"]

    for example in examples:
        result = analyzer.analyze_comment(example)
        sentiment = (
            "positive"
            if result["sentiment_score"] > 0.1
            else "negative" if result["sentiment_score"] < -0.1 else "neutral"
        )
        beat_emoji = "🎵" if result["beat_appreciation"] else "⚪"
        print(f"  '{example}' → {sentiment} ({result['sentiment_score']:+.2f}) {beat_emoji}")
