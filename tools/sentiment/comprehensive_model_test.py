#!/usr/bin/env python3
"""
Comprehensive Music Sentiment Model Testing Framework

This module tests sentiment models against a scientifically classified
music slang dictionary to determine the best model for music industry analytics.
"""

import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from tools.sentiment.music_slang_dictionary import (
    SentimentLabel,
    SlangCategory,
    get_music_slang_dictionary,
)


@dataclass
class ModelResult:
    """Result from testing a sentiment model."""

    model_name: str
    total_tests: int
    correct_predictions: int
    accuracy: float
    category_results: Dict[str, Dict[str, int]]
    failed_cases: List[Tuple[str, str, str]]  # (phrase, expected, predicted)
    available: bool = True
    error_message: str = ""


class SentimentModelTester:
    """Framework for testing sentiment models against music slang."""

    def __init__(self):
        """Initialize with music slang dictionary."""
        self.dictionary = get_music_slang_dictionary()
        self.test_cases = self.dictionary.get_test_cases()

    def test_vader_model(self) -> ModelResult:
        """Test VADER sentiment model."""
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

            vader = SentimentIntensityAnalyzer()

            correct = 0
            failed_cases = []
            category_results = {}

            for phrase, expected_sentiment, category, confidence in self.test_cases:
                # Get VADER prediction
                scores = vader.polarity_scores(phrase)
                compound = scores["compound"]

                # Convert to our labels
                if compound >= 0.05:
                    predicted = "positive"
                elif compound <= -0.05:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                # Track results
                is_correct = predicted == expected_sentiment
                if is_correct:
                    correct += 1
                else:
                    failed_cases.append((phrase, expected_sentiment, predicted))

                # Category tracking
                if category not in category_results:
                    category_results[category] = {"total": 0, "correct": 0}
                category_results[category]["total"] += 1
                if is_correct:
                    category_results[category]["correct"] += 1

            accuracy = correct / len(self.test_cases) * 100

            return ModelResult(
                model_name="VADER",
                total_tests=len(self.test_cases),
                correct_predictions=correct,
                accuracy=accuracy,
                category_results=category_results,
                failed_cases=failed_cases,
            )

        except ImportError as e:
            return ModelResult(
                model_name="VADER",
                total_tests=0,
                correct_predictions=0,
                accuracy=0.0,
                category_results={},
                failed_cases=[],
                available=False,
                error_message=f"VADER not available: {e}",
            )

    def test_textblob_model(self) -> ModelResult:
        """Test TextBlob sentiment model."""
        try:
            from textblob import TextBlob

            correct = 0
            failed_cases = []
            category_results = {}

            for phrase, expected_sentiment, category, confidence in self.test_cases:
                # Get TextBlob prediction
                blob = TextBlob(phrase)
                polarity = blob.sentiment.polarity

                # Convert to our labels
                if polarity > 0.1:
                    predicted = "positive"
                elif polarity < -0.1:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                # Track results
                is_correct = predicted == expected_sentiment
                if is_correct:
                    correct += 1
                else:
                    failed_cases.append((phrase, expected_sentiment, predicted))

                # Category tracking
                if category not in category_results:
                    category_results[category] = {"total": 0, "correct": 0}
                category_results[category]["total"] += 1
                if is_correct:
                    category_results[category]["correct"] += 1

            accuracy = correct / len(self.test_cases) * 100

            return ModelResult(
                model_name="TextBlob",
                total_tests=len(self.test_cases),
                correct_predictions=correct,
                accuracy=accuracy,
                category_results=category_results,
                failed_cases=failed_cases,
            )

        except ImportError as e:
            return ModelResult(
                model_name="TextBlob",
                total_tests=0,
                correct_predictions=0,
                accuracy=0.0,
                category_results={},
                failed_cases=[],
                available=False,
                error_message=f"TextBlob not available: {e}",
            )

    def test_enhanced_music_model(self) -> ModelResult:
        """Test our enhanced music sentiment model."""
        try:
            from src.youtubeviz.enhanced_music_sentiment import (
                ComprehensiveMusicSentimentAnalyzer,
            )

            analyzer = ComprehensiveMusicSentimentAnalyzer()
            correct = 0
            failed_cases = []
            category_results = {}

            for phrase, expected_sentiment, category, confidence in self.test_cases:
                # Get enhanced model prediction
                result = analyzer.analyze_comment(phrase)
                score = result["sentiment_score"]

                # Convert to our labels
                if score > 0.1:
                    predicted = "positive"
                elif score < -0.1:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                # Track results
                is_correct = predicted == expected_sentiment
                if is_correct:
                    correct += 1
                else:
                    failed_cases.append((phrase, expected_sentiment, predicted))

                # Category tracking
                if category not in category_results:
                    category_results[category] = {"total": 0, "correct": 0}
                category_results[category]["total"] += 1
                if is_correct:
                    category_results[category]["correct"] += 1

            accuracy = correct / len(self.test_cases) * 100

            return ModelResult(
                model_name="Enhanced Music",
                total_tests=len(self.test_cases),
                correct_predictions=correct,
                accuracy=accuracy,
                category_results=category_results,
                failed_cases=failed_cases,
            )

        except ImportError as e:
            return ModelResult(
                model_name="Enhanced Music",
                total_tests=0,
                correct_predictions=0,
                accuracy=0.0,
                category_results={},
                failed_cases=[],
                available=False,
                error_message=f"Enhanced Music model not available: {e}",
            )

    def run_comprehensive_test(self) -> List[ModelResult]:
        """Run comprehensive test on all available models."""

        print("🎵 Running Comprehensive Music Sentiment Model Test")
        print("=" * 70)
        print(f"📊 Testing against {len(self.test_cases)} classified music slang phrases")

        # Print dictionary statistics
        self.dictionary.print_statistics()

        print(f"\n🧪 Testing Models...")
        print("-" * 50)

        # Test all models
        results = [self.test_vader_model(), self.test_textblob_model(), self.test_enhanced_music_model()]

        # Print results
        for result in results:
            self._print_model_results(result)

        # Compare models
        self._print_model_comparison(results)

        # Save detailed results
        self._save_detailed_results(results)

        return results

    def _print_model_results(self, result: ModelResult):
        """Print detailed results for a model."""

        if not result.available:
            print(f"\n❌ {result.model_name}: {result.error_message}")
            return

        print(f"\n📊 {result.model_name} Results:")
        print(f"   Total tests: {result.total_tests}")
        print(f"   Correct: {result.correct_predictions}")
        print(f"   Accuracy: {result.accuracy:.1f}%")

        # Category breakdown
        print(f"   Category Performance:")
        for category, stats in result.category_results.items():
            if stats["total"] > 0:
                cat_accuracy = stats["correct"] / stats["total"] * 100
                print(f"     {category}: {cat_accuracy:.1f}% ({stats['correct']}/{stats['total']})")

        # Show worst performing categories
        worst_categories = []
        for category, stats in result.category_results.items():
            if stats["total"] > 0:
                cat_accuracy = stats["correct"] / stats["total"] * 100
                if cat_accuracy < 50:  # Less than 50% accuracy
                    worst_categories.append((category, cat_accuracy))

        if worst_categories:
            worst_categories.sort(key=lambda x: x[1])  # Sort by accuracy
            print(f"   ⚠️  Worst categories:")
            for category, accuracy in worst_categories[:3]:
                print(f"     {category}: {accuracy:.1f}%")

    def _print_model_comparison(self, results: List[ModelResult]):
        """Print comparison between models."""

        available_results = [r for r in results if r.available]
        if not available_results:
            print("\n❌ No models available for comparison")
            return

        print(f"\n🏆 Model Comparison:")
        print("-" * 50)

        # Sort by accuracy
        available_results.sort(key=lambda x: x.accuracy, reverse=True)

        print(f"{'Model':<15} {'Accuracy':<10} {'Correct':<8} {'Total':<8}")
        print("-" * 45)

        for result in available_results:
            print(
                f"{result.model_name:<15} {result.accuracy:<10.1f}% {result.correct_predictions:<8} {result.total_tests:<8}"
            )

        # Recommend best model
        best_model = available_results[0]
        print(f"\n🎯 RECOMMENDED MODEL: {best_model.model_name} ({best_model.accuracy:.1f}% accuracy)")

        if best_model.model_name == "Enhanced Music":
            print("✅ Our enhanced model performs best for music industry analytics!")
        else:
            print(f"⚠️  {best_model.model_name} outperforms our enhanced model.")
            print("   Consider improving the enhanced model or using the better performer.")

    def _save_detailed_results(self, results: List[ModelResult]):
        """Save detailed results to CSV files."""

        # Save summary results
        summary_data = []
        for result in results:
            if result.available:
                summary_data.append(
                    {
                        "model": result.model_name,
                        "accuracy": result.accuracy,
                        "correct": result.correct_predictions,
                        "total": result.total_tests,
                    }
                )

        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv("music_sentiment_model_comparison.csv", index=False)
            print(f"\n💾 Summary results saved to: music_sentiment_model_comparison.csv")

        # Save detailed failed cases for best performing model
        available_results = [r for r in results if r.available]
        if available_results:
            best_model = max(available_results, key=lambda x: x.accuracy)

            if best_model.failed_cases:
                failed_df = pd.DataFrame(best_model.failed_cases, columns=["phrase", "expected", "predicted"])
                failed_df["model"] = best_model.model_name
                failed_df.to_csv(f'{best_model.model_name.lower().replace(" ", "_")}_failed_cases.csv', index=False)
                print(f"💾 Failed cases saved to: {best_model.model_name.lower().replace(' ', '_')}_failed_cases.csv")

    def test_specific_phrases(self, phrases: List[str]) -> Dict[str, Dict]:
        """Test specific phrases against all models."""

        print(f"\n🎯 Testing Specific Phrases:")
        print("=" * 60)

        results = {}

        # Test with each available model
        models = {
            "VADER": self._test_phrases_vader,
            "TextBlob": self._test_phrases_textblob,
            "Enhanced": self._test_phrases_enhanced,
        }

        for model_name, test_func in models.items():
            try:
                model_results = test_func(phrases)
                results[model_name] = model_results
            except Exception as e:
                print(f"❌ {model_name} failed: {e}")
                results[model_name] = None

        # Print comparison table
        print(f"\n{'Phrase':<35} | {'VADER':<8} | {'TextBlob':<8} | {'Enhanced':<8}")
        print("-" * 70)

        for phrase in phrases:
            row = f"{phrase[:32]:<35}"

            for model_name in ["VADER", "TextBlob", "Enhanced"]:
                if results[model_name] and phrase in results[model_name]:
                    score = results[model_name][phrase]["score"]
                    sentiment = results[model_name][phrase]["sentiment"]
                    row += f" | {sentiment[:6]:<8}"
                else:
                    row += f" | {'N/A':<8}"

            print(row)

        return results

    def _test_phrases_vader(self, phrases: List[str]) -> Dict:
        """Test phrases with VADER."""
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()

        results = {}
        for phrase in phrases:
            scores = vader.polarity_scores(phrase)
            compound = scores["compound"]

            if compound >= 0.05:
                sentiment = "positive"
            elif compound <= -0.05:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            results[phrase] = {"score": compound, "sentiment": sentiment}

        return results

    def _test_phrases_textblob(self, phrases: List[str]) -> Dict:
        """Test phrases with TextBlob."""
        from textblob import TextBlob

        results = {}
        for phrase in phrases:
            blob = TextBlob(phrase)
            polarity = blob.sentiment.polarity

            if polarity > 0.1:
                sentiment = "positive"
            elif polarity < -0.1:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            results[phrase] = {"score": polarity, "sentiment": sentiment}

        return results

    def _test_phrases_enhanced(self, phrases: List[str]) -> Dict:
        """Test phrases with enhanced model."""
        from src.youtubeviz.enhanced_music_sentiment import (
            ComprehensiveMusicSentimentAnalyzer,
        )

        analyzer = ComprehensiveMusicSentimentAnalyzer()
        results = {}

        for phrase in phrases:
            result = analyzer.analyze_comment(phrase)
            score = result["sentiment_score"]

            if score > 0.1:
                sentiment = "positive"
            elif score < -0.1:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            results[phrase] = {"score": score, "sentiment": sentiment}

        return results


def main():
    """Run comprehensive model testing."""

    tester = SentimentModelTester()

    # Run full comprehensive test
    results = tester.run_comprehensive_test()

    # Test specific problematic phrases
    problematic_phrases = [
        "this is sick",
        "this hard af",
        "bro this crazy",
        "fucking queen",
        "go off king",
        "the beat though",
        "you slid",
        "periodt",
        "no cap",
        "ate that",
        "mid",
        "this ain't it chief",
        "flop",
    ]

    print(f"\n" + "=" * 70)
    tester.test_specific_phrases(problematic_phrases)

    return results


if __name__ == "__main__":
    main()
