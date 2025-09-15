#!/usr/bin/env python3
"""
Final Model Validation and Comparison

This script provides the definitive comparison between sentiment models
using our comprehensive music industry dataset. It demonstrates why our
production model is superior for music industry analytics.
"""

import os
import sys
from typing import Dict, List, Tuple

import pandas as pd

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from datasets.music_industry_sentiment_dataset import get_music_industry_dataset
from src.youtubeviz.production_music_sentiment import ProductionMusicSentimentAnalyzer


def test_vader_on_full_dataset():
    """Test VADER on the complete dataset."""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()

        dataset = get_music_industry_dataset()
        correct = 0
        total = len(dataset.entries)
        failed_cases = []

        for entry in dataset.entries:
            scores = vader.polarity_scores(entry.phrase)
            compound = scores["compound"]

            if compound >= 0.05:
                predicted = "positive"
            elif compound <= -0.05:
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
                        "score": compound,
                        "category": entry.category.value,
                    }
                )

        accuracy = correct / total * 100
        return {
            "model": "VADER",
            "accuracy": accuracy,
            "correct": correct,
            "total": total,
            "failed_cases": failed_cases,
        }

    except ImportError:
        return {
            "model": "VADER",
            "accuracy": 0.0,
            "correct": 0,
            "total": 0,
            "failed_cases": [],
            "error": "VADER not available",
        }


def test_textblob_on_full_dataset():
    """Test TextBlob on the complete dataset."""
    try:
        from textblob import TextBlob

        dataset = get_music_industry_dataset()
        correct = 0
        total = len(dataset.entries)
        failed_cases = []

        for entry in dataset.entries:
            blob = TextBlob(entry.phrase)
            polarity = blob.sentiment.polarity

            if polarity > 0.1:
                predicted = "positive"
            elif polarity < -0.1:
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
                        "score": polarity,
                        "category": entry.category.value,
                    }
                )

        accuracy = correct / total * 100
        return {
            "model": "TextBlob",
            "accuracy": accuracy,
            "correct": correct,
            "total": total,
            "failed_cases": failed_cases,
        }

    except ImportError:
        return {
            "model": "TextBlob",
            "accuracy": 0.0,
            "correct": 0,
            "total": 0,
            "failed_cases": [],
            "error": "TextBlob not available",
        }


def test_production_model_on_full_dataset():
    """Test our production model on the complete dataset."""

    analyzer = ProductionMusicSentimentAnalyzer()
    dataset = get_music_industry_dataset()

    correct = 0
    total = len(dataset.entries)
    failed_cases = []
    beat_detection_correct = 0
    beat_detection_total = 0

    for entry in dataset.entries:
        result = analyzer.analyze_comment(entry.phrase)
        score = result["sentiment_score"]

        if score > 0.1:
            predicted = "positive"
        elif score < -0.1:
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
                    "score": score,
                    "category": entry.category.value,
                    "beat_appreciation_expected": entry.beat_appreciation,
                    "beat_appreciation_predicted": result["beat_appreciation"],
                }
            )

        # Test beat appreciation detection
        if entry.beat_appreciation is not None:  # Only test where we have ground truth
            beat_detection_total += 1
            if result["beat_appreciation"] == entry.beat_appreciation:
                beat_detection_correct += 1

    accuracy = correct / total * 100
    beat_accuracy = beat_detection_correct / beat_detection_total * 100 if beat_detection_total > 0 else 0

    return {
        "model": "Production Music",
        "accuracy": accuracy,
        "correct": correct,
        "total": total,
        "failed_cases": failed_cases,
        "beat_accuracy": beat_accuracy,
        "beat_correct": beat_detection_correct,
        "beat_total": beat_detection_total,
    }


def run_final_validation():
    """Run final comprehensive validation."""

    print("🎵 Final Music Industry Sentiment Model Validation")
    print("=" * 70)

    # Load dataset info
    dataset = get_music_industry_dataset()
    dataset.print_statistics()

    print(f"\n🧪 Testing All Models on Complete Dataset ({len(dataset.entries)} phrases)")
    print("=" * 70)

    # Test all models
    results = [test_vader_on_full_dataset(), test_textblob_on_full_dataset(), test_production_model_on_full_dataset()]

    # Print results
    print(f"\n📊 Model Performance Comparison:")
    print("-" * 70)
    print(f"{'Model':<20} {'Accuracy':<12} {'Correct/Total':<15} {'Beat Detection':<15}")
    print("-" * 70)

    for result in results:
        if "error" in result:
            print(f"{result['model']:<20} {'N/A':<12} {'N/A':<15} {'N/A':<15}")
            print(f"   Error: {result['error']}")
        else:
            accuracy_str = f"{result['accuracy']:.1f}%"
            correct_str = f"{result['correct']}/{result['total']}"

            if "beat_accuracy" in result:
                beat_str = f"{result['beat_accuracy']:.1f}% ({result['beat_correct']}/{result['beat_total']})"
            else:
                beat_str = "N/A"

            print(f"{result['model']:<20} {accuracy_str:<12} {correct_str:<15} {beat_str:<15}")

    # Find best model
    available_results = [r for r in results if "error" not in r]
    if available_results:
        best_model = max(available_results, key=lambda x: x["accuracy"])

        print(f"\n🏆 BEST MODEL: {best_model['model']}")
        print(f"   Accuracy: {best_model['accuracy']:.1f}%")
        print(f"   Correct predictions: {best_model['correct']}/{best_model['total']}")

        if "beat_accuracy" in best_model:
            print(f"   Beat detection accuracy: {best_model['beat_accuracy']:.1f}%")

        # Show improvement over baselines
        vader_result = next((r for r in results if r["model"] == "VADER"), None)
        textblob_result = next((r for r in results if r["model"] == "TextBlob"), None)

        if vader_result and "error" not in vader_result:
            improvement = best_model["accuracy"] - vader_result["accuracy"]
            print(f"   Improvement over VADER: +{improvement:.1f} percentage points")

        if textblob_result and "error" not in textblob_result:
            improvement = best_model["accuracy"] - textblob_result["accuracy"]
            print(f"   Improvement over TextBlob: +{improvement:.1f} percentage points")

    # Test on original problem cases
    print(f"\n🎯 Testing Original Problem Cases:")
    print("-" * 50)

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
        "this hard af",
        "this hard as shit",
        "Bro this crazy",
        "the beat though!",
        "the beat tho!",
        "who made this beat bro?!",
    ]

    # Test with production model
    analyzer = ProductionMusicSentimentAnalyzer()

    print(f"{'Comment':<35} | {'Sentiment':<10} | {'Score':<8} | {'Beat':<5}")
    print("-" * 65)

    original_correct = 0
    for comment in original_cases:
        result = analyzer.analyze_comment(comment)
        score = result["sentiment_score"]

        if score > 0.1:
            sentiment = "POSITIVE"
            original_correct += 1
        elif score < -0.1:
            sentiment = "NEGATIVE"
        else:
            sentiment = "NEUTRAL"

        beat_emoji = "🎵" if result["beat_appreciation"] else "⚪"

        print(f"{comment[:32]:<35} | {sentiment:<10} | {score:+.2f}   | {beat_emoji:<5}")

    original_accuracy = original_correct / len(original_cases) * 100
    print(f"\n📊 Original Problem Cases: {original_accuracy:.1f}% accuracy ({original_correct}/{len(original_cases)})")

    # Final recommendation
    print(f"\n🎯 FINAL RECOMMENDATION:")
    print("=" * 50)

    if best_model["model"] == "Production Music" and best_model["accuracy"] >= 95:
        print("✅ PRODUCTION MUSIC MODEL is ready for deployment!")
        print("   - Handles music industry slang perfectly")
        print("   - Understands Gen Z language and cultural context")
        print("   - Provides beat appreciation detection")
        print("   - Significantly outperforms baseline models")
        print("   - Ready for music industry analytics")
    else:
        print("🔧 Model needs improvement before production deployment")
        print(f"   Current accuracy: {best_model['accuracy']:.1f}%")
        print("   Target accuracy: 95%+")

    return results


if __name__ == "__main__":
    results = run_final_validation()
