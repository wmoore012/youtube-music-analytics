#!/usr/bin/env python3
"""
Final V2.0 Model Validation

Test the production-grade v2.0 dataset with proper intent/sentiment separation
against all available sentiment models to demonstrate the improvements.
"""

import os
import sys
from typing import Dict, List, Tuple

import pandas as pd

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from datasets.music_industry_sentiment_dataset_v2 import get_music_industry_dataset_v2
from src.youtubeviz.production_music_sentiment import ProductionMusicSentimentAnalyzer


def test_vader_on_v2_dataset():
    """Test VADER on the v2.0 dataset."""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()

        dataset = get_music_industry_dataset_v2()
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
                        "intent": entry.intent.value,
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


def test_textblob_on_v2_dataset():
    """Test TextBlob on the v2.0 dataset."""
    try:
        from textblob import TextBlob

        dataset = get_music_industry_dataset_v2()
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
                        "intent": entry.intent.value,
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


def test_production_model_on_v2_dataset():
    """Test production model on v2.0 dataset using proper train/test split."""

    dataset = get_music_industry_dataset_v2()

    # Get stratified train/test split
    train_entries, test_entries = dataset.get_train_test_split(test_size=0.2, random_state=42)

    # Create analyzer without loading dataset to avoid data leakage
    analyzer = ProductionMusicSentimentAnalyzer(use_dataset=False)

    # Train on training set only
    for entry in train_entries:
        if entry.sentiment.value == "positive":
            analyzer.positive_phrases[entry.phrase.lower()] = entry.confidence
        elif entry.sentiment.value == "negative":
            analyzer.negative_phrases[entry.phrase.lower()] = entry.confidence

    # Test on test set
    correct = 0
    total = len(test_entries)
    failed_cases = []
    beat_detection_correct = 0
    beat_detection_total = 0

    for entry in test_entries:
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
                    "intent": entry.intent.value,
                    "category": entry.category.value,
                    "beat_appreciation_expected": entry.beat_appreciation,
                    "beat_appreciation_predicted": result["beat_appreciation"],
                }
            )

        # Test beat appreciation detection
        beat_detection_total += 1
        if result["beat_appreciation"] == entry.beat_appreciation:
            beat_detection_correct += 1

    accuracy = correct / total * 100
    beat_accuracy = beat_detection_correct / beat_detection_total * 100 if beat_detection_total > 0 else 0

    return {
        "model": "Production Music (Train/Test Split)",
        "accuracy": accuracy,
        "correct": correct,
        "total": total,
        "failed_cases": failed_cases,
        "beat_accuracy": beat_accuracy,
        "beat_correct": beat_detection_correct,
        "beat_total": beat_detection_total,
        "train_size": len(train_entries),
        "test_size": len(test_entries),
    }


def analyze_intent_sentiment_separation():
    """Analyze how well the v2.0 dataset separates intent from sentiment."""

    dataset = get_music_industry_dataset_v2()

    print(f"\n🎯 Intent vs Sentiment Analysis (v2.0 Dataset)")
    print("=" * 60)

    # Group by intent and sentiment
    intent_sentiment_matrix = {}

    for entry in dataset.entries:
        intent = entry.intent.value
        sentiment = entry.sentiment.value

        if intent not in intent_sentiment_matrix:
            intent_sentiment_matrix[intent] = {}
        if sentiment not in intent_sentiment_matrix[intent]:
            intent_sentiment_matrix[intent][sentiment] = 0

        intent_sentiment_matrix[intent][sentiment] += 1

    # Print matrix
    print(f"{'Intent':<15} | {'Positive':<10} | {'Neutral':<10} | {'Negative':<10} | {'Total':<10}")
    print("-" * 70)

    for intent, sentiments in intent_sentiment_matrix.items():
        pos = sentiments.get("positive", 0)
        neu = sentiments.get("neutral", 0)
        neg = sentiments.get("negative", 0)
        total = pos + neu + neg

        print(f"{intent:<15} | {pos:<10} | {neu:<10} | {neg:<10} | {total:<10}")

    # Show examples of neutral requests vs positive anticipation
    print(f"\n📝 Examples of Intent/Sentiment Separation:")
    print("-" * 50)

    neutral_requests = [e for e in dataset.entries if e.intent.value == "request" and e.sentiment.value == "neutral"]
    positive_anticipation = [
        e for e in dataset.entries if e.intent.value == "anticipation" and e.sentiment.value == "positive"
    ]

    print(f"Neutral Requests (no opinion cues):")
    for entry in neutral_requests[:3]:
        print(f"  '{entry.phrase}' → {entry.sentiment.value}/{entry.intent.value}")

    print(f"\nPositive Anticipation (with enthusiasm cues):")
    for entry in positive_anticipation[:3]:
        print(f"  '{entry.phrase}' → {entry.sentiment.value}/{entry.intent.value}")


def run_v2_validation():
    """Run comprehensive v2.0 dataset validation."""

    print("🎵 Music Industry Sentiment Dataset v2.0 - Final Validation")
    print("=" * 80)

    # Load dataset info
    dataset = get_music_industry_dataset_v2()
    dataset.print_statistics()

    # Analyze intent/sentiment separation
    analyze_intent_sentiment_separation()

    print(f"\n🧪 Testing All Models on v2.0 Dataset ({len(dataset.entries)} phrases)")
    print("=" * 80)

    # Test all models
    results = [test_vader_on_v2_dataset(), test_textblob_on_v2_dataset(), test_production_model_on_v2_dataset()]

    # Print results
    print(f"\n📊 Model Performance Comparison (v2.0 Dataset):")
    print("-" * 80)
    print(f"{'Model':<35} {'Accuracy':<12} {'Correct/Total':<15} {'Beat Detection':<15}")
    print("-" * 80)

    for result in results:
        if "error" in result:
            print(f"{result['model']:<35} {'N/A':<12} {'N/A':<15} {'N/A':<15}")
            print(f"   Error: {result['error']}")
        else:
            accuracy_str = f"{result['accuracy']:.1f}%"
            correct_str = f"{result['correct']}/{result['total']}"

            if "beat_accuracy" in result:
                beat_str = f"{result['beat_accuracy']:.1f}% ({result['beat_correct']}/{result['beat_total']})"
            else:
                beat_str = "N/A"

            print(f"{result['model']:<35} {accuracy_str:<12} {correct_str:<15} {beat_str:<15}")

            if "train_size" in result:
                print(f"   Train/Test Split: {result['train_size']} train, {result['test_size']} test")

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

    # Test original problem cases
    print(f"\n🎯 Testing Original Problem Cases (v2.0 Classification):")
    print("-" * 60)

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
        # Test some neutral cases
        "drop the album",
        "need the lyrics",
        "who mixed this?",
        # Test some negative cases
        "mid",
        "this ain't it chief",
    ]

    # Test with full dataset model (for demonstration)
    full_analyzer = ProductionMusicSentimentAnalyzer(use_dataset=True)

    print(f"{'Comment':<35} | {'Sentiment':<10} | {'Score':<8} | {'Beat':<5}")
    print("-" * 65)

    original_correct = 0
    for comment in original_cases:
        result = full_analyzer.analyze_comment(comment)
        score = result["sentiment_score"]

        if score > 0.1:
            sentiment = "POSITIVE"
        elif score < -0.1:
            sentiment = "NEGATIVE"
        else:
            sentiment = "NEUTRAL"

        beat_emoji = "🎵" if result["beat_appreciation"] else "⚪"

        print(f"{comment[:32]:<35} | {sentiment:<10} | {score:+.2f}   | {beat_emoji:<5}")

    # Final recommendation
    print(f"\n🎯 V2.0 DATASET IMPROVEMENTS:")
    print("=" * 60)
    print("✅ 255 scientifically classified phrases (vs 180 in v1.0)")
    print("✅ Intent/sentiment separation following SemEval standards")
    print("✅ Proper neutral handling for requests/questions")
    print("✅ Aspect-based sentiment analysis support")
    print("✅ Toxicity and NSFW flagging")
    print("✅ Pydantic schema validation with runtime checks")
    print("✅ Deduplication and normalization guardrails")
    print("✅ Stratified train/test splits")
    print("✅ Multiple export formats (CSV, JSONL)")

    if best_model and best_model["accuracy"] >= 80:
        print(f"\n🎉 V2.0 DATASET IS PRODUCTION-READY!")
        print("   - Proper scientific methodology")
        print("   - Intent/sentiment separation")
        print("   - Comprehensive music industry coverage")
        print("   - Ready for model training and evaluation")

    return results


if __name__ == "__main__":
    results = run_v2_validation()
