#!/usr/bin/env python3
"""
Benchmark Sentiment Models - Track Performance Over Time

This script establishes baseline performance and tracks improvements
as we implement enhanced sentiment analysis variants.
"""

from datetime import datetime
import json
import time
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine


def create_test_dataset() -> List[Dict]:
    """Create standardized test dataset for consistent benchmarking."""

    # Test phrases with expected sentiment (ground truth)
    test_cases = [
        # Clearly positive music slang
        {"phrase": "this is sick", "expected": "positive", "category": "music_slang"},
        {"phrase": "this slaps", "expected": "positive", "category": "music_slang"},
        {"phrase": "straight fire", "expected": "positive", "category": "music_slang"},
        {"phrase": "goes hard", "expected": "positive", "category": "music_slang"},
        {"phrase": "banger", "expected": "positive", "category": "music_slang"},
        {"phrase": "goated", "expected": "positive", "category": "music_slang"},
        # Gen Z positive expressions
        {"phrase": "no cap this slaps", "expected": "positive", "category": "gen_z_positive"},
        {"phrase": "periodt", "expected": "positive", "category": "gen_z_positive"},
        {"phrase": "it's giving main character energy", "expected": "positive", "category": "gen_z_positive"},
        {"phrase": "chef's kiss", "expected": "positive", "category": "gen_z_positive"},
        {"phrase": "hits different", "expected": "positive", "category": "gen_z_positive"},
        {"phrase": "I'm obsessed", "expected": "positive", "category": "gen_z_positive"},
        # Cultural expressions (positive)
        {"phrase": "fucking queen", "expected": "positive", "category": "cultural_positive"},
        {"phrase": "go off king", "expected": "positive", "category": "cultural_positive"},
        {"phrase": "bad bish", "expected": "positive", "category": "cultural_positive"},
        {"phrase": "YES MOTHER", "expected": "positive", "category": "cultural_positive"},
        # Music production praise
        {"phrase": "the vocals are insane", "expected": "positive", "category": "production_praise"},
        {"phrase": "production is clean", "expected": "positive", "category": "production_praise"},
        {"phrase": "harmonies hit different", "expected": "positive", "category": "production_praise"},
        {"phrase": "the beat though", "expected": "positive", "category": "production_praise"},
        # Enthusiasm/excitement
        {"phrase": "fuck it up", "expected": "positive", "category": "enthusiasm"},
        {"phrase": "the way I screamed", "expected": "positive", "category": "enthusiasm"},
        {"phrase": "lowkey fire", "expected": "positive", "category": "enthusiasm"},
        {"phrase": "highkey obsessed", "expected": "positive", "category": "enthusiasm"},
        # Clearly negative
        {"phrase": "this ain't it chief", "expected": "negative", "category": "negative"},
        {"phrase": "mid", "expected": "negative", "category": "negative"},
        {"phrase": "cringe", "expected": "negative", "category": "negative"},
        {"phrase": "trash", "expected": "negative", "category": "negative"},
        # Neutral/informational
        {"phrase": "I need the lyrics", "expected": "neutral", "category": "neutral_request"},
        {"phrase": "who produced this", "expected": "neutral", "category": "neutral_question"},
        {"phrase": "what's the sample", "expected": "neutral", "category": "neutral_question"},
        {"phrase": "clean version pls", "expected": "neutral", "category": "neutral_request"},
    ]

    return test_cases


def benchmark_vader_model(test_cases: List[Dict]) -> Dict:
    """Benchmark current VADER model performance."""

    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()
    except ImportError:
        print("❌ VADER not installed")
        return {"error": "VADER not available"}

    results = []
    start_time = time.time()

    for case in test_cases:
        phrase = case["phrase"]
        expected = case["expected"]
        category = case["category"]

        # Get VADER scores
        scores = vader.polarity_scores(phrase)
        compound = scores["compound"]

        # Classify based on compound score
        if compound >= 0.05:
            predicted = "positive"
        elif compound <= -0.05:
            predicted = "negative"
        else:
            predicted = "neutral"

        # Check if correct
        correct = predicted == expected

        results.append(
            {
                "phrase": phrase,
                "expected": expected,
                "predicted": predicted,
                "correct": correct,
                "compound_score": compound,
                "category": category,
                "pos_score": scores["pos"],
                "neg_score": scores["neg"],
                "neu_score": scores["neu"],
            }
        )

    end_time = time.time()
    processing_time = end_time - start_time

    # Calculate metrics
    total_cases = len(results)
    correct_cases = sum(1 for r in results if r["correct"])
    accuracy = correct_cases / total_cases if total_cases > 0 else 0

    # Per-category accuracy
    category_stats = {}
    for category in set(case["category"] for case in test_cases):
        category_results = [r for r in results if r["category"] == category]
        category_correct = sum(1 for r in category_results if r["correct"])
        category_total = len(category_results)
        category_accuracy = category_correct / category_total if category_total > 0 else 0

        category_stats[category] = {"correct": category_correct, "total": category_total, "accuracy": category_accuracy}

    return {
        "model_name": "stock_vader",
        "total_cases": total_cases,
        "correct_cases": correct_cases,
        "accuracy": accuracy,
        "processing_time_seconds": processing_time,
        "throughput_cases_per_second": total_cases / processing_time if processing_time > 0 else 0,
        "category_stats": category_stats,
        "detailed_results": results,
        "timestamp": datetime.now().isoformat(),
    }


def benchmark_current_advanced_model(test_cases: List[Dict]) -> Dict:
    """Benchmark our current advanced music sentiment model."""

    try:
        from src.youtubeviz.advanced_music_sentiment import AdvancedMusicSentimentAnalyzer

        analyzer = AdvancedMusicSentimentAnalyzer()
    except ImportError:
        print("❌ Advanced model not available")
        return {"error": "Advanced model not available"}

    results = []
    start_time = time.time()

    for case in test_cases:
        phrase = case["phrase"]
        expected = case["expected"]
        category = case["category"]

        # Get advanced model analysis
        analysis = analyzer.analyze_comment(phrase)
        predicted = analysis.sentiment.value

        # Check if correct
        correct = predicted == expected

        results.append(
            {
                "phrase": phrase,
                "expected": expected,
                "predicted": predicted,
                "correct": correct,
                "confidence": analysis.confidence,
                "intent": analysis.intent.value,
                "aspect": analysis.aspect.value,
                "booster_score": analysis.booster_score,
                "category": category,
            }
        )

    end_time = time.time()
    processing_time = end_time - start_time

    # Calculate metrics
    total_cases = len(results)
    correct_cases = sum(1 for r in results if r["correct"])
    accuracy = correct_cases / total_cases if total_cases > 0 else 0

    # Per-category accuracy
    category_stats = {}
    for category in set(case["category"] for case in test_cases):
        category_results = [r for r in results if r["category"] == category]
        category_correct = sum(1 for r in category_results if r["correct"])
        category_total = len(category_results)
        category_accuracy = category_correct / category_total if category_total > 0 else 0

        category_stats[category] = {"correct": category_correct, "total": category_total, "accuracy": category_accuracy}

    return {
        "model_name": "current_advanced",
        "total_cases": total_cases,
        "correct_cases": correct_cases,
        "accuracy": accuracy,
        "processing_time_seconds": processing_time,
        "throughput_cases_per_second": total_cases / processing_time if processing_time > 0 else 0,
        "category_stats": category_stats,
        "detailed_results": results,
        "timestamp": datetime.now().isoformat(),
    }


def save_benchmark_to_database(benchmark_results: Dict, benchmark_id: str) -> None:
    """Save benchmark results to the project_benchmarks table."""

    try:
        engine = get_engine()

        with engine.connect() as conn:
            # Insert main benchmark record
            conn.execute(
                text(
                    """
                INSERT INTO project_benchmarks (
                    benchmark_id, benchmark_date, sentiment_available,
                    sentiment_avg_time, sentiment_throughput, sentiment_comments_tested,
                    existing_model_benchmarks, notes
                ) VALUES (
                    :benchmark_id, :benchmark_date, 'available',
                    :avg_time, :throughput, :comments_tested,
                    :model_benchmarks, :notes
                )
            """
                ),
                {
                    "benchmark_id": benchmark_id,
                    "benchmark_date": datetime.now(),
                    "avg_time": benchmark_results.get("processing_time_seconds", 0)
                    / benchmark_results.get("total_cases", 1),
                    "throughput": benchmark_results.get("throughput_cases_per_second", 0),
                    "comments_tested": benchmark_results.get("total_cases", 0),
                    "model_benchmarks": json.dumps(benchmark_results),
                    "notes": f"Baseline benchmark for {benchmark_results.get('model_name', 'unknown')} model",
                },
            )

            # Insert model-specific record
            conn.execute(
                text(
                    """
                INSERT INTO project_benchmark_models (
                    benchmark_id, model_name, accuracy_pct
                ) VALUES (
                    :benchmark_id, :model_name, :accuracy_pct
                )
            """
                ),
                {
                    "benchmark_id": benchmark_id,
                    "model_name": benchmark_results.get("model_name", "unknown"),
                    "accuracy_pct": benchmark_results.get("accuracy", 0) * 100,
                },
            )

            conn.commit()
            print(f"✅ Saved benchmark {benchmark_id} to database")

    except Exception as e:
        print(f"❌ Failed to save benchmark to database: {e}")


def run_comprehensive_benchmark() -> None:
    """Run comprehensive benchmark of all available models."""

    print("🎯 COMPREHENSIVE SENTIMENT MODEL BENCHMARK")
    print("=" * 60)

    # Create standardized test dataset
    test_cases = create_test_dataset()
    print(f"📊 Created test dataset with {len(test_cases)} cases")

    # Generate unique benchmark ID
    benchmark_id = f"baseline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Benchmark 1: Stock VADER
    print("\n🧪 Benchmarking Stock VADER...")
    vader_results = benchmark_vader_model(test_cases)
    if "error" not in vader_results:
        print(f"   Accuracy: {vader_results['accuracy']:.1%}")
        print(f"   Throughput: {vader_results['throughput_cases_per_second']:.1f} cases/sec")
        save_benchmark_to_database(vader_results, f"{benchmark_id}_vader")

    # Benchmark 2: Current Advanced Model
    print("\n🧪 Benchmarking Current Advanced Model...")
    advanced_results = benchmark_current_advanced_model(test_cases)
    if "error" not in advanced_results:
        print(f"   Accuracy: {advanced_results['accuracy']:.1%}")
        print(f"   Throughput: {advanced_results['throughput_cases_per_second']:.1f} cases/sec")
        save_benchmark_to_database(advanced_results, f"{benchmark_id}_advanced")

    # Detailed comparison
    if "error" not in vader_results and "error" not in advanced_results:
        print("\n📈 MODEL COMPARISON")
        print("=" * 40)
        print(f"Stock VADER:      {vader_results['accuracy']:.1%} accuracy")
        print(f"Current Advanced: {advanced_results['accuracy']:.1%} accuracy")

        improvement = advanced_results["accuracy"] - vader_results["accuracy"]
        print(f"Improvement:      {improvement:+.1%}")

        # Category breakdown
        print("\n📊 CATEGORY BREAKDOWN")
        print("-" * 40)
        categories = set(vader_results["category_stats"].keys())

        for category in sorted(categories):
            vader_acc = vader_results["category_stats"][category]["accuracy"]
            advanced_acc = advanced_results["category_stats"][category]["accuracy"]

            print(f"{category:20} | VADER: {vader_acc:.1%} | Advanced: {advanced_acc:.1%}")

    # Save detailed results to files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if "error" not in vader_results:
        with open(f"benchmark_vader_{timestamp}.json", "w") as f:
            json.dump(vader_results, f, indent=2)

    if "error" not in advanced_results:
        with open(f"benchmark_advanced_{timestamp}.json", "w") as f:
            json.dump(advanced_results, f, indent=2)

    print(f"\n✅ Benchmark complete! Results saved with ID: {benchmark_id}")
    print("🎯 Now we have baseline metrics to improve upon!")


if __name__ == "__main__":
    run_comprehensive_benchmark()
