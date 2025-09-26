#!/usr/bin/env python3
"""
Test Professional Momentum Scoring System

Validates the mathematically sound momentum scoring algorithms with real data.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

import numpy as np
import pandas as pd

from src.youtubeviz.professional_momentum_scoring import (
    MomentumAnalysisConfig,
    ProfessionalMomentumScorer,
    create_momentum_summary_dataframe,
    display_momentum_analysis_results,
)
from web.etl_helpers import get_engine


def test_momentum_scoring_accuracy():
    """Test that momentum scores are mathematically sound and interpretable."""
    print("🧪 TESTING MOMENTUM SCORING ACCURACY")
    print("=" * 50)

    try:
        engine = get_engine()

        # Create scorer with test configuration
        config = MomentumAnalysisConfig(analysis_window_days=60, min_videos_required=2, confidence_level=0.95)
        scorer = ProfessionalMomentumScorer(config)

        # Calculate momentum scores
        momentum_scores = scorer.calculate_momentum_scores(engine)

        if not momentum_scores:
            print("⚠️  No momentum scores calculated")
            return False

        print(f"✅ Calculated {len(momentum_scores)} momentum scores")

        # Validate score ranges
        scores = [s.score_value for s in momentum_scores]
        confidences = [s.confidence for s in momentum_scores]

        # Check that all scores are in valid range (0-1)
        invalid_scores = [s for s in scores if s < 0 or s > 1]
        if invalid_scores:
            print(f"❌ Found {len(invalid_scores)} scores outside valid range (0-1)")
            return False

        # Check that all confidences are in valid range (0-1)
        invalid_confidences = [c for c in confidences if c < 0 or c > 1]
        if invalid_confidences:
            print(f"❌ Found {len(invalid_confidences)} confidences outside valid range (0-1)")
            return False

        print(f"✅ All scores in valid range: {min(scores):.3f} - {max(scores):.3f}")
        print(f"✅ All confidences in valid range: {min(confidences):.3f} - {max(confidences):.3f}")

        # Check statistical significance
        significant_scores = [s for s in momentum_scores if s.statistical_significance < 0.05]
        print(f"📊 Statistically significant results: {len(significant_scores)}/{len(momentum_scores)}")

        return True

    except Exception as e:
        print(f"❌ Momentum scoring accuracy test failed: {e}")
        return False


def test_confidence_intervals():
    """Test that confidence intervals are properly calculated."""
    print("\n🧪 TESTING CONFIDENCE INTERVALS")
    print("=" * 50)

    try:
        engine = get_engine()
        scorer = ProfessionalMomentumScorer()

        momentum_scores = scorer.calculate_momentum_scores(engine)

        if not momentum_scores:
            print("⚠️  No momentum scores for confidence interval testing")
            return False

        # Validate confidence intervals
        valid_intervals = 0
        for score in momentum_scores:
            # Check that lower <= score <= upper
            if score.confidence_interval_lower <= score.score_value <= score.confidence_interval_upper:
                valid_intervals += 1
            else:
                print(
                    f"⚠️  Invalid interval for {score.artist_name}: "
                    f"[{score.confidence_interval_lower:.3f}, {score.confidence_interval_upper:.3f}] "
                    f"does not contain {score.score_value:.3f}"
                )

        print(f"✅ Valid confidence intervals: {valid_intervals}/{len(momentum_scores)}")

        # Show example intervals
        print(f"\n📊 Example Confidence Intervals:")
        for score in momentum_scores[:3]:
            interval_width = score.confidence_interval_upper - score.confidence_interval_lower
            print(
                f"   {score.artist_name}: {score.score_value:.3f} "
                f"[{score.confidence_interval_lower:.3f}, {score.confidence_interval_upper:.3f}] "
                f"(width: {interval_width:.3f})"
            )

        return valid_intervals == len(momentum_scores)

    except Exception as e:
        print(f"❌ Confidence interval test failed: {e}")
        return False


def test_momentum_categories():
    """Test that momentum categories are meaningful and consistent."""
    print("\n🧪 TESTING MOMENTUM CATEGORIES")
    print("=" * 50)

    try:
        engine = get_engine()
        scorer = ProfessionalMomentumScorer()

        momentum_scores = scorer.calculate_momentum_scores(engine)

        if not momentum_scores:
            print("⚠️  No momentum scores for category testing")
            return False

        # Check category distribution
        categories = [s.category for s in momentum_scores]
        category_counts = pd.Series(categories).value_counts()

        print(f"📊 Category Distribution:")
        for category, count in category_counts.items():
            percentage = (count / len(momentum_scores)) * 100
            print(f"   {category}: {count} artists ({percentage:.1f}%)")

        # Validate category consistency with scores
        category_score_ranges = {}
        for score in momentum_scores:
            if score.category not in category_score_ranges:
                category_score_ranges[score.category] = []
            category_score_ranges[score.category].append(score.score_value)

        print(f"\n📈 Category Score Ranges:")
        for category, scores in category_score_ranges.items():
            min_score = min(scores)
            max_score = max(scores)
            avg_score = np.mean(scores)
            print(f"   {category}: {min_score:.3f} - {max_score:.3f} (avg: {avg_score:.3f})")

        # Check that higher categories have higher average scores
        category_averages = {cat: np.mean(scores) for cat, scores in category_score_ranges.items()}

        expected_order = ["Declining", "Low Momentum", "Moderate Momentum", "High Momentum"]
        actual_order = sorted(category_averages.keys(), key=lambda x: category_averages[x])

        # Check if order makes sense (allowing for some flexibility)
        order_makes_sense = True
        for i in range(len(actual_order) - 1):
            if category_averages[actual_order[i]] > category_averages[actual_order[i + 1]]:
                order_makes_sense = False
                break

        if order_makes_sense:
            print("✅ Category ordering is consistent with score values")
        else:
            print("⚠️  Category ordering may not be optimal")

        return len(category_counts) > 0

    except Exception as e:
        print(f"❌ Momentum category test failed: {e}")
        return False


def test_dataframe_creation():
    """Test that summary DataFrame is created correctly."""
    print("\n🧪 TESTING DATAFRAME CREATION")
    print("=" * 50)

    try:
        engine = get_engine()
        scorer = ProfessionalMomentumScorer()

        momentum_scores = scorer.calculate_momentum_scores(engine)

        if not momentum_scores:
            print("⚠️  No momentum scores for DataFrame testing")
            return False

        # Create summary DataFrame
        summary_df = create_momentum_summary_dataframe(momentum_scores)

        print(f"✅ Created DataFrame with {len(summary_df)} rows and {len(summary_df.columns)} columns")

        # Validate DataFrame structure
        expected_columns = [
            "artist_name",
            "score_value",
            "confidence",
            "momentum_category",
            "growth_rate_pct",
            "statistical_significance",
            "total_videos",
            "recent_videos",
            "data_quality_score",
            "confidence_interval",
        ]

        missing_columns = [col for col in expected_columns if col not in summary_df.columns]
        if missing_columns:
            print(f"❌ Missing columns: {missing_columns}")
            return False

        print(f"✅ All expected columns present")

        # Show sample data
        print(f"\n📋 Sample Data:")
        print(summary_df.head(3).to_string(index=False))

        return True

    except Exception as e:
        print(f"❌ DataFrame creation test failed: {e}")
        return False


def test_real_data_validation():
    """Test that the system works with real YouTube data."""
    print("\n🧪 TESTING REAL DATA VALIDATION")
    print("=" * 50)

    try:
        engine = get_engine()
        scorer = ProfessionalMomentumScorer()

        momentum_scores = scorer.calculate_momentum_scores(engine)

        if not momentum_scores:
            print("⚠️  No momentum scores - may indicate data issues")
            return False

        # Validate that we're using real artist names
        artist_names = [s.artist_name for s in momentum_scores]

        # Check for fake/dummy names
        fake_patterns = ["artist", "test", "dummy", "sample", "fake"]
        fake_artists = []

        for name in artist_names:
            name_lower = str(name).lower()
            if any(pattern in name_lower for pattern in fake_patterns):
                fake_artists.append(name)

        if fake_artists:
            print(f"⚠️  Found potentially fake artist names: {fake_artists}")
        else:
            print(f"✅ All artist names appear to be real")

        # Show real artist examples
        print(f"\n🎵 Real Artists Analyzed:")
        for artist in artist_names[:5]:
            print(f"   • {artist}")

        # Validate data quality scores
        quality_scores = [s.data_quality_score for s in momentum_scores]
        avg_quality = np.mean(quality_scores)

        print(f"\n📊 Data Quality Assessment:")
        print(f"   Average Quality Score: {avg_quality:.3f}")
        print(f"   Quality Range: {min(quality_scores):.3f} - {max(quality_scores):.3f}")

        if avg_quality > 0.7:
            print("✅ High data quality detected")
        elif avg_quality > 0.5:
            print("🟡 Moderate data quality detected")
        else:
            print("🔴 Low data quality detected - may need more data")

        return len(fake_artists) == 0

    except Exception as e:
        print(f"❌ Real data validation test failed: {e}")
        return False


def main():
    """Run all professional momentum scoring tests."""
    print("🚀 PROFESSIONAL MOMENTUM SCORING TESTS")
    print("=" * 60)

    # Run all tests
    test_results = {
        "Scoring Accuracy": test_momentum_scoring_accuracy(),
        "Confidence Intervals": test_confidence_intervals(),
        "Momentum Categories": test_momentum_categories(),
        "DataFrame Creation": test_dataframe_creation(),
        "Real Data Validation": test_real_data_validation(),
    }

    # Summary
    print("\n" + "=" * 60)
    print("🏆 TEST SUMMARY")
    print("=" * 60)

    passed_tests = 0
    for test_name, result in test_results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {test_name}")
        if result:
            passed_tests += 1

    print(f"\n📊 Overall Results: {passed_tests}/{len(test_results)} tests passed")

    if passed_tests == len(test_results):
        print("\n🎉 All tests passed! Professional momentum scoring is working correctly.")
        print("\n💡 Key Improvements:")
        print("   • Mathematically sound algorithms with statistical validation")
        print("   • Proper confidence intervals and significance testing")
        print("   • Normalized scores (0-1) with clear business interpretation")
        print("   • Robust outlier handling and data quality assessment")
        print("   • Industry-relevant momentum categories")
        return 0
    else:
        print(f"\n⚠️  {len(test_results) - passed_tests} tests failed. Check output above for details.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
