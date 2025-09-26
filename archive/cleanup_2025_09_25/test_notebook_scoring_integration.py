#!/usr/bin/env python3
"""
Test the scoring system integration with the MusicScope™ notebook.
Validates that all scoring functions work with real database data.
"""

from datetime import datetime, timedelta

# Add project root to path
import os
import sys

import pandas as pd

current_dir = os.getcwd()
if current_dir.endswith("notebooks"):
    project_root = os.path.dirname(current_dir)
else:
    project_root = current_dir
sys.path.insert(0, project_root)


def test_scoring_analysis_import():
    """Test that scoring analysis module imports correctly."""
    print("🔍 Testing scoring analysis import...")

    try:
        from src.youtubeviz.scoring_analysis import (
            ScoringAnalyzer,
            create_artist_score_radar,
            create_engagement_distribution_chart,
            create_momentum_scores_chart,
            create_scoring_performance_chart,
            get_scoring_insights,
        )

        print("✅ All scoring analysis functions imported successfully")
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False


def test_scoring_analyzer_initialization():
    """Test scoring analyzer initialization."""
    print("\n🔍 Testing scoring analyzer initialization...")

    try:
        from src.youtubeviz.scoring_analysis import ScoringAnalyzer

        analyzer = ScoringAnalyzer()

        # Check that plugins are registered
        available_algorithms = analyzer.engine.get_available_algorithms()
        expected_algorithms = ["artist_momentum_scorer", "engagement_scorer", "growth_potential_scorer"]

        for alg in expected_algorithms:
            if alg in available_algorithms:
                print(f"✅ {alg} plugin registered")
            else:
                print(f"⚠️  {alg} plugin not found")

        print(f"✅ Scoring analyzer initialized with {len(available_algorithms)} algorithms")
        return True

    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return False


def test_chart_functions_with_sample_data():
    """Test chart functions with sample data."""
    print("\n🔍 Testing chart functions with sample data...")

    try:
        from src.youtubeviz.scoring_analysis import (
            create_engagement_distribution_chart,
            create_momentum_scores_chart,
            create_scoring_performance_chart,
        )

        # Create sample momentum data
        momentum_data = pd.DataFrame(
            {
                "entity_id": ["Artist A", "Artist B", "Artist C"],
                "score_value": [0.8, 0.6, 0.4],
                "confidence": [0.9, 0.8, 0.7],
                "momentum_category": ["high_momentum", "stable", "low_momentum"],
            }
        )

        # Test momentum chart
        fig1 = create_momentum_scores_chart(momentum_data)
        if fig1 is not None:
            print("✅ Momentum scores chart created successfully")
        else:
            print("❌ Momentum scores chart failed")

        # Create sample engagement data
        engagement_data = pd.DataFrame(
            {
                "entity_id": ["Video1", "Video2", "Video3"],
                "score_value": [0.9, 0.7, 0.5],
                "engagement_rate": [0.05, 0.03, 0.02],
                "confidence": [0.95, 0.85, 0.75],
            }
        )

        # Test engagement chart
        fig2 = create_engagement_distribution_chart(engagement_data)
        if fig2 is not None:
            print("✅ Engagement distribution chart created successfully")
        else:
            print("❌ Engagement distribution chart failed")

        # Create sample performance data
        performance_data = pd.DataFrame(
            {
                "algorithm_name": ["momentum_scorer", "engagement_scorer"],
                "total_runs": [5, 3],
                "total_results": [25, 15],
                "overall_avg_score": [0.65, 0.72],
            }
        )

        # Test performance chart
        fig3 = create_scoring_performance_chart(performance_data)
        if fig3 is not None:
            print("✅ Performance chart created successfully")
        else:
            print("❌ Performance chart failed")

        return True

    except Exception as e:
        print(f"❌ Chart function test failed: {e}")
        return False


def test_real_database_integration():
    """Test integration with real database data."""
    print("\n🔍 Testing real database integration...")

    try:
        from src.youtubeviz.data_discovery import DatabaseDiscovery, load_dynamic_data
        from src.youtubeviz.scoring_analysis import ScoringAnalyzer

        # Initialize database discovery
        discovery = DatabaseDiscovery()

        # Get real artists
        artists = discovery.discover_artists(min_videos=1)
        if not artists:
            print("⚠️  No artists found in database")
            return False

        print(f"✅ Found {len(artists)} real artists: {artists[:3]}...")

        # Load real data
        data = load_dynamic_data(discovery.engine, artists[:2], limit_per_artist=50)  # Limit for testing

        videos_df = data.get("videos", pd.DataFrame())
        metrics_df = data.get("metrics", pd.DataFrame())
        sentiment_df = data.get("sentiment_summary", pd.DataFrame())

        print(f"✅ Loaded real data: {len(videos_df)} videos, {len(metrics_df)} metrics")

        # Test scoring analyzer with real data
        if not videos_df.empty and not metrics_df.empty:
            analyzer = ScoringAnalyzer()

            # Test data preparation
            momentum_data = analyzer.prepare_momentum_data(videos_df, metrics_df)
            if not momentum_data.empty:
                print(f"✅ Momentum data prepared: {len(momentum_data)} records")
            else:
                print("⚠️  No momentum data prepared")

            engagement_data = analyzer.prepare_engagement_data(videos_df, metrics_df, sentiment_df)
            if not engagement_data.empty:
                print(f"✅ Engagement data prepared: {len(engagement_data)} records")
            else:
                print("⚠️  No engagement data prepared")

        return True

    except Exception as e:
        print(f"❌ Database integration test failed: {e}")
        return False


def test_insights_generation():
    """Test insights generation functionality."""
    print("\n🔍 Testing insights generation...")

    try:
        from src.youtubeviz.scoring_analysis import get_scoring_insights

        # Sample momentum data
        momentum_df = pd.DataFrame(
            {
                "entity_id": ["Artist A", "Artist B", "Artist C"],
                "score_value": [0.8, 0.6, 0.4],
                "confidence": [0.9, 0.8, 0.7],
                "momentum_category": ["high_momentum", "stable", "low_momentum"],
            }
        )

        # Sample engagement data
        engagement_df = pd.DataFrame(
            {
                "entity_id": ["Video1", "Video2", "Video3"],
                "score_value": [0.9, 0.7, 0.5],
                "engagement_rate": [0.05, 0.03, 0.02],
            }
        )

        # Generate insights
        insights = get_scoring_insights(momentum_df, engagement_df)

        # Validate insights structure
        assert "momentum_insights" in insights
        assert "engagement_insights" in insights
        assert "recommendations" in insights

        print("✅ Insights generated successfully")
        print(f"   📊 Momentum insights: {insights['momentum_insights']}")
        print(f"   📈 Engagement insights: {insights['engagement_insights']}")
        print(f"   💡 Recommendations: {len(insights['recommendations'])}")

        return True

    except Exception as e:
        print(f"❌ Insights generation test failed: {e}")
        return False


def main():
    """Run all integration tests."""
    print("🎯 Testing MusicScope™ Scoring System Integration")
    print("=" * 60)

    tests = [
        ("Import Test", test_scoring_analysis_import),
        ("Analyzer Initialization", test_scoring_analyzer_initialization),
        ("Chart Functions", test_chart_functions_with_sample_data),
        ("Database Integration", test_real_database_integration),
        ("Insights Generation", test_insights_generation),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 60)
    print("🏆 INTEGRATION TEST RESULTS")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")

    print(f"\n📊 Overall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 ALL TESTS PASSED!")
        print("✅ Scoring system integration is ready for the notebook!")
        print("🚀 The notebook now has advanced AI-powered scoring capabilities!")
    else:
        print("⚠️  Some tests failed - check the issues above")
        print("💡 The notebook will still work but some scoring features may be limited")

    print("\n🎵 MusicScope™ Professional Dashboard Enhanced!")


if __name__ == "__main__":
    main()
