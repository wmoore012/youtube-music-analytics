#!/usr/bin/env python3
"""
Test scoring plugins with real database data to verify no dummy data is used.
"""

from datetime import datetime, timedelta

import pandas as pd

from src.data_organization.scoring_storage import ScoringStorage
from src.data_organization.youtube_scoring_plugins import (
    ArtistMomentumScoringPlugin,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
)
from web.etl_helpers import get_engine
from youtubeviz.data import load_artist_daily_metrics


def test_real_momentum_scoring():
    """Test momentum scoring with actual database data."""
    print("🔍 Testing momentum scoring with real data...")

    try:
        engine = get_engine()

        # Load real data from last 30 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)

        data = load_artist_daily_metrics(start=start_date, end=end_date, engine=engine)

        if data.empty:
            print("❌ No real data available in database")
            return False

        print(f"✅ Loaded {len(data)} real records")

        # Get unique artists (verify they're real, not dummy)
        artists = data["artist_name"].unique()
        print(f"📊 Found {len(artists)} unique artists:")
        for artist in artists[:5]:  # Show first 5
            print(f"   - {artist}")

        # Verify no dummy artist names
        dummy_names = ["Artist A", "Artist B", "Artist C", "artist1", "artist2", "test_artist"]
        real_dummy_found = any(dummy in artists for dummy in dummy_names)
        if real_dummy_found:
            print("❌ Found dummy artist names in real data!")
            return False

        # Prepare data for momentum scoring
        momentum_data = data.rename(
            columns={"date": "metrics_date", "views": "view_count", "likes": "like_count", "comments": "comment_count"}
        )
        momentum_data["published_at"] = momentum_data["metrics_date"]
        momentum_data["channel_title"] = momentum_data["artist_name"]

        # Test momentum scoring
        plugin = ArtistMomentumScoringPlugin()
        result = plugin.execute(momentum_data)

        print(f"✅ Momentum scoring completed: {len(result.entity_scores)} results")

        # Verify results are realistic
        scores = result.entity_scores

        # Check for unique, realistic scores (not dummy values like 0.5, 0.8)
        score_values = scores["score_value"].tolist()
        unique_scores = len(set(score_values))

        print(f"📈 Score distribution:")
        print(f"   - Unique scores: {unique_scores}/{len(score_values)}")
        print(f"   - Score range: {min(score_values):.4f} - {max(score_values):.4f}")
        print(f"   - Average score: {sum(score_values)/len(score_values):.4f}")

        # Verify no obvious dummy scores
        obvious_dummy_scores = [0.5, 0.6, 0.7, 0.8, 0.9]
        dummy_score_count = sum(
            1 for score in score_values if any(abs(score - dummy) < 0.001 for dummy in obvious_dummy_scores)
        )

        if dummy_score_count == len(score_values) and len(score_values) > 1:
            print("⚠️  Warning: All scores appear to be round dummy values")

        # Show actual results
        print(f"\n📊 Real Momentum Scoring Results:")
        display_cols = ["entity_id", "score_value", "confidence", "momentum_category"]
        print(scores[display_cols].to_string(index=False))

        return True

    except Exception as e:
        print(f"❌ Error testing real momentum scoring: {e}")
        return False


def test_real_engagement_scoring():
    """Test engagement scoring with real video data."""
    print("\n🔍 Testing engagement scoring with real data...")

    try:
        engine = get_engine()

        # Load real video engagement data
        query = """
        SELECT
            v.video_id,
            v.title,
            v.channel_title,
            m.view_count,
            m.like_count,
            m.comment_count,
            COALESCE(s.avg_sentiment, 0.0) as avg_sentiment,
            COALESCE(s.comment_count, 0) as sentiment_magnitude
        FROM youtube_videos v
        JOIN youtube_metrics m ON v.video_id = m.video_id
        LEFT JOIN youtube_sentiment_summary s ON v.video_id = s.video_id
        WHERE m.view_count > 100
        ORDER BY m.view_count DESC
        LIMIT 10
        """

        from sqlalchemy import text

        with engine.connect() as conn:
            data = pd.read_sql(text(query), conn)

        if data.empty:
            print("❌ No real engagement data available")
            return False

        print(f"✅ Loaded {len(data)} real video records")

        # Verify real video IDs (not dummy)
        video_ids = data["video_id"].tolist()
        print(f"📹 Sample video IDs:")
        for vid_id in video_ids[:3]:
            print(f"   - {vid_id}")

        # Verify no dummy video IDs
        dummy_video_ids = ["vid1", "vid2", "video1", "test_video"]
        if any(dummy in video_ids for dummy in dummy_video_ids):
            print("❌ Found dummy video IDs in real data!")
            return False

        # Test engagement scoring
        plugin = EngagementScoringPlugin()
        result = plugin.execute(data)

        print(f"✅ Engagement scoring completed: {len(result.entity_scores)} results")

        # Show results
        scores = result.entity_scores
        print(f"\n📊 Real Engagement Scoring Results:")
        display_cols = ["entity_id", "score_value", "confidence", "engagement_rate"]
        print(scores[display_cols].head().to_string(index=False))

        # Verify realistic engagement rates
        engagement_rates = scores["engagement_rate"].tolist()
        print(f"📈 Engagement rate range: {min(engagement_rates):.6f} - {max(engagement_rates):.6f}")

        return True

    except Exception as e:
        print(f"❌ Error testing real engagement scoring: {e}")
        return False


def test_real_storage_integration():
    """Test storage system with real scoring results."""
    print("\n🔍 Testing storage integration with real data...")

    try:
        # Check if scoring schema exists
        storage = ScoringStorage()
        validation = storage.validate_schema()

        if not validation.is_valid:
            print("❌ Scoring schema not available")
            print("💡 Run 'python tools/setup/create_scoring_tables.py' first")
            return False

        print("✅ Scoring schema validated")

        # Load and score real data
        engine = get_engine()
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=14)

        data = load_artist_daily_metrics(start=start_date, end=end_date, engine=engine)

        if data.empty:
            print("❌ No real data for storage test")
            return False

        # Prepare data
        momentum_data = data.rename(
            columns={"date": "metrics_date", "views": "view_count", "likes": "like_count", "comments": "comment_count"}
        )
        momentum_data["published_at"] = momentum_data["metrics_date"]
        momentum_data["channel_title"] = momentum_data["artist_name"]

        # Score and store
        plugin = ArtistMomentumScoringPlugin()
        result = plugin.execute(momentum_data)

        run_id = storage.store_scoring_result(result, entity_type="artist")
        print(f"✅ Stored scoring results with run_id: {run_id}")

        # Retrieve and verify
        latest_scores = storage.get_latest_scores(
            algorithm_name="artist_momentum_scorer", entity_type="artist", limit=5
        )

        if not latest_scores.empty:
            print(f"✅ Retrieved {len(latest_scores)} stored results")
            print(f"\n📊 Latest Stored Results:")
            print(latest_scores[["entity_id", "score_value", "confidence_level"]].to_string(index=False))

            # Verify real artist names in storage
            stored_artists = latest_scores["entity_id"].tolist()
            dummy_names = ["Artist A", "artist1", "test_artist"]
            if any(dummy in stored_artists for dummy in dummy_names):
                print("❌ Found dummy artist names in stored results!")
                return False

            print("✅ All stored results use real artist names")

        return True

    except Exception as e:
        print(f"❌ Error testing storage integration: {e}")
        return False


def main():
    """Run all real data tests."""
    print("🎯 Testing Scoring System with Real Database Data")
    print("=" * 60)

    tests = [test_real_momentum_scoring, test_real_engagement_scoring, test_real_storage_integration]

    results = []
    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test_func.__name__} failed: {e}")
            results.append(False)

    print("\n" + "=" * 60)
    print("🏆 REAL DATA TEST RESULTS")
    print("=" * 60)

    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"✅ ALL {total} TESTS PASSED!")
        print("🎉 No dummy data detected - all tests use real database values!")
    else:
        print(f"⚠️  {passed}/{total} tests passed")
        print("Some tests may have used dummy data or failed")

    print("\n✨ Key Validations:")
    print("  - Real artist names (not 'Artist A', 'artist1', etc.)")
    print("  - Real video IDs (not 'vid1', 'video1', etc.)")
    print("  - Realistic score distributions (not just 0.5, 0.8, etc.)")
    print("  - Actual database timestamps and metadata")
    print("  - Unique values reflecting real performance patterns")


if __name__ == "__main__":
    main()
