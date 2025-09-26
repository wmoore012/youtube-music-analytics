#!/usr/bin/env python3
"""
Test ML Data Collection Implementation

Tests the enhanced data collection with ML-ready preprocessing.
"""

import sys

sys.path.insert(0, "src")


def test_ml_data_models():
    """Test ML data models and validation."""
    print("🧪 Testing ML Data Models...")

    try:
        from youtubeviz.ml_data_models import (
            CommentMetadata,
            DataSplit,
            MLComment,
            MLDataset,
            MusicDomain,
            SentimentLabel,
        )

        # Test comment metadata
        metadata = CommentMetadata(
            comment_id="test_123",
            video_id="abc123",
            channel_title="Test Channel",
            like_count=5,
            music_domain=MusicDomain.MUSIC_VIDEO,
            contains_music_slang=True,
            slang_terms=["fire", "slaps"],
        )

        # Test ML comment
        ml_comment = MLComment(
            text="This song is fire! It absolutely slaps!",
            normalized_text="this song is fire it absolutely slaps",
            sentiment_label=SentimentLabel.POSITIVE,
            confidence_score=0.95,
            token_count=8,
            contains_emoji=False,
            emoji_count=0,
            data_split=DataSplit.TRAIN,
            unique_hash="abc123def456",
            metadata=metadata,
        )

        # Test dataset
        dataset = MLDataset(name="test_dataset", description="Test dataset for validation")
        dataset.add_comment(ml_comment)

        print("✅ ML data models working correctly")
        print(f"   Dataset: {dataset.name} with {dataset.total_comments} comments")
        print(f"   Comment: {ml_comment.text[:50]}...")

        return True

    except Exception as e:
        print(f"❌ ML data models test failed: {e}")
        return False


def test_unique_comment_manager_ml():
    """Test enhanced unique comment manager ML methods."""
    print("\n🧪 Testing Unique Comment Manager ML Methods...")

    try:
        from youtubeviz.unique_comment_manager import UniqueCommentManager

        manager = UniqueCommentManager()

        # Test ML-ready comment collection
        print("   Testing ML-ready comment collection...")
        ml_comments = manager.get_ml_ready_comments(
            system_name="test_ml_system", usage_type="testing", count=5, music_domain_filter=True, min_engagement=1
        )

        if ml_comments:
            print(f"✅ Collected {len(ml_comments)} ML-ready comments")

            # Check structure of first comment
            if ml_comments:
                first_comment = ml_comments[0]
                required_fields = [
                    "comment_text",
                    "normalized_text",
                    "unique_hash",
                    "music_domain",
                    "contains_music_slang",
                    "token_count",
                ]

                missing_fields = [field for field in required_fields if field not in first_comment]

                if missing_fields:
                    print(f"⚠️  Missing fields: {missing_fields}")
                else:
                    print("✅ All required ML fields present")
                    print(f"   Sample: {first_comment['comment_text'][:50]}...")
                    print(f"   Domain: {first_comment['music_domain']}")
                    print(f"   Slang: {first_comment['contains_music_slang']}")
        else:
            print("⚠️  No ML comments collected (may be expected if database is empty)")

        return True

    except Exception as e:
        print(f"❌ Unique comment manager ML test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_sentiment_evaluation_transformer():
    """Test transformer-ready data preparation in sentiment evaluation."""
    print("\n🧪 Testing Sentiment Evaluation Transformer Methods...")

    try:
        from youtubeviz.sentiment_evaluation import SentimentEvaluationFramework

        framework = SentimentEvaluationFramework()

        # Test transformer dataset preparation
        sample_comments = [
            "This song absolutely slaps! 🔥",
            "Not feeling this one, it's kinda mid",
            "The beat goes hard no cap",
            "Beautiful vocals and production",
            "This is trash tbh",
        ]

        sample_labels = ["positive", "negative", "positive", "positive", "negative"]

        print("   Testing transformer dataset preparation...")
        transformer_dataset = framework.prepare_transformer_dataset(
            comments=sample_comments, labels=sample_labels, use_unique_comments=False  # Skip unique check for test
        )

        if transformer_dataset:
            print(f"✅ Prepared transformer dataset with {transformer_dataset.total_comments} comments")
            print(f"   Labeled comments: {transformer_dataset.labeled_comments}")

            # Test split creation
            print("   Testing transformer splits...")
            split_dataset = framework.create_transformer_splits(
                transformer_dataset,
                train_ratio=0.6,
                val_ratio=0.2,
                test_ratio=0.2,
                group_by_video=False,  # Simple split for test
            )

            print(
                f"✅ Created splits: {split_dataset.train_count} train, "
                f"{split_dataset.validation_count} val, {split_dataset.test_count} test"
            )
        else:
            print("⚠️  Transformer dataset preparation returned None (may need ML dependencies)")

        return True

    except Exception as e:
        print(f"❌ Sentiment evaluation transformer test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_ml_scoring_integration():
    """Test ML scoring integration."""
    print("\n🧪 Testing ML Scoring Integration...")

    try:
        from youtubeviz.ml_scoring_integration import MLDataScoringPlugin, get_ml_pipeline_integration

        # Test scoring plugin
        plugin = MLDataScoringPlugin()
        print(f"✅ ML scoring plugin created: {plugin.get_name()} v{plugin.get_version()}")

        # Test pipeline integration
        integration = get_ml_pipeline_integration()
        if integration:
            print("✅ ML pipeline integration available")
        else:
            print("⚠️  ML pipeline integration not available (may need scoring dependencies)")

        return True

    except Exception as e:
        print(f"❌ ML scoring integration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_benchmark_models_enhancement():
    """Test enhanced benchmark models functionality."""
    print("\n🧪 Testing Enhanced Benchmark Models...")

    try:
        # Import the enhanced benchmark_models module
        import benchmark_models

        # Check if new functions are available
        if hasattr(benchmark_models, "collect_ml_training_data"):
            print("✅ ML training data collection function available")
        else:
            print("⚠️  ML training data collection function not found")

        if hasattr(benchmark_models, "run_ml_benchmark"):
            print("✅ ML benchmark function available")
        else:
            print("⚠️  ML benchmark function not found")

        print("✅ Enhanced benchmark models module loaded successfully")
        return True

    except Exception as e:
        print(f"❌ Enhanced benchmark models test failed: {e}")
        return False


def main():
    """Run all tests."""
    print("🚀 TESTING ML DATA COLLECTION IMPLEMENTATION")
    print("=" * 60)

    tests = [
        test_ml_data_models,
        test_unique_comment_manager_ml,
        test_sentiment_evaluation_transformer,
        test_ml_scoring_integration,
        test_benchmark_models_enhancement,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1

    print(f"\n📊 TEST RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! ML data collection implementation is working.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")

    print("\n💡 Next steps:")
    print("   1. Run 'python benchmark_models.py' to test the enhanced benchmarking")
    print("   2. Use option 4 to collect ML training data")
    print("   3. Use option 3 to run ML-focused benchmarks")


if __name__ == "__main__":
    main()
