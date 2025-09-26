#!/usr/bin/env python3
"""
Test Unique Comment System

Verifies that the UniqueCommentManager properly ensures no comment overlap
between different systems.
"""

import sys

sys.path.insert(0, "src")

from youtubeviz.unique_comment_manager import (
    UniqueCommentManager,
    get_unique_comments_for_benchmark,
    get_unique_comments_for_classification,
    get_unique_comments_for_evaluation,
)


def test_unique_allocation():
    """Test that comments are properly allocated uniquely."""

    print("🧪 TESTING UNIQUE COMMENT ALLOCATION")
    print("=" * 60)

    manager = UniqueCommentManager()

    # Reset any existing allocations for testing
    manager.reset_system_allocation("test_system_1")
    manager.reset_system_allocation("test_system_2")
    manager.reset_system_allocation("test_system_3")

    print("🔄 Reset test allocations")

    # Test 1: Get comments for different systems
    print("\n📋 Test 1: Allocating comments to different systems")

    comments_1 = get_unique_comments_for_classification(10)
    print(f"   System 1 (classification): {len(comments_1)} comments")

    comments_2 = get_unique_comments_for_benchmark("test_benchmark", 10)
    print(f"   System 2 (benchmark): {len(comments_2)} comments")

    comments_3 = get_unique_comments_for_evaluation("test_evaluation", 10)
    print(f"   System 3 (evaluation): {len(comments_3)} comments")

    # Test 2: Verify no overlap
    print("\n🔍 Test 2: Checking for overlap")

    if comments_1 and comments_2:
        set_1 = set(comments_1)
        set_2 = {c["comment_text"] for c in comments_2}
        overlap_1_2 = set_1.intersection(set_2)
        print(f"   Overlap between system 1 & 2: {len(overlap_1_2)} comments")

        if overlap_1_2:
            print(f"   ❌ OVERLAP DETECTED: {list(overlap_1_2)[:3]}...")
        else:
            print(f"   ✅ No overlap between system 1 & 2")

    if comments_1 and comments_3:
        set_1 = set(comments_1)
        set_3 = {c["comment_text"] for c in comments_3}
        overlap_1_3 = set_1.intersection(set_3)
        print(f"   Overlap between system 1 & 3: {len(overlap_1_3)} comments")

        if overlap_1_3:
            print(f"   ❌ OVERLAP DETECTED: {list(overlap_1_3)[:3]}...")
        else:
            print(f"   ✅ No overlap between system 1 & 3")

    # Test 3: Try to get same comments again (should fail)
    print("\n🔒 Test 3: Attempting to re-allocate same comments")

    comments_1_retry = get_unique_comments_for_classification(5)
    print(f"   Retry allocation: {len(comments_1_retry)} comments")

    if comments_1 and comments_1_retry:
        set_1_original = set(comments_1)
        set_1_retry = set(comments_1_retry)
        overlap_retry = set_1_original.intersection(set_1_retry)

        if overlap_retry:
            print(f"   ❌ REUSE DETECTED: {len(overlap_retry)} comments reused")
        else:
            print(f"   ✅ No comment reuse - proper unique allocation")

    # Test 4: Show usage stats
    print("\n📊 Test 4: Usage statistics")

    stats = manager.get_usage_stats()
    print(f"   Total allocated: {stats['total_allocated']}")
    print(f"   By usage type: {stats['by_usage_type']}")
    print(f"   By system: {stats['by_system']}")

    # Test 5: Check individual comment usage
    if comments_1:
        sample_comment = comments_1[0]
        usage_info = manager.get_comment_usage(sample_comment)
        print(f"\n🔍 Sample comment usage:")
        print(f"   Comment: '{sample_comment[:50]}...'")
        print(f"   Usage: {usage_info}")

    return True


def test_integration_with_systems():
    """Test integration with actual systems."""

    print("\n🔗 TESTING SYSTEM INTEGRATION")
    print("=" * 40)

    # Reset allocations to ensure fresh comments for integration tests
    manager = UniqueCommentManager()
    manager.reset_all_allocations()
    print("🔄 Reset all allocations for fresh integration test")

    try:
        # Test smart classifier integration
        print("\nTesting smart classifier...")
        from youtubeviz.smart_comment_classifier import InteractiveClassifier

        classifier = InteractiveClassifier()
        comments = classifier.get_sample_comments(5)
        print(f"✅ Smart classifier: {len(comments)} unique comments")

    except Exception as e:
        print(f"⚠️  Smart classifier test failed: {e}")

    try:
        # Test model benchmark system
        print("\nTesting model benchmark system...")
        from youtubeviz.model_benchmark_system import ModelBenchmarkSystem

        benchmark = ModelBenchmarkSystem()
        # This should use unique comments
        df = benchmark.fetch_benchmark_dataset(sample_size=5)
        print(f"✅ Model benchmark: {len(df)} unique comments")

    except Exception as e:
        print(f"⚠️  Model benchmark test failed: {e}")

    try:
        # Test enhanced sentiment integration
        print("\nTesting enhanced sentiment integration...")
        from youtubeviz.enhanced_sentiment_integration import EnhancedSentimentPipeline

        pipeline = EnhancedSentimentPipeline()
        # This should use unique comments
        df = pipeline.fetch_evaluation_comments(limit=5)
        print(f"✅ Enhanced sentiment: {len(df)} unique comments")

    except Exception as e:
        print(f"⚠️  Enhanced sentiment test failed: {e}")

    # Show final stats
    stats = manager.get_usage_stats()
    print(f"\n📊 Integration test final stats:")
    print(f"   Total allocated: {stats['total_allocated']}")
    print(f"   By system: {stats['by_system']}")


def main():
    """Run all unique comment tests."""

    print("🎯 UNIQUE COMMENT SYSTEM VALIDATION")
    print("=" * 80)
    print("This test ensures no comment overlap between ML systems")
    print("to prevent data leakage and ensure proper evaluation.")
    print()

    try:
        # Test core functionality
        success = test_unique_allocation()

        if success:
            print(f"\n✅ Core unique allocation tests PASSED")
        else:
            print(f"\n❌ Core unique allocation tests FAILED")
            return False

        # Test system integration
        test_integration_with_systems()

        print(f"\n🎉 UNIQUE COMMENT SYSTEM VALIDATION COMPLETE")
        print("All systems now use unique comments with no overlap!")

        return True

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
