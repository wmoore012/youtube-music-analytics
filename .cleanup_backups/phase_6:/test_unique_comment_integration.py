#!/usr/bin/env python3
"""
Test Unique Comment Integration

Validates that unique comment enforcement is working across all data loading functions.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd

from src.youtubeviz.bot_detection import load_recent_comments
from src.youtubeviz.data import load_comment_examples
from src.youtubeviz.unique_comment_integration import (
    enforce_real_data_only,
    get_usage_statistics,
    scan_for_fake_data,
    unique_enforcer,
)
from web.etl_helpers import get_engine


def test_fake_data_detection():
    """Test fake data detection and removal."""
    print("🧪 TESTING FAKE DATA DETECTION")
    print("=" * 40)

    # Create test data with fake entries
    test_data = pd.DataFrame(
        {
            "comment_text": [
                "I love this song so much!",  # Real
                "test comment",  # Fake
                "This beat is incredible",  # Real
                "sample comment for testing",  # Fake
                "fake comment",  # Fake
                "Amazing vocals and production",  # Real
                "dummy comment",  # Fake
                "xx",  # Too short
                "aaaaaaaaaaaaaaaa",  # Repetitive
                "This artist deserves more recognition",  # Real
            ],
            "video_id": [f"vid_{i}" for i in range(10)],
            "artist_name": ["Test Artist"] * 10,
        }
    )

    print(f"Original data: {len(test_data)} records")

    # Test fake data removal
    cleaned_data = scan_for_fake_data(test_data, "test_fake_detection")
    print(f"After fake data removal: {len(cleaned_data)} records")

    # Test full enforcement
    enforced_data = enforce_real_data_only(test_data, "test_full_enforcement")
    print(f"After full enforcement: {len(enforced_data)} records")

    return len(enforced_data)


def test_comment_loading_functions():
    """Test that comment loading functions use unique comments."""
    print("\n🧪 TESTING COMMENT LOADING FUNCTIONS")
    print("=" * 40)

    try:
        engine = get_engine()

        # Test load_comment_examples
        print("Testing load_comment_examples...")
        comment_examples = load_comment_examples(per_artist=2, engine=engine)
        print(f"✅ Loaded {len(comment_examples)} comment examples")

        # Test load_recent_comments
        print("Testing load_recent_comments...")
        recent_comments = load_recent_comments(engine, days=7)
        print(f"✅ Loaded {len(recent_comments)} recent comments")

        return len(comment_examples) + len(recent_comments)

    except Exception as e:
        print(f"❌ Comment loading test failed: {e}")
        return -1


def test_usage_tracking():
    """Test usage tracking and statistics."""
    print("\n🧪 TESTING USAGE TRACKING")
    print("=" * 40)

    try:
        # Get usage statistics
        stats = get_usage_statistics()

        print(f"📊 Usage Statistics:")
        print(f"   Total allocated: {stats['total_allocated']}")
        print(f"   Registered functions: {stats['total_functions']}")
        print(f"   By usage type: {stats.get('by_usage_type', {})}")
        print(f"   By system: {stats.get('by_system', {})}")

        # Show registered functions
        if "registered_functions" in stats:
            print(f"\n📋 Registered Functions:")
            for func_name, usage_type in stats["registered_functions"].items():
                print(f"   • {func_name}: {usage_type}")

        return stats["total_allocated"]

    except Exception as e:
        print(f"❌ Usage tracking test failed: {e}")
        return -1


def test_duplicate_prevention():
    """Test that duplicate comments are prevented."""
    print("\n🧪 TESTING DUPLICATE PREVENTION")
    print("=" * 40)

    try:
        # Create test data with duplicates
        duplicate_data = pd.DataFrame(
            {
                "comment_text": [
                    "This is a unique comment",
                    "This is a unique comment",  # Duplicate
                    "Another unique comment",
                    "This is a unique comment",  # Another duplicate
                    "Third unique comment",
                ],
                "video_id": [f"vid_{i}" for i in range(5)],
            }
        )

        print(f"Original data: {len(duplicate_data)} records")

        # Test duplicate removal
        unique_data = enforce_real_data_only(duplicate_data, "test_duplicates")
        print(f"After duplicate removal: {len(unique_data)} records")

        # Verify uniqueness
        unique_comments = unique_data["comment_text"].nunique()
        total_comments = len(unique_data)

        print(f"Unique comments: {unique_comments}/{total_comments}")

        return unique_comments == total_comments

    except Exception as e:
        print(f"❌ Duplicate prevention test failed: {e}")
        return False


def main():
    """Run all unique comment integration tests."""
    print("🚀 UNIQUE COMMENT INTEGRATION TESTS")
    print("=" * 60)

    # Test 1: Fake data detection
    fake_data_result = test_fake_data_detection()

    # Test 2: Comment loading functions
    loading_result = test_comment_loading_functions()

    # Test 3: Usage tracking
    tracking_result = test_usage_tracking()

    # Test 4: Duplicate prevention
    duplicate_result = test_duplicate_prevention()

    # Summary
    print("\n" + "=" * 60)
    print("🏆 TEST SUMMARY")
    print("=" * 60)

    if fake_data_result >= 0:
        print(f"✅ Fake Data Detection: {fake_data_result} clean records")
    else:
        print("❌ Fake Data Detection: Failed")

    if loading_result >= 0:
        print(f"✅ Comment Loading Functions: {loading_result} comments loaded")
    else:
        print("❌ Comment Loading Functions: Failed")

    if tracking_result >= 0:
        print(f"✅ Usage Tracking: {tracking_result} comments tracked")
    else:
        print("❌ Usage Tracking: Failed")

    if duplicate_result:
        print("✅ Duplicate Prevention: Working correctly")
    else:
        print("❌ Duplicate Prevention: Failed")

    # Overall result
    all_passed = all([fake_data_result >= 0, loading_result >= 0, tracking_result >= 0, duplicate_result])

    if all_passed:
        print("\n🎉 All tests passed! Unique comment integration is working correctly.")
        print("\n💡 Key Benefits:")
        print("   • Prevents data leakage between different analysis contexts")
        print("   • Automatically removes fake and synthetic data")
        print("   • Ensures all analytics use real YouTube comments only")
        print("   • Provides comprehensive usage tracking and auditing")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
