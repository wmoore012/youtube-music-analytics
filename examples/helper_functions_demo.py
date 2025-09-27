#!/usr / bin / env python3
"""
Helper Functions Demonstration

This script demonstrates how to use the common helper functions in real scenarios.
Run this to see the new development standards in action.

Usage:
    python examples / helper_functions_demo.py
"""

import os
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.youtubeviz.common_helpers import (  # Database helpers; Validation helpers; Error handling helpers; Formatting helpers; File helpers; Date helpers
    clean_text_field,
    create_progress_bar,
    execute_query_safely,
    format_duration,
    format_number,
    format_percentage,
    get_current_timestamp,
    parse_youtube_timestamp,
    read_json_file,
    retry_operation,
    safe_divide,
    validate_required_fields,
    validate_youtube_id,
    write_json_file,
)


def demo_data_validation():
    """Demonstrate data validation helper functions."""
    print("🔍 DATA VALIDATION DEMO")
    print("-" * 40)

    # Sample video data
    video_data = {
        "video_id": "dQw4w9WgXcQ",
        "title": "Never Gonna Give You Up",
        "view_count": 1234567890,
        "like_count": 12345678,
        "comment_count": 123456,
        "description": "   Rick Astley's official music video   ",
    }

    # Validate required fields
    required_fields = ["video_id", "title", "view_count"]
    missing_fields = validate_required_fields(video_data, required_fields)

    if missing_fields:
        print(f"❌ Missing fields: {missing_fields}")
    else:
        print(f"✅ All required fields present: {required_fields}")

    # Validate YouTube ID
    video_id = video_data["video_id"]
    if validate_youtube_id(video_id, "video"):
        print(f"✅ Valid YouTube video ID: {video_id}")
    else:
        print(f"❌ Invalid YouTube video ID: {video_id}")

    # Clean text field
    original_description = video_data["description"]
    clean_description = clean_text_field(original_description, max_length=50)
    print(f"📝 Original: '{original_description}'")
    print(f"📝 Cleaned:  '{clean_description}'")

    print()


def demo_number_formatting():
    """Demonstrate number formatting helper functions."""
    print("📊 NUMBER FORMATTING DEMO")
    print("-" * 40)

    # Sample metrics
    view_count = 1234567890
    like_count = 12345678
    duration_seconds = 3661
    engagement_interactions = 500
    total_interactions = 10000

    # Format large numbers
    print(f"Views: {view_count:,} → {format_number(view_count)}")
    print(f"Likes: {like_count:,} → {format_number(like_count)}")

    # Format duration
    print(f"Duration: {duration_seconds}s → {format_duration(duration_seconds)}")

    # Format percentage
    engagement_rate = format_percentage(engagement_interactions, total_interactions)
    print(f"Engagement: {engagement_interactions}/{total_interactions} → {engagement_rate}")

    # Create progress bar
    progress = create_progress_bar(750, 1000, width=30)
    print(f"Progress: {progress}")

    print()


def demo_error_handling():
    """Demonstrate error handling helper functions."""
    print("🛡️ ERROR HANDLING DEMO")
    print("-" * 40)

    # Safe division examples
    print("Safe Division Examples:")
    print(f"10 ÷ 2 = {safe_divide(10, 2)}")
    print(f"10 ÷ 0 = {safe_divide(10, 0, default='N / A')}")
    print(f"10 ÷ 0 (default 0) = {safe_divide(10, 0, default=0)}")

    # Retry operation example
    print("\nRetry Operation Example:")

    attempt_count = 0

    def flaky_operation():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise ConnectionError(f"Attempt {attempt_count} failed")
        return f"Success on attempt {attempt_count}"

    try:
        result = retry_operation(flaky_operation, max_retries=3, delay=0.1)
        print(f"✅ {result}")
    except Exception as e:
        print(f"❌ All retries failed: {e}")

    print()


def demo_real_world_scenario():
    """Demonstrate a real - world scenario using multiple helpers."""
    print("🌍 REAL - WORLD SCENARIO DEMO")
    print("-" * 40)
    print("Processing YouTube video analytics data...")

    # Sample raw data (as might come from API)
    raw_video_data = [
        {
            "id": "dQw4w9WgXcQ",
            "snippet": {"title": "  Never Gonna Give You Up  ", "publishedAt": "2009 - 10 - 25T06:57:33Z"},
            "statistics": {"viewCount": "1234567890", "likeCount": "12345678", "commentCount": "123456"},
        },
        {
            "id": "invalid_id",  # This will cause validation error
            "snippet": {"title": "Invalid Video", "publishedAt": "2023 - 01 - 01T12:00:00Z"},
            "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "10"},
        },
    ]

    processed_videos = []

    for raw_video in raw_video_data:
        try:
            # Extract basic info
            video_id = raw_video["id"]
            title = raw_video["snippet"]["title"]
            published_at_str = raw_video["snippet"]["publishedAt"]

            # Validate video ID
            if not validate_youtube_id(video_id, "video"):
                print(f"⚠️ Skipping invalid video ID: {video_id}")
                continue

            # Parse statistics
            stats = raw_video["statistics"]
            view_count = int(stats["viewCount"])
            like_count = int(stats["likeCount"])
            comment_count = int(stats["commentCount"])

            # Calculate engagement rate
            total_interactions = like_count + comment_count
            engagement_rate = safe_divide(total_interactions, view_count) * 100

            # Clean and format data
            clean_title = clean_text_field(title, max_length=100)
            published_date = parse_youtube_timestamp(published_at_str)

            processed_video = {
                "video_id": video_id,
                "title": clean_title,
                "published_date": published_date.strftime("%Y-%m-%d") if published_date else "Unknown",
                "metrics": {
                    "views": format_number(view_count),
                    "likes": format_number(like_count),
                    "comments": format_number(comment_count),
                    "engagement_rate": f"{engagement_rate:.2f}%",
                },
            }

            processed_videos.append(processed_video)
            print(f"✅ Processed: {clean_title}")

        except Exception as e:
            print(f"❌ Error processing video {raw_video.get('id', 'unknown')}: {e}")
            continue

    print(f"\n📊 Successfully processed {len(processed_videos)} videos")

    # Display results
    for video in processed_videos:
        print(f"\n🎥 {video['title']}")
        print(f"   📅 Published: {video['published_date']}")
        print(f"   👀 Views: {video['metrics']['views']}")
        print(f"   👍 Likes: {video['metrics']['likes']}")
        print(f"   💬 Comments: {video['metrics']['comments']}")
        print(f"   📈 Engagement: {video['metrics']['engagement_rate']}")

    print()


def demo_file_operations():
    """Demonstrate file operation helpers."""
    print("📁 FILE OPERATIONS DEMO")
    print("-" * 40)

    # Create sample data
    sample_data = {
        "analysis_timestamp": get_current_timestamp().isoformat(),
        "video_count": 100,
        "total_views": 1000000,
        "avg_engagement": 5.2,
    }

    # Write JSON file
    temp_file = PROJECT_ROOT / "temp_demo_data.json"
    success = write_json_file(temp_file, sample_data)

    if success:
        print(f"✅ Successfully wrote data to {temp_file}")

        # Read JSON file back
        loaded_data = read_json_file(temp_file)
        if loaded_data:
            print(f"✅ Successfully read data back:")
            for key, value in loaded_data.items():
                print(f"   {key}: {value}")

        # Clean up
        temp_file.unlink()
        print(f"🧹 Cleaned up temporary file")
    else:
        print(f"❌ Failed to write data to {temp_file}")

    print()


def main():
    """Run all demonstrations."""
    print("🚀 HELPER FUNCTIONS DEMONSTRATION")
    print("=" * 60)
    print("This demo shows how to use the new development standards")
    print("and helper functions in real scenarios.")
    print()

    # Run all demos
    demo_data_validation()
    demo_number_formatting()
    demo_error_handling()
    demo_real_world_scenario()
    demo_file_operations()

    print("=" * 60)
    print("🎉 DEMONSTRATION COMPLETE")
    print()
    print("Key Takeaways:")
    print("✅ Helper functions reduce code duplication")
    print("✅ Validation helpers catch errors early")
    print("✅ Formatting helpers provide consistent output")
    print("✅ Error handling helpers make code more robust")
    print("✅ File helpers simplify common operations")
    print()
    print("Next Steps:")
    print("1. Review docs / DEVELOPMENT_STANDARDS.md")
    print("2. Try the exercises in docs / ONBOARDING_WORKSHOP.md")
    print("3. Use docs / QUICK_REFERENCE.md for daily development")
    print("4. Import helpers from src / youtubeviz / common_helpers.py")


if __name__ == "__main__":
    main()
