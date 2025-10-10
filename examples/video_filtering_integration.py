#!/usr / bin / env python3
"""
Example: Video Filtering Integration in ETL Pipeline

This example demonstrates how to integrate the video filtering system
into the YouTube ETL pipeline to filter problematic videos at the API level.

Key Features Demonstrated:
- Loading videos from YouTube API
- Applying video filters before database insertion
- Logging filtering decisions
- Handling personal issue videos
- Configuration-driven filtering
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from web.error_handling import get_error_handler
from web.models import YouTubeVideo
from web.video_filter import filter_videos_at_api_level, get_filter_engine


def simulate_youtube_api_response() -> List[dict]:
    """
    Simulate YouTube API response with mix of valid and problematic videos.

    In real implementation, this would be replaced with actual YouTube API calls.
    """
    return [
        {
            "video_id": "dQw4w9WgXcQ",  # This will be filtered as personal issue
            "title": "Rick Astley-Never Gonna Give You Up",
            "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
            "channel_title": "Rick Astley",
            "published_at": datetime(2009, 10, 25),
            "duration": "PT3M33S",
            "view_count": 1000000000,
            "like_count": 10000000,
            "comment_count": 1000000,
        },
        {
            "video_id": "oHg5SJYRHA0",  # Valid video
            "title": "RickRoll'D",
            "channel_id": "UC-9-kyTW8ZkZNDHQJ6FgpwQ",
            "channel_title": "Music Channel",
            "published_at": datetime(2020, 1, 1),
            "duration": "PT4M15S",
            "view_count": 50000,
            "like_count": 1000,
            "comment_count": 100,
        },
        {
            "video_id": "kJQP7kiw5Fk",  # Valid video
            "title": "Great Music Video",
            "channel_id": "UCsT0YIqwnpJCM-mx7-gSA4Q",
            "channel_title": "Artist Channel",
            "published_at": datetime(2021, 6, 15),
            "duration": "PT3M45S",
            "view_count": 25000,
            "like_count": 500,
            "comment_count": 50,
        },
        {
            "video_id": "jNQXAC9IVRw",  # This will be filtered as too short
            "title": "Short Clip",
            "channel_id": "UCsT0YIqwnpJCM-mx7-gSA4Q",
            "channel_title": "Artist Channel",
            "published_at": datetime(2022, 3, 10),
            "duration": "PT15S",  # 15 seconds-too short
            "view_count": 1000,
            "like_count": 10,
            "comment_count": 5,
        },
        {
            "video_id": "ScMzIvxBSi4",  # This will be filtered by title pattern
            "title": "SPAM CONTENT-Click Here for Free Money!!!",
            "channel_id": "UCsT0YIqwnpJCM-mx7-gSA4Q",
            "channel_title": "Spam Channel",
            "published_at": datetime(2023, 1, 1),
            "duration": "PT2M30S",
            "view_count": 100,
            "like_count": 1,
            "comment_count": 0,
        },
    ]


def validate_and_convert_videos(api_response: List[dict]) -> List[YouTubeVideo]:
    """
    Validate and convert API response to YouTubeVideo models.

    Args:
        api_response: Raw API response data

    Returns:
        List of validated YouTubeVideo objects
    """
    validated_videos = []
    error_handler = get_error_handler()

    for video_data in api_response:
        try:
            video = YouTubeVideo(**video_data)
            validated_videos.append(video)
        except Exception as e:
            # Log validation error but continue processing
            error_handler.handle_error(e, should_raise=False)
            print(f"⚠️ Skipping invalid video data: {video_data.get('video_id', 'unknown')}")

    return validated_videos


def setup_example_filter_config():
    """Set up example filter configuration via environment variables."""
    # Set up personal issue videos (the 4 problematic videos mentioned)
    os.environ["PERSONAL_ISSUE_VIDEO_IDS"] = "dQw4w9WgXcQ,fC7oUOUEEi4,9bZkp7q19f0,example123"

    # Set up other filter rules
    os.environ["BLOCKED_TITLE_PATTERNS"] = "spam.*content|clickbait|free.*money"
    os.environ["MIN_VIDEO_DURATION_SECONDS"] = "30"  # 30 seconds minimum
    os.environ["MAX_VIDEO_DURATION_SECONDS"] = "3600"  # 1 hour maximum

    print("📋 Example filter configuration set up:")
    print(f"   Personal issue videos: {os.environ['PERSONAL_ISSUE_VIDEO_IDS']}")
    print(f"   Blocked title patterns: {os.environ['BLOCKED_TITLE_PATTERNS']}")
    print(f"   Min duration: {os.environ['MIN_VIDEO_DURATION_SECONDS']}s")
    print(f"   Max duration: {os.environ['MAX_VIDEO_DURATION_SECONDS']}s")


def simulate_etl_pipeline():
    """
    Simulate the ETL pipeline with video filtering integration.

    This demonstrates the complete flow:
    1. Get videos from YouTube API
    2. Validate video data
    3. Apply filtering rules
    4. Process only the videos that pass filtering
    """
    print("🚀 Starting ETL Pipeline with Video Filtering")
    print("=" * 60)

    # Step 1: Set up filter configuration
    setup_example_filter_config()

    # Step 2: Simulate getting videos from YouTube API
    print(f"\n📡 Fetching videos from YouTube API...")
    api_response = simulate_youtube_api_response()
    print(f"   Retrieved {len(api_response)} videos from API")

    # Step 3: Validate video data
    print(f"\n✅ Validating video data...")
    validated_videos = validate_and_convert_videos(api_response)
    print(f"   {len(validated_videos)} videos passed validation")

    # Step 4: Apply video filtering
    print(f"\n🚫 Applying video filters...")
    passed_videos, filter_results = filter_videos_at_api_level(validated_videos)

    # Step 5: Show filtering results
    print(f"\n📊 Filtering Results:")
    print(f"   Total videos processed: {len(validated_videos)}")
    print(f"   Videos passed filtering: {len(passed_videos)}")
    print(f"   Videos filtered out: {len(validated_videos) - len(passed_videos)}")

    # Show details of filtered videos
    filtered_results = [r for r in filter_results if r.is_filtered]
    if filtered_results:
        print(f"\n🚫 Filtered Videos Details:")
        for result in filtered_results:
            print(f"   Video {result.video_id}: {result.reason} - {result.details}")

    # Show videos that will be processed
    if passed_videos:
        print(f"\n✅ Videos that will be processed:")
        for video in passed_videos:
            print(f"   {video.video_id}: {video.title}")

    # Step 6: Get filter engine statistics
    filter_engine = get_filter_engine()
    stats = filter_engine.get_stats()

    print(f"\n📈 Filter Engine Statistics:")
    print(f"   Filter rate: {stats.filter_rate:.1f}%")
    print(f"   Filter reasons breakdown:")
    for reason, count in stats.filter_reasons.items():
        print(f"     {reason}: {count}")

    # Step 7: Simulate database insertion (only for passed videos)
    print(f"\n💾 Simulating database insertion...")
    for video in passed_videos:
        # In real implementation, this would insert into database
        print(f"   Inserting video {video.video_id} into database")

    print(f"\n🎉 ETL Pipeline completed successfully!")
    print(f"   {len(passed_videos)} videos processed and stored")
    print(f"   {len(filtered_results)} problematic videos filtered out at API level")


def demonstrate_personal_issue_handling():
    """
    Demonstrate how personal issue videos are handled.

    This shows the specific handling of the "4 videos that need to be deleted
    every time we run ETL" mentioned in the requirements.
    """
    print("\n" + "=" * 60)
    print("🔧 Personal Issue Video Handling Demonstration")
    print("=" * 60)

    print(
        """
📝 Background:
The requirements mentioned "4 videos that need to be deleted every time we run ETL"
due to personal issues. Instead of deleting them from the database repeatedly,
we now filter them at the API level before they ever reach the database.

This approach:
✅ Prevents problematic videos from entering the system
✅ Reduces database operations and improves performance
✅ Provides clear logging of why videos are filtered
✅ Is configurable and maintainable
"""
    )

    # Show current personal issue video configuration
    filter_engine = get_filter_engine()
    personal_videos = filter_engine.personal_issue_videos

    print(f"🚫 Currently configured personal issue videos:")
    for video_id in personal_videos:
        print(f"   {video_id}")

    print(
        f"""
🔧 Configuration:
Personal issue videos can be configured in multiple ways:
1. config / personal_issue_videos.json file (recommended)
2. PERSONAL_ISSUE_VIDEO_IDS environment variable
3. Default fallback list in code

Current configuration source: Environment variable (for this example)
"""
    )


if __name__ == "__main__":
    # Run the complete demonstration
    simulate_etl_pipeline()
    demonstrate_personal_issue_handling()

    print(f"\n💡 Next Steps:")
    print(f"   1. Update config / personal_issue_videos.json with actual problematic video IDs")
    print(f"   2. Configure additional filter rules in .env file")
    print(f"   3. Integrate filter_videos_at_api_level() into your ETL pipeline")
    print(f"   4. Monitor filtering statistics and adjust rules as needed")
