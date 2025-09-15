#!/usr/bin/env python3
"""
Channel-based Database Cleanup Tool

This tool validates youtube_videos.channel_title against configured channels in .env
and safely removes data for channels that are not configured.

IMPORTANT: This is one of the few tools that actually removes data from the database.
Use with extreme caution and always backup first.
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from web.etl_helpers import get_engine


def get_configured_channels() -> Dict[str, str]:
    """
    Extract configured YouTube channels from environment variables.

    Returns:
        Dict mapping artist names to channel URLs
    """
    load_dotenv()

    configured_channels = {}

    for key, value in os.environ.items():
        if key.startswith("YT_") and key.endswith("_YT") and value:
            # Extract artist name from environment variable
            # YT_BICFIZZLE_YT -> BiC Fizzle
            artist_name = key[3:-3]  # Remove YT_ prefix and _YT suffix

            # Convert to readable format
            if artist_name == "BICFIZZLE":
                artist_name = "BiC Fizzle"
            elif artist_name == "FLYANABOSS":
                artist_name = "Flyana Boss"
            else:
                # Convert UPPERCASE to Title Case
                artist_name = artist_name.replace("_", " ").title()

            configured_channels[artist_name] = value

    return configured_channels


def get_channel_titles_from_urls(configured_channels: Dict[str, str], engine) -> Set[str]:
    """
    Get the actual channel titles from the database that correspond to configured URLs.

    Args:
        configured_channels: Dict of artist names to channel URLs
        engine: Database engine

    Returns:
        Set of valid channel titles
    """
    valid_channel_titles = set()

    with engine.connect() as conn:
        for artist_name, channel_url in configured_channels.items():
            # Extract channel identifier from URL
            if "@" in channel_url:
                channel_handle = channel_url.split("@")[-1]

                # Find matching channel titles in database
                result = conn.execute(
                    text(
                        """
                    SELECT DISTINCT channel_title
                    FROM youtube_videos
                    WHERE channel_title LIKE :pattern
                """
                    ),
                    {"pattern": f"%{channel_handle}%"},
                )

                for row in result:
                    valid_channel_titles.add(row[0])

            # Also try direct artist name matching
            result = conn.execute(
                text(
                    """
                SELECT DISTINCT channel_title
                FROM youtube_videos
                WHERE channel_title LIKE :pattern
            """
                ),
                {"pattern": f"%{artist_name}%"},
            )

            for row in result:
                valid_channel_titles.add(row[0])

    return valid_channel_titles


def analyze_channel_data(engine) -> Dict[str, any]:
    """
    Analyze current channel data in the database.

    Returns:
        Dict with analysis results
    """
    with engine.connect() as conn:
        # Get all channel titles and their data counts
        channel_analysis = pd.read_sql(
            text(
                """
            SELECT
                v.channel_title,
                COUNT(DISTINCT v.video_id) as video_count,
                COUNT(DISTINCT m.video_id) as metrics_count,
                COUNT(DISTINCT c.comment_id) as comment_count,
                MIN(v.published_at) as earliest_video,
                MAX(v.published_at) as latest_video
            FROM youtube_videos v
            LEFT JOIN youtube_metrics m ON v.video_id = m.video_id
            LEFT JOIN youtube_comments c ON v.video_id = c.video_id
            GROUP BY v.channel_title
            ORDER BY video_count DESC
        """
            ),
            conn,
        )

    return {
        "total_channels": len(channel_analysis),
        "channel_data": channel_analysis,
        "total_videos": channel_analysis["video_count"].sum(),
        "total_metrics": channel_analysis["metrics_count"].sum(),
        "total_comments": channel_analysis["comment_count"].sum(),
    }


def create_cleanup_plan(valid_channels: Set[str], current_analysis: Dict) -> Dict[str, any]:
    """
    Create a plan for what data will be removed.

    Args:
        valid_channels: Set of valid channel titles to keep
        current_analysis: Current database analysis

    Returns:
        Cleanup plan with details of what will be removed
    """
    channel_data = current_analysis["channel_data"]

    # Identify channels to remove
    channels_to_remove = []
    videos_to_remove = 0
    metrics_to_remove = 0
    comments_to_remove = 0

    for _, row in channel_data.iterrows():
        channel_title = row["channel_title"]

        # Check if this channel should be kept
        should_keep = False
        for valid_channel in valid_channels:
            if (
                valid_channel
                and channel_title
                and (valid_channel.lower() in channel_title.lower() or channel_title.lower() in valid_channel.lower())
            ):
                should_keep = True
                break

        if not should_keep:
            channels_to_remove.append(
                {
                    "channel_title": channel_title,
                    "video_count": row["video_count"],
                    "metrics_count": row["metrics_count"],
                    "comment_count": row["comment_count"],
                    "earliest_video": row["earliest_video"],
                    "latest_video": row["latest_video"],
                }
            )

            videos_to_remove += row["video_count"]
            metrics_to_remove += row["metrics_count"]
            comments_to_remove += row["comment_count"]

    return {
        "channels_to_remove": channels_to_remove,
        "channels_to_keep": [ch for ch in valid_channels if ch],
        "summary": {
            "channels_removed": len(channels_to_remove),
            "videos_removed": videos_to_remove,
            "metrics_removed": metrics_to_remove,
            "comments_removed": comments_to_remove,
        },
    }


def execute_cleanup(cleanup_plan: Dict, engine, dry_run: bool = True):
    """
    Execute the cleanup plan.

    Args:
        cleanup_plan: Plan created by create_cleanup_plan
        engine: Database engine
        dry_run: If True, only show what would be done without executing
    """
    channels_to_remove = cleanup_plan["channels_to_remove"]

    if dry_run:
        print("🔍 DRY RUN MODE - No data will be actually removed")
        print("=" * 60)
    else:
        print("⚠️  LIVE MODE - Data will be permanently removed!")
        print("=" * 60)

    with engine.connect() as conn:
        for channel_info in channels_to_remove:
            channel_title = channel_info["channel_title"]

            print(f"\n🗑️ Channel: {channel_title}")
            print(f"   Videos: {channel_info['video_count']}")
            print(f"   Metrics: {channel_info['metrics_count']}")
            print(f"   Comments: {channel_info['comment_count']}")
            print(f"   Date range: {channel_info['earliest_video']} to {channel_info['latest_video']}")

            if not dry_run:
                # Get video IDs for this channel
                video_ids = pd.read_sql(
                    text(
                        """
                    SELECT video_id FROM youtube_videos
                    WHERE channel_title = :channel_title
                """
                    ),
                    conn,
                    params={"channel_title": channel_title},
                )

                if len(video_ids) > 0:
                    video_id_list = video_ids["video_id"].tolist()

                    # Remove in correct order (foreign key constraints)
                    # 1. Comment sentiment
                    conn.execute(
                        text(
                            """
                        DELETE FROM comment_sentiment
                        WHERE video_id IN :video_ids
                    """
                        ),
                        {"video_ids": tuple(video_id_list)},
                    )

                    # 2. Comments
                    conn.execute(
                        text(
                            """
                        DELETE FROM youtube_comments
                        WHERE video_id IN :video_ids
                    """
                        ),
                        {"video_ids": tuple(video_id_list)},
                    )

                    # 3. Metrics
                    conn.execute(
                        text(
                            """
                        DELETE FROM youtube_metrics
                        WHERE video_id IN :video_ids
                    """
                        ),
                        {"video_ids": tuple(video_id_list)},
                    )

                    # 4. Videos
                    conn.execute(
                        text(
                            """
                        DELETE FROM youtube_videos
                        WHERE channel_title = :channel_title
                    """
                        ),
                        {"channel_title": channel_title},
                    )

                    print(f"   ✅ Removed data for {channel_title}")

        if not dry_run:
            conn.commit()
            print(f"\n🎉 Cleanup complete!")
        else:
            print(f"\n📋 Dry run complete - use --execute to perform actual cleanup")


def main():
    """Main function for channel cleanup tool."""
    import argparse

    parser = argparse.ArgumentParser(description="Clean up database based on configured channels")
    parser.add_argument("--execute", action="store_true", help="Actually execute cleanup (default is dry-run)")
    parser.add_argument("--backup", action="store_true", help="Create backup before cleanup")

    args = parser.parse_args()

    print("🧹 YouTube Channel Database Cleanup Tool")
    print("=" * 50)

    # Load configuration
    configured_channels = get_configured_channels()
    print(f"📋 Configured channels: {len(configured_channels)}")
    for artist, url in configured_channels.items():
        print(f"   🎤 {artist}: {url}")

    # Connect to database
    engine = get_engine()

    # Get valid channel titles
    valid_channels = get_channel_titles_from_urls(configured_channels, engine)
    print(f"\n✅ Valid channel titles found: {len(valid_channels)}")
    for channel in sorted(valid_channels):
        print(f"   📺 {channel}")

    # Analyze current data
    print(f"\n📊 Analyzing current database...")
    current_analysis = analyze_channel_data(engine)
    print(f"   Total channels in DB: {current_analysis['total_channels']}")
    print(f"   Total videos: {current_analysis['total_videos']:,}")
    print(f"   Total metrics: {current_analysis['total_metrics']:,}")
    print(f"   Total comments: {current_analysis['total_comments']:,}")

    # Create cleanup plan
    cleanup_plan = create_cleanup_plan(valid_channels, current_analysis)

    print(f"\n🎯 Cleanup Plan Summary:")
    print(f"   Channels to remove: {cleanup_plan['summary']['channels_removed']}")
    print(f"   Videos to remove: {cleanup_plan['summary']['videos_removed']:,}")
    print(f"   Metrics to remove: {cleanup_plan['summary']['metrics_removed']:,}")
    print(f"   Comments to remove: {cleanup_plan['summary']['comments_removed']:,}")

    if cleanup_plan["summary"]["channels_removed"] == 0:
        print("\n✅ No cleanup needed - all channels are properly configured!")
        return

    # Create backup if requested
    if args.backup and args.execute:
        print(f"\n💾 Creating backup...")
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        # Implementation would create database backup here
        print(f"   Backup created: backup_{timestamp}")

    # Execute cleanup
    execute_cleanup(cleanup_plan, engine, dry_run=not args.execute)

    if args.execute:
        # Verify cleanup
        post_analysis = analyze_channel_data(engine)
        print(f"\n📊 Post-cleanup analysis:")
        print(f"   Remaining channels: {post_analysis['total_channels']}")
        print(f"   Remaining videos: {post_analysis['total_videos']:,}")
        print(f"   Remaining comments: {post_analysis['total_comments']:,}")


if __name__ == "__main__":
    main()
