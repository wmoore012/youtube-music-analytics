#!/usr/bin/env python3
"""
Enhanced Channel Cleanup Tool

This tool cleans the database based on channels configured in .env file.
It removes data for channels that are no longer being tracked.

IMPORTANT: This tool PERMANENTLY DELETES DATA. Use with caution.
"""

import os
import sys
from typing import List, Set

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def get_configured_channels() -> Set[str]:
    """
    Get list of channels configured in .env file.

    Returns:
        Set of channel titles that should be kept in database
    """
    load_dotenv()

    configured_channels = set()

    # Look for YT_*_YT environment variables
    for key, value in os.environ.items():
        if key.startswith("YT_") and key.endswith("_YT") and value:
            # Extract artist name from environment variable
            # YT_ARTISTNAME_YT -> ARTISTNAME
            artist_name = key[3:-3]  # Remove YT_ prefix and _YT suffix

            # Convert underscores to spaces and title case
            channel_title = artist_name.replace("_", " ").title()
            configured_channels.add(channel_title)

            print(f"📺 Found configured channel: {channel_title}")

    if not configured_channels:
        print("⚠️ No channels found in .env file!")
        print("   Make sure you have YT_ARTISTNAME_YT variables configured.")

    return configured_channels


def get_database_channels(engine) -> Set[str]:
    """
    Get list of channels currently in database.

    Returns:
        Set of channel titles found in database
    """
    with engine.connect() as conn:
        # Get unique channel titles from youtube_videos table
        result = conn.execute(
            text(
                """
            SELECT DISTINCT channel_title
            FROM youtube_videos
            WHERE channel_title IS NOT NULL
            ORDER BY channel_title
        """
            )
        )

        db_channels = {row[0] for row in result.fetchall()}

        print(f"\n📊 Found {len(db_channels)} unique channels in database:")
        for channel in sorted(db_channels):
            print(f"   - {channel}")

        return db_channels


def identify_channels_to_remove(configured: Set[str], database: Set[str]) -> Set[str]:
    """
    Identify channels that should be removed from database.

    Args:
        configured: Channels configured in .env
        database: Channels found in database

    Returns:
        Set of channel titles to remove
    """
    to_remove = database - configured

    if to_remove:
        print(f"\n🗑️ Channels to be REMOVED ({len(to_remove)}):")
        for channel in sorted(to_remove):
            print(f"   ❌ {channel}")
    else:
        print("\n✅ No channels need to be removed!")

    if configured - database:
        print(f"\n📝 Configured channels not yet in database ({len(configured - database)}):")
        for channel in sorted(configured - database):
            print(f"   ➕ {channel}")

    return to_remove


def get_removal_impact(engine, channels_to_remove: Set[str]) -> dict:
    """
    Calculate the impact of removing channels.

    Returns:
        Dict with counts of records that will be deleted
    """
    if not channels_to_remove:
        return {}

    impact = {}

    with engine.connect() as conn:
        # Format channel list for SQL IN clause
        channel_list = "', '".join(channels_to_remove)
        channel_filter = f"'{channel_list}'"

        # Count videos
        result = conn.execute(
            text(
                f"""
            SELECT COUNT(*) FROM youtube_videos
            WHERE channel_title IN ({channel_filter})
        """
            )
        )
        impact["videos"] = result.fetchone()[0]

        # Count comments (via video_id)
        result = conn.execute(
            text(
                f"""
            SELECT COUNT(*) FROM youtube_comments c
            JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE v.channel_title IN ({channel_filter})
        """
            )
        )
        impact["comments"] = result.fetchone()[0]

        # Count metrics
        result = conn.execute(
            text(
                f"""
            SELECT COUNT(*) FROM youtube_metrics m
            JOIN youtube_videos v ON m.video_id = v.video_id
            WHERE v.channel_title IN ({channel_filter})
        """
            )
        )
        impact["metrics"] = result.fetchone()[0]

        # Count sentiment records
        result = conn.execute(
            text(
                f"""
            SELECT COUNT(*) FROM comment_sentiment cs
            JOIN youtube_videos v ON cs.video_id = v.video_id
            WHERE v.channel_title IN ({channel_filter})
        """
            )
        )
        impact["sentiment"] = result.fetchone()[0]

    return impact


def confirm_deletion(channels_to_remove: Set[str], impact: dict) -> bool:
    """
    Get user confirmation for deletion with detailed impact information.

    Returns:
        True if user confirms deletion
    """
    if not channels_to_remove:
        return False

    print("\n" + "=" * 70)
    print("⚠️  PERMANENT DATA DELETION WARNING ⚠️")
    print("=" * 70)

    print(f"\nYou are about to PERMANENTLY DELETE data for {len(channels_to_remove)} channels:")
    for channel in sorted(channels_to_remove):
        print(f"   🗑️ {channel}")

    print(f"\nThis will delete:")
    print(f"   📹 {impact.get('videos', 0):,} videos")
    print(f"   💬 {impact.get('comments', 0):,} comments")
    print(f"   📊 {impact.get('metrics', 0):,} metrics records")
    print(f"   😊 {impact.get('sentiment', 0):,} sentiment records")

    total_records = sum(impact.values())
    print(f"\n   🔢 TOTAL: {total_records:,} database records")

    print("\n⚠️  THIS ACTION CANNOT BE UNDONE!")
    print("   Make sure you have a database backup if needed.")

    print("\nReasons for deletion:")
    print("   ✅ These channels are not configured in your .env file")
    print("   ✅ Keeping only actively tracked artists")
    print("   ✅ Reducing database size and improving performance")

    # Multiple confirmation steps
    print("\n" + "-" * 50)
    response1 = input("Do you understand this will permanently delete data? (yes/no): ").strip().lower()
    if response1 != "yes":
        print("❌ Deletion cancelled.")
        return False

    response2 = input(f"Type 'DELETE {len(channels_to_remove)} CHANNELS' to confirm: ").strip()
    expected = f"DELETE {len(channels_to_remove)} CHANNELS"
    if response2 != expected:
        print("❌ Deletion cancelled - confirmation text didn't match.")
        return False

    response3 = input("Final confirmation - type 'I UNDERSTAND' to proceed: ").strip()
    if response3 != "I UNDERSTAND":
        print("❌ Deletion cancelled - final confirmation failed.")
        return False

    return True


def perform_cleanup(engine, channels_to_remove: Set[str]) -> dict:
    """
    Perform the actual database cleanup.

    Returns:
        Dict with deletion results
    """
    if not channels_to_remove:
        return {}

    results = {}

    with engine.connect() as conn:
        # Format channel list for SQL
        channel_list = "', '".join(channels_to_remove)
        channel_filter = f"'{channel_list}'"

        print("\n🗑️ Starting database cleanup...")

        # Delete sentiment records first (foreign key dependency)
        print("   Deleting sentiment records...")
        result = conn.execute(
            text(
                f"""
            DELETE cs FROM comment_sentiment cs
            JOIN youtube_videos v ON cs.video_id = v.video_id
            WHERE v.channel_title IN ({channel_filter})
        """
            )
        )
        results["sentiment_deleted"] = result.rowcount
        print(f"   ✅ Deleted {result.rowcount:,} sentiment records")

        # Delete comments
        print("   Deleting comments...")
        result = conn.execute(
            text(
                f"""
            DELETE c FROM youtube_comments c
            JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE v.channel_title IN ({channel_filter})
        """
            )
        )
        results["comments_deleted"] = result.rowcount
        print(f"   ✅ Deleted {result.rowcount:,} comments")

        # Delete metrics
        print("   Deleting metrics...")
        result = conn.execute(
            text(
                f"""
            DELETE m FROM youtube_metrics m
            JOIN youtube_videos v ON m.video_id = v.video_id
            WHERE v.channel_title IN ({channel_filter})
        """
            )
        )
        results["metrics_deleted"] = result.rowcount
        print(f"   ✅ Deleted {result.rowcount:,} metrics records")

        # Delete videos (this will cascade to any remaining dependent records)
        print("   Deleting videos...")
        result = conn.execute(
            text(
                f"""
            DELETE FROM youtube_videos
            WHERE channel_title IN ({channel_filter})
        """
            )
        )
        results["videos_deleted"] = result.rowcount
        print(f"   ✅ Deleted {result.rowcount:,} videos")

        # Commit all changes
        conn.commit()

        print("\n🎉 Database cleanup completed successfully!")

        total_deleted = sum(results.values())
        print(f"📊 Total records deleted: {total_deleted:,}")

    return results


def main():
    """Main cleanup function."""
    print("🧹 Enhanced Channel Cleanup Tool")
    print("=" * 50)

    # Load environment and create database connection
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL not found in environment variables")
        sys.exit(1)

    engine = create_engine(database_url)

    try:
        # Step 1: Get configured channels from .env
        print("📋 Step 1: Reading channel configuration from .env...")
        configured_channels = get_configured_channels()

        if not configured_channels:
            print("❌ No channels configured. Exiting.")
            sys.exit(1)

        # Step 2: Get channels from database
        print("\n📋 Step 2: Reading channels from database...")
        database_channels = get_database_channels(engine)

        # Step 3: Identify channels to remove
        print("\n📋 Step 3: Identifying channels to remove...")
        channels_to_remove = identify_channels_to_remove(configured_channels, database_channels)

        if not channels_to_remove:
            print("\n✅ Database is already clean! No channels need to be removed.")
            return

        # Step 4: Calculate impact
        print("\n📋 Step 4: Calculating deletion impact...")
        impact = get_removal_impact(engine, channels_to_remove)

        # Step 5: Get confirmation
        print("\n📋 Step 5: Requesting user confirmation...")
        if not confirm_deletion(channels_to_remove, impact):
            print("\n❌ Cleanup cancelled by user.")
            return

        # Step 6: Perform cleanup
        print("\n📋 Step 6: Performing database cleanup...")
        results = perform_cleanup(engine, channels_to_remove)

        print("\n🎉 Channel cleanup completed successfully!")
        print("\nNext steps:")
        print("   1. Run data quality checks to verify cleanup")
        print("   2. Update sentiment analysis with remaining data")
        print("   3. Regenerate analytics notebooks")

    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
