#!/usr/bin/env python3
"""
Database Cleanup Script

Removes all data for unwanted YouTube channels from the database.
Only keeps data for the specified music artists.

Usage: python cleanup_db.py [--dry-run] [--confirm]
"""

import os
import sys
from typing import Any, Dict, List

import pymysql
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Channels to KEEP (based on .env file)
KEEP_CHANNELS = ["COBRAH", "Enchanting", "Flyana Boss"]

# Channels to DELETE
DELETE_CHANNELS = [
    "All-In Podcast",  # Podcast data - unwanted
    "LuvEnchantingINC",  # Secondary Enchanting channel
    "Oh its just arty",  # Unknown/irrelevant
]


def get_db_connection():
    """Get database connection."""
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
    )


def get_channel_video_ids(cursor, channel_title: str) -> List[str]:
    """Get all video IDs for a specific channel."""
    cursor.execute("SELECT video_id FROM youtube_videos WHERE channel_title = %s", (channel_title,))
    return [row[0] for row in cursor.fetchall()]


def cleanup_channel_data(cursor, channel_title: str, dry_run: bool = True) -> Dict[str, int]:
    """Clean up all data for a specific channel."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Cleaning up data for channel: {channel_title}")

    # Get video IDs for this channel
    video_ids = get_channel_video_ids(cursor, channel_title)
    if not video_ids:
        print(f"  No videos found for {channel_title}")
        return {"videos": 0, "comments": 0, "metrics": 0, "sentiment": 0}

    video_ids_tuple = tuple(video_ids)

    stats = {"videos": 0, "comments": 0, "metrics": 0, "sentiment": 0}

    # Delete from youtube_comments
    if len(video_ids) == 1:
        cursor.execute("SELECT COUNT(*) FROM youtube_comments WHERE video_id = %s", (video_ids[0],))
    else:
        cursor.execute(f"SELECT COUNT(*) FROM youtube_comments WHERE video_id IN {video_ids_tuple}")
    stats["comments"] = cursor.fetchone()[0]

    if not dry_run:
        if len(video_ids) == 1:
            cursor.execute("DELETE FROM youtube_comments WHERE video_id = %s", (video_ids[0],))
        else:
            cursor.execute(f"DELETE FROM youtube_comments WHERE video_id IN {video_ids_tuple}")
    print(f"  Comments: {stats['comments']} records")

    # Delete from youtube_metrics
    if len(video_ids) == 1:
        cursor.execute("SELECT COUNT(*) FROM youtube_metrics WHERE video_id = %s", (video_ids[0],))
    else:
        cursor.execute(f"SELECT COUNT(*) FROM youtube_metrics WHERE video_id IN {video_ids_tuple}")
    stats["metrics"] = cursor.fetchone()[0]

    if not dry_run:
        if len(video_ids) == 1:
            cursor.execute("DELETE FROM youtube_metrics WHERE video_id = %s", (video_ids[0],))
        else:
            cursor.execute(f"DELETE FROM youtube_metrics WHERE video_id IN {video_ids_tuple}")
    print(f"  Metrics: {stats['metrics']} records")

    # Delete from sentiment tables
    for table in ["youtube_sentiment", "youtube_sentiment_by_video", "youtube_sentiment_summary"]:
        try:
            if len(video_ids) == 1:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE video_id = %s", (video_ids[0],))
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE video_id IN {video_ids_tuple}")
            count = cursor.fetchone()[0]
            stats["sentiment"] += count

            if not dry_run:
                if len(video_ids) == 1:
                    cursor.execute(f"DELETE FROM {table} WHERE video_id = %s", (video_ids[0],))
                else:
                    cursor.execute(f"DELETE FROM {table} WHERE video_id IN {video_ids_tuple}")
        except Exception as e:
            print(f"  Warning: Could not access {table}: {e}")

    # Delete from youtube_videos (last, because of foreign keys)
    cursor.execute("SELECT COUNT(*) FROM youtube_videos WHERE channel_title = %s", (channel_title,))
    stats["videos"] = cursor.fetchone()[0]

    if not dry_run:
        cursor.execute("DELETE FROM youtube_videos WHERE channel_title = %s", (channel_title,))
    print(f"  Videos: {stats['videos']} records")

    return stats


def main():
    """Main cleanup function."""
    dry_run = "--dry-run" in sys.argv
    confirm = "--confirm" in sys.argv

    print("=== YouTube ETL Database Cleanup ===")
    print(f"Mode: {'DRY RUN (no changes will be made)' if dry_run else 'LIVE CLEANUP'}")
    print(f"Channels to KEEP: {', '.join(KEEP_CHANNELS)}")
    print(f"Channels to DELETE: {', '.join(DELETE_CHANNELS)}")

    if not dry_run and not confirm:
        response = input("\n⚠️  WARNING: This will permanently delete data! Type 'YES' to continue: ")
        if response != "YES":
            print("Cleanup cancelled.")
            return

    conn = get_db_connection()
    cursor = conn.cursor()

    total_stats = {"videos": 0, "comments": 0, "metrics": 0, "sentiment": 0}

    try:
        # Clean up each unwanted channel
        for channel in DELETE_CHANNELS:
            stats = cleanup_channel_data(cursor, channel, dry_run)
            for key in total_stats:
                total_stats[key] += stats[key]

        print("\n=== CLEANUP SUMMARY ===")
        print(f"Total videos to delete: {total_stats['videos']}")
        print(f"Total comments to delete: {total_stats['comments']}")
        print(f"Total metrics to delete: {total_stats['metrics']}")
        print(f"Total sentiment records to delete: {total_stats['sentiment']}")

        if not dry_run:
            conn.commit()
            print("\n✅ Cleanup completed successfully!")
        else:
            print("\n🔍 Dry run completed. Use --confirm to perform actual cleanup.")

    except Exception as e:
        print(f"\n❌ Error during cleanup: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
