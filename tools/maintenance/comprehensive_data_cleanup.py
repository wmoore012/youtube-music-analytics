#!/usr/bin/env python3
"""
Comprehensive Data Cleanup Tool

This tool ensures the database contains ONLY the artists configured in .env
and removes ALL traces of unauthorized channels from ALL tables.

🚨 CRITICAL: This tool performs PERMANENT data deletion!
"""

import os
import sys
from typing import List, Set, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def get_configured_artists() -> Set[str]:
    """Get the list of artists configured in .env"""
    load_dotenv()

    # Mapping from .env names to expected database channel titles
    artist_mapping = {
        "BICFIZZLE": "BiC Fizzle",
        "COBRAH": "COBRAH",
        "COROOK": "Corook",
        "RAICHE": "Raiche",
        "RE6CE": "re6ce",
        "FLYANABOSS": "Flyana Boss",
    }

    configured = set()
    for key, value in os.environ.items():
        if key.startswith("YT_") and key.endswith("_YT"):
            # Handle both main channels and topic channels
            artist_name = key[3:-3]  # Remove YT_ prefix and _YT suffix

            # Remove _TOPIC suffix if present
            if artist_name.endswith("_TOPIC"):
                artist_name = artist_name[:-6]

            if artist_name in artist_mapping:
                configured.add(artist_mapping[artist_name])
            else:
                print(f"⚠️ Unknown artist in .env: {artist_name}")
                # Still add it in case it's a valid artist not in our mapping
                configured.add(artist_name.replace("_", " ").title())

    return configured


def get_database_artists(engine) -> List[Tuple[str, int]]:
    """Get all artists currently in the database"""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT DISTINCT channel_title, COUNT(*) as video_count
            FROM youtube_videos
            WHERE channel_title IS NOT NULL
            GROUP BY channel_title
            ORDER BY video_count DESC
        """
            )
        )
        return [(row[0], row[1]) for row in result]


def remove_unauthorized_artist(engine, artist_name: str) -> dict:
    """Remove all traces of an unauthorized artist from ALL tables"""
    print(f"🗑️ Removing {artist_name} from ALL tables...")

    deleted_counts = {}

    with engine.connect() as conn:
        # Delete from all related tables in proper order
        tables_to_clean = [
            ("youtube_sentiment_summary", "video_id IN (SELECT video_id FROM youtube_videos WHERE channel_title = %s)"),
            (
                "comment_sentiment",
                "comment_id IN (SELECT c.comment_id FROM youtube_comments c JOIN youtube_videos v ON c.video_id = v.video_id WHERE v.channel_title = %s)",
            ),
            (
                "comment_bot_analysis",
                "comment_id IN (SELECT c.comment_id FROM youtube_comments c JOIN youtube_videos v ON c.video_id = v.video_id WHERE v.channel_title = %s)",
            ),
            ("youtube_comments", "video_id IN (SELECT video_id FROM youtube_videos WHERE channel_title = %s)"),
            ("youtube_metrics", "video_id IN (SELECT video_id FROM youtube_videos WHERE channel_title = %s)"),
            ("youtube_videos", "channel_title = %s"),
        ]

        for table_name, where_clause in tables_to_clean:
            try:
                # Use parameterized query to avoid SQL injection
                query = f"DELETE FROM {table_name} WHERE {where_clause.replace('%s', ':artist_name')}"
                deleted = conn.execute(text(query), {"artist_name": artist_name}).rowcount
                deleted_counts[table_name] = deleted
                if deleted > 0:
                    print(f"  ✅ {table_name}: {deleted} records")
            except Exception as e:
                print(f"  ⚠️ {table_name}: Error - {e}")
                deleted_counts[table_name] = 0

        # Also clean raw tables
        try:
            deleted_raw = conn.execute(
                text(
                    """
                DELETE FROM youtube_videos_raw
                WHERE JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.snippet.channelTitle')) = :artist_name
            """
                ),
                {"artist_name": artist_name},
            ).rowcount
            deleted_counts["youtube_videos_raw"] = deleted_raw
            if deleted_raw > 0:
                print(f"  ✅ youtube_videos_raw: {deleted_raw} records")
        except Exception as e:
            print(f"  ⚠️ youtube_videos_raw: Error - {e}")

        conn.commit()

    return deleted_counts


def main():
    """Main cleanup function"""
    print("🧹 COMPREHENSIVE DATA CLEANUP TOOL")
    print("=" * 60)

    # Load environment and connect to database
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))

    # Get configured artists
    configured_artists = get_configured_artists()
    print(f"✅ Configured artists ({len(configured_artists)}):")
    for artist in sorted(configured_artists):
        print(f"  - {artist}")

    # Get current database artists
    db_artists = get_database_artists(engine)
    print(f"\n📊 Current database artists ({len(db_artists)}):")
    for artist, count in db_artists:
        print(f"  - {artist}: {count} videos")

    # Find unauthorized artists
    db_artist_names = {artist for artist, _ in db_artists}
    unauthorized = db_artist_names - configured_artists

    if not unauthorized:
        print(f"\n✅ All artists are authorized! No cleanup needed.")
        return

    print(f"\n🚨 UNAUTHORIZED ARTISTS FOUND ({len(unauthorized)}):")
    for artist in sorted(unauthorized):
        print(f"  ❌ {artist}")

    # Auto-cleanup without confirmation (for automated use)
    print(f"\n🗑️ AUTO-CLEANUP: Removing {len(unauthorized)} unauthorized artists...")
    print(f"Artists to be deleted: {sorted(unauthorized)}")

    # Perform cleanup
    print(f"\n🗑️ Starting cleanup...")
    total_deleted = {}

    for artist in unauthorized:
        deleted_counts = remove_unauthorized_artist(engine, artist)
        for table, count in deleted_counts.items():
            total_deleted[table] = total_deleted.get(table, 0) + count

    # Summary
    print(f"\n🎉 CLEANUP COMPLETE!")
    print(f"📊 Total records deleted:")
    for table, count in total_deleted.items():
        if count > 0:
            print(f"  - {table}: {count:,} records")

    # Verify final state
    final_artists = get_database_artists(engine)
    print(f"\n✅ Final database state ({len(final_artists)} artists):")
    for artist, count in final_artists:
        print(f"  - {artist}: {count} videos")

    # Check if we're missing any configured artists
    final_artist_names = {artist for artist, _ in final_artists}
    missing = configured_artists - final_artist_names
    if missing:
        print(f"\n⚠️ Missing configured artists (need ETL): {sorted(missing)}")
    else:
        print(f"\n🎉 All configured artists are present!")


if __name__ == "__main__":
    main()
