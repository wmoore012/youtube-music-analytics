#!/usr/bin/env python3
"""
Database cleanup script to remove old artist data that's no longer in .env configuration.

This script identifies artists in the database that are not in the current .env configuration
and provides options to remove their data.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from dotenv import load_dotenv

from web.etl_helpers import get_engine


def get_current_artists_from_env():
    """Extract current artist list from .env YouTube channel configurations."""
    load_dotenv()

    current_artists = []

    # Look for YT_*_YT patterns in environment
    for key, value in os.environ.items():
        if key.startswith("YT_") and key.endswith("_YT") and value:
            # Extract artist name from key (e.g., YT_BICFIZZLE_YT -> BiC Fizzle)
            artist_key = key.replace("YT_", "").replace("_YT", "")

            # Map known artist keys to proper names
            artist_mapping = {
                "BICFIZZLE": "BiC Fizzle",
                "COBRAH": "COBRAH",
                "COROOK": "Corook",
                "RAICHE": "Raiche",
                "RE6CE": "re6ce",
                "FLYANABOSS": "Flyana Boss",
            }

            if artist_key in artist_mapping:
                current_artists.append(artist_mapping[artist_key])

    return current_artists


def get_database_artists():
    """Get list of artists currently in the database."""
    try:
        # Connect to local database
        engine = get_engine()
        query = """
        SELECT DISTINCT channel_title, COUNT(*) as video_count
        FROM youtube_videos
        GROUP BY channel_title
        ORDER BY video_count DESC
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Error connecting to database: {e}")
        try:
            # Fallback to default connection
            from sqlalchemy import create_engine

            url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'yt_proj')}?charset=utf8mb4"
            engine = create_engine(url)
            df = pd.read_sql(query, engine)
            return df
        except Exception as e2:
            print(f"Error with fallback connection: {e2}")
            return pd.DataFrame()


def cleanup_artist_data(artist_name, engine, dry_run=True):
    """Remove all data for a specific artist."""
    tables_to_clean = [
        "youtube_videos",
        "youtube_metrics",
        "youtube_comments",
        "comment_sentiment",
        "youtube_sentiment_summary",
    ]

    cleanup_queries = []

    for table in tables_to_clean:
        if table == "youtube_metrics":
            # For metrics, need to join with videos to find artist
            query = f"""
            DELETE m FROM {table} m
            JOIN youtube_videos v ON m.video_id = v.video_id
            WHERE v.channel_title = '{artist_name}'
            """
        elif table == "youtube_comments":
            # For comments, need to join with videos
            query = f"""
            DELETE c FROM {table} c
            JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE v.channel_title = '{artist_name}'
            """
        elif table == "comment_sentiment":
            # For sentiment, need to join through comments and videos
            query = f"""
            DELETE cs FROM {table} cs
            JOIN youtube_comments c ON cs.comment_id = c.comment_id
            JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE v.channel_title = '{artist_name}'
            """
        else:
            # Direct cleanup for videos and summary tables
            query = f"DELETE FROM {table} WHERE channel_title = '{artist_name}'"

        cleanup_queries.append((table, query))

    if dry_run:
        print(f"\n🔍 DRY RUN - Would execute these queries for {artist_name}:")
        for table, query in cleanup_queries:
            print(f"   {table}: {query}")
        return

    # Execute cleanup
    print(f"\n🗑️  CLEANING UP DATA for {artist_name}...")
    with engine.connect() as conn:
        for table, query in cleanup_queries:
            try:
                result = conn.execute(query)
                print(f"   ✅ {table}: {result.rowcount} rows deleted")
                conn.commit()
            except Exception as e:
                print(f"   ❌ {table}: Error - {e}")


def main():
    """Main cleanup process."""
    print("🧹 ARTIST DATA CLEANUP TOOL")
    print("=" * 40)

    # Load environment
    load_dotenv()

    # Get current artists from .env
    current_artists = get_current_artists_from_env()
    print(f"\n✅ CURRENT ARTISTS (from .env):")
    for artist in current_artists:
        print(f"   • {artist}")

    # Get database artists
    db_artists_df = get_database_artists()

    if db_artists_df.empty:
        print("\n❌ Could not connect to database or no data found")
        return

    print(f"\n📊 DATABASE ARTISTS:")
    for _, row in db_artists_df.iterrows():
        status = "✅ CURRENT" if row["channel_title"] in current_artists else "🗑️  OLD"
        print(f"   {status} {row['channel_title']} ({row['video_count']} videos)")

    # Identify artists to remove
    db_artists = set(db_artists_df["channel_title"].tolist())
    current_artists_set = set(current_artists)
    artists_to_remove = db_artists - current_artists_set

    if not artists_to_remove:
        print(f"\n🎉 DATABASE IS CLEAN - No old artists found!")
        return

    print(f"\n🗑️  ARTISTS TO REMOVE:")
    for artist in artists_to_remove:
        artist_rows = db_artists_df[db_artists_df["channel_title"] == artist]
        if not artist_rows.empty:
            video_count = artist_rows["video_count"].iloc[0]
            print(f"   • {artist} ({video_count} videos)")
        else:
            print(f"   • {artist} (unknown video count)")

    # Confirm cleanup
    print(f"\n⚠️  WARNING: This will permanently delete all data for {len(artists_to_remove)} artists!")
    print("   This includes videos, metrics, comments, and sentiment data.")

    response = input("\nProceed with cleanup? (yes/no): ").lower().strip()

    if response != "yes":
        print("❌ Cleanup cancelled")
        return

    # Get database engine
    try:
        engine = get_engine()
    except:
        from sqlalchemy import create_engine

        url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'yt_proj')}?charset=utf8mb4"
        engine = create_engine(url)

    # Execute cleanup for each artist
    for artist in artists_to_remove:
        cleanup_artist_data(artist, engine, dry_run=False)

    print(f"\n✅ CLEANUP COMPLETE!")
    print("   Run the analytics notebooks again to see clean data.")


if __name__ == "__main__":
    main()
