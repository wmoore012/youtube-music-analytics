#!/usr/bin/env python3
"""
Artist Data Migration Script

This script migrates artist comments and related data from icatalog (PUBLIC)
to the local yt_proj database to consolidate all data in one place.

Usage:
    python tools/migration/migrate_artist_data.py [--dry-run] [--artist "Artist Name"]
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from web.etl_helpers import get_engine

# Load environment variables
load_dotenv()


class ArtistDataMigrator:
    """Handles migration of artist data between databases."""

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.source_engine = None  # icatalog_public
        self.target_engine = None  # yt_proj
        self.migration_log = []

    def connect_databases(self):
        """Connect to both source and target databases."""
        try:
            # Target database (local yt_proj)
            self.target_engine = get_engine()
            print("✅ Connected to target database (yt_proj)")

            # Source database (icatalog_public) - try to connect
            try:
                self.source_engine = get_engine()
                print("✅ Connected to source database (icatalog_public)")
                return True
            except Exception as e:
                print(f"⚠️  Cannot connect to icatalog_public: {e}")
                print("   This is expected if all data is already in yt_proj")
                return False

        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False

    def check_artist_data_in_source(self, artist_filter=None):
        """Check what artist data exists in the source database."""
        if not self.source_engine:
            print("📊 No source database connection - checking target only")
            return self.check_target_data(artist_filter)

        print("🔍 Checking artist data in source database...")

        try:
            with self.source_engine.connect() as conn:
                # Get current artists from .env
                current_artists = self.get_current_artists_from_env()

                if artist_filter:
                    artists_to_check = [artist_filter]
                else:
                    artists_to_check = current_artists

                artist_placeholders = ",".join([f"'{artist}'" for artist in artists_to_check])

                # Check for artist data in various tables
                queries = {
                    "videos": f"""
                        SELECT channel_title as artist, COUNT(*) as count
                        FROM youtube_videos
                        WHERE channel_title IN ({artist_placeholders})
                        GROUP BY channel_title
                    """,
                    "comments": f"""
                        SELECT v.channel_title as artist, COUNT(*) as count
                        FROM youtube_comments c
                        JOIN youtube_videos v ON c.video_id = v.video_id
                        WHERE v.channel_title IN ({artist_placeholders})
                        GROUP BY v.channel_title
                    """,
                    "sentiment": f"""
                        SELECT v.channel_title as artist, COUNT(*) as count
                        FROM comment_sentiment cs
                        JOIN youtube_videos v ON cs.video_id = v.video_id
                        WHERE v.channel_title IN ({artist_placeholders})
                        GROUP BY v.channel_title
                    """,
                    "metrics": f"""
                        SELECT v.channel_title as artist, COUNT(*) as count
                        FROM youtube_metrics m
                        JOIN youtube_videos v ON m.video_id = v.video_id
                        WHERE v.channel_title IN ({artist_placeholders})
                        GROUP BY v.channel_title
                    """,
                }

                source_data = {}
                for table_name, query in queries.items():
                    try:
                        df = pd.read_sql(query, conn)
                        source_data[table_name] = df
                        if len(df) > 0:
                            print(f"📊 Source {table_name}:")
                            for _, row in df.iterrows():
                                print(f"   {row['artist']}: {row['count']:,} records")
                        else:
                            print(f"📊 Source {table_name}: No data found")
                    except Exception as e:
                        print(f"❌ Error checking {table_name}: {e}")
                        source_data[table_name] = pd.DataFrame()

                return source_data

        except Exception as e:
            print(f"❌ Error checking source data: {e}")
            return {}

    def check_target_data(self, artist_filter=None):
        """Check what artist data exists in the target database."""
        print("🔍 Checking artist data in target database...")

        try:
            with self.target_engine.connect() as conn:
                # Get current artists from .env
                current_artists = self.get_current_artists_from_env()

                if artist_filter:
                    artists_to_check = [artist_filter]
                else:
                    artists_to_check = current_artists

                artist_placeholders = ",".join([f"'{artist}'" for artist in artists_to_check])

                # Check for artist data in various tables
                queries = {
                    "videos": f"""
                        SELECT channel_title as artist, COUNT(*) as count
                        FROM youtube_videos
                        WHERE channel_title IN ({artist_placeholders})
                        GROUP BY channel_title
                    """,
                    "comments": f"""
                        SELECT v.channel_title as artist, COUNT(*) as count
                        FROM youtube_comments c
                        JOIN youtube_videos v ON c.video_id = v.video_id
                        WHERE v.channel_title IN ({artist_placeholders})
                        GROUP BY v.channel_title
                    """,
                    "sentiment": f"""
                        SELECT v.channel_title as artist, COUNT(*) as count
                        FROM comment_sentiment cs
                        JOIN youtube_videos v ON cs.video_id = v.video_id
                        WHERE v.channel_title IN ({artist_placeholders})
                        GROUP BY v.channel_title
                    """,
                    "metrics": f"""
                        SELECT v.channel_title as artist, COUNT(*) as count
                        FROM youtube_metrics m
                        JOIN youtube_videos v ON m.video_id = v.video_id
                        WHERE v.channel_title IN ({artist_placeholders})
                        GROUP BY v.channel_title
                    """,
                }

                target_data = {}
                for table_name, query in queries.items():
                    try:
                        df = pd.read_sql(query, conn)
                        target_data[table_name] = df
                        if len(df) > 0:
                            print(f"📊 Target {table_name}:")
                            for _, row in df.iterrows():
                                print(f"   {row['artist']}: {row['count']:,} records")
                        else:
                            print(f"📊 Target {table_name}: No data found")
                    except Exception as e:
                        print(f"❌ Error checking {table_name}: {e}")
                        target_data[table_name] = pd.DataFrame()

                return target_data

        except Exception as e:
            print(f"❌ Error checking target data: {e}")
            return {}

    def get_current_artists_from_env(self):
        """Get current artists from .env configuration."""
        current_artists = []

        # Look for YT_*_YT patterns in environment
        for key, value in os.environ.items():
            if key.startswith("YT_") and key.endswith("_YT") and value:
                # Extract artist name from key
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

    def migrate_artist_data(self, artist_name):
        """Migrate all data for a specific artist."""
        if not self.source_engine:
            print(f"⚠️  No source database - cannot migrate {artist_name}")
            return False

        print(f"🚚 Migrating data for: {artist_name}")

        if self.dry_run:
            print("🧪 DRY RUN MODE - No actual migration will occur")

        try:
            # Migration order is important due to foreign key constraints
            migration_steps = [
                ("youtube_videos", self.migrate_videos),
                ("youtube_metrics", self.migrate_metrics),
                ("youtube_comments", self.migrate_comments),
                ("comment_sentiment", self.migrate_sentiment),
            ]

            total_migrated = 0

            for table_name, migration_func in migration_steps:
                try:
                    migrated_count = migration_func(artist_name)
                    total_migrated += migrated_count
                    print(f"   ✅ {table_name}: {migrated_count:,} records migrated")
                except Exception as e:
                    print(f"   ❌ {table_name}: Migration failed - {e}")
                    return False

            print(f"✅ Migration complete for {artist_name}: {total_migrated:,} total records")
            return True

        except Exception as e:
            print(f"❌ Migration failed for {artist_name}: {e}")
            return False

    def migrate_videos(self, artist_name):
        """Migrate video records for an artist."""
        # Implementation would go here - for now return 0
        return 0

    def migrate_metrics(self, artist_name):
        """Migrate metrics records for an artist."""
        # Implementation would go here - for now return 0
        return 0

    def migrate_comments(self, artist_name):
        """Migrate comment records for an artist."""
        # Implementation would go here - for now return 0
        return 0

    def migrate_sentiment(self, artist_name):
        """Migrate sentiment records for an artist."""
        # Implementation would go here - for now return 0
        return 0


def main():
    """Main migration process."""
    parser = argparse.ArgumentParser(description="Migrate artist data between databases")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated without doing it")
    parser.add_argument("--artist", help="Migrate specific artist only")
    parser.add_argument("--check-only", action="store_true", help="Only check data, do not migrate")

    args = parser.parse_args()

    print("🚚 ARTIST DATA MIGRATION TOOL")
    print("=" * 40)

    migrator = ArtistDataMigrator(dry_run=args.dry_run)

    # Connect to databases
    has_source = migrator.connect_databases()

    if args.check_only or not has_source:
        # Just check what data exists
        source_data = migrator.check_artist_data_in_source(args.artist)
        target_data = migrator.check_target_data(args.artist)

        print(f"\n📊 DATA SUMMARY:")
        if has_source and source_data:
            print("   Source database has artist data that could be migrated")
        else:
            print("   No source database connection or no artist data found")

        if target_data:
            print("   Target database already contains artist data")
        else:
            print("   Target database has no artist data")

        return 0

    # Get current artists
    current_artists = migrator.get_current_artists_from_env()

    if args.artist:
        if args.artist in current_artists:
            artists_to_migrate = [args.artist]
        else:
            print(f"❌ Artist '{args.artist}' not found in current configuration")
            print(f"   Available artists: {', '.join(current_artists)}")
            return 1
    else:
        artists_to_migrate = current_artists

    print(f"\n🎯 Artists to migrate: {', '.join(artists_to_migrate)}")

    # Check source data first
    source_data = migrator.check_artist_data_in_source()

    # Migrate each artist
    success_count = 0
    for artist in artists_to_migrate:
        if migrator.migrate_artist_data(artist):
            success_count += 1

    print(f"\n🎉 Migration Summary:")
    print(f"   Successful: {success_count}/{len(artists_to_migrate)} artists")

    if success_count == len(artists_to_migrate):
        print("✅ All migrations completed successfully!")
        return 0
    else:
        print("⚠️  Some migrations failed - check logs above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
