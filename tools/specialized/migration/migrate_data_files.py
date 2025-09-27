#!/usr / bin / env python3
"""
CLI tool for migrating CSV / JSON data files to database tables.

This script provides a command - line interface for the data migration system,
allowing users to migrate scattered data files into organized database tables.
"""

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict

from sqlalchemy import create_engine

# Add src and root to path for imports
root_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_path / "src"))
sys.path.insert(0, str(root_path))

from data_organization.data_migrator import DataMigrator, MigrationError
from web.etl_helpers import get_engine


def load_migration_config(config_path: str) -> Dict[str, Any]:
    """Load migration configuration from JSON file."""
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except Exception as e:
        raise MigrationError(f"Failed to load migration config: {str(e)}")


def create_default_migration_config() -> Dict[str, Any]:
    """Create default migration configuration for known file types."""
    return {
        "csv_mappings": {
            "artist_music_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {
                    "artist_name": "artist_name",
                    "total_videos": "total_videos",
                    "total_views": "total_views",
                    "total_likes": "total_likes",
                    "total_comments": "total_comments",
                    "total_est_revenue_usd": "total_est_revenue_usd",
                    "videos_with_isrc": "videos_with_isrc",
                    "avg_engagement_rate": "avg_engagement_rate",
                    "isrc_percentage": "isrc_percentage",
                    "revenue_per_video": "revenue_per_video",
                },
            },
            "normalized_music_videos.csv": {
                "table": "music_videos_normalized",
                "columns": {
                    "video_id": "video_id",
                    "title": "title",
                    "song_title": "song_title",
                    "artist_name": "artist_name",
                    "video_type": "video_type",
                    "isrc": "isrc",
                    "has_isrc": "has_isrc",
                    "published_at": "published_at",
                    "view_count": "view_count",
                    "like_count": "like_count",
                    "comment_count": "comment_count",
                    "est_revenue_usd": "est_revenue_usd",
                    "like_rate": "like_rate",
                    "comment_rate": "comment_rate",
                    "engagement_rate": "engagement_rate",
                    "days_since_publish": "days_since_publish",
                    "views_per_day": "views_per_day",
                    "metrics_date": "metrics_date",
                    "fetched_at": "fetched_at",
                },
            },
            "video_type_analysis.csv": {
                "table": "video_type_analysis_summary",
                "columns": {
                    "video_type": "video_type",
                    "count": "video_count",
                    "avg_views": "avg_view_count",
                    "avg_engagement": "avg_engagement_rate",
                },
            },
        },
        "json_mappings": {
            "artist_aliases.json": {
                "table": "artist_aliases",
                "key_column": "alias_name",
                "value_column": "canonical_name",
                "transform": "key_value_pairs",
            },
            "artist_colors.json": {
                "table": "artist_visualization_config",
                "key_column": "artist_name",
                "value_column": "color_code",
                "transform": "key_value_pairs",
            },
            "expected_artists.json": {
                "table": "expected_artists_config",
                "key_column": "artist_name",
                "value_column": "is_expected",
                "transform": "array_to_boolean",
            },
        },
    }


def migrate_csv_files(migrator: DataMigrator, source_dir: str, mappings: Dict[str, Any]) -> bool:
    """Migrate CSV files using the provided mappings."""
    print(f"\n🔄 Migrating CSV files from {source_dir}...")

    result = migrator.migrate_csv_files(source_dir, mappings)

    if result.success:
        print(f"✅ Successfully migrated {result.records_migrated} records from {len(result.source_files)} CSV files")
        for file_path in result.source_files:
            print(f"   📄 {Path(file_path).name}")
    else:
        print(f"❌ CSV migration failed with {len(result.errors)} errors")
        for error in result.errors:
            print(f"   🚨 {error}")

    if result.warnings:
        print(f"⚠️  {len(result.warnings)} warnings:")
        for warning in result.warnings:
            print(f"   ⚠️  {warning}")

    return result.success


def migrate_json_files(migrator: DataMigrator, source_dir: str, mappings: Dict[str, Any]) -> bool:
    """Migrate JSON files using the provided mappings."""
    print(f"\n🔄 Migrating JSON files from {source_dir}...")

    result = migrator.migrate_json_files(source_dir, mappings)

    if result.success:
        print(f"✅ Successfully migrated {result.records_migrated} records from {len(result.source_files)} JSON files")
        for file_path in result.source_files:
            print(f"   📄 {Path(file_path).name}")
    else:
        print(f"❌ JSON migration failed with {len(result.errors)} errors")
        for error in result.errors:
            print(f"   🚨 {error}")

    if result.warnings:
        print(f"⚠️  {len(result.warnings)} warnings:")
        for warning in result.warnings:
            print(f"   ⚠️  {warning}")

    return result.success


def create_backup(migrator: DataMigrator, files: list) -> bool:
    """Create backup of files before migration."""
    if not files:
        return True

    print(f"\n💾 Creating backup of {len(files)} files...")

    result = migrator.create_backup(files)

    if result.success:
        print(f"✅ Backup created successfully at: {result.backup_path}")
    else:
        print(f"❌ Backup failed with {len(result.errors)} errors")
        for error in result.errors:
            print(f"   🚨 {error}")

    return result.success


def archive_files(migrator: DataMigrator, files: list, archive_dir: str) -> bool:
    """Archive successfully migrated files."""
    if not files:
        return True

    print(f"\n📦 Archiving {len(files)} migrated files to {archive_dir}...")

    result = migrator.archive_migrated_files(files, archive_dir)

    if result.success:
        print(f"✅ Files archived successfully")
    else:
        print(f"❌ Archiving failed with {len(result.errors)} errors")
        for error in result.errors:
            print(f"   🚨 {error}")

    return result.success


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate CSV / JSON data files to database tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate files from music_analysis_tables directory
  python migrate_data_files.py --source music_analysis_tables --type csv

  # Migrate JSON config files
  python migrate_data_files.py --source config --type json

  # Migrate all files with backup and archiving
  python migrate_data_files.py --source . --type all --backup --archive data / migrated

  # Use custom migration configuration
  python migrate_data_files.py --config custom_migration.json --source data
        """,
    )

    parser.add_argument("--source", required=True, help="Source directory containing files to migrate")

    parser.add_argument(
        "--type", choices=["csv", "json", "all"], default="all", help="Type of files to migrate (default: all)"
    )

    parser.add_argument("--config", help="Path to migration configuration JSON file")

    parser.add_argument("--backup", action="store_true", help="Create backup before migration")

    parser.add_argument("--archive", help="Archive directory for successfully migrated files")

    parser.add_argument("--dry - run", action="store_true",
                        help="Show what would be migrated without actually doing it")

    parser.add_argument("--validate", action="store_true", help="Validate migration results after completion")

    args = parser.parse_args()

    try:
        # Load migration configuration
        if args.config:
            config = load_migration_config(args.config)
        else:
            config = create_default_migration_config()

        # Initialize database connection and migrator
        print("🔌 Connecting to database...")
        engine = get_engine()
        migrator = DataMigrator(engine)

        # Collect files to migrate
        source_path = Path(args.source)
        if not source_path.exists():
            print(f"❌ Source directory does not exist: {args.source}")
            return 1

        csv_files = list(source_path.glob("*.csv")) if args.type in ["csv", "all"] else []
        json_files = list(source_path.glob("*.json")) if args.type in ["json", "all"] else []
        all_files = [str(f) for f in csv_files + json_files]

        if not all_files:
            print(f"⚠️  No files found to migrate in {args.source}")
            return 0

        print(f"📋 Found {len(csv_files)} CSV files and {len(json_files)} JSON files")

        if args.dry_run:
            print("\n🔍 DRY RUN - Files that would be migrated:")
            for file_path in all_files:
                print(f"   📄 {Path(file_path).name}")
            return 0

        # Create backup if requested
        if args.backup:
            if not create_backup(migrator, all_files):
                print("❌ Backup failed, aborting migration")
                return 1

        # Perform migrations
        success = True
        migrated_files = []

        if csv_files and args.type in ["csv", "all"]:
            csv_success = migrate_csv_files(migrator, args.source, config.get("csv_mappings", {}))
            success = success and csv_success
            if csv_success:
                migrated_files.extend([str(f) for f in csv_files])

        if json_files and args.type in ["json", "all"]:
            json_success = migrate_json_files(migrator, args.source, config.get("json_mappings", {}))
            success = success and json_success
            if json_success:
                migrated_files.extend([str(f) for f in json_files])

        # Archive files if requested and migration was successful
        if args.archive and success and migrated_files:
            archive_files(migrator, migrated_files, args.archive)

        # Print final status
        if success:
            print(f"\n🎉 Migration completed successfully!")
            print(f"   📊 Total files processed: {len(migrated_files)}")
        else:
            print(f"\n💥 Migration completed with errors")
            return 1

        return 0

    except Exception as e:
        print(f"💥 Migration failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
