#!/usr/bin/env python3
"""
Demonstration script for the data migration system.

This script shows how to use the DataMigrator class to migrate CSV/JSON files
to database tables with validation, backup, and archiving capabilities.
"""

import json

# Add src to path for imports
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from data_organization.data_migrator import DataMigrator


def create_sample_data():
    """Create sample CSV and JSON files for demonstration."""
    temp_dir = Path(tempfile.mkdtemp())
    print(f"📁 Created temporary directory: {temp_dir}")

    # Create sample CSV data
    csv_dir = temp_dir / "csv_data"
    csv_dir.mkdir()

    # Artist summary data
    artist_data = pd.DataFrame(
        {
            "artist_name": ["Demo Artist A", "Demo Artist B", "Demo Artist C"],
            "total_videos": [25, 18, 32],
            "total_views": [5000000, 3200000, 7800000],
            "total_likes": [250000, 160000, 390000],
            "total_comments": [12000, 8500, 18500],
            "avg_engagement_rate": [5.8, 6.2, 5.4],
        }
    )
    artist_csv = csv_dir / "artist_music_summary.csv"
    artist_data.to_csv(artist_csv, index=False)
    print(f"📄 Created sample CSV: {artist_csv.name}")

    # Create sample JSON data
    json_dir = temp_dir / "json_data"
    json_dir.mkdir()

    # Artist aliases
    aliases = {"Demo Alias A": "Demo Artist A", "Demo Alias B": "Demo Artist B"}
    aliases_json = json_dir / "artist_aliases.json"
    with open(aliases_json, "w") as f:
        json.dump(aliases, f, indent=2)
    print(f"📄 Created sample JSON: {aliases_json.name}")

    # Artist colors
    colors = {"Demo Artist A": "#FF6B6B", "Demo Artist B": "#4ECDC4", "Demo Artist C": "#45B7D1"}
    colors_json = json_dir / "artist_colors.json"
    with open(colors_json, "w") as f:
        json.dump(colors, f, indent=2)
    print(f"📄 Created sample JSON: {colors_json.name}")

    return temp_dir


def create_mock_engine():
    """Create a mock database engine for demonstration."""
    engine = Mock()

    # Mock connection context manager
    connection_mock = Mock()
    connection_mock.__enter__ = Mock(return_value=connection_mock)
    connection_mock.__exit__ = Mock(return_value=None)
    connection_mock.execute.return_value = Mock()
    connection_mock.commit.return_value = None

    engine.connect.return_value = connection_mock

    return engine


def demonstrate_csv_migration(migrator, temp_dir):
    """Demonstrate CSV file migration."""
    print("\n🔄 CSV Migration Demonstration")
    print("=" * 50)

    csv_dir = temp_dir / "csv_data"

    # Define table mapping
    table_mapping = {
        "artist_music_summary.csv": {
            "table": "artist_performance_summary",
            "columns": {
                "artist_name": "artist_name",
                "total_videos": "total_videos",
                "total_views": "total_views",
                "total_likes": "total_likes",
                "total_comments": "total_comments",
                "avg_engagement_rate": "avg_engagement_rate",
            },
        }
    }

    # Mock pandas to_sql to avoid actual database operations
    mock_to_sql = Mock()
    mock_to_sql.return_value = None

    # Patch pandas DataFrame.to_sql
    original_to_sql = pd.DataFrame.to_sql
    pd.DataFrame.to_sql = mock_to_sql

    try:
        # Perform migration
        result = migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=table_mapping)

        # Display results
        print(f"✅ Migration Status: {'SUCCESS' if result.success else 'FAILED'}")
        print(f"📊 Records Migrated: {result.records_migrated}")
        print(f"📁 Files Processed: {len(result.source_files)}")
        print(f"🗄️  Tables Updated: {len(result.target_tables)}")
        print(f"⏱️  Duration: {result.duration_seconds:.3f} seconds")

        if result.errors:
            print(f"❌ Errors: {len(result.errors)}")
            for error in result.errors:
                print(f"   🚨 {error}")

        if result.warnings:
            print(f"⚠️  Warnings: {len(result.warnings)}")
            for warning in result.warnings:
                print(f"   ⚠️  {warning}")

    finally:
        # Restore original to_sql method
        pd.DataFrame.to_sql = original_to_sql


def demonstrate_json_migration(migrator, temp_dir):
    """Demonstrate JSON file migration."""
    print("\n🔄 JSON Migration Demonstration")
    print("=" * 50)

    json_dir = temp_dir / "json_data"

    # Define table mapping
    table_mapping = {
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
    }

    # Mock pandas to_sql to avoid actual database operations
    mock_to_sql = Mock()
    mock_to_sql.return_value = None

    # Patch pandas DataFrame.to_sql
    original_to_sql = pd.DataFrame.to_sql
    pd.DataFrame.to_sql = mock_to_sql

    try:
        # Perform migration
        result = migrator.migrate_json_files(source_dir=str(json_dir), table_mapping=table_mapping)

        # Display results
        print(f"✅ Migration Status: {'SUCCESS' if result.success else 'FAILED'}")
        print(f"📊 Records Migrated: {result.records_migrated}")
        print(f"📁 Files Processed: {len(result.source_files)}")
        print(f"🗄️  Tables Updated: {len(result.target_tables)}")
        print(f"⏱️  Duration: {result.duration_seconds:.3f} seconds")

    finally:
        # Restore original to_sql method
        pd.DataFrame.to_sql = original_to_sql


def demonstrate_backup_and_archiving(migrator, temp_dir):
    """Demonstrate backup and archiving functionality."""
    print("\n💾 Backup & Archiving Demonstration")
    print("=" * 50)

    # Get all files for backup
    all_files = []
    all_files.extend(str(f) for f in (temp_dir / "csv_data").glob("*.csv"))
    all_files.extend(str(f) for f in (temp_dir / "json_data").glob("*.json"))

    print(f"📋 Files to backup: {len(all_files)}")
    for file_path in all_files:
        print(f"   📄 {Path(file_path).name}")

    # Create backup
    backup_result = migrator.create_backup(all_files)

    print(f"\n✅ Backup Status: {'SUCCESS' if backup_result.success else 'FAILED'}")
    if backup_result.success:
        print(f"💾 Backup Location: {backup_result.backup_path}")
        backup_files = list(Path(backup_result.backup_path).glob("*"))
        print(f"📁 Backup Contains: {len(backup_files)} files")

    # Demonstrate archiving
    archive_dir = temp_dir / "archive"
    archive_result = migrator.archive_migrated_files(
        source_files=all_files[:2], archive_dir=str(archive_dir)  # Archive first 2 files
    )

    print(f"\n📦 Archive Status: {'SUCCESS' if archive_result.success else 'FAILED'}")
    if archive_result.success:
        archived_files = list(archive_dir.glob("*"))
        print(f"📁 Archived Files: {len(archived_files)}")
        for archived_file in archived_files:
            print(f"   📄 {archived_file.name}")


def demonstrate_automatic_mapping(migrator):
    """Demonstrate automatic table mapping detection."""
    print("\n🔍 Automatic Mapping Demonstration")
    print("=" * 50)

    known_files = [
        "artist_music_summary.csv",
        "normalized_music_videos.csv",
        "artist_aliases.json",
        "artist_colors.json",
        "unknown_file.csv",
    ]

    for filename in known_files:
        mapping = migrator.get_table_mapping_for_file(filename)
        if mapping:
            print(f"✅ {filename} → {mapping['table']}")
            if "columns" in mapping:
                print(f"   📊 Columns: {len(mapping['columns'])} mapped")
            if "transform" in mapping:
                print(f"   🔄 Transform: {mapping['transform']}")
        else:
            print(f"❓ {filename} → No automatic mapping available")


def main():
    """Main demonstration function."""
    print("🚀 Data Migration System Demonstration")
    print("=" * 60)

    # Create sample data
    temp_dir = create_sample_data()

    # Create mock database engine
    engine = create_mock_engine()

    # Create migrator
    migrator = DataMigrator(engine=engine)

    try:
        # Demonstrate different features
        demonstrate_automatic_mapping(migrator)
        demonstrate_csv_migration(migrator, temp_dir)
        demonstrate_json_migration(migrator, temp_dir)
        demonstrate_backup_and_archiving(migrator, temp_dir)

        print("\n🎉 Demonstration Complete!")
        print(f"📁 Sample data created in: {temp_dir}")
        print("💡 This demonstration used mock database operations.")
        print("💡 In production, data would be migrated to actual database tables.")

    except Exception as e:
        print(f"💥 Demonstration failed: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
