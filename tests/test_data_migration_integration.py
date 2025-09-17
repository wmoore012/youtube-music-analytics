"""
Integration tests for the complete data migration system.

These tests verify the end-to-end functionality of migrating CSV/JSON files
to database tables with validation, backup, and archiving capabilities.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.data_organization.data_migrator import DataMigrator, MigrationResult


class TestDataMigrationIntegration:
    """Integration tests for complete data migration workflow."""

    @pytest.fixture
    def temp_workspace(self):
        """Create temporary workspace with sample data files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)

            # Create sample CSV files
            csv_dir = workspace / "csv_data"
            csv_dir.mkdir()

            # Artist summary CSV
            artist_data = pd.DataFrame(
                {
                    "artist_name": ["Artist A", "Artist B", "Artist C"],
                    "total_videos": [10, 15, 8],
                    "total_views": [1000000, 2500000, 800000],
                    "total_likes": [50000, 125000, 40000],
                    "total_comments": [2000, 5000, 1500],
                    "avg_engagement_rate": [5.2, 7.8, 6.1],
                }
            )
            artist_data.to_csv(csv_dir / "artist_music_summary.csv", index=False)

            # Video analysis CSV
            video_data = pd.DataFrame(
                {
                    "video_id": ["vid1", "vid2", "vid3"],
                    "title": ["Song A", "Song B", "Song C"],
                    "artist_name": ["Artist A", "Artist B", "Artist C"],
                    "view_count": [100000, 250000, 80000],
                    "like_count": [5000, 12500, 4000],
                    "engagement_rate": [5.0, 5.0, 5.0],
                }
            )
            video_data.to_csv(csv_dir / "video_analysis.csv", index=False)

            # Create sample JSON files
            json_dir = workspace / "json_data"
            json_dir.mkdir()

            # Artist aliases JSON
            aliases = {"Artist Alias A": "Artist A", "Artist Alias B": "Artist B"}
            with open(json_dir / "artist_aliases.json", "w") as f:
                json.dump(aliases, f)

            # Artist colors JSON
            colors = {"Artist A": "#1f77b4", "Artist B": "#ff7f0e", "Artist C": "#2ca02c"}
            with open(json_dir / "artist_colors.json", "w") as f:
                json.dump(colors, f)

            yield workspace

    @pytest.fixture
    def mock_database_engine(self):
        """Mock database engine that simulates successful operations."""
        engine = Mock()

        # Mock connection context manager
        connection_mock = Mock()
        connection_mock.__enter__ = Mock(return_value=connection_mock)
        connection_mock.__exit__ = Mock(return_value=None)
        connection_mock.execute.return_value = Mock()
        connection_mock.commit.return_value = None

        engine.connect.return_value = connection_mock

        return engine

    @pytest.fixture
    def data_migrator(self, mock_database_engine):
        """Create DataMigrator with mocked database."""
        return DataMigrator(engine=mock_database_engine)

    @patch("pandas.DataFrame.to_sql")
    def test_complete_csv_migration_workflow(self, mock_to_sql, data_migrator, temp_workspace):
        """Test complete CSV migration workflow with backup and validation."""
        # Mock successful database insertion
        mock_to_sql.return_value = None

        csv_dir = temp_workspace / "csv_data"

        # Define table mappings
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
            },
            "video_analysis.csv": {
                "table": "video_analysis_summary",
                "columns": {
                    "video_id": "video_id",
                    "title": "title",
                    "artist_name": "artist_name",
                    "view_count": "view_count",
                    "like_count": "like_count",
                    "engagement_rate": "engagement_rate",
                },
            },
        }

        # Step 1: Create backup
        csv_files = list(csv_dir.glob("*.csv"))
        backup_result = data_migrator.create_backup([str(f) for f in csv_files])

        assert backup_result.success is True
        assert backup_result.backup_path is not None
        assert Path(backup_result.backup_path).exists()

        # Step 2: Migrate CSV files
        migration_result = data_migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=table_mapping)

        assert migration_result.success is True
        assert migration_result.records_migrated == 6  # 3 + 3 records
        assert len(migration_result.source_files) == 2
        assert len(migration_result.target_tables) == 2
        assert len(migration_result.errors) == 0

        # Step 3: Archive migrated files
        archive_dir = temp_workspace / "archive"
        archive_result = data_migrator.archive_migrated_files(
            source_files=migration_result.source_files, archive_dir=str(archive_dir)
        )

        assert archive_result.success is True
        assert archive_dir.exists()
        assert len(list(archive_dir.glob("*.csv"))) == 2

    @patch("pandas.DataFrame.to_sql")
    def test_complete_json_migration_workflow(self, mock_to_sql, data_migrator, temp_workspace):
        """Test complete JSON migration workflow."""
        # Mock successful database insertion
        mock_to_sql.return_value = None

        json_dir = temp_workspace / "json_data"

        # Define table mappings
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

        # Migrate JSON files
        migration_result = data_migrator.migrate_json_files(source_dir=str(json_dir), table_mapping=table_mapping)

        assert migration_result.success is True
        assert migration_result.records_migrated == 5  # 2 + 3 records
        assert len(migration_result.source_files) == 2
        assert len(migration_result.target_tables) == 2
        assert len(migration_result.errors) == 0

    def test_migration_with_validation_errors(self, data_migrator, temp_workspace):
        """Test migration behavior when validation errors occur."""
        csv_dir = temp_workspace / "csv_data"

        # Create invalid mapping (missing required columns)
        invalid_mapping = {
            "artist_music_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {"nonexistent_column": "artist_name"},  # This column doesn't exist
            }
        }

        # Attempt migration
        migration_result = data_migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=invalid_mapping)

        assert migration_result.success is False
        assert len(migration_result.errors) > 0
        assert migration_result.records_migrated == 0
        assert "missing required columns" in migration_result.errors[0].lower()

    @patch("pandas.DataFrame.to_sql")
    def test_migration_with_database_errors(self, mock_to_sql, temp_workspace):
        """Test migration behavior when database errors occur."""
        # Mock database error
        mock_to_sql.side_effect = Exception("Database connection failed")

        # Create migrator with mock engine
        engine = Mock()
        connection_mock = Mock()
        connection_mock.__enter__ = Mock(return_value=connection_mock)
        connection_mock.__exit__ = Mock(return_value=None)
        engine.connect.return_value = connection_mock

        migrator = DataMigrator(engine=engine)

        csv_dir = temp_workspace / "csv_data"
        table_mapping = {
            "artist_music_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {"artist_name": "artist_name", "total_videos": "total_videos"},
            }
        }

        # Attempt migration
        migration_result = migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=table_mapping)

        assert migration_result.success is False
        assert len(migration_result.errors) > 0
        assert "Database connection failed" in str(migration_result.errors)

    def test_automatic_table_mapping_detection(self, data_migrator):
        """Test automatic detection of table mappings for known file types."""
        # Test known CSV file
        csv_mapping = data_migrator.get_table_mapping_for_file("artist_music_summary.csv")
        assert csv_mapping is not None
        assert csv_mapping["table"] == "artist_performance_summary"
        assert "artist_name" in csv_mapping["columns"]

        # Test known JSON file
        json_mapping = data_migrator.get_table_mapping_for_file("artist_aliases.json")
        assert json_mapping is not None
        assert json_mapping["table"] == "artist_aliases"
        assert json_mapping["transform"] == "key_value_pairs"

        # Test unknown file
        unknown_mapping = data_migrator.get_table_mapping_for_file("unknown_file.csv")
        assert unknown_mapping is None

    @patch("pandas.DataFrame.to_sql")
    def test_migration_result_reporting(self, mock_to_sql, data_migrator, temp_workspace):
        """Test migration result reporting and serialization."""
        # Mock successful database insertion
        mock_to_sql.return_value = None

        csv_dir = temp_workspace / "csv_data"

        table_mapping = {
            "artist_music_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {"artist_name": "artist_name", "total_videos": "total_videos", "total_views": "total_views"},
            }
        }

        # Perform migration
        result = data_migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=table_mapping)

        # Test result serialization
        result_dict = result.to_dict()
        assert isinstance(result_dict, dict)
        assert result_dict["success"] is True
        assert result_dict["records_migrated"] == 3
        assert len(result_dict["source_files"]) == 1

        # Test report generation
        report = result.generate_report()
        assert "Migration Report" in report
        assert "SUCCESS" in report
        assert "3" in report  # Record count
        assert "artist_music_summary.csv" in report

    def test_file_backup_and_restore_capability(self, data_migrator, temp_workspace):
        """Test file backup creation and verification."""
        csv_dir = temp_workspace / "csv_data"
        csv_files = list(csv_dir.glob("*.csv"))

        # Should have exactly 2 CSV files from the fixture
        expected_files = ["artist_music_summary.csv", "video_analysis.csv"]
        actual_files = [f.name for f in csv_files]

        # Create backup
        backup_result = data_migrator.create_backup([str(f) for f in csv_files])

        assert backup_result.success is True
        backup_path = Path(backup_result.backup_path)
        assert backup_path.exists()

        # Verify backup contains all files
        backup_files = list(backup_path.glob("*.csv"))
        assert len(backup_files) == len(csv_files)

        # Verify backup file contents match originals
        for original_file in csv_files:
            backup_file = backup_path / original_file.name
            assert backup_file.exists()

            original_data = pd.read_csv(original_file)
            backup_data = pd.read_csv(backup_file)

            # Compare DataFrames
            pd.testing.assert_frame_equal(original_data, backup_data)

    @patch("pandas.DataFrame.to_sql")
    def test_migration_with_mixed_success_and_failures(self, mock_to_sql, data_migrator, temp_workspace):
        """Test migration behavior with mixed success and failure scenarios."""
        # Mock successful database insertion
        mock_to_sql.return_value = None

        csv_dir = temp_workspace / "csv_data"

        # Create mapping with one valid and one invalid entry
        mixed_mapping = {
            "artist_music_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {"artist_name": "artist_name", "total_videos": "total_videos"},
            },
            "nonexistent_file.csv": {"table": "some_table", "columns": {"col1": "col1"}},
        }

        # Perform migration
        result = data_migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=mixed_mapping)

        # Should have partial success
        assert result.records_migrated > 0  # Some records migrated
        assert len(result.warnings) > 0  # Warning about missing file
        assert len(result.source_files) == 1  # Only one file processed

        # Overall success depends on whether any files were processed successfully
        assert result.success is True  # Should be True since one file succeeded

    @patch("src.data_organization.data_migrator.pd.DataFrame.to_sql")
    def test_database_insertion_with_pandas_to_sql(self, mock_to_sql, data_migrator, temp_workspace):
        """Test that database insertion uses pandas to_sql method correctly."""
        csv_dir = temp_workspace / "csv_data"

        # Configure mock to simulate successful insertion
        mock_to_sql.return_value = 3  # Simulate 3 records inserted

        table_mapping = {
            "artist_music_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {"artist_name": "artist_name", "total_videos": "total_videos"},
            }
        }

        # Perform migration
        result = data_migrator.migrate_csv_files(source_dir=str(csv_dir), table_mapping=table_mapping)

        # Verify to_sql was called with correct parameters
        mock_to_sql.assert_called_once()
        call_args = mock_to_sql.call_args

        assert call_args[1]["name"] == "artist_performance_summary"
        assert call_args[1]["if_exists"] == "append"
        assert call_args[1]["index"] is False
        assert call_args[1]["method"] == "multi"

        # Verify migration result
        assert result.success is True
        assert result.records_migrated == 3
