"""
Tests for data migration system that moves CSV/JSON files to database tables.
"""

from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.data_organization.data_migrator import (
    DataIntegrityError,
    DataMigrator,
    MigrationError,
    MigrationResult,
    SchemaValidationError,
    ValidationResult,
)


class TestDataMigrator:
    """Test suite for DataMigrator class."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def sample_csv_data(self):
        """Sample CSV data for testing."""
        return pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B"],
                "total_videos": [10, 15],
                "total_views": [1000000, 2000000],
                "avg_engagement_rate": [5.5, 7.2],
            }
        )

    @pytest.fixture
    def sample_json_data(self):
        """Sample JSON data for testing."""
        return {"Artist A": "#1f77b4", "Artist B": "#ff7f0e"}

    @pytest.fixture
    def mock_engine(self):
        """Mock database engine for testing."""
        engine = Mock()

        # Mock connection context manager properly
        connection_mock = Mock()
        connection_mock.__enter__ = Mock(return_value=connection_mock)
        connection_mock.__exit__ = Mock(return_value=None)
        connection_mock.execute.return_value = Mock()
        connection_mock.commit.return_value = None

        engine.connect.return_value = connection_mock
        return engine

    @pytest.fixture
    def data_migrator(self, mock_engine):
        """Create DataMigrator instance for testing."""
        return DataMigrator(engine=mock_engine)

    @patch("pandas.DataFrame.to_sql")
    def test_migrate_csv_to_database_success(self, mock_to_sql, data_migrator, temp_dir, sample_csv_data):
        """Test successful CSV migration to database."""
        # Create test CSV file
        csv_file = temp_dir / "artist_summary.csv"
        sample_csv_data.to_csv(csv_file, index=False)

        # Mock successful database insertion
        mock_to_sql.return_value = None  # to_sql returns None on success

        # Define table mapping
        table_mapping = {
            "artist_summary.csv": {
                "table": "artist_performance_summary",
                "columns": {
                    "artist_name": "artist_name",
                    "total_videos": "total_videos",
                    "total_views": "total_views",
                    "avg_engagement_rate": "avg_engagement_rate",
                },
            }
        }

        # Execute migration
        result = data_migrator.migrate_csv_files(source_dir=str(temp_dir), table_mapping=table_mapping)

        # Verify results
        assert result.success is True
        assert result.records_migrated == 2
        assert len(result.errors) == 0
        assert str(csv_file) in result.source_files

    def test_migrate_csv_validation_failure(self, data_migrator, temp_dir):
        """Test CSV migration with validation failure."""
        # Create invalid CSV file (missing required columns)
        invalid_data = pd.DataFrame({"wrong_column": [1, 2, 3]})
        csv_file = temp_dir / "invalid.csv"
        invalid_data.to_csv(csv_file, index=False)

        table_mapping = {
            "invalid.csv": {
                "table": "artist_performance_summary",
                "columns": {"artist_name": "artist_name", "total_videos": "total_videos"},  # Required but missing
            }
        }

        # Execute migration
        result = data_migrator.migrate_csv_files(source_dir=str(temp_dir), table_mapping=table_mapping)

        # Verify validation failure
        assert result.success is False
        assert len(result.errors) > 0
        assert "missing required columns" in result.errors[0].lower()

    @patch("pandas.DataFrame.to_sql")
    def test_migrate_json_to_database_success(self, mock_to_sql, data_migrator, temp_dir, sample_json_data):
        """Test successful JSON migration to database."""
        # Create test JSON file
        json_file = temp_dir / "artist_colors.json"
        with open(json_file, "w") as f:
            json.dump(sample_json_data, f)

        # Mock successful database insertion
        mock_to_sql.return_value = None  # to_sql returns None on success

        # Define table mapping
        table_mapping = {
            "artist_colors.json": {
                "table": "artist_aliases",
                "key_column": "canonical_name",
                "value_column": "alias_name",
                "transform": "key_value_pairs",
            }
        }

        # Execute migration
        result = data_migrator.migrate_json_files(source_dir=str(temp_dir), table_mapping=table_mapping)

        # Verify results
        assert result.success is True
        assert result.records_migrated == 2
        assert len(result.errors) == 0

    def test_validate_migration_data_integrity(self, data_migrator, sample_csv_data):
        """Test data integrity validation during migration."""
        # Mock database query to return different data
        modified_data = sample_csv_data.copy()
        modified_data.loc[0, "total_views"] = 999999  # Different value

        with patch.object(data_migrator, "_query_migrated_data", return_value=modified_data):
            result = data_migrator.validate_migration(
                source_data=sample_csv_data, table_name="artist_performance_summary", key_columns=["artist_name"]
            )

        # Should detect data integrity issue
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert "data integrity" in result.errors[0].lower()

    def test_create_backup_success(self, data_migrator, temp_dir, sample_csv_data):
        """Test successful backup creation."""
        # Create test files
        csv_file = temp_dir / "test.csv"
        sample_csv_data.to_csv(csv_file, index=False)

        json_file = temp_dir / "test.json"
        with open(json_file, "w") as f:
            json.dump({"test": "data"}, f)

        # Create backup
        result = data_migrator.create_backup([str(csv_file), str(json_file)])

        # Verify backup was created
        assert result.success is True
        assert result.backup_path is not None
        assert Path(result.backup_path).exists()

        # Verify backup contains files
        backup_files = list(Path(result.backup_path).glob("*"))
        assert len(backup_files) == 2

    def test_archive_migrated_files(self, data_migrator, temp_dir, sample_csv_data):
        """Test archiving of successfully migrated files."""
        # Create test file
        csv_file = temp_dir / "test.csv"
        sample_csv_data.to_csv(csv_file, index=False)

        # Archive file
        result = data_migrator.archive_migrated_files(
            source_files=[str(csv_file)], archive_dir=str(temp_dir / "archive")
        )

        # Verify archiving
        assert result.success is True
        assert not csv_file.exists()  # Original file should be moved
        assert (temp_dir / "archive" / "test.csv").exists()  # File should be in archive

    def test_get_table_mapping_for_known_files(self, data_migrator):
        """Test automatic table mapping generation for known file types."""
        mapping = data_migrator.get_table_mapping_for_file("artist_music_summary.csv")

        assert mapping is not None
        assert mapping["table"] == "artist_performance_summary"
        assert "artist_name" in mapping["columns"]

    def test_get_table_mapping_for_unknown_files(self, data_migrator):
        """Test table mapping for unknown file types."""
        mapping = data_migrator.get_table_mapping_for_file("unknown_file.csv")
        assert mapping is None

    @patch("pandas.DataFrame.to_sql")
    def test_migration_error_handling(self, mock_to_sql, data_migrator, temp_dir):
        """Test proper error handling during migration."""
        # Create file that will cause database error
        csv_file = temp_dir / "test.csv"
        pd.DataFrame({"test": [1, 2, 3]}).to_csv(csv_file, index=False)

        # Mock database error
        mock_to_sql.side_effect = Exception("DB Error")

        table_mapping = {"test.csv": {"table": "test_table", "columns": {"test": "test_column"}}}

        # Execute migration
        result = data_migrator.migrate_csv_files(source_dir=str(temp_dir), table_mapping=table_mapping)

        # Verify error handling
        assert result.success is False
        assert len(result.errors) > 0
        assert "DB Error" in str(result.errors)


class TestMigrationResult:
    """Test suite for MigrationResult class."""

    def test_migration_result_creation(self):
        """Test MigrationResult creation and properties."""
        result = MigrationResult(
            source_files=["file1.csv", "file2.json"],
            target_tables=["table1", "table2"],
            records_migrated=100,
            errors=["Error 1"],
            warnings=["Warning 1"],
            duration_seconds=5.5,
            success=True,
        )

        assert result.source_files == ["file1.csv", "file2.json"]
        assert result.records_migrated == 100
        assert result.success is True

    def test_migration_result_to_dict(self):
        """Test MigrationResult serialization to dictionary."""
        result = MigrationResult(
            source_files=["file1.csv"],
            target_tables=["table1"],
            records_migrated=50,
            errors=[],
            warnings=[],
            duration_seconds=2.0,
            success=True,
        )

        result_dict = result.to_dict()
        assert result_dict["records_migrated"] == 50
        assert result_dict["success"] is True

    def test_migration_result_report_generation(self):
        """Test MigrationResult report generation."""
        result = MigrationResult(
            source_files=["test.csv"],
            target_tables=["test_table"],
            records_migrated=25,
            errors=["Test error"],
            warnings=["Test warning"],
            duration_seconds=1.5,
            success=False,
        )

        report = result.generate_report()
        assert "Migration Report" in report
        assert "Records Migrated: 25" in report
        assert "Test error" in report
        assert "Test warning" in report


class TestValidationResult:
    """Test suite for ValidationResult class."""

    def test_validation_result_creation(self):
        """Test ValidationResult creation."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Minor issue"],
            checked_items=100,
            passed_items=99,
            metadata={"test": "data"},
        )

        assert result.is_valid is True
        assert result.checked_items == 100
        assert result.passed_items == 99

    def test_validation_result_add_error(self):
        """Test adding errors to ValidationResult."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0, metadata={})

        result.add_error("Test error")
        assert len(result.errors) == 1
        assert result.errors[0] == "Test error"
        assert result.is_valid is False  # Should become invalid when error added

    def test_validation_result_merge(self):
        """Test merging ValidationResult instances."""
        result1 = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Warning 1"],
            checked_items=50,
            passed_items=50,
            metadata={"test1": "data1"},
        )

        result2 = ValidationResult(
            is_valid=False,
            errors=["Error 1"],
            warnings=["Warning 2"],
            checked_items=30,
            passed_items=25,
            metadata={"test2": "data2"},
        )

        merged = result1.merge(result2)
        assert merged.is_valid is False  # Should be False if any result is invalid
        assert len(merged.errors) == 1
        assert len(merged.warnings) == 2
        assert merged.checked_items == 80  # Sum of both
        assert merged.passed_items == 75  # Sum of both
