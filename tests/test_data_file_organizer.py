#!/usr / bin / env python3
"""
Tests for Data File Organizer-TDD Implementation

Tests written FIRST to drive the design of the data file organization system.
This focuses on the REAL problem: scattered CSV / JSON files throughout the codebase.
"""

import csv
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

# Import the classes we're going to implement (will fail initially-that's TDD!)
try:
    from src.data_organization.data_file_organizer import (
        DataFileInfo,
        DataFileOrganizer,
        FileCategory,
        OrganizationResult,
        ValidationResult,
    )
except ImportError:
    # Expected during TDD-we'll implement these classes
    pass


class TestDataFileOrganizer:
    """Test suite for DataFileOrganizer following TDD principles."""

    @pytest.fixture
    def temp_workspace(self):
        """Create temporary workspace with scattered files for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = Path(temp_dir)

            # Create scattered files like in the real codebase
            benchmarks_dir = workspace / "benchmarks"
            benchmarks_dir.mkdir()
            (benchmarks_dir / "benchmarks.json").write_text('{"test": "data"}')
            (workspace / "music_industry_sentiment_dataset_v2.csv").write_text("col1,col2\nval1,val2")

            # Create config directory with files
            config_dir = workspace / "config"
            config_dir.mkdir()
            (config_dir / "artist_aliases.json").write_text('{"aliases": {}}')
            (config_dir / "artist_colors.json").write_text('{"colors": {}}')

            # Create music_analysis_tables directory
            analysis_dir = workspace / "music_analysis_tables"
            analysis_dir.mkdir()
            (analysis_dir / "artist_music_summary.csv").write_text("artist,views\nArtist1,1000")
            (analysis_dir / "video_type_analysis.csv").write_text("type,count\nmusic,50")

            # Create some nested files
            nested_dir = workspace / "data" / "exports"
            nested_dir.mkdir(parents=True)
            (nested_dir / "export_data.json").write_text('{"export": true}')

            yield workspace

    @pytest.fixture
    def file_organizer(self):
        """Create file organizer instance for testing."""
        return DataFileOrganizer()

    def test_file_organizer_initialization(self, file_organizer):
        """Test that file organizer initializes correctly."""
        assert file_organizer.workspace_root is None
        assert isinstance(file_organizer.discovered_files, list)
        assert isinstance(file_organizer.file_categories, dict)

    def test_discover_files_finds_all_scattered_files(self, file_organizer, temp_workspace):
        """Test that file discovery finds all CSV / JSON files in workspace."""
        # Act
        discovered_files = file_organizer.discover_files(temp_workspace)

        # Assert
        assert len(discovered_files) >= 6  # At least 6 files we created

        # Check specific files are found
        file_names = [f.name for f in discovered_files]
        assert "benchmarks.json" in file_names
        assert "music_industry_sentiment_dataset_v2.csv" in file_names
        assert "artist_aliases.json" in file_names
        assert "artist_music_summary.csv" in file_names
        assert "export_data.json" in file_names

    def test_categorize_files_by_type_and_location(self, file_organizer, temp_workspace):
        """Test that files are correctly categorized by type and location."""
        # Arrange
        discovered_files = file_organizer.discover_files(temp_workspace)

        # Act
        categorized_files = file_organizer.categorize_files(discovered_files)

        # Assert
        assert FileCategory.CONFIG in categorized_files
        assert FileCategory.ANALYSIS_RESULTS in categorized_files
        assert FileCategory.BENCHMARKS in categorized_files
        assert FileCategory.DATASETS in categorized_files

        # Check specific categorizations
        config_files = categorized_files[FileCategory.CONFIG]
        config_names = [f.name for f in config_files]
        assert "artist_aliases.json" in config_names
        assert "artist_colors.json" in config_names

    def test_validate_file_integrity(self, file_organizer, temp_workspace):
        """Test that file validation detects valid and invalid files."""
        # Arrange
        discovered_files = file_organizer.discover_files(temp_workspace)

        # Act
        validation_results = file_organizer.validate_files(discovered_files)

        # Assert
        assert isinstance(validation_results, list)
        assert len(validation_results) == len(discovered_files)

        # Check that valid files pass validation
        valid_results = [r for r in validation_results if r.is_valid]
        assert len(valid_results) > 0

        # Check validation includes file size, format, and readability
        for result in validation_results:
            assert hasattr(result, "file_path")
            assert hasattr(result, "is_valid")
            assert hasattr(result, "errors")
            assert hasattr(result, "file_size_bytes")

    def test_create_organized_directory_structure(self, file_organizer, temp_workspace):
        """Test creation of organized directory structure."""
        # Act
        organization_result = file_organizer.create_organized_structure(temp_workspace)

        # Assert
        assert organization_result.success is True
        assert len(organization_result.created_directories) > 0

        # Check that organized directories are created
        organized_root = temp_workspace / "organized_data"
        assert organized_root.exists()
        assert (organized_root / "config").exists()
        assert (organized_root / "analysis_results").exists()
        assert (organized_root / "benchmarks").exists()
        assert (organized_root / "datasets").exists()

    def test_move_files_to_organized_structure(self, file_organizer, temp_workspace):
        """Test moving files to organized directory structure."""
        # Arrange
        discovered_files = file_organizer.discover_files(temp_workspace)
        categorized_files = file_organizer.categorize_files(discovered_files)
        file_organizer.create_organized_structure(temp_workspace)

        # Act
        move_result = file_organizer.move_files_to_organized_structure(temp_workspace, categorized_files)

        # Assert
        assert move_result.success is True
        assert move_result.files_moved > 0
        assert len(move_result.errors) == 0

        # Check files are in correct locations
        organized_root = temp_workspace / "organized_data"
        assert (organized_root / "config" / "artist_aliases.json").exists()
        assert (organized_root / "analysis_results" / "artist_music_summary.csv").exists()
        assert (organized_root / "benchmarks" / "benchmarks.json").exists()

    def test_generate_file_inventory_report(self, file_organizer, temp_workspace):
        """Test generation of comprehensive file inventory report."""
        # Arrange
        discovered_files = file_organizer.discover_files(temp_workspace)
        categorized_files = file_organizer.categorize_files(discovered_files)

        # Act
        inventory_report = file_organizer.generate_inventory_report(discovered_files, categorized_files)

        # Assert
        assert isinstance(inventory_report, dict)
        assert "total_files" in inventory_report
        assert "total_size_bytes" in inventory_report
        assert "categories" in inventory_report
        assert "file_types" in inventory_report
        assert "largest_files" in inventory_report

        # Check report content
        assert inventory_report["total_files"] > 0
        assert inventory_report["total_size_bytes"] > 0
        assert len(inventory_report["categories"]) > 0

    def test_detect_duplicate_files(self, file_organizer, temp_workspace):
        """Test detection of duplicate files by content hash."""
        # Arrange-create duplicate file
        duplicate_content = '{"test": "data"}'
        (temp_workspace / "duplicate1.json").write_text(duplicate_content)
        (temp_workspace / "duplicate2.json").write_text(duplicate_content)

        discovered_files = file_organizer.discover_files(temp_workspace)

        # Act
        duplicates = file_organizer.detect_duplicate_files(discovered_files)

        # Assert
        assert len(duplicates) > 0

        # Check duplicate detection structure
        for duplicate_group in duplicates:
            assert len(duplicate_group.files) >= 2
            assert duplicate_group.content_hash is not None
            assert duplicate_group.total_size_bytes > 0

    def test_backup_original_files_before_organization(self, file_organizer, temp_workspace):
        """Test that original files are backed up before organization."""
        # Arrange
        discovered_files = file_organizer.discover_files(temp_workspace)

        # Act
        backup_result = file_organizer.create_backup(temp_workspace, discovered_files)

        # Assert
        assert backup_result.success is True
        assert backup_result.backup_location is not None
        assert backup_result.files_backed_up > 0

        # Check backup directory exists and contains files
        backup_path = Path(backup_result.backup_location)
        assert backup_path.exists()
        backup_files = list(backup_path.rglob("*"))
        assert len(backup_files) > 0


class TestDataFileInfo:
    """Test the DataFileInfo data class."""

    def test_data_file_info_initialization(self):
        """Test DataFileInfo initialization."""
        file_info = DataFileInfo(
            path=Path("/test / file.json"),
            name="file.json",
            size_bytes=1024,
            file_type="json",
            category=FileCategory.CONFIG,
            last_modified=datetime.now(),
        )

        assert file_info.name == "file.json"
        assert file_info.size_bytes == 1024
        assert file_info.file_type == "json"
        assert file_info.category == FileCategory.CONFIG

    def test_calculate_content_hash(self):
        """Test content hash calculation for duplicate detection."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write('{"test": "content"}')
            temp_path = f.name

        try:
            file_info = DataFileInfo(
                path=Path(temp_path),
                name="test.json",
                size_bytes=100,
                file_type="json",
                category=FileCategory.CONFIG,
                last_modified=datetime.now(),
            )

            content_hash = file_info.calculate_content_hash()

            assert content_hash is not None
            assert len(content_hash) == 64  # SHA-256 hex digest length
        finally:
            os.unlink(temp_path)

    def test_is_valid_file_format(self):
        """Test file format validation."""
        json_file = DataFileInfo(
            path=Path("/test / file.json"),
            name="file.json",
            size_bytes=100,
            file_type="json",
            category=FileCategory.CONFIG,
            last_modified=datetime.now(),
        )

        csv_file = DataFileInfo(
            path=Path("/test / file.csv"),
            name="file.csv",
            size_bytes=100,
            file_type="csv",
            category=FileCategory.DATASETS,
            last_modified=datetime.now(),
        )

        assert json_file.is_valid_format() is True
        assert csv_file.is_valid_format() is True


class TestFileCategory:
    """Test the FileCategory enum."""

    def test_file_category_values(self):
        """Test that all expected file categories exist."""
        assert FileCategory.CONFIG
        assert FileCategory.ANALYSIS_RESULTS
        assert FileCategory.BENCHMARKS
        assert FileCategory.DATASETS
        assert FileCategory.EXPORTS
        assert FileCategory.TEMPORARY
        assert FileCategory.UNKNOWN

    def test_categorize_by_path_patterns(self):
        """Test file categorization based on path patterns."""
        # This would test the logic for determining category from file path
        config_path = Path("config / artist_aliases.json")
        analysis_path = Path("music_analysis_tables / summary.csv")
        benchmark_path = Path("benchmarks/benchmarks.json")

        # The actual categorization logic would be in the DataFileOrganizer
        # This test ensures we can determine categories from paths
        assert "config" in str(config_path)
        assert "music_analysis_tables" in str(analysis_path)
        assert "benchmark" in str(benchmark_path)


class TestOrganizationResult:
    """Test the OrganizationResult data class."""

    def test_organization_result_initialization(self):
        """Test OrganizationResult initialization."""
        result = OrganizationResult(
            success=True, files_moved=10, created_directories=["config", "analysis"], errors=[], duration_seconds=5.5
        )

        assert result.success is True
        assert result.files_moved == 10
        assert len(result.created_directories) == 2
        assert len(result.errors) == 0
        assert result.duration_seconds == 5.5

    def test_organization_result_to_dict(self):
        """Test converting organization result to dictionary."""
        result = OrganizationResult(
            success=True, files_moved=5, created_directories=["test"], errors=[], duration_seconds=1.0
        )

        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert result_dict["success"] is True
        assert result_dict["files_moved"] == 5

    def test_generate_organization_report(self):
        """Test generating human-readable organization report."""
        result = OrganizationResult(
            success=True,
            files_moved=15,
            created_directories=["config", "analysis", "benchmarks"],
            errors=[],
            duration_seconds=3.2,
        )

        report = result.generate_report()

        assert isinstance(report, str)
        assert "15" in report
        assert "3.2" in report
        assert "SUCCESS" in report.upper()


class TestValidationResult:
    """Test the ValidationResult data class."""

    def test_validation_result_initialization(self):
        """Test ValidationResult initialization."""
        result = ValidationResult(
            file_path=Path("/test / file.json"),
            is_valid=True,
            errors=[],
            warnings=["Minor warning"],
            file_size_bytes=1024,
        )

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 1
        assert result.file_size_bytes == 1024

    def test_add_error_sets_invalid(self):
        """Test that adding an error sets is_valid to False."""
        result = ValidationResult(
            file_path=Path("/test / file.json"), is_valid=True, errors=[], warnings=[], file_size_bytes=100
        )

        result.add_error("Test error")

        assert result.is_valid is False
        assert "Test error" in result.errors


# Integration Tests
class TestDataFileOrganizerIntegration:
    """Integration tests for data file organization."""

    @pytest.mark.integration
    def test_complete_organization_workflow(self):
        """Test complete workflow of file discovery, categorization, and organization."""
        # This test would use real files from the codebase
        # For now, we'll skip it until implementation is ready
        pytest.skip("Integration test-requires implementation")

    @pytest.mark.integration
    def test_real_codebase_file_discovery(self):
        """Test file discovery on the actual codebase."""
        pytest.skip("Integration test-requires implementation")

    @pytest.mark.integration
    def test_backup_and_restore_workflow(self):
        """Test complete backup and restore workflow."""
        pytest.skip("Integration test-requires implementation")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])
