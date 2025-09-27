"""
Data migration system for moving CSV / JSON files to database tables.

This module provides functionality to migrate scattered data files into
organized database tables while maintaining data integrity and providing
comprehensive validation.
"""

from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import shutil
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MigrationError(Exception):
    """Base class for migration errors."""

    pass


class DataIntegrityError(MigrationError):
    """Raised when data integrity checks fail."""

    pass


class SchemaValidationError(MigrationError):
    """Raised when schema validation fails."""

    pass


class FileAccessError(MigrationError):
    """Raised when file access fails during migration."""

    pass


@dataclass
class MigrationResult:
    """Result of data migration operation."""

    source_files: List[str]
    target_tables: List[str]
    records_migrated: int
    errors: List[str]
    warnings: List[str]
    duration_seconds: float
    success: bool
    backup_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "source_files": self.source_files,
            "target_tables": self.target_tables,
            "records_migrated": self.records_migrated,
            "errors": self.errors,
            "warnings": self.warnings,
            "duration_seconds": self.duration_seconds,
            "success": self.success,
            "backup_path": self.backup_path,
        }

    def generate_report(self) -> str:
        """Generate human - readable migration report."""
        status = "SUCCESS" if self.success else "FAILED"
        report = f"""
Migration Report - {status}
{'=' * 50}

Files Processed: {len(self.source_files)}
Tables Updated: {len(self.target_tables)}
Records Migrated: {self.records_migrated}
Duration: {self.duration_seconds:.2f} seconds

Source Files:
{chr(10).join(f"  - {file}" for file in self.source_files)}

Target Tables:
{chr(10).join(f"  - {table}" for table in self.target_tables)}
"""

        if self.warnings:
            report += f"\nWarnings ({len(self.warnings)}):\n"
            report += "\n".join(f"  - {warning}" for warning in self.warnings)

        if self.errors:
            report += f"\nErrors ({len(self.errors)}):\n"
            report += "\n".join(f"  - {error}" for error in self.errors)

        if self.backup_path:
            report += f"\nBackup Location: {self.backup_path}"

        return report


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checked_items: int = 0
    passed_items: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, error: str) -> None:
        """Add an error and mark validation as invalid."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning without affecting validity."""
        self.warnings.append(warning)

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge two validation results."""
        return ValidationResult(
            is_valid=self.is_valid and other.is_valid,
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
            checked_items=self.checked_items + other.checked_items,
            passed_items=self.passed_items + other.passed_items,
            metadata={**self.metadata, **other.metadata},
        )


class DataMigrator:
    """
    Migrates CSV / JSON files to database tables with validation and backup.

    This class handles the migration of scattered data files into organized
    database tables while ensuring data integrity and providing rollback
    capabilities.
    """

    def __init__(self, engine: Engine):
        """
        Initialize DataMigrator with database engine.

        Args:
            engine: SQLAlchemy database engine
        """
        self.engine = engine
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def migrate_csv_files(self, source_dir: str, table_mapping: Dict[str, Dict[str, Any]]) -> MigrationResult:
        """
        Migrate CSV files to database tables.

        Args:
            source_dir: Directory containing CSV files
            table_mapping: Mapping of CSV files to database tables

        Returns:
            MigrationResult with migration details
        """
        start_time = time.time()
        source_files = []
        target_tables = []
        total_records = 0
        errors = []
        warnings = []

        try:
            source_path = Path(source_dir)
            if not source_path.exists():
                raise FileAccessError(f"Source directory does not exist: {source_dir}")

            # Process each CSV file in the mapping
            for filename, mapping in table_mapping.items():
                file_path = source_path / filename

                if not file_path.exists():
                    warnings.append(f"File not found: {filename}")
                    continue

                try:
                    # Load and validate CSV data
                    df = pd.read_csv(file_path)
                    validation_result = self._validate_csv_structure(df, mapping)

                    if not validation_result.is_valid:
                        errors.extend([f"{filename}: {error}" for error in validation_result.errors])
                        continue

                    # Transform data according to mapping
                    transformed_df = self._transform_csv_data(df, mapping)

                    # Insert data into database
                    records_inserted = self._insert_dataframe_to_table(transformed_df, mapping["table"])

                    source_files.append(str(file_path))
                    target_tables.append(mapping["table"])
                    total_records += records_inserted

                    self.logger.info(f"Migrated {records_inserted} records from {filename} to {mapping['table']}")

                except Exception as e:
                    error_msg = f"Failed to migrate {filename}: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)

        except Exception as e:
            errors.append(f"Migration failed: {str(e)}")
            self.logger.error(f"Migration failed: {str(e)}")

        duration = time.time() - start_time
        success = len(errors) == 0 and total_records > 0

        return MigrationResult(
            source_files=source_files,
            target_tables=list(set(target_tables)),
            records_migrated=total_records,
            errors=errors,
            warnings=warnings,
            duration_seconds=duration,
            success=success,
        )

    def migrate_json_files(self, source_dir: str, table_mapping: Dict[str, Dict[str, Any]]) -> MigrationResult:
        """
        Migrate JSON files to database tables.

        Args:
            source_dir: Directory containing JSON files
            table_mapping: Mapping of JSON files to database tables

        Returns:
            MigrationResult with migration details
        """
        start_time = time.time()
        source_files = []
        target_tables = []
        total_records = 0
        errors = []
        warnings = []

        try:
            source_path = Path(source_dir)
            if not source_path.exists():
                raise FileAccessError(f"Source directory does not exist: {source_dir}")

            # Process each JSON file in the mapping
            for filename, mapping in table_mapping.items():
                file_path = source_path / filename

                if not file_path.exists():
                    warnings.append(f"File not found: {filename}")
                    continue

                try:
                    # Load and validate JSON data
                    with open(file_path, "r") as f:
                        json_data = json.load(f)

                    validation_result = self._validate_json_structure(json_data, mapping)

                    if not validation_result.is_valid:
                        errors.extend([f"{filename}: {error}" for error in validation_result.errors])
                        continue

                    # Transform JSON data to DataFrame
                    df = self._transform_json_data(json_data, mapping)

                    # Insert data into database
                    records_inserted = self._insert_dataframe_to_table(df, mapping["table"])

                    source_files.append(str(file_path))
                    target_tables.append(mapping["table"])
                    total_records += records_inserted

                    self.logger.info(f"Migrated {records_inserted} records from {filename} to {mapping['table']}")

                except Exception as e:
                    error_msg = f"Failed to migrate {filename}: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)

        except Exception as e:
            errors.append(f"Migration failed: {str(e)}")
            self.logger.error(f"Migration failed: {str(e)}")

        duration = time.time() - start_time
        success = len(errors) == 0 and total_records > 0

        return MigrationResult(
            source_files=source_files,
            target_tables=list(set(target_tables)),
            records_migrated=total_records,
            errors=errors,
            warnings=warnings,
            duration_seconds=duration,
            success=success,
        )

    def validate_migration(
        self, source_data: pd.DataFrame, table_name: str, key_columns: List[str]
    ) -> ValidationResult:
        """
        Validate data integrity after migration.

        Args:
            source_data: Original data before migration
            table_name: Target database table name
            key_columns: Columns to use for data comparison

        Returns:
            ValidationResult with integrity check results
        """
        try:
            # Query migrated data from database
            migrated_data = self._query_migrated_data(table_name, key_columns)

            # Compare source and migrated data
            validation_result = ValidationResult(is_valid=True)

            # Check record counts
            if len(source_data) != len(migrated_data):
                validation_result.add_error(
                    f"Record count mismatch: source={len(source_data)}, migrated={len(migrated_data)}"
                )

            # Check data integrity for each record
            for _, source_row in source_data.iterrows():
                key_values = {col: source_row[col] for col in key_columns}

                # Find matching row in migrated data
                migrated_row = migrated_data
                for col, val in key_values.items():
                    migrated_row = migrated_row[migrated_row[col] == val]

                if migrated_row.empty:
                    validation_result.add_error(f"Missing record with keys: {key_values}")
                    continue

                # Compare data values (excluding auto - generated columns)
                for col in source_data.columns:
                    if col in migrated_row.columns:
                        source_val = source_row[col]
                        migrated_val = migrated_row.iloc[0][col]

                        # Handle NaN comparisons
                        if pd.isna(source_val) and pd.isna(migrated_val):
                            continue

                        if source_val != migrated_val:
                            validation_result.add_error(
                                f"Data integrity issue for {key_values}, column {col}: "
                                f"source={source_val}, migrated={migrated_val}"
                            )

            validation_result.checked_items = len(source_data)
            validation_result.passed_items = len(source_data) - len(validation_result.errors)

            return validation_result

        except Exception as e:
            validation_result = ValidationResult(is_valid=False)
            validation_result.add_error(f"Validation failed: {str(e)}")
            return validation_result

    def create_backup(self, files: List[str]) -> MigrationResult:
        """
        Create backup of files before migration.

        Args:
            files: List of file paths to backup

        Returns:
            MigrationResult with backup details
        """
        start_time = time.time()
        errors = []
        warnings = []

        try:
            # Create backup directory with timestamp and unique identifier
            timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
            import uuid

            unique_id = str(uuid.uuid4())[:8]
            backup_dir = Path(f"data_migration_backup_{timestamp}_{unique_id}")
            backup_dir.mkdir(exist_ok=True)

            backed_up_files = []

            for file_path in files:
                source_path = Path(file_path)

                if not source_path.exists():
                    warnings.append(f"File not found for backup: {file_path}")
                    continue

                try:
                    # Copy file to backup directory
                    backup_file_path = backup_dir / source_path.name
                    shutil.copy2(source_path, backup_file_path)
                    backed_up_files.append(str(backup_file_path))

                except Exception as e:
                    errors.append(f"Failed to backup {file_path}: {str(e)}")

            duration = time.time() - start_time
            success = len(errors) == 0 and len(backed_up_files) > 0

            return MigrationResult(
                source_files=files,
                target_tables=[],
                records_migrated=0,
                errors=errors,
                warnings=warnings,
                duration_seconds=duration,
                success=success,
                backup_path=str(backup_dir) if success else None,
            )

        except Exception as e:
            duration = time.time() - start_time
            return MigrationResult(
                source_files=files,
                target_tables=[],
                records_migrated=0,
                errors=[f"Backup failed: {str(e)}"],
                warnings=warnings,
                duration_seconds=duration,
                success=False,
            )

    def archive_migrated_files(self, source_files: List[str], archive_dir: str) -> MigrationResult:
        """
        Archive successfully migrated files.

        Args:
            source_files: List of source file paths to archive
            archive_dir: Directory to move files to

        Returns:
            MigrationResult with archiving details
        """
        start_time = time.time()
        errors = []
        warnings = []
        archived_files = []

        try:
            archive_path = Path(archive_dir)
            archive_path.mkdir(parents=True, exist_ok=True)

            for file_path in source_files:
                source_path = Path(file_path)

                if not source_path.exists():
                    warnings.append(f"File not found for archiving: {file_path}")
                    continue

                try:
                    # Move file to archive directory
                    archive_file_path = archive_path / source_path.name
                    shutil.move(str(source_path), str(archive_file_path))
                    archived_files.append(str(archive_file_path))

                except Exception as e:
                    errors.append(f"Failed to archive {file_path}: {str(e)}")

            duration = time.time() - start_time
            success = len(errors) == 0

            return MigrationResult(
                source_files=source_files,
                target_tables=[],
                records_migrated=0,
                errors=errors,
                warnings=warnings,
                duration_seconds=duration,
                success=success,
            )

        except Exception as e:
            duration = time.time() - start_time
            return MigrationResult(
                source_files=source_files,
                target_tables=[],
                records_migrated=0,
                errors=[f"Archiving failed: {str(e)}"],
                warnings=warnings,
                duration_seconds=duration,
                success=False,
            )

    def get_table_mapping_for_file(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Get automatic table mapping for known file types.

        Args:
            filename: Name of the file to get mapping for

        Returns:
            Table mapping dictionary or None if unknown
        """
        # Known file mappings based on existing data structure
        known_mappings = {
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

        return known_mappings.get(filename)

    def _validate_csv_structure(self, df: pd.DataFrame, mapping: Dict[str, Any]) -> ValidationResult:
        """Validate CSV structure against mapping requirements."""
        result = ValidationResult(is_valid=True)

        required_columns = set(mapping.get("columns", {}).keys())
        actual_columns = set(df.columns)

        missing_columns = required_columns - actual_columns
        if missing_columns:
            result.add_error(f"Missing required columns: {missing_columns}")

        # Check for empty DataFrame
        if df.empty:
            result.add_warning("DataFrame is empty")

        result.checked_items = len(required_columns)
        result.passed_items = len(required_columns) - len(missing_columns)

        return result

    def _validate_json_structure(self, json_data: Any, mapping: Dict[str, Any]) -> ValidationResult:
        """Validate JSON structure against mapping requirements."""
        result = ValidationResult(is_valid=True)

        transform_type = mapping.get("transform", "direct")

        if transform_type == "key_value_pairs":
            if not isinstance(json_data, dict):
                result.add_error("Expected JSON object for key - value transformation")
            elif len(json_data) == 0:
                result.add_warning("JSON object is empty")

        result.checked_items = 1
        result.passed_items = 1 if result.is_valid else 0

        return result

    def _transform_csv_data(self, df: pd.DataFrame, mapping: Dict[str, Any]) -> pd.DataFrame:
        """Transform CSV data according to column mapping."""
        column_mapping = mapping.get("columns", {})

        # Select and rename columns
        transformed_df = df[list(column_mapping.keys())].copy()
        transformed_df = transformed_df.rename(columns=column_mapping)

        return transformed_df

    def _transform_json_data(self, json_data: Any, mapping: Dict[str, Any]) -> pd.DataFrame:
        """Transform JSON data to DataFrame according to mapping."""
        transform_type = mapping.get("transform", "direct")

        if transform_type == "key_value_pairs":
            # Convert key - value pairs to DataFrame
            key_column = mapping.get("key_column", "key")
            value_column = mapping.get("value_column", "value")

            data = [{key_column: key, value_column: value} for key, value in json_data.items()]

            return pd.DataFrame(data)

        else:
            # Direct transformation (assume JSON is already tabular)
            return pd.DataFrame(json_data)

    def _insert_dataframe_to_table(self, df: pd.DataFrame, table_name: str) -> int:
        """Insert DataFrame data into database table."""
        try:
            with self.engine.connect() as conn:
                # Use pandas to_sql for efficient insertion
                df.to_sql(name=table_name, con=conn, if_exists="append", index=False, method="multi")

                conn.commit()
                return len(df)

        except SQLAlchemyError as e:
            raise MigrationError(f"Database insertion failed: {str(e)}")
        except Exception as e:
            raise MigrationError(f"Database insertion failed: {str(e)}")

    def _query_migrated_data(self, table_name: str, key_columns: List[str]) -> pd.DataFrame:
        """Query migrated data from database for validation."""
        try:
            with self.engine.connect() as conn:
                # Build query to select relevant columns
                columns_str = ", ".join(key_columns + ["*"])
                query = text(f"SELECT * FROM {table_name}")

                result = conn.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                return df

        except SQLAlchemyError as e:
            raise MigrationError(f"Database query failed: {str(e)}")
