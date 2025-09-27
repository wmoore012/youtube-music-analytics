#!/usr / bin / env python3
"""
Data Quality Validation Framework

This module provides comprehensive data quality checks for the YouTube ETL pipeline.
It implements bulletproof validation with clear error messages and detailed context.
"""

from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from .error_handling import (
    ErrorCategory,
    ErrorContext,
    ErrorSeverity,
    ETLError,
    ValidationError,
)

logger = logging.getLogger(__name__)


class DataQualityError(ETLError):
    """Specific error for data quality issues."""

    def __init__(self, message: str, table: Optional[str] = None, column: Optional[str] = None, **kwargs):
        self.table = table
        self.column = column
        context = kwargs.get("context")
        if context:
            if table:
                context.user_data["table"] = table
            if column:
                context.user_data["column"] = column
        super().__init__(message, category=ErrorCategory.DATA_QUALITY, **kwargs)


class SchemaValidationError(ETLError):
    """Specific error for schema validation issues."""

    def __init__(
        self, message: str, expected_schema: Optional[Dict] = None, actual_schema: Optional[Dict] = None, **kwargs
    ):
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema
        context = kwargs.get("context")
        if context:
            if expected_schema:
                context.user_data["expected_schema"] = expected_schema
            if actual_schema:
                context.user_data["actual_schema"] = actual_schema
        super().__init__(message, category=ErrorCategory.VALIDATION, **kwargs)


class DataQualityValidator:
    """Comprehensive data quality validation for YouTube analytics data."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def validate_youtube_video_id(self, video_id: str, context_info: str = "") -> None:
        """
        Validate YouTube video ID format and existence.

        Args:
            video_id: YouTube video ID to validate
            context_info: Additional context for error messages

        Raises:
            ValidationError: If video ID is invalid
        """
        context = ErrorContext(
            component="DataQualityValidator",
            operation="validate_youtube_video_id",
            user_data={"video_id": video_id, "context": context_info},
        )

        if not video_id:
            raise ValidationError(
                "Video ID cannot be empty or None",
                field="video_id",
                value=video_id,
                context=context,
                severity=ErrorSeverity.HIGH,
            )

        if not isinstance(video_id, str):
            raise ValidationError(
                f"Video ID must be a string, got {type(video_id).__name__}",
                field="video_id",
                value=video_id,
                context=context,
                severity=ErrorSeverity.HIGH,
            )

        # YouTube video IDs are exactly 11 characters
        if len(video_id) != 11:
            raise ValidationError(
                f"YouTube video ID must be exactly 11 characters, got {len(video_id)} characters: '{video_id}'",
                field="video_id",
                value=video_id,
                context=context,
                severity=ErrorSeverity.HIGH,
            )

        # YouTube video IDs contain only alphanumeric characters, hyphens, and underscores
        import re

        if not re.match(r"^[a - zA - Z0 - 9_-]{11}$", video_id):
            raise ValidationError(
                f"YouTube video ID contains invalid characters: '{
                    video_id}'. Must contain only letters, numbers, hyphens, and underscores",
                field="video_id",
                value=video_id,
                context=context,
                severity=ErrorSeverity.HIGH,
            )

    def validate_metrics_data(self, metrics_data: Dict[str, Any], video_id: str) -> None:
        """
        Validate YouTube metrics data for consistency and reasonable values.

        Args:
            metrics_data: Dictionary containing metrics data
            video_id: Associated video ID for context

        Raises:
            DataQualityError: If metrics data is invalid
        """
        context = ErrorContext(
            component="DataQualityValidator",
            operation="validate_metrics_data",
            user_data={"video_id": video_id, "metrics_keys": list(metrics_data.keys())},
        )

        required_fields = ["view_count", "like_count", "comment_count"]

        for field in required_fields:
            if field not in metrics_data:
                raise DataQualityError(
                    f"Required metrics field '{field}' is missing for video {video_id}",
                    column=field,
                    context=context,
                    severity=ErrorSeverity.HIGH,
                )

            value = metrics_data[field]

            # Check if value is numeric
            if not isinstance(value, (int, float)) or value < 0:
                raise DataQualityError(
                    f"Metrics field '{field}' must be a non - negative number, got {type(value).__name__}: {value}",
                    column=field,
                    context=context,
                    severity=ErrorSeverity.HIGH,
                )

        # Validate reasonable ranges
        view_count = metrics_data["view_count"]
        like_count = metrics_data["like_count"]
        comment_count = metrics_data["comment_count"]

        # Like count should not exceed view count
        if like_count > view_count:
            raise DataQualityError(
                f"Like count ({like_count:,}) cannot exceed view count ({view_count:,}) for video {video_id}",
                context=context,
                severity=ErrorSeverity.MEDIUM,
            )

        # Comment count should be reasonable relative to views
        if view_count > 0 and comment_count > view_count * 0.1:  # More than 10% comment rate is suspicious
            self.logger.warning(
                f"Unusually high comment rate for video {video_id}: {comment_count:,} comments on {view_count:,} views "
                f"({comment_count / view_count * 100:.1f}%)"
            )

    def validate_database_schema(self, table_name: str, expected_columns: List[str]) -> None:
        """
        Validate that database table has expected schema.

        Args:
            table_name: Name of table to validate
            expected_columns: List of expected column names

        Raises:
            SchemaValidationError: If schema doesn't match expectations
        """
        context = ErrorContext(
            component="DataQualityValidator",
            operation="validate_database_schema",
            user_data={"table_name": table_name, "expected_columns": expected_columns},
        )

        try:
            with self.engine.connect() as conn:
                # Get actual table schema
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                actual_columns = [row[0] for row in result.fetchall()]

                missing_columns = [col for col in expected_columns if col not in actual_columns]
                extra_columns = [col for col in actual_columns if col not in expected_columns]

                if missing_columns:
                    raise SchemaValidationError(
                        f"Table '{table_name}' is missing required columns: {missing_columns}",
                        expected_schema={"columns": expected_columns},
                        actual_schema={"columns": actual_columns},
                        context=context,
                        severity=ErrorSeverity.CRITICAL,
                    )

                if extra_columns:
                    self.logger.info(f"Table '{table_name}' has additional columns: {extra_columns}")

        except SQLAlchemyError as e:
            raise SchemaValidationError(
                f"Failed to validate schema for table '{table_name}': {str(e)}",
                context=context,
                original_error=e,
                severity=ErrorSeverity.CRITICAL,
            )

    def validate_data_freshness(self, table_name: str, timestamp_column: str, max_age_hours: int = 24) -> None:
        """
        Validate that data in table is fresh (not too old).

        Args:
            table_name: Name of table to check
            timestamp_column: Column containing timestamps
            max_age_hours: Maximum age in hours before data is considered stale

        Raises:
            DataQualityError: If data is too old
        """
        context = ErrorContext(
            component="DataQualityValidator",
            operation="validate_data_freshness",
            user_data={"table_name": table_name, "timestamp_column": timestamp_column, "max_age_hours": max_age_hours},
        )

        try:
            with self.engine.connect() as conn:
                query = text(
                    f"""
                    SELECT MAX({timestamp_column}) as latest_timestamp,
                           COUNT(*) as total_records
                    FROM {table_name}
                """
                )
                result = conn.execute(query).fetchone()

                if not result or result[1] == 0:
                    raise DataQualityError(
                        f"Table '{table_name}' is empty - no data to validate freshness",
                        table=table_name,
                        context=context,
                        severity=ErrorSeverity.HIGH,
                    )

                latest_timestamp = result[0]
                total_records = result[1]

                if not latest_timestamp:
                    raise DataQualityError(
                        f"Table '{table_name}' has no valid timestamps in column '{timestamp_column}'",
                        table=table_name,
                        column=timestamp_column,
                        context=context,
                        severity=ErrorSeverity.HIGH,
                    )

                # Calculate age
                now = datetime.utcnow()
                if isinstance(latest_timestamp, str):
                    # Try to parse string timestamp
                    try:
                        latest_timestamp = datetime.fromisoformat(latest_timestamp.replace("Z", "+00:00"))
                    except ValueError:
                        raise DataQualityError(
                            f"Cannot parse timestamp '{latest_timestamp}' in table '{table_name}'",
                            table=table_name,
                            column=timestamp_column,
                            context=context,
                            severity=ErrorSeverity.HIGH,
                        )

                age_hours = (now - latest_timestamp).total_seconds() / 3600

                if age_hours > max_age_hours:
                    raise DataQualityError(
                        f"Data in table '{table_name}' is stale. Latest record is {age_hours:.1f} hours old "
                        f"(max allowed: {max_age_hours} hours). Latest timestamp: {latest_timestamp}",
                        table=table_name,
                        context=context,
                        severity=ErrorSeverity.MEDIUM,
                    )

                self.logger.info(
                    f"Data freshness check passed for '{table_name}': {total_records:,} records, "
                    f"latest data is {age_hours:.1f} hours old"
                )

        except SQLAlchemyError as e:
            raise DataQualityError(
                f"Failed to check data freshness for table '{table_name}': {str(e)}",
                table=table_name,
                context=context,
                original_error=e,
                severity=ErrorSeverity.HIGH,
            )

    def validate_data_completeness(
        self, table_name: str, required_columns: List[str], sample_size: int = 1000
    ) -> Dict[str, float]:
        """
        Validate data completeness by checking for null values.

        Args:
            table_name: Name of table to check
            required_columns: Columns that should not have null values
            sample_size: Number of records to sample for validation

        Returns:
            Dictionary with null percentages for each column

        Raises:
            DataQualityError: If null percentages exceed acceptable thresholds
        """
        context = ErrorContext(
            component="DataQualityValidator",
            operation="validate_data_completeness",
            user_data={"table_name": table_name, "required_columns": required_columns, "sample_size": sample_size},
        )

        try:
            with self.engine.connect() as conn:
                # Get total record count
                total_count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
                total_count = total_count_result[0] if total_count_result else 0

                if total_count == 0:
                    raise DataQualityError(
                        f"Table '{table_name}' is empty - cannot validate completeness",
                        table=table_name,
                        context=context,
                        severity=ErrorSeverity.HIGH,
                    )

                # Sample records for analysis
                sample_query = f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT {sample_size}"
                df = pd.read_sql(sample_query, conn)

                null_percentages = {}
                issues = []

                for column in required_columns:
                    if column not in df.columns:
                        issues.append(f"Required column '{column}' not found in table")
                        continue

                    null_count = df[column].isnull().sum()
                    null_percentage = (null_count / len(df)) * 100
                    null_percentages[column] = null_percentage

                    # Flag high null percentages
                    if null_percentage > 10:  # More than 10% nulls is concerning
                        issues.append(
                            f"Column '{column}' has {null_percentage:.1f}% null values "
                            f"({null_count}/{len(df)} records)"
                        )

                if issues:
                    raise DataQualityError(
                        f"Data completeness issues in table '{table_name}': {'; '.join(issues)}",
                        table=table_name,
                        context=context,
                        severity=ErrorSeverity.MEDIUM,
                    )

                self.logger.info(
                    f"Data completeness check passed for '{table_name}': "
                    f"sampled {len(df):,} records from {total_count:,} total"
                )

                return null_percentages

        except SQLAlchemyError as e:
            raise DataQualityError(
                f"Failed to validate data completeness for table '{table_name}': {str(e)}",
                table=table_name,
                context=context,
                original_error=e,
                severity=ErrorSeverity.HIGH,
            )

    def run_comprehensive_validation(self, tables_config: Dict[str, Dict]) -> Dict[str, Any]:
        """
        Run comprehensive data quality validation across multiple tables.

        Args:
            tables_config: Configuration for each table with validation parameters

        Returns:
            Dictionary with validation results

        Example tables_config:
        {
            "youtube_videos": {
                "required_columns": ["video_id", "title", "channel_title"],
                "timestamp_column": "published_at",
                "max_age_hours": 168  # 1 week
            }
        }
        """
        _context = ErrorContext(
            component="DataQualityValidator",
            operation="run_comprehensive_validation",
            user_data={"tables": list(tables_config.keys())},
        )

        results = {
            "validation_timestamp": datetime.utcnow().isoformat(),
            "tables_validated": len(tables_config),
            "validation_results": {},
            "overall_status": "PASS",
            "issues_found": [],
        }

        for table_name, config in tables_config.items():
            table_results = {
                "schema_valid": False,
                "data_fresh": False,
                "data_complete": False,
                "null_percentages": {},
                "issues": [],
            }

            try:
                # Schema validation
                if "required_columns" in config:
                    self.validate_database_schema(table_name, config["required_columns"])
                    table_results["schema_valid"] = True

                # Freshness validation
                if "timestamp_column" in config:
                    max_age = config.get("max_age_hours", 24)
                    self.validate_data_freshness(table_name, config["timestamp_column"], max_age)
                    table_results["data_fresh"] = True

                # Completeness validation
                if "required_columns" in config:
                    null_percentages = self.validate_data_completeness(table_name, config["required_columns"])
                    table_results["null_percentages"] = null_percentages
                    table_results["data_complete"] = True

            except (DataQualityError, SchemaValidationError, ValidationError) as e:
                table_results["issues"].append(str(e))
                results["issues_found"].append(f"{table_name}: {str(e)}")
                results["overall_status"] = "FAIL"

            results["validation_results"][table_name] = table_results

        return results
