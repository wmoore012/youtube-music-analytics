#!/usr / bin / env python3
"""
Database Schema Validator for YouTube Analytics Platform

This module provides runtime schema validation to prevent schema drift and ensure
data integrity across the YouTube analytics platform.

Key Features:
- Runtime schema validation using SQLAlchemy reflection
- Schema drift detection with detailed reporting
- Data validation decorators for input validation
- Referential integrity checks for foreign key relationships
- Clear error messages for schema mismatches

Usage:
    from web.schema_validator import SchemaValidator

    validator = SchemaValidator(engine)

    # Validate table columns
    result = validator.validate_table_columns('youtube_metrics',
                                            ['video_id', 'view_count', 'metrics_date'])

    # Detect schema drift
    drift_report = validator.detect_schema_drift()

    # Validate referential integrity
    integrity_result = validator.validate_referential_integrity()
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set

from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

# Constants
DEFAULT_TIMEOUT = 30
MAX_SAMPLE_SIZE = 1000

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Information about a database column."""

    name: str
    type_name: str
    nullable: bool
    default: Optional[str] = None
    primary_key: bool = False


@dataclass
class TableSchema:
    """Complete schema information for a database table."""

    table_name: str
    columns: List[ColumnInfo]
    primary_keys: List[str]
    foreign_keys: List[Dict[str, str]]
    indexes: List[str]

    @property
    def column_names(self) -> Set[str]:
        """Get set of column names."""
        return {col.name for col in self.columns}


@dataclass
class ValidationError:
    """Schema validation error details."""

    table_name: str
    error_type: str
    message: str
    expected: Optional[str] = None
    actual: Optional[str] = None
    severity: str = "ERROR"


@dataclass
class ValidationResult:
    """Result of schema validation operation."""

    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationError]
    timestamp: datetime

    @property
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0


@dataclass
class SchemaDriftReport:
    """Report of schema drift detection."""

    tables_added: List[str]
    tables_removed: List[str]
    columns_added: Dict[str, List[str]]
    columns_removed: Dict[str, List[str]]
    columns_modified: Dict[str, List[Dict[str, Any]]]
    timestamp: datetime

    @property
    def has_drift(self) -> bool:
        """Check if any schema drift was detected."""
        return (
            len(self.tables_added) > 0
            or len(self.tables_removed) > 0
            or len(self.columns_added) > 0
            or len(self.columns_removed) > 0
            or len(self.columns_modified) > 0
        )


@dataclass
class IntegrityCheckResult:
    """Result of referential integrity check."""

    table_name: str
    foreign_key: str
    referenced_table: str
    orphaned_count: int
    sample_orphaned_ids: List[str]
    is_valid: bool


class SchemaValidationError(Exception):
    """Exception raised for schema validation errors."""

    def __init__(self, message: str, validation_result: Optional[ValidationResult] = None):
        super().__init__(message)
        self.validation_result = validation_result


class SchemaValidator:
    """
    Database schema validator with drift detection and integrity checking.

    This class provides comprehensive schema validation to ensure database
    operations are performed against the correct schema structure.
    """

    def __init__(self, engine: Engine):
        """
        Initialize schema validator.

        Args:
            engine: SQLAlchemy engine for database operations
        """
        self.engine = engine
        self._cached_schemas: Dict[str, TableSchema] = {}
        self._expected_schemas = self._define_expected_schemas()

        logger.info("SchemaValidator initialized")

    def _define_expected_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Define expected schemas for core tables."""
        return {
            "youtube_videos": {
                "required_columns": [
                    "video_id",
                    "title",
                    "channel_title",
                    "published_at",
                    "duration",
                    "view_count",
                    "like_count",
                    "comment_count",
                    "fetched_at",
                ],
                "primary_key": ["video_id"],
                "nullable_columns": ["isrc", "title", "duration"],
            },
            "youtube_metrics": {
                "required_columns": [
                    "video_id",
                    "view_count",
                    "like_count",
                    "dislike_count",
                    "comment_count",
                    "subscriber_count",
                    "metrics_date",
                    "fetched_at",
                ],
                "primary_key": ["video_id", "metrics_date"],
                "nullable_columns": ["view_count", "like_count", "dislike_count", "comment_count", "subscriber_count"],
            },
            "youtube_comments": {
                "required_columns": [
                    "id",
                    "video_id",
                    "comment_id",
                    "comment_text",
                    "author_name",
                    "like_count",
                    "published_at",
                    "created_at",
                ],
                "primary_key": ["id"],
                "unique_keys": ["comment_id"],
                "foreign_keys": [{"column": "video_id", "references": "youtube_videos.video_id"}],
            },
            "isrc_recordings": {
                "required_columns": ["isrc", "title", "artist_primary", "created_at", "updated_at"],
                "primary_key": ["isrc"],
                "nullable_columns": ["release_date", "metadata"],
            },
            "video_recording_link": {
                "required_columns": ["video_id", "isrc", "match_method", "confidence", "matched_at"],
                "foreign_keys": [
                    {"column": "video_id", "references": "youtube_videos.video_id"},
                    {"column": "isrc", "references": "isrc_recordings.isrc"},
                ],
            },
        }

    def get_table_schema(self, table_name: str, use_cache: bool = True) -> TableSchema:
        """
        Get complete schema information for a table.

        Args:
            table_name: Name of the table to inspect
            use_cache: Whether to use cached schema information

        Returns:
            TableSchema object with complete table information

        Raises:
            SchemaValidationError: If table doesn't exist or can't be inspected
        """
        if use_cache and table_name in self._cached_schemas:
            return self._cached_schemas[table_name]

        try:
            inspector = inspect(self.engine)

            # Check if table exists
            if not inspector.has_table(table_name):
                raise SchemaValidationError(f"Table '{table_name}' does not exist")

            # Get column information
            columns = []
            column_info = inspector.get_columns(table_name)
            pk_constraint = inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get("constrained_columns", [])

            for col in column_info:
                columns.append(
                    ColumnInfo(
                        name=col["name"],
                        type_name=str(col["type"]),
                        nullable=col["nullable"],
                        default=col.get("default"),
                        primary_key=col["name"] in primary_keys,
                    )
                )

            # Get foreign key information
            foreign_keys = []
            fk_constraints = inspector.get_foreign_keys(table_name)
            for fk in fk_constraints:
                foreign_keys.append(
                    {
                        "constrained_columns": fk["constrained_columns"],
                        "referred_table": fk["referred_table"],
                        "referred_columns": fk["referred_columns"],
                    }
                )

            # Get index information
            indexes = []
            index_info = inspector.get_indexes(table_name)
            for idx in index_info:
                indexes.append(idx["name"])

            schema = TableSchema(
                table_name=table_name,
                columns=columns,
                primary_keys=primary_keys,
                foreign_keys=foreign_keys,
                indexes=indexes,
            )

            # Cache the schema
            self._cached_schemas[table_name] = schema

            return schema

        except SQLAlchemyError as e:
            raise SchemaValidationError(f"Failed to inspect table '{table_name}': {str(e)}")

    def validate_table_columns(self, table_name: str, expected_columns: List[str]) -> ValidationResult:
        """
        Validate that a table has the expected columns.

        Args:
            table_name: Name of the table to validate
            expected_columns: List of expected column names

        Returns:
            ValidationResult with validation details
        """
        errors = []
        warnings = []

        try:
            schema = self.get_table_schema(table_name)
            actual_columns = schema.column_names

            # Check for missing columns
            missing_columns = set(expected_columns) - actual_columns
            for col in missing_columns:
                errors.append(
                    ValidationError(
                        table_name=table_name,
                        error_type="MISSING_COLUMN",
                        message=f"Required column '{col}' is missing",
                        expected=col,
                        actual=None,
                    )
                )

            # Check for unexpected columns (warnings only)
            unexpected_columns = actual_columns-set(expected_columns)
            for col in unexpected_columns:
                warnings.append(
                    ValidationError(
                        table_name=table_name,
                        error_type="UNEXPECTED_COLUMN",
                        message=f"Unexpected column '{col}' found",
                        expected=None,
                        actual=col,
                        severity="WARNING",
                    )
                )

            logger.info(f"Column validation for {table_name}: " f"{len(errors)} errors, {len(warnings)} warnings")

        except SchemaValidationError as e:
            errors.append(ValidationError(table_name=table_name, error_type="TABLE_ACCESS_ERROR", message=str(e)))

        return ValidationResult(
            is_valid=len(errors) == 0, errors=errors, warnings=warnings, timestamp=datetime.now(timezone.utc)
        )

    def detect_schema_drift(self) -> SchemaDriftReport:
        """
        Detect schema drift by comparing current schema with expected schema.

        Returns:
            SchemaDriftReport with detailed drift information
        """
        logger.info("Starting schema drift detection")

        tables_added = []
        tables_removed = []
        columns_added = {}
        columns_removed = {}
        columns_modified = {}

        try:
            inspector = inspect(self.engine)
            current_tables = set(inspector.get_table_names())
            expected_tables = set(self._expected_schemas.keys())

            # Check for added / removed tables
            tables_added = list(current_tables-expected_tables)
            tables_removed = list(expected_tables-current_tables)

            # Check column changes for existing tables
            for table_name in expected_tables.intersection(current_tables):
                expected_schema = self._expected_schemas[table_name]
                current_schema = self.get_table_schema(table_name)

                expected_columns = set(expected_schema["required_columns"])
                current_columns = current_schema.column_names

                # Check for added / removed columns
                added_cols = list(current_columns-expected_columns)
                removed_cols = list(expected_columns-current_columns)

                if added_cols:
                    columns_added[table_name] = added_cols
                if removed_cols:
                    columns_removed[table_name] = removed_cols

                # Check for modified columns (type changes, etc.)
                modified_cols = []
                for col in current_schema.columns:
                    if col.name in expected_columns:
                        # Check for nullable changes if specified
                        if "nullable_columns" in expected_schema:
                            expected_nullable = col.name in expected_schema["nullable_columns"]
                            if col.nullable != expected_nullable:
                                modified_cols.append(
                                    {
                                        "column": col.name,
                                        "change": "nullable",
                                        "expected": expected_nullable,
                                        "actual": col.nullable,
                                    }
                                )

                if modified_cols:
                    columns_modified[table_name] = modified_cols

            logger.info(
                f"Schema drift detection complete: "
                f"{len(tables_added)} tables added, "
                f"{len(tables_removed)} tables removed, "
                f"{len(columns_added)} tables with added columns, "
                f"{len(columns_removed)} tables with removed columns"
            )

        except Exception as e:
            logger.error(f"Schema drift detection failed: {e}")

        return SchemaDriftReport(
            tables_added=tables_added,
            tables_removed=tables_removed,
            columns_added=columns_added,
            columns_removed=columns_removed,
            columns_modified=columns_modified,
            timestamp=datetime.now(timezone.utc),
        )

    def validate_referential_integrity(self, table_name: Optional[str] = None) -> List[IntegrityCheckResult]:
        """
        Validate referential integrity for foreign key relationships.

        Args:
            table_name: Specific table to check (if None, checks all tables)

        Returns:
            List of IntegrityCheckResult objects
        """
        logger.info(f"Starting referential integrity check for {table_name or 'all tables'}")

        results = []

        try:
            tables_to_check = [table_name] if table_name else self._expected_schemas.keys()

            for table in tables_to_check:
                if table not in self._expected_schemas:
                    continue

                expected_schema = self._expected_schemas[table]
                foreign_keys = expected_schema.get("foreign_keys", [])

                for fk in foreign_keys:
                    result = self._check_foreign_key_integrity(table, fk["column"], fk["references"])
                    results.append(result)

            valid_count = sum(1 for r in results if r.is_valid)
            logger.info(f"Referential integrity check complete: " f"{valid_count}/{len(results)} relationships valid")

        except Exception as e:
            logger.error(f"Referential integrity check failed: {e}")

        return results

    def _check_foreign_key_integrity(
        self, table_name: str, foreign_key_column: str, references: str
    ) -> IntegrityCheckResult:
        """
        Check integrity of a specific foreign key relationship.

        Args:
            table_name: Name of the table with foreign key
            foreign_key_column: Name of the foreign key column
            references: Referenced table.column (e.g., 'youtube_videos.video_id')

        Returns:
            IntegrityCheckResult with check details
        """
        referenced_table, referenced_column = references.split(".")

        try:
            with self.engine.connect() as conn:
                # Find orphaned records
                query = text(
                    f"""
                    SELECT DISTINCT t.{foreign_key_column}
                    FROM {table_name} t
                    LEFT JOIN {referenced_table} r ON t.{foreign_key_column} = r.{referenced_column}
                    WHERE r.{referenced_column} IS NULL
                    AND t.{foreign_key_column} IS NOT NULL
                    LIMIT :limit
                """
                )

                result = conn.execute(query, {"limit": MAX_SAMPLE_SIZE})
                orphaned_ids = [str(row[0]) for row in result]

                # Get total count of orphaned records
                count_query = text(
                    f"""
                    SELECT COUNT(DISTINCT t.{foreign_key_column})
                    FROM {table_name} t
                    LEFT JOIN {referenced_table} r ON t.{foreign_key_column} = r.{referenced_column}
                    WHERE r.{referenced_column} IS NULL
                    AND t.{foreign_key_column} IS NOT NULL
                """
                )

                orphaned_count = conn.execute(count_query).scalar()

                return IntegrityCheckResult(
                    table_name=table_name,
                    foreign_key=foreign_key_column,
                    referenced_table=referenced_table,
                    orphaned_count=orphaned_count,
                    sample_orphaned_ids=orphaned_ids[:10],  # Limit sample size
                    is_valid=orphaned_count == 0,
                )

        except Exception as e:
            logger.error(f"Foreign key integrity check failed for {table_name}.{foreign_key_column}: {e}")
            return IntegrityCheckResult(
                table_name=table_name,
                foreign_key=foreign_key_column,
                referenced_table=referenced_table,
                orphaned_count=-1,  # Indicates error
                sample_orphaned_ids=[],
                is_valid=False,
            )

    def validate_etl_startup(self) -> ValidationResult:
        """
        Perform comprehensive validation suitable for ETL startup.

        Returns:
            ValidationResult with overall validation status
        """
        logger.info("Starting ETL startup validation")

        all_errors = []
        all_warnings = []

        # Validate core tables
        core_tables = [
            "youtube_videos",
            "youtube_metrics",
            "youtube_comments",
            "isrc_recordings",
            "video_recording_link",
        ]

        for table_name in core_tables:
            if table_name in self._expected_schemas:
                expected_columns = self._expected_schemas[table_name]["required_columns"]
                result = self.validate_table_columns(table_name, expected_columns)
                all_errors.extend(result.errors)
                all_warnings.extend(result.warnings)

        # Check for schema drift
        drift_report = self.detect_schema_drift()
        if drift_report.has_drift:
            all_warnings.append(
                ValidationError(
                    table_name="SYSTEM",
                    error_type="SCHEMA_DRIFT",
                    message=f"Schema drift detected: {len(drift_report.tables_added)} tables added, "
                    f"{len(drift_report.tables_removed)} tables removed",
                    severity="WARNING",
                )
            )

        # Check referential integrity for critical relationships
        integrity_results = self.validate_referential_integrity()
        for result in integrity_results:
            if not result.is_valid and result.orphaned_count > 0:
                all_errors.append(
                    ValidationError(
                        table_name=result.table_name,
                        error_type="REFERENTIAL_INTEGRITY",
                        message=f"Found {result.orphaned_count} orphaned records in "
                        f"{result.foreign_key} referencing {result.referenced_table}",
                    )
                )

        validation_result = ValidationResult(
            is_valid=len(all_errors) == 0,
            errors=all_errors,
            warnings=all_warnings,
            timestamp=datetime.now(timezone.utc),
        )

        logger.info(
            f"ETL startup validation complete: "
            f"{'PASSED' if validation_result.is_valid else 'FAILED'} "
            f"({len(all_errors)} errors, {len(all_warnings)} warnings)"
        )

        return validation_result


def validate_input_types(**type_checks):
    """
    Decorator to validate input types before database operations.

    Args:
        **type_checks: Keyword arguments mapping parameter names to expected types

    Usage:
        @validate_input_types(video_id=str, view_count=int)
        def update_metrics(video_id, view_count):
            pass
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature
            import inspect

            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Validate types
            for param_name, expected_type in type_checks.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if value is not None and not isinstance(value, expected_type):
                        raise TypeError(
                            f"Parameter '{param_name}' must be of type {expected_type.__name__}, "
                            f"got {type(value).__name__}"
                        )

            return func(*args, **kwargs)

        return wrapper

    return decorator


def require_valid_schema(table_name: str, required_columns: List[str]):
    """
    Decorator to ensure schema is valid before executing database operations.

    Args:
        table_name: Name of the table to validate
        required_columns: List of required column names

    Usage:
        @require_valid_schema('youtube_metrics', ['video_id', 'view_count'])
        def update_metrics(engine, video_id, view_count):
            pass
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get engine from first argument or kwargs
            engine = None
            if args and hasattr(args[0], "execute"):  # Likely an engine or connection
                engine = args[0]
            elif "engine" in kwargs:
                engine = kwargs["engine"]

            if engine:
                validator = SchemaValidator(engine)
                result = validator.validate_table_columns(table_name, required_columns)

                if not result.is_valid:
                    error_messages = [error.message for error in result.errors]
                    raise SchemaValidationError(
                        f"Schema validation failed for {table_name}: {'; '.join(error_messages)}",
                        validation_result=result,
                    )

            return func(*args, **kwargs)

        return wrapper

    return decorator
