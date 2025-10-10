"""
Tests for SchemaValidator

This test suite validates the schema validation functionality including
table column validation, schema drift detection, and referential integrity checks.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.engine import Engine

from web.schema_validator import (
    ColumnInfo,
    IntegrityCheckResult,
    SchemaDriftReport,
    SchemaValidationError,
    SchemaValidator,
    TableSchema,
    ValidationError,
    ValidationResult,
    require_valid_schema,
    validate_input_types,
)


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine for testing."""
    engine = Mock(spec=Engine)
    return engine


@pytest.fixture
def test_engine():
    """Create a real SQLite engine for integration testing."""
    engine = create_engine("sqlite:///:memory:")

    # Create test tables
    with engine.connect() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE youtube_videos (
                video_id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(255),
                channel_title VARCHAR(255),
                published_at DATETIME,
                duration VARCHAR(20),
                view_count BIGINT,
                like_count INTEGER,
                comment_count INTEGER,
                fetched_at DATETIME
            )
        """
            )
        )

        conn.execute(
            text(
                """
            CREATE TABLE youtube_metrics (
                video_id VARCHAR(50),
                view_count BIGINT,
                like_count BIGINT,
                dislike_count BIGINT,
                comment_count BIGINT,
                subscriber_count BIGINT,
                metrics_date DATE,
                fetched_at DATETIME,
                PRIMARY KEY (video_id, metrics_date)
            )
        """
            )
        )

        conn.execute(
            text(
                """
            CREATE TABLE youtube_comments (
                id INTEGER PRIMARY KEY,
                video_id VARCHAR(50),
                comment_id VARCHAR(100) UNIQUE,
                comment_text TEXT,
                author_name VARCHAR(255),
                like_count INTEGER,
                published_at DATETIME,
                created_at DATETIME
            )
        """
            )
        )

        conn.commit()

    return engine


@pytest.fixture
def schema_validator(test_engine):
    """Create a SchemaValidator instance for testing."""
    return SchemaValidator(test_engine)


class TestSchemaValidatorInit:
    """Test SchemaValidator initialization."""

    def test_init_with_engine(self, test_engine):
        """Test initialization with SQLAlchemy engine."""
        validator = SchemaValidator(test_engine)
        assert validator.engine == test_engine
        assert isinstance(validator._expected_schemas, dict)
        assert "youtube_videos" in validator._expected_schemas
        assert "youtube_metrics" in validator._expected_schemas

    def test_expected_schemas_structure(self, schema_validator):
        """Test that expected schemas are properly structured."""
        expected = schema_validator._expected_schemas

        # Check youtube_videos schema
        yt_videos = expected["youtube_videos"]
        assert "required_columns" in yt_videos
        assert "primary_key" in yt_videos
        assert "video_id" in yt_videos["required_columns"]
        assert yt_videos["primary_key"] == ["video_id"]

        # Check youtube_metrics schema
        yt_metrics = expected["youtube_metrics"]
        assert "required_columns" in yt_metrics
        assert "primary_key" in yt_metrics
        assert yt_metrics["primary_key"] == ["video_id", "metrics_date"]


class TestGetTableSchema:
    """Test table schema inspection functionality."""

    def test_get_table_schema_success(self, schema_validator):
        """Test successful table schema retrieval."""
        schema = schema_validator.get_table_schema("youtube_videos")

        assert isinstance(schema, TableSchema)
        assert schema.table_name == "youtube_videos"
        assert len(schema.columns) > 0
        assert "video_id" in schema.column_names
        assert "title" in schema.column_names
        assert schema.primary_keys == ["video_id"]

    def test_get_table_schema_nonexistent_table(self, schema_validator):
        """Test schema retrieval for non - existent table."""
        with pytest.raises(SchemaValidationError) as exc_info:
            schema_validator.get_table_schema("nonexistent_table")

        assert "does not exist" in str(exc_info.value)

    def test_get_table_schema_caching(self, schema_validator):
        """Test that schema information is cached."""
        # First call
        schema1 = schema_validator.get_table_schema("youtube_videos")

        # Second call should use cache
        schema2 = schema_validator.get_table_schema("youtube_videos", use_cache=True)

        assert schema1 is schema2  # Same object reference

        # Third call without cache should create new object
        schema3 = schema_validator.get_table_schema("youtube_videos", use_cache=False)

        assert schema1 is not schema3  # Different object reference
        assert schema1.table_name == schema3.table_name  # Same content

    def test_column_info_properties(self, schema_validator):
        """Test ColumnInfo properties are correctly populated."""
        schema = schema_validator.get_table_schema("youtube_videos")

        # Find video_id column
        video_id_col = next(col for col in schema.columns if col.name == "video_id")

        assert video_id_col.name == "video_id"
        assert video_id_col.primary_key is True
        # Note: SQLite may handle nullable differently, so we just check it's a boolean
        assert isinstance(video_id_col.nullable, bool)
        assert isinstance(video_id_col.type_name, str)


class TestValidateTableColumns:
    """Test table column validation functionality."""

    def test_validate_table_columns_success(self, schema_validator):
        """Test successful column validation."""
        expected_columns = ["video_id", "title", "channel_title"]
        result = schema_validator.validate_table_columns("youtube_videos", expected_columns)

        assert isinstance(result, ValidationResult)
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert isinstance(result.timestamp, datetime)

    def test_validate_table_columns_missing_columns(self, schema_validator):
        """Test validation with missing columns."""
        expected_columns = ["video_id", "title", "nonexistent_column"]
        result = schema_validator.validate_table_columns("youtube_videos", expected_columns)

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "MISSING_COLUMN"
        assert "nonexistent_column" in result.errors[0].message

    def test_validate_table_columns_unexpected_columns(self, schema_validator):
        """Test validation with unexpected columns (warnings)."""
        expected_columns = ["video_id", "title"]  # Missing some actual columns
        result = schema_validator.validate_table_columns("youtube_videos", expected_columns)

        assert result.is_valid is True  # Unexpected columns are warnings, not errors
        assert len(result.warnings) > 0

        # Check that warnings are for unexpected columns
        warning_types = [w.error_type for w in result.warnings]
        assert "UNEXPECTED_COLUMN" in warning_types

    def test_validate_table_columns_nonexistent_table(self, schema_validator):
        """Test validation for non - existent table."""
        result = schema_validator.validate_table_columns("nonexistent_table", ["col1"])

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "TABLE_ACCESS_ERROR"


class TestSchemaDriftDetection:
    """Test schema drift detection functionality."""

    def test_detect_schema_drift_no_drift(self, schema_validator):
        """Test drift detection when no drift exists."""
        # Mock the expected schemas to match actual tables (including youtube_comments)
        with patch.object(
            schema_validator,
            "_expected_schemas",
            {
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
                    ]
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
                    ]
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
                    ]
                },
            },
        ):
            report = schema_validator.detect_schema_drift()

        assert isinstance(report, SchemaDriftReport)
        assert report.has_drift is False
        assert len(report.tables_added) == 0
        assert len(report.tables_removed) == 0
        assert len(report.columns_added) == 0
        assert len(report.columns_removed) == 0

    def test_detect_schema_drift_with_drift(self, schema_validator):
        """Test drift detection when drift exists."""
        # Mock expected schemas with missing table and columns
        with patch.object(
            schema_validator,
            "_expected_schemas",
            {
                "youtube_videos": {"required_columns": ["video_id", "title", "missing_column"]},
                "missing_table": {"required_columns": ["id", "name"]},
            },
        ):
            report = schema_validator.detect_schema_drift()

        assert report.has_drift is True
        assert "missing_table" in report.tables_removed
        assert "youtube_videos" in report.columns_removed
        assert "missing_column" in report.columns_removed["youtube_videos"]

    def test_detect_schema_drift_timestamp(self, schema_validator):
        """Test that drift report includes timestamp."""
        report = schema_validator.detect_schema_drift()

        assert isinstance(report.timestamp, datetime)
        assert report.timestamp.tzinfo == timezone.utc


class TestReferentialIntegrityValidation:
    """Test referential integrity validation functionality."""

    def test_validate_referential_integrity_no_data(self, schema_validator):
        """Test integrity validation with no data (should pass)."""
        results = schema_validator.validate_referential_integrity("youtube_comments")

        # Should return empty list since youtube_comments is not in expected schemas with FK
        assert isinstance(results, list)

    def test_check_foreign_key_integrity_valid(self, schema_validator, test_engine):
        """Test foreign key integrity check with valid data."""
        # Insert test data
        with test_engine.connect() as conn:
            conn.execute(text("INSERT INTO youtube_videos (video_id, title) VALUES ('test_video', 'Test Video')"))
            conn.execute(
                text("INSERT INTO youtube_comments (video_id, comment_text) VALUES ('test_video', 'Test Comment')")
            )
            conn.commit()

        # Test the private method directly
        result = schema_validator._check_foreign_key_integrity(
            "youtube_comments", "video_id", "youtube_videos.video_id"
        )

        assert isinstance(result, IntegrityCheckResult)
        assert result.table_name == "youtube_comments"
        assert result.foreign_key == "video_id"
        assert result.referenced_table == "youtube_videos"
        assert result.is_valid is True
        assert result.orphaned_count == 0

    def test_check_foreign_key_integrity_orphaned(self, schema_validator, test_engine):
        """Test foreign key integrity check with orphaned records."""
        # Insert orphaned comment (no corresponding video)
        with test_engine.connect() as conn:
            conn.execute(
                text(
                    "INSERT INTO youtube_comments (video_id, comment_text) VALUES ('orphaned_video', 'Orphaned Comment')"
                )
            )
            conn.commit()

        result = schema_validator._check_foreign_key_integrity(
            "youtube_comments", "video_id", "youtube_videos.video_id"
        )

        assert result.is_valid is False
        assert result.orphaned_count == 1
        assert "orphaned_video" in result.sample_orphaned_ids


class TestETLStartupValidation:
    """Test ETL startup validation functionality."""

    def test_validate_etl_startup_success(self, schema_validator):
        """Test successful ETL startup validation."""
        # Mock successful validations
        with patch.object(schema_validator, "validate_table_columns") as mock_validate:
            mock_validate.return_value = ValidationResult(
                is_valid=True, errors=[], warnings=[], timestamp=datetime.now(timezone.utc)
            )

            with patch.object(schema_validator, "detect_schema_drift") as mock_drift:
                mock_drift.return_value = SchemaDriftReport(
                    tables_added=[],
                    tables_removed=[],
                    columns_added={},
                    columns_removed={},
                    columns_modified={},
                    timestamp=datetime.now(timezone.utc),
                )

                with patch.object(schema_validator, "validate_referential_integrity") as mock_integrity:
                    mock_integrity.return_value = []

                    result = schema_validator.validate_etl_startup()

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_etl_startup_with_errors(self, schema_validator):
        """Test ETL startup validation with errors."""
        # Mock validation with errors
        with patch.object(schema_validator, "validate_table_columns") as mock_validate:
            mock_validate.return_value = ValidationResult(
                is_valid=False,
                errors=[ValidationError("test_table", "MISSING_COLUMN", "Test error")],
                warnings=[],
                timestamp=datetime.now(timezone.utc),
            )

            with patch.object(schema_validator, "detect_schema_drift") as mock_drift:
                mock_drift.return_value = SchemaDriftReport(
                    tables_added=[],
                    tables_removed=[],
                    columns_added={},
                    columns_removed={},
                    columns_modified={},
                    timestamp=datetime.now(timezone.utc),
                )

                with patch.object(schema_validator, "validate_referential_integrity") as mock_integrity:
                    mock_integrity.return_value = []

                    result = schema_validator.validate_etl_startup()

        assert result.is_valid is False
        assert len(result.errors) > 0


class TestValidationDecorators:
    """Test validation decorators."""

    def test_validate_input_types_success(self):
        """Test input type validation decorator with correct types."""

        @validate_input_types(video_id=str, view_count=int)
        def test_function(video_id, view_count):
            return f"{video_id}:{view_count}"

        result = test_function("test_video", 1000)
        assert result == "test_video:1000"

    def test_validate_input_types_failure(self):
        """Test input type validation decorator with incorrect types."""

        @validate_input_types(video_id=str, view_count=int)
        def test_function(video_id, view_count):
            return f"{video_id}:{view_count}"

        with pytest.raises(TypeError) as exc_info:
            test_function("test_video", "not_an_int")

        assert "view_count" in str(exc_info.value)
        assert "must be of type int" in str(exc_info.value)

    def test_validate_input_types_none_values(self):
        """Test input type validation decorator with None values (should pass)."""

        @validate_input_types(video_id=str, view_count=int)
        def test_function(video_id, view_count=None):
            return f"{video_id}:{view_count}"

        result = test_function("test_video", None)
        assert result == "test_video:None"

    def test_require_valid_schema_success(self, test_engine):
        """Test schema validation decorator with valid schema."""

        @require_valid_schema("youtube_videos", ["video_id", "title"])
        def test_function(engine=None):
            return "success"

        result = test_function(engine=test_engine)
        assert result == "success"

    def test_require_valid_schema_failure(self, test_engine):
        """Test schema validation decorator with invalid schema."""

        @require_valid_schema("youtube_videos", ["video_id", "nonexistent_column"])
        def test_function(engine=None):
            return "success"

        with pytest.raises(SchemaValidationError) as exc_info:
            test_function(engine=test_engine)

        assert "Schema validation failed" in str(exc_info.value)
        assert "nonexistent_column" in str(exc_info.value)

    def test_require_valid_schema_no_engine(self):
        """Test schema validation decorator without engine (should pass)."""

        @require_valid_schema("youtube_videos", ["video_id", "title"])
        def test_function(data):
            return "success"

        result = test_function("test_data")
        assert result == "success"


class TestDataClasses:
    """Test data class functionality."""

    def test_column_info_creation(self):
        """Test ColumnInfo data class creation."""
        col = ColumnInfo(
            name="test_column", type_name="VARCHAR(50)", nullable=True, default="default_value", primary_key=False
        )

        assert col.name == "test_column"
        assert col.type_name == "VARCHAR(50)"
        assert col.nullable is True
        assert col.default == "default_value"
        assert col.primary_key is False

    def test_table_schema_column_names_property(self):
        """Test TableSchema column_names property."""
        columns = [
            ColumnInfo("col1", "VARCHAR", False),
            ColumnInfo("col2", "INTEGER", True),
            ColumnInfo("col3", "TEXT", True),
        ]

        schema = TableSchema(
            table_name="test_table", columns=columns, primary_keys=["col1"], foreign_keys=[], indexes=[]
        )

        assert schema.column_names == {"col1", "col2", "col3"}

    def test_validation_result_properties(self):
        """Test ValidationResult property methods."""
        errors = [ValidationError("table1", "ERROR", "Test error")]
        warnings = [ValidationError("table2", "WARNING", "Test warning", severity="WARNING")]

        result = ValidationResult(
            is_valid=False, errors=errors, warnings=warnings, timestamp=datetime.now(timezone.utc)
        )

        assert result.has_errors is True
        assert result.has_warnings is True
        assert result.is_valid is False

    def test_schema_drift_report_has_drift_property(self):
        """Test SchemaDriftReport has_drift property."""
        # No drift
        report_no_drift = SchemaDriftReport(
            tables_added=[],
            tables_removed=[],
            columns_added={},
            columns_removed={},
            columns_modified={},
            timestamp=datetime.now(timezone.utc),
        )
        assert report_no_drift.has_drift is False

        # With drift
        report_with_drift = SchemaDriftReport(
            tables_added=["new_table"],
            tables_removed=[],
            columns_added={},
            columns_removed={},
            columns_modified={},
            timestamp=datetime.now(timezone.utc),
        )
        assert report_with_drift.has_drift is True

    def test_integrity_check_result_creation(self):
        """Test IntegrityCheckResult data class creation."""
        result = IntegrityCheckResult(
            table_name="test_table",
            foreign_key="fk_column",
            referenced_table="ref_table",
            orphaned_count=5,
            sample_orphaned_ids=["id1", "id2", "id3"],
            is_valid=False,
        )

        assert result.table_name == "test_table"
        assert result.foreign_key == "fk_column"
        assert result.referenced_table == "ref_table"
        assert result.orphaned_count == 5
        assert result.sample_orphaned_ids == ["id1", "id2", "id3"]
        assert result.is_valid is False


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_schema_validation_error_with_result(self):
        """Test SchemaValidationError with validation result."""
        validation_result = ValidationResult(
            is_valid=False,
            errors=[ValidationError("table", "ERROR", "Test error")],
            warnings=[],
            timestamp=datetime.now(timezone.utc),
        )

        error = SchemaValidationError("Test message", validation_result)

        assert str(error) == "Test message"
        assert error.validation_result == validation_result

    def test_schema_validation_error_without_result(self):
        """Test SchemaValidationError without validation result."""
        error = SchemaValidationError("Test message")

        assert str(error) == "Test message"
        assert error.validation_result is None

    def test_foreign_key_integrity_check_error_handling(self, schema_validator):
        """Test foreign key integrity check with database error."""
        # Mock engine to raise exception
        with patch.object(schema_validator.engine, "connect") as mock_connect:
            mock_connect.side_effect = Exception("Database connection failed")

            result = schema_validator._check_foreign_key_integrity("test_table", "fk_column", "ref_table.ref_column")

            assert result.is_valid is False
            assert result.orphaned_count == -1  # Indicates error
            assert result.sample_orphaned_ids == []


class TestIntegrationScenarios:
    """Test integration scenarios with real database operations."""

    def test_full_validation_workflow(self, schema_validator, test_engine):
        """Test complete validation workflow."""
        # 1. Validate table columns
        result = schema_validator.validate_table_columns("youtube_videos", ["video_id", "title", "channel_title"])
        assert result.is_valid is True

        # 2. Check schema drift
        drift_report = schema_validator.detect_schema_drift()
        assert isinstance(drift_report, SchemaDriftReport)

        # 3. Validate referential integrity
        integrity_results = schema_validator.validate_referential_integrity()
        assert isinstance(integrity_results, list)

        # 4. ETL startup validation
        startup_result = schema_validator.validate_etl_startup()
        assert isinstance(startup_result, ValidationResult)

    def test_caching_behavior(self, schema_validator):
        """Test that caching works correctly across multiple calls."""
        # Clear cache
        schema_validator._cached_schemas.clear()

        # First call should populate cache
        schema1 = schema_validator.get_table_schema("youtube_videos")
        assert "youtube_videos" in schema_validator._cached_schemas

        # Second call should use cache
        schema2 = schema_validator.get_table_schema("youtube_videos")
        assert schema1 is schema2

        # Force refresh should create new object
        schema3 = schema_validator.get_table_schema("youtube_videos", use_cache=False)
        assert schema1 is not schema3
        assert schema1.table_name == schema3.table_name
