#!/usr/bin/env python3
"""
YouTube Metrics Schema Validation Script

This script validates that the YouTube ETL code is correctly aligned with the
actual database schema for the youtube_metrics table.

Usage:
    python validate_youtube_metrics_schema.py
"""

from datetime import date, datetime
import os
import sys

from sqlalchemy import text

from web.etl_helpers import get_engine
from web.youtube_channel_etl import YouTubeChannelETL


def validate_database_schema():
    """Validate that the database schema matches expectations."""
    print("🔍 Validating database schema...")

    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Check if youtube_metrics table exists
            result = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics'"
                )
            ).scalar()

            if result != 1:
                print("❌ youtube_metrics table does not exist")
                return False

            print("✅ youtube_metrics table exists")

            # Check column structure
            columns_result = conn.execute(
                text(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics' "
                    "ORDER BY ordinal_position"
                )
            ).fetchall()

            expected_columns = {
                "video_id": ("varchar", "NO"),
                "view_count": ("bigint", "YES"),
                "like_count": ("bigint", "YES"),
                "dislike_count": ("bigint", "YES"),
                "comment_count": ("bigint", "YES"),
                "subscriber_count": ("bigint", "YES"),
                "metrics_date": ("date", "NO"),
                "fetched_at": ("datetime", "NO"),
            }

            actual_columns = {row[0]: (row[1], row[2]) for row in columns_result}

            print("\n📋 Column validation:")
            all_columns_valid = True

            for col_name, (expected_type, expected_nullable) in expected_columns.items():
                if col_name not in actual_columns:
                    print(f"❌ Missing column: {col_name}")
                    all_columns_valid = False
                    continue

                actual_type, actual_nullable = actual_columns[col_name]

                if expected_type not in actual_type.lower():
                    print(f"❌ {col_name}: expected {expected_type}, got {actual_type}")
                    all_columns_valid = False
                elif actual_nullable != expected_nullable:
                    print(f"❌ {col_name}: expected nullable={expected_nullable}, got {actual_nullable}")
                    all_columns_valid = False
                else:
                    print(f"✅ {col_name}: {actual_type} (nullable: {actual_nullable})")

            # Check for unexpected columns
            for col_name in actual_columns:
                if col_name not in expected_columns:
                    print(f"⚠️  Unexpected column: {col_name}")

            # Check primary key
            pk_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.key_column_usage "
                    "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics' "
                    "AND constraint_name = 'PRIMARY' "
                    "ORDER BY ordinal_position"
                )
            ).fetchall()

            pk_columns = [row[0] for row in pk_result]
            expected_pk = ["video_id", "metrics_date"]

            print(f"\n🔑 Primary key validation:")
            if pk_columns == expected_pk:
                print(f"✅ Primary key: {pk_columns}")
            else:
                print(f"❌ Primary key: expected {expected_pk}, got {pk_columns}")
                all_columns_valid = False

            return all_columns_valid

    except Exception as e:
        print(f"❌ Database validation failed: {e}")
        return False


def validate_etl_code():
    """Validate that the ETL code uses correct column names."""
    print("\n🔍 Validating ETL code...")

    import inspect

    # Get the source code of the _upsert_daily_metrics method
    etl_method_source = inspect.getsource(YouTubeChannelETL._upsert_daily_metrics)

    # Check for correct columns
    expected_columns = [
        "video_id",
        "view_count",
        "like_count",
        "dislike_count",
        "comment_count",
        "subscriber_count",
        "metrics_date",
        "fetched_at",
    ]

    print("\n📋 ETL code column usage:")
    all_columns_found = True

    for col in expected_columns:
        if col in etl_method_source:
            print(f"✅ {col}: found in ETL code")
        else:
            print(f"❌ {col}: missing from ETL code")
            all_columns_found = False

    # Check for non-existent columns
    non_existent_columns = ["isrc", "favorite_count"]

    print("\n🚫 Non-existent column check:")
    no_bad_columns = True

    for col in non_existent_columns:
        if col in etl_method_source.lower():
            print(f"❌ {col}: found in ETL code (should not be present)")
            no_bad_columns = False
        else:
            print(f"✅ {col}: not found in ETL code (correct)")

    # Check for proper SQL structure
    print("\n🔧 SQL structure validation:")
    sql_checks = [
        ("INSERT INTO youtube_metrics", "INSERT statement"),
        ("ON DUPLICATE KEY UPDATE", "Upsert logic"),
        ("CURDATE()", "Current date for metrics_date"),
        ("NOW()", "Current timestamp for fetched_at"),
        ("VALUES(view_count) > view_count", "Conditional update logic"),
    ]

    sql_structure_valid = True
    for pattern, description in sql_checks:
        if pattern in etl_method_source:
            print(f"✅ {description}: found")
        else:
            print(f"❌ {description}: missing")
            sql_structure_valid = False

    return all_columns_found and no_bad_columns and sql_structure_valid


def validate_parameter_usage():
    """Validate that the ETL method uses parameters correctly."""
    print("\n🔍 Validating parameter usage...")

    # Create a test ETL instance
    etl = YouTubeChannelETL(
        api_key="test_key",
        db_host="localhost",
        db_port=3306,
        db_user="test_user",
        db_pass="test_pass",
        db_name="test_db",
    )

    # Mock connection to capture SQL
    class MockCursor:
        def __init__(self):
            self.sql = None
            self.params = None

        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class MockConnection:
        def __init__(self):
            self.cursor_instance = MockCursor()

        def cursor(self):
            return self.cursor_instance

    mock_conn = MockConnection()

    # Test the method
    test_video_id = "test_video_123"
    test_view_count = 1000
    test_like_count = 50
    test_comment_count = 25

    etl._upsert_daily_metrics(mock_conn, test_video_id, test_view_count, test_like_count, test_comment_count)

    # Validate parameters
    expected_params = (test_video_id, test_view_count, test_like_count, 0, test_comment_count)

    print("\n📋 Parameter validation:")
    if mock_conn.cursor_instance.params == expected_params:
        print(f"✅ Parameters: {mock_conn.cursor_instance.params}")
        return True
    else:
        print(f"❌ Parameters: expected {expected_params}, got {mock_conn.cursor_instance.params}")
        return False


def main():
    """Main validation function."""
    print("🚀 YouTube Metrics Schema Validation")
    print("=" * 50)

    # Check if database connection is available
    if not all([os.getenv("DB_HOST"), os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_NAME")]):
        print("⚠️  Database connection not configured. Skipping database validation.")
        print("   Set DB_HOST, DB_USER, DB_PASS, and DB_NAME environment variables.")
        db_valid = True  # Skip database validation
    else:
        db_valid = validate_database_schema()

    etl_valid = validate_etl_code()
    param_valid = validate_parameter_usage()

    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY")
    print("=" * 50)

    if db_valid:
        print("✅ Database schema: VALID")
    else:
        print("❌ Database schema: INVALID")

    if etl_valid:
        print("✅ ETL code: VALID")
    else:
        print("❌ ETL code: INVALID")

    if param_valid:
        print("✅ Parameter usage: VALID")
    else:
        print("❌ Parameter usage: INVALID")

    overall_valid = db_valid and etl_valid and param_valid

    if overall_valid:
        print("\n🎉 OVERALL RESULT: SCHEMA ALIGNMENT IS CORRECT")
        print("\nThe YouTube ETL code is properly aligned with the database schema.")
        print("No schema mismatches were found.")
        return 0
    else:
        print("\n💥 OVERALL RESULT: SCHEMA ALIGNMENT ISSUES FOUND")
        print("\nPlease review the issues above and fix them.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
