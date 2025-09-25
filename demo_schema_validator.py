#!/usr/bin/env python3
"""
Schema Validator Demonstration

This script demonstrates the schema validation functionality including:
- Table column validation
- Schema drift detection
- Referential integrity checking
- ETL startup validation
- Validation decorators

Usage:
    python demo_schema_validator.py
"""

from datetime import datetime
import os
import sys

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web.etl_helpers import get_engine
from web.schema_validator import SchemaValidationError, SchemaValidator, require_valid_schema, validate_input_types


def demo_table_column_validation():
    """Demonstrate table column validation functionality."""
    print("🔍 TABLE COLUMN VALIDATION DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        validator = SchemaValidator(engine)

        # Test valid column validation
        print("✅ Testing valid columns for youtube_videos...")
        result = validator.validate_table_columns(
            "youtube_videos", ["video_id", "title", "channel_title", "published_at"]
        )

        print(f"   Validation result: {'PASSED' if result.is_valid else 'FAILED'}")
        print(f"   Errors: {len(result.errors)}")
        print(f"   Warnings: {len(result.warnings)}")

        if result.warnings:
            print("   Sample warnings:")
            for warning in result.warnings[:3]:
                print(f"     - {warning.message}")

        print()

        # Test invalid column validation
        print("❌ Testing invalid columns for youtube_videos...")
        result = validator.validate_table_columns("youtube_videos", ["video_id", "title", "nonexistent_column"])

        print(f"   Validation result: {'PASSED' if result.is_valid else 'FAILED'}")
        print(f"   Errors: {len(result.errors)}")

        if result.errors:
            print("   Errors found:")
            for error in result.errors:
                print(f"     - {error.message}")

        print()

        # Test non-existent table
        print("🚫 Testing non-existent table...")
        result = validator.validate_table_columns("nonexistent_table", ["col1"])

        print(f"   Validation result: {'PASSED' if result.is_valid else 'FAILED'}")
        if result.errors:
            print(f"   Error: {result.errors[0].message}")

        print()

    except Exception as e:
        print(f"❌ Error during column validation demo: {e}")


def demo_schema_inspection():
    """Demonstrate schema inspection functionality."""
    print("🔍 SCHEMA INSPECTION DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        validator = SchemaValidator(engine)

        # Inspect youtube_videos table
        print("📋 Inspecting youtube_videos table schema...")
        schema = validator.get_table_schema("youtube_videos")

        print(f"   Table: {schema.table_name}")
        print(f"   Columns: {len(schema.columns)}")
        print(f"   Primary keys: {schema.primary_keys}")
        print(f"   Foreign keys: {len(schema.foreign_keys)}")
        print(f"   Indexes: {len(schema.indexes)}")

        print("\n   Column details:")
        for col in schema.columns[:5]:  # Show first 5 columns
            pk_indicator = " (PK)" if col.primary_key else ""
            nullable_indicator = " (nullable)" if col.nullable else " (not null)"
            print(f"     - {col.name}: {col.type_name}{pk_indicator}{nullable_indicator}")

        if len(schema.columns) > 5:
            print(f"     ... and {len(schema.columns) - 5} more columns")

        print()

        # Inspect youtube_metrics table
        print("📋 Inspecting youtube_metrics table schema...")
        schema = validator.get_table_schema("youtube_metrics")

        print(f"   Table: {schema.table_name}")
        print(f"   Columns: {len(schema.columns)}")
        print(f"   Primary keys: {schema.primary_keys}")

        print()

    except Exception as e:
        print(f"❌ Error during schema inspection demo: {e}")


def demo_schema_drift_detection():
    """Demonstrate schema drift detection functionality."""
    print("🔍 SCHEMA DRIFT DETECTION DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        validator = SchemaValidator(engine)

        print("🔍 Detecting schema drift...")
        drift_report = validator.detect_schema_drift()

        print(f"📊 DRIFT DETECTION RESULTS:")
        print(f"   Drift detected: {'YES' if drift_report.has_drift else 'NO'}")
        print(f"   Tables added: {len(drift_report.tables_added)}")
        print(f"   Tables removed: {len(drift_report.tables_removed)}")
        print(f"   Tables with added columns: {len(drift_report.columns_added)}")
        print(f"   Tables with removed columns: {len(drift_report.columns_removed)}")
        print(f"   Tables with modified columns: {len(drift_report.columns_modified)}")

        if drift_report.tables_added:
            print(f"\n   ➕ Tables added:")
            for table in drift_report.tables_added:
                print(f"     - {table}")

        if drift_report.tables_removed:
            print(f"\n   ➖ Tables removed:")
            for table in drift_report.tables_removed:
                print(f"     - {table}")

        if drift_report.columns_added:
            print(f"\n   ➕ Columns added:")
            for table, columns in drift_report.columns_added.items():
                print(f"     - {table}: {', '.join(columns)}")

        if drift_report.columns_removed:
            print(f"\n   ➖ Columns removed:")
            for table, columns in drift_report.columns_removed.items():
                print(f"     - {table}: {', '.join(columns)}")

        print()

    except Exception as e:
        print(f"❌ Error during schema drift detection demo: {e}")


def demo_referential_integrity():
    """Demonstrate referential integrity checking functionality."""
    print("🔍 REFERENTIAL INTEGRITY DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        validator = SchemaValidator(engine)

        print("🔍 Checking referential integrity...")
        integrity_results = validator.validate_referential_integrity()

        print(f"📊 INTEGRITY CHECK RESULTS:")
        print(f"   Relationships checked: {len(integrity_results)}")

        if integrity_results:
            valid_count = sum(1 for r in integrity_results if r.is_valid)
            print(f"   Valid relationships: {valid_count}/{len(integrity_results)}")

            for result in integrity_results:
                status = "✅ VALID" if result.is_valid else "❌ INVALID"
                print(f"   {status}: {result.table_name}.{result.foreign_key} -> {result.referenced_table}")

                if not result.is_valid and result.orphaned_count > 0:
                    print(f"     Orphaned records: {result.orphaned_count}")
                    if result.sample_orphaned_ids:
                        sample = ", ".join(result.sample_orphaned_ids[:3])
                        print(f"     Sample orphaned IDs: {sample}")
        else:
            print("   No foreign key relationships configured for validation")

        print()

    except Exception as e:
        print(f"❌ Error during referential integrity demo: {e}")


def demo_etl_startup_validation():
    """Demonstrate ETL startup validation functionality."""
    print("🔍 ETL STARTUP VALIDATION DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        validator = SchemaValidator(engine)

        print("🚀 Running ETL startup validation...")
        result = validator.validate_etl_startup()

        print(f"📊 ETL STARTUP VALIDATION RESULTS:")
        print(f"   Overall status: {'✅ PASSED' if result.is_valid else '❌ FAILED'}")
        print(f"   Errors: {len(result.errors)}")
        print(f"   Warnings: {len(result.warnings)}")
        print(f"   Validation timestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        if result.errors:
            print(f"\n   ❌ Errors found:")
            for error in result.errors:
                print(f"     - {error.table_name}: {error.message}")

        if result.warnings:
            print(f"\n   ⚠️  Warnings:")
            for warning in result.warnings[:5]:  # Show first 5 warnings
                print(f"     - {warning.table_name}: {warning.message}")
            if len(result.warnings) > 5:
                print(f"     ... and {len(result.warnings) - 5} more warnings")

        if result.is_valid:
            print(f"\n   🎉 ETL system is ready to start!")
        else:
            print(f"\n   🚨 ETL system has issues that need to be resolved!")

        print()

    except Exception as e:
        print(f"❌ Error during ETL startup validation demo: {e}")


def demo_validation_decorators():
    """Demonstrate validation decorators functionality."""
    print("🔍 VALIDATION DECORATORS DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()

        # Demo input type validation decorator
        print("🔧 Testing input type validation decorator...")

        @validate_input_types(video_id=str, view_count=int)
        def update_video_metrics(video_id, view_count):
            return f"Updated {video_id} with {view_count} views"

        # Test valid input
        try:
            result = update_video_metrics("test_video", 1000)
            print(f"   ✅ Valid input: {result}")
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")

        # Test invalid input
        try:
            result = update_video_metrics("test_video", "not_a_number")
            print(f"   ❌ Should have failed: {result}")
        except TypeError as e:
            print(f"   ✅ Caught type error as expected: {e}")

        print()

        # Demo schema validation decorator
        print("🔧 Testing schema validation decorator...")

        @require_valid_schema("youtube_videos", ["video_id", "title"])
        def process_video_data(engine=None, video_id=None):
            return f"Processed video {video_id}"

        # Test valid schema
        try:
            result = process_video_data(engine=engine, video_id="test_video")
            print(f"   ✅ Valid schema: {result}")
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")

        # Test invalid schema
        @require_valid_schema("youtube_videos", ["video_id", "nonexistent_column"])
        def process_invalid_schema(engine=None):
            return "Should not reach here"

        try:
            result = process_invalid_schema(engine=engine)
            print(f"   ❌ Should have failed: {result}")
        except SchemaValidationError as e:
            print(f"   ✅ Caught schema validation error as expected")
            print(f"       Error: {str(e)[:100]}...")

        print()

    except Exception as e:
        print(f"❌ Error during validation decorators demo: {e}")


def demo_performance_info():
    """Show performance information about schema validation."""
    print("📊 PERFORMANCE INFORMATION")
    print("=" * 60)

    try:
        engine = get_engine()
        validator = SchemaValidator(engine)

        # Time schema inspection
        import time

        start_time = time.time()
        schema = validator.get_table_schema("youtube_videos")
        inspection_time = time.time() - start_time

        print(f"⏱️  Schema inspection performance:")
        print(f"   Table inspection time: {inspection_time:.4f} seconds")
        print(f"   Columns inspected: {len(schema.columns)}")
        print(f"   Time per column: {inspection_time / len(schema.columns):.6f} seconds")

        # Time cached vs non-cached access
        start_time = time.time()
        schema_cached = validator.get_table_schema("youtube_videos", use_cache=True)
        cached_time = time.time() - start_time

        start_time = time.time()
        schema_fresh = validator.get_table_schema("youtube_videos", use_cache=False)
        fresh_time = time.time() - start_time

        print(f"\n   Cached access time: {cached_time:.6f} seconds")
        print(f"   Fresh access time: {fresh_time:.6f} seconds")
        print(f"   Cache speedup: {fresh_time / cached_time:.1f}x faster")

        print()

    except Exception as e:
        print(f"❌ Error during performance demo: {e}")


def main():
    """Run all demonstrations."""
    print("🚀 SCHEMA VALIDATOR DEMONSTRATION")
    print("=" * 80)
    print()

    # Check database connection
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        print("✅ Database connection successful")
        print()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Please check your .env configuration and database setup.")
        return 1

    # Run demonstrations
    demo_table_column_validation()
    demo_schema_inspection()
    demo_schema_drift_detection()
    demo_referential_integrity()
    demo_etl_startup_validation()
    demo_validation_decorators()
    demo_performance_info()

    print("🎉 DEMONSTRATION COMPLETE")
    print("=" * 80)
    print()
    print("💡 Key takeaways:")
    print("   1. Schema validation helps prevent runtime errors")
    print("   2. Drift detection identifies schema changes over time")
    print("   3. Referential integrity checks find orphaned data")
    print("   4. ETL startup validation ensures system readiness")
    print("   5. Decorators provide automatic validation for functions")
    print("   6. Caching improves performance for repeated validations")
    print()
    print("🔧 Integration tips:")
    print("   1. Add schema validation to ETL startup process")
    print("   2. Use decorators on database operation functions")
    print("   3. Run drift detection periodically (daily/weekly)")
    print("   4. Monitor referential integrity in production")
    print("   5. Cache schema information for better performance")
    print()

    return 0


if __name__ == "__main__":
    # Import text here to avoid circular imports
    from sqlalchemy import text

    sys.exit(main())
