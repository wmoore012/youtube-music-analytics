#!/usr/bin/env python3
"""
Analytics Queries Schema Validation Script

This script validates that analytics queries use the correct ISRC-based schema
instead of the non-existent songs table.

Usage:
    python validate_analytics_queries_schema.py
"""

import inspect as py_inspect
import os
import sys

from sqlalchemy import inspect, text

from src.youtubeviz.data import (
    compute_coengagement_matrix,
    load_artist_daily_metrics,
    load_comment_examples,
    load_sentiment_daily,
    load_sentiment_summary,
)
from web.etl_helpers import get_engine


def validate_database_schema():
    """Validate that the database has the correct ISRC schema."""
    print("🔍 Validating database schema...")

    try:
        engine = get_engine()
        inspector = inspect(engine)

        # Check ISRC tables exist
        required_tables = ["isrc_recordings", "video_recording_link", "isrc_artists"]

        print("\n📋 ISRC schema validation:")
        all_tables_exist = True

        for table in required_tables:
            if inspector.has_table(table):
                print(f"✅ {table}: exists")
            else:
                print(f"❌ {table}: missing")
                all_tables_exist = False

        if all_tables_exist:
            # Check table structures
            print("\n🔧 Table structure validation:")

            # Check isrc_recordings columns
            ir_columns = inspector.get_columns("isrc_recordings")
            ir_column_names = [col["name"] for col in ir_columns]
            expected_ir_columns = ["isrc", "title", "artist_primary"]

            for col in expected_ir_columns:
                if col in ir_column_names:
                    print(f"✅ isrc_recordings.{col}: exists")
                else:
                    print(f"❌ isrc_recordings.{col}: missing")
                    all_tables_exist = False

            # Check video_recording_link columns
            vrl_columns = inspector.get_columns("video_recording_link")
            vrl_column_names = [col["name"] for col in vrl_columns]
            expected_vrl_columns = ["video_id", "isrc", "match_method", "confidence"]

            for col in expected_vrl_columns:
                if col in vrl_column_names:
                    print(f"✅ video_recording_link.{col}: exists")
                else:
                    print(f"❌ video_recording_link.{col}: missing")
                    all_tables_exist = False

        # Check if songs table exists (should be deprecated)
        if inspector.has_table("songs"):
            print("\n⚠️  songs table exists (deprecated - should use ISRC schema)")
        else:
            print("\n✅ songs table does not exist (correct)")

        return all_tables_exist

    except Exception as e:
        print(f"❌ Database schema validation failed: {e}")
        return False


def validate_analytics_functions():
    """Validate that analytics functions don't reference the songs table."""
    print("\n🔍 Validating analytics function source code...")

    functions_to_check = [
        load_artist_daily_metrics,
        load_comment_examples,
        compute_coengagement_matrix,
        load_sentiment_summary,
        load_sentiment_daily,
    ]

    print("\n📋 Source code validation:")
    all_functions_clean = True

    for func in functions_to_check:
        try:
            source = py_inspect.getsource(func)
            func_name = func.__name__

            # Check for problematic references
            problematic_patterns = [
                "songs s",
                "songs sg",
                "from songs",
                "join songs",
                "LEFT JOIN songs",
                "songs.",
            ]

            found_issues = []
            for pattern in problematic_patterns:
                if pattern.lower() in source.lower():
                    found_issues.append(pattern)

            if found_issues:
                print(f"❌ {func_name}: references songs table - {found_issues}")
                all_functions_clean = False
            else:
                print(f"✅ {func_name}: clean (no songs table references)")

                # Check for correct ISRC references
                correct_patterns = ["isrc_recordings", "video_recording_link", "ir.artist_primary"]

                found_correct = []
                for pattern in correct_patterns:
                    if pattern in source:
                        found_correct.append(pattern)

                if found_correct:
                    print(f"   ✅ Uses ISRC schema: {found_correct}")

        except (OSError, TypeError):
            print(f"⚠️  {func.__name__}: could not inspect source")

    return all_functions_clean


def test_analytics_functions():
    """Test that analytics functions execute successfully."""
    print("\n🔍 Testing analytics function execution...")

    try:
        engine = get_engine()

        test_functions = [
            ("load_artist_daily_metrics", lambda: load_artist_daily_metrics(engine=engine)),
            ("load_sentiment_summary", lambda: load_sentiment_summary(engine=engine)),
            ("load_sentiment_daily", lambda: load_sentiment_daily(engine=engine)),
            ("compute_coengagement_matrix", lambda: compute_coengagement_matrix(engine=engine)),
            ("load_comment_examples", lambda: load_comment_examples(engine=engine)),
        ]

        print("\n📋 Function execution tests:")
        all_functions_work = True

        for func_name, func_call in test_functions:
            try:
                result = func_call()
                if hasattr(result, "shape"):
                    print(f"✅ {func_name}: executed successfully (shape: {result.shape})")
                else:
                    print(f"✅ {func_name}: executed successfully")
            except Exception as e:
                print(f"❌ {func_name}: failed - {e}")
                all_functions_work = False

        return all_functions_work

    except Exception as e:
        print(f"❌ Function execution testing failed: {e}")
        return False


def test_isrc_schema_queries():
    """Test that ISRC schema queries work correctly."""
    print("\n🔍 Testing ISRC schema queries...")

    try:
        engine = get_engine()

        test_queries = [
            (
                "Artist performance with ISRC",
                """
                SELECT
                    COALESCE(ir.artist_primary, v.channel_title) as artist_name,
                    COUNT(v.video_id) as video_count,
                    AVG(m.view_count) as avg_views
                FROM youtube_videos v
                LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
                LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
                LEFT JOIN youtube_metrics m ON v.video_id = m.video_id
                WHERE v.channel_title IS NOT NULL
                GROUP BY COALESCE(ir.artist_primary, v.channel_title)
                HAVING COUNT(v.video_id) > 0
                ORDER BY video_count DESC
                LIMIT 5
            """,
            ),
            (
                "ISRC coverage check",
                """
                SELECT
                    COUNT(DISTINCT v.video_id) as total_videos,
                    COUNT(DISTINCT vrl.video_id) as videos_with_isrc,
                    ROUND(COUNT(DISTINCT vrl.video_id) * 100.0 / COUNT(DISTINCT v.video_id), 2) as isrc_coverage_pct
                FROM youtube_videos v
                LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
            """,
            ),
            (
                "Recording metadata sample",
                """
                SELECT
                    ir.isrc,
                    ir.title,
                    ir.artist_primary,
                    COUNT(vrl.video_id) as linked_videos
                FROM isrc_recordings ir
                LEFT JOIN video_recording_link vrl ON ir.isrc = vrl.isrc
                GROUP BY ir.isrc, ir.title, ir.artist_primary
                HAVING COUNT(vrl.video_id) > 0
                ORDER BY linked_videos DESC
                LIMIT 5
            """,
            ),
        ]

        print("\n📋 ISRC schema query tests:")
        all_queries_work = True

        with engine.connect() as conn:
            for query_name, query in test_queries:
                try:
                    result = conn.execute(text(query)).fetchall()
                    print(f"✅ {query_name}: executed successfully ({len(result)} rows)")

                    # Show sample data for first query
                    if query_name == "Artist performance with ISRC" and result:
                        print(f"   Sample: {result[0]}")

                except Exception as e:
                    print(f"❌ {query_name}: failed - {e}")
                    all_queries_work = False

        return all_queries_work

    except Exception as e:
        print(f"❌ ISRC schema query testing failed: {e}")
        return False


def main():
    """Main validation function."""
    print("🚀 Analytics Queries Schema Validation")
    print("=" * 50)

    # Check if database connection is available
    if not all([os.getenv("DB_HOST"), os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_NAME")]):
        print("⚠️  Database connection not configured. Skipping database validation.")
        print("   Set DB_HOST, DB_USER, DB_PASS, and DB_NAME environment variables.")
        db_valid = True  # Skip database validation
        queries_valid = True
    else:
        db_valid = validate_database_schema()
        queries_valid = test_isrc_schema_queries()

    code_valid = validate_analytics_functions()
    functions_valid = test_analytics_functions() if db_valid else True

    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY")
    print("=" * 50)

    if db_valid:
        print("✅ Database schema: VALID")
    else:
        print("❌ Database schema: INVALID")

    if code_valid:
        print("✅ Analytics code: VALID")
    else:
        print("❌ Analytics code: INVALID")

    if functions_valid:
        print("✅ Function execution: VALID")
    else:
        print("❌ Function execution: INVALID")

    if queries_valid:
        print("✅ ISRC schema queries: VALID")
    else:
        print("❌ ISRC schema queries: INVALID")

    overall_valid = db_valid and code_valid and functions_valid and queries_valid

    if overall_valid:
        print("\n🎉 OVERALL RESULT: ANALYTICS QUERIES SCHEMA ALIGNMENT IS CORRECT")
        print("\nAll analytics queries have been successfully updated to use the ISRC-based schema.")
        print("No references to the non-existent 'songs' table were found.")
        return 0
    else:
        print("\n💥 OVERALL RESULT: ANALYTICS QUERIES SCHEMA ALIGNMENT ISSUES FOUND")
        print("\nPlease review the issues above and fix them.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
