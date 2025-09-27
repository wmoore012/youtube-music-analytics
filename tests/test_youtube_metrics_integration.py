"""
Integration test for YouTube metrics schema alignment.

This test validates that the metrics operations work correctly with a real database connection.
"""

from datetime import date, datetime
import os

import pytest

from web.etl_helpers import get_engine
from web.youtube_channel_etl import YouTubeChannelETL


class TestYouTubeMetricsIntegration:
    """Integration tests for YouTube metrics operations."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        # Skip if no database connection available
        if not all([os.getenv("DB_HOST"), os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_NAME")]):
            pytest.skip("Database connection not configured")

        self.engine = get_engine()
        self.etl = YouTubeChannelETL(
            api_key="test_key",
            db_host=os.getenv("DB_HOST"),
            db_port=int(os.getenv("DB_PORT", 3306)),
            db_user=os.getenv("DB_USER"),
            db_pass=os.getenv("DB_PASS"),
            db_name=os.getenv("DB_NAME"),
        )

    def test_youtube_metrics_table_exists_with_correct_schema(self):
        """Test that youtube_metrics table exists with expected schema."""
        with self.engine.connect() as conn:
            # Check if table exists
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics'"
            ).scalar()
            assert result == 1, "youtube_metrics table should exist"

            # Check column structure
            columns_result = conn.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics' "
                "ORDER BY ordinal_position"
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

            for col_name, (expected_type, expected_nullable) in expected_columns.items():
                assert col_name in actual_columns, f"Column {col_name} should exist"
                actual_type, actual_nullable = actual_columns[col_name]
                assert expected_type in actual_type.lower(), f"Column {col_name} should be {expected_type}"
                assert actual_nullable == expected_nullable, f"Column {col_name} nullable should be {expected_nullable}"

    def test_youtube_metrics_primary_key_is_correct(self):
        """Test that youtube_metrics has the correct composite primary key."""
        with self.engine.connect() as conn:
            # Check primary key structure
            pk_result = conn.execute(
                "SELECT column_name FROM information_schema.key_column_usage "
                "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics' "
                "AND constraint_name = 'PRIMARY' "
                "ORDER BY ordinal_position"
            ).fetchall()

            pk_columns = [row[0] for row in pk_result]
            expected_pk = ["video_id", "metrics_date"]

            assert pk_columns == expected_pk, f"Primary key should be {expected_pk}, got {pk_columns}"

    def test_metrics_upsert_operation_works_with_real_database(self):
        """Test that metrics upsert operation works with real database connection."""
        test_video_id = "test_video_integration_123"

        try:
            conn = self.etl._connect()

            # Clean up any existing test data
            with conn.cursor() as cur:
                cur.execute("DELETE FROM youtube_metrics WHERE video_id = %s", (test_video_id,))
            conn.commit()

            # Test initial insert
            self.etl._upsert_daily_metrics(conn, test_video_id, 1000, 50, 25)
            conn.commit()

            # Verify data was inserted
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT video_id, view_count, like_count, dislike_count, comment_count, "
                    "subscriber_count, metrics_date, fetched_at "
                    "FROM youtube_metrics WHERE video_id = %s AND metrics_date = CURDATE()",
                    (test_video_id,),
                )
                result = cur.fetchone()

            assert result is not None, "Metrics record should be inserted"
            assert result["video_id"] == test_video_id
            assert result["view_count"] == 1000
            assert result["like_count"] == 50
            assert result["dislike_count"] == 0  # Hardcoded to 0
            assert result["comment_count"] == 25
            assert result["subscriber_count"] is None  # Set to NULL
            assert result["metrics_date"] == date.today()
            assert isinstance(result["fetched_at"], datetime)

            # Test update with higher values (should update)
            self.etl._upsert_daily_metrics(conn, test_video_id, 2000, 100, 50)
            conn.commit()

            # Verify data was updated
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT view_count, like_count, comment_count "
                    "FROM youtube_metrics WHERE video_id = %s AND metrics_date = CURDATE()",
                    (test_video_id,),
                )
                result = cur.fetchone()

            assert result["view_count"] == 2000  # Should be updated
            assert result["like_count"] == 100  # Should be updated
            assert result["comment_count"] == 50  # Should be updated

            # Test update with lower values (should not update counts)
            self.etl._upsert_daily_metrics(conn, test_video_id, 1500, 75, 30)
            conn.commit()

            # Verify counts were not decreased
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT view_count, like_count, comment_count "
                    "FROM youtube_metrics WHERE video_id = %s AND metrics_date = CURDATE()",
                    (test_video_id,),
                )
                result = cur.fetchone()

            assert result["view_count"] == 2000  # Should remain higher value
            assert result["like_count"] == 100  # Should remain higher value
            assert result["comment_count"] == 50  # Should remain higher value

        finally:
            # Clean up test data
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM youtube_metrics WHERE video_id = %s", (test_video_id,))
                conn.commit()
                conn.close()
            except Exception:
                pass

    def test_no_references_to_nonexistent_columns_in_database(self):
        """Test that the database doesn't have non - existent columns that might be referenced."""
        with self.engine.connect() as conn:
            # Check that non - existent columns are not in the table
            columns_result = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = 'youtube_metrics'"
            ).fetchall()

            column_names = [row[0].lower() for row in columns_result]

            # Verify non - existent columns are not present
            assert "isrc" not in column_names, "youtube_metrics should not have isrc column"
            assert "favorite_count" not in column_names, "youtube_metrics should not have favorite_count column"

            # Verify expected columns are present
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

            for col in expected_columns:
                assert col in column_names, f"youtube_metrics should have {col} column"
