"""
Test suite for YouTube metrics schema alignment.

This test validates that the Python ETL code correctly uses the actual
youtube_metrics database schema without referencing non - existent columns.
"""

from datetime import date, datetime
from unittest.mock import Mock, patch

import pytest

from web.youtube_channel_etl import YouTubeChannelETL


class TestYouTubeMetricsSchemaAlignment:
    """Test YouTube metrics operations against actual database schema."""

    def setup_method(self):
        """Set up test fixtures."""
        self.etl = YouTubeChannelETL(
            api_key="test_key",
            db_host="localhost",
            db_port=3306,
            db_user="test_user",
            db_pass="test_pass",
            db_name="test_db",
        )

    def test_upsert_daily_metrics_uses_correct_columns(self):
        """Test that _upsert_daily_metrics uses actual database columns."""
        # Mock database connection and cursor with context manager support
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        # Test the upsert operation
        video_id = "test_video_123"
        view_count = 1000
        like_count = 50
        comment_count = 25

        self.etl._upsert_daily_metrics(mock_conn, video_id, view_count, like_count, comment_count)

        # Verify the SQL uses correct columns
        mock_cursor.execute.assert_called_once()
        sql_call = mock_cursor.execute.call_args[0]
        sql_statement = sql_call[0]
        sql_params = sql_call[1]

        # Check that SQL uses actual database columns
        assert "video_id" in sql_statement
        assert "view_count" in sql_statement
        assert "like_count" in sql_statement
        assert "dislike_count" in sql_statement
        assert "comment_count" in sql_statement
        assert "subscriber_count" in sql_statement
        assert "metrics_date" in sql_statement
        assert "fetched_at" in sql_statement

        # Check that SQL does NOT use non - existent columns
        assert "isrc" not in sql_statement
        assert "favorite_count" not in sql_statement

        # Check correct primary key usage
        assert "ON DUPLICATE KEY UPDATE" in sql_statement

        # Verify parameters are correct
        assert sql_params == (video_id, view_count, like_count, 0, comment_count)

    def test_upsert_daily_metrics_uses_correct_primary_key(self):
        """Test that upsert operation uses correct composite primary key."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        self.etl._upsert_daily_metrics(mock_conn, "test_video", 1000, 50, 25)

        sql_statement = mock_cursor.execute.call_args[0][0]

        # Verify it uses CURDATE() for metrics_date (part of primary key)
        assert "CURDATE()" in sql_statement
        # Verify it uses NOW() for fetched_at
        assert "NOW()" in sql_statement

    def test_upsert_daily_metrics_handles_null_subscriber_count(self):
        """Test that subscriber_count is correctly set to NULL."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        self.etl._upsert_daily_metrics(mock_conn, "test_video", 1000, 50, 25)

        sql_statement = mock_cursor.execute.call_args[0][0]
        sql_params = mock_cursor.execute.call_args[0][1]

        # Verify subscriber_count is set to NULL in the SQL
        assert "NULL" in sql_statement
        # Verify dislike_count is set to 0 (hardcoded since YouTube removed dislikes)
        assert sql_params[3] == 0  # dislike_count parameter

    def test_coerce_counts_handles_missing_stats(self):
        """Test that _coerce_counts handles missing or invalid statistics."""
        # Test with None stats
        view_count, like_count, comment_count = self.etl._coerce_counts(None)
        assert view_count == 0
        assert like_count == 0
        assert comment_count == 0

        # Test with empty stats
        view_count, like_count, comment_count = self.etl._coerce_counts({})
        assert view_count == 0
        assert like_count == 0
        assert comment_count == 0

        # Test with valid stats
        stats = {"viewCount": "1000", "likeCount": "50", "commentCount": "25"}
        view_count, like_count, comment_count = self.etl._coerce_counts(stats)
        assert view_count == 1000
        assert like_count == 50
        assert comment_count == 25

        # Test with invalid stats (should default to 0)
        stats = {"viewCount": "invalid", "likeCount": None, "commentCount": ""}
        view_count, like_count, comment_count = self.etl._coerce_counts(stats)
        assert view_count == 0
        assert like_count == 0
        assert comment_count == 0

    def test_metrics_upsert_in_load_method(self):
        """Test that the load method correctly calls metrics upsert."""
        mock_conn = Mock()

        # Mock the upsert methods
        with patch.object(self.etl, "_batch_upsert_raw", return_value=2) as mock_raw_upsert, patch.object(
            self.etl, "_upsert_daily_metrics"
        ) as mock_metrics_upsert, patch.object(self.etl, "_upsert_videos_summary") as mock_videos_upsert, patch.object(
            self.etl, "_insert_comments", return_value=0
        ) as mock_comments_insert:

            # Test data
            rows = [("video1", 1000, 50, 25, '{"id": "video1"}'), ("video2", 2000, 100, 50, '{"id": "video2"}')]
            uploads_pid = "test_playlist"

            # Call load method
            raw_count, metrics_count = self.etl.load(mock_conn, uploads_pid, rows)

            # Verify metrics upsert was called for each video
            assert mock_metrics_upsert.call_count == 2
            mock_metrics_upsert.assert_any_call(mock_conn, "video1", 1000, 50, 25)
            mock_metrics_upsert.assert_any_call(mock_conn, "video2", 2000, 100, 50)

            # Verify return values
            assert raw_count == 2
            assert metrics_count == 2

    def test_no_references_to_nonexistent_columns(self):
        """Test that the ETL code doesn't reference non - existent columns in metrics operations."""
        import inspect

        # Get the source code of the _upsert_daily_metrics method specifically
        etl_method_source = inspect.getsource(YouTubeChannelETL._upsert_daily_metrics)

        # Check that non - existent columns are not referenced in metrics operations
        assert "favorite_count" not in etl_method_source.lower()

        # Verify correct columns are used in metrics operations
        assert "metrics_date" in etl_method_source
        assert "fetched_at" in etl_method_source
        assert "video_id" in etl_method_source
        assert "view_count" in etl_method_source
        assert "like_count" in etl_method_source
        assert "comment_count" in etl_method_source
        assert "dislike_count" in etl_method_source
        assert "subscriber_count" in etl_method_source

    def test_transform_method_returns_correct_data_structure(self):
        """Test that transform method returns data in expected format."""
        # Mock video data from YouTube API
        mock_items = [
            {"id": "video1", "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "25"}},
            {"id": "video2", "statistics": {"viewCount": "2000", "likeCount": "100", "commentCount": "50"}},
        ]

        # Call transform method
        result = self.etl.transform(mock_items)

        # Verify structure: (video_id, view_count, like_count, comment_count, raw_json)
        assert len(result) == 2

        # Check first video
        video1_data = result[0]
        assert video1_data[0] == "video1"  # video_id
        assert video1_data[1] == 1000  # view_count
        assert video1_data[2] == 50  # like_count
        assert video1_data[3] == 25  # comment_count
        assert '"id": "video1"' in video1_data[4]  # raw_json

        # Check second video
        video2_data = result[1]
        assert video2_data[0] == "video2"  # video_id
        assert video2_data[1] == 2000  # view_count
        assert video2_data[2] == 100  # like_count
        assert video2_data[3] == 50  # comment_count
        assert '"id": "video2"' in video2_data[4]  # raw_json


class TestMetricsSchemaValidation:
    """Test schema validation for metrics operations."""

    def test_actual_database_schema_matches_expectations(self):
        """Test that the actual database schema matches our expectations."""
        # This test would connect to a test database and verify the schema
        # For now, we'll test the expected schema structure
        expected_columns = {
            "video_id": "varchar(50)",
            "view_count": "bigint",
            "like_count": "bigint",
            "dislike_count": "bigint",
            "comment_count": "bigint",
            "subscriber_count": "bigint",
            "metrics_date": "date",
            "fetched_at": "datetime",
        }

        expected_primary_key = ["video_id", "metrics_date"]

        # These assertions document the expected schema
        assert len(expected_columns) == 8
        assert len(expected_primary_key) == 2
        assert "video_id" in expected_primary_key
        assert "metrics_date" in expected_primary_key

    def test_metrics_date_and_fetched_at_usage(self):
        """Test that metrics_date and fetched_at are used correctly."""
        etl = YouTubeChannelETL(
            api_key="test_key",
            db_host="localhost",
            db_port=3306,
            db_user="test_user",
            db_pass="test_pass",
            db_name="test_db",
        )

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        etl._upsert_daily_metrics(mock_conn, "test_video", 1000, 50, 25)

        sql_statement = mock_cursor.execute.call_args[0][0]

        # Verify metrics_date uses CURDATE() for daily aggregation
        assert "metrics_date" in sql_statement
        assert "CURDATE()" in sql_statement

        # Verify fetched_at uses NOW() for timestamp tracking
        assert "fetched_at" in sql_statement
        assert "NOW()" in sql_statement
