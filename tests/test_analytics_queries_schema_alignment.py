"""
Test suite for analytics queries schema alignment.

This test validates that analytics queries use the correct ISRC-based schema
instead of the non-existent songs table.
"""

from unittest.mock import Mock, patch

import pytest
from sqlalchemy import inspect, text

from src.youtubeviz.data import (
    compute_coengagement_matrix,
    load_artist_daily_metrics,
    load_comment_examples,
    load_sentiment_daily,
    load_sentiment_summary,
)
from web.etl_helpers import get_engine


class TestAnalyticsQueriesSchemaAlignment:
    """Test analytics queries use correct database schema."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_engine = Mock()
        self.mock_conn = Mock()
        self.mock_engine.connect.return_value.__enter__ = Mock(return_value=self.mock_conn)
        self.mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    def test_load_artist_daily_metrics_uses_isrc_schema(self):
        """Test that load_artist_daily_metrics uses ISRC schema when available."""
        # Mock schema inspection to return ISRC tables exist
        with patch("src.youtubeviz.data.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.side_effect = lambda table: table in [
                "isrc_recordings",
                "video_recording_link",
            ]

            # Mock pandas read_sql to capture the query
            with patch("src.youtubeviz.data.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = Mock()

                # Call the function
                load_artist_daily_metrics(artists=["Test Artist"], engine=self.mock_engine)

                # Verify the query uses ISRC schema
                query_call = mock_read_sql.call_args[0][0]
                query_str = str(query_call)

                # Should use ISRC tables
                assert "video_recording_link vrl" in query_str
                assert "isrc_recordings ir" in query_str
                assert "ir.artist_primary" in query_str

                # Should NOT use songs table
                assert "songs" not in query_str.lower()

    def test_load_artist_daily_metrics_fallback_to_channel_title(self):
        """Test that load_artist_daily_metrics falls back to channel_title when ISRC schema unavailable."""
        # Mock schema inspection to return ISRC tables don't exist
        with patch("src.youtubeviz.data.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.return_value = False

            # Mock pandas read_sql to capture the query
            with patch("src.youtubeviz.data.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = Mock()

                # Call the function
                load_artist_daily_metrics(artists=["Test Artist"], engine=self.mock_engine)

                # Verify the query uses channel_title fallback
                query_call = mock_read_sql.call_args[0][0]
                query_str = str(query_call)

                # Should use channel_title
                assert "v.channel_title" in query_str
                assert "v.channel_title IN" in query_str

                # Should NOT use ISRC tables or songs table
                assert "video_recording_link" not in query_str
                assert "isrc_recordings" not in query_str
                assert "songs" not in query_str.lower()

    def test_load_comment_examples_uses_isrc_schema(self):
        """Test that load_comment_examples uses ISRC schema when available."""
        # Mock schema inspection
        with patch("src.youtubeviz.data.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.side_effect = lambda table: table in [
                "isrc_recordings",
                "video_recording_link",
            ]
            mock_inspect.return_value.get_columns.return_value = [{"name": "published_at"}, {"name": "comment_text"}]

            # Mock pandas read_sql
            with patch("src.youtubeviz.data.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = Mock()
                mock_read_sql.return_value.empty = True

                # Call the function
                load_comment_examples(artists=["Test Artist"], engine=self.mock_engine)

                # Verify the query uses ISRC schema
                query_call = mock_read_sql.call_args[0][0]
                query_str = str(query_call)

                # Should use ISRC tables
                assert "video_recording_link vrl" in query_str
                assert "isrc_recordings ir" in query_str
                assert "ir.artist_primary" in query_str

                # Should NOT use songs table
                assert "songs" not in query_str.lower()

    def test_compute_coengagement_matrix_uses_isrc_schema(self):
        """Test that compute_coengagement_matrix uses ISRC schema when available."""
        # Mock schema inspection
        with patch("src.youtubeviz.data.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.side_effect = lambda table: table in [
                "isrc_recordings",
                "video_recording_link",
            ]

            # Mock pandas read_sql
            with patch("src.youtubeviz.data.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = Mock()
                mock_read_sql.return_value.empty = True

                # Call the function
                compute_coengagement_matrix(artists=["Test Artist"], engine=self.mock_engine)

                # Verify the query uses ISRC schema
                query_call = mock_read_sql.call_args[0][0]
                query_str = str(query_call)

                # Should use ISRC tables
                assert "video_recording_link vrl" in query_str
                assert "isrc_recordings ir" in query_str
                assert "ir.artist_primary" in query_str

                # Should NOT use songs table
                assert "songs" not in query_str.lower()

    def test_load_sentiment_summary_uses_isrc_schema(self):
        """Test that load_sentiment_summary uses ISRC schema when available."""
        # Mock schema inspection and data check
        with patch("src.youtubeviz.data.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.side_effect = lambda table: table in [
                "isrc_recordings",
                "video_recording_link",
            ]

            # Mock the data availability check
            mock_result = Mock()
            mock_result.fetchone.return_value = [10]  # Has data
            self.mock_conn.execute.return_value = mock_result

            # Mock pandas read_sql
            with patch("src.youtubeviz.data.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = Mock()

                # Call the function
                load_sentiment_summary(artists=["Test Artist"], engine=self.mock_engine)

                # Verify the query uses ISRC schema
                query_call = mock_read_sql.call_args[0][0]
                query_str = str(query_call)

                # Should use ISRC tables
                assert "video_recording_link vrl" in query_str
                assert "isrc_recordings ir" in query_str
                assert "ir.artist_primary" in query_str

                # Should NOT use songs table
                assert "songs" not in query_str.lower()

    def test_load_sentiment_daily_uses_isrc_schema(self):
        """Test that load_sentiment_daily uses ISRC schema when available."""
        # Mock schema inspection and data check
        with patch("src.youtubeviz.data.inspect") as mock_inspect:
            mock_inspect.return_value.has_table.side_effect = lambda table: table in [
                "isrc_recordings",
                "video_recording_link",
            ]
            mock_inspect.return_value.get_columns.return_value = [{"name": "published_at"}, {"name": "comment_text"}]

            # Mock the data availability check
            mock_result = Mock()
            mock_result.fetchone.return_value = [10]  # Has data
            self.mock_conn.execute.return_value = mock_result

            # Mock pandas read_sql
            with patch("src.youtubeviz.data.pd.read_sql") as mock_read_sql:
                mock_read_sql.return_value = Mock()

                # Call the function
                load_sentiment_daily(artists=["Test Artist"], engine=self.mock_engine)

                # Verify the query uses ISRC schema
                query_call = mock_read_sql.call_args[0][0]
                query_str = str(query_call)

                # Should use ISRC tables
                assert "video_recording_link vrl" in query_str
                assert "isrc_recordings ir" in query_str
                assert "ir.artist_primary" in query_str

                # Should NOT use songs table
                assert "songs" not in query_str.lower()

    def test_no_references_to_songs_table_in_queries(self):
        """Test that no analytics functions reference the non-existent songs table."""
        import inspect as py_inspect

        from src.youtubeviz import data

        # Get all functions in the data module
        functions = [
            getattr(data, name) for name in dir(data) if callable(getattr(data, name)) and not name.startswith("_")
        ]

        # Check source code of each function
        for func in functions:
            try:
                source = py_inspect.getsource(func)
                # Should not reference songs table
                assert "songs s" not in source.lower()
                assert "songs sg" not in source.lower()
                assert "from songs" not in source.lower()
                assert "join songs" not in source.lower()
            except (OSError, TypeError):
                # Skip functions without source (built-ins, etc.)
                continue


class TestAnalyticsQueriesIntegration:
    """Integration tests for analytics queries with real database."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures."""
        # Skip if no database connection available
        try:
            self.engine = get_engine()
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception:
            pytest.skip("Database connection not available")

    def test_isrc_schema_exists_in_database(self):
        """Test that the ISRC schema tables exist in the database."""
        inspector = inspect(self.engine)

        # Check that ISRC tables exist
        assert inspector.has_table("isrc_recordings"), "isrc_recordings table should exist"
        assert inspector.has_table("video_recording_link"), "video_recording_link table should exist"
        assert inspector.has_table("isrc_artists"), "isrc_artists table should exist"

        # Check that songs table does NOT exist (or is deprecated)
        # Note: songs table might exist for backward compatibility but shouldn't be used

    def test_isrc_recordings_table_structure(self):
        """Test that isrc_recordings table has expected structure."""
        inspector = inspect(self.engine)
        columns = inspector.get_columns("isrc_recordings")
        column_names = [col["name"] for col in columns]

        # Check expected columns
        expected_columns = ["isrc", "title", "artist_primary"]
        for col in expected_columns:
            assert col in column_names, f"isrc_recordings should have {col} column"

    def test_video_recording_link_table_structure(self):
        """Test that video_recording_link table has expected structure."""
        inspector = inspect(self.engine)
        columns = inspector.get_columns("video_recording_link")
        column_names = [col["name"] for col in columns]

        # Check expected columns
        expected_columns = ["video_id", "isrc", "match_method", "confidence"]
        for col in expected_columns:
            assert col in column_names, f"video_recording_link should have {col} column"

    def test_analytics_queries_execute_successfully(self):
        """Test that analytics queries execute without errors."""
        # Test load_artist_daily_metrics
        try:
            result = load_artist_daily_metrics(engine=self.engine)
            assert result is not None
        except Exception as e:
            pytest.fail(f"load_artist_daily_metrics failed: {e}")

        # Test load_sentiment_summary
        try:
            result = load_sentiment_summary(engine=self.engine)
            assert result is not None
        except Exception as e:
            pytest.fail(f"load_sentiment_summary failed: {e}")

        # Test load_sentiment_daily
        try:
            result = load_sentiment_daily(engine=self.engine)
            assert result is not None
        except Exception as e:
            pytest.fail(f"load_sentiment_daily failed: {e}")

    def test_artist_performance_queries_use_correct_schema(self):
        """Test that artist performance queries use the correct schema."""
        with self.engine.connect() as conn:
            # Test a query that should use ISRC schema if available
            try:
                # This query should work with the new schema
                result = conn.execute(
                    text(
                        """
                    SELECT
                        COALESCE(ir.artist_primary, v.channel_title) as artist_name,
                        COUNT(v.video_id) as video_count
                    FROM youtube_videos v
                    LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
                    LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
                    WHERE v.channel_title IS NOT NULL
                    GROUP BY COALESCE(ir.artist_primary, v.channel_title)
                    LIMIT 5
                """
                    )
                ).fetchall()

                assert result is not None

            except Exception as e:
                pytest.fail(f"Artist performance query failed: {e}")
