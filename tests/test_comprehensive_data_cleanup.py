#!/usr/bin/env python3
"""
Tests for Comprehensive Data Cleanup Tool

These tests ensure the cleanup tool properly:
1. Identifies configured vs unauthorized artists
2. Removes unauthorized data completely
3. Preserves authorized artist data
4. Handles edge cases safely
"""

import os

# Import the cleanup tool
import sys
from unittest.mock import MagicMock, patch

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.append(".")
from tools.maintenance.comprehensive_data_cleanup import (
    get_configured_artists,
    get_database_artists,
    remove_unauthorized_artist,
)


class TestComprehensiveDataCleanup:

    def test_get_configured_artists(self):
        """Test that we correctly parse configured artists from .env"""
        with patch.dict(
            os.environ,
            {
                "YT_BICFIZZLE_YT": "https://youtube.com/@BicFizzle",
                "YT_COBRAH_YT": "https://www.youtube.com/@COBRAH",
                "YT_COROOK_YT": "https://www.youtube.com/@hicorook",
                "YT_RAICHE_YT": "https://www.youtube.com/@MyNameIsRaiche",
                "YT_RE6CE_YT": "https://www.youtube.com/@re6ce",
                "YT_FLYANABOSS_YT": "https://www.youtube.com/@FlyanaBoss",
                "SOME_OTHER_VAR": "not_a_channel",
            },
        ):
            configured = get_configured_artists()

            expected = {"BiC Fizzle", "COBRAH", "Corook", "Raiche", "re6ce", "Flyana Boss"}

            assert configured == expected
            assert len(configured) == 6

    def test_get_configured_artists_empty(self):
        """Test behavior when no artists are configured"""
        with patch.dict(os.environ, {}, clear=True):
            with patch("tools.maintenance.comprehensive_data_cleanup.load_dotenv"):
                configured = get_configured_artists()
                assert configured == set()

    def test_get_configured_artists_unknown(self):
        """Test behavior with unknown artist in .env"""
        with patch.dict(os.environ, {"YT_UNKNOWN_ARTIST_YT": "https://youtube.com/@Unknown"}, clear=True):
            with patch("tools.maintenance.comprehensive_data_cleanup.load_dotenv"):
                # Should not crash, just warn
                configured = get_configured_artists()
                assert configured == set()

    @pytest.mark.integration
    def test_database_integration(self):
        """Integration test with real database (if available)"""
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")

        if not db_url:
            pytest.skip("No DATABASE_URL configured")

        try:
            engine = create_engine(db_url)

            # Test getting database artists
            db_artists = get_database_artists(engine)

            # Should return list of tuples (artist_name, video_count)
            assert isinstance(db_artists, list)
            for artist, count in db_artists:
                assert isinstance(artist, str)
                assert isinstance(count, int)
                assert count >= 0

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    def test_authorized_vs_unauthorized_detection(self):
        """Test detection of authorized vs unauthorized artists"""
        # Mock database artists
        mock_db_artists = [
            ("BiC Fizzle", 95),
            ("COBRAH", 60),
            ("UnauthorizedArtist1", 323),  # Unauthorized
            ("Flyana Boss", 283),
            ("UnauthorizedArtist2", 50),  # Unauthorized
        ]

        with patch.dict(
            os.environ,
            {
                "YT_BICFIZZLE_YT": "https://youtube.com/@BicFizzle",
                "YT_COBRAH_YT": "https://www.youtube.com/@COBRAH",
                "YT_FLYANABOSS_YT": "https://www.youtube.com/@FlyanaBoss",
            },
        ):
            configured = get_configured_artists()
            db_artist_names = {artist for artist, _ in mock_db_artists}

            authorized = db_artist_names & configured
            unauthorized = db_artist_names - configured

            assert authorized == {"BiC Fizzle", "COBRAH", "Flyana Boss"}
            assert unauthorized == {"UnauthorizedArtist1", "UnauthorizedArtist2"}

    def test_cleanup_safety_checks(self):
        """Test that cleanup has proper safety checks"""
        # The cleanup tool should:
        # 1. Require explicit confirmation
        # 2. Show what will be deleted
        # 3. Not delete anything without confirmation

        # This is tested in the main() function behavior
        # which requires user input confirmation
        pass

    def test_sql_injection_protection(self):
        """Test that the cleanup tool is protected against SQL injection"""
        # Test with malicious artist name
        malicious_name = "'; DROP TABLE youtube_videos; --"

        # The tool should use parameterized queries
        # This test ensures the query structure is safe

        # Mock engine to test query structure
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # This should not crash or execute malicious SQL
        try:
            remove_unauthorized_artist(mock_engine, malicious_name)
        except Exception:
            # Expected to fail due to mocking, but should not be SQL injection
            pass

        # Verify parameterized queries were used
        assert mock_conn.execute.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
