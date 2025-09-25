"""
Tests for configuration validation utilities.
"""

import os
from unittest.mock import mock_open, patch

import pandas as pd
import pytest

from src.youtubeviz.config_validation import (
    format_artist_name,
    get_artists_from_env,
    get_expected_artist_count,
    print_validation_results,
    validate_artist_count_in_data,
)


class TestConfigValidation:

    def test_format_artist_name(self):
        """Test artist name formatting from env variables."""
        assert format_artist_name("BICFIZZLE") == "BiC Fizzle"
        assert format_artist_name("COBRAH") == "COBRAH"
        assert format_artist_name("FLYANABOSS") == "Flyana Boss"
        assert format_artist_name("RE6CE") == "re6ce"
        assert format_artist_name("UNKNOWN") == "Unknown"

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="""
# Test .env file
YT_BICFIZZLE_YT=https://youtube.com/@BicFizzle
YT_COBRAH_YT=https://www.youtube.com/@COBRAH
YT_FLYANABOSS_YT=https://www.youtube.com/@FlyanaBoss
# Comment line
OTHER_CONFIG=value
""",
    )
    @patch("os.path.exists", return_value=True)
    def test_get_artists_from_env(self, mock_exists, mock_file):
        """Test extracting artists from .env file."""
        artists, count = get_artists_from_env()

        assert count == 3
        assert "BiC Fizzle" in artists
        assert "COBRAH" in artists
        assert "Flyana Boss" in artists
        assert len(artists) == 3

    def test_validate_artist_count_valid_data(self):
        """Test validation with correct artist data."""
        df = pd.DataFrame(
            {"artist_name": ["BiC Fizzle", "COBRAH", "Flyana Boss", "BiC Fizzle"], "views": [1000, 2000, 3000, 1500]}
        )

        with patch(
            "src.youtubeviz.config_validation.get_artists_from_env",
            return_value=(["BiC Fizzle", "COBRAH", "Flyana Boss"], 3),
        ):
            result = validate_artist_count_in_data(df)

            assert result["valid"] is True
            assert result["count_match"] is True
            assert result["names_match"] is True
            assert result["expected_count"] == 3
            assert result["actual_count"] == 3

    def test_validate_artist_count_missing_artists(self):
        """Test validation with missing artists."""
        df = pd.DataFrame({"artist_name": ["BiC Fizzle", "COBRAH"], "views": [1000, 2000]})

        with patch(
            "src.youtubeviz.config_validation.get_artists_from_env",
            return_value=(["BiC Fizzle", "COBRAH", "Flyana Boss"], 3),
        ):
            result = validate_artist_count_in_data(df)

            assert result["valid"] is False
            assert result["count_match"] is False
            assert result["expected_count"] == 3
            assert result["actual_count"] == 2
            assert "Flyana Boss" in result["missing_artists"]

    def test_validate_artist_count_extra_artists(self):
        """Test validation with extra artists."""
        df = pd.DataFrame(
            {"artist_name": ["BiC Fizzle", "COBRAH", "Flyana Boss", "Unknown Artist"], "views": [1000, 2000, 3000, 500]}
        )

        with patch(
            "src.youtubeviz.config_validation.get_artists_from_env",
            return_value=(["BiC Fizzle", "COBRAH", "Flyana Boss"], 3),
        ):
            result = validate_artist_count_in_data(df)

            assert result["valid"] is False
            assert result["count_match"] is False
            assert result["expected_count"] == 3
            assert result["actual_count"] == 4
            assert "Unknown Artist" in result["extra_artists"]

    def test_validate_artist_count_missing_column(self):
        """Test validation with missing artist column."""
        df = pd.DataFrame({"views": [1000, 2000, 3000]})

        result = validate_artist_count_in_data(df, "artist_name")

        assert result["valid"] is False
        assert "not found in data" in result["error"]

    def test_print_validation_results_success(self, capsys):
        """Test printing successful validation results."""
        result = {"valid": True, "actual_count": 3, "actual_artists": ["BiC Fizzle", "COBRAH", "Flyana Boss"]}

        print_validation_results(result, loud=False)
        captured = capsys.readouterr()

        assert "✅ Artist validation PASSED" in captured.out
        assert "3 artists found" in captured.out

    def test_print_validation_results_failure_loud(self, capsys):
        """Test printing failed validation with loud warnings."""
        result = {
            "valid": False,
            "count_match": False,
            "expected_count": 3,
            "actual_count": 2,
            "expected_artists": ["BiC Fizzle", "COBRAH", "Flyana Boss"],
            "actual_artists": ["BiC Fizzle", "COBRAH"],
            "missing_artists": ["Flyana Boss"],
            "extra_artists": [],
        }

        print_validation_results(result, loud=True)
        captured = capsys.readouterr()

        assert "🚨 LOUD WARNING" in captured.out
        assert "ARTIST COUNT MISMATCH" in captured.out
        assert "Expected 3 artists, found 2" in captured.out
        assert "Missing:  ['Flyana Boss']" in captured.out
        assert "RECOMMENDED ACTIONS" in captured.out


if __name__ == "__main__":
    pytest.main([__file__])
