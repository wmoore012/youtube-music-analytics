"""
Tests for notebook artist validation functionality.
Ensures notebooks properly validate artist counts and throw loud warnings.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.youtubeviz.config_validation import (
    get_artists_from_env,
    print_validation_results,
    validate_artist_count_in_data,
    validate_database_artist_count,
)


class TestNotebookArtistValidation:

    def test_notebook_should_have_6_artists_from_env(self):
        """Test that .env configuration returns exactly 6 artists."""
        artists, count = get_artists_from_env()

        # This should match the actual .env file
        assert count == 6, f"Expected 6 artists in .env, found {count}: {artists}"

        expected_artists = ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"]
        for expected in expected_artists:
            assert expected in artists, f"Expected artist '{expected}' not found in .env configuration"

    def test_notebook_validation_passes_with_correct_data(self):
        """Test notebook validation passes with 6 artists."""
        # Create sample data with all 6 artists
        df = pd.DataFrame(
            {
                "artist_name": ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"] * 5,
                "views": [1000, 2000, 3000, 4000, 5000, 6000] * 5,
                "date": pd.date_range("2024-01-01", periods=30, freq="D"),
            }
        )

        result = validate_artist_count_in_data(df, "artist_name")

        assert result["valid"] is True
        assert result["expected_count"] == 6
        assert result["actual_count"] == 6
        assert len(result["missing_artists"]) == 0
        assert len(result["extra_artists"]) == 0

    def test_notebook_validation_fails_with_missing_artists(self):
        """Test notebook validation fails loudly with missing artists."""
        # Create sample data with only 4 artists (missing 2)
        df = pd.DataFrame(
            {
                "artist_name": ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss"] * 5,
                "views": [1000, 2000, 3000, 4000] * 5,
            }
        )

        result = validate_artist_count_in_data(df, "artist_name")

        assert result["valid"] is False
        assert result["expected_count"] == 6
        assert result["actual_count"] == 4
        assert len(result["missing_artists"]) == 2
        assert "Raiche" in result["missing_artists"]
        assert "re6ce" in result["missing_artists"]

    def test_notebook_validation_fails_with_extra_artists(self):
        """Test notebook validation fails with unexpected artists."""
        # Create sample data with extra artists
        df = pd.DataFrame(
            {
                "artist_name": ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce", "Unknown Artist"]
                * 3,
                "views": [1000, 2000, 3000, 4000, 5000, 6000, 7000] * 3,
            }
        )

        result = validate_artist_count_in_data(df, "artist_name")

        assert result["valid"] is False
        assert result["expected_count"] == 6
        assert result["actual_count"] == 7
        assert len(result["extra_artists"]) == 1
        assert "Unknown Artist" in result["extra_artists"]

    def test_loud_warning_output_for_mismatch(self, capsys):
        """Test that loud warnings are properly displayed for mismatches."""
        result = {
            "valid": False,
            "count_match": False,
            "expected_count": 6,
            "actual_count": 4,
            "expected_artists": ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"],
            "actual_artists": ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss"],
            "missing_artists": ["Raiche", "re6ce"],
            "extra_artists": [],
        }

        print_validation_results(result, loud=True)
        captured = capsys.readouterr()

        # Check for loud warning elements
        assert "🚨 LOUD WARNING" in captured.out
        assert "ARTIST COUNT MISMATCH DETECTED" in captured.out
        assert "Expected 6 artists, found 4" in captured.out
        assert "Missing:  ['Raiche', 're6ce']" in captured.out
        assert "RECOMMENDED ACTIONS" in captured.out
        assert "Run ETL pipeline" in captured.out

    def test_database_validation_with_mock_engine(self):
        """Test database validation with mocked database connection."""
        # Mock database response with correct artists
        mock_df = pd.DataFrame({"artist_name": ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"]})

        with patch("pandas.read_sql", return_value=mock_df):
            mock_engine = MagicMock()
            result = validate_database_artist_count(mock_engine)

            assert result["valid"] is True
            assert result["expected_count"] == 6
            assert result["actual_count"] == 6

    def test_database_validation_with_missing_data(self):
        """Test database validation when no data exists."""
        # Mock empty database response
        mock_df = pd.DataFrame({"artist_name": []})

        with patch("pandas.read_sql", return_value=mock_df):
            mock_engine = MagicMock()
            result = validate_database_artist_count(mock_engine)

            assert result["valid"] is False
            assert result["expected_count"] == 6
            assert result["actual_count"] == 0

    def test_database_validation_connection_error(self):
        """Test database validation handles connection errors gracefully."""
        with patch("pandas.read_sql", side_effect=Exception("Connection failed")):
            mock_engine = MagicMock()
            result = validate_database_artist_count(mock_engine)

            assert result["valid"] is False
            assert "Database validation failed" in result["error"]
            assert result["expected_count"] == 6
            assert result["actual_count"] == 0

    def test_notebook_should_stop_execution_on_validation_failure(self):
        """Test that notebook should raise ValueError on validation failure."""
        # This simulates what should happen in the notebook
        df = pd.DataFrame({"artist_name": ["BiC Fizzle", "COBRAH"], "views": [1000, 2000]})  # Only 2 artists

        result = validate_artist_count_in_data(df, "artist_name")

        # Notebook should check this and raise an error
        assert result["valid"] is False

        # This is what the notebook should do:
        with pytest.raises(ValueError, match="Artist validation failed"):
            if not result["valid"]:
                raise ValueError("Artist validation failed! Check .env configuration or run ETL pipeline.")


if __name__ == "__main__":
    pytest.main([__file__])
