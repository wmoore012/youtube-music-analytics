"""
Tests for storytelling error handling and data validation functions.

This module tests the comprehensive error handling system that provides
educational error messages and graceful degradation for missing data scenarios.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from src.youtubeviz.storytelling import (
    DataQualityWarning,
    StorytellingDataError,
    create_confidence_indicator,
    create_error_recovery_suggestions,
    generate_data_quality_report,
    handle_missing_data_gracefully,
    validate_data_for_storytelling,
)


class TestDataValidation:
    """Test data validation functions for storytelling analysis."""

    def test_validate_empty_data(self):
        """Test validation with empty DataFrame."""
        empty_df = pd.DataFrame()

        with pytest.raises(StorytellingDataError):
            validate_data_for_storytelling(
                empty_df, required_columns=["artist_name", "view_count"], analysis_type="artist_comparison"
            )

    def test_validate_none_data(self):
        """Test validation with None data."""
        with pytest.raises(StorytellingDataError):
            validate_data_for_storytelling(
                None, required_columns=["artist_name", "view_count"], analysis_type="artist_comparison"
            )

    def test_validate_missing_columns(self):
        """Test validation with missing required columns."""
        df = pd.DataFrame({"artist_name": ["Artist A", "Artist B"], "title": ["Song 1", "Song 2"]})

        with pytest.raises(StorytellingDataError):
            validate_data_for_storytelling(
                df, required_columns=["artist_name", "view_count", "like_count"], analysis_type="artist_comparison"
            )

    def test_validate_good_data(self):
        """Test validation with good quality data."""
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B", "Artist C"],
                "view_count": [1000, 2000, 1500],
                "like_count": [100, 200, 150],
                "published_at": pd.date_range("2024-01-01", periods=3),
            }
        )

        result = validate_data_for_storytelling(
            df,
            required_columns=["artist_name", "view_count", "like_count"],
            analysis_type="artist_comparison",
            min_rows=3,
        )

        assert result["is_valid"] is True
        assert result["confidence_score"] == 1.0
        assert len(result["errors"]) == 0

    def test_validate_insufficient_rows(self):
        """Test validation with insufficient data rows."""
        df = pd.DataFrame({"artist_name": ["Artist A"], "view_count": [1000], "like_count": [100]})

        result = validate_data_for_storytelling(
            df,
            required_columns=["artist_name", "view_count", "like_count"],
            analysis_type="artist_comparison",
            min_rows=5,
        )

        assert result["is_valid"] is True  # Still valid, just warning
        assert result["confidence_score"] < 1.0
        assert len(result["warnings"]) > 0
        assert "Limited Data Warning" in result["warnings"][0]

    def test_validate_high_null_values(self):
        """Test validation with high percentage of null values."""
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B", "Artist C", "Artist D"],
                "view_count": [1000, None, None, None],  # 75% null
                "like_count": [100, 200, 150, 180],
            }
        )

        result = validate_data_for_storytelling(
            df, required_columns=["artist_name", "view_count", "like_count"], analysis_type="artist_comparison"
        )

        assert result["is_valid"] is True
        assert result["confidence_score"] < 1.0
        assert len(result["warnings"]) > 0
        assert "High Missing Data" in result["warnings"][0]
        assert "high_nulls_view_count" in result["data_quality_issues"]

    def test_validate_artist_comparison_single_artist(self):
        """Test artist comparison validation with single artist."""
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist A", "Artist A"],
                "view_count": [1000, 2000, 1500],
                "like_count": [100, 200, 150],
            }
        )

        result = validate_data_for_storytelling(
            df, required_columns=["artist_name", "view_count", "like_count"], analysis_type="artist_comparison"
        )

        assert result["is_valid"] is True
        assert result["confidence_score"] < 1.0
        assert any("Single Artist Detected" in warning for warning in result["warnings"])

    def test_validate_sentiment_analysis_low_comments(self):
        """Test sentiment analysis validation with low comment count."""
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B"],
                "comment_count": [10, 20],  # Total 30 comments < 100 threshold
                "view_count": [1000, 2000],
            }
        )

        result = validate_data_for_storytelling(
            df, required_columns=["artist_name", "comment_count"], analysis_type="sentiment_analysis"
        )

        assert result["is_valid"] is True
        assert result["confidence_score"] < 1.0
        assert any("Limited Comment Data" in warning for warning in result["warnings"])


class TestConfidenceIndicator:
    """Test confidence indicator generation."""

    def test_high_confidence_indicator(self):
        """Test high confidence indicator generation."""
        indicator = create_confidence_indicator(
            confidence_score=0.95, data_quality_issues=[], analysis_type="artist comparison"
        )

        assert "🟢" in indicator
        assert "High" in indicator
        assert "highly reliable" in indicator
        assert "#28a745" in indicator  # Green color

    def test_medium_confidence_indicator(self):
        """Test medium confidence indicator generation."""
        indicator = create_confidence_indicator(
            confidence_score=0.75, data_quality_issues=["limited_data"], analysis_type="sentiment analysis"
        )

        assert "🟡" in indicator
        assert "Medium" in indicator
        assert "generally reliable" in indicator
        assert "Limited data volume" in indicator

    def test_low_confidence_indicator(self):
        """Test low confidence indicator generation."""
        indicator = create_confidence_indicator(
            confidence_score=0.6,
            data_quality_issues=["high_nulls_view_count", "single_artist"],
            analysis_type="trend analysis",
        )

        assert "🟠" in indicator
        assert "Low" in indicator
        assert "with caution" in indicator
        assert "High missing data" in indicator
        assert "Single artist only" in indicator

    def test_very_low_confidence_indicator(self):
        """Test very low confidence indicator generation."""
        indicator = create_confidence_indicator(
            confidence_score=0.3, data_quality_issues=["high_nulls", "date_issues"], analysis_type="analysis"
        )

        assert "🔴" in indicator
        assert "Very Low" in indicator
        assert "exploration only" in indicator


class TestMissingDataHandling:
    """Test missing data handling functions."""

    def test_handle_missing_data_exclude_strategy(self):
        """Test exclude strategy for missing data."""
        df = pd.DataFrame({"artist_name": ["Artist A", "Artist B", "Artist C"], "view_count": [1000, None, 1500]})

        cleaned_df, explanation = handle_missing_data_gracefully(df, "view_count", "exclude", "artist comparison")

        assert len(cleaned_df) == 2  # One row excluded
        assert cleaned_df["view_count"].isnull().sum() == 0
        assert "Excluding rows" in explanation
        assert "complete data gives more reliable results" in explanation

    def test_handle_missing_data_fill_zero_strategy(self):
        """Test fill zero strategy for missing data."""
        df = pd.DataFrame({"artist_name": ["Artist A", "Artist B", "Artist C"], "engagement_rate": [5.2, None, 3.8]})

        cleaned_df, explanation = handle_missing_data_gracefully(
            df, "engagement_rate", "fill_zero", "engagement analysis"
        )

        assert len(cleaned_df) == 3  # No rows excluded
        assert cleaned_df["engagement_rate"].isnull().sum() == 0
        assert cleaned_df.loc[1, "engagement_rate"] == 0
        assert "Replacing missing" in explanation
        assert "zero activity" in explanation

    def test_handle_missing_data_fill_mean_strategy(self):
        """Test fill mean strategy for missing data."""
        df = pd.DataFrame({"artist_name": ["Artist A", "Artist B", "Artist C"], "view_count": [1000, None, 2000]})

        cleaned_df, explanation = handle_missing_data_gracefully(df, "view_count", "fill_mean", "trend analysis")

        assert len(cleaned_df) == 3  # No rows excluded
        assert cleaned_df["view_count"].isnull().sum() == 0
        assert cleaned_df.loc[1, "view_count"] == 1500  # Mean of 1000 and 2000
        assert "average" in explanation
        assert "statistical properties" in explanation

    def test_handle_missing_data_no_missing_values(self):
        """Test handling when no missing values exist."""
        df = pd.DataFrame({"artist_name": ["Artist A", "Artist B"], "view_count": [1000, 2000]})

        cleaned_df, explanation = handle_missing_data_gracefully(df, "view_count", "exclude")

        assert cleaned_df.equals(df)
        assert explanation == ""

    def test_handle_missing_data_column_not_found(self):
        """Test handling when column doesn't exist."""
        df = pd.DataFrame({"artist_name": ["Artist A", "Artist B"]})

        cleaned_df, explanation = handle_missing_data_gracefully(df, "nonexistent_column", "exclude")

        assert cleaned_df.equals(df)
        assert "not found" in explanation


class TestDataQualityReport:
    """Test data quality report generation."""

    def test_generate_quality_report_good_data(self):
        """Test quality report with good data."""
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B", "Artist C"],
                "view_count": [1000, 2000, 1500],
                "like_count": [100, 200, 150],
                "published_at": pd.date_range("2024-01-01", periods=3),
            }
        )

        report = generate_data_quality_report(df, "artist_comparison")

        assert "Data Quality Report" in report
        assert "Total records: 3" in report
        assert "Columns: 4" in report
        assert "No missing data detected" in report
        assert "Numeric Data Summary" in report

    def test_generate_quality_report_missing_data(self):
        """Test quality report with missing data."""
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B", None],
                "view_count": [1000, None, 1500],
                "like_count": [100, 200, 150],
            }
        )

        report = generate_data_quality_report(df, "sentiment_analysis")

        assert "Missing Data Analysis" in report
        assert "artist_name: 1 missing" in report
        assert "view_count: 1 missing" in report
        assert "🔴" in report  # Status indicators (33.3% missing > 20% threshold)

    def test_generate_quality_report_empty_data(self):
        """Test quality report with empty data."""
        report = generate_data_quality_report(None, "analysis")

        assert "No data available" in report

    def test_generate_quality_report_recommendations(self):
        """Test that recommendations are included in report."""
        # Small dataset to trigger recommendations
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A"] * 50,  # Small but not tiny
                "view_count": [None] * 25 + [1000] * 25,  # 50% missing data
            }
        )

        report = generate_data_quality_report(df, "trend_analysis")

        assert "Recommendations" in report
        assert "data cleaning" in report or "imputation" in report


class TestErrorRecoverySuggestions:
    """Test error recovery suggestion generation."""

    def test_no_data_error_suggestions(self):
        """Test suggestions for no data error."""
        suggestions = create_error_recovery_suggestions(
            "no_data", {"date_range": "2024-01-01 to 2024-01-07", "artists": ["Artist A"]}
        )

        assert "No Data Found" in suggestions
        assert "date range" in suggestions
        assert "artist names" in suggestions
        assert "database connection" in suggestions
        assert "Context:" in suggestions
        assert "date_range: 2024-01-01 to 2024-01-07" in suggestions

    def test_insufficient_data_error_suggestions(self):
        """Test suggestions for insufficient data error."""
        suggestions = create_error_recovery_suggestions("insufficient_data")

        assert "Insufficient Data" in suggestions
        assert "statistical significance" in suggestions
        assert "Expand your date range" in suggestions
        assert "30 data points" in suggestions

    def test_data_quality_error_suggestions(self):
        """Test suggestions for data quality issues."""
        suggestions = create_error_recovery_suggestions("data_quality")

        assert "Data Quality Issues" in suggestions
        assert "quality report" in suggestions
        assert "confidence indicators" in suggestions
        assert "real-world analytics" in suggestions

    def test_calculation_error_suggestions(self):
        """Test suggestions for calculation errors."""
        suggestions = create_error_recovery_suggestions("calculation_error")

        assert "Calculation Error" in suggestions
        assert "division by zero" in suggestions
        assert "data types" in suggestions
        assert "edge cases" in suggestions

    def test_unknown_error_suggestions(self):
        """Test suggestions for unknown error types."""
        suggestions = create_error_recovery_suggestions("unknown_error_type")

        assert "Unknown Error Type" in suggestions
        assert "check the logs" in suggestions


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple error handling features."""

    def test_complete_validation_workflow(self):
        """Test complete validation workflow with various data issues."""
        # Create data with multiple issues
        df = pd.DataFrame(
            {
                "artist_name": ["Artist A"] * 10,  # Single artist (warning)
                "view_count": [1000, None, None, 1500, 2000, None, 1800, 1200, None, 1600],  # 40% missing
                "like_count": [100, 200, 150, 180, 220, 160, 190, 140, 170, 200],
                "comment_count": [5, 8, 3, 7, 12, 4, 9, 6, 2, 10],  # Total 66 < 100 threshold
                "published_at": pd.date_range("2024-01-01", periods=10),
            }
        )

        # Validate data
        validation_result = validate_data_for_storytelling(
            df,
            required_columns=["artist_name", "view_count", "like_count", "comment_count"],
            analysis_type="artist_comparison",
            min_rows=5,
        )

        # Should be valid but with warnings and reduced confidence
        assert validation_result["is_valid"] is True
        assert validation_result["confidence_score"] < 0.8  # Multiple issues reduce confidence
        assert len(validation_result["warnings"]) >= 2  # Single artist + missing data warnings

        # Handle missing data
        cleaned_df, explanation = handle_missing_data_gracefully(df, "view_count", "fill_mean", "artist comparison")

        assert cleaned_df["view_count"].isnull().sum() == 0
        assert "average" in explanation

        # Generate confidence indicator
        confidence_indicator = create_confidence_indicator(
            validation_result["confidence_score"], validation_result["data_quality_issues"], "artist comparison"
        )

        assert "🟡" in confidence_indicator or "🟠" in confidence_indicator  # Medium/Low confidence

        # Generate quality report
        quality_report = generate_data_quality_report(cleaned_df, "artist_comparison")

        assert "Data Quality Report" in quality_report
        assert "Total records: 10" in quality_report

    def test_error_recovery_workflow(self):
        """Test error recovery workflow for common scenarios."""
        # Test empty data scenario
        try:
            validate_data_for_storytelling(
                pd.DataFrame(), required_columns=["artist_name"], analysis_type="trend_analysis"
            )
        except StorytellingDataError:
            # Generate recovery suggestions
            suggestions = create_error_recovery_suggestions(
                "no_data", {"analysis_type": "trend_analysis", "filters_applied": "date_range, artist_filter"}
            )

            assert "No Data Found" in suggestions
            assert "analysis_type: trend_analysis" in suggestions
