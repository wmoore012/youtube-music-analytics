"""
Tests for data consistency across different functions.

These tests catch bugs where different functions return inconsistent
artist counts for the same underlying data.
"""

import pytest

from src.icatalogviz.data import qa_artist_consistency_check
from web.etl_helpers import get_engine


class TestDataConsistency:
    """Test data consistency across all analytics functions."""

    @pytest.fixture
    def engine(self):
        """Database engine fixture."""
        return get_engine()

    def test_artist_consistency_check_basic(self, engine):
        """Test that artist consistency check runs without errors."""
        result = qa_artist_consistency_check(days=30, engine=engine)

        # Should always return a result
        assert isinstance(result, dict)
        assert "status" in result
        assert "consistent" in result

        # Status should be success, no_data, or error
        assert result["status"] in ["success", "no_data", "error"]

    def test_artist_consistency_with_data(self, engine):
        """Test artist consistency when data exists."""
        result = qa_artist_consistency_check(days=90, engine=engine)  # Longer window

        if result["status"] == "success":
            # All count fields should be present
            required_fields = ["data_artists", "kpi_artists", "sentiment_artists", "color_artists", "revenue_artists"]
            for field in required_fields:
                assert field in result
                assert isinstance(result[field], int)
                assert result[field] >= 0

            # If we have data, counts should be consistent
            if result["data_artists"] > 0:
                assert result["consistent"] is True, f"Artist counts inconsistent: {result['message']}"

                # All counts should match the base data count
                base_count = result["data_artists"]
                assert result["kpi_artists"] == base_count
                assert result["color_artists"] == base_count
                assert result["revenue_artists"] == base_count
                # Sentiment might be different if no sentiment data exists

        elif result["status"] == "no_data":
            # No data case is acceptable
            assert result["data_artists"] == 0
            assert result["consistent"] is True

        else:  # error status
            # Error case should have message
            assert "message" in result
            assert len(result["message"]) > 0

    def test_artist_consistency_short_window(self, engine):
        """Test consistency with a short time window."""
        result = qa_artist_consistency_check(days=7, engine=engine)

        # Should handle short windows gracefully
        assert result["status"] in ["success", "no_data", "error"]

        if result["status"] == "success" and result["data_artists"] > 0:
            # Even with limited data, counts should be consistent
            assert result["consistent"] is True

    def test_artist_consistency_edge_cases(self, engine):
        """Test consistency check with edge case parameters."""
        # Test with 1 day (minimal window)
        result_1day = qa_artist_consistency_check(days=1, engine=engine)
        assert isinstance(result_1day, dict)

        # Test with large window
        result_large = qa_artist_consistency_check(days=365, engine=engine)
        assert isinstance(result_large, dict)

        # Both should be consistent if they have data
        for result in [result_1day, result_large]:
            if result["status"] == "success" and result["data_artists"] > 0:
                assert result["consistent"] is True, f"Inconsistency found: {result['message']}"


class TestConsistencyIntegration:
    """Integration tests for consistency across the full pipeline."""

    @pytest.fixture
    def engine(self):
        """Database engine fixture."""
        return get_engine()

    def test_full_pipeline_consistency(self, engine):
        """Test that the full analytics pipeline maintains consistency."""
        from datetime import date, timedelta

        from src.icatalogviz.charts import get_artist_color_map
        from src.icatalogviz.data import (
            compute_estimated_revenue,
            compute_kpis,
            load_recent_window_days,
            load_sentiment_daily,
        )

        # Load data the same way notebooks would
        recent_data = load_recent_window_days(days=30, engine=engine)

        if len(recent_data) > 0:
            # Get unique artists
            unique_artists = recent_data["artist_name"].dropna().unique()

            # Test each function individually
            kpis = compute_kpis(recent_data)
            colors = get_artist_color_map(unique_artists)  # Correct usage
            revenue = compute_estimated_revenue(recent_data, rpm_usd=1.0)

            # All should have same artist count
            assert len(unique_artists) == len(kpis), "KPI artist count mismatch"
            assert len(unique_artists) == len(colors), "Color mapping artist count mismatch"
            assert len(unique_artists) == len(revenue), "Revenue artist count mismatch"

            # Test sentiment separately (might have different count)
            end_date = date.today()
            start_date = end_date - timedelta(days=30)
            sentiment_data = load_sentiment_daily(start=start_date, end=end_date, engine=engine)

            if len(sentiment_data) > 0:
                sentiment_artists = sentiment_data["artist_name"].nunique()
                # Sentiment artists should be subset of or equal to main artists
                assert sentiment_artists <= len(unique_artists), "More sentiment artists than data artists"

    def test_consistency_check_catches_bugs(self, engine):
        """Test that the consistency check would catch the color mapping bug we found."""
        from src.icatalogviz.charts import get_artist_color_map
        from src.icatalogviz.data import load_recent_window_days

        recent_data = load_recent_window_days(days=30, engine=engine)

        if len(recent_data) > 0:
            unique_artists = recent_data["artist_name"].dropna().unique()

            # Correct usage
            colors_correct = get_artist_color_map(unique_artists)
            assert len(colors_correct) == len(unique_artists)

            # Incorrect usage (what we had before) - this should fail
            try:
                colors_wrong = get_artist_color_map(recent_data)  # Passing DataFrame instead of artist list
                # If this doesn't fail, it means the function is too permissive
                # The consistency check should catch this
                if len(colors_wrong) != len(unique_artists):
                    pytest.fail("get_artist_color_map should reject DataFrame input or return consistent count")
            except (TypeError, AttributeError):
                # This is expected - function should reject wrong input type
                pass
