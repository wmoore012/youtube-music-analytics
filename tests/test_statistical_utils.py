"""
Tests for statistical utilities including Wilson intervals, Bayesian shrinkage, and LOESS smoothing.
"""

import numpy as np
import pandas as pd
import pytest

from src.youtubeviz.statistical_utils import (
    apply_bayesian_shrinkage,
    apply_loess_smoothing,
    calculate_residuals,
    calculate_wilson_intervals,
    detect_needs_more_data,
    standardize_residuals,
)


class TestWilsonIntervals:
    """Test Wilson confidence interval calculations."""

    def test_wilson_intervals_basic(self):
        """Test basic Wilson interval calculation."""
        successes = np.array([8, 15, 3])
        totals = np.array([10, 20, 5])

        lower, upper = calculate_wilson_intervals(successes, totals)

        # Check that intervals are valid
        assert len(lower) == len(successes)
        assert len(upper) == len(successes)
        assert np.all(lower >= 0)
        assert np.all(upper <= 1)
        assert np.all(lower <= upper)

        # Check that observed proportions are within intervals
        proportions = successes / totals
        assert np.all(proportions >= lower)
        assert np.all(proportions <= upper)

    def test_wilson_intervals_edge_cases(self):
        """Test Wilson intervals with edge cases."""
        # Perfect success rate
        successes = np.array([10])
        totals = np.array([10])
        lower, upper = calculate_wilson_intervals(successes, totals)
        assert lower[0] < 1.0  # Should not be exactly 1
        assert upper[0] >= 0.99  # Should be very close to 1

        # Zero success rate
        successes = np.array([0])
        totals = np.array([10])
        lower, upper = calculate_wilson_intervals(successes, totals)
        assert lower[0] == 0.0
        assert upper[0] > 0.0  # Should not be exactly 0

    def test_wilson_intervals_small_samples(self):
        """Test that Wilson intervals are wider for small samples."""
        # Small sample
        successes_small = np.array([1])
        totals_small = np.array([2])
        lower_small, upper_small = calculate_wilson_intervals(successes_small, totals_small)

        # Large sample with same proportion
        successes_large = np.array([50])
        totals_large = np.array([100])
        lower_large, upper_large = calculate_wilson_intervals(successes_large, totals_large)

        # Small sample should have wider interval
        width_small = upper_small[0] - lower_small[0]
        width_large = upper_large[0] - lower_large[0]
        assert width_small > width_large


class TestBayesianShrinkage:
    """Test Bayesian shrinkage for rate stabilization."""

    def test_bayesian_shrinkage_basic(self):
        """Test basic Bayesian shrinkage functionality."""
        # Create test data with extreme rates for small samples
        rates = pd.Series([0.9, 0.1, 0.8], index=["Artist A", "Artist B", "Artist C"])
        totals = pd.Series([5, 3, 20], index=["Artist A", "Artist B", "Artist C"])

        shrunken = apply_bayesian_shrinkage(rates, totals)

        # Check that extreme rates are pulled toward center
        overall_rate = (rates * totals).sum() / totals.sum()

        # Artist B (extreme low rate, small sample) should be pulled up
        assert shrunken["Artist B"] > rates["Artist B"]
        assert shrunken["Artist B"] < overall_rate

        # Artist C (large sample) should be less affected
        assert abs(shrunken["Artist C"] - rates["Artist C"]) < abs(shrunken["Artist B"] - rates["Artist B"])

    def test_bayesian_shrinkage_min_observations(self):
        """Test that shrinkage only applies to small samples."""
        rates = pd.Series([0.9, 0.8], index=["Artist A", "Artist B"])
        totals = pd.Series([25, 5], index=["Artist A", "Artist B"])  # One above, one below threshold

        shrunken = apply_bayesian_shrinkage(rates, totals, min_observations=20)

        # Artist A (above threshold) should be unchanged
        assert shrunken["Artist A"] == rates["Artist A"]

        # Artist B (below threshold) should be shrunken
        assert shrunken["Artist B"] != rates["Artist B"]


class TestLoessSmoothing:
    """Test LOESS smoothing functionality."""

    def test_loess_smoothing_basic(self):
        """Test basic LOESS smoothing."""
        # Create test data with trend + noise (fixed seed for reproducible test)
        np.random.seed(42)
        x = np.linspace(0, 10, 20)
        y_true = 0.5 * x + 2
        noise = np.random.normal(0, 0.2, len(x))  # Increased noise for more obvious smoothing effect
        y = y_true + noise

        result = apply_loess_smoothing(x, y)

        # Check that result contains expected keys
        assert "x_smooth" in result
        assert "y_smooth" in result
        assert "lower" in result
        assert "upper" in result

        # Check that we get reasonable smoothed values
        assert len(result["x_smooth"]) == len(x)
        assert len(result["y_smooth"]) == len(x)

        # Check that confidence bands make sense
        assert np.all(result["lower"] <= result["y_smooth"])
        assert np.all(result["y_smooth"] <= result["upper"])

    def test_loess_confidence_bands(self):
        """Test that confidence bands are reasonable."""
        x = np.array([1, 2, 3, 4, 5])
        y = np.array([1, 2, 3, 4, 5])  # Perfect linear relationship

        result = apply_loess_smoothing(x, y, return_confidence_bands=True)

        # Confidence bands should bracket the smooth line
        assert np.all(result["lower"] <= result["y_smooth"])
        assert np.all(result["y_smooth"] <= result["upper"])


class TestUtilityFunctions:
    """Test utility functions for data quality and residual analysis."""

    def test_detect_needs_more_data(self):
        """Test detection of insufficient data."""
        totals = np.array([5, 15, 25, 35])
        needs_more = detect_needs_more_data(totals, min_threshold=20)

        expected = np.array([True, True, False, False])
        np.testing.assert_array_equal(needs_more, expected)

    def test_calculate_residuals(self):
        """Test residual calculation."""
        observed = np.array([1, 2, 3, 4, 5])
        predicted = np.array([1.1, 1.9, 3.1, 3.8, 5.2])

        residuals = calculate_residuals(observed, predicted)
        expected = np.array([-0.1, 0.1, -0.1, 0.2, -0.2])

        np.testing.assert_array_almost_equal(residuals, expected)

    def test_standardize_residuals(self):
        """Test residual standardization."""
        residuals = np.array([-2, -1, 0, 1, 2])
        standardized = standardize_residuals(residuals)

        # Standardized residuals should have mean ~0 and std ~1
        assert abs(np.mean(standardized)) < 1e - 10
        assert abs(np.std(standardized) - 1.0) < 1e - 10


class TestIntegrationScenarios:
    """Test realistic scenarios combining multiple statistical methods."""

    def test_new_artist_analysis_pipeline(self):
        """Test complete pipeline for new artist with limited data."""
        # Simulate new artist data
        artist_data = {
            "artist_name": ["New Artist"] * 10,
            "positive_comments": [2, 1, 3, 0, 4, 1, 2, 3, 1, 2],
            "total_comments": [3, 2, 5, 1, 6, 2, 4, 5, 2, 3],
            "views": [100, 200, 150, 80, 300, 120, 180, 250, 90, 200],
        }

        df = pd.DataFrame(artist_data)

        # Calculate positive rates
        positive_rates = df["positive_comments"] / df["total_comments"]

        # Apply Wilson intervals
        lower, upper = calculate_wilson_intervals(df["positive_comments"], df["total_comments"])

        # Apply Bayesian shrinkage
        shrunken_rates = apply_bayesian_shrinkage(positive_rates, df["total_comments"])

        # Check that all methods produce reasonable results
        assert len(lower) == len(df)
        assert len(upper) == len(df)
        assert len(shrunken_rates) == len(df)

        # Verify uncertainty handling
        needs_more = detect_needs_more_data(df["total_comments"], min_threshold=5)
        assert np.any(needs_more)  # Should flag some low - comment videos

    def test_standout_video_detection(self):
        """Test pipeline for detecting standout videos using residual analysis."""
        # Simulate video performance data
        np.random.seed(42)  # For reproducible results
        n_videos = 50

        log_views = np.random.uniform(2, 5, n_videos)  # Log10 of views (100 to 100k)

        # Create trend: higher views generally correlate with positive sentiment
        true_trend = 0.3 + 0.1 * log_views
        noise = np.random.normal(0, 0.05, n_videos)
        positive_rates = np.clip(true_trend + noise, 0, 1)

        # Add a few standout videos (high positive rate for their view count)
        standout_indices = [10, 25, 40]
        positive_rates[standout_indices] += 0.2
        positive_rates = np.clip(positive_rates, 0, 1)

        # Apply LOESS smoothing to detect trend
        smooth_result = apply_loess_smoothing(log_views, positive_rates)

        # Calculate residuals
        predicted = np.interp(log_views, smooth_result["x_smooth"], smooth_result["y_smooth"])
        residuals = calculate_residuals(positive_rates, predicted)
        standardized_residuals = standardize_residuals(residuals)

        # Standout videos should have high positive residuals
        for idx in standout_indices:
            assert standardized_residuals[idx] > 1.0  # Above average performance


if __name__ == "__main__":
    pytest.main([__file__])
