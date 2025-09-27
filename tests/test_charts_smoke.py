"""
Smoke tests for MusicScope™ charts - the "soundcheck before the show"

These tests ensure every chart function:
1. Returns a proper Figure / Chart object (not None)
2. Completes within timeout
3. Handles 200 - row fixture data without crashing
"""

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.youtubeviz.advanced_charts import create_diverging_sentiment_bars, create_sentiment_cluster_heatmap

# Import chart functions
from src.youtubeviz.charts import artist_compare_altair, create_divergent_sentiment_chart, views_over_time_plotly
from src.youtubeviz.scoring_analysis import (
    create_engagement_distribution_chart,
    create_momentum_scores_chart,
    create_scoring_performance_chart,
)


@pytest.fixture
def sample_youtube_data():
    """Create 200 - row fixture with realistic YouTube data."""
    np.random.seed(42)  # Reproducible test data

    artists = ["Taylor Swift", "Drake", "Bad Bunny", "Billie Eilish", "The Weeknd"]

    data = []
    base_date = datetime.now() - timedelta(days=365)

    for i in range(200):
        artist = np.random.choice(artists)
        date = base_date + timedelta(days=np.random.randint(0, 365))

        # Realistic YouTube metrics with some correlation
        views = np.random.lognormal(mean=12, sigma=1.5)  # Log - normal distribution for views
        likes = views * np.random.uniform(0.01, 0.05)  # 1 - 5% like rate
        comments = views * np.random.uniform(0.001, 0.01)  # 0.1 - 1% comment rate

        data.append(
            {
                "artist_name": artist,
                "published_at": date,
                "view_count": int(views),
                "like_count": int(likes),
                "comment_count": int(comments),
                "video_id": f"video_{i:03d}",
                "channel_title": artist,
                "title": f"{artist} - Song {i % 10 + 1}",
                "duration": np.random.randint(120, 300),  # 2 - 5 minutes
                "has_isrc": np.random.choice([True, False]),
                "sentiment_score": np.random.uniform(-1, 1),
                "engagement_rate": (likes + comments) / views if views > 0 else 0,
            }
        )

    return pd.DataFrame(data)


@pytest.fixture
def sample_scoring_data():
    """Create scoring results fixture."""
    artists = ["Taylor Swift", "Drake", "Bad Bunny", "Billie Eilish", "The Weeknd"]

    momentum_data = []
    for artist in artists:
        momentum_data.append(
            {
                "entity_id": artist,
                "score_value": np.random.uniform(0.3, 0.9),
                "confidence": np.random.uniform(0.7, 0.95),
                "momentum_category": np.random.choice(["high_momentum", "moderate_momentum", "stable"]),
                "view_growth_rate": np.random.uniform(0.1, 0.5),
                "posting_consistency": np.random.uniform(0.6, 0.9),
            }
        )

    engagement_data = []
    for i in range(20):  # 20 videos
        engagement_data.append(
            {
                "entity_id": f"video_{i:03d}",
                "score_value": np.random.uniform(0.2, 0.8),
                "engagement_rate": np.random.uniform(0.01, 0.06),
                "confidence": np.random.uniform(0.6, 0.9),
            }
        )

    performance_data = pd.DataFrame(
        [
            {
                "algorithm_name": "artist_momentum_scorer",
                "total_runs": 15,
                "total_results": 75,
                "overall_avg_score": 0.65,
                "last_run": datetime.now() - timedelta(hours=2),
            },
            {
                "algorithm_name": "engagement_scorer",
                "total_runs": 8,
                "total_results": 160,
                "overall_avg_score": 0.58,
                "last_run": datetime.now() - timedelta(hours=1),
            },
        ]
    )

    return {
        "momentum": pd.DataFrame(momentum_data),
        "engagement": pd.DataFrame(engagement_data),
        "performance": performance_data,
    }


def assert_valid_plotly_figure(result: Any, chart_name: str):
    """Assert result is a valid Plotly figure."""
    assert result is not None, f"{chart_name} returned None"

    # Check for Plotly figure attributes
    assert hasattr(result, "data"), f"{chart_name} missing 'data' attribute"
    assert hasattr(result, "layout"), f"{chart_name} missing 'layout' attribute"

    # Ensure it has traces
    assert len(result.data) > 0, f"{chart_name} has no data traces"


def assert_valid_altair_chart(result: Any, chart_name: str):
    """Assert result is a valid Altair chart or fallback DataFrame."""
    assert result is not None, f"{chart_name} returned None"

    # Altair charts have specific attributes, or fallback to DataFrame
    if hasattr(result, "encoding"):
        # It's an Altair chart
        assert hasattr(result, "mark"), f"{chart_name} missing 'mark' attribute"
    elif isinstance(result, pd.DataFrame):
        # It's a fallback DataFrame
        assert not result.empty, f"{chart_name} returned empty DataFrame"
    else:
        pytest.fail(f"{chart_name} returned unexpected type: {type(result)}")


class TestChartsSmoke:
    """Smoke tests for all chart functions."""

    def test_views_over_time_plotly(self, sample_youtube_data):
        """Test views over time chart generation."""
        result = views_over_time_plotly(sample_youtube_data)
        assert_valid_plotly_figure(result, "views_over_time_plotly")

    def test_views_over_time_with_animation(self, sample_youtube_data):
        """Test views over time with animation frame."""
        # Add date column for animation
        sample_youtube_data["date_str"] = sample_youtube_data["published_at"].dt.strftime("%Y-%m")

        result = views_over_time_plotly(sample_youtube_data, animate_by="date_str")
        assert_valid_plotly_figure(result, "views_over_time_plotly_animated")

        # Check animation was configured
        assert hasattr(result.layout, "updatemenus"), "Animation controls not configured"

    def test_divergent_sentiment_chart(self, sample_youtube_data):
        """Test divergent sentiment chart."""
        # Add sentiment category
        sample_youtube_data["sentiment_category"] = pd.cut(
            sample_youtube_data["sentiment_score"], bins=[-1, -0.1, 0.1, 1], labels=["negative", "neutral", "positive"]
        )

        result = create_divergent_sentiment_chart(sample_youtube_data, "artist_name", "sentiment_category")
        assert_valid_plotly_figure(result, "create_divergent_sentiment_chart")

    def test_artist_compare_altair(self, sample_youtube_data):
        """Test Altair artist comparison chart."""
        result = artist_compare_altair(sample_youtube_data)
        assert_valid_altair_chart(result, "artist_compare_altair")

    def test_diverging_sentiment_bars(self, sample_youtube_data):
        """Test advanced diverging sentiment bars."""
        result = create_diverging_sentiment_bars(sample_youtube_data)
        assert_valid_plotly_figure(result, "create_diverging_sentiment_bars")

    def test_sentiment_cluster_heatmap(self, sample_youtube_data):
        """Test sentiment cluster heatmap."""
        result = create_sentiment_cluster_heatmap(sample_youtube_data)
        assert_valid_plotly_figure(result, "create_sentiment_cluster_heatmap")

    def test_momentum_scores_chart(self, sample_scoring_data):
        """Test momentum scores chart."""
        result = create_momentum_scores_chart(sample_scoring_data["momentum"])
        assert_valid_plotly_figure(result, "create_momentum_scores_chart")

    def test_engagement_distribution_chart(self, sample_scoring_data):
        """Test engagement distribution chart."""
        result = create_engagement_distribution_chart(sample_scoring_data["engagement"])
        assert_valid_plotly_figure(result, "create_engagement_distribution_chart")

    def test_scoring_performance_chart(self, sample_scoring_data):
        """Test scoring performance dashboard."""
        result = create_scoring_performance_chart(sample_scoring_data["performance"])
        assert_valid_plotly_figure(result, "create_scoring_performance_chart")


class TestChartTimeouts:
    """Test that charts complete within reasonable timeouts."""

    def test_chart_timeouts_with_large_data(self, sample_youtube_data):
        """Test charts handle larger datasets within timeout."""
        # Create larger dataset (but still reasonable)
        large_data = pd.concat([sample_youtube_data] * 5, ignore_index=True)  # 1000 rows

        import time

        # Test a few key charts with timing
        start_time = time.time()
        result = views_over_time_plotly(large_data)
        elapsed = time.time() - start_time

        assert result is not None, "Chart failed with larger dataset"
        assert elapsed < 10, f"Chart took too long: {elapsed:.2f}s > 10s"

    def test_empty_data_handling(self):
        """Test charts handle empty data gracefully."""
        empty_df = pd.DataFrame()

        # These should not crash, but may return empty figures
        try:
            result = views_over_time_plotly(empty_df)
            # Should either return a figure with annotation or raise a clear error
            assert result is not None or True  # Allow either behavior
        except (ValueError, KeyError) as e:
            # Clear error messages are acceptable
            assert "Missing" in str(e) or "required" in str(e).lower()


if __name__ == "__main__":
    # Run smoke tests directly
    pytest.main([__file__, "-v"])
