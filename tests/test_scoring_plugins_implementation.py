"""
Tests for scoring plugin implementations.

This module tests the concrete scoring plugin implementations that work
with existing database tables for momentum, engagement, and growth potential scoring.
"""

from datetime import datetime, timedelta
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_organization.scoring_plugin import ScoringPlugin, ValidationResult
from src.data_organization.scoring_plugins_implementation import (
    DataValidationError,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
    MomentumScoringPlugin,
    ScoringPluginError,
)


class TestMomentumScoringPlugin:
    """Test MomentumScoringPlugin implementation."""

    @pytest.fixture
    def momentum_plugin(self):
        """Create MomentumScoringPlugin instance for testing."""
        config = {
            "threshold": 0.6,
            "window_days": 30,
            "min_videos": 5,
            "view_growth_weight": 0.4,
            "engagement_weight": 0.3,
            "consistency_weight": 0.3,
        }
        return MomentumScoringPlugin(config)

    @pytest.fixture
    def sample_video_data(self):
        """Create sample video data for testing."""
        base_date = datetime.now() - timedelta(days=45)
        data = []

        for i in range(20):
            data.append(
                {
                    "video_id": f"video_{i}",
                    "artist_name": "test_artist",
                    "title": f"Test Video {i}",
                    "view_count": 1000 + (i * 100),  # Growing views
                    "like_count": 50 + (i * 5),
                    "comment_count": 10 + i,
                    "published_date": base_date + timedelta(days=i),
                    "analytics_date": base_date + timedelta(days=i + 1),
                }
            )

        return pd.DataFrame(data)

    def test_momentum_plugin_initialization(self, momentum_plugin):
        """Test MomentumScoringPlugin initialization."""
        assert momentum_plugin.get_name() == "momentum_scoring"
        assert momentum_plugin.get_version() == "1.0.0"

        params = momentum_plugin.get_parameters()
        assert params["threshold"] == 0.6
        assert params["window_days"] == 30
        assert params["min_videos"] == 5

    def test_momentum_plugin_input_validation_success(self, momentum_plugin, sample_video_data):
        """Test successful input validation for momentum scoring."""
        result = momentum_plugin.validate_input(sample_video_data)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_momentum_plugin_input_validation_failure(self, momentum_plugin):
        """Test input validation failure for momentum scoring."""
        # Test with missing required columns
        invalid_data = pd.DataFrame(
            {
                "video_id": ["v1", "v2"],
                "artist_name": ["artist1", "artist2"],
                # Missing view_count, published_date, etc.
            }
        )

        result = momentum_plugin.validate_input(invalid_data)

        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("view_count" in error for error in result.errors)

    def test_momentum_scoring_calculation(self, momentum_plugin, sample_video_data):
        """Test momentum score calculation."""
        scores = momentum_plugin.calculate_scores(sample_video_data)

        assert isinstance(scores, pd.DataFrame)
        assert "artist_name" in scores.columns
        assert "momentum_score" in scores.columns
        assert "view_growth_score" in scores.columns
        assert "engagement_score" in scores.columns
        assert "consistency_score" in scores.columns

        # Check score ranges
        assert all(0 <= score <= 1 for score in scores["momentum_score"])

        # Should have one row per artist
        assert len(scores) == 1
        assert scores.iloc[0]["artist_name"] == "test_artist"

    def test_momentum_scoring_insufficient_data(self, momentum_plugin):
        """Test momentum scoring with insufficient data."""
        # Only 2 videos, but min_videos is 5
        insufficient_data = pd.DataFrame(
            {
                "video_id": ["v1", "v2"],
                "artist_name": ["test_artist", "test_artist"],
                "view_count": [1000, 1100],
                "like_count": [50, 55],
                "comment_count": [10, 12],
                "published_date": [datetime.now() - timedelta(days=10), datetime.now() - timedelta(days=5)],
                "analytics_date": [datetime.now() - timedelta(days=9), datetime.now() - timedelta(days=4)],
            }
        )

        scores = momentum_plugin.calculate_scores(insufficient_data)

        # Should still return a score, but likely lower due to insufficient data
        assert isinstance(scores, pd.DataFrame)
        assert len(scores) == 1
        assert scores.iloc[0]["momentum_score"] >= 0

    def test_momentum_scoring_multiple_artists(self, momentum_plugin):
        """Test momentum scoring with multiple artists."""
        # Create data for multiple artists
        data = []
        artists = ["artist_a", "artist_b", "artist_c"]
        base_date = datetime.now() - timedelta(days=30)

        for artist in artists:
            for i in range(10):
                data.append(
                    {
                        "video_id": f"{artist}_video_{i}",
                        "artist_name": artist,
                        "view_count": 1000 + (i * 100),
                        "like_count": 50 + (i * 5),
                        "comment_count": 10 + i,
                        "published_date": base_date + timedelta(days=i),
                        "analytics_date": base_date + timedelta(days=i + 1),
                    }
                )

        multi_artist_data = pd.DataFrame(data)
        scores = momentum_plugin.calculate_scores(multi_artist_data)

        assert len(scores) == 3
        assert set(scores["artist_name"]) == set(artists)


class TestEngagementScoringPlugin:
    """Test EngagementScoringPlugin implementation."""

    @pytest.fixture
    def engagement_plugin(self):
        """Create EngagementScoringPlugin instance for testing."""
        config = {
            "min_comments": 5,
            "sentiment_weight": 0.4,
            "like_ratio_weight": 0.3,
            "comment_ratio_weight": 0.3,
            "sentiment_threshold": 0.1,
        }
        return EngagementScoringPlugin(config)

    @pytest.fixture
    def sample_engagement_data(self):
        """Create sample engagement data for testing."""
        return pd.DataFrame(
            {
                "video_id": ["v1", "v2", "v3", "v4", "v5"],
                "artist_name": ["test_artist"] * 5,
                "view_count": [10000, 15000, 8000, 12000, 20000],
                "like_count": [500, 750, 400, 600, 1000],
                "comment_count": [50, 75, 40, 60, 100],
                "avg_sentiment_score": [0.3, 0.5, -0.1, 0.2, 0.7],
                "positive_comment_ratio": [0.6, 0.8, 0.4, 0.5, 0.9],
                "analytics_date": [datetime.now() - timedelta(days=i) for i in range(5)],
            }
        )

    def test_engagement_plugin_initialization(self, engagement_plugin):
        """Test EngagementScoringPlugin initialization."""
        assert engagement_plugin.get_name() == "engagement_scoring"
        assert engagement_plugin.get_version() == "1.0.0"

        params = engagement_plugin.get_parameters()
        assert params["min_comments"] == 5
        assert params["sentiment_weight"] == 0.4

    def test_engagement_scoring_calculation(self, engagement_plugin, sample_engagement_data):
        """Test engagement score calculation."""
        scores = engagement_plugin.calculate_scores(sample_engagement_data)

        assert isinstance(scores, pd.DataFrame)
        assert "artist_name" in scores.columns
        assert "engagement_score" in scores.columns
        assert "like_ratio_score" in scores.columns
        assert "comment_ratio_score" in scores.columns
        assert "sentiment_score" in scores.columns

        # Check score ranges
        assert all(0 <= score <= 1 for score in scores["engagement_score"])

        # Should have one row per artist
        assert len(scores) == 1
        assert scores.iloc[0]["artist_name"] == "test_artist"

    def test_engagement_scoring_low_comments(self, engagement_plugin):
        """Test engagement scoring with low comment counts."""
        low_comment_data = pd.DataFrame(
            {
                "video_id": ["v1", "v2"],
                "artist_name": ["test_artist"] * 2,
                "view_count": [10000, 15000],
                "like_count": [500, 750],
                "comment_count": [2, 3],  # Below min_comments threshold
                "avg_sentiment_score": [0.3, 0.5],
                "positive_comment_ratio": [0.6, 0.8],
                "analytics_date": [datetime.now() - timedelta(days=i) for i in range(2)],
            }
        )

        scores = engagement_plugin.calculate_scores(low_comment_data)

        # Should still calculate scores but penalize for low engagement
        assert isinstance(scores, pd.DataFrame)
        assert len(scores) == 1
        assert scores.iloc[0]["engagement_score"] >= 0


class TestGrowthPotentialScoringPlugin:
    """Test GrowthPotentialScoringPlugin implementation."""

    @pytest.fixture
    def growth_plugin(self):
        """Create GrowthPotentialScoringPlugin instance for testing."""
        config = {
            "lookback_months": 6,
            "growth_threshold": 0.15,
            "min_data_points": 10,
            "trend_weight": 0.4,
            "acceleration_weight": 0.3,
            "consistency_weight": 0.3,
        }
        return GrowthPotentialScoringPlugin(config)

    @pytest.fixture
    def sample_growth_data(self):
        """Create sample growth data for testing."""
        base_date = datetime.now() - timedelta(days=180)  # 6 months ago
        data = []

        # Create monthly data points with growth trend
        for month in range(6):
            month_start = base_date + timedelta(days=month * 30)
            base_views = 10000 + (month * 2000)  # Growing trend

            for week in range(4):  # 4 weeks per month
                data.append(
                    {
                        "artist_name": "test_artist",
                        "total_views": base_views + (week * 500),
                        "total_subscribers": 1000 + (month * 100),
                        "video_count": 10 + month,
                        "avg_engagement_rate": 0.05 + (month * 0.01),
                        "analytics_date": month_start + timedelta(days=week * 7),
                    }
                )

        return pd.DataFrame(data)

    def test_growth_plugin_initialization(self, growth_plugin):
        """Test GrowthPotentialScoringPlugin initialization."""
        assert growth_plugin.get_name() == "growth_potential"
        assert growth_plugin.get_version() == "1.0.0"

        params = growth_plugin.get_parameters()
        assert params["lookback_months"] == 6
        assert params["growth_threshold"] == 0.15

    def test_growth_scoring_calculation(self, growth_plugin, sample_growth_data):
        """Test growth potential score calculation."""
        scores = growth_plugin.calculate_scores(sample_growth_data)

        assert isinstance(scores, pd.DataFrame)
        assert "artist_name" in scores.columns
        assert "growth_potential_score" in scores.columns
        assert "trend_score" in scores.columns
        assert "acceleration_score" in scores.columns
        assert "consistency_score" in scores.columns

        # Check score ranges
        assert all(0 <= score <= 1 for score in scores["growth_potential_score"])

        # Should have one row per artist
        assert len(scores) == 1
        assert scores.iloc[0]["artist_name"] == "test_artist"

    def test_growth_scoring_declining_trend(self, growth_plugin):
        """Test growth scoring with declining trend."""
        declining_data = pd.DataFrame(
            {
                "artist_name": ["test_artist"] * 12,
                "total_views": [20000 - (i * 500) for i in range(12)],  # Declining
                "total_subscribers": [2000 - (i * 50) for i in range(12)],
                "video_count": [20] * 12,
                "avg_engagement_rate": [0.08 - (i * 0.005) for i in range(12)],
                "analytics_date": [datetime.now() - timedelta(days=i * 15) for i in range(12)],
            }
        )

        scores = growth_plugin.calculate_scores(declining_data)

        # Should detect declining trend and give lower score
        assert isinstance(scores, pd.DataFrame)
        assert len(scores) == 1
        # Declining trend should result in lower growth potential
        assert scores.iloc[0]["growth_potential_score"] < 0.5


class TestScoringPluginIntegration:
    """Test integration between scoring plugins and configuration system."""

    def test_plugin_configuration_loading(self):
        """Test loading plugin configuration from config manager."""
        # Mock configuration manager
        mock_config_manager = MagicMock()
        mock_config_manager.load_scoring_config.return_value.parameters = {
            "threshold": 0.7,
            "window_days": 45,
            "min_videos": 8,
        }

        # Test plugin creation with config manager
        plugin = MomentumScoringPlugin.from_config_manager(mock_config_manager)

        assert plugin.get_parameters()["threshold"] == 0.7
        assert plugin.get_parameters()["window_days"] == 45
        assert plugin.get_parameters()["min_videos"] == 8

    def test_plugin_error_handling(self):
        """Test plugin error handling for invalid configurations."""
        # Test with invalid configuration
        invalid_config = {
            "threshold": 1.5,  # Invalid: > 1.0
            "window_days": -10,  # Invalid: negative
            "min_videos": 0,  # Invalid: zero
        }

        with pytest.raises(ScoringPluginError):
            MomentumScoringPlugin(invalid_config)

    def test_plugin_data_validation_error(self):
        """Test plugin data validation error handling."""
        config = {"threshold": 0.6, "window_days": 30, "min_videos": 5}
        plugin = MomentumScoringPlugin(config)

        # Test with completely invalid data
        invalid_data = pd.DataFrame({"invalid_column": [1, 2, 3]})

        with pytest.raises(DataValidationError):
            plugin.calculate_scores(invalid_data)

    def test_all_plugins_consistent_interface(self):
        """Test that all plugins implement consistent interface."""
        plugins = [
            MomentumScoringPlugin({"threshold": 0.6, "window_days": 30, "min_videos": 5}),
            EngagementScoringPlugin({"min_comments": 5, "sentiment_weight": 0.4}),
            GrowthPotentialScoringPlugin({"lookback_months": 6, "growth_threshold": 0.15}),
        ]

        for plugin in plugins:
            # All plugins should implement required methods
            assert hasattr(plugin, "get_name")
            assert hasattr(plugin, "get_version")
            assert hasattr(plugin, "get_parameters")
            assert hasattr(plugin, "calculate_scores")
            assert hasattr(plugin, "validate_input")

            # All should return proper types
            assert isinstance(plugin.get_name(), str)
            assert isinstance(plugin.get_version(), str)
            assert isinstance(plugin.get_parameters(), dict)

    def test_plugin_metadata_consistency(self):
        """Test plugin metadata consistency."""
        momentum_plugin = MomentumScoringPlugin({"threshold": 0.6, "window_days": 30, "min_videos": 5})

        metadata = momentum_plugin.get_metadata()

        assert "name" in metadata
        assert "version" in metadata
        assert "description" in metadata
        assert "input_requirements" in metadata
        assert "output_schema" in metadata

        # Validate metadata content
        assert metadata["name"] == momentum_plugin.get_name()
        assert metadata["version"] == momentum_plugin.get_version()
        assert isinstance(metadata["input_requirements"], list)
        assert isinstance(metadata["output_schema"], dict)


if __name__ == "__main__":
    pytest.main([__file__])
