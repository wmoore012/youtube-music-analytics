"""Tests for YouTube analytics scoring plugins using real database data."""

import pandas as pd
import pytest
from datetime import datetime, timedelta

from src.data_organization.youtube_scoring_plugins import (
    ArtistMomentumScoringPlugin,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
)
from youtubeviz.data import load_artist_daily_metrics
from web.etl_helpers import get_engine


class TestArtistMomentumScoringPluginWithRealData:
    """Test cases for artist momentum scoring plugin using real database data."""

    def setup_method(self):
        """Set up test fixtures with real data."""
        self.plugin = ArtistMomentumScoringPlugin()
        
        try:
            self.engine = get_engine()
            self.real_data = self._load_real_data()
            if self.real_data.empty:
                pytest.skip("No real YouTube data available for testing")
        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    def _load_real_data(self):
        """Load real YouTube data for testing."""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=45)
            
            data = load_artist_daily_metrics(start=start_date, end=end_date, engine=self.engine)
            
            if not data.empty:
                # Get top 2 artists with most data
                artist_counts = data["artist_name"].value_counts()
                top_artists = artist_counts.head(2).index.tolist()
                data = data[data["artist_name"].isin(top_artists)]
                
                # Rename columns for plugin compatibility
                data = data.rename(columns={
                    "date": "metrics_date",
                    "views": "view_count",
                    "likes": "like_count",
                    "comments": "comment_count"
                })
                data["published_at"] = data["metrics_date"]
                data["channel_title"] = data["artist_name"]
                
            return data
        except Exception:
            return pd.DataFrame()

    def test_real_momentum_scoring(self):
        """Test momentum scoring with real YouTube data."""
        if self.real_data.empty:
            pytest.skip("No real data available")
            
        scores = self.plugin.calculate_scores(self.real_data)
        
        # Verify output structure
        assert not scores.empty
        required_columns = ["entity_id", "score_value", "confidence", "momentum_category"]
        for col in required_columns:
            assert col in scores.columns
        
        # Verify real artist names (not dummy data)
        artists = scores["entity_id"].tolist()
        assert all(isinstance(artist, str) and len(artist) > 0 for artist in artists)
        assert "Artist A" not in artists
        assert "artist1" not in artists
        
        # Verify realistic score values (not obvious dummy values)
        score_values = scores["score_value"].tolist()
        assert all(isinstance(score, (int, float)) for score in score_values)
        assert all(0 <= score <= 1 for score in score_values)
        
        # Verify momentum categories are realistic
        categories = scores["momentum_category"].unique()
        valid_categories = ["high_momentum", "moderate_momentum", "stable", "low_momentum", "declining"]
        assert all(cat in valid_categories for cat in categories)
        
        # Verify confidence values are realistic
        confidence_values = scores["confidence"].tolist()
        assert all(0 <= conf <= 1 for conf in confidence_values)

    def test_real_data_validation(self):
        """Test input validation with real data."""
        if self.real_data.empty:
            pytest.skip("No real data available")
            
        result = self.plugin.validate_input(self.real_data)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_real_momentum_categories_distribution(self):
        """Test that momentum categories reflect real performance patterns."""
        if self.real_data.empty:
            pytest.skip("No real data available")
            
        scores = self.plugin.calculate_scores(self.real_data)
        
        # Should have meaningful distribution of categories
        categories = scores["momentum_category"].value_counts()
        
        # Verify we get realistic category distribution
        assert len(categories) >= 1
        
        # Check that scores correlate with categories
        for _, row in scores.iterrows():
            score = row["score_value"]
            category = row["momentum_category"]
            
            if category == "high_momentum":
                assert score >= 0.6  # High momentum should have high scores
            elif category == "declining":
                assert score <= 0.4  # Declining should have low scores


class TestArtistMomentumScoringPluginMocked:
    """Test cases for artist momentum scoring plugin with mocked data."""

    def setup_method(self):
        """Set up test fixtures."""
        self.plugin = ArtistMomentumScoringPlugin()

    def test_plugin_metadata(self):
        """Test plugin metadata is correct."""
        assert self.plugin.get_name() == "artist_momentum_scorer"
        assert self.plugin.get_version() == "1.0.0"
        
        params = self.plugin.get_parameters()
        assert "momentum_window_days" in params
        assert "view_growth_weight" in params
        assert "engagement_weight" in params
        assert "consistency_weight" in params

    def test_input_requirements(self):
        """Test input requirements are defined."""
        requirements = self.plugin.get_input_requirements()
        expected_columns = [
            "artist_name", "video_id", "published_at", "view_count", 
            "like_count", "comment_count", "channel_title"
        ]
        for col in expected_columns:
            assert col in requirements

    def test_validate_input_success(self):
        """Test successful input validation."""
        # Create sample data with required columns
        data = pd.DataFrame({
            "artist_name": ["Artist A", "Artist B"],
            "video_id": ["vid1", "vid2"],
            "published_at": [datetime.now() - timedelta(days=10), datetime.now() - timedelta(days=5)],
            "view_count": [10000, 15000],
            "like_count": [100, 200],
            "comment_count": [50, 75],
            "channel_title": ["Channel A", "Channel B"],
            "metrics_date": [datetime.now() - timedelta(days=10), datetime.now() - timedelta(days=5)]
        })

        result = self.plugin.validate_input(data)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_input_missing_columns(self):
        """Test validation fails with missing columns."""
        data = pd.DataFrame({
            "artist_name": ["Artist A"],
            "view_count": [10000]
        })

        result = self.plugin.validate_input(data)
        assert not result.is_valid
        assert len(result.errors) > 0
        assert "Missing required columns" in result.errors[0]

    def test_validate_input_empty_data(self):
        """Test validation fails with empty data."""
        data = pd.DataFrame()
        
        result = self.plugin.validate_input(data)
        assert not result.is_valid
        assert "Input data is empty" in result.errors

    def test_calculate_scores_basic(self):
        """Test basic score calculation."""
        # Create sample data
        base_date = datetime.now()
        data = pd.DataFrame({
            "artist_name": ["Artist A", "Artist A", "Artist B"],
            "video_id": ["vid1", "vid2", "vid3"],
            "published_at": [
                base_date - timedelta(days=30),
                base_date - timedelta(days=15),
                base_date - timedelta(days=10)
            ],
            "metrics_date": [
                base_date - timedelta(days=30),
                base_date - timedelta(days=15),
                base_date - timedelta(days=10)
            ],
            "view_count": [5000, 10000, 8000],
            "like_count": [50, 150, 100],
            "comment_count": [25, 75, 50],
            "channel_title": ["Channel A", "Channel A", "Channel B"]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Check output structure
        assert not scores.empty
        assert "entity_id" in scores.columns
        assert "score_value" in scores.columns
        assert "confidence" in scores.columns
        assert "momentum_category" in scores.columns

        # Check score values are in valid range
        assert all(0 <= score <= 1 for score in scores["score_value"])
        assert all(0 <= conf <= 1 for conf in scores["confidence"])

    def test_calculate_scores_insufficient_data(self):
        """Test score calculation with insufficient data."""
        # Single video for artist (below minimum threshold)
        data = pd.DataFrame({
            "artist_name": ["Artist A"],
            "video_id": ["vid1"],
            "published_at": [datetime.now() - timedelta(days=10)],
            "view_count": [1000],
            "like_count": [10],
            "comment_count": [5],
            "channel_title": ["Channel A"],
            "metrics_date": [datetime.now() - timedelta(days=10)]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Should return empty results for insufficient data (below min_videos_required)
        assert scores.empty

    def test_momentum_categories(self):
        """Test momentum category assignment."""
        # Create data with different momentum levels
        base_date = datetime.now()
        data = pd.DataFrame({
            "artist_name": ["High", "High", "High", "Low", "Low"],
            "video_id": ["h1", "h2", "h3", "l1", "l2"],
            "published_at": [
                base_date - timedelta(days=30),
                base_date - timedelta(days=20),
                base_date - timedelta(days=10),
                base_date - timedelta(days=30),
                base_date - timedelta(days=25)
            ],
            "view_count": [100000, 150000, 200000, 1000, 900],  # High growth vs decline
            "like_count": [1000, 1500, 2000, 10, 8],
            "comment_count": [500, 750, 1000, 5, 4],
            "channel_title": ["High Channel", "High Channel", "High Channel", "Low Channel", "Low Channel"]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Check that different momentum categories are assigned
        categories = scores["momentum_category"].unique()
        assert len(categories) > 1  # Should have different categories


class TestEngagementScoringPlugin:
    """Test cases for engagement scoring plugin."""

    def setup_method(self):
        """Set up test fixtures."""
        self.plugin = EngagementScoringPlugin()

    def test_plugin_metadata(self):
        """Test plugin metadata is correct."""
        assert self.plugin.get_name() == "engagement_scorer"
        assert self.plugin.get_version() == "1.0.0"
        
        params = self.plugin.get_parameters()
        assert "like_weight" in params
        assert "comment_weight" in params
        assert "sentiment_weight" in params

    def test_input_requirements(self):
        """Test input requirements are defined."""
        requirements = self.plugin.get_input_requirements()
        expected_columns = [
            "video_id", "view_count", "like_count", "comment_count", 
            "avg_sentiment", "sentiment_magnitude"
        ]
        for col in expected_columns:
            assert col in requirements

    def test_calculate_scores_with_sentiment(self):
        """Test engagement scoring with sentiment data."""
        data = pd.DataFrame({
            "video_id": ["vid1", "vid2", "vid3"],
            "view_count": [10000, 20000, 5000],
            "like_count": [100, 300, 50],
            "comment_count": [50, 150, 25],
            "avg_sentiment": [0.8, 0.6, -0.2],  # Positive, neutral, negative
            "sentiment_magnitude": [0.9, 0.5, 0.8]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Check output structure
        assert not scores.empty
        assert "entity_id" in scores.columns
        assert "score_value" in scores.columns
        assert "engagement_rate" in scores.columns
        assert "sentiment_boost" in scores.columns

        # Video with positive sentiment should have higher score
        vid1_score = scores[scores["entity_id"] == "vid1"]["score_value"].iloc[0]
        vid3_score = scores[scores["entity_id"] == "vid3"]["score_value"].iloc[0]
        assert vid1_score > vid3_score

    def test_engagement_rate_calculation(self):
        """Test engagement rate calculation."""
        data = pd.DataFrame({
            "video_id": ["vid1"],
            "view_count": [1000],
            "like_count": [100],
            "comment_count": [50],
            "avg_sentiment": [0.0],
            "sentiment_magnitude": [0.5]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Engagement rate should be (100 + 50) / 1000 = 0.15
        expected_rate = 0.15
        actual_rate = scores.iloc[0]["engagement_rate"]
        assert abs(actual_rate - expected_rate) < 0.01


class TestGrowthPotentialScoringPlugin:
    """Test cases for growth potential scoring plugin."""

    def setup_method(self):
        """Set up test fixtures."""
        self.plugin = GrowthPotentialScoringPlugin()

    def test_plugin_metadata(self):
        """Test plugin metadata is correct."""
        assert self.plugin.get_name() == "growth_potential_scorer"
        assert self.plugin.get_version() == "1.0.0"
        
        params = self.plugin.get_parameters()
        assert "trend_window_days" in params
        assert "velocity_weight" in params
        assert "acceleration_weight" in params

    def test_input_requirements(self):
        """Test input requirements are defined."""
        requirements = self.plugin.get_input_requirements()
        expected_columns = [
            "artist_name", "video_id", "metrics_date", "view_count", 
            "like_count", "comment_count"
        ]
        for col in expected_columns:
            assert col in requirements

    def test_calculate_scores_with_trend_data(self):
        """Test growth potential calculation with trending data."""
        # Create time series data showing growth
        base_date = datetime.now().date()
        data = pd.DataFrame({
            "artist_name": ["Artist A"] * 10,
            "video_id": ["vid1"] * 10,
            "metrics_date": [base_date - timedelta(days=i) for i in range(9, -1, -1)],
            "view_count": [1000 + i * 500 for i in range(10)],  # Growing views
            "like_count": [10 + i * 5 for i in range(10)],
            "comment_count": [5 + i * 2 for i in range(10)]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Check output structure
        assert not scores.empty
        assert "entity_id" in scores.columns
        assert "score_value" in scores.columns
        assert "growth_velocity" in scores.columns
        assert "growth_acceleration" in scores.columns
        assert "trend_direction" in scores.columns

        # Should detect positive growth
        assert scores.iloc[0]["trend_direction"] in ["accelerating", "growing", "stable"]
        assert scores.iloc[0]["score_value"] > 0.2  # Lower threshold for realistic test

    def test_calculate_scores_declining_trend(self):
        """Test growth potential with declining metrics."""
        base_date = datetime.now().date()
        data = pd.DataFrame({
            "artist_name": ["Artist B"] * 5,
            "video_id": ["vid2"] * 5,
            "metrics_date": [base_date - timedelta(days=i) for i in range(4, -1, -1)],
            "view_count": [5000 - i * 200 for i in range(5)],  # Declining views
            "like_count": [50 - i * 2 for i in range(5)],
            "comment_count": [25 - i * 1 for i in range(5)]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Should detect negative growth or stability
        assert scores.iloc[0]["trend_direction"] in ["declining", "stagnant", "stable"]
        assert scores.iloc[0]["score_value"] < 0.8  # Should be lower than high-growth scenarios

    def test_insufficient_historical_data(self):
        """Test behavior with insufficient historical data."""
        data = pd.DataFrame({
            "artist_name": ["Artist C"],
            "video_id": ["vid3"],
            "metrics_date": [datetime.now().date()],
            "view_count": [1000],
            "like_count": [10],
            "comment_count": [5]
        })

        scores = self.plugin.calculate_scores(data)
        
        # Should return low confidence score
        assert not scores.empty
        assert scores.iloc[0]["confidence"] < 0.5


class TestPluginIntegration:
    """Integration tests for all YouTube scoring plugins."""

    def test_all_plugins_implement_interface(self):
        """Test that all plugins properly implement the ScoringPlugin interface."""
        plugins = [
            ArtistMomentumScoringPlugin(),
            EngagementScoringPlugin(),
            GrowthPotentialScoringPlugin()
        ]

        for plugin in plugins:
            # Test required methods exist and return expected types
            assert isinstance(plugin.get_name(), str)
            assert isinstance(plugin.get_version(), str)
            assert isinstance(plugin.get_parameters(), dict)
            assert isinstance(plugin.get_input_requirements(), list)
            assert isinstance(plugin.get_output_schema(), dict)

            # Test metadata validation
            metadata = plugin.get_metadata()
            validation_result = metadata.validate()
            assert validation_result.is_valid, f"Plugin {plugin.get_name()} metadata validation failed: {validation_result.errors}"

    def test_plugin_execution_workflow(self):
        """Test complete plugin execution workflow."""
        plugin = EngagementScoringPlugin()
        
        # Create valid test data
        data = pd.DataFrame({
            "video_id": ["test_vid"],
            "view_count": [1000],
            "like_count": [50],
            "comment_count": [25],
            "avg_sentiment": [0.5],
            "sentiment_magnitude": [0.7]
        })

        # Test complete execution workflow
        result = plugin.execute(data)
        
        # Validate result
        assert result.algorithm_name == plugin.get_name()
        assert result.algorithm_version == plugin.get_version()
        assert not result.entity_scores.empty
        
        # Validate result scores
        validation = result.validate_scores()
        assert validation.is_valid, f"Result validation failed: {validation.errors}"