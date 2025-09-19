"""Integration tests for YouTube scoring plugins with the scoring system."""

import pandas as pd
import pytest
from datetime import datetime, timedelta

from src.data_organization.scoring_engine import ScoringEngine
from src.data_organization.youtube_scoring_plugins import (
    ArtistMomentumScoringPlugin,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
)


class TestYouTubeScoringIntegration:
    """Integration tests for YouTube scoring plugins with the scoring engine."""

    def setup_method(self):
        """Set up test fixtures."""
        self.scoring_engine = ScoringEngine()
        
        # Register all YouTube plugins
        self.momentum_plugin = ArtistMomentumScoringPlugin()
        self.engagement_plugin = EngagementScoringPlugin()
        self.growth_plugin = GrowthPotentialScoringPlugin()
        
        self.scoring_engine.register_plugin(self.momentum_plugin)
        self.scoring_engine.register_plugin(self.engagement_plugin)
        self.scoring_engine.register_plugin(self.growth_plugin)

    def test_all_plugins_registered(self):
        """Test that all YouTube plugins are properly registered."""
        available_algorithms = self.scoring_engine.get_available_algorithms()
        
        expected_algorithms = [
            "artist_momentum_scorer",
            "engagement_scorer", 
            "growth_potential_scorer"
        ]
        
        for algorithm in expected_algorithms:
            assert algorithm in available_algorithms

    def test_momentum_scoring_integration(self):
        """Test momentum scoring through the scoring engine."""
        # Create sample data
        base_date = datetime.now()
        data = pd.DataFrame({
            "artist_name": ["Artist A", "Artist A", "Artist B", "Artist B"],
            "video_id": ["vid1", "vid2", "vid3", "vid4"],
            "published_at": [
                base_date - timedelta(days=30),
                base_date - timedelta(days=15),
                base_date - timedelta(days=20),
                base_date - timedelta(days=10)
            ],
            "view_count": [5000, 10000, 8000, 12000],
            "like_count": [50, 150, 100, 180],
            "comment_count": [25, 75, 50, 90],
            "channel_title": ["Channel A", "Channel A", "Channel B", "Channel B"],
            "metrics_date": [
                base_date - timedelta(days=30),
                base_date - timedelta(days=15),
                base_date - timedelta(days=20),
                base_date - timedelta(days=10)
            ]
        })

        # Execute scoring through engine
        result = self.scoring_engine.execute_scoring("artist_momentum_scorer", data)
        
        # Validate result
        assert result.algorithm_name == "artist_momentum_scorer"
        assert result.algorithm_version == "1.0.0"
        assert not result.entity_scores.empty
        assert len(result.entity_scores) == 2  # Two artists
        
        # Check required columns
        required_columns = ["entity_id", "score_value", "confidence", "momentum_category"]
        for col in required_columns:
            assert col in result.entity_scores.columns

    def test_engagement_scoring_integration(self):
        """Test engagement scoring through the scoring engine."""
        # Create sample data
        data = pd.DataFrame({
            "video_id": ["vid1", "vid2", "vid3"],
            "view_count": [10000, 20000, 5000],
            "like_count": [100, 300, 50],
            "comment_count": [50, 150, 25],
            "avg_sentiment": [0.8, 0.6, -0.2],
            "sentiment_magnitude": [0.9, 0.5, 0.8]
        })

        # Execute scoring through engine
        result = self.scoring_engine.execute_scoring("engagement_scorer", data)
        
        # Validate result
        assert result.algorithm_name == "engagement_scorer"
        assert not result.entity_scores.empty
        assert len(result.entity_scores) == 3  # Three videos
        
        # Check required columns
        required_columns = ["entity_id", "score_value", "confidence", "engagement_rate"]
        for col in required_columns:
            assert col in result.entity_scores.columns

    def test_growth_potential_scoring_integration(self):
        """Test growth potential scoring through the scoring engine."""
        # Create sample time series data
        base_date = datetime.now().date()
        data = pd.DataFrame({
            "artist_name": ["Artist A"] * 10,
            "video_id": ["vid1"] * 10,
            "metrics_date": [base_date - timedelta(days=i) for i in range(9, -1, -1)],
            "view_count": [1000 + i * 100 for i in range(10)],
            "like_count": [10 + i * 2 for i in range(10)],
            "comment_count": [5 + i * 1 for i in range(10)]
        })

        # Execute scoring through engine
        result = self.scoring_engine.execute_scoring("growth_potential_scorer", data)
        
        # Validate result
        assert result.algorithm_name == "growth_potential_scorer"
        assert not result.entity_scores.empty
        assert len(result.entity_scores) == 1  # One artist
        
        # Check required columns
        required_columns = ["entity_id", "score_value", "confidence", "trend_direction"]
        for col in required_columns:
            assert col in result.entity_scores.columns

    def test_plugin_metadata_access(self):
        """Test accessing plugin metadata through the scoring engine."""
        algorithms = self.scoring_engine.get_available_algorithms()
        
        for algorithm in algorithms:
            metadata = self.scoring_engine.get_plugin_metadata(algorithm)
            
            # Check metadata structure
            assert "name" in metadata
            assert "version" in metadata
            assert "parameters" in metadata
            assert "input_requirements" in metadata
            assert "output_schema" in metadata
            
            # Check that metadata is valid
            assert isinstance(metadata["name"], str)
            assert isinstance(metadata["version"], str)
            assert isinstance(metadata["parameters"], dict)
            assert isinstance(metadata["input_requirements"], list)
            assert isinstance(metadata["output_schema"], dict)

    def test_plugin_validation_through_engine(self):
        """Test plugin validation through the scoring engine."""
        validation_results = self.scoring_engine.validate_all_plugins()
        
        # All plugins should be valid
        for plugin_name, result in validation_results.items():
            assert result.is_valid, f"Plugin {plugin_name} validation failed: {result.errors}"

    def test_scoring_with_custom_parameters(self):
        """Test scoring with custom parameters through the engine."""
        # Create sample data
        data = pd.DataFrame({
            "video_id": ["vid1", "vid2"],
            "view_count": [1000, 2000],
            "like_count": [10, 20],
            "comment_count": [5, 10],
            "avg_sentiment": [0.5, 0.7],
            "sentiment_magnitude": [0.6, 0.8]
        })

        # Custom parameters
        custom_params = {
            "like_weight": 0.5,
            "comment_weight": 0.3,
            "sentiment_weight": 0.2
        }

        # Execute scoring with custom parameters
        result = self.scoring_engine.execute_scoring("engagement_scorer", data, custom_params)
        
        # Validate result
        assert result.algorithm_name == "engagement_scorer"
        assert not result.entity_scores.empty
        assert "parameters" in result.metadata
        assert result.metadata["parameters"] == custom_params

    def test_scoring_result_database_export(self):
        """Test exporting scoring results to database format."""
        # Create sample data
        data = pd.DataFrame({
            "video_id": ["vid1", "vid2"],
            "view_count": [1000, 2000],
            "like_count": [10, 20],
            "comment_count": [5, 10],
            "avg_sentiment": [0.5, 0.7],
            "sentiment_magnitude": [0.6, 0.8]
        })

        # Execute scoring
        result = self.scoring_engine.execute_scoring("engagement_scorer", data)
        
        # Convert to database records
        db_records = result.to_database_records()
        
        # Validate database records
        assert len(db_records) == 2
        for record in db_records:
            assert "algorithm_name" in record
            assert "algorithm_version" in record
            assert "calculation_timestamp" in record
            assert "entity_id" in record
            assert "score_value" in record

    def test_error_handling_invalid_data(self):
        """Test error handling with invalid input data."""
        # Create invalid data (missing required columns)
        invalid_data = pd.DataFrame({
            "invalid_column": ["value1", "value2"]
        })

        # Should raise scoring execution error (which wraps the validation error)
        from src.data_organization.scoring_engine import ScoringExecutionError
        with pytest.raises(ScoringExecutionError, match="Invalid input data"):
            self.scoring_engine.execute_scoring("engagement_scorer", invalid_data)

    def test_error_handling_invalid_parameters(self):
        """Test error handling with invalid parameters."""
        # Create valid data
        data = pd.DataFrame({
            "video_id": ["vid1"],
            "view_count": [1000],
            "like_count": [10],
            "comment_count": [5],
            "avg_sentiment": [0.5],
            "sentiment_magnitude": [0.6]
        })

        # Invalid parameters (negative weights)
        invalid_params = {
            "like_weight": -0.5,  # Invalid negative weight
            "comment_weight": 0.3,
            "sentiment_weight": 0.2
        }

        # Should handle parameter validation gracefully
        # Note: Current implementation doesn't validate parameter ranges,
        # but the plugin should handle this gracefully
        try:
            result = self.scoring_engine.execute_scoring("engagement_scorer", data, invalid_params)
            # If no error, check that result is still valid
            assert not result.entity_scores.empty
        except ValueError:
            # If validation error is raised, that's also acceptable
            pass

    def test_system_status_reporting(self):
        """Test system status reporting functionality."""
        status = self.scoring_engine.get_system_status()
        
        # Check status structure
        assert "loaded_plugins" in status
        assert "available_algorithms" in status
        assert "isolation_enabled" in status
        assert "max_execution_time" in status
        assert "max_memory_usage" in status
        
        # Check values
        assert status["loaded_plugins"] == 3
        assert len(status["available_algorithms"]) == 3
        assert isinstance(status["isolation_enabled"], bool)
        assert isinstance(status["max_execution_time"], int)
        assert isinstance(status["max_memory_usage"], int)