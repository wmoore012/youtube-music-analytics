"""Integration tests for the complete scoring system."""

from pathlib import Path
import tempfile

import pandas as pd
import pytest

from src.data_organization.example_plugins import EngagementScoringPlugin, MomentumScoringPlugin, SimpleTestPlugin
from src.data_organization.scoring_engine import ScoringEngine


class TestScoringSystemIntegration:
    """Integration tests for the complete scoring system."""

    def setup_method(self):
        """Set up test fixtures."""
        self.engine = ScoringEngine()

    def test_complete_plugin_workflow(self):
        """Test complete workflow: register, validate, execute, get results."""
        # Register plugins
        momentum_plugin = MomentumScoringPlugin()
        engagement_plugin = EngagementScoringPlugin()

        self.engine.register_plugin(momentum_plugin)
        self.engine.register_plugin(engagement_plugin)

        # Verify plugins are available
        available = self.engine.get_available_algorithms()
        assert "momentum_scorer" in available
        assert "engagement_scorer" in available

        # Validate all plugins
        validation_results = self.engine.validate_all_plugins()
        assert validation_results["momentum_scorer"].is_valid
        assert validation_results["engagement_scorer"].is_valid

        # Prepare test data for momentum scoring
        momentum_data = pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist B", "Artist C"],
                "video_count": [15, 8, 25],
                "total_views": [500000, 150000, 1200000],
                "total_likes": [25000, 8000, 60000],
                "total_comments": [5000, 1500, 12000],
                "avg_views_per_video": [33333, 18750, 48000],
                "recent_growth_rate": [25.5, 10.2, 45.8],
            }
        )

        # Execute momentum scoring
        momentum_result = self.engine.execute_scoring("momentum_scorer", momentum_data)

        assert momentum_result.algorithm_name == "momentum_scorer"
        assert momentum_result.algorithm_version == "1.0.0"
        assert len(momentum_result.entity_scores) == 3
        assert "momentum_category" in momentum_result.entity_scores.columns
        assert "confidence" in momentum_result.entity_scores.columns

        # Prepare test data for engagement scoring
        engagement_data = pd.DataFrame(
            {
                "entity_id": ["video_1", "video_2", "video_3"],
                "total_views": [100000, 50000, 200000],
                "total_likes": [5000, 2000, 8000],
                "total_comments": [500, 200, 1000],
                "subscriber_count": [10000, 5000, 20000],
            }
        )

        # Execute engagement scoring
        engagement_result = self.engine.execute_scoring("engagement_scorer", engagement_data)

        assert engagement_result.algorithm_name == "engagement_scorer"
        assert len(engagement_result.entity_scores) == 3
        assert "like_rate" in engagement_result.entity_scores.columns
        assert "comment_rate" in engagement_result.entity_scores.columns

    def test_plugin_parameter_customization(self):
        """Test plugin execution with custom parameters."""
        plugin = MomentumScoringPlugin()
        self.engine.register_plugin(plugin)

        test_data = pd.DataFrame(
            {
                "artist_name": ["Test Artist"],
                "video_count": [10],
                "total_views": [100000],
                "total_likes": [5000],
                "total_comments": [500],
                "avg_views_per_video": [10000],
                "recent_growth_rate": [30.0],
            }
        )

        # Execute with custom parameters
        custom_params = {"growth_weight": 0.8, "engagement_weight": 0.2, "min_videos_required": 5}

        result = self.engine.execute_scoring("momentum_scorer", test_data, custom_params)

        assert result.metadata["parameters"] == custom_params
        assert len(result.entity_scores) == 1

    def test_plugin_error_handling_and_isolation(self):
        """Test plugin error handling and isolation."""

        # Create a plugin that will fail
        class FailingPlugin(SimpleTestPlugin):
            def get_name(self) -> str:
                return "failing_plugin"

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                raise RuntimeError("Intentional failure for testing")

        failing_plugin = FailingPlugin()
        working_plugin = SimpleTestPlugin()

        self.engine.register_plugin(failing_plugin)
        self.engine.register_plugin(working_plugin)

        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        # Failing plugin should raise error but not affect the engine
        with pytest.raises(Exception):
            self.engine.execute_scoring("failing_plugin", test_data)

        # Working plugin should still work
        result = self.engine.execute_scoring("simple_test", test_data)
        assert isinstance(result.entity_scores, pd.DataFrame)
        assert len(result.entity_scores) == 3

    def test_plugin_metadata_and_system_status(self):
        """Test plugin metadata retrieval and system status."""
        momentum_plugin = MomentumScoringPlugin()
        self.engine.register_plugin(momentum_plugin)

        # Get plugin metadata
        metadata = self.engine.get_plugin_metadata("momentum_scorer")

        assert metadata["name"] == "momentum_scorer"
        assert metadata["version"] == "1.0.0"
        assert "time_window_days" in metadata["parameters"]
        assert "artist_name" in metadata["input_requirements"]
        assert "momentum_category" in metadata["output_schema"]

        # Get system status
        status = self.engine.get_system_status()

        assert status["loaded_plugins"] == 1
        assert "momentum_scorer" in status["available_algorithms"]
        assert isinstance(status["isolation_enabled"], bool)
        assert isinstance(status["max_execution_time"], int)

    def test_plugin_unload_and_reload(self):
        """Test plugin unloading and reloading."""
        plugin = SimpleTestPlugin()
        self.engine.register_plugin(plugin)

        # Verify plugin is loaded
        assert "simple_test" in self.engine.get_available_algorithms()

        # Unload plugin
        self.engine.unload_plugin("simple_test")
        assert "simple_test" not in self.engine.get_available_algorithms()

        # Re-register plugin
        self.engine.register_plugin(plugin)
        assert "simple_test" in self.engine.get_available_algorithms()

    def test_multiple_plugin_execution_sequence(self):
        """Test executing multiple plugins in sequence."""
        # Register multiple plugins
        plugins = [MomentumScoringPlugin(), EngagementScoringPlugin(), SimpleTestPlugin()]

        for plugin in plugins:
            self.engine.register_plugin(plugin)

        # Prepare different datasets for each plugin
        momentum_data = pd.DataFrame(
            {
                "artist_name": ["Artist A"],
                "video_count": [10],
                "total_views": [100000],
                "total_likes": [5000],
                "total_comments": [500],
                "avg_views_per_video": [10000],
                "recent_growth_rate": [25.0],
            }
        )

        engagement_data = pd.DataFrame(
            {
                "entity_id": ["video_1"],
                "total_views": [100000],
                "total_likes": [5000],
                "total_comments": [500],
                "subscriber_count": [10000],
            }
        )

        simple_data = pd.DataFrame({"column1": [1, 2, 3]})

        # Execute all plugins
        results = {}
        results["momentum"] = self.engine.execute_scoring("momentum_scorer", momentum_data)
        results["engagement"] = self.engine.execute_scoring("engagement_scorer", engagement_data)
        results["simple"] = self.engine.execute_scoring("simple_test", simple_data)

        # Verify all results
        assert len(results) == 3
        for algorithm_name, result in results.items():
            assert hasattr(result, "entity_scores")
            assert isinstance(result.entity_scores, pd.DataFrame)
            assert not result.entity_scores.empty

    def test_plugin_validation_edge_cases(self):
        """Test plugin validation with edge cases."""
        plugin = MomentumScoringPlugin()
        self.engine.register_plugin(plugin)

        # Test with empty data
        empty_data = pd.DataFrame()
        with pytest.raises(Exception):
            self.engine.execute_scoring("momentum_scorer", empty_data)

        # Test with missing required columns
        incomplete_data = pd.DataFrame(
            {
                "artist_name": ["Artist A"],
                "video_count": [10],
                # Missing other required columns
            }
        )

        with pytest.raises(Exception):
            self.engine.execute_scoring("momentum_scorer", incomplete_data)

        # Test with invalid data types
        invalid_data = pd.DataFrame(
            {
                "artist_name": ["Artist A"],
                "video_count": ["not_a_number"],  # Should be numeric
                "total_views": [100000],
                "total_likes": [5000],
                "total_comments": [500],
                "avg_views_per_video": [10000],
                "recent_growth_rate": [25.0],
            }
        )

        with pytest.raises(Exception):
            self.engine.execute_scoring("momentum_scorer", invalid_data)

    def test_scoring_result_export_and_database_conversion(self):
        """Test scoring result export and database conversion."""
        plugin = SimpleTestPlugin()
        self.engine.register_plugin(plugin)

        test_data = pd.DataFrame({"column1": [1, 2, 3]})
        result = self.engine.execute_scoring("simple_test", test_data)

        # Test database record conversion
        db_records = result.to_database_records()

        assert len(db_records) == 3
        for record in db_records:
            assert "algorithm_name" in record
            assert "algorithm_version" in record
            assert "entity_id" in record
            assert "score_value" in record
            assert record["algorithm_name"] == "simple_test"

        # Test CSV export
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as temp_file:
            result.export_to_csv(temp_file.name)

            # Verify file was created and has content
            exported_df = pd.read_csv(temp_file.name)
            assert len(exported_df) == 3
            assert "algorithm_name" in exported_df.columns
            assert "entity_id" in exported_df.columns
            assert "score_value" in exported_df.columns

    def test_isolation_settings_configuration(self):
        """Test plugin isolation settings configuration."""
        plugin = SimpleTestPlugin()
        self.engine.register_plugin(plugin)

        # Test default isolation settings
        status = self.engine.get_system_status()
        assert status["isolation_enabled"] is True
        assert status["max_execution_time"] == 300

        # Update isolation settings
        self.engine.set_isolation_settings(
            enable_isolation=False, max_execution_time=600, max_memory_usage=2048 * 1024 * 1024
        )

        # Verify settings were updated
        status = self.engine.get_system_status()
        assert status["isolation_enabled"] is False
        assert status["max_execution_time"] == 600
        assert status["max_memory_usage"] == 2048 * 1024 * 1024

        # Test execution with updated settings
        test_data = pd.DataFrame({"column1": [1, 2, 3]})
        result = self.engine.execute_scoring("simple_test", test_data)

        assert isinstance(result.entity_scores, pd.DataFrame)
        assert len(result.entity_scores) == 3
