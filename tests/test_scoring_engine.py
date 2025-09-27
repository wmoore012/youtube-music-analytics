"""Tests for scoring engine with plugin management capabilities."""

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_organization.plugin_manager import PluginLoadingError, PluginValidationError
from src.data_organization.scoring_engine import (
    AlgorithmNotFoundError,
    ScoringEngine,
    ScoringEngineError,
    ScoringExecutionError,
)
from src.data_organization.scoring_plugin import ScoringPlugin, ScoringResult, ValidationResult


class MockScoringPlugin(ScoringPlugin):
    """Test scoring plugin for engine tests."""

    def get_name(self) -> str:
        return "test_plugin"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> dict:
        return {"param1": "value1"}

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {"entity_id": data.index.astype(str), "score_value": [0.8] * len(data), "confidence": [0.9] * len(data)}
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        errors = []
        if data.empty:
            errors.append("Input data is empty")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, checked_items=1, passed_items=1 - len(errors))


class FailingScoringPlugin(ScoringPlugin):
    """Plugin that fails during execution."""

    def get_name(self) -> str:
        return "failing_plugin"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> dict:
        return {}

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        raise RuntimeError("Calculation failed")

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        return ValidationResult(is_valid=True, checked_items=1, passed_items=1)


class InvalidInputPlugin(ScoringPlugin):
    """Plugin that fails input validation."""

    def get_name(self) -> str:
        return "invalid_input_plugin"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> dict:
        return {}

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"entity_id": [], "score_value": []})

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        return ValidationResult(is_valid=False, errors=["Input validation failed"], checked_items=1, passed_items=0)


class TestScoringEngine:
    """Test ScoringEngine class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.engine = ScoringEngine()

    def test_scoring_engine_initialization(self):
        """Test scoring engine initialization."""
        assert self.engine.config_manager is None
        assert self.engine.plugin_manager is not None
        assert self.engine._enable_plugin_isolation is True
        assert self.engine._max_execution_time == 300
        assert self.engine._max_memory_usage == 1024 * 1024 * 1024

    def test_scoring_engine_with_config_manager(self):
        """Test scoring engine initialization with config manager."""
        mock_config = MagicMock()
        engine = ScoringEngine(config_manager=mock_config)
        assert engine.config_manager is mock_config

    def test_register_plugin_success(self):
        """Test successful plugin registration."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        assert "test_plugin" in self.engine.get_available_algorithms()
        assert self.engine.plugin_manager._plugins["test_plugin"] == MockScoringPlugin
        assert self.engine.plugin_manager._plugin_instances["test_plugin"] is plugin

    def test_register_plugin_validation_failure(self):
        """Test plugin registration with validation failure."""
        # Create an invalid plugin (not a ScoringPlugin instance)
        invalid_plugin = "not_a_plugin"

        with pytest.raises(ScoringEngineError, match="Plugin registration failed"):
            self.engine.register_plugin(invalid_plugin)

    def test_register_plugin_with_invalid_metadata(self):
        """Test plugin registration with invalid metadata."""

        class InvalidMetadataPlugin(ScoringPlugin):
            def get_name(self) -> str:
                return ""  # Invalid empty name

            def get_version(self) -> str:
                return "1.0.0"

            def get_parameters(self) -> dict:
                return {}

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return pd.DataFrame()

            def validate_input(self, data: pd.DataFrame) -> ValidationResult:
                return ValidationResult(is_valid=True)

        plugin = InvalidMetadataPlugin()

        with pytest.raises(ScoringEngineError, match="Plugin registration failed"):
            self.engine.register_plugin(plugin)

    def test_load_plugin_from_path_success(self):
        """Test successful plugin loading from path."""
        with patch.object(self.engine.plugin_manager, "load_plugin") as mock_load:
            self.engine.load_plugin_from_path("test.module.TestPlugin")
            mock_load.assert_called_once_with("test.module.TestPlugin")

    def test_load_plugin_from_path_failure(self):
        """Test plugin loading failure from path."""
        with patch.object(self.engine.plugin_manager, "load_plugin", side_effect=PluginLoadingError("Load failed")):
            with pytest.raises(ScoringEngineError, match="Plugin loading failed"):
                self.engine.load_plugin_from_path("test.module.TestPlugin")

    def test_discover_and_load_plugins_success(self):
        """Test successful plugin discovery and loading."""
        with patch.object(self.engine.plugin_manager, "add_search_path") as mock_add_path:
            with patch.object(
                self.engine.plugin_manager, "discover_plugins", return_value=["plugin1", "plugin2"]
            ) as mock_discover:
                with patch.object(self.engine.plugin_manager, "load_plugin") as mock_load:

                    results = self.engine.discover_and_load_plugins(["/path1", "/path2"])

                    assert mock_add_path.call_count == 2
                    mock_discover.assert_called_once()
                    assert mock_load.call_count == 2
                    assert results == {"plugin1": True, "plugin2": True}

    def test_discover_and_load_plugins_partial_failure(self):
        """Test plugin discovery with some loading failures."""
        with patch.object(self.engine.plugin_manager, "add_search_path"):
            with patch.object(self.engine.plugin_manager, "discover_plugins", return_value=["plugin1", "plugin2"]):
                with patch.object(
                    self.engine.plugin_manager, "load_plugin", side_effect=[None, Exception("Load failed")]
                ):

                    results = self.engine.discover_and_load_plugins(["/path1"])

                    assert results == {"plugin1": True, "plugin2": False}

    def test_execute_scoring_success(self):
        """Test successful scoring execution."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        test_data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

        result = self.engine.execute_scoring("test_plugin", test_data)

        assert isinstance(result, ScoringResult)
        assert result.algorithm_name == "test_plugin"
        assert result.algorithm_version == "1.0.0"
        assert len(result.entity_scores) == 3

    def test_execute_scoring_with_parameters(self):
        """Test scoring execution with parameters."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        test_data = pd.DataFrame({"column1": [1, 2, 3]})
        parameters = {"param1": "test_value"}

        result = self.engine.execute_scoring("test_plugin", test_data, parameters)

        assert result.metadata["parameters"] == parameters

    def test_execute_scoring_algorithm_not_found(self):
        """Test scoring execution with non - existent algorithm."""
        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        with pytest.raises(AlgorithmNotFoundError, match="Algorithm 'nonexistent' not found"):
            self.engine.execute_scoring("nonexistent", test_data)

    def test_execute_scoring_plugin_failure(self):
        """Test scoring execution with plugin failure."""
        plugin = FailingScoringPlugin()
        self.engine.register_plugin(plugin)

        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        with pytest.raises(ScoringExecutionError, match="Scoring execution failed"):
            self.engine.execute_scoring("failing_plugin", test_data)

    def test_execute_scoring_input_validation_failure(self):
        """Test scoring execution with input validation failure."""
        plugin = InvalidInputPlugin()
        self.engine.register_plugin(plugin)

        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        with pytest.raises(ScoringExecutionError, match="Scoring execution failed"):
            self.engine.execute_scoring("invalid_input_plugin", test_data)

    def test_execute_scoring_without_isolation(self):
        """Test scoring execution without plugin isolation."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)
        self.engine.set_isolation_settings(enable_isolation=False)

        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        result = self.engine.execute_scoring("test_plugin", test_data)

        assert isinstance(result, ScoringResult)
        assert result.algorithm_name == "test_plugin"

    @patch("signal.signal")
    @patch("signal.alarm")
    @patch("resource.setrlimit")
    def test_execute_with_isolation_success(self, mock_setrlimit, mock_alarm, mock_signal):
        """Test successful execution with isolation."""
        plugin = MockScoringPlugin()
        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        result = self.engine._execute_with_isolation(plugin, test_data)

        assert isinstance(result, ScoringResult)
        mock_setrlimit.assert_called_once()
        mock_alarm.assert_called()
        mock_signal.assert_called()

    @patch("signal.signal")
    @patch("signal.alarm")
    def test_execute_with_isolation_timeout(self, mock_alarm, mock_signal):
        """Test execution with timeout."""
        plugin = MockScoringPlugin()
        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        # Mock timeout by raising TimeoutError
        with patch.object(plugin, "execute", side_effect=TimeoutError("Timeout")):
            with pytest.raises(ScoringExecutionError, match="Plugin execution timed out"):
                self.engine._execute_with_isolation(plugin, test_data)

    def test_get_available_algorithms(self):
        """Test getting available algorithms."""
        plugin1 = MockScoringPlugin()
        self.engine.register_plugin(plugin1)

        algorithms = self.engine.get_available_algorithms()
        assert "test_plugin" in algorithms

    def test_validate_plugin(self):
        """Test plugin validation."""
        plugin = MockScoringPlugin()
        result = self.engine.validate_plugin(plugin)

        assert result.is_valid
        assert len(result.errors) == 0

    def test_get_plugin_metadata(self):
        """Test getting plugin metadata."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        metadata = self.engine.get_plugin_metadata("test_plugin")

        assert metadata["name"] == "test_plugin"
        assert metadata["version"] == "1.0.0"
        assert metadata["parameters"] == {"param1": "value1"}

    def test_get_plugin_metadata_not_found(self):
        """Test getting metadata for non - existent plugin."""
        with pytest.raises(AlgorithmNotFoundError, match="Algorithm 'nonexistent' not found"):
            self.engine.get_plugin_metadata("nonexistent")

    def test_validate_all_plugins(self):
        """Test validating all plugins."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        results = self.engine.validate_all_plugins()

        assert "test_plugin" in results
        assert results["test_plugin"].is_valid

    def test_unload_plugin(self):
        """Test unloading plugin."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        assert "test_plugin" in self.engine.get_available_algorithms()

        self.engine.unload_plugin("test_plugin")

        assert "test_plugin" not in self.engine.get_available_algorithms()

    def test_reload_plugin_success(self):
        """Test successful plugin reload."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        with patch.object(self.engine.plugin_manager, "reload_plugin") as mock_reload:
            self.engine.reload_plugin("test_plugin")
            mock_reload.assert_called_once_with("test_plugin")

    def test_reload_plugin_failure(self):
        """Test plugin reload failure."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        with patch.object(self.engine.plugin_manager, "reload_plugin", side_effect=Exception("Reload failed")):
            with pytest.raises(ScoringEngineError, match="Plugin reload failed"):
                self.engine.reload_plugin("test_plugin")

    def test_set_isolation_settings(self):
        """Test setting isolation settings."""
        self.engine.set_isolation_settings(
            enable_isolation=False, max_execution_time=600, max_memory_usage=2048 * 1024 * 1024
        )

        assert self.engine._enable_plugin_isolation is False
        assert self.engine._max_execution_time == 600
        assert self.engine._max_memory_usage == 2048 * 1024 * 1024

    def test_get_system_status(self):
        """Test getting system status."""
        plugin = MockScoringPlugin()
        self.engine.register_plugin(plugin)

        status = self.engine.get_system_status()

        assert status["loaded_plugins"] == 1
        assert "test_plugin" in status["available_algorithms"]
        assert status["isolation_enabled"] is True
        assert status["max_execution_time"] == 300
        assert status["max_memory_usage"] == 1024 * 1024 * 1024
        assert isinstance(status["search_paths"], list)

    def test_logging_configuration(self):
        """Test that logging is properly configured."""
        # Verify logger is created
        assert hasattr(self.engine, "_logger")
        assert isinstance(self.engine._logger, logging.Logger)
        assert self.engine._logger.name == "src.data_organization.scoring_engine"
