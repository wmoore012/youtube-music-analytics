"""Tests for plugin discovery and loading mechanisms."""

import os
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_organization.plugin_manager import (
    PluginDiscoveryError,
    PluginLoadingError,
    PluginManager,
    PluginValidationError,
)
from src.data_organization.scoring_plugin import ScoringPlugin, ValidationResult


class MockScoringPlugin(ScoringPlugin):
    """Test scoring plugin for plugin manager tests."""

    def get_name(self) -> str:
        return "test_plugin"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> dict:
        return {"param1": "value1"}

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"entity_id": ["test1", "test2"], "score_value": [0.8, 0.6]})

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        return ValidationResult(is_valid=True, checked_items=1, passed_items=1)


class InvalidScoringPlugin:
    """Invalid plugin that doesn't inherit from ScoringPlugin."""

    pass


class AbstractScoringPlugin(ScoringPlugin):
    """Abstract plugin that can't be instantiated."""

    def get_name(self) -> str:
        return "abstract_plugin"

    # Missing required method implementations to make it abstract


class TestPluginManager:
    """Test PluginManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.plugin_manager = PluginManager()

    def test_plugin_manager_initialization(self):
        """Test plugin manager initialization."""
        assert len(self.plugin_manager._plugins) == 0
        assert len(self.plugin_manager._plugin_instances) == 0
        assert len(self.plugin_manager._search_paths) == 0

    def test_add_search_path_success(self):
        """Test adding valid search path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            self.plugin_manager.add_search_path(temp_dir)
            assert Path(temp_dir) in self.plugin_manager._search_paths

    def test_add_search_path_nonexistent(self):
        """Test adding non-existent search path."""
        with pytest.raises(PluginDiscoveryError, match="does not exist"):
            self.plugin_manager.add_search_path("/nonexistent/path")

    def test_add_search_path_not_directory(self):
        """Test adding file as search path."""
        with tempfile.NamedTemporaryFile() as temp_file:
            with pytest.raises(PluginDiscoveryError, match="not a directory"):
                self.plugin_manager.add_search_path(temp_file.name)

    def test_discover_plugins_empty_directory(self):
        """Test plugin discovery in empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            self.plugin_manager.add_search_path(temp_dir)
            plugins = self.plugin_manager.discover_plugins()
            assert plugins == []

    def test_discover_plugins_with_valid_plugin(self):
        """Test plugin discovery with valid plugin file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the entire discovery process to avoid complex file loading issues
            with patch.object(self.plugin_manager, "_discover_plugins_in_path") as mock_discover:
                mock_discover.return_value = ["test.module.DiscoveredPlugin"]

                self.plugin_manager.add_search_path(temp_dir)
                plugins = self.plugin_manager.discover_plugins()

                assert len(plugins) == 1
                assert "test.module.DiscoveredPlugin" in plugins

    def test_load_plugin_success(self):
        """Test successful plugin loading."""
        # Register the test plugin class manually for this test
        plugin_class = MockScoringPlugin

        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.MockScoringPlugin = plugin_class
            mock_import.return_value = mock_module

            self.plugin_manager.load_plugin("test_module.MockScoringPlugin")

            assert "test_plugin" in self.plugin_manager._plugins
            assert self.plugin_manager._plugins["test_plugin"] == plugin_class

    def test_load_plugin_invalid_path(self):
        """Test loading plugin with invalid class path."""
        with pytest.raises(PluginLoadingError, match="Invalid plugin class path"):
            self.plugin_manager.load_plugin("invalid_path")

    def test_load_plugin_module_not_found(self):
        """Test loading plugin with non-existent module."""
        with patch("importlib.import_module", side_effect=ImportError("Module not found")):
            with pytest.raises(PluginLoadingError, match="Failed to import plugin module"):
                self.plugin_manager.load_plugin("nonexistent.module.Plugin")

    def test_load_plugin_class_not_found(self):
        """Test loading plugin with non-existent class."""
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            # Simulate hasattr returning False for non-existent class
            mock_module.configure_mock(**{"NonExistentPlugin": None})
            del mock_module.NonExistentPlugin  # Remove the attribute
            mock_import.return_value = mock_module

            with pytest.raises(PluginLoadingError, match="Class NonExistentPlugin not found"):
                self.plugin_manager.load_plugin("test_module.NonExistentPlugin")

    def test_load_plugin_not_scoring_plugin_subclass(self):
        """Test loading plugin that's not a ScoringPlugin subclass."""
        with patch("importlib.import_module") as mock_import:
            mock_module = MagicMock()
            mock_module.InvalidPlugin = InvalidScoringPlugin
            mock_import.return_value = mock_module

            with pytest.raises(PluginValidationError, match="not a ScoringPlugin subclass"):
                self.plugin_manager.load_plugin("test_module.InvalidPlugin")

    def test_validate_plugin_class_success(self):
        """Test successful plugin class validation."""
        result = self.plugin_manager._validate_plugin_class(MockScoringPlugin)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_plugin_class_abstract(self):
        """Test validation of abstract plugin class."""
        with patch("inspect.isabstract", return_value=True):
            result = self.plugin_manager._validate_plugin_class(AbstractScoringPlugin)
            assert not result.is_valid
            assert "Plugin class is abstract" in result.errors[0]

    def test_validate_plugin_class_instantiation_failure(self):
        """Test validation when plugin instantiation fails."""

        class FailingPlugin(ScoringPlugin):
            def __init__(self):
                raise Exception("Instantiation failed")

            def get_name(self) -> str:
                return "failing"

            def get_version(self) -> str:
                return "1.0.0"

            def get_parameters(self) -> dict:
                return {}

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return pd.DataFrame()

            def validate_input(self, data: pd.DataFrame) -> ValidationResult:
                return ValidationResult(is_valid=True)

        result = self.plugin_manager._validate_plugin_class(FailingPlugin)
        assert not result.is_valid
        assert "Failed to instantiate plugin" in result.errors[0]

    def test_get_plugin_instance_success(self):
        """Test getting plugin instance."""
        # Manually register plugin
        self.plugin_manager._plugins["test_plugin"] = MockScoringPlugin

        instance = self.plugin_manager.get_plugin_instance("test_plugin")
        assert isinstance(instance, MockScoringPlugin)
        assert instance.get_name() == "test_plugin"

    def test_get_plugin_instance_not_loaded(self):
        """Test getting instance of non-loaded plugin."""
        with pytest.raises(PluginLoadingError, match="Plugin nonexistent not loaded"):
            self.plugin_manager.get_plugin_instance("nonexistent")

    def test_get_plugin_instance_caching(self):
        """Test plugin instance caching."""
        # Manually register plugin
        self.plugin_manager._plugins["test_plugin"] = MockScoringPlugin

        instance1 = self.plugin_manager.get_plugin_instance("test_plugin")
        instance2 = self.plugin_manager.get_plugin_instance("test_plugin")

        # Should return the same cached instance
        assert instance1 is instance2

    def test_get_available_plugins(self):
        """Test getting available plugins list."""
        # Manually register plugins
        self.plugin_manager._plugins["plugin1"] = MockScoringPlugin
        self.plugin_manager._plugins["plugin2"] = MockScoringPlugin

        available = self.plugin_manager.get_available_plugins()
        assert set(available) == {"plugin1", "plugin2"}

    def test_unload_plugin(self):
        """Test unloading plugin."""
        # Manually register plugin and instance
        self.plugin_manager._plugins["test_plugin"] = MockScoringPlugin
        self.plugin_manager._plugin_instances["test_plugin"] = MockScoringPlugin()

        self.plugin_manager.unload_plugin("test_plugin")

        assert "test_plugin" not in self.plugin_manager._plugins
        assert "test_plugin" not in self.plugin_manager._plugin_instances

    def test_unload_plugin_nonexistent(self):
        """Test unloading non-existent plugin."""
        # Should not raise error
        self.plugin_manager.unload_plugin("nonexistent")

    def test_reload_plugin_success(self):
        """Test successful plugin reload."""
        # Manually register plugin and instance
        plugin_class = MockScoringPlugin
        self.plugin_manager._plugins["test_plugin"] = plugin_class
        self.plugin_manager._plugin_instances["test_plugin"] = plugin_class()

        with patch("inspect.getmodule") as mock_getmodule:
            with patch("importlib.reload") as mock_reload:
                mock_module = MagicMock()
                mock_getmodule.return_value = mock_module

                self.plugin_manager.reload_plugin("test_plugin")

                mock_reload.assert_called_once_with(mock_module)
                assert "test_plugin" not in self.plugin_manager._plugin_instances

    def test_reload_plugin_not_loaded(self):
        """Test reloading non-loaded plugin."""
        with pytest.raises(PluginLoadingError, match="Plugin nonexistent not loaded"):
            self.plugin_manager.reload_plugin("nonexistent")

    def test_reload_plugin_no_module(self):
        """Test reloading plugin with no module."""
        self.plugin_manager._plugins["test_plugin"] = MockScoringPlugin

        with patch("inspect.getmodule", return_value=None):
            with pytest.raises(PluginLoadingError, match="Cannot find module"):
                self.plugin_manager.reload_plugin("test_plugin")

    def test_validate_all_plugins(self):
        """Test validating all loaded plugins."""
        # Manually register plugins
        self.plugin_manager._plugins["plugin1"] = MockScoringPlugin
        self.plugin_manager._plugins["plugin2"] = MockScoringPlugin

        results = self.plugin_manager.validate_all_plugins()

        assert len(results) == 2
        assert "plugin1" in results
        assert "plugin2" in results
        assert results["plugin1"].is_valid
        assert results["plugin2"].is_valid

    def test_clear_plugins(self):
        """Test clearing all plugins."""
        # Manually register plugins and instances
        self.plugin_manager._plugins["plugin1"] = MockScoringPlugin
        self.plugin_manager._plugins["plugin2"] = MockScoringPlugin
        self.plugin_manager._plugin_instances["plugin1"] = MockScoringPlugin()

        self.plugin_manager.clear_plugins()

        assert len(self.plugin_manager._plugins) == 0
        assert len(self.plugin_manager._plugin_instances) == 0

    def test_discover_plugins_with_search_paths_parameter(self):
        """Test plugin discovery with temporary search paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Should not permanently add the search path
            original_paths = self.plugin_manager._search_paths.copy()

            plugins = self.plugin_manager.discover_plugins([temp_dir])

            # Search paths should be restored
            assert self.plugin_manager._search_paths == original_paths

    def test_discover_plugins_handles_file_processing_errors(self):
        """Test that plugin discovery handles file processing errors gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a file that will cause processing errors
            bad_file = Path(temp_dir) / "bad_plugin.py"
            bad_file.write_text("invalid python syntax !!!")

            self.plugin_manager.add_search_path(temp_dir)

            # Should not raise exception, just return empty list
            plugins = self.plugin_manager.discover_plugins()
            assert plugins == []
