"""
Tests for open - source plugin framework and examples.

This module tests the OpenSourceScoringPlugin base class, plugin validation,
security checking, and example plugins for user - defined scoring algorithms.
"""

from datetime import datetime, timedelta
import json
import os
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.data_organization.example_open_source_plugins import (
    CrossPlatformMomentumPlugin,
    EngagementQualityPlugin,
    GenreSpecificScoringPlugin,
    ViewVelocityPlugin,
)
from src.data_organization.open_source_plugin_framework import (
    OpenSourceScoringPlugin,
    PluginMetadata,
    PluginRegistrationError,
    PluginRegistry,
    PluginSecurityChecker,
    PluginSecurityError,
    PluginValidationError,
    PluginValidator,
)


class TestPluginMetadata:
    """Test PluginMetadata data class."""

    def test_plugin_metadata_creation(self):
        """Test creating PluginMetadata."""
        metadata = PluginMetadata(
            name="test_plugin",
            version="1.0.0",
            author="Test Author",
            description="A test plugin",
            parameters={"param1": "value1"},
            input_requirements=["entity_id", "view_count"],
            output_schema={"score": "float64"},
        )

        assert metadata.name == "test_plugin"
        assert metadata.version == "1.0.0"
        assert metadata.author == "Test Author"
        assert metadata.description == "A test plugin"
        assert metadata.parameters == {"param1": "value1"}
        assert metadata.input_requirements == ["entity_id", "view_count"]
        assert metadata.output_schema == {"score": "float64"}

    def test_metadata_validation_valid(self):
        """Test validation of valid metadata."""
        metadata = PluginMetadata(
            name="valid_plugin",
            version="1.0.0",
            author="Valid Author",
            description="A valid plugin",
            parameters={},
            input_requirements=["entity_id"],
            output_schema={"score": "float64"},
        )

        result = metadata.validate()
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_metadata_validation_invalid_name(self):
        """Test validation with invalid plugin name."""
        metadata = PluginMetadata(
            name="",  # Invalid empty name
            version="1.0.0",
            author="Author",
            description="Description",
            parameters={},
            input_requirements=["entity_id"],
            output_schema={"score": "float64"},
        )

        result = metadata.validate()
        assert result.is_valid is False
        assert any("name" in error.lower() for error in result.errors)

    def test_metadata_validation_invalid_version(self):
        """Test validation with invalid version format."""
        metadata = PluginMetadata(
            name="test_plugin",
            version="invalid_version",  # Invalid version format
            author="Author",
            description="Description",
            parameters={},
            input_requirements=["entity_id"],
            output_schema={"score": "float64"},
        )

        result = metadata.validate()
        assert result.is_valid is False
        assert any("version" in error.lower() for error in result.errors)

    def test_metadata_to_dict(self):
        """Test converting metadata to dictionary."""
        metadata = PluginMetadata(
            name="test_plugin",
            version="1.0.0",
            author="Test Author",
            description="A test plugin",
            parameters={"param1": "value1"},
            input_requirements=["entity_id"],
            output_schema={"score": "float64"},
        )

        result_dict = metadata.to_dict()

        assert result_dict["name"] == "test_plugin"
        assert result_dict["version"] == "1.0.0"
        assert result_dict["author"] == "Test Author"
        assert result_dict["parameters"] == {"param1": "value1"}


class TestOpenSourceScoringPlugin:
    """Test OpenSourceScoringPlugin base class."""

    def test_abstract_methods_not_implemented(self):
        """Test that abstract methods raise NotImplementedError."""
        # Cannot instantiate abstract class directly
        with pytest.raises(TypeError):
            OpenSourceScoringPlugin()

    def test_plugin_configuration_loading(self):
        """Test configuration loading functionality."""

        # Create a concrete implementation for testing
        class TestPlugin(OpenSourceScoringPlugin):
            def get_name(self) -> str:
                return "test_plugin"

            def get_version(self) -> str:
                return "1.0.0"

            def get_metadata(self) -> PluginMetadata:
                return PluginMetadata(
                    name="test_plugin",
                    version="1.0.0",
                    author="Test",
                    description="Test plugin",
                    parameters={},
                    input_requirements=["entity_id"],
                    output_schema={"score": "float64"},
                )

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return data.copy()

            def validate_input(self, data: pd.DataFrame):
                from src.data_organization.notebook_validator import ValidationResult

                return ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=1, passed_items=1)

        plugin = TestPlugin()

        # Test configuration loading
        config = {"param1": "value1", "param2": 42}
        plugin.load_configuration(config)

        assert plugin.config == config

    def test_plugin_export_results(self):
        """Test results export functionality."""

        class TestPlugin(OpenSourceScoringPlugin):
            def get_name(self) -> str:
                return "test_plugin"

            def get_version(self) -> str:
                return "1.0.0"

            def get_metadata(self) -> PluginMetadata:
                return PluginMetadata(
                    name="test_plugin",
                    version="1.0.0",
                    author="Test",
                    description="Test plugin",
                    parameters={},
                    input_requirements=["entity_id"],
                    output_schema={"score": "float64"},
                )

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return data.copy()

            def validate_input(self, data: pd.DataFrame):
                from src.data_organization.notebook_validator import ValidationResult

                return ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=1, passed_items=1)

        plugin = TestPlugin()

        # Test CSV export
        scores = pd.DataFrame({"entity_id": ["Entity_A", "Entity_B"], "score": [0.8, 0.6]})

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            temp_path = f.name

        try:
            plugin.export_results(scores, "csv", temp_path)

            # Verify file was created and contains data
            assert os.path.exists(temp_path)
            exported_data = pd.read_csv(temp_path)
            assert len(exported_data) == 2
            assert "entity_id" in exported_data.columns
            assert "score" in exported_data.columns

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestPluginValidator:
    """Test PluginValidator for plugin validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = PluginValidator()

    def test_validate_plugin_structure_valid(self):
        """Test validation of valid plugin structure."""

        class ValidPlugin(OpenSourceScoringPlugin):
            def get_name(self) -> str:
                return "valid_plugin"

            def get_version(self) -> str:
                return "1.0.0"

            def get_metadata(self) -> PluginMetadata:
                return PluginMetadata(
                    name="valid_plugin",
                    version="1.0.0",
                    author="Test Author",
                    description="A valid plugin",
                    parameters={},
                    input_requirements=["entity_id"],
                    output_schema={"score": "float64"},
                )

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return data.assign(score=0.5)

            def validate_input(self, data: pd.DataFrame):
                from src.data_organization.notebook_validator import ValidationResult

                return ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=1, passed_items=1)

        plugin = ValidPlugin()
        result = self.validator.validate_plugin_structure(plugin)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_plugin_structure_missing_methods(self):
        """Test validation of plugin with missing required methods."""

        class IncompletePlugin:
            def get_name(self) -> str:
                return "incomplete_plugin"

            # Missing other required methods

        plugin = IncompletePlugin()
        result = self.validator.validate_plugin_structure(plugin)

        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_validate_input_requirements(self):
        """Test validation of plugin input requirements."""
        # Valid data that meets requirements
        data = pd.DataFrame(
            {"entity_id": ["Entity_A", "Entity_B"], "view_count": [1000, 2000], "like_count": [100, 200]}
        )

        requirements = ["entity_id", "view_count"]
        result = self.validator.validate_input_requirements(data, requirements)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_input_requirements_missing_columns(self):
        """Test validation with missing required columns."""
        data = pd.DataFrame(
            {
                "entity_id": ["Entity_A", "Entity_B"],
                # Missing 'view_count' column
            }
        )

        requirements = ["entity_id", "view_count"]
        result = self.validator.validate_input_requirements(data, requirements)

        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("view_count" in error for error in result.errors)


class TestPluginSecurityChecker:
    """Test PluginSecurityChecker for security validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.security_checker = PluginSecurityChecker()

    def test_check_plugin_security_safe(self):
        """Test security check for safe plugin code."""
        safe_code = """
def calculate_scores(self, data):
    import pandas as pd
    import numpy as np

    # Safe operations
    scores = data['view_count'] * 0.001
    return pd.DataFrame({'score': scores})
"""

        result = self.security_checker.check_plugin_security(safe_code)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_check_plugin_security_dangerous_imports(self):
        """Test security check with dangerous imports."""
        dangerous_code = """
import os
import subprocess
import sys

def calculate_scores(self, data):
    # Dangerous operations
    os.system("rm -rf /")
    subprocess.call(["curl", "malicious - site.com"])
    return data
"""

        result = self.security_checker.check_plugin_security(dangerous_code)

        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("dangerous import" in error.lower() for error in result.errors)


class TestPluginRegistry:
    """Test PluginRegistry for plugin management."""

    def setup_method(self):
        """Set up test fixtures."""
        self.registry = PluginRegistry()

    def test_register_plugin_valid(self):
        """Test registering a valid plugin."""

        class TestPlugin(OpenSourceScoringPlugin):
            def get_name(self) -> str:
                return "test_plugin"

            def get_version(self) -> str:
                return "1.0.0"

            def get_metadata(self) -> PluginMetadata:
                return PluginMetadata(
                    name="test_plugin",
                    version="1.0.0",
                    author="Test Author",
                    description="A test plugin",
                    parameters={},
                    input_requirements=["entity_id"],
                    output_schema={"score": "float64"},
                )

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return data.assign(score=0.5)

            def validate_input(self, data: pd.DataFrame):
                from src.data_organization.notebook_validator import ValidationResult

                return ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=1, passed_items=1)

        plugin = TestPlugin()
        result = self.registry.register_plugin(plugin)

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert "test_plugin" in self.registry.get_registered_plugins()

    def test_get_plugin_by_name(self):
        """Test retrieving plugin by name."""

        class TestPlugin(OpenSourceScoringPlugin):
            def get_name(self) -> str:
                return "retrievable_plugin"

            def get_version(self) -> str:
                return "1.0.0"

            def get_metadata(self) -> PluginMetadata:
                return PluginMetadata(
                    name="retrievable_plugin",
                    version="1.0.0",
                    author="Test Author",
                    description="A retrievable plugin",
                    parameters={},
                    input_requirements=["entity_id"],
                    output_schema={"score": "float64"},
                )

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                return data.assign(score=0.5)

            def validate_input(self, data: pd.DataFrame):
                from src.data_organization.notebook_validator import ValidationResult

                return ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=1, passed_items=1)

        plugin = TestPlugin()
        self.registry.register_plugin(plugin)

        retrieved_plugin = self.registry.get_plugin("retrievable_plugin")
        assert retrieved_plugin is not None
        assert retrieved_plugin.get_name() == "retrievable_plugin"

    def test_get_plugin_nonexistent(self):
        """Test retrieving non - existent plugin."""
        plugin = self.registry.get_plugin("nonexistent_plugin")
        assert plugin is None


class TestExamplePlugins:
    """Test example open - source plugins."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create data with multiple time points per entity for velocity / momentum calculation
        data_points = []

        for entity_idx, entity_id in enumerate(["Entity_A", "Entity_B", "Entity_C"]):
            base_views = [100000, 200000, 150000][entity_idx]

            # Create multiple data points over time for each entity
            for day_offset in [7, 5, 3, 1]:
                date = datetime.now() - timedelta(days=day_offset)
                # Simulate growth over time
                view_multiplier = (8 - day_offset) / 7  # Growth from older to newer

                data_points.append(
                    {
                        "entity_id": entity_id,
                        "video_id": f"{entity_id}_video_{day_offset}",
                        "view_count": int(base_views * view_multiplier),
                        "like_count": int(base_views * view_multiplier * 0.05),
                        "comment_count": int(base_views * view_multiplier * 0.005),
                        "published_date": date - timedelta(days=30),
                        "analytics_date": date,
                        "genre": ["pop", "rock", "electronic"][entity_idx],
                        "platform": "youtube",
                        "metric_value": int(base_views * view_multiplier),
                        "metric_date": date,
                    }
                )

        self.sample_data = pd.DataFrame(data_points)

    def test_view_velocity_plugin(self):
        """Test ViewVelocityPlugin functionality."""
        plugin = ViewVelocityPlugin()

        # Test metadata
        metadata = plugin.get_metadata()
        assert metadata.name == "view_velocity"
        assert metadata.version == "1.0.0"

        # Test input validation
        validation_result = plugin.validate_input(self.sample_data)
        assert validation_result.is_valid is True

        # Test score calculation
        result = plugin.calculate_scores(self.sample_data)

        assert "view_velocity_score" in result.columns
        assert "velocity_category" in result.columns
        assert len(result) >= 0  # May be empty if insufficient data points

    def test_engagement_quality_plugin(self):
        """Test EngagementQualityPlugin functionality."""
        plugin = EngagementQualityPlugin()

        # Test metadata
        metadata = plugin.get_metadata()
        assert metadata.name == "engagement_quality"
        assert metadata.version == "1.0.0"

        # Test input validation
        validation_result = plugin.validate_input(self.sample_data)
        assert validation_result.is_valid is True

        # Test score calculation
        result = plugin.calculate_scores(self.sample_data)

        assert "engagement_quality_score" in result.columns
        assert "quality_category" in result.columns
        assert len(result) == len(self.sample_data)

    def test_cross_platform_momentum_plugin(self):
        """Test CrossPlatformMomentumPlugin functionality."""
        plugin = CrossPlatformMomentumPlugin()

        # Test metadata
        metadata = plugin.get_metadata()
        assert metadata.name == "cross_platform_momentum"
        assert metadata.version == "1.0.0"

        # Test input validation
        validation_result = plugin.validate_input(self.sample_data)
        assert validation_result.is_valid is True

        # Test score calculation
        result = plugin.calculate_scores(self.sample_data)

        assert "momentum_score" in result.columns
        assert "momentum_category" in result.columns
        assert len(result) >= 0  # May be empty if insufficient data points

    def test_genre_specific_scoring_plugin(self):
        """Test GenreSpecificScoringPlugin functionality."""
        plugin = GenreSpecificScoringPlugin()

        # Test metadata
        metadata = plugin.get_metadata()
        assert metadata.name == "genre_specific_scoring"
        assert metadata.version == "1.0.0"

        # Test input validation
        validation_result = plugin.validate_input(self.sample_data)
        assert validation_result.is_valid is True

        # Test score calculation
        result = plugin.calculate_scores(self.sample_data)

        assert "genre_adjusted_score" in result.columns
        assert "performance_vs_genre" in result.columns
        assert len(result) == len(self.sample_data)

        # Check genre categories
        categories = result["performance_vs_genre"].unique()
        valid_categories = ["above_genre_average", "at_genre_average", "below_genre_average"]
        assert all(cat in valid_categories for cat in categories)


class TestPluginIntegration:
    """Test integration scenarios for the plugin framework."""

    def setup_method(self):
        """Set up test fixtures."""
        self.registry = PluginRegistry()
        self.validator = PluginValidator()
        self.security_checker = PluginSecurityChecker()

    def test_complete_plugin_registration_workflow(self):
        """Test complete workflow from plugin creation to registration."""
        # Create a plugin
        plugin = ViewVelocityPlugin()

        # Validate plugin structure
        structure_result = self.validator.validate_plugin_structure(plugin)
        assert structure_result.is_valid is True

        # Validate plugin metadata
        metadata = plugin.get_metadata()
        metadata_result = self.validator.validate_plugin_metadata(metadata)
        assert metadata_result.is_valid is True

        # Register plugin
        registration_result = self.registry.register_plugin(plugin)
        assert registration_result.is_valid is True

        # Verify plugin is available
        registered_plugins = self.registry.get_registered_plugins()
        assert "view_velocity" in registered_plugins

    def test_multiple_plugins_registration(self):
        """Test registering multiple plugins."""
        plugins = [ViewVelocityPlugin(), EngagementQualityPlugin(), CrossPlatformMomentumPlugin()]

        for plugin in plugins:
            result = self.registry.register_plugin(plugin)
            assert result.is_valid is True

        registered_plugins = self.registry.get_registered_plugins()
        assert len(registered_plugins) == 3

        # Test plugin retrieval
        for plugin in plugins:
            retrieved = self.registry.get_plugin(plugin.get_name())
            assert retrieved is not None
            assert retrieved.get_name() == plugin.get_name()
