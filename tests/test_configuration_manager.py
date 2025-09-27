"""
Tests for configuration management system for scoring parameters.

This module tests the ConfigurationManager, ScoringConfig, and related
configuration management functionality for the scoring system.
"""

from datetime import datetime
import os
import tempfile
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from src.data_organization.configuration_manager import (
    ConfigChange,
    ConfigurationError,
    ConfigurationManager,
    EnvironmentConfig,
    ParameterValidationError,
    ScoringConfig,
    ValidationResult,
)


class TestScoringConfig:
    """Test ScoringConfig data class functionality."""

    def test_scoring_config_creation(self):
        """Test creating a ScoringConfig instance."""
        config = ScoringConfig(
            algorithm_name="momentum_scoring",
            version="1.0.0",
            parameters={"threshold": 0.5, "window_days": 30},
            environment="development",
        )

        assert config.algorithm_name == "momentum_scoring"
        assert config.version == "1.0.0"
        assert config.parameters["threshold"] == 0.5
        assert config.environment == "development"
        assert isinstance(config.created_at, datetime)
        assert isinstance(config.updated_at, datetime)

    def test_scoring_config_validation_success(self):
        """Test successful validation of ScoringConfig."""
        config = ScoringConfig(
            algorithm_name="engagement_scoring",
            version="2.1.0",
            parameters={"min_comments": 10, "sentiment_weight": 0.7},
            environment="production",
        )

        result = config.validate()
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_scoring_config_validation_failure(self):
        """Test validation failure for invalid ScoringConfig."""
        config = ScoringConfig(
            algorithm_name="",  # Invalid empty name
            version="1.0.0",
            parameters={},
            environment="invalid_env",  # Invalid environment
        )

        result = config.validate()
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("algorithm_name" in error for error in result.errors)
        assert any("environment" in error for error in result.errors)

    def test_scoring_config_to_dict(self):
        """Test converting ScoringConfig to dictionary."""
        config = ScoringConfig(
            algorithm_name="growth_potential",
            version="1.5.0",
            parameters={"lookback_months": 6, "growth_threshold": 0.15},
            environment="staging",
        )

        config_dict = config.to_dict()
        assert config_dict["algorithm_name"] == "growth_potential"
        assert config_dict["version"] == "1.5.0"
        assert config_dict["parameters"]["lookback_months"] == 6
        assert config_dict["environment"] == "staging"
        assert "created_at" in config_dict
        assert "updated_at" in config_dict

    def test_scoring_config_from_env_vars(self):
        """Test creating ScoringConfig from environment variables."""
        env_vars = {
            "SCORING_MOMENTUM_ALGORITHM_NAME": "momentum_scoring",
            "SCORING_MOMENTUM_VERSION": "2.0.0",
            "SCORING_MOMENTUM_THRESHOLD": "0.6",
            "SCORING_MOMENTUM_WINDOW_DAYS": "45",
            "SCORING_MOMENTUM_ENVIRONMENT": "production",
        }

        with patch.dict(os.environ, env_vars):
            config = ScoringConfig.from_env_vars("SCORING_MOMENTUM_")

        assert config.algorithm_name == "momentum_scoring"
        assert config.version == "2.0.0"
        assert config.parameters["threshold"] == 0.6
        assert config.parameters["window_days"] == 45
        assert config.environment == "production"


class TestConfigurationManager:
    """Test ConfigurationManager functionality."""

    @pytest.fixture
    def mock_database(self):
        """Mock database connection for testing."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value = mock_cursor
        return mock_db, mock_cursor

    @pytest.fixture
    def config_manager(self, mock_database):
        """Create ConfigurationManager instance for testing."""
        mock_db, _ = mock_database
        return ConfigurationManager(database_connection=mock_db)

    def test_load_scoring_config_from_env(self, config_manager):
        """Test loading scoring configuration from environment variables."""
        env_vars = {
            "SCORING_ENGAGEMENT_ALGORITHM_NAME": "engagement_scoring",
            "SCORING_ENGAGEMENT_VERSION": "1.0.0",
            "SCORING_ENGAGEMENT_MIN_COMMENTS": "5",
            "SCORING_ENGAGEMENT_SENTIMENT_WEIGHT": "0.8",
            "SCORING_ENGAGEMENT_ENVIRONMENT": "development",
        }

        with patch.dict(os.environ, env_vars, clear=False):
            # Test direct loading from env vars
            config = ScoringConfig.from_env_vars("SCORING_ENGAGEMENT_")

        assert config.algorithm_name == "engagement_scoring"
        assert config.version == "1.0.0"
        assert config.parameters["min_comments"] == 5
        assert config.parameters["sentiment_weight"] == 0.8

    def test_load_scoring_config_from_database(self, config_manager, mock_database):
        """Test loading scoring configuration from database."""
        _, mock_cursor = mock_database

        # Mock database response
        mock_cursor.fetchone.return_value = {
            "algorithm_name": "momentum_scoring",
            "version": "2.0.0",
            "parameters": '{"threshold": 0.7, "window_days": 30}',
            "environment": "production",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }

        config = config_manager.load_scoring_config("momentum_scoring")

        assert config.algorithm_name == "momentum_scoring"
        assert config.version == "2.0.0"
        assert config.parameters["threshold"] == 0.7
        assert config.parameters["window_days"] == 30

    def test_validate_parameters_success(self, config_manager):
        """Test successful parameter validation."""
        config = ScoringConfig(
            algorithm_name="momentum_scoring",
            version="1.0.0",
            parameters={"threshold": 0.5, "window_days": 30},
            environment="development",
        )

        result = config_manager.validate_parameters(config)
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_parameters_failure(self, config_manager):
        """Test parameter validation failure."""
        config = ScoringConfig(
            algorithm_name="momentum_scoring",
            version="1.0.0",
            parameters={"threshold": 1.5, "window_days": -10},  # Invalid values
            environment="development",
        )

        result = config_manager.validate_parameters(config)
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("threshold" in error for error in result.errors)
        assert any("window_days" in error for error in result.errors)

    def test_update_configuration(self, config_manager, mock_database):
        """Test updating configuration in database."""
        _, mock_cursor = mock_database

        new_config = {
            "threshold": 0.8,
            "window_days": 45,
            "min_engagement": 0.1,
        }

        config_manager.update_configuration("momentum_scoring", new_config)

        # Verify database update was called
        mock_cursor.execute.assert_called()
        call_args = mock_cursor.execute.call_args[0]
        assert "UPDATE" in call_args[0]
        assert "momentum_scoring" in str(call_args[1])

    def test_get_environment_config(self, config_manager):
        """Test getting environment - specific configuration."""
        env_vars = {
            "ENVIRONMENT": "staging",
            "DATABASE_URL": "mysql://localhost / test",
            "DEBUG_MODE": "true",
            "MAX_WORKERS": "4",
        }

        with patch.dict(os.environ, env_vars):
            # Create a new config manager to pick up the env vars
            from src.data_organization.configuration_manager import ConfigurationManager

            new_config_manager = ConfigurationManager(database_connection=config_manager.database_connection)
            env_config = new_config_manager.get_environment_config()

        assert env_config.environment == "staging"
        assert env_config.database_url == "mysql://localhost / test"
        assert env_config.debug_mode == "enabled"
        assert env_config.max_workers == 4

    def test_audit_configuration_changes(self, config_manager, mock_database):
        """Test auditing configuration changes."""
        _, mock_cursor = mock_database

        changes = [
            ConfigChange(
                algorithm_name="momentum_scoring",
                parameter_name="threshold",
                old_value=0.5,
                new_value=0.7,
                changed_by="admin",
                change_reason="Performance optimization",
            ),
            ConfigChange(
                algorithm_name="engagement_scoring",
                parameter_name="min_comments",
                old_value=10,
                new_value=5,
                changed_by="analyst",
                change_reason="Lower threshold for better coverage",
            ),
        ]

        config_manager.audit_configuration_changes(changes)

        # Verify audit log entries were created
        assert mock_cursor.execute.call_count >= len(changes)

    def test_configuration_error_handling(self, config_manager):
        """Test error handling for configuration operations."""
        # Test with invalid algorithm name
        with pytest.raises(ConfigurationError):
            config_manager.load_scoring_config("")

        # Test with invalid parameters - this should return validation result, not raise exception
        invalid_config = ScoringConfig(
            algorithm_name="test_algorithm",
            version="1.0.0",
            parameters={"invalid_param": "invalid_value"},
            environment="development",
        )

        # validate_parameters returns ValidationResult, doesn't raise exception
        result = config_manager.validate_parameters(invalid_config)
        # For unknown algorithm, it should still be valid but with warnings
        assert result.is_valid is True  # Structure is valid, just unknown algorithm

    def test_parameter_type_conversion(self, config_manager):
        """Test automatic parameter type conversion from environment variables."""
        env_vars = {
            "SCORING_TEST_FLOAT_PARAM": "0.75",
            "SCORING_TEST_INT_PARAM": "42",
            "SCORING_TEST_ENABLED_PARAM": "enabled",
            "SCORING_TEST_STRING_PARAM": "test_value",
        }

        with patch.dict(os.environ, env_vars):
            config = ScoringConfig.from_env_vars("SCORING_TEST_")

        assert isinstance(config.parameters["float_param"], float)
        assert config.parameters["float_param"] == 0.75
        assert isinstance(config.parameters["int_param"], int)
        assert config.parameters["int_param"] == 42
        assert isinstance(config.parameters["enabled_param"], str)
        assert config.parameters["enabled_param"] == "enabled"
        assert isinstance(config.parameters["string_param"], str)
        assert config.parameters["string_param"] == "test_value"


class TestEnvironmentConfig:
    """Test EnvironmentConfig functionality."""

    def test_environment_config_creation(self):
        """Test creating EnvironmentConfig instance."""
        config = EnvironmentConfig(
            environment="production",
            database_url="mysql://prod - server / analytics",
            debug_mode=False,
            max_workers=8,
            log_level="INFO",
        )

        assert config.environment == "production"
        assert config.database_url == "mysql://prod - server / analytics"
        assert config.debug_mode is False
        assert config.max_workers == 8
        assert config.log_level == "INFO"

    def test_environment_config_from_env_vars(self):
        """Test creating EnvironmentConfig from environment variables."""
        env_vars = {
            "ENVIRONMENT": "staging",
            "DATABASE_URL": "mysql://staging - server / analytics",
            "DEBUG_MODE": "true",
            "MAX_WORKERS": "6",
            "LOG_LEVEL": "DEBUG",
        }

        with patch.dict(os.environ, env_vars):
            config = EnvironmentConfig.from_env_vars()

        assert config.environment == "staging"
        assert config.database_url == "mysql://staging - server / analytics"
        assert config.debug_mode == "enabled"
        assert config.max_workers == 6
        assert config.log_level == "debug"


class TestValidationResult:
    """Test ValidationResult functionality."""

    def test_validation_result_creation(self):
        """Test creating ValidationResult instance."""
        result = ValidationResult(
            is_valid=False,
            errors=["Parameter 'threshold' must be between 0 and 1"],
            warnings=["Parameter 'window_days' is using default value"],
            checked_items=5,
            passed_items=4,
            metadata={"validation_time": 0.05},
        )

        assert result.is_valid is False
        assert len(result.errors) == 1
        assert len(result.warnings) == 1
        assert result.checked_items == 5
        assert result.passed_items == 4
        assert result.metadata["validation_time"] == 0.05

    def test_add_error_and_warning(self):
        """Test adding errors and warnings to ValidationResult."""
        result = ValidationResult()

        result.add_error("Critical error occurred")
        result.add_warning("Minor issue detected")

        assert len(result.errors) == 1
        assert len(result.warnings) == 1
        assert result.errors[0] == "Critical error occurred"
        assert result.warnings[0] == "Minor issue detected"

    def test_merge_validation_results(self):
        """Test merging multiple ValidationResult instances."""
        result1 = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Warning 1"],
            checked_items=3,
            passed_items=3,
        )

        result2 = ValidationResult(
            is_valid=False,
            errors=["Error 1"],
            warnings=["Warning 2"],
            checked_items=2,
            passed_items=1,
        )

        merged = result1.merge(result2)

        assert merged.is_valid is False  # False if any result is invalid
        assert len(merged.errors) == 1
        assert len(merged.warnings) == 2
        assert merged.checked_items == 5
        assert merged.passed_items == 4


class TestConfigChange:
    """Test ConfigChange functionality."""

    def test_config_change_creation(self):
        """Test creating ConfigChange instance."""
        change = ConfigChange(
            algorithm_name="momentum_scoring",
            parameter_name="threshold",
            old_value=0.5,
            new_value=0.7,
            changed_by="admin",
            change_reason="Performance optimization",
        )

        assert change.algorithm_name == "momentum_scoring"
        assert change.parameter_name == "threshold"
        assert change.old_value == 0.5
        assert change.new_value == 0.7
        assert change.changed_by == "admin"
        assert change.change_reason == "Performance optimization"
        assert isinstance(change.change_timestamp, datetime)

    def test_config_change_to_dict(self):
        """Test converting ConfigChange to dictionary."""
        change = ConfigChange(
            algorithm_name="engagement_scoring",
            parameter_name="min_comments",
            old_value=10,
            new_value=5,
            changed_by="analyst",
            change_reason="Lower threshold for better coverage",
        )

        change_dict = change.to_dict()
        assert change_dict["algorithm_name"] == "engagement_scoring"
        assert change_dict["parameter_name"] == "min_comments"
        assert change_dict["old_value"] == 10
        assert change_dict["new_value"] == 5
        assert change_dict["changed_by"] == "analyst"
        assert "change_timestamp" in change_dict


if __name__ == "__main__":
    pytest.main([__file__])
