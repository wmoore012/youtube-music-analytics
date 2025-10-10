"""
Integration tests for the configuration management system.

This module tests the complete configuration management workflow including
database schema creation, configuration loading, validation, and auditing.
"""

import json
import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.data_organization.configuration_manager import (
    ConfigChange,
    ConfigurationManager,
    EnvironmentConfig,
    ScoringConfig,
)
from src.data_organization.configuration_schema_manager import ConfigurationSchemaManager


class TestConfigurationIntegration:
    """Integration tests for the complete configuration management system."""

    @pytest.fixture
    def mock_database(self):
        """Create a comprehensive mock database for testing."""
        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_db.cursor.return_value = mock_cursor

        # Mock successful operations
        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_db.commit.return_value = None
        mock_db.rollback.return_value = None

        return mock_db, mock_cursor

    @pytest.fixture
    def config_manager(self, mock_database):
        """Create ConfigurationManager with mock database."""
        mock_db, _ = mock_database
        return ConfigurationManager(database_connection=mock_db)

    @pytest.fixture
    def schema_manager(self, mock_database):
        """Create ConfigurationSchemaManager with mock database."""
        mock_db, _ = mock_database
        return ConfigurationSchemaManager(mock_db)

    def test_complete_configuration_workflow(self, config_manager, mock_database):
        """Test the complete configuration management workflow."""
        mock_db, mock_cursor = mock_database

        # Step 1: Load configuration from environment variables
        env_vars = {
            "SCORING_MOMENTUM_ALGORITHM_NAME": "momentum_scoring",
            "SCORING_MOMENTUM_VERSION": "2.0.0",
            "SCORING_MOMENTUM_THRESHOLD": "0.7",
            "SCORING_MOMENTUM_WINDOW_DAYS": "45",
            "SCORING_MOMENTUM_ENVIRONMENT": "production",
        }

        with patch.dict(os.environ, env_vars):
            config = ScoringConfig.from_env_vars("SCORING_MOMENTUM_")

        # Verify configuration loaded correctly
        assert config.algorithm_name == "momentum_scoring"
        assert config.version == "2.0.0"
        assert config.parameters["threshold"] == 0.7
        assert config.parameters["window_days"] == 45

        # Step 2: Validate the configuration
        validation_result = config_manager.validate_parameters(config)
        assert validation_result.is_valid is True
        assert len(validation_result.errors) == 0

        # Step 3: Update configuration in database
        new_parameters = {"threshold": 0.8, "window_days": 60, "min_videos": 10}

        # Mock successful database update
        mock_cursor.execute.return_value = None
        mock_db.commit.return_value = None

        config_manager.update_configuration("momentum_scoring", new_parameters)

        # Verify database operations were called
        assert mock_cursor.execute.called
        assert mock_db.commit.called

        # Step 4: Test configuration change auditing
        changes = [
            ConfigChange(
                algorithm_name="momentum_scoring",
                parameter_name="threshold",
                old_value=0.7,
                new_value=0.8,
                changed_by="test_user",
                change_reason="Integration test update",
            )
        ]

        config_manager.audit_configuration_changes(changes)

        # Verify audit logging was called
        assert mock_cursor.execute.call_count >= 2  # Update + audit

    def test_multi_environment_configuration(self, config_manager):
        """Test configuration management across multiple environments."""
        environments = ["development", "staging", "production"]

        for env in environments:
            env_vars = {
                "ENVIRONMENT": env,
                "DEBUG_MODE": "true" if env == "development" else "false",
                "MAX_WORKERS": "2" if env == "development" else "8",
            }

            with patch.dict(os.environ, env_vars):
                env_config = EnvironmentConfig.from_env_vars()

            assert env_config.environment == env
            assert env_config.debug_mode == ("enabled" if env == "development" else "disabled")

            # Validate environment configuration
            validation = env_config.validate()
            assert validation.is_valid is True

    def test_parameter_validation_across_algorithms(self, config_manager):
        """Test parameter validation for different scoring algorithms."""
        test_cases = [
            {
                "algorithm": "momentum_scoring",
                "valid_params": {"threshold": 0.5, "window_days": 30, "min_videos": 5},
                "invalid_params": {"threshold": 1.5, "window_days": -10, "min_videos": 2000},
            },
            {
                "algorithm": "engagement_scoring",
                "valid_params": {"min_comments": 10, "sentiment_weight": 0.7, "like_ratio_weight": 0.3},
                "invalid_params": {"min_comments": -5, "sentiment_weight": 1.5, "like_ratio_weight": -0.1},
            },
            {
                "algorithm": "growth_potential",
                "valid_params": {"lookback_months": 6, "growth_threshold": 0.15, "min_data_points": 10},
                "invalid_params": {"lookback_months": 0, "growth_threshold": -0.5, "min_data_points": 1},
            },
        ]

        for test_case in test_cases:
            algorithm = test_case["algorithm"]

            # Test valid parameters
            valid_config = ScoringConfig(
                algorithm_name=algorithm, version="1.0.0", parameters=test_case["valid_params"], environment="test"
            )

            result = config_manager.validate_parameters(valid_config)
            assert result.is_valid is True, f"Valid params failed for {algorithm}: {result.errors}"

            # Test invalid parameters
            invalid_config = ScoringConfig(
                algorithm_name=algorithm, version="1.0.0", parameters=test_case["invalid_params"], environment="test"
            )

            result = config_manager.validate_parameters(invalid_config)
            assert result.is_valid is False, f"Invalid params should fail for {algorithm}"
            assert len(result.errors) > 0, f"Should have validation errors for {algorithm}"

    def test_configuration_caching(self, config_manager, mock_database):
        """Test configuration caching functionality."""
        mock_db, mock_cursor = mock_database

        # Enable caching
        config_manager.environment_config.cache_enabled = True

        # Mock database response
        mock_cursor.fetchone.return_value = {
            "algorithm_name": "test_algorithm",
            "version": "1.0.0",
            "parameters": '{"param1": "value1"}',
            "environment": "development",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }

        # First load should hit database
        config1 = config_manager._load_config_from_database("test_algorithm")
        assert mock_cursor.execute.call_count == 1

        # Clear cache and test again
        config_manager.clear_cache()

        # Verify cache was cleared
        assert len(config_manager._config_cache) == 0

    def test_schema_management_integration(self, schema_manager, mock_database):
        """Test database schema management integration."""
        mock_db, mock_cursor = mock_database

        # Test schema verification
        mock_cursor.fetchone.side_effect = [
            {"Tables_in_test": "scoring_algorithms"},
            {"Tables_in_test": "scoring_configurations"},
            {"Tables_in_test": "configuration_audit_log"},
            {"Tables_in_test": "environment_settings"},
        ]

        schema_exists = schema_manager.verify_schema_exists()
        assert schema_exists is True

        # Test getting configuration statistics
        mock_cursor.fetchone.side_effect = [
            {"count": 5},  # active algorithms
            {"count": 25},  # recent changes
            {"count": 12},  # environment settings
        ]
        mock_cursor.fetchall.return_value = [
            {"environment": "development", "count": 3},
            {"environment": "production", "count": 3},
        ]

        stats = schema_manager.get_configuration_statistics()
        assert "active_algorithms" in stats
        assert "configurations_by_environment" in stats

    def test_backup_and_restore_workflow(self, schema_manager, mock_database):
        """Test configuration backup and restore workflow."""
        mock_db, mock_cursor = mock_database

        # Mock data for backup
        mock_cursor.fetchall.side_effect = [
            [{"algorithm_id": "test1", "algorithm_name": "test_algo", "version": "1.0.0"}],  # algorithms
            [{"config_id": 1, "algorithm_id": "test1", "environment": "dev", "parameters": {}}],  # configs
            [{"audit_id": 1, "algorithm_name": "test_algo", "parameter_name": "test_param"}],  # audit
            [{"setting_id": 1, "environment": "dev", "setting_name": "test_setting"}],  # settings
        ]

        # Test backup
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            backup_file = f.name

        try:
            result = schema_manager.backup_configuration_data(backup_file)
            assert result is True

            # Verify backup file was created and contains data
            assert os.path.exists(backup_file)

            with open(backup_file, "r") as f:
                backup_data = json.load(f)

            assert "backup_timestamp" in backup_data
            assert "algorithms" in backup_data
            assert "configurations" in backup_data
            assert "audit_log" in backup_data
            assert "environment_settings" in backup_data

        finally:
            # Clean up
            if os.path.exists(backup_file):
                os.unlink(backup_file)

    def test_error_handling_integration(self, config_manager, mock_database):
        """Test error handling across the configuration system."""
        mock_db, mock_cursor = mock_database

        # Test database connection failure
        mock_db.cursor.side_effect = Exception("Database connection failed")

        # Should fall back to default configuration
        config = config_manager.load_scoring_config("test_algorithm")
        assert config.algorithm_name == "test_algorithm"
        assert config.parameters == {}

        # Reset mock
        mock_db.cursor.side_effect = None
        mock_db.cursor.return_value = mock_cursor

        # Test validation error handling
        invalid_config = ScoringConfig(
            algorithm_name="", version="", parameters={}, environment="invalid_env"  # Invalid empty name
        )

        validation_result = config_manager.validate_parameters(invalid_config)
        assert validation_result.is_valid is False
        assert len(validation_result.errors) > 0

    def test_configuration_change_tracking(self, config_manager, mock_database):
        """Test comprehensive configuration change tracking."""
        mock_db, mock_cursor = mock_database

        # Create multiple configuration changes
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
            ConfigChange(
                algorithm_name="growth_potential",
                parameter_name="lookback_months",
                old_value=6,
                new_value=12,
                changed_by="data_scientist",
                change_reason="Longer historical analysis period",
            ),
        ]

        # Test auditing multiple changes
        config_manager.audit_configuration_changes(changes)

        # Verify all changes were logged
        assert mock_cursor.execute.call_count == len(changes)

        # Test change serialization
        for change in changes:
            change_dict = change.to_dict()
            assert "algorithm_name" in change_dict
            assert "parameter_name" in change_dict
            assert "old_value" in change_dict
            assert "new_value" in change_dict
            assert "changed_by" in change_dict
            assert "change_reason" in change_dict
            assert "change_timestamp" in change_dict

    def test_environment_specific_configurations(self, config_manager):
        """Test loading different configurations for different environments."""
        base_env_vars = {
            "SCORING_TEST_ALGORITHM_NAME": "test_algorithm",
            "SCORING_TEST_VERSION": "1.0.0",
            "SCORING_TEST_PARAM1": "base_value",
        }

        # Test development environment
        dev_env_vars = {
            **base_env_vars,
            "SCORING_TEST_ENVIRONMENT": "development",
            "SCORING_TEST_DEBUG_PARAM": "enabled",
        }

        with patch.dict(os.environ, dev_env_vars):
            dev_config = ScoringConfig.from_env_vars("SCORING_TEST_")

        assert dev_config.environment == "development"
        assert dev_config.parameters["debug_param"] == "enabled"

        # Test production environment
        prod_env_vars = {
            **base_env_vars,
            "SCORING_TEST_ENVIRONMENT": "production",
            "SCORING_TEST_DEBUG_PARAM": "disabled",
            "SCORING_TEST_PERFORMANCE_MODE": "high",
        }

        with patch.dict(os.environ, prod_env_vars):
            prod_config = ScoringConfig.from_env_vars("SCORING_TEST_")

        assert prod_config.environment == "production"
        assert prod_config.parameters["debug_param"] == "disabled"
        assert prod_config.parameters["performance_mode"] == "high"

        # Validate both configurations
        dev_validation = config_manager.validate_parameters(dev_config)
        prod_validation = config_manager.validate_parameters(prod_config)

        assert dev_validation.is_valid is True
        assert prod_validation.is_valid is True


if __name__ == "__main__":
    pytest.main([__file__])
