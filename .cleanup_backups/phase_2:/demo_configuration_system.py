#!/usr/bin/env python3
"""
Demo script for the configuration management system.

This script demonstrates the configuration management system functionality
including loading configurations from environment variables and database,
parameter validation, and change auditing.
"""

from datetime import datetime
import os
import sys
import tempfile
from unittest.mock import MagicMock

# Add src to path for imports
sys.path.insert(0, "src")

from data_organization.configuration_manager import (
    ConfigChange,
    ConfigurationManager,
    EnvironmentConfig,
    ParameterValidator,
    ScoringConfig,
    ValidationResult,
)
from data_organization.configuration_schema_manager import ConfigurationSchemaManager


def demo_scoring_config():
    """Demo ScoringConfig functionality."""
    print("=" * 60)
    print("DEMO: ScoringConfig Functionality")
    print("=" * 60)

    # Create a scoring config
    config = ScoringConfig(
        algorithm_name="momentum_scoring",
        version="1.0.0",
        parameters={"threshold": 0.6, "window_days": 30, "min_videos": 5},
        environment="development",
    )

    print(f"Created config: {config.algorithm_name} v{config.version}")
    print(f"Parameters: {config.parameters}")
    print(f"Environment: {config.environment}")

    # Validate the config
    validation = config.validate()
    print(f"\nValidation result: {'PASS' if validation.is_valid else 'FAIL'}")
    if validation.errors:
        print(f"Errors: {validation.errors}")
    if validation.warnings:
        print(f"Warnings: {validation.warnings}")

    # Convert to dict
    config_dict = config.to_dict()
    print(f"\nConfig as dict keys: {list(config_dict.keys())}")

    print("\n" + "-" * 40)


def demo_environment_config():
    """Demo EnvironmentConfig functionality."""
    print("DEMO: EnvironmentConfig Functionality")
    print("=" * 60)

    # Set some environment variables
    test_env_vars = {
        "ENVIRONMENT": "staging",
        "DATABASE_URL": "mysql://localhost/test_analytics",
        "DEBUG_MODE": "enabled",
        "MAX_WORKERS": "6",
        "LOG_LEVEL": "debug",
    }

    # Temporarily set environment variables
    original_env = {}
    for key, value in test_env_vars.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        # Create environment config from env vars
        env_config = EnvironmentConfig.from_env_vars()

        print(f"Environment: {env_config.environment}")
        print(f"Database URL: {env_config.database_url}")
        print(f"Debug mode: {env_config.debug_mode}")
        print(f"Max workers: {env_config.max_workers}")
        print(f"Log level: {env_config.log_level}")

        # Validate environment config
        validation = env_config.validate()
        print(f"\nValidation result: {'PASS' if validation.is_valid else 'FAIL'}")
        print(f"Checked items: {validation.checked_items}")
        print(f"Passed items: {validation.passed_items}")

        if validation.errors:
            print(f"Errors: {validation.errors}")
        if validation.warnings:
            print(f"Warnings: {validation.warnings}")

    finally:
        # Restore original environment variables
        for key, original_value in original_env.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value

    print("\n" + "-" * 40)


def demo_parameter_validation():
    """Demo parameter validation functionality."""
    print("DEMO: Parameter Validation")
    print("=" * 60)

    validator = ParameterValidator()

    # Test valid parameters
    print("Testing valid parameters for momentum_scoring:")
    valid_params = {"threshold": 0.7, "window_days": 45, "min_videos": 10}

    result = validator.validate_parameters("momentum_scoring", valid_params)
    print(f"Result: {'PASS' if result.is_valid else 'FAIL'}")
    print(f"Checked: {result.checked_items}, Passed: {result.passed_items}")

    # Test invalid parameters
    print("\nTesting invalid parameters for momentum_scoring:")
    invalid_params = {"threshold": 1.5, "window_days": -10, "min_videos": 2000}  # Too high  # Negative  # Too high

    result = validator.validate_parameters("momentum_scoring", invalid_params)
    print(f"Result: {'PASS' if result.is_valid else 'FAIL'}")
    print(f"Errors: {result.errors}")

    # Test unknown algorithm
    print("\nTesting unknown algorithm:")
    result = validator.validate_parameters("unknown_algorithm", {"param": "value"})
    print(f"Result: {'PASS' if result.is_valid else 'FAIL'}")
    print(f"Warnings: {result.warnings}")

    print("\n" + "-" * 40)


def demo_config_from_env_vars():
    """Demo loading configuration from environment variables."""
    print("DEMO: Loading Config from Environment Variables")
    print("=" * 60)

    # Set environment variables for momentum scoring
    env_vars = {
        "SCORING_MOMENTUM_ALGORITHM_NAME": "momentum_scoring",
        "SCORING_MOMENTUM_VERSION": "2.0.0",
        "SCORING_MOMENTUM_THRESHOLD": "0.8",
        "SCORING_MOMENTUM_WINDOW_DAYS": "60",
        "SCORING_MOMENTUM_MIN_VIDEOS": "15",
        "SCORING_MOMENTUM_ENVIRONMENT": "production",
    }

    # Temporarily set environment variables
    original_env = {}
    for key, value in env_vars.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        # Load config from environment variables
        config = ScoringConfig.from_env_vars("SCORING_MOMENTUM_")

        print(f"Loaded algorithm: {config.algorithm_name}")
        print(f"Version: {config.version}")
        print(f"Environment: {config.environment}")
        print(f"Parameters: {config.parameters}")

        # Check parameter types
        print(f"\nParameter types:")
        for param, value in config.parameters.items():
            print(f"  {param}: {type(value).__name__} = {value}")

    finally:
        # Restore original environment variables
        for key, original_value in original_env.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value

    print("\n" + "-" * 40)


def demo_configuration_manager():
    """Demo ConfigurationManager functionality with mock database."""
    print("DEMO: ConfigurationManager with Mock Database")
    print("=" * 60)

    # Create mock database connection
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value = mock_cursor

    # Mock database response for loading config
    mock_cursor.fetchone.return_value = {
        "algorithm_name": "engagement_scoring",
        "version": "1.5.0",
        "parameters": '{"min_comments": 8, "sentiment_weight": 0.75}',
        "environment": "production",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }

    # Create configuration manager
    config_manager = ConfigurationManager(database_connection=mock_db)

    print(f"Environment: {config_manager.environment_config.environment}")
    print(f"Debug mode: {config_manager.environment_config.debug_mode}")

    # Try to load config from database
    try:
        config = config_manager._load_config_from_database("engagement_scoring")
        print(f"\nLoaded from database:")
        print(f"  Algorithm: {config.algorithm_name}")
        print(f"  Version: {config.version}")
        print(f"  Parameters: {config.parameters}")
    except Exception as e:
        print("Database load failed (expected in demo):", e)

    # Test parameter validation
    test_config = ScoringConfig(
        algorithm_name="momentum_scoring",
        version="1.0.0",
        parameters={"threshold": 0.5, "window_days": 30},
        environment="development",
    )

    validation = config_manager.validate_parameters(test_config)
    print(f"\nParameter validation: {'PASS' if validation.is_valid else 'FAIL'}")

    print("\n" + "-" * 40)


def demo_config_changes():
    """Demo configuration change tracking."""
    print("DEMO: Configuration Change Tracking")
    print("=" * 60)

    # Create some config changes
    changes = [
        ConfigChange(
            algorithm_name="momentum_scoring",
            parameter_name="threshold",
            old_value=0.5,
            new_value=0.7,
            changed_by="admin",
            change_reason="Performance optimization based on A/B testing",
        ),
        ConfigChange(
            algorithm_name="engagement_scoring",
            parameter_name="min_comments",
            old_value=10,
            new_value=5,
            changed_by="data_scientist",
            change_reason="Lower threshold to capture more engagement data",
        ),
    ]

    print("Configuration changes:")
    for i, change in enumerate(changes, 1):
        print(f"\n{i}. {change.algorithm_name} - {change.parameter_name}")
        print(f"   Changed: {change.old_value} → {change.new_value}")
        print(f"   By: {change.changed_by}")
        print(f"   Reason: {change.change_reason}")
        print(f"   Timestamp: {change.change_timestamp}")

    # Convert to dict format
    change_dict = changes[0].to_dict()
    print(f"\nChange as dict keys: {list(change_dict.keys())}")

    print("\n" + "-" * 40)


def demo_validation_result_merging():
    """Demo ValidationResult merging functionality."""
    print("DEMO: ValidationResult Merging")
    print("=" * 60)

    # Create two validation results
    result1 = ValidationResult(
        is_valid=True,
        errors=[],
        warnings=["Minor warning in result 1"],
        checked_items=5,
        passed_items=5,
        metadata={"source": "config_validation"},
    )

    result2 = ValidationResult(
        is_valid=False,
        errors=["Critical error in result 2"],
        warnings=["Warning in result 2"],
        checked_items=3,
        passed_items=2,
        metadata={"source": "parameter_validation"},
    )

    print("Result 1:")
    print(f"  Valid: {result1.is_valid}")
    print(f"  Errors: {result1.errors}")
    print(f"  Warnings: {result1.warnings}")
    print(f"  Checked/Passed: {result1.checked_items}/{result1.passed_items}")

    print("\nResult 2:")
    print(f"  Valid: {result2.is_valid}")
    print(f"  Errors: {result2.errors}")
    print(f"  Warnings: {result2.warnings}")
    print(f"  Checked/Passed: {result2.checked_items}/{result2.passed_items}")

    # Merge results
    merged = result1.merge(result2)
    print(f"\nMerged Result:")
    print(f"  Valid: {merged.is_valid}")
    print(f"  Errors: {merged.errors}")
    print(f"  Warnings: {merged.warnings}")
    print(f"  Checked/Passed: {merged.checked_items}/{merged.passed_items}")
    print(f"  Metadata: {merged.metadata}")

    print("\n" + "-" * 40)


def demo_schema_manager():
    """Demo ConfigurationSchemaManager functionality."""
    print("DEMO: Configuration Schema Manager")
    print("=" * 60)

    # Create mock database connection
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value = mock_cursor

    # Create schema manager
    schema_manager = ConfigurationSchemaManager(mock_db)

    print("Schema manager created successfully")

    # Mock table existence check
    mock_cursor.fetchone.side_effect = [
        {"Tables_in_test": "scoring_algorithms"},
        {"Tables_in_test": "scoring_configurations"},
        {"Tables_in_test": "configuration_audit_log"},
        {"Tables_in_test": "environment_settings"},
    ]

    # Test schema verification
    try:
        exists = schema_manager.verify_schema_exists()
        print(f"Schema exists: {exists}")
    except Exception as e:
        print("Schema verification failed (expected in demo):", e)

    # Mock statistics
    mock_cursor.fetchone.side_effect = [{"count": 3}, {"count": 15}]  # active algorithms  # recent changes
    mock_cursor.fetchall.return_value = [
        {"environment": "development", "count": 3},
        {"environment": "staging", "count": 3},
        {"environment": "production", "count": 3},
    ]

    try:
        stats = schema_manager.get_configuration_statistics()
        print(f"\nConfiguration statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    except Exception as e:
        print("Statistics failed (expected in demo):", e)

    print("\n" + "-" * 40)


def main():
    """Run all configuration system demos."""
    print("🔧 CONFIGURATION MANAGEMENT SYSTEM DEMO")
    print("=" * 80)
    print("This demo shows the configuration management system functionality")
    print("for scoring parameters with environment variables, database storage,")
    print("validation, and change auditing.")
    print("=" * 80)
    print()

    try:
        demo_scoring_config()
        demo_environment_config()
        demo_parameter_validation()
        demo_config_from_env_vars()
        demo_configuration_manager()
        demo_config_changes()
        demo_validation_result_merging()
        demo_schema_manager()

        print("=" * 80)
        print("✅ CONFIGURATION MANAGEMENT SYSTEM DEMO COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print()
        print("Key features demonstrated:")
        print("• ScoringConfig creation and validation")
        print("• Environment-based configuration loading")
        print("• Parameter validation with type checking and ranges")
        print("• Configuration loading from environment variables")
        print("• Database configuration management")
        print("• Configuration change tracking and auditing")
        print("• Validation result merging")
        print("• Database schema management")
        print()
        print("The configuration management system provides:")
        print("• Environment variable and database configuration loading")
        print("• Comprehensive parameter validation with clear error messages")
        print("• Configuration change auditing and logging")
        print("• Multi-environment support (development, staging, production)")
        print("• Caching and performance optimization")
        print("• Database schema management and backup/restore")

    except Exception as e:
        print("❌ Demo failed with error:", e)
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
