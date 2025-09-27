"""
Configuration management system for scoring parameters.

This module provides a comprehensive configuration management system that handles
scoring algorithm parameters from environment variables and database storage,
with validation, auditing, and change tracking capabilities.
"""

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import logging
import os
from typing import Any, Dict, List, Optional, Union

# Configure logging
logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Base exception for configuration - related errors."""

    pass


class ParameterValidationError(ConfigurationError):
    """Exception raised when parameter validation fails."""

    pass


class EnvironmentConfigError(ConfigurationError):
    """Exception raised when environment configuration is invalid."""

    pass


@dataclass
class ValidationResult:
    """Result of validation operations with detailed feedback."""

    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checked_items: int = 0
    passed_items: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, error: str) -> None:
        """Add an error to the validation result."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning to the validation result."""
        self.warnings.append(warning)

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge another validation result into this one."""
        return ValidationResult(
            is_valid=self.is_valid and other.is_valid,
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
            checked_items=self.checked_items + other.checked_items,
            passed_items=self.passed_items + other.passed_items,
            metadata={**self.metadata, **other.metadata},
        )


@dataclass
class ConfigChange:
    """Represents a configuration change for auditing purposes."""

    algorithm_name: str
    parameter_name: str
    old_value: Any
    new_value: Any
    changed_by: str
    change_reason: str
    change_timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert ConfigChange to dictionary for storage."""
        return asdict(self)


@dataclass
class EnvironmentConfig:
    """Environment - specific configuration settings."""

    environment: str = "development"
    database_url: str = ""
    debug_mode: str = "disabled"  # 'enabled' or 'disabled'
    max_workers: int = 4
    log_level: str = "info"
    scoring_timeout_seconds: int = 300
    cache_mode: str = "enabled"  # 'enabled' or 'disabled'
    audit_mode: str = "enabled"  # 'enabled' or 'disabled'

    @classmethod
    def from_env_vars(cls) -> "EnvironmentConfig":
        """Create EnvironmentConfig from environment variables."""
        return cls(
            environment=os.getenv("ENVIRONMENT", "development"),
            database_url=os.getenv("DATABASE_URL", ""),
            debug_mode=_parse_enabled_disabled(os.getenv("DEBUG_MODE", "disabled")),
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            log_level=os.getenv("LOG_LEVEL", "info").lower(),
            scoring_timeout_seconds=int(os.getenv("SCORING_TIMEOUT_SECONDS", "300")),
            cache_mode=_parse_enabled_disabled(os.getenv("CACHE_MODE", "enabled")),
            audit_mode=_parse_enabled_disabled(os.getenv("AUDIT_MODE", "enabled")),
        )

    def validate(self) -> ValidationResult:
        """Validate environment configuration."""
        result = ValidationResult()
        result.checked_items = 8

        # Validate environment
        valid_environments = ["development", "staging", "production", "test"]
        if self.environment not in valid_environments:
            result.add_error(f"Invalid environment '{self.environment}'. Must be one of: {valid_environments}")
        else:
            result.passed_items += 1

        # Validate database URL
        if not self.database_url:
            result.add_warning("Database URL is empty - using default connection")
        result.passed_items += 1

        # Validate max_workers
        if self.max_workers < 1 or self.max_workers > 32:
            result.add_error("max_workers must be between 1 and 32")
        else:
            result.passed_items += 1

        # Validate log_level
        valid_log_levels = ["debug", "info", "warning", "error", "critical"]
        if self.log_level not in valid_log_levels:
            result.add_error(f"Invalid log_level '{self.log_level}'. Must be one of: {valid_log_levels}")
        else:
            result.passed_items += 1

        # Validate timeout
        if self.scoring_timeout_seconds < 10 or self.scoring_timeout_seconds > 3600:
            result.add_error("scoring_timeout_seconds must be between 10 and 3600")
        else:
            result.passed_items += 1

        # Validate enabled / disabled fields
        valid_modes = ["enabled", "disabled"]

        if self.debug_mode not in valid_modes:
            result.add_error(f"Invalid debug_mode '{self.debug_mode}'. Must be 'enabled' or 'disabled'")
        else:
            result.passed_items += 1

        if self.cache_mode not in valid_modes:
            result.add_error(f"Invalid cache_mode '{self.cache_mode}'. Must be 'enabled' or 'disabled'")
        else:
            result.passed_items += 1

        if self.audit_mode not in valid_modes:
            result.add_error(f"Invalid audit_mode '{self.audit_mode}'. Must be 'enabled' or 'disabled'")
        else:
            result.passed_items += 1

        return result


@dataclass
class ScoringConfig:
    """Configuration for scoring algorithms with validation and serialization."""

    algorithm_name: str
    version: str
    parameters: Dict[str, Any]
    environment: str
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def validate(self) -> ValidationResult:
        """Validate the scoring configuration."""
        result = ValidationResult()
        result.checked_items = 4

        # Validate algorithm name
        if not self.algorithm_name or not self.algorithm_name.strip():
            result.add_error("algorithm_name cannot be empty")
        elif not self.algorithm_name.replace("_", "").replace("-", "").isalnum():
            result.add_error("algorithm_name must contain only alphanumeric characters, hyphens, and underscores")
        else:
            result.passed_items += 1

        # Validate version
        if not self.version or not self.version.strip():
            result.add_error("version cannot be empty")
        else:
            result.passed_items += 1

        # Validate environment
        valid_environments = ["development", "staging", "production", "test"]
        if self.environment not in valid_environments:
            result.add_error(f"Invalid environment '{self.environment}'. Must be one of: {valid_environments}")
        else:
            result.passed_items += 1

        # Validate parameters
        if not isinstance(self.parameters, dict):
            result.add_error("parameters must be a dictionary")
        else:
            result.passed_items += 1

        return result

    def to_dict(self) -> Dict[str, Any]:
        """Convert ScoringConfig to dictionary for storage."""
        return asdict(self)

    @classmethod
    def from_env_vars(cls, prefix: str) -> "ScoringConfig":
        """Create ScoringConfig from environment variables with given prefix."""
        # Extract basic configuration
        algorithm_name = os.getenv(f"{prefix}ALGORITHM_NAME", "")
        version = os.getenv(f"{prefix}VERSION", "1.0.0")
        environment = os.getenv(f"{prefix}ENVIRONMENT", "development")

        # Extract parameters by finding all env vars with the prefix
        parameters = {}
        for key, value in os.environ.items():
            if key.startswith(prefix) and key not in [
                f"{prefix}ALGORITHM_NAME",
                f"{prefix}VERSION",
                f"{prefix}ENVIRONMENT",
            ]:
                param_name = key[len(prefix) :].lower()
                parameters[param_name] = _parse_parameter_value(value)

        return cls(
            algorithm_name=algorithm_name,
            version=version,
            parameters=parameters,
            environment=environment,
        )


class ParameterValidator:
    """Validates scoring algorithm parameters based on predefined rules."""

    # Parameter validation rules for different algorithms
    VALIDATION_RULES = {
        "momentum_scoring": {
            "threshold": {"type": float, "min": 0.0, "max": 1.0},
            "window_days": {"type": int, "min": 1, "max": 365},
            "min_videos": {"type": int, "min": 1, "max": 1000},
        },
        "engagement_scoring": {
            "min_comments": {"type": int, "min": 0, "max": 10000},
            "sentiment_weight": {"type": float, "min": 0.0, "max": 1.0},
            "like_ratio_weight": {"type": float, "min": 0.0, "max": 1.0},
        },
        "growth_potential": {
            "lookback_months": {"type": int, "min": 1, "max": 24},
            "growth_threshold": {"type": float, "min": 0.0, "max": 10.0},
            "min_data_points": {"type": int, "min": 3, "max": 100},
        },
    }

    @classmethod
    def validate_parameters(cls, algorithm_name: str, parameters: Dict[str, Any]) -> ValidationResult:
        """Validate parameters for a specific algorithm."""
        result = ValidationResult()

        if algorithm_name not in cls.VALIDATION_RULES:
            result.add_warning(f"No validation rules defined for algorithm '{algorithm_name}'")
            return result

        rules = cls.VALIDATION_RULES[algorithm_name]
        result.checked_items = len(parameters)

        for param_name, param_value in parameters.items():
            if param_name not in rules:
                result.add_warning(f"Unknown parameter '{param_name}' for algorithm '{algorithm_name}'")
                continue

            rule = rules[param_name]
            param_valid = True

            # Check type
            expected_type = rule["type"]
            if not isinstance(param_value, expected_type):
                try:
                    # Try to convert the value
                    param_value = expected_type(param_value)
                except (ValueError, TypeError):
                    result.add_error(f"Parameter '{param_name}' must be of type {expected_type.__name__}")
                    param_valid = False

            if param_valid:
                # Check range constraints
                if "min" in rule and param_value < rule["min"]:
                    result.add_error(f"Parameter '{param_name}' must be >= {rule['min']}")
                    param_valid = False

                if "max" in rule and param_value > rule["max"]:
                    result.add_error(f"Parameter '{param_name}' must be <= {rule['max']}")
                    param_valid = False

            if param_valid:
                result.passed_items += 1

        return result


class ConfigurationManager:
    """Manages scoring configuration from environment variables and database."""

    def __init__(self, database_connection=None):
        """Initialize ConfigurationManager with optional database connection."""
        self.database_connection = database_connection
        self.parameter_validator = ParameterValidator()
        self._config_cache = {}
        self.environment_config = EnvironmentConfig.from_env_vars()

        # Validate environment configuration
        env_validation = self.environment_config.validate()
        if not env_validation.is_valid:
            logger.warning(f"Environment configuration issues: {env_validation.errors}")

    def load_scoring_config(self, algorithm_name: str) -> ScoringConfig:
        """Load scoring configuration for an algorithm from environment or database."""
        if not algorithm_name:
            raise ConfigurationError("Algorithm name cannot be empty")

        # Try to load from cache first
        cache_key = f"{algorithm_name}_{self.environment_config.environment}"
        if self.environment_config.cache_mode == "enabled" and cache_key in self._config_cache:
            logger.debug(f"Loading config for '{algorithm_name}' from cache")
            return self._config_cache[cache_key]

        # Try to load from environment variables first
        env_prefix = f"SCORING_{algorithm_name.upper()}_"
        try:
            config = ScoringConfig.from_env_vars(env_prefix)
            # Check if we have any parameters or a valid algorithm name from env vars
            if (config.algorithm_name and config.algorithm_name.strip()) or config.parameters:
                logger.info(f"Loaded config for '{algorithm_name}' from environment variables")
                if self.environment_config.cache_mode == "enabled":
                    self._config_cache[cache_key] = config
                return config
        except Exception as e:
            logger.warning(f"Failed to load config from environment: {e}")

        # Fall back to database
        if self.database_connection:
            try:
                config = self._load_config_from_database(algorithm_name)
                logger.info(f"Loaded config for '{algorithm_name}' from database")
                if self.environment_config.cache_mode == "enabled":
                    self._config_cache[cache_key] = config
                return config
            except Exception as e:
                logger.warning(f"Failed to load config from database: {e}")

        # If all else fails, create a default config
        logger.warning(f"Creating default config for '{algorithm_name}'")
        return ScoringConfig(
            algorithm_name=algorithm_name,
            version="1.0.0",
            parameters={},
            environment=self.environment_config.environment,
        )

    def _load_config_from_database(self, algorithm_name: str) -> ScoringConfig:
        """Load configuration from database."""
        if not self.database_connection:
            raise ConfigurationError("Database connection not available")

        cursor = self.database_connection.cursor()
        try:
            query = """
                SELECT sc.algorithm_name, sa.version, sc.parameters, sc.environment,
                       sc.created_at, sc.updated_at
                FROM scoring_configurations sc
                JOIN scoring_algorithms sa ON sc.algorithm_id = sa.algorithm_id
                WHERE sa.algorithm_name = %s AND sc.environment = %s AND sc.status = 'active'
                ORDER BY sc.updated_at DESC
                LIMIT 1
            """
            cursor.execute(query, (algorithm_name, self.environment_config.environment))
            row = cursor.fetchone()

            if not row:
                raise ConfigurationError(f"No configuration found for algorithm '{algorithm_name}'")

            # Parse JSON parameters
            parameters = json.loads(row["parameters"]) if isinstance(row["parameters"], str) else row["parameters"]

            return ScoringConfig(
                algorithm_name=row["algorithm_name"],
                version=row["version"],
                parameters=parameters,
                environment=row["environment"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

        finally:
            cursor.close()

    def validate_parameters(self, config: ScoringConfig) -> ValidationResult:
        """Validate scoring configuration parameters."""
        # First validate the config structure
        structure_result = config.validate()
        if not structure_result.is_valid:
            return structure_result

        # Then validate the parameters
        param_result = self.parameter_validator.validate_parameters(config.algorithm_name, config.parameters)

        # Merge results
        return structure_result.merge(param_result)

    def update_configuration(self, algorithm_name: str, new_config: Dict[str, Any]) -> None:
        """Update configuration in database with change auditing."""
        if not self.database_connection:
            raise ConfigurationError("Database connection not available")

        # Load current configuration for comparison
        try:
            current_config = self.load_scoring_config(algorithm_name)
        except ConfigurationError:
            current_config = None

        # Validate new configuration
        updated_config = ScoringConfig(
            algorithm_name=algorithm_name,
            version=current_config.version if current_config else "1.0.0",
            parameters=new_config,
            environment=self.environment_config.environment,
        )

        validation_result = self.validate_parameters(updated_config)
        if not validation_result.is_valid:
            raise ParameterValidationError(f"Configuration validation failed: {validation_result.errors}")

        # Update database
        cursor = self.database_connection.cursor()
        try:
            # Update or insert configuration
            query = """
                INSERT INTO scoring_configurations (algorithm_id, environment, parameters, status)
                SELECT sa.algorithm_id, %s, %s, 'active'
                FROM scoring_algorithms sa
                WHERE sa.algorithm_name = %s
                ON DUPLICATE KEY UPDATE
                parameters = VALUES(parameters),
                updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(query, (self.environment_config.environment, json.dumps(new_config), algorithm_name))
            self.database_connection.commit()

            # Clear cache
            if self.environment_config.cache_mode == "enabled":
                cache_key = f"{algorithm_name}_{self.environment_config.environment}"
                self._config_cache.pop(cache_key, None)

            # Audit changes if enabled
            if self.environment_config.audit_mode == "enabled" and current_config:
                changes = self._detect_parameter_changes(current_config.parameters, new_config)
                if changes:
                    self.audit_configuration_changes(changes)

            logger.info(f"Updated configuration for algorithm '{algorithm_name}'")

        except Exception as e:
            self.database_connection.rollback()
            raise ConfigurationError(f"Failed to update configuration: {e}")
        finally:
            cursor.close()

    def _detect_parameter_changes(self, old_params: Dict[str, Any], new_params: Dict[str, Any]) -> List[ConfigChange]:
        """Detect changes between old and new parameters."""
        changes = []

        # Check for modified and new parameters
        for param_name, new_value in new_params.items():
            old_value = old_params.get(param_name)
            if old_value != new_value:
                changes.append(
                    ConfigChange(
                        algorithm_name="",  # Will be set by caller
                        parameter_name=param_name,
                        old_value=old_value,
                        new_value=new_value,
                        changed_by=os.getenv("USER", "system"),
                        change_reason="Configuration update",
                    )
                )

        # Check for removed parameters
        for param_name, old_value in old_params.items():
            if param_name not in new_params:
                changes.append(
                    ConfigChange(
                        algorithm_name="",  # Will be set by caller
                        parameter_name=param_name,
                        old_value=old_value,
                        new_value=None,
                        changed_by=os.getenv("USER", "system"),
                        change_reason="Parameter removed",
                    )
                )

        return changes

    def get_environment_config(self) -> EnvironmentConfig:
        """Get current environment configuration."""
        return self.environment_config

    def audit_configuration_changes(self, changes: List[ConfigChange]) -> None:
        """Audit configuration changes to database."""
        if not self.database_connection or self.environment_config.audit_mode != "enabled":
            return

        if not changes:
            return

        cursor = self.database_connection.cursor()
        try:
            query = """
                INSERT INTO configuration_audit_log
                (algorithm_name, parameter_name, old_value, new_value, changed_by,
                 change_reason, change_timestamp, environment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            for change in changes:
                cursor.execute(
                    query,
                    (
                        change.algorithm_name,
                        change.parameter_name,
                        json.dumps(change.old_value) if change.old_value is not None else None,
                        json.dumps(change.new_value) if change.new_value is not None else None,
                        change.changed_by,
                        change.change_reason,
                        change.change_timestamp,
                        self.environment_config.environment,
                    ),
                )

            self.database_connection.commit()
            logger.info(f"Audited {len(changes)} configuration changes")

        except Exception as e:
            self.database_connection.rollback()
            logger.error(f"Failed to audit configuration changes: {e}")
        finally:
            cursor.close()

    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._config_cache.clear()
        logger.info("Configuration cache cleared")

    def get_all_algorithm_configs(self) -> Dict[str, ScoringConfig]:
        """Get configurations for all available algorithms."""
        if not self.database_connection:
            return {}

        cursor = self.database_connection.cursor()
        try:
            query = """
                SELECT DISTINCT sa.algorithm_name
                FROM scoring_algorithms sa
                WHERE sa.status = 'active'
            """
            cursor.execute(query)
            algorithms = [row["algorithm_name"] for row in cursor.fetchall()]

            configs = {}
            for algorithm_name in algorithms:
                try:
                    configs[algorithm_name] = self.load_scoring_config(algorithm_name)
                except Exception as e:
                    logger.warning(f"Failed to load config for '{algorithm_name}': {e}")

            return configs

        finally:
            cursor.close()


def _parse_enabled_disabled(value: str) -> str:
    """Parse string value to enabled / disabled."""
    if value.lower() in ("true", "1", "yes", "on", "enabled"):
        return "enabled"
    elif value.lower() in ("false", "0", "no", "off", "disabled"):
        return "disabled"
    else:
        return value.lower()


def _parse_parameter_value(value: str) -> Union[str, int, float]:
    """Parse parameter value from string to appropriate type."""
    # Try integer first
    try:
        return int(value)
    except ValueError:
        pass

    # Try float
    try:
        return float(value)
    except ValueError:
        pass

    # Return as string (including enabled / disabled values)
    return value
