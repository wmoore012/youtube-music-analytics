"""
Common base classes and utilities for all tools.

This module implements the foundation for standardized tool development:
- Consistent logging setup across all tools
- Standardized configuration management
- Robust error handling with clear error types
- Tool discovery and validation system
"""

import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


# Standardized Error Classes
class ToolError(Exception):
    """Base exception for all tool errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ConfigurationError(ToolError):
    """Configuration-related errors (missing env vars, invalid config files, etc.)."""

    pass


class ExecutionError(ToolError):
    """Runtime execution errors (database connection failures, API errors, etc.)."""

    pass


class ValidationError(ToolError):
    """Data validation errors (invalid inputs, schema mismatches, etc.)."""

    pass


@dataclass
class ToolConfig:
    """Standardized configuration for all tools."""

    name: str
    version: str
    description: str
    dependencies: List[str] = field(default_factory=list)
    environment_vars: List[str] = field(default_factory=list)
    usage_examples: List[str] = field(default_factory=list)
    category: str = "general"  # core, specialized, development, legacy

    def validate(self) -> List[str]:
        """Validate tool configuration and return list of issues."""
        issues = []

        if not self.name:
            issues.append("Tool name is required")
        if not self.version:
            issues.append("Tool version is required")
        if not self.description:
            issues.append("Tool description is required")

        # Check if required environment variables are set
        for env_var in self.environment_vars:
            if not os.getenv(env_var):
                issues.append(f"Required environment variable {env_var} is not set")

        return issues


class ToolRegistry:
    """Central registry for tool discovery and validation."""

    def __init__(self):
        self._tools: Dict[str, ToolConfig] = {}
        self._logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        """Set up logging for the registry."""
        logger = logging.getLogger("ToolRegistry")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def register_tool(self, tool_config: ToolConfig) -> None:
        """Register a tool in the system."""
        validation_issues = tool_config.validate()
        if validation_issues:
            raise ValidationError(
                f"Tool configuration validation failed for {tool_config.name}", {"issues": validation_issues}
            )

        self._tools[tool_config.name] = tool_config
        self._logger.info(f"Registered tool: {tool_config.name} v{tool_config.version}")

    def find_tool(self, name: str) -> Optional[ToolConfig]:
        """Find tool by name."""
        return self._tools.get(name)

    def list_tools(self, category: Optional[str] = None) -> List[ToolConfig]:
        """List all registered tools, optionally filtered by category."""
        tools = list(self._tools.values())
        if category:
            tools = [tool for tool in tools if tool.category == category]
        return sorted(tools, key=lambda t: t.name)

    def validate_tools(self) -> List[ValidationError]:
        """Validate all registered tools and return any errors."""
        errors = []
        for tool_name, tool_config in self._tools.items():
            validation_issues = tool_config.validate()
            if validation_issues:
                errors.append(ValidationError(f"Tool {tool_name} validation failed", {"issues": validation_issues}))
        return errors


class ToolBase(ABC):
    """
    Base class for all tools with standardized logging, configuration, and error handling.

    This class provides:
    - Consistent logging setup with proper formatting
    - Environment variable loading and validation
    - Standardized error handling patterns
    - Configuration management
    - Progress reporting capabilities
    """

    def __init__(self, name: str, version: str = "1.0.0", log_level: str = "INFO"):
        self.name = name
        self.version = version
        self.config = self._load_configuration()
        self.logger = self._setup_logging(log_level)
        self._validate_environment()

    def _setup_logging(self, log_level: str) -> logging.Logger:
        """Set up standardized logging for the tool."""
        logger = logging.getLogger(self.name)

        # Avoid duplicate handlers
        if logger.handlers:
            return logger

        # Create console handler with formatting
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            f"%(asctime)s - {self.name} - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Set log level
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)
        logger.setLevel(numeric_level)

        return logger

    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration from environment variables and config files."""
        config = {}

        # Load from .env file if it exists
        env_file = Path(".env")
        if env_file.exists():
            try:
                with open(env_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, value = line.split("=", 1)
                            # Only set if not already in environment
                            if key not in os.environ:
                                os.environ[key] = value
                                config[key] = value
            except Exception:
                # Don't fail if .env can't be loaded, just continue silently
                pass

        # Add all environment variables to config
        config.update(os.environ)
        return config

    def _validate_environment(self) -> None:
        """Validate required environment variables are set."""
        required_vars = self.get_required_environment_vars()
        missing_vars = []

        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            raise ConfigurationError(
                f"Missing required environment variables: {', '.join(missing_vars)}",
                {"missing_variables": missing_vars},
            )

    @abstractmethod
    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables for this tool."""
        return []

    @abstractmethod
    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        pass

    @abstractmethod
    def run(self) -> None:
        """Main execution method for the tool."""
        pass

    def handle_error(self, error: Exception, context: Optional[str] = None) -> None:
        """Standardized error handling with logging and context."""
        error_msg = f"Error in {self.name}"
        if context:
            error_msg += f" ({context})"
        error_msg += f": {str(error)}"

        # Log the error with full traceback for debugging
        self.logger.error(error_msg, exc_info=True)

        # Re-raise as appropriate tool error type
        if isinstance(error, ToolError):
            raise error
        elif "configuration" in str(error).lower() or "environment" in str(error).lower():
            raise ConfigurationError(error_msg) from error
        elif "validation" in str(error).lower() or "invalid" in str(error).lower():
            raise ValidationError(error_msg) from error
        else:
            raise ExecutionError(error_msg) from error

    def log_progress(self, message: str, level: str = "INFO") -> None:
        """Log progress message with consistent formatting."""
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(f"[{self.name}] {message}")

    def get_config_value(self, key: str, default: Any = None, required: bool = False) -> Any:
        """Get configuration value with validation."""
        value = self.config.get(key, default)

        if required and value is None:
            raise ConfigurationError(f"Required configuration key '{key}' not found")

        return value

    def validate_input(self, value: Any, validator_func: callable, error_message: str) -> Any:
        """Validate input using provided validator function."""
        try:
            if not validator_func(value):
                raise ValidationError(error_message)
            return value
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            raise ValidationError(f"Validation failed: {error_message}") from e

    def cleanup_resources(self) -> None:
        """Clean up any resources used by the tool. Override in subclasses."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with resource cleanup."""
        try:
            self.cleanup_resources()
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")

        # Don't suppress exceptions
        return False


# Global registry instance
_global_registry = ToolRegistry()


def get_tool_registry() -> ToolRegistry:
    """Get the global tool registry instance."""
    return _global_registry


def register_tool(tool_config: ToolConfig) -> None:
    """Register a tool in the global registry."""
    _global_registry.register_tool(tool_config)


def find_tool(name: str) -> Optional[ToolConfig]:
    """Find a tool by name in the global registry."""
    return _global_registry.find_tool(name)
