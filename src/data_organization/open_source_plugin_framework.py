"""
Open-source plugin framework for music analytics scoring algorithms.

This module provides a framework for music data researchers to create their own
scoring algorithms for YouTube music analytics. It includes security validation,
plugin registration, and examples of common music analytics patterns.

Designed for the music data community on GitHub to extend and customize
scoring algorithms for their specific research needs.
"""

from abc import ABC, abstractmethod
import ast
from dataclasses import dataclass, field
from datetime import datetime
import inspect
import json
import logging
import os
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from src.data_organization.notebook_validator import ValidationResult

# Configure logging
logger = logging.getLogger(__name__)


class PluginValidationError(Exception):
    """Raised when plugin validation fails."""

    pass


class PluginSecurityError(Exception):
    """Raised when plugin security checks fail."""

    pass


class PluginRegistrationError(Exception):
    """Raised when plugin registration fails."""

    pass


@dataclass
class PluginMetadata:
    """
    Metadata for music analytics scoring plugins.

    This class defines the required metadata that all plugins must provide
    to help users understand what the plugin does and how to use it.
    """

    name: str
    version: str
    author: str
    description: str
    parameters: Dict[str, Any]
    input_requirements: List[str]
    output_schema: Dict[str, str]
    license: str = "MIT"
    repository_url: str = ""
    documentation_url: str = ""
    tags: List[str] = field(default_factory=list)

    def validate(self) -> ValidationResult:
        """
        Validate plugin metadata for completeness and correctness.

        Returns:
            ValidationResult with validation details
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        # Check required fields
        required_fields = {
            "name": self.name,
            "version": self.version,
            "author": self.author,
            "description": self.description,
        }

        for field_name, field_value in required_fields.items():
            result.checked_items += 1
            if not field_value or not isinstance(field_value, str) or len(field_value.strip()) == 0:
                result.add_error(f"Plugin {field_name} is required and cannot be empty")
            else:
                result.passed_items += 1

        # Validate version format (semantic versioning)
        result.checked_items += 1
        if self.version:
            version_parts = self.version.split(".")
            if len(version_parts) != 3 or not all(part.isdigit() for part in version_parts):
                result.add_error("Plugin version must follow semantic versioning (e.g., '1.0.0')")
            else:
                result.passed_items += 1

        # Validate plugin name format
        result.checked_items += 1
        if self.name:
            if not self.name.replace("_", "").replace("-", "").isalnum():
                result.add_error("Plugin name must contain only letters, numbers, hyphens, and underscores")
            else:
                result.passed_items += 1

        # Check input requirements
        result.checked_items += 1
        if not self.input_requirements or len(self.input_requirements) == 0:
            result.add_error("Plugin must specify input requirements (required DataFrame columns)")
        else:
            result.passed_items += 1

        # Check output schema
        result.checked_items += 1
        if not self.output_schema or len(self.output_schema) == 0:
            result.add_error("Plugin must specify output schema (expected output columns and types)")
        else:
            result.passed_items += 1

        return result

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary for serialization."""
        return {
            "name": self.name,
            "version": self.version,
            "author": self.author,
            "description": self.description,
            "parameters": self.parameters,
            "input_requirements": self.input_requirements,
            "output_schema": self.output_schema,
            "license": self.license,
            "repository_url": self.repository_url,
            "documentation_url": self.documentation_url,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PluginMetadata":
        """Create metadata from dictionary."""
        return cls(
            name=data.get("name", ""),
            version=data.get("version", ""),
            author=data.get("author", ""),
            description=data.get("description", ""),
            parameters=data.get("parameters", {}),
            input_requirements=data.get("input_requirements", []),
            output_schema=data.get("output_schema", {}),
            license=data.get("license", "MIT"),
            repository_url=data.get("repository_url", ""),
            documentation_url=data.get("documentation_url", ""),
            tags=data.get("tags", []),
        )


class OpenSourceScoringPlugin(ABC):
    """
    Abstract base class for open-source music analytics scoring plugins.

    This class provides the interface that all music analytics plugins must implement.
    It's designed to be extended by researchers and developers in the music data community.

    Example usage:
        class MyMusicPlugin(OpenSourceScoringPlugin):
            def get_name(self) -> str:
                return "my_music_algorithm"

            def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
                # Your music analytics algorithm here
                return data.assign(score=data['view_count'] * 0.001)
    """

    def __init__(self):
        """Initialize the plugin with default configuration."""
        self.config: Dict[str, Any] = {}
        self.execution_metadata: Dict[str, Any] = {}

    @abstractmethod
    def get_name(self) -> str:
        """
        Get the unique name of this plugin.

        Returns:
            Unique plugin name (used for registration and identification)
        """
        pass

    @abstractmethod
    def get_version(self) -> str:
        """
        Get the version of this plugin.

        Returns:
            Plugin version in semantic versioning format (e.g., "1.0.0")
        """
        pass

    @abstractmethod
    def get_metadata(self) -> PluginMetadata:
        """
        Get comprehensive metadata about this plugin.

        Returns:
            PluginMetadata with complete plugin information
        """
        pass

    @abstractmethod
    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate scores for the given music analytics data.

        This is the main method where your music analytics algorithm logic goes.

        Args:
            data: DataFrame containing music analytics data (views, likes, etc.)

        Returns:
            DataFrame with calculated scores added

        Raises:
            ValueError: If input data doesn't meet requirements
        """
        pass

    @abstractmethod
    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """
        Validate that input data meets plugin requirements.

        Args:
            data: DataFrame to validate

        Returns:
            ValidationResult indicating if data is valid for this plugin
        """
        pass

    def load_configuration(self, config: Dict[str, Any]) -> None:
        """
        Load configuration parameters for the plugin.

        Args:
            config: Dictionary of configuration parameters
        """
        self.config = config.copy()
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        """Validate plugin configuration. Override in subclasses for custom validation."""
        pass

    def export_results(self, scores: pd.DataFrame, format: str, output_path: str = None) -> None:
        """
        Export scoring results to various formats.

        Args:
            scores: DataFrame with calculated scores
            format: Export format ('csv', 'json', 'parquet')
            output_path: Path to save results (optional)
        """
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"{self.get_name()}_results_{timestamp}.{format}"

        if format.lower() == "csv":
            scores.to_csv(output_path, index=False)
        elif format.lower() == "json":
            scores.to_json(output_path, orient="records", indent=2)
        elif format.lower() == "parquet":
            scores.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported export format: {format}")

        logger.info(f"Results exported to {output_path}")

    def get_execution_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the last plugin execution.

        Returns:
            Dictionary with execution metadata
        """
        return self.execution_metadata.copy()

    def _record_execution_start(self) -> None:
        """Record the start of plugin execution."""
        self.execution_metadata = {
            "start_time": datetime.now(),
            "plugin_name": self.get_name(),
            "plugin_version": self.get_version(),
            "configuration": self.config.copy(),
        }

    def _record_execution_end(self, success: bool, error_message: str = None) -> None:
        """Record the end of plugin execution."""
        self.execution_metadata.update(
            {
                "end_time": datetime.now(),
                "success": success,
                "error_message": error_message,
                "duration_seconds": (datetime.now() - self.execution_metadata["start_time"]).total_seconds(),
            }
        )


class PluginValidator:
    """
    Validates music analytics plugins for structure, metadata, and functionality.

    This class ensures that plugins meet the required standards for the
    music analytics community and can be safely executed.
    """

    def __init__(self):
        """Initialize the plugin validator."""
        self.required_methods = ["get_name", "get_version", "get_metadata", "calculate_scores", "validate_input"]

    def validate_plugin_structure(self, plugin: OpenSourceScoringPlugin) -> ValidationResult:
        """
        Validate that plugin has required methods and structure.

        Args:
            plugin: Plugin instance to validate

        Returns:
            ValidationResult with structure validation details
        """
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], checked_items=len(self.required_methods), passed_items=0
        )

        # Check if plugin inherits from OpenSourceScoringPlugin
        if not isinstance(plugin, OpenSourceScoringPlugin):
            result.add_error("Plugin must inherit from OpenSourceScoringPlugin")
            return result

        # Check required methods
        for method_name in self.required_methods:
            if not hasattr(plugin, method_name):
                result.add_error(f"Plugin missing required method: {method_name}")
            elif not callable(getattr(plugin, method_name)):
                result.add_error(f"Plugin {method_name} is not callable")
            else:
                result.passed_items += 1

        return result

    def validate_plugin_metadata(self, metadata: PluginMetadata) -> ValidationResult:
        """
        Validate plugin metadata completeness and correctness.

        Args:
            metadata: Plugin metadata to validate

        Returns:
            ValidationResult with metadata validation details
        """
        return metadata.validate()

    def validate_input_requirements(self, data: pd.DataFrame, requirements: List[str]) -> ValidationResult:
        """
        Validate that data meets plugin input requirements.

        Args:
            data: DataFrame to validate
            requirements: List of required column names

        Returns:
            ValidationResult with input validation details
        """
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], checked_items=len(requirements), passed_items=0
        )

        for column in requirements:
            if column not in data.columns:
                result.add_error(f"Required column '{column}' not found in input data")
            else:
                result.passed_items += 1

        # Check for empty data
        if len(data) == 0:
            result.add_warning("Input data is empty")

        return result

    def validate_output_schema(self, output: pd.DataFrame, expected_schema: Dict[str, str]) -> ValidationResult:
        """
        Validate that plugin output matches expected schema.

        Args:
            output: DataFrame output from plugin
            expected_schema: Expected column names and types

        Returns:
            ValidationResult with output validation details
        """
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], checked_items=len(expected_schema), passed_items=0
        )

        for column, expected_type in expected_schema.items():
            if column not in output.columns:
                result.add_error(f"Expected output column '{column}' not found")
            else:
                actual_type = str(output[column].dtype)
                if not self._types_compatible(actual_type, expected_type):
                    result.add_error(f"Column '{column}' has type '{actual_type}', expected '{expected_type}'")
                else:
                    result.passed_items += 1

        return result

    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if actual and expected types are compatible."""
        type_mappings = {
            "int64": ["int64", "int32", "int"],
            "float64": ["float64", "float32", "float"],
            "object": ["object", "string"],
            "bool": ["bool", "boolean"],
        }

        if actual == expected:
            return True

        for base_type, compatible_types in type_mappings.items():
            if expected == base_type and actual in compatible_types:
                return True

        return False


class PluginSecurityChecker:
    """
    Security validation for music analytics plugins.

    This class ensures that plugins don't contain malicious code and
    follow security best practices for the open-source community.
    """

    def __init__(self):
        """Initialize the security checker."""
        self.dangerous_imports = [
            "os",
            "subprocess",
            "sys",
            "eval",
            "exec",
            "compile",
            "importlib",
            "__import__",
            "open",
            "file",
            "input",
            "raw_input",
            "execfile",
            "reload",
            "socket",
            "urllib",
            "requests",
            "http",
            "ftplib",
            "smtplib",
        ]

        self.dangerous_functions = [
            "eval",
            "exec",
            "compile",
            "open",
            "__import__",
            "getattr",
            "setattr",
            "delattr",
            "globals",
            "locals",
            "vars",
            "dir",
            "hasattr",
        ]

    def check_plugin_security(self, plugin_code: str) -> ValidationResult:
        """
        Check plugin code for security issues.

        Args:
            plugin_code: Source code of the plugin to check

        Returns:
            ValidationResult with security validation details
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        try:
            # Parse the code to check for dangerous patterns
            tree = ast.parse(plugin_code)

            # Check for dangerous imports
            result.checked_items += 1
            dangerous_imports_found = self._check_dangerous_imports(tree)
            if dangerous_imports_found:
                result.add_error(f"Dangerous imports detected: {', '.join(dangerous_imports_found)}")
            else:
                result.passed_items += 1

            # Check for dangerous function calls
            result.checked_items += 1
            dangerous_calls_found = self._check_dangerous_calls(tree)
            if dangerous_calls_found:
                result.add_error(f"Dangerous function calls detected: {', '.join(dangerous_calls_found)}")
            else:
                result.passed_items += 1

            # Check for file operations
            result.checked_items += 1
            file_operations_found = self._check_file_operations(tree)
            if file_operations_found:
                result.add_warning("File operations detected - ensure they are necessary and safe")
                result.passed_items += 1
            else:
                result.passed_items += 1

        except SyntaxError as e:
            result.add_error(f"Plugin code has syntax errors: {str(e)}")

        return result

    def _check_dangerous_imports(self, tree: ast.AST) -> List[str]:
        """Check for dangerous import statements."""
        dangerous_found = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in self.dangerous_imports:
                        dangerous_found.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module in self.dangerous_imports:
                    dangerous_found.append(node.module)

        return dangerous_found

    def _check_dangerous_calls(self, tree: ast.AST) -> List[str]:
        """Check for dangerous function calls."""
        dangerous_found = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self.dangerous_functions:
                        dangerous_found.append(node.func.id)

        return dangerous_found

    def _check_file_operations(self, tree: ast.AST) -> bool:
        """Check for file operations."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ["open", "file"]:
                        return True
        return False

    def check_resource_limits(self, plugin_function: Callable) -> ValidationResult:
        """
        Check if plugin respects resource limits.

        Args:
            plugin_function: Function to test for resource usage

        Returns:
            ValidationResult with resource limit validation
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=1, passed_items=0)

        try:
            # Test execution time (should complete within reasonable time)
            start_time = time.time()
            plugin_function()
            execution_time = time.time() - start_time

            if execution_time > 30:  # 30 second limit for testing
                result.add_warning(f"Plugin execution took {execution_time:.2f} seconds - consider optimization")

            result.passed_items += 1

        except Exception as e:
            result.add_error(f"Plugin function failed during resource check: {str(e)}")

        return result

    def validate_plugin_permissions(self, permissions: Dict[str, bool]) -> ValidationResult:
        """
        Validate requested plugin permissions.

        Args:
            permissions: Dictionary of requested permissions

        Returns:
            ValidationResult with permission validation
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=len(permissions), passed_items=0)

        dangerous_permissions = [
            "execute_commands",
            "system_access",
            "network_write",
            "file_system_write",
            "registry_access",
        ]

        for permission, granted in permissions.items():
            if granted and permission in dangerous_permissions:
                result.add_error(f"Dangerous permission requested: {permission}")
            else:
                result.passed_items += 1

        return result


class PluginRegistry:
    """
    Registry for managing music analytics plugins.

    This class provides a central registry where plugins can be registered,
    discovered, and managed by the music analytics community.
    """

    def __init__(self):
        """Initialize the plugin registry."""
        self.plugins: Dict[str, OpenSourceScoringPlugin] = {}
        self.plugin_metadata: Dict[str, PluginMetadata] = {}
        self.validator = PluginValidator()
        self.security_checker = PluginSecurityChecker()

    def register_plugin(self, plugin: OpenSourceScoringPlugin) -> ValidationResult:
        """
        Register a new plugin in the registry.

        Args:
            plugin: Plugin instance to register

        Returns:
            ValidationResult indicating success or failure of registration
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        try:
            # Validate plugin structure
            structure_result = self.validator.validate_plugin_structure(plugin)
            result = result.merge(structure_result)

            if not structure_result.is_valid:
                return result

            # Get and validate metadata
            metadata = plugin.get_metadata()
            metadata_result = self.validator.validate_plugin_metadata(metadata)
            result = result.merge(metadata_result)

            if not metadata_result.is_valid:
                return result

            # Check for duplicate names
            plugin_name = plugin.get_name()
            if plugin_name in self.plugins:
                result.add_error(f"Plugin '{plugin_name}' is already registered")
                return result

            # Register the plugin
            self.plugins[plugin_name] = plugin
            self.plugin_metadata[plugin_name] = metadata

            result.checked_items += 1
            result.passed_items += 1

            logger.info(f"Successfully registered plugin: {plugin_name}")

        except Exception as e:
            result.add_error(f"Failed to register plugin: {str(e)}")

        return result

    def get_plugin(self, name: str) -> Optional[OpenSourceScoringPlugin]:
        """
        Get a registered plugin by name.

        Args:
            name: Name of the plugin to retrieve

        Returns:
            Plugin instance or None if not found
        """
        return self.plugins.get(name)

    def get_registered_plugins(self) -> List[str]:
        """
        Get list of all registered plugin names.

        Returns:
            List of registered plugin names
        """
        return list(self.plugins.keys())

    def unregister_plugin(self, name: str) -> bool:
        """
        Unregister a plugin from the registry.

        Args:
            name: Name of plugin to unregister

        Returns:
            True if plugin was unregistered, False if not found
        """
        if name in self.plugins:
            del self.plugins[name]
            del self.plugin_metadata[name]
            logger.info(f"Unregistered plugin: {name}")
            return True
        return False

    def list_plugins_with_metadata(self) -> List[Dict[str, Any]]:
        """
        Get list of all plugins with their metadata.

        Returns:
            List of dictionaries containing plugin information
        """
        plugins_info = []

        for name, metadata in self.plugin_metadata.items():
            plugin_info = metadata.to_dict()
            plugin_info["registered_at"] = datetime.now().isoformat()
            plugins_info.append(plugin_info)

        return plugins_info

    def search_plugins(self, query: str = None, tags: List[str] = None) -> List[str]:
        """
        Search for plugins by name, description, or tags.

        Args:
            query: Text to search for in plugin names and descriptions
            tags: List of tags to filter by

        Returns:
            List of matching plugin names
        """
        matching_plugins = []

        for name, metadata in self.plugin_metadata.items():
            match = True

            # Text search
            if query:
                query_lower = query.lower()
                if query_lower not in name.lower() and query_lower not in metadata.description.lower():
                    match = False

            # Tag search
            if tags and match:
                if not any(tag in metadata.tags for tag in tags):
                    match = False

            if match:
                matching_plugins.append(name)

        return matching_plugins

    def export_registry(self, file_path: str) -> None:
        """
        Export plugin registry to JSON file.

        Args:
            file_path: Path to save registry data
        """
        registry_data = {
            "plugins": self.list_plugins_with_metadata(),
            "exported_at": datetime.now().isoformat(),
            "total_plugins": len(self.plugins),
        }

        with open(file_path, "w") as f:
            json.dump(registry_data, f, indent=2)

        logger.info(f"Registry exported to {file_path}")

    def import_registry(self, file_path: str) -> ValidationResult:
        """
        Import plugin registry from JSON file.

        Args:
            file_path: Path to registry data file

        Returns:
            ValidationResult indicating success or failure
        """
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        try:
            with open(file_path, "r") as f:
                registry_data = json.load(f)

            # Note: This only imports metadata, not actual plugin instances
            # Actual plugins need to be registered separately
            plugins_data = registry_data.get("plugins", [])

            for plugin_data in plugins_data:
                result.checked_items += 1
                try:
                    metadata = PluginMetadata.from_dict(plugin_data)
                    self.plugin_metadata[metadata.name] = metadata
                    result.passed_items += 1
                except Exception as e:
                    result.add_error(f"Failed to import plugin {plugin_data.get('name', 'unknown')}: {str(e)}")

            logger.info(f"Imported {result.passed_items} plugin metadata entries")

        except Exception as e:
            result.add_error(f"Failed to import registry: {str(e)}")

        return result
