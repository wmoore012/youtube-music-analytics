"""Plugin discovery and loading mechanisms."""

import importlib
import importlib.util
import inspect
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional, Type

from .scoring_plugin import ScoringPlugin, ValidationResult


class PluginDiscoveryError(Exception):
    """Raised when plugin discovery fails."""

    pass


class PluginLoadingError(Exception):
    """Raised when plugin loading fails."""

    pass


class PluginValidationError(Exception):
    """Raised when plugin validation fails."""

    pass


class PluginManager:
    """Manages plugin discovery, loading, and validation."""

    def __init__(self):
        """Initialize the plugin manager."""
        self._plugins: Dict[str, Type[ScoringPlugin]] = {}
        self._plugin_instances: Dict[str, ScoringPlugin] = {}
        self._search_paths: List[Path] = []

    def add_search_path(self, path: str) -> None:
        """Add a directory to search for plugins."""
        search_path = Path(path)
        if not search_path.exists():
            raise PluginDiscoveryError(f"Plugin search path does not exist: {path}")

        if not search_path.is_dir():
            raise PluginDiscoveryError(f"Plugin search path is not a directory: {path}")

        self._search_paths.append(search_path)

    def discover_plugins(self, search_paths: Optional[List[str]] = None) -> List[str]:
        """Discover available plugins in search paths."""
        if search_paths:
            # Temporarily add search paths
            original_paths = self._search_paths.copy()
            for path in search_paths:
                self.add_search_path(path)

        discovered_plugins = []

        try:
            for search_path in self._search_paths:
                discovered_plugins.extend(self._discover_plugins_in_path(search_path))
        finally:
            if search_paths:
                # Restore original search paths
                self._search_paths = original_paths

        return discovered_plugins

    def _discover_plugins_in_path(self, search_path: Path) -> List[str]:
        """Discover plugins in a specific path."""
        plugins = []

        # Look for Python files
        for py_file in search_path.glob("**/*.py"):
            if py_file.name.startswith("_"):
                continue  # Skip private modules

            try:
                plugin_classes = self._extract_plugin_classes_from_file(py_file)
                plugins.extend(plugin_classes)
            except Exception as e:
                # Log warning but continue discovery
                print(f"Warning: Failed to process {py_file}: {e}")

        return plugins

    def _extract_plugin_classes_from_file(self, file_path: Path) -> List[str]:
        """Extract plugin classes from a Python file."""
        plugin_classes = []

        try:
            # Load the module
            spec = importlib.util.spec_from_file_location("temp_module", file_path)
            if spec is None or spec.loader is None:
                return plugin_classes

            module = importlib.util.module_from_spec(spec)

            try:
                spec.loader.exec_module(module)
            except Exception as e:
                raise PluginLoadingError(f"Failed to load module {file_path}: {e}")

            # Find ScoringPlugin subclasses
            for name, klass in inspect.getmembers(module, inspect.isclass):
                try:
                    if (
                        issubclass(klass, ScoringPlugin)
                        and klass is not ScoringPlugin
                        and not inspect.isabstract(klass)
                    ):

                        plugin_classes.append(f"{module.__name__}.{name}")
                except TypeError:
                    # Skip objects that can't be checked with issubclass
                    continue

        except Exception as e:
            # Log the error but don't fail the entire discovery process
            print(f"Warning: Failed to process {file_path}: {e}")

        return plugin_classes

    def load_plugin(self, plugin_class_path: str) -> None:
        """Load a plugin by its class path."""
        try:
            # Split module and class name
            if "." not in plugin_class_path:
                raise PluginLoadingError(f"Invalid plugin class path: {plugin_class_path}")

            module_path, class_name = plugin_class_path.rsplit(".", 1)

            # Import the module
            module = importlib.import_module(module_path)

            # Get the plugin class
            if not hasattr(module, class_name):
                raise PluginLoadingError(f"Class {class_name} not found in module {module_path}")

            plugin_class = getattr(module, class_name)

            # Check if it's None or not a class
            if plugin_class is None:
                raise PluginLoadingError(f"Class {class_name} not found in module {module_path}")

            if not inspect.isclass(plugin_class):
                raise PluginValidationError(f"{class_name} is not a class")

            # Validate it's a ScoringPlugin subclass
            if not issubclass(plugin_class, ScoringPlugin):
                raise PluginValidationError(f"Class {class_name} is not a ScoringPlugin subclass")

            # Validate the plugin class
            validation_result = self._validate_plugin_class(plugin_class)
            if not validation_result.is_valid:
                raise PluginValidationError(f"Plugin validation failed: {validation_result.errors}")

            # Store the plugin class
            plugin_name = plugin_class().get_name()
            self._plugins[plugin_name] = plugin_class

        except ImportError as e:
            raise PluginLoadingError(f"Failed to import plugin module {module_path}: {e}")
        except (PluginLoadingError, PluginValidationError):
            raise  # Re - raise these specific exceptions
        except Exception as e:
            raise PluginLoadingError(f"Failed to load plugin {plugin_class_path}: {e}")

    def _validate_plugin_class(self, plugin_class: Type[ScoringPlugin]) -> ValidationResult:
        """Validate a plugin class before loading."""
        errors = []
        warnings = []

        # Check if class is abstract
        if inspect.isabstract(plugin_class):
            errors.append("Plugin class is abstract and cannot be instantiated")

        # Try to instantiate the plugin
        try:
            plugin_instance = plugin_class()
        except Exception as e:
            errors.append(f"Failed to instantiate plugin: {e}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings, checked_items=1, passed_items=0)

        # Validate required methods
        required_methods = ["get_name", "get_version", "get_parameters", "calculate_scores", "validate_input"]

        for method_name in required_methods:
            if not hasattr(plugin_instance, method_name):
                errors.append(f"Plugin missing required method: {method_name}")
            elif not callable(getattr(plugin_instance, method_name)):
                errors.append(f"Plugin method {method_name} is not callable")

        # Validate plugin metadata
        try:
            metadata = plugin_instance.get_metadata()
            metadata_validation = metadata.validate()
            if not metadata_validation.is_valid:
                errors.extend(metadata_validation.errors)
                warnings.extend(metadata_validation.warnings)
        except Exception as e:
            errors.append(f"Failed to get plugin metadata: {e}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(required_methods) + 1,
            passed_items=len(required_methods) + 1 - len(errors),
        )

    def get_plugin_instance(self, plugin_name: str) -> ScoringPlugin:
        """Get an instance of a loaded plugin."""
        if plugin_name not in self._plugins:
            raise PluginLoadingError(f"Plugin {plugin_name} not loaded")

        # Create new instance if not cached
        if plugin_name not in self._plugin_instances:
            plugin_class = self._plugins[plugin_name]
            self._plugin_instances[plugin_name] = plugin_class()

        return self._plugin_instances[plugin_name]

    def get_available_plugins(self) -> List[str]:
        """Get list of available plugin names."""
        return list(self._plugins.keys())

    def unload_plugin(self, plugin_name: str) -> None:
        """Unload a plugin."""
        if plugin_name in self._plugins:
            del self._plugins[plugin_name]

        if plugin_name in self._plugin_instances:
            del self._plugin_instances[plugin_name]

    def reload_plugin(self, plugin_name: str) -> None:
        """Reload a plugin (useful for development)."""
        if plugin_name not in self._plugins:
            raise PluginLoadingError(f"Plugin {plugin_name} not loaded")

        # Get the plugin class
        plugin_class = self._plugins[plugin_name]
        module = inspect.getmodule(plugin_class)

        if module is None:
            raise PluginLoadingError(f"Cannot find module for plugin {plugin_name}")

        # Reload the module
        importlib.reload(module)

        # Remove cached instance
        if plugin_name in self._plugin_instances:
            del self._plugin_instances[plugin_name]

    def validate_all_plugins(self) -> Dict[str, ValidationResult]:
        """Validate all loaded plugins."""
        results = {}

        for plugin_name, plugin_class in self._plugins.items():
            results[plugin_name] = self._validate_plugin_class(plugin_class)

        return results

    def clear_plugins(self) -> None:
        """Clear all loaded plugins."""
        self._plugins.clear()
        self._plugin_instances.clear()
