"""Main scoring engine with plugin management capabilities."""

import logging
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd

from .plugin_manager import PluginLoadingError, PluginManager, PluginValidationError
from .scoring_plugin import ScoringPlugin, ScoringResult, ValidationResult


class ScoringEngineError(Exception):
    """Base class for scoring engine errors."""

    pass


class AlgorithmNotFoundError(ScoringEngineError):
    """Raised when requested algorithm is not available."""

    pass


class ScoringExecutionError(ScoringEngineError):
    """Raised when scoring execution fails."""

    pass


class ScoringEngine:
    """Main scoring engine with plugin support."""

    def __init__(self, config_manager=None):
        """Initialize the scoring engine."""
        self.config_manager = config_manager
        self.plugin_manager = PluginManager()
        self._logger = logging.getLogger(__name__)

        # Plugin isolation settings
        self._enable_plugin_isolation = True
        self._max_execution_time = 300  # 5 minutes default timeout
        self._max_memory_usage = 1024 * 1024 * 1024  # 1GB default limit

    def register_plugin(self, plugin: ScoringPlugin) -> None:
        """Register a plugin instance directly."""
        try:
            # Validate the plugin
            validation_result = self._validate_plugin_instance(plugin)
            if not validation_result.is_valid:
                raise PluginValidationError(f"Plugin validation failed: {validation_result.errors}")

            # Store the plugin class for the plugin manager
            plugin_name = plugin.get_name()
            plugin_class = type(plugin)
            self.plugin_manager._plugins[plugin_name] = plugin_class
            self.plugin_manager._plugin_instances[plugin_name] = plugin

            self._logger.info(f"Successfully registered plugin: {plugin_name}")

        except Exception as e:
            self._logger.error(f"Failed to register plugin: {e}")
            raise ScoringEngineError(f"Plugin registration failed: {e}")

    def _validate_plugin_instance(self, plugin: ScoringPlugin) -> ValidationResult:
        """Validate a plugin instance."""
        errors = []
        warnings = []

        # Check if it's a ScoringPlugin instance
        if not isinstance(plugin, ScoringPlugin):
            errors.append("Object is not a ScoringPlugin instance")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings, checked_items=1, passed_items=0)

        # Validate required methods work
        try:
            name = plugin.get_name()
            if not name or not isinstance(name, str):
                errors.append("Plugin name must be a non-empty string")
        except Exception as e:
            errors.append(f"Failed to get plugin name: {e}")

        try:
            version = plugin.get_version()
            if not version or not isinstance(version, str):
                errors.append("Plugin version must be a non-empty string")
        except Exception as e:
            errors.append(f"Failed to get plugin version: {e}")

        try:
            parameters = plugin.get_parameters()
            if not isinstance(parameters, dict):
                errors.append("Plugin parameters must be a dictionary")
        except Exception as e:
            errors.append(f"Failed to get plugin parameters: {e}")

        # Validate metadata
        try:
            metadata = plugin.get_metadata()
            metadata_validation = metadata.validate()
            if not metadata_validation.is_valid:
                errors.extend(metadata_validation.errors)
                warnings.extend(metadata_validation.warnings)
        except Exception as e:
            errors.append(f"Failed to get plugin metadata: {e}")

        return ValidationResult(
            is_valid=len(errors) == 0, errors=errors, warnings=warnings, checked_items=4, passed_items=4 - len(errors)
        )

    def load_plugin_from_path(self, plugin_class_path: str) -> None:
        """Load a plugin from its class path."""
        try:
            self.plugin_manager.load_plugin(plugin_class_path)
            self._logger.info(f"Successfully loaded plugin from path: {plugin_class_path}")
        except (PluginLoadingError, PluginValidationError) as e:
            self._logger.error(f"Failed to load plugin from path {plugin_class_path}: {e}")
            raise ScoringEngineError(f"Plugin loading failed: {e}")

    def discover_and_load_plugins(self, search_paths: List[str]) -> Dict[str, bool]:
        """Discover and load plugins from search paths."""
        results = {}

        try:
            # Add search paths
            for path in search_paths:
                self.plugin_manager.add_search_path(path)

            # Discover plugins
            discovered_plugins = self.plugin_manager.discover_plugins()
            self._logger.info(f"Discovered {len(discovered_plugins)} plugins")

            # Load each discovered plugin
            for plugin_path in discovered_plugins:
                try:
                    self.plugin_manager.load_plugin(plugin_path)
                    results[plugin_path] = True
                    self._logger.info(f"Successfully loaded plugin: {plugin_path}")
                except Exception as e:
                    results[plugin_path] = False
                    self._logger.warning(f"Failed to load plugin {plugin_path}: {e}")

        except Exception as e:
            self._logger.error(f"Plugin discovery failed: {e}")
            raise ScoringEngineError(f"Plugin discovery failed: {e}")

        return results

    def execute_scoring(
        self, algorithm_name: str, data: pd.DataFrame, parameters: Optional[Dict[str, Any]] = None
    ) -> ScoringResult:
        """Execute scoring with the specified algorithm."""
        if algorithm_name not in self.get_available_algorithms():
            raise AlgorithmNotFoundError(f"Algorithm '{algorithm_name}' not found")

        try:
            # Get plugin instance
            plugin = self.plugin_manager.get_plugin_instance(algorithm_name)

            # Execute with isolation if enabled
            if self._enable_plugin_isolation:
                return self._execute_with_isolation(plugin, data, parameters)
            else:
                return plugin.execute(data, parameters)

        except Exception as e:
            self._logger.error(f"Scoring execution failed for algorithm {algorithm_name}: {e}")
            self._logger.error(f"Traceback: {traceback.format_exc()}")
            raise ScoringExecutionError(f"Scoring execution failed: {e}")

    def _execute_with_isolation(
        self, plugin: ScoringPlugin, data: pd.DataFrame, parameters: Optional[Dict[str, Any]] = None
    ) -> ScoringResult:
        """Execute plugin with isolation and error handling."""
        import resource
        import signal
        from contextlib import contextmanager

        @contextmanager
        def timeout_context(seconds):
            """Context manager for execution timeout."""

            def timeout_handler(signum, frame):
                raise TimeoutError(f"Plugin execution exceeded {seconds} seconds")

            # Set up the timeout
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(seconds)

            try:
                yield
            finally:
                # Clean up
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

        try:
            # Set memory limit (if supported on the platform)
            try:
                resource.setrlimit(resource.RLIMIT_AS, (self._max_memory_usage, self._max_memory_usage))
            except (OSError, AttributeError, ValueError):
                # Memory limiting not supported on this platform or current limit exceeds maximum
                self._logger.warning("Memory limiting not supported on this platform")

            # Execute with timeout
            with timeout_context(self._max_execution_time):
                result = plugin.execute(data, parameters)

            return result

        except TimeoutError as e:
            self._logger.error(f"Plugin execution timed out: {e}")
            raise ScoringExecutionError(f"Plugin execution timed out: {e}")
        except MemoryError as e:
            self._logger.error(f"Plugin execution exceeded memory limit: {e}")
            raise ScoringExecutionError(f"Plugin execution exceeded memory limit: {e}")
        except Exception as e:
            self._logger.error(f"Plugin execution failed with error: {e}")
            raise ScoringExecutionError(f"Plugin execution failed: {e}")

    def get_available_algorithms(self) -> List[str]:
        """Get list of available algorithm names."""
        return self.plugin_manager.get_available_plugins()

    def validate_plugin(self, plugin: ScoringPlugin) -> ValidationResult:
        """Validate a plugin instance."""
        return self._validate_plugin_instance(plugin)

    def get_plugin_metadata(self, algorithm_name: str) -> Dict[str, Any]:
        """Get metadata for a specific algorithm."""
        if algorithm_name not in self.get_available_algorithms():
            raise AlgorithmNotFoundError(f"Algorithm '{algorithm_name}' not found")

        plugin = self.plugin_manager.get_plugin_instance(algorithm_name)
        return plugin.get_metadata().to_dict()

    def validate_all_plugins(self) -> Dict[str, ValidationResult]:
        """Validate all loaded plugins."""
        return self.plugin_manager.validate_all_plugins()

    def unload_plugin(self, algorithm_name: str) -> None:
        """Unload a specific plugin."""
        self.plugin_manager.unload_plugin(algorithm_name)
        self._logger.info(f"Unloaded plugin: {algorithm_name}")

    def reload_plugin(self, algorithm_name: str) -> None:
        """Reload a specific plugin."""
        try:
            self.plugin_manager.reload_plugin(algorithm_name)
            self._logger.info(f"Reloaded plugin: {algorithm_name}")
        except Exception as e:
            self._logger.error(f"Failed to reload plugin {algorithm_name}: {e}")
            raise ScoringEngineError(f"Plugin reload failed: {e}")

    def set_isolation_settings(
        self, enable_isolation: bool = True, max_execution_time: int = 300, max_memory_usage: int = 1024 * 1024 * 1024
    ) -> None:
        """Configure plugin isolation settings."""
        self._enable_plugin_isolation = enable_isolation
        self._max_execution_time = max_execution_time
        self._max_memory_usage = max_memory_usage

        self._logger.info(
            f"Updated isolation settings: isolation={enable_isolation}, "
            f"timeout={max_execution_time}s, memory_limit={max_memory_usage} bytes"
        )

    def get_system_status(self) -> Dict[str, Any]:
        """Get system status information."""
        return {
            "loaded_plugins": len(self.get_available_algorithms()),
            "available_algorithms": self.get_available_algorithms(),
            "isolation_enabled": self._enable_plugin_isolation,
            "max_execution_time": self._max_execution_time,
            "max_memory_usage": self._max_memory_usage,
            "search_paths": [str(path) for path in self.plugin_manager._search_paths],
        }
