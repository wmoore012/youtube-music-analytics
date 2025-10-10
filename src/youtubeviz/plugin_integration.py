"""Plugin system integration for the main youtubeviz package."""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Add the project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.data_organization.scoring_engine import ScoringEngine
from src.data_organization.scoring_storage import ScoringStorage


class PluginIntegrationError(Exception):
    """Raised when plugin integration fails."""

    pass


class YouTubeVizPluginManager:
    """Main plugin manager for youtubeviz package integration."""

    def __init__(self, enable_storage: bool = True):
        """Initialize the plugin manager with youtubeviz integration."""
        self._logger = logging.getLogger(__name__)

        # Initialize core plugin system components
        self.scoring_engine = ScoringEngine(enable_storage=enable_storage)
        self.plugin_manager = self.scoring_engine.plugin_manager
        self.storage = ScoringStorage() if enable_storage else None

        # Track initialization status
        self._initialized = False
        self._default_plugins_loaded = False

    def initialize(self, auto_discover: bool = True) -> Dict[str, Any]:
        """Initialize the plugin system with default configuration."""
        if self._initialized:
            self._logger.warning("Plugin system already initialized")
            return self.get_system_status()

        try:
            # Set up default search paths
            self._setup_default_search_paths()

            # Auto-discover and load plugins if requested
            if auto_discover:
                self._load_default_plugins()

            self._initialized = True
            self._logger.info("Plugin system initialized successfully")

            return self.get_system_status()

        except Exception as e:
            self._logger.error(f"Failed to initialize plugin system: {e}")
            raise PluginIntegrationError(f"Plugin system initialization failed: {e}")

    def _setup_default_search_paths(self) -> None:
        """Set up default search paths for plugin discovery."""
        # Get the project root directory
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent  # Go up to project root

        # Default plugin locations
        default_paths = [
            str(project_root / "src" / "data_organization"),  # Main plugin directory
            str(project_root / "src" / "youtubeviz"),  # YouTubeViz plugins
            str(project_root / "plugins"),  # User plugins directory
        ]

        # Add paths that exist
        for path in default_paths:
            if Path(path).exists():
                try:
                    self.plugin_manager.add_search_path(path)
                    self._logger.debug(f"Added plugin search path: {path}")
                except Exception as e:
                    self._logger.warning(f"Failed to add search path {path}: {e}")

    def _load_default_plugins(self) -> Dict[str, bool]:
        """Load default plugins from the data_organization directory."""
        if self._default_plugins_loaded:
            return {}

        try:
            # Discover and load plugins
            results = self.scoring_engine.discover_and_load_plugins([])

            # Register built-in plugins manually if discovery fails
            self._register_builtin_plugins()

            self._default_plugins_loaded = True
            self._logger.info(f"Loaded {len(results)} plugins from discovery")

            return results

        except Exception as e:  # pragma: no cover - this should be rare and is surfaced loudly
            self._logger.error(f"Failed to load default plugins: {e}")
            raise PluginIntegrationError("Failed to load default plugins") from e

    def _register_builtin_plugins(self) -> None:
        """Register built-in plugins directly."""
        try:
            # Import and register example plugins
            from src.data_organization.example_plugins import (
                EngagementScoringPlugin,
                MomentumScoringPlugin,
                SimpleTestPlugin,
            )

            # Register each plugin
            plugins = [MomentumScoringPlugin(), EngagementScoringPlugin(), SimpleTestPlugin()]

            for plugin in plugins:
                try:
                    self.scoring_engine.register_plugin(plugin)
                    self._logger.info(f"Registered built-in plugin: {plugin.get_name()}")
                except Exception as e:
                    self._logger.warning(f"Failed to register plugin {plugin.get_name()}: {e}")

        except ImportError as e:
            self._logger.warning(f"Could not import built-in plugins: {e}")

    def execute_scoring(
        self,
        algorithm_name: str,
        data: pd.DataFrame,
        parameters: Optional[Dict[str, Any]] = None,
        entity_type: str = "artist",
    ) -> pd.DataFrame:
        """Execute scoring and return results as DataFrame."""
        if not self._initialized:
            self.initialize()

        try:
            result = self.scoring_engine.execute_scoring(
                algorithm_name=algorithm_name, data=data, parameters=parameters, entity_type=entity_type
            )

            return result.entity_scores

        except Exception as e:
            self._logger.error(f"Scoring execution failed: {e}")
            raise PluginIntegrationError(f"Scoring execution failed: {e}")

    def get_available_algorithms(self) -> List[str]:
        """Get list of available scoring algorithms."""
        if not self._initialized:
            self.initialize()

        return self.scoring_engine.get_available_algorithms()

    def get_algorithm_info(self, algorithm_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific algorithm."""
        if not self._initialized:
            self.initialize()

        try:
            return self.scoring_engine.get_plugin_metadata(algorithm_name)
        except Exception as e:
            self._logger.error(f"Failed to get algorithm info for {algorithm_name}: {e}")
            raise PluginIntegrationError(f"Failed to get algorithm info: {e}")

    def validate_plugin_system(self) -> Dict[str, Any]:
        """Validate the entire plugin system."""
        if not self._initialized:
            self.initialize()

        try:
            validation_results = self.scoring_engine.validate_all_plugins()

            # Summarize validation results
            summary = {
                "total_plugins": len(validation_results),
                "valid_plugins": sum(1 for r in validation_results.values() if r.is_valid),
                "invalid_plugins": sum(1 for r in validation_results.values() if not r.is_valid),
                "validation_details": validation_results,
            }

            return summary

        except Exception as e:
            self._logger.error(f"Plugin system validation failed: {e}")
            raise PluginIntegrationError(f"Plugin system validation failed: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        try:
            status = {
                "initialized": self._initialized,
                "default_plugins_loaded": self._default_plugins_loaded,
            }

            if self._initialized:
                engine_status = self.scoring_engine.get_system_status()
                status.update(engine_status)

            return status

        except Exception as e:
            self._logger.error(f"Failed to get system status: {e}")
            return {"error": str(e), "initialized": False}

    def get_scoring_history(
        self, entity_id: str, entity_type: str = "artist", algorithm_name: Optional[str] = None, days_back: int = 30
    ) -> pd.DataFrame:
        """Get scoring history for an entity."""
        if not self._initialized:
            self.initialize()

        if not self.storage:
            raise PluginIntegrationError("Storage is not enabled")

        try:
            return self.scoring_engine.get_scoring_history(
                entity_id=entity_id, entity_type=entity_type, algorithm_name=algorithm_name, days_back=days_back
            )
        except Exception as e:
            self._logger.error(f"Failed to get scoring history: {e}")
            raise PluginIntegrationError(f"Failed to get scoring history: {e}")

    def get_latest_scores(
        self,
        algorithm_name: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_ids: Optional[List[str]] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Get latest scoring results."""
        if not self._initialized:
            self.initialize()

        if not self.storage:
            raise PluginIntegrationError("Storage is not enabled")

        try:
            return self.scoring_engine.get_latest_scores(
                algorithm_name=algorithm_name, entity_type=entity_type, entity_ids=entity_ids, limit=limit
            )
        except Exception as e:
            self._logger.error(f"Failed to get latest scores: {e}")
            raise PluginIntegrationError(f"Failed to get latest scores: {e}")


# Global plugin manager instance
_plugin_manager: Optional[YouTubeVizPluginManager] = None


def get_plugin_manager(enable_storage: bool = True) -> YouTubeVizPluginManager:
    """Get the global plugin manager instance."""
    global _plugin_manager

    if _plugin_manager is None:
        _plugin_manager = YouTubeVizPluginManager(enable_storage=enable_storage)

    return _plugin_manager


def initialize_plugins(auto_discover: bool = True, enable_storage: bool = True) -> Dict[str, Any]:
    """Initialize the plugin system."""
    manager = get_plugin_manager(enable_storage=enable_storage)
    return manager.initialize(auto_discover=auto_discover)


def execute_scoring(
    algorithm_name: str, data: pd.DataFrame, parameters: Optional[Dict[str, Any]] = None, entity_type: str = "artist"
) -> pd.DataFrame:
    """Execute scoring with the specified algorithm."""
    manager = get_plugin_manager()
    return manager.execute_scoring(
        algorithm_name=algorithm_name, data=data, parameters=parameters, entity_type=entity_type
    )


def get_available_algorithms() -> List[str]:
    """Get list of available scoring algorithms."""
    manager = get_plugin_manager()
    return manager.get_available_algorithms()


def get_algorithm_info(algorithm_name: str) -> Dict[str, Any]:
    """Get information about a specific algorithm."""
    manager = get_plugin_manager()
    return manager.get_algorithm_info(algorithm_name)


def validate_plugin_system() -> Dict[str, Any]:
    """Validate the plugin system."""
    manager = get_plugin_manager()
    return manager.validate_plugin_system()


def get_system_status() -> Dict[str, Any]:
    """Get plugin system status."""
    manager = get_plugin_manager()
    return manager.get_system_status()
