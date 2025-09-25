"""Integration layer between ETL pipeline and plugin system."""

import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import pymysql

from src.youtubeviz.plugin_integration import get_plugin_manager


class ETLPluginIntegrationError(Exception):
    """Raised when ETL plugin integration fails."""

    pass


class ETLPluginIntegrator:
    """Integrates plugin system with ETL pipeline operations."""

    def __init__(self):
        """Initialize ETL plugin integrator."""
        self._logger = logging.getLogger(__name__)

        # Database connection parameters from environment
        self.db_args = dict(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER") or "",
            password=os.getenv("DB_PASS") or "",
            db=os.getenv("DB_NAME") or "",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10,
            read_timeout=15,
            write_timeout=15,
        )

        # Plugin manager
        self._plugin_manager = None

    def _get_plugin_manager(self):
        """Get or initialize plugin manager."""
        if self._plugin_manager is None:
            self._plugin_manager = get_plugin_manager(enable_storage=True)
            # Initialize if not already done
            if not self._plugin_manager._initialized:
                self._plugin_manager.initialize()
        return self._plugin_manager

    def _connect(self) -> pymysql.Connection:
        """Create database connection."""
        return pymysql.connect(**self.db_args)

    def execute_artist_scoring(
        self, algorithm_name: str, parameters: Optional[Dict[str, Any]] = None, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Execute scoring for artists using real database data."""
        try:
            # Get artist data from database
            artist_data = self._get_artist_data(limit=limit)

            if artist_data.empty:
                self._logger.warning("No artist data found for scoring")
                return {"success": False, "message": "No artist data available"}

            # Execute scoring
            plugin_manager = self._get_plugin_manager()
            scores_df = plugin_manager.execute_scoring(
                algorithm_name=algorithm_name, data=artist_data, parameters=parameters, entity_type="artist"
            )

            self._logger.info(f"Scored {len(scores_df)} artists with algorithm {algorithm_name}")

            return {
                "success": True,
                "algorithm": algorithm_name,
                "scored_entities": len(scores_df),
                "scores": scores_df.to_dict("records"),
            }

        except Exception as e:
            self._logger.error(f"Artist scoring failed: {e}")
            raise ETLPluginIntegrationError(f"Artist scoring failed: {e}")

    def execute_video_scoring(
        self, algorithm_name: str, parameters: Optional[Dict[str, Any]] = None, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Execute scoring for videos using real database data."""
        try:
            # Get video data from database
            video_data = self._get_video_data(limit=limit)

            if video_data.empty:
                self._logger.warning("No video data found for scoring")
                return {"success": False, "message": "No video data available"}

            # Execute scoring
            plugin_manager = self._get_plugin_manager()
            scores_df = plugin_manager.execute_scoring(
                algorithm_name=algorithm_name, data=video_data, parameters=parameters, entity_type="video"
            )

            self._logger.info(f"Scored {len(scores_df)} videos with algorithm {algorithm_name}")

            return {
                "success": True,
                "algorithm": algorithm_name,
                "scored_entities": len(scores_df),
                "scores": scores_df.to_dict("records"),
            }

        except Exception as e:
            self._logger.error(f"Video scoring failed: {e}")
            raise ETLPluginIntegrationError(f"Video scoring failed: {e}")

    def _get_artist_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Get artist performance data from database."""
        query = """
        SELECT
            artist_name as entity_id,
            COUNT(DISTINCT v.video_id) as video_count,
            AVG(CAST(v.view_count AS SIGNED)) as avg_views,
            AVG(CAST(v.like_count AS SIGNED)) as avg_likes,
            AVG(CAST(v.comment_count AS SIGNED)) as avg_comments,
            MAX(v.published_at) as latest_video,
            AVG(COALESCE(s.avg_sentiment, 0)) as avg_sentiment,
            COUNT(DISTINCT c.comment_id) as total_comments
        FROM youtube_videos v
        LEFT JOIN youtube_sentiment_summary s ON v.video_id = s.video_id
        LEFT JOIN youtube_comments c ON v.video_id = c.video_id
        WHERE v.artist_name IS NOT NULL
        AND v.artist_name != ''
        GROUP BY artist_name
        HAVING video_count > 0
        ORDER BY avg_views DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            with self._connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            self._logger.error(f"Failed to get artist data: {e}")
            return pd.DataFrame()

    def _get_video_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Get video performance data from database."""
        query = """
        SELECT
            v.video_id as entity_id,
            v.title,
            v.artist_name,
            CAST(v.view_count AS SIGNED) as view_count,
            CAST(v.like_count AS SIGNED) as like_count,
            CAST(v.comment_count AS SIGNED) as comment_count,
            v.published_at,
            COALESCE(s.avg_sentiment, 0) as avg_sentiment,
            COALESCE(s.comment_count, 0) as sentiment_comment_count,
            DATEDIFF(NOW(), v.published_at) as days_since_published
        FROM youtube_videos v
        LEFT JOIN youtube_sentiment_summary s ON v.video_id = s.video_id
        WHERE v.view_count IS NOT NULL
        AND v.view_count > 0
        ORDER BY v.published_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            with self._connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            self._logger.error(f"Failed to get video data: {e}")
            return pd.DataFrame()

    def run_scoring_pipeline(
        self,
        algorithms: Optional[List[str]] = None,
        entity_types: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Run complete scoring pipeline for specified algorithms and entity types."""
        if algorithms is None:
            plugin_manager = self._get_plugin_manager()
            algorithms = plugin_manager.get_available_algorithms()

        if entity_types is None:
            entity_types = ["artist", "video"]

        results = {
            "success": True,
            "algorithms_run": [],
            "entity_types_processed": [],
            "total_scores_generated": 0,
            "errors": [],
        }

        try:
            for algorithm in algorithms:
                for entity_type in entity_types:
                    try:
                        if entity_type == "artist":
                            result = self.execute_artist_scoring(algorithm, limit=limit)
                        elif entity_type == "video":
                            result = self.execute_video_scoring(algorithm, limit=limit)
                        else:
                            self._logger.warning(f"Unknown entity type: {entity_type}")
                            continue

                        if result["success"]:
                            results["algorithms_run"].append(algorithm)
                            if entity_type not in results["entity_types_processed"]:
                                results["entity_types_processed"].append(entity_type)
                            results["total_scores_generated"] += result["scored_entities"]

                            self._logger.info(
                                f"Successfully scored {result['scored_entities']} {entity_type}s " f"with {algorithm}"
                            )
                        else:
                            results["errors"].append(f"{algorithm} on {entity_type}: {result['message']}")

                    except Exception as e:
                        error_msg = f"Failed to run {algorithm} on {entity_type}: {e}"
                        results["errors"].append(error_msg)
                        self._logger.error(error_msg)

            # Update success status based on whether any scoring succeeded
            results["success"] = results["total_scores_generated"] > 0

            return results

        except Exception as e:
            self._logger.error(f"Scoring pipeline failed: {e}")
            results["success"] = False
            results["errors"].append(f"Pipeline failure: {e}")
            return results

    def get_plugin_system_status(self) -> Dict[str, Any]:
        """Get status of the plugin system integration."""
        try:
            plugin_manager = self._get_plugin_manager()
            status = plugin_manager.get_system_status()

            # Add database connectivity check
            try:
                with self._connect() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        status["database_connected"] = True
            except Exception as e:
                status["database_connected"] = False
                status["database_error"] = str(e)

            # Add data availability check
            try:
                artist_count = len(self._get_artist_data(limit=1))
                video_count = len(self._get_video_data(limit=1))
                status["data_available"] = {"artists": artist_count > 0, "videos": video_count > 0}
            except Exception as e:
                status["data_available"] = {"error": str(e)}

            return status

        except Exception as e:
            self._logger.error(f"Failed to get plugin system status: {e}")
            return {"error": str(e), "initialized": False}


# Global integrator instance
_etl_integrator: Optional[ETLPluginIntegrator] = None


def get_etl_integrator() -> ETLPluginIntegrator:
    """Get the global ETL plugin integrator instance."""
    global _etl_integrator

    if _etl_integrator is None:
        _etl_integrator = ETLPluginIntegrator()

    return _etl_integrator


def run_etl_scoring_pipeline(
    algorithms: Optional[List[str]] = None, entity_types: Optional[List[str]] = None, limit: Optional[int] = None
) -> Dict[str, Any]:
    """Run scoring pipeline as part of ETL process."""
    integrator = get_etl_integrator()
    return integrator.run_scoring_pipeline(algorithms=algorithms, entity_types=entity_types, limit=limit)


def get_etl_plugin_status() -> Dict[str, Any]:
    """Get ETL plugin integration status."""
    integrator = get_etl_integrator()
    return integrator.get_plugin_system_status()
