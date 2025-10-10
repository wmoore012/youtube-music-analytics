"""Scoring results storage system for database persistence."""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .scoring_plugin import ScoringResult, ValidationResult


class ScoringStorageError(Exception):
    """Base class for scoring storage errors."""

    pass


class AlgorithmNotRegisteredError(ScoringStorageError):
    """Raised when trying to store results for unregistered algorithm."""

    pass


class ScoringStorage:
    """
    Manages storage and retrieval of scoring results in the database.

    Handles:
    - Algorithm registration and metadata storage
    - Scoring result persistence with full metadata
    - Query and filtering capabilities for historical analysis
    - Configuration management per environment
    """

    def __init__(self, engine=None):
        """Initialize scoring storage with database engine."""
        if engine is None:
            from web.etl_helpers import get_engine

            engine = get_engine()
        self.engine = engine

    def register_algorithm(self, algorithm_name: str, version: str, description: str = None, author: str = None) -> str:
        """
        Register a scoring algorithm in the database.

        Args:
            algorithm_name: Unique name for the algorithm
            version: Version string (e.g., "1.0.0")
            description: Optional description of the algorithm
            author: Optional author information

        Returns:
            algorithm_id: Generated unique ID for the algorithm

        Raises:
            ScoringStorageError: If registration fails
        """
        algorithm_id = f"{algorithm_name}_{version}".replace(".", "_")

        try:
            with self.engine.connect() as conn:
                # Check if algorithm already exists
                check_query = text(
                    """
                    SELECT algorithm_id FROM scoring_algorithms
                    WHERE algorithm_name = :name AND version = :version
                """
                )
                existing = conn.execute(check_query, {"name": algorithm_name, "version": version}).fetchone()

                if existing:
                    return existing[0]

                # Insert new algorithm
                insert_query = text(
                    """
                    INSERT INTO scoring_algorithms
                    (algorithm_id, algorithm_name, version, description, author, is_active)
                    VALUES (:id, :name, :version, :description, :author, TRUE)
                """
                )

                conn.execute(
                    insert_query,
                    {
                        "id": algorithm_id,
                        "name": algorithm_name,
                        "version": version,
                        "description": description,
                        "author": author,
                    },
                )
                conn.commit()

                return algorithm_id

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to register algorithm: {e}")

    def store_scoring_result(
        self, result: ScoringResult, entity_type: str = "artist", run_metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Store a scoring result in the database.

        Args:
            result: ScoringResult object from plugin execution
            entity_type: Type of entity being scored (artist, video, channel, etc.)
            run_metadata: Optional metadata about the scoring run

        Returns:
            run_id: Unique identifier for this scoring run

        Raises:
            ScoringStorageError: If storage fails
        """
        run_id = str(uuid.uuid4())

        try:
            # Register algorithm if not exists
            algorithm_id = self.register_algorithm(
                result.algorithm_name,
                result.algorithm_version,
                description=f"Scoring algorithm: {result.algorithm_name}",
            )

            with self.engine.connect() as conn:
                # Create scoring run record
                run_query = text(
                    """
                    INSERT INTO scoring_runs
                    (run_id, algorithm_id, run_timestamp, input_record_count,
                     output_record_count, status, parameters_used, metadata)
                    VALUES (:run_id, :algorithm_id, :timestamp, :input_count,
                            :output_count, 'completed', :parameters, :metadata)
                """
                )

                conn.execute(
                    run_query,
                    {
                        "run_id": run_id,
                        "algorithm_id": algorithm_id,
                        "timestamp": result.calculation_timestamp,
                        "input_count": result.metadata.get("input_record_count", 0),
                        "output_count": len(result.entity_scores),
                        "parameters": json.dumps(result.metadata.get("parameters", {})),
                        "metadata": json.dumps(run_metadata or {}),
                    },
                )

                # Store individual scoring results
                self._store_entity_scores(
                    conn, run_id, algorithm_id, result.entity_scores, entity_type, result.calculation_timestamp
                )

                conn.commit()
                return run_id

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to store scoring result: {e}")

    def _store_entity_scores(
        self, conn, run_id: str, algorithm_id: str, scores_df: pd.DataFrame, entity_type: str, timestamp: datetime
    ):
        """Store individual entity scores in the database."""

        # Prepare batch insert data
        results_data = []
        metrics_data = []

        for _, row in scores_df.iterrows():
            entity_id = str(row.get("entity_id", "unknown"))
            score_value = float(row.get("score_value", 0.0))
            confidence = row.get("confidence", None)

            # Main score record
            result_record = {
                "run_id": run_id,
                "algorithm_id": algorithm_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "score_type": "primary",  # Default score type
                "score_value": score_value,
                "confidence_level": float(confidence) if confidence is not None else None,
                "calculation_timestamp": timestamp,
                "metadata": json.dumps(
                    {k: v for k, v in row.items() if k not in ["entity_id", "score_value", "confidence"]}
                ),
            }
            results_data.append(result_record)

            # Additional metrics as separate records
            for col, value in row.items():
                if col not in ["entity_id", "score_value", "confidence"] and pd.notna(value):
                    if isinstance(value, (int, float)):
                        metrics_data.append(
                            {
                                "entity_id": entity_id,
                                "metric_name": col,
                                "metric_value": float(value),
                                "metric_text": None,
                            }
                        )
                    else:
                        metrics_data.append(
                            {
                                "entity_id": entity_id,
                                "metric_name": col,
                                "metric_value": None,
                                "metric_text": str(value)[:500],  # Truncate long text
                            }
                        )

        # Batch insert results
        if results_data:
            results_query = text(
                """
                INSERT INTO scoring_results
                (run_id, algorithm_id, entity_type, entity_id, score_type,
                 score_value, confidence_level, calculation_timestamp, metadata)
                VALUES (:run_id, :algorithm_id, :entity_type, :entity_id, :score_type,
                        :score_value, :confidence_level, :calculation_timestamp, :metadata)
            """
            )
            conn.execute(results_query, results_data)

            # Get result IDs for metrics
            if metrics_data:
                result_ids_query = text(
                    """
                    SELECT result_id, entity_id FROM scoring_results
                    WHERE run_id = :run_id ORDER BY result_id
                """
                )
                result_ids = conn.execute(result_ids_query, {"run_id": run_id}).fetchall()

                # Map entity_id to result_id
                entity_to_result = {row[1]: row[0] for row in result_ids}

                # Add result_id to metrics data
                for metric in metrics_data:
                    metric["result_id"] = entity_to_result.get(metric["entity_id"])
                    if metric["result_id"] is None:
                        continue  # Skip if no matching result_id

                # Insert metrics
                metrics_query = text(
                    """
                    INSERT INTO scoring_metrics
                    (result_id, metric_name, metric_value, metric_text)
                    VALUES (:result_id, :metric_name, :metric_value, :metric_text)
                """
                )
                valid_metrics = [m for m in metrics_data if m.get("result_id")]
                if valid_metrics:
                    conn.execute(metrics_query, valid_metrics)

    def get_latest_scores(
        self, algorithm_name: str = None, entity_type: str = None, entity_ids: List[str] = None, limit: int = 100
    ) -> pd.DataFrame:
        """
        Retrieve latest scoring results with optional filtering.

        Args:
            algorithm_name: Filter by algorithm name
            entity_type: Filter by entity type (artist, video, etc.)
            entity_ids: Filter by specific entity IDs
            limit: Maximum number of results to return

        Returns:
            DataFrame with latest scoring results
        """
        try:
            with self.engine.connect() as conn:
                query = """
                    SELECT
                        lsr.entity_type,
                        lsr.entity_id,
                        lsr.algorithm_name,
                        lsr.score_type,
                        lsr.score_value,
                        lsr.confidence_level,
                        lsr.calculation_timestamp,
                        lsr.metadata
                    FROM latest_scoring_results lsr
                    WHERE lsr.rn = 1
                """

                params = {}
                conditions = []

                if algorithm_name:
                    conditions.append("lsr.algorithm_name = :algorithm_name")
                    params["algorithm_name"] = algorithm_name

                if entity_type:
                    conditions.append("lsr.entity_type = :entity_type")
                    params["entity_type"] = entity_type

                if entity_ids:
                    placeholders = ",".join([f":entity_{i}" for i in range(len(entity_ids))])
                    conditions.append(f"lsr.entity_id IN ({placeholders})")
                    for i, entity_id in enumerate(entity_ids):
                        params[f"entity_{i}"] = entity_id

                if conditions:
                    query += " AND " + " AND ".join(conditions)

                query += f" ORDER BY lsr.calculation_timestamp DESC LIMIT {limit}"

                return pd.read_sql(text(query), conn, params=params)

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to retrieve scores: {e}")

    def get_scoring_history(
        self, entity_id: str, entity_type: str = "artist", algorithm_name: str = None, days_back: int = 30
    ) -> pd.DataFrame:
        """
        Get scoring history for a specific entity.

        Args:
            entity_id: ID of the entity to get history for
            entity_type: Type of entity
            algorithm_name: Optional algorithm filter
            days_back: Number of days of history to retrieve

        Returns:
            DataFrame with scoring history over time
        """
        try:
            with self.engine.connect() as conn:
                query = text(
                    """
                    SELECT
                        sr.calculation_timestamp,
                        sa.algorithm_name,
                        sa.version,
                        sr.score_value,
                        sr.confidence_level,
                        sr.metadata
                    FROM scoring_results sr
                    JOIN scoring_algorithms sa ON sr.algorithm_id = sa.algorithm_id
                    WHERE sr.entity_id = :entity_id
                    AND sr.entity_type = :entity_type
                    AND sr.calculation_timestamp >= DATE_SUB(NOW(), INTERVAL :days_back DAY)
                    AND (:algorithm_name IS NULL OR sa.algorithm_name = :algorithm_name)
                    ORDER BY sr.calculation_timestamp DESC
                """
                )

                return pd.read_sql(
                    query,
                    conn,
                    params={
                        "entity_id": entity_id,
                        "entity_type": entity_type,
                        "algorithm_name": algorithm_name,
                        "days_back": days_back,
                    },
                )

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to retrieve scoring history: {e}")

    def get_algorithm_performance(self, algorithm_name: str = None) -> pd.DataFrame:
        """
        Get performance statistics for scoring algorithms.

        Args:
            algorithm_name: Optional filter for specific algorithm

        Returns:
            DataFrame with algorithm performance metrics
        """
        try:
            with self.engine.connect() as conn:
                query = """
                    SELECT
                        srs.algorithm_name,
                        srs.version,
                        COUNT(DISTINCT srs.run_id) as total_runs,
                        SUM(srs.result_count) as total_results,
                        AVG(srs.avg_score) as overall_avg_score,
                        AVG(srs.avg_confidence) as overall_avg_confidence,
                        MAX(srs.run_timestamp) as last_run,
                        MIN(srs.run_timestamp) as first_run
                    FROM scoring_result_summary srs
                    WHERE (:algorithm_name IS NULL OR srs.algorithm_name = :algorithm_name)
                    GROUP BY srs.algorithm_name, srs.version
                    ORDER BY last_run DESC
                """

                return pd.read_sql(text(query), conn, params={"algorithm_name": algorithm_name})

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to retrieve algorithm performance: {e}")

    def cleanup_old_results(self, days_to_keep: int = 90) -> int:
        """
        Clean up old scoring results to manage database size.

        Args:
            days_to_keep: Number of days of results to keep

        Returns:
            Number of records deleted
        """
        try:
            with self.engine.connect() as conn:
                # Delete old results (cascades to metrics)
                delete_query = text(
                    """
                    DELETE FROM scoring_results
                    WHERE calculation_timestamp < DATE_SUB(NOW(), INTERVAL :days_to_keep DAY)
                """
                )

                result = conn.execute(delete_query, {"days_to_keep": days_to_keep})
                deleted_count = result.rowcount

                # Clean up orphaned runs
                cleanup_runs_query = text(
                    """
                    DELETE sr FROM scoring_runs sr
                    LEFT JOIN scoring_results res ON sr.run_id = res.run_id
                    WHERE res.run_id IS NULL
                """
                )
                conn.execute(cleanup_runs_query)

                conn.commit()
                return deleted_count

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to cleanup old results: {e}")

    def get_entity_rankings(
        self, algorithm_name: str, entity_type: str = "artist", score_type: str = "primary", limit: int = 50
    ) -> pd.DataFrame:
        """
        Get entity rankings based on latest scores.

        Args:
            algorithm_name: Algorithm to rank by
            entity_type: Type of entities to rank
            score_type: Type of score to rank by
            limit: Maximum number of entities to return

        Returns:
            DataFrame with ranked entities
        """
        try:
            with self.engine.connect() as conn:
                query = text(
                    """
                    SELECT
                        sr.entity_id,
                        sr.score_value,
                        sr.confidence_level,
                        sr.calculation_timestamp,
                        RANK() OVER (ORDER BY sr.score_value DESC) as ranking
                    FROM scoring_results sr
                    JOIN scoring_algorithms sa ON sr.algorithm_id = sa.algorithm_id
                    WHERE sa.algorithm_name = :algorithm_name
                    AND sr.entity_type = :entity_type
                    AND sr.score_type = :score_type
                    AND sr.calculation_timestamp = (
                        SELECT MAX(sr2.calculation_timestamp)
                        FROM scoring_results sr2
                        JOIN scoring_algorithms sa2 ON sr2.algorithm_id = sa2.algorithm_id
                        WHERE sa2.algorithm_name = :algorithm_name
                        AND sr2.entity_type = :entity_type
                        AND sr2.entity_id = sr.entity_id
                    )
                    ORDER BY sr.score_value DESC
                    LIMIT :limit
                """
                )

                return pd.read_sql(
                    query,
                    conn,
                    params={
                        "algorithm_name": algorithm_name,
                        "entity_type": entity_type,
                        "score_type": score_type,
                        "limit": limit,
                    },
                )

        except SQLAlchemyError as e:
            raise ScoringStorageError(f"Failed to retrieve entity rankings: {e}")

    def validate_schema(self) -> ValidationResult:
        """
        Validate that the scoring schema exists and is properly configured.

        Returns:
            ValidationResult indicating schema status
        """
        errors = []
        warnings = []

        required_tables = [
            "scoring_algorithms",
            "scoring_configurations",
            "scoring_runs",
            "scoring_results",
            "scoring_metrics",
        ]

        try:
            with self.engine.connect() as conn:
                # Check if tables exist
                for table in required_tables:
                    check_query = text(
                        f"""
                        SELECT COUNT(*) FROM information_schema.tables
                        WHERE table_schema = DATABASE() AND table_name = '{table}'
                    """
                    )
                    result = conn.execute(check_query).fetchone()
                    if result[0] == 0:
                        errors.append(f"Required table '{table}' does not exist")

                # Check if views exist
                view_query = text(
                    """
                    SELECT COUNT(*) FROM information_schema.views
                    WHERE table_schema = DATABASE() AND table_name = 'latest_scoring_results'
                """
                )
                result = conn.execute(view_query).fetchone()
                if result[0] == 0:
                    warnings.append("View 'latest_scoring_results' does not exist")

        except SQLAlchemyError as e:
            errors.append(f"Failed to validate schema: {e}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(required_tables) + 1,
            passed_items=len(required_tables) + 1-len(errors),
        )
