"""Enhanced sentiment job with plugin system integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pymysql
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.youtubeviz.plugin_integration import get_plugin_manager


@dataclass
class EnhancedSentimentStats:
    """Enhanced sentiment processing statistics."""

    processed: int
    updated: int
    skipped: int
    plugin_scores_generated: int = 0
    plugin_algorithms_used: List[str] = None
    errors: List[str] = None

    def __post_init__(self):
        if self.plugin_algorithms_used is None:
            self.plugin_algorithms_used = []
        if self.errors is None:
            self.errors = []


class EnhancedYouTubeCommentSentimentJob:
    """Enhanced sentiment job with plugin system integration.

    Extends the original sentiment job to support:
    - Plugin - based sentiment analysis
    - Multiple sentiment algorithms
    - Enhanced scoring and storage
    - Integration with the scoring engine
    """

    def __init__(self, enable_plugins: bool = True):
        """Initialize enhanced sentiment job."""
        self._logger = logging.getLogger(__name__)

        # Database connection parameters
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

        # Traditional VADER analyzer
        self.analyzer = SentimentIntensityAnalyzer()

        # Plugin system integration
        self._enable_plugins = enable_plugins
        self._plugin_manager = None

        if self._enable_plugins:
            try:
                self._plugin_manager = get_plugin_manager(enable_storage=True)
                if not self._plugin_manager._initialized:
                    self._plugin_manager.initialize()
                self._logger.info("Plugin system initialized for sentiment analysis")
            except Exception as e:
                self._logger.warning(f"Failed to initialize plugin system: {e}")
                self._enable_plugins = False

    def _connect(self) -> pymysql.Connection:
        """Create database connection."""
        return pymysql.connect(**self.db_args)

    @staticmethod
    def _to_decimal_2(v: float) -> float:
        """Clamp to [-1, 1] then 2 decimal places; MySQL DECIMAL(3,2) compatible."""
        v = max(-1.0, min(1.0, v))
        return float(f"{v:.2f}")

    def score_batch_enhanced(
        self, limit: int = 500, use_plugins: bool = True, plugin_algorithms: Optional[List[str]] = None
    ) -> EnhancedSentimentStats:
        """Enhanced batch scoring with plugin support."""
        stats = EnhancedSentimentStats(processed=0, updated=0, skipped=0)

        try:
            # Get comments to process
            comments_data = self._get_comments_for_processing(limit)

            if comments_data.empty:
                return stats

            stats.processed = len(comments_data)

            # Process with VADER (traditional method)
            vader_updates = self._process_with_vader(comments_data)
            stats.updated = len(vader_updates)

            # Apply VADER updates to database
            if vader_updates:
                self._apply_vader_updates(vader_updates)

            # Process with plugins if enabled
            if use_plugins and self._enable_plugins and self._plugin_manager:
                plugin_stats = self._process_with_plugins(comments_data, plugin_algorithms)
                stats.plugin_scores_generated = plugin_stats.get("scores_generated", 0)
                stats.plugin_algorithms_used = plugin_stats.get("algorithms_used", [])
                stats.errors.extend(plugin_stats.get("errors", []))

            return stats

        except Exception as e:
            self._logger.error(f"Enhanced batch scoring failed: {e}")
            stats.errors.append(f"Batch scoring failed: {e}")
            return stats

    def _get_comments_for_processing(self, limit: int) -> pd.DataFrame:
        """Get comments that need sentiment processing."""
        query = """
        SELECT
            id,
            video_id,
            comment_text,
            author_name,
            published_at
        FROM youtube_comments
        WHERE sentiment_score IS NULL
        AND comment_text IS NOT NULL
        AND comment_text <> ''
        ORDER BY id ASC
        LIMIT %s
        """

        try:
            with self._connect() as conn:
                return pd.read_sql(query, conn, params=(limit,))
        except Exception as e:
            self._logger.error(f"Failed to get comments for processing: {e}")
            return pd.DataFrame()

    def _process_with_vader(self, comments_data: pd.DataFrame) -> List[Tuple[float, int]]:
        """Process comments with VADER sentiment analyzer."""
        updates = []

        for _, row in comments_data.iterrows():
            comment_id = row["id"]
            text = (row.get("comment_text") or "").strip()

            if not text:
                continue

            try:
                scores = self.analyzer.polarity_scores(text)
                compound = self._to_decimal_2(scores.get("compound", 0.0))
                updates.append((compound, comment_id))
            except Exception as e:
                self._logger.warning(f"VADER scoring failed for comment {comment_id}: {e}")

        return updates

    def _apply_vader_updates(self, updates: List[Tuple[float, int]]) -> None:
        """Apply VADER sentiment updates to database."""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany("UPDATE youtube_comments SET sentiment_score=%s WHERE id=%s", updates)
                conn.commit()
                self._logger.info(f"Applied {len(updates)} VADER sentiment updates")
        except Exception as e:
            self._logger.error(f"Failed to apply VADER updates: {e}")

    def _process_with_plugins(
        self, comments_data: pd.DataFrame, plugin_algorithms: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Process comments with sentiment plugins."""
        result = {"scores_generated": 0, "algorithms_used": [], "errors": []}

        try:
            # Get available sentiment algorithms
            available_algorithms = self._plugin_manager.get_available_algorithms()

            # Filter for sentiment - related algorithms
            sentiment_algorithms = [
                alg for alg in available_algorithms if "sentiment" in alg.lower() or alg in (plugin_algorithms or [])
            ]

            if not sentiment_algorithms and plugin_algorithms:
                # Use specified algorithms even if they don't have 'sentiment' in name
                sentiment_algorithms = [alg for alg in plugin_algorithms if alg in available_algorithms]

            if not sentiment_algorithms:
                self._logger.info("No sentiment algorithms available in plugin system")
                return result

            # Prepare data for plugin processing
            plugin_data = self._prepare_plugin_data(comments_data)

            # Process with each sentiment algorithm
            for algorithm in sentiment_algorithms:
                try:
                    scores_df = self._plugin_manager.execute_scoring(
                        algorithm_name=algorithm, data=plugin_data, entity_type="comment"
                    )

                    if not scores_df.empty:
                        # Store plugin results
                        stored_count = self._store_plugin_sentiment_results(scores_df, algorithm, comments_data)

                        result["scores_generated"] += stored_count
                        result["algorithms_used"].append(algorithm)

                        self._logger.info(f"Generated {stored_count} sentiment scores with {algorithm}")

                except Exception as e:
                    error_msg = f"Plugin {algorithm} failed: {e}"
                    result["errors"].append(error_msg)
                    self._logger.warning(error_msg)

            return result

        except Exception as e:
            error_msg = f"Plugin processing failed: {e}"
            result["errors"].append(error_msg)
            self._logger.error(error_msg)
            return result

    def _prepare_plugin_data(self, comments_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare comment data for plugin processing."""
        # Create a standardized format for plugins
        plugin_data = comments_data.copy()

        # Ensure required columns exist
        plugin_data["entity_id"] = plugin_data["id"].astype(str)

        # Add derived features that plugins might use
        plugin_data["text_length"] = plugin_data["comment_text"].str.len()
        plugin_data["word_count"] = plugin_data["comment_text"].str.split().str.len()

        # Add time - based features
        if "published_at" in plugin_data.columns:
            plugin_data["days_since_published"] = (
                pd.Timestamp.now() - pd.to_datetime(plugin_data["published_at"])
            ).dt.days

        return plugin_data

    def _store_plugin_sentiment_results(
        self, scores_df: pd.DataFrame, algorithm_name: str, original_data: pd.DataFrame
    ) -> int:
        """Store plugin sentiment results in database."""
        try:
            # Map plugin results back to comment IDs
            score_mapping = {}
            for _, row in scores_df.iterrows():
                entity_id = str(row["entity_id"])
                score_value = row.get("score_value", 0.0)
                confidence = row.get("confidence", 0.0)

                # Find corresponding comment ID
                matching_rows = original_data[original_data["id"].astype(str) == entity_id]
                if not matching_rows.empty:
                    comment_id = matching_rows.iloc[0]["id"]
                    score_mapping[comment_id] = {
                        "score": self._to_decimal_2(score_value),
                        "confidence": confidence,
                        "algorithm": algorithm_name,
                    }

            if not score_mapping:
                return 0

            # Store results in a plugin sentiment table (create if needed)
            self._ensure_plugin_sentiment_table()

            # Insert plugin sentiment results
            inserts = []
            for comment_id, result in score_mapping.items():
                inserts.append((comment_id, result["algorithm"], result["score"], result["confidence"], datetime.now()))

            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO youtube_comment_plugin_sentiment
                        (comment_id, algorithm_name, sentiment_score, confidence_score, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        sentiment_score=VALUES(sentiment_score),
                        confidence_score=VALUES(confidence_score),
                        updated_at=NOW()
                        """,
                        inserts,
                    )
                conn.commit()

            return len(inserts)

        except Exception as e:
            self._logger.error(f"Failed to store plugin sentiment results: {e}")
            return 0

    def _ensure_plugin_sentiment_table(self) -> None:
        """Ensure plugin sentiment table exists."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS youtube_comment_plugin_sentiment (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comment_id INT NOT NULL,
            algorithm_name VARCHAR(100) NOT NULL,
            sentiment_score DECIMAL(3,2) NOT NULL,
            confidence_score DECIMAL(3,2) DEFAULT 0.00,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_comment_algorithm (comment_id, algorithm_name),
            FOREIGN KEY (comment_id) REFERENCES youtube_comments(id) ON DELETE CASCADE,
            INDEX idx_algorithm (algorithm_name),
            INDEX idx_sentiment_score (sentiment_score)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_sql)
                conn.commit()
        except Exception as e:
            self._logger.warning(f"Failed to create plugin sentiment table: {e}")

    def refresh_summary_enhanced(self, include_plugin_scores: bool = True) -> Dict[str, int]:
        """Enhanced summary refresh including plugin scores."""
        result = {"vader_summaries": 0, "plugin_summaries": 0}

        try:
            # Refresh traditional VADER summaries
            result["vader_summaries"] = self._refresh_vader_summary()

            # Refresh plugin - based summaries if enabled
            if include_plugin_scores and self._enable_plugins:
                result["plugin_summaries"] = self._refresh_plugin_summaries()

            return result

        except Exception as e:
            self._logger.error(f"Enhanced summary refresh failed: {e}")
            return result

    def _refresh_vader_summary(self) -> int:
        """Refresh traditional VADER sentiment summaries."""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    # Get VADER sentiment averages per video
                    cur.execute(
                        """
                        SELECT video_id, AVG(sentiment_score) AS avg_sentiment, COUNT(*) AS cnt
                        FROM youtube_comments
                        WHERE sentiment_score IS NOT NULL
                        GROUP BY video_id
                    """
                    )
                    rows = cur.fetchall()

                if not rows:
                    return 0

                # Upsert summaries
                upserts = []
                for row in rows:
                    vid = row["video_id"]
                    avg_sent = float(row["avg_sentiment"] or 0.0)
                    cnt = int(row["cnt"] or 0)
                    upserts.append((vid, avg_sent, cnt))

                sql = """
                INSERT INTO youtube_sentiment_summary
                (video_id, avg_sentiment, comment_count, last_updated)
                VALUES (%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                avg_sentiment=VALUES(avg_sentiment),
                comment_count=VALUES(comment_count),
                last_updated=NOW()
                """

                with conn.cursor() as cur:
                    cur.executemany(sql, upserts)
                conn.commit()

                return len(upserts)

        except Exception as e:
            self._logger.error(f"VADER summary refresh failed: {e}")
            return 0

    def _refresh_plugin_summaries(self) -> int:
        """Refresh plugin - based sentiment summaries."""
        try:
            # Create plugin summary table if needed
            self._ensure_plugin_summary_table()

            with self._connect() as conn:
                with conn.cursor() as cur:
                    # Get plugin sentiment averages per video and algorithm
                    cur.execute(
                        """
                        SELECT
                            c.video_id,
                            ps.algorithm_name,
                            AVG(ps.sentiment_score) AS avg_sentiment,
                            AVG(ps.confidence_score) AS avg_confidence,
                            COUNT(*) AS comment_count
                        FROM youtube_comment_plugin_sentiment ps
                        JOIN youtube_comments c ON c.id = ps.comment_id
                        GROUP BY c.video_id, ps.algorithm_name
                    """
                    )
                    rows = cur.fetchall()

                if not rows:
                    return 0

                # Upsert plugin summaries
                upserts = []
                for row in rows:
                    upserts.append(
                        (
                            row["video_id"],
                            row["algorithm_name"],
                            float(row["avg_sentiment"] or 0.0),
                            float(row["avg_confidence"] or 0.0),
                            int(row["comment_count"] or 0),
                        )
                    )

                sql = """
                INSERT INTO youtube_plugin_sentiment_summary
                (video_id, algorithm_name, avg_sentiment, avg_confidence, comment_count, last_updated)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                avg_sentiment=VALUES(avg_sentiment),
                avg_confidence=VALUES(avg_confidence),
                comment_count=VALUES(comment_count),
                last_updated=NOW()
                """

                with conn.cursor() as cur:
                    cur.executemany(sql, upserts)
                conn.commit()

                return len(upserts)

        except Exception as e:
            self._logger.error(f"Plugin summary refresh failed: {e}")
            return 0

    def _ensure_plugin_summary_table(self) -> None:
        """Ensure plugin sentiment summary table exists."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS youtube_plugin_sentiment_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id VARCHAR(20) NOT NULL,
            algorithm_name VARCHAR(100) NOT NULL,
            avg_sentiment DECIMAL(3,2) NOT NULL,
            avg_confidence DECIMAL(3,2) DEFAULT 0.00,
            comment_count INT DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY unique_video_algorithm (video_id, algorithm_name),
            INDEX idx_video_id (video_id),
            INDEX idx_algorithm (algorithm_name),
            INDEX idx_avg_sentiment (avg_sentiment)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_sql)
                conn.commit()
        except Exception as e:
            self._logger.warning(f"Failed to create plugin summary table: {e}")

    def get_plugin_system_status(self) -> Dict[str, Any]:
        """Get status of plugin system integration."""
        status = {
            "plugins_enabled": self._enable_plugins,
            "plugin_manager_initialized": self._plugin_manager is not None,
            "available_algorithms": [],
            "sentiment_algorithms": [],
        }

        if self._plugin_manager:
            try:
                algorithms = self._plugin_manager.get_available_algorithms()
                status["available_algorithms"] = algorithms
                status["sentiment_algorithms"] = [alg for alg in algorithms if "sentiment" in alg.lower()]
            except Exception as e:
                status["error"] = str(e)

        return status


# Backward compatibility functions
def create_enhanced_sentiment_job(enable_plugins: bool = True) -> EnhancedYouTubeCommentSentimentJob:
    """Create enhanced sentiment job instance."""
    return EnhancedYouTubeCommentSentimentJob(enable_plugins=enable_plugins)


def run_enhanced_sentiment_batch(
    limit: int = 500, use_plugins: bool = True, plugin_algorithms: Optional[List[str]] = None
) -> EnhancedSentimentStats:
    """Run enhanced sentiment batch processing."""
    job = create_enhanced_sentiment_job(enable_plugins=use_plugins)
    return job.score_batch_enhanced(limit=limit, use_plugins=use_plugins, plugin_algorithms=plugin_algorithms)
