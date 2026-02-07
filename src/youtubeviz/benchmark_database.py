#!/usr/bin/env python3
"""
Benchmark Database Storage

Provides database storage for benchmark results alongside JSON files.
Enables SQL queries, trend analysis, and better long-term storage.
"""

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List


class BenchmarkDatabase:
    """Database storage for benchmark results."""

    def __init__(self, db_path: str = "benchmark_results / benchmarks.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self.init_database()

    def init_database(self):
        """Initialize database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Benchmark runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id TEXT UNIQUE NOT NULL,
                experiment_name TEXT NOT NULL,
                timestamp DATETIME NOT NULL,

                -- Dataset info
                total_samples INTEGER,
                train_samples INTEGER,
                test_samples INTEGER,

                -- Dataset quality metrics
                quality_level TEXT,
                balance_score REAL,
                positive_count INTEGER,
                negative_count INTEGER,
                neutral_count INTEGER,
                imbalance_ratio REAL,

                -- Configuration
                test_size REAL,
                random_state INTEGER,
                min_samples_per_class INTEGER,
                min_balance_score REAL,

                -- Metadata
                json_file_path TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Model results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS model_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                benchmark_run_id INTEGER,
                model_name TEXT NOT NULL,
                model_type TEXT NOT NULL,

                -- Performance metrics
                accuracy REAL NOT NULL,
                precision_score REAL NOT NULL,
                recall REAL NOT NULL,
                f1_score REAL NOT NULL,
                processing_time REAL,

                -- Metadata
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (benchmark_run_id) REFERENCES benchmark_runs (id)
            )
        """)

        # Quality recommendations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quality_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                benchmark_run_id INTEGER,
                recommendation TEXT NOT NULL,
                priority INTEGER DEFAULT 1,

                FOREIGN KEY (benchmark_run_id) REFERENCES benchmark_runs (id)
            )
        """)

        # Create indexes for better query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_experiment_name ON benchmark_runs(experiment_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON benchmark_runs(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_quality_level ON benchmark_runs(quality_level)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_model_name ON model_results(model_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_f1_score ON model_results(f1_score)")

        conn.commit()
        conn.close()

    def store_benchmark_run(self, benchmark_run, json_file_path: str = None) -> int:
        """
        Store a benchmark run in the database.

        Args:
            benchmark_run: BenchmarkRun object
            json_file_path: Path to the JSON file

        Returns:
            Database ID of the stored run
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Insert benchmark run
            cursor.execute(
                """
                INSERT INTO benchmark_runs (
                    experiment_id, experiment_name, timestamp,
                    total_samples, train_samples, test_samples,
                    quality_level, balance_score, positive_count, negative_count, neutral_count, imbalance_ratio,
                    test_size, random_state, min_samples_per_class, min_balance_score,
                    json_file_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    benchmark_run.experiment_id,
                    benchmark_run.config.experiment_name,
                    benchmark_run.timestamp,
                    benchmark_run.dataset_info.get("total_samples"),
                    benchmark_run.dataset_info.get("train_samples"),
                    benchmark_run.dataset_info.get("test_samples"),
                    benchmark_run.dataset_quality.quality_level,
                    benchmark_run.dataset_quality.balance_score,
                    benchmark_run.dataset_quality.positive_count,
                    benchmark_run.dataset_quality.negative_count,
                    benchmark_run.dataset_quality.neutral_count,
                    benchmark_run.dataset_quality.imbalance_ratio,
                    benchmark_run.config.test_size,
                    benchmark_run.config.random_state,
                    benchmark_run.config.min_samples_per_class,
                    benchmark_run.config.min_balance_score,
                    json_file_path,
                ),
            )

            run_id = cursor.lastrowid

            # Insert model results
            for model in benchmark_run.models:
                cursor.execute(
                    """
                    INSERT INTO model_results (
                        benchmark_run_id, model_name, model_type,
                        accuracy, precision_score, recall, f1_score, processing_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        run_id,
                        model.model_name,
                        model.model_type,
                        model.accuracy,
                        model.precision,
                        model.recall,
                        model.f1_score,
                        model.processing_time,
                    ),
                )

            # Insert quality recommendations
            for rec in benchmark_run.dataset_quality.recommendations:
                cursor.execute(
                    """
                    INSERT INTO quality_recommendations (benchmark_run_id, recommendation)
                    VALUES (?, ?)
                """,
                    (run_id, rec),
                )

            conn.commit()
            print(f"💾 Benchmark stored in database with ID: {run_id}")
            return run_id

        except Exception as e:
            conn.rollback()
            print(f"❌ Error storing benchmark in database: {e}")
            raise
        finally:
            conn.close()

    def get_benchmark_trends(self, experiment_name: str = None, days: int = 30) -> Dict[str, Any]:
        """Get benchmark trends over time."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        where_clause = "WHERE timestamp >= datetime('now', '-{} days')".format(days)
        if experiment_name:
            where_clause += f" AND experiment_name = '{experiment_name}'"

        # Get trend data
        cursor.execute(f"""
            SELECT
                DATE(timestamp) as date,
                AVG(balance_score) as avg_balance_score,
                COUNT(*) as run_count,
                AVG(total_samples) as avg_samples
            FROM benchmark_runs
            {where_clause}
            GROUP BY DATE(timestamp)
            ORDER BY date
        """)

        trend_data = cursor.fetchall()

        # Get model performance trends
        cursor.execute(f"""
            SELECT
                mr.model_name,
                AVG(mr.f1_score) as avg_f1,
                AVG(mr.accuracy) as avg_accuracy,
                COUNT(*) as run_count
            FROM model_results mr
            JOIN benchmark_runs br ON mr.benchmark_run_id = br.id
            {where_clause.replace('timestamp', 'br.timestamp')}
            GROUP BY mr.model_name
            ORDER BY avg_f1 DESC
        """)

        model_trends = cursor.fetchall()

        conn.close()

        return {"trend_data": trend_data, "model_trends": model_trends, "period_days": days}

    def get_quality_analysis(self) -> Dict[str, Any]:
        """Analyze dataset quality trends."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Quality level distribution
        cursor.execute("""
            SELECT quality_level, COUNT(*) as count
            FROM benchmark_runs
            GROUP BY quality_level
            ORDER BY count DESC
        """)
        quality_distribution = dict(cursor.fetchall())

        # Balance score trends
        cursor.execute("""
            SELECT
                DATE(timestamp) as date,
                AVG(balance_score) as avg_balance,
                MIN(balance_score) as min_balance,
                MAX(balance_score) as max_balance
            FROM benchmark_runs
            WHERE timestamp >= datetime('now', '-30 days')
            GROUP BY DATE(timestamp)
            ORDER BY date
        """)
        balance_trends = cursor.fetchall()

        # Most common recommendations
        cursor.execute("""
            SELECT recommendation, COUNT(*) as frequency
            FROM quality_recommendations
            GROUP BY recommendation
            ORDER BY frequency DESC
            LIMIT 10
        """)
        common_recommendations = cursor.fetchall()

        conn.close()

        return {
            "quality_distribution": quality_distribution,
            "balance_trends": balance_trends,
            "common_recommendations": common_recommendations,
        }

    def get_best_models(self, metric: str = "f1_score", limit: int = 10) -> List[Dict]:
        """Get best performing models."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT
                mr.model_name,
                mr.model_type,
                mr.{metric},
                mr.accuracy,
                mr.f1_score,
                br.experiment_name,
                br.timestamp,
                br.quality_level,
                br.balance_score
            FROM model_results mr
            JOIN benchmark_runs br ON mr.benchmark_run_id = br.id
            ORDER BY mr.{metric} DESC
            LIMIT ?
        """,
            (limit,),
        )

        results = cursor.fetchall()
        conn.close()

        columns = [
            "model_name",
            "model_type",
            metric,
            "accuracy",
            "f1_score",
            "experiment_name",
            "timestamp",
            "quality_level",
            "balance_score",
        ]

        return [dict(zip(columns, row)) for row in results]

    def export_analysis_report(self, output_file: str = "benchmark_analysis.json"):
        """Export comprehensive analysis report."""
        report = {
            "generated_at": datetime.now().isoformat(),
            "trends": self.get_benchmark_trends(),
            "quality_analysis": self.get_quality_analysis(),
            "best_models": self.get_best_models(),
            "database_stats": self.get_database_stats(),
        }

        with open(output_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        print(f"📊 Analysis report exported to: {output_file}")
        return report

    def get_database_stats(self) -> Dict[str, int]:
        """Get basic database statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM benchmark_runs")
        total_runs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM model_results")
        total_models = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT model_name) FROM model_results")
        unique_models = cursor.fetchone()[0]

        conn.close()

        return {"total_benchmark_runs": total_runs, "total_model_results": total_models, "unique_models": unique_models}


def demo_database_storage():
    """Demonstrate database storage capabilities."""
    print("💾 BENCHMARK DATABASE STORAGE DEMO")
    print("=" * 50)

    db = BenchmarkDatabase()

    # Show stats
    stats = db.get_database_stats()
    print(f"Database Stats:")
    print(f"  Total runs: {stats['total_benchmark_runs']}")
    print(f"  Total model results: {stats['total_model_results']}")
    print(f"  Unique models: {stats['unique_models']}")

    if stats["total_benchmark_runs"] > 0:
        # Show trends
        print(f"\n📈 Recent Trends:")
        trends = db.get_benchmark_trends(days=30)
        for model_name, avg_f1, avg_acc, count in trends["model_trends"][:5]:
            print(f"  {model_name}: F1={avg_f1:.3f}, Acc={avg_acc:.3f} ({count} runs)")

        # Show quality analysis
        print(f"\n📊 Quality Analysis:")
        quality = db.get_quality_analysis()
        print(f"  Quality distribution: {quality['quality_distribution']}")

        if quality["common_recommendations"]:
            print(f"  Most common issues:")
            for rec, freq in quality["common_recommendations"][:3]:
                print(f"    - {rec} ({freq} times)")


if __name__ == "__main__":
    demo_database_storage()
