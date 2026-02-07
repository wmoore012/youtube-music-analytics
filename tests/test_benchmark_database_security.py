from __future__ import annotations

from pathlib import Path
import sqlite3

from src.youtubeviz.benchmark_database import BenchmarkDatabase


def _seed_benchmark_rows(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO benchmark_runs (
            experiment_id, experiment_name, timestamp, total_samples,
            train_samples, test_samples, quality_level, balance_score,
            positive_count, negative_count, neutral_count, imbalance_ratio,
            test_size, random_state, min_samples_per_class, min_balance_score, json_file_path
        )
        VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "exp-001",
            "baseline",
            100,
            80,
            20,
            "GOOD",
            0.9,
            40,
            30,
            30,
            1.0,
            0.2,
            42,
            5,
            0.7,
            "report.json",
        ),
    )
    run_id = cur.lastrowid
    cur.execute(
        """
        INSERT INTO model_results (
            benchmark_run_id, model_name, model_type, accuracy, precision_score, recall, f1_score, processing_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, "model-a", "baseline", 0.8, 0.79, 0.78, 0.81, 1.2),
    )
    conn.commit()
    conn.close()


def test_get_benchmark_trends_uses_parameterized_filters(tmp_path: Path) -> None:
    db_path = tmp_path / "benchmarks.db"
    db = BenchmarkDatabase(str(db_path))
    _seed_benchmark_rows(db_path)

    malicious_name = "baseline' OR 1=1 --"
    trends = db.get_benchmark_trends(experiment_name=malicious_name, days=30)
    assert isinstance(trends, dict)
    assert trends["period_days"] == 30
    assert trends["trend_data"] == []

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM benchmark_runs")
    assert cur.fetchone()[0] == 1
    conn.close()


def test_get_best_models_rejects_metric_injection(tmp_path: Path) -> None:
    db_path = tmp_path / "benchmarks.db"
    db = BenchmarkDatabase(str(db_path))
    _seed_benchmark_rows(db_path)

    models = db.get_best_models(metric="f1_score; DROP TABLE model_results;--", limit=5)
    assert len(models) == 1
    assert "f1_score" in models[0]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM model_results")
    assert cur.fetchone()[0] == 1
    conn.close()
