"""Tests for the benchmark progress utility script.

These tests load the script as a module so we can validate the helper
functions that power the CLI without executing the CLI itself.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_progress.py"


def load_benchmark_module():
    """Dynamically import the benchmark module for testing."""

    spec = importlib.util.spec_from_file_location("benchmark_progress", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)  # type: ignore[assignment]
    return module


@pytest.fixture()
def benchmark_module():
    """Fixture that loads a fresh copy of the benchmark module."""

    return load_benchmark_module()


def test_summarize_series_returns_expected_keys(benchmark_module):
    """The summary helper should compute standard descriptive statistics."""

    series = pd.Series([1, 2, 3, 4], dtype="float64")
    result = benchmark_module.summarize_series(series)

    assert result is not None
    assert result["n"] == 4
    assert pytest.approx(result["mean"], rel=1e-6) == 2.5
    assert result["median"] == 2.5
    assert set(result) >= {
        "n",
        "mean",
        "median",
        "mode",
        "std",
        "se",
        "ci95_low",
        "ci95_high",
        "q25",
        "q75",
        "iqr",
        "mad",
    }


def test_flag_anomalies_high_z_scores_raise_flag(benchmark_module):
    """Large swings relative to history should emit a 🚩 warning."""

    stats = {
        "mean": 10.0,
        "median": 10.0,
        "std": 1.0,
        "se": 0.5,
        "ci95_low": 9.0,
        "ci95_high": 11.0,
        "q25": 9.5,
        "q75": 10.5,
        "iqr": 1.0,
        "mad": 0.6745,
        "_prev": 10.0,
    }
    spec = {"higher_is_better": True}

    flags = benchmark_module.flag_anomalies("coverage", latest=14.0, stats=stats, spec=spec)

    assert any(flag.startswith("🚩 coverage") for flag in flags)


def test_run_subprocess_returns_exit_code_and_streams(benchmark_module):
    """The helper should surface return code, stdout, and stderr."""

    rc, out, err = benchmark_module.run_subprocess([sys.executable, "-c", "print('hello')"])

    assert rc == 0
    assert out == "hello"
    assert err == ""


def test_analyze_history_prints_table_header(benchmark_module, capsys):
    """History analysis should render a compact table for known metrics."""

    data = {
        "date": "2024-01-01T00:00:00",
        "test_coverage": 75.0,
        "duplicate_functions": 3,
    }

    benchmark_module.analyze_history_and_print(data, history_path=Path("/tmp / nonexistent_history.json"))
    captured = capsys.readouterr()

    assert "Data Nerd Pack" in captured.out
    assert "Mean±SE (95% CI)" in captured.out
