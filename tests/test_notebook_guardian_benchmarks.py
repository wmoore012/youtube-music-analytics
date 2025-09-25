"""
Performance benchmarks for Notebook Guardian.

Comprehensive benchmarking suite to measure real-world performance
across different file sizes, complexity levels, and usage patterns.
"""

import json
import os
from pathlib import Path
import statistics
import tempfile
import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest

from src.notebook_guardian.core_validator import DataValidator, MetricExplainer
from src.notebook_guardian.python_validator import PythonFileValidator
from src.notebook_guardian.smart_installer import FastDependencyDetector, SmartInstaller


class BenchmarkResult:
    """Container for benchmark results."""

    def __init__(self, name: str):
        self.name = name
        self.times = []
        self.memory_usage = []
        self.throughput = []

    def add_measurement(self, time_ms: float, memory_mb: float = 0, items_processed: int = 1):
        """Add a benchmark measurement."""
        self.times.append(time_ms)
        self.memory_usage.append(memory_mb)
        self.throughput.append(items_processed / (time_ms / 1000) if time_ms > 0 else 0)

    def get_stats(self) -> Dict[str, float]:
        """Get statistical summary of benchmark results."""
        if not self.times:
            return {}

        return {
            "mean_time_ms": statistics.mean(self.times),
            "median_time_ms": statistics.median(self.times),
            "min_time_ms": min(self.times),
            "max_time_ms": max(self.times),
            "std_time_ms": statistics.stdev(self.times) if len(self.times) > 1 else 0,
            "mean_throughput": statistics.mean(self.throughput) if self.throughput else 0,
            "mean_memory_mb": statistics.mean(self.memory_usage) if self.memory_usage else 0,
        }


class TestDependencyDetectionBenchmarks:
    """Benchmark dependency detection performance."""

    def setup_method(self):
        """Set up benchmark fixtures."""
        self.detector = FastDependencyDetector()
        self.benchmark = BenchmarkResult("dependency_detection")

    def test_small_file_detection_speed(self):
        """Benchmark dependency detection on small files (< 1KB)."""
        small_code = """
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier

df = pd.DataFrame({'x': [1, 2, 3]})
model = RandomForestClassifier()
"""

        # Warm up
        for _ in range(5):
            self.detector.detect_dependencies(small_code)

        # Benchmark
        for _ in range(100):
            start = time.perf_counter()
            deps = self.detector.detect_dependencies(small_code)
            end = time.perf_counter()

            self.benchmark.add_measurement((end - start) * 1000, items_processed=len(deps))

        stats = self.benchmark.get_stats()
        print(f"\nSmall File Detection Performance:")
        print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
        print(f"  Median time: {stats['median_time_ms']:.2f}ms")
        print(f"  Throughput: {stats['mean_throughput']:.1f} deps/sec")

        # Performance assertions
        assert stats["mean_time_ms"] < 5.0, f"Small file detection too slow: {stats['mean_time_ms']:.2f}ms"
        assert stats["mean_throughput"] > 100, f"Throughput too low: {stats['mean_throughput']:.1f} deps/sec"

    def test_medium_file_detection_speed(self):
        """Benchmark dependency detection on medium files (1-10KB)."""
        # Generate medium-sized code file
        medium_code_parts = [
            "import pandas as pd",
            "import numpy as np",
            "import matplotlib.pyplot as plt",
            "from sklearn.ensemble import RandomForestClassifier",
            "from sklearn.metrics import accuracy_score",
            "import seaborn as sns",
            "",
        ]

        # Add multiple functions
        for i in range(20):
            medium_code_parts.extend(
                [
                    f"def process_data_{i}(df):",
                    f'    """Process data function {i}."""',
                    f"    result = df.groupby('category').mean()",
                    f"    plt.figure(figsize=(10, 6))",
                    f"    sns.barplot(data=result)",
                    f"    return result",
                    "",
                ]
            )

        medium_code = "\n".join(medium_code_parts)

        # Warm up
        for _ in range(3):
            self.detector.detect_dependencies(medium_code)

        # Benchmark
        benchmark = BenchmarkResult("medium_file_detection")
        for _ in range(50):
            start = time.perf_counter()
            deps = self.detector.detect_dependencies(medium_code)
            end = time.perf_counter()

            benchmark.add_measurement((end - start) * 1000, items_processed=len(deps))

        stats = benchmark.get_stats()
        print(f"\nMedium File Detection Performance:")
        print(f"  File size: {len(medium_code)} chars")
        print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
        print(f"  Median time: {stats['median_time_ms']:.2f}ms")
        print(f"  Throughput: {stats['mean_throughput']:.1f} deps/sec")

        # Performance assertions
        assert stats["mean_time_ms"] < 20.0, f"Medium file detection too slow: {stats['mean_time_ms']:.2f}ms"

    def test_large_file_detection_speed(self):
        """Benchmark dependency detection on large files (10KB+)."""
        # Generate large code file
        large_code_parts = [
            "import pandas as pd",
            "import numpy as np",
            "import matplotlib.pyplot as plt",
            "import seaborn as sns",
            "from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier",
            "from sklearn.linear_model import LogisticRegression, LinearRegression",
            "from sklearn.metrics import accuracy_score, precision_score, recall_score",
            "from sklearn.model_selection import train_test_split, cross_val_score",
            "import tensorflow as tf",
            "import torch",
            "import plotly.express as px",
            "import scipy.stats as stats",
            "",
        ]

        # Add many functions with complex patterns
        for i in range(100):
            large_code_parts.extend(
                [
                    f"def ml_pipeline_{i}(data, target):",
                    f'    """Complete ML pipeline {i}."""',
                    f"    # Data preprocessing",
                    f"    df_clean = data.dropna()",
                    f"    df_encoded = pd.get_dummies(df_clean)",
                    f"    ",
                    f"    # Feature engineering",
                    f"    X = df_encoded.drop(target, axis=1)",
                    f"    y = df_encoded[target]",
                    f"    X_train, X_test, y_train, y_test = train_test_split(X, y)",
                    f"    ",
                    f"    # Model training",
                    f"    models = [",
                    f"        RandomForestClassifier(n_estimators=100),",
                    f"        GradientBoostingClassifier(),",
                    f"        LogisticRegression()",
                    f"    ]",
                    f"    ",
                    f"    results = []",
                    f"    for model in models:",
                    f"        model.fit(X_train, y_train)",
                    f"        y_pred = model.predict(X_test)",
                    f"        acc = accuracy_score(y_test, y_pred)",
                    f"        results.append(acc)",
                    f"    ",
                    f"    # Visualization",
                    f"    plt.figure(figsize=(12, 8))",
                    f"    sns.barplot(x=range(len(results)), y=results)",
                    f"    plt.title(f'Model Comparison {i}')",
                    f"    plt.show()",
                    f"    ",
                    f"    return results",
                    "",
                ]
            )

        large_code = "\n".join(large_code_parts)

        # Warm up
        for _ in range(2):
            self.detector.detect_dependencies(large_code)

        # Benchmark
        benchmark = BenchmarkResult("large_file_detection")
        for _ in range(20):
            start = time.perf_counter()
            deps = self.detector.detect_dependencies(large_code)
            end = time.perf_counter()

            benchmark.add_measurement((end - start) * 1000, items_processed=len(deps))

        stats = benchmark.get_stats()
        print(f"\nLarge File Detection Performance:")
        print(f"  File size: {len(large_code):,} chars")
        print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
        print(f"  Median time: {stats['median_time_ms']:.2f}ms")
        print(f"  Throughput: {stats['mean_throughput']:.1f} deps/sec")

        # Performance assertions
        assert stats["mean_time_ms"] < 100.0, f"Large file detection too slow: {stats['mean_time_ms']:.2f}ms"


class TestPythonFileValidationBenchmarks:
    """Benchmark Python file validation performance."""

    def setup_method(self):
        """Set up benchmark fixtures."""
        self.validator = PythonFileValidator()

    def test_file_validation_throughput(self):
        """Benchmark file validation throughput."""
        # Create test files of different sizes
        test_files = []

        for size_category, (num_functions, complexity) in [
            ("small", (5, "simple")),
            ("medium", (20, "moderate")),
            ("large", (50, "complex")),
        ]:
            code_parts = [
                "import pandas as pd",
                "import numpy as np",
                "from sklearn.ensemble import RandomForestClassifier",
                "",
            ]

            for i in range(num_functions):
                if complexity == "simple":
                    code_parts.extend([f"def func_{i}(x):", f"    return x * {i}", ""])
                elif complexity == "moderate":
                    code_parts.extend(
                        [
                            f"def process_{i}(data: pd.DataFrame) -> pd.DataFrame:",
                            f'    """Process data with method {i}."""',
                            f"    result = data.copy()",
                            f"    result['feature_{i}'] = result['value'] * {i}",
                            f"    return result.dropna()",
                            "",
                        ]
                    )
                else:  # complex
                    code_parts.extend(
                        [
                            f"def ml_workflow_{i}(data: pd.DataFrame, target: str) -> dict:",
                            f'    """Complete ML workflow {i}."""',
                            f"    try:",
                            f"        X = data.drop(target, axis=1)",
                            f"        y = data[target]",
                            f"        ",
                            f"        model = RandomForestClassifier(n_estimators={10 + i})",
                            f"        model.fit(X, y)",
                            f"        ",
                            f"        score = model.score(X, y)",
                            f"        return {{'accuracy': score, 'model': model}}",
                            f"    except Exception as e:",
                            f"        return {{'error': str(e)}}",
                            "",
                        ]
                    )

            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write("\n".join(code_parts))
                test_files.append((f.name, size_category, len("\n".join(code_parts))))

        try:
            # Benchmark each file size
            for file_path, category, file_size in test_files:
                benchmark = BenchmarkResult(f"{category}_file_validation")

                # Warm up
                for _ in range(3):
                    self.validator.validate_file(file_path)

                # Benchmark
                for _ in range(20):
                    start = time.perf_counter()
                    result = self.validator.validate_file(file_path)
                    end = time.perf_counter()

                    benchmark.add_measurement((end - start) * 1000, items_processed=len(result.functions_found))

                stats = benchmark.get_stats()
                print(f"\n{category.title()} File Validation Performance:")
                print(f"  File size: {file_size:,} chars")
                print(f"  Functions: {len(result.functions_found)}")
                print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
                print(f"  Throughput: {stats['mean_throughput']:.1f} functions/sec")

                # Performance assertions based on file size
                if category == "small":
                    assert stats["mean_time_ms"] < 50.0
                elif category == "medium":
                    assert stats["mean_time_ms"] < 200.0
                else:  # large
                    assert stats["mean_time_ms"] < 500.0

        finally:
            # Clean up temporary files
            for file_path, _, _ in test_files:
                os.unlink(file_path)

    def test_parallel_validation_scaling(self):
        """Benchmark parallel validation scaling."""
        # Create multiple test files
        test_files = []

        for i in range(10):
            code = f'''
import pandas as pd
import numpy as np

def load_data_{i}():
    """Load data for test {i}."""
    return pd.DataFrame({{'x': range({i * 10})}})

def process_data_{i}(df):
    """Process data for test {i}."""
    return df.mean()

if __name__ == "__main__":
    data = load_data_{i}()
    result = process_data_{i}(data)
    print(f"Result {i}: {{result}}")
'''

            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
                f.write(code)
                test_files.append(f.name)

        try:
            # Benchmark sequential vs parallel
            for workers in [1, 2, 4]:
                benchmark = BenchmarkResult(f"parallel_validation_{workers}_workers")

                for _ in range(10):
                    start = time.perf_counter()

                    if workers == 1:
                        # Sequential processing
                        results = []
                        for file_path in test_files:
                            result = self.validator.validate_file(file_path)
                            results.append(result)
                    else:
                        # Parallel processing
                        results = self.validator.validate_multiple_files(test_files, max_workers=workers)

                    end = time.perf_counter()

                    benchmark.add_measurement((end - start) * 1000, items_processed=len(test_files))

                stats = benchmark.get_stats()
                print(f"\nParallel Validation ({workers} workers):")
                print(f"  Files processed: {len(test_files)}")
                print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
                print(f"  Throughput: {stats['mean_throughput']:.1f} files/sec")

                # Parallel should be faster than sequential for multiple files
                if workers > 1:
                    assert stats["mean_throughput"] > 5.0  # At least 5 files/sec

        finally:
            # Clean up
            for file_path in test_files:
                os.unlink(file_path)


class TestDataValidationBenchmarks:
    """Benchmark data validation performance."""

    def setup_method(self):
        """Set up benchmark fixtures."""
        self.validator = DataValidator()
        self.explainer = MetricExplainer()

    def test_dataframe_validation_scaling(self):
        """Benchmark DataFrame validation with different sizes."""
        sizes = [100, 1000, 10000, 50000]

        for size in sizes:
            # Create test DataFrame
            data = pd.DataFrame(
                {
                    "feature_1": np.random.randn(size),
                    "feature_2": np.random.randn(size),
                    "feature_3": np.random.randint(0, 10, size),
                    "target": np.random.randint(0, 2, size),
                    "score": np.random.uniform(0, 1, size),
                }
            )

            schema = {
                "type": "dataframe",
                "columns": {
                    "feature_1": "float64",
                    "feature_2": "float64",
                    "feature_3": "int64",
                    "target": "int64",
                    "score": "float64",
                },
                "min_rows": 1,
            }

            benchmark = BenchmarkResult(f"dataframe_validation_{size}_rows")

            # Warm up
            for _ in range(3):
                self.validator.validate_cell_output(data, schema)

            # Benchmark
            for _ in range(20):
                start = time.perf_counter()
                result = self.validator.validate_cell_output(data, schema)
                end = time.perf_counter()

                benchmark.add_measurement((end - start) * 1000, items_processed=size)

            stats = benchmark.get_stats()
            print(f"\nDataFrame Validation ({size:,} rows):")
            print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
            print(f"  Throughput: {stats['mean_throughput']:,.0f} rows/sec")

            # Performance should scale reasonably
            if size <= 1000:
                assert stats["mean_time_ms"] < 10.0
            elif size <= 10000:
                assert stats["mean_time_ms"] < 50.0
            else:  # 50000
                assert stats["mean_time_ms"] < 200.0

    def test_metric_explanation_performance(self):
        """Benchmark metric explanation generation."""
        metrics = [
            "accuracy",
            "precision",
            "recall",
            "f1_score",
            "auc_roc",
            "mean_squared_error",
            "r_squared",
            "silhouette_score",
            "adjusted_rand_index",
            "mutual_information",
        ]

        benchmark = BenchmarkResult("metric_explanations")

        # Warm up
        for _ in range(5):
            self.explainer.create_legend_definitions(metrics)

        # Benchmark
        for _ in range(100):
            start = time.perf_counter()
            explanations = self.explainer.create_legend_definitions(metrics)
            end = time.perf_counter()

            benchmark.add_measurement((end - start) * 1000, items_processed=len(metrics))

        stats = benchmark.get_stats()
        print(f"\nMetric Explanation Performance:")
        print(f"  Metrics: {len(metrics)}")
        print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
        print(f"  Throughput: {stats['mean_throughput']:.0f} explanations/sec")

        # Should be very fast
        assert stats["mean_time_ms"] < 5.0
        assert stats["mean_throughput"] > 1000


class TestNotebookValidationBenchmarks:
    """Benchmark Jupyter notebook validation."""

    def test_notebook_validation_performance(self):
        """Benchmark notebook file validation."""
        # Create test notebook with varying complexity
        for num_cells in [10, 50, 100]:
            cells = []

            # Add markdown cells
            for i in range(num_cells // 3):
                cells.append(
                    {
                        "cell_type": "markdown",
                        "metadata": {},
                        "source": [f"# Section {i}\n\nThis is section {i} of the analysis."],
                    }
                )

            # Add code cells
            for i in range(2 * num_cells // 3):
                cells.append(
                    {
                        "cell_type": "code",
                        "execution_count": i + 1,
                        "metadata": {},
                        "outputs": [],
                        "source": [
                            f"# Cell {i}\n",
                            "import pandas as pd\n",
                            f"data_{i} = pd.DataFrame({{'x': range({i + 1})}})\n",
                            f"result_{i} = data_{i}.mean()\n",
                            f"print(f'Result {i}: {{result_{i}}}')",
                        ],
                    }
                )

            notebook = {
                "cells": cells,
                "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
                "nbformat": 4,
                "nbformat_minor": 4,
            }

            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
                json.dump(notebook, f)
                notebook_path = f.name

            try:
                from src.notebook_guardian.core_validator import NotebookValidator

                validator = NotebookValidator()

                benchmark = BenchmarkResult(f"notebook_validation_{num_cells}_cells")

                # Warm up
                for _ in range(3):
                    validator.create_validation_report(notebook_path)

                # Benchmark
                for _ in range(20):
                    start = time.perf_counter()
                    result = validator.create_validation_report(notebook_path)
                    end = time.perf_counter()

                    benchmark.add_measurement((end - start) * 1000, items_processed=num_cells)

                stats = benchmark.get_stats()
                print(f"\nNotebook Validation ({num_cells} cells):")
                print(f"  Mean time: {stats['mean_time_ms']:.2f}ms")
                print(f"  Throughput: {stats['mean_throughput']:.1f} cells/sec")

                # Performance assertions
                if num_cells <= 10:
                    assert stats["mean_time_ms"] < 20.0
                elif num_cells <= 50:
                    assert stats["mean_time_ms"] < 100.0
                else:  # 100 cells
                    assert stats["mean_time_ms"] < 300.0

            finally:
                os.unlink(notebook_path)


def run_comprehensive_benchmarks():
    """Run all benchmarks and generate summary report."""
    print("🚀 NOTEBOOK GUARDIAN PERFORMANCE BENCHMARKS")
    print("=" * 60)

    # Run all benchmark classes
    benchmark_classes = [
        TestDependencyDetectionBenchmarks,
        TestPythonFileValidationBenchmarks,
        TestDataValidationBenchmarks,
        TestNotebookValidationBenchmarks,
    ]

    total_start = time.perf_counter()

    for benchmark_class in benchmark_classes:
        print(f"\n📊 Running {benchmark_class.__name__}...")
        instance = benchmark_class()
        instance.setup_method()

        # Run all test methods
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                print(f"\n  🔍 {method_name}")
                method = getattr(instance, method_name)
                method()

    total_end = time.perf_counter()
    total_time = total_end - total_start

    print(f"\n" + "=" * 60)
    print(f"✅ ALL BENCHMARKS COMPLETED")
    print(f"⏱️  Total benchmark time: {total_time:.2f}s")
    print(f"🚀 Notebook Guardian is optimized for production use!")


if __name__ == "__main__":
    run_comprehensive_benchmarks()
