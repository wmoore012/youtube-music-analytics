#!/usr/bin/env python3
"""Tests for specialized benchmarking tools and their organization.

This suite verifies that tools exist, are importable, expose consistent
metadata via ToolConfig, and provide a clean CLI entry. It also checks
for common repository hygiene (shebangs, README sections).
"""
from __future__ import annotations

import importlib
from pathlib import Path
from typing import List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_DIR = PROJECT_ROOT / "tools" / "specialized" / "benchmarking"


class TestBenchmarkingToolsOrganization:
    def test_benchmarking_directory_exists(self):
        assert BENCHMARK_DIR.is_dir(), f"Missing dir: {BENCHMARK_DIR}"

    def test_required_benchmarking_tools_exist(self):
        required: List[str] = [
            "unified_benchmark_tool.py",
            "model_benchmark_tool.py",
            "system_benchmark_tool.py",
            "data_quality_benchmark_tool.py",
            "README.md",
        ]
        for name in required:
            path = BENCHMARK_DIR / name
            assert path.exists(), f"Missing: {path}"

    def test_benchmarking_tools_are_executable(self):
        for py in [
            "unified_benchmark_tool.py",
            "model_benchmark_tool.py",
            "system_benchmark_tool.py",
            "data_quality_benchmark_tool.py",
        ]:
            p = BENCHMARK_DIR / py
            with p.open("r", encoding="utf-8") as fh:
                first = fh.readline().strip()
            assert first.startswith("#!/usr/bin/env python3"), f"Bad shebang in {p}: {first!r}"

    def test_benchmarking_readme_comprehensive(self):
        readme = (BENCHMARK_DIR / "README.md").read_text(encoding="utf-8")
        for section in [
            "Model Benchmark Tool",
            "System Performance Benchmark Tool",
            "Data Quality Benchmark Tool",
            "Unified Benchmark Tool",
        ]:
            assert section in readme, f"README missing section: {section}"


class TestUnifiedBenchmarkTool:
    def test_unified_benchmark_tool_imports(self):
        mod = importlib.import_module(
            "tools.specialized.benchmarking.unified_benchmark_tool"
        )
        Tool = getattr(mod, "UnifiedBenchmarkTool")
        tool = Tool()
        assert tool.name == "unified-benchmark"
        assert tool.version == "1.0.0"
        assert (tool.results_dir).name == "benchmarks"

    def test_unified_list_available_benchmarks(self):
        from tools.specialized.benchmarking.unified_benchmark_tool import (
            UnifiedBenchmarkTool,
        )

        tool = UnifiedBenchmarkTool()
        catalog = tool.list_available_benchmarks()
        for key in ["model", "system", "data_quality"]:
            assert key in catalog, f"Missing benchmark key: {key}"
            assert "module" in catalog[key] and "cli" in catalog[key]


class TestModelBenchmarkTool:
    def test_model_benchmark_tool_config(self):
        mod = importlib.import_module(
            "tools.specialized.benchmarking.model_benchmark_tool"
        )
        Tool = getattr(mod, "ModelBenchmarkTool")
        tool = Tool()
        cfg = tool.get_tool_config()
        assert cfg.category == "specialized"
        assert "DATABASE_URL" in cfg.environment_vars
        assert any("scikit-learn" in dep for dep in cfg.dependencies)


class TestSystemBenchmarkTool:
    def test_system_benchmark_tool_config(self):
        mod = importlib.import_module(
            "tools.specialized.benchmarking.system_benchmark_tool"
        )
        Tool = getattr(mod, "SystemBenchmarkTool")
        tool = Tool()
        cfg = tool.get_tool_config()
        assert cfg.category == "specialized"
        assert "DATABASE_URL" in cfg.environment_vars
        assert any("pandas" in dep for dep in cfg.dependencies)

    def test_system_benchmark_tool_performance_thresholds(self):
        from tools.specialized.benchmarking.system_benchmark_tool import (
            SystemBenchmarkTool,
        )

        tool = SystemBenchmarkTool()
        thresholds = tool.performance_thresholds
        assert "etl_throughput_rows_per_sec" in thresholds
        assert "database_avg_query_time_ms" in thresholds


class TestDataQualityBenchmarkTool:
    def test_data_quality_benchmark_tool_config(self):
        mod = importlib.import_module(
            "tools.specialized.benchmarking.data_quality_benchmark_tool"
        )
        Tool = getattr(mod, "DataQualityBenchmarkTool")
        tool = Tool()
        cfg = tool.get_tool_config()
        assert cfg.category == "specialized"
        assert "DATABASE_URL" in cfg.environment_vars

    def test_data_quality_benchmark_tool_thresholds(self):
        from tools.specialized.benchmarking.data_quality_benchmark_tool import (
            DataQualityBenchmarkTool,
        )

        tool = DataQualityBenchmarkTool()
        qt = tool.quality_thresholds
        for key in ["completeness_score", "accuracy_score", "consistency_score"]:
            assert key in qt


class TestBenchmarkingToolsIntegration:
    def test_all_tools_register_in_registry(self):
        from tools.shared.common import get_tool_registry
        from tools.specialized.benchmarking.data_quality_benchmark_tool import (
            DataQualityBenchmarkTool,
        )
        from tools.specialized.benchmarking.system_benchmark_tool import (
            SystemBenchmarkTool,
        )
        from tools.specialized.benchmarking.model_benchmark_tool import (
            ModelBenchmarkTool,
        )
        from tools.specialized.benchmarking.unified_benchmark_tool import (
            UnifiedBenchmarkTool,
        )

        # Instantiate to ensure registration occurs
        DataQualityBenchmarkTool()
        SystemBenchmarkTool()
        ModelBenchmarkTool()
        UnifiedBenchmarkTool()

        reg = get_tool_registry()
        names = [cfg.name for cfg in reg.list_tools()]
        for expected in [
            "data-quality-benchmark",
            "system-benchmark",
            "unified-benchmark",
        ]:
            assert expected in names

    def test_consistent_interfaces(self):
        from tools.shared.common import ToolBase
        from tools.specialized.benchmarking.data_quality_benchmark_tool import (
            DataQualityBenchmarkTool,
        )
        from tools.specialized.benchmarking.system_benchmark_tool import (
            SystemBenchmarkTool,
        )
        from tools.specialized.benchmarking.model_benchmark_tool import (
            ModelBenchmarkTool,
        )

        for Tool in [
            DataQualityBenchmarkTool,
            SystemBenchmarkTool,
            ModelBenchmarkTool,
        ]:
            tool = Tool()
            assert isinstance(tool, ToolBase)
            cfg = tool.get_tool_config()
            assert cfg.name and cfg.version and cfg.category == "specialized"
            assert hasattr(tool, "cleanup_resources")

    def test_binaries_have_main_and_argparse(self):
        for py in [
            "unified_benchmark_tool.py",
            "model_benchmark_tool.py",
            "system_benchmark_tool.py",
            "data_quality_benchmark_tool.py",
        ]:
            content = (BENCHMARK_DIR / py).read_text(encoding="utf-8")
            assert "if __name__ == \"__main__\"" in content
            assert "def main()" in content
            assert "argparse" in content

