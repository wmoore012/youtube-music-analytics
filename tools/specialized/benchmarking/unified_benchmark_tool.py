#!/usr/bin/env python3
"""
Unified Benchmark Tool

Provides a single entry point to list and trigger specialized benchmarks
across the repository (model, system, and data quality).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

# Add project root for imports when executed as a script
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from tools.shared.common import ToolBase, ToolConfig, register_tool  # noqa: E402


class UnifiedBenchmarkTool(ToolBase):
    """Coordinator for available benchmarking tools.

    Exposes a consistent interface and metadata via ToolBase/ToolConfig.
    """

    def __init__(self) -> None:
        super().__init__(name="unified-benchmark", version="1.0.0")
        # Register this tool for discovery
        register_tool(self.get_tool_config())
        # Simple results directory hint used by tests/tools
        self.results_dir = PROJECT_ROOT / "reports" / "benchmarks"

    def get_required_environment_vars(self) -> List[str]:
        # Keep minimal but real requirements used by other tools/tests
        return [
            "DATABASE_URL",
            "YOUTUBE_API_KEY",
        ]

    def get_tool_config(self) -> ToolConfig:
        return ToolConfig(
            name="unified-benchmark",
            version="1.0.0",
            description="Unified entry point to model/system/data-quality benchmarks",
            dependencies=[
                "python>=3.8",
                "pandas>=2.0",
                "numpy>=1.20",
                "sqlalchemy>=2.0",
                "scikit-learn>=1.0",
            ],
            environment_vars=self.get_required_environment_vars(),
            usage_examples=[
                "python tools/specialized/benchmarking/unified_benchmark_tool.py --list-benchmarks",
                "python tools/specialized/benchmarking/unified_benchmark_tool.py --model-benchmark",
                "python tools/specialized/benchmarking/unified_benchmark_tool.py --system-benchmark",
                "python tools/specialized/benchmarking/unified_benchmark_tool.py --data-quality-benchmark",
            ],
            category="specialized",
        )

    def list_available_benchmarks(self) -> Dict[str, Dict[str, str]]:
        """Return a mapping of available benchmarks and how to invoke them."""
        return {
            "model": {
                "module": "tools.specialized.benchmarking.model_benchmark_tool",
                "entry": "ModelBenchmarkTool",
                "cli": "python tools/specialized/benchmarking/model_benchmark_tool.py --help",
            },
            "system": {
                "module": "tools.specialized.benchmarking.system_benchmark_tool",
                "entry": "SystemBenchmarkTool",
                "cli": "python tools/specialized/benchmarking/system_benchmark_tool.py --help",
            },
            "data_quality": {
                "module": "tools.specialized.benchmarking.data_quality_benchmark_tool",
                "entry": "DataQualityBenchmarkTool",
                "cli": "python tools/specialized/benchmarking/data_quality_benchmark_tool.py --help",
            },
        }

    def run(self) -> None:  # pragma: no cover - main entry is through specific sub-tools
        self.log_progress("Use --model-benchmark/--system-benchmark/--data-quality-benchmark or --list-benchmarks")

    def cleanup_resources(self) -> None:
        # Nothing to clean up for coordinator
        pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unified Benchmark Tool",
    )
    parser.add_argument("--list-benchmarks", action="store_true", help="List available benchmarks")
    parser.add_argument("--model-benchmark", action="store_true", help="Print model benchmark entry info")
    parser.add_argument("--system-benchmark", action="store_true", help="Print system benchmark entry info")
    parser.add_argument(
        "--data-quality-benchmark", action="store_true", help="Print data quality benchmark entry info"
    )
    args = parser.parse_args()

    tool = UnifiedBenchmarkTool()

    if args.list_benchmarks:
        for name, meta in tool.list_available_benchmarks().items():
            print(f"{name}: {meta['cli']}")
        return 0

    if args.model_benchmark:
        print(tool.list_available_benchmarks()["model"]["cli"])
        return 0

    if args.system_benchmark:
        print(tool.list_available_benchmarks()["system"]["cli"])
        return 0

    if args.data_quality_benchmark:
        print(tool.list_available_benchmarks()["data_quality"]["cli"])
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())

