#!/usr / bin / env python3
"""
Model Benchmark Tool for Sentiment Analysis

This tool provides specialized functionality for benchmarking sentiment analysis models
within the tools directory organization framework.
"""

import argparse
from pathlib import Path
import sys
from typing import Any, Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from tools.shared.common import (
    ToolBase,
    ToolConfig,
    register_tool,
)

# Import the existing model benchmark system
try:
    from youtubeviz.model_benchmark_system import (
        BenchmarkConfig,
        BenchmarkRun,
        ModelBenchmarkSystem,
    )

    MODEL_BENCHMARK_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Model benchmark system not available: {e}")
    MODEL_BENCHMARK_AVAILABLE = False


class ModelBenchmarkTool(ToolBase):
    """
    Specialized tool for model performance benchmarking.

    Wraps the existing ModelBenchmarkSystem with standardized tool interfaces
    and enhanced functionality for the tools directory organization.
    """

    def __init__(self):
        super().__init__(name="model - benchmark", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        # Initialize the underlying benchmark system
        self.benchmark_system = None
        if MODEL_BENCHMARK_AVAILABLE:
            self._initialize_benchmark_system()

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return [
            "DATABASE_URL",  # For real data validation
        ]

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="model - benchmark",
            version="1.0.0",
            description="Specialized model performance benchmarking tool for sentiment analysis",
            dependencies=[
                "python>=3.8",
                "pandas>=2.0",
                "numpy>=1.20",
                "scikit - learn>=1.0",
                "sqlalchemy>=2.0",
                "vaderSentiment>=3.3",
                "textblob>=0.17",
            ],
            environment_vars=self.get_required_environment_vars(),
            usage_examples=[
                "python tools / specialized / benchmarking / model_benchmark_tool.py --run - benchmark",
                "python tools / specialized / benchmarking / model_benchmark_tool.py --list - models",
                "python tools / specialized / benchmarking / model_benchmark_tool.py --validate - data",
            ],
            category="specialized",
        )

    def run(self) -> None:
        """Main execution method - should not be called directly."""
        self.log_progress("Use specific methods like run_benchmark() or list_models()")

    def _initialize_benchmark_system(self) -> None:
        """Initialize the underlying model benchmark system."""
        try:
            self.benchmark_system = ModelBenchmarkSystem(results_dir="benchmark_results / models", use_database=True)
            self.log_progress("✅ Model benchmark system initialized")
        except Exception as e:
            self.handle_error(e, "benchmark system initialization")
            self.benchmark_system = None

    def list_available_models(self) -> Dict[str, Any]:
        """
        List all available models for benchmarking.

        Returns:
            Dictionary with model information
        """
        self.log_progress("📋 Listing available models for benchmarking")

        if not self.benchmark_system:
            return {
                "status": "ERROR",
                "error": "Model benchmark system not available",
                "models": {},
            }

        try:
            models_info = {
                "status": "SUCCESS",
                "total_models": len(self.benchmark_system.models),
                "categories": {},
                "models": {},
            }

            # Group models by type
            for model_key, model_info in self.benchmark_system.models.items():
                model_type = model_info["type"]

                if model_type not in models_info["categories"]:
                    models_info["categories"][model_type] = []

                model_entry = {
                    "key": model_key,
                    "name": model_info["name"],
                    "description": model_info["description"],
                    "type": model_type,
                }

                models_info["categories"][model_type].append(model_entry)
                models_info["models"][model_key] = model_entry

            return models_info

        except Exception as e:
            self.handle_error(e, "model listing")
            return {
                "status": "ERROR",
                "error": str(e),
                "models": {},
            }

    def validate_benchmark_data(self, sample_size: int = 1000) -> Dict[str, Any]:
        """
        Validate that benchmark data meets quality requirements.

        Args:
            sample_size: Number of samples to validate

        Returns:
            Dictionary with validation results
        """
        self.log_progress(f"🔍 Validating benchmark data (sample size: {sample_size})")

        if not self.benchmark_system:
            return {
                "status": "ERROR",
                "error": "Model benchmark system not available",
            }

        try:
            # Run pre - benchmark tests
            self.benchmark_system._run_pre_benchmark_tests()

            # Fetch and validate dataset
            dataset = self.benchmark_system.fetch_benchmark_dataset(sample_size=sample_size, random_state=42)

            if dataset.empty:
                return {
                    "status": "ERROR",
                    "error": "No valid benchmark data available",
                    "dataset_size": 0,
                }

            # Assess dataset quality
            labels = dataset["ground_truth"].tolist()
            quality_metrics = self.benchmark_system.assess_dataset_quality(labels)

            return {
                "status": "SUCCESS",
                "dataset_size": len(dataset),
                "quality_level": quality_metrics.quality_level,
                "balance_score": quality_metrics.balance_score,
                "class_distribution": {
                    "positive": quality_metrics.positive_count,
                    "negative": quality_metrics.negative_count,
                    "neutral": quality_metrics.neutral_count,
                },
                "recommendations": quality_metrics.recommendations,
                "validation_passed": quality_metrics.quality_level in ["acceptable", "good", "excellent"],
            }

        except Exception as e:
            self.handle_error(e, "data validation")
            return {
                "status": "ERROR",
                "error": str(e),
                "dataset_size": 0,
            }

    def run_benchmark(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run comprehensive model benchmark.

        Args:
            config: Optional benchmark configuration

        Returns:
            Dictionary with benchmark results
        """
        self.log_progress("🚀 Starting model benchmark")

        if not self.benchmark_system:
            return {
                "status": "ERROR",
                "error": "Model benchmark system not available",
            }

        if config is None:
            config = {}

        try:
            # Create benchmark configuration
            benchmark_config = BenchmarkConfig(
                experiment_name=config.get("experiment_name", f"model_benchmark_{self.get_timestamp()}"),
                test_size=config.get("test_size", 0.3),
                random_state=config.get("random_state", 42),
                min_samples_per_class=config.get("min_samples_per_class", 50),
                include_proprietary=config.get("include_proprietary", True),
                include_open_source=config.get("include_open_source", True),
                save_predictions=config.get("save_predictions", True),
            )

            # Run the benchmark
            self.log_progress(f"Running benchmark: {benchmark_config.experiment_name}")
            benchmark_run = self.benchmark_system.run_benchmark(benchmark_config)

            # Convert to standardized format
            results = {
                "status": "SUCCESS",
                "experiment_id": benchmark_run.experiment_id,
                "timestamp": benchmark_run.timestamp.isoformat(),
                "config": {
                    "experiment_name": benchmark_config.experiment_name,
                    "test_size": benchmark_config.test_size,
                    "random_state": benchmark_config.random_state,
                },
                "dataset_info": {
                    "total_samples": benchmark_run.dataset_quality.total_samples,
                    "quality_level": benchmark_run.dataset_quality.quality_level,
                    "balance_score": benchmark_run.dataset_quality.balance_score,
                },
                "models": [
                    {
                        "name": model.model_name,
                        "type": model.model_type,
                        "accuracy": model.accuracy,
                        "precision": model.precision,
                        "recall": model.recall,
                        "f1_score": model.f1_score,
                        "processing_time": model.processing_time,
                    }
                    for model in benchmark_run.models
                ],
                "summary": {
                    "total_models_tested": len(benchmark_run.models),
                    "best_model": max(benchmark_run.models, key=lambda m: m.f1_score).model_name,
                    "best_f1_score": max(model.f1_score for model in benchmark_run.models),
                    "avg_f1_score": sum(model.f1_score for model in benchmark_run.models) / len(benchmark_run.models),
                },
            }

            best_f1 = results["summary"]["best_f1_score"]
            best_model = results["summary"]["best_model"]
            self.log_progress(f"✅ Benchmark completed: {best_model} (F1: {best_f1:.3f})")
            return results

        except Exception as e:
            self.handle_error(e, "benchmark execution")
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def cleanup_resources(self) -> None:
        """Clean up any resources used during benchmarking."""
        if self.benchmark_system:
            self.benchmark_system.cleanup_resources()


def main():
    """Main entry point for the model benchmark tool."""
    parser = argparse.ArgumentParser(
        description="Model Benchmark Tool for Sentiment Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / specialized / benchmarking / model_benchmark_tool.py --list - models
  python tools / specialized / benchmarking / model_benchmark_tool.py --validate - data
  python tools / specialized / benchmarking / model_benchmark_tool.py --run - benchmark
        """,
    )

    # Operations
    parser.add_argument("--list - models", action="store_true", help="List all available models for benchmarking")
    parser.add_argument("--validate - data", action="store_true", help="Validate benchmark data quality")
    parser.add_argument("--run - benchmark", action="store_true", help="Run comprehensive model benchmark")

    # Configuration
    parser.add_argument("--experiment - name", type=str, help="Name for the benchmark experiment")
    parser.add_argument("--sample - size", type=int, default=1000, help="Sample size for data validation")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if not MODEL_BENCHMARK_AVAILABLE:
        print("❌ Model benchmark system not available. Please check dependencies.")
        return 1

    # Create model benchmark tool instance
    with ModelBenchmarkTool() as benchmark_tool:
        try:
            if args.list_models:
                result = benchmark_tool.list_available_models()
                if result["status"] == "SUCCESS":
                    print(f"🤖 Available Models ({result['total_models']} total):")
                    print("=" * 50)

                    for model_type, models in result["categories"].items():
                        print(f"\n📊 {model_type.replace('_', ' ').title()} Models:")
                        for model in models:
                            print(f"   • {model['name']}")
                            print(f"     Key: {model['key']}")
                            print(f"     Description: {model['description']}")
                else:
                    print(f"❌ Failed to list models: {result.get('error', 'Unknown error')}")
                    return 1

            elif args.validate_data:
                result = benchmark_tool.validate_benchmark_data(sample_size=args.sample_size)
                if result["status"] == "SUCCESS":
                    print("🔍 Data Validation Results:")
                    print(f"   Dataset size: {result['dataset_size']} samples")
                    print(f"   Quality level: {result['quality_level']}")
                    print(f"   Balance score: {result['balance_score']:.3f}")
                    validation_status = "✅" if result["validation_passed"] else "❌"
                    print(f"   Validation passed: {validation_status}")

                    print("\n📊 Class Distribution:")
                    dist = result["class_distribution"]
                    print(f"   Positive: {dist['positive']}")
                    print(f"   Negative: {dist['negative']}")
                    print(f"   Neutral: {dist['neutral']}")

                    if result.get("recommendations"):
                        print("\n💡 Recommendations:")
                        for rec in result["recommendations"]:
                            print(f"   • {rec}")
                else:
                    print(f"❌ Data validation failed: {result.get('error', 'Unknown error')}")
                    return 1

            elif args.run_benchmark:
                config = {}
                if args.experiment_name:
                    config["experiment_name"] = args.experiment_name

                result = benchmark_tool.run_benchmark(config)
                if result["status"] == "SUCCESS":
                    print("🚀 Benchmark Results:")
                    print(f"   Experiment: {result['config']['experiment_name']}")
                    dataset_info = result["dataset_info"]
                    print(f"   Dataset: {dataset_info['total_samples']} samples ({dataset_info['quality_level']})")
                    print(f"   Models tested: {result['summary']['total_models_tested']}")
                    print(f"   Best model: {result['summary']['best_model']}")
                    print(f"   Best F1 - score: {result['summary']['best_f1_score']:.3f}")
                    print(f"   Average F1 - score: {result['summary']['avg_f1_score']:.3f}")

                    print("\n📊 Model Performance:")
                    for model in sorted(result["models"], key=lambda m: m["f1_score"], reverse=True):
                        name = model["name"]
                        f1 = model["f1_score"]
                        acc = model["accuracy"]
                        time = model["processing_time"]
                        print(f"   {name}: F1={f1:.3f}, Acc={acc:.3f}, Time={time:.2f}s")
                else:
                    print(f"❌ Benchmark failed: {result.get('error', 'Unknown error')}")
                    return 1

            else:
                print("❌ No operation specified. Use --help for options.")
                return 1

            return 0

        except KeyboardInterrupt:
            benchmark_tool.log_progress("Model benchmark cancelled by user")
            return 1
        except Exception as e:
            benchmark_tool.handle_error(e, "main execution")
            return 1


if __name__ == "__main__":
    sys.exit(main())
