#!/usr/bin/env python3
"""
Easy Model Benchmarking Runner

Quick script to benchmark sentiment models with professional methodology.
Perfect for generating resume-worthy performance metrics!
"""

import sys

sys.path.insert(0, "src")

from youtubeviz.model_benchmark_system import BenchmarkConfig, ModelBenchmarkSystem
from youtubeviz.unique_comment_manager import UniqueCommentManager


def run_comprehensive_benchmark():
    """Run comprehensive benchmark of all available models with music domain filtering."""

    print("🚀 COMPREHENSIVE MODEL BENCHMARK")
    print("=" * 60)
    print("Professional random split testing with statistical rigor")
    print("Music domain filtering for enhanced accuracy")
    print("Results logged to JSON for tracking over time")
    print()

    # Create benchmark system with music domain filtering
    benchmark_system = ModelBenchmarkSystem()

    # Configure comprehensive benchmark with music domain focus
    config = BenchmarkConfig(
        experiment_name="comprehensive_sentiment_comparison",
        test_size=0.3,  # Professional 70/30 split
        random_state=42,  # Reproducible results
        min_samples_per_class=50,  # Ensure statistical power
        confidence_level=0.95,  # 95% confidence intervals
        include_proprietary=True,  # Include our secret sauce
        include_open_source=True,  # Include baseline models
        save_predictions=True,  # Save for detailed analysis
        # NEW: Dataset quality requirements
        min_balance_score=0.7,  # Allow slightly imbalanced for real data
        warn_on_imbalance=True,  # Warn about quality issues
        require_quality_check=True,  # Mandatory quality assessment
    )

    print(f"📋 Benchmark Configuration:")
    print(f"   Experiment: {config.experiment_name}")
    print(f"   Test split: {config.test_size:.1%}")
    print(f"   Random seed: {config.random_state}")
    print(f"   Min samples per class: {config.min_samples_per_class}")
    print(f"   Confidence level: {config.confidence_level:.1%}")
    print()

    # Run benchmark
    try:
        results = benchmark_system.run_benchmark(config)

        print(f"\n🎉 BENCHMARK COMPLETED SUCCESSFULLY!")
        print(f"📊 Experiment ID: {results.experiment_id}")
        print(f"💾 Results saved to: benchmark_results/")

        # Show quick summary for resume
        best_model = max(results.models, key=lambda x: x.f1_score)
        print(f"\n🏆 RESUME HIGHLIGHTS:")
        print(f"   Best Model: {best_model.model_name}")
        print(f"   F1-Score: {best_model.f1_score:.3f}")
        print(f"   Accuracy: {best_model.accuracy:.3f}")
        print(f"   Models Compared: {len(results.models)}")
        print(f"   Dataset Size: {results.dataset_info['total_samples']}")

        return True

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def run_quick_benchmark():
    """Run quick benchmark with just key models."""

    print("⚡ QUICK MODEL BENCHMARK")
    print("=" * 40)

    benchmark_system = ModelBenchmarkSystem()

    # Quick config
    config = BenchmarkConfig(
        experiment_name="quick_sentiment_test",
        test_size=0.3,
        random_state=42,
        include_proprietary=True,
        include_open_source=True,
    )

    # Test only key models including our new ML classifier
    key_models = [
        "ml_classifier",  # Our new ML champion
        "proprietary_enhanced",
        "enhanced_vader_comprehensive",
        "stock_vader",
        "textblob",
    ]

    try:
        results = benchmark_system.run_benchmark(config, models_to_test=key_models)
        print(f"\n✅ Quick benchmark completed!")
        return True
    except Exception as e:
        print(f"❌ Quick benchmark failed: {e}")
        return False


def view_benchmark_history():
    """View historical benchmark results."""

    import json
    from pathlib import Path

    print("📈 BENCHMARK HISTORY")
    print("=" * 40)

    history_file = Path("benchmark_results/benchmark_history.json")

    if not history_file.exists():
        print("No benchmark history found. Run a benchmark first!")
        return

    with open(history_file, "r") as f:
        history = json.load(f)

    print(f"Total benchmark runs: {history['summary']['total_runs']}")
    print(f"Best F1-score ever: {history['summary']['best_f1_ever']:.3f}")
    print(f"Average F1-score: {history['summary']['avg_f1_score']:.3f}")
    print()

    print("Recent runs:")
    for run in history["runs"][-5:]:  # Last 5 runs
        print(f"  {run['timestamp'][:19]} | {run['experiment_name']} | F1: {run['best_f1_score']:.3f}")


def collect_ml_training_data():
    """Collect ML-ready training data with music domain filtering."""

    print("🤖 ML TRAINING DATA COLLECTION")
    print("=" * 50)
    print("Collecting unique, music-focused comments for ML training")
    print()

    # Initialize unique comment manager
    comment_manager = UniqueCommentManager()

    # Get current allocation stats
    stats = comment_manager.get_usage_stats()
    print(f"📊 Current allocation stats:")
    print(f"   Total allocated: {stats['total_allocated']}")
    print(f"   By system: {stats['by_system']}")
    print()

    # Collect ML training data
    print("🎵 Collecting music domain comments...")

    try:
        # Export a complete ML dataset
        dataset_file = comment_manager.export_ml_dataset(
            dataset_name="music_sentiment_training",
            train_count=2000,
            val_count=400,
            test_count=400,
            export_format="jsonl",
        )

        if dataset_file:
            print(f"✅ ML dataset exported successfully: {dataset_file}")

            # Generate quality report
            print("\n📋 Generating data quality report...")
            ml_comments = comment_manager.get_ml_ready_comments(
                system_name="quality_check", usage_type="evaluation", count=100, music_domain_filter=True
            )

            quality_report = comment_manager.generate_data_quality_report(
                dataset_id="music_sentiment_training", comments=ml_comments
            )

            if quality_report:
                print(f"📊 Quality Report:")
                print(f"   Total samples: {quality_report['total_samples']}")
                print(f"   Valid samples: {quality_report['valid_samples']}")
                print(f"   Avg text length: {quality_report['avg_text_length']:.1f}")
                print(f"   Duplicates: {quality_report['duplicate_count']}")

                if quality_report["recommendations"]:
                    print(f"   Recommendations:")
                    for rec in quality_report["recommendations"]:
                        print(f"     - {rec}")
        else:
            print("❌ Failed to export ML dataset")

    except Exception as e:
        print(f"❌ Error collecting ML data: {e}")
        import traceback

        traceback.print_exc()


def run_ml_benchmark():
    """Run benchmark specifically optimized for ML model evaluation."""

    print("🤖 ML MODEL BENCHMARK")
    print("=" * 40)
    print("Specialized benchmark for transformer and ML models")
    print()

    benchmark_system = ModelBenchmarkSystem()

    # ML-focused config
    config = BenchmarkConfig(
        experiment_name="ml_model_comparison",
        test_size=0.2,  # 80/20 split for ML
        random_state=42,
        min_samples_per_class=100,  # Higher threshold for ML
        confidence_level=0.95,
        include_proprietary=True,
        include_open_source=True,
        save_predictions=True,
        # NEW: Dataset quality requirements for ML
        min_balance_score=0.8,  # Higher standard for ML
        warn_on_imbalance=True,
        require_quality_check=True,
    )

    # Test ML-specific models including all transformers
    ml_models = [
        "ml_classifier",
        "transformer_sentiment",
        "transformer_distilbert_base_uncased",
        "transformer_roberta_base",
        "transformer_cardiffnlp_twitter_roberta_base_sentiment_latest",
        "transformer_j_hartmann_emotion_english_distilroberta_base",
        "enhanced_vader_comprehensive",
        "proprietary_enhanced",
        "stock_vader",
    ]

    try:
        results = benchmark_system.run_benchmark(config, models_to_test=ml_models)
        print(f"\n✅ ML benchmark completed!")
        print(f"📊 Focus: Music domain with unique comments only")
        return True
    except Exception as e:
        print(f"❌ ML benchmark failed: {e}")
        return False


def main():
    """Main menu for benchmarking."""

    print("🧪 PROFESSIONAL MODEL BENCHMARKING SYSTEM")
    print("=" * 80)
    print("Generate resume-worthy performance metrics with statistical rigor!")
    print()

    while True:
        print("Choose an option:")
        print("1. 🚀 Run Comprehensive Benchmark (all models)")
        print("2. ⚡ Run Quick Benchmark (key models only)")
        print("3. 🤖 Run ML Model Benchmark (transformer focus)")
        print("4. 🎵 Collect ML Training Data (music domain)")
        print("5. 📈 View Benchmark History")
        print("6. 🚪 Exit")
        print()

        choice = input("Enter choice (1-6): ").strip()

        if choice == "1":
            run_comprehensive_benchmark()
        elif choice == "2":
            run_quick_benchmark()
        elif choice == "3":
            run_ml_benchmark()
        elif choice == "4":
            collect_ml_training_data()
        elif choice == "5":
            view_benchmark_history()
        elif choice == "6":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")

        print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
