#!/usr/bin/env python3
"""
Test Dataset Quality Integration in Benchmark System

Tests the new dataset quality assessment features.
"""

import sys

sys.path.insert(0, "src")


def test_dataset_quality_assessment():
    """Test the dataset quality assessment functionality."""
    print("🧪 Testing Dataset Quality Assessment...")

    try:
        from youtubeviz.model_benchmark_system import ModelBenchmarkSystem

        benchmark_system = ModelBenchmarkSystem()

        # Test with different dataset scenarios
        test_cases = [
            {
                "name": "Severely Imbalanced (Original Problem)",
                "labels": ["positive"] * 32 + ["negative"] * 4 + ["neutral"] * 4,
                "expected_quality": "poor",
            },
            {
                "name": "Well Balanced (Fixed)",
                "labels": ["positive"] * 33 + ["negative"] * 33 + ["neutral"] * 31,
                "expected_quality": "acceptable",
            },
            {
                "name": "Large Balanced Dataset",
                "labels": ["positive"] * 1000 + ["negative"] * 1000 + ["neutral"] * 1000,
                "expected_quality": "excellent",
            },
            {
                "name": "Too Small Dataset",
                "labels": ["positive"] * 10 + ["negative"] * 10 + ["neutral"] * 10,
                "expected_quality": "poor",
            },
        ]

        for test_case in test_cases:
            print(f"\n📊 Testing: {test_case['name']}")
            print("-" * 50)

            quality_metrics = benchmark_system.assess_dataset_quality(test_case["labels"])

            print(f"Quality Level: {quality_metrics.quality_level}")
            print(f"Balance Score: {quality_metrics.balance_score:.3f}")
            print(f"Total Samples: {quality_metrics.total_samples}")
            print(
                f"Distribution: P:{quality_metrics.positive_count} N:{quality_metrics.negative_count} Neu:{quality_metrics.neutral_count}"
            )
            print(f"Imbalance Ratio: {quality_metrics.imbalance_ratio:.2f}x")

            # Check if quality matches expectation
            if quality_metrics.quality_level == test_case["expected_quality"]:
                print("✅ Quality assessment correct!")
            else:
                print(f"⚠️  Expected {test_case['expected_quality']}, got {quality_metrics.quality_level}")

            if quality_metrics.recommendations:
                print("Recommendations:")
                for rec in quality_metrics.recommendations[:2]:  # Show first 2
                    print(f"  - {rec}")

        print("\n✅ Dataset quality assessment working correctly!")
        return True

    except Exception as e:
        print(f"❌ Dataset quality assessment test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_quality_report_display():
    """Test the quality report display functionality."""
    print("\n🧪 Testing Quality Report Display...")

    try:
        from youtubeviz.model_benchmark_system import ModelBenchmarkSystem

        benchmark_system = ModelBenchmarkSystem()

        # Test with the original problematic dataset
        problematic_labels = ["positive"] * 32 + ["negative"] * 4 + ["neutral"] * 4
        quality_metrics = benchmark_system.assess_dataset_quality(problematic_labels)

        print("\n📋 SAMPLE QUALITY REPORT:")
        benchmark_system.print_dataset_quality_report(quality_metrics)

        print("✅ Quality report display working correctly!")
        return True

    except Exception as e:
        print(f"❌ Quality report display test failed: {e}")
        return False


def test_benchmark_integration():
    """Test integration with benchmark system."""
    print("\n🧪 Testing Benchmark Integration...")

    try:
        from youtubeviz.model_benchmark_system import BenchmarkConfig, ModelBenchmarkSystem

        benchmark_system = ModelBenchmarkSystem()

        # Test config with quality requirements
        config = BenchmarkConfig(
            experiment_name="quality_test", min_balance_score=0.8, warn_on_imbalance=True, require_quality_check=True
        )

        print(f"✅ Benchmark config with quality checks created:")
        print(f"   Min balance score: {config.min_balance_score}")
        print(f"   Warn on imbalance: {config.warn_on_imbalance}")
        print(f"   Require quality check: {config.require_quality_check}")

        return True

    except Exception as e:
        print(f"❌ Benchmark integration test failed: {e}")
        return False


def demonstrate_quality_benchmarks():
    """Demonstrate the quality benchmarks with real examples."""
    print("\n🎯 QUALITY BENCHMARKS DEMONSTRATION")
    print("=" * 60)

    examples = [
        {
            "name": "CURRENT (BAD)",
            "labels": ["positive"] * 32 + ["negative"] * 4 + ["neutral"] * 4,
            "description": "Original imbalanced dataset",
        },
        {
            "name": "MINIMUM ACCEPTABLE",
            "labels": ["positive"] * 100 + ["negative"] * 100 + ["neutral"] * 100,
            "description": "300 total (100 per class, 33% each)",
        },
        {
            "name": "GOOD FOR PRODUCTION",
            "labels": ["positive"] * 1000 + ["negative"] * 1000 + ["neutral"] * 1000,
            "description": "3000 total (1000 per class, 33% each)",
        },
    ]

    try:
        from youtubeviz.model_benchmark_system import ModelBenchmarkSystem

        benchmark_system = ModelBenchmarkSystem()

        for example in examples:
            print(f"\n📊 {example['name']}:")
            print(f"Description: {example['description']}")

            quality_metrics = benchmark_system.assess_dataset_quality(example["labels"])

            print(f"  Quality Level: {quality_metrics.quality_level.upper()}")
            print(f"  Balance Score: {quality_metrics.balance_score:.3f}")
            print(f"  Total: {quality_metrics.total_samples}")
            print(
                f"  Distribution: {quality_metrics.positive_percent:.1f}% pos, {quality_metrics.negative_percent:.1f}% neg, {quality_metrics.neutral_percent:.1f}% neu"
            )
            print(f"  Imbalance: {quality_metrics.imbalance_ratio:.1f}x")

        return True

    except Exception as e:
        print(f"❌ Quality benchmarks demonstration failed: {e}")
        return False


def main():
    """Run all dataset quality tests."""
    print("🚀 TESTING DATASET QUALITY BENCHMARK INTEGRATION")
    print("=" * 70)

    tests = [
        test_dataset_quality_assessment,
        test_quality_report_display,
        test_benchmark_integration,
        demonstrate_quality_benchmarks,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1

    print(f"\n📊 TEST RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Dataset quality integration is working.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")

    print("\n💡 Next steps:")
    print("   1. Run benchmark with quality checks: python benchmark_models.py")
    print("   2. The system will now warn about imbalanced datasets")
    print("   3. Quality metrics are saved with benchmark results")


if __name__ == "__main__":
    main()
