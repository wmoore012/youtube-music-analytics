#!/usr/bin/env python3
"""
Enhanced Sentiment Analysis System - Test Runner

Runs the comprehensive testing suite for the enhanced sentiment analysis system.
This script demonstrates all the testing capabilities including:

- Deterministic ID generation and Unicode normalization
- VADER variant creation and scoring consistency
- Evaluation framework with statistical rigor
- Performance and memory usage testing
- Statistical test validation and reproducibility
- Integration with existing system components

Usage:
    python run_enhanced_sentiment_tests.py
"""

from pathlib import Path
import subprocess
import sys
import time


def run_test_category(category_name: str, test_pattern: str) -> tuple[int, int]:
    """Run a specific test category and return (passed, total) counts."""
    print(f"\n🧪 Running {category_name}")
    print("=" * 60)

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        f"tests/test_enhanced_sentiment_analysis_system.py::{test_pattern}",
        "-v",
        "--tb=short",
    ]

    start_time = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    duration = time.time() - start_time

    # Parse results from pytest output
    output_lines = result.stdout.split("\n")
    summary_line = [line for line in output_lines if "passed" in line and ("failed" in line or "skipped" in line)]

    if summary_line:
        summary = summary_line[-1]
        print(f"📊 {summary}")
    else:
        print("📊 Test results not parsed correctly")

    print(f"⏱️  Duration: {duration:.2f}s")

    # Count passed tests
    passed = result.stdout.count(" PASSED")
    total = passed + result.stdout.count(" FAILED") + result.stdout.count(" SKIPPED")

    if result.returncode != 0 and "FAILED" in result.stdout:
        print("❌ Some tests failed:")
        failed_lines = [line for line in output_lines if "FAILED" in line]
        for line in failed_lines[:3]:  # Show first 3 failures
            print(f"   {line}")

    return passed, total


def main():
    """Run comprehensive enhanced sentiment analysis tests."""
    print("🎯 Enhanced Sentiment Analysis System - Comprehensive Test Suite")
    print("=" * 80)
    print("Testing all components of the enhanced sentiment analysis system...")

    # Test categories to run
    test_categories = [
        ("Deterministic ID Generation", "TestDeterministicIDGeneration"),
        ("VADER Variant Consistency", "TestVADERVariantConsistency"),
        ("Evaluation Framework", "TestEvaluationFrameworkWithRealData"),
        ("Performance & Memory", "TestPerformanceAndMemoryUsage"),
        ("Statistical Validation", "TestStatisticalTestValidation"),
        ("System Integration", "TestIntegrationWithExistingSystem"),
    ]

    total_passed = 0
    total_tests = 0
    start_time = time.time()

    for category_name, test_pattern in test_categories:
        try:
            passed, tests = run_test_category(category_name, test_pattern)
            total_passed += passed
            total_tests += tests
        except Exception as e:
            print(f"❌ Error running {category_name}: {e}")

    # Final summary
    duration = time.time() - start_time
    print(f"\n🎯 Final Results")
    print("=" * 60)
    print(f"✅ Tests Passed: {total_passed}")
    print(f"📊 Total Tests: {total_tests}")
    print(f"📈 Success Rate: {total_passed/total_tests*100:.1f}%" if total_tests > 0 else "📈 Success Rate: N/A")
    print(f"⏱️  Total Duration: {duration:.2f}s")

    if total_passed == total_tests:
        print("\n🎉 All tests passed! Enhanced sentiment analysis system is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total_tests - total_passed} tests failed or were skipped.")
        print("Note: Database-dependent tests are skipped when database is not available.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
