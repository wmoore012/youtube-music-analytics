#!/usr/bin/env python3
"""
CI/CD Benchmark Integrity Test Runner

Run this in CI/CD to ensure benchmark system never uses fake data.
MUST pass before any benchmarking is allowed.
"""

import subprocess
import sys


def run_integrity_tests():
    """Run all benchmark integrity tests."""

    print("🔒 CI/CD BENCHMARK INTEGRITY TESTS")
    print("=" * 60)
    print("🚨 CRITICAL: These tests MUST pass before benchmarking")
    print("❌ NO FAKE DATA: Ensures only real database data is used")
    print()

    # Run the integrity test suite
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_benchmark_data_integrity.py", "-v", "--tb=short"],
            capture_output=True,
            text=True,
            timeout=60,
        )

        print("📋 TEST OUTPUT:")
        print("-" * 30)
        print(result.stdout)

        if result.stderr:
            print("⚠️  STDERR:")
            print(result.stderr)

        if result.returncode == 0:
            print("✅ ALL INTEGRITY TESTS PASSED")
            print("🔒 BENCHMARK SYSTEM IS SAFE TO USE")
            print("✅ GUARANTEE: No fake data will be used")
            return True
        else:
            print("❌ INTEGRITY TESTS FAILED")
            print("🚨 BENCHMARK SYSTEM IS NOT SAFE")
            print("❌ DO NOT RUN BENCHMARKS UNTIL FIXED")
            return False

    except subprocess.TimeoutExpired:
        print("❌ INTEGRITY TESTS TIMED OUT")
        return False
    except Exception as e:
        print(f"❌ ERROR RUNNING INTEGRITY TESTS: {e}")
        return False


def main():
    """Main CI/CD entry point."""

    success = run_integrity_tests()

    if success:
        print("\n🎉 CI/CD INTEGRITY CHECK PASSED")
        print("✅ Benchmark system is ready for use")
        sys.exit(0)
    else:
        print("\n💥 CI/CD INTEGRITY CHECK FAILED")
        print("❌ Fix integrity issues before benchmarking")
        sys.exit(1)


if __name__ == "__main__":
    main()
