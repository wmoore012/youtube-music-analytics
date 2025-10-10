#!/usr / bin / env python3
"""
Test Runner Script for ETL Pipeline

This script provides a convenient way to run the comprehensive test suite
with various options and configurations.

Usage:
    python scripts / run_tests.py                    # Run all tests
    python scripts / run_tests.py --unit             # Run only unit tests
    python scripts / run_tests.py --integration      # Run only integration tests
    python scripts / run_tests.py --coverage         # Run with coverage analysis
    python scripts / run_tests.py --fast             # Run fast tests only
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and return success status."""
    if description:
        print(f"🚀 {description}")

    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)

    if result.returncode == 0:
        print(f"✅ {description or 'Command'} completed successfully")
        return True
    else:
        print(f"❌ {description or 'Command'} failed with exit code {result.returncode}")
        return False


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="Run ETL pipeline tests")

    # Test selection options
    parser.add_argument("--unit", action="store_true", help="Run only unit tests")
    parser.add_argument("--integration", action="store_true", help="Run only integration tests")
    parser.add_argument("--video-filter", action="store_true", help="Run only video filter tests")

    # Coverage options
    parser.add_argument("--coverage", action="store_true", help="Run with coverage analysis")
    parser.add_argument("--coverage-target", type=float, default=80.0, help="Coverage target percentage")
    parser.add_argument("--coverage-fail", action="store_true", help="Fail if coverage below target")

    # Execution options
    parser.add_argument("--fast", action="store_true", help="Run fast tests only (skip slow integration tests)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--parallel", "-n", type=int, help="Run tests in parallel (number of workers)")
    parser.add_argument("--timeout", type=int, default=30, help="Test timeout in seconds")

    # Output options
    parser.add_argument("--html-report", action="store_true", help="Generate HTML coverage report")
    parser.add_argument("--junit", action="store_true", help="Generate JUnit XML report")

    args = parser.parse_args()

    # Set up project root
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    print("🧪 ETL Pipeline Test Runner")
    print("=" * 50)

    # Build pytest command
    pytest_cmd = [sys.executable, "-m", "pytest"]

    # Add test selection
    if args.unit:
        pytest_cmd.append("tests / test_etl_components.py")
        print("📋 Running unit tests only")
    elif args.integration:
        pytest_cmd.append("tests / test_integration.py")
        print("📋 Running integration tests only")
    elif args.video_filter:
        pytest_cmd.append("tests / test_video_filter.py")
        print("📋 Running video filter tests only")
    else:
        pytest_cmd.append("tests/")
        print("📋 Running all tests")

    # Add execution options
    if args.verbose:
        pytest_cmd.append("-v")

    if args.parallel:
        pytest_cmd.extend(["-n", str(args.parallel)])
        print(f"🔄 Running tests in parallel with {args.parallel} workers")

    pytest_cmd.extend(["--timeout", str(args.timeout)])

    if args.fast:
        pytest_cmd.extend(["-m", "not slow"])
        print("⚡ Running fast tests only")

    # Add coverage options
    if args.coverage:
        pytest_cmd.extend(["--cov=web", "--cov=src", "--cov-report=term-missing"])

        if args.html_report:
            pytest_cmd.append("--cov-report=html")
            print("📄 HTML coverage report will be generated")

    # Add output options
    if args.junit:
        pytest_cmd.extend(["--junit-xml", "test-results.xml"])
        print("📊 JUnit XML report will be generated")

    # Run tests
    success = run_command(pytest_cmd, "Running test suite")

    if not success:
        print("\n❌ Tests failed!")
        return 1

    # Run coverage analysis if requested
    if args.coverage:
        print("\n" + "=" * 50)
        coverage_cmd = [sys.executable, "tests / test_coverage.py", "--target", str(args.coverage_target)]

        if args.coverage_fail:
            coverage_cmd.append("--fail-under")

        coverage_success = run_command(coverage_cmd, "Running coverage analysis")

        if not coverage_success and args.coverage_fail:
            print(f"\n❌ Coverage below target ({args.coverage_target}%)!")
            return 1

    # Summary
    print("\n" + "=" * 50)
    print("🎉 Test execution completed successfully!")

    if args.coverage and args.html_report:
        print("📄 HTML coverage report: htmlcov / index.html")

    if args.junit:
        print("📊 JUnit XML report: test-results.xml")

    return 0


if __name__ == "__main__":
    sys.exit(main())
