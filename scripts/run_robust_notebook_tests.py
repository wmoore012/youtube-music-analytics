#!/usr / bin / env python3
"""
Robust Notebook Testing Script

This script runs comprehensive notebook tests to ensure notebooks
execute correctly and produce readable, meaningful outputs.

Usage:
    python scripts / run_robust_notebook_tests.py [--notebook NOTEBOOK_PATH] [--comprehensive]

Options:
    --notebook: Test a specific notebook file
    --comprehensive: Run the full comprehensive test suite
    --post - archive: Run post - archive validation tests
    --quick: Run quick validation tests only
"""

import argparse
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tests.test_notebook_execution_robust import RobustNotebookTester
from tests.test_notebook_integration_comprehensive import ComprehensiveNotebookIntegrationTester
from tests.test_post_archive_notebook_validation import PostArchiveNotebookValidator


def run_single_notebook_test(notebook_path: str) -> bool:
    """Test a single notebook."""
    print(f"🧪 Testing single notebook: {notebook_path}")

    if not Path(notebook_path).exists():
        print(f"❌ Notebook not found: {notebook_path}")
        return False

    try:
        tester = RobustNotebookTester(timeout=300)
        results = tester.execute_notebook_with_validation(notebook_path)

        print(f"\n📊 Results for {Path(notebook_path).name}:")
        print(f"  Execution successful: {'✅' if results['execution_successful'] else '❌'}")
        print(f"  Execution time: {results['execution_time']:.2f}s")
        print(f"  Validation passed: {'✅' if results['validation_results'].is_valid else '❌'}")
        print(f"  Outputs readable: {'✅' if results['readability_results'].is_valid else '❌'}")
        print(f"  Charts valid: {'✅' if results['chart_results'].is_valid else '❌'}")

        chart_count = results["chart_results"].metadata.get("total_charts", 0)
        if chart_count > 0:
            interactive_count = results["chart_results"].metadata.get("interactive_charts", 0)
            print(f"  Charts: {interactive_count}/{chart_count} interactive ({interactive_count / chart_count:.1%})")

        # Print errors if any
        all_errors = (
            results["validation_results"].errors
            + results["readability_results"].errors
            + results["chart_results"].errors
        )

        if all_errors:
            print(f"\n❌ Errors found:")
            for error in all_errors:
                print(f"  - {error}")

        # Generate report
        report_path = tester.generate_test_report()
        print(f"\n📋 Detailed report: {report_path}")

        return results["execution_successful"] and results["validation_results"].is_valid

    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        return False


def run_post_archive_tests() -> bool:
    """Run post - archive validation tests."""
    print("🔍 Running post - archive validation tests...")

    try:
        validator = PostArchiveNotebookValidator()

        # Run all validations
        print("\n1. Validating archive structure...")
        archive_results = validator.validate_archive_structure()
        print(f"   Archive structure: {'✅ VALID' if archive_results['is_valid'] else '❌ INVALID'}")

        if archive_results.get("archive_stats"):
            print("   Archive contents:")
            for archive_name, stats in archive_results["archive_stats"].items():
                print(f"     {archive_name}: {stats['original_count']} original, {stats['executed_count']} executed")

        print("\n2. Testing current notebook quality...")
        quality_results = validator.validate_notebook_quality_standards()
        pass_rate = quality_results["passing_notebooks"] / max(quality_results["total_notebooks"], 1)
        print(
            f"   Quality pass rate: {
                pass_rate:.1%} ({quality_results['passing_notebooks']}/{quality_results['total_notebooks']})"
        )

        if quality_results["failures"]:
            print("   Quality failures:")
            for failure in quality_results["failures"][:3]:  # Show first 3
                if "error" in failure:
                    print(f"     {failure['notebook']}: {failure['error']}")
                else:
                    failed_checks = ", ".join(failure["failed_checks"])
                    print(f"     {failure['notebook']}: {failed_checks}")

        print("\n3. Comparing current vs archived...")
        comparison_results = validator.compare_current_vs_archived()
        print(f"   Current notebooks: {comparison_results['current_count']}")
        print(f"   Archived notebooks: {comparison_results['archived_count']}")
        print(f"   Improvements: {len(comparison_results['improvements'])}")
        print(f"   Regressions: {len(comparison_results['regressions'])}")

        # Determine overall success
        success = (
            archive_results["is_valid"]
            and pass_rate >= 0.8
            and len(comparison_results["regressions"]) <= len(comparison_results["improvements"])
        )

        print(f"\n🏁 Post - archive validation: {'✅ PASSED' if success else '❌ FAILED'}")

        return success

    except Exception as e:
        print(f"❌ Post - archive validation failed: {str(e)}")
        return False


def run_comprehensive_tests() -> bool:
    """Run comprehensive test suite."""
    print("🚀 Running comprehensive notebook test suite...")

    try:
        tester = ComprehensiveNotebookIntegrationTester()
        results = tester.run_full_notebook_test_suite()

        # Print summary
        summary = results["summary"]
        print(f"\n📊 Comprehensive Test Results:")
        print(f"  Overall Status: {results['overall_status']}")
        print(f"  Phases Passed: {summary['phases_passed']}")
        print(f"  Phases Failed: {summary['phases_failed']}")
        print(f"  Phases Partial: {summary['phases_partial']}")
        print(f"  Phases Skipped: {summary['phases_skipped']}")

        # Print phase details
        print(f"\n📋 Phase Details:")
        for phase_name, phase_results in results["phases"].items():
            status_emoji = {"PASSED": "✅", "FAILED": "❌", "PARTIAL": "⚠️", "SKIPPED": "⏭️"}.get(
                phase_results.get("status", "UNKNOWN"), "❓"
            )

            print(f"  {phase_name.title()}: {status_emoji} {phase_results.get('status', 'UNKNOWN')}")

        # Print recommendations
        if summary["recommendations"]:
            print(f"\n💡 Recommendations:")
            for rec in summary["recommendations"]:
                print(f"  - {rec}")

        return results["overall_status"] in ["PASSED", "PARTIAL"]

    except Exception as e:
        print(f"❌ Comprehensive testing failed: {str(e)}")
        return False


def run_quick_tests() -> bool:
    """Run quick validation tests."""
    print("⚡ Running quick notebook validation tests...")

    try:
        # Find current notebooks
        notebooks_dir = Path("notebooks")

        if not notebooks_dir.exists():
            print("⚠️  No notebooks directory found")
            return True

        current_notebooks = [
            f
            for f in notebooks_dir.glob("*.ipynb")
            if not f.name.endswith("-executed.ipynb") and not f.name.startswith(".") and "archive" not in str(f)
        ]

        if not current_notebooks:
            print("⚠️  No current notebooks found")
            return True

        print(f"📝 Found {len(current_notebooks)} notebooks to test")

        # Test each notebook quickly
        tester = RobustNotebookTester(timeout=120)  # Shorter timeout for quick tests

        successful = 0
        failed = 0

        for notebook in current_notebooks:
            try:
                print(f"  Testing {notebook.name}...", end=" ")

                results = tester.execute_notebook_with_validation(str(notebook))

                if results["execution_successful"] and results["validation_results"].is_valid:
                    print("✅")
                    successful += 1
                else:
                    print("❌")
                    failed += 1

            except Exception as e:
                print(f"❌ ({str(e)[:50]}...)")
                failed += 1

        print(f"\n📊 Quick Test Results:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Success rate: {successful / (successful + failed):.1%}")

        return failed == 0

    except Exception as e:
        print(f"❌ Quick testing failed: {str(e)}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run robust notebook tests")
    parser.add_argument("--notebook", help="Test a specific notebook file")
    parser.add_argument("--comprehensive", action="store_true", help="Run comprehensive test suite")
    parser.add_argument("--post - archive", action="store_true", help="Run post - archive validation")
    parser.add_argument("--quick", action="store_true", help="Run quick validation tests")

    args = parser.parse_args()

    success = True

    if args.notebook:
        success = run_single_notebook_test(args.notebook)

    elif args.comprehensive:
        success = run_comprehensive_tests()

    elif args.post_archive:
        success = run_post_archive_tests()

    elif args.quick:
        success = run_quick_tests()

    else:
        # Default: run quick tests
        print("No specific test type specified, running quick tests...")
        success = run_quick_tests()

    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
