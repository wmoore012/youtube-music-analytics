"""
Comprehensive notebook integration testing.

This module provides the main integration point for all notebook testing,
ensuring notebooks run correctly, produce readable outputs, and maintain
quality standards throughout the development lifecycle.

This test suite is designed to be run as part of CI / CD and includes:
- Pre-commit notebook validation
- Post-archive validation
- Performance monitoring
- Quality assurance
- Regression testing
"""

import os
import sys
from pathlib import Path
from typing import Dict, List

import pytest

from tests.test_notebook_execution_robust import RobustNotebookTester
from tests.test_post_archive_notebook_validation import PostArchiveNotebookValidator


class ComprehensiveNotebookIntegrationTester:
    """
    Main integration tester for comprehensive notebook validation.

    This class orchestrates all notebook testing activities and provides
    a single entry point for validating notebook functionality across
    the entire development lifecycle.
    """

    def __init__(self):
        """Initialize the comprehensive tester."""
        self.robust_tester = RobustNotebookTester(timeout=300)
        self.post_archive_validator = PostArchiveNotebookValidator()
        self.test_results = {}

    def run_full_notebook_test_suite(self) -> Dict[str, any]:
        """
        Run the complete notebook test suite.

        Returns:
            Dictionary with comprehensive test results
        """
        results = {
            "timestamp": self.robust_tester.execution_results,
            "phases": {},
            "overall_status": "UNKNOWN",
            "summary": {},
        }

        print("🚀 Starting comprehensive notebook test suite...")

        # Phase 1: Basic execution testing
        print("\n📋 Phase 1: Basic Execution Testing")
        try:
            execution_results = self._run_basic_execution_tests()
            results["phases"]["execution"] = execution_results
            print(f"   ✅ Execution tests: {execution_results['status']}")
        except Exception as e:
            results["phases"]["execution"] = {"status": "FAILED", "error": str(e)}
            print(f"   ❌ Execution tests failed: {str(e)}")

        # Phase 2: Post-archive validation
        print("\n📋 Phase 2: Post-Archive Validation")
        try:
            archive_results = self._run_post_archive_validation()
            results["phases"]["post_archive"] = archive_results
            print(f"   ✅ Post-archive validation: {archive_results['status']}")
        except Exception as e:
            results["phases"]["post_archive"] = {"status": "FAILED", "error": str(e)}
            print(f"   ❌ Post-archive validation failed: {str(e)}")

        # Phase 3: Quality assurance
        print("\n📋 Phase 3: Quality Assurance")
        try:
            quality_results = self._run_quality_assurance()
            results["phases"]["quality"] = quality_results
            print(f"   ✅ Quality assurance: {quality_results['status']}")
        except Exception as e:
            results["phases"]["quality"] = {"status": "FAILED", "error": str(e)}
            print(f"   ❌ Quality assurance failed: {str(e)}")

        # Phase 4: Performance monitoring
        print("\n📋 Phase 4: Performance Monitoring")
        try:
            performance_results = self._run_performance_monitoring()
            results["phases"]["performance"] = performance_results
            print(f"   ✅ Performance monitoring: {performance_results['status']}")
        except Exception as e:
            results["phases"]["performance"] = {"status": "FAILED", "error": str(e)}
            print(f"   ❌ Performance monitoring failed: {str(e)}")

        # Determine overall status
        phase_statuses = [phase.get("status", "FAILED") for phase in results["phases"].values()]

        if all(status == "PASSED" for status in phase_statuses):
            results["overall_status"] = "PASSED"
        elif any(status == "FAILED" for status in phase_statuses):
            results["overall_status"] = "FAILED"
        else:
            results["overall_status"] = "PARTIAL"

        # Generate summary
        results["summary"] = self._generate_test_summary(results)

        print(f"\n🏁 Comprehensive test suite complete: {results['overall_status']}")

        return results

    def _run_basic_execution_tests(self) -> Dict[str, any]:
        """Run basic notebook execution tests."""
        notebooks_dir = Path("notebooks")

        if not notebooks_dir.exists():
            return {"status": "SKIPPED", "reason": "No notebooks directory found"}

        # Find current notebooks
        current_notebooks = [
            f
            for f in notebooks_dir.glob("*.ipynb")
            if not f.name.endswith("-executed.ipynb") and not f.name.startswith(".") and "archive" not in str(f)
        ]

        if not current_notebooks:
            return {"status": "SKIPPED", "reason": "No current notebooks found"}

        results = {
            "status": "PASSED",
            "notebooks_tested": len(current_notebooks),
            "successful_executions": 0,
            "failed_executions": 0,
            "execution_details": {},
        }

        for notebook in current_notebooks:
            try:
                test_result = self.robust_tester.execute_notebook_with_validation(str(notebook))

                if test_result["execution_successful"]:
                    results["successful_executions"] += 1
                else:
                    results["failed_executions"] += 1
                    results["status"] = "FAILED"

                results["execution_details"][notebook.name] = {
                    "successful": test_result["execution_successful"],
                    "execution_time": test_result.get("execution_time", 0),
                    "chart_count": test_result.get("chart_results", {}).get("metadata", {}).get("total_charts", 0),
                }

            except Exception as e:
                results["failed_executions"] += 1
                results["status"] = "FAILED"
                results["execution_details"][notebook.name] = {"successful": False, "error": str(e)}

        return results

    def _run_post_archive_validation(self) -> Dict[str, any]:
        """Run post-archive validation tests."""
        try:
            # Validate archive structure
            archive_results = self.post_archive_validator.validate_archive_structure()

            # Validate current notebook quality
            quality_results = self.post_archive_validator.validate_notebook_quality_standards()

            # Compare current vs archived
            comparison_results = self.post_archive_validator.compare_current_vs_archived()

            # Determine status
            status = "PASSED"

            if not archive_results["is_valid"]:
                status = "FAILED"

            if quality_results["total_notebooks"] > 0:
                pass_rate = quality_results["passing_notebooks"] / quality_results["total_notebooks"]
                if pass_rate < 0.8:  # Less than 80% pass rate
                    status = "FAILED"

            if len(comparison_results["regressions"]) > len(comparison_results["improvements"]):
                status = "PARTIAL"  # More regressions than improvements

            return {
                "status": status,
                "archive_valid": archive_results["is_valid"],
                "quality_pass_rate": quality_results["passing_notebooks"] / max(quality_results["total_notebooks"], 1),
                "improvements": len(comparison_results["improvements"]),
                "regressions": len(comparison_results["regressions"]),
            }

        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    def _run_quality_assurance(self) -> Dict[str, any]:
        """Run quality assurance tests."""
        try:
            # Check for interactive charts
            current_notebooks = self.post_archive_validator.get_current_notebooks()

            if not current_notebooks:
                return {"status": "SKIPPED", "reason": "No current notebooks found"}

            chart_stats = {
                "notebooks_with_charts": 0,
                "total_charts": 0,
                "interactive_charts": 0,
                "notebooks_tested": 0,
            }

            for notebook in current_notebooks:
                try:
                    results = self.robust_tester.execute_notebook_with_validation(str(notebook))
                    chart_stats["notebooks_tested"] += 1

                    chart_meta = results.get("chart_results", {}).get("metadata", {})
                    total_charts = chart_meta.get("total_charts", 0)
                    interactive_charts = chart_meta.get("interactive_charts", 0)

                    if total_charts > 0:
                        chart_stats["notebooks_with_charts"] += 1
                        chart_stats["total_charts"] += total_charts
                        chart_stats["interactive_charts"] += interactive_charts

                except Exception:
                    continue  # Skip failed notebooks for chart analysis

            # Determine quality status
            status = "PASSED"

            if chart_stats["total_charts"] > 0:
                interactivity_rate = chart_stats["interactive_charts"] / chart_stats["total_charts"]
                if interactivity_rate < 0.8:
                    status = "FAILED"
            else:
                status = "PARTIAL"  # No charts found

            return {
                "status": status,
                "chart_statistics": chart_stats,
                "interactivity_rate": chart_stats["interactive_charts"] / max(chart_stats["total_charts"], 1),
            }

        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    def _run_performance_monitoring(self) -> Dict[str, any]:
        """Run performance monitoring tests."""
        try:
            # Analyze execution times from previous tests
            execution_times = []

            for notebook_path, results in self.robust_tester.execution_results.items():
                if "execution_time" in results:
                    execution_times.append(results["execution_time"])

            if not execution_times:
                return {"status": "SKIPPED", "reason": "No execution time data available"}

            # Calculate performance metrics
            avg_execution_time = sum(execution_times) / len(execution_times)
            max_execution_time = max(execution_times)
            min_execution_time = min(execution_times)

            # Determine performance status
            status = "PASSED"

            if avg_execution_time > 180:  # Average over 3 minutes
                status = "PARTIAL"
            if max_execution_time > 300:  # Any notebook over 5 minutes
                status = "FAILED"

            return {
                "status": status,
                "notebooks_analyzed": len(execution_times),
                "avg_execution_time": avg_execution_time,
                "max_execution_time": max_execution_time,
                "min_execution_time": min_execution_time,
                "performance_grade": "A" if avg_execution_time < 60 else "B" if avg_execution_time < 120 else "C",
            }

        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    def _generate_test_summary(self, results: Dict[str, any]) -> Dict[str, any]:
        """Generate comprehensive test summary."""
        summary = {
            "overall_status": results["overall_status"],
            "phases_passed": sum(1 for phase in results["phases"].values() if phase.get("status") == "PASSED"),
            "phases_failed": sum(1 for phase in results["phases"].values() if phase.get("status") == "FAILED"),
            "phases_partial": sum(1 for phase in results["phases"].values() if phase.get("status") == "PARTIAL"),
            "phases_skipped": sum(1 for phase in results["phases"].values() if phase.get("status") == "SKIPPED"),
            "recommendations": [],
        }

        # Generate recommendations based on results
        if results["phases"].get("execution", {}).get("status") == "FAILED":
            summary["recommendations"].append("Fix notebook execution errors before proceeding")

        if results["phases"].get("quality", {}).get("status") == "FAILED":
            summary["recommendations"].append("Improve chart interactivity and output quality")

        if results["phases"].get("performance", {}).get("status") == "FAILED":
            summary["recommendations"].append("Optimize notebook execution performance")

        if results["phases"].get("post_archive", {}).get("status") == "FAILED":
            summary["recommendations"].append("Review archive structure and current notebook quality")

        if not summary["recommendations"]:
            summary["recommendations"].append("All tests passed-notebooks are ready for production")

        return summary


class TestComprehensiveNotebookIntegration:
    """Pytest test class for comprehensive notebook integration testing."""

    def setup_method(self):
        """Set up test fixtures."""
        self.tester = ComprehensiveNotebookIntegrationTester()

    def test_comprehensive_notebook_suite(self):
        """Run the complete comprehensive notebook test suite."""
        print("\n🚀 Running comprehensive notebook integration test suite...")

        # Run full test suite
        results = self.tester.run_full_notebook_test_suite()

        # Print summary
        summary = results["summary"]
        print(f"\n📊 Test Suite Summary:")
        print(f"  Overall Status: {results['overall_status']}")
        print(f"  Phases Passed: {summary['phases_passed']}")
        print(f"  Phases Failed: {summary['phases_failed']}")
        print(f"  Phases Partial: {summary['phases_partial']}")
        print(f"  Phases Skipped: {summary['phases_skipped']}")

        # Print recommendations
        if summary["recommendations"]:
            print(f"\n💡 Recommendations:")
            for rec in summary["recommendations"]:
                print(f"  - {rec}")

        # Generate comprehensive report
        self._generate_comprehensive_report(results)

        # Assert overall success
        assert results["overall_status"] in ["PASSED", "PARTIAL"], (
            f"Comprehensive notebook test suite failed: {results['overall_status']}\n"
            f"Failed phases: {[name for name, phase in results['phases'].items() if phase.get('status') == 'FAILED']}"
        )

        print(f"\n✅ Comprehensive notebook integration test suite: {results['overall_status']}")

    def test_notebooks_ready_for_production(self):
        """Test that notebooks are ready for production deployment."""
        # Run basic checks
        tester = ComprehensiveNotebookIntegrationTester()

        # Check execution
        execution_results = tester._run_basic_execution_tests()
        assert execution_results["status"] in ["PASSED", "SKIPPED"], f"Execution tests failed: {execution_results}"

        # Check quality
        quality_results = tester._run_quality_assurance()
        assert quality_results["status"] in ["PASSED", "PARTIAL"], f"Quality tests failed: {quality_results}"

        # Check performance
        performance_results = tester._run_performance_monitoring()
        if performance_results["status"] not in ["SKIPPED"]:
            assert performance_results["status"] in [
                "PASSED",
                "PARTIAL",
            ], f"Performance tests failed: {performance_results}"

        print("✅ Notebooks are ready for production deployment")

    def _generate_comprehensive_report(self, results: Dict[str, any]) -> str:
        """Generate comprehensive test report."""
        from datetime import datetime

        report_lines = [
            "# Comprehensive Notebook Integration Test Report",
            f"Generated: {datetime.utcnow().isoformat()}Z",
            "",
            f"## Overall Status: {results['overall_status']}",
            "",
            "## Phase Results",
        ]

        for phase_name, phase_results in results["phases"].items():
            status_emoji = {"PASSED": "✅", "FAILED": "❌", "PARTIAL": "⚠️", "SKIPPED": "⏭️"}.get(
                phase_results.get("status", "UNKNOWN"), "❓"
            )

            report_lines.extend(
                [f"### {phase_name.title()} {status_emoji}", f"Status: {phase_results.get('status', 'UNKNOWN')}"]
            )

            # Add phase-specific details
            if phase_name == "execution" and "execution_details" in phase_results:
                report_lines.extend(
                    [
                        f"- Notebooks tested: {phase_results['notebooks_tested']}",
                        f"- Successful executions: {phase_results['successful_executions']}",
                        f"- Failed executions: {phase_results['failed_executions']}",
                    ]
                )

            elif phase_name == "quality" and "chart_statistics" in phase_results:
                chart_stats = phase_results["chart_statistics"]
                report_lines.extend(
                    [
                        f"- Notebooks with charts: {chart_stats['notebooks_with_charts']}",
                        f"- Total charts: {chart_stats['total_charts']}",
                        f"- Interactive charts: {chart_stats['interactive_charts']}",
                        f"- Interactivity rate: {phase_results['interactivity_rate']:.1%}",
                    ]
                )

            elif phase_name == "performance" and "avg_execution_time" in phase_results:
                report_lines.extend(
                    [
                        f"- Average execution time: {phase_results['avg_execution_time']:.1f}s",
                        f"- Performance grade: {phase_results.get('performance_grade', 'N / A')}",
                    ]
                )

            report_lines.append("")

        # Add recommendations
        if results["summary"]["recommendations"]:
            report_lines.extend(
                ["## Recommendations", *[f"- {rec}" for rec in results["summary"]["recommendations"]], ""]
            )

        # Write report
        report_path = Path("test_reports / comprehensive_notebook_integration_report.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))

        print(f"📋 Comprehensive integration report: {report_path}")

        return str(report_path)


# Standalone execution
if __name__ == "__main__":
    tester = ComprehensiveNotebookIntegrationTester()

    print("🚀 Running comprehensive notebook integration test suite...")
    results = tester.run_full_notebook_test_suite()

    print(f"\n🏁 Final Result: {results['overall_status']}")

    if results["overall_status"] == "FAILED":
        sys.exit(1)
    else:
        sys.exit(0)
