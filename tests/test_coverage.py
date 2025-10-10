#!/usr / bin / env python3
"""
Test Coverage Analysis and Reporting

This module provides comprehensive test coverage analysis for the ETL system:
- Code coverage measurement and reporting
- Coverage threshold enforcement
- Missing coverage identification
- Coverage improvement recommendations
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pytest


class CoverageAnalyzer:
    """Analyzes and reports test coverage for ETL components."""

    def __init__(self, target_coverage: float = 80.0):
        """
        Initialize coverage analyzer.

        Args:
            target_coverage: Target coverage percentage (default 80%)
        """
        self.target_coverage = target_coverage
        self.project_root = Path(__file__).parent.parent

    def run_coverage_analysis(self) -> Dict[str, any]:
        """
        Run comprehensive coverage analysis.

        Returns:
            Dictionary with coverage results and analysis
        """
        print("🔍 Running comprehensive test coverage analysis...")

        # Run tests with coverage
        coverage_data = self._run_tests_with_coverage()

        # Analyze coverage results
        analysis = self._analyze_coverage_results(coverage_data)

        # Generate recommendations
        recommendations = self._generate_recommendations(analysis)

        return {
            "coverage_data": coverage_data,
            "analysis": analysis,
            "recommendations": recommendations,
            "meets_target": analysis.get("overall_coverage", 0) >= self.target_coverage,
        }

    def _run_tests_with_coverage(self) -> Dict[str, any]:
        """Run tests with coverage measurement."""
        try:
            # Install coverage if not available
            subprocess.run([sys.executable, "-m", "pip", "install", "coverage"], capture_output=True, check=False)

            # Run coverage
            cmd = [
                sys.executable,
                "-m",
                "coverage",
                "run",
                "--source=web,src",
                "--omit=*/tests/*,*/test_*,*/__pycache__/*",
                "-m",
                "pytest",
                "tests/",
                "-v",
                "--tb=short",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_root)

            if result.returncode != 0:
                print(f"⚠️ Some tests failed, but continuing with coverage analysis")
                print(f"Test output: {result.stdout}")
                if result.stderr:
                    print(f"Test errors: {result.stderr}")

            # Generate coverage report
            report_cmd = [sys.executable, "-m", "coverage", "report", "--format=text"]
            report_result = subprocess.run(report_cmd, capture_output=True, text=True, cwd=self.project_root)

            # Generate detailed HTML report
            html_cmd = [sys.executable, "-m", "coverage", "html", "-d", "htmlcov"]
            subprocess.run(html_cmd, capture_output=True, text=True, cwd=self.project_root)

            return {
                "test_output": result.stdout,
                "test_errors": result.stderr,
                "test_returncode": result.returncode,
                "coverage_report": report_result.stdout,
                "coverage_errors": report_result.stderr,
                "html_report_generated": True,
            }

        except Exception as e:
            return {"error": str(e), "test_output": "", "coverage_report": "", "html_report_generated": False}

    def _analyze_coverage_results(self, coverage_data: Dict[str, any]) -> Dict[str, any]:
        """Analyze coverage results and extract metrics."""
        analysis = {
            "overall_coverage": 0.0,
            "module_coverage": {},
            "uncovered_lines": {},
            "critical_missing": [],
            "well_covered": [],
        }

        if "coverage_report" not in coverage_data or not coverage_data["coverage_report"]:
            return analysis

        report_lines = coverage_data["coverage_report"].split("\n")

        for line in report_lines:
            line = line.strip()
            if not line or line.startswith("-") or line.startswith("Name"):
                continue

            # Parse coverage line: Name    Stmts   Miss  Cover
            parts = line.split()
            if len(parts) >= 4 and parts[-1].endswith("%"):
                module_name = parts[0]
                try:
                    statements = int(parts[1])
                    missed = int(parts[2])
                    coverage_pct = float(parts[3].rstrip("%"))

                    analysis["module_coverage"][module_name] = {
                        "statements": statements,
                        "missed": missed,
                        "coverage": coverage_pct,
                    }

                    # Categorize modules
                    if coverage_pct >= 90:
                        analysis["well_covered"].append(module_name)
                    elif coverage_pct < 50:
                        analysis["critical_missing"].append(module_name)

                except (ValueError, IndexError):
                    continue

            # Look for total coverage
            if line.startswith("TOTAL"):
                parts = line.split()
                if len(parts) >= 4 and parts[-1].endswith("%"):
                    try:
                        analysis["overall_coverage"] = float(parts[-1].rstrip("%"))
                    except ValueError:
                        pass

        return analysis

    def _generate_recommendations(self, analysis: Dict[str, any]) -> List[str]:
        """Generate coverage improvement recommendations."""
        recommendations = []

        overall_coverage = analysis.get("overall_coverage", 0)
        target = self.target_coverage

        if overall_coverage < target:
            gap = target-overall_coverage
            recommendations.append(
                f"📈 Overall coverage is {overall_coverage:.1f}%, need {gap:.1f}% more to reach {target}% target"
            )

        # Critical missing coverage
        critical_missing = analysis.get("critical_missing", [])
        if critical_missing:
            recommendations.append(f"🚨 Critical: These modules have <50% coverage: {', '.join(critical_missing)}")

        # Module-specific recommendations
        module_coverage = analysis.get("module_coverage", {})
        for module, data in module_coverage.items():
            coverage_pct = data["coverage"]
            missed = data["missed"]

            if coverage_pct < target and missed > 0:
                recommendations.append(
                    f"📝 {module}: Add tests for {missed} uncovered statements ({coverage_pct:.1f}% coverage)"
                )

        # Positive feedback
        well_covered = analysis.get("well_covered", [])
        if well_covered:
            recommendations.append(f"✅ Well covered modules (>90%): {', '.join(well_covered)}")

        # General recommendations
        if overall_coverage < 60:
            recommendations.extend(
                [
                    "🎯 Focus on unit tests for core business logic",
                    "🔧 Add integration tests for database operations",
                    "🧪 Create test fixtures for common scenarios",
                ]
            )
        elif overall_coverage < target:
            recommendations.extend(
                [
                    "🎯 Add edge case testing for existing functions",
                    "🔧 Test error handling and exception paths",
                    "🧪 Add integration tests for component interactions",
                ]
            )

        return recommendations

    def generate_coverage_report(self) -> str:
        """Generate a comprehensive coverage report."""
        results = self.run_coverage_analysis()

        report = []
        report.append("📊 TEST COVERAGE ANALYSIS REPORT")
        report.append("=" * 50)

        # Overall status
        overall_coverage = results["analysis"].get("overall_coverage", 0)
        meets_target = results["meets_target"]

        status_emoji = "✅" if meets_target else "❌"
        report.append(f"{status_emoji} Overall Coverage: {overall_coverage:.1f}%")
        report.append(f"🎯 Target Coverage: {self.target_coverage:.1f}%")

        if meets_target:
            report.append("🎉 Coverage target achieved!")
        else:
            gap = self.target_coverage-overall_coverage
            report.append(f"📈 Need {gap:.1f}% more coverage to reach target")

        # Module breakdown
        module_coverage = results["analysis"].get("module_coverage", {})
        if module_coverage:
            report.append("\n📋 Module Coverage Breakdown:")
            report.append("-" * 40)

            for module, data in sorted(module_coverage.items(), key=lambda x: x[1]["coverage"], reverse=True):
                coverage_pct = data["coverage"]
                missed = data["missed"]
                statements = data["statements"]

                status = "✅" if coverage_pct >= 90 else "⚠️" if coverage_pct >= 70 else "❌"
                report.append(f"{status} {module:<30} {coverage_pct:>6.1f}% ({statements-missed}/{statements} lines)")

        # Recommendations
        recommendations = results["recommendations"]
        if recommendations:
            report.append("\n💡 Recommendations:")
            report.append("-" * 30)
            for rec in recommendations:
                report.append(f"  {rec}")

        # Test results summary
        coverage_data = results["coverage_data"]
        if coverage_data.get("test_returncode") == 0:
            report.append("\n✅ All tests passed")
        else:
            report.append("\n⚠️ Some tests failed-check test output for details")

        # HTML report info
        if coverage_data.get("html_report_generated"):
            report.append("\n📄 Detailed HTML report generated: htmlcov / index.html")

        return "\n".join(report)


def run_coverage_check(target_coverage: float = 80.0) -> bool:
    """
    Run coverage check and return whether target is met.

    Args:
        target_coverage: Target coverage percentage

    Returns:
        True if coverage target is met, False otherwise
    """
    analyzer = CoverageAnalyzer(target_coverage)
    results = analyzer.run_coverage_analysis()

    print(analyzer.generate_coverage_report())

    return results["meets_target"]


def main():
    """Main function for running coverage analysis."""
    import argparse

    parser = argparse.ArgumentParser(description="Run test coverage analysis")
    parser.add_argument("--target", type=float, default=80.0, help="Target coverage percentage (default: 80.0)")
    parser.add_argument("--fail-under", action="store_true", help="Exit with error code if coverage is below target")

    args = parser.parse_args()

    meets_target = run_coverage_check(args.target)

    if args.fail_under and not meets_target:
        print(f"\n❌ Coverage below target ({args.target}%) - exiting with error")
        sys.exit(1)
    elif meets_target:
        print(f"\n✅ Coverage target ({args.target}%) achieved!")
        sys.exit(0)
    else:
        print(f"\n📈 Coverage below target ({args.target}%) but continuing")
        sys.exit(0)


if __name__ == "__main__":
    main()
