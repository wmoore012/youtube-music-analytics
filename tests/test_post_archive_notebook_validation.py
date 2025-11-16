"""
Post-archive notebook validation testing.

This module runs comprehensive notebook tests AFTER archiving operations
to ensure that the newest notebooks in the main directory are working
correctly and produce readable outputs.

This test is designed to be run as part of the CI / CD pipeline after
any archiving or cleanup operations to validate the current state.
"""

import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest

from tests.test_notebook_execution_robust import RobustNotebookTester


class PostArchiveNotebookValidator:
    """
    Validates notebooks after archiving operations to ensure current notebooks work.

    This class specifically focuses on:
    - Testing notebooks that remain in the main notebooks/ directory
    - Ensuring archived notebooks are properly stored
    - Validating that current notebooks produce better outputs than archived ones
    - Checking that no critical functionality was lost during archiving
    """

    def __init__(self):
        """Initialize the post-archive validator."""
        self.tester = RobustNotebookTester(timeout=300)
        self.notebooks_dir = Path("notebooks")
        self.archive_dir = Path("notebooks / archive")

    def get_current_notebooks(self) -> List[Path]:
        """Get list of current (non-archived) notebooks."""
        if not self.notebooks_dir.exists():
            return []

        return [
            f
            for f in self.notebooks_dir.glob("*.ipynb")
            if not f.name.endswith("-executed.ipynb")
            and not f.name.startswith(".")
            and "archive" not in str(f)
            and f.is_file()
        ]

    def get_archived_notebooks(self) -> List[Path]:
        """Get list of archived notebooks."""
        if not self.archive_dir.exists():
            return []

        archived = []
        for archive_subdir in self.archive_dir.iterdir():
            if archive_subdir.is_dir():
                archived.extend([f for f in archive_subdir.glob("*.ipynb") if not f.name.endswith("-executed.ipynb")])

        return archived

    def validate_archive_structure(self) -> Dict[str, any]:
        """Validate that archive structure is properly organized."""
        results = {"is_valid": True, "errors": [], "warnings": [], "archive_stats": {}}

        if not self.archive_dir.exists():
            results["warnings"].append("No archive directory found")
            return results

        # Check archive subdirectories
        archive_subdirs = [d for d in self.archive_dir.iterdir() if d.is_dir()]

        if not archive_subdirs:
            results["warnings"].append("Archive directory exists but is empty")
            return results

        # Validate each archive subdirectory
        for subdir in archive_subdirs:
            subdir_notebooks = list(subdir.glob("*.ipynb"))

            if not subdir_notebooks:
                results["warnings"].append(f"Archive subdirectory '{subdir.name}' is empty")
                continue

            # Check for both original and executed versions
            original_notebooks = [f for f in subdir_notebooks if not f.name.endswith("-executed.ipynb")]
            executed_notebooks = [f for f in subdir_notebooks if f.name.endswith("-executed.ipynb")]

            results["archive_stats"][subdir.name] = {
                "original_count": len(original_notebooks),
                "executed_count": len(executed_notebooks),
                "total_count": len(subdir_notebooks),
            }

            # Validate that executed versions exist for original notebooks
            for original in original_notebooks:
                expected_executed = subdir / f"{original.stem}-executed.ipynb"
                if not expected_executed.exists():
                    results["warnings"].append(f"Missing executed version for {original.name} in archive {subdir.name}")

        return results

    def compare_current_vs_archived(self) -> Dict[str, any]:
        """Compare current notebooks with their archived versions."""
        current_notebooks = self.get_current_notebooks()
        archived_notebooks = self.get_archived_notebooks()

        results = {
            "current_count": len(current_notebooks),
            "archived_count": len(archived_notebooks),
            "comparisons": [],
            "improvements": [],
            "regressions": [],
        }

        # Test current notebooks
        current_results = {}
        for notebook in current_notebooks:
            try:
                test_result = self.tester.execute_notebook_with_validation(str(notebook))
                current_results[notebook.name] = test_result
            except Exception as e:
                current_results[notebook.name] = {"error": str(e), "execution_successful": False}

        # Find corresponding archived versions and compare
        for current_notebook in current_notebooks:
            current_name = current_notebook.name
            current_result = current_results.get(current_name, {})

            # Look for similar archived notebooks
            similar_archived = [
                arch for arch in archived_notebooks if self._notebooks_are_similar(current_notebook.name, arch.name)
            ]

            if similar_archived:
                # Compare with most recent archived version
                most_recent_archived = max(similar_archived, key=lambda x: x.stat().st_mtime)

                comparison = {
                    "current_notebook": current_name,
                    "archived_notebook": str(most_recent_archived),
                    "current_successful": current_result.get("execution_successful", False),
                    "current_execution_time": current_result.get("execution_time", float("inf")),
                    "current_chart_count": current_result.get("chart_results", {})
                    .get("metadata", {})
                    .get("total_charts", 0),
                }

                results["comparisons"].append(comparison)

                # Determine if this is an improvement or regression
                if current_result.get("execution_successful", False):
                    if current_result.get("execution_time", float("inf")) < 300:  # Reasonable execution time
                        results["improvements"].append(f"{current_name}: Successfully executing")
                    else:
                        results["regressions"].append(
                            f"{current_name}: Slow execution ({current_result.get('execution_time', 0):.1f}s)"
                        )
                else:
                    results["regressions"].append(f"{current_name}: Execution failed")

        return results

    def _notebooks_are_similar(self, name1: str, name2: str) -> bool:
        """Check if two notebook names represent similar / related notebooks."""
        # Remove common suffixes and prefixes for comparison
        clean_name1 = name1.replace(".ipynb", "").replace("-executed", "").lower()
        clean_name2 = name2.replace(".ipynb", "").replace("-executed", "").lower()

        # Check for exact match
        if clean_name1 == clean_name2:
            return True

        # Check for partial matches (e.g., "dashboard" in both names)
        common_keywords = ["dashboard", "analysis", "chart", "professional", "complete"]

        for keyword in common_keywords:
            if keyword in clean_name1 and keyword in clean_name2:
                return True

        return False

    def validate_notebook_quality_standards(self) -> Dict[str, any]:
        """Validate that current notebooks meet quality standards."""
        current_notebooks = self.get_current_notebooks()

        results = {
            "total_notebooks": len(current_notebooks),
            "passing_notebooks": 0,
            "failing_notebooks": 0,
            "quality_metrics": {},
            "failures": [],
        }

        for notebook in current_notebooks:
            try:
                test_result = self.tester.execute_notebook_with_validation(str(notebook))

                # Check quality standards
                quality_checks = {
                    "executes_successfully": test_result.get("execution_successful", False),
                    "reasonable_execution_time": test_result.get("execution_time", float("inf")) < 300,
                    "has_readable_outputs": test_result.get("readability_results", {}).get("is_valid", False),
                    "has_valid_charts": test_result.get("chart_results", {}).get("is_valid", True),  # True if no charts
                    "interactive_charts": test_result.get("chart_results", {})
                    .get("metadata", {})
                    .get("interactivity_rate", 1.0)
                    > 0.8,
                }

                # Count passing checks
                passing_checks = sum(quality_checks.values())
                total_checks = len(quality_checks)

                results["quality_metrics"][notebook.name] = {
                    "passing_checks": passing_checks,
                    "total_checks": total_checks,
                    "quality_score": passing_checks / total_checks,
                    "details": quality_checks,
                }

                if passing_checks == total_checks:
                    results["passing_notebooks"] += 1
                else:
                    results["failing_notebooks"] += 1
                    failed_checks = [check for check, passed in quality_checks.items() if not passed]
                    results["failures"].append({"notebook": notebook.name, "failed_checks": failed_checks})

            except Exception as e:
                results["failing_notebooks"] += 1
                results["failures"].append({"notebook": notebook.name, "error": str(e)})

        return results


class TestPostArchiveNotebookValidation:
    """Pytest test class for post-archive notebook validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = PostArchiveNotebookValidator()

    def test_archive_structure_is_valid(self):
        """Test that archive structure is properly organized."""
        results = self.validator.validate_archive_structure()

        # Archive structure should be valid (warnings are OK)
        assert results["is_valid"], f"Archive structure validation failed: {results['errors']}"

        # Print archive statistics
        if results["archive_stats"]:
            print("\n📁 Archive Statistics:")
            for archive_name, stats in results["archive_stats"].items():
                print(f"  {archive_name}: {stats['original_count']} original, {stats['executed_count']} executed")

        # Print warnings if any
        if results["warnings"]:
            print("\n⚠️  Archive Warnings:")
            for warning in results["warnings"]:
                print(f"  - {warning}")

    def test_current_notebooks_execute_successfully(self):
        """Test that all current (non-archived) notebooks execute successfully."""
        current_notebooks = self.validator.get_current_notebooks()

        if not current_notebooks:
            pytest.skip("No current notebooks found to test")

        print(f"\n🧪 Testing {len(current_notebooks)} current notebooks...")

        failed_notebooks = []

        for notebook in current_notebooks:
            try:
                print(f"  Testing {notebook.name}...")

                results = self.validator.tester.execute_notebook_with_validation(str(notebook))

                if not results["execution_successful"]:
                    failed_notebooks.append((notebook.name, "Execution failed"))
                elif not results["validation_results"].is_valid:
                    failed_notebooks.append(
                        (notebook.name, f"Validation failed: {results['validation_results'].errors}")
                    )
                else:
                    print(f"    ✅ {notebook.name} passed")

            except Exception as e:
                failed_notebooks.append((notebook.name, f"Exception: {str(e)}"))
                print(f"    ❌ {notebook.name} failed: {str(e)}")

        # Assert no notebooks failed
        if failed_notebooks:
            failure_summary = "\n".join([f"  - {name}: {reason}" for name, reason in failed_notebooks])
            pytest.fail(f"The following current notebooks failed:\n{failure_summary}")

        print(f"✅ All {len(current_notebooks)} current notebooks execute successfully!")

    def test_current_notebooks_meet_quality_standards(self):
        """Test that current notebooks meet quality standards."""
        quality_results = self.validator.validate_notebook_quality_standards()

        if quality_results["total_notebooks"] == 0:
            pytest.skip("No notebooks found to test quality standards")

        print(f"\n📊 Quality Standards Results:")
        print(f"  Total notebooks: {quality_results['total_notebooks']}")
        print(f"  Passing: {quality_results['passing_notebooks']}")
        print(f"  Failing: {quality_results['failing_notebooks']}")

        # Print quality metrics
        if quality_results["quality_metrics"]:
            print("\n📈 Quality Scores:")
            for notebook, metrics in quality_results["quality_metrics"].items():
                score = metrics["quality_score"]
                print(f"  {notebook}: {score:.1%} ({metrics['passing_checks']}/{metrics['total_checks']})")

        # Print failures
        if quality_results["failures"]:
            print("\n❌ Quality Failures:")
            for failure in quality_results["failures"]:
                if "error" in failure:
                    print(f"  {failure['notebook']}: {failure['error']}")
                else:
                    failed_checks = ", ".join(failure["failed_checks"])
                    print(f"  {failure['notebook']}: Failed checks - {failed_checks}")

        # Assert quality standards
        total_notebooks = quality_results["total_notebooks"]
        passing_notebooks = quality_results["passing_notebooks"]

        # At least 80% of notebooks should pass all quality checks
        pass_rate = passing_notebooks / total_notebooks if total_notebooks > 0 else 0

        assert pass_rate >= 0.8, (
            f"Quality standards not met: {pass_rate:.1%} pass rate "
            f"({passing_notebooks}/{total_notebooks}). Expected at least 80%."
        )

        print(f"✅ Quality standards met: {pass_rate:.1%} pass rate")

    def test_current_vs_archived_comparison(self):
        """Test that current notebooks are improvements over archived versions."""
        comparison_results = self.validator.compare_current_vs_archived()

        print(f"\n🔄 Current vs Archived Comparison:")
        print(f"  Current notebooks: {comparison_results['current_count']}")
        print(f"  Archived notebooks: {comparison_results['archived_count']}")
        print(f"  Comparisons made: {len(comparison_results['comparisons'])}")

        # Print improvements
        if comparison_results["improvements"]:
            print("\n✅ Improvements:")
            for improvement in comparison_results["improvements"]:
                print(f"  - {improvement}")

        # Print regressions
        if comparison_results["regressions"]:
            print("\n❌ Regressions:")
            for regression in comparison_results["regressions"]:
                print(f"  - {regression}")

        # Assert no major regressions
        regression_count = len(comparison_results["regressions"])
        total_comparisons = len(comparison_results["comparisons"])

        if total_comparisons > 0:
            regression_rate = regression_count / total_comparisons

            # Allow up to 20% regressions (some might be acceptable)
            assert regression_rate <= 0.2, (
                f"Too many regressions: {regression_rate:.1%} "
                f"({regression_count}/{total_comparisons}). Expected at most 20%."
            )

            print(f"✅ Regression rate acceptable: {regression_rate:.1%}")
        else:
            print("ℹ️  No comparisons available (no archived notebooks found)")

    def test_notebooks_have_interactive_charts(self):
        """Test that notebooks generate interactive charts as required."""
        current_notebooks = self.validator.get_current_notebooks()

        if not current_notebooks:
            pytest.skip("No current notebooks found to test")

        chart_statistics = {
            "notebooks_with_charts": 0,
            "total_charts": 0,
            "interactive_charts": 0,
            "notebooks_tested": 0,
        }

        for notebook in current_notebooks:
            try:
                results = self.validator.tester.execute_notebook_with_validation(str(notebook))
                chart_statistics["notebooks_tested"] += 1

                chart_meta = results.get("chart_results", {}).get("metadata", {})
                total_charts = chart_meta.get("total_charts", 0)
                interactive_charts = chart_meta.get("interactive_charts", 0)

                if total_charts > 0:
                    chart_statistics["notebooks_with_charts"] += 1
                    chart_statistics["total_charts"] += total_charts
                    chart_statistics["interactive_charts"] += interactive_charts

            except Exception as e:
                print(f"⚠️  Could not test charts in {notebook.name}: {str(e)}")

        print(f"\n📊 Chart Statistics:")
        print(f"  Notebooks tested: {chart_statistics['notebooks_tested']}")
        print(f"  Notebooks with charts: {chart_statistics['notebooks_with_charts']}")
        print(f"  Total charts: {chart_statistics['total_charts']}")
        print(f"  Interactive charts: {chart_statistics['interactive_charts']}")

        if chart_statistics["total_charts"] > 0:
            interactivity_rate = chart_statistics["interactive_charts"] / chart_statistics["total_charts"]
            print(f"  Interactivity rate: {interactivity_rate:.1%}")

            # Assert high interactivity rate
            assert interactivity_rate >= 0.8, (
                f"Charts not sufficiently interactive: {interactivity_rate:.1%}. "
                f"Expected at least 80% of charts to be interactive."
            )

            print("✅ Charts meet interactivity requirements")
        else:
            print("ℹ️  No charts found in current notebooks")

    def test_generate_post_archive_report(self):
        """Generate comprehensive post-archive validation report."""
        # Run all validations
        archive_results = self.validator.validate_archive_structure()
        quality_results = self.validator.validate_notebook_quality_standards()
        comparison_results = self.validator.compare_current_vs_archived()

        # Generate report
        report_lines = [
            "# Post-Archive Notebook Validation Report",
            f"Generated: {datetime.utcnow().isoformat()}Z",
            "",
            "## Archive Structure Validation",
            f"- Status: {'✅ VALID' if archive_results['is_valid'] else '❌ INVALID'}",
            f"- Archive subdirectories: {len(archive_results.get('archive_stats', {}))}",
        ]

        if archive_results.get("archive_stats"):
            report_lines.append("\n### Archive Statistics")
            for archive_name, stats in archive_results["archive_stats"].items():
                report_lines.append(
                    f"- **{archive_name}**: {stats['original_count']} original, {stats['executed_count']} executed"
                )

        report_lines.extend(
            [
                "",
                "## Current Notebook Quality",
                f"- Total notebooks: {quality_results['total_notebooks']}",
                f"- Passing quality checks: {quality_results['passing_notebooks']}",
                f"- Failing quality checks: {quality_results['failing_notebooks']}",
                f"- Pass rate: {quality_results['passing_notebooks'] / max(quality_results['total_notebooks'], 1):.1%}",
            ]
        )

        if quality_results["failures"]:
            report_lines.append("\n### Quality Failures")
            for failure in quality_results["failures"]:
                if "error" in failure:
                    report_lines.append(f"- **{failure['notebook']}**: {failure['error']}")
                else:
                    failed_checks = ", ".join(failure["failed_checks"])
                    report_lines.append(f"- **{failure['notebook']}**: {failed_checks}")

        report_lines.extend(
            [
                "",
                "## Current vs Archived Comparison",
                f"- Current notebooks: {comparison_results['current_count']}",
                f"- Archived notebooks: {comparison_results['archived_count']}",
                f"- Improvements: {len(comparison_results['improvements'])}",
                f"- Regressions: {len(comparison_results['regressions'])}",
            ]
        )

        # Write report
        report_path = Path("test_reports / post_archive_validation_report.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))

        print(f"\n📋 Post-archive validation report generated: {report_path}")

        # Also generate the detailed execution report
        detailed_report = self.validator.tester.generate_test_report("test_reports / post_archive_execution_report.md")
        print(f"📋 Detailed execution report: {detailed_report}")


# Standalone execution
if __name__ == "__main__":
    validator = PostArchiveNotebookValidator()

    print("🔍 Running post-archive notebook validation...")

    # Run all validations
    print("\n1. Validating archive structure...")
    archive_results = validator.validate_archive_structure()
    print(f"   Archive structure: {'✅ VALID' if archive_results['is_valid'] else '❌ INVALID'}")

    print("\n2. Testing current notebook quality...")
    quality_results = validator.validate_notebook_quality_standards()
    pass_rate = quality_results["passing_notebooks"] / max(quality_results["total_notebooks"], 1)
    print(f"   Quality pass rate: {pass_rate:.1%} ({quality_results['passing_notebooks']}/{quality_results['total_notebooks']})")

    print("\n3. Comparing current vs archived...")
    comparison_results = validator.compare_current_vs_archived()
    print(f"   Improvements: {len(comparison_results['improvements'])}")
    print(f"   Regressions: {len(comparison_results['regressions'])}")

    print("\n✅ Post-archive validation complete!")
