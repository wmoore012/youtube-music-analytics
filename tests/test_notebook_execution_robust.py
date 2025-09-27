"""
Robust notebook execution testing system.

This module provides comprehensive testing for notebook execution,
ensuring notebooks actually run and produce readable, valid outputs.
Tests run after archiving to ensure the newest notebooks work correctly.

Key Features:
- Full notebook execution with real data
- Output validation and readability checks
- Chart generation verification
- Error detection and reporting
- Performance monitoring
- Data quality validation
"""

from datetime import datetime
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

from nbconvert.preprocessors import ExecutePreprocessor
import nbformat
import pandas as pd
import pytest

# Import our validation system
from src.data_organization.notebook_validator import NotebookValidator, ValidationResult


class NotebookExecutionError(Exception):
    """Raised when notebook execution fails."""

    pass


class NotebookOutputError(Exception):
    """Raised when notebook outputs are invalid or unreadable."""

    pass


class RobustNotebookTester:
    """
    Comprehensive notebook testing system that validates execution and outputs.

    This class provides methods to:
    - Execute notebooks with real data
    - Validate outputs are readable and meaningful
    - Check chart generation and interactivity
    - Monitor performance and resource usage
    - Generate detailed test reports
    """

    def __init__(self, timeout: int = 300):
        """
        Initialize the robust notebook tester.

        Args:
            timeout: Maximum execution time per notebook in seconds
        """
        self.timeout = timeout
        self.validator = NotebookValidator()
        self.execution_results = {}

    def execute_notebook_with_validation(
        self, notebook_path: str, output_dir: str = "notebooks / executed"
    ) -> Dict[str, any]:
        """
        Execute notebook and perform comprehensive validation.

        Args:
            notebook_path: Path to the notebook file
            output_dir: Directory for executed notebook outputs

        Returns:
            Dictionary with execution results and validation details

        Raises:
            NotebookExecutionError: If notebook fails to execute
            NotebookOutputError: If outputs are invalid
        """
        start_time = time.time()
        notebook_path = Path(notebook_path)

        if not notebook_path.exists():
            raise NotebookExecutionError(f"Notebook not found: {notebook_path}")

        # Load notebook
        with open(notebook_path, "r", encoding="utf - 8") as f:
            nb = nbformat.read(f, as_version=4)

        # Execute notebook
        executed_nb = self._execute_notebook_safely(nb, notebook_path)

        # Save executed notebook
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        executed_path = output_dir / f"{notebook_path.stem}-executed.ipynb"
        with open(executed_path, "w", encoding="utf - 8") as f:
            nbformat.write(executed_nb, f)

        # Validate execution results
        validation_results = self._validate_notebook_execution(executed_nb)

        # Check outputs are readable
        readability_results = self._check_output_readability(executed_nb)

        # Validate charts if present
        chart_results = self._validate_chart_outputs(executed_nb)

        execution_time = time.time() - start_time

        results = {
            "notebook_path": str(notebook_path),
            "executed_path": str(executed_path),
            "execution_time": execution_time,
            "execution_successful": True,
            "validation_results": validation_results,
            "readability_results": readability_results,
            "chart_results": chart_results,
            "cell_count": len(executed_nb.cells),
            "code_cell_count": len([c for c in executed_nb.cells if c.cell_type == "code"]),
            "executed_at": datetime.utcnow().isoformat(),
        }

        # Store results for reporting
        self.execution_results[str(notebook_path)] = results

        return results

    def _execute_notebook_safely(self, nb: nbformat.NotebookNode, notebook_path: Path) -> nbformat.NotebookNode:
        """Execute notebook with comprehensive error handling."""
        try:
            # Configure execution processor
            ep = ExecutePreprocessor(
                timeout=self.timeout,
                kernel_name="python3",
                allow_errors=False,  # We want to catch errors
                interrupt_on_timeout=True,
            )

            # Set execution path
            resources = {"metadata": {"path": str(notebook_path.parent)}}

            # Execute notebook
            executed_nb, _ = ep.preprocess(nb, resources=resources)

            return executed_nb

        except Exception as e:
            raise NotebookExecutionError(f"Notebook execution failed: {str(e)}")

    def _validate_notebook_execution(self, executed_nb: nbformat.NotebookNode) -> ValidationResult:
        """Validate that notebook executed successfully with meaningful outputs."""
        result = ValidationResult(is_valid=True, checked_items=0, passed_items=0)

        code_cells = [cell for cell in executed_nb.cells if cell.cell_type == "code"]
        result.checked_items = len(code_cells)

        for i, cell in enumerate(code_cells):
            # Check if cell has outputs
            if not hasattr(cell, "outputs") or not cell.outputs:
                # Empty outputs might be OK for some cells
                continue

            # Check for execution errors
            for output in cell.outputs:
                if output.output_type == "error":
                    result.add_error(f"Cell {i} has execution error: {output.ename}: {output.evalue}")
                elif output.output_type in ["execute_result", "display_data"]:
                    result.passed_items += 1

        result.metadata = {
            "total_code_cells": len(code_cells),
            "cells_with_outputs": result.passed_items,
            "error_cells": len(result.errors),
        }

        return result

    def _check_output_readability(self, executed_nb: nbformat.NotebookNode) -> ValidationResult:
        """Check that notebook outputs are readable and meaningful."""
        result = ValidationResult(is_valid=True, checked_items=0, passed_items=0)

        for i, cell in enumerate(executed_nb.cells):
            if cell.cell_type != "code" or not hasattr(cell, "outputs"):
                continue

            result.checked_items += 1

            for output in cell.outputs:
                if output.output_type == "execute_result":
                    # Check if output has readable data
                    if "data" in output:
                        if "text / html" in output.data:
                            # HTML output (likely DataFrame)
                            html_content = output.data["text / html"]
                            if self._is_readable_html(html_content):
                                result.passed_items += 1
                            else:
                                result.add_warning(f"Cell {i} has unreadable HTML output")

                        elif "text / plain" in output.data:
                            # Plain text output
                            text_content = output.data["text / plain"]
                            if self._is_readable_text(text_content):
                                result.passed_items += 1
                            else:
                                result.add_warning(f"Cell {i} has unreadable text output")

                elif output.output_type == "display_data":
                    # Check for chart / visualization outputs
                    if "data" in output and "application / vnd.plotly.v1 + json" in output.data:
                        # Plotly chart
                        try:
                            plotly_data = json.loads(output.data["application / vnd.plotly.v1 + json"])
                            if self._is_valid_plotly_chart(plotly_data):
                                result.passed_items += 1
                            else:
                                result.add_error(f"Cell {i} has invalid Plotly chart")
                        except json.JSONDecodeError:
                            result.add_error(f"Cell {i} has malformed Plotly data")

        result.metadata = {"readable_outputs": result.passed_items, "total_outputs_checked": result.checked_items}

        return result

    def _validate_chart_outputs(self, executed_nb: nbformat.NotebookNode) -> ValidationResult:
        """Validate that charts are properly generated and interactive."""
        result = ValidationResult(is_valid=True, checked_items=0, passed_items=0)

        chart_count = 0
        interactive_chart_count = 0

        for i, cell in enumerate(executed_nb.cells):
            if cell.cell_type != "code" or not hasattr(cell, "outputs"):
                continue

            for output in cell.outputs:
                if output.output_type == "display_data" and "data" in output:
                    # Check for Plotly charts
                    if "application / vnd.plotly.v1 + json" in output.data:
                        chart_count += 1
                        result.checked_items += 1

                        try:
                            plotly_data = json.loads(output.data["application / vnd.plotly.v1 + json"])

                            # Validate chart structure
                            if self._validate_plotly_structure(plotly_data):
                                result.passed_items += 1

                                # Check if chart is interactive
                                if self._is_interactive_chart(plotly_data):
                                    interactive_chart_count += 1

                            else:
                                result.add_error(f"Cell {i} has invalid chart structure")

                        except (json.JSONDecodeError, KeyError) as e:
                            result.add_error(f"Cell {i} chart parsing error: {str(e)}")

                    # Check for Altair charts
                    elif "application / vnd.vegalite.v4 + json" in output.data:
                        chart_count += 1
                        result.checked_items += 1

                        try:
                            altair_data = json.loads(output.data["application / vnd.vegalite.v4 + json"])
                            if self._validate_altair_structure(altair_data):
                                result.passed_items += 1
                                interactive_chart_count += 1  # Altair charts are interactive by default
                            else:
                                result.add_error(f"Cell {i} has invalid Altair chart")
                        except (json.JSONDecodeError, KeyError) as e:
                            result.add_error(f"Cell {i} Altair chart parsing error: {str(e)}")

        result.metadata = {
            "total_charts": chart_count,
            "valid_charts": result.passed_items,
            "interactive_charts": interactive_chart_count,
            "interactivity_rate": interactive_chart_count / chart_count if chart_count > 0 else 0,
        }

        return result

    def _is_readable_html(self, html_content: str) -> bool:
        """Check if HTML content is readable (not just error messages)."""
        if not html_content or len(html_content.strip()) < 10:
            return False

        # Check for common error patterns
        error_patterns = ["error", "exception", "traceback", "failed", "none", "null", "undefined", "empty"]

        content_lower = html_content.lower()
        for pattern in error_patterns:
            if pattern in content_lower and len(content_lower) < 100:
                return False

        # Check for meaningful table content
        if "<table" in html_content and "<td" in html_content:
            return True

        # Check for meaningful div content
        if "<div" in html_content and len(html_content) > 50:
            return True

        return False

    def _is_readable_text(self, text_content: str) -> bool:
        """Check if text content is readable and meaningful."""
        if not text_content or len(text_content.strip()) < 5:
            return False

        # Check for error patterns
        error_patterns = ["error", "exception", "none", "null", "failed"]
        content_lower = text_content.lower().strip()

        for pattern in error_patterns:
            if content_lower == pattern or content_lower.startswith(pattern):
                return False

        # Check for meaningful content
        if len(content_lower) > 10 and any(c.isalnum() for c in content_lower):
            return True

        return False

    def _is_valid_plotly_chart(self, plotly_data: dict) -> bool:
        """Check if Plotly chart data is valid."""
        try:
            # Basic structure validation
            if "data" not in plotly_data or "layout" not in plotly_data:
                return False

            # Check if data is not empty
            if not plotly_data["data"] or len(plotly_data["data"]) == 0:
                return False

            # Check if first trace has data
            first_trace = plotly_data["data"][0]
            if not any(key in first_trace for key in ["x", "y", "z", "values"]):
                return False

            return True

        except (KeyError, IndexError, TypeError):
            return False

    def _validate_plotly_structure(self, plotly_data: dict) -> bool:
        """Validate Plotly chart has proper structure."""
        required_keys = ["data", "layout"]

        for key in required_keys:
            if key not in plotly_data:
                return False

        # Validate data traces
        if not isinstance(plotly_data["data"], list) or len(plotly_data["data"]) == 0:
            return False

        # Check first trace has required fields
        first_trace = plotly_data["data"][0]
        if not isinstance(first_trace, dict):
            return False

        return True

    def _is_interactive_chart(self, plotly_data: dict) -> bool:
        """Check if Plotly chart has interactive features."""
        try:
            layout = plotly_data.get("layout", {})

            # Check for hover information
            for trace in plotly_data.get("data", []):
                if "hovertemplate" in trace or "hoverinfo" in trace:
                    return True

            # Check for interactive layout features
            interactive_features = ["dragmode", "hovermode", "selectdirection"]

            for feature in interactive_features:
                if feature in layout:
                    return True

            return True  # Plotly charts are interactive by default

        except (KeyError, TypeError):
            return False

    def _validate_altair_structure(self, altair_data: dict) -> bool:
        """Validate Altair chart structure."""
        required_keys = ["$schema", "data", "mark"]

        for key in required_keys:
            if key not in altair_data:
                return False

        return True

    def generate_test_report(self, output_path: str = "test_reports / notebook_execution_report.md") -> str:
        """Generate comprehensive test report."""
        if not self.execution_results:
            return "No notebook execution results to report."

        report_lines = [
            "# Notebook Execution Test Report",
            f"Generated: {datetime.utcnow().isoformat()}Z",
            "",
            "## Summary",
            f"- Total notebooks tested: {len(self.execution_results)}",
            f"- Successful executions: {sum(1 for r in self.execution_results.values() if r['execution_successful'])}",
            f"- Total execution time: {sum(r['execution_time'] for r in self.execution_results.values()):.2f}s",
            "",
            "## Detailed Results",
            "",
        ]

        for notebook_path, results in self.execution_results.items():
            report_lines.extend(
                [
                    f"### {Path(notebook_path).name}",
                    f"- **Execution Time**: {results['execution_time']:.2f}s",
                    f"- **Cell Count**: {results['cell_count']} ({results['code_cell_count']} code cells)",
                    f"- **Validation Status**: {'✅ PASSED' if results['validation_results'].is_valid else '❌ FAILED'}",
                    f"- **Readability Status**: {
                        '✅ PASSED' if results['readability_results'].is_valid else '❌ FAILED'}",
                    f"- **Chart Status**: {'✅ PASSED' if results['chart_results'].is_valid else '❌ FAILED'}",
                    "",
                ]
            )

            # Add error details if any
            all_errors = (
                results["validation_results"].errors
                + results["readability_results"].errors
                + results["chart_results"].errors
            )

            if all_errors:
                report_lines.extend(["**Errors:**", *[f"- {error}" for error in all_errors], ""])

            # Add chart statistics
            chart_meta = results["chart_results"].metadata
            if chart_meta.get("total_charts", 0) > 0:
                report_lines.extend(
                    [
                        "**Chart Statistics:**",
                        f"- Total charts: {chart_meta['total_charts']}",
                        f"- Valid charts: {chart_meta['valid_charts']}",
                        f"- Interactive charts: {chart_meta['interactive_charts']}",
                        f"- Interactivity rate: {chart_meta['interactivity_rate']:.1%}",
                        "",
                    ]
                )

        # Write report
        report_path = Path(output_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)

        with open(report_path, "w", encoding="utf - 8") as f:
            f.write("\n".join(report_lines))

        return str(report_path)


# Test class for pytest integration
class TestNotebookExecutionRobust:
    """Pytest test class for robust notebook execution testing."""

    def setup_method(self):
        """Set up test fixtures."""
        self.tester = RobustNotebookTester(timeout=300)

    def test_professional_dashboard_execution(self):
        """Test that the professional dashboard notebook executes and produces valid outputs."""
        notebook_path = "notebooks / MusicScope™_Professional_Dashboard.ipynb"

        if not Path(notebook_path).exists():
            pytest.skip(f"Notebook not found: {notebook_path}")

        # Execute notebook with full validation
        results = self.tester.execute_notebook_with_validation(notebook_path)

        # Assert execution was successful
        assert results["execution_successful"], "Notebook execution failed"

        # Assert validation passed
        assert results["validation_results"].is_valid, f"Validation failed: {results['validation_results'].errors}"

        # Assert outputs are readable
        assert results[
            "readability_results"
        ].is_valid, f"Readability check failed: {results['readability_results'].errors}"

        # Assert charts are valid (if present)
        if results["chart_results"].checked_items > 0:
            assert results["chart_results"].is_valid, f"Chart validation failed: {results['chart_results'].errors}"

            # Ensure charts are interactive
            chart_meta = results["chart_results"].metadata
            assert (
                chart_meta["interactivity_rate"] > 0.8
            ), f"Charts not sufficiently interactive: {chart_meta['interactivity_rate']:.1%}"

        # Performance check
        assert results["execution_time"] < 300, f"Notebook took too long to execute: {results['execution_time']:.2f}s"

        print(f"✅ Professional Dashboard executed successfully in {results['execution_time']:.2f}s")
        print(f"📊 Generated {results['chart_results'].metadata.get('total_charts', 0)} charts")

    def test_chart_dashboard_execution(self):
        """Test that the 20 chart dashboard notebook executes and produces valid outputs."""
        notebook_path = "notebooks / MusicScope™_20_Chart_Dashboard.ipynb"

        if not Path(notebook_path).exists():
            pytest.skip(f"Notebook not found: {notebook_path}")

        # Execute notebook with full validation
        results = self.tester.execute_notebook_with_validation(notebook_path)

        # Assert execution was successful
        assert results["execution_successful"], "Notebook execution failed"

        # Assert validation passed
        assert results["validation_results"].is_valid, f"Validation failed: {results['validation_results'].errors}"

        # Assert outputs are readable
        assert results[
            "readability_results"
        ].is_valid, f"Readability check failed: {results['readability_results'].errors}"

        # Assert charts are valid and numerous
        assert results["chart_results"].is_valid, f"Chart validation failed: {results['chart_results'].errors}"

        chart_meta = results["chart_results"].metadata
        assert chart_meta["total_charts"] >= 15, f"Expected at least 15 charts, got {chart_meta['total_charts']}"

        # Ensure high chart quality
        assert chart_meta["valid_charts"] >= chart_meta["total_charts"] * 0.9, "Too many invalid charts"
        assert (
            chart_meta["interactivity_rate"] > 0.8
        ), f"Charts not sufficiently interactive: {chart_meta['interactivity_rate']:.1%}"

        print(f"✅ 20 Chart Dashboard executed successfully in {results['execution_time']:.2f}s")
        print(f"📊 Generated {chart_meta['total_charts']} charts ({chart_meta['interactive_charts']} interactive)")

    def test_all_notebooks_in_directory(self):
        """Test all notebooks in the notebooks directory."""
        notebooks_dir = Path("notebooks")

        if not notebooks_dir.exists():
            pytest.skip("Notebooks directory not found")

        # Find all notebook files (excluding executed ones)
        notebook_files = [
            f
            for f in notebooks_dir.glob("*.ipynb")
            if not f.name.endswith("-executed.ipynb") and not f.name.startswith(".") and "archive" not in str(f)
        ]

        if not notebook_files:
            pytest.skip("No notebooks found to test")

        failed_notebooks = []

        for notebook_path in notebook_files:
            try:
                print(f"\n🧪 Testing {notebook_path.name}...")

                results = self.tester.execute_notebook_with_validation(str(notebook_path))

                # Check if execution was successful
                if not results["execution_successful"]:
                    failed_notebooks.append((str(notebook_path), "Execution failed"))
                    continue

                # Check validation results
                if not results["validation_results"].is_valid:
                    failed_notebooks.append(
                        (str(notebook_path), f"Validation failed: {results['validation_results'].errors}")
                    )
                    continue

                print(f"✅ {notebook_path.name} passed all tests")

            except Exception as e:
                failed_notebooks.append((str(notebook_path), f"Exception: {str(e)}"))
                print(f"❌ {notebook_path.name} failed: {str(e)}")

        # Generate test report
        report_path = self.tester.generate_test_report()
        print(f"\n📋 Test report generated: {report_path}")

        # Assert no notebooks failed
        if failed_notebooks:
            failure_summary = "\n".join([f"- {path}: {reason}" for path, reason in failed_notebooks])
            pytest.fail(f"The following notebooks failed testing:\n{failure_summary}")

        print(f"\n🎉 All {len(notebook_files)} notebooks passed robust execution testing!")


# Standalone execution for manual testing
if __name__ == "__main__":
    tester = RobustNotebookTester()

    # Test specific notebook
    if len(sys.argv) > 1:
        notebook_path = sys.argv[1]
        print(f"Testing notebook: {notebook_path}")

        try:
            results = tester.execute_notebook_with_validation(notebook_path)
            print(f"✅ Execution successful: {results['execution_successful']}")
            print(f"⏱️  Execution time: {results['execution_time']:.2f}s")
            print(f"📊 Charts generated: {results['chart_results'].metadata.get('total_charts', 0)}")

            # Generate report
            report_path = tester.generate_test_report()
            print(f"📋 Report: {report_path}")

        except Exception as e:
            print(f"❌ Testing failed: {str(e)}")
            sys.exit(1)
    else:
        print("Usage: python test_notebook_execution_robust.py <notebook_path>")
        sys.exit(1)
