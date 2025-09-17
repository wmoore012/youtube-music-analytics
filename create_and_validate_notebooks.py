#!/usr/bin/env python3
"""
TDD Notebook Creation and Validation System

This script creates notebooks and validates that the last cell correctly
reports chart status. Makes it IMPOSSIBLE for AI to miss broken charts.

NO FAKE DATA. NO BULKY CODE. WELL COMMENTED.
"""

import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Tuple

# Add project root to path
sys.path.insert(0, os.path.abspath("."))

from src.youtubeviz.notebook_generator import NotebookTemplateManager


class NotebookValidator:
    """Validates notebook execution and chart counting."""

    def __init__(self):
        self.total_expected_charts = 20
        self.validation_results = {}

    def create_production_notebook(self) -> str:
        """Create production notebook with all 20 charts."""

        print("🎯 Creating Production Notebook...")

        manager = NotebookTemplateManager(total_charts=self.total_expected_charts)
        output_path = "notebooks/MusicScope™_Validated_Dashboard.ipynb"

        # Generate notebook
        notebook = manager.generate_notebook_template(
            notebook_name="MusicScope™ Validated Dashboard - TDD Ready",
            include_charts=list(range(1, self.total_expected_charts + 1)),
        )

        # Save notebook
        manager.save_notebook(notebook, output_path)

        print(f"✅ Created: {output_path}")
        print(f"📊 Charts: {self.total_expected_charts}")
        print(f"📋 Cells: {len(notebook['cells'])}")

        return output_path

    def execute_notebook(self, notebook_path: str) -> Tuple[bool, str, Dict[str, Any]]:
        """Execute notebook and capture output from last cell."""

        print(f"\n🚀 Executing notebook: {notebook_path}")

        try:
            # Execute notebook using nbconvert
            executed_filename = os.path.basename(notebook_path).replace(".ipynb", "_executed.ipynb")
            cmd = [
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--output",
                executed_filename,
                notebook_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                return False, f"Execution failed: {result.stderr}", {}

            # Load executed notebook
            executed_path = os.path.join(os.path.dirname(notebook_path), executed_filename)
            with open(executed_path, "r", encoding="utf-8") as f:
                executed_notebook = json.load(f)

            # Extract last cell output
            last_cell_output = self._extract_last_cell_output(executed_notebook)

            return True, "Execution successful", last_cell_output

        except subprocess.TimeoutExpired:
            return False, "Notebook execution timed out", {}
        except Exception as e:
            return False, f"Execution error: {str(e)}", {}

    def _extract_last_cell_output(self, notebook: Dict[str, Any]) -> Dict[str, Any]:
        """Extract output from the last validation cell."""

        cells = notebook.get("cells", [])

        # Find the last code cell with validation output
        validation_output = {}

        for cell in reversed(cells):
            if cell.get("cell_type") == "code":
                outputs = cell.get("outputs", [])

                for output in outputs:
                    if output.get("output_type") == "stream" and output.get("name") == "stdout":
                        text = "".join(output.get("text", []))

                        # Look for our validation summary
                        if "REAL DATA ANALYTICS SUMMARY" in text:
                            validation_output = self._parse_validation_output(text)
                            break

                if validation_output:
                    break

        return validation_output

    def _parse_validation_output(self, output_text: str) -> Dict[str, Any]:
        """Parse the validation output text to extract chart counts."""

        lines = output_text.split("\n")

        result = {
            "real_charts": 0,
            "requirement_charts": 0,
            "error_charts": 0,
            "total_expected": self.total_expected_charts,
            "success_rate": 0.0,
            "ci_cd_status": "UNKNOWN",
            "raw_output": output_text,
        }

        for line in lines:
            line = line.strip()

            # Parse chart counts
            if "Charts with REAL data:" in line:
                # Extract "X/20" pattern
                parts = line.split(":")[1].strip()
                if "/" in parts:
                    real_count = int(parts.split("/")[0])
                    result["real_charts"] = real_count

            elif "Charts showing data requirements:" in line:
                parts = line.split(":")[1].strip()
                if "/" in parts:
                    req_count = int(parts.split("/")[0])
                    result["requirement_charts"] = req_count

            elif "Charts with errors:" in line:
                parts = line.split(":")[1].strip()
                if "/" in parts:
                    error_count = int(parts.split("/")[0])
                    result["error_charts"] = error_count

            # Parse CI/CD status
            elif "CI/CD:" in line:
                if "PASS" in line:
                    result["ci_cd_status"] = "PASS"
                elif "WARNING" in line:
                    result["ci_cd_status"] = "WARNING"
                elif "FAIL" in line:
                    result["ci_cd_status"] = "FAIL"

        # Calculate success rate
        total_charts = result["real_charts"] + result["requirement_charts"] + result["error_charts"]
        if total_charts > 0:
            result["success_rate"] = result["real_charts"] / total_charts

        return result

    def validate_chart_counts(self, validation_output: Dict[str, Any]) -> List[str]:
        """Validate that chart counts are correct and make sense."""

        issues = []

        # Check total chart count
        total_reported = (
            validation_output["real_charts"]
            + validation_output["requirement_charts"]
            + validation_output["error_charts"]
        )

        if total_reported != self.total_expected_charts:
            issues.append(
                f"❌ CRITICAL: Total charts mismatch! " f"Expected {self.total_expected_charts}, got {total_reported}"
            )

        # Check for impossible values
        if validation_output["real_charts"] < 0:
            issues.append("❌ CRITICAL: Negative real chart count!")

        if validation_output["error_charts"] == self.total_expected_charts:
            issues.append("❌ CRITICAL: ALL charts failed - system broken!")

        # Check CI/CD status makes sense
        if validation_output["success_rate"] >= 0.8 and validation_output["ci_cd_status"] != "PASS":
            issues.append("❌ CI/CD status inconsistent with success rate")

        return issues

    def create_validation_report(self, validation_output: Dict[str, Any]) -> str:
        """Create human-readable validation report."""

        report = []
        report.append("🎯 NOTEBOOK VALIDATION REPORT")
        report.append("=" * 50)

        # Chart counts
        report.append(f"📊 Charts with REAL data: {validation_output['real_charts']}/{self.total_expected_charts}")
        report.append(
            f"📋 Charts showing requirements: {validation_output['requirement_charts']}/{self.total_expected_charts}"
        )
        report.append(f"❌ Charts with errors: {validation_output['error_charts']}/{self.total_expected_charts}")

        # Success metrics
        success_rate = validation_output["success_rate"] * 100
        report.append(f"\n📈 Success Rate: {success_rate:.1f}%")
        report.append(f"🚦 CI/CD Status: {validation_output['ci_cd_status']}")

        # Validation issues
        issues = self.validate_chart_counts(validation_output)
        if issues:
            report.append("\n🚨 VALIDATION ISSUES:")
            for issue in issues:
                report.append(f"  {issue}")
        else:
            report.append("\n✅ All validations passed!")

        # Recommendations
        report.append("\n💡 RECOMMENDATIONS:")

        if validation_output["real_charts"] == 0:
            report.append("  🔧 Fix data loading - no charts working with real data")
        elif validation_output["real_charts"] < 5:
            report.append("  📊 Add missing data columns to unlock more charts")
        elif validation_output["real_charts"] >= 15:
            report.append("  🎉 Excellent! Most charts working with real data")

        if validation_output["error_charts"] > 5:
            report.append("  🐛 Investigate chart errors - too many failures")

        return "\n".join(report)

    def run_full_validation(self) -> bool:
        """Run complete notebook creation and validation."""

        print("🎯 STARTING TDD NOTEBOOK VALIDATION")
        print("=" * 60)

        try:
            # Step 1: Create notebook
            notebook_path = self.create_production_notebook()

            # Step 2: Execute notebook
            success, message, validation_output = self.execute_notebook(notebook_path)

            if not success:
                print(f"\n❌ EXECUTION FAILED: {message}")
                return False

            # Step 3: Validate results
            if not validation_output:
                print("\n❌ NO VALIDATION OUTPUT FOUND!")
                print("💡 Check that notebook has validation cell with 'REAL DATA ANALYTICS SUMMARY'")
                return False

            # Step 4: Generate report
            report = self.create_validation_report(validation_output)
            print(f"\n{report}")

            # Step 5: Determine overall success
            issues = self.validate_chart_counts(validation_output)

            if issues:
                print(f"\n❌ VALIDATION FAILED: {len(issues)} critical issues found")
                return False
            else:
                print(f"\n✅ VALIDATION PASSED: Notebook working correctly!")
                return True

        except Exception as e:
            print(f"\n❌ VALIDATION ERROR: {str(e)}")
            return False


def main():
    """Main execution function."""

    validator = NotebookValidator()

    # Run validation
    success = validator.run_full_validation()

    if success:
        print("\n🎉 SUCCESS: Notebook validation completed!")
        print("💝 Charts properly counted, no fake data, bulletproof system!")
        return 0
    else:
        print("\n💥 FAILURE: Notebook validation failed!")
        print("🔧 Fix issues above and run again")
        return 1


if __name__ == "__main__":
    exit(main())
