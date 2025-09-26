#!/usr/bin/env python3
"""
Execute and Validate Notebook System

1. Creates MusicScope™ notebook with real data
2. Archives old version to datetime folder
3. Executes the notebook (like pressing play)
4. Validates outputs for errors
5. FAILS LOUDLY if issues found
"""

import sys

sys.path.insert(0, ".")

from datetime import datetime
import json
import logging
from pathlib import Path
import re
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NotebookExecutionSystem:
    """Complete system for notebook execution and validation."""

    def __init__(self):
        """Initialize the execution system."""
        self.notebooks_dir = Path("notebooks")
        self.error_patterns = [
            r"❌.*Not Available",
            r"🚨 CRITICAL ERROR",
            r"CRITICAL FAILURE",
            r"🚨.*ERROR",
            r"Chart.*returned None",
            r"Database connection failed",
            r"KeyError:",
            r"ConnectionError:",
            r"Traceback \(most recent call last\):",
        ]

        self.success_patterns = [
            r"✅.*SUCCESS",
            r"✅ REAL DATA",
            r"Charts: \d+/\d+ \(100% success\)",
            r"📊.*generated successfully",
            r"✅.*charts generated: 20/20",
        ]

        logger.info("🚀 NotebookExecutionSystem initialized")

    def create_notebook(self) -> Path:
        """Create MusicScope™ notebook with archiving."""
        logger.info("📝 Creating MusicScope™ notebook...")

        # Use existing create_notebook.py
        result = subprocess.run([sys.executable, "create_notebook.py"], capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"Notebook creation failed: {result.stderr}")

        # Find the created notebook
        notebook_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard.ipynb"
        if not notebook_path.exists():
            raise RuntimeError("Created notebook not found!")

        logger.info(f"✅ Notebook created: {notebook_path}")
        return notebook_path

    def execute_notebook(self, notebook_path: Path) -> Path:
        """Execute notebook using nbconvert."""
        logger.info(f"🚀 Executing notebook: {notebook_path}")

        # Create executed filename
        executed_name = notebook_path.stem + "_executed.ipynb"
        executed_path = notebook_path.parent / executed_name

        try:
            # Execute using nbconvert
            cmd = [
                sys.executable,
                "-m",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--output",
                str(executed_path),
                str(notebook_path),
                "--ExecutePreprocessor.timeout=600",
            ]

            logger.info(f"🔧 Running: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=700)

            if result.returncode != 0:
                logger.error(f"🚨 Execution failed: {result.stderr}")
                raise RuntimeError(f"Notebook execution failed: {result.stderr}")

            logger.info(f"✅ Notebook executed: {executed_path}")
            return executed_path

        except subprocess.TimeoutExpired:
            raise RuntimeError("Notebook execution timed out")
        except Exception as e:
            raise RuntimeError(f"Execution error: {e}")

    def validate_notebook_outputs(self, executed_path: Path) -> dict:
        """Validate executed notebook outputs for errors."""
        logger.info(f"🔍 Validating outputs: {executed_path}")

        if not executed_path.exists():
            return {
                "success": False,
                "errors": [f"Executed notebook not found: {executed_path}"],
                "summary": "File not found",
            }

        try:
            with open(executed_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)

            errors = []
            success_indicators = []

            # Check each cell's outputs
            for i, cell in enumerate(notebook.get("cells", [])):
                if cell.get("cell_type") == "code":
                    outputs = cell.get("outputs", [])

                    for output in outputs:
                        # Check stdout text
                        if output.get("output_type") == "stream":
                            text_lines = output.get("text", [])
                            if isinstance(text_lines, str):
                                text_lines = [text_lines]

                            for line in text_lines:
                                # Check for errors
                                for pattern in self.error_patterns:
                                    if re.search(pattern, line, re.IGNORECASE):
                                        errors.append(f"Cell {i+1}: {line.strip()}")
                                        logger.warning(f"🚨 Error in cell {i+1}: {line.strip()}")

                                # Check for success
                                for pattern in self.success_patterns:
                                    if re.search(pattern, line, re.IGNORECASE):
                                        success_indicators.append(f"Cell {i+1}: {line.strip()}")

                        # Check error outputs
                        elif output.get("output_type") == "error":
                            error_name = output.get("ename", "Unknown")
                            error_value = output.get("evalue", "Unknown error")
                            errors.append(f"Cell {i+1}: {error_name}: {error_value}")
                            logger.error(f"🚨 Cell {i+1} error: {error_name}: {error_value}")

            # Determine success
            success = len(errors) == 0 and len(success_indicators) > 0

            result = {
                "success": success,
                "errors": errors,
                "success_indicators": success_indicators,
                "error_count": len(errors),
                "success_count": len(success_indicators),
                "summary": f"{'✅ PASSED' if success else '🚨 FAILED'}: {len(errors)} errors, {len(success_indicators)} success indicators",
            }

            logger.info(f"📊 Validation result: {result['summary']}")
            return result

        except Exception as e:
            error_msg = f"Validation failed: {e}"
            logger.error(f"🚨 {error_msg}")
            return {"success": False, "errors": [error_msg], "summary": "Validation exception"}

    def complete_workflow(self) -> dict:
        """Execute complete workflow: create, execute, validate."""
        logger.info("🎯 Starting Complete Notebook Workflow")
        logger.info("=" * 60)

        try:
            # Step 1: Create notebook
            notebook_path = self.create_notebook()

            # Step 2: Execute notebook
            executed_path = self.execute_notebook(notebook_path)

            # Step 3: Validate outputs
            validation_result = self.validate_notebook_outputs(executed_path)

            # Step 4: FAIL LOUDLY if validation fails
            if not validation_result["success"]:
                logger.error("🚨 WORKFLOW FAILED - VALIDATION ERRORS DETECTED!")
                logger.error(f"   📄 Executed notebook: {executed_path}")
                logger.error(f"   🚨 Errors: {validation_result['error_count']}")

                for error in validation_result["errors"][:5]:  # Show first 5 errors
                    logger.error(f"      - {error}")

                # FAIL LOUDLY
                raise RuntimeError(
                    f"🚨 NOTEBOOK VALIDATION FAILED LOUDLY! "
                    f"Found {validation_result['error_count']} errors in executed notebook. "
                    f"Issues detected: {', '.join(validation_result['errors'][:3])}... "
                    f"FIX YOUR DATA AND CHARTS!"
                )

            logger.info("🎉 COMPLETE WORKFLOW SUCCESS!")
            logger.info(f"   📄 Notebook: {notebook_path.name}")
            logger.info(f"   🚀 Executed: {executed_path.name}")
            logger.info(f"   ✅ Validation: PASSED")
            logger.info(f"   📊 Success indicators: {validation_result['success_count']}")

            return {
                "success": True,
                "notebook_path": notebook_path,
                "executed_path": executed_path,
                "validation_result": validation_result,
                "summary": f"✅ Complete workflow successful: {validation_result['summary']}",
            }

        except Exception as e:
            error_msg = f"🚨 COMPLETE WORKFLOW FAILED: {e}"
            logger.error(error_msg)

            return {"success": False, "error": str(e), "summary": error_msg}


def main():
    """Execute the complete workflow."""

    try:
        system = NotebookExecutionSystem()
        result = system.complete_workflow()

        print(f"\n" + "=" * 70)
        print(f"🎯 COMPLETE WORKFLOW RESULTS")
        print(f"=" * 70)
        print(f"✅ Success: {result['success']}")

        if result["success"]:
            print(f"📄 Notebook: {result['notebook_path'].name}")
            print(f"🚀 Executed: {result['executed_path'].name}")
            print(f"📊 Validation: {result['validation_result']['summary']}")
            print(f"✅ Success Indicators: {result['validation_result']['success_count']}")
            print(f"🚨 Errors: {result['validation_result']['error_count']}")

            if result["validation_result"]["success_indicators"]:
                print(f"\n🎉 SUCCESS INDICATORS:")
                for indicator in result["validation_result"]["success_indicators"][:5]:
                    print(f"   - {indicator}")
        else:
            print(f"🚨 Error: {result.get('error', 'Unknown error')}")

        print(f"\n🎵 MusicScope™ Professional Analytics System")
        print(f"🚀 {'OPERATIONAL' if result['success'] else 'NEEDS ATTENTION'}")

        return result["success"]

    except Exception as e:
        print(f"\n🚨 CRITICAL SYSTEM ERROR: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
