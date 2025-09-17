#!/usr/bin/env python3
"""
Notebook Execution Validator

Executes notebooks and validates their outputs for errors.
FAILS LOUDLY when issues are detected like missing ISRC data or chart errors.
"""

import json
import logging
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NotebookExecutionValidator:
    """
    Professional notebook execution and validation system.

    Features:
    - Executes notebooks using nbconvert/jupyter
    - Validates cell outputs for error patterns
    - FAILS LOUDLY when issues are detected
    - Integrates with archiving system
    """

    def __init__(self):
        """Initialize the notebook execution validator."""
        self.error_patterns = [
            r"❌ Not Available",
            r"🚨 CRITICAL ERROR",
            r"CRITICAL FAILURE:",
            r"🚨 FIX YOUR DATABASE",
            r"Chart returned None",
            r"Database connection failed",
            r"🚨.*ERROR",
            r"FAILED.*charts?",
            r"No fake data ever.*FAILED",
            r"KeyError:",
            r"ConnectionError:",
            r"DatabaseError:",
        ]

        self.success_patterns = [
            r"✅.*SUCCESS",
            r"✅ REAL DATA",
            r"Charts: \d+/\d+ \(100% success\)",
            r"✅ All systems operational",
            r"📊.*generated successfully",
            r"MISSION ACCOMPLISHED",
        ]

        logger.info("📊 NotebookExecutionValidator initialized")
        logger.info(f"   🔍 Monitoring {len(self.error_patterns)} error patterns")
        logger.info(f"   ✅ Recognizing {len(self.success_patterns)} success patterns")

    def execute_notebook(self, notebook_path: Path, timeout: int = 300) -> Path:
        """
        Execute a notebook using nbconvert.

        Args:
            notebook_path: Path to the notebook to execute
            timeout: Execution timeout in seconds

        Returns:
            Path to the executed notebook

        Raises:
            RuntimeError: If execution fails
        """
        if not notebook_path.exists():
            raise FileNotFoundError(f"Notebook not found: {notebook_path}")

        # Create executed filename
        executed_name = notebook_path.stem + "_executed.ipynb"
        executed_path = notebook_path.parent / executed_name

        logger.info(f"🚀 Executing notebook: {notebook_path}")
        logger.info(f"   📄 Output: {executed_path}")
        logger.info(f"   ⏱️ Timeout: {timeout}s")

        try:
            # Execute notebook using nbconvert
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
                f"--ExecutePreprocessor.timeout={timeout}",
            ]

            logger.info(f"🔧 Running command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout + 30  # Extra buffer for nbconvert overhead
            )

            if result.returncode != 0:
                logger.error(f"🚨 Notebook execution failed!")
                logger.error(f"   📄 Notebook: {notebook_path}")
                logger.error(f"   💥 Return code: {result.returncode}")
                logger.error(f"   📝 STDOUT: {result.stdout}")
                logger.error(f"   🚨 STDERR: {result.stderr}")
                raise RuntimeError(f"Notebook execution failed: {result.stderr}")

            logger.info(f"✅ Notebook executed successfully: {executed_path}")
            return executed_path

        except subprocess.TimeoutExpired:
            logger.error(f"🚨 Notebook execution timed out after {timeout}s")
            raise RuntimeError(f"Notebook execution timed out after {timeout}s")
        except Exception as e:
            logger.error(f"🚨 Notebook execution error: {e}")
            raise RuntimeError(f"Notebook execution error: {e}")

    def detect_error_patterns(self, text_lines: List[str]) -> List[str]:
        """
        Detect error patterns in text lines.

        Args:
            text_lines: List of text lines to check

        Returns:
            List of detected errors
        """
        errors = []

        for line in text_lines:
            for pattern in self.error_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    errors.append(f"Error pattern detected: '{pattern}' in line: '{line.strip()}'")
                    logger.warning(f"🚨 Error pattern detected: {pattern}")
                    logger.warning(f"   📝 Line: {line.strip()}")

        return errors

    def validate_notebook_outputs(self, notebook_path: Path) -> Dict[str, Any]:
        """
        Validate notebook outputs for error patterns.

        Args:
            notebook_path: Path to the executed notebook

        Returns:
            Dictionary with validation results
        """
        if not notebook_path.exists():
            return {
                "success": False,
                "errors": [f"Notebook not found: {notebook_path}"],
                "warnings": [],
                "summary": "Notebook file not found",
            }

        logger.info(f"🔍 Validating notebook outputs: {notebook_path}")

        try:
            # Load notebook
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)

            errors = []
            warnings = []
            success_indicators = []

            # Check each cell's outputs
            for i, cell in enumerate(notebook.get("cells", [])):
                if cell.get("cell_type") == "code":
                    outputs = cell.get("outputs", [])

                    for output in outputs:
                        # Check stdout/stderr text
                        if output.get("output_type") == "stream":
                            text_lines = output.get("text", [])
                            if isinstance(text_lines, str):
                                text_lines = [text_lines]

                            # Detect errors
                            cell_errors = self.detect_error_patterns(text_lines)
                            for error in cell_errors:
                                errors.append(f"Cell {i+1}: {error}")

                            # Detect success indicators
                            for line in text_lines:
                                for pattern in self.success_patterns:
                                    if re.search(pattern, line, re.IGNORECASE):
                                        success_indicators.append(f"Cell {i+1}: {line.strip()}")

                        # Check error outputs
                        elif output.get("output_type") == "error":
                            error_name = output.get("ename", "Unknown")
                            error_value = output.get("evalue", "Unknown error")
                            errors.append(f"Cell {i+1}: {error_name}: {error_value}")
                            logger.error(f"🚨 Cell {i+1} error: {error_name}: {error_value}")

            # Determine overall success
            success = len(errors) == 0

            # Create summary
            if success:
                summary = f"✅ Validation passed: {len(success_indicators)} success indicators, 0 errors"
            else:
                summary = f"🚨 Validation failed: {len(errors)} errors detected"

            logger.info(f"📊 Validation complete: {summary}")

            return {
                "success": success,
                "errors": errors,
                "warnings": warnings,
                "success_indicators": success_indicators,
                "summary": summary,
                "total_cells": len(notebook.get("cells", [])),
                "error_count": len(errors),
            }

        except Exception as e:
            error_msg = f"Failed to validate notebook: {e}"
            logger.error(f"🚨 {error_msg}")
            return {
                "success": False,
                "errors": [error_msg],
                "warnings": [],
                "summary": "Validation failed due to exception",
            }

    def execute_and_validate_workflow(
        self, notebook_filename: str, notebook_content: Dict[str, Any], timeout: int = 300
    ) -> Dict[str, Any]:
        """
        Complete workflow: Create, archive, execute, and validate notebook.

        Args:
            notebook_filename: Base filename for the notebook
            notebook_content: Notebook content dictionary
            timeout: Execution timeout in seconds

        Returns:
            Dictionary with workflow results
        """
        from notebook_archiver import NotebookArchiver

        logger.info(f"🔄 Starting complete workflow for: {notebook_filename}")

        try:
            # Step 1: Archive and create new notebook
            notebooks_dir = Path("notebooks")
            archiver = NotebookArchiver(notebooks_dir)

            notebook_path = archiver.archive_and_create_new(notebook_filename, notebook_content)
            logger.info(f"📝 Created notebook: {notebook_path}")

            # Step 2: Execute notebook
            executed_path = self.execute_notebook(notebook_path, timeout)
            logger.info(f"🚀 Executed notebook: {executed_path}")

            # Step 3: Validate outputs
            validation_result = self.validate_notebook_outputs(executed_path)

            # Step 4: FAIL LOUDLY if validation fails
            if not validation_result["success"]:
                logger.error(f"🚨 WORKFLOW FAILED - VALIDATION ERRORS DETECTED!")
                logger.error(f"   📄 Notebook: {executed_path}")
                logger.error(f"   🚨 Errors: {len(validation_result['errors'])}")
                for error in validation_result["errors"]:
                    logger.error(f"      - {error}")

                # FAIL LOUDLY
                raise RuntimeError(
                    f"🚨 NOTEBOOK VALIDATION FAILED! "
                    f"Detected {len(validation_result['errors'])} errors in executed notebook. "
                    f"FIX YOUR DATA AND CHARTS!"
                )

            logger.info(f"✅ Workflow completed successfully!")
            logger.info(f"   📊 {validation_result['summary']}")

            return {
                "success": True,
                "notebook_path": notebook_path,
                "executed_path": executed_path,
                "validation_result": validation_result,
                "validation_errors": validation_result["errors"],
                "summary": f"✅ Complete workflow successful: {validation_result['summary']}",
            }

        except Exception as e:
            error_msg = f"🚨 WORKFLOW FAILED: {e}"
            logger.error(error_msg)

            return {"success": False, "error": str(e), "validation_errors": [str(e)], "summary": error_msg}

    def get_validation_summary(self, notebook_path: Path) -> str:
        """
        Get a human-readable validation summary.

        Args:
            notebook_path: Path to the notebook to summarize

        Returns:
            Formatted summary string
        """
        validation_result = self.validate_notebook_outputs(notebook_path)

        summary = f"""
🎯 Notebook Validation Summary
{'='*50}
📄 Notebook: {notebook_path.name}
📊 Total Cells: {validation_result.get('total_cells', 'Unknown')}
✅ Success: {validation_result['success']}
🚨 Errors: {len(validation_result['errors'])}
⚠️  Warnings: {len(validation_result.get('warnings', []))}

{validation_result['summary']}
"""

        if validation_result["errors"]:
            summary += "\n🚨 ERRORS DETECTED:\n"
            for error in validation_result["errors"]:
                summary += f"   - {error}\n"

        if validation_result.get("success_indicators"):
            summary += "\n✅ SUCCESS INDICATORS:\n"
            for indicator in validation_result["success_indicators"][:5]:  # Show first 5
                summary += f"   - {indicator}\n"

        return summary


def main():
    """Example usage of the NotebookExecutionValidator."""
    import tempfile

    # Example usage
    with tempfile.TemporaryDirectory() as temp_dir:
        os.chdir(temp_dir)

        validator = NotebookExecutionValidator()

        # Create sample notebook
        sample_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "print('🎵 MusicScope™ Professional Dashboard')\\n",
                        "print('✅ REAL DATA ONLY - No fake data ever')\\n",
                        "print('📊 Charts: 20/20 (100% success with REAL data)')\\n",
                    ],
                }
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Execute complete workflow
        result = validator.execute_and_validate_workflow("MusicScope™_Test_Dashboard.ipynb", sample_notebook)

        print(f"✅ Workflow result: {result['success']}")
        print(f"📊 Summary: {result['summary']}")


if __name__ == "__main__":
    main()
