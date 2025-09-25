#!/usr/bin/env python3
"""
Test-Driven Development for Notebook Execution Validation

Tests for executing notebooks and validating their outputs for errors.
"""

from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

# Add current directory to path for imports
sys.path.insert(0, ".")


class TestNotebookExecutionValidation(unittest.TestCase):
    """Test suite for notebook execution and validation."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create basic directory structure
        os.makedirs("notebooks", exist_ok=True)

        # Create a mock .env file
        with open(".env", "w") as f:
            f.write(
                """
DB_HOST=localhost
DB_USER=test_user
DB_PASSWORD=test_pass
DB_NAME=test_db
"""
            )

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)

    def test_notebook_execution_validator_creation(self):
        """Test that NotebookExecutionValidator can be created."""
        from notebook_execution_validator import NotebookExecutionValidator

        validator = NotebookExecutionValidator()
        self.assertIsNotNone(validator)

    def test_execute_notebook_with_success(self):
        """Test executing a notebook that should succeed."""
        from notebook_execution_validator import NotebookExecutionValidator

        # Create a simple successful notebook
        successful_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["print('✅ Test successful')\\nresult = 1 + 1\\nprint(f'Result: {result}')"],
                }
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Save notebook
        notebook_path = Path("notebooks/test_success.ipynb")
        with open(notebook_path, "w") as f:
            json.dump(successful_notebook, f)

        validator = NotebookExecutionValidator()

        # Execute notebook
        executed_path = validator.execute_notebook(notebook_path)

        # Should return path to executed notebook
        self.assertTrue(executed_path.exists())
        self.assertIn("executed", str(executed_path))

    def test_validate_notebook_outputs_success(self):
        """Test validating notebook outputs for success indicators."""
        from notebook_execution_validator import NotebookExecutionValidator

        # Create notebook with successful outputs
        successful_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["✅ REAL DATA DISCOVERED\\n", "📊 Charts: 20/20 (100% success)\\n"],
                        }
                    ],
                    "source": ["print('✅ REAL DATA DISCOVERED')\\nprint('📊 Charts: 20/20 (100% success)')"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        notebook_path = Path("notebooks/test_validation.ipynb")
        with open(notebook_path, "w") as f:
            json.dump(successful_notebook, f)

        validator = NotebookExecutionValidator()

        # Validate outputs
        validation_result = validator.validate_notebook_outputs(notebook_path)

        # Should pass validation
        self.assertTrue(validation_result["success"])
        self.assertEqual(validation_result["errors"], [])

    def test_validate_notebook_outputs_failure_isrc(self):
        """Test validating notebook outputs that show ISRC data missing."""
        from notebook_execution_validator import NotebookExecutionValidator

        # Create notebook with ISRC failure
        failing_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["✅ REAL DATA DISCOVERED\\n", "- **ISRC Data:** ❌ Not Available\\n"],
                        }
                    ],
                    "source": ["print('✅ REAL DATA DISCOVERED')\\nprint('- **ISRC Data:** ❌ Not Available')"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        notebook_path = Path("notebooks/test_isrc_fail.ipynb")
        with open(notebook_path, "w") as f:
            json.dump(failing_notebook, f)

        validator = NotebookExecutionValidator()

        # Validate outputs
        validation_result = validator.validate_notebook_outputs(notebook_path)

        # Should fail validation
        self.assertFalse(validation_result["success"])
        self.assertGreater(len(validation_result["errors"]), 0)
        self.assertIn("ISRC", str(validation_result["errors"]))

    def test_validate_notebook_outputs_failure_chart_error(self):
        """Test validating notebook outputs that show chart errors."""
        from notebook_execution_validator import NotebookExecutionValidator

        # Create notebook with chart error
        failing_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["🚨 CRITICAL ERROR in Chart 20: KeyError('missing_column')\\n"],
                        }
                    ],
                    "source": ["print('🚨 CRITICAL ERROR in Chart 20: KeyError(\\'missing_column\\')')"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        notebook_path = Path("notebooks/test_chart_fail.ipynb")
        with open(notebook_path, "w") as f:
            json.dump(failing_notebook, f)

        validator = NotebookExecutionValidator()

        # Validate outputs
        validation_result = validator.validate_notebook_outputs(notebook_path)

        # Should fail validation
        self.assertFalse(validation_result["success"])
        self.assertGreater(len(validation_result["errors"]), 0)
        self.assertIn("CRITICAL ERROR", str(validation_result["errors"]))

    def test_complete_workflow_success(self):
        """Test the complete workflow: create, archive, execute, validate."""
        from notebook_archiver import NotebookArchiver
        from notebook_execution_validator import NotebookExecutionValidator

        # Create archiver and validator
        archiver = NotebookArchiver(Path("notebooks"))
        validator = NotebookExecutionValidator()

        # Create successful notebook content
        successful_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "print('🎵 MusicScope™ Professional Dashboard Initialized!')\\n",
                        "print('✅ REAL DATA ONLY - No fake data ever')\\n",
                        "print('📊 Charts: 20/20 (100% success with REAL data)')\\n",
                        "print('✅ ISRC data: True')\\n",
                    ],
                }
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Execute complete workflow
        result = validator.execute_and_validate_workflow(
            "MusicScope™_Professional_Dashboard.ipynb", successful_notebook
        )

        # Should succeed
        self.assertTrue(result["success"])
        self.assertTrue(result["executed_path"].exists())
        self.assertEqual(result["validation_errors"], [])

    def test_complete_workflow_failure(self):
        """Test the complete workflow with a failing notebook."""
        from notebook_execution_validator import NotebookExecutionValidator

        validator = NotebookExecutionValidator()

        # Create failing notebook content
        failing_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "print('🎵 MusicScope™ Professional Dashboard Initialized!')\\n",
                        "print('- **ISRC Data:** ❌ Not Available')\\n",
                        "print('🚨 CRITICAL ERROR in Chart 15: Database connection failed')\\n",
                    ],
                }
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Execute complete workflow
        result = validator.execute_and_validate_workflow("MusicScope™_Professional_Dashboard.ipynb", failing_notebook)

        # Should fail
        self.assertFalse(result["success"])
        self.assertGreater(len(result["validation_errors"]), 0)

    def test_error_pattern_detection(self):
        """Test that error patterns are correctly detected."""
        from notebook_execution_validator import NotebookExecutionValidator

        validator = NotebookExecutionValidator()

        # Test various error patterns
        error_patterns = [
            "❌ Not Available",
            "🚨 CRITICAL ERROR in Chart",
            "CRITICAL FAILURE:",
            "🚨 FIX YOUR DATABASE",
            "Chart returned None",
            "Database connection failed",
        ]

        success_patterns = [
            "✅ SUCCESS: Chart generated",
            "✅ REAL DATA DISCOVERED",
            "📊 Charts: 20/20 (100% success)",
            "✅ All systems operational",
        ]

        # Test error detection
        for pattern in error_patterns:
            errors = validator.detect_error_patterns([pattern])
            self.assertGreater(len(errors), 0, f"Should detect error in: {pattern}")

        # Test success patterns don't trigger errors
        for pattern in success_patterns:
            errors = validator.detect_error_patterns([pattern])
            self.assertEqual(len(errors), 0, f"Should not detect error in: {pattern}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
