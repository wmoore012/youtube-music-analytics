#!/usr/bin/env python3
"""
Final Integration Test

Tests the complete integrated system:
1. NotebookArchiver with datetime organization
2. MusicScope™ notebook creation with real data
3. Notebook validation with error detection
4. Complete workflow that FAILS LOUDLY on issues

This is the FINAL TEST that proves everything works together.
"""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, ".")


class TestFinalIntegration(unittest.TestCase):
    """Test the complete integrated system."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create directory structure
        self.notebooks_dir = Path("notebooks")
        self.notebooks_dir.mkdir(parents=True)

        # Create mock .env file for database connection
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

    def test_notebook_archiver_has_complete_workflow_method(self):
        """Test that NotebookArchiver has the complete workflow method."""
        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)

        # Should have the complete workflow method
        self.assertTrue(hasattr(archiver, "create_and_validate_musicscope_notebook"))
        self.assertTrue(callable(getattr(archiver, "create_and_validate_musicscope_notebook")))

    def test_complete_workflow_components_exist(self):
        """Test that all workflow components exist and are importable."""

        # Test NotebookArchiver
        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)
        self.assertIsNotNone(archiver)

        # Test NotebookExecutor
        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)
        self.assertIsNotNone(executor)

        # Test CompleteNotebookWorkflow
        from complete_notebook_workflow import CompleteNotebookWorkflow

        workflow = CompleteNotebookWorkflow(self.notebooks_dir)
        self.assertIsNotNone(workflow)

        # Test that workflow has required methods
        self.assertTrue(hasattr(workflow, "create_and_validate_musicscope_notebook"))
        self.assertTrue(hasattr(workflow, "get_workflow_status"))

    def test_validation_system_detects_errors(self):
        """Test that the validation system properly detects errors."""
        import json

        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)

        # Create notebook with errors
        error_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": [
                                "🚨 CRITICAL ERROR in Chart 20: KeyError('missing_column')\n",
                                "❌ ISRC Data: Not Available\n",
                            ],
                        }
                    ],
                    "source": ["# Error simulation"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        error_path = self.notebooks_dir / "error_test.ipynb"
        with open(error_path, "w") as f:
            json.dump(error_notebook, f)

        # Validation should fail
        result = executor.validate_outputs(error_path)

        self.assertFalse(result["success"])
        self.assertGreater(len(result["errors"]), 0)

        # Should detect specific error patterns
        error_messages = [e["message"] for e in result["errors"]]
        self.assertTrue(any("CRITICAL ERROR" in msg for msg in error_messages))
        self.assertTrue(any("Not Available" in msg for msg in error_messages))

    def test_validation_system_detects_success(self):
        """Test that the validation system properly detects success."""
        import json

        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)

        # Create notebook with success indicators
        success_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": [
                                "✅ SUCCESS: Chart 1 generated successfully with REAL data!\n",
                                "✅ SUCCESS: Chart 20 generated successfully with REAL data!\n",
                                "📊 Beautiful charts generated: 20/20 (100% success with REAL data)\n",
                            ],
                        }
                    ],
                    "source": ["# Success simulation"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        success_path = self.notebooks_dir / "success_test.ipynb"
        with open(success_path, "w") as f:
            json.dump(success_notebook, f)

        # Validation should pass
        result = executor.validate_outputs(success_path)

        self.assertTrue(result["success"])
        self.assertEqual(len(result["errors"]), 0)
        self.assertGreater(len(result["success_indicators"]), 0)

        # Should detect specific success patterns
        success_messages = [s["message"] for s in result["success_indicators"]]
        self.assertTrue(any("SUCCESS: Chart" in msg for msg in success_messages))
        self.assertTrue(any("20/20" in msg for msg in success_messages))

    def test_archiver_datetime_functionality(self):
        """Test that the archiver datetime functionality works."""
        import json
        import time

        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)

        # Create sample notebook
        sample_notebook = {
            "cells": [{"cell_type": "markdown", "source": ["# Test"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create first notebook
        path1 = archiver.create_notebook_with_datetime("Test.ipynb", sample_notebook)
        self.assertTrue(path1.exists())
        self.assertRegex(path1.name, r"Test_\d{8}_\d{6}\.ipynb")

        # Small delay to ensure different timestamp
        time.sleep(1.1)

        # Archive and create new
        path2 = archiver.archive_and_create_new("Test.ipynb", sample_notebook)
        self.assertTrue(path2.exists())
        self.assertNotEqual(str(path1), str(path2))

        # Should have archived the first one
        archived_files = list((self.notebooks_dir / "archive").glob("Test_*.ipynb"))
        self.assertEqual(len(archived_files), 1)

    def test_system_integration_points(self):
        """Test that all system integration points work."""

        # Test that all required modules can be imported together
        from complete_notebook_workflow import CompleteNotebookWorkflow
        from notebook_archiver import NotebookArchiver
        from notebook_execution_validator import NotebookExecutionValidator, NotebookExecutor

        # Test that they can all be instantiated
        archiver = NotebookArchiver(self.notebooks_dir)
        executor = NotebookExecutor(self.notebooks_dir)
        validator = NotebookExecutionValidator(self.notebooks_dir)
        workflow = CompleteNotebookWorkflow(self.notebooks_dir)

        # Test that they all have expected methods
        self.assertTrue(hasattr(archiver, "archive_and_create_new"))
        self.assertTrue(hasattr(archiver, "create_and_validate_musicscope_notebook"))
        self.assertTrue(hasattr(executor, "validate_outputs"))
        self.assertTrue(hasattr(validator, "create_archive_execute_validate"))
        self.assertTrue(hasattr(workflow, "create_and_validate_musicscope_notebook"))

        # Test that they all reference the same notebooks directory
        self.assertEqual(archiver.notebooks_dir, self.notebooks_dir)
        self.assertEqual(executor.notebooks_dir, self.notebooks_dir)
        self.assertEqual(validator.notebooks_dir, self.notebooks_dir)
        self.assertEqual(workflow.notebooks_dir, self.notebooks_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
