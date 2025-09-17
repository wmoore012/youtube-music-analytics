#!/usr/bin/env python3
"""
Simplified Test for Notebook Validation Logic

Tests the core validation functionality without relying on nbconvert.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, ".")


class TestNotebookValidationSimple(unittest.TestCase):
    """Test the core notebook validation logic."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create directory structure
        self.notebooks_dir = Path("notebooks")
        self.notebooks_dir.mkdir(parents=True)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)

    def test_validate_successful_notebook_outputs(self):
        """Test validating notebook with successful outputs."""
        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)

        # Create notebook with successful outputs
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
                                "🎵 MusicScope™ Professional Dashboard Initialized!\n",
                                "✅ REAL DATA ONLY - No fake data ever\n",
                                "✅ SUCCESS: Chart 1 generated successfully with REAL data!\n",
                                "✅ SUCCESS: Chart 2 generated successfully with REAL data!\n",
                                "📊 Beautiful charts generated: 20/20 (100% success with REAL data)\n",
                            ],
                        }
                    ],
                    "source": ["print('Success indicators')"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        notebook_path = self.notebooks_dir / "success_test.ipynb"
        with open(notebook_path, "w") as f:
            json.dump(success_notebook, f)

        # Validate outputs
        result = executor.validate_outputs(notebook_path)

        # Should pass validation
        self.assertTrue(result["success"])
        self.assertEqual(len(result["errors"]), 0)
        self.assertGreater(len(result["success_indicators"]), 0)

        # Check specific success indicators
        success_messages = [s["message"] for s in result["success_indicators"]]
        self.assertTrue(any("SUCCESS: Chart" in msg for msg in success_messages))
        self.assertTrue(any("20/20" in msg for msg in success_messages))

    def test_validate_failed_notebook_outputs(self):
        """Test validating notebook with failed outputs."""
        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)

        # Create notebook with error outputs
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
                                "🚨 FIX YOUR DATABASE CONNECTION!\n",
                            ],
                        }
                    ],
                    "source": ["print('Error indicators')"],
                },
                {
                    "cell_type": "code",
                    "execution_count": 2,
                    "metadata": {},
                    "outputs": [
                        {
                            "output_type": "error",
                            "ename": "ValueError",
                            "evalue": "Database connection failed",
                            "traceback": ["ValueError: Database connection failed"],
                        }
                    ],
                    "source": ["raise ValueError('Database connection failed')"],
                },
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        notebook_path = self.notebooks_dir / "error_test.ipynb"
        with open(notebook_path, "w") as f:
            json.dump(error_notebook, f)

        # Validate outputs
        result = executor.validate_outputs(notebook_path)

        # Should fail validation
        self.assertFalse(result["success"])
        self.assertGreater(len(result["errors"]), 0)

        # Check specific error indicators
        error_messages = [e["message"] for e in result["errors"]]
        self.assertTrue(any("CRITICAL ERROR" in msg for msg in error_messages))
        self.assertTrue(any("Not Available" in msg for msg in error_messages))
        self.assertTrue(any("ValueError" in msg for msg in error_messages))

    def test_validate_mixed_outputs(self):
        """Test validating notebook with both success and error indicators."""
        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)

        # Create notebook with mixed outputs
        mixed_notebook = {
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
                                "✅ SUCCESS: Chart 2 generated successfully with REAL data!\n",
                            ],
                        }
                    ],
                    "source": ["print('Some success')"],
                },
                {
                    "cell_type": "code",
                    "execution_count": 2,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["🚨 CRITICAL ERROR in Chart 20: Database connection failed!\n"],
                        }
                    ],
                    "source": ["print('But also error')"],
                },
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        notebook_path = self.notebooks_dir / "mixed_test.ipynb"
        with open(notebook_path, "w") as f:
            json.dump(mixed_notebook, f)

        # Validate outputs
        result = executor.validate_outputs(notebook_path)

        # Should fail validation due to errors (even with some success)
        self.assertFalse(result["success"])
        self.assertGreater(len(result["errors"]), 0)
        self.assertGreater(len(result["success_indicators"]), 0)

    def test_validate_empty_notebook(self):
        """Test validating empty notebook."""
        from notebook_execution_validator import NotebookExecutor

        executor = NotebookExecutor(self.notebooks_dir)

        # Create empty notebook
        empty_notebook = {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 4}

        notebook_path = self.notebooks_dir / "empty_test.ipynb"
        with open(notebook_path, "w") as f:
            json.dump(empty_notebook, f)

        # Validate outputs
        result = executor.validate_outputs(notebook_path)

        # Should fail validation (no success indicators)
        self.assertFalse(result["success"])
        self.assertEqual(len(result["errors"]), 0)
        self.assertEqual(len(result["success_indicators"]), 0)

    def test_integration_with_archiver(self):
        """Test integration with NotebookArchiver without execution."""
        from notebook_archiver import NotebookArchiver
        from notebook_execution_validator import NotebookExecutionValidator

        # Create existing notebook
        existing_notebook = self.notebooks_dir / "Test_Dashboard.ipynb"
        old_content = {
            "cells": [{"cell_type": "markdown", "source": ["# Old Version"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }
        with open(existing_notebook, "w") as f:
            json.dump(old_content, f)

        # Test archiver integration
        archiver = NotebookArchiver(self.notebooks_dir)

        new_content = {
            "cells": [{"cell_type": "markdown", "source": ["# New Version"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Archive and create new
        new_path = archiver.archive_and_create_new("Test_Dashboard.ipynb", new_content)

        # Should have archived old version
        archived_files = list((self.notebooks_dir / "archive").glob("Test_Dashboard_*.ipynb"))
        self.assertEqual(len(archived_files), 1)

        # Should have created new version
        self.assertTrue(new_path.exists())
        self.assertRegex(new_path.name, r"Test_Dashboard_\d{8}_\d{6}\.ipynb")


if __name__ == "__main__":
    unittest.main(verbosity=2)
