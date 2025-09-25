#!/usr/bin/env python3
"""
Test Complete Workflow Failure Scenarios

Tests that the system FAILS LOUDLY when notebooks have issues.
"""

import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

# Add current directory to path for imports
sys.path.insert(0, ".")


class TestCompleteWorkflowFailureScenarios(unittest.TestCase):
    """Test that the complete workflow fails loudly when it should."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create directory structure
        self.notebooks_dir = Path("notebooks")
        self.notebooks_dir.mkdir(parents=True)

        # Create mock .env file
        with open(".env", "w") as f:
            f.write("DB_HOST=localhost\nDB_USER=test\nDB_PASSWORD=test\nDB_NAME=test\n")

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)

    def test_validation_fails_on_critical_errors(self):
        """Test that validation fails when notebook has critical errors."""
        from complete_notebook_workflow import CompleteNotebookWorkflow

        workflow = CompleteNotebookWorkflow(self.notebooks_dir)

        # Create a notebook with critical errors
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
                    "source": ["# Simulated error outputs"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create the error notebook
        error_path = self.notebooks_dir / "error_test.ipynb"
        with open(error_path, "w") as f:
            json.dump(error_notebook, f)

        # Validate should fail
        validation_result = workflow.validator.validate_outputs(error_path)

        self.assertFalse(validation_result["success"])
        self.assertGreater(len(validation_result["errors"]), 0)

        # Check specific error patterns
        error_messages = [e["message"] for e in validation_result["errors"]]
        self.assertTrue(any("CRITICAL ERROR" in msg for msg in error_messages))
        self.assertTrue(any("Not Available" in msg for msg in error_messages))

    def test_validation_fails_on_missing_success_indicators(self):
        """Test that validation fails when notebook has no success indicators."""
        from complete_notebook_workflow import CompleteNotebookWorkflow

        workflow = CompleteNotebookWorkflow(self.notebooks_dir)

        # Create a notebook with no success indicators
        no_success_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["Some output\n", "But no success indicators\n"],
                        }
                    ],
                    "source": ["# No success indicators"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create the notebook
        no_success_path = self.notebooks_dir / "no_success_test.ipynb"
        with open(no_success_path, "w") as f:
            json.dump(no_success_notebook, f)

        # Validate should fail
        validation_result = workflow.validator.validate_outputs(no_success_path)

        self.assertFalse(validation_result["success"])
        self.assertEqual(len(validation_result["success_indicators"]), 0)

    def test_validation_detects_isrc_not_available(self):
        """Test that validation specifically detects ISRC data not available."""
        from complete_notebook_workflow import CompleteNotebookWorkflow

        workflow = CompleteNotebookWorkflow(self.notebooks_dir)

        # Create notebook with ISRC not available
        isrc_error_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["- **ISRC Data:** ❌ Not Available\n", "✅ Sentiment data: True\n"],
                        }
                    ],
                    "source": ["# ISRC not available"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create the notebook
        isrc_error_path = self.notebooks_dir / "isrc_error_test.ipynb"
        with open(isrc_error_path, "w") as f:
            json.dump(isrc_error_notebook, f)

        # Validate should fail
        validation_result = workflow.validator.validate_outputs(isrc_error_path)

        self.assertFalse(validation_result["success"])

        # Should specifically detect ISRC error
        error_messages = [e["message"] for e in validation_result["errors"]]
        isrc_errors = [msg for msg in error_messages if "Not Available" in msg]
        self.assertGreater(len(isrc_errors), 0)

    def test_validation_detects_chart_errors(self):
        """Test that validation detects chart generation errors."""
        from complete_notebook_workflow import CompleteNotebookWorkflow

        workflow = CompleteNotebookWorkflow(self.notebooks_dir)

        # Create notebook with chart errors
        chart_error_notebook = {
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
                                "🚨 CRITICAL ERROR in Chart 20: KeyError('artist_name')\n",
                                "Chart 20 returned None - CHECK YOUR DATA!\n",
                                "FIX YOUR DATABASE CONNECTION!\n",
                            ],
                        }
                    ],
                    "source": ["# Chart errors"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create the notebook
        chart_error_path = self.notebooks_dir / "chart_error_test.ipynb"
        with open(chart_error_path, "w") as f:
            json.dump(chart_error_notebook, f)

        # Validate should fail
        validation_result = workflow.validator.validate_outputs(chart_error_path)

        self.assertFalse(validation_result["success"])

        # Should detect multiple error types
        error_messages = [e["message"] for e in validation_result["errors"]]
        self.assertTrue(any("CRITICAL ERROR" in msg for msg in error_messages))
        self.assertTrue(any("returned None" in msg for msg in error_messages))
        self.assertTrue(any("FIX YOUR DATABASE" in msg for msg in error_messages))

    def test_validation_passes_with_all_success_indicators(self):
        """Test that validation passes when all success indicators are present."""
        from complete_notebook_workflow import CompleteNotebookWorkflow

        workflow = CompleteNotebookWorkflow(self.notebooks_dir)

        # Create notebook with all success indicators
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
                                "✅ SUCCESS: Chart 20 generated successfully with REAL data!\n",
                                "📊 Beautiful charts generated: 20/20 (100% success with REAL data)\n",
                                "🎯 TOTAL CHARTS: 20/20 (100% SUCCESS)\n",
                            ],
                        }
                    ],
                    "source": ["# All success indicators"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create the notebook
        success_path = self.notebooks_dir / "success_test.ipynb"
        with open(success_path, "w") as f:
            json.dump(success_notebook, f)

        # Validate should pass
        validation_result = workflow.validator.validate_outputs(success_path)

        self.assertTrue(validation_result["success"])
        self.assertEqual(len(validation_result["errors"]), 0)
        self.assertGreater(len(validation_result["success_indicators"]), 0)

        # Should detect specific success patterns
        success_messages = [s["message"] for s in validation_result["success_indicators"]]
        self.assertTrue(any("SUCCESS: Chart" in msg for msg in success_messages))
        self.assertTrue(any("20/20" in msg for msg in success_messages))


if __name__ == "__main__":
    unittest.main(verbosity=2)
