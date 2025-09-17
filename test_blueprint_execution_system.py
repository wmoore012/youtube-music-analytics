#!/usr/bin/env python3
"""
Test-Driven Development for Blueprint Execution System

Tests for maintaining a blueprint notebook and creating executed versions.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, ".")


class TestBlueprintExecutionSystem(unittest.TestCase):
    """Test suite for blueprint and execution system."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create notebooks directory
        self.notebooks_dir = Path("notebooks")
        self.notebooks_dir.mkdir(parents=True)

        # Create archive directory
        self.archive_dir = self.notebooks_dir / "archive"
        self.archive_dir.mkdir(parents=True)

        # Create mock .env file
        with open(".env", "w") as f:
            f.write("DB_HOST=localhost\nDB_USER=test\nDB_PASSWORD=test\nDB_NAME=test\n")

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)

    def test_blueprint_execution_manager_creation(self):
        """Test that BlueprintExecutionManager can be created."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)
        self.assertIsNotNone(manager)
        self.assertEqual(manager.notebooks_dir, self.notebooks_dir)

    def test_create_blueprint_notebook(self):
        """Test creating a blueprint notebook."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create blueprint
        blueprint_path = manager.create_blueprint_notebook()

        # Should exist and be named correctly
        self.assertTrue(blueprint_path.exists())
        self.assertEqual(blueprint_path.name, "MusicScope™_Professional_Dashboard.ipynb")
        self.assertEqual(blueprint_path.parent, self.notebooks_dir)

        # Should be a valid notebook
        with open(blueprint_path) as f:
            notebook = json.load(f)
        self.assertIn("cells", notebook)
        self.assertGreater(len(notebook["cells"]), 0)

    def test_execute_blueprint_creates_executed_version(self):
        """Test executing blueprint creates executed version with datetime."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create blueprint first
        blueprint_path = manager.create_blueprint_notebook()

        # Execute blueprint
        executed_path = manager.execute_blueprint()

        # Should create executed version with datetime
        self.assertTrue(executed_path.exists())
        self.assertRegex(executed_path.name, r"MusicScope™_Professional_Dashboard_\d{8}_\d{6}_executed\.ipynb")
        self.assertEqual(executed_path.parent, self.notebooks_dir)

        # Blueprint should still exist
        self.assertTrue(blueprint_path.exists())

    def test_validate_executed_notebook_success(self):
        """Test validating executed notebook for success patterns."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create executed notebook with success outputs
        executed_notebook = {
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
                                "✅ ISRC data: True\n",
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

        # Save executed notebook
        executed_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard_20250917_120000_executed.ipynb"
        with open(executed_path, "w") as f:
            json.dump(executed_notebook, f)

        # Validate
        validation_result = manager.validate_executed_notebook(executed_path)

        # Should pass
        self.assertTrue(validation_result["success"])
        self.assertEqual(len(validation_result["errors"]), 0)
        self.assertGreater(len(validation_result["success_indicators"]), 0)

    def test_validate_executed_notebook_failure_isrc(self):
        """Test validating executed notebook that fails on ISRC data."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create executed notebook with ISRC failure
        executed_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["✅ REAL DATA DISCOVERED\n", "- **ISRC Data:** ❌ Not Available\n"],
                        }
                    ],
                    "source": ["# ISRC failure simulation"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Save executed notebook
        executed_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard_20250917_120000_executed.ipynb"
        with open(executed_path, "w") as f:
            json.dump(executed_notebook, f)

        # Validate
        validation_result = manager.validate_executed_notebook(executed_path)

        # Should fail
        self.assertFalse(validation_result["success"])
        self.assertGreater(len(validation_result["errors"]), 0)

        # Should detect ISRC error
        error_messages = str(validation_result["errors"])
        self.assertIn("ISRC", error_messages)
        self.assertIn("Not Available", error_messages)

    def test_validate_executed_notebook_failure_chart_error(self):
        """Test validating executed notebook that fails on chart errors."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create executed notebook with chart error
        executed_notebook = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [
                        {
                            "name": "stdout",
                            "output_type": "stream",
                            "text": ["🚨 CRITICAL ERROR in Chart 20: KeyError('missing_column')\n"],
                        }
                    ],
                    "source": ["# Chart error simulation"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Save executed notebook
        executed_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard_20250917_120000_executed.ipynb"
        with open(executed_path, "w") as f:
            json.dump(executed_notebook, f)

        # Validate
        validation_result = manager.validate_executed_notebook(executed_path)

        # Should fail
        self.assertFalse(validation_result["success"])
        self.assertGreater(len(validation_result["errors"]), 0)

        # Should detect chart error
        error_messages = str(validation_result["errors"])
        self.assertIn("CRITICAL ERROR", error_messages)

    def test_complete_workflow_maintains_two_files(self):
        """Test that complete workflow maintains blueprint + executed files."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Execute complete workflow
        result = manager.execute_complete_workflow()

        # Should have both files
        self.assertTrue(result["blueprint_path"].exists())
        self.assertTrue(result["executed_path"].exists())

        # Should be different files
        self.assertNotEqual(result["blueprint_path"], result["executed_path"])

        # Blueprint should not have datetime
        self.assertEqual(result["blueprint_path"].name, "MusicScope™_Professional_Dashboard.ipynb")

        # Executed should have datetime
        self.assertRegex(
            result["executed_path"].name, r"MusicScope™_Professional_Dashboard_\d{8}_\d{6}_executed\.ipynb"
        )

        # Both should be in notebooks directory
        self.assertEqual(result["blueprint_path"].parent, self.notebooks_dir)
        self.assertEqual(result["executed_path"].parent, self.notebooks_dir)

    def test_archive_old_executed_versions(self):
        """Test that old executed versions get archived."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create old executed version
        old_executed = self.notebooks_dir / "MusicScope™_Professional_Dashboard_20250916_120000_executed.ipynb"
        with open(old_executed, "w") as f:
            json.dump({"cells": [], "metadata": {}, "nbformat": 4}, f)

        # Execute workflow
        result = manager.execute_complete_workflow()

        # Old executed should be archived
        self.assertFalse(old_executed.exists())

        # Should find it in archive
        archived_files = list(self.archive_dir.glob("*executed*.ipynb"))
        self.assertEqual(len(archived_files), 1)

        # New executed should exist
        self.assertTrue(result["executed_path"].exists())

        # Blueprint should still exist
        self.assertTrue(result["blueprint_path"].exists())

    def test_workflow_fails_loudly_on_validation_errors(self):
        """Test that workflow fails loudly when validation detects errors."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Mock the validation to return errors
        def mock_validate_with_errors(path):
            return {
                "success": False,
                "errors": ["🚨 CRITICAL ERROR in Chart 20: Database connection failed"],
                "summary": "Validation failed with critical errors",
            }

        # Replace validation method
        manager.validate_executed_notebook = mock_validate_with_errors

        # Execute workflow should fail loudly
        with self.assertRaises(RuntimeError) as context:
            manager.execute_complete_workflow()

        # Should contain error information
        self.assertIn("VALIDATION FAILED", str(context.exception))
        self.assertIn("CRITICAL ERROR", str(context.exception))

    def test_archive_validation_before_execution_validation(self):
        """Test that we validate archive was done correctly before checking execution outputs."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Create old executed version
        old_executed_content = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [{"name": "stdout", "output_type": "stream", "text": ["Old executed version output\n"]}],
                    "source": ["print('Old executed version output')"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        old_executed_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard_20250916_120000_executed.ipynb"
        with open(old_executed_path, "w") as f:
            json.dump(old_executed_content, f)

        # Verify old file exists before workflow
        self.assertTrue(old_executed_path.exists())

        # Execute workflow
        result = manager.execute_complete_workflow()

        # VALIDATE ARCHIVE STEP: Old executed file should be moved to archive
        self.assertFalse(old_executed_path.exists(), "Old executed file should be archived")

        # Check that old file is in archive with datetime folder
        archived_files = list(self.archive_dir.glob("*/*executed*.ipynb"))
        self.assertEqual(len(archived_files), 1, "Should have exactly 1 archived executed file")

        archived_file = archived_files[0]
        self.assertIn("executed", archived_file.name, "Archived file should contain 'executed'")

        # Verify archived content is correct
        with open(archived_file) as f:
            archived_content = json.load(f)
        self.assertEqual(
            archived_content["cells"][0]["outputs"][0]["text"],
            ["Old executed version output\n"],
            "Archived file should contain original content",
        )

        # ONLY AFTER archive validation passes, check new execution
        self.assertTrue(result["executed_path"].exists(), "New executed file should exist")
        self.assertTrue(result["blueprint_path"].exists(), "Blueprint should still exist")

        # Verify we have exactly 2 files in notebooks directory
        notebook_files = list(self.notebooks_dir.glob("*.ipynb"))
        self.assertEqual(len(notebook_files), 2, "Should have exactly 2 files: blueprint + executed")

    def test_get_current_files_status(self):
        """Test getting current status of blueprint and executed files."""
        from blueprint_execution_system import BlueprintExecutionManager

        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Initially no files
        status = manager.get_current_files_status()
        self.assertFalse(status["blueprint_exists"])
        self.assertFalse(status["executed_exists"])

        # Create blueprint
        blueprint_path = manager.create_blueprint_notebook()

        status = manager.get_current_files_status()
        self.assertTrue(status["blueprint_exists"])
        self.assertFalse(status["executed_exists"])
        self.assertEqual(status["blueprint_path"], blueprint_path)

        # Execute blueprint
        executed_path = manager.execute_blueprint()

        status = manager.get_current_files_status()
        self.assertTrue(status["blueprint_exists"])
        self.assertTrue(status["executed_exists"])
        self.assertEqual(status["blueprint_path"], blueprint_path)
        self.assertEqual(status["executed_path"], executed_path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
