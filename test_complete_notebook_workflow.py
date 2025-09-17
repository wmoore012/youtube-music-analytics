#!/usr/bin/env python3
"""
Complete test of the notebook archiving and datetime workflow.
Tests the integration between NotebookArchiver and create_notebook.py
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


class TestCompleteNotebookWorkflow(unittest.TestCase):
    """Test the complete notebook creation and archiving workflow."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create basic directory structure
        os.makedirs("notebooks", exist_ok=True)

        # Create a mock .env file for database connection
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

    def test_notebook_archiver_integration(self):
        """Test that NotebookArchiver works correctly with the workflow."""
        from notebook_archiver import NotebookArchiver

        notebooks_dir = Path("notebooks")
        archiver = NotebookArchiver(notebooks_dir)

        # Create sample notebook content
        sample_notebook = {
            "cells": [{"cell_type": "markdown", "metadata": {}, "source": ["# Test Notebook"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Test creating notebook with datetime
        notebook_path = archiver.create_notebook_with_datetime(
            "MusicScope™_Professional_Dashboard.ipynb", sample_notebook
        )

        # Verify notebook was created with datetime
        self.assertTrue(notebook_path.exists())
        self.assertRegex(notebook_path.name, r"MusicScope™_Professional_Dashboard_\d{8}_\d{6}\.ipynb")

        # Verify it's in the notebooks directory (not archive)
        self.assertEqual(notebook_path.parent.name, "notebooks")

        # Test archiving workflow
        # Create another notebook to archive the first one
        import time

        time.sleep(1.1)  # Ensure different timestamp
        new_notebook_path = archiver.archive_and_create_new("MusicScope™_Professional_Dashboard.ipynb", sample_notebook)

        # Original should be archived
        archived_files = list((notebooks_dir / "archive").glob("MusicScope™_Professional_Dashboard_*.ipynb"))
        self.assertEqual(len(archived_files), 1)

        # New one should exist
        self.assertTrue(new_notebook_path.exists())
        self.assertNotEqual(str(notebook_path), str(new_notebook_path))

    def test_datetime_filename_format(self):
        """Test that datetime filenames follow the correct format."""
        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(Path("notebooks"))

        # Test multiple filename generations
        for i in range(3):
            filename = archiver.generate_datetime_filename("Test_Notebook.ipynb")

            # Should match YYYYMMDD_HHMMSS format
            self.assertRegex(filename, r"Test_Notebook_\d{8}_\d{6}\.ipynb")

            # Should be parseable as datetime
            datetime_part = filename.replace("Test_Notebook_", "").replace(".ipynb", "")
            parsed_datetime = datetime.strptime(datetime_part, "%Y%m%d_%H%M%S")
            self.assertIsInstance(parsed_datetime, datetime)

            # Should be recent (within last minute)
            time_diff = datetime.now() - parsed_datetime
            self.assertLess(time_diff.total_seconds(), 60)

    def test_archive_directory_structure(self):
        """Test that archive directory is created and organized correctly."""
        from notebook_archiver import NotebookArchiver

        notebooks_dir = Path("notebooks")
        archiver = NotebookArchiver(notebooks_dir)

        # Archive directory should be created
        self.assertTrue((notebooks_dir / "archive").exists())

        # Create and archive multiple notebooks
        sample_notebook = {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 4}

        for i in range(3):
            # Create notebook
            notebook_name = f"Test_Notebook_{i}.ipynb"
            notebook_path = notebooks_dir / notebook_name
            with open(notebook_path, "w") as f:
                json.dump(sample_notebook, f)

            # Archive it
            archived_path = archiver.archive_existing_notebook(notebook_name)

            # Should be in archive directory
            self.assertTrue(str(archived_path).startswith(str(notebooks_dir / "archive")))
            self.assertTrue(archived_path.exists())

            # Original should be gone
            self.assertFalse(notebook_path.exists())

        # Should have 3 archived files
        archived_files = list((notebooks_dir / "archive").glob("Test_Notebook_*_*.ipynb"))
        self.assertEqual(len(archived_files), 3)

    def test_workflow_with_existing_notebook(self):
        """Test the complete workflow when an existing notebook exists."""
        from notebook_archiver import NotebookArchiver

        notebooks_dir = Path("notebooks")
        archiver = NotebookArchiver(notebooks_dir)

        # Create existing notebook
        existing_notebook = notebooks_dir / "MusicScope™_Professional_Dashboard.ipynb"
        old_content = {
            "cells": [{"cell_type": "markdown", "source": ["# Old Version"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }
        with open(existing_notebook, "w") as f:
            json.dump(old_content, f)

        # Create new content
        new_content = {
            "cells": [{"cell_type": "markdown", "source": ["# New Version"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Execute workflow
        new_notebook_path = archiver.archive_and_create_new("MusicScope™_Professional_Dashboard.ipynb", new_content)

        # Old notebook should be archived
        archived_files = list((notebooks_dir / "archive").glob("MusicScope™_Professional_Dashboard_*.ipynb"))
        self.assertEqual(len(archived_files), 1)

        # Verify old content is in archive
        with open(archived_files[0]) as f:
            archived_content = json.load(f)
        self.assertEqual(archived_content["cells"][0]["source"], ["# Old Version"])

        # New notebook should exist with datetime
        self.assertTrue(new_notebook_path.exists())
        self.assertRegex(new_notebook_path.name, r"MusicScope™_Professional_Dashboard_\d{8}_\d{6}\.ipynb")

        # Verify new content
        with open(new_notebook_path) as f:
            new_notebook_content = json.load(f)
        self.assertEqual(new_notebook_content["cells"][0]["source"], ["# New Version"])

    def test_multiple_workflow_executions(self):
        """Test multiple executions of the workflow create separate archives."""
        import time

        from notebook_archiver import NotebookArchiver

        notebooks_dir = Path("notebooks")
        archiver = NotebookArchiver(notebooks_dir)

        sample_content = {
            "cells": [{"cell_type": "markdown", "source": ["# Test"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Execute workflow multiple times
        created_notebooks = []
        for i in range(3):
            notebook_path = archiver.archive_and_create_new("MusicScope™_Professional_Dashboard.ipynb", sample_content)
            created_notebooks.append(notebook_path)

            # Small delay to ensure different timestamps
            if i < 2:
                time.sleep(1.1)

        # Should have 2 archived notebooks (first execution creates new, subsequent ones archive previous)
        archived_files = list((notebooks_dir / "archive").glob("MusicScope™_Professional_Dashboard_*.ipynb"))
        self.assertEqual(len(archived_files), 2)

        # Should have 1 current notebook (the latest one)
        current_notebooks = list(notebooks_dir.glob("MusicScope™_Professional_Dashboard_*.ipynb"))
        self.assertEqual(len(current_notebooks), 1)

        # All created notebooks should have different names
        notebook_names = [nb.name for nb in created_notebooks]
        self.assertEqual(len(set(notebook_names)), 3)  # All unique


if __name__ == "__main__":
    # Run tests
    unittest.main(verbosity=2)
