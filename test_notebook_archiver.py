#!/usr/bin/env python3
"""
Test-Driven Development for Notebook Archiver System

Tests for organizing notebooks with datetime stamps and archiving old versions.
"""

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


class TestNotebookArchiver(unittest.TestCase):
    """Test suite for notebook archiving and datetime organization."""

    def setUp(self):
        """Set up test environment with temporary directories."""
        self.test_dir = tempfile.mkdtemp()
        self.notebooks_dir = Path(self.test_dir) / "notebooks"
        self.archive_dir = self.notebooks_dir / "archive"

        # Create directory structure
        self.notebooks_dir.mkdir(parents=True)
        self.archive_dir.mkdir(parents=True)

        # Create a sample notebook file
        self.sample_notebook = {
            "cells": [{"cell_type": "markdown", "metadata": {}, "source": ["# Test Notebook"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_datetime_filename_generation(self):
        """Test that datetime filenames are generated correctly."""
        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)

        # Test datetime filename generation
        filename = archiver.generate_datetime_filename("MusicScope_Dashboard.ipynb")

        # Should contain datetime in YYYYMMDD_HHMMSS format
        self.assertRegex(filename, r"MusicScope_Dashboard_\d{8}_\d{6}\.ipynb")

        # Should be parseable as datetime
        datetime_part = filename.replace("MusicScope_Dashboard_", "").replace(".ipynb", "")
        parsed_datetime = datetime.strptime(datetime_part, "%Y%m%d_%H%M%S")
        self.assertIsInstance(parsed_datetime, datetime)

    def test_archive_existing_notebook(self):
        """Test archiving an existing notebook to archive folder."""
        from notebook_archiver import NotebookArchiver

        # Create existing notebook
        existing_notebook = self.notebooks_dir / "MusicScope_Dashboard.ipynb"
        with open(existing_notebook, "w") as f:
            json.dump(self.sample_notebook, f)

        archiver = NotebookArchiver(self.notebooks_dir)

        # Archive the existing notebook
        archived_path = archiver.archive_existing_notebook("MusicScope_Dashboard.ipynb")

        # Original should be gone
        self.assertFalse(existing_notebook.exists())

        # Archived version should exist in archive folder
        self.assertTrue(archived_path.exists())
        self.assertTrue(str(archived_path).startswith(str(self.archive_dir)))

        # Archived file should have datetime in name
        self.assertRegex(archived_path.name, r"MusicScope_Dashboard_\d{8}_\d{6}\.ipynb")

    def test_create_new_notebook_with_datetime(self):
        """Test creating a new notebook with datetime in filename."""
        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)

        # Create new notebook with datetime
        new_path = archiver.create_notebook_with_datetime("MusicScope_Dashboard.ipynb", self.sample_notebook)

        # Should exist in notebooks directory (not archive)
        self.assertTrue(new_path.exists())
        self.assertEqual(new_path.parent, self.notebooks_dir)

        # Should have datetime in filename
        self.assertRegex(new_path.name, r"MusicScope_Dashboard_\d{8}_\d{6}\.ipynb")

        # Should contain the notebook content
        with open(new_path) as f:
            content = json.load(f)
        self.assertEqual(content["cells"][0]["source"], ["# Test Notebook"])

    def test_full_workflow_archive_and_create(self):
        """Test the complete workflow: archive old, create new with datetime."""
        from notebook_archiver import NotebookArchiver

        # Create existing notebook
        existing_notebook = self.notebooks_dir / "MusicScope_Dashboard.ipynb"
        old_content = {"cells": [{"source": ["# Old Notebook"]}]}
        with open(existing_notebook, "w") as f:
            json.dump(old_content, f)

        archiver = NotebookArchiver(self.notebooks_dir)

        # Execute full workflow
        new_path = archiver.archive_and_create_new("MusicScope_Dashboard.ipynb", self.sample_notebook)

        # Old notebook should be archived
        archived_files = list(self.archive_dir.glob("MusicScope_Dashboard_*.ipynb"))
        self.assertEqual(len(archived_files), 1)

        # New notebook should exist with datetime
        self.assertTrue(new_path.exists())
        self.assertEqual(new_path.parent, self.notebooks_dir)
        self.assertRegex(new_path.name, r"MusicScope_Dashboard_\d{8}_\d{6}\.ipynb")

        # Verify content is correct
        with open(new_path) as f:
            content = json.load(f)
        self.assertEqual(content["cells"][0]["source"], ["# Test Notebook"])

    def test_multiple_archives_create_separate_datetime_folders(self):
        """Test that multiple archives create separate datetime-organized folders."""
        import time

        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)

        # Create and archive multiple notebooks with small delays
        for i in range(3):
            notebook_path = self.notebooks_dir / f"Test_Notebook_{i}.ipynb"
            with open(notebook_path, "w") as f:
                json.dump({"cells": [{"source": [f"# Test {i}"]}]}, f)

            archiver.archive_existing_notebook(f"Test_Notebook_{i}.ipynb")

            # Small delay to ensure different timestamps
            if i < 2:  # Don't delay after the last one
                time.sleep(1.1)  # Just over 1 second to ensure different timestamps

        # Should have 3 archived files
        archived_files = list(self.archive_dir.glob("Test_Notebook_*_*.ipynb"))
        self.assertEqual(len(archived_files), 3)

        # Each should have datetime in correct format
        datetime_parts = []
        for archived_file in archived_files:
            # Extract datetime part
            name_parts = archived_file.stem.split("_")
            datetime_part = "_".join(name_parts[-2:])  # Last two parts are date_time
            datetime_parts.append(datetime_part)

            # Verify datetime format
            self.assertRegex(datetime_part, r"\d{8}_\d{6}")

        # Should have 3 datetime parts (may not be unique if executed very quickly)
        self.assertEqual(len(datetime_parts), 3)

    def test_no_existing_notebook_creates_new_directly(self):
        """Test that when no existing notebook exists, creates new one directly."""
        from notebook_archiver import NotebookArchiver

        archiver = NotebookArchiver(self.notebooks_dir)

        # No existing notebook, should create new one
        new_path = archiver.archive_and_create_new("New_Dashboard.ipynb", self.sample_notebook)

        # Should create new notebook with datetime
        self.assertTrue(new_path.exists())
        self.assertRegex(new_path.name, r"New_Dashboard_\d{8}_\d{6}\.ipynb")

        # Archive should be empty
        archived_files = list(self.archive_dir.glob("*.ipynb"))
        self.assertEqual(len(archived_files), 0)

    def test_archive_directory_creation(self):
        """Test that archive directory is created if it doesn't exist."""
        from notebook_archiver import NotebookArchiver

        # Remove archive directory
        shutil.rmtree(self.archive_dir)
        self.assertFalse(self.archive_dir.exists())

        archiver = NotebookArchiver(self.notebooks_dir)

        # Create existing notebook
        existing_notebook = self.notebooks_dir / "Test.ipynb"
        with open(existing_notebook, "w") as f:
            json.dump(self.sample_notebook, f)

        # Archive should create the directory
        archived_path = archiver.archive_existing_notebook("Test.ipynb")

        # Archive directory should now exist
        self.assertTrue(self.archive_dir.exists())
        self.assertTrue(archived_path.exists())


if __name__ == "__main__":
    unittest.main()
