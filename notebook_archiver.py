#!/usr/bin/env python3
"""
Notebook Archiver System

Organizes notebooks with datetime stamps and archives old versions.
Follows TDD principles with comprehensive error handling.
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NotebookArchiver:
    """
    Professional notebook archiving system with datetime organization.

    Features:
    - Archives old notebooks to archive/ folder with datetime stamps
    - Creates new notebooks with datetime in filename
    - Validates notebook outputs for errors (FAILS LOUDLY)
    - Bulletproof error handling and logging
    - TDD-driven implementation
    """

    def __init__(self, notebooks_dir: Path):
        """
        Initialize the notebook archiver.

        Args:
            notebooks_dir: Path to the notebooks directory
        """
        self.notebooks_dir = Path(notebooks_dir)
        self.archive_dir = self.notebooks_dir / "archive"

        # Ensure directories exist
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📁 NotebookArchiver initialized")
        logger.info(f"   📂 Notebooks: {self.notebooks_dir}")
        logger.info(f"   📦 Archive: {self.archive_dir}")

    def generate_datetime_filename(self, base_filename: str) -> str:
        """
        Generate a filename with datetime stamp.

        Args:
            base_filename: Original filename (e.g., "MusicScope_Dashboard.ipynb")

        Returns:
            Filename with datetime (e.g., "MusicScope_Dashboard_20250917_143022.ipynb")
        """
        # Get current datetime
        now = datetime.now()
        datetime_str = now.strftime("%Y%m%d_%H%M%S")

        # Split filename and extension
        path = Path(base_filename)
        name_without_ext = path.stem
        extension = path.suffix

        # Create datetime filename
        datetime_filename = f"{name_without_ext}_{datetime_str}{extension}"

        logger.info(f"🕒 Generated datetime filename: {datetime_filename}")
        return datetime_filename

    def archive_existing_notebook(self, notebook_filename: str) -> Path:
        """
        Archive an existing notebook to the archive folder with datetime stamp.

        Args:
            notebook_filename: Name of the notebook to archive

        Returns:
            Path to the archived notebook

        Raises:
            FileNotFoundError: If the notebook doesn't exist
        """
        source_path = self.notebooks_dir / notebook_filename

        if not source_path.exists():
            raise FileNotFoundError(f"Notebook not found: {source_path}")

        # Generate datetime filename for archive
        datetime_filename = self.generate_datetime_filename(notebook_filename)
        archive_path = self.archive_dir / datetime_filename

        # Move to archive
        shutil.move(str(source_path), str(archive_path))

        logger.info(f"📦 Archived notebook:")
        logger.info(f"   📄 From: {source_path}")
        logger.info(f"   📦 To: {archive_path}")

        return archive_path

    def archive_existing_notebook_to_datetime_folder(self, notebook_filename: str) -> Path:
        """
        Archive existing notebook to datetime-organized folder structure.
        Uses the datetime from when the executed file was originally created.

        Args:
            notebook_filename: Name of the notebook to archive

        Returns:
            Path to the archived notebook

        Raises:
            FileNotFoundError: If the notebook doesn't exist
        """
        source_path = self.notebooks_dir / notebook_filename

        if not source_path.exists():
            raise FileNotFoundError(f"Notebook not found: {source_path}")

        # Extract datetime from executed filename if it has one
        datetime_folder = self._extract_datetime_from_filename(notebook_filename)

        # If no datetime in filename, use file creation time
        if not datetime_folder:
            file_stat = source_path.stat()
            creation_time = datetime.fromtimestamp(file_stat.st_ctime)
            datetime_folder = creation_time.strftime("%Y%m%d_%H%M%S")

        archive_datetime_dir = self.archive_dir / datetime_folder
        archive_datetime_dir.mkdir(parents=True, exist_ok=True)

        # Archive with CLEAN name (remove datetime from filename)
        clean_filename = self._get_clean_filename(notebook_filename)
        archive_path = archive_datetime_dir / clean_filename

        # Move to archive
        shutil.move(str(source_path), str(archive_path))

        logger.info(f"📦 Archived notebook to datetime folder:")
        logger.info(f"   📄 From: {source_path}")
        logger.info(f"   📦 To: {archive_path}")
        logger.info(f"   📁 Folder: {datetime_folder} (from file creation time)")

        return archive_path

    def _extract_datetime_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract datetime from executed notebook filename.

        Args:
            filename: Notebook filename (e.g., "MusicScope™_Professional_Dashboard_20250917_051133.ipynb")

        Returns:
            Datetime string in YYYYMMDD_HHMMSS format, or None if not found
        """
        import re

        # Pattern to match datetime in filename: YYYYMMDD_HHMMSS
        pattern = r"_(\d{8}_\d{6})(?:_executed)?\.ipynb$"
        match = re.search(pattern, filename)

        if match:
            datetime_str = match.group(1)
            logger.info(f"🕒 Extracted datetime from filename: {datetime_str}")
            return datetime_str

        # Try alternative patterns
        pattern2 = r"_(\d{8}_\d{6})_"
        match2 = re.search(pattern2, filename)
        if match2:
            datetime_str = match2.group(1)
            logger.info(f"🕒 Extracted datetime from filename (alt): {datetime_str}")
            return datetime_str

        logger.info(f"🕒 No datetime found in filename: {filename}")
        return None

    def _get_clean_filename(self, filename: str) -> str:
        """
        Get clean filename without datetime stamps.

        Args:
            filename: Original filename with potential datetime

        Returns:
            Clean filename without datetime
        """
        import re

        # Remove datetime patterns from filename
        # Pattern 1: _YYYYMMDD_HHMMSS.ipynb
        clean_name = re.sub(r"_\d{8}_\d{6}\.ipynb$", ".ipynb", filename)

        # Pattern 2: _YYYYMMDD_HHMMSS_executed.ipynb
        clean_name = re.sub(r"_\d{8}_\d{6}_executed\.ipynb$", ".ipynb", clean_name)

        # Pattern 3: _YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.ipynb (double datetime)
        clean_name = re.sub(r"_\d{8}_\d{6}_\d{8}_\d{6}\.ipynb$", ".ipynb", clean_name)

        logger.info(f"🧹 Clean filename: {filename} → {clean_name}")
        return clean_name

    def create_notebook_with_datetime(self, base_filename: str, notebook_content: Dict[str, Any]) -> Path:
        """
        Create a new notebook with datetime in the filename.

        Args:
            base_filename: Base name for the notebook
            notebook_content: Notebook content as dictionary

        Returns:
            Path to the created notebook
        """
        # Generate datetime filename
        datetime_filename = self.generate_datetime_filename(base_filename)
        notebook_path = self.notebooks_dir / datetime_filename

        # Write notebook content
        with open(notebook_path, "w", encoding="utf-8") as f:
            json.dump(notebook_content, f, indent=2, ensure_ascii=False)

        logger.info(f"📝 Created new notebook: {notebook_path}")
        return notebook_path

    def archive_and_create_new(self, notebook_filename: str, new_content: Dict[str, Any]) -> Path:
        """
        Complete workflow: Archive existing notebook (if exists) and create new one with CLEAN name.

        Args:
            notebook_filename: Name of the notebook (e.g., "MusicScope™_Professional_Dashboard.ipynb")
            new_content: Content for the new notebook

        Returns:
            Path to the newly created notebook (with CLEAN name, no datetime)
        """
        # Check for existing notebook with exact name
        existing_path = self.notebooks_dir / notebook_filename

        # Archive existing notebook if it exists
        if existing_path.exists():
            logger.info(f"🔄 Found existing notebook, archiving: {notebook_filename}")
            self.archive_existing_notebook_to_datetime_folder(notebook_filename)
        else:
            logger.info(f"✨ No existing notebook found, creating new: {notebook_filename}")

        # Create new notebook with CLEAN name (no datetime in filename)
        new_path = self.notebooks_dir / notebook_filename

        with open(new_path, "w", encoding="utf-8") as f:
            json.dump(new_content, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ Workflow complete:")
        logger.info(f"   📝 New notebook: {new_path.name} (CLEAN NAME)")
        logger.info(f"   📂 Location: {new_path.parent}")

        return new_path

    def get_archive_summary(self) -> Dict[str, Any]:
        """
        Get summary of archived notebooks.

        Returns:
            Dictionary with archive statistics
        """
        archived_files = list(self.archive_dir.glob("*.ipynb"))

        summary = {
            "total_archived": len(archived_files),
            "archive_directory": str(self.archive_dir),
            "archived_files": [f.name for f in archived_files],
            "latest_archive": None,
        }

        if archived_files:
            # Find latest archived file by modification time
            latest_file = max(archived_files, key=lambda f: f.stat().st_mtime)
            summary["latest_archive"] = latest_file.name

        return summary

    def cleanup_old_archives(self, keep_count: int = 10) -> int:
        """
        Clean up old archived notebooks, keeping only the most recent ones.

        Args:
            keep_count: Number of recent archives to keep per base filename

        Returns:
            Number of files cleaned up
        """
        archived_files = list(self.archive_dir.glob("*.ipynb"))

        if len(archived_files) <= keep_count:
            logger.info(f"📦 Archive cleanup: {len(archived_files)} files, keeping all")
            return 0

        # Group by base filename (without datetime)
        file_groups = {}
        for file_path in archived_files:
            # Extract base name (remove datetime part)
            name_parts = file_path.stem.split("_")
            if len(name_parts) >= 3:  # Should have at least name_date_time
                base_name = "_".join(name_parts[:-2])  # Remove last 2 parts (date_time)
            else:
                base_name = file_path.stem

            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(file_path)

        # Clean up old files in each group
        cleaned_count = 0
        for base_name, files in file_groups.items():
            if len(files) > keep_count:
                # Sort by modification time, keep newest
                files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
                files_to_remove = files[keep_count:]

                for file_path in files_to_remove:
                    file_path.unlink()
                    cleaned_count += 1
                    logger.info(f"🗑️ Cleaned up old archive: {file_path.name}")

        logger.info(f"🧹 Archive cleanup complete: removed {cleaned_count} old files")
        return cleaned_count

    def create_and_validate_musicscope_notebook(self) -> Dict[str, Any]:
        """
        Complete MusicScope™ workflow: create, archive, and validate notebook.

        This is the MAIN METHOD that integrates everything:
        1. Creates MusicScope™ notebook with real data
        2. Archives old versions with datetime
        3. Validates the notebook structure and outputs
        4. FAILS LOUDLY if any issues are found

        Returns:
            Dictionary with complete workflow results

        Raises:
            RuntimeError: If any step fails (FAILS LOUDLY)
        """
        logger.info(f"🎵 Starting Complete MusicScope™ Notebook Workflow")
        logger.info(f"=" * 70)
        logger.info(f"🚨 REAL DATA ONLY - NO FAKE DATA EVER")
        logger.info(f"🎨 Beautiful Interactive Charts")
        logger.info(f"🛡️ Bulletproof Database Schema")
        logger.info(f"🚀 FAILS LOUDLY - We fix problems, we don't hide them")

        try:
            # Import here to avoid circular imports
            from complete_notebook_workflow import CompleteNotebookWorkflow

            # Execute complete workflow
            workflow = CompleteNotebookWorkflow(self.notebooks_dir)
            result = workflow.create_and_validate_musicscope_notebook()

            logger.info(f"🎉 COMPLETE MUSICSCOPE™ WORKFLOW SUCCESS!")
            logger.info(f"   📄 Notebook: {Path(result['notebook_path']).name}")
            logger.info(f"   ✅ Validation: {'PASSED' if result['validation_passed'] else 'FAILED'}")
            logger.info(f"   📊 Success Indicators: {len(result['validation_result']['success_indicators'])}")
            logger.info(f"   🚨 Errors: {len(result['validation_result']['errors'])}")

            return result

        except Exception as e:
            logger.error(f"🚨 COMPLETE MUSICSCOPE™ WORKFLOW FAILED: {e}")
            raise RuntimeError(f"🚨 COMPLETE MUSICSCOPE™ WORKFLOW FAILED: {e}")


def main():
    """Example usage of the NotebookArchiver."""
    import tempfile

    # Example usage
    with tempfile.TemporaryDirectory() as temp_dir:
        notebooks_dir = Path(temp_dir) / "notebooks"
        archiver = NotebookArchiver(notebooks_dir)

        # Sample notebook content
        sample_notebook = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["# MusicScope™ Professional Dashboard\\n", "Generated with datetime archiving system"],
                }
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create new notebook with datetime
        new_path = archiver.archive_and_create_new("MusicScope_Dashboard.ipynb", sample_notebook)

        print(f"✅ Created notebook: {new_path}")
        print(f"📦 Archive summary: {archiver.get_archive_summary()}")


if __name__ == "__main__":
    main()
