#!/usr / bin / env python3
"""
Proper Notebook Archiving System

This system maintains a clean notebook directory by:
1. Automatically archiving old executed notebooks
2. Preventing accumulation of unnecessary files
3. Maintaining only essential notebooks in the main directory
4. No .md result files (they're unnecessary)

Usage:
    from tools.maintenance.notebook_archiver import NotebookArchiver

    archiver = NotebookArchiver()
    archiver.archive_executed_notebook("MusicScope™_Professional_Dashboard.ipynb")
"""

from datetime import datetime
import logging
import os
from pathlib import Path
import shutil
from typing import Any, Dict, Optional

# Set up logging
logger = logging.getLogger(__name__)


class NotebookArchiver:
    """
    Clean and simple notebook archiving system.

    Maintains a clean notebooks directory by archiving old versions
    and preventing file accumulation.
    """

    def __init__(self, notebooks_dir: str = "notebooks"):
        """Initialize the archiver."""
        self.notebooks_dir = Path(notebooks_dir)
        self.executed_dir = self.notebooks_dir / "executed"
        self.archive_dir = self.notebooks_dir / "archive"

        # Ensure directories exist
        self.executed_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"📁 NotebookArchiver initialized for {self.notebooks_dir}")

    def archive_executed_notebook(self, notebook_name: str) -> Optional[Path]:
        """
        Archive an executed notebook if it exists.

        Args:
            notebook_name: Name of the base notebook (e.g., "MusicScope™_Professional_Dashboard.ipynb")

        Returns:
            Path to archived notebook if archived, None if no executed version found
        """
        # Look for executed version
        executed_name = notebook_name.replace(".ipynb", "-executed.ipynb")
        executed_path = self.executed_dir / executed_name

        if not executed_path.exists():
            logger.info(f"📝 No executed version found for {notebook_name}")
            return None

        # Create timestamp for archive
        timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
        archive_subdir = self.archive_dir / timestamp
        archive_subdir.mkdir(exist_ok=True)

        # Archive with clean name (no -executed suffix)
        archive_path = archive_subdir / notebook_name
        shutil.move(str(executed_path), str(archive_path))

        logger.info(f"📦 Archived executed notebook:")
        logger.info(f"   From: {executed_path}")
        logger.info(f"   To: {archive_path}")

        return archive_path

    def cleanup_old_archives(self, keep_count: int = 3) -> int:
        """
        Clean up old archive directories, keeping only the most recent ones.

        Args:
            keep_count: Number of recent archives to keep

        Returns:
            Number of directories removed
        """
        if not self.archive_dir.exists():
            return 0

        # Get timestamped directories
        archive_dirs = []
        for item in self.archive_dir.iterdir():
            if item.is_dir() and item.name != "first":  # Keep special directories
                try:
                    # Parse timestamp
                    timestamp = datetime.strptime(item.name, "%Y % m%d_ % H%M % S")
                    archive_dirs.append((timestamp, item))
                except ValueError:
                    # Not a timestamp directory, keep it
                    continue

        # Sort by timestamp, newest first
        archive_dirs.sort(key=lambda x: x[0], reverse=True)

        removed_count = 0
        if len(archive_dirs) > keep_count:
            for _, old_dir in archive_dirs[keep_count:]:
                logger.info(f"🗑️ Removing old archive: {old_dir.name}")
                shutil.rmtree(old_dir)
                removed_count += 1

        if removed_count > 0:
            logger.info(f"✅ Cleaned up {removed_count} old archive directories")

        return removed_count

    def auto_archive_and_cleanup(self, notebook_name: str) -> Dict[str, Any]:
        """
        Complete archiving workflow: archive executed notebook and cleanup old archives.

        Args:
            notebook_name: Name of the notebook to process

        Returns:
            Dictionary with operation results
        """
        logger.info(f"🔄 Auto - archiving workflow for {notebook_name}")

        # Archive executed notebook
        archived_path = self.archive_executed_notebook(notebook_name)

        # Cleanup old archives
        cleaned_count = self.cleanup_old_archives()

        result = {
            "notebook_name": notebook_name,
            "archived": archived_path is not None,
            "archived_path": str(archived_path) if archived_path else None,
            "old_archives_cleaned": cleaned_count,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(f"✅ Auto - archiving complete for {notebook_name}")
        return result

    def get_archive_status(self) -> Dict[str, Any]:
        """Get current archive status and statistics."""
        status = {"total_archives": 0, "archive_directories": [], "executed_notebooks": [], "base_notebooks": []}

        # Count archive directories
        if self.archive_dir.exists():
            for item in self.archive_dir.iterdir():
                if item.is_dir():
                    status["archive_directories"].append(item.name)
                    status["total_archives"] += 1

        # Count executed notebooks
        if self.executed_dir.exists():
            for item in self.executed_dir.glob("*.ipynb"):
                status["executed_notebooks"].append(item.name)

        # Count base notebooks
        for item in self.notebooks_dir.glob("*.ipynb"):
            status["base_notebooks"].append(item.name)

        return status


def setup_auto_archiving():
    """
    Set up automatic archiving for notebook execution.

    This can be called from notebook execution scripts to ensure
    old versions are automatically archived.
    """
    archiver = NotebookArchiver()

    # Archive any existing executed notebooks
    essential_notebooks = ["MusicScope™_Professional_Dashboard.ipynb", "MusicScope™_20_Chart_Dashboard.ipynb"]

    for notebook in essential_notebooks:
        archiver.auto_archive_and_cleanup(notebook)

    logger.info("🎯 Auto - archiving setup complete")


def main():
    """Demo the archiving system."""
    archiver = NotebookArchiver()

    print("📁 NOTEBOOK ARCHIVING SYSTEM")
    print("=" * 40)

    # Show current status
    status = archiver.get_archive_status()
    print(f"📊 Current Status:")
    print(f"   Base notebooks: {len(status['base_notebooks'])}")
    print(f"   Executed notebooks: {len(status['executed_notebooks'])}")
    print(f"   Archive directories: {len(status['archive_directories'])}")

    if status["executed_notebooks"]:
        print(f"\n📝 Executed notebooks found:")
        for notebook in status["executed_notebooks"]:
            print(f"   - {notebook}")

    if status["archive_directories"]:
        print(f"\n📦 Archive directories:")
        for archive_dir in sorted(status["archive_directories"]):
            print(f"   - {archive_dir}")

    print(f"\n💡 This system prevents notebook directory clutter by:")
    print(f"   1. Archiving old executed notebooks automatically")
    print(f"   2. Keeping only 3 recent archive versions")
    print(f"   3. No .md result files (they're unnecessary)")
    print(f"   4. Clean main directory with only essential notebooks")


if __name__ == "__main__":
    main()
