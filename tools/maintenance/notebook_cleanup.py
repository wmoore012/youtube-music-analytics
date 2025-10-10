#!/usr / bin / env python3
"""
Notebook Cleanup Tool

Cleans up the notebooks directory by:
1. Removing unnecessary .md result files
2. Consolidating executed notebooks
3. Cleaning up excessive archive directories
4. Maintaining only the essential notebooks

Usage:
    python tools / maintenance / notebook_cleanup.py
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class NotebookCleanup:
    """Clean up notebook directory organization."""

    def __init__(self, notebooks_dir: str = "notebooks"):
        """Initialize the cleanup tool."""
        self.notebooks_dir = Path(notebooks_dir)
        self.executed_dir = self.notebooks_dir / "executed"
        self.archive_dir = self.notebooks_dir / "archive"

        logger.info(f"🧹 NotebookCleanup initialized for {self.notebooks_dir}")

    def analyze_current_state(self) -> Dict[str, Any]:
        """Analyze the current state of the notebooks directory."""
        logger.info("🔍 Analyzing current notebook directory state...")

        analysis = {
            "base_notebooks": [],
            "executed_notebooks": [],
            "result_md_files": [],
            "archive_directories": [],
            "python_files": [],
            "other_files": [],
        }

        # Analyze main notebooks directory
        if self.notebooks_dir.exists():
            for item in self.notebooks_dir.iterdir():
                if item.is_file():
                    if item.suffix == ".ipynb":
                        analysis["base_notebooks"].append(str(item))
                    elif item.suffix == ".py":
                        analysis["python_files"].append(str(item))
                    else:
                        analysis["other_files"].append(str(item))

        # Analyze executed directory
        if self.executed_dir.exists():
            for item in self.executed_dir.iterdir():
                if item.is_file():
                    if item.suffix == ".ipynb":
                        analysis["executed_notebooks"].append(str(item))
                    elif item.suffix == ".md":
                        analysis["result_md_files"].append(str(item))

        # Analyze archive directories
        if self.archive_dir.exists():
            for item in self.archive_dir.iterdir():
                if item.is_dir():
                    analysis["archive_directories"].append(str(item))

        # Print analysis
        logger.info("📊 Current state analysis:")
        logger.info(f"   Base notebooks: {len(analysis['base_notebooks'])}")
        logger.info(f"   Executed notebooks: {len(analysis['executed_notebooks'])}")
        logger.info(f"   Result .md files: {len(analysis['result_md_files'])}")
        logger.info(f"   Archive directories: {len(analysis['archive_directories'])}")
        logger.info(f"   Python files: {len(analysis['python_files'])}")
        logger.info(f"   Other files: {len(analysis['other_files'])}")

        return analysis

    def remove_result_md_files(self) -> int:
        """Remove unnecessary .md result files."""
        logger.info("🗑️ Removing unnecessary .md result files...")

        removed_count = 0
        if self.executed_dir.exists():
            for md_file in self.executed_dir.glob("*.md"):
                logger.info(f"   Removing: {md_file.name}")
                md_file.unlink()
                removed_count += 1

        logger.info(f"✅ Removed {removed_count} .md result files")
        return removed_count

    def consolidate_executed_notebooks(self) -> int:
        """Consolidate executed notebooks-keep only the latest version of each."""
        logger.info("📦 Consolidating executed notebooks...")

        if not self.executed_dir.exists():
            logger.info("   No executed directory found")
            return 0

        # Group notebooks by base name
        notebook_groups = {}
        for notebook in self.executed_dir.glob("*.ipynb"):
            # Extract base name (remove -executed suffix and any timestamps)
            base_name = notebook.stem
            if base_name.endswith("-executed"):
                base_name = base_name[:-9]  # Remove "-executed"

            # Remove any timestamp patterns
            import re

            base_name = re.sub(r"_\d{8}_\d{6}", "", base_name)

            if base_name not in notebook_groups:
                notebook_groups[base_name] = []
            notebook_groups[base_name].append(notebook)

        removed_count = 0
        for base_name, notebooks in notebook_groups.items():
            if len(notebooks) > 1:
                # Sort by modification time, keep the newest
                notebooks.sort(key=lambda x: x.stat().st_mtime, reverse=True)

                # Keep the first (newest), remove the rest
                for old_notebook in notebooks[1:]:
                    logger.info(f"   Removing old version: {old_notebook.name}")
                    old_notebook.unlink()
                    removed_count += 1

                logger.info(f"   Kept latest: {notebooks[0].name}")

        logger.info(f"✅ Consolidated executed notebooks, removed {removed_count} old versions")
        return removed_count

    def cleanup_excessive_archives(self, keep_recent: int = 3) -> int:
        """Clean up excessive archive directories, keeping only recent ones."""
        logger.info(f"🗂️ Cleaning up archive directories (keeping {keep_recent} recent)...")

        if not self.archive_dir.exists():
            logger.info("   No archive directory found")
            return 0

        # Get all archive directories with timestamps
        archive_dirs = []
        for item in self.archive_dir.iterdir():
            if item.is_dir() and item.name != "first":  # Keep the "first" directory
                try:
                    # Try to parse timestamp from directory name
                    timestamp = datetime.strptime(item.name, "%Y % m%d_ % H%M % S")
                    archive_dirs.append((timestamp, item))
                except ValueError:
                    # Not a timestamp directory, keep it
                    logger.info(f"   Keeping non-timestamp directory: {item.name}")

        # Sort by timestamp, newest first
        archive_dirs.sort(key=lambda x: x[0], reverse=True)

        removed_count = 0
        if len(archive_dirs) > keep_recent:
            # Remove old directories
            for _, old_dir in archive_dirs[keep_recent:]:
                logger.info(f"   Removing old archive: {old_dir.name}")
                shutil.rmtree(old_dir)
                removed_count += 1

        logger.info(f"✅ Cleaned up {removed_count} old archive directories")
        return removed_count

    def organize_notebooks(self) -> Dict[str, Any]:
        """Organize notebooks into a clean structure."""
        logger.info("📁 Organizing notebooks into clean structure...")

        # Ensure we have the essential notebooks
        essential_notebooks = ["MusicScope™_Professional_Dashboard.ipynb", "MusicScope™_20_Chart_Dashboard.ipynb"]

        organization_result = {"essential_notebooks_found": [], "missing_notebooks": [], "extra_notebooks": []}

        for notebook_name in essential_notebooks:
            notebook_path = self.notebooks_dir / notebook_name
            if notebook_path.exists():
                organization_result["essential_notebooks_found"].append(notebook_name)
                logger.info(f"   ✅ Found essential: {notebook_name}")
            else:
                organization_result["missing_notebooks"].append(notebook_name)
                logger.info(f"   ❌ Missing essential: {notebook_name}")

        # Check for extra notebooks
        for notebook in self.notebooks_dir.glob("*.ipynb"):
            if notebook.name not in essential_notebooks:
                organization_result["extra_notebooks"].append(notebook.name)
                logger.info(f"   📝 Extra notebook: {notebook.name}")

        return organization_result

    def create_clean_structure(self) -> None:
        """Create a clean notebook directory structure."""
        logger.info("🏗️ Creating clean notebook directory structure...")

        # Ensure directories exist
        self.notebooks_dir.mkdir(exist_ok=True)

        # Create a simple README if it doesn't exist
        readme_path = self.notebooks_dir / "README.md"
        if not readme_path.exists():
            readme_content = """# MusicScope™ Notebooks

This directory contains the core analytics notebooks for the YouTube ETL system.

## Structure

- `MusicScope™_Professional_Dashboard.ipynb` - Main analytics dashboard
- `MusicScope™_20_Chart_Dashboard.ipynb` - Comprehensive chart dashboard
- `executed/` - Executed notebook outputs (auto-managed)
- `archive/` - Archived notebook versions (auto-managed)

## Usage

Run notebooks directly or use the execution scripts in the tools directory.
"""
            readme_path.write_text(readme_content)
            logger.info("   Created README.md")

    def full_cleanup(self) -> Dict[str, Any]:
        """Perform a full cleanup of the notebooks directory."""
        logger.info("🚀 Starting full notebook cleanup...")
        logger.info("=" * 60)

        # Analyze current state
        initial_state = self.analyze_current_state()

        # Perform cleanup operations
        results = {
            "initial_state": initial_state,
            "md_files_removed": self.remove_result_md_files(),
            "executed_notebooks_consolidated": self.consolidate_executed_notebooks(),
            "archive_directories_cleaned": self.cleanup_excessive_archives(),
            "organization_result": self.organize_notebooks(),
        }

        # Create clean structure
        self.create_clean_structure()

        # Final analysis
        final_state = self.analyze_current_state()
        results["final_state"] = final_state

        logger.info("🎉 Full cleanup complete!")
        logger.info("=" * 60)
        logger.info("📊 Cleanup Summary:")
        logger.info(f"   .md files removed: {results['md_files_removed']}")
        logger.info(f"   Old executed notebooks removed: {results['executed_notebooks_consolidated']}")
        logger.info(f"   Archive directories cleaned: {results['archive_directories_cleaned']}")
        logger.info(f"   Essential notebooks found: {len(results['organization_result']['essential_notebooks_found'])}")

        return results


def main():
    """Run the notebook cleanup."""
    cleanup = NotebookCleanup()

    print("🧹 NOTEBOOK CLEANUP TOOL")
    print("=" * 50)
    print()
    print("This tool will:")
    print("1. Remove unnecessary .md result files")
    print("2. Consolidate executed notebooks (keep latest only)")
    print("3. Clean up excessive archive directories")
    print("4. Organize notebooks into clean structure")
    print()

    response = input("Proceed with cleanup? (y / N): ").strip().lower()
    if response != "y":
        print("Cleanup cancelled.")
        return 1

    try:
        _results = cleanup.full_cleanup()  # noqa: F841

        print("\n✅ CLEANUP COMPLETED SUCCESSFULLY")
        print("\n💡 Recommendations:")
        print("1. Use the notebooks directly from the main directory")
        print("2. Executed versions will be auto-managed in executed/")
        print("3. Archives are kept minimal (3 recent versions)")
        print("4. No more .md result files cluttering the directory")

        return 0

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
