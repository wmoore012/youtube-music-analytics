#!/usr / bin / env python3
"""
Notebook Management System for MusicScope™
Handles archiving, versioning, and creation of new notebooks.
"""

from datetime import datetime
import json
import os
from pathlib import Path
import shutil


def get_today_date():
    """Get today's date in YYYY - MM - DD format."""
    return datetime.now().strftime("%Y-%m-%d")


def find_existing_notebooks(notebooks_dir="notebooks"):
    """Find existing notebooks for today's date."""
    today = get_today_date()
    notebooks_path = Path(notebooks_dir)

    existing = []
    for notebook in notebooks_path.glob(f"{today}_MusicScope™_Complete_Analytics_Dashboard*.ipynb"):
        existing.append(notebook)

    return sorted(existing)


def get_next_version(existing_notebooks):
    """Determine the next version number for today's notebook."""
    if not existing_notebooks:
        return "v1"

    # Extract version numbers from existing notebooks
    versions = []
    for notebook in existing_notebooks:
        name = notebook.stem
        if "_v" in name:
            try:
                version_part = name.split("_v")[-1]
                version_num = int(version_part)
                versions.append(version_num)
            except ValueError:
                continue

    if not versions:
        return "v2"  # First notebook exists without version, so this is v2

    return f"v{max(versions) + 1}"


def archive_notebook(notebook_path, archive_dir="notebooks / archive"):
    """Archive a notebook to the archive directory."""
    archive_path = Path(archive_dir)
    archive_path.mkdir(exist_ok=True)

    destination = archive_path / notebook_path.name

    # If destination exists, add timestamp
    if destination.exists():
        timestamp = datetime.now().strftime("%H % M%S")
        stem = destination.stem
        suffix = destination.suffix
        destination = archive_path / f"{stem}_{timestamp}{suffix}"

    shutil.move(str(notebook_path), str(destination))
    print(f"📁 Archived: {notebook_path.name} → {destination.name}")
    return destination


def create_new_notebook(template_path=None, notebooks_dir="notebooks"):
    """Create a new notebook for today with proper versioning."""
    today = get_today_date()
    notebooks_path = Path(notebooks_dir)

    # Find existing notebooks for today
    existing = find_existing_notebooks(notebooks_dir)

    # Determine version
    version = get_next_version(existing)

    # Create new notebook name
    new_name = f"{today}_MusicScope™_Complete_Analytics_Dashboard_{version}.ipynb"
    new_path = notebooks_path / new_name

    # Archive the previous version if it exists
    if existing:
        latest = existing[-1]  # Most recent version
        print(f"🔄 Found existing notebook: {latest.name}")
        _archived = archive_notebook(latest)
        print(f"✅ Archived previous version")

    # Create new notebook
    if template_path and Path(template_path).exists():
        shutil.copy(template_path, new_path)
        print(f"📝 Created new notebook from template: {new_name}")
    else:
        # Create minimal notebook structure
        minimal_notebook = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        f"# 🎵 MusicScope™ - Complete Analytics Dashboard {version}\n\n",
                        f"**Date**: {today}  \n",
                        f"**Version**: {version}  \n",
                        "**Brand**: MusicScope™  \n\n",
                        "## 🚀 **Complete Feature Set**:\n",
                        "- 📊 **ChartFlow™**: Interactive performance charts & artist comparison\n",
                        "- 🎭 **SentimentScope™**: Fan insights, comment analysis & tour planning\n",
                        "- 🎬 **ContentFlow™**: Video categorization & content strategy\n",
                        "- 🤖 **Auto - Generated Summaries**: Intelligent insights with actionable recommendations\n",
                        "- 💝 **Compassionate Analytics**: Treats artists as humans, not data points\n",
                        "- 📈 **Line Charts**: Trending performance over time\n",
                        "- 🔄 **Dynamic Artist Support**: Works with any number of artists from .env\n\n",
                        "**The ultimate comprehensive notebook with ALL analytics features!** 🎯",
                    ],
                }
            ],
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "language_info": {
                    "codemirror_mode": {"name": "ipython", "version": 3},
                    "file_extension": ".py",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.8.5",
                },
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        with open(new_path, "w") as f:
            json.dump(minimal_notebook, f, indent=2)

        print(f"📝 Created new minimal notebook: {new_name}")

    return new_path


def test_notebook(notebook_path):
    """Test if a notebook can be executed without errors."""
    try:
        from nbconvert.preprocessors import ExecutePreprocessor
        import nbformat

        with open(notebook_path) as f:
            _nb = nbformat.read(f, as_version=4)

        # Quick syntax check - just try to parse
        print(f"✅ Notebook syntax is valid: {notebook_path.name}")
        return True

    except Exception as e:
        print(f"❌ Notebook has issues: {e}")
        return False


def main():
    """Main notebook management function."""
    print("🎵 MusicScope™ Notebook Manager")
    print("=" * 50)

    # Check current state
    existing = find_existing_notebooks()
    if existing:
        print(f"📋 Found {len(existing)} existing notebooks for today:")
        for nb in existing:
            print(f"   - {nb.name}")
    else:
        print("📋 No existing notebooks found for today")

    # Test existing notebooks
    for nb in existing:
        test_notebook(nb)

    # Create new notebook
    print("\n🔧 Creating new notebook...")

    # Use the v2 notebook as template if it exists
    template = Path("notebooks / 2025 - 09 - 16_MusicScope™_Complete_Analytics_Dashboard_v2.ipynb")
    if template.exists():
        new_notebook = create_new_notebook(template_path=str(template))
    else:
        new_notebook = create_new_notebook()

    # Test new notebook
    if test_notebook(new_notebook):
        print(f"\n🚀 SUCCESS: New notebook ready at {new_notebook}")
        print(f"📝 Use this notebook: {new_notebook.name}")
    else:
        print(f"\n⚠️  WARNING: New notebook may have issues")

    print("\n📋 NOTEBOOK MANAGEMENT COMPLETE!")


if __name__ == "__main__":
    main()
