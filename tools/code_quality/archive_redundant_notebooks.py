#!/usr/bin/env python3
"""
📓 Archive Redundant Notebooks

Moves redundant/executed notebook versions to archive, keeping only the active ones.
Based on analysis: keep 20-Chart Dashboard and Professional Dashboard as the main ones.
"""

import os
from pathlib import Path
import shutil


def main():
    """Archive redundant notebook files."""
    print("📓 Archiving Redundant Notebooks")
    print("=" * 50)

    # Define notebooks to archive (redundant/executed versions)
    notebooks_to_archive = [
        "MusicScope™_Real_Data_Dashboard_executed.ipynb",  # Executed version
        "MusicScope™_Simple_Dashboard.ipynb",  # Simpler version, superseded
        "Simple_Scoring_Demo.ipynb",  # Demo version
        "Simple_Scoring_Demo_executed.ipynb",  # Executed demo
        "Scoring_System_Real_Data_Demo.ipynb",  # Demo version
        "Scoring_System_Real_Data_Demo_executed.ipynb",  # Executed demo
        "Validated_Analytics_Dashboard.ipynb",  # Older version
        "Validated_Analytics_Dashboard_executed.ipynb",  # Executed older version
    ]

    # Keep these active notebooks
    keep_notebooks = [
        "MusicScope™_20_Chart_Dashboard.ipynb",  # Main 20-chart dashboard
        "MusicScope™_Professional_Dashboard.ipynb",  # Professional version
        "🔧_CHECK_DEPENDENCIES.py",  # Utility script
        "🚀_RUN_NOTEBOOK_CREATION.py",  # Utility script
        "README.md",  # Documentation
        "run_dashboard.sh",  # Shell script
    ]

    # Create archive directory
    notebooks_archive = Path("archive/cleanup_2025_09_25/notebooks")
    notebooks_archive.mkdir(parents=True, exist_ok=True)

    notebooks_dir = Path("notebooks")
    archived_count = 0

    print("📦 Archiving redundant notebooks...")
    for notebook in notebooks_to_archive:
        notebook_path = notebooks_dir / notebook
        if notebook_path.exists():
            print(f"   📋 {notebook}")
            shutil.move(str(notebook_path), str(notebooks_archive / notebook))
            archived_count += 1
        else:
            print(f"   ⚠️  Not found: {notebook}")

    print(f"\n✅ Keeping active notebooks:")
    for notebook in keep_notebooks:
        notebook_path = notebooks_dir / notebook
        if notebook_path.exists():
            print(f"   📊 {notebook}")

    # Check what's left in notebooks directory
    remaining_notebooks = []
    if notebooks_dir.exists():
        for item in notebooks_dir.iterdir():
            if item.is_file() and item.name.endswith((".ipynb", ".py", ".md", ".sh")):
                if item.name not in keep_notebooks:
                    remaining_notebooks.append(item.name)

    if remaining_notebooks:
        print(f"\n🤔 Other files still in notebooks/ directory:")
        for nb in remaining_notebooks:
            print(f"   📄 {nb}")

    print(f"\n📋 Archive Summary:")
    print(f"   📓 Notebooks archived: {archived_count}")
    print(f"   📊 Active notebooks kept: {len([nb for nb in keep_notebooks if (notebooks_dir / nb).exists()])}")
    print(f"   📁 Archive location: {notebooks_archive}")

    print(f"\n🎯 Result: Clean notebooks directory with only essential files!")


if __name__ == "__main__":
    main()
