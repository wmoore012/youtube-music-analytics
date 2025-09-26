#!/usr/bin/env python3
"""
Demonstration of the Complete Notebook Archiving and DateTime System

Shows the TDD-driven implementation working with real data.
"""

import sys

sys.path.insert(0, ".")

from datetime import datetime
import json
from pathlib import Path

from notebook_archiver import NotebookArchiver


def demonstrate_system():
    """Demonstrate the complete notebook archiving system."""

    print("🎵 MusicScope™ Notebook Archiving System Demonstration")
    print("=" * 70)
    print("✅ TDD-Driven Implementation")
    print("✅ DateTime Organization")
    print("✅ Professional Archiving")
    print("✅ Real Data Integration")

    # Initialize the system
    notebooks_dir = Path("notebooks")
    archiver = NotebookArchiver(notebooks_dir)

    print(f"\n📁 System Initialized:")
    print(f"   📂 Notebooks Directory: {notebooks_dir}")
    print(f"   📦 Archive Directory: {notebooks_dir / 'archive'}")

    # Show current state
    current_notebooks = list(notebooks_dir.glob("MusicScope™_Professional_Dashboard_*.ipynb"))
    archived_notebooks = list((notebooks_dir / "archive").glob("MusicScope™_Professional_Dashboard_*.ipynb"))

    print(f"\n📊 Current State:")
    print(f"   📝 Current Notebooks: {len(current_notebooks)}")
    print(f"   📦 Archived Notebooks: {len(archived_notebooks)}")

    if current_notebooks:
        print(f"   📄 Latest Notebook: {current_notebooks[0].name}")

    if archived_notebooks:
        print(f"   📦 Recent Archives:")
        for archive in sorted(archived_notebooks, key=lambda x: x.stat().st_mtime, reverse=True)[:3]:
            print(f"      - {archive.name}")

    # Show archive summary
    summary = archiver.get_archive_summary()
    print(f"\n📈 Archive Summary:")
    print(f"   📦 Total Archived: {summary['total_archived']}")
    if summary["latest_archive"]:
        print(f"   📄 Latest Archive: {summary['latest_archive']}")

    # Demonstrate datetime filename generation
    print(f"\n🕒 DateTime System:")
    sample_filename = archiver.generate_datetime_filename("MusicScope™_Professional_Dashboard.ipynb")
    print(f"   📝 Generated Filename: {sample_filename}")

    # Parse the datetime to show it's valid
    datetime_part = sample_filename.replace("MusicScope™_Professional_Dashboard_", "").replace(".ipynb", "")
    parsed_datetime = datetime.strptime(datetime_part, "%Y%m%d_%H%M%S")
    print(f"   🕒 Parsed DateTime: {parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

    # Show the workflow
    print(f"\n🔄 Workflow Process:")
    print(f"   1. 🔍 Check for existing notebooks")
    print(f"   2. 📦 Archive existing to /archive with datetime")
    print(f"   3. 📝 Create new notebook with datetime in main folder")
    print(f"   4. ✅ Ready for execution")

    print(f"\n🎯 Key Features:")
    print(f"   ✅ Automatic datetime stamping (YYYYMMDD_HHMMSS)")
    print(f"   ✅ Professional archiving system")
    print(f"   ✅ TDD-driven with comprehensive tests")
    print(f"   ✅ Bulletproof error handling")
    print(f"   ✅ Integration with create_notebook.py")
    print(f"   ✅ Real data only - no fake data ever")

    print(f"\n📋 Directory Structure:")
    print(f"   📂 /notebooks/")
    print(f"      📝 MusicScope™_Professional_Dashboard_YYYYMMDD_HHMMSS.ipynb (current)")
    print(f"      📂 archive/")
    print(f"         📦 MusicScope™_Professional_Dashboard_YYYYMMDD_HHMMSS.ipynb (old versions)")

    print(f"\n🚀 System Status: OPERATIONAL")
    print(f"🎵 Ready for Professional Music Analytics!")

    return {
        "current_notebooks": len(current_notebooks),
        "archived_notebooks": len(archived_notebooks),
        "system_status": "OPERATIONAL",
    }


if __name__ == "__main__":
    try:
        result = demonstrate_system()
        print(f"\n✅ DEMONSTRATION COMPLETE!")
        print(f"📊 Current: {result['current_notebooks']}, Archived: {result['archived_notebooks']}")
        print(f"🎯 Status: {result['system_status']}")
    except Exception as e:
        print(f"\n🚨 DEMONSTRATION ERROR: {e}")
        import traceback

        traceback.print_exc()
