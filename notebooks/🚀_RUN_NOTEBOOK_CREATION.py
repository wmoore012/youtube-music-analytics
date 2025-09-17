#!/usr/bin/env python3
"""
🚀 PLAY BUTTON - Run Notebook Creation Manually

This is your "play button" to create and execute the MusicScope™ Professional Dashboard.

Usage:
    python 🚀_RUN_NOTEBOOK_CREATION.py

What this does:
1. 📦 Archives any old executed notebooks
2. 🔄 Creates/updates the blueprint notebook
3. 🚀 Executes the blueprint to create a new executed version
4. 🔍 Validates the execution for errors
5. ✅ Reports success or 🚨 FAILS LOUDLY with clear errors

The system maintains exactly 2 files in this directory:
- MusicScope™_Professional_Dashboard.ipynb (blueprint)
- MusicScope™_Professional_Dashboard_YYYYMMDD_HHMMSS_executed.ipynb (current execution)

Old executed versions are archived to: archive/YYYYMMDD_HHMMSS/
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to find our modules
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

try:
    from blueprint_execution_system import BlueprintExecutionManager

    print("✅ Blueprint execution system imported successfully")
except ImportError as e:
    print(f"❌ Failed to import blueprint system: {e}")
    print("Make sure you're running this from the notebooks directory")
    sys.exit(1)


def main():
    """Run the complete notebook creation workflow."""
    print("🚀" + "=" * 60)
    print("🎵 MusicScope™ Professional Dashboard - PLAY BUTTON")
    print("🚀" + "=" * 60)
    print()

    # Initialize the blueprint manager
    notebooks_dir = Path(__file__).parent
    manager = BlueprintExecutionManager(notebooks_dir)

    try:
        print("🔄 Starting complete notebook creation workflow...")
        print()

        # Execute the complete workflow
        result = manager.execute_complete_workflow()

        # Report success
        print("🎉" + "=" * 60)
        print("✅ NOTEBOOK CREATION SUCCESSFUL!")
        print("🎉" + "=" * 60)
        print()
        print(f"📄 Blueprint: {result['blueprint_path'].name}")
        print(f"📄 Executed: {result['executed_path'].name}")
        print(f"📦 Archived: {len(result['archived_files'])} old files")
        print(f"🔍 Validation: {result['validation_result']['summary']}")
        print()
        print("🎯 Your beautiful dashboard is ready!")
        print(f"📂 Open: {result['executed_path']}")
        print()

    except Exception as e:
        print("🚨" + "=" * 60)
        print("❌ NOTEBOOK CREATION FAILED!")
        print("🚨" + "=" * 60)
        print()
        print(f"💥 Error: {e}")
        print()
        print("🔧 Common fixes:")
        print("   • Make sure you have all required dependencies installed")
        print("   • Check that your database connection is working")
        print("   • Verify your .env file has the correct settings")
        print("   • Run from the notebooks directory")
        print()
        sys.exit(1)


if __name__ == "__main__":
    main()
