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
    # Try to import the blueprint system
    from blueprint_execution_system import BlueprintExecutionManager
    print("✅ Blueprint execution system imported successfully")
    BLUEPRINT_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Blueprint system not available: {e}")
    BLUEPRINT_AVAILABLE = False

# Try to import auto-installer for dependency checking
try:
    from src.youtubeviz.auto_install import ensure_packages, ANALYTICS_ESSENTIALS, JUPYTER_ESSENTIALS
    AUTO_INSTALL_AVAILABLE = True
except ImportError:
    AUTO_INSTALL_AVAILABLE = False


def check_and_install_dependencies():
    """Check and optionally install required dependencies."""
    if not AUTO_INSTALL_AVAILABLE:
        print("⚠️  Auto-installer not available - skipping dependency check")
        return True
    
    print("📦 Checking essential dependencies...")
    
    # Check essential packages
    essential_packages = {
        'pandas': 'pandas',
        'numpy': 'numpy', 
        'plotly': 'plotly',
        'nbconvert': 'nbconvert',
        'pydantic': 'pydantic'
    }
    
    results = ensure_packages('pandas', 'numpy', 'plotly', 'nbconvert')
    
    missing = [pkg for pkg, module in results.items() if module is None]
    
    if missing:
        print(f"❌ Missing essential packages: {', '.join(missing)}")
        return False
    else:
        print("✅ All essential dependencies available")
        return True


def create_simple_notebook():
    """Create a simple notebook if blueprint system is not available."""
    print("📝 Creating simple analytics notebook...")
    
    notebook_content = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# 🎵 MusicScope™ Analytics Dashboard\n",
                    "\n",
                    "Simple analytics dashboard for YouTube music data.\n",
                    "\n",
                    "## 📊 Quick Analytics\n"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# 📦 Import essential packages\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "import plotly.express as px\n",
                    "import plotly.graph_objects as go\n",
                    "from datetime import datetime, timedelta\n",
                    "\n",
                    "print('📊 Analytics packages loaded successfully!')\n",
                    "print(f'🕐 Generated at: {datetime.now()}')"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# 🎯 Sample analytics data\n",
                    "sample_data = pd.DataFrame({\n",
                    "    'date': pd.date_range('2024-01-01', periods=30, freq='D'),\n",
                    "    'views': np.random.randint(1000, 10000, 30),\n",
                    "    'engagement': np.random.uniform(0.02, 0.08, 30)\n",
                    "})\n",
                    "\n",
                    "print(f'📈 Generated {len(sample_data)} days of sample data')\n",
                    "sample_data.head()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# 📊 Create visualization\n",
                    "fig = px.line(sample_data, x='date', y='views', \n",
                    "              title='📈 Views Over Time',\n",
                    "              labels={'views': 'Daily Views', 'date': 'Date'})\n",
                    "\n",
                    "fig.update_layout(\n",
                    "    template='plotly_white',\n",
                    "    height=400\n",
                    ")\n",
                    "\n",
                    "fig.show()\n",
                    "print('✅ Chart created successfully!')"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    # Save the notebook
    notebooks_dir = Path(__file__).parent
    notebook_path = notebooks_dir / "MusicScope™_Simple_Dashboard.ipynb"
    
    import json
    with open(notebook_path, 'w') as f:
        json.dump(notebook_content, f, indent=2)
    
    print(f"✅ Simple notebook created: {notebook_path.name}")
    return notebook_path


def main():
    """Run the complete notebook creation workflow."""
    print("🚀" + "=" * 60)
    print("🎵 MusicScope™ Professional Dashboard - PLAY BUTTON")
    print("🚀" + "=" * 60)
    print()

    # Check dependencies first
    if not check_and_install_dependencies():
        print("🚨 Dependency check failed!")
        print("💡 Try running: python 🔧_CHECK_DEPENDENCIES.py --auto-install")
        sys.exit(1)

    if BLUEPRINT_AVAILABLE:
        # Use the full blueprint system
        try:
            print("🔄 Starting complete notebook creation workflow...")
            print()

            # Initialize the blueprint manager
            notebooks_dir = Path(__file__).parent
            manager = BlueprintExecutionManager(notebooks_dir)

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
            
            # Fallback to simple notebook
            print("🔄 Falling back to simple notebook creation...")
            try:
                notebook_path = create_simple_notebook()
                print(f"✅ Simple notebook created as fallback: {notebook_path}")
            except Exception as fallback_error:
                print(f"❌ Fallback also failed: {fallback_error}")
                sys.exit(1)
    else:
        # Blueprint system not available, create simple notebook
        print("📝 Blueprint system not available - creating simple notebook...")
        try:
            notebook_path = create_simple_notebook()
            print()
            print("🎉" + "=" * 60)
            print("✅ SIMPLE NOTEBOOK CREATED!")
            print("🎉" + "=" * 60)
            print()
            print(f"📄 Notebook: {notebook_path.name}")
            print("💡 For full features, ensure blueprint_execution_system.py is available")
            print()
        except Exception as e:
            print(f"❌ Simple notebook creation failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
