#!/usr/bin/env python3
"""
Test blueprint execution system with a simple notebook that doesn't require complex dependencies
"""

import json
import tempfile
import unittest
from pathlib import Path

from blueprint_execution_system import BlueprintExecutionManager


class TestBlueprintSystemSimple(unittest.TestCase):
    def setUp(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.notebooks_dir = Path(self.temp_dir) / "notebooks"
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)

    def test_simple_blueprint_execution_and_validation(self):
        """Test the complete blueprint system with a simple notebook."""
        # Create a simple blueprint notebook that mimics the structure but without complex dependencies
        simple_blueprint = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "# 🎵 Simple Test Dashboard\n",
                        "\n",
                        "**Generated:** Test execution  \n",
                        "**Status:** Testing blueprint system  \n",
                        "\n",
                        "This notebook tests the blueprint execution system without complex dependencies.",
                    ],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# 🚀 Simple Bootstrap\n",
                        "import sys\n",
                        "import os\n",
                        "from datetime import datetime\n",
                        "\n",
                        "print('🎵 Simple Test Dashboard Initialized!')\n",
                        "print('✅ No complex dependencies')\n",
                        "print('✅ Basic Python only')\n",
                        "print(f'📅 Current time: {datetime.now()}')\n",
                        "print(f'🐍 Python version: {sys.version.split()[0]}')",
                    ],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# 🔍 Simple Data Discovery\n",
                        "print('🔍 Connecting to test data...')\n",
                        "\n",
                        "# Simulate data discovery\n",
                        "test_data = {\n",
                        "    'artists': ['Artist A', 'Artist B', 'Artist C'],\n",
                        "    'videos': 100,\n",
                        "    'comments': 5000\n",
                        "}\n",
                        "\n",
                        "print(f'📋 Found test data')\n",
                        'print(f\'🎭 Found {len(test_data["artists"])} test artists: {test_data["artists"]}\')\n',
                        "print(f'📊 Test Videos: {test_data[\"videos\"]:,}')\n",
                        "print(f'💬 Test Comments: {test_data[\"comments\"]:,}')\n",
                        "print('✅ ISRC data: Available (simulated)')\n",
                        "\n",
                        "print('\\n🎯 Simple Data Discovery Complete!')\n",
                        "print(f'Ready to generate test charts with data from {len(test_data[\"artists\"])} artists')",
                    ],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# 📊 Simple Chart Generation\n",
                        "print('📊 Generating simple charts...')\n",
                        "\n",
                        "# Simulate chart generation\n",
                        "charts_generated = []\n",
                        "for i in range(1, 21):\n",
                        "    chart_name = f'Chart {i}: Test Visualization'\n",
                        "    charts_generated.append(chart_name)\n",
                        "    print(f'✅ {chart_name} - Generated successfully')\n",
                        "\n",
                        "print(f'\\n🎉 Generated {len(charts_generated)} beautiful charts!')\n",
                        "print('✅ All charts completed successfully')\n",
                        "print('🎯 Simple dashboard generation complete!')",
                    ],
                },
            ],
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "language_info": {"name": "python", "version": "3.8.0"},
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Save the simple blueprint
        blueprint_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard.ipynb"
        with open(blueprint_path, "w") as f:
            json.dump(simple_blueprint, f, indent=2)

        # Create old executed version to test archiving
        old_executed_content = {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 1,
                    "metadata": {},
                    "outputs": [{"name": "stdout", "output_type": "stream", "text": ["Old executed version output\n"]}],
                    "source": ["print('Old executed version output')"],
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        old_executed_path = self.notebooks_dir / "MusicScope™_Professional_Dashboard_20250916_120000_executed.ipynb"
        with open(old_executed_path, "w") as f:
            json.dump(old_executed_content, f)

        # Test the complete blueprint execution system
        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Verify old file exists before workflow
        self.assertTrue(old_executed_path.exists())

        # Execute complete workflow
        result = manager.execute_complete_workflow()

        # Verify workflow results
        self.assertTrue(result["success"])
        self.assertTrue(result["blueprint_path"].exists())
        self.assertTrue(result["executed_path"].exists())
        self.assertEqual(len(result["archived_files"]), 1)
        self.assertTrue(result["validation_result"]["success"])

        # Verify old file was archived
        self.assertFalse(old_executed_path.exists())

        # Verify archive directory was created
        archive_dirs = list((self.notebooks_dir / "archive").glob("*"))
        self.assertEqual(len(archive_dirs), 1)

        # Verify archived file exists in archive directory
        archived_files = list(archive_dirs[0].glob("*.ipynb"))
        self.assertEqual(len(archived_files), 1)

        # Verify executed notebook has outputs
        with open(result["executed_path"], "r") as f:
            executed_notebook = json.load(f)

        code_cells = [cell for cell in executed_notebook["cells"] if cell["cell_type"] == "code"]
        self.assertTrue(len(code_cells) > 0)

        # Check that at least one code cell has outputs
        has_outputs = any(cell.get("outputs", []) for cell in code_cells)
        self.assertTrue(has_outputs, "Executed notebook should have cell outputs")

        print(f"✅ Complete blueprint system test passed!")
        print(f"   📄 Blueprint: {result['blueprint_path']}")
        print(f"   📄 Executed: {result['executed_path']}")
        print(f"   📦 Archived: {len(result['archived_files'])} files")
        print(f"   🔍 Validation: {result['validation_result']['summary']}")


if __name__ == "__main__":
    unittest.main()
