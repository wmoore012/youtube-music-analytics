#!/usr/bin/env python3
"""
Test simple notebook execution without src module dependencies
"""

import json
from pathlib import Path
import tempfile
import unittest

from blueprint_execution_system import BlueprintExecutionManager


class TestSimpleNotebookExecution(unittest.TestCase):
    def setUp(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.notebooks_dir = Path(self.temp_dir) / "notebooks"
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)

    def test_simple_notebook_execution(self):
        """Test execution of a simple notebook without src dependencies."""
        # Create a simple test notebook
        simple_notebook = {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        "# Simple Test Notebook\n",
                        "This notebook tests basic execution without external dependencies.",
                    ],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [
                        "# Simple Python code that should work\n",
                        "import sys\n",
                        "import os\n",
                        "from datetime import datetime\n",
                        "\n",
                        "print('✅ Basic imports successful')\n",
                        "print(f'📅 Current time: {datetime.now()}')\n",
                        "print(f'🐍 Python version: {sys.version.split()[0]}')\n",
                        "print(f'📁 Working directory: {os.getcwd()}')\n",
                        "\n",
                        "# Test some basic operations\n",
                        "numbers = [1, 2, 3, 4, 5]\n",
                        "result = sum(numbers)\n",
                        "print(f'🔢 Sum of {numbers} = {result}')\n",
                        "\n",
                        "# Success indicator\n",
                        "print('🎉 Simple notebook execution successful!')",
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

        # Save the simple notebook
        simple_notebook_path = self.notebooks_dir / "Simple_Test_Notebook.ipynb"
        with open(simple_notebook_path, "w") as f:
            json.dump(simple_notebook, f, indent=2)

        # Test execution using BlueprintExecutionManager
        manager = BlueprintExecutionManager(self.notebooks_dir)

        # Manually execute the simple notebook
        executed_path = manager.execute_blueprint_file(simple_notebook_path)

        # Verify executed file was created
        self.assertTrue(executed_path.exists())

        # Load and verify executed notebook has outputs
        with open(executed_path, "r") as f:
            executed_notebook = json.load(f)

        # Check that code cells have outputs
        code_cells = [cell for cell in executed_notebook["cells"] if cell["cell_type"] == "code"]
        self.assertTrue(len(code_cells) > 0)

        # Check that at least one code cell has outputs
        has_outputs = any(cell.get("outputs", []) for cell in code_cells)
        self.assertTrue(has_outputs, "Executed notebook should have cell outputs")

        print(f"✅ Simple notebook execution test passed!")
        print(f"   📄 Original: {simple_notebook_path}")
        print(f"   📄 Executed: {executed_path}")


if __name__ == "__main__":
    unittest.main()
