#!/usr/bin/env python3
"""
Debug nbconvert execution environment
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Create a simple debug notebook
debug_notebook = {
    "cells": [
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "import os\n",
                "import sys\n",
                "print(f'🔍 Current working directory: {os.getcwd()}')\n",
                "print(f'🐍 Python executable: {sys.executable}')\n",
                "print(f'📁 sys.path first 5 entries:')\n",
                "for i, path in enumerate(sys.path[:5]):\n",
                "    print(f'  {i}: {path}')\n",
                "\n",
                "# Test path manipulation\n",
                "sys.path.insert(0, '..')\n",
                "print(f'\\n📁 After sys.path.insert(0, \"..\"):')\n",
                "for i, path in enumerate(sys.path[:5]):\n",
                "    print(f'  {i}: {path}')\n",
                "\n",
                "# Test if src is visible\n",
                "import importlib.util\n",
                "src_path = os.path.join('..', 'src')\n",
                "print(f'\\n🔍 Looking for src at: {os.path.abspath(src_path)}')\n",
                "print(f'📂 src directory exists: {os.path.exists(src_path)}')\n",
                "if os.path.exists(src_path):\n",
                "    print(f'📂 src contents: {os.listdir(src_path)}')\n",
                "\n",
                "# Try to import src\n",
                "try:\n",
                "    import src\n",
                "    print('✅ Successfully imported src')\n",
                "    print(f'📍 src location: {src.__file__}')\n",
                "except ImportError as e:\n",
                "    print(f'❌ Failed to import src: {e}')\n",
                "\n",
                "# Try to import src.youtubeviz\n",
                "try:\n",
                "    import src.youtubeviz\n",
                "    print('✅ Successfully imported src.youtubeviz')\n",
                "except ImportError as e:\n",
                "    print(f'❌ Failed to import src.youtubeviz: {e}')",
            ],
        }
    ],
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.8.0"},
    },
    "nbformat": 4,
    "nbformat_minor": 4,
}

# Create temp directory and notebook
temp_dir = Path(tempfile.mkdtemp())
notebooks_dir = temp_dir / "notebooks"
notebooks_dir.mkdir(parents=True, exist_ok=True)

debug_notebook_path = notebooks_dir / "debug_notebook.ipynb"
with open(debug_notebook_path, "w") as f:
    json.dump(debug_notebook, f, indent=2)

print(f"🔧 Created debug notebook at: {debug_notebook_path}")
print(f"🔧 Running from: {temp_dir}")

# Execute notebook using nbconvert
executed_path = notebooks_dir / "debug_notebook_executed.ipynb"

cmd = [
    sys.executable,
    "-m",
    "nbconvert",
    "--to",
    "notebook",
    "--execute",
    "--output",
    str(executed_path),
    str(debug_notebook_path),
    "--ExecutePreprocessor.timeout=60",
]

# Set environment
env = os.environ.copy()
env["PYTHONPATH"] = str(temp_dir)

print(f"🚀 Executing nbconvert...")
print(f"   📄 Command: {' '.join(cmd)}")
print(f"   📁 CWD: {temp_dir}")
print(f"   🌍 PYTHONPATH: {env.get('PYTHONPATH', 'Not set')}")

result = subprocess.run(cmd, capture_output=True, text=True, cwd=temp_dir, env=env, timeout=120)

print(f"\n📊 Results:")
print(f"   💥 Return code: {result.returncode}")
if result.stdout:
    print(f"   📤 STDOUT: {result.stdout}")
if result.stderr:
    print(f"   📤 STDERR: {result.stderr}")

if executed_path.exists():
    print(f"\n✅ Executed notebook created successfully!")
    # Read and show the outputs
    with open(executed_path, "r") as f:
        executed_notebook = json.load(f)

    for cell in executed_notebook["cells"]:
        if cell["cell_type"] == "code" and cell.get("outputs"):
            print(f"\n📋 Cell outputs:")
            for output in cell["outputs"]:
                if output.get("output_type") == "stream" and output.get("name") == "stdout":
                    for line in output.get("text", []):
                        print(f"   {line.rstrip()}")
else:
    print(f"\n❌ Executed notebook was not created")

# Cleanup
import shutil

shutil.rmtree(temp_dir)
print(f"\n🧹 Cleaned up temp directory: {temp_dir}")
