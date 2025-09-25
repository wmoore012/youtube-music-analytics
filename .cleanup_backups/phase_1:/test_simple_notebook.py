#!/usr/bin/env python3
"""Simple notebook execution test."""

import json
import subprocess

# Create a simple test notebook
simple_notebook = {
    "cells": [
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": ["print('Hello from notebook!')\n", "print('This is a test')"],
        }
    ],
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.8.0"},
    },
    "nbformat": 4,
    "nbformat_minor": 4,
}

# Save test notebook
with open("test_notebook.ipynb", "w") as f:
    json.dump(simple_notebook, f, indent=2)

print("📄 Created test notebook")

# Execute it
result = subprocess.run(
    ["jupyter", "nbconvert", "--to", "notebook", "--execute", "--inplace", "test_notebook.ipynb"],
    capture_output=True,
    text=True,
)

print(f"🚀 Execution result: {result.returncode}")
if result.stderr:
    print(f"❌ Error: {result.stderr}")

# Check results
with open("test_notebook.ipynb", "r") as f:
    executed_nb = json.load(f)

outputs = executed_nb["cells"][0].get("outputs", [])
print(f"📊 Outputs found: {len(outputs)}")

if outputs:
    for output in outputs:
        if "text" in output:
            print(f"✅ Output: {''.join(output['text']).strip()}")
else:
    print("❌ No outputs found")
