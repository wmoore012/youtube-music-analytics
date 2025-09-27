#!/usr / bin / env python3
"""
Fix JSON escape issues in notebooks.
"""

import json
import os
from pathlib import Path
import re


def fix_json_escapes(content):
    """Fix common JSON escape issues in notebook content."""

    # Fix invalid escape sequences in strings
    # Common issues: \n, \t, \', \" in Python strings within JSON

    # Replace problematic escape sequences
    fixes = [
        (r"\\n", r"\\\\n"),  # Fix \n -> \\n
        (r"\\t", r"\\\\t"),  # Fix \t -> \\t
        (r"\\\'", r"\\\\\'"),  # Fix \' -> \\'
        (r'\\"', r'\\\\"'),  # Fix \" -> \\"
        (r"\\\\\\\\", r"\\\\"),  # Fix quadruple backslashes
    ]

    for pattern, replacement in fixes:
        content = re.sub(pattern, replacement, content)

    return content


def fix_notebook(notebook_path):
    """Fix a single notebook file."""
    print(f"Fixing: {notebook_path}")

    try:
        # Read the file as text first
        with open(notebook_path, "r", encoding="utf - 8") as f:
            content = f.read()

        # Try to parse as JSON
        try:
            notebook_data = json.loads(content)
            print(f"  ✅ Already valid JSON: {notebook_path}")
            return True
        except json.JSONDecodeError as e:
            print(f"  🔧 Fixing JSON errors in: {notebook_path}")
            print(f"     Error: {e}")

            # Apply fixes
            fixed_content = fix_json_escapes(content)

            # Try parsing again
            try:
                notebook_data = json.loads(fixed_content)

                # Write back the fixed content
                with open(notebook_path, "w", encoding="utf - 8") as f:
                    json.dump(notebook_data, f, indent=2, ensure_ascii=False)

                print(f"  ✅ Fixed and saved: {notebook_path}")
                return True

            except json.JSONDecodeError as e2:
                print(f"  ❌ Still invalid after fixes: {notebook_path}")
                print(f"     Error: {e2}")

                # Create a minimal valid notebook as fallback
                minimal_notebook = {
                    "cells": [
                        {
                            "cell_type": "markdown",
                            "metadata": {},
                            "source": [
                                "# Notebook Repair Required\\n",
                                "\\n",
                                "This notebook had JSON formatting issues and needs to be recreated.\\n",
                                "Please regenerate this notebook using the appropriate execute script.",
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

                with open(notebook_path, "w", encoding="utf - 8") as f:
                    json.dump(minimal_notebook, f, indent=2)

                print(f"  🔄 Created minimal notebook: {notebook_path}")
                return False

    except Exception as e:
        print(f"  ❌ Error processing {notebook_path}: {e}")
        return False


def main():
    """Fix all notebooks with JSON issues."""
    print("🔧 FIXING NOTEBOOK JSON ISSUES")
    print("=" * 40)

    notebook_dirs = ["notebooks"]

    fixed_count = 0
    total_count = 0

    for notebook_dir in notebook_dirs:
        if not os.path.exists(notebook_dir):
            continue

        print(f"\n📁 Processing: {notebook_dir}")

        for notebook_file in Path(notebook_dir).glob("*.ipynb"):
            total_count += 1
            if fix_notebook(notebook_file):
                fixed_count += 1

    print(f"\n🎉 NOTEBOOK JSON FIX COMPLETE")
    print(f"   Fixed: {fixed_count}/{total_count} notebooks")

    if fixed_count == total_count:
        print("✅ All notebooks have valid JSON")
    else:
        print("⚠️  Some notebooks may need manual recreation")


if __name__ == "__main__":
    main()
