#!/usr/bin/env python3
"""
Fix trailing whitespace in Python files across the codebase.
This is safer than sed for files with various encodings.
"""

import os
from pathlib import Path
import sys
from typing import List


def fix_trailing_whitespace_in_file(file_path: Path) -> bool:
    """
    Fix trailing whitespace in a single file.

    Args:
        file_path: Path to the file to fix

    Returns:
        True if file was modified, False otherwise
    """
    try:
        # Read file with UTF-8 encoding, fallback to latin-1 if needed
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(file_path, "r", encoding="latin-1") as f:
                lines = f.readlines()

        # Fix trailing whitespace
        fixed_lines = []
        modified = False

        for line in lines:
            original_line = line
            # Remove trailing whitespace but preserve newlines
            if line.endswith("\n"):
                fixed_line = line.rstrip() + "\n"
            else:
                fixed_line = line.rstrip()

            if fixed_line != original_line:
                modified = True

            fixed_lines.append(fixed_line)

        # Write back if modified
        if modified:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.writelines(fixed_lines)
            except UnicodeEncodeError:
                with open(file_path, "w", encoding="latin-1") as f:
                    f.writelines(fixed_lines)

            print(f"Fixed trailing whitespace in: {file_path}")
            return True

        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def find_python_files(root_dir: Path) -> List[Path]:
    """Find all Python files in the directory tree."""
    python_files = []

    # Directories to skip
    skip_dirs = {
        ".git",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        "venv",
        ".venv",
        "node_modules",
        "build",
        "dist",
        ".eggs",
        "htmlcov",
        "benchmark_results",
    }

    for file_path in root_dir.rglob("*.py"):
        # Skip files in excluded directories
        if any(skip_dir in file_path.parts for skip_dir in skip_dirs):
            continue

        python_files.append(file_path)

    return python_files


def main():
    """Main function to fix trailing whitespace in all Python files."""
    root_dir = Path(".")

    print("🔧 Finding Python files...")
    python_files = find_python_files(root_dir)
    print(f"Found {len(python_files)} Python files")

    print("\n🧹 Fixing trailing whitespace...")
    modified_count = 0

    for file_path in python_files:
        if fix_trailing_whitespace_in_file(file_path):
            modified_count += 1

    print(f"\n✅ Fixed trailing whitespace in {modified_count} files")

    if modified_count == 0:
        print("🎉 No trailing whitespace found!")
    else:
        print("🎉 All trailing whitespace has been fixed!")


if __name__ == "__main__":
    main()
