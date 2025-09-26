#!/usr/bin/env python3
"""
Script to fix all icatalogviz imports to youtubeviz in notebooks
"""
import glob
import os
import re


def fix_imports_in_file(filepath):
    """Fix imports in a single file"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Replace icatalogviz with youtubeviz
        original_content = content
        content = content.replace("icatalogviz", "youtubeviz")
        content = content.replace("src.youtubeviz", "youtubeviz")

        # Add path setup for notebooks that need it
        if filepath.endswith(".ipynb") and "from web.etl_helpers import" in content:
            # Check if path setup is already there
            if "sys.path.append" not in content:
                # Find the first import cell and add path setup
                import_pattern = r'("import pandas as pd\\n")'
                replacement = r'"# Setup Python path for imports\\n",\n    "import sys\\n",\n    "import os\\n",\n    "sys.path.append(os.path.abspath(\'.\'))\\n",\n    "sys.path.append(os.path.abspath(\'../..\'))\\n",\n    "\\n",\n    \1'
                content = re.sub(import_pattern, replacement, content)

        if content != original_content:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"✅ Fixed imports in: {filepath}")
            return True
        else:
            print(f"⚪ No changes needed: {filepath}")
            return False

    except Exception as e:
        print(f"❌ Error fixing {filepath}: {e}")
        return False


def main():
    """Fix imports in all notebooks"""

    # Find all notebook files
    notebook_patterns = ["notebooks/**/*.ipynb", "notebooks/*.ipynb"]

    files_to_fix = []
    for pattern in notebook_patterns:
        files_to_fix.extend(glob.glob(pattern, recursive=True))

    # Remove duplicates and sort
    files_to_fix = sorted(set(files_to_fix))

    print(f"🔍 Found {len(files_to_fix)} notebook files to check")

    fixed_count = 0
    for filepath in files_to_fix:
        if fix_imports_in_file(filepath):
            fixed_count += 1

    print(f"\n🎉 Fixed imports in {fixed_count}/{len(files_to_fix)} files")


if __name__ == "__main__":
    main()
