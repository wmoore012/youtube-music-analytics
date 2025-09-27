#!/usr / bin / env python3
"""
Fix F - String Syntax Errors in Notebook

This script fixes the f - string syntax errors in the MusicScope™ Professional Dashboard notebook
where single quotes are used inside f - strings that are also delimited by single quotes.
"""

import json
from pathlib import Path
import re


def fix_fstring_syntax(text):
    """Fix f - string syntax errors by replacing single quotes with double quotes inside f - strings."""

    # Pattern to match problematic f - strings with lists containing single quotes
    # Example: f'   🔍 Required columns: ['artist_name', 'sentiment_score', 'comment_text']'
    pattern = r"f'([^']*🔍 Required columns: \[)([^']*)'([^']*)'([^']*)'([^']*)\]([^']*)"

    def replace_quotes(match):
        # Replace single quotes with double quotes in the list part
        prefix = match.group(1)
        col1 = match.group(2)
        col2 = match.group(4)
        col3 = match.group(5)
        suffix = match.group(6)

        # Reconstruct with double quotes
        return f'f\'{prefix}["{col1.strip()}", "{col2.strip()}", "{col3.strip()}"]{suffix}\''

    # Apply the fix
    fixed_text = re.sub(pattern, replace_quotes, text)

    # Also fix the simpler case with just two columns
    pattern2 = r"f'([^']*🔍 Required columns: \[)([^']*)'([^']*)'([^']*)\]([^']*)"

    def replace_quotes2(match):
        prefix = match.group(1)
        col1 = match.group(2)
        col2 = match.group(4)
        suffix = match.group(5)
        return f'f\'{prefix}["{col1.strip()}", "{col2.strip()}"]]{suffix}\''

    fixed_text = re.sub(pattern2, replace_quotes2, fixed_text)

    return fixed_text


def fix_notebook_syntax(notebook_path):
    """Fix syntax errors in the notebook file."""

    print(f"🔧 Fixing syntax errors in {notebook_path}")

    # Read the notebook
    with open(notebook_path, "r", encoding="utf - 8") as f:
        notebook = json.load(f)

    fixes_made = 0

    # Process each cell
    for i, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] == "code":
            # Get the source code
            source_lines = cell["source"]

            # Join lines to work with the full source
            source_text = "".join(source_lines)

            # Apply fixes
            fixed_source = fix_fstring_syntax(source_text)

            # Check if changes were made
            if fixed_source != source_text:
                print(f"  📝 Fixed syntax in cell {i}")

                # Split back into lines, preserving original line structure
                fixed_lines = []
                current_pos = 0

                for original_line in source_lines:
                    line_length = len(original_line)
                    fixed_line = fixed_source[current_pos : current_pos + line_length]
                    fixed_lines.append(fixed_line)
                    current_pos += line_length

                # Update the cell
                cell["source"] = fixed_lines
                fixes_made += 1

    if fixes_made > 0:
        # Save the fixed notebook
        with open(notebook_path, "w", encoding="utf - 8") as f:
            json.dump(notebook, f, indent=1, ensure_ascii=False)

        print(f"✅ Fixed {fixes_made} syntax errors in {notebook_path}")
    else:
        print(f"ℹ️  No syntax errors found in {notebook_path}")

    return fixes_made


def main():
    """Main function to fix notebook syntax errors."""

    notebook_path = Path("notebooks / MusicScope™_Professional_Dashboard.ipynb")

    if not notebook_path.exists():
        print(f"❌ Notebook not found: {notebook_path}")
        return

    try:
        fixes_made = fix_notebook_syntax(notebook_path)

        if fixes_made > 0:
            print(f"\n🎉 Successfully fixed {fixes_made} syntax errors!")
            print("📝 The notebook should now run without syntax errors.")
        else:
            print("\n✅ No syntax errors found to fix.")

    except Exception as e:
        print(f"❌ Error fixing notebook: {e}")
        raise


if __name__ == "__main__":
    main()
