#!/usr / bin / env python3
"""
Script to automatically fix common linting issues in the codebase.
"""

import os
import re
import subprocess
from pathlib import Path


def fix_trailing_whitespace():
    """Remove trailing whitespace from all Python files."""
    print("🧹 Fixing trailing whitespace...")

    for root, dirs, files in os.walk("."):
        # Skip certain directories
        if any(skip in root for skip in [".git", "__pycache__", ".venv", "node_modules"]):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()

                    # Remove trailing whitespace
                    lines = content.splitlines()
                    fixed_lines = [line.rstrip() for line in lines]
                    fixed_content = "\n".join(fixed_lines)

                    # Add final newline if missing
                    if fixed_content and not fixed_content.endswith("\n"):
                        fixed_content += "\n"

                    if content != fixed_content:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(fixed_content)
                        print(f"   Fixed: {file_path}")

                except Exception as e:
                    print(f"   Error fixing {file_path}: {e}")


def fix_arithmetic_operators():
    """Add missing whitespace around arithmetic operators."""
    print("🔧 Fixing arithmetic operators...")

    # Pattern to match arithmetic operators without proper spacing
    patterns = [
        (r"(\w)(\+)(\w)", r"\1 \2 \3"),  # word + word -> word + word
        (r"(\w)(\-)(\w)", r"\1 \2 \3"),  # word-word -> word-word
        (r"(\w)(\*)(\w)", r"\1 \2 \3"),  # word * word -> word * word
        (r"(\w)(/)(\w)", r"\1 \2 \3"),  # word / word -> word / word
        (r"(\w)(\%)(\w)", r"\1 \2 \3"),  # word % word -> word % word
    ]

    for root, dirs, files in os.walk("."):
        # Skip certain directories
        if any(skip in root for skip in [".git", "__pycache__", ".venv", "node_modules"]):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()

                    original_content = content

                    # Apply all patterns
                    for pattern, replacement in patterns:
                        content = re.sub(pattern, replacement, content)

                    if content != original_content:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(content)
                        print(f"   Fixed: {file_path}")

                except Exception as e:
                    print(f"   Error fixing {file_path}: {e}")


def fix_boolean_comparisons():
    """Fix boolean comparisons to use 'is True' or 'is False'."""
    print("🔍 Fixing boolean comparisons...")

    patterns = [
        (r"is True\b", "is True"),
        (r"is False\b", "is False"),
        (r"is not True\b", "is not True"),
        (r"is not False\b", "is not False"),
    ]

    for root, dirs, files in os.walk("."):
        # Skip certain directories
        if any(skip in root for skip in [".git", "__pycache__", ".venv", "node_modules"]):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()

                    original_content = content

                    # Apply all patterns
                    for pattern, replacement in patterns:
                        content = re.sub(pattern, replacement, content)

                    if content != original_content:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(content)
                        print(f"   Fixed: {file_path}")

                except Exception as e:
                    print(f"   Error fixing {file_path}: {e}")


def fix_unused_variables():
    """Comment out or remove simple unused variable assignments."""
    print("🗑️  Fixing unused variables...")

    for root, dirs, files in os.walk("."):
        # Skip certain directories
        if any(skip in root for skip in [".git", "__pycache__", ".venv", "node_modules"]):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()

                    modified = False
                    new_lines = []

                    for line in lines:
                        # Simple pattern for unused variables that can be safely prefixed with _
                        if re.match(r"\s*(\w+)\s*=.*", line.strip()):
                            # Check if it's a simple assignment we can prefix with _
                            match = re.match(r"(\s*)(\w+)(\s*=.*)", line)
                            if match:
                                indent, var_name, rest = match.groups()
                                # Don't modify if already starts with _
                                if not var_name.startswith("_"):
                                    # Add underscore prefix to indicate it's intentionally unused
                                    new_line = f"{indent}_{var_name}{rest}\n"
                                    new_lines.append(new_line)
                                    modified = True
                                    continue

                        new_lines.append(line)

                    if modified:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.writelines(new_lines)
                        print(f"   Fixed: {file_path}")

                except Exception as e:
                    print(f"   Error fixing {file_path}: {e}")


def fix_line_length():  # noqa: C901
    """Try to fix some simple line length issues."""
    print("📏 Fixing line length issues...")

    for root, dirs, files in os.walk("."):
        # Skip certain directories
        if any(skip in root for skip in [".git", "__pycache__", ".venv", "node_modules"]):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()

                    modified = False
                    new_lines = []

                    for line in lines:
                        if len(line.rstrip()) > 120:
                            # Try to break long strings
                            if '"""' in line or "'''" in line:
                                # Skip docstrings for now
                                new_lines.append(line)
                                continue

                            # Try to break at commas in function calls
                            if "," in line and "(" in line:
                                # Simple approach: break after commas
                                stripped = line.rstrip()
                                if len(stripped) > 120:
                                    # Find the indentation
                                    indent = len(line) - len(line.lstrip())
                                    base_indent = " " * indent
                                    extra_indent = " " * 4

                                    # Try to break at commas
                                    parts = stripped.split(",")
                                    if len(parts) > 1:
                                        new_line = parts[0] + ",\n"
                                        for part in parts[1:-1]:
                                            new_line += base_indent + extra_indent + part.strip() + ",\n"
                                        if parts[-1].strip():
                                            new_line += base_indent + extra_indent + parts[-1].strip() + "\n"
                                        else:
                                            new_line = new_line.rstrip(",\n") + "\n"

                                        new_lines.append(new_line)
                                        modified = True
                                        continue

                        new_lines.append(line)

                    if modified:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.writelines(new_lines)
                        print(f"   Fixed: {file_path}")

                except Exception as e:
                    print(f"   Error fixing {file_path}: {e}")


def main():
    """Run all linting fixes."""
    print("🚀 Starting automatic linting fixes...")

    # Run fixes in order of safety
    fix_trailing_whitespace()
    fix_arithmetic_operators()
    fix_boolean_comparisons()

    print("\n✅ Automatic fixes completed!")
    print("\nRunning flake8 to check remaining issues...")

    # Run flake8 to see what's left
    try:
        result = subprocess.run(["flake8", "--max-line-length=120", "--count"], capture_output=True, text=True)
        if result.returncode == 0:
            print("🎉 No linting errors remaining!")
        else:
            lines = result.stdout.strip().split("\n")
            error_count = lines[-1] if lines else "unknown"
            print(f"📊 Remaining linting errors: {error_count}")

            # Show first 10 errors as examples
            if len(lines) > 1:
                print("\nFirst 10 remaining errors:")
                for line in lines[:10]:
                    if line.strip():
                        print(f"  {line}")

    except FileNotFoundError:
        print("⚠️  flake8 not found, skipping final check")


if __name__ == "__main__":
    main()
