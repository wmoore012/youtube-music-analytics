#!/usr / bin / env python3
"""
Fix common linting issues across the codebase.
"""

import re
from pathlib import Path
from typing import List


def fix_bare_except(file_path: Path) -> bool:
    """Fix bare except clauses by adding Exception."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        original_content = content

        # Replace bare except with except Exception
        # This is a simple regex-for complex cases, AST parsing would be better
        content = re.sub(r"\bexcept\s*:", "except Exception:", content)

        if content != original_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Fixed bare except in: {file_path}")
            return True

        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def fix_missing_imports(file_path: Path) -> bool:  # noqa: C901
    """Fix common missing imports."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        original_content = content
        lines = content.split("\n")

        # Check if file uses 'os' but doesn't import it
        uses_os = any("os." in line or "os.path" in line for line in lines)
        has_os_import = any("import os" in line or "from os" in line for line in lines)

        if uses_os and not has_os_import:
            # Find the right place to add import
            import_section_end = 0
            for i, line in enumerate(lines):
                if line.startswith("import ") or line.startswith("from "):
                    import_section_end = i + 1
                elif line.strip() == "" and import_section_end > 0:
                    break

            if import_section_end == 0:
                # No imports found, add at the beginning
                lines.insert(0, "import os")
                lines.insert(1, "")
            else:
                lines.insert(import_section_end, "import os")

            content = "\n".join(lines)

        # Check for Optional usage without import
        uses_optional = "Optional[" in content
        has_optional_import = "from typing import" in content and "Optional" in content

        if uses_optional and not has_optional_import:
            # Add Optional to existing typing import or create new one
            for i, line in enumerate(lines):
                if line.startswith("from typing import"):
                    if "Optional" not in line:
                        # Add Optional to existing import
                        lines[i] = line.rstrip() + ", Optional"
                        content = "\n".join(lines)
                    break
            else:
                # No typing import found, add one
                import_section_end = 0
                for i, line in enumerate(lines):
                    if line.startswith("import ") or line.startswith("from "):
                        import_section_end = i + 1

                if import_section_end == 0:
                    lines.insert(0, "from typing import Optional")
                    lines.insert(1, "")
                else:
                    lines.insert(import_section_end, "from typing import Optional")

                content = "\n".join(lines)

        if content != original_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Fixed missing imports in: {file_path}")
            return True

        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def fix_shadowed_imports(file_path: Path) -> bool:
    """Fix variable names that shadow imports."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        original_content = content

        # Common shadowing patterns
        # Replace 'text' loop variable with 'text_item' when it shadows import
        if "import text" in content or "from text" in content:
            # Look for for loops using 'text' as variable
            content = re.sub(r"for text_item in ", "for text_item in ", content)
            content = re.sub(r"text_item = ", "text_item = ", content)

        if content != original_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Fixed shadowed imports in: {file_path}")
            return True

        return False

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def find_python_files_with_issues() -> List[Path]:
    """Find Python files that likely have linting issues."""
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

    for file_path in Path(".").rglob("*.py"):
        # Skip files in excluded directories
        if any(skip_dir in file_path.parts for skip_dir in skip_dirs):
            continue

        python_files.append(file_path)

    return python_files


def main():
    """Main function to fix common linting issues."""
    print("🔧 Finding Python files with potential linting issues...")
    python_files = find_python_files_with_issues()
    print(f"Found {len(python_files)} Python files to check")

    print("\n🧹 Fixing common linting issues...")

    fixed_files = set()

    # Fix bare except clauses
    for file_path in python_files:
        if fix_bare_except(file_path):
            fixed_files.add(file_path)

    # Fix missing imports
    for file_path in python_files:
        if fix_missing_imports(file_path):
            fixed_files.add(file_path)

    # Fix shadowed imports
    for file_path in python_files:
        if fix_shadowed_imports(file_path):
            fixed_files.add(file_path)

    print(f"\n✅ Fixed linting issues in {len(fixed_files)} files")

    if len(fixed_files) == 0:
        print("🎉 No common linting issues found!")
    else:
        print("🎉 Common linting issues have been fixed!")
        print("\nRun 'flake8 .' to check for remaining issues.")


if __name__ == "__main__":
    main()
