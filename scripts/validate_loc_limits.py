#!/usr/bin/env python3
"""
Validate Lines of Code (LOC) limits for maintainability.

This script enforces:
- Maximum 200 lines per module
- Maximum 25 lines per function
- Excludes comments, docstrings, and blank lines from count
"""

import ast
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


class LOCValidator:
    """Validates Lines of Code limits for Python files."""

    def __init__(self, max_module_lines: int = 200, max_function_lines: int = 25):
        self.max_module_lines = max_module_lines
        self.max_function_lines = max_function_lines
        self.violations = []

    def count_effective_lines(self, file_path: Path) -> int:
        """Count lines excluding comments, docstrings, and blank lines."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            effective_lines = 0
            in_multiline_string = False

            for line in lines:
                stripped = line.strip()

                # Skip blank lines
                if not stripped:
                    continue

                # Skip comment lines
                if stripped.startswith("#"):
                    continue

                # Handle multiline strings (docstrings)
                if '"""' in stripped or "'''" in stripped:
                    quote_count = stripped.count('"""') + stripped.count("'''")
                    if quote_count % 2 == 1:
                        in_multiline_string = not in_multiline_string
                    if in_multiline_string or quote_count >= 2:
                        continue

                if in_multiline_string:
                    continue

                effective_lines += 1

            return effective_lines

        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return 0

    def get_function_lines(self, file_path: Path) -> List[Tuple[str, int, int]]:
        """Get function names with their line counts."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content)
            functions = []

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    start_line = node.lineno
                    end_line = node.end_lineno or start_line
                    line_count = end_line - start_line + 1
                    functions.append((node.name, line_count, start_line))

            return functions

        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
            return []

    def validate_file(self, file_path: Path) -> Dict[str, Any]:
        """Validate a single Python file."""
        result = {
            "file": str(file_path),
            "module_lines": 0,
            "module_violation": False,
            "function_violations": [],
            "valid": True,
        }

        # Check module line count
        module_lines = self.count_effective_lines(file_path)
        result["module_lines"] = module_lines

        if module_lines > self.max_module_lines:
            result["module_violation"] = True
            result["valid"] = False
            self.violations.append(f"Module {file_path}: {module_lines} lines (max: {self.max_module_lines})")

        # Check function line counts
        functions = self.get_function_lines(file_path)
        for func_name, line_count, start_line in functions:
            if line_count > self.max_function_lines:
                violation = {"function": func_name, "lines": line_count, "start_line": start_line}
                result["function_violations"].append(violation)
                result["valid"] = False
                self.violations.append(
                    f"Function {func_name} in {file_path}:{start_line}: {line_count} lines (max: {self.max_function_lines})"
                )

        return result

    def validate_directory(self, directory: Path, exclude_patterns: List[str] = None) -> Dict[str, Any]:
        """Validate all Python files in a directory."""
        if exclude_patterns is None:
            exclude_patterns = ["__pycache__", ".git", "venv", ".venv", "notebooks"]

        results = {"total_files": 0, "valid_files": 0, "violations": [], "files": {}}

        for py_file in directory.rglob("*.py"):
            # Skip excluded directories
            if any(pattern in str(py_file) for pattern in exclude_patterns):
                continue

            results["total_files"] += 1
            file_result = self.validate_file(py_file)
            results["files"][str(py_file)] = file_result

            if file_result["valid"]:
                results["valid_files"] += 1
            else:
                results["violations"].extend(self.violations[-len([v for v in self.violations if str(py_file) in v]) :])

        return results


def main():
    """Main validation function."""
    print("🔍 Validating Lines of Code (LOC) limits...")
    print("=" * 60)

    validator = LOCValidator(max_module_lines=200, max_function_lines=25)
    project_root = Path(__file__).parent.parent

    # Validate main source directories
    directories_to_check = ["web", "src", "tools", "scripts"]

    all_valid = True
    total_files = 0
    total_valid = 0

    for dir_name in directories_to_check:
        dir_path = project_root / dir_name
        if not dir_path.exists():
            continue

        print(f"\n📁 Checking {dir_name}/ directory...")
        results = validator.validate_directory(dir_path)

        total_files += results["total_files"]
        total_valid += results["valid_files"]

        if results["violations"]:
            all_valid = False
            print(f"❌ Found {len(results['violations'])} violations in {dir_name}/:")
            for violation in results["violations"]:
                print(f"   • {violation}")
        else:
            print(f"✅ All {results['total_files']} files in {dir_name}/ are within LOC limits")

    print("\n" + "=" * 60)
    print(f"📊 Summary: {total_valid}/{total_files} files passed LOC validation")

    if all_valid:
        print("🎉 All files are within LOC limits!")
        return 0
    else:
        print("⚠️  LOC violations found. Please refactor large modules/functions.")
        print("\n💡 Tips:")
        print("   • Extract helper functions from large functions")
        print("   • Split large modules into smaller, focused modules")
        print("   • Use single responsibility principle")
        return 1


if __name__ == "__main__":
    sys.exit(main())
