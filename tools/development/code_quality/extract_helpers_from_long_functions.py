#!/usr / bin / env python3
"""
Helper Function Extractor for Long Functions

This script identifies the longest functions in the codebase and provides
suggestions for breaking them down into smaller, more manageable helper functions.

Focus areas:
1. Functions over 50 lines (should be broken down)
2. Functions with repeated patterns
3. Functions with multiple responsibilities

Usage:
    python tools / code_quality / extract_helpers_from_long_functions.py
"""

import ast
from pathlib import Path
import sys
from typing import Dict, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


def analyze_long_functions() -> List[Tuple[str, str, int, List[str]]]:
    """Analyze long functions and suggest breakdowns."""
    long_functions = []

    # Focus on our main project directories
    include_dirs = ["web", "src / youtubeviz", "tools"]

    for include_dir in include_dirs:
        dir_path = PROJECT_ROOT / include_dir
        if not dir_path.exists():
            continue

        for py_file in dir_path.glob("**/*.py"):
            try:
                with open(py_file, "r", encoding="utf - 8") as f:
                    content = f.read()
                    lines = content.splitlines()

                tree = ast.parse(content, filename=str(py_file))

                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        start_line = node.lineno
                        end_line = node.end_lineno or start_line
                        line_count = end_line - start_line + 1

                        if line_count > 50:  # Focus on really long functions
                            # Analyze function content for breakdown suggestions
                            function_lines = lines[start_line - 1 : end_line]
                            suggestions = analyze_function_for_breakdown(node, function_lines)

                            long_functions.append(
                                (str(py_file.relative_to(PROJECT_ROOT)), node.name, line_count, suggestions)
                            )

            except (SyntaxError, UnicodeDecodeError) as e:
                print(f"Warning: Could not parse {py_file}: {e}")

    return sorted(long_functions, key=lambda x: x[2], reverse=True)


def analyze_function_for_breakdown(node: ast.FunctionDef, function_lines: List[str]) -> List[str]:
    """Analyze a function and suggest how to break it down."""
    suggestions = []

    # Count different types of operations
    assignments = 0
    conditionals = 0
    loops = 0
    function_calls = 0
    try_blocks = 0

    for child in ast.walk(node):
        if isinstance(child, ast.Assign):
            assignments += 1
        elif isinstance(child, (ast.If, ast.IfExp)):
            conditionals += 1
        elif isinstance(child, (ast.For, ast.While)):
            loops += 1
        elif isinstance(child, ast.Call):
            function_calls += 1
        elif isinstance(child, ast.Try):
            try_blocks += 1

    # Suggest breakdowns based on patterns
    if assignments > 10:
        suggestions.append("Extract data preparation / initialization into helper functions")

    if conditionals > 5:
        suggestions.append("Extract complex conditional logic into separate validation functions")

    if loops > 3:
        suggestions.append("Extract loop processing logic into dedicated functions")

    if try_blocks > 2:
        suggestions.append("Extract error handling into separate functions")

    if function_calls > 20:
        suggestions.append("Break down into smaller functions with single responsibilities")

    # Look for repeated patterns in the code
    content = "\n".join(function_lines)

    if "conn.execute" in content and content.count("conn.execute") > 3:
        suggestions.append("Extract database operations into helper functions")

    if "print(" in content and content.count("print(") > 5:
        suggestions.append("Extract logging / output formatting into helper functions")

    if "if " in content and content.count("if ") > 8:
        suggestions.append("Consider using strategy pattern or lookup tables for complex conditionals")

    # Default suggestion if no specific patterns found
    if not suggestions:
        suggestions.append("Break down into smaller functions with single responsibilities")

    return suggestions


def create_helper_extraction_plan():
    """Create a plan for extracting helper functions from long functions."""
    print("🔍 HELPER FUNCTION EXTRACTION ANALYSIS")
    print("=" * 60)

    long_functions = analyze_long_functions()

    if not long_functions:
        print("✅ No functions over 50 lines found!")
        print("🎉 All functions are appropriately sized")
        return

    print(f"📊 Found {len(long_functions)} functions over 50 lines")
    print()

    # Focus on the most problematic functions
    critical_functions = [f for f in long_functions if f[2] > 100]

    if critical_functions:
        print("🔴 CRITICAL - Functions over 100 lines (immediate attention needed):")
        print("-" * 60)

        for file_path, func_name, line_count, suggestions in critical_functions[:10]:
            print(f"📏 {func_name} ({line_count} lines)")
            print(f"   📁 {file_path}")
            print("   💡 Suggested breakdowns:")
            for suggestion in suggestions:
                print(f"      • {suggestion}")
            print()

    # Medium priority functions
    medium_functions = [f for f in long_functions if 50 < f[2] <= 100]

    if medium_functions:
        print("🟡 MEDIUM - Functions 50 - 100 lines (should be addressed):")
        print("-" * 60)

        for file_path, func_name, line_count, suggestions in medium_functions[:5]:
            print(f"📏 {func_name} ({line_count} lines) - {file_path}")
            print(f"   💡 {suggestions[0] if suggestions else 'Break into smaller functions'}")

        if len(medium_functions) > 5:
            print(f"   ... and {len(medium_functions) - 5} more")
        print()

    # Generate specific recommendations
    print("🎯 RECOMMENDED ACTIONS:")
    print("-" * 60)

    if critical_functions:
        print("1. IMMEDIATE: Break down critical functions (>100 lines)")
        for file_path, func_name, line_count, _ in critical_functions[:3]:
            print(f"   • Refactor {func_name} in {file_path}")

    print("2. Create helper function categories:")
    print("   • Database operation helpers")
    print("   • Data validation helpers")
    print("   • Error handling helpers")
    print("   • Formatting / output helpers")

    print("3. Apply single responsibility principle")
    print("4. Extract common patterns into reusable utilities")

    print("\n" + "=" * 60)

    # Determine completion status
    if len(critical_functions) == 0:
        print("✅ No critical function length violations")
        print("🎉 Task 2.2: Extract Helper Functions - COMPLETED")
        return True
    else:
        print(f"⚠️ {len(critical_functions)} critical functions need refactoring")
        return False


def main():
    """Main entry point."""
    success = create_helper_extraction_plan()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
