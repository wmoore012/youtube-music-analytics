#!/usr / bin / env python3
"""
Fake Data Scanner and Remover-YouTube Analytics Platform

This script identifies fake data generation code in the codebase and provides
suggestions for replacing it with real data access patterns. It also audits
error handling to ensure fail-loud behavior.

Focus areas:
1. Fake data generation functions
2. Mock / dummy data creation
3. Silent error handling (try / except without proper logging)
4. Boolean database fields that should be descriptive strings

Usage:
    python tools / code_quality / fake_data_scanner.py --scan
    python tools / code_quality / fake_data_scanner.py --fix
    python tools / code_quality / fake_data_scanner.py --report
"""

import ast
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


@dataclass
class CodeIssue:
    """Represents a code quality issue."""

    file_path: str
    line_number: int
    issue_type: str  # "fake_data", "silent_error", "boolean_field"
    description: str
    code_snippet: str
    suggested_fix: str
    severity: str  # "HIGH", "MEDIUM", "LOW"
    auto_fixable: bool = False


@dataclass
class CodeQualityReport:
    """Complete code quality analysis report."""

    issues: List[CodeIssue] = field(default_factory=list)
    files_scanned: int = 0
    fake_data_issues: int = 0
    error_handling_issues: int = 0
    boolean_field_issues: int = 0


class FakeDataScanner:
    """Scans codebase for fake data and error handling issues."""

    def __init__(self, fix_mode: bool = False):
        self.fix_mode = fix_mode
        self.report = CodeQualityReport()

        # Project directories to scan
        self.include_dirs = ["web", "src", "tools", "scripts"]

        # Patterns that indicate fake data generation
        self.fake_data_patterns = [
            r"fake\w*\(",  # faker library calls
            r"random\.(choice|randint|uniform|sample)",  # random data generation
            r"lorem\s + ipsum",  # lorem ipsum text
            r'test_\w+\s*=\s*["\']',  # test data assignments
            r"dummy\w*\s*=",  # dummy variables
            r"mock\w*\s*=",  # mock data
            r"sample_\w+\s*=",  # sample data
            r"generate_\w * _data\(",  # data generation functions
            r"create_\w * _dummy\(",  # dummy creation functions
            r"np\.random\.",  # numpy random
            r"pd\.DataFrame\(\{.*:\s*\[.*\]\s*\}\)",  # hardcoded DataFrames
        ]

        # Patterns that indicate silent error handling
        self.silent_error_patterns = [
            r"except.*:\s * pass",  # except Exception: pass
            r"except.*:\s * continue",  # except Exception: continue
            r"except.*:\s * return\s*$",  # except Exception: return (without value)
            r"except.*:\s * return\s + None",  # except Exception: return None
        ]

        # Boolean field patterns in database operations
        self.boolean_field_patterns = [
            r"(is_|has_|can_|should_)\w+.*BOOLEAN",
            r"(is_|has_|can_|should_)\w+.*TINYINT\(1\)",
            r"(is_|has_|can_|should_)\w+.*BIT\(1\)",
        ]

    def _extract_code_snippet(self, lines: List[str], line_number: int, context: int = 2) -> str:
        """Extract code snippet with context."""
        start = max(0, line_number-context-1)
        end = min(len(lines), line_number + context)
        snippet_lines = lines[start:end]

        # Add line numbers
        numbered_lines = []
        for i, line in enumerate(snippet_lines, start + 1):
            marker = ">>> " if i == line_number else "    "
            numbered_lines.append(f"{marker}{i:3d}: {line.rstrip()}")

        return "\n".join(numbered_lines)

    def _scan_python_file(self, file_path: Path) -> List[CodeIssue]:
        """Scan a Python file for code quality issues."""
        issues = []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                lines = content.splitlines()

            # Scan for fake data patterns
            for i, line in enumerate(lines, 1):
                for pattern in self.fake_data_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        issues.append(
                            CodeIssue(
                                file_path=str(file_path.relative_to(PROJECT_ROOT)),
                                line_number=i,
                                issue_type="fake_data",
                                description=f"Potential fake data generation: {pattern}",
                                code_snippet=self._extract_code_snippet(lines, i),
                                suggested_fix="Replace with real data access from database or API",
                                severity="HIGH",
                                auto_fixable=False,
                            )
                        )

                # Scan for silent error handling
                for pattern in self.silent_error_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        issues.append(
                            CodeIssue(
                                file_path=str(file_path.relative_to(PROJECT_ROOT)),
                                line_number=i,
                                issue_type="silent_error",
                                description=f"Silent error handling: {pattern}",
                                code_snippet=self._extract_code_snippet(lines, i),
                                suggested_fix="Add proper logging and error handling with clear messages",
                                severity="MEDIUM",
                                auto_fixable=False,
                            )
                        )

            # Use AST to find more complex patterns
            try:
                tree = ast.parse(content, filename=str(file_path))
                issues.extend(self._analyze_ast_for_issues(tree, file_path, lines))
            except SyntaxError:
                pass  # Skip files with syntax errors

        except (UnicodeDecodeError, FileNotFoundError):
            pass  # Skip files that can't be read

        return issues

    def _analyze_ast_for_issues(self, tree: ast.AST, file_path: Path, lines: List[str]) -> List[CodeIssue]:
        """Analyze AST for more complex code quality issues."""
        issues = []

        for node in ast.walk(tree):
            # Look for functions that generate fake data
            if isinstance(node, ast.FunctionDef):
                if any(keyword in node.name.lower() for keyword in ["fake", "mock", "dummy", "sample", "test_data"]):
                    issues.append(
                        CodeIssue(
                            file_path=str(file_path.relative_to(PROJECT_ROOT)),
                            line_number=node.lineno,
                            issue_type="fake_data",
                            description=f"Function appears to generate fake data: {node.name}",
                            code_snippet=self._extract_code_snippet(lines, node.lineno),
                            suggested_fix="Replace with real data access function",
                            severity="HIGH",
                            auto_fixable=False,
                        )
                    )

            # Look for hardcoded data structures
            elif isinstance(node, ast.Dict):
                if len(node.keys) > 5:  # Large dictionaries might be fake data
                    # Check if keys look like fake data
                    key_names = []
                    for key in node.keys:
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            key_names.append(key.value.lower())

                    if any(keyword in " ".join(key_names) for keyword in ["test", "sample", "dummy", "fake"]):
                        issues.append(
                            CodeIssue(
                                file_path=str(file_path.relative_to(PROJECT_ROOT)),
                                line_number=node.lineno,
                                issue_type="fake_data",
                                description="Large dictionary with test / sample data",
                                code_snippet=self._extract_code_snippet(lines, node.lineno),
                                suggested_fix="Load data from database or configuration file",
                                severity="MEDIUM",
                                auto_fixable=False,
                            )
                        )

            # Look for bare except clauses
            elif isinstance(node, ast.ExceptHandler):
                if node.type is None:  # bare except Exception:
                    issues.append(
                        CodeIssue(
                            file_path=str(file_path.relative_to(PROJECT_ROOT)),
                            line_number=node.lineno,
                            issue_type="silent_error",
                            description="Bare except clause-catches all exceptions",
                            code_snippet=self._extract_code_snippet(lines, node.lineno),
                            suggested_fix="Specify exception types and add proper error handling",
                            severity="HIGH",
                            auto_fixable=False,
                        )
                    )

        return issues

    def _scan_sql_files(self) -> List[CodeIssue]:
        """Scan SQL files for boolean field issues."""
        issues = []

        sql_files = list(PROJECT_ROOT.glob("**/*.sql"))

        for sql_file in sql_files:
            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    lines = content.splitlines()

                for i, line in enumerate(lines, 1):
                    for pattern in self.boolean_field_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            issues.append(
                                CodeIssue(
                                    file_path=str(sql_file.relative_to(PROJECT_ROOT)),
                                    line_number=i,
                                    issue_type="boolean_field",
                                    description="Boolean field that could be more descriptive",
                                    code_snippet=self._extract_code_snippet(lines, i),
                                    suggested_fix="Consider using ENUM or VARCHAR with descriptive values",
                                    severity="LOW",
                                    auto_fixable=False,
                                )
                            )

            except (UnicodeDecodeError, FileNotFoundError):
                continue

        return issues

    def scan_codebase(self) -> CodeQualityReport:
        """Scan the entire codebase for fake data and error handling issues."""
        print("🔍 Scanning codebase for fake data and error handling issues...")

        all_issues = []

        # Scan Python files
        for include_dir in self.include_dirs:
            dir_path = PROJECT_ROOT / include_dir
            if dir_path.exists():
                python_files = list(dir_path.glob("**/*.py"))

                for py_file in python_files:
                    self.report.files_scanned += 1
                    issues = self._scan_python_file(py_file)
                    all_issues.extend(issues)

        # Scan SQL files
        sql_issues = self._scan_sql_files()
        all_issues.extend(sql_issues)

        # Categorize issues
        self.report.issues = all_issues
        for issue in all_issues:
            if issue.issue_type == "fake_data":
                self.report.fake_data_issues += 1
            elif issue.issue_type == "silent_error":
                self.report.error_handling_issues += 1
            elif issue.issue_type == "boolean_field":
                self.report.boolean_field_issues += 1

        print(f"✅ Scanned {self.report.files_scanned} files")
        print(f"📊 Found {len(all_issues)} total issues:")
        print(f"   • Fake data: {self.report.fake_data_issues}")
        print(f"   • Error handling: {self.report.error_handling_issues}")
        print(f"   • Boolean fields: {self.report.boolean_field_issues}")

        return self.report

    def generate_fixes(self) -> Dict[str, List[str]]:
        """Generate specific fix suggestions for high-priority issues."""
        fixes = {"fake_data": [], "silent_error": [], "boolean_field": []}

        for issue in self.report.issues:
            if issue.severity in ["HIGH", "MEDIUM"]:
                fixes[issue.issue_type].append(f"{issue.file_path}:{issue.line_number} - {issue.suggested_fix}")

        return fixes

    def print_report(self) -> None:
        """Print detailed code quality report."""
        print("\n" + "=" * 80)
        print("FAKE DATA AND ERROR HANDLING AUDIT REPORT")
        print("=" * 80)
        print(f"Files Scanned: {self.report.files_scanned}")
        print(f"Total Issues: {len(self.report.issues)}")
        print(f"Fake Data Issues: {self.report.fake_data_issues}")
        print(f"Error Handling Issues: {self.report.error_handling_issues}")
        print(f"Boolean Field Issues: {self.report.boolean_field_issues}")
        print()

        # Group issues by severity and type
        by_severity = {"HIGH": [], "MEDIUM": [], "LOW": []}
        for issue in self.report.issues:
            by_severity[issue.severity].append(issue)

        for severity in ["HIGH", "MEDIUM", "LOW"]:
            issues = by_severity[severity]
            if issues:
                print(f"{severity} PRIORITY ISSUES:")
                print("-" * 40)

                for issue in issues[:10]:  # Show first 10
                    severity_symbol = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}[severity]
                    print(f"{severity_symbol} {issue.file_path}:{issue.line_number}")
                    print(f"   Type: {issue.issue_type}")
                    print(f"   Issue: {issue.description}")
                    print(f"   Fix: {issue.suggested_fix}")
                    print()

                if len(issues) > 10:
                    print(f"   ... and {len(issues) - 10} more {severity.lower()} priority issues")
                print()

        print("=" * 80)


def main():
    """Main entry point for fake data scanner."""
    import argparse

    parser = argparse.ArgumentParser(description="Fake Data Scanner and Error Handler Auditor")
    parser.add_argument("--scan", action="store_true", help="Scan codebase for issues")
    parser.add_argument("--fix", action="store_true", help="Generate fix suggestions")
    parser.add_argument("--report", action="store_true", help="Generate detailed report")

    args = parser.parse_args()

    if not any([args.scan, args.fix, args.report]):
        args.scan = True  # Default to scan mode

    # Create scanner
    scanner = FakeDataScanner(fix_mode=args.fix)

    # Scan codebase
    report = scanner.scan_codebase()

    # Generate fixes if requested
    if args.fix:
        fixes = scanner.generate_fixes()
        print(f"\n🔧 Generated fix suggestions:")
        for issue_type, fix_list in fixes.items():
            if fix_list:
                print(f"\n{issue_type.upper()} FIXES:")
                for fix in fix_list[:5]:  # Show first 5
                    print(f"  • {fix}")
                if len(fix_list) > 5:
                    print(f"  ... and {len(fix_list) - 5} more")

    # Print report if requested or if issues found
    if args.report or len(report.issues) > 0:
        scanner.print_report()

    # Determine completion status
    high_priority_issues = [i for i in report.issues if i.severity == "HIGH"]

    if len(high_priority_issues) == 0:
        print("\n✅ No high-priority fake data or error handling issues found!")
        print("🎉 Task 2.3: Remove Fake Data and Improve Error Handling-COMPLETED")
        sys.exit(0)
    else:
        print(f"\n⚠️ Found {len(high_priority_issues)} high-priority issues that need attention")
        sys.exit(1)


if __name__ == "__main__":
    main()
