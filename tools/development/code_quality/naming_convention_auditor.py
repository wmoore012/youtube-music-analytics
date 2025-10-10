#!/usr / bin / env python3
"""
Naming Convention Auditor-YouTube Analytics Platform

This script audits the codebase for naming convention violations and provides
automated fixes where possible. It enforces:

1. snake_case for variables and functions
2. PascalCase for classes
3. lowercase_snake_case for database columns
4. UPPER_CASE for constants

Usage:
    python tools / code_quality / naming_convention_auditor.py --scan
    python tools / code_quality / naming_convention_auditor.py --fix
    python tools / code_quality / naming_convention_auditor.py --report
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
class NamingViolation:
    """Represents a naming convention violation."""

    file_path: str
    line_number: int
    violation_type: str  # "variable", "function", "class", "constant"
    current_name: str
    suggested_name: str
    context: str
    severity: str  # "HIGH", "MEDIUM", "LOW"
    auto_fixable: bool = False


@dataclass
class NamingAuditReport:
    """Complete naming convention audit report."""

    violations: List[NamingViolation] = field(default_factory=list)
    files_scanned: int = 0
    total_violations: int = 0
    auto_fixable_count: int = 0
    violation_types: Dict[str, int] = field(default_factory=dict)


class NamingConventionAuditor:
    """Audits and fixes naming convention violations."""

    def __init__(self, fix_mode: bool = False):
        self.fix_mode = fix_mode
        self.report = NamingAuditReport()

        # Patterns for different naming conventions
        self.snake_case_pattern = re.compile(r"^[a-z][a-z0-9_]*$")
        self.pascal_case_pattern = re.compile(r"^[A-Z][a-zA-Z0-9]*$")
        self.camel_case_pattern = re.compile(r"^[a-z][a-zA-Z0-9]*$")
        self.upper_case_pattern = re.compile(r"^[A-Z][A-Z0-9_]*$")

        # Common exceptions that should not be changed
        self.exceptions = {
            # Standard Python / library names
            "setUp",
            "tearDown",
            "setUpClass",
            "tearDownClass",
            "assertTrue",
            "assertFalse",
            "assertEqual",
            "assertNotEqual",
            "assertIn",
            "assertNotIn",
            "assertIsNone",
            "assertIsNotNone",
            # Database / SQL related
            "fetchone",
            "fetchall",
            "fetchmany",
            "rowcount",
            # Common abbreviations
            "id",
            "url",
            "api",
            "sql",
            "db",
            "etl",
            "csv",
            "json",
            "xml",
            # YouTube API specific
            "videoId",
            "channelId",
            "playlistId",
            "commentId",
            # Pandas / DataFrame methods
            "DataFrame",
            "groupby",
            "fillna",
            "dropna",
            "isna",
            "notna",
            # SQLAlchemy
            "autocommit",
            "autoflush",
            "expire_on_commit",
            # Common single letters
            "i",
            "j",
            "k",
            "x",
            "y",
            "z",
            "n",
            "m",
            "a",
            "b",
            "c",
            # Special methods
            "__init__",
            "__str__",
            "__repr__",
            "__len__",
            "__iter__",
            "__enter__",
            "__exit__",
            "__call__",
            "__getitem__",
            "__setitem__",
        }

        # Files to exclude from scanning
        self.exclude_patterns = [
            "*/.*",  # Hidden files
            "*/__pycache__/*",
            "*/.venv/*",
            "*/venv/*",
            "*/env/*",
            "*/node_modules/*",
            "*.pyc",
            "*.pyo",
            "*/migrations/*",  # Database migrations often have generated names
            "*/test_*",  # Test files may have different conventions
            "*/site-packages/*",  # External packages
        ]

        # Only scan our project directories
        self.include_dirs = ["web", "src", "tools", "notebooks", "scripts", "tests"]

    def _should_exclude_file(self, file_path: Path) -> bool:
        """Check if file should be excluded from scanning."""
        _file_str = str(file_path)  # noqa: F841
        for pattern in self.exclude_patterns:
            if file_path.match(pattern):
                return True
        return False

    def _convert_to_snake_case(self, name: str) -> str:
        """Convert camelCase or PascalCase to snake_case."""
        # Handle acronyms and consecutive capitals
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
        return s2.lower()

    def _convert_to_pascal_case(self, name: str) -> str:
        """Convert snake_case to PascalCase."""
        components = name.split("_")
        return "".join(word.capitalize() for word in components)

    def _is_constant(self, name: str, context: str) -> bool:
        """Determine if a name should be treated as a constant."""
        # Constants are typically all uppercase or assigned at module level
        # But private constants starting with _ are already properly named
        return (name.isupper() and not name.startswith("_")) or ("module" in context.lower() and name.isupper())

    def _analyze_python_file(self, file_path: Path) -> List[NamingViolation]:
        """Analyze a Python file for naming violations."""
        violations = []

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content, filename=str(file_path))

            for node in ast.walk(tree):
                violations.extend(self._check_node_naming(node, file_path, content))

        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"Warning: Could not parse {file_path}: {e}")

        return violations

    def _check_node_naming(self, node: ast.AST, file_path: Path, content: str) -> List[NamingViolation]:
        """Check naming conventions for an AST node."""
        violations = []

        if isinstance(node, ast.ClassDef):
            violations.extend(self._check_class_name(node, file_path, content))
        elif isinstance(node, ast.FunctionDef):
            violations.extend(self._check_function_name(node, file_path, content))
        elif isinstance(node, ast.Name):
            violations.extend(self._check_variable_name(node, file_path, content))
        elif isinstance(node, ast.Assign):
            violations.extend(self._check_assignment(node, file_path, content))

        return violations

    def _check_class_name(self, node: ast.ClassDef, file_path: Path, content: str) -> List[NamingViolation]:
        """Check class naming conventions."""
        violations = []
        name = node.name

        if name in self.exceptions:
            return violations

        if not self.pascal_case_pattern.match(name):
            suggested = self._convert_to_pascal_case(name) if "_" in name else name.capitalize()

            violations.append(
                NamingViolation(
                    file_path=str(file_path),
                    line_number=node.lineno,
                    violation_type="class",
                    current_name=name,
                    suggested_name=suggested,
                    context=f"Class definition",
                    severity="HIGH",
                    auto_fixable=True,
                )
            )

        return violations

    def _check_function_name(self, node: ast.FunctionDef, file_path: Path, content: str) -> List[NamingViolation]:
        """Check function naming conventions."""
        violations = []
        name = node.name

        if name in self.exceptions or name.startswith("__") and name.endswith("__"):
            return violations

        if not self.snake_case_pattern.match(name) and self.camel_case_pattern.match(name):
            suggested = self._convert_to_snake_case(name)

            violations.append(
                NamingViolation(
                    file_path=str(file_path),
                    line_number=node.lineno,
                    violation_type="function",
                    current_name=name,
                    suggested_name=suggested,
                    context=f"Function definition",
                    severity="MEDIUM",
                    auto_fixable=True,
                )
            )

        return violations

    def _check_variable_name(self, node: ast.Name, file_path: Path, content: str) -> List[NamingViolation]:
        """Check variable naming conventions."""
        violations = []
        name = node.id  # Use .id instead of .name for ast.Name nodes

        if name in self.exceptions or len(name) <= 2:
            return violations

        # Skip if it's a known library / module attribute
        if any(lib in str(file_path).lower() for lib in ["pandas", "numpy", "matplotlib", "plotly"]):
            return violations

        # Check for camelCase variables
        if self.camel_case_pattern.match(name) and not self.snake_case_pattern.match(name):
            suggested = self._convert_to_snake_case(name)

            violations.append(
                NamingViolation(
                    file_path=str(file_path),
                    line_number=node.lineno,
                    violation_type="variable",
                    current_name=name,
                    suggested_name=suggested,
                    context=f"Variable usage",
                    severity="LOW",
                    auto_fixable=False,  # Variables are harder to auto-fix safely
                )
            )

        return violations

    def _check_assignment(self, node: ast.Assign, file_path: Path, content: str) -> List[NamingViolation]:
        """Check assignment naming conventions."""
        violations = []

        for target in node.targets:
            if isinstance(target, ast.Name):
                name = target.id  # Use .id instead of .name for ast.Name nodes

                if name in self.exceptions:
                    continue

                # Check if this looks like a constant
                if self._is_constant(name, "assignment"):
                    if not self.upper_case_pattern.match(name) and not name.startswith("_"):
                        suggested = name.upper().replace(" ", "_")
                        violations.append(
                            NamingViolation(
                                file_path=str(file_path),
                                line_number=node.lineno,
                                violation_type="constant",
                                current_name=name,
                                suggested_name=suggested,
                                context="Constant assignment",
                                severity="MEDIUM",
                                auto_fixable=True,
                            )
                        )
                else:
                    # Regular variable assignment
                    if self.camel_case_pattern.match(name) and not self.snake_case_pattern.match(name):
                        suggested = self._convert_to_snake_case(name)
                        violations.append(
                            NamingViolation(
                                file_path=str(file_path),
                                line_number=node.lineno,
                                violation_type="variable",
                                current_name=name,
                                suggested_name=suggested,
                                context="Variable assignment",
                                severity="LOW",
                                auto_fixable=False,
                            )
                        )

        return violations

    def _scan_database_schema(self) -> List[NamingViolation]:
        """Scan database schema files for naming violations."""
        violations = []

        # Look for SQL schema files
        schema_files = list(PROJECT_ROOT.glob("*.sql")) + list(PROJECT_ROOT.glob("**/*.sql"))

        for schema_file in schema_files:
            if self._should_exclude_file(schema_file):
                continue

            try:
                with open(schema_file, "r", encoding="utf-8") as f:
                    content = f.read()

                # Find column names in CREATE TABLE statements
                table_pattern = re.compile(r"CREATE TABLE.*?\((.*?)\)", re.DOTALL | re.IGNORECASE)
                column_pattern = re.compile(r"`([^`]+)`\s+\w+", re.IGNORECASE)

                for table_match in table_pattern.finditer(content):
                    table_def = table_match.group(1)
                    line_offset = content[: table_match.start()].count("\n") + 1

                    for col_match in column_pattern.finditer(table_def):
                        column_name = col_match.group(1)

                        # Check if column name follows lowercase_snake_case
                        if not self.snake_case_pattern.match(column_name):
                            suggested = self._convert_to_snake_case(column_name)

                            violations.append(
                                NamingViolation(
                                    file_path=str(schema_file),
                                    line_number=line_offset + table_def[: col_match.start()].count("\n"),
                                    violation_type="database_column",
                                    current_name=column_name,
                                    suggested_name=suggested,
                                    context="Database column definition",
                                    severity="HIGH",
                                    auto_fixable=True,
                                )
                            )

            except Exception as e:
                print(f"Warning: Could not scan schema file {schema_file}: {e}")

        return violations

    def scan_codebase(self) -> NamingAuditReport:
        """Scan the entire codebase for naming violations."""
        print("🔍 Scanning codebase for naming convention violations...")

        # Scan Python files in our project directories only
        python_files = []
        for include_dir in self.include_dirs:
            dir_path = PROJECT_ROOT / include_dir
            if dir_path.exists():
                python_files.extend(dir_path.glob("**/*.py"))

        # Also scan root level Python files
        python_files.extend(PROJECT_ROOT.glob("*.py"))

        for py_file in python_files:
            if self._should_exclude_file(py_file):
                continue

            self.report.files_scanned += 1
            violations = self._analyze_python_file(py_file)
            self.report.violations.extend(violations)

        # Scan database schema files
        schema_violations = self._scan_database_schema()
        self.report.violations.extend(schema_violations)

        # Update report statistics
        self.report.total_violations = len(self.report.violations)
        self.report.auto_fixable_count = sum(1 for v in self.report.violations if v.auto_fixable)

        # Count violations by type
        for violation in self.report.violations:
            vtype = violation.violation_type
            self.report.violation_types[vtype] = self.report.violation_types.get(vtype, 0) + 1

        print(f"✅ Scanned {self.report.files_scanned} files")
        print(f"📊 Found {self.report.total_violations} naming violations")
        print(f"🔧 {self.report.auto_fixable_count} violations can be auto-fixed")

        return self.report

    def apply_fixes(self) -> int:
        """Apply automatic fixes to naming violations."""
        if not self.fix_mode:
            print("❌ Fix mode not enabled. Use --fix flag to apply changes.")
            return 0

        print("🔧 Applying automatic fixes...")
        fixes_applied = 0

        # Group violations by file for efficient processing
        violations_by_file = {}
        for violation in self.report.violations:
            if violation.auto_fixable:
                file_path = violation.file_path
                if file_path not in violations_by_file:
                    violations_by_file[file_path] = []
                violations_by_file[file_path].append(violation)

        for file_path, violations in violations_by_file.items():
            try:
                fixes_applied += self._fix_file(file_path, violations)
            except Exception as e:
                print(f"❌ Error fixing {file_path}: {e}")

        print(f"✅ Applied {fixes_applied} automatic fixes")
        return fixes_applied

    def _fix_file(self, file_path: str, violations: List[NamingViolation]) -> int:
        """Apply fixes to a single file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            original_content = content
            fixes_applied = 0

            # Sort violations by line number (descending) to avoid offset issues
            violations.sort(key=lambda v: v.line_number, reverse=True)

            for violation in violations:
                # Simple string replacement (this is basic-more sophisticated AST-based
                # replacement would be better for production use)
                if violation.violation_type in ["class", "function", "constant"]:
                    # Use word boundaries to avoid partial matches
                    pattern = r"\b" + re.escape(violation.current_name) + r"\b"
                    new_content = re.sub(pattern, violation.suggested_name, content)

                    if new_content != content:
                        content = new_content
                        fixes_applied += 1
                        print(f"  Fixed {violation.current_name} → {violation.suggested_name} in {file_path}")

            # Write back if changes were made
            if content != original_content:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(content)

            return fixes_applied

        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
            return 0

    def print_report(self) -> None:
        """Print detailed naming convention audit report."""
        print("\n" + "=" * 80)
        print("NAMING CONVENTION AUDIT REPORT")
        print("=" * 80)
        print(f"Files Scanned: {self.report.files_scanned}")
        print(f"Total Violations: {self.report.total_violations}")
        print(f"Auto-fixable: {self.report.auto_fixable_count}")
        print()

        # Violations by type
        if self.report.violation_types:
            print("VIOLATIONS BY TYPE:")
            print("-" * 40)
            for vtype, count in sorted(self.report.violation_types.items()):
                print(f"  {vtype.replace('_', ' ').title()}: {count}")
            print()

        # Detailed violations
        if self.report.violations:
            print("DETAILED VIOLATIONS:")
            print("-" * 40)

            # Group by severity
            by_severity = {}
            for violation in self.report.violations:
                severity = violation.severity
                if severity not in by_severity:
                    by_severity[severity] = []
                by_severity[severity].append(violation)

            for severity in ["HIGH", "MEDIUM", "LOW"]:
                if severity in by_severity:
                    print(f"\n{severity} PRIORITY:")
                    for violation in by_severity[severity][:10]:  # Show first 10
                        symbol = "🔧" if violation.auto_fixable else "⚠️"
                        print(f"  {symbol} {violation.file_path}:{violation.line_number}")
                        print(f"     {violation.violation_type}: {violation.current_name} → {violation.suggested_name}")
                        print(f"     Context: {violation.context}")

                    if len(by_severity[severity]) > 10:
                        print(f"     ... and {len(by_severity[severity]) - 10} more")

        print("\n" + "=" * 80)


def main():
    """Main entry point for naming convention auditor."""
    import argparse

    parser = argparse.ArgumentParser(description="Naming Convention Auditor")
    parser.add_argument("--scan", action="store_true", help="Scan codebase for violations")
    parser.add_argument("--fix", action="store_true", help="Apply automatic fixes")
    parser.add_argument("--report", action="store_true", help="Generate detailed report")

    args = parser.parse_args()

    if not any([args.scan, args.fix, args.report]):
        args.scan = True  # Default to scan mode

    # Create auditor
    auditor = NamingConventionAuditor(fix_mode=args.fix)

    # Scan codebase
    report = auditor.scan_codebase()

    # Apply fixes if requested
    if args.fix:
        fixes_applied = auditor.apply_fixes()
        print(f"\n🎉 Applied {fixes_applied} automatic fixes")

    # Print report if requested or if violations found
    if args.report or report.total_violations > 0:
        auditor.print_report()

    # Exit with appropriate code
    if report.total_violations == 0:
        print("\n✅ No naming convention violations found!")
        sys.exit(0)
    elif args.fix and report.auto_fixable_count > 0:
        print(f"\n🔧 Fixed {report.auto_fixable_count} violations automatically")
        remaining = report.total_violations-report.auto_fixable_count
        if remaining > 0:
            print(f"⚠️ {remaining} violations require manual attention")
            sys.exit(1)
        else:
            sys.exit(0)
    else:
        print(f"\n⚠️ Found {report.total_violations} naming violations")
        print("Run with --fix to apply automatic fixes")
        sys.exit(1)


if __name__ == "__main__":
    main()
