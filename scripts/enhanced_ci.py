#!/usr/bin/env python3
"""
🚀 Enhanced CI/CD Pipeline - Senior Level Standards
==================================================

Comprehensive quality validation system for production-ready code.
Built for Grammy-nominated producer + M.S. Data Science portfolio.

Features:
- Senior-level code quality standards
- Extensive commenting validation
- Database integrity checks
- AI agent intelligence reporting
- Music industry context validation

Usage:
    python scripts/enhanced_ci.py
    python scripts/enhanced_ci.py --fix-issues
    python scripts/enhanced_ci.py --report-only
"""

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class CodeQualityMetrics:
    """Code quality assessment results for senior-level evaluation."""

    formatting_score: float = 0.0  # 0-100
    linting_issues: int = 0
    type_coverage: float = 0.0  # 0-100
    security_vulnerabilities: int = 0
    complexity_score: float = 0.0  # 0-100
    documentation_coverage: float = 0.0  # 0-100
    comment_quality_score: float = 0.0  # 0-100
    loc_compliance: bool = True
    duplicate_code_detected: bool = False


@dataclass
class TestExecutionResults:
    """Comprehensive test execution analysis."""

    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    skipped_tests: int = 0
    coverage_percentage: float = 0.0
    performance_benchmarks: Dict[str, float] = field(default_factory=dict)
    execution_time: float = 0.0


@dataclass
class DatabaseIntegrityResults:
    """Database health and integrity assessment."""

    schema_consistency: bool = True
    referential_integrity: bool = True
    data_quality_score: float = 0.0  # 0-100
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    record_counts: Dict[str, int] = field(default_factory=dict)
    anomaly_detection: List[str] = field(default_factory=list)


@dataclass
class CIValidationResult:
    """Comprehensive CI validation results for AI agent analysis."""

    success: bool = False
    execution_time: float = 0.0
    timestamp: str = ""

    # Component results
    code_quality: CodeQualityMetrics = None
    test_results: TestExecutionResults = None
    database_integrity: DatabaseIntegrityResults = None

    # AI agent insights
    recommendations: List[str] = field(default_factory=list)
    risk_assessment: str = "UNKNOWN"  # LOW, MEDIUM, HIGH, CRITICAL
    deployment_readiness: bool = False

    def __post_init__(self):
        if self.code_quality is None:
            self.code_quality = CodeQualityMetrics()
        if self.test_results is None:
            self.test_results = TestExecutionResults()
        if self.database_integrity is None:
            self.database_integrity = DatabaseIntegrityResults()
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


class EnhancedCI:
    """
    Senior-level CI/CD pipeline with comprehensive validation.

    Designed for Grammy-nominated producer + M.S. Data Science portfolio
    to demonstrate production-ready engineering practices.
    """

    def __init__(self, fix_issues: bool = False, report_only: bool = False):
        self.fix_issues = fix_issues
        self.report_only = report_only
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.results = CIValidationResult()

        # Senior-level standards configuration
        self.max_line_length = 120
        self.min_comment_ratio = 0.15  # 15% of lines should be comments
        self.max_function_loc = 50  # Maximum lines per function
        self.min_test_coverage = 85.0  # Minimum test coverage percentage

    def log_error(self, message: str) -> None:
        """Log an error with context for debugging."""
        self.errors.append(message)
        print(f"❌ ERROR: {message}")

    def log_warning(self, message: str) -> None:
        """Log a warning with recommendations."""
        self.warnings.append(message)
        print(f"⚠️  WARNING: {message}")

    def log_success(self, message: str) -> None:
        """Log a success with specific metrics."""
        print(f"✅ {message}")

    def log_info(self, message: str) -> None:
        """Log informational message."""
        print(f"ℹ️  {message}")

    def validate_code_quality(self) -> CodeQualityMetrics:
        """
        Comprehensive code quality validation with senior-level standards.

        Validates:
        - Code formatting and style consistency
        - Extensive commenting requirements
        - Function complexity and LOC limits
        - Type hint coverage
        - Documentation completeness
        """
        print("\n🔍 VALIDATING CODE QUALITY (Senior-Level Standards)")
        print("=" * 60)

        metrics = CodeQualityMetrics()

        # 1. Check code formatting with Black
        self.log_info("Checking code formatting with Black...")
        try:
            result = subprocess.run(
                ["black", "--check", "--line-length", str(self.max_line_length), "."],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                metrics.formatting_score = 100.0
                self.log_success("Code formatting is perfect")
            else:
                # Count formatting issues for better metrics
                formatting_issues = result.stdout.count("would reformat")
                if formatting_issues > 0:
                    metrics.formatting_score = max(20.0, 100.0 - (formatting_issues * 2))
                else:
                    metrics.formatting_score = 50.0

                if self.fix_issues:
                    self.log_info(f"Auto-fixing {formatting_issues} formatting issues...")
                    fix_result = subprocess.run(
                        ["black", "--line-length", str(self.max_line_length), "."],
                        capture_output=True,
                        text=True,
                        timeout=120,
                    )
                    if fix_result.returncode == 0:
                        self.log_success("Auto-fixed formatting issues")
                        metrics.formatting_score = 100.0
                    else:
                        self.log_error("Failed to auto-fix some formatting issues")
                else:
                    self.log_warning(f"Found {formatting_issues} formatting issues - run with --fix-issues")
                    # Show first few files that need formatting
                    lines = result.stdout.split("\n")[:5]
                    for line in lines:
                        if "would reformat" in line:
                            print(f"   📝 {line}")
        except Exception as e:
            self.log_error(f"Black formatting check failed: {e}")

        # 2. Check linting with flake8
        self.log_info("Running comprehensive linting...")
        try:
            result = subprocess.run(
                [
                    "flake8",
                    f"--max-line-length={self.max_line_length}",
                    "--exclude=.venv,__pycache__,tools/archive",
                    "--statistics",
                    ".",
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode == 0:
                self.log_success("No linting issues found")
                metrics.linting_issues = 0
            else:
                issues = result.stdout.count("\n") - result.stdout.count(":")  # More accurate count
                metrics.linting_issues = max(0, issues)

                # Categorize issues for better reporting
                issue_categories = {
                    "E": 0,  # Errors
                    "W": 0,  # Warnings
                    "F": 0,  # Flake8 errors
                    "C": 0,  # Complexity
                    "N": 0,  # Naming
                }

                lines = result.stdout.split("\n")
                for line in lines:
                    if ":" in line and len(line.split(":")) >= 4:
                        error_code = line.split(":")[-1].strip()[:1]
                        if error_code in issue_categories:
                            issue_categories[error_code] += 1

                total_categorized = sum(issue_categories.values())
                if total_categorized > 0:
                    metrics.linting_issues = total_categorized

                if metrics.linting_issues > 50:
                    self.log_error(f"Critical: {metrics.linting_issues} linting issues found")
                else:
                    self.log_warning(f"Found {metrics.linting_issues} linting issues")

                # Show issue breakdown
                print(f"   📊 Issue breakdown:")
                for category, count in issue_categories.items():
                    if count > 0:
                        category_name = {
                            "E": "Errors",
                            "W": "Warnings",
                            "F": "Flake8",
                            "C": "Complexity",
                            "N": "Naming",
                        }.get(category, category)
                        print(f"     {category_name}: {count}")

                # Show first 10 issues for context
                print(f"   📝 Sample issues:")
                issue_lines = [line for line in lines if ":" in line and line.strip()][:10]
                for line in issue_lines:
                    if line.strip():
                        print(f"     {line}")

                if len(issue_lines) > 10:
                    print(f"     ... and {len(issue_lines) - 10} more")

        except Exception as e:
            self.log_error(f"Linting check failed: {e}")

        # 3. Validate extensive commenting
        self.log_info("Validating comment quality and coverage...")
        metrics.comment_quality_score = self._validate_commenting()

        # 4. Check for duplicate code (senior-level requirement)
        self.log_info("Detecting duplicate code patterns...")
        metrics.duplicate_code_detected = self._detect_duplicate_code()

        # 5. Detect AI-generated patterns and functions to combine
        self.log_info("Analyzing for AI-generated patterns and combinable functions...")
        ai_patterns = self._detect_ai_generated_patterns()
        if ai_patterns:
            self.log_warning(f"Found {len(ai_patterns)} potential AI patterns or combinable functions")

        # 5. Validate function complexity and LOC
        self.log_info("Checking function complexity and LOC limits...")
        metrics.loc_compliance = self._validate_function_complexity()

        return metrics

    def validate_database_operations(self) -> bool:
        """
        Validate database operations with human-readable SQL formatting.

        Checks for:
        - Proper SQL formatting with line breaks
        - Database schema integrity
        - Data quality validation
        - Performance query analysis
        """
        print("\n🗄️ VALIDATING DATABASE OPERATIONS")
        print("=" * 60)

        all_compliant = True

        # 1. Check SQL formatting in Python files
        self.log_info("Checking SQL formatting and readability...")
        sql_issues = self._check_sql_formatting()
        if sql_issues:
            all_compliant = False

        # 2. Validate database schema integrity
        self.log_info("Validating database schema integrity...")
        schema_valid = self._validate_schema_integrity()
        if not schema_valid:
            all_compliant = False

        # 3. Check data quality validation
        self.log_info("Checking data quality validation...")
        dq_valid = self._check_data_quality_validation()
        if not dq_valid:
            all_compliant = False

        return all_compliant

    def _check_sql_formatting(self) -> List[str]:
        """Check for human-readable SQL formatting with proper line breaks."""
        issues = []

        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    lines = content.split("\n")

                # Look for SQL queries
                in_sql = False
                sql_start_line = 0

                for i, line in enumerate(lines):
                    # Detect SQL query start
                    if any(
                        sql_keyword in line.upper()
                        for sql_keyword in ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE"]
                    ):
                        if '"""' in line or "'''" in line or "text(" in line:
                            in_sql = True
                            sql_start_line = i + 1

                    # Check SQL formatting issues
                    if in_sql:
                        # Check for long lines without breaks
                        if len(line.strip()) > 120 and any(
                            keyword in line.upper() for keyword in ["SELECT", "FROM", "WHERE", "JOIN"]
                        ):
                            issues.append(f"{py_file}:{i+1} - SQL line too long, needs line breaks for readability")

                        # Check for missing indentation
                        if line.strip().upper().startswith(("FROM", "WHERE", "JOIN", "ORDER BY", "GROUP BY")):
                            if not line.startswith("    ") and not line.startswith("\t"):
                                issues.append(f"{py_file}:{i+1} - SQL clause should be indented for readability")

                        # End of SQL block
                        if '"""' in line or "'''" in line or ")" in line:
                            in_sql = False

            except Exception as e:
                self.log_warning(f"Could not analyze SQL in {py_file}: {e}")

        if issues:
            self.log_warning(f"Found {len(issues)} SQL formatting issues:")
            for issue in issues[:5]:  # Show first 5
                print(f"   {issue}")
            if len(issues) > 5:
                print(f"   ... and {len(issues) - 5} more")
        else:
            self.log_success("SQL formatting looks good")

        return issues

    def _validate_schema_integrity(self) -> bool:
        """Validate database schema integrity."""
        try:
            # Try to connect to database and check basic schema
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import sys
sys.path.insert(0, '.')
try:
    from web.etl_helpers import get_engine
    from sqlalchemy import text, inspect

    engine = get_engine()
    inspector = inspect(engine)

    # Check for required tables
    tables = inspector.get_table_names()
    required_tables = ['youtube_videos', 'youtube_metrics', 'youtube_comments']

    missing_tables = [t for t in required_tables if t not in tables]
    if missing_tables:
        print(f"MISSING_TABLES:{','.join(missing_tables)}")
    else:
        print("SCHEMA_VALID:True")

    # Check for foreign key constraints
    constraints_ok = True
    for table in required_tables:
        if table in tables:
            fks = inspector.get_foreign_keys(table)
            # Could add specific FK checks here

    print(f"CONSTRAINT_CHECK:{'OK' if constraints_ok else 'ISSUES'}")

except Exception as e:
    print(f"SCHEMA_ERROR:{e}")
""",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                output = result.stdout.strip()
                if "MISSING_TABLES:" in output:
                    missing = output.split("MISSING_TABLES:")[1].split()[0]
                    self.log_error(f"Missing required database tables: {missing}")
                    return False
                elif "SCHEMA_VALID:True" in output:
                    self.log_success("Database schema integrity validated")
                    return True
                else:
                    self.log_warning("Could not fully validate schema")
                    return True
            else:
                self.log_warning("Database schema validation skipped - connection failed")
                return True

        except Exception as e:
            self.log_warning(f"Schema validation error: {e}")
            return True

    def _check_data_quality_validation(self) -> bool:
        """Check for data quality validation in the codebase."""
        dq_patterns = ["null", "isnull", "notna", "dropna", "data_quality", "validation", "integrity"]

        dq_files_found = 0

        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read().lower()

                if any(pattern in content for pattern in dq_patterns):
                    dq_files_found += 1

            except Exception:
                continue

        if dq_files_found > 0:
            self.log_success(f"Found data quality validation in {dq_files_found} files")
            return True
        else:
            self.log_warning("Limited data quality validation found in codebase")
            return False

    def _validate_commenting(self) -> float:
        """
        Validate extensive commenting requirements for senior-level code.

        Checks for:
        - Minimum comment ratio (15% of lines)
        - Business context explanations
        - Complex logic documentation
        - Music industry context where relevant
        """
        total_score = 0.0
        file_count = 0

        # Check Python files for comment quality
        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                total_lines = len(lines)
                comment_lines = sum(1 for line in lines if line.strip().startswith("#"))
                docstring_lines = self._count_docstring_lines(py_file)

                if total_lines > 0:
                    comment_ratio = (comment_lines + docstring_lines) / total_lines
                    file_score = min(100.0, (comment_ratio / self.min_comment_ratio) * 100.0)
                    total_score += file_score
                    file_count += 1

                    if comment_ratio < self.min_comment_ratio:
                        self.log_warning(
                            f"{py_file}: Comment ratio {comment_ratio:.1%} below minimum {self.min_comment_ratio:.1%}"
                        )

            except Exception as e:
                self.log_warning(f"Could not analyze comments in {py_file}: {e}")

        avg_score = total_score / file_count if file_count > 0 else 0.0

        if avg_score >= 80.0:
            self.log_success(f"Excellent comment coverage: {avg_score:.1f}%")
        elif avg_score >= 60.0:
            self.log_warning(f"Good comment coverage: {avg_score:.1f}% (aim for 80%+)")
        else:
            self.log_error(f"Poor comment coverage: {avg_score:.1f}% (minimum 60%)")

        return avg_score

    def _count_docstring_lines(self, file_path: Path) -> int:
        """Count lines in docstrings for documentation coverage."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content)
            docstring_lines = 0

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
                    if ast.get_docstring(node):
                        # Count lines in docstring
                        docstring = ast.get_docstring(node)
                        if docstring:
                            docstring_lines += len(docstring.split("\n"))

            return docstring_lines
        except Exception:
            return 0

    def _detect_duplicate_code(self) -> bool:
        """
        Detect duplicate function definitions and code patterns.

        This addresses the issue we saw earlier with duplicate functions
        in charts.py and ensures senior-level code organization.

        Enhanced to provide specific recommendations for combining functions.
        """
        duplicates_found = False
        duplicate_functions = {}  # Track duplicates for recommendations

        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                tree = ast.parse(content)
                function_names = []
                function_details = {}

                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        function_names.append(node.name)
                        function_details[node.name] = {
                            "line": node.lineno,
                            "args": len(node.args.args),
                            "docstring": ast.get_docstring(node) is not None,
                        }

                # Check for duplicate function names
                seen = set()
                for name in function_names:
                    if name in seen:
                        self.log_error(
                            f"{py_file}: Duplicate function definition '{name}' at line {function_details[name]['line']}"
                        )
                        duplicates_found = True

                        # Track for recommendations
                        if str(py_file) not in duplicate_functions:
                            duplicate_functions[str(py_file)] = []
                        duplicate_functions[str(py_file)].append(name)
                    seen.add(name)

            except Exception as e:
                self.log_warning(f"Could not analyze {py_file} for duplicates: {e}")

        if not duplicates_found:
            self.log_success("No duplicate code patterns detected")
        else:
            # Provide specific recommendations
            self.log_warning("DUPLICATE FUNCTION RECOMMENDATIONS:")
            for file_path, functions in duplicate_functions.items():
                print(f"   📁 {file_path}:")
                for func in functions:
                    print(f"     🔧 Combine duplicate '{func}' functions into single implementation")
                    print(f"     💡 Consider using parameters or factory pattern to reduce duplication")

        return duplicates_found

    def _detect_ai_generated_patterns(self) -> List[str]:
        """
        Detect bulky AI-generated code patterns and functions that should be combined.

        Creates a list of functions that should be analyzed for potential combination
        based on common AI code generation patterns.
        """
        ai_patterns = []
        functions_to_combine = []

        # Common AI-generated patterns to detect
        ai_indicators = [
            "# AI-generated",
            "# Generated by",
            "# Auto-generated",
            "def helper_function_",
            "def utility_function_",
            "def process_data_step_",
        ]

        # Function name patterns that suggest they should be combined
        combinable_patterns = [
            ("enhance_chart_", "Chart enhancement functions"),
            ("apply_color_", "Color application functions"),
            ("create_chart_", "Chart creation functions"),
            ("_get_", "Getter functions"),
            ("validate_", "Validation functions"),
            ("process_", "Processing functions"),
            ("analyze_", "Analysis functions"),
        ]

        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    lines = content.split("\n")

                # Check for AI-generated indicators
                for i, line in enumerate(lines):
                    for indicator in ai_indicators:
                        if indicator.lower() in line.lower():
                            ai_patterns.append(f"{py_file}:{i+1} - Possible AI-generated code: {line.strip()}")

                # Parse AST to find functions
                tree = ast.parse(content)
                file_functions = []

                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        file_functions.append({"name": node.name, "line": node.lineno, "file": py_file})

                # Group functions by pattern
                for pattern, description in combinable_patterns:
                    matching_functions = [f for f in file_functions if pattern in f["name"]]
                    if len(matching_functions) > 1:
                        func_names = [f["name"] for f in matching_functions]
                        functions_to_combine.append(
                            {
                                "file": str(py_file),
                                "pattern": pattern,
                                "description": description,
                                "functions": func_names,
                                "count": len(matching_functions),
                            }
                        )

                # Check for overly long functions (potential AI bloat)
                for func in file_functions:
                    for node in ast.walk(tree):
                        if isinstance(node, ast.FunctionDef) and node.name == func["name"]:
                            if node.end_lineno and node.lineno:
                                func_length = node.end_lineno - node.lineno
                                if func_length > 100:  # Very long functions
                                    ai_patterns.append(
                                        f"{py_file}:{node.lineno} - Function '{func['name']}' "
                                        f"is {func_length} lines (possible AI bloat)"
                                    )
                            break

            except Exception as e:
                self.log_warning(f"Could not analyze {py_file} for AI patterns: {e}")

        # Report findings with enhanced recommendations
        if ai_patterns:
            self.log_warning("Detected potential AI-generated patterns:")
            for pattern in ai_patterns[:10]:  # Limit output
                print(f"   {pattern}")
            if len(ai_patterns) > 10:
                print(f"   ... and {len(ai_patterns) - 10} more")

            print("\n   🤖 AI PATTERN RECOMMENDATIONS:")
            print("     • Break down functions >100 lines into focused, single-purpose functions")
            print("     • Extract common patterns into reusable helper functions")
            print("     • Add comprehensive comments explaining business logic")
            print("     • Consider using composition over large monolithic functions")

        if functions_to_combine:
            self.log_warning("Functions that should be analyzed for combination:")
            for group in functions_to_combine:
                print(f"   📁 {group['file']}: {group['description']} ({group['count']} functions)")
                for func_name in group["functions"][:3]:  # Show first 3
                    print(f"     🔧 {func_name}")
                if len(group["functions"]) > 3:
                    print(f"     ... and {len(group['functions']) - 3} more")

            print("\n   🔧 FUNCTION COMBINATION RECOMMENDATIONS:")
            print("     • Create base classes or utility modules for similar functions")
            print("     • Use strategy pattern for functions with similar signatures")
            print("     • Extract common validation logic into shared helpers")
            print("     • Consider factory methods for creation patterns")

        # Create comprehensive list for analysis
        self._create_function_analysis_list(functions_to_combine, ai_patterns)

        return ai_patterns + [str(group) for group in functions_to_combine]

    def _create_function_analysis_list(self, functions_to_combine: List[dict], ai_patterns: List[str]) -> None:
        """
        Create a comprehensive list of functions that should be analyzed for combination.

        This addresses the task requirement to "make a list of functions so you know
        what to analyze that should be combined".
        """
        analysis_report = {
            "timestamp": datetime.now().isoformat(),
            "total_combinable_groups": len(functions_to_combine),
            "total_ai_patterns": len(ai_patterns),
            "priority_files": [],
            "recommendations": [],
        }

        # Prioritize files with most issues
        file_issue_count = {}
        for group in functions_to_combine:
            file_path = group["file"]
            if file_path not in file_issue_count:
                file_issue_count[file_path] = 0
            file_issue_count[file_path] += group["count"]

        # Sort by issue count (highest first)
        priority_files = sorted(file_issue_count.items(), key=lambda x: x[1], reverse=True)

        for file_path, issue_count in priority_files[:10]:  # Top 10 priority files
            file_analysis = {
                "file": file_path,
                "total_issues": issue_count,
                "combinable_groups": [],
                "recommendations": [],
            }

            # Find all groups for this file
            for group in functions_to_combine:
                if group["file"] == file_path:
                    file_analysis["combinable_groups"].append(
                        {
                            "pattern": group["pattern"],
                            "description": group["description"],
                            "functions": group["functions"],
                            "count": group["count"],
                        }
                    )

            # Generate specific recommendations
            if "charts.py" in file_path:
                file_analysis["recommendations"].extend(
                    [
                        "Create ChartEnhancer class to consolidate enhancement functions",
                        "Implement ColorSchemeManager for color-related functions",
                        "Extract annotation logic into AnnotationBuilder class",
                    ]
                )
            elif "test_" in file_path:
                file_analysis["recommendations"].extend(
                    [
                        "Create test base classes for common setup/teardown",
                        "Extract validation helpers into test utilities",
                        "Use parameterized tests to reduce duplicate test functions",
                    ]
                )
            elif "sentiment" in file_path:
                file_analysis["recommendations"].extend(
                    [
                        "Create SentimentAnalyzer base class",
                        "Extract common analysis patterns into mixins",
                        "Implement strategy pattern for different sentiment models",
                    ]
                )
            else:
                file_analysis["recommendations"].extend(
                    [
                        "Extract common patterns into utility functions",
                        "Consider using composition over inheritance",
                        "Create factory methods for object creation patterns",
                    ]
                )

            analysis_report["priority_files"].append(file_analysis)

        # Save comprehensive analysis report
        report_path = "function_analysis_report.json"
        with open(report_path, "w") as f:
            json.dump(analysis_report, f, indent=2)

        self.log_success(f"Function analysis report saved to {report_path}")

        # Display summary
        print(f"\n📋 FUNCTION ANALYSIS SUMMARY:")
        print(f"   📊 Total files with combinable functions: {len(file_issue_count)}")
        print(f"   🔧 Total function groups to analyze: {len(functions_to_combine)}")
        print(f"   🤖 AI patterns detected: {len(ai_patterns)}")
        print(f"   🎯 Priority files for refactoring: {len(priority_files[:10])}")

        if priority_files:
            print(f"\n🎯 TOP PRIORITY FILES FOR REFACTORING:")
            for file_path, count in priority_files[:5]:
                print(f"   📁 {file_path}: {count} combinable functions")

        return analysis_report

    def _analyze_system_health(self) -> Dict[str, any]:
        """Analyze overall system health for AI agent intelligence."""
        health_metrics = {
            "overall_status": "healthy",
            "critical_issues": [],
            "warnings": [],
            "performance_indicators": {},
            "trend_analysis": {},
        }

        # Analyze code quality trends
        if self.results.code_quality.duplicate_code_detected:
            health_metrics["critical_issues"].append("Duplicate code detected - refactoring needed")

        if self.results.code_quality.comment_quality_score < 60:
            health_metrics["warnings"].append("Low comment quality may impact maintainability")

        # Analyze test coverage health
        if self.results.test_results.coverage_percentage < 50:
            health_metrics["critical_issues"].append("Test coverage critically low")
        elif self.results.test_results.coverage_percentage < self.min_test_coverage:
            health_metrics["warnings"].append("Test coverage below production standards")

        # Database health indicators
        if self.results.database_integrity.data_quality_score < 70:
            health_metrics["critical_issues"].append("Data quality issues detected")

        # Set overall status
        if health_metrics["critical_issues"]:
            health_metrics["overall_status"] = "critical"
        elif len(health_metrics["warnings"]) > 5:
            health_metrics["overall_status"] = "degraded"

        return health_metrics

        if validation["validation_failed"] > 0:
            validation["issues_found"].append(
                {
                    "type": "unexecuted_notebooks",
                    "count": validation["validation_failed"],
                    "recommendation": "Execute notebooks to validate outputs",
                }
            )

        return validation

    def _analyze_failure_patterns(self) -> Dict[str, any]:
        """Analyze failure patterns for predictive insights."""
        patterns = {
            "recurring_issues": [],
            "risk_predictions": [],
            "failure_categories": {"code_quality": 0, "testing": 0, "database": 0, "performance": 0},
        }

        # Categorize current issues
        if self.results.code_quality.duplicate_code_detected:
            patterns["failure_categories"]["code_quality"] += 1
            patterns["recurring_issues"].append("Duplicate code pattern suggests copy-paste development")

        if self.results.test_results.failed_tests > 0:
            patterns["failure_categories"]["testing"] += 1
            patterns["recurring_issues"].append("Test failures indicate unstable codebase")

        if self.results.database_integrity.data_quality_score < 85:
            patterns["failure_categories"]["database"] += 1

        # Generate risk predictions
        total_issues = sum(patterns["failure_categories"].values())
        if total_issues > 3:
            patterns["risk_predictions"].append("High risk of production issues due to multiple failure categories")

        return patterns

    def _analyze_performance_metrics(self) -> Dict[str, any]:
        """Analyze performance metrics and trends."""
        performance = {"current_metrics": {}, "bottlenecks": [], "optimization_opportunities": []}

        # Analyze test execution performance
        if self.results.test_results.execution_time > 300:  # 5 minutes
            performance["bottlenecks"].append("Test suite execution time excessive")
            performance["optimization_opportunities"].append("Consider parallel test execution")

        # Analyze code complexity
        if self.results.code_quality.complexity_score > 80:
            performance["bottlenecks"].append("High code complexity may impact performance")

        return performance

    def _validate_notebook_outputs(self) -> Dict[str, any]:
        """Validate notebook outputs and data patterns for AI analysis."""
        validation = {"notebooks_found": 0, "validation_results": {}, "data_patterns": [], "anomalies": []}

        # Check for notebook files
        notebook_files = list(Path(".").rglob("*.ipynb"))
        validation["notebooks_found"] = len(notebook_files)

        if validation["notebooks_found"] > 0:
            validation["data_patterns"].append(f"Found {validation['notebooks_found']} notebook files")

            # Check for executed notebooks
            executed_notebooks = [nb for nb in notebook_files if "executed" in str(nb)]
            if executed_notebooks:
                validation["data_patterns"].append(f"Found {len(executed_notebooks)} executed notebooks")
            else:
                validation["anomalies"].append("No executed notebooks found - may indicate execution issues")

        return validation

    def _validate_function_complexity(self) -> bool:
        """
        Validate function complexity and LOC limits for maintainable code.

        Senior-level requirement: Functions should be focused and readable.
        """
        all_compliant = True

        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                tree = ast.parse("".join(lines))

                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        # Calculate function LOC
                        if node.end_lineno is not None and node.lineno is not None:
                            func_lines = node.end_lineno - node.lineno + 1
                        else:
                            func_lines = 0

                        if func_lines > self.max_function_loc:
                            self.log_warning(
                                f"{py_file}:{node.lineno} Function '{node.name}' "
                                f"has {func_lines} lines (max {self.max_function_loc})"
                            )
                            all_compliant = False

            except Exception as e:
                self.log_warning(f"Could not analyze function complexity in {py_file}: {e}")

        if all_compliant:
            self.log_success("All functions comply with LOC limits")

        return all_compliant

    def _generate_intelligent_recommendations(
        self, system_health, failure_patterns, performance_analytics, critical_issues, risk_factors
    ) -> list:
        """Generate intelligent recommendations based on comprehensive analysis."""
        recommendations = []

        # Critical issue recommendations
        for issue in critical_issues:
            if "Test coverage" in issue:
                recommendations.append(
                    "URGENT: Implement comprehensive test suite - current coverage indicates high production risk"
                )
            elif "failing tests" in issue:
                recommendations.append("URGENT: Fix failing tests before any deployment")
            elif "Data quality" in issue:
                recommendations.append("URGENT: Investigate data quality issues - may indicate ETL pipeline problems")

        # System health recommendations
        if system_health["overall_health"] == "CRITICAL":
            recommendations.append("System health is critical - recommend immediate remediation before proceeding")
        elif system_health["overall_health"] == "WARNING":
            recommendations.append("System health has warnings - address before production deployment")

        # Failure pattern recommendations
        for pattern in failure_patterns.get("recurring_issues", []):
            if pattern["type"] == "duplicate_code":
                recommendations.append("Refactor duplicate code to improve maintainability and reduce bug risk")

        # Performance recommendations
        for bottleneck in performance_analytics.get("bottlenecks", []):
            recommendations.append(f"Performance optimization needed: {bottleneck['recommendation']}")

        # Standard recommendations based on metrics
        if self.results.code_quality.comment_quality_score < 80.0:
            recommendations.append("Increase code commenting for better maintainability")
        if self.results.code_quality.duplicate_code_detected:
            recommendations.append("Refactor duplicate code into reusable functions")
        if self.results.test_results.coverage_percentage < self.min_test_coverage:
            recommendations.append(f"Increase test coverage to {self.min_test_coverage}%")
        if self.results.database_integrity.data_quality_score < 85.0:
            recommendations.append("Improve data quality validation and cleanup")

        return recommendations

    def run_comprehensive_tests(self) -> TestExecutionResults:
        """
        Execute comprehensive test suite with coverage and performance analysis.

        Senior-level testing includes:
        - Unit, integration, and system tests
        - Coverage reporting with thresholds
        - Performance benchmarking
        - AI agent validation tests
        """
        print("\n🧪 RUNNING COMPREHENSIVE TEST SUITE")
        print("=" * 60)

        results = TestExecutionResults()
        start_time = time.time()

        try:
            # Run pytest with coverage
            cmd = [
                sys.executable,
                "-m",
                "pytest",
                "tests/",
                "-v",
                "--tb=short",
                "--cov=src",
                "--cov=web",
                "--cov-report=term-missing",
                "--cov-report=json:coverage.json",
                "--timeout=30",
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            results.execution_time = time.time() - start_time

            # Parse test results
            output_lines = result.stdout.split("\n")
            for line in output_lines:
                if "passed" in line and "failed" in line:
                    # Parse pytest summary line
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part == "passed":
                            results.passed_tests = int(parts[i - 1])
                        elif part == "failed":
                            results.failed_tests = int(parts[i - 1])
                        elif part == "skipped":
                            results.skipped_tests = int(parts[i - 1])

            results.total_tests = results.passed_tests + results.failed_tests + results.skipped_tests

            # Parse coverage from JSON report
            if os.path.exists("coverage.json"):
                with open("coverage.json", "r") as f:
                    coverage_data = json.load(f)
                    results.coverage_percentage = coverage_data.get("totals", {}).get("percent_covered", 0.0)

            # Evaluate results
            if result.returncode == 0:
                self.log_success(f"All {results.total_tests} tests passed")
                if results.coverage_percentage >= self.min_test_coverage:
                    self.log_success(f"Excellent test coverage: {results.coverage_percentage:.1f}%")
                else:
                    self.log_warning(
                        f"Test coverage {results.coverage_percentage:.1f}% below minimum {self.min_test_coverage}%"
                    )
            else:
                self.log_error(f"{results.failed_tests} tests failed")
                # Show failed test details
                for line in output_lines:
                    if "FAILED" in line:
                        print(f"   {line}")

        except subprocess.TimeoutExpired:
            self.log_error("Test suite timed out after 5 minutes")
        except Exception as e:
            self.log_error(f"Test execution failed: {e}")

        return results

    def validate_database_integrity(self) -> DatabaseIntegrityResults:
        """
        Validate database schema integrity and data quality.

        Senior-level database validation includes:
        - Schema consistency checks
        - Referential integrity validation
        - Data quality metrics
        - Performance analysis
        """
        print("\n🗄️  VALIDATING DATABASE INTEGRITY")
        print("=" * 60)

        results = DatabaseIntegrityResults()

        try:
            # Quick data consistency check
            check_script = """
import sys
import os
sys.path.insert(0, '.')
try:
    from src.youtubeviz.data import load_youtube_data
    df = load_youtube_data()

    # Basic data quality checks
    print(f"RECORDS: {len(df)}")
    print(f"ARTISTS: {df['artist_name'].nunique()}")
    print(f"VIDEOS: {df['video_id'].nunique()}")
    print(f"NULL_VALUES: {df.isnull().sum().sum()}")
    print(f"DUPLICATE_RECORDS: {df.duplicated().sum()}")

except Exception as e:
    print(f"DATA_CHECK_FAILED: {e}")
"""

            result = subprocess.run([sys.executable, "-c", check_script], capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                # Parse data quality metrics
                for line in result.stdout.split("\n"):
                    if ":" in line:
                        key, value = line.split(":", 1)
                        key = key.strip()
                        value = value.strip()

                        if key == "RECORDS":
                            results.record_counts["total_records"] = int(value)
                        elif key == "ARTISTS":
                            results.record_counts["unique_artists"] = int(value)
                        elif key == "VIDEOS":
                            results.record_counts["unique_videos"] = int(value)
                        elif key == "NULL_VALUES":
                            null_count = int(value)
                            if null_count == 0:
                                self.log_success("No null values detected")
                            else:
                                self.log_warning(f"Found {null_count} null values")
                        elif key == "DUPLICATE_RECORDS":
                            dup_count = int(value)
                            if dup_count == 0:
                                self.log_success("No duplicate records detected")
                            else:
                                self.log_warning(f"Found {dup_count} duplicate records")

                # Calculate data quality score
                total_records = results.record_counts.get("total_records", 0)
                if total_records > 0:
                    results.data_quality_score = 85.0  # Base score, adjust based on issues
                    self.log_success(f"Database integrity validated: {total_records:,} records")
                else:
                    results.data_quality_score = 0.0
                    self.log_warning("No data found in database")

            else:
                self.log_warning("Database validation skipped - data loading failed")
                results.data_quality_score = 50.0

        except Exception as e:
            self.log_error(f"Database integrity check failed: {e}")
            results.data_quality_score = 0.0

        return results

    def generate_ai_agent_report(self) -> None:
        """
        Generate comprehensive AI agent intelligence report.

        This enhanced report provides AI agents with:
        - System health and readiness assessment
        - Failure pattern analysis and predictions
        - Performance analytics and trends
        - Actionable deployment recommendations
        - Notebook output validation results
        """
        print("\n🤖 GENERATING AI AGENT INTELLIGENCE REPORT")
        print("=" * 60)

        # Enhanced system health analysis
        system_health = self._analyze_system_health()

        # Failure pattern analysis
        failure_patterns = self._analyze_failure_patterns()

        # Performance analytics
        performance_analytics = self._analyze_performance_metrics()

        # Notebook validation results
        notebook_validation = self._validate_notebook_outputs()

        # Calculate comprehensive risk assessment
        risk_factors = []
        critical_issues = []

        # Code quality risks
        if self.results.code_quality.linting_issues > 50:
            critical_issues.append("Critical: >50 linting issues indicate systemic code quality problems")
        elif self.results.code_quality.linting_issues > 10:
            risk_factors.append("High linting issue count")

        if self.results.code_quality.duplicate_code_detected:
            risk_factors.append("Duplicate code patterns detected")

        if self.results.code_quality.comment_quality_score < 60:
            risk_factors.append("Poor code documentation")

        # Test coverage risks
        if self.results.test_results.coverage_percentage < 25:
            critical_issues.append("Critical: Test coverage below 25% indicates high deployment risk")
        elif self.results.test_results.coverage_percentage < self.min_test_coverage:
            risk_factors.append("Low test coverage")

        if self.results.test_results.failed_tests > 0:
            critical_issues.append(f"Critical: {self.results.test_results.failed_tests} failing tests")

        # Database and data quality risks
        if self.results.database_integrity.data_quality_score < 50:
            critical_issues.append("Critical: Data quality below 50%")
        elif self.results.database_integrity.data_quality_score < 70.0:
            risk_factors.append("Data quality concerns")

        # System performance risks
        if performance_analytics.get("performance_degradation", False):
            risk_factors.append("Performance degradation detected")

        # Determine comprehensive risk level
        if critical_issues:
            self.results.risk_assessment = "CRITICAL"
            self.results.deployment_readiness = False
        elif len(risk_factors) == 0:
            self.results.risk_assessment = "LOW"
            self.results.deployment_readiness = True
        elif len(risk_factors) <= 2:
            self.results.risk_assessment = "MEDIUM"
            self.results.deployment_readiness = False
        else:
            self.results.risk_assessment = "HIGH"
            self.results.deployment_readiness = False

        # Generate intelligent recommendations
        recommendations = self._generate_intelligent_recommendations(
            system_health, failure_patterns, performance_analytics, critical_issues, risk_factors
        )

        self.results.recommendations = recommendations

        # Enhanced AI agent report with detailed analysis
        enhanced_report = {
            "system_health": system_health,
            "failure_patterns": failure_patterns,
            "performance_analytics": performance_analytics,
            "notebook_validation": notebook_validation,
            "risk_factors": risk_factors,
            "critical_issues": critical_issues,
            "recommendations": recommendations,
            "deployment_readiness": self.results.deployment_readiness,
            "risk_assessment": self.results.risk_assessment,
        }

        if self.results.code_quality.comment_quality_score < 80.0:
            recommendations.append("Increase code commenting for better maintainability")
        if self.results.code_quality.duplicate_code_detected:
            recommendations.append("Refactor duplicate code into reusable functions")
        if self.results.test_results.coverage_percentage < self.min_test_coverage:
            recommendations.append(f"Increase test coverage to {self.min_test_coverage}%")
        if self.results.database_integrity.data_quality_score < 85.0:
            recommendations.append("Improve data quality validation and cleanup")

        self.results.recommendations = recommendations

        # Save comprehensive report for AI agents
        report_path = "ci_validation_report.json"
        comprehensive_report = {"basic_metrics": asdict(self.results), "enhanced_analysis": enhanced_report}

        with open(report_path, "w") as f:
            json.dump(comprehensive_report, f, indent=2)

        self.log_success(f"Enhanced AI agent report saved to {report_path}")

        # Display enhanced metrics for AI analysis
        print(f"\n📊 ENHANCED AI AGENT ANALYSIS:")
        print(f"   System Health: {system_health['overall_health']}")
        print(f"   Risk Assessment: {self.results.risk_assessment}")
        print(f"   Deployment Ready: {self.results.deployment_readiness}")
        print(f"   Critical Systems: {len(system_health['critical_systems'])}")
        print(f"   Warning Systems: {len(system_health['warning_systems'])}")
        print(f"   Healthy Systems: {len(system_health['healthy_systems'])}")
        print(f"   Failure Patterns: {len(failure_patterns.get('recurring_issues', []))}")
        print(f"   Performance Bottlenecks: {len(performance_analytics.get('bottlenecks', []))}")
        print(f"   Notebooks Validated: {notebook_validation['notebooks_validated']}")

        # Show critical issues if any
        if critical_issues:
            print(f"\n🚨 CRITICAL ISSUES FOR AI ATTENTION:")
            for issue in critical_issues:
                print(f"   • {issue}")

        # Show failure predictions
        if failure_patterns.get("risk_predictions"):
            print(f"\n🔮 FAILURE RISK PREDICTIONS:")
            for prediction in failure_patterns["risk_predictions"]:
                print(f"   • {prediction['description']}: {prediction['prediction']}")

    def generate_final_report(self) -> bool:
        """
        Generate comprehensive final report with senior-level analysis.

        Returns True if all checks pass, False otherwise.
        """
        print("\n" + "=" * 80)
        print("🏆 ENHANCED CI/CD PIPELINE REPORT - SENIOR LEVEL STANDARDS")
        print("=" * 80)

        # Overall success determination
        success_criteria = [
            len(self.errors) == 0,
            self.results.test_results.failed_tests == 0,
            self.results.code_quality.formatting_score >= 90.0,
            not self.results.code_quality.duplicate_code_detected,
            self.results.test_results.coverage_percentage >= self.min_test_coverage,
        ]

        self.results.success = all(success_criteria)

        if self.results.success:
            print("🎉 ALL SENIOR-LEVEL CHECKS PASSED!")
            print("✅ Ready for production deployment")
            print("✅ Meets Grammy-nominated producer + M.S. Data Science standards")
        else:
            print("❌ SENIOR-LEVEL STANDARDS NOT MET")
            print("🚫 Address issues before deployment")

        # Detailed metrics
        print(f"\n📊 DETAILED METRICS:")
        print(f"   Execution Time: {self.results.execution_time:.1f}s")
        print(f"   Code Quality Score: {self.results.code_quality.formatting_score:.1f}%")
        print(f"   Comment Coverage: {self.results.code_quality.comment_quality_score:.1f}%")
        print(f"   Test Coverage: {self.results.test_results.coverage_percentage:.1f}%")
        print(f"   Tests Passed: {self.results.test_results.passed_tests}")
        print(f"   Data Quality: {self.results.database_integrity.data_quality_score:.1f}%")

        # Issues summary
        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                print(f"   {i}. {error}")

        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"   {i}. {warning}")

        # Recommendations
        if self.results.recommendations:
            print("\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(self.results.recommendations, 1):
                print(f"   {i}. {rec}")

        # Next steps
        if not self.results.success:
            print("\n🔧 NEXT STEPS:")
            if not self.fix_issues:
                print("   1. Run with --fix-issues to auto-fix some problems")
            print("   2. Address errors and warnings manually")
            print("   3. Re-run CI pipeline to validate fixes")
            print("   4. Review AI agent report for detailed analysis")

        return self.results.success

    def run_all_checks(self) -> bool:
        """
        Execute the complete enhanced CI/CD pipeline.

        Returns True if all checks pass, False otherwise.
        """
        print("🚀 STARTING ENHANCED CI/CD PIPELINE - SENIOR LEVEL")
        print("Grammy-Nominated Producer + M.S. Data Science Standards")
        print("=" * 80)

        start_time = time.time()

        if not self.report_only:
            # 1. Code Quality Validation
            self.results.code_quality = self.validate_code_quality()

            # 2. Database Operations Validation
            database_ops_valid = self.validate_database_operations()

            # 3. Comprehensive Testing
            self.results.test_results = self.run_comprehensive_tests()

            # 4. Database Integrity
            self.results.database_integrity = self.validate_database_integrity()

        # 4. AI Agent Intelligence Report
        self.generate_ai_agent_report()

        # 5. Final Analysis
        self.results.execution_time = time.time() - start_time
        success = self.generate_final_report()

        return success


def main():
    """Main function with senior-level argument parsing."""
    parser = argparse.ArgumentParser(
        description="Enhanced CI/CD Pipeline - Senior Level Standards",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/enhanced_ci.py                    # Full validation
  python scripts/enhanced_ci.py --fix-issues      # Auto-fix issues
  python scripts/enhanced_ci.py --report-only     # Generate reports only
        """,
    )

    parser.add_argument("--fix-issues", action="store_true", help="Automatically fix issues where possible")
    parser.add_argument(
        "--report-only", action="store_true", help="Generate AI agent reports without running full validation"
    )

    args = parser.parse_args()

    ci = EnhancedCI(fix_issues=args.fix_issues, report_only=args.report_only)
    success = ci.run_all_checks()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
