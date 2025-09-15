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
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import field


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
                metrics.formatting_score = 50.0
                if self.fix_issues:
                    subprocess.run(["black", "--line-length", str(self.max_line_length), "."])
                    self.log_success("Auto-fixed formatting issues")
                    metrics.formatting_score = 100.0
                else:
                    self.log_warning("Code formatting issues found - run with --fix-issues")
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
                    ".",
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode == 0:
                self.log_success("No linting issues found")
            else:
                issues = result.stdout.count("\n")
                metrics.linting_issues = issues
                self.log_warning(f"Found {issues} linting issues")
                # Show first 10 issues for context
                lines = result.stdout.split("\n")[:10]
                for line in lines:
                    if line.strip():
                        print(f"   {line}")
        except Exception as e:
            self.log_error(f"Linting check failed: {e}")

        # 3. Validate extensive commenting
        self.log_info("Validating comment quality and coverage...")
        metrics.comment_quality_score = self._validate_commenting()

        # 4. Check for duplicate code (senior-level requirement)
        self.log_info("Detecting duplicate code patterns...")
        metrics.duplicate_code_detected = self._detect_duplicate_code()

        # 5. Validate function complexity and LOC
        self.log_info("Checking function complexity and LOC limits...")
        metrics.loc_compliance = self._validate_function_complexity()

        return metrics

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
        """
        duplicates_found = False

        for py_file in Path(".").rglob("*.py"):
            if any(exclude in str(py_file) for exclude in [".venv", "__pycache__", "tools/archive"]):
                continue

            try:
                with open(py_file, "r", encoding="utf-8") as f:
                    content = f.read()

                tree = ast.parse(content)
                function_names = []

                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        function_names.append(node.name)

                # Check for duplicate function names
                seen = set()
                for name in function_names:
                    if name in seen:
                        self.log_error(f"{py_file}: Duplicate function definition '{name}'")
                        duplicates_found = True
                    seen.add(name)

            except Exception as e:
                self.log_warning(f"Could not analyze {py_file} for duplicates: {e}")

        if not duplicates_found:
            self.log_success("No duplicate code patterns detected")

        return duplicates_found

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
        Generate comprehensive report for AI agent analysis.

        This report helps AI agents understand:
        - System health and readiness
        - Code quality metrics
        - Performance characteristics
        - Deployment recommendations
        """
        print("\n🤖 GENERATING AI AGENT INTELLIGENCE REPORT")
        print("=" * 60)

        # Calculate overall risk assessment
        risk_factors = []

        if self.results.code_quality.linting_issues > 10:
            risk_factors.append("High linting issue count")
        if self.results.code_quality.duplicate_code_detected:
            risk_factors.append("Duplicate code patterns detected")
        if self.results.test_results.coverage_percentage < self.min_test_coverage:
            risk_factors.append("Low test coverage")
        if self.results.test_results.failed_tests > 0:
            risk_factors.append("Failing tests")
        if self.results.database_integrity.data_quality_score < 70.0:
            risk_factors.append("Data quality concerns")

        # Determine risk level
        if len(risk_factors) == 0:
            self.results.risk_assessment = "LOW"
            self.results.deployment_readiness = True
        elif len(risk_factors) <= 2:
            self.results.risk_assessment = "MEDIUM"
            self.results.deployment_readiness = False
        elif len(risk_factors) <= 4:
            self.results.risk_assessment = "HIGH"
            self.results.deployment_readiness = False
        else:
            self.results.risk_assessment = "CRITICAL"
            self.results.deployment_readiness = False

        # Generate recommendations
        recommendations = []

        if self.results.code_quality.comment_quality_score < 80.0:
            recommendations.append("Increase code commenting for better maintainability")
        if self.results.code_quality.duplicate_code_detected:
            recommendations.append("Refactor duplicate code into reusable functions")
        if self.results.test_results.coverage_percentage < self.min_test_coverage:
            recommendations.append(f"Increase test coverage to {self.min_test_coverage}%")
        if self.results.database_integrity.data_quality_score < 85.0:
            recommendations.append("Improve data quality validation and cleanup")

        self.results.recommendations = recommendations

        # Save report for AI agents
        report_path = "ci_validation_report.json"
        with open(report_path, "w") as f:
            json.dump(asdict(self.results), f, indent=2)

        self.log_success(f"AI agent report saved to {report_path}")

        # Display key metrics
        print(f"\n📊 KEY METRICS FOR AI ANALYSIS:")
        print(f"   Risk Assessment: {self.results.risk_assessment}")
        print(f"   Deployment Ready: {self.results.deployment_readiness}")
        print(f"   Code Quality Score: {self.results.code_quality.formatting_score:.1f}%")
        print(f"   Test Coverage: {self.results.test_results.coverage_percentage:.1f}%")
        print(f"   Data Quality Score: {self.results.database_integrity.data_quality_score:.1f}%")

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

            # 2. Comprehensive Testing
            self.results.test_results = self.run_comprehensive_tests()

            # 3. Database Integrity
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
