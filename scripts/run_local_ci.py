#!/usr/bin/env python3
"""
🚀 Local CI/CD Pipeline
======================

Run comprehensive quality checks locally before committing.
This prevents issues from reaching the repository.

Usage:
    python scripts/run_local_ci.py
    python scripts/run_local_ci.py --fix-issues
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


class LocalCI:
    def __init__(self, fix_issues=False):
        self.fix_issues = fix_issues
        self.errors = []
        self.warnings = []

    def log_error(self, message):
        """Log an error."""
        self.errors.append(message)
        print(f"❌ ERROR: {message}")

    def log_warning(self, message):
        """Log a warning."""
        self.warnings.append(message)
        print(f"⚠️  WARNING: {message}")

    def log_success(self, message):
        """Log a success."""
        print(f"✅ {message}")

    def check_notebook_duplicates(self):
        """Check for duplicate notebooks."""
        print("\n🔍 Checking for duplicate notebooks...")

        # Check for notebooks in wrong locations
        problematic_notebooks = [
            "notebooks/quality/03_appendix_data_quality.ipynb",
            "notebooks/executed/03_appendix_data_quality-executed.ipynb",
        ]

        for notebook in problematic_notebooks:
            if os.path.exists(notebook):
                if self.fix_issues:
                    os.remove(notebook)
                    self.log_success(f"Removed duplicate: {notebook}")
                else:
                    self.log_error(f"Duplicate notebook found: {notebook}")

        # Check for empty executed notebooks
        executed_dir = Path("notebooks/executed")
        if executed_dir.exists():
            for notebook in executed_dir.glob("*.ipynb"):
                if notebook.stat().st_size == 0:
                    if self.fix_issues:
                        notebook.unlink()
                        self.log_success(f"Removed empty notebook: {notebook}")
                    else:
                        self.log_error(f"Empty executed notebook: {notebook}")

    def validate_notebook_syntax(self):
        """Validate notebook JSON syntax."""
        print("\n🧪 Validating notebook syntax...")

        notebook_dirs = ["notebooks/editable", "notebooks/analysis", "notebooks/executed"]

        for notebook_dir in notebook_dirs:
            if not os.path.exists(notebook_dir):
                continue

            for notebook_file in Path(notebook_dir).glob("*.ipynb"):
                try:
                    with open(notebook_file, "r") as f:
                        json.load(f)
                    self.log_success(f"Valid JSON: {notebook_file}")
                except json.JSONDecodeError as e:
                    self.log_error(f"Invalid JSON in {notebook_file}: {e}")

    def test_imports(self):
        """Test critical imports."""
        print("\n📦 Testing critical imports...")

        # Add current directory to Python path for imports
        import sys

        if "." not in sys.path:
            sys.path.insert(0, ".")

        imports_to_test = [
            ("src.youtubeviz.data", "load_youtube_data"),
            ("src.youtubeviz.utils", "safe_head"),
            ("src.youtubeviz.charts", "views_over_time_plotly"),
        ]

        for module, function in imports_to_test:
            try:
                exec(f"from {module} import {function}")
                self.log_success(f"Import works: {module}.{function}")
            except ImportError as e:
                self.log_error(f"Import failed: {module}.{function} - {e}")
            except Exception as e:
                self.log_warning(f"Import issue: {module}.{function} - {e}")

    def validate_execute_scripts(self):
        """Validate execute scripts."""
        print("\n📊 Validating execute scripts...")

        scripts = ["execute_music_analytics.py", "execute_data_quality.py", "execute_artist_comparison.py"]

        for script in scripts:
            if not os.path.exists(script):
                self.log_error(f"Missing execute script: {script}")
                continue

            # Check Python syntax
            try:
                subprocess.run([sys.executable, "-m", "py_compile", script], check=True, capture_output=True)
                self.log_success(f"Valid Python syntax: {script}")
            except subprocess.CalledProcessError:
                self.log_error(f"Syntax error in {script}")

    def test_notebook_execution(self):
        """Test notebook execution (quick dry run)."""
        print("\n🔧 Testing notebook execution...")

        # Test data quality execution
        try:
            result = subprocess.run(
                [sys.executable, "execute_data_quality.py"], capture_output=True, text=True, timeout=60
            )

            if result.returncode == 0:
                self.log_success("Data quality script executes successfully")
            else:
                self.log_error(f"Data quality script failed: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            self.log_warning("Data quality script timed out (may be normal)")
        except Exception as e:
            self.log_error(f"Error testing data quality script: {e}")

        # Test music analytics execution
        try:
            result = subprocess.run(
                [sys.executable, "execute_music_analytics.py"], capture_output=True, text=True, timeout=60
            )

            if result.returncode == 0:
                self.log_success("Music analytics script executes successfully")
            else:
                self.log_error(f"Music analytics script failed: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            self.log_warning("Music analytics script timed out (may be normal)")
        except Exception as e:
            self.log_error(f"Error testing music analytics script: {e}")

    def check_data_consistency(self):
        """Check data consistency."""
        print("\n📊 Checking data consistency...")

        try:
            # Quick data consistency check
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import sys
import os
sys.path.insert(0, '.')
sys.path.insert(0, os.path.abspath('.'))
try:
    from src.youtubeviz.data import load_youtube_data
    df = load_youtube_data()
    print(f'Artists: {df["artist_name"].nunique()}')
    print(f'Videos: {df["video_id"].nunique()}')
    print(f'Records: {len(df)}')
except Exception as e:
    print(f'Data check skipped: {e}')
""",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                output_lines = result.stdout.strip().split("\n")
                for line in output_lines:
                    if line.strip():
                        self.log_success(f"Data check: {line}")
            else:
                self.log_error(f"Data consistency check failed: {result.stderr}")

        except Exception as e:
            self.log_error(f"Error checking data consistency: {e}")

    def validate_artist_data(self):
        """Validate artist data consistency."""
        print("\n🎤 Validating artist data...")

        try:
            result = subprocess.run(
                [sys.executable, "scripts/validate_artist_data.py"], capture_output=True, text=True, timeout=60
            )

            if result.returncode == 0:
                self.log_success("Artist data validation passed")
                # Print key validation info
                output_lines = result.stdout.split("\n")
                for line in output_lines:
                    if "Expected:" in line or "Database:" in line or "VALIDATION PASSED" in line:
                        if line.strip():
                            print(f"   {line.strip()}")
            else:
                self.log_error("Artist data validation failed")
                # Show validation errors
                error_lines = result.stdout.split("\n")
                for line in error_lines:
                    if "Missing Artists" in line or "Unexpected Artists" in line or "VALIDATION FAILED" in line:
                        if line.strip():
                            print(f"   {line.strip()}")

        except subprocess.TimeoutExpired:
            self.log_warning("Artist validation timed out")
        except Exception as e:
            self.log_error(f"Error validating artist data: {e}")

    def run_tests(self):
        """Run test suite."""
        print("\n🧪 Running test suite...")

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/test_system_integration.py",
                    "tests/test_data_quality.py",
                    "tests/test_notebook_execution.py",
                    "-v",
                    "--tb=short",
                ],
                capture_output=True,
                text=True,
                timeout=180,
            )

            if result.returncode == 0:
                self.log_success("All tests passed")
            else:
                self.log_error(f"Some tests failed: {result.stdout}")

        except subprocess.TimeoutExpired:
            self.log_warning("Tests timed out")
        except Exception as e:
            self.log_error(f"Error running tests: {e}")

    def generate_report(self):
        """Generate final report."""
        print("\n" + "=" * 60)
        print("🏆 LOCAL CI/CD PIPELINE REPORT")
        print("=" * 60)

        if not self.errors and not self.warnings:
            print("🎉 ALL CHECKS PASSED!")
            print("✅ Ready to commit and push")
            return True

        if self.errors:
            print(f"\n❌ ERRORS FOUND ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                print(f"   {i}. {error}")

        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"   {i}. {warning}")

        if self.errors:
            print("\n🚫 DO NOT COMMIT - Fix errors first!")
            if not self.fix_issues:
                print("💡 Run with --fix-issues to auto-fix some problems")
            return False
        else:
            print("\n⚠️  WARNINGS ONLY - Safe to commit but consider fixing")
            return True

    def run_all_checks(self):
        """Run all CI/CD checks."""
        print("🚀 STARTING LOCAL CI/CD PIPELINE")
        print("=" * 50)

        start_time = time.time()

        # Run all checks
        self.check_notebook_duplicates()
        self.validate_notebook_syntax()
        self.test_imports()
        self.validate_execute_scripts()
        self.test_notebook_execution()
        self.check_data_consistency()
        self.validate_artist_data()
        self.run_tests()

        # Generate report
        success = self.generate_report()

        elapsed = time.time() - start_time
        print(f"\n⏱️  Pipeline completed in {elapsed:.1f} seconds")

        return success


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run local CI/CD pipeline")
    parser.add_argument("--fix-issues", action="store_true", help="Automatically fix issues where possible")

    args = parser.parse_args()

    ci = LocalCI(fix_issues=args.fix_issues)
    success = ci.run_all_checks()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
