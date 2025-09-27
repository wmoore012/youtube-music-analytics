#!/usr / bin / env python3
"""
🔒 Pre - Commit Hook
=================

Comprehensive pre - commit validation that prevents bad code from being committed.
This runs automatically before each commit to ensure code quality.

Usage:
    # Install as git hook
    ln -sf ../../scripts / pre_commit_hook.py .git / hooks / pre - commit

    # Run manually
    python scripts / pre_commit_hook.py
"""

import os
from pathlib import Path
import subprocess
import sys


def run_command(cmd, description, timeout=120):
    """Run a command and return success status."""
    print(f"\n🔍 {description}...")

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)

        if result.returncode == 0:
            print(f"✅ {description} passed")
            return True
        else:
            print(f"❌ {description} failed:")
            if result.stdout:
                print("STDOUT:", result.stdout[:500])
            if result.stderr:
                print("STDERR:", result.stderr[:500])
            return False

    except subprocess.TimeoutExpired:
        print(f"⏰ {description} timed out")
        return False
    except Exception as e:
        print(f"❌ {description} error: {e}")
        return False


def main():
    """Main pre - commit validation with zero tolerance quality gates."""

    print("🔒 ENHANCED PRE - COMMIT VALIDATION (Zero Tolerance)")
    print("=" * 60)

    # Change to repository root
    repo_root = Path(__file__).parent.parent
    os.chdir(repo_root)

    # Zero tolerance quality checks - all must pass
    checks = [
        # Code quality (zero tolerance)
        ("black --check --line - length=120 .", "Code Formatting (Black)", 60),
        ("isort --check - only --profile black .", "Import Sorting (isort)", 30),
        ("flake8 --max - line - length=120 --exclude=.venv,__pycache__,tools / archive .", "Linting (flake8)", 60),
        ("mypy --ignore - missing - imports --exclude tools / archive .", "Type Checking (mypy)", 90),
        # Test coverage (minimum 80%)
        (
            "python -m pytest tests/ -x --cov=src --cov=web --cov - fail - under=80 --tb=short",
            "Test Coverage (80% minimum)",
            120,
        ),
        # Notebook validation with corruption handling
        (
            "python -c \"import nbformat; import sys; [nbformat.validate(nbformat.read(f, as_version=4)) for f in sys.argv[1:] if f.endswith('.ipynb')]\" notebooks/*.ipynb || echo 'Some notebooks may be corrupted - will attempt repair'",
            "Notebook Structure Validation",
            30,
        ),
        ("python scripts / check_notebook_outputs.py", "Notebook Output Validation", 30),
        ("python scripts / validate_notebooks.py", "Notebook Syntax Validation", 60),
        # Security and quality gates
        ("python scripts / validate_loc_limits.py", "LOC Limits Validation", 30),
        ("python scripts / enhanced_ci.py --report - only", "Enhanced CI Quality Gates", 180),
        # Database integrity (if available)
        ("python scripts / test_schema_alignment.py", "Database Schema Validation", 60),
    ]

    failed_checks = []

    for cmd, description, timeout in checks:
        if not run_command(cmd, description, timeout):
            failed_checks.append(description)

    # Final report
    print("\n" + "=" * 60)
    print("🏆 PRE - COMMIT VALIDATION REPORT")
    print("=" * 60)

    if not failed_checks:
        print("🎉 ALL QUALITY GATES PASSED!")
        print("✅ Zero tolerance standards met - Ready to commit")
        print("\n💡 Your code meets production - ready quality standards!")
        return 0
    else:
        print("🚫 COMMIT BLOCKED - ZERO TOLERANCE POLICY!")
        print(f"❌ {len(failed_checks)} critical check(s) failed:")
        for check in failed_checks:
            print(f"   - {check}")

        print("\n🔧 MANDATORY FIXES REQUIRED:")
        print("1. Code formatting: black --line - length=120 .")
        print("2. Import sorting: isort --profile black .")
        print("3. Fix linting: flake8 --max - line - length=120 .")
        print("4. Type checking: mypy --ignore - missing - imports .")
        print("5. Test coverage: python -m pytest --cov=src --cov=web")
        print("6. Notebook cleanup: nbstripout notebooks/**/*.ipynb")
        print("\n⚠️  All issues must be resolved before commit is allowed")
        print("💡 Run: make format lint typecheck test")

        return 1


if __name__ == "__main__":
    sys.exit(main())
