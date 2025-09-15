#!/usr/bin/env python3
"""
🔒 Pre-Commit Hook
=================

Comprehensive pre-commit validation that prevents bad code from being committed.
This runs automatically before each commit to ensure code quality.

Usage:
    # Install as git hook
    ln -sf ../../scripts/pre_commit_hook.py .git/hooks/pre-commit

    # Run manually
    python scripts/pre_commit_hook.py
"""

import os
import subprocess
import sys
from pathlib import Path


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
    """Main pre-commit validation."""

    print("🔒 PRE-COMMIT VALIDATION")
    print("=" * 50)

    # Change to repository root
    repo_root = Path(__file__).parent.parent
    os.chdir(repo_root)

    checks = [
        # Core validation
        ("python scripts/run_local_ci.py", "Local CI/CD Pipeline", 180),
        # Artist data validation
        ("python scripts/validate_artist_data.py", "Artist Data Validation", 60),
        # Quick syntax checks
        ("python -m py_compile execute_music_analytics.py", "Music Analytics Syntax", 10),
        ("python -m py_compile execute_data_quality.py", "Data Quality Syntax", 10),
        # Test suite (quick tests only)
        ("python -m pytest tests/test_system_integration.py -x -q", "Integration Tests", 60),
    ]

    failed_checks = []

    for cmd, description, timeout in checks:
        if not run_command(cmd, description, timeout):
            failed_checks.append(description)

    # Final report
    print("\n" + "=" * 60)
    print("🏆 PRE-COMMIT VALIDATION REPORT")
    print("=" * 60)

    if not failed_checks:
        print("🎉 ALL CHECKS PASSED!")
        print("✅ Ready to commit")
        print("\n💡 Pro tip: Your code meets all quality standards!")
        return 0
    else:
        print("🚫 COMMIT BLOCKED!")
        print(f"❌ {len(failed_checks)} check(s) failed:")
        for check in failed_checks:
            print(f"   - {check}")

        print("\n🔧 How to fix:")
        print("1. Run: python scripts/run_local_ci.py --fix-issues")
        print("2. Fix any remaining issues manually")
        print("3. Run: python scripts/validate_artist_data.py --update-config")
        print("4. Try committing again")

        return 1


if __name__ == "__main__":
    sys.exit(main())
