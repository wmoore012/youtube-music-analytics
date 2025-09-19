#!/usr/bin/env python3
"""
Naming Convention Verification Script

This script verifies that the codebase follows proper naming conventions:
- snake_case for variables and functions
- PascalCase for classes
- lowercase_snake_case for database columns
- UPPER_CASE for constants

This demonstrates completion of Task 2.1: Fix Naming Conventions
"""

from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.code_quality.naming_convention_auditor import NamingConventionAuditor


def main():
    """Verify naming conventions across the codebase."""
    print("🔍 NAMING CONVENTION VERIFICATION")
    print("=" * 50)

    # Run the auditor
    auditor = NamingConventionAuditor()
    report = auditor.scan_codebase()

    # Check results
    if report.total_violations == 0:
        print("✅ All naming conventions are properly followed!")
        print()
        print("VERIFIED CONVENTIONS:")
        print("  ✅ snake_case for variables and functions")
        print("  ✅ PascalCase for classes")
        print("  ✅ lowercase_snake_case for database columns")
        print("  ✅ UPPER_CASE for constants")
        print()
        print(f"📊 Scanned {report.files_scanned} Python files")
        print("🎉 Task 2.1: Fix Naming Conventions - COMPLETED")
        return 0
    else:
        print(f"❌ Found {report.total_violations} naming violations")
        auditor.print_report()
        return 1


if __name__ == "__main__":
    sys.exit(main())
