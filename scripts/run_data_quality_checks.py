#!/usr/bin/env python3
"""
Data Quality CI/CD Integration Script

This script runs comprehensive data quality checks and can be integrated into CI/CD pipelines.
It validates data integrity, checks for duplicates, and ensures notebooks will work correctly.

Usage:
    python scripts/run_data_quality_checks.py [--fail-on-duplicates] [--output-format json|text]
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.youtubeviz.data import load_recent_window_days
from tests.test_data_quality import TestDataQuality
from web.etl_helpers import get_engine


def run_quality_checks(fail_on_duplicates=False, output_format="text"):
    """Run comprehensive data quality checks."""

    results = {"timestamp": datetime.now().isoformat(), "status": "PASSED", "checks": {}, "summary": {}, "errors": []}

    try:
        # Initialize test class
        test_class = TestDataQuality()

        # Get database connection using unified .env configuration
        engine = get_engine()
        sample_data = load_recent_window_days(days=90, engine=engine)

        if output_format == "text":
            print("🧪 RUNNING DATA QUALITY CI/CD CHECKS")
            print("=" * 50)
            print(f"📊 Loaded {len(sample_data):,} records for testing")

        # Run individual checks
        checks_to_run = [
            ("enchanting_check", "test_no_enchanting_data", sample_data),
            ("video_duplicates", "test_duplicate_videos_detection", sample_data),
            ("song_versions", "test_duplicate_songs_detection", sample_data),
            ("comment_duplicates", "test_comment_duplicates", engine),
            ("data_freshness", "test_data_freshness", sample_data),
            ("required_columns", "test_required_columns", sample_data),
            ("data_types", "test_data_types", sample_data),
            ("artist_consistency", "test_artist_consistency", sample_data),
        ]

        for check_name, method_name, test_data in checks_to_run:
            try:
                method = getattr(test_class, method_name)

                if output_format == "text":
                    print(f"\n🔍 Running {check_name}...")

                if check_name == "comment_duplicates":
                    result = method(test_data)  # engine
                else:
                    result = method(test_data)  # sample_data

                results["checks"][check_name] = {
                    "status": "PASSED",
                    "result": result,
                    "message": f"Check completed successfully",
                }

                if output_format == "text":
                    print(f"✅ {check_name}: PASSED")

                # Handle specific failure conditions
                if check_name == "enchanting_check" and result > 0:
                    results["checks"][check_name]["status"] = "FAILED"
                    results["status"] = "FAILED"
                    error_msg = f"Found {result} Enchanting records - should be 0"
                    results["errors"].append(error_msg)
                    if output_format == "text":
                        print(f"❌ {check_name}: FAILED - {error_msg}")

                elif check_name in ["video_duplicates", "song_versions"] and result > 0:
                    if fail_on_duplicates:
                        results["checks"][check_name]["status"] = "FAILED"
                        results["status"] = "FAILED"
                        error_msg = f"Found {result} duplicates"
                        results["errors"].append(error_msg)
                        if output_format == "text":
                            print(f"❌ {check_name}: FAILED - {error_msg}")
                    else:
                        results["checks"][check_name]["status"] = "WARNING"
                        if output_format == "text":
                            print(f"⚠️  {check_name}: WARNING - {result} duplicates found")

            except Exception as e:
                results["checks"][check_name] = {"status": "ERROR", "result": None, "message": str(e)}
                results["errors"].append(f"{check_name}: {str(e)}")
                if output_format == "text":
                    print(f"❌ {check_name}: ERROR - {e}")

        # Generate summary
        passed = sum(1 for check in results["checks"].values() if check["status"] == "PASSED")
        warnings = sum(1 for check in results["checks"].values() if check["status"] == "WARNING")
        failed = sum(1 for check in results["checks"].values() if check["status"] == "FAILED")
        errors = sum(1 for check in results["checks"].values() if check["status"] == "ERROR")

        results["summary"] = {
            "total_checks": len(results["checks"]),
            "passed": passed,
            "warnings": warnings,
            "failed": failed,
            "errors": errors,
        }

        if output_format == "text":
            print(f"\n🎯 DATA QUALITY SUMMARY:")
            print(f"   ✅ Passed: {passed}")
            print(f"   ⚠️  Warnings: {warnings}")
            print(f"   ❌ Failed: {failed}")
            print(f"   🚫 Errors: {errors}")

            if results["status"] == "PASSED":
                print(f"\n🎉 ALL DATA QUALITY CHECKS PASSED!")
            else:
                print(f"\n💥 DATA QUALITY ISSUES DETECTED!")
                for error in results["errors"]:
                    print(f"   • {error}")

    except Exception as e:
        results["status"] = "ERROR"
        results["errors"].append(f"Critical error: {str(e)}")
        if output_format == "text":
            print(f"💥 CRITICAL ERROR: {e}")

    # Output results
    if output_format == "json":
        print(json.dumps(results, indent=2))

    # Return appropriate exit code
    if results["status"] in ["FAILED", "ERROR"]:
        return 1
    else:
        return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Run data quality checks for CI/CD")
    parser.add_argument("--fail-on-duplicates", action="store_true", help="Fail the build if duplicates are found")
    parser.add_argument("--output-format", choices=["text", "json"], default="text", help="Output format for results")

    args = parser.parse_args()

    exit_code = run_quality_checks(fail_on_duplicates=args.fail_on_duplicates, output_format=args.output_format)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
