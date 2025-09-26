#!/usr/bin/env python3
"""
Test Enhanced Data Quality Manager

Validates the professional data quality system with real database data.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.youtubeviz.enhanced_data_quality import EnhancedDataQualityManager, run_enhanced_data_quality_check
from web.etl_helpers import get_engine


def test_data_quality_detection():
    """Test data quality issue detection with real data."""
    print("🧪 TESTING DATA QUALITY DETECTION")
    print("=" * 50)

    try:
        engine = get_engine()
        manager = EnhancedDataQualityManager(engine)

        # Test issue detection
        issues = manager.detect_all_data_issues()

        print(f"\n📊 Detection Results:")
        print(f"   Issues Found: {len(issues)}")

        for issue in issues:
            severity_emoji = "🔴" if issue.severity == "critical" else "🟡"
            print(f"   {severity_emoji} {issue.description}: {issue.count:,} records")

        return len(issues)

    except Exception as e:
        print(f"❌ Detection test failed: {e}")
        return -1


def test_full_quality_analysis():
    """Test complete data quality analysis and cleanup."""
    print("\n🧪 TESTING FULL QUALITY ANALYSIS")
    print("=" * 50)

    try:
        engine = get_engine()

        # Run complete analysis
        report = run_enhanced_data_quality_check(engine)

        print(f"\n📊 Analysis Results:")
        print(f"   Quality Score: {report.quality_score:.1f}/100")
        print(f"   Issues Detected: {len(report.issues_detected)}")
        print(f"   Cleanup Operations: {len(report.cleanup_operations)}")
        print(f"   Records Cleaned: {report.total_records_cleaned:,}")

        if report.bot_analysis_summary.get("total_analyzed", 0) > 0:
            bot_stats = report.bot_analysis_summary
            print(f"   Bot Analysis: {bot_stats['total_analyzed']:,} comments analyzed")
            print(f"   High-Risk Bots: {bot_stats['high_risk']:,}")

        print(f"\n💡 Recommendations:")
        for rec in report.recommendations:
            print(f"   {rec}")

        return report.quality_score

    except Exception as e:
        print(f"❌ Full analysis test failed: {e}")
        return -1


def test_bot_analysis():
    """Test bot detection and educational display."""
    print("\n🧪 TESTING BOT ANALYSIS")
    print("=" * 50)

    try:
        engine = get_engine()
        manager = EnhancedDataQualityManager(engine)

        # Test bot analysis
        bot_summary = manager.analyze_and_display_bot_patterns()

        print(f"\n📊 Bot Analysis Results:")
        print(f"   Total Analyzed: {bot_summary.get('total_analyzed', 0):,}")
        print(f"   High Risk: {bot_summary.get('high_risk', 0):,}")
        print(f"   Medium Risk: {bot_summary.get('medium_risk', 0):,}")
        print(f"   Low Risk: {bot_summary.get('low_risk', 0):,}")

        return bot_summary.get("total_analyzed", 0)

    except Exception as e:
        print(f"❌ Bot analysis test failed: {e}")
        return -1


def main():
    """Run all enhanced data quality tests."""
    print("🚀 ENHANCED DATA QUALITY MANAGER TESTS")
    print("=" * 60)

    # Test 1: Data quality detection
    issues_found = test_data_quality_detection()

    # Test 2: Full quality analysis
    quality_score = test_full_quality_analysis()

    # Test 3: Bot analysis
    comments_analyzed = test_bot_analysis()

    # Summary
    print("\n" + "=" * 60)
    print("🏆 TEST SUMMARY")
    print("=" * 60)

    if issues_found >= 0:
        print(f"✅ Data Quality Detection: {issues_found} issues found")
    else:
        print("❌ Data Quality Detection: Failed")

    if quality_score >= 0:
        print(f"✅ Full Quality Analysis: {quality_score:.1f}/100 score")
    else:
        print("❌ Full Quality Analysis: Failed")

    if comments_analyzed >= 0:
        print(f"✅ Bot Analysis: {comments_analyzed:,} comments analyzed")
    else:
        print("❌ Bot Analysis: Failed")

    # Overall result
    all_passed = all(result >= 0 for result in [issues_found, quality_score, comments_analyzed])

    if all_passed:
        print("\n🎉 All tests passed! Enhanced data quality system is working correctly.")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
