#!/usr/bin/env python3
"""
📊 YouTube Analytics Monitoring Tool

Comprehensive monitoring for data quality, pipeline health, and system status.
Consolidates all monitoring functionality into one robust tool.

Usage:
    python tools/monitor.py                 # Quick health check
    python tools/monitor.py --consistency   # Artist consistency validation
    python tools/monitor.py --quality       # Full data quality report
    python tools/monitor.py --etl-status    # ETL pipeline status
    python tools/monitor.py --full-check    # Complete system check
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    """Main monitoring tool with comprehensive options."""
    parser = argparse.ArgumentParser(
        description="YouTube Analytics Monitoring Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/monitor.py                  # Quick health check
  python tools/monitor.py --consistency    # Artist consistency check
  python tools/monitor.py --quality        # Data quality report
  python tools/monitor.py --etl-status     # ETL pipeline status
  python tools/monitor.py --full-check     # Complete system check
        """,
    )

    # Monitoring options
    parser.add_argument("--consistency", action="store_true", help="Run artist consistency validation")
    parser.add_argument("--quality", action="store_true", help="Full data quality report")
    parser.add_argument("--etl-status", action="store_true", help="ETL pipeline status and history")
    parser.add_argument("--full-check", action="store_true", help="Complete system health check")

    # Options
    parser.add_argument("--days", type=int, default=30, help="Number of days to analyze (default: 30)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")

    args = parser.parse_args()

    try:
        print("📊 YouTube Analytics System Monitor")
        print("=" * 50)

        # Import monitoring modules
        from src.youtubeviz.data import (
            qa_artist_consistency_check,
            qa_nulls_and_orphans,
        )
        from web.etl_helpers import get_engine, get_etl_run_summary

        engine = get_engine()

        if args.consistency:
            return check_consistency(engine, args.days, args.verbose)
        elif args.quality:
            return check_data_quality(engine, args.verbose)
        elif args.etl_status:
            return check_etl_status(engine, args.verbose)
        elif args.full_check:
            return full_system_check(engine, args.days, args.verbose)
        else:
            # Default: quick health check
            return quick_health_check(engine, args.days)

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure you're running from the project root directory")
        return 1
    except Exception as e:
        print(f"❌ Monitoring error: {e}")
        return 1


def quick_health_check(engine, days):
    """Quick system health check."""
    print("⚡ Quick Health Check")
    print("-" * 25)

    try:
        # Database connectivity
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        print("✅ Database: Connected")

        # Basic data counts
        from sqlalchemy import text

        with engine.connect() as conn:
            tables = {"Videos": "youtube_videos", "Comments": "youtube_comments", "Metrics": "youtube_metrics"}

            for name, table in tables.items():
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    count = result.fetchone()[0]
                    print(f"✅ {name}: {count:,} records")
                except Exception as e:
                    print(f"❌ {name}: Error - {e}")

        # Quick consistency check
        from src.youtubeviz.data import qa_artist_consistency_check

        consistency = qa_artist_consistency_check(days=days, engine=engine)

        if consistency["status"] == "success" and consistency["consistent"]:
            print(f"✅ Consistency: {consistency['data_artists']} artists")
        else:
            print(f"⚠️  Consistency: {consistency.get('message', 'Issues detected')}")

        print(f"\n🎉 System Status: HEALTHY")
        print(f"💡 For detailed analysis, run: python tools/monitor.py --full-check")
        return 0

    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return 1


def check_consistency(engine, days, verbose):
    """Check artist consistency across all functions."""
    print(f"🔄 Artist Consistency Check ({days} days)")
    print("-" * 40)

    from src.youtubeviz.data import qa_artist_consistency_check

    result = qa_artist_consistency_check(days=days, engine=engine)

    if result["status"] == "success":
        print(f"📊 Data artists: {result['data_artists']}")
        print(f"📈 KPI artists: {result['kpi_artists']}")
        print(f"💭 Sentiment artists: {result['sentiment_artists']}")
        print(f"🎨 Color mapping artists: {result['color_artists']}")
        print(f"💰 Revenue artists: {result['revenue_artists']}")

        print(f"\n📝 {result['explanation']}")

        if result["consistent"]:
            print("\n✅ CONSISTENCY CHECK PASSED")
            return 0
        else:
            print(f"\n❌ CONSISTENCY CHECK FAILED")
            print(f"   {result['message']}")
            return 1
    else:
        print(f"❌ Consistency check error: {result['message']}")
        return 1


def check_data_quality(engine, verbose):
    """Full data quality report."""
    print("🔍 Data Quality Report")
    print("-" * 25)

    from src.youtubeviz.data import qa_nulls_and_orphans

    qa_results = qa_nulls_and_orphans(engine=engine)

    total_issues = 0
    for check_name, result_df in qa_results.items():
        issue_count = len(result_df)
        total_issues += issue_count

        status = "✅" if issue_count == 0 else "⚠️ "
        print(f"{status} {check_name}: {issue_count} issues")

        if verbose and issue_count > 0:
            print(f"   Sample issues: {result_df.head(3).to_dict('records')}")

    if total_issues == 0:
        print(f"\n🎉 Data Quality: EXCELLENT (no issues found)")
        return 0
    else:
        print(f"\n⚠️  Data Quality: {total_issues} total issues found")
        print(f"💡 Review issues and consider running maintenance")
        return 1


def check_etl_status(engine, verbose):
    """Check ETL pipeline status and history."""
    print("⚙️  ETL Pipeline Status")
    print("-" * 25)

    try:
        from web.etl_helpers import get_etl_run_summary

        summary = get_etl_run_summary(engine)

        if len(summary) == 0:
            print("⚠️  No ETL runs found")
            print("💡 Run 'python tools/etl.py' to start processing data")
            return 1

        print(f"📊 Total ETL runs: {len(summary)}")

        if len(summary) > 0:
            latest = summary.iloc[0]
            print(f"📅 Latest run: {latest['run_date']}")
            print(f"✅ Success rate: {(summary['success'].sum() / len(summary) * 100):.1f}%")

            if verbose:
                print(f"\n📋 Recent runs:")
                for _, run in summary.head(5).iterrows():
                    status = "✅" if run["success"] else "❌"
                    print(f"   {status} {run['run_date']} - {run.get('message', 'No message')}")

        return 0

    except Exception as e:
        print(f"❌ ETL status check failed: {e}")
        return 1


def full_system_check(engine, days, verbose):
    """Complete system health check."""
    print("🔬 Complete System Health Check")
    print("=" * 35)

    checks = [
        ("Database Health", lambda: quick_health_check(engine, days)),
        ("Artist Consistency", lambda: check_consistency(engine, days, False)),
        ("Data Quality", lambda: check_data_quality(engine, False)),
        ("ETL Status", lambda: check_etl_status(engine, False)),
    ]

    results = []
    for check_name, check_func in checks:
        print(f"\n🔍 {check_name}:")
        try:
            result = check_func()
            results.append((check_name, result == 0))
            if result == 0:
                print(f"   ✅ PASSED")
            else:
                print(f"   ❌ FAILED")
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            results.append((check_name, False))

    # Summary
    passed = sum(1 for _, success in results if success)
    total = len(results)

    print(f"\n📊 SYSTEM HEALTH SUMMARY")
    print(f"=" * 30)
    print(f"✅ Passed: {passed}/{total} checks")

    if passed == total:
        print(f"🎉 System Status: EXCELLENT")
        return 0
    else:
        print(f"⚠️  System Status: NEEDS ATTENTION")
        print(f"💡 Address failed checks before production use")
        return 1


if __name__ == "__main__":
    sys.exit(main())
