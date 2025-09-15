#!/usr/bin/env python3
"""
YouTube Terms of Service Compliance Script

This script ensures compliance with YouTube's Terms of Service by automatically
deleting user-generated content (comments) after the specified retention period.
This is required to maintain good standing with YouTube's API and protect user privacy.

Key Compliance Requirements:
- Delete comments after YOUTUBE_COMMENT_RETENTION_DAYS (default: 30 days)
- Keep metrics data longer for trend analysis (YOUTUBE_METRICS_RETENTION_DAYS)
- Log all deletion activities for audit purposes
- Provide clear reporting on compliance status

Usage:
    python tools/maintenance/youtube_tos_compliance.py --dry-run    # Preview deletions
    python tools/maintenance/youtube_tos_compliance.py --execute    # Perform deletions
    python tools/maintenance/youtube_tos_compliance.py --status     # Check compliance status
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import logging

from dotenv import load_dotenv
from sqlalchemy import text

from web.etl_helpers import get_engine

# Load environment variables from .env file
load_dotenv()

# Configure logging with professional formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_tos_compliance_settings() -> Dict[str, int]:
    """
    Load YouTube TOS compliance settings from environment variables.

    Returns:
        Dictionary with retention periods in days for different data types
    """
    return {
        "comment_retention_days": int(os.getenv("YOUTUBE_COMMENT_RETENTION_DAYS", 30)),
        "metrics_retention_days": int(os.getenv("YOUTUBE_METRICS_RETENTION_DAYS", 365)),
        "data_retention_days": int(os.getenv("YOUTUBE_DATA_RETENTION_DAYS", 30)),
        "tos_enabled": os.getenv("YOUTUBE_TOS_COMPLIANCE_ENABLED", "true").lower() == "true",
    }


def check_compliance_status(engine) -> Dict[str, any]:
    """
    Check current compliance status by analyzing data retention periods.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        Dictionary with compliance status and statistics
    """
    settings = get_tos_compliance_settings()

    if not settings["tos_enabled"]:
        return {"status": "DISABLED", "message": "TOS compliance checking is disabled"}

    # Calculate cutoff dates
    comment_cutoff = datetime.now() - timedelta(days=settings["comment_retention_days"])
    metrics_cutoff = datetime.now() - timedelta(days=settings["metrics_retention_days"])

    with engine.connect() as conn:
        # Check old comments that should be deleted
        old_comments_query = text(
            """
            SELECT COUNT(*) as old_comment_count,
                   MIN(created_at) as oldest_comment,
                   MAX(created_at) as newest_comment
            FROM youtube_comments 
            WHERE created_at < :cutoff_date
        """
        )

        result = conn.execute(old_comments_query, {"cutoff_date": comment_cutoff})
        comment_stats = result.fetchone()

        # Check old metrics that should be deleted
        old_metrics_query = text(
            """
            SELECT COUNT(*) as old_metrics_count,
                   MIN(fetched_at) as oldest_metric,
                   MAX(fetched_at) as newest_metric
            FROM youtube_metrics 
            WHERE fetched_at < :cutoff_date
        """
        )

        result = conn.execute(old_metrics_query, {"cutoff_date": metrics_cutoff})
        metrics_stats = result.fetchone()

        # Determine compliance status
        violations = []
        if comment_stats[0] > 0:
            violations.append(f"{comment_stats[0]:,} comments older than {settings['comment_retention_days']} days")

        if metrics_stats[0] > 0:
            violations.append(f"{metrics_stats[0]:,} metrics older than {settings['metrics_retention_days']} days")

        status = "COMPLIANT" if not violations else "VIOLATIONS_FOUND"

        return {
            "status": status,
            "violations": violations,
            "comment_stats": {
                "old_count": comment_stats[0],
                "oldest_date": comment_stats[1],
                "newest_date": comment_stats[2],
                "cutoff_date": comment_cutoff,
            },
            "metrics_stats": {
                "old_count": metrics_stats[0],
                "oldest_date": metrics_stats[1],
                "newest_date": metrics_stats[2],
                "cutoff_date": metrics_cutoff,
            },
            "settings": settings,
        }


def delete_old_comments(engine, dry_run: bool = True) -> Dict[str, int]:
    """
    Delete comments older than the retention period to comply with YouTube TOS.

    Args:
        engine: SQLAlchemy database engine
        dry_run: If True, only count records without deleting

    Returns:
        Dictionary with deletion statistics
    """
    settings = get_tos_compliance_settings()
    cutoff_date = datetime.now() - timedelta(days=settings["comment_retention_days"])

    with engine.connect() as conn:
        # Count comments to be deleted
        count_query = text(
            """
            SELECT COUNT(*) as comment_count
            FROM youtube_comments 
            WHERE created_at < :cutoff_date
        """
        )

        result = conn.execute(count_query, {"cutoff_date": cutoff_date})
        comment_count = result.fetchone()[0]

        if comment_count == 0:
            print(f"✅ No comments older than {settings['comment_retention_days']} days found")
            return {"deleted_comments": 0}

        print(f"{'🔍 [PREVIEW] ' if dry_run else '🗑️  [DELETING] '}")
        print(f"Comments older than {settings['comment_retention_days']} days: {comment_count:,}")
        print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")

        if not dry_run:
            # Perform actual deletion
            delete_query = text(
                """
                DELETE FROM youtube_comments 
                WHERE created_at < :cutoff_date
            """
            )

            conn.execute(delete_query, {"cutoff_date": cutoff_date})
            conn.commit()

            # Log the deletion for audit purposes
            logger.info(f"TOS Compliance: Deleted {comment_count:,} comments older than {cutoff_date}")
            print(f"✅ Successfully deleted {comment_count:,} old comments")

        return {"deleted_comments": comment_count}


def delete_old_metrics(engine, dry_run: bool = True) -> Dict[str, int]:
    """
    Delete metrics older than the retention period.

    Args:
        engine: SQLAlchemy database engine
        dry_run: If True, only count records without deleting

    Returns:
        Dictionary with deletion statistics
    """
    settings = get_tos_compliance_settings()
    cutoff_date = datetime.now() - timedelta(days=settings["metrics_retention_days"])

    with engine.connect() as conn:
        # Count metrics to be deleted
        count_query = text(
            """
            SELECT COUNT(*) as metrics_count
            FROM youtube_metrics 
            WHERE fetched_at < :cutoff_date
        """
        )

        result = conn.execute(count_query, {"cutoff_date": cutoff_date})
        metrics_count = result.fetchone()[0]

        if metrics_count == 0:
            print(f"✅ No metrics older than {settings['metrics_retention_days']} days found")
            return {"deleted_metrics": 0}

        print(f"{'🔍 [PREVIEW] ' if dry_run else '🗑️  [DELETING] '}")
        print(f"Metrics older than {settings['metrics_retention_days']} days: {metrics_count:,}")
        print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")

        if not dry_run:
            # Perform actual deletion
            delete_query = text(
                """
                DELETE FROM youtube_metrics 
                WHERE fetched_at < :cutoff_date
            """
            )

            conn.execute(delete_query, {"cutoff_date": cutoff_date})
            conn.commit()

            # Log the deletion for audit purposes
            logger.info(f"TOS Compliance: Deleted {metrics_count:,} metrics older than {cutoff_date}")
            print(f"✅ Successfully deleted {metrics_count:,} old metrics")

        return {"deleted_metrics": metrics_count}


def show_compliance_status(status: Dict[str, any]):
    """
    Display comprehensive compliance status report.

    Args:
        status: Compliance status dictionary from check_compliance_status()
    """
    print("\n" + "=" * 70)
    print("📋 YOUTUBE TERMS OF SERVICE COMPLIANCE REPORT")
    print("=" * 70)

    if status["status"] == "DISABLED":
        print("⚠️  TOS compliance checking is DISABLED")
        print("   Set YOUTUBE_TOS_COMPLIANCE_ENABLED=true in .env to enable")
        return

    settings = status["settings"]
    print(f"\n⚙️  Retention Settings:")
    print(f"   • Comments: {settings['comment_retention_days']} days")
    print(f"   • Metrics: {settings['metrics_retention_days']} days")
    print(f"   • General Data: {settings['data_retention_days']} days")

    if status["status"] == "COMPLIANT":
        print(f"\n✅ COMPLIANT - All data within retention periods")
    else:
        print(f"\n❌ VIOLATIONS FOUND:")
        for violation in status["violations"]:
            print(f"   • {violation}")

    # Comment statistics
    comment_stats = status["comment_stats"]
    print(f"\n💬 Comment Data:")
    print(f"   • Old comments to delete: {comment_stats['old_count']:,}")
    if comment_stats["oldest_date"]:
        print(f"   • Oldest comment: {comment_stats['oldest_date']}")
    print(f"   • Retention cutoff: {comment_stats['cutoff_date'].strftime('%Y-%m-%d %H:%M:%S')}")

    # Metrics statistics
    metrics_stats = status["metrics_stats"]
    print(f"\n📊 Metrics Data:")
    print(f"   • Old metrics to delete: {metrics_stats['old_count']:,}")
    if metrics_stats["oldest_date"]:
        print(f"   • Oldest metric: {metrics_stats['oldest_date']}")
    print(f"   • Retention cutoff: {metrics_stats['cutoff_date'].strftime('%Y-%m-%d %H:%M:%S')}")

    if status["violations"]:
        print(f"\n💡 Recommended Actions:")
        print(f"   1. Run: python tools/maintenance/youtube_tos_compliance.py --dry-run")
        print(f"   2. Review what will be deleted")
        print(f"   3. Run: python tools/maintenance/youtube_tos_compliance.py --execute")
        print(f"   4. Set up automated cron job for daily compliance checks")


def main():
    """
    Main TOS compliance orchestration function.

    Handles command line arguments and coordinates compliance operations.
    """
    dry_run = "--dry-run" in sys.argv
    execute = "--execute" in sys.argv
    status_only = "--status" in sys.argv

    print("📋 YouTube Terms of Service Compliance Tool")
    print("=" * 50)

    try:
        # Connect to database using unified .env configuration
        engine = get_engine()
        logger.info("✅ Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error("Please check your database configuration in .env")
        sys.exit(1)

    # Check current compliance status
    try:
        compliance_status = check_compliance_status(engine)
    except Exception as e:
        logger.error(f"Failed to check compliance status: {e}")
        sys.exit(1)

    # Handle status-only mode
    if status_only:
        show_compliance_status(compliance_status)
        sys.exit(0)

    # Show current status
    show_compliance_status(compliance_status)

    # Handle execution modes
    if compliance_status["status"] == "DISABLED":
        print("\n💡 Enable TOS compliance in .env to use this tool")
        sys.exit(0)

    if compliance_status["status"] == "COMPLIANT":
        print(f"\n✅ System is already compliant - no action needed")
        sys.exit(0)

    # Perform cleanup operations
    if dry_run or execute:
        print(f"\n🚀 Starting TOS compliance cleanup...")

        try:
            # Delete old comments
            comment_stats = delete_old_comments(engine, dry_run=dry_run)

            # Delete old metrics
            metrics_stats = delete_old_metrics(engine, dry_run=dry_run)

            # Summary
            total_deleted = comment_stats.get("deleted_comments", 0) + metrics_stats.get("deleted_metrics", 0)

            print(f"\n📊 Cleanup Summary:")
            print(f"   • Comments: {comment_stats.get('deleted_comments', 0):,}")
            print(f"   • Metrics: {metrics_stats.get('deleted_metrics', 0):,}")
            print(f"   • Total: {total_deleted:,}")

            if dry_run:
                print(f"\n🔍 Preview complete - no data was deleted")
                print(f"💡 Run with --execute to perform actual cleanup")
            else:
                print(f"\n✅ TOS compliance cleanup completed successfully!")
                print(f"💡 Set up a daily cron job to maintain ongoing compliance")

        except Exception as e:
            logger.error(f"TOS compliance cleanup failed: {e}")
            sys.exit(1)
    else:
        print(f"\n💡 Use --dry-run to preview or --execute to perform cleanup")


if __name__ == "__main__":
    main()
