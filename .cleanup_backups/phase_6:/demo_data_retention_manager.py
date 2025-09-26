#!/usr/bin/env python3
"""
Data Retention Manager Demonstration

This script demonstrates the safe data retention functionality including:
- Checking which videos are eligible for deletion
- Identifying dependencies that prevent deletion
- Performing safe deletion operations
- Cleaning up orphaned data

Usage:
    python demo_data_retention_manager.py
"""

from datetime import datetime, timedelta, timezone
import os
import sys

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text

from web.data_retention_manager import DataRetentionManager
from web.etl_helpers import get_engine


def demo_deletion_safety_check():
    """Demonstrate the deletion safety check functionality."""
    print("🔍 DELETION SAFETY CHECK DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        manager = DataRetentionManager(engine, retention_days=30)

        print(f"📅 Retention policy: {manager.retention_days} days")
        print(f"📅 Cutoff date: {manager.cutoff_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()

        # Check deletion safety
        print("🔍 Checking which videos are eligible for deletion...")
        report = manager.check_deletion_safety()

        print(f"📊 SAFETY CHECK RESULTS:")
        print(f"   Total videos checked: {report.total_videos_checked}")
        print(f"   Videos eligible for deletion: {len(report.eligible_videos)}")
        print(f"   Videos safe to delete: {len(report.videos_safe_to_delete)}")
        print(f"   Videos with dependencies: {len(report.videos_with_dependencies)}")
        print()

        # Show sample safe videos
        if report.videos_safe_to_delete:
            print("✅ VIDEOS SAFE TO DELETE (sample):")
            for video in report.videos_safe_to_delete[:5]:
                print(f"   - {video.video_id}: {video.title[:50]}...")
                print(f"     Age: {video.age_days} days, Published: {video.published_at.strftime('%Y-%m-%d')}")
            if len(report.videos_safe_to_delete) > 5:
                print(f"   ... and {len(report.videos_safe_to_delete) - 5} more videos")
            print()

        # Show sample videos with dependencies
        if report.videos_with_dependencies:
            print("⚠️  VIDEOS WITH DEPENDENCIES (cannot delete):")
            for video in report.videos_with_dependencies[:3]:
                print(f"   - {video.video_id}: {video.title[:50]}...")
                print(f"     Age: {video.age_days} days")
                for dep in video.dependencies:
                    print(f"     └─ {dep.table_name}: {dep.count} records")
                    if dep.sample_ids:
                        print(f"        Sample IDs: {', '.join(dep.sample_ids[:3])}")
            if len(report.videos_with_dependencies) > 3:
                print(f"   ... and {len(report.videos_with_dependencies) - 3} more videos")
            print()

        return report

    except Exception as e:
        print(f"❌ Error during safety check: {e}")
        return None


def demo_dry_run_deletion():
    """Demonstrate dry run deletion (safe testing)."""
    print("🧪 DRY RUN DELETION DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        manager = DataRetentionManager(engine, retention_days=30)

        print("🧪 Performing dry run deletion (no actual changes)...")
        result = manager.delete_expired_videos(dry_run=True)

        print(f"📊 DRY RUN RESULTS:")
        print(f"   Videos that would be deleted: {result.videos_deleted}")
        print(f"   Deletion timestamp: {result.deletion_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        if result.errors:
            print(f"   Errors encountered: {len(result.errors)}")
            for error in result.errors:
                print(f"     - {error}")
        else:
            print("   ✅ No errors encountered")

        print()
        return result

    except Exception as e:
        print(f"❌ Error during dry run: {e}")
        return None


def demo_orphaned_data_cleanup():
    """Demonstrate orphaned data cleanup functionality."""
    print("🧹 ORPHANED DATA CLEANUP DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        manager = DataRetentionManager(engine, retention_days=30)

        print("🧹 Checking for orphaned data (dry run)...")
        result = manager.cleanup_orphaned_data(dry_run=True)

        print(f"📊 ORPHANED DATA CLEANUP RESULTS:")
        print(f"   Orphaned metrics records: {result.related_data_deleted.get('youtube_metrics', 0)}")
        print(f"   Orphaned comment records: {result.related_data_deleted.get('youtube_comments', 0)}")
        print(f"   Orphaned ISRC link records: {result.related_data_deleted.get('video_recording_link', 0)}")

        total_orphaned = sum(result.related_data_deleted.values())
        print(f"   Total orphaned records: {total_orphaned}")

        if result.errors:
            print(f"   Errors encountered: {len(result.errors)}")
            for error in result.errors:
                print(f"     - {error}")
        else:
            print("   ✅ No errors encountered")

        print()
        return result

    except Exception as e:
        print(f"❌ Error during orphaned data cleanup: {e}")
        return None


def demo_specific_video_check():
    """Demonstrate checking specific videos for deletion safety."""
    print("🎯 SPECIFIC VIDEO CHECK DEMONSTRATION")
    print("=" * 60)

    try:
        engine = get_engine()
        manager = DataRetentionManager(engine, retention_days=30)

        # Get some sample video IDs from the database
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT video_id, title, published_at
                FROM youtube_videos
                ORDER BY published_at DESC
                LIMIT 3
            """
                )
            )
            sample_videos = result.fetchall()

        if not sample_videos:
            print("   No videos found in database for demonstration")
            return None

        video_ids = [row[0] for row in sample_videos]
        print(f"🎯 Checking specific videos: {', '.join(video_ids)}")

        report = manager.check_deletion_safety(video_ids)

        print(f"📊 SPECIFIC VIDEO CHECK RESULTS:")
        print(f"   Videos checked: {report.total_videos_checked}")
        print(f"   Videos eligible for deletion: {len(report.eligible_videos)}")
        print(f"   Videos safe to delete: {len(report.videos_safe_to_delete)}")

        for video in report.eligible_videos:
            print(f"   - {video.video_id}: {video.title[:40]}...")
            print(f"     Age: {video.age_days} days, Dependencies: {len(video.dependencies)}")
            if video.dependencies:
                for dep in video.dependencies:
                    print(f"       └─ {dep.table_name}: {dep.count} records")

        print()
        return report

    except Exception as e:
        print(f"❌ Error during specific video check: {e}")
        return None


def demo_retention_policy_info():
    """Show information about the current retention policy."""
    print("📋 RETENTION POLICY INFORMATION")
    print("=" * 60)

    try:
        engine = get_engine()
        manager = DataRetentionManager(engine)

        print(f"📅 Current retention policy:")
        print(f"   Retention period: {manager.retention_days} days")
        print(f"   Cutoff date: {manager.cutoff_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(
            f"   Environment variable: YOUTUBE_DATA_RETENTION_DAYS = {os.getenv('YOUTUBE_DATA_RETENTION_DAYS', 'not set')}"
        )
        print()

        # Get some database statistics
        with engine.connect() as conn:
            # Total videos
            total_videos = conn.execute(text("SELECT COUNT(*) FROM youtube_videos")).scalar()

            # Videos older than retention period
            old_videos = conn.execute(
                text(
                    """
                SELECT COUNT(*) FROM youtube_videos
                WHERE published_at < :cutoff_date
            """
                ),
                {"cutoff_date": manager.cutoff_date},
            ).scalar()

            # Recent videos
            recent_videos = total_videos - old_videos

            print(f"📊 Database statistics:")
            print(f"   Total videos: {total_videos:,}")
            print(f"   Videos older than {manager.retention_days} days: {old_videos:,}")
            print(f"   Recent videos (within retention): {recent_videos:,}")

            if total_videos > 0:
                old_percentage = (old_videos / total_videos) * 100
                print(f"   Percentage of old videos: {old_percentage:.1f}%")

        print()

    except Exception as e:
        print(f"❌ Error getting retention policy info: {e}")


def main():
    """Run all demonstrations."""
    print("🚀 DATA RETENTION MANAGER DEMONSTRATION")
    print("=" * 80)
    print()

    # Check database connection
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        print("✅ Database connection successful")
        print()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Please check your .env configuration and database setup.")
        return 1

    # Run demonstrations
    demo_retention_policy_info()
    demo_deletion_safety_check()
    demo_dry_run_deletion()
    demo_orphaned_data_cleanup()
    demo_specific_video_check()

    print("🎉 DEMONSTRATION COMPLETE")
    print("=" * 80)
    print()
    print("💡 Next steps:")
    print("   1. Review the safety check results above")
    print("   2. If you want to perform actual deletion:")
    print("      manager.delete_expired_videos(dry_run=False, confirm_deletion=True)")
    print("   3. To clean up orphaned data:")
    print("      manager.cleanup_orphaned_data(dry_run=False)")
    print("   4. Always test with dry_run=True first!")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
