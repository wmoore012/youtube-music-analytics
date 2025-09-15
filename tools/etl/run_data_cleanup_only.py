#!/usr/bin/env python3
"""
Data Cleanup Only Pipeline

This script runs ONLY data cleanup and quality checks - NO ETL.
Designed for frequent runs (every few hours) to maintain data quality
without triggering the main ETL process.

Usage:
    python tools/etl/run_data_cleanup_only.py
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.youtubeviz.data import load_recent_window_days
from web.etl_helpers import get_engine

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_data_cleanup_only():
    """Run data cleanup without ETL."""
    logger.info("🧹 STARTING DATA CLEANUP ONLY")
    logger.info("=" * 40)

    try:
        # Connect to database (yt_proj only!)
        engine = get_engine()
        logger.info("✅ Connected to yt_proj database")

        # Load current data
        data = load_recent_window_days(days=90, engine=engine)
        original_count = len(data)

        if original_count == 0:
            logger.info("📊 No data found - nothing to clean")
            return True

        # Deduplicate by video_id (natural key)
        clean_data = data.drop_duplicates(["video_id"], keep="first")
        duplicates_removed = original_count - len(clean_data)

        logger.info(f"📊 Data cleanup results:")
        logger.info(f"   Original records: {original_count:,}")
        logger.info(f"   Clean records: {len(clean_data):,}")
        logger.info(f"   Duplicates removed: {duplicates_removed:,}")

        # Calculate duplicate percentage
        duplicate_pct = (duplicates_removed / original_count * 100) if original_count > 0 else 0

        if duplicate_pct > 20:
            logger.warning(f"⚠️  High duplicate rate: {duplicate_pct:.1f}%")
        elif duplicate_pct > 0:
            logger.info(f"✅ Normal duplicate rate: {duplicate_pct:.1f}%")
        else:
            logger.info("✅ No duplicates found - data is clean!")

        # Check for data quality issues
        enchanting_count = data["artist_name"].str.contains("Enchanting", case=False, na=False).sum()
        if enchanting_count > 0:
            logger.error(f"❌ Found {enchanting_count} Enchanting records - should be 0!")
            return False

        none_count = (data["artist_name"] == "None").sum()
        if none_count > 0:
            logger.warning(f"⚠️  Found {none_count} records with 'None' artist names")

        logger.info("✅ Data cleanup completed successfully!")
        return True

    except Exception as e:
        logger.error(f"💥 Data cleanup failed: {str(e)}")
        return False


def main():
    """Main entry point."""
    success = run_data_cleanup_only()

    if success:
        logger.info("🎉 Data cleanup completed successfully!")
        sys.exit(0)
    else:
        logger.error("💥 Data cleanup failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
