#!/usr/bin/env python3
"""
Professional Database Cleanup Utility for YouTube Channel Data Management

This script provides bulletproof functionality to clean up YouTube channel data
from your local database. It automatically reads your .env configuration to
determine which channels to keep vs delete, ensuring your analytics stay focused
on your target artists while removing irrelevant content.

Key Features:
- Automatically parses channel URLs from .env to extract channel names
- Provides detailed reasoning for each deletion decision
- Supports configurable channel analysis types (music artists, podcasts, etc.)
- Includes comprehensive backup warnings and safety confirmations
- Formats all SQL queries for human readability and debugging

⚠️ Data deletion operations are irreversible. Please create a backup.

Usage Examples:
    python cleanup_db.py --dry-run          # Preview what will be deleted (safe)
    python cleanup_db.py --confirm          # Actually perform the cleanup
    python cleanup_db.py --validate-config  # Check .env channel configuration
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def extract_channel_name_from_url(url: str) -> Optional[str]:
    """
    Extract YouTube channel handle from various URL formats found in .env file.

    Handles these common YouTube URL patterns:
    - https://www.youtube.com/@COBRAH → "COBRAH"
    - https://youtube.com/@BicFizzle → "BicFizzle"
    - https://www.youtube.com/channel/UC123... → "UC123..." (channel ID)
    - https://www.youtube.com/c/ChannelName → "ChannelName"

    Args:
        url: YouTube channel URL from .env configuration

    Returns:
        Channel handle/name or None if URL format not recognized

    Note:
        This extracts the handle from URLs, but the database may store different
        display names. Use get_channel_handle_to_display_name_mapping() to resolve.
    """
    if not url or not isinstance(url, str):
        return None

    # Handle @username format (most common for new channels)
    if "@" in url:
        # Extract everything after the @ symbol
        match = re.search(r"@([^/?&]+)", url)
        if match:
            return match.group(1)

    # Handle /channel/UC... format (channel ID)
    if "/channel/" in url:
        match = re.search(r"/channel/([^/?&]+)", url)
        if match:
            return match.group(1)

    # Handle /c/ChannelName format (custom URL)
    if "/c/" in url:
        match = re.search(r"/c/([^/?&]+)", url)
        if match:
            return match.group(1)

    # Handle /user/username format (legacy)
    if "/user/" in url:
        match = re.search(r"/user/([^/?&]+)", url)
        if match:
            return match.group(1)

    logger.warning(f"Could not extract channel name from URL: {url}")
    return None


def get_channel_handle_to_display_name_mapping() -> Dict[str, str]:
    """
    Create mapping from YouTube handles (from URLs) to actual database channel names.

    This handles the common discrepancy where:
    - .env URLs contain handles like @BicFizzle, @FlyanaBoss
    - Database stores display names like "BiC Fizzle", "Flyana Boss"

    Returns:
        Dictionary mapping URL handles to database channel names

    Note:
        This mapping should be updated when new artists are added or when
        YouTube channels change their display names.
    """
    return {
        # Handle from URL → Actual channel name in database
        "BicFizzle": "BiC Fizzle",  # @BicFizzle → "BiC Fizzle"
        "FlyanaBoss": "Flyana Boss",  # @FlyanaBoss → "Flyana Boss"
        "COBRAH": "COBRAH",  # @COBRAH → "COBRAH" (exact match)
        "re6ce": "re6ce",  # @re6ce → "re6ce" (exact match)
        "hicorook": "hicorook",  # @hicorook → "hicorook" (exact match)
        "MyNameIsRaiche": "MyNameIsRaiche",  # @MyNameIsRaiche → "MyNameIsRaiche" (exact match)
    }


def load_channels_from_env() -> Tuple[Set[str], Dict[str, str]]:
    """
    Parse all YouTube channel URLs from .env file to determine keep/delete lists.

    This function scans your .env file for variables ending in '_YT' which should
    contain YouTube channel URLs. It extracts the channel handles and maps them
    to the actual database channel names using the handle-to-display-name mapping.

    Returns:
        Tuple of (keep_channels_set, url_to_name_mapping)

    Example .env variables it looks for:
        YT_COBRAH_YT=https://www.youtube.com/@COBRAH
        YT_BICFIZZLE_YT=https://youtube.com/@BicFizzle
        YT_FLYANABOSS_YT=https://www.youtube.com/@FlyanaBoss
    """
    keep_channels = set()
    url_mapping = {}
    handle_to_display = get_channel_handle_to_display_name_mapping()

    # Scan environment variables for YouTube channel URLs
    for key, value in os.environ.items():
        if key.endswith("_YT") and value:
            channel_handle = extract_channel_name_from_url(value)
            if channel_handle:
                # Map handle to actual database channel name
                display_name = handle_to_display.get(channel_handle, channel_handle)
                keep_channels.add(display_name)
                url_mapping[value] = display_name

                if channel_handle != display_name:
                    logger.info(f"Found channel in .env: {channel_handle} → {display_name} from {key}")
                else:
                    logger.info(f"Found channel in .env: {display_name} from {key}")
            else:
                logger.warning(f"Could not parse channel URL in {key}: {value}")

    if not keep_channels:
        logger.error("No valid YouTube channel URLs found in .env file!")
        logger.error("Please add channel URLs with variables ending in '_YT'")
        logger.error("Example: YT_COBRAH_YT=https://www.youtube.com/@COBRAH")

    return keep_channels, url_mapping


def get_channel_analysis_type() -> str:
    """
    Get the type of channel analysis from .env configuration.

    This determines what kind of data quality flags and warnings to show users.
    Different analysis types have different cleanup strategies and user guidance.

    Returns:
        Analysis type string (music_artists, podcasts, mixed, general)
    """
    analysis_type = os.getenv("CHANNEL_ANALYSIS_TYPE", "music_artists").lower()

    valid_types = {"music_artists", "podcasts", "mixed", "general"}
    if analysis_type not in valid_types:
        logger.warning(f"Invalid CHANNEL_ANALYSIS_TYPE: {analysis_type}")
        logger.warning(f"Valid options: {', '.join(valid_types)}")
        logger.warning("Defaulting to 'music_artists'")
        return "music_artists"

    return analysis_type


def validate_database_channels(engine, keep_channels: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Check which channels exist in database vs .env configuration.

    This critical validation step shows users exactly what will happen:
    - Which channels from .env are already in the database (safe)
    - Which database channels are NOT in .env (will be deleted)
    - Which .env channels are missing from database (will be added by ETL)

    Args:
        engine: SQLAlchemy database engine
        keep_channels: Set of channel names from .env that should be kept

    Returns:
        Tuple of (channels_in_db_to_delete, channels_in_env_missing_from_db)
    """
    # Query all unique channel names currently in the database
    query = text(
        """
        SELECT DISTINCT channel_title 
        FROM youtube_videos 
        WHERE channel_title IS NOT NULL 
          AND channel_title != ''
        ORDER BY channel_title
    """
    )

    with engine.connect() as conn:
        result = conn.execute(query)
        db_channels = {row[0] for row in result.fetchall()}

    # Calculate differences between .env config and database reality
    channels_to_delete = db_channels - keep_channels
    missing_from_db = keep_channels - db_channels

    return channels_to_delete, missing_from_db


def show_configuration_summary(
    keep_channels: Set[str], channels_to_delete: Set[str], missing_from_db: Set[str], analysis_type: str
):
    """
    Display comprehensive configuration summary with friendly warnings.

    This gives users complete visibility into what the cleanup will do and
    helps them understand the impact on their analytics before proceeding.
    """
    print("\n" + "=" * 70)
    print("🔍 DATABASE CLEANUP CONFIGURATION SUMMARY")
    print("=" * 70)

    print(f"\n📊 Analysis Type: {analysis_type}")
    if analysis_type == "music_artists":
        print("   → Optimized for music artist analytics and sentiment analysis")
        print("   → Will preserve all content from artist channels (music + other)")
        print("   → Removes non-artist channels (podcasts, unrelated content)")
    elif analysis_type == "podcasts":
        print("   → Optimized for podcast analytics")
        print("   → Will preserve podcast channels and remove music content")

    print(f"\n✅ Channels to KEEP ({len(keep_channels)} total):")
    if keep_channels:
        for channel in sorted(keep_channels):
            print(f"   • {channel}")
    else:
        print("   ⚠️  No channels configured in .env file!")

    print(f"\n❌ Channels to DELETE ({len(channels_to_delete)} total):")
    if channels_to_delete:
        for channel in sorted(channels_to_delete):
            print(f"   • {channel}")
    else:
        print("   ✨ No unwanted channels found in database")

    print(f"\n📥 Channels in .env but missing from database ({len(missing_from_db)} total):")
    if missing_from_db:
        for channel in sorted(missing_from_db):
            print(f"   • {channel} (will be added when ETL runs)")
    else:
        print("   ✨ All .env channels already exist in database")

    # Show serious backup warning
    if channels_to_delete:
        print("\n" + "⚠️ " * 20)
        print("⚠️  Data deletion operations are irreversible. Please create a backup.")
        print("⚠️  This will permanently remove videos, comments, metrics, and sentiment data.")
        print("⚠️  Consider exporting important analytics before proceeding.")
        print("⚠️ " * 20)


def get_channel_video_ids(engine, channel_title: str) -> List[str]:
    """
    Retrieve all video IDs associated with a specific YouTube channel.

    This is the foundation for all cleanup operations since video_id is the
    natural key that links records across all YouTube-related tables.

    Args:
        engine: SQLAlchemy database engine
        channel_title: Name of the YouTube channel to look up

    Returns:
        List of video IDs (strings) for the specified channel
    """
    query = text(
        """
        SELECT video_id 
        FROM youtube_videos 
        WHERE channel_title = :channel_title
        ORDER BY published_at DESC
    """
    )

    with engine.connect() as conn:
        result = conn.execute(query, {"channel_title": channel_title})
        return [row[0] for row in result.fetchall()]


def cleanup_channel_data(engine, channel_title: str, dry_run: bool = True) -> Dict[str, int]:
    """
    Remove all database records associated with a specific YouTube channel.

    This function performs a comprehensive cleanup across all YouTube-related tables:
    1. youtube_comments - User comments and engagement data
    2. youtube_metrics - View counts, likes, subscriber metrics over time
    3. youtube_sentiment_* - Sentiment analysis results and summaries
    4. youtube_videos - Core video metadata (deleted last due to foreign keys)

    The cleanup preserves referential integrity by deleting child records first,
    then parent records. All SQL is formatted for readability and debugging.

    Args:
        engine: SQLAlchemy database engine
        channel_title: YouTube channel name to remove from database
        dry_run: If True, only count records without deleting (safe preview mode)

    Returns:
        Dictionary with counts of records that were/would be deleted by table type
    """
    print(f"\n{'🔍 [PREVIEW] ' if dry_run else '🗑️  [DELETING] '}Channel: {channel_title}")

    # Get all video IDs for this channel (our natural key for cleanup)
    video_ids = get_channel_video_ids(engine, channel_title)
    if not video_ids:
        print(f"   ℹ️  No videos found for channel '{channel_title}' - nothing to clean")
        return {"videos": 0, "comments": 0, "metrics": 0, "sentiment": 0}

    print(f"   📹 Found {len(video_ids)} videos to process")

    # Initialize statistics tracking
    stats = {"videos": 0, "comments": 0, "metrics": 0, "sentiment": 0}

    with engine.connect() as conn:
        # Step 1: Clean up YouTube comments (user engagement data)
        # Comments contain valuable sentiment data but take up significant storage
        comment_count_query = text(
            """
            SELECT COUNT(*) as comment_count
            FROM youtube_comments 
            WHERE video_id = ANY(:video_ids)
        """
        )

        result = conn.execute(comment_count_query, {"video_ids": video_ids})
        stats["comments"] = result.fetchone()[0]

        if stats["comments"] > 0:
            print(f"   💬 Comments: {stats['comments']:,} records")
            if not dry_run:
                delete_comments_query = text(
                    """
                    DELETE FROM youtube_comments 
                    WHERE video_id = ANY(:video_ids)
                """
                )
                conn.execute(delete_comments_query, {"video_ids": video_ids})

        # Step 2: Clean up YouTube metrics (performance data over time)
        # Metrics track views, likes, subscriber changes - critical for trend analysis
        metrics_count_query = text(
            """
            SELECT COUNT(*) as metrics_count
            FROM youtube_metrics 
            WHERE video_id = ANY(:video_ids)
        """
        )

        result = conn.execute(metrics_count_query, {"video_ids": video_ids})
        stats["metrics"] = result.fetchone()[0]

        if stats["metrics"] > 0:
            print(f"   📊 Metrics: {stats['metrics']:,} records")
            if not dry_run:
                delete_metrics_query = text(
                    """
                    DELETE FROM youtube_metrics 
                    WHERE video_id = ANY(:video_ids)
                """
                )
                conn.execute(delete_metrics_query, {"video_ids": video_ids})

        # Step 3: Clean up sentiment analysis data (AI-generated insights)
        # Sentiment tables contain VADER analysis results for comment emotional tone
        sentiment_tables = ["youtube_sentiment", "youtube_sentiment_by_video", "youtube_sentiment_summary"]

        for table_name in sentiment_tables:
            try:
                sentiment_count_query = text(
                    f"""
                    SELECT COUNT(*) as sentiment_count
                    FROM {table_name} 
                    WHERE video_id = ANY(:video_ids)
                """
                )

                result = conn.execute(sentiment_count_query, {"video_ids": video_ids})
                table_count = result.fetchone()[0]
                stats["sentiment"] += table_count

                if table_count > 0 and not dry_run:
                    delete_sentiment_query = text(
                        f"""
                        DELETE FROM {table_name} 
                        WHERE video_id = ANY(:video_ids)
                    """
                    )
                    conn.execute(delete_sentiment_query, {"video_ids": video_ids})

            except Exception as e:
                logger.warning(f"Could not access sentiment table {table_name}: {e}")

        if stats["sentiment"] > 0:
            print(f"   🧠 Sentiment: {stats['sentiment']:,} records across all sentiment tables")

        # Step 4: Clean up core video records (metadata and ISRC data)
        # Videos table is the parent - must be deleted last to maintain referential integrity
        video_count_query = text(
            """
            SELECT COUNT(*) as video_count
            FROM youtube_videos 
            WHERE channel_title = :channel_title
        """
        )

        result = conn.execute(video_count_query, {"channel_title": channel_title})
        stats["videos"] = result.fetchone()[0]

        if stats["videos"] > 0:
            print(f"   🎵 Videos: {stats['videos']:,} records (includes ISRC metadata)")
            if not dry_run:
                delete_videos_query = text(
                    """
                    DELETE FROM youtube_videos 
                    WHERE channel_title = :channel_title
                """
                )
                conn.execute(delete_videos_query, {"channel_title": channel_title})

        # Commit all changes if this is not a dry run
        if not dry_run:
            conn.commit()

    return stats


def get_deletion_reasoning(channel_name: str, analysis_type: str) -> str:
    """
    Provide clear, human-readable reasoning for why a channel should be deleted.

    This helps users understand the cleanup logic and builds confidence that
    the system is making intelligent decisions about their data.
    """
    # Common patterns that indicate non-artist content
    podcast_indicators = ["podcast", "show", "talk", "interview", "news"]
    business_indicators = ["inc", "llc", "corp", "company", "official"]

    channel_lower = channel_name.lower()

    # Check for obvious podcast content
    if any(indicator in channel_lower for indicator in podcast_indicators):
        return f"Podcast/talk show content - not relevant for {analysis_type} analysis"

    # Check for business/label channels vs artist channels
    if any(indicator in channel_lower for indicator in business_indicators):
        return f"Business/label channel - may contain mixed content not specific to artist"

    # Check for secondary/fan channels
    if "fan" in channel_lower or "unofficial" in channel_lower:
        return "Fan or unofficial channel - may contain low-quality or duplicate content"

    # Default reasoning
    return f"Channel not found in .env configuration - not part of target {analysis_type} analysis"


def confirm_deletion_with_user(channels_to_delete: Set[str], analysis_type: str) -> bool:
    """
    Get explicit user confirmation for each channel deletion with detailed reasoning.

    This implements the "Don't Make Me Think" principle by clearly explaining
    each decision and giving users control over the cleanup process.
    """
    if not channels_to_delete:
        return True

    print(f"\n🤔 DELETION CONFIRMATION REQUIRED")
    print(f"The following {len(channels_to_delete)} channels will be permanently deleted:")
    print("-" * 60)

    for channel in sorted(channels_to_delete):
        reasoning = get_deletion_reasoning(channel, analysis_type)
        print(f"\n❌ {channel}")
        print(f"   Reason: {reasoning}")

    print("\n" + "⚠️ " * 15)
    print("This action cannot be undone. All videos, comments, metrics,")
    print("and sentiment analysis data for these channels will be lost forever.")
    print("⚠️ " * 15)

    while True:
        response = input(f"\nType 'DELETE' to confirm removal of {len(channels_to_delete)} channels: ").strip()

        if response == "DELETE":
            return True
        elif response.lower() in ["no", "n", "cancel", "abort", "quit"]:
            print("✅ Cleanup cancelled - no data was deleted")
            return False
        else:
            print("❌ Invalid response. Type 'DELETE' to confirm or 'cancel' to abort.")


def main():
    """
    Main cleanup orchestration with comprehensive validation and user guidance.

    This function coordinates the entire cleanup process:
    1. Parse command line arguments and .env configuration
    2. Validate database connectivity and channel configuration
    3. Show detailed preview of what will be deleted and why
    4. Get explicit user confirmation for destructive operations
    5. Perform cleanup with detailed progress reporting
    6. Provide summary of results and next steps
    """
    # Parse command line arguments
    dry_run = "--dry-run" in sys.argv
    confirm = "--confirm" in sys.argv
    validate_only = "--validate-config" in sys.argv

    print("🧹 YouTube ETL Database Cleanup Utility")
    print("=" * 50)

    # Load configuration from .env file
    try:
        keep_channels, url_mapping = load_channels_from_env()
        analysis_type = get_channel_analysis_type()
    except Exception as e:
        logger.error(f"Failed to load configuration from .env: {e}")
        logger.error("Please check your .env file and try again")
        sys.exit(1)

    # Connect to database using unified engine
    try:
        engine = get_engine()  # Uses .env configuration automatically
        logger.info("✅ Database connection established")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.error("Please check your database configuration in .env")
        sys.exit(1)

    # Validate current database state vs .env configuration
    try:
        channels_to_delete, missing_from_db = validate_database_channels(engine, keep_channels)
    except Exception as e:
        logger.error(f"Failed to validate database channels: {e}")
        sys.exit(1)

    # Show comprehensive configuration summary
    show_configuration_summary(keep_channels, channels_to_delete, missing_from_db, analysis_type)

    # Handle validation-only mode
    if validate_only:
        print("\n✅ Configuration validation complete")
        if missing_from_db:
            print(f"\n💡 Next steps: Run your ETL pipeline to add {len(missing_from_db)} missing channels")
        sys.exit(0)

    # Handle dry run mode (safe preview)
    if dry_run:
        print(f"\n🔍 DRY RUN MODE - No data will be deleted (safe preview)")
    else:
        print(f"\n🗑️  LIVE CLEANUP MODE - Data will be permanently deleted")

    # Get user confirmation for destructive operations
    if not dry_run and not confirm:
        if not confirm_deletion_with_user(channels_to_delete, analysis_type):
            sys.exit(0)

    # Perform the actual cleanup operations
    total_stats = {"videos": 0, "comments": 0, "metrics": 0, "sentiment": 0}

    try:
        print(f"\n🚀 Starting cleanup of {len(channels_to_delete)} channels...")

        for channel in sorted(channels_to_delete):
            stats = cleanup_channel_data(engine, channel, dry_run)
            for key in total_stats:
                total_stats[key] += stats[key]

        # Display final summary
        print("\n" + "=" * 50)
        print("📊 CLEANUP SUMMARY")
        print("=" * 50)
        print(f"Videos: {total_stats['videos']:,} records")
        print(f"Comments: {total_stats['comments']:,} records")
        print(f"Metrics: {total_stats['metrics']:,} records")
        print(f"Sentiment: {total_stats['sentiment']:,} records")
        print(f"Total: {sum(total_stats.values()):,} records")

        if dry_run:
            print(f"\n🔍 Preview complete - no data was modified")
            print(f"💡 Run with --confirm to perform actual cleanup")
        else:
            print(f"\n✅ Cleanup completed successfully!")
            print(f"💡 Your database now contains only data for: {', '.join(sorted(keep_channels))}")

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        logger.error("Database may be in an inconsistent state - please check manually")
        sys.exit(1)


if __name__ == "__main__":
    main()
