#!/usr/bin/env python3
"""
Create Normalized Music Videos Table

Creates a comprehensive table focused on MUSIC content with:
- ISRC codes for official music releases
- Revenue tracking over time
- All music video types (official, lyric, visualizer, live, remix)
- Normalized metrics for time-series analysis
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging
import re
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def classify_music_video_type(title: str) -> str:
    """Classify video type based on title patterns."""
    title_lower = title.lower()

    # Official Music Videos
    if re.search(r"\b(official music video|official video)\b", title_lower):
        return "Official Music Video"

    # Official Audio
    elif re.search(r"\b(official audio)\b", title_lower):
        return "Official Audio"

    # Lyric Videos
    elif re.search(r"\b(lyric video|lyrics video|official lyric|lyric)\b", title_lower):
        return "Lyric Video"

    # Visualizers
    elif re.search(r"\b(visualizer|visual|official visual)\b", title_lower):
        return "Visualizer"

    # Live Performances
    elif re.search(r"\b(live|live performance|live at|concert|acoustic)\b", title_lower):
        return "Live Performance"

    # Remixes
    elif re.search(r"\b(remix|rmx|rework|edit)\b", title_lower):
        return "Remix"

    # Behind the Scenes / Making Of
    elif re.search(r"\b(behind the scenes|making of|bts|studio session)\b", title_lower):
        return "Behind The Scenes"

    # Music Videos (general)
    elif re.search(r"\b(music video|mv)\b", title_lower):
        return "Music Video"

    # Default for music content
    else:
        return "Music Content"


def extract_song_title(video_title: str, artist_name: str) -> str:
    """Extract clean song title from video title."""
    title = video_title

    # Remove artist name from beginning
    if artist_name and title.lower().startswith(artist_name.lower()):
        title = title[len(artist_name) :].strip()
        if title.startswith(" - "):
            title = title[3:]
        elif title.startswith("-"):
            title = title[1:].strip()

    # Remove common video type indicators
    patterns_to_remove = [
        r"\[Official Music Video\]",
        r"\[Official Video\]",
        r"\[Official Audio\]",
        r"\[Official Lyric Video\]",
        r"\(Official Music Video\)",
        r"\(Official Video\)",
        r"\(Official Audio\)",
        r"\(Official Lyric Video\)",
        r"- Official Music Video",
        r"- Official Video",
        r"- Official Audio",
        r"Official Music Video",
        r"Official Video",
        r"Official Audio",
        r"Lyric Video",
        r"Visualizer",
        r"Live Performance",
        r"Remix",
    ]

    for pattern in patterns_to_remove:
        title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip()

    # Clean up extra spaces and punctuation
    title = re.sub(r"\s+", " ", title).strip()
    title = title.strip("- ")

    return title if title else video_title


def create_music_videos_table():
    """Create normalized music videos table."""
    logger.info("🎵 Creating normalized music videos table...")

    engine = get_engine()

    # Load all video and metrics data
    query = """
    SELECT
        yv.video_id,
        yv.title,
        yv.channel_title as artist_name,
        yv.isrc,
        yv.published_at,
        ym.view_count,
        ym.like_count,
        ym.comment_count,
        ym.metrics_date,
        ym.fetched_at
    FROM youtube_videos yv
    JOIN youtube_metrics ym ON yv.video_id = ym.video_id
    WHERE yv.channel_title IS NOT NULL
    ORDER BY yv.published_at DESC, ym.metrics_date DESC
    """

    df = pd.read_sql(query, engine)
    logger.info(f"📊 Loaded {len(df)} video-metrics records")

    # Get latest metrics for each video (deduplicated)
    latest_metrics = df.sort_values("metrics_date").groupby("video_id").last().reset_index()
    logger.info(f"📊 Deduplicated to {len(latest_metrics)} unique videos")

    # Classify video types and extract song titles
    latest_metrics["video_type"] = latest_metrics["title"].apply(classify_music_video_type)
    latest_metrics["song_title"] = latest_metrics.apply(
        lambda row: extract_song_title(row["title"], row["artist_name"]), axis=1
    )

    # Calculate revenue (using $2.50 RPM)
    rpm_usd = 2.50
    latest_metrics["est_revenue_usd"] = (latest_metrics["view_count"] / 1000) * rpm_usd

    # Add music content flags
    latest_metrics["has_isrc"] = latest_metrics["isrc"].notna()
    latest_metrics["is_music_content"] = True  # All content is music-related

    # Calculate engagement metrics
    latest_metrics["like_rate"] = (latest_metrics["like_count"] / latest_metrics["view_count"] * 100).fillna(0)
    latest_metrics["comment_rate"] = (latest_metrics["comment_count"] / latest_metrics["view_count"] * 100).fillna(0)
    latest_metrics["engagement_rate"] = latest_metrics["like_rate"] + latest_metrics["comment_rate"]

    # Add time-based metrics
    latest_metrics["days_since_publish"] = (
        pd.to_datetime(latest_metrics["metrics_date"]) - pd.to_datetime(latest_metrics["published_at"])
    ).dt.days

    latest_metrics["views_per_day"] = (
        latest_metrics["view_count"] / latest_metrics["days_since_publish"].replace(0, 1)
    ).fillna(0)

    # Create final normalized table
    music_videos_table = latest_metrics[
        [
            "video_id",
            "title",
            "song_title",
            "artist_name",
            "video_type",
            "isrc",
            "has_isrc",
            "published_at",
            "view_count",
            "like_count",
            "comment_count",
            "est_revenue_usd",
            "like_rate",
            "comment_rate",
            "engagement_rate",
            "days_since_publish",
            "views_per_day",
            "metrics_date",
            "fetched_at",
        ]
    ].copy()

    # Sort by artist and views
    music_videos_table = music_videos_table.sort_values(["artist_name", "view_count"], ascending=[True, False])

    logger.info(f"✅ Created normalized music videos table with {len(music_videos_table)} records")

    return music_videos_table


def create_music_summary_by_artist():
    """Create summary table by artist focusing on music content with ISRC."""
    logger.info("📊 Creating music summary by artist...")

    music_videos = create_music_videos_table()

    # Overall summary by artist
    artist_summary = (
        music_videos.groupby("artist_name")
        .agg(
            {
                "video_id": "count",
                "view_count": "sum",
                "like_count": "sum",
                "comment_count": "sum",
                "est_revenue_usd": "sum",
                "has_isrc": "sum",
                "engagement_rate": "mean",
            }
        )
        .round(2)
    )

    artist_summary.columns = [
        "total_videos",
        "total_views",
        "total_likes",
        "total_comments",
        "total_est_revenue_usd",
        "videos_with_isrc",
        "avg_engagement_rate",
    ]

    # Add ISRC percentage
    artist_summary["isrc_percentage"] = (
        artist_summary["videos_with_isrc"] / artist_summary["total_videos"] * 100
    ).round(1)

    # Add revenue per video
    artist_summary["revenue_per_video"] = (
        artist_summary["total_est_revenue_usd"] / artist_summary["total_videos"]
    ).round(2)

    # Reset index to make artist_name a column
    artist_summary = artist_summary.reset_index()

    # Sort by total revenue
    artist_summary = artist_summary.sort_values("total_est_revenue_usd", ascending=False)

    logger.info(f"✅ Created artist summary with {len(artist_summary)} artists")

    return artist_summary


def create_isrc_focused_analysis():
    """Create analysis focused specifically on videos with ISRC codes."""
    logger.info("🎵 Creating ISRC-focused analysis...")

    music_videos = create_music_videos_table()

    # Filter to only videos with ISRC codes
    isrc_videos = music_videos[music_videos["has_isrc"]].copy()

    if len(isrc_videos) == 0:
        logger.warning("⚠️ No videos with ISRC codes found")
        return pd.DataFrame()

    # Summary by artist for ISRC videos only
    isrc_summary = (
        isrc_videos.groupby("artist_name")
        .agg(
            {
                "video_id": "count",
                "view_count": "sum",
                "est_revenue_usd": "sum",
                "engagement_rate": "mean",
                "views_per_day": "mean",
            }
        )
        .round(2)
    )

    isrc_summary.columns = [
        "isrc_videos",
        "isrc_total_views",
        "isrc_total_revenue_usd",
        "isrc_avg_engagement_rate",
        "isrc_avg_views_per_day",
    ]

    # Add revenue per ISRC video
    isrc_summary["isrc_revenue_per_video"] = (
        isrc_summary["isrc_total_revenue_usd"] / isrc_summary["isrc_videos"]
    ).round(2)

    # Reset index
    isrc_summary = isrc_summary.reset_index()

    # Sort by ISRC revenue
    isrc_summary = isrc_summary.sort_values("isrc_total_revenue_usd", ascending=False)

    logger.info(f"✅ Created ISRC analysis with {len(isrc_summary)} artists")

    return isrc_summary


def create_video_type_analysis():
    """Analyze performance by video type."""
    logger.info("🎬 Creating video type analysis...")

    music_videos = create_music_videos_table()

    # Summary by video type
    type_summary = (
        music_videos.groupby("video_type")
        .agg(
            {
                "video_id": "count",
                "view_count": ["sum", "mean"],
                "est_revenue_usd": ["sum", "mean"],
                "engagement_rate": "mean",
                "has_isrc": "sum",
            }
        )
        .round(2)
    )

    # Flatten column names
    type_summary.columns = [
        "video_count",
        "total_views",
        "avg_views_per_video",
        "total_revenue_usd",
        "avg_revenue_per_video",
        "avg_engagement_rate",
        "videos_with_isrc",
    ]

    # Add ISRC percentage by type
    type_summary["isrc_percentage"] = (type_summary["videos_with_isrc"] / type_summary["video_count"] * 100).round(1)

    # Reset index
    type_summary = type_summary.reset_index()

    # Sort by total revenue
    type_summary = type_summary.sort_values("total_revenue_usd", ascending=False)

    logger.info(f"✅ Created video type analysis with {len(type_summary)} types")

    return type_summary


def save_music_tables():
    """Save all music analysis tables."""
    logger.info("💾 Saving music analysis tables...")

    # Create output directory
    output_dir = "music_analysis_tables"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Create all tables
        music_videos = create_music_videos_table()
        artist_summary = create_music_summary_by_artist()
        isrc_analysis = create_isrc_focused_analysis()
        video_type_analysis = create_video_type_analysis()

        # Save as CSV files
        music_videos.to_csv(f"{output_dir}/normalized_music_videos.csv", index=False)
        artist_summary.to_csv(f"{output_dir}/artist_music_summary.csv", index=False)

        if not isrc_analysis.empty:
            isrc_analysis.to_csv(f"{output_dir}/isrc_focused_analysis.csv", index=False)

        video_type_analysis.to_csv(f"{output_dir}/video_type_analysis.csv", index=False)

        # Save as Excel with multiple sheets (if openpyxl is available)
        try:
            with pd.ExcelWriter(f"{output_dir}/music_analysis_complete.xlsx") as writer:
                music_videos.to_excel(writer, sheet_name="Normalized_Videos", index=False)
                artist_summary.to_excel(writer, sheet_name="Artist_Summary", index=False)
                if not isrc_analysis.empty:
                    isrc_analysis.to_excel(writer, sheet_name="ISRC_Analysis", index=False)
                video_type_analysis.to_excel(writer, sheet_name="Video_Types", index=False)
        except ImportError:
            logger.warning("⚠️ Excel export skipped (openpyxl not available)")

        logger.info(f"✅ Saved music analysis tables to {output_dir}/")

        return {
            "music_videos": music_videos,
            "artist_summary": artist_summary,
            "isrc_analysis": isrc_analysis,
            "video_type_analysis": video_type_analysis,
        }

    except Exception as e:
        logger.error(f"❌ Failed to save tables: {e}")
        return None


def print_music_analysis_summary():
    """Print comprehensive music analysis summary."""
    logger.info("📊 Generating music analysis summary...")

    tables = save_music_tables()
    if not tables:
        return

    music_videos = tables["music_videos"]
    artist_summary = tables["artist_summary"]
    isrc_analysis = tables["isrc_analysis"]
    video_type_analysis = tables["video_type_analysis"]

    print("\n🎵 NORMALIZED MUSIC VIDEOS ANALYSIS")
    print("=" * 60)

    # Overall stats
    total_videos = len(music_videos)
    total_views = music_videos["view_count"].sum()
    total_revenue = music_videos["est_revenue_usd"].sum()
    videos_with_isrc = music_videos["has_isrc"].sum()

    print(f"📊 Total Music Videos: {total_videos:,}")
    print(f"👀 Total Views: {total_views:,}")
    print(f"💰 Total Estimated Revenue: ${total_revenue:,.2f}")
    print(f"🎵 Videos with ISRC: {videos_with_isrc:,} ({videos_with_isrc/total_videos*100:.1f}%)")

    # Artist summary
    print(f"\n🎤 ARTIST MUSIC PERFORMANCE:")
    print(
        artist_summary[
            [
                "artist_name",
                "total_videos",
                "total_views",
                "total_est_revenue_usd",
                "videos_with_isrc",
                "isrc_percentage",
            ]
        ].to_string(index=False)
    )

    # ISRC analysis
    if not isrc_analysis.empty:
        print(f"\n🎵 ISRC-CODED MUSIC PERFORMANCE:")
        print(
            isrc_analysis[
                ["artist_name", "isrc_videos", "isrc_total_views", "isrc_total_revenue_usd", "isrc_revenue_per_video"]
            ].to_string(index=False)
        )

    # Video type analysis
    print(f"\n🎬 PERFORMANCE BY VIDEO TYPE:")
    print(
        video_type_analysis[
            [
                "video_type",
                "video_count",
                "total_views",
                "total_revenue_usd",
                "avg_revenue_per_video",
                "isrc_percentage",
            ]
        ].to_string(index=False)
    )

    # Top performing music videos
    print(f"\n🔥 TOP PERFORMING MUSIC VIDEOS:")
    top_videos = music_videos.nlargest(10, "view_count")[
        ["artist_name", "song_title", "video_type", "view_count", "est_revenue_usd", "has_isrc"]
    ]
    for _, video in top_videos.iterrows():
        isrc_indicator = "🎵" if video["has_isrc"] else "📹"
        print(
            f"   {isrc_indicator} {video['artist_name']}: {video['song_title']} ({video['video_type']}) - {video['view_count']:,} views (${video['est_revenue_usd']:.2f})"
        )

    print(f"\n✅ Music analysis complete! Tables saved to music_analysis_tables/")


def main():
    """Main function."""
    try:
        print_music_analysis_summary()
        return True
    except Exception as e:
        logger.error(f"❌ Music analysis failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
