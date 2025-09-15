#!/usr/bin/env python3
"""
Create Time Series Tracking Table

Creates a table for tracking music video performance over time with:
- Daily/weekly snapshots of key metrics
- Revenue tracking over time
- Growth rate calculations
- Trend analysis for long-term projects
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_time_series_tracking_table():
    """Create comprehensive time series tracking table."""
    logger.info("📈 Creating time series tracking table...")

    engine = get_engine()

    # Load ALL video metrics data (not deduplicated) for time series
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
        ym.fetched_at,
        DATEDIFF(ym.metrics_date, yv.published_at) as days_since_publish
    FROM youtube_videos yv
    JOIN youtube_metrics ym ON yv.video_id = ym.video_id
    WHERE yv.channel_title IS NOT NULL
    ORDER BY yv.video_id, ym.metrics_date
    """

    df = pd.read_sql(query, engine)
    logger.info(f"📊 Loaded {len(df)} time series records")

    # Calculate revenue for each time point
    rpm_usd = 2.50
    df["est_revenue_usd"] = (df["view_count"] / 1000) * rpm_usd

    # Calculate engagement metrics
    df["like_rate"] = (df["like_count"] / df["view_count"] * 100).fillna(0)
    df["comment_rate"] = (df["comment_count"] / df["view_count"] * 100).fillna(0)
    df["engagement_rate"] = df["like_rate"] + df["comment_rate"]

    # Calculate daily velocity (views per day since publish)
    df["views_per_day"] = (df["view_count"] / df["days_since_publish"].replace(0, 1)).fillna(0)

    # Calculate growth rates (day-over-day)
    df = df.sort_values(["video_id", "metrics_date"])
    df["prev_views"] = df.groupby("video_id")["view_count"].shift(1)
    df["prev_revenue"] = df.groupby("video_id")["est_revenue_usd"].shift(1)

    df["views_growth_rate"] = ((df["view_count"] - df["prev_views"]) / df["prev_views"] * 100).fillna(0)
    df["revenue_growth_rate"] = ((df["est_revenue_usd"] - df["prev_revenue"]) / df["prev_revenue"] * 100).fillna(0)

    # Add video classification
    def classify_video_type(title):
        title_lower = title.lower()
        if "official music video" in title_lower or "official video" in title_lower:
            return "Official Music Video"
        elif "official audio" in title_lower:
            return "Official Audio"
        elif "lyric" in title_lower:
            return "Lyric Video"
        elif "visualizer" in title_lower or "visual" in title_lower:
            return "Visualizer"
        elif "live" in title_lower or "acoustic" in title_lower:
            return "Live Performance"
        elif "remix" in title_lower or "rmx" in title_lower:
            return "Remix"
        else:
            return "Music Content"

    df["video_type"] = df["title"].apply(classify_video_type)
    df["has_isrc"] = df["isrc"].notna()

    # Add time-based features
    df["metrics_date"] = pd.to_datetime(df["metrics_date"])
    df["year"] = df["metrics_date"].dt.year
    df["month"] = df["metrics_date"].dt.month
    df["week"] = df["metrics_date"].dt.isocalendar().week
    df["day_of_week"] = df["metrics_date"].dt.day_name()

    # Create final time series table
    time_series_table = df[
        [
            "video_id",
            "title",
            "artist_name",
            "video_type",
            "has_isrc",
            "published_at",
            "metrics_date",
            "year",
            "month",
            "week",
            "day_of_week",
            "days_since_publish",
            "view_count",
            "like_count",
            "comment_count",
            "est_revenue_usd",
            "like_rate",
            "comment_rate",
            "engagement_rate",
            "views_per_day",
            "views_growth_rate",
            "revenue_growth_rate",
            "fetched_at",
        ]
    ].copy()

    logger.info(f"✅ Created time series table with {len(time_series_table)} records")

    return time_series_table


def create_artist_performance_over_time():
    """Create artist performance tracking over time."""
    logger.info("🎤 Creating artist performance over time...")

    time_series = create_time_series_tracking_table()

    # Aggregate by artist and date
    artist_daily = (
        time_series.groupby(["artist_name", "metrics_date"])
        .agg(
            {
                "video_id": "nunique",
                "view_count": "sum",
                "like_count": "sum",
                "comment_count": "sum",
                "est_revenue_usd": "sum",
                "engagement_rate": "mean",
                "has_isrc": "sum",
            }
        )
        .reset_index()
    )

    artist_daily.columns = [
        "artist_name",
        "date",
        "active_videos",
        "total_views",
        "total_likes",
        "total_comments",
        "total_revenue_usd",
        "avg_engagement_rate",
        "videos_with_isrc",
    ]

    # Calculate growth rates
    artist_daily = artist_daily.sort_values(["artist_name", "date"])
    artist_daily["prev_views"] = artist_daily.groupby("artist_name")["total_views"].shift(1)
    artist_daily["prev_revenue"] = artist_daily.groupby("artist_name")["total_revenue_usd"].shift(1)

    artist_daily["daily_views_growth"] = (
        (artist_daily["total_views"] - artist_daily["prev_views"]) / artist_daily["prev_views"] * 100
    ).fillna(0)

    artist_daily["daily_revenue_growth"] = (
        (artist_daily["total_revenue_usd"] - artist_daily["prev_revenue"]) / artist_daily["prev_revenue"] * 100
    ).fillna(0)

    # Add time features
    artist_daily["year"] = pd.to_datetime(artist_daily["date"]).dt.year
    artist_daily["month"] = pd.to_datetime(artist_daily["date"]).dt.month
    artist_daily["week"] = pd.to_datetime(artist_daily["date"]).dt.isocalendar().week

    logger.info(f"✅ Created artist performance tracking with {len(artist_daily)} records")

    return artist_daily


def create_video_lifecycle_analysis():
    """Analyze video performance lifecycle."""
    logger.info("📹 Creating video lifecycle analysis...")

    time_series = create_time_series_tracking_table()

    # Focus on videos with multiple data points
    video_counts = time_series["video_id"].value_counts()
    multi_point_videos = video_counts[video_counts > 1].index

    lifecycle_data = time_series[time_series["video_id"].isin(multi_point_videos)].copy()

    # Calculate lifecycle metrics
    lifecycle_summary = (
        lifecycle_data.groupby("video_id")
        .agg(
            {
                "title": "first",
                "artist_name": "first",
                "video_type": "first",
                "has_isrc": "first",
                "published_at": "first",
                "view_count": ["first", "last", "max"],
                "est_revenue_usd": ["first", "last", "max"],
                "engagement_rate": "mean",
                "views_growth_rate": "mean",
                "metrics_date": ["min", "max", "count"],
            }
        )
        .reset_index()
    )

    # Flatten column names
    lifecycle_summary.columns = [
        "video_id",
        "title",
        "artist_name",
        "video_type",
        "has_isrc",
        "published_at",
        "initial_views",
        "final_views",
        "peak_views",
        "initial_revenue",
        "final_revenue",
        "peak_revenue",
        "avg_engagement_rate",
        "avg_growth_rate",
        "first_tracked",
        "last_tracked",
        "tracking_points",
    ]

    # Calculate total growth
    lifecycle_summary["total_views_growth"] = (
        (lifecycle_summary["final_views"] - lifecycle_summary["initial_views"])
        / lifecycle_summary["initial_views"]
        * 100
    ).fillna(0)

    lifecycle_summary["total_revenue_growth"] = (
        (lifecycle_summary["final_revenue"] - lifecycle_summary["initial_revenue"])
        / lifecycle_summary["initial_revenue"]
        * 100
    ).fillna(0)

    # Calculate tracking duration
    lifecycle_summary["tracking_days"] = (
        pd.to_datetime(lifecycle_summary["last_tracked"]) - pd.to_datetime(lifecycle_summary["first_tracked"])
    ).dt.days

    logger.info(f"✅ Created lifecycle analysis for {len(lifecycle_summary)} videos")

    return lifecycle_summary


def save_time_series_tables():
    """Save all time series tracking tables."""
    logger.info("💾 Saving time series tracking tables...")

    # Create output directory
    output_dir = "time_series_tracking"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Create all tables
        time_series = create_time_series_tracking_table()
        artist_performance = create_artist_performance_over_time()
        video_lifecycle = create_video_lifecycle_analysis()

        # Save as CSV files
        time_series.to_csv(f"{output_dir}/complete_time_series.csv", index=False)
        artist_performance.to_csv(f"{output_dir}/artist_performance_over_time.csv", index=False)
        video_lifecycle.to_csv(f"{output_dir}/video_lifecycle_analysis.csv", index=False)

        logger.info(f"✅ Saved time series tables to {output_dir}/")

        return {
            "time_series": time_series,
            "artist_performance": artist_performance,
            "video_lifecycle": video_lifecycle,
        }

    except Exception as e:
        logger.error(f"❌ Failed to save time series tables: {e}")
        return None


def print_time_series_summary():
    """Print time series analysis summary."""
    logger.info("📈 Generating time series summary...")

    tables = save_time_series_tables()
    if not tables:
        return

    time_series = tables["time_series"]
    artist_performance = tables["artist_performance"]
    video_lifecycle = tables["video_lifecycle"]

    print("\n📈 TIME SERIES TRACKING ANALYSIS")
    print("=" * 60)

    # Overall stats
    total_snapshots = len(time_series)
    unique_videos = time_series["video_id"].nunique()
    date_range = f"{time_series['metrics_date'].min().date()} to {time_series['metrics_date'].max().date()}"

    print(f"📊 Total Time Series Snapshots: {total_snapshots:,}")
    print(f"🎵 Unique Videos Tracked: {unique_videos:,}")
    print(f"📅 Date Range: {date_range}")

    # Latest performance by artist
    latest_performance = artist_performance.sort_values("date").groupby("artist_name").last()
    print(f"\n🎤 LATEST ARTIST PERFORMANCE:")
    print(
        latest_performance[
            ["active_videos", "total_views", "total_revenue_usd", "daily_views_growth", "videos_with_isrc"]
        ].to_string()
    )

    # Top growing videos
    top_growth = video_lifecycle.nlargest(10, "total_views_growth")
    print(f"\n🚀 TOP GROWING VIDEOS (by % growth):")
    for _, video in top_growth.iterrows():
        print(
            f"   📈 {video['artist_name']}: {video['title'][:50]}... - {video['total_views_growth']:.1f}% growth ({video['initial_views']:,} → {video['final_views']:,} views)"
        )

    # Revenue growth leaders
    top_revenue_growth = video_lifecycle.nlargest(5, "total_revenue_growth")
    print(f"\n💰 TOP REVENUE GROWTH:")
    for _, video in top_revenue_growth.iterrows():
        print(
            f"   💵 {video['artist_name']}: ${video['initial_revenue']:.2f} → ${video['final_revenue']:.2f} ({video['total_revenue_growth']:.1f}% growth)"
        )

    print(f"\n✅ Time series analysis complete! Tables saved to time_series_tracking/")


def main():
    """Main function."""
    try:
        print_time_series_summary()
        return True
    except Exception as e:
        logger.error(f"❌ Time series analysis failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
