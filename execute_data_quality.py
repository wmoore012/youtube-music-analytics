#!/usr/bin/env python3
"""
Execute data quality analysis directly
"""
import os
from pathlib import Path
import sys

from dotenv import load_dotenv

# Setup
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
load_dotenv()

print("🔍 Running comprehensive data quality assessment...")

import numpy as np
import pandas as pd
from scipy import stats

from web.etl_helpers import get_engine
from youtubeviz.data import load_recent_window_days, qa_nulls_and_orphans

print("✅ All imports successful!")

# Connect to database
engine = get_engine()

# Load comprehensive dataset for quality checks
print("📊 Loading data for quality assessment...")
recent = load_recent_window_days(days=90, engine=engine)

print(f"📈 Dataset loaded: {len(recent):,} records")
print(f"🎤 Artists: {recent['artist_name'].nunique()}")
print(f"🎵 Videos: {recent['video_id'].nunique()}")
print(f"📅 Date range: {recent['date'].min()} to {recent['date'].max()}")

# Show sample artists
print(f"🎤 All artists: {sorted(list(recent['artist_name'].unique()))}")

# Run built-in quality checks
print("\n🔍 Running database integrity checks...")
qa = qa_nulls_and_orphans(engine=engine)
null_isrc = qa["null_isrc"]
metrics_orphans = qa["metrics_orphans"]
videos_no_metrics = qa["videos_no_metrics"]

print(f"🔍 Missing ISRC codes: {len(null_isrc):,} records")
print(f"🔗 Orphaned metrics: {len(metrics_orphans):,} records")
print(f"📹 Videos without metrics: {len(videos_no_metrics):,} records")

# Statistical outlier detection using z-score method
print("📊 Detecting statistical outliers...")
outliers = pd.DataFrame()
if not recent.empty:
    # Calculate z-scores by artist (to account for different scales)
    g = recent.groupby("artist_name")["views"]
    z_scores = (recent["views"] - g.transform("mean")) / g.transform("std").replace(0, pd.NA)

    # Flag extreme outliers (z-score > 3 or < -3)
    outlier_mask = z_scores.abs() > 3
    outliers = recent.loc[outlier_mask, ["artist_name", "video_title", "date", "views"]].copy()
    outliers["z_score"] = z_scores.loc[outlier_mask].round(2)
    outliers = outliers.sort_values("z_score", ascending=False)

print(f"📊 Statistical outliers: {len(outliers):,} records")

# Additional quality checks
print("🎯 Running additional data quality checks...")

# Check for duplicate video entries
duplicates = recent.groupby(["video_id", "date"]).size().reset_index(name="count")
duplicates = duplicates[duplicates["count"] > 1]

# Check for impossible values
impossible_values = recent[
    (recent["views"] < 0)
    | (recent["likes"] < 0)
    | (recent["comments"] < 0)
    | (recent["likes"] > recent["views"])  # More likes than views is suspicious
]

# Check for missing critical fields
missing_critical = recent[recent["video_id"].isna() | recent["artist_name"].isna() | recent["date"].isna()]

print(f"🔄 Duplicate entries: {len(duplicates):,} date-video combinations")
print(f"❌ Impossible values: {len(impossible_values):,} records")
print(f"🚫 Missing critical fields: {len(missing_critical):,} records")

# Bot Detection Analysis
print("\n🤖 Running bot detection analysis...")

from youtubeviz.bot_detection import BotDetectionConfig, analyze_bot_patterns

# Configure bot detection
config = BotDetectionConfig()
print(f"🎯 Bot detection threshold: {config.near_dupe_threshold}")
print(f"📅 Analysis lookback: {os.getenv('BOT_DETECTION_DAYS_LOOKBACK', '30')} days")

# Load comment data for bot analysis
from sqlalchemy import text

with engine.connect() as conn:
    comments_df = pd.read_sql(
        text(
            """
        SELECT c.comment_text, c.author_name, c.published_at,
               v.channel_title as artist_name, v.video_id
        FROM youtube_comments c
        JOIN youtube_videos v ON c.video_id = v.video_id
        WHERE c.comment_text IS NOT NULL
        AND c.published_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        LIMIT 1000
    """
        ),
        conn,
    )

print(f"📊 Comments loaded for analysis: {len(comments_df):,}")

if len(comments_df) > 0:
    # Simple bot detection analysis without using the complex function
    # Check for duplicate comments
    duplicate_comments = comments_df.groupby("comment_text").size().reset_index(name="count")
    duplicate_comments = duplicate_comments[duplicate_comments["count"] > 1]

    # Check for very short comments (potential spam)
    short_comments = comments_df[comments_df["comment_text"].str.len() < 10]

    # Check for comments with excessive emojis
    emoji_heavy = comments_df[comments_df["comment_text"].str.count("🔥|💯|🌊") > 5]

    print(f"\n🤖 Bot Detection Results:")
    print(f"   Duplicate comments: {len(duplicate_comments):,} unique texts")
    print(f"   Very short comments: {len(short_comments):,} comments")
    print(f"   Emoji-heavy comments: {len(emoji_heavy):,} comments")

    # Show sample suspicious comments
    if len(duplicate_comments) > 0:
        print("\n⚠️ Sample duplicate comment texts:")
        print(duplicate_comments.head()[["comment_text", "count"]].to_string())
else:
    print("⚠️ No recent comments found for bot analysis")

# Quality Assessment Summary
print("\n📋 DATA QUALITY ASSESSMENT RESULTS:")
print("=" * 60)
print(f"🔍 Missing ISRC codes: {len(null_isrc):,} records")
print(f"🔗 Orphaned metrics: {len(metrics_orphans):,} records")
print(f"📹 Videos without metrics: {len(videos_no_metrics):,} records")
print(f"📊 Statistical outliers: {len(outliers):,} records")
print(f"🔄 Duplicate entries: {len(duplicates):,} date-video combinations")
print(f"❌ Impossible values: {len(impossible_values):,} records")
print(f"🚫 Missing critical fields: {len(missing_critical):,} records")

# Quality Score Calculation
total_records = len(recent)
quality_issues = len(outliers) + len(duplicates) + len(impossible_values) + len(missing_critical)
quality_score = max(0, (total_records - quality_issues) / total_records * 100) if total_records > 0 else 0

print(f"\n🏆 OVERALL DATA QUALITY SCORE: {quality_score:.1f}%")

if quality_score >= 95:
    print("✅ EXCELLENT - Data is highly reliable for analysis")
elif quality_score >= 90:
    print("🟢 GOOD - Minor issues, safe to proceed with analysis")
elif quality_score >= 80:
    print("🟡 FAIR - Some issues present, review before major decisions")
else:
    print("🔴 POOR - Significant data quality issues, cleanup recommended")

print("\n✅ Data quality assessment complete!")
