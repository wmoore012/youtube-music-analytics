#!/usr/bin/env python3
"""
Execute artist comparison analysis directly
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Setup
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
load_dotenv()

print("🎤 Running Artist Comparison Analysis...")

import numpy as np
import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine
from youtubeviz.data import (
    compute_estimated_revenue,
    compute_kpis,
    load_recent_window_days,
)

print("✅ All imports successful!")

# Connect to database
engine = get_engine()

# Load comprehensive dataset
print("📊 Loading artist data...")
recent = load_recent_window_days(days=90, engine=engine)

# Deduplicate by video_id to get latest metrics for each video
print("🔄 Deduplicating data...")
recent = recent.sort_values("date").groupby("video_id").last().reset_index()

print(f"📈 Dataset loaded: {len(recent):,} records")
print(f"🎤 Artists: {recent['artist_name'].nunique()}")
print(f"🎵 Videos: {recent['video_id'].nunique()}")
print(f"📅 Date range: {recent['date'].min()} to {recent['date'].max()}")

# Compute KPIs per artist
print("\n📊 Computing artist KPIs...")
kpis = compute_kpis(recent)
print("Artist Performance Overview:")
print(kpis.to_string(index=False))

# Compute estimated revenue
print("\n💰 Computing estimated revenue...")
revenue = compute_estimated_revenue(recent, rpm_usd=2.5)
print("Estimated Revenue (USD):")
print(revenue[["artist_name", "total_views", "est_revenue_usd", "videos"]].to_string(index=False))

# Artist comparison metrics
print("\n🏆 Artist Comparison Metrics:")
print("=" * 60)

# Views per video analysis
views_per_video = recent.groupby(["artist_name", "video_id"])["views"].max().reset_index()
avg_views_per_video = views_per_video.groupby("artist_name")["views"].agg(["mean", "median", "std"]).round(0)
avg_views_per_video.columns = ["avg_views_per_video", "median_views_per_video", "std_views_per_video"]

print("Views per Video Statistics:")
print(avg_views_per_video.to_string())

# Engagement rate analysis (likes/views ratio)
print("\n📈 Engagement Analysis:")
engagement = recent.groupby("artist_name").agg({"views": "sum", "likes": "sum", "comments": "sum"}).reset_index()

engagement["like_rate"] = (engagement["likes"] / engagement["views"] * 100).round(2)
engagement["comment_rate"] = (engagement["comments"] / engagement["views"] * 100).round(2)

print("Engagement Rates (%):")
print(engagement[["artist_name", "like_rate", "comment_rate"]].to_string(index=False))

# Growth trend analysis
print("\n📈 Growth Trend Analysis:")
daily_views = recent.groupby(["artist_name", "date"])["views"].sum().reset_index()

growth_trends = []
for artist in daily_views["artist_name"].unique():
    artist_data = daily_views[daily_views["artist_name"] == artist].sort_values("date")
    if len(artist_data) > 1:
        # Calculate daily growth rate
        artist_data["views_change"] = artist_data["views"].pct_change()
        avg_growth = artist_data["views_change"].mean() * 100
        growth_trends.append(
            {"artist_name": artist, "avg_daily_growth_rate": round(avg_growth, 2), "total_days": len(artist_data)}
        )

growth_df = pd.DataFrame(growth_trends)
if not growth_df.empty:
    print("Average Daily Growth Rates (%):")
    print(growth_df.to_string(index=False))

# Content volume analysis
print("\n🎵 Content Volume Analysis:")
content_volume = recent.groupby("artist_name").agg({"video_id": "nunique", "date": ["min", "max"]}).round(2)

content_volume.columns = ["unique_videos", "first_date", "last_date"]
content_volume["days_active"] = (
    pd.to_datetime(content_volume["last_date"]) - pd.to_datetime(content_volume["first_date"])
).dt.days + 1
content_volume["videos_per_day"] = (content_volume["unique_videos"] / content_volume["days_active"]).round(2)

print("Content Production Metrics:")
print(content_volume[["unique_videos", "days_active", "videos_per_day"]].to_string())

# Top performing videos per artist
print("\n🏆 Top Performing Videos by Artist:")
print("=" * 60)

top_videos = recent.groupby(["artist_name", "video_id", "video_title"])["views"].max().reset_index()
for artist in top_videos["artist_name"].unique():
    artist_videos = top_videos[top_videos["artist_name"] == artist].nlargest(3, "views")
    print(f"\n🎤 {artist}:")
    for _, video in artist_videos.iterrows():
        print(f"   📹 {video['video_title'][:50]}... - {video['views']:,} views")

# Artist ranking summary
print("\n🏆 ARTIST RANKING SUMMARY:")
print("=" * 60)

# Create comprehensive ranking
ranking_data = []
for artist in recent["artist_name"].unique():
    artist_data = recent[recent["artist_name"] == artist]

    total_views = artist_data["views"].sum()
    total_videos = artist_data["video_id"].nunique()
    avg_views_per_video = total_views / total_videos if total_videos > 0 else 0
    total_likes = artist_data["likes"].sum()
    total_comments = artist_data["comments"].sum()

    ranking_data.append(
        {
            "artist_name": artist,
            "total_views": total_views,
            "total_videos": total_videos,
            "avg_views_per_video": round(avg_views_per_video, 0),
            "total_likes": total_likes,
            "total_comments": total_comments,
            "engagement_score": round((total_likes + total_comments) / total_views * 100, 2) if total_views > 0 else 0,
        }
    )

ranking_df = pd.DataFrame(ranking_data).sort_values("total_views", ascending=False)
ranking_df["rank"] = range(1, len(ranking_df) + 1)

print("Final Artist Rankings:")
print(
    ranking_df[
        ["rank", "artist_name", "total_views", "total_videos", "avg_views_per_video", "engagement_score"]
    ].to_string(index=False)
)

# Key insights
print("\n💡 KEY INSIGHTS:")
print("=" * 60)

top_artist = ranking_df.iloc[0]
print(f"🥇 Top Performer: {top_artist['artist_name']} with {top_artist['total_views']:,} total views")

most_prolific = ranking_df.loc[ranking_df["total_videos"].idxmax()]
print(f"🎵 Most Prolific: {most_prolific['artist_name']} with {most_prolific['total_videos']} videos")

highest_engagement = ranking_df.loc[ranking_df["engagement_score"].idxmax()]
print(
    f"💬 Highest Engagement: {highest_engagement['artist_name']} with {highest_engagement['engagement_score']}% engagement rate"
)

best_avg_performance = ranking_df.loc[ranking_df["avg_views_per_video"].idxmax()]
print(
    f"⭐ Best Average Performance: {best_avg_performance['artist_name']} with {best_avg_performance['avg_views_per_video']:,.0f} avg views per video"
)

print("\n✅ Artist comparison analysis complete!")
