#!/usr/bin/env python3
"""
Execute music-focused analytics
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Setup
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
load_dotenv()

print("🎵 Running Music-Focused Analytics...")

from datetime import datetime, timedelta

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
print("📊 Loading music data...")
recent = load_recent_window_days(days=90, engine=engine)

print(f"📈 Dataset loaded: {len(recent):,} records")
print(f"🎤 Artists: {recent['artist_name'].nunique()}")
print(f"🎵 Videos: {recent['video_id'].nunique()}")
print(f"📅 Date range: {recent['date'].min()} to {recent['date'].max()}")

# Music Industry KPIs
print("\n🎵 MUSIC INDUSTRY PERFORMANCE DASHBOARD")
print("=" * 60)

# 1. Artist Portfolio Overview
kpis = compute_kpis(recent)
revenue = compute_estimated_revenue(recent, rpm_usd=2.5)

portfolio_summary = kpis.merge(revenue[["artist_name", "est_revenue_usd"]], on="artist_name")
portfolio_summary["revenue_per_video"] = (portfolio_summary["est_revenue_usd"] / portfolio_summary["videos"]).round(2)

print("🏆 Artist Portfolio Performance:")
print(
    portfolio_summary[["artist_name", "total_views", "videos", "est_revenue_usd", "revenue_per_video"]].to_string(
        index=False
    )
)

# 2. Market Share Analysis
total_views = portfolio_summary["total_views"].sum()
portfolio_summary["market_share"] = (portfolio_summary["total_views"] / total_views * 100).round(2)

print(f"\n📊 Market Share Analysis (Total Views: {total_views:,}):")
for _, artist in portfolio_summary.iterrows():
    print(f"   🎤 {artist['artist_name']}: {artist['market_share']}% ({artist['total_views']:,} views)")

# 3. Revenue Analysis
total_revenue = portfolio_summary["est_revenue_usd"].sum()
print(f"\n💰 Revenue Analysis (Total Estimated: ${total_revenue:,.2f}):")
for _, artist in portfolio_summary.iterrows():
    revenue_share = (artist["est_revenue_usd"] / total_revenue * 100) if total_revenue > 0 else 0
    print(f"   💵 {artist['artist_name']}: ${artist['est_revenue_usd']:,.2f} ({revenue_share:.1f}%)")

# 4. Content Strategy Analysis
print("\n🎬 Content Strategy Analysis:")
content_analysis = (
    recent.groupby("artist_name")
    .agg({"video_id": "nunique", "views": ["sum", "mean", "std"], "likes": "sum", "comments": "sum"})
    .round(0)
)

content_analysis.columns = ["videos", "total_views", "avg_views", "std_views", "total_likes", "total_comments"]
content_analysis["consistency_score"] = (content_analysis["avg_views"] / (content_analysis["std_views"] + 1)).round(2)

print("Content Performance Metrics:")
print(content_analysis[["videos", "avg_views", "consistency_score"]].to_string())

# 5. Viral Content Identification
print("\n🚀 Viral Content Analysis:")

# Get latest metrics for each unique video (deduplicate by video_id)
latest_metrics = recent.sort_values("date").groupby("video_id").last().reset_index()

# Define viral threshold as 3 standard deviations above mean
viral_threshold = latest_metrics["views"].mean() + (3 * latest_metrics["views"].std())
viral_content = latest_metrics[latest_metrics["views"] > viral_threshold]

print(f"Viral Threshold: {viral_threshold:,.0f} views")
print(f"Viral Videos Found: {len(viral_content)}")

if len(viral_content) > 0:
    print("\n🔥 Top Viral Videos:")
    viral_top = viral_content.nlargest(5, "views")[["artist_name", "video_title", "views", "date"]]
    for _, video in viral_top.iterrows():
        print(
            f"   🎵 {video['artist_name']}: {video['video_title'][:40]}... - {video['views']:,} views ({video['date'].strftime('%Y-%m-%d')})"
        )

# 6. Engagement Quality Analysis
print("\n💬 Engagement Quality Analysis:")


# Calculate engagement quality metrics
def calculate_engagement_metrics(group):
    return pd.Series(
        {
            "avg_like_rate": (group["likes"].sum() / group["views"].sum() * 100) if group["views"].sum() > 0 else 0,
            "avg_comment_rate": (
                (group["comments"].sum() / group["views"].sum() * 100) if group["views"].sum() > 0 else 0
            ),
            "engagement_consistency": group["likes"].std() / (group["likes"].mean() + 1) if len(group) > 1 else 0,
        }
    )


# Suppress the FutureWarning by using a different approach
import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore", FutureWarning)
    engagement_quality = recent.groupby("artist_name").apply(calculate_engagement_metrics).round(3)

print("Engagement Rates (%):")
print(engagement_quality.to_string())

# 7. Growth Momentum Analysis
print("\n📈 Growth Momentum Analysis:")
# Calculate daily growth rates
daily_metrics = recent.groupby(["artist_name", "date"])["views"].sum().reset_index()

momentum_analysis = []
for artist in daily_metrics["artist_name"].unique():
    artist_data = daily_metrics[daily_metrics["artist_name"] == artist].sort_values("date")
    if len(artist_data) > 1:
        # Calculate momentum indicators
        latest_views = artist_data["views"].iloc[-1]
        previous_views = artist_data["views"].iloc[-2] if len(artist_data) > 1 else 0
        growth_rate = ((latest_views - previous_views) / previous_views * 100) if previous_views > 0 else 0

        momentum_analysis.append(
            {
                "artist_name": artist,
                "latest_daily_views": latest_views,
                "growth_rate": round(growth_rate, 2),
                "momentum_score": "High" if growth_rate > 10 else "Medium" if growth_rate > 0 else "Low",
            }
        )

if momentum_analysis:
    momentum_df = pd.DataFrame(momentum_analysis).sort_values("growth_rate", ascending=False)
    print("Artist Momentum Rankings:")
    print(momentum_df.to_string(index=False))

# 8. Investment Recommendations
print("\n💡 INVESTMENT RECOMMENDATIONS:")
print("=" * 60)

# Combine all metrics for investment scoring
investment_scores = []
for artist in portfolio_summary["artist_name"]:
    artist_data = portfolio_summary[portfolio_summary["artist_name"] == artist].iloc[0]

    # Scoring factors (0-100 scale)
    view_score = min(100, (artist_data["total_views"] / portfolio_summary["total_views"].max()) * 100)
    revenue_score = min(100, (artist_data["est_revenue_usd"] / portfolio_summary["est_revenue_usd"].max()) * 100)
    efficiency_score = min(100, (artist_data["revenue_per_video"] / portfolio_summary["revenue_per_video"].max()) * 100)

    # Get engagement score
    eng_data = engagement_quality.loc[artist]
    engagement_score = min(100, (eng_data["avg_like_rate"] + eng_data["avg_comment_rate"]) * 10)

    # Overall investment score
    overall_score = view_score * 0.3 + revenue_score * 0.3 + efficiency_score * 0.2 + engagement_score * 0.2

    investment_scores.append(
        {
            "artist_name": artist,
            "investment_score": round(overall_score, 1),
            "recommendation": (
                "HIGH PRIORITY" if overall_score > 70 else "MEDIUM PRIORITY" if overall_score > 40 else "MONITOR"
            ),
        }
    )

investment_df = pd.DataFrame(investment_scores).sort_values("investment_score", ascending=False)

print("Investment Priority Rankings:")
for _, artist in investment_df.iterrows():
    print(f"   {artist['recommendation']:15} | {artist['artist_name']:15} | Score: {artist['investment_score']}")

# 9. Market Opportunities
print("\n🎯 MARKET OPPORTUNITIES:")
print("=" * 60)

# Identify underperforming artists with potential
underperformers = portfolio_summary[portfolio_summary["market_share"] < 5]  # Less than 5% market share
high_engagement = engagement_quality[engagement_quality["avg_like_rate"] > 1.5]  # Above average engagement

opportunities = []
for artist in underperformers["artist_name"]:
    if artist in high_engagement.index:
        artist_revenue = portfolio_summary[portfolio_summary["artist_name"] == artist]["est_revenue_usd"].iloc[0]
        artist_engagement = high_engagement.loc[artist]["avg_like_rate"]

        opportunities.append(
            {
                "artist_name": artist,
                "current_revenue": artist_revenue,
                "engagement_rate": artist_engagement,
                "opportunity_type": "High Engagement, Low Revenue - Growth Potential",
            }
        )

if opportunities:
    print("Growth Opportunities Identified:")
    for opp in opportunities:
        print(f"   🚀 {opp['artist_name']}: {opp['opportunity_type']}")
        print(f"      Current Revenue: ${opp['current_revenue']:,.2f} | Engagement: {opp['engagement_rate']:.2f}%")
else:
    print("   📊 All artists performing within expected ranges")

# 10. Executive Summary
print("\n📋 EXECUTIVE SUMMARY:")
print("=" * 60)

top_performer = portfolio_summary.iloc[0]
total_portfolio_value = portfolio_summary["est_revenue_usd"].sum()
avg_engagement = engagement_quality["avg_like_rate"].mean()

print(f"🏆 Portfolio Leader: {top_performer['artist_name']}")
print(f"💰 Total Portfolio Value: ${total_portfolio_value:,.2f}")
print(f"📊 Average Engagement Rate: {avg_engagement:.2f}%")
print(f"🎵 Total Content Library: {portfolio_summary['videos'].sum()} videos")
print(f"👀 Total Portfolio Views: {portfolio_summary['total_views'].sum():,}")

# Key recommendations
print(f"\n🎯 KEY RECOMMENDATIONS:")
print(f"   1. Focus marketing budget on {investment_df.iloc[0]['artist_name']} (highest investment score)")
print(f"   2. Monitor {len(viral_content)} viral videos for replication strategies")
print(f"   3. Average engagement rate of {avg_engagement:.2f}% indicates healthy fan engagement")

if len(opportunities) > 0:
    print(f"   4. Develop growth strategies for {len(opportunities)} high-potential artists")

print("\n✅ Music-focused analytics complete!")
