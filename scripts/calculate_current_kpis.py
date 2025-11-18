#!/usr/bin/env python3
"""Calculate current KPI metrics for presentation update.

This script calculates the current values for:
- Average weeks in breakout (momentum >= 75)
- Average days of pre-warning (before crossing threshold)
- Number of artists currently at/above breakout threshold
- Current momentum scores for all artists

These metrics are used in the presentation slides.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.youtubeviz.data_discovery import discover_data
from src.youtubeviz.advanced_charts import compute_breakout_kpi_numbers, calculate_momentum_index


def main():
    """Calculate and display current KPI metrics."""
    print("🔍 Loading current data...")

    # Load data
    data = discover_data(min_videos=5, limit_per_artist=10000)
    videos_df = data.get("videos")
    
    if videos_df is None or videos_df.empty:
        print("❌ No video data available. Run ETL first.")
        return 1
    
    print(f"✅ Loaded {len(videos_df)} videos")
    print(f"   Artists: {videos_df['artist_name'].nunique()}")
    print(f"   Date range: {videos_df['published_at'].min()} to {videos_df['published_at'].max()}")
    
    # Calculate KPI metrics with threshold of 75
    print("\n📊 Calculating KPI metrics (breakout threshold = 75)...")
    kpis = compute_breakout_kpi_numbers(videos_df, artist_col="artist_name", breakout_threshold=75.0)
    
    print("\n" + "="*60)
    print("CURRENT KPI METRICS FOR PRESENTATION")
    print("="*60)
    
    print(f"\n🎯 Breakout Performance (Threshold = 75):")
    print(f"   Average time IN breakout:     {kpis['avg_breakout_weeks']:.1f} weeks ({kpis['avg_breakout_days']:.1f} days)")
    print(f"   Average pre-warning window:   {kpis['avg_build_weeks']:.1f} weeks ({kpis['avg_build_days']:.1f} days)")
    print(f"   Number of artists analyzed:   {kpis['n_artists']}")
    
    # Calculate current momentum scores
    print("\n📈 Current Momentum Scores by Artist:")
    momentum_df = calculate_momentum_index(videos_df, artist_col="artist_name")
    
    if not momentum_df.empty:
        # Get latest momentum score per artist
        latest_momentum = (
            momentum_df.sort_values("week_start")
            .groupby("artist_name")
            .tail(1)
            .sort_values("momentum_index", ascending=False)
        )
        
        for _, row in latest_momentum.iterrows():
            artist = row["artist_name"]
            score = row["momentum_index"]
            status = "🔥 BREAKOUT" if score >= 75 else "📊 Building" if score >= 55 else "⏳ Baseline"
            print(f"   {artist:20s}: {score:5.1f}  {status}")
        
        # Count artists at different thresholds
        n_breakout = (latest_momentum["momentum_index"] >= 75).sum()
        n_pre_breakout = ((latest_momentum["momentum_index"] >= 55) & (latest_momentum["momentum_index"] < 75)).sum()
        n_baseline = (latest_momentum["momentum_index"] < 55).sum()
        
        print(f"\n📊 Current Distribution:")
        print(f"   Breakout (≥75):      {n_breakout} artists")
        print(f"   Pre-breakout (55-74): {n_pre_breakout} artists")
        print(f"   Baseline (<55):       {n_baseline} artists")
    
    # Compare to presentation values
    print("\n" + "="*60)
    print("COMPARISON TO PRESENTATION SLIDES")
    print("="*60)
    print("\n📸 Slide values (from images):")
    print("   Time in breakout:     1.0 weeks")
    print("   Pre-warning window:   2.1 days")
    print("   Breakout threshold:   75")
    print("   Artists above threshold: 0 / 6")
    
    print("\n🔄 Current values:")
    print(f"   Time in breakout:     {kpis['avg_breakout_weeks']:.1f} weeks")
    print(f"   Pre-warning window:   {kpis['avg_build_days']:.1f} days")
    print("   Breakout threshold:   75")
    if not momentum_df.empty:
        print(f"   Artists above threshold: {n_breakout} / {kpis['n_artists']}")
    
    # Determine if update is needed
    weeks_diff = abs(kpis['avg_breakout_weeks'] - 1.0)
    days_diff = abs(kpis['avg_build_days'] - 2.1)
    
    print("\n" + "="*60)
    if weeks_diff > 0.2 or days_diff > 0.5:
        print("⚠️  METRICS HAVE CHANGED SIGNIFICANTLY")
        print("    Presentation slides should be updated with current data.")
    else:
        print("✅ Metrics are relatively stable")
        print("   Presentation slides are still accurate.")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

