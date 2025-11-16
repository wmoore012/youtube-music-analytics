#!/usr/bin/env python3
"""Generate updated data for presentation slides.

This script generates:
1. Updated KPI metrics (JSON)
2. Current artist momentum scores (CSV)
3. Momentum time-series data for charts (CSV)
4. Summary statistics for context slides (JSON)

Output files can be used to update presentation slides manually or programmatically.
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.youtubeviz.data_discovery import discover_data
from src.youtubeviz.advanced_charts import compute_breakout_kpi_numbers, calculate_momentum_index


def main():
    """Generate updated presentation data files."""
    print("🔍 Loading current data...")
    
    # Load data
    data = discover_data(min_videos=5, limit_per_artist=10000)
    videos_df = data.get("videos")
    
    if videos_df is None or videos_df.empty:
        print("❌ No video data available. Run ETL first.")
        return 1
    
    print(f"✅ Loaded {len(videos_df)} videos from {videos_df['artist_name'].nunique()} artists")
    
    # Create output directory
    output_dir = project_root / "reports" / "presentation_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Calculate KPI metrics
    print("\n📊 Calculating KPI metrics...")
    kpis = compute_breakout_kpi_numbers(videos_df, artist_col="artist_name", breakout_threshold=75.0)
    
    kpi_output = {
        "generated_at": datetime.now().isoformat(),
        "breakout_threshold": 75.0,
        "avg_breakout_weeks": round(kpis["avg_breakout_weeks"], 1),
        "avg_breakout_days": round(kpis["avg_breakout_days"], 1),
        "avg_build_weeks": round(kpis["avg_build_weeks"], 1),
        "avg_build_days": round(kpis["avg_build_days"], 1),
        "n_artists": kpis["n_artists"],
    }
    
    kpi_file = output_dir / "kpi_metrics.json"
    with open(kpi_file, "w") as f:
        json.dump(kpi_output, f, indent=2)
    print(f"   ✅ Saved KPI metrics to {kpi_file}")
    
    # 2. Calculate current momentum scores
    print("\n📈 Calculating current momentum scores...")
    momentum_df = calculate_momentum_index(videos_df, artist_col="artist_name")
    
    if not momentum_df.empty:
        # Get latest momentum score per artist
        latest_momentum = (
            momentum_df.sort_values("week_start")
            .groupby("artist_name")
            .tail(1)
            .sort_values("momentum_index", ascending=False)
        )
        
        # Add status labels
        latest_momentum["status"] = latest_momentum["momentum_index"].apply(
            lambda x: "Breakout" if x >= 75 else "Pre-breakout" if x >= 55 else "Baseline"
        )
        
        # Save to CSV
        momentum_file = output_dir / "current_momentum_scores.csv"
        latest_momentum[["artist_name", "momentum_index", "status", "week_start"]].to_csv(
            momentum_file, index=False
        )
        print(f"   ✅ Saved current momentum scores to {momentum_file}")
        
        # Count distribution
        n_breakout = (latest_momentum["momentum_index"] >= 75).sum()
        n_pre_breakout = ((latest_momentum["momentum_index"] >= 55) & (latest_momentum["momentum_index"] < 75)).sum()
        n_baseline = (latest_momentum["momentum_index"] < 55).sum()
        
        distribution = {
            "breakout_count": int(n_breakout),
            "pre_breakout_count": int(n_pre_breakout),
            "baseline_count": int(n_baseline),
            "total_artists": int(len(latest_momentum)),
        }
        
        dist_file = output_dir / "momentum_distribution.json"
        with open(dist_file, "w") as f:
            json.dump(distribution, f, indent=2)
        print(f"   ✅ Saved momentum distribution to {dist_file}")
    
    # 3. Generate time-series data for momentum chart
    print("\n📉 Generating momentum time-series data...")
    if not momentum_df.empty:
        # Last 90 days of momentum data
        recent_cutoff = momentum_df["week_start"].max() - pd.Timedelta(days=90)
        recent_momentum = momentum_df[momentum_df["week_start"] >= recent_cutoff].copy()
        
        # Pivot for easier charting
        momentum_pivot = recent_momentum.pivot(
            index="week_start", columns="artist_name", values="momentum_index"
        ).reset_index()
        
        timeseries_file = output_dir / "momentum_timeseries.csv"
        momentum_pivot.to_csv(timeseries_file, index=False)
        print(f"   ✅ Saved momentum time-series to {timeseries_file}")
    
    # 4. Generate summary statistics
    print("\n📊 Generating summary statistics...")
    summary_stats = {
        "generated_at": datetime.now().isoformat(),
        "total_videos": int(len(videos_df)),
        "total_artists": int(videos_df["artist_name"].nunique()),
        "date_range": {
            "start": videos_df["published_at"].min().isoformat(),
            "end": videos_df["published_at"].max().isoformat(),
        },
        "total_views": int(videos_df["view_count"].sum()),
        "total_likes": int(videos_df["like_count"].sum()),
        "total_comments": int(videos_df["comment_count"].sum()),
        "avg_engagement_rate": round(
            ((videos_df["like_count"] + videos_df["comment_count"]) / videos_df["view_count"] * 100).mean(), 2
        ),
    }
    
    # Per-artist summary
    artist_summary = []
    for artist in videos_df["artist_name"].unique():
        artist_df = videos_df[videos_df["artist_name"] == artist]
        artist_summary.append({
            "artist_name": artist,
            "total_videos": int(len(artist_df)),
            "total_views": int(artist_df["view_count"].sum()),
            "total_likes": int(artist_df["like_count"].sum()),
            "total_comments": int(artist_df["comment_count"].sum()),
            "avg_engagement_rate": round(
                ((artist_df["like_count"] + artist_df["comment_count"]) / artist_df["view_count"] * 100).mean(), 2
            ),
        })
    
    summary_stats["artists"] = sorted(artist_summary, key=lambda x: x["total_views"], reverse=True)
    
    summary_file = output_dir / "summary_statistics.json"
    with open(summary_file, "w") as f:
        json.dump(summary_stats, f, indent=2)
    print(f"   ✅ Saved summary statistics to {summary_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("PRESENTATION DATA GENERATED SUCCESSFULLY")
    print("="*60)
    print(f"\n📁 Output directory: {output_dir}")
    print("\nGenerated files:")
    print(f"   1. kpi_metrics.json - Updated KPI card data")
    print(f"   2. current_momentum_scores.csv - Latest momentum scores")
    print(f"   3. momentum_distribution.json - Artist distribution by status")
    print(f"   4. momentum_timeseries.csv - 90-day momentum trends")
    print(f"   5. summary_statistics.json - Overall and per-artist stats")
    
    print("\n💡 Next steps:")
    print("   - Review generated files in reports/presentation_data/")
    print("   - Update presentation slides with new metrics")
    print("   - See docs/PRESENTATION_AUDIT_AND_RECOMMENDATIONS.md for guidance")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

