#!/usr/bin/env python3
"""
Simple Momentum Visualizations

Creates basic momentum trend charts without complex dependencies.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from web.etl_helpers import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_momentum_data():
    """Load data for momentum analysis."""
    engine = get_engine()

    query = """
    SELECT
        yv.video_id,
        yv.title,
        yv.channel_title as artist_name,
        yv.published_at,
        ym.view_count,
        ym.like_count,
        ym.comment_count,
        ym.fetched_at,
        DATEDIFF(ym.fetched_at, yv.published_at) as days_since_publish
    FROM youtube_videos yv
    JOIN youtube_metrics ym ON yv.video_id = ym.video_id
    WHERE yv.channel_title IS NOT NULL
    ORDER BY yv.published_at DESC, ym.fetched_at DESC
    """

    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} records for momentum analysis")
    return df


def calculate_simple_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate simple momentum metrics."""

    # Calculate engagement rate
    df["engagement_rate"] = (df["like_count"] + df["comment_count"]) / df["view_count"].replace(0, 1) * 100

    # Calculate views per day since publish
    df["views_per_day"] = df["view_count"] / df["days_since_publish"].replace(0, 1)

    # Simple momentum score based on engagement and velocity
    df["momentum_score"] = df["engagement_rate"] * 0.6 + (df["views_per_day"] / df["views_per_day"].max() * 100) * 0.4

    return df


def create_momentum_trends_chart(df: pd.DataFrame):
    """Create momentum trends over time."""

    # Group by artist and date
    daily_momentum = (
        df.groupby(["artist_name", df["fetched_at"].dt.date])
        .agg({"momentum_score": "mean", "view_count": "sum", "engagement_rate": "mean"})
        .reset_index()
    )

    fig = px.line(
        daily_momentum,
        x="fetched_at",
        y="momentum_score",
        color="artist_name",
        title="🚀 Artist Momentum Trends Over Time",
        labels={"fetched_at": "Date", "momentum_score": "Momentum Score", "artist_name": "Artist"},
        height=600,
    )

    fig.update_layout(xaxis_title="Date", yaxis_title="Momentum Score", hovermode="x unified")

    return fig


def create_artist_comparison_chart(df: pd.DataFrame):
    """Create artist momentum comparison."""

    # Calculate average momentum by artist
    artist_momentum = (
        df.groupby("artist_name")
        .agg({"momentum_score": "mean", "view_count": "sum", "engagement_rate": "mean", "video_id": "count"})
        .reset_index()
    )

    artist_momentum.columns = ["artist_name", "avg_momentum", "total_views", "avg_engagement", "video_count"]

    fig = px.bar(
        artist_momentum.sort_values("avg_momentum", ascending=True),
        x="avg_momentum",
        y="artist_name",
        orientation="h",
        title="📊 Artist Momentum Comparison",
        labels={"avg_momentum": "Average Momentum Score", "artist_name": "Artist"},
        height=400,
    )

    fig.update_layout(xaxis_title="Average Momentum Score", yaxis_title="Artist")

    return fig


def create_momentum_vs_views_scatter(df: pd.DataFrame):
    """Create momentum vs views scatter plot."""

    # Aggregate by video
    video_momentum = (
        df.groupby(["video_id", "artist_name", "title"])
        .agg({"momentum_score": "mean", "view_count": "max", "engagement_rate": "mean"})
        .reset_index()
    )

    fig = px.scatter(
        video_momentum,
        x="view_count",
        y="momentum_score",
        color="artist_name",
        size="engagement_rate",
        hover_data=["title"],
        title="⚡ Momentum vs Views: Performance Analysis",
        labels={"view_count": "Total Views", "momentum_score": "Momentum Score", "artist_name": "Artist"},
        height=600,
    )

    fig.update_layout(xaxis_title="Total Views", yaxis_title="Momentum Score", xaxis_type="log")

    return fig


def save_momentum_charts():
    """Save momentum charts as HTML files."""
    logger.info("📊 Creating momentum visualizations...")

    # Load and process data
    df = load_momentum_data()

    if df.empty:
        logger.error("No data available for momentum analysis")
        return False

    df = calculate_simple_momentum(df)

    # Create output directory
    output_dir = "notebooks/momentum_charts"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Create and save charts
        logger.info("   - Creating momentum trends chart...")
        trends_fig = create_momentum_trends_chart(df)
        trends_fig.write_html(f"{output_dir}/momentum_trends.html")

        logger.info("   - Creating artist comparison chart...")
        comparison_fig = create_artist_comparison_chart(df)
        comparison_fig.write_html(f"{output_dir}/artist_momentum_comparison.html")

        logger.info("   - Creating momentum vs views scatter...")
        scatter_fig = create_momentum_vs_views_scatter(df)
        scatter_fig.write_html(f"{output_dir}/momentum_vs_views.html")

        logger.info(f"✅ Momentum charts saved to {output_dir}/")

        # Print summary stats
        logger.info("📈 Momentum Analysis Summary:")
        artist_stats = df.groupby("artist_name")["momentum_score"].agg(["mean", "std", "count"])
        for artist, stats in artist_stats.iterrows():
            logger.info(f"   🎤 {artist}: {stats['mean']:.1f} ± {stats['std']:.1f} ({stats['count']} records)")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to create charts: {e}")
        return False


def main():
    """Main function."""
    try:
        success = save_momentum_charts()
        if success:
            logger.info("🎉 Momentum visualizations created successfully!")
            return True
        else:
            logger.error("❌ Failed to create momentum visualizations")
            return False

    except Exception as e:
        logger.error(f"❌ Momentum visualization creation failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
