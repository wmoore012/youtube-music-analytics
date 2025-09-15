#!/usr/bin/env python3
"""
Create Momentum Trend Visualizations for Notebooks

This script creates interactive momentum analysis visualizations that can be
embedded in the analytics notebooks.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from web.etl_helpers import get_engine
from youtubeviz.momentum_analysis import ArtistMomentumAnalyzer

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


def create_momentum_trend_chart(momentum_df: pd.DataFrame):
    """Create momentum trend visualization."""

    # Calculate daily momentum scores
    momentum_analyzer = ArtistMomentumAnalyzer()

    # Group by artist and calculate momentum over time
    artist_momentum = []

    for artist in momentum_df["artist_name"].unique():
        if pd.isna(artist):
            continue

        artist_data = momentum_df[momentum_df["artist_name"] == artist].copy()

        # Calculate momentum score for the artist
        try:
            momentum_result = momentum_analyzer.calculate_momentum_score(artist_data)
            momentum_score = momentum_result.get("overall_momentum", 0.0)
        except Exception:
            momentum_score = 0.0

        # Assign momentum score to all rows for this artist
        artist_data["momentum_score"] = momentum_score

        # Group by date and get average momentum
        daily_momentum = (
            artist_data.groupby(artist_data["fetched_at"].dt.date)
            .agg({"momentum_score": "mean", "view_count": "sum", "like_count": "sum", "comment_count": "sum"})
            .reset_index()
        )

        daily_momentum["artist_name"] = artist
        artist_momentum.append(daily_momentum)

    if not artist_momentum:
        logger.warning("No momentum data to visualize")
        return None

    momentum_combined = pd.concat(artist_momentum, ignore_index=True)

    # Create momentum trend chart
    fig = px.line(
        momentum_combined,
        x="fetched_at",
        y="momentum_score",
        color="artist_name",
        title="🚀 Artist Momentum Trends Over Time",
        labels={"fetched_at": "Date", "momentum_score": "Momentum Score", "artist_name": "Artist"},
        height=600,
    )

    fig.update_layout(xaxis_title="Date", yaxis_title="Momentum Score", hovermode="x unified", showlegend=True)

    return fig


def create_momentum_heatmap(momentum_df: pd.DataFrame):
    """Create momentum heatmap by artist and time period."""

    momentum_analyzer = MomentumAnalyzer()

    # Calculate momentum scores
    momentum_df["momentum_score"] = momentum_df.apply(
        lambda row: momentum_analyzer.calculate_momentum_score(
            {
                "view_count": row["view_count"],
                "like_count": row["like_count"],
                "comment_count": row["comment_count"],
                "days_since_publish": row["days_since_publish"],
            }
        ),
        axis=1,
    )

    # Create time periods (weeks)
    momentum_df["week"] = momentum_df["fetched_at"].dt.to_period("W").astype(str)

    # Pivot for heatmap
    heatmap_data = momentum_df.groupby(["artist_name", "week"])["momentum_score"].mean().reset_index()
    heatmap_pivot = heatmap_data.pivot(index="artist_name", columns="week", values="momentum_score")

    # Create heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=heatmap_pivot.values,
            x=heatmap_pivot.columns,
            y=heatmap_pivot.index,
            colorscale="Viridis",
            hoverongaps=False,
        )
    )

    fig.update_layout(
        title="🔥 Momentum Heatmap: Artist Performance by Week", xaxis_title="Week", yaxis_title="Artist", height=400
    )

    return fig


def create_momentum_distribution_chart(momentum_df: pd.DataFrame):
    """Create momentum score distribution chart."""

    momentum_analyzer = MomentumAnalyzer()

    # Calculate momentum scores
    momentum_df["momentum_score"] = momentum_df.apply(
        lambda row: momentum_analyzer.calculate_momentum_score(
            {
                "view_count": row["view_count"],
                "like_count": row["like_count"],
                "comment_count": row["comment_count"],
                "days_since_publish": row["days_since_publish"],
            }
        ),
        axis=1,
    )

    # Create distribution chart
    fig = px.box(
        momentum_df,
        x="artist_name",
        y="momentum_score",
        title="📊 Momentum Score Distribution by Artist",
        labels={"artist_name": "Artist", "momentum_score": "Momentum Score"},
        height=500,
    )

    fig.update_layout(xaxis_title="Artist", yaxis_title="Momentum Score", xaxis_tickangle=-45)

    return fig


def create_momentum_vs_engagement_scatter(momentum_df: pd.DataFrame):
    """Create momentum vs engagement scatter plot."""

    momentum_analyzer = MomentumAnalyzer()

    # Calculate momentum and engagement metrics
    momentum_df["momentum_score"] = momentum_df.apply(
        lambda row: momentum_analyzer.calculate_momentum_score(
            {
                "view_count": row["view_count"],
                "like_count": row["like_count"],
                "comment_count": row["comment_count"],
                "days_since_publish": row["days_since_publish"],
            }
        ),
        axis=1,
    )

    # Calculate engagement rate
    momentum_df["engagement_rate"] = (
        (momentum_df["like_count"] + momentum_df["comment_count"]) / momentum_df["view_count"].replace(0, 1) * 100
    )

    # Create scatter plot
    fig = px.scatter(
        momentum_df,
        x="engagement_rate",
        y="momentum_score",
        color="artist_name",
        size="view_count",
        hover_data=["title", "published_at"],
        title="⚡ Momentum vs Engagement: Finding the Sweet Spot",
        labels={"engagement_rate": "Engagement Rate (%)", "momentum_score": "Momentum Score", "artist_name": "Artist"},
        height=600,
    )

    fig.update_layout(xaxis_title="Engagement Rate (%)", yaxis_title="Momentum Score")

    return fig


def create_momentum_dashboard():
    """Create comprehensive momentum dashboard."""

    # Load data
    momentum_df = load_momentum_data()

    if momentum_df.empty:
        logger.error("No data available for momentum analysis")
        return None

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=[
            "🚀 Momentum Trends",
            "📊 Score Distribution",
            "🔥 Weekly Heatmap",
            "⚡ Momentum vs Engagement",
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}], [{"secondary_y": False}, {"secondary_y": False}]],
    )

    # Add momentum trend (simplified for subplot)
    momentum_analyzer = MomentumAnalyzer()

    for i, artist in enumerate(momentum_df["artist_name"].unique()[:6]):  # Limit to 6 artists
        if pd.isna(artist):
            continue

        artist_data = momentum_df[momentum_df["artist_name"] == artist].copy()

        # Calculate momentum scores
        artist_data["momentum_score"] = artist_data.apply(
            lambda row: momentum_analyzer.calculate_momentum_score(
                {
                    "view_count": row["view_count"],
                    "like_count": row["like_count"],
                    "comment_count": row["comment_count"],
                    "days_since_publish": row["days_since_publish"],
                }
            ),
            axis=1,
        )

        # Daily average
        daily_momentum = artist_data.groupby(artist_data["fetched_at"].dt.date)["momentum_score"].mean().reset_index()

        fig.add_trace(
            go.Scatter(
                x=daily_momentum["fetched_at"], y=daily_momentum["momentum_score"], name=artist, mode="lines+markers"
            ),
            row=1,
            col=1,
        )

    fig.update_layout(height=800, title_text="🎵 Comprehensive Momentum Analysis Dashboard", showlegend=True)

    return fig


def save_momentum_visualizations():
    """Save momentum visualizations as HTML files."""
    logger.info("📊 Creating momentum visualizations...")

    # Load data
    momentum_df = load_momentum_data()

    if momentum_df.empty:
        logger.error("No data available for momentum analysis")
        return False

    # Create output directory
    output_dir = "notebooks/momentum_charts"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Create and save individual charts
        logger.info("   - Creating momentum trend chart...")
        trend_fig = create_momentum_trend_chart(momentum_df)
        if trend_fig:
            trend_fig.write_html(f"{output_dir}/momentum_trends.html")

        logger.info("   - Creating momentum heatmap...")
        heatmap_fig = create_momentum_heatmap(momentum_df)
        if heatmap_fig:
            heatmap_fig.write_html(f"{output_dir}/momentum_heatmap.html")

        logger.info("   - Creating distribution chart...")
        dist_fig = create_momentum_distribution_chart(momentum_df)
        if dist_fig:
            dist_fig.write_html(f"{output_dir}/momentum_distribution.html")

        logger.info("   - Creating scatter plot...")
        scatter_fig = create_momentum_vs_engagement_scatter(momentum_df)
        if scatter_fig:
            scatter_fig.write_html(f"{output_dir}/momentum_vs_engagement.html")

        logger.info("   - Creating comprehensive dashboard...")
        dashboard_fig = create_momentum_dashboard()
        if dashboard_fig:
            dashboard_fig.write_html(f"{output_dir}/momentum_dashboard.html")

        logger.info(f"✅ Momentum visualizations saved to {output_dir}/")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to create visualizations: {e}")
        return False


def main():
    """Main function."""
    try:
        success = save_momentum_visualizations()
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
