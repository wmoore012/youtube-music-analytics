"""
Professional Scoring Analysis Module for MusicScope™ Dashboard

Integrates the advanced scoring system with notebook visualizations.
Uses REAL database data with bulletproof execution patterns.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..data_organization.scoring_engine import ScoringEngine
from ..data_organization.scoring_storage import ScoringStorage
from ..data_organization.youtube_scoring_plugins import (
    ArtistMomentumScoringPlugin,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
)
from .bulletproof import bulletproof_chart


class ScoringAnalyzer:
    """Professional scoring analysis for MusicScope™ dashboard."""

    def __init__(self):
        """Initialize scoring analyzer with real database integration."""
        self.engine = ScoringEngine(enable_storage=True)
        self.storage = ScoringStorage()

        # Register all scoring plugins
        self.plugins = {
            "momentum": ArtistMomentumScoringPlugin(),
            "engagement": EngagementScoringPlugin(),
            "growth": GrowthPotentialScoringPlugin(),
        }

        for plugin in self.plugins.values():
            self.engine.register_plugin(plugin)

    def prepare_momentum_data(self, videos_df: pd.DataFrame, metrics_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare real data for momentum scoring."""
        if videos_df.empty or metrics_df.empty:
            return pd.DataFrame()

        # Merge videos with metrics using real database data
        merged = pd.merge(videos_df, metrics_df, on="video_id", how="inner")

        # Rename columns for plugin compatibility
        column_mapping = {"channel_title": "artist_name", "published_at": "published_at", "fetched_at": "metrics_date"}

        for old_col, new_col in column_mapping.items():
            if old_col in merged.columns:
                merged[new_col] = merged[old_col]

        # Ensure required columns exist
        required_cols = [
            "artist_name",
            "video_id",
            "published_at",
            "view_count",
            "like_count",
            "comment_count",
            "channel_title",
            "metrics_date",
        ]

        missing_cols = [col for col in required_cols if col not in merged.columns]
        if missing_cols:
            print(f"⚠️  Missing columns for momentum scoring: {missing_cols}")
            return pd.DataFrame()

        return merged

    def prepare_engagement_data(
        self, videos_df: pd.DataFrame, metrics_df: pd.DataFrame, sentiment_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Prepare real data for engagement scoring."""
        if videos_df.empty or metrics_df.empty:
            return pd.DataFrame()

        # Merge videos with metrics
        merged = pd.merge(videos_df, metrics_df, on="video_id", how="inner")

        # Add sentiment data if available
        if not sentiment_df.empty:
            merged = pd.merge(merged, sentiment_df, on="video_id", how="left")
            merged["avg_sentiment"] = merged.get("avg_sentiment", 0.0).fillna(0.0)
            merged["sentiment_magnitude"] = merged.get("comment_count", 0).fillna(0)
        else:
            merged["avg_sentiment"] = 0.0
            merged["sentiment_magnitude"] = 0.0

        return merged

    def calculate_real_scores(
        self, videos_df: pd.DataFrame, metrics_df: pd.DataFrame, sentiment_df: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        """Calculate scoring results using real database data."""
        results = {}

        # Momentum scoring
        momentum_data = self.prepare_momentum_data(videos_df, metrics_df)
        if not momentum_data.empty:
            try:
                momentum_result = self.engine.execute_scoring(
                    "artist_momentum_scorer", momentum_data, store_results=True, entity_type="artist"
                )
                results["momentum"] = momentum_result.entity_scores
                print(f"✅ Momentum scoring: {len(momentum_result.entity_scores)} artists")
            except Exception as e:
                print(f"⚠️  Momentum scoring failed: {e}")
                results["momentum"] = pd.DataFrame()

        # Engagement scoring
        engagement_data = self.prepare_engagement_data(videos_df, metrics_df, sentiment_df)
        if not engagement_data.empty:
            try:
                engagement_result = self.engine.execute_scoring(
                    "engagement_scorer", engagement_data, store_results=True, entity_type="video"
                )
                results["engagement"] = engagement_result.entity_scores
                print(f"✅ Engagement scoring: {len(engagement_result.entity_scores)} videos")
            except Exception as e:
                print(f"⚠️  Engagement scoring failed: {e}")
                results["engagement"] = pd.DataFrame()

        return results


# Chart Functions with Bulletproof Execution


@bulletproof_chart("Artist Momentum Scores", ["entity_id", "score_value", "confidence"], timeout_sec=10.0)
def create_momentum_scores_chart(df: pd.DataFrame) -> go.Figure:
    """Create professional momentum scores visualization."""
    if df.empty:
        return go.Figure().add_annotation(text="No momentum data available", x=0.5, y=0.5, showarrow=False)

    # Sort by score for better visualization
    df_sorted = df.sort_values("score_value", ascending=True)

    # Create horizontal bar chart
    fig = go.Figure()

    # Color mapping based on momentum category
    colors = []
    for _, row in df_sorted.iterrows():
        category = row.get("momentum_category", "stable")
        if category == "high_momentum":
            colors.append("#00CC96")  # Green
        elif category == "moderate_momentum":
            colors.append("#FFA15A")  # Orange
        elif category == "stable":
            colors.append("#19D3F3")  # Blue
        elif category == "low_momentum":
            colors.append("#FECB52")  # Yellow
        else:  # declining
            colors.append("#EF553B")  # Red

    fig.add_trace(
        go.Bar(
            y=df_sorted["entity_id"],
            x=df_sorted["score_value"],
            orientation="h",
            marker_color=colors,
            text=[f"{score:.3f}" for score in df_sorted["score_value"]],
            textposition="auto",
            hovertemplate="<b>%{y}</b><br>"
            + "Momentum Score: %{x:.3f}<br>"
            + "Confidence: %{customdata:.3f}<extra></extra>",
            customdata=df_sorted["confidence"],
        )
    )

    fig.update_layout(
        title={
            "text": "🚀 Artist Momentum Scores (Real Database Data)",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 18, "family": "Arial Black"},
        },
        xaxis_title="Momentum Score",
        yaxis_title="Artist",
        height=max(400, len(df_sorted) * 40),
        template="plotly_white",
        showlegend=False,
    )

    return fig


@bulletproof_chart("Engagement Score Distribution", ["entity_id", "score_value", "engagement_rate"], timeout_sec=10.0)
def create_engagement_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """Create professional engagement score distribution."""
    if df.empty:
        return go.Figure().add_annotation(text="No engagement data available", x=0.5, y=0.5, showarrow=False)

    # Create scatter plot of engagement rate vs score
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["engagement_rate"],
            y=df["score_value"],
            mode="markers",
            marker=dict(
                size=10,
                color=df["score_value"],
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title="Engagement Score"),
            ),
            text=df["entity_id"],
            hovertemplate="<b>%{text}</b><br>" + "Engagement Rate: %{x:.6f}<br>" + "Score: %{y:.3f}<extra></extra>",
        )
    )

    fig.update_layout(
        title={
            "text": "📊 Video Engagement Analysis (Real Database Data)",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 18, "family": "Arial Black"},
        },
        xaxis_title="Engagement Rate (likes + comments / views)",
        yaxis_title="Engagement Score",
        template="plotly_white",
        height=500,
    )

    return fig


@bulletproof_chart(
    "Scoring System Performance", ["algorithm_name", "total_runs", "overall_avg_score"], timeout_sec=10.0
)
def create_scoring_performance_chart(df: pd.DataFrame) -> go.Figure:
    """Create scoring system performance dashboard."""
    if df.empty:
        return go.Figure().add_annotation(text="No performance data available", x=0.5, y=0.5, showarrow=False)

    # Create subplots
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=("Algorithm Usage", "Average Scores", "Total Results", "Performance Trends"),
        specs=[[{"type": "bar"}, {"type": "bar"}], [{"type": "bar"}, {"type": "scatter"}]],
    )

    # Algorithm usage (total runs)
    fig.add_trace(
        go.Bar(x=df["algorithm_name"], y=df["total_runs"], name="Total Runs", marker_color="#636EFA"), row=1, col=1
    )

    # Average scores
    fig.add_trace(
        go.Bar(x=df["algorithm_name"], y=df["overall_avg_score"], name="Avg Score", marker_color="#EF553B"),
        row=1,
        col=2,
    )

    # Total results
    fig.add_trace(
        go.Bar(x=df["algorithm_name"], y=df["total_results"], name="Total Results", marker_color="#00CC96"),
        row=2,
        col=1,
    )

    # Performance trend (if we have timestamps)
    if "last_run" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["last_run"],
                y=df["overall_avg_score"],
                mode="markers+lines",
                name="Score Trend",
                marker_color="#AB63FA",
            ),
            row=2,
            col=2,
        )

    fig.update_layout(
        title={
            "text": "⚡ Scoring System Performance Dashboard",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 18, "family": "Arial Black"},
        },
        height=600,
        template="plotly_white",
        showlegend=False,
    )

    return fig


@bulletproof_chart("Artist Score Comparison", ["entity_id", "score_value"], timeout_sec=10.0)
def create_artist_score_radar(df: pd.DataFrame) -> go.Figure:
    """Create radar chart comparing artist scores across different metrics."""
    if df.empty or len(df) == 0:
        return go.Figure().add_annotation(text="No artist data available", x=0.5, y=0.5, showarrow=False)

    # Take top 5 artists for radar chart
    top_artists = df.nlargest(5, "score_value")

    fig = go.Figure()

    # Create radar chart for each artist
    for _, artist in top_artists.iterrows():
        # Extract available metrics
        metrics = []
        values = []

        for col in ["score_value", "confidence", "view_growth_rate", "engagement_rate", "posting_consistency"]:
            if col in artist and pd.notna(artist[col]):
                metrics.append(col.replace("_", " ").title())
                values.append(float(artist[col]))

        if metrics:
            fig.add_trace(go.Scatterpolar(r=values, theta=metrics, fill="toself", name=str(artist["entity_id"])))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title={
            "text": "🎯 Artist Performance Radar (Top 5)",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 18, "family": "Arial Black"},
        },
        template="plotly_white",
        height=500,
    )

    return fig


def get_scoring_insights(momentum_df: pd.DataFrame, engagement_df: pd.DataFrame) -> Dict[str, any]:
    """Generate professional insights from scoring analysis."""
    insights = {"momentum_insights": {}, "engagement_insights": {}, "recommendations": []}

    if not momentum_df.empty:
        # Momentum insights
        top_momentum = momentum_df.nlargest(1, "score_value")
        avg_momentum = momentum_df["score_value"].mean()

        insights["momentum_insights"] = {
            "top_artist": top_momentum.iloc[0]["entity_id"] if len(top_momentum) > 0 else "N/A",
            "top_score": top_momentum.iloc[0]["score_value"] if len(top_momentum) > 0 else 0,
            "average_score": avg_momentum,
            "total_artists": len(momentum_df),
        }

        # High momentum artists
        high_momentum = momentum_df[momentum_df.get("momentum_category", "") == "high_momentum"]
        if not high_momentum.empty:
            insights["recommendations"].append(
                f"🚀 {len(high_momentum)} artists showing high momentum - consider increased investment"
            )

    if not engagement_df.empty:
        # Engagement insights
        top_engagement = engagement_df.nlargest(1, "score_value")
        avg_engagement = engagement_df["score_value"].mean()

        insights["engagement_insights"] = {
            "top_video": top_engagement.iloc[0]["entity_id"] if len(top_engagement) > 0 else "N/A",
            "top_score": top_engagement.iloc[0]["score_value"] if len(top_engagement) > 0 else 0,
            "average_score": avg_engagement,
            "total_videos": len(engagement_df),
        }

        # High engagement videos
        high_engagement = engagement_df[engagement_df["score_value"] > 0.8]
        if not high_engagement.empty:
            insights["recommendations"].append(
                f"📊 {len(high_engagement)} videos with exceptional engagement - analyze for patterns"
            )

    return insights


# Export functions for notebook use
__all__ = [
    "ScoringAnalyzer",
    "create_momentum_scores_chart",
    "create_engagement_distribution_chart",
    "create_scoring_performance_chart",
    "create_artist_score_radar",
    "get_scoring_insights",
]
