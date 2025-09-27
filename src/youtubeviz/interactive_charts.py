"""
Interactive and animated charts for MusicScope™

These charts use proper validation, timeouts, and interactive features
without hiding real errors.
"""

from typing import List, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .chart_contracts import CHART_SPECS, bulletproof_chart, create_interactive_plotly_config, setup_plotly_animation


@bulletproof_chart(CHART_SPECS["views_over_time"])
def create_views_over_time_animated(
    df: pd.DataFrame,
    date_col: str = "date",
    views_col: str = "views",
    artist_col: str = "artist_name",
    animate: bool = True,
) -> go.Figure:
    """
    Create animated views over time chart with proper interactivity.

    Features:
    - Animation controls (with autoplay OFF by default)
    - Fixed axis ranges to prevent jitter
    - Hover tooltips with key metrics
    - Interactive legend
    """
    # Ensure date column is datetime
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    # Sort by date for proper animation
    df = df.sort_values([date_col, artist_col])

    if animate:
        # Create animated line chart
        fig = px.line(
            df,
            x=date_col,
            y=views_col,
            color=artist_col,
            animation_frame=df[date_col].dt.strftime("%Y-%m-%d"),
            hover_data={artist_col: True, views_col: ":,.0f", date_col: "|%Y-%m-%d"},
            title="📈 Views Over Time (Animated)",
        )

        # Fix axis ranges to prevent jumping
        fig.update_layout(
            xaxis_range=[df[date_col].min(), df[date_col].max()], yaxis_range=[0, df[views_col].max() * 1.1]
        )

        # Setup animation controls (autoplay OFF)
        setup_plotly_animation(fig, autoplay=False, frame_duration=300)

    else:
        # Create static interactive line chart
        fig = px.line(
            df,
            x=date_col,
            y=views_col,
            color=artist_col,
            hover_data={artist_col: True, views_col: ":,.0f", date_col: "|%Y-%m-%d"},
            title="📈 Views Over Time",
        )

    # Apply interactive configuration
    fig.update_layout(
        hovermode="x unified",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


@bulletproof_chart(CHART_SPECS["sentiment_analysis"])
def create_sentiment_distribution_interactive(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    sentiment_col: str = "sentiment_score",
    comment_col: str = "comment_text",
) -> go.Figure:
    """
    Create interactive sentiment distribution with brushing capability.

    Features:
    - Hover shows actual comments
    - Color - coded sentiment ranges
    - Interactive selection
    """
    # Create sentiment categories
    df = df.copy()
    df["sentiment_category"] = pd.cut(
        df[sentiment_col], bins=[-1, -0.1, 0.1, 1], labels=["Negative", "Neutral", "Positive"]
    )

    # Create scatter plot with sentiment coloring
    fig = px.scatter(
        df,
        x=artist_col,
        y=sentiment_col,
        color="sentiment_category",
        hover_data={comment_col: True, sentiment_col: ":.3f", artist_col: True},
        color_discrete_map={"Negative": "#ff4444", "Neutral": "#ffaa00", "Positive": "#44ff44"},
        title="🎭 Sentiment Distribution by Artist",
    )

    # Add horizontal reference lines
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_hline(y=0.1, line_dash="dot", line_color="green", opacity=0.3)
    fig.add_hline(y=-0.1, line_dash="dot", line_color="red", opacity=0.3)

    # Update layout for better interactivity
    fig.update_layout(yaxis_title="Sentiment Score", xaxis_title="Artist", hovermode="closest", showlegend=True)

    return fig


@bulletproof_chart(CHART_SPECS["engagement_metrics"])
def create_engagement_bubble_chart(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    views_col: str = "views",
    likes_col: str = "likes",
    comments_col: str = "comments",
) -> go.Figure:
    """
    Create interactive bubble chart showing engagement metrics.

    Features:
    - Bubble size represents total engagement
    - Color represents engagement rate
    - Hover shows detailed metrics
    """
    # Calculate engagement metrics
    df = df.copy()
    df["total_engagement"] = df[likes_col] + df[comments_col]
    df["engagement_rate"] = (df["total_engagement"] / df[views_col].clip(lower=1)) * 100

    # Aggregate by artist
    artist_metrics = (
        df.groupby(artist_col)
        .agg(
            {
                views_col: "sum",
                likes_col: "sum",
                comments_col: "sum",
                "total_engagement": "sum",
                "engagement_rate": "mean",
            }
        )
        .reset_index()
    )

    # Create bubble chart
    fig = px.scatter(
        artist_metrics,
        x=views_col,
        y="engagement_rate",
        size="total_engagement",
        color="engagement_rate",
        hover_name=artist_col,
        hover_data={views_col: ":,.0f", likes_col: ":,.0f", comments_col: ":,.0f", "engagement_rate": ":.2f%"},
        title="💝 Artist Engagement Metrics",
        color_continuous_scale="Viridis",
    )

    # Update layout
    fig.update_layout(xaxis_title="Total Views", yaxis_title="Engagement Rate (%)", xaxis_type="log", showlegend=False)

    return fig


@bulletproof_chart(CHART_SPECS["content_analysis"])
def create_content_strategy_sunburst(
    df: pd.DataFrame, artist_col: str = "artist_name", content_col: str = "content_type", views_col: str = "views"
) -> go.Figure:
    """
    Create interactive sunburst chart for content strategy analysis.

    Features:
    - Hierarchical view: Artist -> Content Type
    - Click to drill down
    - Hover shows performance metrics
    """
    # Aggregate data
    content_summary = df.groupby([artist_col, content_col]).agg({views_col: ["sum", "count", "mean"]}).round(0)

    content_summary.columns = ["total_views", "video_count", "avg_views"]
    content_summary = content_summary.reset_index()

    # Create sunburst chart
    fig = px.sunburst(
        content_summary,
        path=[artist_col, content_col],
        values="total_views",
        hover_data={"video_count": True, "avg_views": ":,.0f", "total_views": ":,.0f"},
        title="🎪 Content Strategy Analysis",
    )

    # Update layout for better interactivity
    fig.update_layout(font_size=12, showlegend=False)

    return fig


def create_chart_with_fallback(chart_func, df: pd.DataFrame, chart_name: str, **kwargs) -> Optional[go.Figure]:
    """
    Execute chart function with proper error handling and fallback.

    This function:
    - Tries to create the chart
    - Shows clear error messages if it fails
    - Returns None if chart cannot be created
    - Does NOT hide the real error
    """
    try:
        return chart_func(df, **kwargs)
    except Exception as e:
        print(f"❌ {chart_name} failed: {e}")
        print(f"   💡 Check your data and try again")
        # Re - raise the error so it's visible in the notebook
        raise
