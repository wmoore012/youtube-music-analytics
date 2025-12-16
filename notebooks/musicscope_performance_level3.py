"""
MusicScope™ Level 3 - Performance Intelligence Section

Renders the complete Performance Intelligence section with Level 3 business framing.
Uses hybrid architecture: design system for UI + existing helpers for calculations.

Expected Charts:
    - 4 charts total (always rendered)
    - Engagement matrix scatter (Hidden Gems quadrant)
    - Content leaderboard (top 10 by weighted engagement)
    - Artist-level performance (total engagement by artist)
    - Engagement efficiency distribution (box plot)

Usage:
    # Production mode (strict validation)
    from musicscope_performance_level3 import render_performance_section
    meta = render_performance_section(videos_df, comments_df, demo=False)

    # Demo mode (synthetic data for testing)
    meta = render_performance_section(videos_df, comments_df, demo=True)
"""

from __future__ import annotations
from typing import Optional, Dict, Any
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
from IPython.display import display, HTML

# Design System imports
from musicscope_design_system import (
    ms_hero_card, ms_subsection_card, ms_insight_card, ms_closing_card,
    ms_apply_plotly_layout, ms_matplotlib_title, ms_clean_spines,
    ms_require_data, MUSICSCOPE_COLORS, CHART_PURPOSE
)

# Existing helper imports (REUSE, don't reimplement!)
from tools.advanced_charts import ensure_year_on_dates, label_bars, base100
from tools.data_utils import resolve_artist_column, pick_content_column


# Expected chart counts for this section
EXPECTED_CHARTS = {
    "min": 4,
    "max": 4,
    "description": "4 core charts (matrix + leaderboard + artist summary + efficiency)"
}


def _build_demo_videos_df() -> pd.DataFrame:
    """
    Private helper: Build synthetic videos DataFrame for testing/demo only.

    Returns:
        DataFrame with minimal required columns for performance analysis
    """
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    return pd.DataFrame({
        'artist_name': ['Artist A'] * 15 + ['Artist B'] * 15,
        'view_count': np.random.randint(1000, 10000, 30),
        'published_at': dates,
        'video_id': [f'video_{i}' for i in range(30)],
        'likes': np.random.randint(50, 500, 30),
        'comments': np.random.randint(5, 50, 30),
        'title': [f'Video Title {i}' for i in range(30)]
    })


def render_performance_section(
    videos_df: pd.DataFrame,
    comments_df: Optional[pd.DataFrame] = None,
    demo: bool = False
) -> Dict[str, Any]:
    """
    Render the complete Performance Intelligence section with Level 3 framing.

    Parameters:
        videos_df: DataFrame with columns [artist_name, view_count, likes, comments, published_at]
        comments_df: Optional DataFrame for additional engagement context
        demo: If True, use synthetic data for testing; if False, require real data

    Returns:
        Dict with metadata: {
            'section': 'performance',
            'charts_rendered': int,
            'hidden_gems_count': int,
            'total_engagement': int
        }
    """

    # Connection logging so notebook users can see that Level 3 is wired correctly
    print(f"[MusicScope Performance v1.0] render_performance_section() called - demo={demo}")

    # ============================================================================
    # HERO CARD
    # ============================================================================
    ms_hero_card(
        title="🚀 Performance Intelligence",
        body_html=(
            "<strong>Core Question:</strong> Which assets are actually converting attention?<br><br>"
            "This section identifies:<br>"
            "• 💎 <strong>Hidden Gems</strong> — high engagement rate, low views (undervalued)<br>"
            "• 🏆 <strong>Content Leaders</strong> — weighted engagement (likes + 2× comments)<br>"
            "• 👥 <strong>Artist Performance</strong> — who drives the most engagement<br>"
            "• 📉 <strong>Efficiency Spread</strong> — engagement rate distribution"
        ),
        gradient="purple"
    )

    # ============================================================================
    # DATA VALIDATION / DEMO MODE
    # ============================================================================
    if demo:
        print("🎭 Demo mode: Using synthetic data for testing")
        videos = _build_demo_videos_df()
    else:
        # Strict validation for production mode
        ms_require_data(
            name="videos_df",
            value=videos_df,
            required_cols=["view_count"],
            context="Performance Intelligence"
        )
        videos = videos_df

    # Optional: comments_df for sentiment overlays
    if comments_df is None or (not demo and comments_df.empty):
        ms_insight_card(
            message="Comments not available → running performance analysis without sentiment overlays.",
            card_type="info"
        )

    # ============================================================================
    # 4.1 — DATA PREPARATION
    # ============================================================================
    print("🔄 Preparing performance data...")

    # Detect artist column
    artist_col = resolve_artist_column(videos)
    title_col = pick_content_column(videos)

    # Ensure engagement columns exist
    perf_df = videos.copy()
    if "likes" not in perf_df.columns:
        perf_df["likes"] = 0
    if "comments" not in perf_df.columns:
        perf_df["comments"] = 0
    
    # Calculate engagement metrics
    perf_df["engagement_score"] = perf_df["likes"] + (2 * perf_df["comments"])  # Weighted: comments worth 2x likes
    perf_df["engagement_rate"] = (
        (perf_df["engagement_score"] / perf_df["view_count"] * 100)
        .fillna(0)
        .replace([np.inf, -np.inf], 0)
    )
    
    print(f"✅ Performance data ready: {len(perf_df):,} videos")
    
    # Track metadata
    metadata = {
        'section': 'performance',
        'charts_rendered': 0,
        'hidden_gems_count': 0,
        'total_engagement': int(perf_df["engagement_score"].sum())
    }
    
    # ============================================================================
    # 4.2 — ENGAGEMENT MATRIX (HIDDEN GEMS QUADRANT)
    # ============================================================================
    ms_subsection_card(
        title="💎 Engagement Matrix: Hidden Gems",
        subtitle=CHART_PURPOSE.get("performance_engagement_matrix", {}).get(
            "question",
            "Which videos have high engagement but low views?"
        )
    )
    
    # Define quadrants
    median_views = perf_df["view_count"].median()
    median_engagement = perf_df["engagement_rate"].median()
    
    # Identify hidden gems (high engagement rate, low views)
    gems = perf_df[
        (perf_df["engagement_rate"] > median_engagement) &
        (perf_df["view_count"] < median_views)
    ]
    metadata['hidden_gems_count'] = len(gems)
    
    # Create scatter plot
    fig = px.scatter(
        perf_df,
        x="view_count",
        y="engagement_rate",
        color=(perf_df["engagement_rate"] > median_engagement).map({True: "High Engagement", False: "Low Engagement"}),
        hover_data=[title_col] if title_col in perf_df.columns else None,
        log_x=True,
        color_discrete_map={
            "High Engagement": MUSICSCOPE_COLORS["success_green"],
            "Low Engagement": MUSICSCOPE_COLORS["baseline_gray"]
        }
    )
    
    # Add quadrant lines
    fig.add_hline(y=median_engagement, line_dash="dash", line_color=MUSICSCOPE_COLORS["text_primary"], opacity=0.3)
    fig.add_vline(x=median_views, line_dash="dash", line_color=MUSICSCOPE_COLORS["text_primary"], opacity=0.3)
    
    ms_apply_plotly_layout(
        fig,
        title="Hidden Gems Quadrant",
        subtitle=f"{len(gems)} videos with high engagement but low views",
        height=500
    )
    fig.update_xaxes(title="Views (log scale)")
    fig.update_yaxes(title="Engagement Rate (%)")
    fig.show(config={"displayModeBar": False})
    metadata['charts_rendered'] += 1

    # Insight card
    if len(gems) > 0:
        ms_insight_card(
            message=f"{len(gems)} hidden gems found → Promote these high-engagement videos to increase reach.",
            card_type="success"
        )
    else:
        ms_insight_card(
            message="No hidden gems detected. All high-engagement content already has good reach.",
            card_type="info"
        )

    # ============================================================================
    # 4.3 — CONTENT LEADERBOARD (WEIGHTED ENGAGEMENT)
    # ============================================================================
    ms_subsection_card(
        title="🏆 Content Leaderboard",
        subtitle="Top videos by weighted engagement (likes + 2× comments)"
    )

    # Top 10 by engagement score
    top_content = perf_df.nlargest(10, "engagement_score")

    if not top_content.empty:
        # Create horizontal bar chart
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=top_content[title_col] if title_col in top_content.columns else top_content.index,
            x=top_content["engagement_score"],
            orientation="h",
            marker=dict(color=MUSICSCOPE_COLORS["brand_purple_start"]),
            text=top_content["engagement_score"].apply(lambda x: f"{x:,.0f}"),
            textposition="outside"
        ))

        ms_apply_plotly_layout(
            fig,
            title="Top 10 Videos by Engagement",
            subtitle="Weighted: likes + 2× comments",
            height=400
        )
        fig.update_xaxes(title="Engagement Score")
        fig.update_yaxes(showticklabels=True)
        fig.show(config={"displayModeBar": False})
        metadata['charts_rendered'] += 1

    # ============================================================================
    # 4.4 — ARTIST-LEVEL PERFORMANCE
    # ============================================================================
    if artist_col in perf_df.columns:
        ms_subsection_card(
            title="👥 Artist Performance Summary",
            subtitle="Total engagement by artist"
        )

        # Aggregate by artist
        artist_perf = (
            perf_df
            .groupby(artist_col)
            .agg(
                total_engagement=("engagement_score", "sum"),
                avg_engagement_rate=("engagement_rate", "mean"),
                video_count=("view_count", "count")
            )
            .reset_index()
            .sort_values("total_engagement", ascending=False)
        )

        if not artist_perf.empty:
            # Create bar chart
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=artist_perf[artist_col],
                y=artist_perf["total_engagement"],
                marker=dict(color=MUSICSCOPE_COLORS["brand_blue_start"]),
                text=artist_perf["total_engagement"].apply(lambda x: f"{x:,.0f}"),
                textposition="outside"
            ))

            ms_apply_plotly_layout(
                fig,
                title="Artist Engagement Contribution",
                subtitle="Who drives the most engagement?",
                height=400
            )
            fig.update_xaxes(title="Artist")
            fig.update_yaxes(title="Total Engagement Score")
            fig.show(config={"displayModeBar": False})
            metadata['charts_rendered'] += 1

    # ============================================================================
    # 4.5 — ENGAGEMENT EFFICIENCY DISTRIBUTION
    # ============================================================================
    ms_subsection_card(
        title="📉 Engagement Efficiency Distribution",
        subtitle="How engagement rates vary across your catalog"
    )

    # Box plot of engagement rates
    fig = go.Figure()
    fig.add_trace(go.Box(
        y=perf_df["engagement_rate"],
        name="Engagement Rate",
        marker=dict(color=MUSICSCOPE_COLORS["brand_pink_start"]),
        boxmean='sd'  # Show mean and standard deviation
    ))

    ms_apply_plotly_layout(
        fig,
        title="Engagement Rate Distribution",
        subtitle="Identify outliers and efficiency spread",
        height=400
    )
    fig.update_yaxes(title="Engagement Rate (%)")
    fig.update_xaxes(showticklabels=False)
    fig.show(config={"displayModeBar": False})
    metadata['charts_rendered'] += 1

    # Insight on efficiency spread
    low_eff = perf_df["engagement_rate"].quantile(0.25)
    high_eff = perf_df["engagement_rate"].quantile(0.75)
    ms_insight_card(
        message=(
            f"25th percentile engagement efficiency is {low_eff:.1f}% "
            f"vs 75th percentile at {high_eff:.1f}% → "
            "Clear upside by reallocating promo away from low-quartile assets."
        ),
        card_type="warning"
    )

    # ============================================================================
    # CLOSING CARD
    # ============================================================================
    ms_closing_card(
        section_title="🚀 Performance Intelligence",
        metrics=[
            ("💎", f"{len(gems)} hidden gems flagged" if isinstance(gems, pd.DataFrame) else "Hidden gems scanned"),
            ("🏆", "Leaders ranked"),
            ("👥", "Artist contribution mapped"),
            ("📉", "Efficiency spread quantified"),
        ],
        next_section="Content Strategy — connect sentiment + performance into release decisions"
    )

    print(f"\n✅ Performance Intelligence section complete: {metadata['charts_rendered']} charts rendered")
    print(f"[DEBUG] Performance charts rendered: {metadata['charts_rendered']} (expected {EXPECTED_CHARTS['description']})")
    return metadata

