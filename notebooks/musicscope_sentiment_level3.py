"""
MusicScope™ Level 3 - Sentiment Intelligence Section

Renders the complete Sentiment Intelligence section with Level 3 business framing.
Uses hybrid architecture: design system for UI + existing helpers for calculations.

Expected Charts:
    - 3 charts total (always rendered)
    - Global sentiment distribution (donut chart)
    - Emotional volatility timeline (dual-axis line chart)
    - Asset-level sentiment (horizontal bar chart)

Usage:
    # Production mode (strict validation)
    from musicscope_sentiment_level3 import render_sentiment_section
    meta = render_sentiment_section(videos_df, comments_df, demo=False)

    # Demo mode (synthetic data for testing)
    meta = render_sentiment_section(videos_df, comments_df, demo=True)
"""

from __future__ import annotations
from typing import Dict, Any

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
from tools.advanced_charts import (
    diverging_sentiment_df, plot_diverging_sentiment, ensure_year_on_dates, label_bars
)
from tools.data_utils import resolve_artist_column, pick_content_column


# Expected chart counts for this section
EXPECTED_CHARTS = {
    "min": 3,
    "max": 3,
    "description": "3 core charts (distribution + volatility + asset-level)"
}


def categorize_sentiment(scores: pd.Series) -> pd.Series:
    """Bucket sentiment scores into negative/neutral/positive safely."""
    numeric = pd.to_numeric(scores, errors="coerce").clip(-1.0, 1.0)
    return pd.cut(
        numeric,
        bins=[-1.0, -0.1, 0.1, 1.0],
        labels=["negative", "neutral", "positive"],
        include_lowest=True,
    )


def _build_demo_comments_df() -> pd.DataFrame:
    """Build a deterministic demo comments DataFrame for storytelling.

    The demo dataset is structured to always surface:
        - A clearly loved artist with consistently positive sentiment
        - An at-risk artist with a toxic pocket in the middle of the window
        - A steady neutral artist for contrast
    """
    dates = pd.date_range(start="2024-01-01", periods=30, freq="D")
    rows = []

    rng = np.random.default_rng(20251219)

    for idx, dt in enumerate(dates):
        # Rising Artist — consistently positive
        rows.append({
            "artist_name": "Rising Artist",
            "sentiment_score": float(rng.uniform(0.4, 0.9)),
            "published_at": dt + pd.Timedelta(hours=1),
            "comment_text": f"Rising Artist fan comment {idx}",
        })

        # At-Risk Artist — concentrated toxic pocket in the center of the window
        if 10 <= idx <= 20:
            for extra in range(3):
                rows.append({
                    "artist_name": "At-Risk Artist",
                    "sentiment_score": float(rng.uniform(-0.9, -0.4)),
                    "published_at": dt + pd.Timedelta(hours=2 + extra),
                    "comment_text": f"At-Risk Artist issue comment {idx}-{extra}",
                })
        else:
            rows.append({
                "artist_name": "At-Risk Artist",
                "sentiment_score": float(rng.uniform(-0.2, 0.1)),
                "published_at": dt + pd.Timedelta(hours=2),
                "comment_text": f"At-Risk Artist neutral comment {idx}",
            })

        # Steady Artist — near-neutral baseline
        rows.append({
            "artist_name": "Steady Artist",
            "sentiment_score": float(rng.uniform(-0.1, 0.3)),
            "published_at": dt + pd.Timedelta(hours=3),
            "comment_text": f"Steady Artist comment {idx}",
        })

    df = pd.DataFrame(rows)
    df["sentiment_category"] = categorize_sentiment(df["sentiment_score"])
    return df


def render_sentiment_section(
    videos_df: pd.DataFrame,
    comments_df: pd.DataFrame,
    demo: bool = False
) -> Dict[str, Any]:
    """
    Render the complete Sentiment Intelligence section with Level 3 framing.

    Parameters:
        videos_df: DataFrame with video metadata
        comments_df: DataFrame with columns [artist_name, sentiment_score, comment_text, published_at]
        demo: If True, use synthetic data for testing; if False, require real data

    Returns:
        Dict with metadata: {
            'section': 'sentiment',
            'charts_rendered': int,
            'comments_analyzed': int,
            'net_sentiment_score': float
        }
    """

    # Connection logging so notebook users can see that Level 3 is wired correctly
    print(f"[MusicScope Sentiment v1.0] render_sentiment_section() called - demo={demo}")

    # ============================================================================
    # HERO CARD
    # ============================================================================
    ms_hero_card(
        title="❤️ Sentiment Intelligence",
        body_html=(
            "<strong>Core Question:</strong> How does your audience feel?<br><br>"
            "This section reveals:<br>"
            "• 🌡️ <strong>Net Sentiment Score (NSS)</strong> — overall mood of your fanbase<br>"
            "• 🌊 <strong>Emotional volatility</strong> — when sentiment swings happen<br>"
            "• 💔 <strong>At-risk assets</strong> — videos with negative sentiment pockets<br>"
            "• 💚 <strong>Most loved content</strong> — what resonates positively"
        ),
        gradient="pink"
    )

    # ============================================================================
    # DATA VALIDATION / DEMO MODE
    # ============================================================================
    if demo:
        print("🎭 Demo mode: Using synthetic data for testing")
        comments = _build_demo_comments_df()
    else:
        # Strict validation for production mode
        ms_require_data(
            name="comments_df",
            value=comments_df,
            required_cols=["comment_text", "published_at"],
            context="Sentiment Intelligence"
        )
        comments = comments_df

    # ============================================================================
    # 3.1 — DATA PREPARATION
    # ============================================================================
    print("🔄 Preparing sentiment data...")

    # Detect artist column
    artist_col = resolve_artist_column(comments)

    # Ensure sentiment_score exists (fallback to TextBlob if needed)
    if "sentiment_score" not in comments.columns:
        print("⚠️  sentiment_score not found. Attempting TextBlob fallback...")
        try:
            from textblob import TextBlob
            text_col = pick_content_column(comments)
            if text_col in comments.columns:
                comments["sentiment_score"] = (
                    comments[text_col]
                    .fillna("")
                    .apply(lambda x: TextBlob(str(x)).sentiment.polarity)
                )
                print("✅ Sentiment scores computed via TextBlob")
            else:
                print("❌ No text column found for sentiment analysis")
                comments["sentiment_score"] = 0.0
        except ImportError:
            print("❌ TextBlob not available. Using neutral sentiment (0.0)")
            comments["sentiment_score"] = 0.0

    # Ensure sentiment_category exists
    if "sentiment_category" not in comments.columns:
        comments["sentiment_category"] = categorize_sentiment(comments["sentiment_score"])

    sent_df = comments.copy()
    sent_df["date"] = pd.to_datetime(sent_df["published_at"]).dt.normalize()

    print(f"✅ Sentiment data ready: {len(sent_df):,} comments")
    
    # Track metadata
    metadata = {
        'section': 'sentiment',
        'charts_rendered': 0,
        'comments_analyzed': len(sent_df),
        'net_sentiment_score': 0.0
    }
    
    # ============================================================================
    # 3.2 — GLOBAL SENTIMENT DISTRIBUTION + NET SENTIMENT SCORE (NSS)
    # ============================================================================
    ms_subsection_card(
        title="🌡️ Global Sentiment Distribution",
        subtitle=CHART_PURPOSE.get("sentiment_global_distribution", {}).get(
            "question",
            "What is the overall mood of your audience?"
        )
    )
    
    # Calculate sentiment counts
    sentiment_counts = sent_df["sentiment_category"].value_counts()
    pos_count = sentiment_counts.get("positive", 0)
    neg_count = sentiment_counts.get("negative", 0)
    neu_count = sentiment_counts.get("neutral", 0)
    total = pos_count + neg_count + neu_count
    
    # Net Sentiment Score (NSS) = (positive - negative) / total * 100
    nss = ((pos_count - neg_count) / total * 100) if total > 0 else 0.0
    metadata['net_sentiment_score'] = nss
    
    # Create donut chart
    fig = go.Figure(data=[go.Pie(
        labels=["Positive", "Neutral", "Negative"],
        values=[pos_count, neu_count, neg_count],
        hole=0.4,
        marker=dict(colors=[
            MUSICSCOPE_COLORS["positive_green"],
            MUSICSCOPE_COLORS["neutral_gray"],
            MUSICSCOPE_COLORS["negative_red"]
        ]),
        textinfo="label+percent",
        textposition="outside"
    )])
    
    ms_apply_plotly_layout(
        fig,
        title="Is your fan conversation net positive or negative?",
        subtitle=f"Net Sentiment Score: {int(round(nss))} - based on {total:,} comments analyzed",
        height=400
    )
    fig.show(config={"displayModeBar": False})
    metadata['charts_rendered'] += 1

    # Insight card based on NSS
    if nss > 10:
        ms_insight_card(
            message=f"Net Sentiment Score is {int(round(nss))} → Strong positive momentum. Amplify what's working!",
            card_type="success"
        )
    elif nss > -10:
        ms_insight_card(
            message=f"Net Sentiment Score is {int(round(nss))} → Mixed mood. Monitor closely and address concerns.",
            card_type="warning"
        )
    else:
        ms_insight_card(
            message=f"Net Sentiment Score is {int(round(nss))} → High negativity risk. Audit top videos for issues.",
            card_type="action"
        )

    # ============================================================================
    # 3.3 — EMOTIONAL VOLATILITY TIMELINE
    # ============================================================================
    ms_subsection_card(
        title="🌊 Emotional Volatility Timeline",
        subtitle="When mood and comment volume move together (or fall apart)"
    )

    # Aggregate daily sentiment
    sentiment_daily = (
        sent_df
        .groupby("date")
        .agg(
            avg_sentiment=("sentiment_score", "mean"),
            comment_volume=("sentiment_score", "count")
        )
        .reset_index()
        .sort_values("date")
    )

    if not sentiment_daily.empty:
        # 7-day moving average for smoothing
        sentiment_daily["ma7_sentiment"] = sentiment_daily["avg_sentiment"].rolling(window=7, min_periods=1).mean()

        # Create dual-axis chart
        fig = go.Figure()

        # Sentiment line (left axis)
        fig.add_trace(go.Scatter(
            x=sentiment_daily["date"],
            y=sentiment_daily["ma7_sentiment"],
            name="Sentiment (7-day MA)",
            line=dict(color=MUSICSCOPE_COLORS["brand_blue_start"], width=3),
            yaxis="y1"
        ))

        # Volume bars (right axis)
        fig.add_trace(go.Bar(
            x=sentiment_daily["date"],
            y=sentiment_daily["comment_volume"],
            name="Comment Volume",
            marker=dict(color=MUSICSCOPE_COLORS["baseline_gray"], opacity=0.3),
            yaxis="y2"
        ))

        fig.update_layout(
            yaxis=dict(title="Sentiment Score", side="left", range=[-1, 1]),
            yaxis2=dict(title="Comment Volume", side="right", overlaying="y"),
            hovermode="x unified",
            showlegend=True,
            height=400
        )

        ms_apply_plotly_layout(
            fig,
            title="When does fan sentiment spike or crash over time?",
            subtitle="Spikes in volume + drops in sentiment flag potential campaign risk.",
            height=400
        )
        fig.show(config={"displayModeBar": False})
        metadata['charts_rendered'] += 1

        # Check for volatility spikes
        volatility = sentiment_daily["avg_sentiment"].std()
        if volatility > 0.3:
            ms_insight_card(
                message=f"High sentiment volatility detected (σ={volatility:.2f}) → Investigate swings around releases or news.",
                card_type="warning"
            )
        else:
            ms_insight_card(
                message=f"Sentiment is relatively stable (σ={volatility:.2f}) → No major spikes; keep messaging steady.",
                card_type="info"
            )
    else:
        ms_insight_card(
            message="Not enough dated comments to plot an emotional volatility timeline.",
            card_type="info"
        )

    # ============================================================================
    # 3.4 — ASSET-LEVEL SENTIMENT (MOST LOVED VS AT-RISK)
    # ============================================================================
    if artist_col in sent_df.columns:
        ms_subsection_card(
            title="💚 Most Loved vs 💔 Most At-Risk Assets",
            subtitle="Where to protect reputation vs double down on fan favorites."
        )

        # Aggregate by artist/video
        asset_sentiment = (
            sent_df
            .groupby(artist_col)
            .agg(
                avg_sentiment=("sentiment_score", "mean"),
                comment_count=("sentiment_score", "count")
            )
            .reset_index()
            .sort_values("avg_sentiment")
        )

        # Filter to assets with meaningful comment volume (>10 comments)
        asset_sentiment = asset_sentiment[asset_sentiment["comment_count"] >= 10]

        if not asset_sentiment.empty:
            # Top 5 loved and top 5 at-risk
            top_loved = asset_sentiment.nlargest(5, "avg_sentiment")
            top_risk = asset_sentiment.nsmallest(5, "avg_sentiment")

            # Use existing helper for diverging bars
            combined = pd.concat([top_risk, top_loved])
            combined_with_pct = diverging_sentiment_df(
                combined.assign(
                    pos=(combined["avg_sentiment"] > 0).astype(int),
                    neu=0,
                    neg=(combined["avg_sentiment"] < 0).astype(int)
                )
            )

            # Create horizontal bar chart
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=combined[artist_col],
                x=combined["avg_sentiment"],
                orientation="h",
                marker=dict(color=[
                    MUSICSCOPE_COLORS["positive_green"] if s > 0 else MUSICSCOPE_COLORS["negative_red"]
                    for s in combined["avg_sentiment"]
                ])
            ))

            ms_apply_plotly_layout(
                fig,
                title="Which assets are loved vs at risk (with real volume)?",
                subtitle="Filtered to assets with ≥10 comments so you act on statistically meaningful signals.",
                height=400
            )
            fig.update_xaxes(title="Average Sentiment Score")
            fig.update_yaxes(showticklabels=True)
            fig.show(config={"displayModeBar": False})
            metadata['charts_rendered'] += 1

            # Insights for loved vs at-risk assets
            risk_count = len(top_risk)
            loved_count = len(top_loved)

            if risk_count > 0:
                ms_insight_card(
                    message=f"{risk_count} assets flagged as at-risk → Review comments with trust & safety / PR before the next release.",
                    card_type="action",
                )
            else:
                ms_insight_card(
                    message="No assets currently flagged as at-risk at the ≥10 comment threshold — keep a light monitoring cadence.",
                    card_type="info",
                )

            ms_insight_card(
                message=f"{loved_count} assets are clear fan favorites → Prioritise these in playlists, paid media, and live setlists.",
                card_type="success",
            )
        else:
            ms_insight_card(
                message="No assets have at least 10 comments yet — sentiment rankings will unlock once more feedback accumulates.",
                card_type="info",
            )
    else:
        ms_insight_card(
            message=f"'{artist_col}' column not found — cannot compute asset-level sentiment rankings.",
            card_type="warning",
        )

    # ============================================================================
    # CLOSING CARD
    # ============================================================================
    ms_closing_card(
        section_title="❤️ Sentiment Intelligence",
        metrics=[
            ("🌡️", f"NSS: {int(round(nss))}"),
            ("💬", f"{len(sent_df):,} comments analyzed"),
            ("📺", f"{sent_df[artist_col].nunique() if artist_col in sent_df.columns else 0} assets mapped"),
            ("⚠️", "Risk quadrants highlighted"),
        ],
        next_section="Performance Intelligence — which assets actually convert?"
    )

    print(f"\n✅ Sentiment Intelligence section complete: {metadata['charts_rendered']} charts rendered")
    print(
        f"[DEBUG] Sentiment charts rendered: {metadata['charts_rendered']} "
        f"(expected {EXPECTED_CHARTS['description']})"
    )

    if metadata["charts_rendered"] < EXPECTED_CHARTS["min"]:
        ms_insight_card(
            message=(
                f"Only {metadata['charts_rendered']} of {EXPECTED_CHARTS['min']}+ planned "
                "views are visible. Some views require richer history "
                "(more days of data or more comments per asset)."
            ),
            card_type="warning",
        )

    return metadata
