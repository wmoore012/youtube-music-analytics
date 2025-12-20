"""
Action-oriented chart title generation for professional data visualizations.

This module provides functions to generate insight-first, punchline-leading titles
that communicate the "so what?" immediately, following evidence-based best practices
for data visualization.

Design Principles:
- Lead with the insight, not the description
- Answer "what happened and why it matters" in the title
- Use specific numbers and comparisons
- Avoid jargon that management won't understand
- Make the business impact immediately obvious
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def generate_sentiment_title(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    sentiment_col: str = "sentiment_category",
) -> str:
    """
    Generate action-oriented title for sentiment diverging bars chart.

    Example outputs:
    - "Raiche leads fan positivity at 78% — 3x higher than BiC Fizzle's 21%"
    - "hicorook dominates positive sentiment with 63% approval vs 50% roster average"

    Args:
        df: DataFrame with sentiment data
        artist_col: Column name for artists
        sentiment_col: Column name for sentiment categories

    Returns:
        Action-oriented title string
    """
    if df.empty or artist_col not in df.columns:
        return "Fan Sentiment Analysis — Insufficient Data"

    # Calculate sentiment proportions by artist
    sentiment_counts = df.groupby([artist_col, sentiment_col]).size().unstack(fill_value=0)

    # Ensure we have positive column
    if "positive" not in sentiment_counts.columns:
        return "Fan Sentiment Breakdown by Artist"

    sentiment_counts["total"] = sentiment_counts.sum(axis=1)
    sentiment_counts["positive_pct"] = (sentiment_counts["positive"] / sentiment_counts["total"] * 100).round(1)

    # Find leader and laggard
    sorted_artists = sentiment_counts.sort_values("positive_pct", ascending=False)

    if len(sorted_artists) < 2:
        leader = sorted_artists.index[0]
        leader_pct = sorted_artists.iloc[0]["positive_pct"]
        return f"{leader} shows {leader_pct:.0f}% positive fan sentiment"

    leader = sorted_artists.index[0]
    leader_pct = sorted_artists.iloc[0]["positive_pct"]
    laggard = sorted_artists.index[-1]
    laggard_pct = sorted_artists.iloc[-1]["positive_pct"]

    # Calculate multiplier
    if laggard_pct > 0:
        multiplier = leader_pct / laggard_pct
        if multiplier >= 2:
            return f"{leader} leads fan positivity at {leader_pct:.0f}% — {multiplier:.1f}x higher than {laggard}'s {laggard_pct:.0f}%"

    # Fallback to percentage point difference
    diff = leader_pct - laggard_pct
    return f"{leader} leads with {leader_pct:.0f}% positive sentiment — {diff:.0f} points ahead of {laggard}"


def generate_heatmap_title(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    metric_cols: Optional[list[str]] = None,
) -> str:
    """
    Generate action-oriented title for engagement heatmap.

    Example outputs:
    - "hicorook dominates engagement metrics — 3.5% total rate vs 1.5% roster average"
    - "Raiche and hicorook lead engagement — both 2x above roster baseline"

    Args:
        df: DataFrame with engagement data
        artist_col: Column name for artists
        metric_cols: List of metric column names

    Returns:
        Action-oriented title string
    """
    if df.empty or artist_col not in df.columns:
        return "Artist Engagement Metrics — Insufficient Data"

    # Default metrics if not provided
    if metric_cols is None:
        metric_cols = ["engagement_rate", "like_rate", "comment_rate"]

    # Filter to available metrics
    available_metrics = [col for col in metric_cols if col in df.columns]

    if not available_metrics:
        return "Artist Performance Comparison"

    # Avoid double-counting engagement rate when component rates are present.
    if "engagement_rate" in available_metrics:
        effective_metrics = ["engagement_rate"]
    else:
        effective_metrics = available_metrics

    # Calculate average engagement per artist
    artist_avg = df.groupby(artist_col)[effective_metrics].mean()

    if artist_avg.empty:
        return "Artist Engagement Analysis"

    # Calculate total engagement score
    if len(effective_metrics) == 1:
        artist_avg["total_engagement"] = artist_avg[effective_metrics[0]]
    else:
        artist_avg["total_engagement"] = artist_avg[effective_metrics].sum(axis=1)
    sorted_artists = artist_avg.sort_values("total_engagement", ascending=False)

    if len(sorted_artists) < 1:
        return "Artist Engagement Metrics"

    leader = sorted_artists.index[0]
    leader_score = sorted_artists.iloc[0]["total_engagement"] * 100  # Convert to percentage
    roster_avg = sorted_artists["total_engagement"].mean() * 100

    if leader_score > roster_avg * 1.5:
        multiplier = leader_score / roster_avg
        return f"{leader} dominates engagement — {leader_score:.1f}% total rate vs {roster_avg:.1f}% roster average ({multiplier:.1f}x)"

    return f"{leader} leads engagement at {leader_score:.1f}% — above {roster_avg:.1f}% roster baseline"


def generate_polarity_ridgeline_title(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
) -> str:
    """
    Generate action-oriented title for sentiment polarity ridgeline chart.

    Example outputs:
    - "Raiche shows most polarized fanbase — wide sentiment spread vs narrow consensus for BiC Fizzle"
    - "hicorook fans cluster positive — tight distribution shows unified sentiment"

    Args:
        df: DataFrame with sentiment scores
        artist_col: Column name for artists

    Returns:
        Action-oriented title string
    """
    if df.empty or artist_col not in df.columns:
        return "Fan Sentiment Distribution by Artist"

    # Calculate sentiment variance by artist (proxy for polarization)
    if "sentiment_score" in df.columns:
        artist_variance = df.groupby(artist_col)["sentiment_score"].var()
        sorted_variance = artist_variance.sort_values(ascending=False)

        if len(sorted_variance) >= 2:
            most_polarized = sorted_variance.index[0]
            least_polarized = sorted_variance.index[-1]
            return f"{most_polarized} shows most polarized fanbase — wide sentiment spread vs narrow consensus for {least_polarized}"

    return "Fan Sentiment Distribution Shows Varying Consensus Levels Across Artists"


def generate_standout_videos_title(
    df: pd.DataFrame,
    view_col: str = "view_count",
    sentiment_col: str = "positive_sentiment_rate",
) -> str:
    """
    Generate action-oriented title for standout videos scatter plot.

    Example outputs:
    - "Viral videos lose fan approval — 70% sentiment at 1K views drops to 30% at 1M+ views"
    - "High-view content shows declining sentiment quality — LOESS trend reveals engagement-approval tradeoff"

    Args:
        df: DataFrame with video performance data
        view_col: Column name for view counts
        sentiment_col: Column name for sentiment rates

    Returns:
        Action-oriented title string
    """
    if df.empty or view_col not in df.columns or sentiment_col not in df.columns:
        return "Video Performance vs Fan Sentiment"

    # Calculate correlation between views and sentiment
    import numpy as np

    # Use log views for better correlation
    df_clean = df[[view_col, sentiment_col]].dropna()
    if len(df_clean) < 10:
        return "Video Virality vs Fan Approval Analysis"

    log_views = np.log10(df_clean[view_col].clip(lower=1))
    sentiment = df_clean[sentiment_col]

    correlation = np.corrcoef(log_views, sentiment)[0, 1]
    if not np.isfinite(correlation):
        return "Video virality shows no clear sentiment pattern — insufficient variance in data"

    if correlation < -0.3:
        return "Viral videos lose fan approval — sentiment drops as view counts rise (negative correlation)"
    elif correlation > 0.3:
        return "Popular videos earn fan approval — higher views correlate with positive sentiment"
    else:
        return "Video virality shows no clear sentiment pattern — quality and reach operate independently"


def generate_growth_signal_title(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    date_col: str = "published_at",
    engagement_col: str = "engagement_rate",
) -> str:
    """
    Generate action-oriented title for growth signal breakdown chart.

    Example outputs:
    - "hicorook surges 8x in 2024 — engagement jumps from 2% to 13% while roster stagnates"
    - "Raiche maintains steady 4% engagement — consistent performance vs volatile competitors"

    Args:
        df: DataFrame with time-series engagement data
        artist_col: Column name for artists
        date_col: Column name for dates
        engagement_col: Column name for engagement rates

    Returns:
        Action-oriented title string
    """
    if df.empty or artist_col not in df.columns or engagement_col not in df.columns:
        return "Artist Engagement Growth Over Time"

    # Calculate growth rates by artist
    df_sorted = df.copy()
    df_sorted[date_col] = pd.to_datetime(df_sorted[date_col], errors="coerce")
    df_sorted = df_sorted.dropna(subset=[date_col]).sort_values(date_col)

    growth_rates = {}
    for artist in df_sorted[artist_col].unique():
        artist_data = df_sorted[df_sorted[artist_col] == artist][engagement_col].dropna()
        if len(artist_data) >= 2:
            first_val = artist_data.iloc[0]
            last_val = artist_data.iloc[-1]
            if first_val > 0:
                growth = (last_val / first_val - 1) * 100
                growth_rates[artist] = growth

    if not growth_rates:
        return "Artist Engagement Trends Show Varying Growth Trajectories"

    # Find top grower
    top_grower = max(growth_rates, key=growth_rates.get)
    top_growth = growth_rates[top_grower]

    if top_growth > 100:
        multiplier = (top_growth / 100) + 1
        return f"{top_grower} surges {multiplier:.0f}x in recent period — engagement growth outpaces roster"
    elif top_growth > 50:
        return f"{top_grower} shows {top_growth:.0f}% engagement growth — strongest momentum in roster"
    else:
        return "Roster shows modest engagement growth — no breakout performers yet"


def generate_category_areas_title(
    df: pd.DataFrame,
    category_col: str = "content_type",
    date_col: str = "published_at",
    views_col: str = "view_count",
) -> str:
    """
    Generate action-oriented title for views by category stacked area chart.

    Example outputs:
    - "Music videos dominate 2024 views — 85% of traffic vs 10% for behind-the-scenes content"
    - "'Other' content surges in 2023 — non-music videos capture 40% viewership spike"

    Args:
        df: DataFrame with categorized view data
        category_col: Column name for content categories
        date_col: Column name for dates
        views_col: Column name for view counts

    Returns:
        Action-oriented title string
    """
    if df.empty or category_col not in df.columns or views_col not in df.columns:
        return "Content Category Performance Over Time"

    # Calculate category shares
    category_totals = df.groupby(category_col)[views_col].sum()
    if isinstance(category_totals, pd.DataFrame):
        category_totals = category_totals.sum(axis=1)  # sum duplicates
    category_totals = pd.to_numeric(category_totals, errors="coerce").fillna(0).sort_values(ascending=False)
    total_views = category_totals.sum()

    if total_views == 0 or len(category_totals) == 0:
        return "Content Mix Analysis Across Categories"

    top_category = category_totals.index[0]
    top_share = category_totals.iloc[0] / total_views * 100

    if top_share > 70:
        return f"{top_category} dominates viewership — {top_share:.0f}% of total views vs fragmented alternatives"
    elif top_share > 50:
        return f"{top_category} leads content mix at {top_share:.0f}% — balanced portfolio with diverse categories"
    else:
        return f"Diversified content strategy — no single category exceeds 50% viewership"


def generate_content_type_dots_title(
    df: pd.DataFrame,
    artist_col: str = "artist_name",
    category_col: str = "content_type",
) -> str:
    """
    Generate action-oriented title for content type distribution dot chart.

    Example outputs:
    - "hicorook diversifies content — 350+ 'Other' videos vs music-only focus for BiC Fizzle"
    - "Roster relies on music videos — limited content experimentation across artists"

    Args:
        df: DataFrame with artist and content type data
        artist_col: Column name for artists
        category_col: Column name for content categories

    Returns:
        Action-oriented title string
    """
    if df.empty or artist_col not in df.columns or category_col not in df.columns:
        return "Content Type Distribution by Artist"

    # Calculate content diversity by artist
    artist_diversity = df.groupby(artist_col)[category_col].nunique()
    content_counts = df.groupby([artist_col, category_col]).size().unstack(fill_value=0)

    # Find most diverse artist
    most_diverse = artist_diversity.idxmax()
    diversity_score = artist_diversity.max()

    # Find artist with most content in non-music categories
    if "Other" in content_counts.columns:
        most_other = content_counts["Other"].idxmax()
        other_count = content_counts["Other"].max()

        if other_count > 100:
            return (
                f"{most_other} diversifies content — {other_count:.0f}+ experimental videos vs traditional music focus"
            )

    if diversity_score >= 4:
        return f"{most_diverse} experiments across {diversity_score} content types — most diverse strategy in roster"
    else:
        return "Roster focuses on core music content — limited category experimentation"


def format_date_axis_label(date_str: str, include_year: bool = True) -> str:
    """
    Format date axis labels with optional year inclusion.

    Args:
        date_str: Date string to format
        include_year: Whether to include year (default: True)

    Returns:
        Formatted date string (falls back to original if parsing fails)
    """
    parsed = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(parsed):
        return date_str
    return parsed.strftime("%b %d %Y" if include_year else "%b %d")
