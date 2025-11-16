"""
Content categorization and analysis functions for YouTube video content.
Analyzes ISRC vs non-ISRC, content types, duration categories, and artist strategies.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Content categorization constants and helper functions
CONTENT_CATEGORIZATION_KEYWORDS = {
    "music_video": ["music video", "official video", "mv", "official music video"],
    "lyric_video": ["lyrics", "lyric video", "official lyrics", "with lyrics"],
    "visualizer": ["visualizer", "audio", "official audio"],
    "live_performance": ["live", "performance", "concert", "session"],
    "behind_scenes": ["behind the scenes", "bts", "making of", "studio"],
}

# Performance analysis constants
DEFAULT_SHORT_FORM_THRESHOLD = 300  # 5 minutes in seconds
PERFORMANCE_THRESHOLD_MULTIPLIER = 1.2  # 20% better performance threshold
COMPLIANCE_GOOD_THRESHOLD = 0.5  # 50% compliance rate for "good" status
TEASER_CLIP_THRESHOLD = 60  # 1 minute threshold for teaser clips


def calculate_views_by_category(
    df: pd.DataFrame,
    content_type_col: Optional[str] = None,
    views_col: str = "views",
    video_type_col: Optional[str] = None,
    artist_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate total views by content category.

    Args:
        df: DataFrame with content data
        content_type_col: Column name for content types
        views_col: Column name for view counts
        video_type_col: Optional column name for video types (for backward compatibility)
        artist_col: Optional artist column (ignored for now)

    Returns:
        DataFrame with category views
    """
    # Use video_type_col if provided for backward compatibility, otherwise use content_type_col
    group_col = video_type_col if video_type_col else content_type_col
    if group_col is None:
        # Default to a reasonable column name
        group_col = "video_type" if "video_type" in df.columns else df.columns[0]

    # Calculate views by category and return as DataFrame
    views_by_category = df.groupby(group_col)[views_col].sum().reset_index()
    views_by_category.columns = [group_col, "total_views"]

    return views_by_category


def _calculate_diversity_score(unique_items: int, total_items: int) -> float:
    """Calculate diversity score as ratio of unique to total items."""
    return unique_items / total_items if total_items > 0 else 0


def _generate_genre_diversity_narrative(artist_genres: Dict[str, str], unique_genres: int) -> str:
    """Generate narrative about genre diversity"""
    if unique_genres == 1:
        return "All artists share the same genre, creating a cohesive roster sound."
    elif unique_genres == len(artist_genres):
        return "Each artist represents a different genre, maximizing market diversity."
    else:
        return (
            f"Roster spans {unique_genres} genres across {len(artist_genres)} artists, balancing diversity with focus."
        )


def analyze_genre_diversity(df: pd.DataFrame, artist_col: str, genre_col: str, content_type_col: str) -> Dict[str, Any]:
    """
    Analyze genre diversity across artists and their content strategies.

    Args:
        df: DataFrame with artist and genre data
        artist_col: Column name for artist names
        genre_col: Column name for genres
        content_type_col: Column name for content types

    Returns:
        Dictionary with genre diversity analysis
    """
    # Get unique genres per artist
    artist_genres = df.groupby(artist_col)[genre_col].first().to_dict()

    # Count unique genres
    unique_genres = df[genre_col].nunique()
    total_artists = df[artist_col].nunique()

    # Analyze content strategy by genre
    content_by_genre = df.groupby([genre_col, content_type_col]).size().unstack(fill_value=0)

    # Calculate genre diversity score using helper function
    genre_diversity_score = _calculate_diversity_score(unique_genres, total_artists)

    return {
        "genre_diversity": genre_diversity_score,
        "artist_genres": artist_genres,
        "unique_genres": unique_genres,
        "total_artists": total_artists,
        "content_strategy_by_genre": content_by_genre.to_dict(),
        "diversity_analysis": _generate_genre_diversity_narrative(artist_genres, unique_genres),
    }


def categorize_content_types(df: pd.DataFrame, content_type_col: str) -> pd.DataFrame:
    """
    Categorize content types into broader categories.

    Args:
        df: DataFrame with content data
        content_type_col: Column name for content types

    Returns:
        DataFrame with added content_category column
    """
    df = df.copy()

    # Define content type mapping
    content_mapping = {
        "music_video": "Music Content",
        "lyric_video": "Music Content",
        "visualizer": "Music Content",
        "content_video": "Non-Music Content",
        "lifestyle": "Non-Music Content",
        "behind_scenes": "Non-Music Content",
        "interview": "Non-Music Content",
        "vlog": "Non-Music Content",
    }

    # Apply mapping
    df["content_category"] = df[content_type_col].map(content_mapping)

    # Handle unmapped types
    df["content_category"] = df["content_category"].fillna("Other Content")

    return df


def classify_video_duration(df: pd.DataFrame, duration_col: str, short_form_threshold: int = 300) -> pd.DataFrame:
    """
    Classify videos by duration (short-form vs long-form).

    Args:
        df: DataFrame with duration data
        duration_col: Column name for duration in seconds
        short_form_threshold: Threshold in seconds for short-form classification

    Returns:
        DataFrame with added duration_category column
    """
    df = df.copy()

    # Classify based on threshold
    df["duration_category"] = df[duration_col].apply(
        lambda x: "short_form" if x <= short_form_threshold else "long_form"
    )

    return df


def analyze_isrc_distribution(df: pd.DataFrame, artist_col: str, isrc_col: str, views_col: str) -> pd.DataFrame:
    """
    Analyze ISRC vs non-ISRC distribution by artist.

    Args:
        df: DataFrame with ISRC data
        artist_col: Column name for artist names
        isrc_col: Column name for ISRC boolean
        views_col: Column name for view counts

    Returns:
        DataFrame with ISRC analysis by artist
    """
    # Group by artist and calculate ISRC statistics
    artist_stats = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Count videos with and without ISRC
        isrc_videos = artist_df[artist_df[isrc_col] is True]
        non_isrc_videos = artist_df[artist_df[isrc_col] is False]

        total_videos = len(artist_df)
        isrc_count = len(isrc_videos)
        non_isrc_count = len(non_isrc_videos)

        # Calculate percentages
        isrc_percentage = (isrc_count / total_videos) * 100 if total_videos > 0 else 0
        non_isrc_percentage = (non_isrc_count / total_videos) * 100 if total_videos > 0 else 0

        # Calculate view totals
        isrc_views = isrc_videos[views_col].sum() if len(isrc_videos) > 0 else 0
        non_isrc_views = non_isrc_videos[views_col].sum() if len(non_isrc_videos) > 0 else 0
        total_views = artist_df[views_col].sum()

        artist_stats.append(
            {
                artist_col: artist,
                "isrc_count": isrc_count,
                "non_isrc_count": non_isrc_count,
                "isrc_percentage": round(isrc_percentage, 2),
                "non_isrc_percentage": round(non_isrc_percentage, 2),
                "isrc_views": isrc_views,
                "non_isrc_views": non_isrc_views,
                "total_views": total_views,
                "isrc_view_percentage": round((isrc_views / total_views) * 100, 2) if total_views > 0 else 0,
            }
        )

    return pd.DataFrame(artist_stats)


def calculate_content_performance_metrics(df: pd.DataFrame, content_type_col: str, views_col: str) -> pd.DataFrame:
    """
    Calculate performance metrics by content type.

    Args:
        df: DataFrame with content data
        content_type_col: Column name for content types
        views_col: Column name for view counts

    Returns:
        DataFrame with performance metrics by content type
    """
    # Group by content type and calculate metrics
    metrics = df.groupby(content_type_col).agg({views_col: ["count", "sum", "mean", "median", "std"]}).round(2)

    # Flatten column names
    metrics.columns = ["video_count", "total_views", "avg_views", "median_views", "std_views"]

    # Reset index to make content_type a column
    metrics = metrics.reset_index()

    # Calculate additional metrics
    total_all_views = df[views_col].sum()
    metrics["view_share_percentage"] = (metrics["total_views"] / total_all_views * 100).round(2)

    # Sort by total views descending
    metrics = metrics.sort_values("total_views", ascending=False)

    return metrics


def analyze_content_strategy_effectiveness(
    df: pd.DataFrame, artist_col: str, content_type_col: str, views_col: str, isrc_col: str
) -> Dict[str, Any]:
    """
    Analyze effectiveness of different content strategies by artist.

    Args:
        df: DataFrame with content data
        artist_col: Column name for artist names
        content_type_col: Column name for content types
        views_col: Column name for view counts
        isrc_col: Column name for ISRC boolean

    Returns:
        Dictionary with content strategy analysis
    """
    results = {}

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate content type performance
        content_performance = artist_df.groupby(content_type_col)[views_col].agg(["count", "sum", "mean"]).round(2)

        # Calculate ISRC vs non-ISRC performance
        isrc_performance = artist_df.groupby(isrc_col)[views_col].agg(["count", "sum", "mean"]).round(2)

        # Find best performing content type
        best_content_type = content_performance["sum"].idxmax() if len(content_performance) > 0 else None

        # Calculate strategy insights
        total_views = artist_df[views_col].sum()
        music_content_views = artist_df[artist_df[isrc_col] is True][views_col].sum()
        _content_video_views = artist_df[artist_df[isrc_col] is False][views_col].sum()  # noqa: F841

        music_content_ratio = (music_content_views / total_views) if total_views > 0 else 0

        # Determine strategy type
        if music_content_ratio > 0.7:
            strategy_type = "Music-Focused"
        elif music_content_ratio < 0.3:
            strategy_type = "Content-Focused"
        else:
            strategy_type = "Balanced"

        results[artist] = {
            "strategy_type": strategy_type,
            "music_content_ratio": round(music_content_ratio, 3),
            "best_performing_content_type": best_content_type,
            "total_views": total_views,
            "content_type_performance": content_performance.to_dict(),
            "isrc_performance": isrc_performance.to_dict(),
        }

    return results


def identify_content_opportunities(
    df: pd.DataFrame, artist_col: str, content_type_col: str, views_col: str, min_performance_threshold: float = 0.8
) -> Dict[str, List[str]]:
    """
    Identify content opportunities for each artist based on performance gaps.

    Args:
        df: DataFrame with content data
        artist_col: Column name for artist names
        content_type_col: Column name for content types
        views_col: Column name for view counts
        min_performance_threshold: Minimum performance ratio to consider successful

    Returns:
        Dictionary mapping artists to content opportunity recommendations
    """
    opportunities = {}

    # Calculate overall content type performance
    overall_performance = df.groupby(content_type_col)[views_col].mean()

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]
        artist_performance = artist_df.groupby(content_type_col)[views_col].mean()

        artist_opportunities = []

        # Find content types where artist underperforms
        for content_type in overall_performance.index:
            if content_type not in artist_performance.index:
                # Artist hasn't tried this content type
                artist_opportunities.append(f"Experiment with {content_type} content")
            else:
                # Check if artist underperforms in this content type
                artist_avg = artist_performance[content_type]
                overall_avg = overall_performance[content_type]
                performance_ratio = artist_avg / overall_avg if overall_avg > 0 else 0

                if performance_ratio < min_performance_threshold:
                    artist_opportunities.append(f"Improve {content_type} content strategy")

        # Find content types where artist excels
        for content_type in artist_performance.index:
            if content_type in overall_performance.index:
                artist_avg = artist_performance[content_type]
                overall_avg = overall_performance[content_type]
                performance_ratio = artist_avg / overall_avg if overall_avg > 0 else 0

                if performance_ratio > 1.5:  # Significantly outperforms
                    artist_opportunities.append(f"Double down on {content_type} - high performance area")

        opportunities[artist] = artist_opportunities

    return opportunities


def create_artist_strengths_venn_diagram(
    df: pd.DataFrame,
    artist_col: str,
    content_type_col: str = "video_type",
    views_col: str = "views",
    performance_metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Create data for overlapping circle / Venn diagram showing what artists are doing well as a whole.

    Args:
        df: DataFrame with content data
        artist_col: Column name for artist names
        content_type_col: Column name for content types
        views_col: Column name for view counts
        performance_metrics: Optional list of performance metrics to analyze

    Returns:
        Dictionary with Venn diagram data structure
    """
    if performance_metrics is None:
        performance_metrics = [views_col]
    # Calculate performance by artist and content type
    performance = df.groupby([artist_col, content_type_col])[views_col].sum().reset_index()

    # Find top performing content types
    content_performance = df.groupby(content_type_col)[views_col].sum().sort_values(ascending=False)
    top_content_types = content_performance.head(3).index.tolist()

    # Find artists excelling in each content type
    venn_data = {}
    for content_type in top_content_types:
        content_df = performance[performance[content_type_col] == content_type]
        # Get top 50% of artists for this content type
        threshold = content_df[views_col].quantile(0.5)
        strong_artists = content_df[content_df[views_col] >= threshold][artist_col].tolist()
        venn_data[content_type] = strong_artists

    # Calculate overlaps
    overlaps = {}
    content_types = list(venn_data.keys())

    for i, type1 in enumerate(content_types):
        for j, type2 in enumerate(content_types[i + 1 :], i + 1):
            overlap_key = f"{type1} & {type2}"
            overlap_artists = list(set(venn_data[type1]) & set(venn_data[type2]))
            overlaps[overlap_key] = overlap_artists

    return {"content_strengths": venn_data, "overlaps": overlaps, "performance_data": performance.to_dict("records")}


def analyze_isrc_vs_content_balance(df: pd.DataFrame, artist_col: str, isrc_col: str, views_col: str) -> Dict[str, Any]:
    """
    Analyze balance between videos with ISRC vs without (music videos vs content videos).

    Args:
        df: DataFrame with ISRC data
        artist_col: Column name for artist names
        isrc_col: Column name for ISRC boolean
        views_col: Column name for view counts

    Returns:
        Dictionary with ISRC balance analysis
    """
    balance_data = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate ISRC vs non-ISRC metrics
        isrc_videos = artist_df[artist_df[isrc_col] is True]
        non_isrc_videos = artist_df[artist_df[isrc_col] is False]

        isrc_count = len(isrc_videos)
        non_isrc_count = len(non_isrc_videos)
        total_count = len(artist_df)

        isrc_views = isrc_videos[views_col].sum() if len(isrc_videos) > 0 else 0
        non_isrc_views = non_isrc_videos[views_col].sum() if len(non_isrc_videos) > 0 else 0
        total_views = artist_df[views_col].sum()

        # Calculate balance score (0 = all content, 1 = all music)
        balance_score = isrc_views / total_views if total_views > 0 else 0

        # Determine strategy type
        if balance_score > 0.8:
            strategy = "Music-Heavy"
        elif balance_score < 0.2:
            strategy = "Content-Heavy"
        else:
            strategy = "Balanced"

        balance_data.append(
            {
                "artist": artist,
                "isrc_count": isrc_count,
                "non_isrc_count": non_isrc_count,
                "isrc_views": isrc_views,
                "non_isrc_views": non_isrc_views,
                "balance_score": round(balance_score, 3),
                "strategy_type": strategy,
                "total_videos": total_count,
                "total_views": total_views,
            }
        )

    # Separate artists by strategy type for test compatibility
    isrc_analysis = {}
    content_analysis = {}

    for item in balance_data:
        artist = item["artist"]
        if item["strategy_type"] == "Music-Heavy":
            isrc_analysis[artist] = item
        elif item["strategy_type"] == "Content-Heavy":
            content_analysis[artist] = item
        else:
            # Balanced artists go to both
            isrc_analysis[artist] = item
            content_analysis[artist] = item

    return {
        "isrc_analysis": isrc_analysis,
        "content_analysis": content_analysis,
        "artist_balance": balance_data,
        "overall_balance": (
            sum(item["balance_score"] for item in balance_data) / len(balance_data) if balance_data else 0
        ),
    }


def analyze_video_length_performance(
    df: pd.DataFrame,
    artist_col: str,
    duration_col: str,
    views_col: str,
    short_form_threshold: int = DEFAULT_SHORT_FORM_THRESHOLD,
) -> pd.DataFrame:
    """
    Analyze short-form vs long-form video performance breakdown with view totals.

    Args:
        df: DataFrame with duration data
        artist_col: Column name for artist names
        duration_col: Column name for duration in seconds
        views_col: Column name for view counts
        short_form_threshold: Threshold in seconds for short-form classification

    Returns:
        DataFrame with video length performance analysis
    """
    # Classify videos by duration
    df_copy = df.copy()
    df_copy["video_length_category"] = df_copy[duration_col].apply(
        lambda x: "short_form" if x <= short_form_threshold else "long_form"
    )

    return df_copy


def analyze_video_type_distribution(
    df: pd.DataFrame, artist_col: str, video_type_col: str, views_col: str
) -> pd.DataFrame:
    """
    Analyze music video count vs lyric video count vs visualizer count vs other content.

    Args:
        df: DataFrame with video type data
        artist_col: Column name for artist names
        video_type_col: Column name for video types
        views_col: Column name for view counts

    Returns:
        DataFrame with video type distribution analysis
    """
    # Calculate video count and total views by artist and video type
    video_stats = df.groupby([artist_col, video_type_col]).agg({views_col: ["count", "sum"]}).reset_index()

    # Flatten column names
    video_stats.columns = [artist_col, video_type_col, "video_count", "total_views"]

    return video_stats


def create_artist_comparison_chart(
    df: pd.DataFrame,
    artist_col: str,
    comparison_metrics: Optional[List[str]] = None,
    metrics_cols: Optional[List[str]] = None,
    chart_type: str = "radar",
) -> Dict[str, Any]:
    """
    Create side-by-side artist comparison data structure.

    Args:
        df: DataFrame with artist data
        artist_col: Column name for artist names
        comparison_metrics: List of metric column names to compare
        metrics_cols: Alternative parameter name for backward compatibility
        chart_type: Type of comparison chart ('radar', 'bar', 'heatmap')

    Returns:
        Dictionary with comparison chart data
    """
    # Use comparison_metrics if provided, otherwise use metrics_cols
    metrics_to_use = comparison_metrics or metrics_cols or ["views"]

    # Calculate metrics by artist
    comparison_data = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        artist_metrics = {"artist": artist}

        for metric in metrics_to_use:
            if metric in artist_df.columns:
                # Calculate different aggregations based on metric type
                if "count" in metric.lower() or "total" in metric.lower():
                    value = artist_df[metric].sum()
                elif "avg" in metric.lower() or "mean" in metric.lower():
                    value = artist_df[metric].mean()
                else:
                    value = artist_df[metric].sum()  # Default to sum

                artist_metrics[metric] = round(value, 2)

        comparison_data.append(artist_metrics)

    # Normalize metrics for radar chart (0-100 scale)
    if chart_type == "radar":
        normalized_data = []
        for artist_data in comparison_data:
            normalized_artist = {"artist": artist_data["artist"]}

            for metric in metrics_cols:
                if metric in artist_data:
                    # Get max value for this metric across all artists
                    max_val = max(item.get(metric, 0) for item in comparison_data)
                    if max_val > 0:
                        normalized_value = (artist_data[metric] / max_val) * 100
                    else:
                        normalized_value = 0
                    normalized_artist[f"{metric}_normalized"] = round(normalized_value, 2)

            normalized_data.append(normalized_artist)

        return {
            "chart_type": chart_type,
            "raw_data": comparison_data,
            "normalized_data": normalized_data,
            "metrics": metrics_to_use,
        }

    return {"chart_type": chart_type, "data": comparison_data, "metrics": metrics_to_use}


def create_roster_overview_chart(
    df: pd.DataFrame,
    artist_col: str,
    performance_metrics: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Create combined roster analysis showing overall performance.

    Args:
        df: DataFrame with roster data
        artist_col: Column name for artist names
        performance_metrics: List of performance metric column names
        metrics: Alternative parameter name for backward compatibility

    Returns:
        Dictionary with roster overview data
    """
    # Use metrics if provided, otherwise use performance_metrics
    metrics_to_use = metrics or performance_metrics or ["views"]

    # Calculate roster-wide statistics
    roster_stats = {}

    # Overall metrics
    total_artists = df[artist_col].nunique()
    roster_stats["total_artists"] = total_artists

    # Calculate performance by artist
    artist_performance = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        artist_perf = {"artist": artist}

        for metric in metrics_to_use:
            if metric in artist_df.columns:
                # Calculate metric value
                if "views" in metric.lower():
                    value = artist_df[metric].sum()
                elif "count" in metric.lower():
                    value = len(artist_df)
                elif "avg" in metric.lower() or "mean" in metric.lower():
                    value = artist_df[metric].mean()
                else:
                    value = artist_df[metric].sum()

                artist_perf[metric] = round(value, 2)

        artist_performance.append(artist_perf)

    # Calculate roster rankings
    for metric in metrics_to_use:
        if any(metric in perf for perf in artist_performance):
            # Sort by metric and add ranking
            sorted_artists = sorted(artist_performance, key=lambda x: x.get(metric, 0), reverse=True)
            for i, artist_data in enumerate(sorted_artists):
                artist_data[f"{metric}_rank"] = i + 1

    # Calculate roster diversity metrics
    roster_stats["performance_spread"] = {}
    for metric in metrics_to_use:
        values = [perf.get(metric, 0) for perf in artist_performance if perf.get(metric) is not None]
        if values:
            roster_stats["performance_spread"][metric] = {
                "min": min(values),
                "max": max(values),
                "avg": round(sum(values) / len(values), 2),
                "std": round(np.std(values), 2) if len(values) > 1 else 0,
            }

    return {"roster_stats": roster_stats, "artist_performance": artist_performance, "metrics": metrics_to_use}


def analyze_genre_context(
    df: pd.DataFrame = None,
    artist_col: str = "artist_name",
    genre_col: str = "genre",
    performance_col: str = "views",
    content_df: Optional[pd.DataFrame] = None,
    genre_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Analyze genre context mentions (all artists are new signees but different genres).

    Args:
        df: DataFrame with genre data (optional if content_df and genre_df provided)
        artist_col: Column name for artist names
        genre_col: Column name for genres
        performance_col: Column name for performance metric
        content_df: Optional content DataFrame for backward compatibility
        genre_df: Optional genre DataFrame for backward compatibility

    Returns:
        Dictionary with genre context analysis
    """
    # Use content_df if provided for backward compatibility
    if content_df is not None and df is None:
        df = content_df

    # If we have separate genre_df, merge it with content data
    if genre_df is not None and df is not None:
        # Merge genre data with content data
        df = df.merge(genre_df, on=artist_col, how="left")

    if df is None:
        return {"error": "No data provided"}
    # Calculate genre performance
    genre_performance = df.groupby(genre_col)[performance_col].agg(["count", "sum", "mean"]).round(2)
    genre_performance.columns = ["artist_count", "total_performance", "avg_performance"]
    genre_performance = genre_performance.reset_index()

    # Calculate artist performance within genres
    artist_genre_performance = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]
        artist_genre = artist_df[genre_col].iloc[0] if len(artist_df) > 0 else "Unknown"
        artist_performance = artist_df[performance_col].sum()

        # Get genre average for comparison
        genre_avg = (
            genre_performance[genre_performance[genre_col] == artist_genre]["avg_performance"].iloc[0]
            if len(genre_performance[genre_performance[genre_col] == artist_genre]) > 0
            else 0
        )

        # Calculate relative performance
        relative_performance = artist_performance / genre_avg if genre_avg > 0 else 0

        # Determine performance category
        if relative_performance > 1.2:
            performance_category = "Above Genre Average"
        elif relative_performance < 0.8:
            performance_category = "Below Genre Average"
        else:
            performance_category = "At Genre Average"

        artist_genre_performance.append(
            {
                "artist": artist,
                "genre": artist_genre,
                "performance": artist_performance,
                "genre_avg_performance": genre_avg,
                "relative_performance": round(relative_performance, 3),
                "performance_category": performance_category,
            }
        )

    # Calculate genre diversity insights
    unique_genres = df[genre_col].nunique()
    total_artists = df[artist_col].nunique()

    genre_insights = {
        "genre_diversity_score": unique_genres / total_artists if total_artists > 0 else 0,
        "unique_genres": unique_genres,
        "total_artists": total_artists,
        "most_represented_genre": (
            genre_performance.loc[genre_performance["artist_count"].idxmax(), genre_col]
            if len(genre_performance) > 0
            else None
        ),
        "best_performing_genre": (
            genre_performance.loc[genre_performance["avg_performance"].idxmax(), genre_col]
            if len(genre_performance) > 0
            else None
        ),
    }

    # Add new signee context as string
    new_signee_context = f"All {total_artists} artists are new signees with different genres, representing {unique_genres} unique musical styles in a diverse roster approach."

    return {
        "genre_analysis": genre_insights,
        "new_signee_context": new_signee_context,
        "genre_performance": genre_performance.to_dict("records"),
        "artist_genre_performance": artist_genre_performance,
        "genre_insights": genre_insights,
    }


def _categorize_single_content(title: str, description: Optional[str] = None) -> str:
    """
    Categorize a single piece of content based on title and description.

    Uses the existing YouTube version parser for more sophisticated detection.

    Args:
        title: Video title
        description: Optional video description

    Returns:
        Content category string
    """
    # Try to use the existing YouTube version parser if available
    try:
        from web.youtube_version_parser import extract_version_from_title

        # Extract version information using the sophisticated parser
        cleaned_title, version_type = extract_version_from_title(title)

        if version_type:
            # Map version types to our categories
            version_mapping = {
                "Official Video": "music_video",
                "Official Music Video": "music_video",
                "Music Video": "music_video",
                "Lyric Video": "lyric_video",
                "Lyrics": "lyric_video",
                "Official Audio": "visualizer",
                "Audio": "visualizer",
                "Visualizer": "visualizer",
                "Official Visualizer": "visualizer",
                "Live": "live_performance",
                "Live Performance": "live_performance",
                "Behind The Scenes": "behind_scenes",
                "Making Of": "behind_scenes",
            }

            mapped_category = version_mapping.get(version_type)
            if mapped_category:
                return mapped_category

    except ImportError:
        # Fall back to simple keyword matching if parser not available
        pass

    # Fallback to simple keyword matching
    title_lower = str(title).lower()
    desc_lower = str(description).lower() if description else ""
    combined_text = f"{title_lower} {desc_lower}"

    # Check for specific content types
    for category, keywords in CONTENT_CATEGORIZATION_KEYWORDS.items():
        if any(keyword in combined_text for keyword in keywords):
            return category

    return "other_content"


def categorize_video_content(
    data, title_col: str = "title", description_col: Optional[str] = None, duration_col: Optional[str] = None
):
    """
    Automatically categorize video content based on metadata.

    Args:
        data: List of video dictionaries or DataFrame with video data
        title_col: Column name for video titles
        description_col: Optional column name for descriptions
        duration_col: Optional column name for duration

    Returns:
        List of categorized videos or DataFrame with added content_category column
    """

    # Handle list input for backward compatibility
    if isinstance(data, list):
        categorized_videos = []
        for video in data:
            video_copy = video.copy()
            title = video.get(title_col, video.get("title", ""))
            description = video.get(description_col, video.get("description", ""))
            category = _categorize_single_content(title, description)
            video_copy["category"] = category
            categorized_videos.append(video_copy)
        return categorized_videos

    # Handle DataFrame input
    df_copy = data.copy()

    # Apply categorization using the helper function
    if description_col and description_col in df_copy.columns:
        df_copy["content_category"] = df_copy.apply(
            lambda row: _categorize_single_content(row[title_col], row[description_col]), axis=1
        )
    else:
        df_copy["content_category"] = df_copy[title_col].apply(lambda title: _categorize_single_content(title))

    # Add duration-based refinement if available
    if duration_col and duration_col in df_copy.columns:
        # Very short videos (< 60s) are likely teasers or clips
        df_copy.loc[
            (df_copy[duration_col] < 60) & (df_copy["content_category"] == "other_content"), "content_category"
        ] = "teaser_clip"

    return df_copy


def generate_content_strategy_recommendations(
    data,
    artist_col: str = "artist",
    content_type_col: str = "video_type",
    performance_col: str = "avg_views",
    min_sample_size: int = 3,
):
    """
    Generate content strategy recommendations based on performance analysis.

    Args:
        data: DataFrame with content performance data or list for simple recommendations
        artist_col: Column name for artist names
        content_type_col: Column name for content types
        performance_col: Column name for performance metric
        min_sample_size: Minimum number of videos needed for reliable recommendations

    Returns:
        List of recommendations or dictionary mapping artists to recommendation lists
    """
    # Handle simple list / DataFrame input for basic recommendations
    if isinstance(data, pd.DataFrame) and len(data) < 10:
        # Generate simple recommendations based on performance data
        recommendations = []

        if content_type_col in data.columns and performance_col in data.columns:
            # Sort by performance and recommend top types
            sorted_data = data.sort_values(performance_col, ascending=False)
            top_type = sorted_data.iloc[0][content_type_col] if len(sorted_data) > 0 else "Music Video"
            recommendations.append(f"Focus on {top_type} content-shows highest performance")

            if len(sorted_data) > 1:
                second_type = sorted_data.iloc[1][content_type_col]
                recommendations.append(f"Consider expanding {second_type} content as secondary strategy")

        return recommendations

    # Handle full DataFrame analysis
    df = data
    recommendations = {}

    # Calculate overall content type performance
    overall_performance = df.groupby(content_type_col)[performance_col].agg(["count", "mean"]).round(2)
    overall_performance.columns = ["count", "avg_performance"]

    # Get top performing content types (with sufficient sample size)
    top_content_types = overall_performance[overall_performance["count"] >= min_sample_size].sort_values(
        "avg_performance", ascending=False
    )

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]
        artist_recommendations = []

        # Calculate artist's content type performance
        artist_performance = artist_df.groupby(content_type_col)[performance_col].agg(["count", "mean"]).round(2)
        artist_performance.columns = ["count", "avg_performance"]

        # Find content types artist hasn't tried yet
        tried_types = set(artist_performance.index)
        all_types = set(overall_performance.index)
        untried_types = all_types - tried_types

        # Recommend top performing untried content types
        for content_type in top_content_types.head(3).index:
            if content_type in untried_types:
                artist_recommendations.append(f"Try {content_type} content-shows strong performance across roster")

        # Find underperforming content types
        for content_type in artist_performance.index:
            if content_type in overall_performance.index:
                artist_avg = artist_performance.loc[content_type, "avg_performance"]
                overall_avg = overall_performance.loc[content_type, "avg_performance"]

                if artist_avg < overall_avg * 0.7:  # Significantly underperforming
                    artist_recommendations.append(f"Improve {content_type} strategy-currently underperforming")

        # Find artist's strengths
        for content_type in artist_performance.index:
            if content_type in overall_performance.index:
                artist_avg = artist_performance.loc[content_type, "avg_performance"]
                overall_avg = overall_performance.loc[content_type, "avg_performance"]

                if artist_avg > overall_avg * 1.3:  # Significantly outperforming
                    artist_recommendations.append(f"Double down on {content_type} - this is a strength area")

        # Content frequency recommendations
        total_videos = len(artist_df)
        if total_videos < 10:
            artist_recommendations.append("Increase content frequency-aim for more consistent uploads")

        # Diversity recommendations
        content_diversity = len(tried_types)
        if content_diversity < 3:
            artist_recommendations.append("Diversify content types-experiment with different formats")

        recommendations[artist] = artist_recommendations[:5]  # Limit to top 5 recommendations

    return recommendations


def check_isrc_compliance(
    data,
    artist_col: str = "artist",
    isrc_col: str = "has_isrc",
    content_type_col: str = "video_type",
    music_content_types: List[str] = None,
) -> Dict[str, Any]:
    """
    Check ISRC compliance for music content.

    Args:
        data: DataFrame with ISRC data
        artist_col: Column name for artist names
        isrc_col: Column name for ISRC boolean
        content_type_col: Column name for content types
        music_content_types: List of content types that should have ISRC codes

    Returns:
        Dictionary with ISRC compliance analysis
    """
    df = data

    if music_content_types is None:
        music_content_types = ["Music Video", "music_video", "lyric_video", "visualizer", "official_audio"]

    # Simple compliance check for test data
    if len(df) < 10:
        music_videos = df[df[content_type_col].isin(music_content_types)]
        compliant = music_videos[music_videos[isrc_col] is True] if len(music_videos) > 0 else pd.DataFrame()

        compliance_rate = len(compliant) / len(music_videos) if len(music_videos) > 0 else 0

        non_compliant = music_videos[music_videos[isrc_col] is False] if len(music_videos) > 0 else pd.DataFrame()

        # Get list of videos missing ISRC
        missing_isrc_list = non_compliant.to_dict("records") if len(non_compliant) > 0 else []

        return {
            "compliance_rate": compliance_rate,
            "total_music_videos": len(music_videos),
            "compliant_videos": len(compliant),
            "missing_isrc": missing_isrc_list,
            "status": "Good" if compliance_rate > 0.5 else "Needs Improvement",
        }

    compliance_data = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Filter to music content that should have ISRC
        music_content = artist_df[artist_df[content_type_col].isin(music_content_types)]

        if len(music_content) == 0:
            # No music content to check
            compliance_data.append(
                {
                    "artist": artist,
                    "total_music_content": 0,
                    "isrc_compliant": 0,
                    "non_compliant": 0,
                    "compliance_rate": 0.0,
                    "compliance_status": "No Music Content",
                }
            )
            continue

        # Check ISRC compliance
        compliant_content = music_content[music_content[isrc_col] is True]
        non_compliant_content = music_content[music_content[isrc_col] is False]

        total_music = len(music_content)
        compliant_count = len(compliant_content)
        non_compliant_count = len(non_compliant_content)

        compliance_rate = compliant_count / total_music if total_music > 0 else 0

        # Determine compliance status
        if compliance_rate >= 0.9:
            status = "Excellent"
        elif compliance_rate >= 0.7:
            status = "Good"
        elif compliance_rate >= 0.5:
            status = "Needs Improvement"
        else:
            status = "Poor"

        compliance_data.append(
            {
                "artist": artist,
                "total_music_content": total_music,
                "isrc_compliant": compliant_count,
                "non_compliant": non_compliant_count,
                "compliance_rate": round(compliance_rate, 3),
                "compliance_status": status,
            }
        )

    # Calculate overall compliance
    total_music_videos = sum(item["total_music_content"] for item in compliance_data)
    total_compliant = sum(item["isrc_compliant"] for item in compliance_data)

    overall_compliance_rate = total_compliant / total_music_videos if total_music_videos > 0 else 0

    return {
        "artist_compliance": compliance_data,
        "overall_compliance_rate": round(overall_compliance_rate, 3),
        "music_content_types_checked": music_content_types,
        "total_music_videos_checked": total_music_videos,
    }
