"""
Sentiment analysis functions for YouTube comments and fan feedback.
Implements fact-based sentiment summarization with extractive approach.
"""

from __future__ import annotations

from collections import Counter
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def extract_top_positive_comments(
    df: pd.DataFrame, artist_col: str, comment_col: str, sentiment_col: str, top_n: int = 3
) -> Dict[str, List[str]]:
    """
    Extract top N positive comments for each artist.

    Args:
        df: DataFrame with comments and sentiment data
        artist_col: Column name for artist names
        comment_col: Column name for comment text
        sentiment_col: Column name for sentiment category
        top_n: Number of top comments to extract

    Returns:
        Dictionary mapping artist names to lists of positive comments
    """
    result = {}

    # Filter for positive sentiment
    positive_df = df[df[sentiment_col] == "positive"].copy()

    for artist in df[artist_col].unique():
        artist_comments = positive_df[positive_df[artist_col] == artist]

        # Get top N comments (could be enhanced with sentiment scores)
        top_comments = artist_comments[comment_col].head(top_n).tolist()
        result[artist] = top_comments

    return result


def extract_top_negative_comments_with_percentages(
    df: pd.DataFrame, artist_col: str, comment_col: str, sentiment_col: str, top_n: int = 3
) -> Dict[str, Dict[str, Any]]:
    """
    Extract top N negative comments with sentiment percentage breakdown.

    Args:
        df: DataFrame with comments and sentiment data
        artist_col: Column name for artist names
        comment_col: Column name for comment text
        sentiment_col: Column name for sentiment category
        top_n: Number of top negative comments to extract

    Returns:
        Dictionary with artist data including negative comments and percentages
    """
    result = {}

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate sentiment percentages
        sentiment_counts = artist_df[sentiment_col].value_counts()
        total_comments = len(artist_df)

        positive_pct = (sentiment_counts.get("positive", 0) / total_comments) * 100
        negative_pct = (sentiment_counts.get("negative", 0) / total_comments) * 100

        # Get top negative comments
        negative_comments = artist_df[artist_df[sentiment_col] == "negative"][comment_col].head(top_n).tolist()

        result[artist] = {
            "negative_comments": negative_comments,
            "positive_percentage": round(positive_pct, 2),
            "negative_percentage": round(negative_pct, 2),
        }

    return result


def identify_standout_videos(
    df: pd.DataFrame,
    views_col: str,
    positive_sentiment_col: str,
    min_positive_threshold: float = 80.0,
    max_views_threshold: int = 50000,
) -> pd.DataFrame:
    """
    Identify videos with high positive sentiment but normal view counts for experimentation.

    Args:
        df: DataFrame with video data
        views_col: Column name for view counts
        positive_sentiment_col: Column name for positive sentiment percentage
        min_positive_threshold: Minimum positive sentiment percentage
        max_views_threshold: Maximum view count to be considered "normal"

    Returns:
        DataFrame with standout videos meeting criteria
    """
    standout_videos = df[
        (df[positive_sentiment_col] >= min_positive_threshold) & (df[views_col] <= max_views_threshold)
    ].copy()

    return standout_videos


def analyze_roster_sentiment(
    df: pd.DataFrame, artist_col: str, sentiment_col: str, comment_col: str
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze sentiment across entire roster and categorize fan types.

    Args:
        df: DataFrame with sentiment data
        artist_col: Column name for artist names
        sentiment_col: Column name for sentiment category
        comment_col: Column name for comment text

    Returns:
        Dictionary with sentiment analysis for each artist
    """
    result = {}

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate sentiment distribution
        sentiment_counts = artist_df[sentiment_col].value_counts()
        total_comments = len(artist_df)

        positive_ratio = sentiment_counts.get("positive", 0) / total_comments

        # Categorize fan type based on sentiment patterns
        if positive_ratio >= 0.8:
            fan_type = "enthusiastic"
        elif positive_ratio >= 0.6:
            fan_type = "supportive"
        elif positive_ratio >= 0.4:
            fan_type = "mixed"
        else:
            fan_type = "critical"

        # Determine sentiment summary
        if positive_ratio >= 0.7:
            sentiment_summary = "positive"
        elif positive_ratio >= 0.4:
            sentiment_summary = "mixed"
        else:
            sentiment_summary = "negative"

        # Determine tour compatibility based on fan engagement style
        if fan_type in ["enthusiastic", "supportive"]:
            tour_compatibility = "high_energy"
        elif fan_type == "mixed":
            tour_compatibility = "moderate_energy"
        else:
            tour_compatibility = "intimate_venue"

        result[artist] = {
            "fan_type": fan_type,
            "sentiment_summary": sentiment_summary,
            "tour_compatibility": tour_compatibility,
            "positive_ratio": round(positive_ratio, 3),
        }

    return result


def group_artists_for_tours(sentiment_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Group artists by fan type compatibility for tour planning.

    Args:
        sentiment_analysis: Results from analyze_roster_sentiment()

    Returns:
        Dictionary with tour grouping recommendations
    """
    # Group by tour compatibility
    compatibility_groups = {}

    for artist, analysis in sentiment_analysis.items():
        # Get tour compatibility, or derive it from fan_type if not present
        compatibility = analysis.get("tour_compatibility")

        if compatibility is None:
            # Derive compatibility from fan_type
            fan_type = analysis.get("fan_type", "unknown")
            if fan_type in ["enthusiastic", "supportive", "energetic"]:
                compatibility = "high_energy"
            elif fan_type == "mixed":
                compatibility = "moderate_energy"
            elif fan_type in ["critical", "intense"]:
                compatibility = "intimate_venue"
            else:
                compatibility = "moderate_energy"

        if compatibility not in compatibility_groups:
            compatibility_groups[compatibility] = []

        compatibility_groups[compatibility].append(
            {
                "artist": artist,
                "fan_type": analysis.get("fan_type", "unknown"),
                "sentiment_summary": analysis.get("sentiment_summary", "unknown"),
            }
        )

    return {
        "tour_groups": compatibility_groups,
        "recommendations": _generate_tour_recommendations(compatibility_groups),
    }


def _generate_tour_recommendations(compatibility_groups: Dict[str, List[Dict]]) -> List[str]:
    """Generate tour recommendations based on compatibility groups"""
    recommendations = []

    for compatibility, artists in compatibility_groups.items():
        if len(artists) > 1:
            artist_names = [a["artist"] for a in artists]
            recommendations.append(f"{compatibility.replace('_', ' ').title()} tour: {', '.join(artist_names)}")

    return recommendations


def calculate_sentiment_percentages(df: pd.DataFrame, artist_col: str, sentiment_col: str) -> pd.DataFrame:
    """
    Calculate sentiment percentages for each artist.

    Args:
        df: DataFrame with sentiment data
        artist_col: Column name for artist names
        sentiment_col: Column name for sentiment category

    Returns:
        DataFrame with sentiment percentages by artist
    """
    # Group by artist and calculate sentiment percentages
    sentiment_stats = df.groupby([artist_col, sentiment_col]).size().unstack(fill_value=0)

    # Calculate percentages
    sentiment_percentages = sentiment_stats.div(sentiment_stats.sum(axis=1), axis=0) * 100

    # Reset index and rename columns
    result = sentiment_percentages.reset_index()

    # Ensure we have positive and negative columns
    if "positive" not in result.columns:
        result["positive"] = 0
    if "negative" not in result.columns:
        result["negative"] = 0

    # Rename columns for clarity
    result = result.rename(columns={"positive": "positive_pct", "negative": "negative_pct"})

    # Round percentages
    result["positive_pct"] = result["positive_pct"].round(2)
    result["negative_pct"] = result["negative_pct"].round(2)

    return result


def validate_sentiment_model_performance(
    predictions: List[float], actual: List[str], threshold: float = 0.0
) -> Dict[str, Any]:
    """
    Validate custom sentiment model performance with educational explanation.

    Args:
        predictions: List of sentiment scores from model
        actual: List of actual sentiment labels
        threshold: Threshold for converting scores to binary classification

    Returns:
        Dictionary with performance metrics and explanation
    """
    # Convert predictions to binary based on threshold
    pred_binary = ["positive" if score > threshold else "negative" for score in predictions]

    # Calculate metrics
    correct = sum(1 for p, a in zip(pred_binary, actual) if p == a)
    total = len(actual)
    accuracy = correct / total if total > 0 else 0

    # Calculate precision and recall for positive class
    true_positives = sum(1 for p, a in zip(pred_binary, actual) if p == "positive" and a == "positive")
    false_positives = sum(1 for p, a in zip(pred_binary, actual) if p == "positive" and a == "negative")
    false_negatives = sum(1 for p, a in zip(pred_binary, actual) if p == "negative" and a == "positive")

    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0

    # Generate educational explanation
    explanation = _generate_model_explanation(accuracy, precision, recall)

    return {
        "accuracy": round(accuracy, 3),
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "explanation": explanation,
        "sample_size": total,
    }


def _generate_model_explanation(accuracy: float, precision: float, recall: float) -> str:
    """Generate educational explanation of model performance"""
    explanation = f"""
    📊 **Sentiment Model Performance Analysis**

    **Accuracy ({accuracy:.1%})**: How often the model is correct overall.
    - For music industry sentiment, {accuracy:.1%} accuracy is {'excellent' if accuracy > 0.85 else 'good' if accuracy > 0.7 else 'needs improvement'}.
    - Music comments often use slang and context that general models miss.

    **Precision ({precision:.1%})**: When model predicts positive, how often is it right?
    - High precision means fewer false positives (wrongly calling negative comments positive).

    **Recall ({recall:.1%})**: How many actual positive comments does the model catch?
    - High recall means we don't miss positive fan feedback.

    **Why 95% accuracy isn't required for music industry:**
    - Music sentiment is subjective and context-dependent
    - Slang and cultural references are constantly evolving
    - Fan emotions are complex (can love song but hate video quality)
    - Our custom training on music slang makes domain-specific accuracy more valuable
    """

    return explanation.strip()


def detect_music_slang(comments: List[str]) -> Dict[str, Any]:
    """
    Detect and analyze music industry slang in comments.

    Args:
        comments: List of comment strings

    Returns:
        Dictionary with slang analysis results
    """
    # Music industry slang terms (positive and negative)
    positive_slang = {
        "fire",
        "banger",
        "slaps",
        "hits different",
        "no cap",
        "bussin",
        "goes hard",
        "absolute unit",
        "chef's kiss",
        "vibes",
        "mood",
    }

    negative_slang = {"mid", "trash", "cap", "cringe", "flop", "ain't it", "nah fam", "not it", "skip", "pass"}

    all_slang = positive_slang | negative_slang

    # Find slang terms in comments
    found_terms = []
    slang_sentiment_impact = {"positive": 0, "negative": 0}

    for comment in comments:
        comment_lower = comment.lower()

        for term in all_slang:
            if term in comment_lower:
                found_terms.append(term)

                if term in positive_slang:
                    slang_sentiment_impact["positive"] += 1
                else:
                    slang_sentiment_impact["negative"] += 1

    # Count frequency of each term
    term_frequency = Counter(found_terms)

    return {
        "slang_terms_found": list(term_frequency.keys()),
        "term_frequency": dict(term_frequency),
        "slang_sentiment_impact": slang_sentiment_impact,
        "total_slang_instances": len(found_terms),
    }
