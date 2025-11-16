"""
Auto-generated summary system for MusicScope™ analytics.
Creates intelligent summaries based on data patterns with compassionate insights.
"""

from datetime import datetime
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


def get_summary_config() -> Dict[str, Any]:
    """
    Get summary configuration from environment variables with sensible defaults.

    Returns:
        Dictionary with summary configuration parameters
    """
    return {
        "growth_threshold": float(os.getenv("SUMMARY_GROWTH_THRESHOLD", "0.1")),  # 10% growth threshold
        "sentiment_threshold": float(os.getenv("SUMMARY_SENTIMENT_THRESHOLD", "0.65")),  # 65% positive threshold
        "top_performer_percentile": int(os.getenv("SUMMARY_TOP_PERFORMER_PERCENTILE", "75")),  # Top 25%
        "summary_tone": os.getenv("SUMMARY_TONE", "compassionate"),  # Compassionate by default
        "min_data_points": int(os.getenv("SUMMARY_MIN_DATA_POINTS", "5")),  # Minimum data for analysis
        "engagement_threshold": float(os.getenv("SUMMARY_ENGAGEMENT_THRESHOLD", "4.0")),  # 4% engagement threshold
    }


def detect_performance_patterns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Detect performance patterns in artist data that trigger summary generation.

    Args:
        df: DataFrame with performance data (artist_name, views, date, engagement_rate)

    Returns:
        Dictionary with categorized artists based on performance patterns
    """
    if df.empty:
        return {
            "growth_artists": [],
            "declining_artists": [],
            "top_performers": [],
            "engagement_leaders": [],
            "stable_artists": [],
        }

    config = get_summary_config()
    patterns = {
        "growth_artists": [],
        "declining_artists": [],
        "top_performers": [],
        "engagement_leaders": [],
        "stable_artists": [],
    }

    # Determine the correct column name for views
    view_col = "views" if "views" in df.columns else "daily_views"

    # Group by artist and calculate metrics
    artist_metrics = (
        df.groupby("artist_name").agg({view_col: ["sum", "mean", "std"], "engagement_rate": "mean"}).round(3)
    )

    artist_metrics.columns = ["total_views", "avg_views", "view_volatility", "avg_engagement"]
    artist_metrics = artist_metrics.reset_index()

    # Calculate growth trends (simplified)
    for artist in artist_metrics["artist_name"]:
        artist_data = df[df["artist_name"] == artist].sort_values("date")

        if len(artist_data) >= config["min_data_points"]:  # noqa: F821
            # Simple growth calculation: compare first half vs second half
            mid_point = len(artist_data) // 2
            first_half_avg = artist_data[view_col].iloc[:mid_point].mean()
            second_half_avg = artist_data[view_col].iloc[mid_point:].mean()

            if first_half_avg > 0:
                growth_rate = (second_half_avg - first_half_avg) / first_half_avg

                if growth_rate > config["growth_threshold"]:  # noqa: F821
                    patterns["growth_artists"].append(artist)
                elif growth_rate < -config["growth_threshold"]:  # noqa: F821
                    patterns["declining_artists"].append(artist)
                else:
                    patterns["stable_artists"].append(artist)

    # Identify top performers by total views
    top_percentile = np.percentile(artist_metrics["total_views"], config["top_performer_percentile"])  # noqa: F821
    patterns["top_performers"] = artist_metrics[artist_metrics["total_views"] >= top_percentile]["artist_name"].tolist()

    # Identify engagement leaders
    patterns["engagement_leaders"] = artist_metrics[
        artist_metrics["avg_engagement"] >= config["engagement_threshold"] / 100  # noqa: F821
    ]["artist_name"].tolist()

    return patterns


def generate_executive_summary(
    performance_df: pd.DataFrame, sentiment_df: pd.DataFrame, content_df: pd.DataFrame, artist_col: str = "artist_name"
) -> str:
    """
    Generate executive summary based on performance, sentiment, and content data.

    Args:
        performance_df: DataFrame with performance metrics
        sentiment_df: DataFrame with sentiment analysis
        content_df: DataFrame with content analysis
        artist_col: Column name for artist names

    Returns:
        Executive summary as formatted string
    """
    _config = get_summary_config()  # noqa: F841

    # Detect patterns
    patterns = detect_performance_patterns(performance_df)

    # Calculate key metrics
    total_artists = len(performance_df[artist_col].unique())
    total_views = performance_df["daily_views"].sum()
    avg_engagement = performance_df["engagement_rate"].mean() * 100

    # Sentiment analysis
    positive_sentiment = (sentiment_df["sentiment_category"] == "positive").mean() * 100

    # Content analysis
    total_videos = len(content_df)
    isrc_coverage = content_df["has_isrc"].mean() * 100 if "has_isrc" in content_df.columns else 0

    # Build summary
    summary_lines = [
        f"📊 PORTFOLIO OVERVIEW: {total_artists} artists generated {total_views:,} total views",
        f"📈 ENGAGEMENT: {avg_engagement:.1f}% average engagement rate",
        f"💬 FAN SENTIMENT: {positive_sentiment:.1f}% positive fan feedback",
        f"🎼 CONTENT: {total_videos} videos with {isrc_coverage:.1f}% ISRC coverage",
        "",
    ]

    # Performance insights
    if patterns["growth_artists"]:
        summary_lines.append(f"🚀 RISING STARS: {', '.join(patterns['growth_artists'])} showing strong growth momentum")

    if patterns["top_performers"]:
        summary_lines.append(f"🏆 TOP PERFORMERS: {', '.join(patterns['top_performers'])} leading in total views")

    if patterns["engagement_leaders"]:
        summary_lines.append(
            f"💝 FAN FAVORITES: {', '.join(patterns['engagement_leaders'])} excelling in fan engagement"
        )

    if patterns["declining_artists"]:
        summary_lines.append(
            f"⚠️  NEEDS ATTENTION: {', '.join(patterns['declining_artists'])} require strategic support"
        )

    return "\n".join(summary_lines)


def create_actionable_recommendations(
    performance_df: pd.DataFrame, sentiment_df: pd.DataFrame, content_df: pd.DataFrame, artist_col: str = "artist_name"
) -> List[str]:
    """
    Create actionable recommendations based on data analysis.

    Args:
        performance_df: DataFrame with performance metrics
        sentiment_df: DataFrame with sentiment analysis
        content_df: DataFrame with content analysis
        artist_col: Column name for artist names

    Returns:
        List of actionable recommendation strings
    """
    recommendations = []
    patterns = detect_performance_patterns(performance_df)

    # Growth-based recommendations
    if patterns["growth_artists"]:
        recommendations.append(
            f"🚀 AMPLIFY SUCCESS: Increase marketing budget for {', '.join(patterns['growth_artists'])} "
            "while momentum is strong"
        )

    if patterns["declining_artists"]:
        recommendations.append(
            f"🔧 STRATEGIC PIVOT: Review content strategy for {', '.join(patterns['declining_artists'])} "
            "- consider new collaborations or genre exploration"
        )

    # Content recommendations
    if "has_isrc" in content_df.columns:
        low_isrc_artists = content_df.groupby(artist_col)["has_isrc"].mean()
        low_isrc_artists = low_isrc_artists[low_isrc_artists < 0.5].index.tolist()

        if low_isrc_artists:
            recommendations.append(
                f"🎼 INCREASE MUSIC RELEASES: {', '.join(low_isrc_artists)} need more official music content "
                "for streaming revenue optimization"
            )

    # Sentiment recommendations
    if not sentiment_df.empty:
        negative_sentiment_artists = sentiment_df.groupby(artist_col)["sentiment_category"].apply(
            lambda x: (x == "negative").mean()
        )
        high_negative = negative_sentiment_artists[negative_sentiment_artists > 0.4].index.tolist()

        if high_negative:
            recommendations.append(
                f"💬 FAN ENGAGEMENT: {', '.join(high_negative)} should focus on community building "
                "and addressing fan feedback"
            )

    # Engagement recommendations
    if patterns["engagement_leaders"]:
        recommendations.append(
            f"🤝 CROSS-PROMOTION: Use {', '.join(patterns['engagement_leaders'])} to boost "
            "visibility for developing artists through collaborations"
        )

    # General recommendations
    recommendations.extend(
        [
            "📊 MONTHLY REVIEW: Schedule regular analytics reviews to track progress",
            "🎯 A / B TEST: Experiment with different content types and posting schedules",
            "💝 ARTIST SUPPORT: Remember these are real people-celebrate wins and provide support during challenges",
        ]
    )

    return recommendations


def create_markdown_summary(
    performance_df: pd.DataFrame,
    sentiment_df: pd.DataFrame,
    content_df: pd.DataFrame,
    artist_col: str = "artist_name",
    output_file: Optional[str] = None,
) -> str:
    """
    Create a comprehensive markdown summary report.

    Args:
        performance_df: DataFrame with performance metrics
        sentiment_df: DataFrame with sentiment analysis
        content_df: DataFrame with content analysis
        artist_col: Column name for artist names
        output_file: Optional file path to save markdown

    Returns:
        Markdown formatted summary string
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Generate components
    executive_summary = generate_executive_summary(performance_df, sentiment_df, content_df, artist_col)
    recommendations = create_actionable_recommendations(performance_df, sentiment_df, content_df, artist_col)

    # Build markdown
    markdown_content = f"""# 🎵 MusicScope™ Analytics Report

**Generated**: {timestamp}
**Artists Analyzed**: {len(performance_df[artist_col].unique())}
**Data Period**: {performance_df['date'].min().strftime('%Y-%m-%d')} to {performance_df['date'].max().strftime('%Y-%m-%d')}  # noqa: E501

## 📊 Executive Summary

{executive_summary}

## 📋 Actionable Recommendations

"""

    for i, rec in enumerate(recommendations, 1):
        markdown_content += f"{i}. {rec}\n"

    markdown_content += f"""

## 📈 Key Metrics

- **Total Views**: {performance_df['daily_views'].sum():,}
- **Average Engagement**: {performance_df['engagement_rate'].mean() * 100:.1f}%
- **Positive Sentiment**: {(sentiment_df['sentiment_category'] == 'positive').mean() * 100:.1f}%
- **Content Videos**: {len(content_df)}
- **ISRC Coverage**: {content_df['has_isrc'].mean() * 100:.1f}%

---
*Generated by MusicScope™ Analytics-Treating artists as humans, not data points* 💝
"""

    if output_file:
        with open(output_file, "w") as f:
            f.write(markdown_content)
        print(f"📄 Summary saved to {output_file}")

    return markdown_content


def generate_markdown_summary(
    performance_df: pd.DataFrame, sentiment_df: pd.DataFrame, content_df: pd.DataFrame, artist_col: str = "artist_name"
) -> str:
    """
    Generate markdown summary based on current chart insights.

    Args:
        performance_df: DataFrame with performance metrics
        sentiment_df: DataFrame with sentiment analysis
        content_df: DataFrame with content analysis
        artist_col: Column name for artist names

    Returns:
        Markdown formatted summary string
    """
    return create_markdown_summary(performance_df, sentiment_df, content_df, artist_col)


def generate_compassionate_insights(
    performance_df: pd.DataFrame, sentiment_df: pd.DataFrame, content_df: pd.DataFrame, artist_col: str = "artist_name"
) -> List[str]:
    """
    Generate compassionate insights with hard truths and growth opportunities.

    Args:
        performance_df: DataFrame with performance metrics
        sentiment_df: DataFrame with sentiment analysis
        content_df: DataFrame with content analysis
        artist_col: Column name for artist names

    Returns:
        List of compassionate insight strings
    """
    insights = []
    patterns = detect_performance_patterns(performance_df)

    # Growth-focused insights
    if patterns["growth_artists"]:
        insights.append(
            f"🌟 **Rising Stars**: {', '.join(patterns['growth_artists'])} are building real momentum. "
            "Their fans are responding-this is the time to amplify their success with strategic support."
        )

    if patterns["declining_artists"]:
        insights.append(
            f"💪 **Growth Opportunities**: {', '.join(patterns['declining_artists'])} have incredible potential. "
            "Every artist goes through different phases-let's explore"
            " new creative directions and fan engagement strategies."
        )

    # Sentiment-based insights
    if not sentiment_df.empty:
        positive_artists = (
            sentiment_df.groupby(artist_col)["sentiment_category"]
            .apply(lambda x: (x == "positive").mean())
            .sort_values(ascending=False)
        )

        top_sentiment_artist = positive_artists.index[0] if len(positive_artists) > 0 else None
        if top_sentiment_artist:
            insights.append(
                f"❤️ **Fan Love**: {top_sentiment_artist} has the strongest positive fan sentiment. "
                "Their community is engaged and supportive-a great foundation for growth."
            )

    # Content strategy insights
    if "has_isrc" in content_df.columns:
        low_music_artists = content_df.groupby(artist_col)["has_isrc"].mean()
        low_music_artists = low_music_artists[low_music_artists < 0.3].index.tolist()

        if low_music_artists:
            insights.append(
                f"🎵 **Music Focus Opportunity**: {', '.join(low_music_artists)} could benefit from more official music releases. "
                "Fans are engaged with their content-now let's give them more music to stream and share."
            )

    # Encouragement and support
    insights.append(
        "🎯 **Remember**: Every artist's journey is unique. These"
        " insights help us support each artist's individual path to success."
    )

    return insights


def create_notebook_summary_section(
    performance_df: pd.DataFrame, sentiment_df: pd.DataFrame, content_df: pd.DataFrame, artist_col: str = "artist_name"
) -> str:
    """
    Create a complete notebook summary section with executive summary and insights.

    Args:
        performance_df: DataFrame with performance metrics
        sentiment_df: DataFrame with sentiment analysis
        content_df: DataFrame with content analysis
        artist_col: Column name for artist names

    Returns:
        Complete markdown section for notebook
    """
    executive_summary = generate_executive_summary(performance_df, sentiment_df, content_df, artist_col)
    compassionate_insights = generate_compassionate_insights(performance_df, sentiment_df, content_df, artist_col)
    recommendations = create_actionable_recommendations(performance_df, sentiment_df, content_df, artist_col)

    section = f"""
## 📊 Executive Summary

{executive_summary}

## 💝 Compassionate Insights

"""

    for insight in compassionate_insights:
        section += f"- {insight}\n"

    section += f"""
## 🎯 Actionable Recommendations

"""

    for i, rec in enumerate(recommendations, 1):
        section += f"{i}. {rec}\n"

    section += """
---
*Generated by MusicScope™ Analytics-Supporting artists with data-driven compassion* 💝
"""

    return section
