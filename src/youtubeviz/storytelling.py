from __future__ import annotations

import random
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import warnings

import pandas as pd


def _join_bullets(lines: Sequence[str]) -> str:
    items = [f"<li>{l}</li>" for l in lines if str(l).strip()]
    return "\n".join(items)


def story_block(
    fig,
    title: str,
    bullets: Iterable[str],
    caption: Optional[str] = None,
    width_left: str = "58%",
    theme: str = "light",
    return_html: bool = False,
):
    """Display a chart with a human narrative beside it in notebooks.

    - Places an interactive Plotly/Altair figure on the left
    - Places concise, humanized bullets on the right
    - Falls back to returning a tuple if IPython is not available

    Args:
        fig: Plotly/Altair figure (or any object with ``to_html`` or rich repr)
        title: Short, engaging headline
        bullets: Key points that explain the chart and the story
        caption: Optional one-line caption (tone, takeaway)
        width_left: CSS width for chart column
        theme: "light" or "dark" (affects text colors)
    """
    try:
        from IPython.display import HTML, display  # type: ignore
    except Exception:
        # Outside IPython: just return the content so callers can handle
        return {"figure": fig, "title": title, "bullets": list(bullets), "caption": caption}

    # Render figure HTML if supported, else rely on notebook renderer
    fig_html: str
    try:
        # Plotly
        fig_html = fig.to_html(include_plotlyjs="cdn", full_html=False)  # type: ignore[attr-defined]
    except Exception:
        try:
            # Altair
            fig_html = fig.to_html()  # type: ignore[attr-defined]
        except Exception:
            # Fallback: rely on IPython repr
            fig_html = f"<div class='figure-fallback'>{fig}</div>"

    text_color = "#111" if theme == "light" else "#fafafa"
    sub_color = "#444" if theme == "light" else "#ddd"

    html = f"""
    <div style="display:flex; gap:18px; align-items:flex-start; width:100%;">
      <div style="flex:0 0 {width_left}; max-width:{width_left};">{fig_html}</div>
      <div style="flex:1; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; color:{text_color};">
        <h3 style="margin:0 0 6px 0; font-weight:700;">{title}</h3>
        <ul style="margin:6px 0 8px 18px; padding:0; line-height:1.4;">{_join_bullets(list(bullets))}</ul>
        {f"<div style='font-size:12px; color:{sub_color}; margin-top:4px;'>{caption}</div>" if caption else ''}
      </div>
    </div>
    """
    if return_html:
        return html
    display(HTML(html))
    return None


def quick_takeaways(
    artist: str,
    last_7d_change_pct: Optional[float] = None,
    engagement_rate: Optional[float] = None,
    standout_video: Optional[str] = None,
) -> list[str]:
    """Generate friendly, executive-style bullets for a single artist.

    This is intentionally simple and safe; callers can pass real metrics.
    """
    out: list[str] = []
    if last_7d_change_pct is not None:
        arrow = "⤴️" if last_7d_change_pct >= 0 else "⤵️"
        out.append(f"Momentum {arrow} last 7 days: {last_7d_change_pct:+.1f}%")
    if engagement_rate is not None:
        out.append(f"Fan engagement: {engagement_rate:.1f}% — community leaning in 🎧")
    if standout_video:
        out.append(f"Breakout track: “{standout_video}” — consider boosting promo 💸")
    if not out:
        out.append("Steady performance — watch for emerging spikes and collabs 🤝")
    return out


def narrative_intro(
    analysis_type: str = "artist_comparison",
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate engaging narrative introductions for different analysis types.

    Creates fun, educational openings that set the stage for data exploration
    while explaining the business context and what readers will discover.

    Args:
        analysis_type: Type of analysis (artist_comparison, sentiment_deep_dive, etc.)
        context: Optional context dict with artist names, date ranges, etc.

    Returns:
        Markdown-formatted introduction text
    """
    context = context or {}

    if analysis_type == "artist_comparison":
        artists = context.get("artists", ["our featured artists"])
        if isinstance(artists, list) and len(artists) > 1:
            artist_list = ", ".join(artists[:-1]) + f" and {artists[-1]}"
        else:
            artist_list = str(artists[0]) if artists else "our featured artists"

        intros = [
            f"🎵 **The Music Data Detective Story** 🕵️‍♀️\n\nWelcome to the fascinating world where music meets data science! Today we're diving deep into the YouTube performance of {artist_list}. Think of this as your backstage pass to understanding how artists build their digital empires, one view at a time.\n\n*What makes an artist's content resonate? How do engagement patterns reveal fan loyalty? Let's find out together!*",
            f"🚀 **From Bedroom Studios to Billboard Charts** 📈\n\nEvery chart-topping artist started somewhere, and YouTube has become the modern equivalent of playing local venues. We're analyzing {artist_list} to uncover the data-driven secrets behind their success.\n\n*Spoiler alert: It's not just about the music anymore. It's about understanding your audience, timing your releases, and building genuine connections through content.*",
            f"💡 **The Algorithm Whisperers** 🤖\n\nIn today's music industry, understanding YouTube's algorithm is as important as understanding chord progressions. We're examining how {artist_list} navigate this digital landscape, turning data insights into career momentum.\n\n*Ready to see how the sausage gets made? Let's decode the patterns that separate viral hits from hidden gems.*",
        ]

        return random.choice(intros)

    elif analysis_type == "sentiment_analysis":
        return "💬 **Reading Between the Lines** 📊\n\nComments sections are the modern equivalent of fan mail, and they're goldmines of insight. We're using sentiment analysis to understand how audiences really feel about content, beyond just likes and views.\n\n*Every comment tells a story. Let's listen to what the data is saying.*"

    else:
        return f"📊 **Data-Driven Music Insights** 🎶\n\nWelcome to an exploration of {analysis_type}! We're combining the art of music with the science of data to uncover insights that can shape careers and inform decisions.\n\n*Let's turn numbers into narratives and metrics into music industry magic.*"


def educational_sidebar(
    concept: str,
    complexity_level: str = "beginner",
) -> str:
    """Generate educational sidebars explaining music industry and data science concepts.

    Creates engaging explanations that help readers understand both the technical
    and business aspects of what they're seeing in the analysis.

    Args:
        concept: The concept to explain (engagement_rate, momentum, etc.)
        complexity_level: beginner, intermediate, or advanced

    Returns:
        Markdown-formatted educational content
    """
    explanations = {
        "engagement_rate": {
            "beginner": "📚 **What's Engagement Rate?**\n\nEngagement rate measures how actively fans interact with content beyond just watching. It includes likes, comments, shares, and saves divided by total views.\n\n*Think of it like applause at a concert - views are attendance, but engagement shows how much the audience loved the show!*",
            "intermediate": "📊 **Deep Dive: Engagement Metrics**\n\nEngagement rate = (Likes + Comments + Shares) / Views × 100\n\nHigh engagement (>3%) suggests strong fan loyalty and algorithmic favor. Low engagement might indicate passive consumption or content-audience mismatch.\n\n*Industry benchmark: 2-4% is solid, 5%+ is exceptional for established artists.*",
            "advanced": "🔬 **Engagement Rate Analytics**\n\nEngagement velocity (rate of engagement over time) often predicts viral potential better than absolute numbers. Consider engagement quality (comment sentiment, share context) alongside quantity.\n\n*Advanced tip: Engagement patterns in first 24 hours strongly correlate with long-term performance and algorithmic promotion.*",
        },
        "momentum": {
            "beginner": "🚀 **Understanding Momentum**\n\nMomentum tracks how fast an artist's metrics are changing. Positive momentum means growing views, subscribers, or engagement. It's like measuring if a song is climbing or falling on the charts.\n\n*Momentum matters more than absolute numbers for investment decisions!*",
            "intermediate": "📈 **Momentum Calculations**\n\nWe calculate momentum using percentage change over rolling time windows (7-day, 30-day). Sustained positive momentum across multiple metrics indicates genuine growth vs. one-hit wonders.\n\n*Key insight: Consistent 10% monthly growth often outperforms sporadic viral spikes.*",
            "advanced": "⚡ **Advanced Momentum Analysis**\n\nMomentum analysis includes trend decomposition, seasonality adjustment, and cross-metric correlation. Leading indicators (comment sentiment, subscriber velocity) often predict view momentum.\n\n*Pro tip: Momentum inflection points often coincide with strategic content pivots or external events.*",
        },
        "youtube_algorithm": {
            "beginner": "🤖 **The YouTube Algorithm Explained**\n\nYouTube's algorithm decides which videos get recommended to viewers. It considers watch time, engagement, click-through rates, and viewer behavior patterns.\n\n*Think of it as a digital DJ that learns what each listener likes and creates personalized playlists!*",
            "intermediate": "🎯 **Algorithm Optimization Strategies**\n\nKey factors: Session duration, audience retention curves, engagement velocity, and topic authority. The algorithm rewards creators who keep viewers on the platform longer.\n\n*Strategy: Focus on series content and playlists to increase session watch time.*",
            "advanced": "🧠 **Algorithmic Ranking Factors**\n\nMulti-objective optimization balancing user satisfaction, advertiser value, and creator ecosystem health. Recent updates emphasize authentic engagement over vanity metrics.\n\n*Advanced insight: Cross-video engagement patterns and subscriber notification rates heavily influence reach.*",
        },
    }

    if concept in explanations and complexity_level in explanations[concept]:
        return explanations[concept][complexity_level]

    return f"💡 **About {concept.replace('_', ' ').title()}**\n\nThis is an important concept in music industry analytics. Understanding {concept} helps artists and labels make data-driven decisions about content strategy and resource allocation."


def section_transition(
    from_section: str,
    to_section: str,
    key_insight: Optional[str] = None,
) -> str:
    """Generate smooth narrative transitions between analysis sections.

    Creates engaging bridges that maintain story flow while preparing readers
    for the next type of analysis or visualization.

    Args:
        from_section: The section we're leaving
        to_section: The section we're entering
        key_insight: Optional key insight to bridge the sections

    Returns:
        Markdown-formatted transition text
    """
    transitions = {
        ("overview", "comparison"): [
            "Now that we've set the stage, let's dive into the head-to-head comparison. This is where the real insights emerge! 🥊",
            "With the landscape mapped out, it's time to zoom in on the competitive dynamics. Who's winning the engagement game? 🏆",
            "The overview gave us the big picture - now let's get tactical and see how these artists stack up against each other. 📊",
        ],
        ("comparison", "deep_dive"): [
            "The numbers tell one story, but let's dig deeper into what's driving these patterns. Time for some detective work! 🔍",
            "Surface-level metrics are just the beginning. Let's dig deep and uncover the strategic insights hiding in the data. 💎",
            "Interesting patterns are emerging! Let's investigate what's really happening behind these trends. 🕵️‍♀️",
        ],
        ("deep_dive", "recommendations"): [
            "All this analysis leads to one crucial question: What should we do about it? Let's get strategic! 💡",
            "Data without action is just pretty charts. Time to turn these insights into investment decisions. 💰",
            "The evidence is clear - now let's translate these findings into concrete next steps. 🎯",
        ],
        ("analysis", "sentiment"): [
            "Numbers tell us what happened, but sentiment analysis reveals how people feel about it. Let's listen to the audience! 💬",
            "Beyond the metrics lies the human story. What are fans actually saying in the comments? 🗣️",
            "Time to add the human element to our data story. Sentiment analysis reveals the emotional connection. ❤️",
        ],
    }

    key = (from_section.lower(), to_section.lower())
    if key in transitions:
        transition = random.choice(transitions[key])
    else:
        transition = f"Let's shift our focus from {from_section} to explore {to_section}. 🔄"

    if key_insight:
        transition += f"\n\n*Key insight from {from_section}: {key_insight}*"

    return f"\n---\n\n{transition}\n"


def chart_context(
    chart_type: str,
    what_to_look_for: List[str],
    business_implications: Optional[List[str]] = None,
) -> str:
    """Generate context that explains what to look for in visualizations.

    Helps readers understand how to interpret charts and what patterns
    indicate success, problems, or opportunities in the music industry.

    Args:
        chart_type: Type of chart (line_chart, bar_chart, scatter_plot, etc.)
        what_to_look_for: List of patterns or elements to focus on
        business_implications: Optional list of what patterns mean for business

    Returns:
        Markdown-formatted chart reading guide
    """
    chart_guides = {
        "line_chart": "📈 **Reading the Timeline**\n\nLine charts show trends over time. Look for:",
        "bar_chart": "📊 **Comparing Performance**\n\nBar charts make comparisons easy. Focus on:",
        "scatter_plot": "🎯 **Finding Relationships**\n\nScatter plots reveal correlations. Watch for:",
        "heatmap": "🌡️ **Pattern Recognition**\n\nHeatmaps show intensity patterns. Notice:",
        "pie_chart": "🥧 **Understanding Proportions**\n\nPie charts show how parts make up the whole. Examine:",
    }

    guide_start = chart_guides.get(
        chart_type, f"📊 **Understanding This {chart_type.replace('_', ' ').title()}**\n\nLook for:"
    )

    # Format what to look for
    lookfor_bullets = "\n".join([f"• {item}" for item in what_to_look_for])

    result = f"{guide_start}\n\n{lookfor_bullets}"

    # Add business implications if provided
    if business_implications:
        implications_bullets = "\n".join([f"• {item}" for item in business_implications])
        result += f"\n\n**💼 Business Impact:**\n\n{implications_bullets}"

    return result


# ============================================================================
# ERROR HANDLING AND DATA VALIDATION FUNCTIONS
# ============================================================================


class StorytellingDataError(Exception):
    """Custom exception for storytelling data validation errors."""

    pass


class DataQualityWarning(UserWarning):
    """Warning for data quality issues that don't prevent analysis."""

    pass


def validate_data_for_storytelling(
    data: pd.DataFrame,
    required_columns: List[str],
    analysis_type: str = "general",
    min_rows: int = 1,
) -> Dict[str, Any]:
    """
    Validate data quality for storytelling analysis with educational error messages.

    Args:
        data: DataFrame to validate
        required_columns: List of columns that must be present
        analysis_type: Type of analysis for context-specific validation
        min_rows: Minimum number of rows required

    Returns:
        Dict with validation results and recommendations

    Raises:
        StorytellingDataError: For critical validation failures
    """
    validation_result = {
        "is_valid": True,
        "warnings": [],
        "errors": [],
        "recommendations": [],
        "confidence_score": 1.0,
        "data_quality_issues": [],
    }

    # Check if data exists
    if data is None or data.empty:
        validation_result["is_valid"] = False
        validation_result["errors"].append(
            "📊 **No Data Available** 📊\n\n"
            "We couldn't find any data to analyze! This could happen for several reasons:\n"
            "• The database might be empty or not properly connected\n"
            "• Your date range might be too restrictive\n"
            "• The artists you're looking for might not be in our dataset\n\n"
            "💡 **What to try next:**\n"
            "• Check your database connection\n"
            "• Expand your date range\n"
            "• Verify artist names are spelled correctly"
        )
        raise StorytellingDataError("No data available for analysis")

    # Check minimum rows
    if len(data) < min_rows:
        validation_result["warnings"].append(
            f"📉 **Limited Data Warning** 📉\n\n"
            f"We found only {len(data)} rows of data, which might not be enough for reliable {analysis_type} analysis. "
            f"For best results, we recommend at least {min_rows} data points.\n\n"
            f"💡 **This might affect:**\n"
            f"• Statistical significance of trends\n"
            f"• Reliability of comparisons\n"
            f"• Confidence in recommendations"
        )
        validation_result["confidence_score"] *= 0.7

    # Check required columns
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        validation_result["is_valid"] = False
        validation_result["errors"].append(
            f"🔍 **Missing Data Columns** 🔍\n\n"
            f"We're missing some essential data columns for {analysis_type} analysis:\n"
            f"Missing: {', '.join(missing_columns)}\n"
            f"Available: {', '.join(data.columns.tolist())}\n\n"
            f"💡 **This usually means:**\n"
            f"• The data extraction didn't complete properly\n"
            f"• Database schema has changed\n"
            f"• Different data source than expected"
        )
        raise StorytellingDataError(f"Missing required columns: {', '.join(missing_columns)}")

    # Check for null values in critical columns
    for col in required_columns:
        if col in data.columns:
            null_count = data[col].isnull().sum()
            null_pct = (null_count / len(data)) * 100

            if null_pct > 50:
                validation_result["warnings"].append(
                    f"⚠️ **High Missing Data in {col}** ⚠️\n\n"
                    f"{null_pct:.1f}% of {col} values are missing. This could significantly impact analysis quality.\n\n"
                    f"💡 **Consider:**\n"
                    f"• Filtering out incomplete records\n"
                    f"• Using alternative metrics\n"
                    f"• Investigating data collection issues"
                )
                validation_result["confidence_score"] *= 0.8
                validation_result["data_quality_issues"].append(f"high_nulls_{col}")
            elif null_pct > 10:
                validation_result["warnings"].append(
                    f"📝 **Some Missing Data in {col}** 📝\n\n"
                    f"{null_pct:.1f}% of {col} values are missing. Analysis will continue but results may be affected."
                )
                validation_result["confidence_score"] *= 0.9

    # Analysis-specific validations
    if analysis_type == "artist_comparison":
        _validate_artist_comparison_data(data, validation_result)
    elif analysis_type == "sentiment_analysis":
        _validate_sentiment_data(data, validation_result)
    elif analysis_type == "trend_analysis":
        _validate_trend_data(data, validation_result)

    return validation_result


def _validate_artist_comparison_data(data: pd.DataFrame, validation_result: Dict[str, Any]) -> None:
    """Validate data specifically for artist comparison analysis."""
    if "artist_name" in data.columns:
        unique_artists = data["artist_name"].nunique()
        if unique_artists < 2:
            validation_result["warnings"].append(
                "🎤 **Single Artist Detected** 🎤\n\n"
                "We found data for only one artist. Artist comparison works best with multiple artists to compare.\n\n"
                "💡 **Suggestions:**\n"
                "• Add more artists to your analysis\n"
                "• Consider switching to single-artist deep dive\n"
                "• Check if artist names are being grouped correctly"
            )
            validation_result["confidence_score"] *= 0.6
        elif unique_artists > 10:
            validation_result["warnings"].append(
                f"📊 **Many Artists ({unique_artists})** 📊\n\n"
                "With many artists, comparisons might become cluttered. Consider focusing on top performers or grouping by category."
            )


def _validate_sentiment_data(data: pd.DataFrame, validation_result: Dict[str, Any]) -> None:
    """Validate data specifically for sentiment analysis."""
    if "comment_count" in data.columns:
        total_comments = data["comment_count"].sum()
        if total_comments < 100:
            validation_result["warnings"].append(
                "💬 **Limited Comment Data** 💬\n\n"
                f"Only {total_comments} total comments found. Sentiment analysis is more reliable with larger comment volumes.\n\n"
                "💡 **This affects:**\n"
                "• Statistical significance of sentiment scores\n"
                "• Ability to detect sentiment trends\n"
                "• Confidence in audience insights"
            )
            validation_result["confidence_score"] *= 0.7


def _validate_trend_data(data: pd.DataFrame, validation_result: Dict[str, Any]) -> None:
    """Validate data specifically for trend analysis."""
    if "published_at" in data.columns or "metrics_date" in data.columns:
        date_col = "published_at" if "published_at" in data.columns else "metrics_date"
        try:
            data[date_col] = pd.to_datetime(data[date_col])
            date_range = (data[date_col].max() - data[date_col].min()).days

            if date_range < 7:
                validation_result["warnings"].append(
                    f"📅 **Short Time Range ({date_range} days)** 📅\n\n"
                    "Trend analysis works best with longer time periods. Consider expanding your date range for more reliable trends."
                )
                validation_result["confidence_score"] *= 0.8
        except Exception:
            validation_result["warnings"].append(
                "📅 **Date Format Issues** 📅\n\n"
                "Having trouble parsing dates in your data. This might affect trend calculations."
            )


def create_confidence_indicator(
    confidence_score: float, data_quality_issues: List[str], analysis_type: str = "analysis"
) -> str:
    """
    Create a visual confidence indicator for analysis results.

    Args:
        confidence_score: Float between 0 and 1 indicating confidence level
        data_quality_issues: List of data quality issue identifiers
        analysis_type: Type of analysis for context

    Returns:
        HTML-formatted confidence indicator
    """
    if confidence_score >= 0.9:
        color = "#28a745"  # Green
        icon = "🟢"
        level = "High"
        message = f"Excellent data quality! This {analysis_type} is highly reliable."
    elif confidence_score >= 0.7:
        color = "#ffc107"  # Yellow
        icon = "🟡"
        level = "Medium"
        message = f"Good data quality with minor issues. This {analysis_type} is generally reliable."
    elif confidence_score >= 0.5:
        color = "#fd7e14"  # Orange
        icon = "🟠"
        level = "Low"
        message = f"Some data quality concerns. Interpret this {analysis_type} with caution."
    else:
        color = "#dc3545"  # Red
        icon = "🔴"
        level = "Very Low"
        message = f"Significant data quality issues. This {analysis_type} should be used for exploration only."

    # Add specific issue details
    issue_details = ""
    if data_quality_issues:
        issue_map = {
            "high_nulls": "High missing data",
            "limited_data": "Limited data volume",
            "date_issues": "Date formatting problems",
            "single_artist": "Single artist only",
            "many_artists": "Too many artists",
        }

        issues_text = []
        for issue in data_quality_issues:
            for key, description in issue_map.items():
                if key in issue:
                    issues_text.append(description)

        if issues_text:
            issue_details = f"<br><small>Issues: {', '.join(issues_text)}</small>"

    return f"""
    <div style="
        background: {color}15;
        border-left: 4px solid {color};
        padding: 12px;
        margin: 10px 0;
        border-radius: 4px;
        font-family: system-ui, sans-serif;
    ">
        <strong>{icon} Confidence Level: {level} ({confidence_score:.0%})</strong><br>
        <span style="color: #666;">{message}{issue_details}</span>
    </div>
    """


def handle_missing_data_gracefully(
    data: pd.DataFrame, column: str, fallback_strategy: str = "exclude", context: str = "analysis"
) -> tuple[pd.DataFrame, str]:
    """
    Handle missing data with educational explanations.

    Args:
        data: DataFrame with potential missing data
        column: Column to handle missing data for
        fallback_strategy: How to handle missing data (exclude, fill_zero, fill_mean)
        context: Context for educational messaging

    Returns:
        Tuple of (cleaned_data, explanation_message)
    """
    if column not in data.columns:
        return data, f"⚠️ Column '{column}' not found in data"

    missing_count = data[column].isnull().sum()
    total_count = len(data)
    missing_pct = (missing_count / total_count) * 100 if total_count > 0 else 0

    if missing_count == 0:
        return data, ""

    explanation = f"📊 **Handling Missing Data in {column}** 📊\n\n"
    explanation += f"Found {missing_count} missing values ({missing_pct:.1f}% of data) in {column}.\n\n"

    if fallback_strategy == "exclude":
        cleaned_data = data.dropna(subset=[column])
        explanation += f"**Strategy:** Excluding rows with missing {column} values.\n"
        explanation += f"**Impact:** Analysis will use {len(cleaned_data)} rows instead of {total_count}.\n"
        explanation += (
            f"**Why this works:** For {context}, complete data gives more reliable results than estimated values."
        )

    elif fallback_strategy == "fill_zero":
        cleaned_data = data.copy()
        cleaned_data[column] = cleaned_data[column].fillna(0)
        explanation += f"**Strategy:** Replacing missing {column} values with 0.\n"
        explanation += f"**Impact:** Assumes missing values represent zero activity.\n"
        explanation += f"**Why this works:** For metrics like engagement, missing often means no activity occurred."

    elif fallback_strategy == "fill_mean":
        mean_value = data[column].mean()
        cleaned_data = data.copy()
        cleaned_data[column] = cleaned_data[column].fillna(mean_value)
        explanation += f"**Strategy:** Replacing missing {column} values with average ({mean_value:.2f}).\n"
        explanation += f"**Impact:** Maintains overall statistical properties while filling gaps.\n"
        explanation += (
            f"**Why this works:** For {context}, average values provide reasonable estimates without skewing trends."
        )

    else:
        cleaned_data = data
        explanation += f"**Strategy:** No action taken - missing values remain.\n"
        explanation += f"**Impact:** Some calculations may be affected by missing data."

    return cleaned_data, explanation


def _test_imports():
    """Helper function to test imports - can be mocked for testing"""
    import src.youtubeviz.charts
    import src.youtubeviz.data
    import src.youtubeviz.storytelling

    return src.youtubeviz.storytelling


def validate_package_installation() -> bool:
    """
    Verify that 'pip install -e .' was run and src.youtubeviz is importable.

    This function tests the critical import paths that were causing the
    ModuleNotFoundError in notebooks.

    Returns:
        bool: True if all imports work, False if any fail
    """
    try:
        # Test the critical imports that were failing
        storytelling_module = _test_imports()

        # Verify key functions are available
        assert hasattr(storytelling_module, "story_block")
        assert hasattr(storytelling_module, "quick_takeaways")

        return True

    except ImportError as e:
        print(f"❌ Package installation issue: {e}")
        print("💡 Solution: Run 'pip install -e .' in the project root directory")
        return False
    except AssertionError as e:
        print(f"❌ Package structure issue: Missing expected functions")
        print("💡 Check that the storytelling module has the required functions")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during package validation: {e}")
        return False


def generate_data_quality_report(data: pd.DataFrame, analysis_type: str = "general") -> str:
    """
    Generate a comprehensive data quality report for educational purposes.

    Args:
        data: DataFrame to analyze
        analysis_type: Type of analysis for context

    Returns:
        Markdown-formatted data quality report
    """
    if data is None or data.empty:
        return "❌ **No data available for quality assessment**"

    report = f"📋 **Data Quality Report for {analysis_type.title()} Analysis** 📋\n\n"

    # Basic data info
    report += f"**Dataset Overview:**\n"
    report += f"• Total records: {len(data):,}\n"
    report += f"• Columns: {len(data.columns)}\n"
    report += f"• Memory usage: {data.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB\n\n"

    # Missing data analysis
    missing_data = data.isnull().sum()
    if missing_data.sum() > 0:
        report += f"**Missing Data Analysis:**\n"
        for col in missing_data[missing_data > 0].index:
            missing_count = missing_data[col]
            missing_pct = (missing_count / len(data)) * 100
            if missing_pct > 20:
                status = "🔴"
            elif missing_pct > 5:
                status = "🟡"
            else:
                status = "🟢"
            report += f"• {col}: {missing_count:,} missing ({missing_pct:.1f}%) {status}\n"
        report += "\n"
    else:
        report += "✅ **No missing data detected**\n\n"

    # Data type analysis
    report += f"**Data Types:**\n"
    for dtype in data.dtypes.value_counts().index:
        count = data.dtypes.value_counts()[dtype]
        report += f"• {dtype}: {count} columns\n"
    report += "\n"

    # Numeric columns summary
    numeric_cols = data.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        report += f"**Numeric Data Summary:**\n"
        for col in numeric_cols[:5]:  # Limit to first 5 numeric columns
            series = data[col].dropna()
            if len(series) > 0:
                report += f"• {col}: min={series.min():.2f}, max={series.max():.2f}, mean={series.mean():.2f}\n"
        if len(numeric_cols) > 5:
            report += f"• ... and {len(numeric_cols) - 5} more numeric columns\n"
        report += "\n"

    # Recommendations
    report += f"**💡 Recommendations for {analysis_type.title()} Analysis:**\n"

    if missing_data.sum() > len(data) * 0.1:  # More than 10% missing data overall
        report += "• Consider data cleaning or imputation strategies\n"

    if len(data) < 100:
        report += "• Small dataset - results may have limited statistical significance\n"

    if len(data.columns) > 50:
        report += "• Large number of columns - consider feature selection\n"

    report += "• Validate data freshness and accuracy with source systems\n"
    report += "• Consider outlier detection for numeric metrics\n"

    return report


def create_error_recovery_suggestions(error_type: str, context: Dict[str, Any] = None) -> str:
    """
    Generate helpful error recovery suggestions with educational context.

    Args:
        error_type: Type of error encountered
        context: Additional context about the error

    Returns:
        Markdown-formatted recovery suggestions
    """
    context = context or {}

    suggestions = {
        "no_data": {
            "title": "🔍 **No Data Found**",
            "explanation": "This usually happens when your filters are too restrictive or the data hasn't been collected yet.",
            "steps": [
                "Check your date range - try expanding it",
                "Verify artist names are spelled correctly",
                "Confirm the database connection is working",
                "Check if data collection is up to date",
            ],
            "learn_more": "Data availability depends on ETL pipeline runs and API rate limits.",
        },
        "insufficient_data": {
            "title": "📊 **Insufficient Data for Analysis**",
            "explanation": "We need more data points to generate reliable insights and statistical significance.",
            "steps": [
                "Expand your date range to include more time periods",
                "Add more artists to your comparison",
                "Lower the minimum threshold requirements",
                "Consider switching to a different analysis type",
            ],
            "learn_more": "Most statistical analyses need at least 30 data points for meaningful results.",
        },
        "data_quality": {
            "title": "⚠️ **Data Quality Issues Detected**",
            "explanation": "The data has quality issues that might affect analysis reliability.",
            "steps": [
                "Review the data quality report above",
                "Consider filtering out problematic records",
                "Check if recent data collection had issues",
                "Use confidence indicators to interpret results",
            ],
            "learn_more": "Data quality issues are common in real-world analytics - the key is understanding their impact.",
        },
        "calculation_error": {
            "title": "🧮 **Calculation Error**",
            "explanation": "Something went wrong during the analysis calculations.",
            "steps": [
                "Check for division by zero or invalid operations",
                "Verify all required columns are present",
                "Look for unexpected data types or formats",
                "Try simplifying the analysis parameters",
            ],
            "learn_more": "Calculation errors often indicate data format mismatches or edge cases.",
        },
    }

    if error_type not in suggestions:
        return f"❌ **Unknown Error Type: {error_type}**\n\nPlease check the logs for more details."

    suggestion = suggestions[error_type]

    result = f"{suggestion['title']}\n\n"
    result += f"{suggestion['explanation']}\n\n"
    result += "**🔧 What to try:**\n"

    for i, step in enumerate(suggestion["steps"], 1):
        result += f"{i}. {step}\n"

    result += f"\n💡 **Learn more:** {suggestion['learn_more']}"

    # Add context-specific information
    if context:
        result += f"\n\n**Context:**\n"
        for key, value in context.items():
            result += f"• {key}: {value}\n"

    return result
