from __future__ import annotations

import random
from typing import Iterable, Optional, Sequence, Dict, Any, List


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
            
            f"💡 **The Algorithm Whisperers** 🤖\n\nIn today's music industry, understanding YouTube's algorithm is as important as understanding chord progressions. We're examining how {artist_list} navigate this digital landscape, turning data insights into career momentum.\n\n*Ready to see how the sausage gets made? Let's decode the patterns that separate viral hits from hidden gems.*"
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
            
            "advanced": "🔬 **Engagement Rate Analytics**\n\nEngagement velocity (rate of engagement over time) often predicts viral potential better than absolute numbers. Consider engagement quality (comment sentiment, share context) alongside quantity.\n\n*Advanced tip: Engagement patterns in first 24 hours strongly correlate with long-term performance and algorithmic promotion.*"
        },
        
        "momentum": {
            "beginner": "🚀 **Understanding Momentum**\n\nMomentum tracks how fast an artist's metrics are changing. Positive momentum means growing views, subscribers, or engagement. It's like measuring if a song is climbing or falling on the charts.\n\n*Momentum matters more than absolute numbers for investment decisions!*",
            
            "intermediate": "📈 **Momentum Calculations**\n\nWe calculate momentum using percentage change over rolling time windows (7-day, 30-day). Sustained positive momentum across multiple metrics indicates genuine growth vs. one-hit wonders.\n\n*Key insight: Consistent 10% monthly growth often outperforms sporadic viral spikes.*",
            
            "advanced": "⚡ **Advanced Momentum Analysis**\n\nMomentum analysis includes trend decomposition, seasonality adjustment, and cross-metric correlation. Leading indicators (comment sentiment, subscriber velocity) often predict view momentum.\n\n*Pro tip: Momentum inflection points often coincide with strategic content pivots or external events.*"
        },
        
        "youtube_algorithm": {
            "beginner": "🤖 **The YouTube Algorithm Explained**\n\nYouTube's algorithm decides which videos get recommended to viewers. It considers watch time, engagement, click-through rates, and viewer behavior patterns.\n\n*Think of it as a digital DJ that learns what each listener likes and creates personalized playlists!*",
            
            "intermediate": "🎯 **Algorithm Optimization Strategies**\n\nKey factors: Session duration, audience retention curves, engagement velocity, and topic authority. The algorithm rewards creators who keep viewers on the platform longer.\n\n*Strategy: Focus on series content and playlists to increase session watch time.*",
            
            "advanced": "🧠 **Algorithmic Ranking Factors**\n\nMulti-objective optimization balancing user satisfaction, advertiser value, and creator ecosystem health. Recent updates emphasize authentic engagement over vanity metrics.\n\n*Advanced insight: Cross-video engagement patterns and subscriber notification rates heavily influence reach.*"
        }
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
            "The overview gave us the big picture - now let's get tactical and see how these artists stack up against each other. 📊"
        ],
        
        ("comparison", "deep_dive"): [
            "The numbers tell one story, but let's dig deeper into what's driving these patterns. Time for some detective work! 🔍",
            "Surface-level metrics are just the beginning. Let's dig deep and uncover the strategic insights hiding in the data. 💎",
            "Interesting patterns are emerging! Let's investigate what's really happening behind these trends. 🕵️‍♀️"
        ],
        
        ("deep_dive", "recommendations"): [
            "All this analysis leads to one crucial question: What should we do about it? Let's get strategic! 💡",
            "Data without action is just pretty charts. Time to turn these insights into investment decisions. 💰",
            "The evidence is clear - now let's translate these findings into concrete next steps. 🎯"
        ],
        
        ("analysis", "sentiment"): [
            "Numbers tell us what happened, but sentiment analysis reveals how people feel about it. Let's listen to the audience! 💬",
            "Beyond the metrics lies the human story. What are fans actually saying in the comments? 🗣️",
            "Time to add the human element to our data story. Sentiment analysis reveals the emotional connection. ❤️"
        ]
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
        "pie_chart": "🥧 **Understanding Proportions**\n\nPie charts show how parts make up the whole. Examine:"
    }
    
    guide_start = chart_guides.get(chart_type, f"📊 **Understanding This {chart_type.replace('_', ' ').title()}**\n\nLook for:")
    
    # Format what to look for
    lookfor_bullets = "\n".join([f"• {item}" for item in what_to_look_for])
    
    result = f"{guide_start}\n\n{lookfor_bullets}"
    
    # Add business implications if provided
    if business_implications:
        implications_bullets = "\n".join([f"• {item}" for item in business_implications])
        result += f"\n\n**💼 Business Impact:**\n\n{implications_bullets}"
    
    return result