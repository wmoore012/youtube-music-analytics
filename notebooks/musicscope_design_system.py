"""
MusicScope™ Design System
==========================
Unified design tokens, UI components, and visual helpers for the MusicScope™ dashboard.

This module provides:
- Design tokens: colors, thresholds, chart purposes
- Safety/validation: data requirement checks
- UI components: hero cards, subsection cards, insight cards, closing cards
- Visual helpers: Plotly/Matplotlib styling utilities

Usage:
    from musicscope_design_system import (
        ms_hero_card, ms_subsection_card, ms_insight_card, ms_closing_card,
        ms_apply_plotly_layout, ms_matplotlib_title, ms_clean_spines,
        ms_require_data, MUSICSCOPE_COLORS, MOMENTUM_THRESHOLDS, CHART_PURPOSE
    )
"""

from __future__ import annotations
from typing import Any, Optional
import pandas as pd

# Optional IPython display support (for notebooks vs. terminal runs)
try:  # pragma: no cover - environment detection
    from IPython.display import display, HTML  # type: ignore
    from IPython import get_ipython  # type: ignore
    _MS_HAS_IPYTHON = get_ipython() is not None
except Exception:
    _MS_HAS_IPYTHON = False
    display = None  # type: ignore[assignment]

    class HTML(str):  # type: ignore[no-redef]
        """Fallback HTML wrapper for non-notebook environments."""

        pass

# ============================================================================
# DESIGN TOKENS
# ============================================================================

MUSICSCOPE_COLORS = {
    # Brand gradients
    "brand_purple_start": "#667eea",
    "brand_purple_end": "#764ba2",
    "brand_pink_start": "#f093fb",
    "brand_pink_end": "#f5576c",
    "brand_blue_start": "#4facfe",
    "brand_blue_end": "#00f2fe",
    
    # Status colors
    "success_green": "#10b981",
    "warning_orange": "#f59e0b",
    "urgent_red": "#ef4444",
    "info_blue": "#3b82f6",
    
    # Neutral palette
    "baseline_gray": "#9ca3af",
    "text_primary": "#1f2937",
    "text_secondary": "#6b7280",
    "background_light": "#f9fafb",
    
    # Sentiment colors
    "positive_green": "#22c55e",
    "neutral_gray": "#94a3b8",
    "negative_red": "#ef4444",
}

MOMENTUM_THRESHOLDS = {
    "pre_breakout": 55,
    "breakout": 60,
    "legacy": 75,
    "high_momentum": 75,
    "medium_momentum": 47,
}

CHART_PURPOSE = {
    # Momentum charts
    "momentum_threshold_control": {
        "question": "How do different thresholds change what we call a 'breakout'?",
        "action": "Use this to calibrate how strict your breakout definition should be.",
    },
    "momentum_bar_race": {
        "question": "Which videos are climbing fastest day-to-day?",
        "action": "Watch for purple bars with ⚡ — these are your next investment candidates.",
    },
    "momentum_bar_race_historical": {
        "question": "Where did we miss breakouts in the past?",
        "action": "Red or highlighted bars indicate periods where demand spiked but budget did not follow.",
    },
    "breakout_calendar": {
        "question": "On which days does momentum naturally spike?",
        "action": "Cluster promotional pushes on the hottest days for maximum impact.",
    },
    "kpi22_breakout_timing": {
        "question": "How long do breakouts last, and how much warning do we get?",
        "action": "Use duration to plan campaign length; use warning time to prepare resources.",
    },
    "artist_momentum_tracker": {
        "question": "At a roster level, which artists are structurally strongest?",
        "action": "Use this to compare artists on consistent growth vs spikes.",
    },
    "breakout_kpi_card": {
        "question": "Are we currently in a breakout state worth escalating?",
        "action": "Use this card in exec reviews to justify reallocating spend this week.",
    },
    "growth_signal_breakdown": {
        "question": "Which growth signals are driving momentum across the catalog?",
        "action": "Use this to decide which levers (search, social, playlisting) deserve more budget.",
    },
    "budget_reallocation": {
        "question": "Where should we shift marketing budget this quarter?",
        "action": "Reallocate from plateaus to rising stars for maximum ROI.",
    },

    # Sentiment charts
    "sentiment_global_distribution": {
        "question": "Is the buzz positive, toxic, or indifferent?",
        "action": "NSS (Net Sentiment Score) is your portfolio health check — track it weekly.",
    },
    "sentiment_volatility": {
        "question": "Are audience emotions stable or swinging wildly?",
        "action": "High volatility = reputational risk; investigate spikes immediately.",
    },
    "sentiment_by_asset": {
        "question": "Which videos are loved vs. dragged?",
        "action": "Amplify positive content; audit or pull negative outliers.",
    },
    "sentiment_risk_pockets": {
        "question": "Where are the toxic comment clusters?",
        "action": "High-exposure + negative sentiment = crisis risk; escalate to PR team.",
    },
    "sentiment_polarity": {
        "question": "How do fans feel about each artist?",
        "action": "Positive sentiment = safe to invest; negative = needs crisis management.",
    },

    # Performance charts
    "performance_views_over_time": {
        "question": "Are views growing, stable, or declining?",
        "action": "Identify trends early to adjust strategy before it's too late.",
    },
    "performance_engagement_trends": {
        "question": "Is the audience actively engaging or passively watching?",
        "action": "High engagement = loyal fanbase; low engagement = content quality issue.",
    },
    "performance_content_type": {
        "question": "Which content formats drive the most value?",
        "action": "Double down on high-performing formats; test or cut underperformers.",
    },
    "performance_geographic": {
        "question": "Where are our strongest markets?",
        "action": "Localize campaigns for top markets; explore growth in emerging regions.",
    },
}

# ============================================================================
# SAFETY & VALIDATION
# ============================================================================

def ms_require_data(
    name: str,
    value: Any,
    required_cols: Optional[list[str]] = None,
    context: str = "",
) -> None:
    """
    Validate that required data exists and has expected columns.

    Args:
        name: Name of the variable (for error messages)
        value: The actual variable value
        required_cols: List of required column names (for DataFrames)
        context: Additional context for error messages (e.g., section name)

    Raises:
        ValueError: If data is missing or invalid
    """
    context_msg = f" in {context}" if context else ""

    if value is None:
        raise ValueError(
            f"❌ {name} is not defined{context_msg}.\n"
            f"   → Make sure the data discovery section ran successfully."
        )

    if isinstance(value, pd.DataFrame):
        if value.empty:
            raise ValueError(
                f"❌ {name} is empty{context_msg}.\n"
                f"   → Check your data pipeline and filters."
            )

        if required_cols:
            missing = set(required_cols) - set(value.columns)
            if missing:
                raise ValueError(
                    f"❌ {name} is missing required columns: {missing}{context_msg}\n"
                    f"   → Available columns: {list(value.columns)}"
                )

# ============================================================================
# UI COMPONENTS
# ============================================================================

def ms_hero_card(
    title: str,
    body_html: str,
    gradient: str = "purple",
) -> None:
    """
    Display a hero card at the start of a major section.

    Args:
        title: Section title (e.g., "⚡ Momentum Intelligence")
        body_html: HTML content for the card body
        gradient: Color scheme - "purple", "pink", or "blue"
    """
    gradient_map = {
        "purple": f"{MUSICSCOPE_COLORS['brand_purple_start']}, {MUSICSCOPE_COLORS['brand_purple_end']}",
        "pink": f"{MUSICSCOPE_COLORS['brand_pink_start']}, {MUSICSCOPE_COLORS['brand_pink_end']}",
        "blue": f"{MUSICSCOPE_COLORS['brand_blue_start']}, {MUSICSCOPE_COLORS['brand_blue_end']}",
    }

    gradient_colors = gradient_map.get(gradient, gradient_map["purple"])

    html = f"""
    <div style="
        background: linear-gradient(135deg, {gradient_colors});
        border-radius: 12px;
        padding: 32px 40px;
        margin: 24px 0;
        color: white;
        font-family: Inter, 'Segoe UI', sans-serif;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    ">
        <h1 style="margin: 0 0 16px 0; font-size: 32px; font-weight: 700;">
            {title}
        </h1>
        <div style="font-size: 16px; line-height: 1.6;">
            {body_html}
        </div>
    </div>
    """
    if _MS_HAS_IPYTHON and display is not None:
        display(HTML(html))
    else:
        print(f"[MUSICSCOPE HERO] {title}")


def ms_subsection_card(
    title: str,
    subtitle: str,
) -> None:
    """
    Display a subsection header before a chart.

    Args:
        title: Chart title (e.g., "📊 Momentum Bar Race")
        subtitle: Business question or context
    """
    html = f"""
    <div style="
        background: {MUSICSCOPE_COLORS['background_light']};
        border-left: 4px solid {MUSICSCOPE_COLORS['brand_purple_start']};
        border-radius: 6px;
        padding: 16px 20px;
        margin: 20px 0 12px 0;
        font-family: Inter, 'Segoe UI', sans-serif;
    ">
        <h3 style="margin: 0 0 8px 0; font-size: 20px; font-weight: 600; color: {MUSICSCOPE_COLORS['text_primary']};">
            {title}
        </h3>
        <p style="margin: 0; font-size: 14px; color: {MUSICSCOPE_COLORS['text_secondary']}; line-height: 1.5;">
            {subtitle}
        </p>
    </div>
    """
    if _MS_HAS_IPYTHON and display is not None:
        display(HTML(html))
    else:
        print(f"[MUSICSCOPE SUBSECTION] {title} - {subtitle}")


def ms_insight_card(
    message: str,
    card_type: str = "info",
) -> None:
    """
    Display an insight or recommendation card.

    Args:
        message: The insight text
        card_type: "info", "success", "warning", or "action"
    """
    type_config = {
        "info": {
            "icon": "💡",
            "bg_color": MUSICSCOPE_COLORS['info_blue'],
        },
        "success": {
            "icon": "✅",
            "bg_color": MUSICSCOPE_COLORS['success_green'],
        },
        "warning": {
            "icon": "⚠️",
            "bg_color": MUSICSCOPE_COLORS['warning_orange'],
        },
        "action": {
            "icon": "🎯",
            "bg_color": MUSICSCOPE_COLORS['brand_purple_start'],
        },
    }

    config = type_config.get(card_type, type_config["info"])

    html = f"""
    <div style="
        background: {config['bg_color']};
        border-radius: 8px;
        padding: 14px 20px;
        margin: 12px 0;
        color: white;
        font-family: Inter, 'Segoe UI', sans-serif;
        font-size: 15px;
        line-height: 1.5;
    ">
        <strong>{config['icon']}</strong> {message}
    </div>
    """
    if _MS_HAS_IPYTHON and display is not None:
        display(HTML(html))
    else:
        print(f"[MUSICSCOPE CARD] {message}")


def ms_closing_card(
    section_title: str = "Section",
    metrics: Optional[list[tuple[str, str]]] = None,
    next_section: str = "",
) -> None:
    """
    Display a closing card at the end of a major section.

    Args:
        section_title: Name of the section (e.g., "⚡ Momentum Intelligence")
        metrics: List of (icon, label) tuples for completed items
        next_section: Description of the next section
    """
    metrics_html = ""
    if metrics:
        metrics_items = "".join(
            f"<li style='margin: 4px 0;'>{icon} {label}</li>"
            for icon, label in metrics
        )
        metrics_html = f"""
        <ul style="
            list-style: none;
            padding: 0;
            margin: 12px 0;
            font-size: 14px;
            text-align: left;
            display: inline-block;
        ">
            {metrics_items}
        </ul>
        """

    next_html = ""
    if next_section:
        next_html = f"""
        <p style="margin: 16px 0 0 0; font-size: 14px; opacity: 0.9;">
            <strong>Next:</strong> {next_section}
        </p>
        """

    html = f"""
    <div style="
        background: linear-gradient(135deg, {MUSICSCOPE_COLORS['text_primary']}, {MUSICSCOPE_COLORS['text_secondary']});
        border-radius: 12px;
        padding: 24px 32px;
        margin: 32px 0;
        color: white;
        font-family: Inter, 'Segoe UI', sans-serif;
        text-align: center;
    ">
        <h3 style="margin: 0; font-size: 18px; font-weight: 600;">
            ✓ {section_title} Complete
        </h3>
        {metrics_html}
        {next_html}
    </div>
    """
    if _MS_HAS_IPYTHON and display is not None:
        display(HTML(html))
    else:
        print(f"[MUSICSCOPE CLOSING] {section_title}")

# ============================================================================
# VISUAL HELPERS
# ============================================================================

def ms_apply_plotly_layout(
    fig: Any,
    title: str,
    subtitle: str = "",
    height: int = 600,
) -> None:
    """
    Apply consistent MusicScope™ styling to a Plotly figure.

    Args:
        fig: Plotly figure object
        title: Main chart title
        subtitle: Subtitle or action text (optional)
        height: Chart height in pixels
    """
    title_text = title
    if subtitle:
        title_text = f"{title}<br><sub>{subtitle}</sub>"

    fig.update_layout(
        title=dict(
            text=title_text,
            x=0.5,
            xanchor="center",
            font=dict(size=18, family="Inter, 'Segoe UI', sans-serif"),
        ),
        template="plotly_white",
        height=height,
        font=dict(family="Inter, 'Segoe UI', sans-serif", size=12),
        margin=dict(t=100, b=60, l=80, r=80),
    )


def ms_status_colors(
    value: float,
    thresholds: Optional[dict[str, float]] = None,
) -> str:
    """
    Get status color based on momentum thresholds.

    Args:
        value: Momentum score or metric value
        thresholds: Custom thresholds (defaults to MOMENTUM_THRESHOLDS)

    Returns:
        Hex color code
    """
    if thresholds is None:
        thresholds = MOMENTUM_THRESHOLDS

    if value >= thresholds.get("high_momentum", 75):
        return MUSICSCOPE_COLORS["success_green"]
    elif value >= thresholds.get("medium_momentum", 47):
        return MUSICSCOPE_COLORS["warning_orange"]
    else:
        return MUSICSCOPE_COLORS["baseline_gray"]


def ms_matplotlib_title(
    ax: Any,
    title: str,
    subtitle: str = "",
    y_position: float = 1.05,
) -> None:
    """
    Add a styled title to a Matplotlib axis.

    Args:
        ax: Matplotlib axis object
        title: Main title text
        subtitle: Subtitle text (optional)
        y_position: Vertical position of title (default: 1.05)
    """
    ax.text(
        0.5,
        y_position,
        title,
        transform=ax.transAxes,
        fontsize=16,
        fontweight="bold",
        ha="center",
        va="top",
        color=MUSICSCOPE_COLORS["text_primary"],
        family="Inter, 'Segoe UI', sans-serif",
    )

    if subtitle:
        ax.text(
            0.5,
            y_position - 0.06,
            subtitle,
            transform=ax.transAxes,
            fontsize=12,
            ha="center",
            va="top",
            color=MUSICSCOPE_COLORS["text_secondary"],
            family="Inter, 'Segoe UI', sans-serif",
            style="italic",
        )


def ms_clean_spines(ax: Any) -> None:
    """
    Remove chart clutter by hiding spines.

    Args:
        ax: Matplotlib axis object
    """
    for spine in ["top", "right", "left", "bottom"]:
        ax.spines[spine].set_visible(False)
    ax.tick_params(left=False, bottom=False)

