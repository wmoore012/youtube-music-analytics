"""
MusicScope™ Momentum Intelligence Section

Renders the complete Momentum Intelligence section with signal-focused framing.
Uses hybrid architecture: design system for UI + existing helpers for calculations.

Expected Charts:
    - 1-2 charts total (control panel + KPI-22 if breakouts detected)
    - Control panel: Interactive threshold sliders (always rendered)
    - KPI-22: Breakout duration + warning window dual panel (conditional on breakouts)

Usage:
    # Production mode (strict validation)
    from tools.notebook_helpers.musicscope_momentum import render_momentum_section
    meta = render_momentum_section(videos_df, demo=False)

    # Demo mode (synthetic data for testing)
    meta = render_momentum_section(videos_df, demo=True)
"""

from __future__ import annotations
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from matplotlib.colors import ListedColormap
import calendar
from ipywidgets import interact, IntSlider

# Optional IPython display support (for notebooks vs. terminal runs)
try:  # pragma: no cover - environment detection
    from IPython.display import display, HTML  # type: ignore
    from IPython import get_ipython  # type: ignore
    _MS_HAS_IPYTHON = get_ipython() is not None
except Exception:
    _MS_HAS_IPYTHON = False
    display = None  # type: ignore[assignment]

    class HTML(str):  # type: ignore[no-redef]
        """Fallback HTML wrapper when IPython is unavailable."""

        pass

# Design System imports
from .musicscope_design_system import (
    ms_hero_card, ms_subsection_card, ms_insight_card, ms_closing_card,
    ms_apply_plotly_layout, ms_matplotlib_title, ms_clean_spines,
    ms_require_data, ms_status_colors, MUSICSCOPE_COLORS, MOMENTUM_THRESHOLDS, CHART_PURPOSE
)

# Existing helper imports (REUSE, don't reimplement!)
from tools.advanced_charts import (
    compute_kpi22_video_breakouts, ensure_year_on_dates, label_bars, direct_line_label,
    state_color, base100, modified_z
)
from tools.scoring import score_component_daily
from tools.momentum_bar_race import color_for_score, colors_for_scores
from tools.data_utils import resolve_artist_column, pick_content_column


# Expected chart counts for this section
EXPECTED_CHARTS = {
    "min": 3,  # Control panel + bar race + calendar
    "max": 5,  # Control panel + bar race + calendar + KPI-22 dual panel
    "description": "3–5 (control panel + bar race + calendar + KPI-22 if breakouts detected)"
}


def _build_demo_videos_df() -> pd.DataFrame:
    """
    Private helper: Build synthetic videos DataFrame for testing/demo only.

    Returns:
        DataFrame with minimal required columns for momentum analysis
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


def render_momentum_section(videos_df: pd.DataFrame, demo: bool = False) -> Dict[str, Any]:
    """
    Render the complete Momentum Intelligence section with signal framing.

    Parameters:
        videos_df: DataFrame with columns [artist_name, view_count, published_at, video_id]
        demo: If True, use synthetic data for testing; if False, require real data

    Returns:
        Dict with metadata: {
            'section': 'momentum',
            'charts_rendered': int,
            'artists_analyzed': int,
            'breakout_count': int,
            'momentum_daily': pd.DataFrame
        }
    """

    # Connection logging so notebook users can see the module is wired correctly
    print(f"[MusicScope Momentum v1.0] render_momentum_section() called — demo={demo}")

    # ============================================================================
    # HERO CARD
    # ============================================================================
    ms_hero_card(
        title="⚡ Momentum Intelligence",
        body_html=(
            "<strong>Core Question:</strong> Who is heating up, and when should we lean in?<br><br>"
            "This section surfaces:<br>"
            "• 🔥 <strong>Breakout artists</strong> — who crossed the momentum threshold<br>"
            "• 📅 <strong>Hottest days</strong> — when to schedule releases and campaigns<br>"
            "• ⏰ <strong>Breakout timing</strong> — how long momentum lasts and how much warning you get<br>"
            "• 📊 <strong>Growth signals</strong> — which artists need support vs which are self-sustaining"
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
            required_cols=["artist_name", "view_count", "published_at"],
            context="Momentum Intelligence"
        )
        videos = videos_df

    # ============================================================================
    # 2.1 — DATASET PREPARATION
    # ============================================================================
    print("🔄 Building momentum dataset...")

    # Detect artist column (handles artist_name, artist, channel_title, etc.)
    artist_col = resolve_artist_column(videos)

    # Aggregate to daily artist-level metrics
    momentum_daily = (
        videos
        .assign(date=pd.to_datetime(videos["published_at"]).dt.normalize())
        .groupby([artist_col, "date"], as_index=False)
        .agg(
            views=("view_count", "sum"),
            videos=("video_id", "nunique") if "video_id" in videos.columns else ("view_count", "count"),
        )
        .sort_values([artist_col, "date"])
        .rename(columns={artist_col: "artist_name"})
    )
    
    # Compute momentum score using existing helper (7-day rolling avg)
    momentum_daily["momentum_score"] = (
        momentum_daily
        .groupby("artist_name")["views"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )
    
    # Normalize to 0-100 scale
    max_score = momentum_daily["momentum_score"].max()
    if max_score > 0:
        momentum_daily["momentum_score"] = (momentum_daily["momentum_score"] / max_score) * 100.0
    
    print(f"✅ Momentum dataset ready: {len(momentum_daily):,} artist-days")
    
    # Track metadata
    metadata = {
        'section': 'momentum',
        'charts_rendered': 0,
        'artists_analyzed': momentum_daily["artist_name"].nunique(),
        'breakout_count': 0,
        'momentum_daily': momentum_daily
    }
    
    # ============================================================================
    # 2.2 — CONTROL PANEL (INTERACTIVE SLIDERS)
    # ============================================================================
    ms_subsection_card(
        title="🎚️ Momentum Control Panel",
        subtitle=CHART_PURPOSE.get("threshold_control", {}).get("question", "Adjust thresholds to tune breakout detection")
    )
    
    # Default thresholds from design system
    global pre_breakout_threshold, breakout_threshold, legacy_threshold
    pre_breakout_threshold = MOMENTUM_THRESHOLDS["pre_breakout"]
    breakout_threshold = MOMENTUM_THRESHOLDS["breakout"]
    legacy_threshold = MOMENTUM_THRESHOLDS["legacy"]
    
    @interact(
        pre_breakout=IntSlider(min=40, max=70, step=5, value=pre_breakout_threshold, description="Pre-Breakout"),
        breakout=IntSlider(min=50, max=80, step=5, value=breakout_threshold, description="Breakout"),
        legacy=IntSlider(min=60, max=90, step=5, value=legacy_threshold, description="Legacy"),
    )
    def update_thresholds(pre_breakout, breakout, legacy):
        """Interactive threshold tuning with live artist counts."""
        global pre_breakout_threshold, breakout_threshold, legacy_threshold
        pre_breakout_threshold = pre_breakout
        breakout_threshold = breakout
        legacy_threshold = legacy

        # Count artists in each tier
        latest = momentum_daily.sort_values("date").groupby("artist_name").tail(1)
        pre_count = ((latest["momentum_score"] >= pre_breakout) & (latest["momentum_score"] < breakout)).sum()
        breakout_count = ((latest["momentum_score"] >= breakout) & (latest["momentum_score"] < legacy)).sum()
        legacy_count = (latest["momentum_score"] >= legacy).sum()

        print(f"🔥 Breakout: {breakout_count} artists (≥{breakout})")
        print(f"📈 Pre-Breakout: {pre_count} artists ({pre_breakout}–{breakout})")
        print(f"🏆 Legacy: {legacy_count} artists (≥{legacy})")

        metadata['breakout_count'] = breakout_count

    print("✅ Control panel ready. Adjust sliders above to tune thresholds.")
    metadata['charts_rendered'] += 1

    # ============================================================================
    # 2.3 — MOMENTUM BAR RACE: WHO'S RISING OVER TIME?
    # ============================================================================
    if _MS_HAS_IPYTHON and display is not None:
        display(HTML("""
        <div style="
            background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
            border-radius: 12px;
            padding: 20px 28px;
            margin: 40px 0 24px 0;
            border-left: 6px solid #f76b1c;
        ">
            <h3 style="
                color: #2C3E50;
                font-size: 26px;
                font-weight: 700;
                margin: 0;
            ">🏁 Momentum Bar Race — Watch Artists Rise in Real-Time</h3>
            <p style="
                color: #34495E;
                font-size: 15px;
                margin: 8px 0 0 0;
            ">See which videos are building heat day by day. Blue bars = pre-breakout (invest now!)</p>
        </div>
        """))
    else:
        print("[MUSICSCOPE CARD] 🏁 Momentum Bar Race — Watch Artists Rise in Real-Time")

    # Prepare data for animated bar race
    race = momentum_daily.copy()
    race["date_str"] = race["date"].dt.strftime("%b %d, %Y")
    race["invest_label"] = np.where(race["momentum_score"] >= pre_breakout_threshold, "⚡ Invest", "")
    race["state"] = np.where(race["momentum_score"] >= pre_breakout_threshold, "pre_breakout", "baseline")

    # Calculate appropriate x-axis range
    max_score = float(race["momentum_score"].max()) if not race.empty else 60.0
    x_range = [0, max(60, max_score + 8)]

    # Create animated bar race
    frames = []
    dates = sorted(race["date_str"].unique())

    for date in dates:
        frame_data = race[race["date_str"] == date].sort_values("momentum_score")

        colors = [MUSICSCOPE_COLORS["brand_purple_start"] if state == "pre_breakout"
                  else MUSICSCOPE_COLORS["neutral_gray"] for state in frame_data["state"]]

        frames.append(go.Frame(
            data=[go.Bar(
                y=frame_data["artist_name"],
                x=frame_data["momentum_score"],
                orientation="h",
                marker=dict(color=colors, line=dict(color="white", width=1.5)),
                text=frame_data["invest_label"],
                textposition="outside",
                textfont=dict(size=11, color=MUSICSCOPE_COLORS["text_primary"]),
                hovertemplate="<b>%{y}</b><br>Score: %{x:.1f}<extra></extra>"
            )],
            name=date
        ))

    # Initial frame (first date)
    initial_data = race[race["date_str"] == dates[0]].sort_values("momentum_score")
    colors = [MUSICSCOPE_COLORS["brand_purple_start"] if state == "pre_breakout"
              else MUSICSCOPE_COLORS["neutral_gray"] for state in initial_data["state"]]

    fig = go.Figure(
        data=[go.Bar(
            y=initial_data["artist_name"],
            x=initial_data["momentum_score"],
            orientation="h",
            marker=dict(color=colors, line=dict(color="white", width=1.5)),
            text=initial_data["invest_label"],
            textposition="outside",
            textfont=dict(size=11, color=MUSICSCOPE_COLORS["text_primary"])
        )],
        frames=frames
    )

    # Apply design system layout
    ms_apply_plotly_layout(
        fig,
        title="Who's Building Momentum Right Now?",
        subtitle=f"Blue = Pre-breakout (≥{pre_breakout_threshold}) • Gray = Baseline • Press Play to watch rankings change",
        height=700
    )

    fig.update_layout(
        xaxis=dict(
            title="Momentum Score",
            range=x_range,
            showgrid=True,
            gridcolor="rgba(0,0,0,0.05)"
        ),
        yaxis=dict(
            title="",
            categoryorder="total ascending"
        ),
        updatemenus=[{
            "type": "buttons",
            "showactive": False,
            "buttons": [
                {
                    "label": "▶ Play",
                    "method": "animate",
                    "args": [None, {"frame": {"duration": 200}, "fromcurrent": True}]
                },
                {
                    "label": "⏸ Pause",
                    "method": "animate",
                    "args": [[None], {"frame": {"duration": 0}, "mode": "immediate"}]
                }
            ],
            "x": 0.5,
            "xanchor": "center",
            "y": -0.08,
            "yanchor": "top"
        }],
        margin=dict(l=20, r=100, t=100, b=60)
    )

    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)

    fig.show(config={'displayModeBar': False})

    ms_insight_card(
        message=f"💡 Blue bars show artists hitting {pre_breakout_threshold}+ momentum — these are your invest-now opportunities!",
        card_type="success"
    )
    metadata['charts_rendered'] += 1

    # ============================================================================
    # 2.4 — CALENDAR HEATMAP: HOTTEST DAYS FOR CAMPAIGN TIMING
    # ============================================================================
    if _MS_HAS_IPYTHON and display is not None:
        display(HTML("""
        <div style="
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            border-radius: 12px;
            padding: 20px 28px;
            margin: 40px 0 24px 0;
            border-left: 6px solid #c44569;
        ">
            <h3 style="
                color: white;
                font-size: 26px;
                font-weight: 700;
                margin: 0;
            ">📅 Breakout Calendar — When to Schedule Your Campaigns</h3>
            <p style="
                color: rgba(255, 255, 255, 0.95);
                font-size: 15px;
                margin: 8px 0 0 0;
            ">Cluster promotional pushes on hot days for maximum impact</p>
        </div>
        """))
    else:
        print("[MUSICSCOPE CARD] 📅 Breakout Calendar — When to Schedule Your Campaigns")

    # Prepare calendar data (last full month)
    md_cal = momentum_daily.assign(is_breakout=(momentum_daily["momentum_score"] >= breakout_threshold).astype(int))
    daily_breakouts = md_cal.groupby("date")["is_breakout"].sum()

    # Select last full month
    last_date = daily_breakouts.index.max()
    first_of_current = (last_date - pd.offsets.MonthBegin(1)).normalize()
    prev_month_end = first_of_current - pd.Timedelta(days=1)
    month_start = (prev_month_end - pd.offsets.MonthBegin(1)).normalize() + pd.offsets.MonthBegin(0)
    month_end = month_start + pd.offsets.MonthEnd(0)

    # Create date range for the month
    month_range = pd.date_range(month_start, month_end, freq="D")
    month_values = daily_breakouts.reindex(month_range).fillna(0).astype(int)

    # Build calendar matrix (weeks x days)
    cal = calendar.Calendar()
    weeks = cal.monthdayscalendar(month_start.year, month_start.month)

    matrix = np.zeros((len(weeks), 7), dtype=int)
    for week_idx, week in enumerate(weeks):
        for day_idx, day_num in enumerate(week):
            if day_num != 0:
                date = pd.Timestamp(year=month_start.year, month=month_start.month, day=day_num)
                matrix[week_idx, day_idx] = int(month_values.get(date, 0))

    # Create visualization
    fig_cal, ax_cal = plt.subplots(figsize=(14, 6), facecolor='white')

    # Color scale: gray → yellow → orange → magenta (0 to 3+ breakouts)
    cmap = ListedColormap([
        MUSICSCOPE_COLORS["neutral_gray"],
        MUSICSCOPE_COLORS["warning_orange"],
        MUSICSCOPE_COLORS["negative_red"],
        MUSICSCOPE_COLORS["brand_purple_end"]
    ])

    im = ax_cal.imshow(matrix, cmap=cmap, aspect="auto", vmin=0, vmax=3)

    # Format axes
    ax_cal.set_xticks(range(7))
    ax_cal.set_xticklabels(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                           fontsize=13, fontweight='600', color=MUSICSCOPE_COLORS["text_primary"])
    ax_cal.set_yticks(range(len(weeks)))
    ax_cal.set_yticklabels([f"Week {i+1}" for i in range(len(weeks))],
                           fontsize=12, color=MUSICSCOPE_COLORS["text_primary"])

    # Title with action-oriented insight
    month_name = month_start.strftime("%B %Y")
    ms_matplotlib_title(
        ax_cal,
        f"Daily Breakout Intensity — {month_name}",
        "Cluster promotional pushes on hot days for maximum impact",
        y_position=1.12
    )

    # Add count labels to non-zero cells
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            if matrix[i, j] > 0:
                ax_cal.text(
                    j, i, str(matrix[i, j]),
                    ha="center", va="center",
                    fontsize=14, color="white", fontweight="bold",
                    bbox=dict(boxstyle='circle,pad=0.3',
                             facecolor=MUSICSCOPE_COLORS["text_primary"],
                             alpha=0.6, edgecolor='none')
                )

    ms_clean_spines(ax_cal)
    plt.tight_layout()
    plt.show()

    # Insight card with hottest days
    top_days = month_values.nlargest(3)
    if len(top_days) > 0:
        hottest_days_html = "".join(
            f"<li>{date.strftime('%A, %b %d')}: {count} breakouts</li>"
            for date, count in top_days.items()
        )
        if _MS_HAS_IPYTHON and display is not None:
            display(HTML(f"""
            <div style="
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                border-radius: 8px;
                padding: 16px 24px;
                margin: 16px 0;
                color: white;
            ">
                <strong style="font-size: 16px;">🔥 Hottest Days in {month_name}:</strong><br>
                <ul style="margin: 8px 0 0 0; padding-left: 20px; font-size: 15px;">
                    {hottest_days_html}
                </ul>
                <p style="margin: 12px 0 0 0; font-size: 14px; opacity: 0.9;">
                    💡 <strong>Action:</strong> Schedule releases and campaigns on these high-momentum days
                </p>
            </div>
            """))
        else:
            print(f"[MUSICSCOPE CARD] 🔥 Hottest Days in {month_name}")

    metadata['charts_rendered'] += 1

    # ============================================================================
    # 2.5 — KPI-22: BREAKOUT DURATION + WARNING WINDOW
    # ============================================================================
    ms_subsection_card(
        title="⏰ KPI-22: Breakout Timing Intelligence",
        subtitle=CHART_PURPOSE.get("kpi22_breakout_timing", {}).get(
            "question",
            "How long do breakouts last, and how much warning do we get?"
        )
    )

    # Use existing helper function (don't reimplement!)
    if "video_id" in videos.columns:
        # Prepare data for KPI-22 (needs video_id, date, momentum_score)
        try:
            kpi22_input = (
                videos
                .assign(date=pd.to_datetime(videos["published_at"]).dt.normalize())
                .merge(
                    momentum_daily[["artist_name", "date", "momentum_score"]],
                    left_on=[artist_col, "date"],
                    right_on=["artist_name", "date"],
                    how="left"
                )
                .rename(columns={"video_id": "video_id"})
            )

            # Compute breakout episodes using existing helper
            breakouts = compute_kpi22_video_breakouts(
                kpi22_input,
                pre=pre_breakout_threshold,
                brk=breakout_threshold,
                cap_hours=720  # 30 days max
            )
        except KeyError as exc:
            ms_insight_card(
                message=f"KPI-22 panel skipped (missing column: {exc}). Check momentum pipeline.",
                card_type="warning"
            )
            breakouts = pd.DataFrame()  # Empty DataFrame to skip rendering

        if not breakouts.empty:
            # Create dual-panel matplotlib chart
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

            # Panel 1: Breakout Duration Distribution
            durations = breakouts["duration_hours"] / 24  # Convert to days
            ax1.hist(durations, bins=20, color=MUSICSCOPE_COLORS["brand_purple_start"], alpha=0.7, edgecolor="white")
            ms_matplotlib_title(ax1, "Breakout Duration", f"Median: {durations.median():.1f} days", y_position=1.05)
            ax1.set_xlabel("Duration (days)", fontsize=11)
            ax1.set_ylabel("Number of Breakouts", fontsize=11)
            ms_clean_spines(ax1)

            # Panel 2: Pre-Breakout Warning Window
            warnings_hrs = breakouts["pre_warning_hours"]
            warnings_days = warnings_hrs / 24
            ax2.hist(warnings_days, bins=20, color=MUSICSCOPE_COLORS["warning_orange"], alpha=0.7, edgecolor="white")
            ms_matplotlib_title(ax2, "Pre-Breakout Warning", f"Median: {warnings_days.median():.1f} days", y_position=1.05)
            ax2.set_xlabel("Warning Window (days)", fontsize=11)
            ax2.set_ylabel("Number of Breakouts", fontsize=11)
            ms_clean_spines(ax2)

            plt.tight_layout()
            plt.show()

            # Insight card
            avg_duration = durations.mean()
            avg_warning = warnings_days.mean()
            ms_insight_card(
                message=(
                    f"Average breakout lasts {avg_duration:.1f} days with {avg_warning:.1f} days warning → "
                    f"Plan {int(avg_duration)} day campaigns and monitor pre-breakout artists {int(avg_warning)} days ahead."
                ),
                card_type="success"
            )
            metadata['charts_rendered'] += 1
        else:
            ms_insight_card(
                message="No breakout episodes detected with current thresholds. Try lowering the breakout threshold.",
                card_type="info"
            )
    else:
        ms_insight_card(
            message="KPI-22 requires video_id column. Skipping breakout timing analysis.",
            card_type="info"
        )

    # ============================================================================
    # CLOSING CARD
    # ============================================================================
    latest = momentum_daily.sort_values("date").groupby("artist_name").tail(1)
    breakout_count = (latest["momentum_score"] >= breakout_threshold).sum()

    ms_closing_card(
        section_title="⚡ Momentum Intelligence",
        metrics=[
            ("🔥", f"{breakout_count} artists in breakout zone"),
            ("📊", f"{len(momentum_daily):,} artist-days analyzed"),
            ("🎚️", f"Thresholds: {pre_breakout_threshold}/{breakout_threshold}/{legacy_threshold}"),
            ("📈", f"{metadata['artists_analyzed']} artists tracked"),
            ("🎬", f"{metadata['charts_rendered']} visualizations rendered"),
        ],
        next_section="Sentiment Intelligence — understand how your audience feels"
    )

    print(f"\n✅ Momentum Intelligence section complete: {metadata['charts_rendered']} charts rendered")
    print(f"[DEBUG] Momentum charts rendered: {metadata['charts_rendered']} (expected {EXPECTED_CHARTS['description']})")
    return metadata
