# %% [markdown]
# # 🎵 MusicScope™ YouTube Dashboard
# 
# **Production-quality analytics for A&R decision-making**
# 
# ---
# 
# ## 📋 Executive Summary
# 
# This dashboard supports **6 artists** whose videos we're actively promoting for viral breakout.
# 
# **Key Question:** Should we lower our initial breakout velocity threshold from **75 → 55**?
# 
# **Data Source:** YouTube Data API v3 only (no Analytics API, no CTR/impressions, no revenue)
# 
# ---
# 
# ## 🎯 Dashboard Sections
# 
# 1. **Artist & Audience Intelligence** — Roster overview, engagement patterns, current performance
# 2. **Breakout Threshold Experiment** — Compare outcomes at threshold 55 vs 75
# 3. **Budget Reallocation Strategies** — Three alternative approaches with pros/cons
# 4. **Fun FYI** — Additional insights (celebratory, non-blocking)
# 
# ---

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CONFIG & HUMAN-REVIEW GATES
# ═══════════════════════════════════════════════════════════════════════════

# --- Artist Roster (HUMAN-REVIEW) ---
ARTISTS_OVERRIDE = [
    # Optional roster override — edit or leave empty to infer from data.
    # "hicorook", "Flyana Boss", "Raiche", "BiC Fizzle", "COBRAH", "re6ce"
]

# --- Approval Flags (HUMAN-REVIEW) ---
HUMAN_REVIEW_APPROVED = {
    "artists_override": False,   # set True after you review ARTISTS_OVERRIDE
    "budget_inputs":    False,   # set True after you set BUDGET amounts
}

# --- Thresholds ---
THRESHOLDS = {
    "legacy":       75,  # for comparison plots (historical)
    "pre_breakout": 55,  # BLUE highlight + invest-gradually signal
    "breakout":     60,  # episode computation (true breakout)
}

# --- Budget Parameters (HUMAN-REVIEW REQUIRED) ---
BUDGET = {
    # >>>>> HUMAN REVIEW REQUIRED — set your real amounts/percentages <<<<<
    "tier_55_pct":      None,  # e.g., 0.20 for 20% of pool
    "tier_75_pct":      None,  # e.g., 0.35
    "cap_per_artist":   None,  # e.g., 1500.0 (USD)
}

# --- Analysis Period ---
import pandas as pd
from datetime import datetime, timedelta

END_DATE = pd.Timestamp.now().normalize()
START_DATE = END_DATE - timedelta(days=90)  # 90-day analysis window

# --- Human-Review Badge ---
def _hr_badge():
    msgs = []
    if ARTISTS_OVERRIDE and not HUMAN_REVIEW_APPROVED["artists_override"]:
        msgs.append("Artist roster override present but not approved.")
    if any(BUDGET[k] in (None, 0) for k in ["tier_55_pct","tier_75_pct","cap_per_artist"]) \
       and not HUMAN_REVIEW_APPROVED["budget_inputs"]:
        msgs.append("Budget inputs not approved or unset.")
    if msgs:
        from IPython.display import HTML, display
        html = "<br>".join(f"• {m}" for m in msgs)
        display(HTML(f'''
        <div style="padding:14px;border:3px solid #d95f02;border-radius:12px;background:#fff3e6">
          <b>⚠️  HUMAN-REVIEW REQUIRED</b><br>{html}
        </div>
        '''))
    else:
        from IPython.display import HTML, display
        display(HTML(f'''
        <div style="padding:14px;border:2px solid #1b9e77;border-radius:12px;background:#e6fff9">
          <b>✅ All human-review gates approved</b>
        </div>
        '''))

_hr_badge()

print(f"\n📅 Analysis Period: {START_DATE.date()} → {END_DATE.date()}")
print(f"🎯 Thresholds: Pre-breakout={THRESHOLDS['pre_breakout']}, Breakout={THRESHOLDS['breakout']}, Legacy={THRESHOLDS['legacy']}")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# IMPORTS
# ═══════════════════════════════════════════════════════════════════════════

import warnings
warnings.filterwarnings('ignore')

# Standard library
import re
from typing import Optional
from datetime import datetime, timedelta

# Data & computation
import numpy as np
import pandas as pd

# Visualization
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import DateFormatter

# MusicScope™ imports
from src.youtubeviz.data_discovery import discover_artists, discover_data, get_discovery

# Plotting style
plt.style.use('seaborn-v0_8-darkgrid')
mpl.rcParams['figure.dpi'] = 100
mpl.rcParams['font.size'] = 11

print('✅ All imports loaded successfully')

# %%
# ═══════════════════════════════════════════════════════════════════════════
# VISUALIZATION HELPERS (Professional Standards)
# ═══════════════════════════════════════════════════════════════════════════

# --- Accessible Color Palette (4.5:1 contrast, no red/green combos) ---
PALETTE = [
    "#1b9e77",  # teal (primary signal)
    "#d95f02",  # orange (secondary signal)
    "#7570b3",  # purple (tertiary)
    "#e7298a",  # magenta (accent)
    "#66a61e",  # olive (context)
    "#e6ab02",  # gold (highlight)
]
GREY_1 = "#cccccc"  # light grey (context, non-data)
GREY_2 = "#888888"  # medium grey (neutral sentiment)
GREY_3 = "#444444"  # dark grey (text)
BLUE_BREAKOUT = "#3498db"  # BLUE for pre-breakout state (≥55)

def get_color(i: int) -> str:
    return PALETTE[i % len(PALETTE)]

def slide(figsize=(11,6), watermark: Optional[str]=None):
    """Create a professional slide-style figure."""
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_anchor("NW")
    if watermark:
        fig.text(0.5, 0.5, watermark, color=GREY_1, fontsize=60, ha="center",
                 va="center", alpha=0.35, rotation=30)
    return fig, ax

def action_title(ax: mpl.axes.Axes, finding: str, implication: str, action: str) -> None:
    """Set action-oriented title: Finding → Implication → Action."""
    ax.set_title(f"{finding} → {implication} → {action}", fontsize=13, fontweight="bold", pad=12)

def direct_line_labels(ax: mpl.axes.Axes, fontsize: int = 10):
    """Add direct labels to line chart (remove legend)."""
    lines = [ln for ln in ax.get_lines() if not ln.get_label().startswith("_")]
    for ln in lines:
        x, y = ln.get_xdata(), ln.get_ydata()
        if len(x)==0: continue
        ax.annotate(ln.get_label(), xy=(x[-1], y[-1]), xytext=(5,0), textcoords="offset points",
                    va="center", fontsize=fontsize, color=ln.get_color(), fontweight="bold")
    if ax.get_legend(): ax.get_legend().remove()

def label_bars(ax: mpl.axes.Axes, fmt="{:.0f}", fontsize=10):
    """Add value labels to bar chart."""
    for p in ax.patches:
        h = p.get_height()
        if h == 0: continue
        ax.text(p.get_x()+p.get_width()/2, p.get_y()+h, fmt.format(h),
                ha="center", va="bottom", fontsize=fontsize, color=GREY_3)

def iso8601_to_seconds(iso: str) -> int:
    """Parse ISO 8601 duration (PT1H2M3S) to seconds."""
    if not isinstance(iso, str): return 0
    h = m = s = 0
    mobj = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if mobj:
        h = int(mobj.group(1) or 0); m = int(mobj.group(2) or 0); s = int(mobj.group(3) or 0)
    return h*3600 + m*60 + s

print('✅ Visualization helpers loaded')

# %%
# ═══════════════════════════════════════════════════════════════════════════
# DATA LOADING & VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

# Discover and load data from YouTube API v3
chart_data = discover_data()

print('📈 Data Summary:')
for data_type, df in chart_data.items():
    print(f'   {data_type}: {len(df):,} rows, {len(df.columns)} columns')
    if 'artist_name' in df.columns:
        unique_artists = df['artist_name'].nunique()
        print(f'      → {unique_artists} unique artists')

# Extract core DataFrames
videos_df = chart_data.get('videos', pd.DataFrame())
comments_df = chart_data.get('comments', pd.DataFrame())
metrics_df = chart_data.get('metrics_timeseries', pd.DataFrame())

# --- Data Contract Validation ---
REQUIRED_VIDEO_COLS = ['video_id', 'title', 'artist_name', 'published_at', 'view_count', 'like_count', 'comment_count']
REQUIRED_COMMENT_COLS = ['video_id', 'published_at', 'text']

missing_video_cols = set(REQUIRED_VIDEO_COLS) - set(videos_df.columns)
missing_comment_cols = set(REQUIRED_COMMENT_COLS) - set(comments_df.columns)

if missing_video_cols:
    raise ValueError(f"❌ Missing required video columns: {missing_video_cols}")
if missing_comment_cols:
    raise ValueError(f"❌ Missing required comment columns: {missing_comment_cols}")

# Ensure datetime types
videos_df['published_at'] = pd.to_datetime(videos_df['published_at'])
comments_df['published_at'] = pd.to_datetime(comments_df['published_at'])

# Parse duration if available
if 'duration_iso' in videos_df.columns:
    videos_df['duration_sec'] = videos_df['duration_iso'].apply(iso8601_to_seconds)

# Determine artist roster
if ARTISTS_OVERRIDE:
    artists = ARTISTS_OVERRIDE
    print(f"\n🎵 Using override roster: {len(artists)} artists")
else:
    artists = sorted(videos_df['artist_name'].unique())
    print(f"\n🎵 Discovered roster: {len(artists)} artists")

for i, artist in enumerate(artists, 1):
    print(f"   {i}. {artist}")

ARTIST_COUNT = len(artists)

print(f"\n✅ Data loaded: {len(videos_df):,} videos, {len(comments_df):,} comments from {ARTIST_COUNT} artists")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# DATA PREPARATION
# ═══════════════════════════════════════════════════════════════════════════

# Create working copies
vids = videos_df.copy()
comments = comments_df.copy()

# Derived video features
vids["age_days"] = (END_DATE - vids["published_at"]).dt.days.clip(lower=1)
vids["views_per_day"] = (vids["view_count"] / vids["age_days"]).replace([np.inf, np.nan], 0.0)
vids["like_rate"] = (vids["like_count"] / vids["view_count"].replace(0, np.nan)).fillna(0.0).clip(0,1)
vids["comment_rate"] = (vids["comment_count"] / vids["view_count"].replace(0, np.nan)).fillna(0.0).clip(0,1)
vids["publish_week"] = vids["published_at"].dt.to_period("W").dt.to_timestamp()
vids["publish_month"] = vids["published_at"].dt.to_period("M").dt.to_timestamp()
vids["publish_hour"] = vids["published_at"].dt.hour
vids["publish_dow"] = vids["published_at"].dt.day_name()

# Comment features
if 'text' in comments.columns:
    comments["comment_length"] = comments["text"].str.len().fillna(0)

print(f"✅ Data prepared: {len(vids):,} videos with derived features")
print(f"   Age range: {vids['age_days'].min():.0f} - {vids['age_days'].max():.0f} days")
print(f"   Views/day range: {vids['views_per_day'].min():.1f} - {vids['views_per_day'].max():.1f}")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# MOMENTUM SCORING (Cross-sectional daily normalization)
# ═══════════════════════════════════════════════════════════════════════════

# Daily comment counts per video
comments["_date"] = comments["published_at"].dt.floor("D")
c_daily = (comments.groupby(["video_id","_date"]).size()
                  .rename("comments_d").reset_index())

# Create daily panel for each video
rows = []
for _, vid in vids.iterrows():
    pub = vid["published_at"].floor("D")
    dates = pd.date_range(pub, END_DATE, freq="D")
    for d in dates:
        rows.append({
            "video_id": vid["video_id"],
            "title": vid["title"],
            "artist_name": vid["artist_name"],
            "date": d,
            "views_per_day": vid["views_per_day"],
            "like_rate": vid["like_rate"],
        })

momentum_daily = pd.DataFrame(rows)

# Merge comment counts
momentum_daily = momentum_daily.merge(c_daily, left_on=["video_id","date"], right_on=["video_id","_date"], how="left")
momentum_daily["comments_d"] = momentum_daily["comments_d"].fillna(0)

# Rolling comment velocity (14-day window)
momentum_daily = momentum_daily.sort_values(["video_id","date"])
momentum_daily["cmt_14d"] = momentum_daily.groupby("video_id")["comments_d"].transform(
    lambda x: x.rolling(14, min_periods=1).sum()
)
momentum_daily["comments_per_day_14d"] = (momentum_daily["cmt_14d"]/14.0).fillna(0)

# Robust per-day normalization (cross-sectional), then weighted momentum
def _score_component_daily(df, col):
    """Robust z-score → percentile → 0-100 scale."""
    med = df[col].median()
    mad = (df[col] - med).abs().median() + 1e-9
    z = (df[col] - med) / (1.4826*mad)
    return (z.rank(pct=True)*100).clip(0,100)

momentum_daily = momentum_daily.groupby("date", group_keys=False).apply(
    lambda d: d.assign(
        s_views=_score_component_daily(d, "views_per_day"),
        s_like=_score_component_daily(d, "like_rate"),
        s_cmtv=_score_component_daily(d, "comments_per_day_14d"),
    )
)

# Weighted momentum score (0-100)
momentum_daily["momentum_score"] = (0.45*momentum_daily["s_views"]
                                    +0.25*momentum_daily["s_like"]
                                    +0.30*momentum_daily["s_cmtv"]).round(1)

# State classification
momentum_daily["state"] = np.where(momentum_daily["momentum_score"]>=THRESHOLDS['pre_breakout'], "pre_breakout", "baseline")
momentum_daily["is_breakout"] = (momentum_daily["momentum_score"]>=THRESHOLDS['breakout']).astype(int)

print(f"✅ Momentum calculated: {len(momentum_daily):,} daily observations")
print(f"   Score range: {momentum_daily['momentum_score'].min():.1f} - {momentum_daily['momentum_score'].max():.1f}")
print(f"   Pre-breakout days (≥{THRESHOLDS['pre_breakout']}): {(momentum_daily['momentum_score']>=THRESHOLDS['pre_breakout']).sum():,}")
print(f"   Breakout days (≥{THRESHOLDS['breakout']}): {(momentum_daily['momentum_score']>=THRESHOLDS['breakout']).sum():,}")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# BREAKOUT EPISODE DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def detect_episodes(df, video_id, threshold=60):
    """Detect contiguous breakout episodes for a video."""
    vid_data = df[df['video_id']==video_id].sort_values('date').copy()
    vid_data['is_breakout'] = (vid_data['momentum_score'] >= threshold).astype(int)
    
    # Find run starts and ends
    vid_data['run_start'] = (vid_data['is_breakout'] == 1) & (vid_data['is_breakout'].shift(1, fill_value=0) == 0)
    vid_data['run_end'] = (vid_data['is_breakout'] == 1) & (vid_data['is_breakout'].shift(-1, fill_value=0) == 0)
    
    episodes = []
    starts = vid_data[vid_data['run_start']]['date'].tolist()
    ends = vid_data[vid_data['run_end']]['date'].tolist()
    
    for start, end in zip(starts, ends):
        # Calculate pre-breakout warning hours (55≤score<60 immediately before episode)
        pre_window = vid_data[(vid_data['date'] < start) & 
                              (vid_data['momentum_score'] >= THRESHOLDS['pre_breakout']) &
                              (vid_data['momentum_score'] < threshold)]
        
        # Find contiguous pre-breakout window immediately before start
        if len(pre_window) > 0:
            pre_window = pre_window.sort_values('date', ascending=False)
            contiguous_days = 0
            prev_date = start
            for _, row in pre_window.iterrows():
                if (prev_date - row['date']).days == 1:
                    contiguous_days += 1
                    prev_date = row['date']
                else:
                    break
            pre_warning_hours = contiguous_days * 24
        else:
            pre_warning_hours = 0
        
        # Real days calculation: (end - start) + 1
        duration_days = (end - start).days + 1
        
        episodes.append({
            'video_id': video_id,
            'start_date': start,
            'end_date': end,
            'duration_days': duration_days,
            'pre_warning_hours': pre_warning_hours,
        })
    
    return episodes

# Detect episodes for all videos
all_episodes = []
for video_id in momentum_daily['video_id'].unique():
    episodes = detect_episodes(momentum_daily, video_id, threshold=THRESHOLDS['breakout'])
    all_episodes.extend(episodes)

episodes_df = pd.DataFrame(all_episodes)

if len(episodes_df) > 0:
    # Add video metadata
    episodes_df = episodes_df.merge(
        vids[['video_id', 'title', 'artist_name']], 
        on='video_id', 
        how='left'
    )
    
    print(f"✅ Episodes detected: {len(episodes_df):,} breakout episodes")
    print(f"   Videos with episodes: {episodes_df['video_id'].nunique()}")
    print(f"   Avg duration: {episodes_df['duration_days'].mean():.1f} days")
    print(f"   Avg pre-warning: {episodes_df['pre_warning_hours'].mean():.1f} hours")
else:
    print("⚠️  No breakout episodes detected at threshold {THRESHOLDS['breakout']}")

# %% [markdown]
# ---
# 
# # 📊 Section 1: Artist & Audience Intelligence
# 
# Roster overview, engagement patterns, and current performance metrics.
# 
# ---

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CHART 1.1: Artist Roster Overview
# ═══════════════════════════════════════════════════════════════════════════

# Aggregate metrics by artist
artist_summary = vids.groupby('artist_name').agg({
    'video_id': 'count',
    'view_count': 'sum',
    'like_count': 'sum',
    'comment_count': 'sum',
    'views_per_day': 'mean',
    'like_rate': 'mean',
}).rename(columns={
    'video_id': 'videos',
    'view_count': 'total_views',
    'like_count': 'total_likes',
    'comment_count': 'total_comments',
    'views_per_day': 'avg_views_per_day',
    'like_rate': 'avg_like_rate',
}).round(2)

artist_summary = artist_summary.sort_values('total_views', ascending=False)

# Display as table
from IPython.display import display, HTML

html_table = artist_summary.to_html(classes='table table-striped', border=0)
display(HTML(f"""
<div style="padding:20px;border:2px solid {PALETTE[0]};border-radius:12px;background:#f9f9f9">
    <h3>🎵 Artist Roster Overview ({ARTIST_COUNT} Artists)</h3>
    {html_table}
</div>
"""))

print(f"\n✅ Roster summary: {ARTIST_COUNT} artists, {artist_summary['videos'].sum():.0f} total videos")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CHART 1.2: Engagement Patterns by Artist
# ═══════════════════════════════════════════════════════════════════════════

fig, ax = slide(figsize=(12,6))

# Scatter: views/day vs like_rate, sized by comment_count
for i, artist in enumerate(artists):
    artist_vids = vids[vids['artist_name']==artist]
    ax.scatter(
        artist_vids['views_per_day'], 
        artist_vids['like_rate']*100,
        s=artist_vids['comment_count']*2,
        alpha=0.6,
        color=get_color(i),
        label=artist
    )

ax.set_xlabel('Views per Day', fontsize=12)
ax.set_ylabel('Like Rate (%)', fontsize=12)
action_title(ax, 
    "Engagement quality varies across roster",
    "Some artists drive high interaction despite lower reach",
    "Tailor promotion strategy by artist engagement profile"
)
ax.legend(loc='upper right', fontsize=9)
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

print("✅ Chart 1.2: Engagement patterns")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CHART 1.3: Performance Trends (30-day rolling)
# ═══════════════════════════════════════════════════════════════════════════

# Daily aggregation
daily_perf = vids.copy()
daily_perf['pub_date'] = daily_perf['published_at'].dt.floor('D')
daily_agg = daily_perf.groupby('pub_date').agg({
    'views_per_day': 'mean',
    'like_rate': 'mean',
    'comment_rate': 'mean',
}).sort_index()

# 30-day rolling average
daily_agg['views_per_day_30d'] = daily_agg['views_per_day'].rolling(30, min_periods=1).mean()
daily_agg['like_rate_30d'] = daily_agg['like_rate'].rolling(30, min_periods=1).mean()

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12,8), sharex=True)

# Views/day trend
ax1.plot(daily_agg.index, daily_agg['views_per_day_30d'], color=PALETTE[0], linewidth=2.5, label='30-day avg')
ax1.fill_between(daily_agg.index, 0, daily_agg['views_per_day_30d'], alpha=0.2, color=PALETTE[0])
ax1.set_ylabel('Avg Views/Day', fontsize=11)
action_title(ax1,
    "Views/day trending upward in recent weeks",
    "Momentum building across roster",
    "Maintain promotion intensity"
)
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(DateFormatter('%b %d %Y'))

# Like rate trend
ax2.plot(daily_agg.index, daily_agg['like_rate_30d']*100, color=PALETTE[1], linewidth=2.5, label='30-day avg')
ax2.fill_between(daily_agg.index, 0, daily_agg['like_rate_30d']*100, alpha=0.2, color=PALETTE[1])
ax2.set_ylabel('Avg Like Rate (%)', fontsize=11)
ax2.set_xlabel('Publish Date', fontsize=11)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(DateFormatter('%b %d %Y'))

plt.tight_layout()
plt.show()

print("✅ Chart 1.3: Performance trends")

# %% [markdown]
# ---
# 
# # 🎯 Section 2: Breakout Threshold Experiment Analysis
# 
# **Key Question:** Should we lower the initial breakout velocity threshold from **75 → 55**?
# 
# This section compares outcomes at both thresholds to enable data-driven decision-making.
# 
# ---

# %%
# ═══════════════════════════════════════════════════════════════════════════
# THRESHOLD COMPARISON ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

# Calculate metrics at both thresholds
threshold_55 = THRESHOLDS['pre_breakout']
threshold_75 = THRESHOLDS['legacy']

# Daily counts
qualifying_by_threshold_daily = momentum_daily.groupby('date').apply(
    lambda d: pd.Series({
        'videos_at_55': (d['momentum_score'] >= threshold_55).sum(),
        'videos_at_75': (d['momentum_score'] >= threshold_75).sum(),
        'artists_at_55': d[d['momentum_score'] >= threshold_55]['artist_name'].nunique(),
        'artists_at_75': d[d['momentum_score'] >= threshold_75]['artist_name'].nunique(),
    })
).reset_index()

# Overall summary
total_days_55 = (momentum_daily['momentum_score'] >= threshold_55).sum()
total_days_75 = (momentum_daily['momentum_score'] >= threshold_75).sum()
unique_videos_55 = momentum_daily[momentum_daily['momentum_score'] >= threshold_55]['video_id'].nunique()
unique_videos_75 = momentum_daily[momentum_daily['momentum_score'] >= threshold_75]['video_id'].nunique()
unique_artists_55 = momentum_daily[momentum_daily['momentum_score'] >= threshold_55]['artist_name'].nunique()
unique_artists_75 = momentum_daily[momentum_daily['momentum_score'] >= threshold_75]['artist_name'].nunique()

# Artist consistency (how many days each artist hits each threshold)
artist_consistency = momentum_daily.groupby('artist_name').apply(
    lambda d: pd.Series({
        'days_at_55': (d['momentum_score'] >= threshold_55).sum(),
        'days_at_75': (d['momentum_score'] >= threshold_75).sum(),
        'videos': d['video_id'].nunique(),
    })
).reset_index()

artist_consistency['consistency_ratio'] = (
    artist_consistency['days_at_75'] / artist_consistency['days_at_55'].replace(0, np.nan)
).fillna(0).round(2)

print("✅ Threshold analysis complete")
print(f"\n📊 Threshold Comparison Summary:")
print(f"   At threshold {threshold_55}:")
print(f"      → {total_days_55:,} qualifying video-days")
print(f"      → {unique_videos_55} unique videos")
print(f"      → {unique_artists_55} unique artists")
print(f"   At threshold {threshold_75}:")
print(f"      → {total_days_75:,} qualifying video-days")
print(f"      → {unique_videos_75} unique videos")
print(f"      → {unique_artists_75} unique artists")
print(f"   Incremental opportunity (55 vs 75):")
print(f"      → +{total_days_55 - total_days_75:,} video-days ({((total_days_55/total_days_75-1)*100 if total_days_75>0 else 0):.0f}% increase)")
print(f"      → +{unique_videos_55 - unique_videos_75} videos")
print(f"      → +{unique_artists_55 - unique_artists_75} artists")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CHART 2.1: Threshold Comparison (55 vs 75)
# ═══════════════════════════════════════════════════════════════════════════

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14,6))

# Chart 1: Videos & Artists qualifying
metrics = ['Videos', 'Artists']
at_55 = [unique_videos_55, unique_artists_55]
at_75 = [unique_videos_75, unique_artists_75]

x = np.arange(len(metrics))
width = 0.35

bars1 = ax1.bar(x - width/2, at_55, width, label=f'Threshold {threshold_55}', color=BLUE_BREAKOUT)
bars2 = ax1.bar(x + width/2, at_75, width, label=f'Threshold {threshold_75}', color=PALETTE[1])

ax1.set_ylabel('Count', fontsize=11)
ax1.set_xticks(x)
ax1.set_xticklabels(metrics)
action_title(ax1,
    f"Lowering threshold to {threshold_55} captures {unique_videos_55 - unique_videos_75} more videos",
    "Broader reach across roster",
    "Evaluate if team can activate additional opportunities"
)
ax1.legend()
ax1.grid(True, alpha=0.3, axis='y')
label_bars(ax1)

# Chart 2: Cumulative opportunity shift
qualifying_by_threshold_daily['incremental_videos'] = (
    qualifying_by_threshold_daily['videos_at_55'] - qualifying_by_threshold_daily['videos_at_75']
)
qualifying_by_threshold_daily['cumulative_incremental'] = qualifying_by_threshold_daily['incremental_videos'].cumsum()

ax2.fill_between(
    qualifying_by_threshold_daily['date'],
    0,
    qualifying_by_threshold_daily['cumulative_incremental'],
    color=BLUE_BREAKOUT,
    alpha=0.4
)
ax2.plot(
    qualifying_by_threshold_daily['date'],
    qualifying_by_threshold_daily['cumulative_incremental'],
    color=BLUE_BREAKOUT,
    linewidth=2.5
)
ax2.set_ylabel('Cumulative Additional Video-Days', fontsize=11)
ax2.set_xlabel('Date', fontsize=11)
action_title(ax2,
    f"Threshold {threshold_55} unlocks {total_days_55 - total_days_75:,} additional video-days",
    "Opportunity compounds over time",
    "Assess activation capacity before committing"
)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(DateFormatter('%b %d %Y'))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.show()

print("✅ Chart 2.1: Threshold comparison")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CHART 2.2: KPI-22 (Breakout Duration + Pre-Warning Hours)
# ═══════════════════════════════════════════════════════════════════════════

if len(episodes_df) > 0:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14,6))
    
    # Panel 1: Breakout Duration
    episodes_sorted = episodes_df.sort_values('duration_days', ascending=False).head(10)
    bars1 = ax1.barh(
        range(len(episodes_sorted)),
        episodes_sorted['duration_days'],
        color=PALETTE[1]
    )
    ax1.set_yticks(range(len(episodes_sorted)))
    ax1.set_yticklabels([f"{row['artist_name'][:15]}..." for _, row in episodes_sorted.iterrows()], fontsize=9)
    ax1.set_xlabel('Duration (Days)', fontsize=11)
    action_title(ax1,
        f"Top breakout episodes average {episodes_sorted['duration_days'].mean():.1f} days",
        "Sustained momentum is achievable",
        "Study top performers for replication patterns"
    )
    ax1.grid(True, alpha=0.3, axis='x')
    label_bars(ax1, fmt="{:.0f}d")
    
    # Panel 2: Pre-Breakout Warning Hours
    episodes_warning = episodes_df[episodes_df['pre_warning_hours'] > 0].sort_values('pre_warning_hours', ascending=False).head(10)
    if len(episodes_warning) > 0:
        bars2 = ax2.barh(
            range(len(episodes_warning)),
            episodes_warning['pre_warning_hours'],
            color=BLUE_BREAKOUT
        )
        ax2.set_yticks(range(len(episodes_warning)))
        ax2.set_yticklabels([f"{row['artist_name'][:15]}..." for _, row in episodes_warning.iterrows()], fontsize=9)
        ax2.set_xlabel('Pre-Warning Hours', fontsize=11)
        action_title(ax2,
            f"Median {episodes_df['pre_warning_hours'].median():.0f}h advance warning before breakout",
            "Early signal enables proactive budget allocation",
            "Set up alerts at threshold {threshold_55}"
        )
        ax2.grid(True, alpha=0.3, axis='x')
        label_bars(ax2, fmt="{:.0f}h")
    else:
        ax2.text(0.5, 0.5, 'No pre-warning data available', ha='center', va='center', fontsize=12, color=GREY_2)
        ax2.set_xticks([])
        ax2.set_yticks([])
    
    plt.tight_layout()
    plt.show()
    
    print("✅ Chart 2.2: KPI-22 (dual-panel)")
else:
    print("⚠️  Skipping KPI-22: No episodes detected")

# %% [markdown]
# ---
# 
# # 💰 Section 3: Budget Reallocation Strategy Comparison
# 
# Three alternative strategies for budget allocation based on momentum thresholds.
# 
# **⚠️  HUMAN-REVIEW REQUIRED:** Budget parameters must be set in the config cell before running this section.
# 
# ---

# %%
# ═══════════════════════════════════════════════════════════════════════════
# BUDGET STRATEGY VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

# Check if budget inputs are approved
if not HUMAN_REVIEW_APPROVED['budget_inputs']:
    from IPython.display import HTML, display
    display(HTML(f'''
    <div style="padding:20px;border:3px solid #d95f02;border-radius:12px;background:#fff3e6">
        <h3>⚠️  HUMAN-REVIEW REQUIRED</h3>
        <p><b>Budget parameters are not set or approved.</b></p>
        <p>Please update the BUDGET dictionary in the config cell and set <code>HUMAN_REVIEW_APPROVED['budget_inputs'] = True</code></p>
        <p>This section will display placeholder analysis only.</p>
    </div>
    '''))
    BUDGET_APPROVED = False
else:
    BUDGET_APPROVED = True
    print("✅ Budget parameters approved")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# BUDGET STRATEGY CALCULATIONS
# ═══════════════════════════════════════════════════════════════════════════

# Strategy A: Tiered Escalation
# Gradual budget increase at 55, additional funding at 75
strategy_a = artist_consistency.copy()
if BUDGET_APPROVED:
    strategy_a['tier_55_allocation'] = strategy_a['days_at_55'] * BUDGET['tier_55_pct']
    strategy_a['tier_75_allocation'] = strategy_a['days_at_75'] * BUDGET['tier_75_pct']
    strategy_a['total_allocation'] = (strategy_a['tier_55_allocation'] + strategy_a['tier_75_allocation']).clip(upper=BUDGET['cap_per_artist'])
else:
    strategy_a['tier_55_allocation'] = strategy_a['days_at_55'] * 0.20  # placeholder
    strategy_a['tier_75_allocation'] = strategy_a['days_at_75'] * 0.35  # placeholder
    strategy_a['total_allocation'] = strategy_a['tier_55_allocation'] + strategy_a['tier_75_allocation']

strategy_a = strategy_a.sort_values('total_allocation', ascending=False)

# Strategy B: Consistency Bonus
# Reward artists who consistently reach 55 AND routinely achieve 75
strategy_b = artist_consistency.copy()
strategy_b['consistency_score'] = (
    0.6 * (strategy_b['days_at_55'] / strategy_b['days_at_55'].max()) +
    0.4 * (strategy_b['days_at_75'] / strategy_b['days_at_75'].max().clip(lower=1))
)
if BUDGET_APPROVED:
    total_pool = BUDGET['cap_per_artist'] * ARTIST_COUNT
    strategy_b['allocation'] = (strategy_b['consistency_score'] / strategy_b['consistency_score'].sum() * total_pool).clip(upper=BUDGET['cap_per_artist'])
else:
    strategy_b['allocation'] = strategy_b['consistency_score'] * 1000  # placeholder

strategy_b = strategy_b.sort_values('allocation', ascending=False)

# Strategy C: Consistency-Based Allocation
# Rank by frequency of reaching 55, allocate proportionally
strategy_c = artist_consistency.copy()
strategy_c['rank'] = strategy_c['days_at_55'].rank(ascending=False, method='dense')
strategy_c['rank_weight'] = (ARTIST_COUNT + 1 - strategy_c['rank']) / strategy_c['rank'].sum()
if BUDGET_APPROVED:
    total_pool = BUDGET['cap_per_artist'] * ARTIST_COUNT
    strategy_c['allocation'] = (strategy_c['rank_weight'] * total_pool).clip(upper=BUDGET['cap_per_artist'])
else:
    strategy_c['allocation'] = strategy_c['rank_weight'] * 5000  # placeholder

strategy_c = strategy_c.sort_values('allocation', ascending=False)

print("✅ Budget strategies calculated")
if not BUDGET_APPROVED:
    print("⚠️  Using placeholder values (budget not approved)")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# CHART 3.1: Budget Strategy Comparison
# ═══════════════════════════════════════════════════════════════════════════

fig, axes = plt.subplots(1, 3, figsize=(16,6))

# Strategy A
axes[0].barh(range(len(strategy_a)), strategy_a['total_allocation'], color=PALETTE[0])
axes[0].set_yticks(range(len(strategy_a)))
axes[0].set_yticklabels(strategy_a['artist_name'], fontsize=9)
axes[0].set_xlabel('Total Allocation', fontsize=10)
axes[0].set_title('Strategy A: Tiered Escalation\n(Gradual unlock at 55, boost at 75)', fontsize=11, fontweight='bold')
axes[0].grid(True, alpha=0.3, axis='x')

# Strategy B
axes[1].barh(range(len(strategy_b)), strategy_b['allocation'], color=PALETTE[1])
axes[1].set_yticks(range(len(strategy_b)))
axes[1].set_yticklabels(strategy_b['artist_name'], fontsize=9)
axes[1].set_xlabel('Allocation', fontsize=10)
axes[1].set_title('Strategy B: Consistency Bonus\n(Reward reliable performers)', fontsize=11, fontweight='bold')
axes[1].grid(True, alpha=0.3, axis='x')

# Strategy C
axes[2].barh(range(len(strategy_c)), strategy_c['allocation'], color=PALETTE[2])
axes[2].set_yticks(range(len(strategy_c)))
axes[2].set_yticklabels(strategy_c['artist_name'], fontsize=9)
axes[2].set_xlabel('Allocation', fontsize=10)
axes[2].set_title('Strategy C: Consistency Ranking\n(Proportional to 55-threshold hits)', fontsize=11, fontweight='bold')
axes[2].grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()

print("✅ Chart 3.1: Budget strategy comparison")
if not BUDGET_APPROVED:
    print("⚠️  Chart shows placeholder values only")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY PROS/CONS SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

from IPython.display import HTML, display

html = f"""
<div style="padding:20px;border:2px solid {PALETTE[0]};border-radius:12px;background:#f9f9f9">
    <h3>📊 Strategy Comparison Summary</h3>
    
    <h4 style="color:{PALETTE[0]}">Strategy A: Tiered Escalation</h4>
    <p><b>✅ Pros:</b></p>
    <ul>
        <li>Gradual risk management (test at 55, scale at 75)</li>
        <li>Rewards sustained performance over time</li>
        <li>Clear decision gates for budget increases</li>
    </ul>
    <p><b>❌ Cons:</b></p>
    <ul>
        <li>Complex tracking across two tiers</li>
        <li>May miss fast-moving viral opportunities</li>
        <li>Requires real-time monitoring infrastructure</li>
    </ul>
    
    <h4 style="color:{PALETTE[1]}">Strategy B: Consistency Bonus</h4>
    <p><b>✅ Pros:</b></p>
    <ul>
        <li>Rewards reliable performers (reduces one-hit-wonder risk)</li>
        <li>Balances frequency (55) with quality (75)</li>
        <li>Encourages sustainable artist development</li>
    </ul>
    <p><b>❌ Cons:</b></p>
    <ul>
        <li>May penalize emerging artists with limited history</li>
        <li>Slower to capitalize on viral moments</li>
        <li>Requires longer evaluation period</li>
    </ul>
    
    <h4 style="color:{PALETTE[2]}">Strategy C: Consistency-Based Allocation</h4>
    <p><b>✅ Pros:</b></p>
    <ul>
        <li>Data-driven, transparent ranking system</li>
        <li>Simple to explain and implement</li>
        <li>Focuses on pre-breakout signal (55 threshold)</li>
    </ul>
    <p><b>❌ Cons:</b></p>
    <ul>
        <li>May create internal competition between artists</li>
        <li>Doesn't account for growth potential or trajectory</li>
        <li>Winner-take-all dynamics could demotivate lower-ranked artists</li>
    </ul>
</div>
"""

display(HTML(html))
print("✅ Strategy pros/cons summary displayed")

# %% [markdown]
# ---
# 
# # 🎉 Fun FYI: Additional Insights
# 
# Celebratory, non-blocking insights for deeper exploration.
# 
# ---

# %%
# ═══════════════════════════════════════════════════════════════════════════
# FYI CHART 1: Comment Length Distribution
# ═══════════════════════════════════════════════════════════════════════════

if 'comment_length' in comments.columns:
    fig, ax = slide(figsize=(10,5))
    
    ax.hist(comments['comment_length'], bins=50, color=PALETTE[3], alpha=0.7, edgecolor='white')
    ax.axvline(comments['comment_length'].median(), color=PALETTE[1], linestyle='--', linewidth=2, label=f"Median: {comments['comment_length'].median():.0f} chars")
    ax.set_xlabel('Comment Length (characters)', fontsize=11)
    ax.set_ylabel('Frequency', fontsize=11)
    action_title(ax,
        f"Most comments are {comments['comment_length'].median():.0f} chars (quick reactions)",
        "Audience prefers short, punchy engagement",
        "Optimize for mobile-first comment UX"
    )
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.show()
    
    print("✅ FYI Chart 1: Comment length distribution")
else:
    print("⚠️  Skipping FYI Chart 1: comment_length not available")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# FYI CHART 2: Publish Hour vs Performance
# ═══════════════════════════════════════════════════════════════════════════

fig, ax = slide(figsize=(12,5))

hour_perf = vids.groupby('publish_hour').agg({
    'views_per_day': 'mean',
    'video_id': 'count'
}).rename(columns={'video_id': 'count'})

bars = ax.bar(hour_perf.index, hour_perf['views_per_day'], color=GREY_1, alpha=0.7)

# Highlight best hour
best_hour = hour_perf['views_per_day'].idxmax()
bars[best_hour].set_color(PALETTE[0])
bars[best_hour].set_alpha(1.0)

ax.set_xlabel('Publish Hour (24h)', fontsize=11)
ax.set_ylabel('Avg Views/Day', fontsize=11)
action_title(ax,
    f"Hour {best_hour}:00 drives highest views/day ({hour_perf.loc[best_hour, 'views_per_day']:.0f})",
    "Timing matters for initial momentum",
    f"Schedule high-priority releases around {best_hour}:00"
)
ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.show()

print("✅ FYI Chart 2: Publish hour vs performance")

# %%
# ═══════════════════════════════════════════════════════════════════════════
# NOTEBOOK HYGIENE REMINDER
# ═══════════════════════════════════════════════════════════════════════════

print("""
✅ Notebook hygiene (run once per repo):

1) Strip outputs & pair to text for clean diffs:
   pip install nbstripout jupytext
   nbstripout --install
   jupytext --set-formats 'ipynb,py:percent' MusicScope_YouTube_Dashboard.ipynb

2) Lint / type-check notebooks via nbQA:
   pip install nbqa ruff mypy
   nbqa ruff MusicScope_YouTube_Dashboard.ipynb --fix
   nbqa mypy MusicScope_YouTube_Dashboard.ipynb --ignore-missing-imports

3) Commit with conventional commit message:
   git add MusicScope_YouTube_Dashboard.ipynb
   git commit -m "feat(dashboard): add MusicScope YouTube analytics dashboard"
""")


