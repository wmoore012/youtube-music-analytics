#!/usr/bin/env python3
"""
Integration Script for MusicScope™ Professional Dashboard

This script replaces the existing Momentum, Sentiment, and Performance sections
with the new module code that uses the MusicScope design system helpers.

Usage:
    python3 notebooks/scripts/run_integration.py
"""

import json
import sys
from pathlib import Path

# Notebook path
NOTEBOOK_PATH = Path(__file__).resolve().parents[1] / "MusicScope™_Professional_Dashboard.ipynb"
BACKUP_PATH = Path(__file__).resolve().parents[1] / "MusicScope™_Professional_Dashboard.ipynb.backup"

def create_code_cell(code_content):
    """Create a code cell with proper formatting."""
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": None,
        "metadata": {},
        "outputs": [],
        "source": [line + '\n' for line in code_content.rstrip('\n').split('\n')]
    }

def create_markdown_cell(markdown_content):
    """Create a markdown cell with proper formatting."""
    return {
        "cell_type": "markdown",
        "id": None,
        "metadata": {},
        "source": [line + '\n' for line in markdown_content.rstrip('\n').split('\n')]
    }

def find_section_indices(cells):
    """Find the start indices of each major section."""
    indices = {}
    
    for i, cell in enumerate(cells):
        if cell['cell_type'] == 'markdown':
            source = ''.join(cell['source'])
            if '## ⚡ Momentum Intelligence' in source:
                indices['momentum'] = i
            elif '## 📊 Sentiment Analysis' in source:
                indices['sentiment'] = i
            elif '## 📊 Performance Analysis' in source:
                indices['performance'] = i
    
    return indices

# ============================================================================
# CELL GENERATORS
# ============================================================================

def get_momentum_section_cells():
    """Generate all cells for the Momentum Intelligence section."""
    cells = []
    
    # Cell 1: Markdown header
    cells.append(create_markdown_cell("""## ⚡ Momentum Intelligence

Track who is heating up, when to lean in, and how thresholds affect the breakout narrative."""))
    
    # Cell 2: Hero card + data prep + control panel
    cells.append(create_code_cell("""# ============================================================================
# SECTION 2: ⚡ MOMENTUM INTELLIGENCE
# Answers: "Who is heating up, and when should we lean in?"
# ============================================================================

from __future__ import annotations
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from ipywidgets import interact, IntSlider

# ----------------------------------------------------------------------------
# HERO CARD
# ----------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------
# 2.1 — DATASET PREPARATION
# ----------------------------------------------------------------------------
print("🔄 Building momentum dataset...")

ms_require_data(
    name="videos_df",
    value=videos_df,
    required_cols=["artist_name", "view_count", "published_at"],
    context="Momentum Intelligence"
)

# Aggregate to daily artist-level metrics
momentum_daily = (
    videos_df
    .assign(date=pd.to_datetime(videos_df["published_at"]).dt.normalize())
    .groupby(["artist_name", "date"], as_index=False)
    .agg(
        views=("view_count", "sum"),
        videos=("video_id", "count"),
    )
    .sort_values(["artist_name", "date"])
)

# Compute momentum score (simplified: 7-day rolling avg of views)
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

# ----------------------------------------------------------------------------
# 2.2 — CONTROL PANEL (INTERACTIVE SLIDERS)
# ----------------------------------------------------------------------------
ms_subsection_card(
    title="🎚️ Momentum Control Panel",
    subtitle="Adjust thresholds to tune breakout detection sensitivity"
)

# Default thresholds
pre_breakout_threshold = MOMENTUM_THRESHOLDS["pre_breakout"]
breakout_threshold = MOMENTUM_THRESHOLDS["breakout"]
legacy_threshold = MOMENTUM_THRESHOLDS["legacy"]

@interact(
    pre_breakout=IntSlider(min=40, max=70, step=5, value=pre_breakout_threshold, description="Pre-Breakout"),
    breakout=IntSlider(min=50, max=80, step=5, value=breakout_threshold, description="Breakout"),
    legacy=IntSlider(min=60, max=90, step=5, value=legacy_threshold, description="Legacy"),
)
def update_thresholds(pre_breakout, breakout, legacy):
    global pre_breakout_threshold, breakout_threshold, legacy_threshold
    pre_breakout_threshold = pre_breakout
    breakout_threshold = breakout
    legacy_threshold = legacy
    
    latest = momentum_daily.sort_values("date").groupby("artist_name").tail(1)
    pre_count = ((latest["momentum_score"] >= pre_breakout) & (latest["momentum_score"] < breakout)).sum()
    breakout_count = ((latest["momentum_score"] >= breakout) & (latest["momentum_score"] < legacy)).sum()
    legacy_count = (latest["momentum_score"] >= legacy).sum()
    
    print(f"🔥 Breakout: {breakout_count} artists (≥{breakout})")
    print(f"📈 Pre-Breakout: {pre_count} artists ({pre_breakout}–{breakout})")
    print(f"🏆 Legacy: {legacy_count} artists (≥{legacy})")

print("✅ Control panel ready. Adjust sliders above to tune thresholds.")"""))
    
    return cells

def main():
    print("🔄 Loading notebook...")
    
    # Create backup
    import shutil
    shutil.copy(NOTEBOOK_PATH, BACKUP_PATH)
    print(f"✅ Backup created: {BACKUP_PATH}")
    
    with open(NOTEBOOK_PATH, 'r') as f:
        nb = json.load(f)
    
    cells = nb['cells']
    print(f"📊 Original cell count: {len(cells)}")
    
    # Find section indices
    indices = find_section_indices(cells)
    print(f"\n📍 Section indices:")
    for key, value in sorted(indices.items()):
        print(f"  {key}: cell {value}")
    
    print("\n⚠️  Integration script ready but not yet complete.")
    print("   The full module code needs to be added to this script.")
    print("   Run this script after adding all required cell generators.")

if __name__ == '__main__':
    main()
