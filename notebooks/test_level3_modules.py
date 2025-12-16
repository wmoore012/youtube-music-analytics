#!/usr/bin/env python3
"""
Smoke Test for MusicScope™ Level 3 Modules

Tests that all 3 Level 3 modules can:
1. Import successfully
2. Execute without errors in DEMO mode (synthetic data)
3. Execute without errors in REAL mode (minimal structurally correct data)
4. Return expected metadata structure

Run from project root:
    cd notebooks && python3 test_level3_modules.py
"""

import sys
import os
from pathlib import Path

# Setup path: add project root and notebooks directory
project_root = Path(__file__).parent.parent
notebooks_dir = Path(__file__).parent

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(notebooks_dir))

print(f"📂 Project root: {project_root}")
print(f"📂 Notebooks dir: {notebooks_dir}")
print(f"📂 sys.path[0:2]: {sys.path[0:2]}\n")

# Suppress warnings and matplotlib display
import warnings
warnings.filterwarnings('ignore')

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend (no display)

# Suppress Plotly browser auto-open (CRITICAL for smoke tests!)
import plotly.io as pio
pio.renderers.default = 'json'  # Don't open browser tabs!

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============================================================================
# TEST 1: Import Design System
# ============================================================================
print("=" * 80)
print("TEST 1: Import Design System")
print("=" * 80)

try:
    from musicscope_design_system import (
        ms_hero_card, ms_subsection_card, ms_insight_card, ms_closing_card,
        ms_apply_plotly_layout, ms_matplotlib_title, ms_clean_spines,
        ms_require_data, ms_status_colors, MUSICSCOPE_COLORS, MOMENTUM_THRESHOLDS, CHART_PURPOSE
    )
    print("✅ Design system imported successfully")
    print(f"   - MUSICSCOPE_COLORS: {len(MUSICSCOPE_COLORS)} colors")
    print(f"   - MOMENTUM_THRESHOLDS: {MOMENTUM_THRESHOLDS}")
    print(f"   - CHART_PURPOSE: {len(CHART_PURPOSE)} chart definitions")
except Exception as e:
    print(f"❌ Design system import FAILED: {e}")
    sys.exit(1)

# ============================================================================
# TEST 2: Import Level 3 Modules
# ============================================================================
print("\n" + "=" * 80)
print("TEST 2: Import Level 3 Modules")
print("=" * 80)

try:
    from musicscope_momentum_level3 import render_momentum_section
    print("✅ Momentum module imported successfully")
except Exception as e:
    print(f"❌ Momentum module import FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

try:
    from musicscope_sentiment_level3 import render_sentiment_section
    print("✅ Sentiment module imported successfully")
except Exception as e:
    print(f"❌ Sentiment module import FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

try:
    from musicscope_performance_level3 import render_performance_section
    print("✅ Performance module imported successfully")
except Exception as e:
    print(f"❌ Performance module import FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# TEST 3: Create Minimal Test Data (for REAL mode testing)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 3: Create Minimal Test Data (for REAL mode)")
print("=" * 80)


def _build_minimal_videos_df() -> pd.DataFrame:
    """Build structurally correct minimal videos DataFrame for real mode testing."""
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


def _build_minimal_comments_df() -> pd.DataFrame:
    """Build structurally correct minimal comments DataFrame for real mode testing."""
    df = pd.DataFrame({
        'artist_name': ['Artist A'] * 50 + ['Artist B'] * 50,
        'sentiment_score': np.random.uniform(-1, 1, 100),
        'published_at': pd.date_range(start='2024-01-01', periods=100, freq='7H'),
        'comment_text': [f'Comment {i}' for i in range(100)]
    })
    # Add sentiment_category
    df['sentiment_category'] = pd.cut(
        df['sentiment_score'],
        bins=[-1.0, -0.1, 0.1, 1.0],
        labels=['negative', 'neutral', 'positive']
    )
    return df


# Create test data for real mode
videos_df = _build_minimal_videos_df()
comments_df = _build_minimal_comments_df()

print(f"✅ Test data created:")
print(f"   - videos_df: {len(videos_df)} rows, {list(videos_df.columns)}")
print(f"   - comments_df: {len(comments_df)} rows, {list(comments_df.columns)}")

# ============================================================================
# TEST 4: Execute Momentum Module (DEMO mode)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 4: Execute Momentum Module (DEMO mode)")
print("=" * 80)

try:
    momentum_meta_demo = render_momentum_section(videos_df=None, demo=True)
    print(f"✅ Momentum module (DEMO) executed successfully")
    print(f"   Metadata: {momentum_meta_demo}")

    # Validate metadata structure
    assert 'section' in momentum_meta_demo, "Missing 'section' key"
    assert 'charts_rendered' in momentum_meta_demo, "Missing 'charts_rendered' key"
    assert 'artists_analyzed' in momentum_meta_demo, "Missing 'artists_analyzed' key"
    assert momentum_meta_demo['section'] == 'momentum', f"Expected section='momentum', got {momentum_meta_demo['section']}"
    print(f"   ✅ Metadata structure validated")

except Exception as e:
    print(f"❌ Momentum module (DEMO) execution FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# TEST 5: Execute Momentum Module (REAL mode)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 5: Execute Momentum Module (REAL mode)")
print("=" * 80)

try:
    momentum_meta_real = render_momentum_section(videos_df=videos_df, demo=False)
    print(f"✅ Momentum module (REAL) executed successfully")
    print(f"   Metadata: {momentum_meta_real}")

    # Validate metadata structure
    assert 'section' in momentum_meta_real, "Missing 'section' key"
    assert 'charts_rendered' in momentum_meta_real, "Missing 'charts_rendered' key"
    assert 'artists_analyzed' in momentum_meta_real, "Missing 'artists_analyzed' key"
    assert momentum_meta_real['section'] == 'momentum', f"Expected section='momentum', got {momentum_meta_real['section']}"
    assert momentum_meta_real['charts_rendered'] >= 3, "Expected at least 3 charts (control panel + bar race + calendar)"
    print(f"   ✅ Metadata structure validated")

except Exception as e:
    print(f"❌ Momentum module (REAL) execution FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# TEST 6: Execute Sentiment Module (DEMO mode)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 6: Execute Sentiment Module (DEMO mode)")
print("=" * 80)

try:
    sentiment_meta_demo = render_sentiment_section(videos_df=None, comments_df=None, demo=True)
    print(f"✅ Sentiment module (DEMO) executed successfully")
    print(f"   Metadata: {sentiment_meta_demo}")

    # Validate metadata structure
    assert 'section' in sentiment_meta_demo, "Missing 'section' key"
    assert 'charts_rendered' in sentiment_meta_demo, "Missing 'charts_rendered' key"
    assert 'comments_analyzed' in sentiment_meta_demo, "Missing 'comments_analyzed' key"
    assert 'net_sentiment_score' in sentiment_meta_demo, "Missing 'net_sentiment_score' key"
    assert sentiment_meta_demo['section'] == 'sentiment', f"Expected section='sentiment', got {sentiment_meta_demo['section']}"
    print(f"   ✅ Metadata structure validated")

except Exception as e:
    print(f"❌ Sentiment module (DEMO) execution FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# TEST 7: Execute Sentiment Module (REAL mode)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 7: Execute Sentiment Module (REAL mode)")
print("=" * 80)

try:
    sentiment_meta_real = render_sentiment_section(videos_df=videos_df, comments_df=comments_df, demo=False)
    print(f"✅ Sentiment module (REAL) executed successfully")
    print(f"   Metadata: {sentiment_meta_real}")

    # Validate metadata structure
    assert 'section' in sentiment_meta_real, "Missing 'section' key"
    assert 'charts_rendered' in sentiment_meta_real, "Missing 'charts_rendered' key"
    assert 'comments_analyzed' in sentiment_meta_real, "Missing 'comments_analyzed' key"
    assert 'net_sentiment_score' in sentiment_meta_real, "Missing 'net_sentiment_score' key"
    assert sentiment_meta_real['section'] == 'sentiment', f"Expected section='sentiment', got {sentiment_meta_real['section']}"
    assert sentiment_meta_real['charts_rendered'] >= 3, "Expected at least 3 charts"
    print(f"   ✅ Metadata structure validated")

except Exception as e:
    print(f"❌ Sentiment module (REAL) execution FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# TEST 8: Execute Performance Module (DEMO mode)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 8: Execute Performance Module (DEMO mode)")
print("=" * 80)

try:
    performance_meta_demo = render_performance_section(videos_df=None, comments_df=None, demo=True)
    print(f"✅ Performance module (DEMO) executed successfully")
    print(f"   Metadata: {performance_meta_demo}")

    # Validate metadata structure
    assert 'section' in performance_meta_demo, "Missing 'section' key"
    assert 'charts_rendered' in performance_meta_demo, "Missing 'charts_rendered' key"
    assert 'hidden_gems_count' in performance_meta_demo, "Missing 'hidden_gems_count' key"
    assert 'total_engagement' in performance_meta_demo, "Missing 'total_engagement' key"
    assert performance_meta_demo['section'] == 'performance', f"Expected section='performance', got {performance_meta_demo['section']}"
    print(f"   ✅ Metadata structure validated")

except Exception as e:
    print(f"❌ Performance module (DEMO) execution FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# TEST 9: Execute Performance Module (REAL mode)
# ============================================================================
print("\n" + "=" * 80)
print("TEST 9: Execute Performance Module (REAL mode)")
print("=" * 80)

try:
    performance_meta_real = render_performance_section(videos_df=videos_df, comments_df=comments_df, demo=False)
    print(f"✅ Performance module (REAL) executed successfully")
    print(f"   Metadata: {performance_meta_real}")

    # Validate metadata structure
    assert 'section' in performance_meta_real, "Missing 'section' key"
    assert 'charts_rendered' in performance_meta_real, "Missing 'charts_rendered' key"
    assert 'hidden_gems_count' in performance_meta_real, "Missing 'hidden_gems_count' key"
    assert 'total_engagement' in performance_meta_real, "Missing 'total_engagement' key"
    assert performance_meta_real['section'] == 'performance', f"Expected section='performance', got {performance_meta_real['section']}"
    assert performance_meta_real['charts_rendered'] >= 4, "Expected at least 4 charts"
    print(f"   ✅ Metadata structure validated")

except Exception as e:
    print(f"❌ Performance module (REAL) execution FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================================
# FINAL SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("🎉 ALL TESTS PASSED")
print("=" * 80)

print("\n📊 Summary (DEMO mode):")
print(f"   ✅ Design system: Imported successfully")
print(f"   ✅ Momentum module: {momentum_meta_demo['charts_rendered']} charts rendered")
print(f"   ✅ Sentiment module: {sentiment_meta_demo['charts_rendered']} charts rendered, NSS={sentiment_meta_demo['net_sentiment_score']:.1f}")
print(f"   ✅ Performance module: {performance_meta_demo['charts_rendered']} charts rendered, {performance_meta_demo['hidden_gems_count']} hidden gems")

print("\n📊 Summary (REAL mode):")
print(f"   ✅ Momentum module: {momentum_meta_real['charts_rendered']} charts rendered")
print(f"   ✅ Sentiment module: {sentiment_meta_real['charts_rendered']} charts rendered, NSS={sentiment_meta_real['net_sentiment_score']:.1f}")
print(f"   ✅ Performance module: {performance_meta_real['charts_rendered']} charts rendered, {performance_meta_real['hidden_gems_count']} hidden gems")

print("\n✅ All Level 3 modules are production-ready for notebook integration.")
print("\nNext steps:")
print("   1. Integrate into MusicScope™ Professional Dashboard notebook")
print("   2. Test with real data (videos_df, comments_df)")
print("   3. Visual verification of all charts")
print("   4. Create git commit after successful integration")


