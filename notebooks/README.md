# 📊 YouTube Analytics Notebooks

**Professional-grade analytics for music industry executives, analysts, and data scientists.**

Transform YouTube data into actionable insights with interactive visualizations, sentiment analysis, and comprehensive performance tracking.

## 🎯 Quick Start Guide

### For Music Industry Executives
1. **Portfolio Overview**: `analysis/01_descriptive_overview.ipynb` - High-level KPIs and trends
2. **Artist Benchmarking**: `analysis/02_artist_comparison.ipynb` - Side-by-side performance analysis
3. **Investment Decisions**: `analysis/02_artist_deepdives.ipynb` - Deep dive into individual artists

### For Data Teams & Operations
4. **Data Quality**: `quality/03_appendix_data_quality.ipynb` - Validate data integrity and consistency
5. **Pipeline Health**: `operations/etl_dashboard.ipynb` - Monitor ETL performance and issues

## 📁 Human-Friendly Folder Structure

### 📈 **`analysis/`** - Business Intelligence & Decision Making
**Target Users**: Executives, A&R teams, marketing managers, analysts
- **Portfolio Performance**: Track overall label/roster performance
- **Artist Comparison**: Benchmark artists against peers and identify opportunities
- **Individual Deep Dives**: Detailed analysis for investment and marketing decisions
- **Revenue Analytics**: Estimate monetization potential and ROI

### ⚙️ **`operations/`** - Data Pipeline & System Health
**Target Users**: Data engineers, technical administrators, DevOps teams
- **ETL Monitoring**: Track data pipeline health and performance
- **Error Tracking**: Identify and resolve data processing issues
- **System Metrics**: Monitor database performance and API usage
- **Maintenance Tools**: Automated cleanup and optimization notebooks

### 🔍 **`quality/`** - Data Validation & Trust
**Target Users**: Data scientists, analysts, quality assurance teams
- **Consistency Validation**: Ensure data integrity across all functions
- **Anomaly Detection**: Identify unusual patterns or data quality issues
- **Temporal Analysis**: Track data freshness and identify gaps
- **Compliance Monitoring**: YouTube API ToS compliance and retention policies

### 📋 **`templates/`** - Reusable Analysis Templates
**Target Users**: Analysts creating new reports, data scientists extending analysis
- **Clean Templates**: Output-free versions ready for customization
- **Standardized Structure**: Consistent formatting and best practices
- **Extension Ready**: Base templates for creating specialized analysis

## 🎨 Notebook Design Philosophy

### Storytelling Approach
- **Narrative Flow**: Each notebook tells a complete, compelling story
- **Human Connection**: Remember these are real artists' careers and dreams
- **Educational Focus**: Explain music industry concepts for data science students
- **Actionable Insights**: Every analysis leads to clear recommendations

### Technical Standards
- **Interactive Visualizations**: All charts use Plotly/Altair for interactivity
- **Consistent Branding**: Global color schemes for artists and categories
- **Mobile-Friendly**: Charts work across different screen sizes
- **Performance Optimized**: Under 25 cells, <200 LOC per notebook

## 🛠️ Development Guidelines

### Code Organization
```python
# Standard imports and configuration
from youtubeviz.utils import safe_head, filter_artists
from youtubeviz.charts import views_over_time_plotly, artist_compare_altair
from youtubeviz.data import load_recent_window_days, compute_kpis

# Artist selection (load respectfully from shared roster configuration)
from pathlib import Path
import json

roster_path = Path("config/expected_artists.json")
expected_roster = json.loads(roster_path.read_text())
tracked_artists = expected_roster["expected_artists"]

# Load and filter data
df = load_recent_window_days(days=90, engine=engine)
artist_data = filter_artists(df, "artist_name", tracked_artists)
```

### Visualization Patterns
```python
# KPI Summary Table
kpis = compute_kpis(artist_data)
safe_head(kpis, ["artist_name", "total_views", "videos", "median_views"])

# Interactive Time Series
fig = views_over_time_plotly(
    artist_data,
    date_col="date",
    value_col="views",
    group_col="artist_name",
    hover_col="video_title"
)
fig.show()

# Linked Exploration (Altair)
linked_scatter_detail_altair(artist_data, "views", "likes", "artist_name", "video_title")
```

## ⚙️ Configuration & Customization

### Environment Variables (.env)
```bash
# Artist visualization colors
ARTIST_COLORS_JSON='{"Artist A":"#1f77b4", "Artist B":"#ff7f0e"}'

# Revenue estimation
REVENUE_RPM_DEFAULT=2.50
REVENUE_RPM_MAP_JSON='{"Premium Artist":"5.00", "Emerging Artist":"1.50"}'

# Analysis parameters
MOMENTUM_THRESHOLD_DAYS=30
GROWTH_RATE_MINIMUM=0.05
```

### Quality Standards
- **Pre-commit hooks**: Automatically strip outputs with `pre-commit install`
- **Modular design**: Reusable code goes in `youtubeviz` package
- **Error handling**: Graceful failures with clear user messages
- **Documentation**: Every complex analysis includes explanatory text

## 🎵 Music Industry Context

### Respectful Analysis
- **Artist-Centric**: Show compassion for artists' journeys and challenges
- **Privacy Conscious**: Protect sensitive performance data appropriately
- **Cultural Awareness**: Consider diverse musical backgrounds and markets
- **Growth-Focused**: Highlight opportunities, not just current performance

### Business Applications
- **A&R Intelligence**: Identify emerging talent and market opportunities
- **Marketing ROI**: Justify budget allocation with data-driven insights
- **Competitive Analysis**: Benchmark performance within appropriate peer groups
- **Investment Decisions**: Provide clear metrics for label executive decisions

---

**Ready to dive in?** Start with `analysis/01_descriptive_overview.ipynb` for a comprehensive portfolio view, then explore individual artists with the deep dive notebooks.
## 🧭 Editable vs Executed

- `editable/`: Authoring notebooks you open and run in Jupyter. Keep these short, story-first, and output-light.
- `executed/`: Auto-generated results (executed `.ipynb` and `.md` summaries) produced by `execute_*.py` and CI.

This separation keeps authoring clean while preserving auditable outputs. If CI runs your notebooks, it writes the
human-friendly markdown summaries here (e.g., `02_artist_comparison_results.md`).

## 🎭 Story Blocks (Charts + Human Narrative, Side-by-Side)

Use the storytelling helper to place interactive charts next to executive-friendly bullets in the same notebook cell:

```python
import json
from pathlib import Path

import pandas as pd

from youtubeviz.storytelling import story_block, quick_takeaways
from youtubeviz.charts import views_over_time_advanced
from youtubeviz.data import load_recent_window_days

roster = json.loads(Path("config/expected_artists.json").read_text())
tracked_artists = roster["expected_artists"]

recent_metrics = load_recent_window_days(artists=tracked_artists, days=90)

fig = views_over_time_advanced(
    recent_metrics,
    date_col="date",
    value_col="views",
    group_col="artist_name",
    rolling_window=7,
    highlight_artists=[tracked_artists[0]],
)

momentum = (
    recent_metrics.groupby("artist_name")
    .apply(
        lambda frame: pd.Series(
            {
                "seven_day_views": frame.sort_values("date").tail(7)["views"].sum(),
                "previous_week_views": frame.sort_values("date").tail(14).head(7)["views"].sum(),
                "engagement_rate": (frame["likes"] + frame["comments"]).sum()
                / frame["views"].clip(lower=1).sum()
                * 100,
                "latest_video": frame.sort_values("date").iloc[-1]["video_title"],
            }
        )
    )
)
momentum["seven_day_growth_pct"] = (
    (momentum["seven_day_views"] - momentum["previous_week_views"])
    / momentum["previous_week_views"].clip(lower=1)
    * 100
)

highlight_artist = momentum.sort_values("seven_day_growth_pct", ascending=False).index[0]
metrics = momentum.loc[highlight_artist]

bullets = quick_takeaways(
    artist=highlight_artist,
    last_7d_change_pct=metrics["seven_day_growth_pct"],
    engagement_rate=metrics["engagement_rate"],
    standout_video=metrics["latest_video"],
)

story_block(
    fig,
    title=f"🚀 {highlight_artist} is accelerating — seven-day momentum is climbing",
    bullets=bullets,
    caption="Recommendation: celebrate the progress and plan supportive outreach this week",
)
```

Tip: Repeat `story_block` across descriptive (what happened), prescriptive (what to do), and predictive (what’s next)
sections to keep analysis fun, human, and visually grounded.
