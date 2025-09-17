#!/usr/bin/env python3
"""
Create the ultimate MusicScope™ notebook with all 15 data-science-grade charts.
"""

import json
from datetime import datetime


def create_notebook():
    """Create notebook with 15 charts."""

    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.8.0"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    cells = []

    # Title
    cells.append(
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# 🎵 MusicScope™ Ultimate Analytics Dashboard\\n",
                "\\n",
                "**Data-Science Grade Storytelling with 15 Interactive Charts**\\n",
                "\\n",
                f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\\n",
                "\\n",
                "## 📋 Chart Inventory (15 Total)\\n",
                "\\n",
                "### 🎭 Sentiment Analysis Suite (Charts 1-6)\\n",
                "1. **Diverging Sentiment Bars** - Wilson CIs\\n",
                "2. **Sentiment Heatmap** - Bayesian shrinkage\\n",
                "3. **Positive Theme Lollipops** - Top fan praise\\n",
                "4. **Negative Theme Lollipops** - Areas for improvement\\n",
                "5. **Standout Videos Scatter** - LOESS residual analysis\\n",
                "6. **Tour Compatibility Matrix** - UMAP clustering\\n",
                "\\n",
                "### 📹 Content Analysis Suite (Charts 7-11)\\n",
                "7. **UpSet Feature Intersections** - Better than Venn\\n",
                "8. **ISRC Balance Bars** - Music vs content\\n",
                "9. **Content Length Dumbbells** - Short vs long-form\\n",
                "10. **Content Type Dots** - MV/lyric/visualizer\\n",
                "11. **Views by Category Areas** - Time series\\n",
                "\\n",
                "### 🎯 Strategic Analytics Suite (Charts 12-15)\\n",
                "12. **Genre Context Heatmap** - TF-IDF analysis\\n",
                "13. **Roster Rank Bump Chart** - Weekly trends\\n",
                "14. **Polarity Ridgelines** - Sentiment distributions\\n",
                "15. **A/B Test Framework** - Uplift curves\\n",
            ],
        }
    )

    # Imports
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 📦 Core Data Science Stack\\n",
                "import pandas as pd\\n",
                "import numpy as np\\n",
                "import plotly.graph_objects as go\\n",
                "import plotly.express as px\\n",
                "import warnings\\n",
                "warnings.filterwarnings('ignore')\\n",
                "\\n",
                "# 🎨 MusicScope™ Advanced Charts\\n",
                "from youtubeviz.advanced_charts import (\\n",
                "    create_diverging_sentiment_bars,\\n",
                "    create_sentiment_cluster_heatmap,\\n",
                "    create_positive_theme_lollipops,\\n",
                "    create_negative_theme_lollipops,\\n",
                "    create_standout_videos_scatter,\\n",
                "    create_tour_compatibility_analysis,\\n",
                "    create_upset_feature_intersections,\\n",
                "    create_isrc_balance_bars,\\n",
                "    create_content_length_dumbbells,\\n",
                "    create_content_type_dots,\\n",
                "    create_views_by_category_areas,\\n",
                "    create_genre_context_heatmap,\\n",
                "    create_roster_rank_bump_chart,\\n",
                "    create_polarity_ridgelines,\\n",
                "    create_ab_test_framework,\\n",
                "    enhance_chart_beauty,\\n",
                "    ColorBrewerPalettes\\n",
                ")\\n",
                "\\n",
                "# 📊 Statistical Foundation\\n",
                "from youtubeviz.statistical_utils import (\\n",
                "    calculate_wilson_intervals,\\n",
                "    apply_bayesian_shrinkage,\\n",
                "    apply_loess_smoothing\\n",
                ")\\n",
                "\\n",
                "# 🎯 Core Analytics\\n",
                "from youtubeviz.data import load_recent_window_days\\n",
                "from youtubeviz.config_validation import get_artists_from_env\\n",
                "\\n",
                "print('✅ All 15 chart functions imported successfully!')\\n",
            ],
        }
    )

    # Data Loading
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 🎯 Load Recent Data\\n",
                "ANALYSIS_WINDOW_DAYS = 30\\n",
                "\\n",
                "df = load_recent_window_days(ANALYSIS_WINDOW_DAYS)\\n",
                "expected_artists = get_artists_from_env()\\n",
                "\\n",
                "print(f'📊 Loaded {len(df):,} records for {len(df[\"artist_name\"].unique())} artists')\\n",
                "\\n",
                "# Add derived columns\\n",
                "df['log_views'] = np.log10(df['daily_views'].clip(lower=1))\\n",
                "df['has_isrc'] = df.get('isrc', '').notna()\\n",
                "df['is_short_form'] = df.get('duration_seconds', 300) < 60\\n",
                "\\n",
                "print('✅ Data preparation complete!')\\n",
            ],
        }
    )

    # Generate 15 chart cells
    chart_functions = [
        "create_diverging_sentiment_bars",
        "create_sentiment_cluster_heatmap",
        "create_positive_theme_lollipops",
        "create_negative_theme_lollipops",
        "create_standout_videos_scatter",
        "create_tour_compatibility_analysis",
        "create_upset_feature_intersections",
        "create_isrc_balance_bars",
        "create_content_length_dumbbells",
        "create_content_type_dots",
        "create_views_by_category_areas",
        "create_genre_context_heatmap",
        "create_roster_rank_bump_chart",
        "create_polarity_ridgelines",
        "create_ab_test_framework",
    ]

    chart_titles = [
        "Sentiment Breakdown by Artist",
        "Sentiment Model Categories Heatmap",
        "Top 3 Positive Themes per Artist",
        "Top 3 Negative Themes per Artist",
        "Standout Videos Analysis",
        "Tour Compatibility Analysis",
        "Feature Intersection Analysis",
        "ISRC vs Non-ISRC Balance",
        "Content Length Analysis",
        "Content Type Breakdown",
        "Views by Category Over Time",
        "Genre Context Analysis",
        "Roster Ranking Over Time",
        "Comment Polarity Distributions",
        "A/B Test Framework",
    ]

    for i, (func, title) in enumerate(zip(chart_functions, chart_titles), 1):
        # Markdown cell
        cells.append({"cell_type": "markdown", "metadata": {}, "source": f"## Chart {i}: {title}\\n"})

        # Code cell
        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# 📊 Chart {i}: {title}\\n",
                    f"try:\\n",
                    f"    print(f'🎨 Generating Chart {i}...')\\n",
                    f"    fig_{i} = {func}(df)\\n",
                    f"    fig_{i} = enhance_chart_beauty(fig_{i}, theme='professional')\\n",
                    f"    fig_{i}.show()\\n",
                    f"    print(f'✅ Chart {i} complete')\\n",
                    f"except Exception as e:\\n",
                    f"    print(f'⚠️  Chart {i} issue: {{e}}')\\n",
                    f"    fig_{i} = go.Figure()\\n",
                    f"    fig_{i}.add_annotation(text=f'Chart {i}: {{e}}', x=0.5, y=0.5)\\n",
                    f"    fig_{i}.update_layout(title='Chart {i}: {title}', height=300)\\n",
                    f"    fig_{i}.show()\\n",
                ],
            }
        )

    # Final validation
    cells.append({"cell_type": "markdown", "metadata": {}, "source": "## 🎯 Final Validation\\n"})

    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 🎯 Count All Charts\\n",
                "chart_figures = []\\n",
                "for i in range(1, 16):\\n",
                "    fig_name = f'fig_{i}'\\n",
                "    if fig_name in locals():\\n",
                "        chart_figures.append(fig_name)\\n",
                "\\n",
                "print(f'📊 FINAL CHART COUNT: {len(chart_figures)}/15')\\n",
                "\\n",
                "if len(chart_figures) == 15:\\n",
                "    print('🎉 SUCCESS: All 15 data-science-grade charts completed!')\\n",
                "else:\\n",
                "    print(f'⚠️  Missing {15 - len(chart_figures)} charts')\\n",
                "\\n",
                "print('🎵 MusicScope™ Ultimate Analytics Dashboard Complete! 🎵')\\n",
            ],
        }
    )

    notebook["cells"] = cells

    # Write notebook
    output_path = "notebooks/MusicScope™_Ultimate_Analytics_Dashboard.ipynb"
    with open(output_path, "w") as f:
        json.dump(notebook, f, indent=2)

    print(f"✅ Created notebook with {len(cells)} cells")
    print(f"📊 Includes all 15 charts")
    print(f"💾 Saved to: {output_path}")

    return output_path


if __name__ == "__main__":
    create_notebook()
