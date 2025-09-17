#!/usr/bin/env python3
"""
Create the final notebook that uses ONLY real data - no fake/mock data.
"""

import json


def create_real_data_notebook():
    """Create notebook that uses only real data."""

    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {"display_name": "YouTube Analytics", "language": "python", "name": "youtubeviz"},
            "language_info": {"name": "python", "version": "3.12.0"},
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
                "# 🎵 MusicScope™ Real Data Analytics Dashboard\n",
                "\n",
                "**All 15 Data-Science Grade Charts - REAL DATA ONLY**\n",
                "\n",
                "This notebook uses ONLY real data from the database. No fake/mock data.\n",
                "Charts will show data requirements if columns are missing.\n",
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
                "# Core imports\n",
                "import pandas as pd\n",
                "import numpy as np\n",
                "import plotly.graph_objects as go\n",
                "import warnings\n",
                "warnings.filterwarnings('ignore')\n",
                "\n",
                "print('✅ Core imports loaded')\n",
                "\n",
                "# Import all 15 chart functions\n",
                "from youtubeviz.advanced_charts import (\n",
                "    create_diverging_sentiment_bars,\n",
                "    create_sentiment_cluster_heatmap,\n",
                "    create_positive_theme_lollipops,\n",
                "    create_negative_theme_lollipops,\n",
                "    create_standout_videos_scatter,\n",
                "    create_tour_compatibility_analysis,\n",
                "    create_upset_feature_intersections,\n",
                "    create_isrc_balance_bars,\n",
                "    create_content_length_dumbbells,\n",
                "    create_content_type_dots,\n",
                "    create_views_by_category_areas,\n",
                "    create_genre_context_heatmap,\n",
                "    create_roster_rank_bump_chart,\n",
                "    create_polarity_ridgelines,\n",
                "    create_ab_test_framework,\n",
                "    enhance_chart_beauty\n",
                ")\n",
                "\n",
                "print('✅ All 15 chart functions imported successfully!')\n",
            ],
        }
    )

    # Real data loading only
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Load REAL data only - no fake data fallback\n",
                "try:\n",
                "    from youtubeviz.data import load_recent_window_days\n",
                "    df = load_recent_window_days(30)\n",
                "    print(f'✅ Loaded REAL data: {len(df):,} records')\n",
                '    print(f\'🎭 Artists: {", ".join(df["artist_name"].unique())}\')\n',
                "    print(f'📊 Columns: {list(df.columns)}')\n",
                "    \n",
                "    # Show data quality summary\n",
                "    print(f'\\n📈 Data Quality Summary:')\n",
                "    for col in ['sentiment_category', 'daily_views', 'engagement_rate', 'has_isrc', 'content_type']:\n",
                "        if col in df.columns:\n",
                "            non_null = df[col].notna().sum()\n",
                "            print(f'   ✅ {col}: {non_null:,}/{len(df):,} records ({non_null/len(df)*100:.1f}%)')\n",
                "        else:\n",
                "            print(f'   ❌ {col}: Missing - charts will show requirements')\n",
                "    \n",
                "except Exception as e:\n",
                "    print(f'❌ Real data loading failed: {e}')\n",
                "    print('🚫 NO FAKE DATA FALLBACK')\n",
                "    print('💡 Charts will show data requirements')\n",
                "    \n",
                "    # Create empty dataframe\n",
                "    df = pd.DataFrame()\n",
                "    print('⚠️  Empty dataframe - fix data loading to see real analytics')\n",
                "\n",
                "print(f'\\n🎯 Ready for real data analysis!')\n",
            ],
        }
    )

    # Generate all 15 charts
    chart_functions = [
        ("create_diverging_sentiment_bars", "Sentiment Breakdown by Artist"),
        ("create_sentiment_cluster_heatmap", "Sentiment Model Categories Heatmap"),
        ("create_positive_theme_lollipops", "Top 3 Positive Themes per Artist"),
        ("create_negative_theme_lollipops", "Top 3 Negative Themes per Artist"),
        ("create_standout_videos_scatter", "Standout Videos Analysis"),
        ("create_tour_compatibility_analysis", "Tour Compatibility Analysis"),
        ("create_upset_feature_intersections", "Feature Intersection Analysis"),
        ("create_isrc_balance_bars", "ISRC vs Non-ISRC Balance"),
        ("create_content_length_dumbbells", "Content Length Analysis"),
        ("create_content_type_dots", "Content Type Breakdown"),
        ("create_views_by_category_areas", "Views by Category Over Time"),
        ("create_genre_context_heatmap", "Genre Context Analysis"),
        ("create_roster_rank_bump_chart", "Roster Ranking Over Time"),
        ("create_polarity_ridgelines", "Comment Polarity Distributions"),
        ("create_ab_test_framework", "A/B Test Framework"),
    ]

    for i, (func_name, title) in enumerate(chart_functions, 1):
        # Markdown header
        cells.append({"cell_type": "markdown", "metadata": {}, "source": f"## Chart {i}: {title}\n"})

        # Chart generation code
        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# Chart {i}: {title}\n",
                    f"print(f'🎨 Generating Chart {i}: {title}...')\n",
                    f"\n",
                    f"try:\n",
                    f"    fig_{i} = {func_name}(df)\n",
                    f"    fig_{i} = enhance_chart_beauty(fig_{i}, theme='professional')\n",
                    f"    fig_{i}.show()\n",
                    f"    \n",
                    f"    # Check if it's a real chart or data requirement message\n",
                    f"    if hasattr(fig_{i}, 'data') and len(fig_{i}.data) > 0:\n",
                    f"        print(f'✅ Chart {i} SUCCESS: {{len(fig_{i}.data)}} traces, {{fig_{i}.layout.height or 500}}px')\n",
                    f"    else:\n",
                    f"        print(f'📋 Chart {i} shows data requirements')\n",
                    f"    \n",
                    f"except Exception as e:\n",
                    f"    print(f'❌ Chart {i} FAILED: {{e}}')\n",
                    f"    \n",
                    f"    # Create error chart\n",
                    f"    fig_{i} = go.Figure()\n",
                    f"    fig_{i}.add_annotation(\n",
                    f"        text=f'Chart {i} Error: {{str(e)[:100]}}',\n",
                    f"        x=0.5, y=0.5, showarrow=False,\n",
                    f"        font=dict(size=14, color='red')\n",
                    f"    )\n",
                    f"    fig_{i}.update_layout(\n",
                    f"        title='Chart {i}: {title} (Error)',\n",
                    f"        height=300,\n",
                    f"        template='plotly_white'\n",
                    f"    )\n",
                    f"    fig_{i}.show()\n",
                ],
            }
        )

    # Final validation
    cells.append({"cell_type": "markdown", "metadata": {}, "source": "## 🎯 Real Data Analytics Summary\n"})

    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Count charts with real data vs data requirements\n",
                "real_charts = []\n",
                "requirement_charts = []\n",
                "error_charts = []\n",
                "\n",
                "for i in range(1, 16):\n",
                "    fig_name = f'fig_{i}'\n",
                "    if fig_name in locals():\n",
                "        fig = locals()[fig_name]\n",
                "        if hasattr(fig, 'data') and len(fig.data) > 0:\n",
                "            # Check if it's a real chart or just annotation\n",
                "            has_real_data = any(\n",
                "                hasattr(trace, 'x') and len(getattr(trace, 'x', [])) > 1 \n",
                "                for trace in fig.data\n",
                "            )\n",
                "            if has_real_data:\n",
                "                real_charts.append(i)\n",
                "            else:\n",
                "                # Check if it's showing data requirements\n",
                "                annotations = getattr(fig.layout, 'annotations', [])\n",
                "                if any('need' in str(ann.text).lower() for ann in annotations):\n",
                "                    requirement_charts.append(i)\n",
                "                else:\n",
                "                    error_charts.append(i)\n",
                "        else:\n",
                "            error_charts.append(i)\n",
                "\n",
                "print('🎯 REAL DATA ANALYTICS SUMMARY')\n",
                "print('=' * 50)\n",
                "print(f'📊 Charts with REAL data: {len(real_charts)}/15')\n",
                "print(f'📋 Charts showing data requirements: {len(requirement_charts)}/15')\n",
                "print(f'❌ Charts with errors: {len(error_charts)}/15')\n",
                "\n",
                "if real_charts:\n",
                "    print(f'\\n✅ Working with real data: {real_charts}')\n",
                "if requirement_charts:\n",
                "    print(f'📋 Need data columns: {requirement_charts}')\n",
                "if error_charts:\n",
                "    print(f'❌ Have errors: {error_charts}')\n",
                "\n",
                "# Success message\n",
                "if len(real_charts) >= 5:\n",
                "    print(f'\\n🎉 SUCCESS: {len(real_charts)} charts working with REAL data!')\n",
                "    print('💝 No fake data used - authentic analytics only!')\n",
                "elif len(real_charts) >= 1:\n",
                "    print(f'\\n🌱 PROGRESS: {len(real_charts)} charts working with real data!')\n",
                "    print('🔧 Add missing data columns to unlock more charts!')\n",
                "else:\n",
                "    print('\\n📋 All charts show data requirements - add real data to see analytics!')\n",
                "\n",
                "print('\\n🎵 MusicScope™ Real Data Analytics Complete! 🎵')\n",
            ],
        }
    )

    notebook["cells"] = cells

    # Write the notebook
    output_path = "notebooks/MusicScope™_Real_Data_Dashboard.ipynb"
    with open(output_path, "w") as f:
        json.dump(notebook, f, indent=2)

    print(f"✅ Created real data notebook: {output_path}")
    print(f"📊 Contains {len(cells)} cells with NO FAKE DATA")
    print(f"🎯 Charts will show data requirements if columns missing")

    return output_path


if __name__ == "__main__":
    create_real_data_notebook()
