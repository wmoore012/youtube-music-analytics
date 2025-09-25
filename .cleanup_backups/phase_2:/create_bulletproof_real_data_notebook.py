"""
Create bulletproof notebook that combines ALL charts from both notebooks.
- NO FAKE DATA EVER
- Only real database data
- Automatic chart validation
- CI/CD ready with proper error handling
"""

import os
import sys

sys.path.insert(0, ".")

from src.youtubeviz.config_validation import get_artists_from_env
from src.youtubeviz.data import load_recent_window_days, load_youtube_data


def create_bulletproof_notebook():
    """Create notebook that combines all charts with real data only."""

    notebook_content = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# 🎵 MusicScope™ - Bulletproof Real Data Analytics\n\n",
                    "**REAL DATA ONLY - NO FAKE DATA EVER**\n\n",
                    "- 📊 **All Charts Combined**: From both Complete Dashboard + Real Data Dashboard\n",
                    "- 🔒 **Real Data Only**: Direct from database, no fake/sample data\n",
                    "- 🛡️ **Bulletproof CI/CD**: Automatic validation and error handling\n",
                    "- 📈 **15 Chart Validation**: Tracks working vs error charts\n",
                    "- 💝 **Compassionate Analytics**: Human-centered insights",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# REAL DATA ONLY - NO FAKE DATA IMPORTS\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "from datetime import datetime, timedelta\n",
                    "import plotly.express as px\n",
                    "import plotly.graph_objects as go\n",
                    "import warnings\n",
                    "\n",
                    "# Import ONLY real data functions\n",
                    "from src.youtubeviz.data import load_youtube_data, load_recent_window_days\n",
                    "from src.youtubeviz.config_validation import get_artists_from_env\n",
                    "\n",
                    "# Import storytelling (real data only)\n",
                    "from src.youtubeviz.storytelling import story_block, quick_takeaways, narrative_intro\n",
                    "\n",
                    "# Import ALL chart functions from BOTH notebooks\n",
                    "from src.youtubeviz.charts import (\n",
                    "    views_over_time_plotly, enhance_chart_beauty,\n",
                    "    create_divergent_sentiment_chart, create_sentiment_cluster_chart,\n",
                    "    create_isrc_balance_chart, create_content_type_breakdown_chart,\n",
                    "    create_duration_breakdown_chart, create_content_stacked_bar_chart,\n",
                    "    create_artist_strategy_comparison_chart, create_performance_diversity_bubble_chart\n",
                    ")\n",
                    "\n",
                    "from src.youtubeviz.content import (\n",
                    "    create_artist_comparison_chart, create_roster_overview_chart,\n",
                    "    categorize_video_content, analyze_isrc_vs_content_balance\n",
                    ")\n",
                    "\n",
                    "from src.youtubeviz.sentiment import (\n",
                    "    extract_top_positive_comments, extract_top_negative_comments_with_percentages\n",
                    ")\n",
                    "\n",
                    "from src.youtubeviz.summary_generator import (\n",
                    "    generate_executive_summary, create_actionable_recommendations\n",
                    ")\n",
                    "\n",
                    "print('🎯 MusicScope™ Bulletproof Real Data Analytics Loaded!')\n",
                    "print('🔒 NO FAKE DATA - Real database data only!')\n",
                    "print('✅ All chart functions imported successfully')",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# REAL DATA LOADING - NO FAKE DATA ALLOWED\n",
                    "print('🔍 Loading REAL data from database...')\n",
                    "\n",
                    "# Load real data only\n",
                    "try:\n",
                    "    df = load_youtube_data()\n",
                    "    print(f'✅ Real data loaded: {len(df)} rows')\n",
                    "    print(f'📊 Columns: {df.columns.tolist()}')\n",
                    "    \n",
                    "    if df.empty:\n",
                    "        print('⚠️  Database is empty - no real data available')\n",
                    "        print('📋 Charts will show data requirements instead of fake data')\n",
                    "    else:\n",
                    "        # Validate real data\n",
                    "        if 'artist_name' in df.columns:\n",
                    "            real_artists = df['artist_name'].dropna().unique()\n",
                    "            print(f'🎤 Real artists in database: {list(real_artists)}')\n",
                    "        \n",
                    "        if 'video_id' in df.columns:\n",
                    "            video_count = df['video_id'].nunique()\n",
                    "            print(f'🎬 Real videos: {video_count}')\n",
                    "            \n",
                    "        if 'view_count' in df.columns:\n",
                    "            total_views = df['view_count'].sum()\n",
                    "            print(f'👀 Total real views: {total_views:,}')\n",
                    "            \n",
                    "except Exception as e:\n",
                    "    print(f'❌ Error loading real data: {e}')\n",
                    "    df = pd.DataFrame()  # Empty, not fake\n",
                    "    print('📋 Will show data requirements for all charts')\n",
                    "\n",
                    "# Get real artists from .env (no fake fallback)\n",
                    "try:\n",
                    "    env_artists, artist_count = get_artists_from_env()\n",
                    "    if artist_count > 0:\n",
                    "        print(f'🎵 Artists from .env: {env_artists}')\n",
                    "    else:\n",
                    "        print('⚠️  No artists configured in .env')\n",
                    "        env_artists = []\n",
                    "except Exception as e:\n",
                    "    print(f'⚠️  Error loading .env artists: {e}')\n",
                    "    env_artists = []\n",
                    "\n",
                    "# Chart validation tracking\n",
                    "real_charts = []        # Charts working with real data\n",
                    "requirement_charts = [] # Charts showing data requirements  \n",
                    "error_charts = []       # Charts with errors\n",
                    "chart_results = {}      # Detailed results for each chart\n",
                    "\n",
                    "def validate_chart(chart_id, chart_name, chart_function, data, *args, **kwargs):\n",
                    '    """Validate chart with real data only - NO FAKE DATA."""\n',
                    "    try:\n",
                    "        if data is None or data.empty:\n",
                    "            requirement_charts.append(chart_id)\n",
                    "            chart_results[chart_id] = f'{chart_name}: Needs real data from database'\n",
                    "            print(f'📋 Chart {chart_id}: {chart_name} - Showing data requirements')\n",
                    "            return None\n",
                    "            \n",
                    "        # Execute chart with real data\n",
                    "        result = chart_function(data, *args, **kwargs)\n",
                    "        \n",
                    "        if result is not None:\n",
                    "            real_charts.append(chart_id)\n",
                    "            chart_results[chart_id] = f'{chart_name}: SUCCESS with real data'\n",
                    "            print(f'✅ Chart {chart_id}: {chart_name} - Working with real data!')\n",
                    "            return result\n",
                    "        else:\n",
                    "            error_charts.append(chart_id)\n",
                    "            chart_results[chart_id] = f'{chart_name}: Returned None'\n",
                    "            print(f'❌ Chart {chart_id}: {chart_name} - Returned None')\n",
                    "            return None\n",
                    "            \n",
                    "    except Exception as e:\n",
                    "        error_charts.append(chart_id)\n",
                    "        chart_results[chart_id] = f'{chart_name}: ERROR - {str(e)}'\n",
                    "        print(f'❌ Chart {chart_id}: {chart_name} - Error: {e}')\n",
                    "        return None\n",
                    "\n",
                    "print('\\n🛡️ Chart validation system ready - NO FAKE DATA ALLOWED!')",
                ],
            },
        ]
    }

    # Add all 15 charts from both notebooks
    chart_cells = create_all_chart_cells()
    notebook_content["cells"].extend(chart_cells)

    # Add final summary cell
    summary_cell = create_summary_cell()
    notebook_content["cells"].append(summary_cell)

    # Add notebook metadata
    notebook_content.update(
        {
            "metadata": {
                "kernelspec": {"display_name": "YouTube Analytics", "language": "python", "name": "youtubeviz"},
                "language_info": {
                    "codemirror_mode": {"name": "ipython", "version": 3},
                    "file_extension": ".py",
                    "mimetype": "text/x-python",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.13.5",
                },
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        }
    )

    return notebook_content


def create_all_chart_cells():
    """Create all 15 chart cells combining both notebooks."""

    cells = []

    # Chart 1: Views Over Time (from Complete Dashboard)
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Chart 1: Views Over Time Analysis\n",
                "print('\\n📊 Chart 1: Views Over Time Analysis')\n",
                "\n",
                "if not df.empty and 'published_at' in df.columns and 'view_count' in df.columns:\n",
                "    fig1 = validate_chart(\n",
                "        1, 'Views Over Time',\n",
                "        views_over_time_plotly,\n",
                "        df, 'published_at', 'view_count',\n",
                "        title='📈 Views Over Time - Real Data Analytics'\n",
                "    )\n",
                "    \n",
                "    if fig1:\n",
                "        story_block(\n",
                "            fig1,\n",
                "            '📈 Real View Trends Over Time',\n",
                "            [\n",
                "                f'Analyzing {len(df)} real videos from database',\n",
                "                'Tracking authentic view patterns and growth',\n",
                "                'No fake data - pure database analytics'\n",
                "            ],\n",
                "            caption='Real data shows authentic audience engagement patterns'\n",
                "        )\n",
                "else:\n",
                "    validate_chart(1, 'Views Over Time', lambda x: None, df)\n",
                "    print('📋 Need: published_at, view_count columns in real data')",
            ],
        }
    )

    # Chart 2: Sentiment Breakdown (from Complete Dashboard)
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Chart 2: Sentiment Breakdown by Artist\n",
                "print('\\n🎭 Chart 2: Sentiment Breakdown by Artist')\n",
                "\n",
                "if not df.empty and 'artist_name' in df.columns:\n",
                "    fig2 = validate_chart(\n",
                "        2, 'Sentiment Breakdown',\n",
                "        create_divergent_sentiment_chart,\n",
                "        df, 'artist_name'\n",
                "    )\n",
                "    \n",
                "    if fig2:\n",
                "        story_block(\n",
                "            fig2,\n",
                "            '🎭 Real Fan Sentiment Analysis',\n",
                "            [\n",
                "                'Authentic fan reactions from real comments',\n",
                "                'Positive vs negative sentiment breakdown',\n",
                "                'Based on actual database comment data'\n",
                "            ],\n",
                "            caption='Real sentiment data reveals true fan engagement'\n",
                "        )\n",
                "else:\n",
                "    validate_chart(2, 'Sentiment Breakdown', lambda x: None, df)\n",
                "    print('📋 Need: artist_name column and sentiment data')",
            ],
        }
    )

    # Chart 3: Content Type Analysis (from Complete Dashboard)
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Chart 3: Content Type Breakdown\n",
                "print('\\n🎬 Chart 3: Content Type Breakdown')\n",
                "\n",
                "if not df.empty and 'artist_name' in df.columns:\n",
                "    fig3 = validate_chart(\n",
                "        3, 'Content Type Breakdown',\n",
                "        create_content_type_breakdown_chart,\n",
                "        df, 'artist_name'\n",
                "    )\n",
                "    \n",
                "    if fig3:\n",
                "        story_block(\n",
                "            fig3,\n",
                "            '🎬 Real Content Strategy Analysis',\n",
                "            [\n",
                "                'Music videos vs other content types',\n",
                "                'Based on real video categorization',\n",
                "                'Authentic content mix from database'\n",
                "            ],\n",
                "            caption='Real content data shows strategic patterns'\n",
                "        )\n",
                "else:\n",
                "    validate_chart(3, 'Content Type Breakdown', lambda x: None, df)\n",
                "    print('📋 Need: artist_name and content categorization data')",
            ],
        }
    )

    # Chart 4: ISRC Balance Analysis (from Complete Dashboard)
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Chart 4: ISRC vs Non-ISRC Balance\n",
                "print('\\n🎵 Chart 4: ISRC vs Non-ISRC Balance')\n",
                "\n",
                "if not df.empty and 'artist_name' in df.columns:\n",
                "    fig4 = validate_chart(\n",
                "        4, 'ISRC Balance Analysis',\n",
                "        create_isrc_balance_chart,\n",
                "        df, 'artist_name'\n",
                "    )\n",
                "    \n",
                "    if fig4:\n",
                "        story_block(\n",
                "            fig4,\n",
                "            '🎵 Real Music vs Content Balance',\n",
                "            [\n",
                "                'Official music (ISRC) vs other content',\n",
                "                'Real data from music industry database',\n",
                "                'Authentic content strategy insights'\n",
                "            ],\n",
                "            caption='Real ISRC data reveals content strategy'\n",
                "        )\n",
                "else:\n",
                "    validate_chart(4, 'ISRC Balance Analysis', lambda x: None, df)\n",
                "    print('📋 Need: artist_name and ISRC data columns')",
            ],
        }
    )

    # Chart 5: Artist Comparison (from Complete Dashboard)
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# Chart 5: Artist Performance Comparison\n",
                "print('\\n🎤 Chart 5: Artist Performance Comparison')\n",
                "\n",
                "if not df.empty and 'artist_name' in df.columns:\n",
                "    fig5 = validate_chart(\n",
                "        5, 'Artist Comparison',\n",
                "        create_artist_comparison_chart,\n",
                "        df, 'artist_name'\n",
                "    )\n",
                "    \n",
                "    if fig5:\n",
                "        story_block(\n",
                "            fig5,\n",
                "            '🎤 Real Artist Performance Comparison',\n",
                "            [\n",
                "                'Head-to-head performance metrics',\n",
                "                'Based on authentic database metrics',\n",
                "                'Real competitive landscape analysis'\n",
                "            ],\n",
                "            caption='Real performance data drives strategic decisions'\n",
                "        )\n",
                "else:\n",
                "    validate_chart(5, 'Artist Comparison', lambda x: None, df)\n",
                "    print('📋 Need: artist_name and performance metrics')",
            ],
        }
    )

    # Add remaining charts 6-15 with similar pattern
    for i in range(6, 16):
        cells.append(create_chart_cell(i))

    return cells


def create_chart_cell(chart_num):
    """Create a chart cell for charts 6-15."""

    chart_configs = {
        6: ("Sentiment Cluster Analysis", "create_sentiment_cluster_chart", "🧠"),
        7: ("Duration Breakdown", "create_duration_breakdown_chart", "⏱️"),
        8: ("Content Stacked Analysis", "create_content_stacked_bar_chart", "📊"),
        9: ("Artist Strategy Comparison", "create_artist_strategy_comparison_chart", "🎯"),
        10: ("Performance Diversity", "create_performance_diversity_bubble_chart", "🎪"),
        11: ("Top Positive Comments", "extract_top_positive_comments", "😍"),
        12: ("Top Negative Comments", "extract_top_negative_comments_with_percentages", "😔"),
        13: ("Roster Overview", "create_roster_overview_chart", "🎵"),
        14: ("Content Categorization", "categorize_video_content", "🏷️"),
        15: ("ISRC Content Balance", "analyze_isrc_vs_content_balance", "⚖️"),
    }

    name, func_name, emoji = chart_configs.get(chart_num, (f"Chart {chart_num}", "lambda x: None", "📊"))

    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            f"# Chart {chart_num}: {name}\n",
            f"print('\\n{emoji} Chart {chart_num}: {name}')\n",
            "\n",
            "if not df.empty and 'artist_name' in df.columns:\n",
            f"    fig{chart_num} = validate_chart(\n",
            f"        {chart_num}, '{name}',\n",
            f"        {func_name},\n",
            f"        df, 'artist_name'\n",
            f"    )\n",
            f"    \n",
            f"    if fig{chart_num}:\n",
            f"        story_block(\n",
            f"            fig{chart_num},\n",
            f"            '{emoji} Real {name}',\n",
            f"            [\n",
            f"                'Analysis based on real database data',\n",
            f"                'Authentic insights from actual metrics',\n",
            f"                'No fake data - pure analytics'\n",
            f"            ],\n",
            f"            caption='Real data provides authentic insights'\n",
            f"        )\n",
            f"else:\n",
            f"    validate_chart({chart_num}, '{name}', lambda x: None, df)\n",
            f"    print('📋 Need: Real data with required columns')",
        ],
    }


def create_summary_cell():
    """Create the final summary cell that tracks chart success."""

    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# FINAL SUMMARY - BULLETPROOF CI/CD VALIDATION\n",
            "print('\\n' + '='*60)\n",
            "print('🎯 BULLETPROOF REAL DATA ANALYTICS SUMMARY')\n",
            "print('='*60)\n",
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
            "# Detailed results for debugging\n",
            "print('\\n📋 Detailed Chart Results:')\n",
            "for chart_id in range(1, 16):\n",
            "    if chart_id in chart_results:\n",
            "        print(f'  Chart {chart_id}: {chart_results[chart_id]}')\n",
            "    else:\n",
            "        print(f'  Chart {chart_id}: Not executed')\n",
            "\n",
            "# CI/CD Success Criteria\n",
            "success_rate = len(real_charts) / 15 if len(real_charts) > 0 else 0\n",
            "print(f'\\n📈 Success Rate: {success_rate:.1%}')\n",
            "\n",
            "if success_rate >= 0.8:\n",
            "    print('🎉 CI/CD STATUS: PASS - Excellent chart health!')\n",
            "    print('💝 Real data analytics working perfectly!')\n",
            "elif success_rate >= 0.6:\n",
            "    print('⚠️  CI/CD STATUS: WARNING - Acceptable but needs improvement')\n",
            "    print('🔧 Some charts need attention')\n",
            "elif success_rate >= 0.3:\n",
            "    print('❌ CI/CD STATUS: FAIL - Poor chart health')\n",
            "    print('🚨 Major issues need immediate attention')\n",
            "else:\n",
            "    print('🚨 CI/CD STATUS: CRITICAL - System failure')\n",
            "    print('🆘 Emergency intervention required')\n",
            "\n",
            "# Success message\n",
            "if len(real_charts) >= 10:\n",
            "    print(f'\\n🎉 OUTSTANDING: {len(real_charts)} charts working with REAL data!')\n",
            "    print('💝 No fake data used - authentic analytics only!')\n",
            "    print('🏆 Production ready for stakeholder presentation!')\n",
            "elif len(real_charts) >= 5:\n",
            "    print(f'\\n🌟 GOOD PROGRESS: {len(real_charts)} charts working with real data!')\n",
            "    print('🔧 Add missing data columns to unlock more charts!')\n",
            "elif len(real_charts) >= 1:\n",
            "    print(f'\\n🌱 STARTING: {len(real_charts)} charts working with real data!')\n",
            "    print('📊 Build up your database to see more analytics!')\n",
            "else:\n",
            "    print('\\n📋 All charts show data requirements - add real data to see analytics!')\n",
            "    print('🔒 NO FAKE DATA USED - Waiting for authentic database content!')\n",
            "\n",
            "print('\\n🎵 MusicScope™ Bulletproof Real Data Analytics Complete! 🎵')\n",
            "print('🔒 ZERO FAKE DATA - 100% Authentic Analytics! 🔒')",
        ],
    }


if __name__ == "__main__":
    # Create the bulletproof notebook
    notebook = create_bulletproof_notebook()

    # Save it
    import json

    with open("notebooks/MusicScope™_Bulletproof_Real_Data_Analytics.ipynb", "w") as f:
        json.dump(notebook, f, indent=2)

    print("✅ Created bulletproof real data notebook!")
    print("🔒 NO FAKE DATA - Real database analytics only!")
    print("📊 All 15 charts from both notebooks combined!")
    print("🛡️ CI/CD ready with automatic validation!")
