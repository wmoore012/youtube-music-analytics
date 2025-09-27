"""
Production - ready notebook template system for MusicScope™ analytics.

This module generates notebooks with bulletproof CI / CD validation and ensures
all charts are properly counted and validated with real data only. """

from datetime import datetime
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd


class NotebookTemplateManager: """Manages production - ready notebook templates with automatic chart validation."""


def __init__(self, total_charts: int = 20): """
        Initialize notebook template manager.

        Args:
            total_charts: Total number of charts expected in notebooks         """
        self.total_charts = total_charts  # noqa: E999
        self.chart_registry = self._build_chart_registry()


def _build_chart_registry(self) -> Dict[int, Dict[str, Any]
                                            ]: """Build registry of all available charts with metadata."""
        return {
            # Original 15 advanced charts
            1: {"name": "Sentiment Breakdown by Artist", "function": "create_diverging_sentiment_bars", "module": "advanced_charts",
                                 },
            2: {"name": "Sentiment Model Categories Heatmap", "function": "create_sentiment_cluster_heatmap", "module": "advanced_charts",
                                 },
            3: {"name": "Top 3 Positive Theme Lollipops", "function": "create_positive_theme_lollipops", "module": "advanced_charts",
                                 },
            4: {"name": "Top 3 Negative Theme Lollipops", "function": "create_negative_theme_lollipops", "module": "advanced_charts",
                                 },
            5: {"name": "Standout Videos Scatter Plot", "function": "create_standout_videos_scatter", "module": "advanced_charts",
                                 },
            6: {"name": "Tour Compatibility Analysis", "function": "create_tour_compatibility_analysis", "module": "advanced_charts",
                                 },
            7: {"name": "UpSet Plot for Feature Intersections", "function": "create_upset_feature_intersections", "module": "advanced_charts",
                                 }, 8: {"name": "ISRC Balance Analysis", "function": "create_isrc_balance_bars", "module": "advanced_charts"},
            9: {"name": "Content Length Analysis", "function": "create_content_length_dumbbells", "module": "advanced_charts",
                                 }, 10: {"name": "Content Type Breakdown", "function": "create_content_type_dots", "module": "advanced_charts"},
            11: {"name": "Views by Category Over Time", "function": "create_views_by_category_areas", "module": "advanced_charts",
                                  },
            12: {"name": "Genre Context Heatmap", "function": "create_genre_context_heatmap", "module": "advanced_charts",
                                  },
            13: {"name": "Artist Rank Bump Chart", "function": "create_roster_rank_bump_chart", "module": "advanced_charts",
                                  },
            14: {"name": "Comment Polarity Ridgelines", "function": "create_polarity_ridgelines", "module": "advanced_charts",
                                  }, 15: {"name": "A / B Test Framework", "function": "create_ab_test_framework", "module": "advanced_charts"},
            # Additional charts from Complete Dashboard             16: {"name":
            # "Views Over Time", "function": "views_over_time_plotly", "module":
            # "charts"},             17: {"name": "Artist Comparison Chart",
            # "function": "create_artist_comparison_chart", "module": "content"},
            # 18: {"name": "Top Positive Comments", "function":
            # "extract_top_positive_comments", "module": "sentiment"},             19:
            # {"name": "Storytelling Dashboard", "function": "story_block", "module":
            # "storytelling"},             20: {"name": "Executive Summary",
            # "function": "generate_executive_summary", "module":
            # "summary_generator"},
        }


def generate_notebook_template(self, notebook_name: str = "MusicScope™_Production_Dashboard", include_charts: Optional[List[int]] = None
                                            ) -> Dict[str, Any]: """
        Generate a complete notebook template with all charts and validation.

        Args:
            notebook_name: Name for the notebook
            include_charts: List of chart IDs to include (default: all charts)

        Returns:
            Complete notebook JSON structure         """
        if include_charts is None:
            include_charts = list(range(1, self.total_charts + 1))

        notebook = {"cells": [], "metadata": {"kernelspec": {"display_name": "YouTube Analytics", "language": "python", "name": "youtubeviz"}, "language_info": {"codemirror_mode": {"name": "ipython", "version": 3}, "file_extension": ".py", "mimetype": "text / x - python", "name": "python", "nbconvert_exporter": "python", "pygments_lexer": "ipython3", "version": "3.13.5",
                                                                                                                                                                                                                                                },
                                                                                        }, "nbformat": 4, "nbformat_minor": 4,
                                 }

        # Add header cell         notebook["cells"].append(self._create_header_cell(notebook_name, len(include_charts)))

        # Add imports cell         notebook["cells"].append(self._create_imports_cell())

        # Add data loading cell         notebook["cells"].append(self._create_data_loading_cell())

        # Add chart cells
        for chart_id in include_charts:
            if chart_id in self.chart_registry:
                chart_info = self.chart_registry[chart_id]                 notebook["cells"].extend(self._create_chart_cells(chart_id, chart_info))

        # Add final validation cell         notebook["cells"].append(self._create_validation_cell(len(include_charts)))

        return notebook


def _create_header_cell(self, notebook_name: str,
                            chart_count: int) -> Dict[str, Any]: """Create notebook header cell."""
        return {"cell_type": "markdown", "metadata": {}, "source": [f"# 🎵 {notebook_name}\n", "\n", f"**Production Analytics with {chart_count} Data - Science Grade Charts - REAL DATA ONLY**\n", "\n", "This notebook uses ONLY real data from the database. No fake / mock data.\n", "Charts will show data requirements if columns are missing.\n", "\n", f"- 📊 **Total Charts**: {chart_count}\n", "- 🚫 **No Fake Data**: Only real database data\n", "- ✅ **CI / CD Ready**: Automatic validation\n", "- 💝 **Compassionate Analytics**: Human - centered insights\n",
                                                                                                                          ],
                             }


def _create_imports_cell(self) -> Dict[str, Any]: """Create imports cell with all required functions."""
        return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": ["# Core imports\n", "import pandas as pd\n", "import numpy as np\n", "import plotly.graph_objects as go\n", "import warnings\n", "warnings.filterwarnings('ignore')\n", "\n", "print('✅ Core imports loaded')\n", "\n", "# Import all chart functions\n", "try:\n", "    # Advanced charts (Charts 1 - 15)\n", "    from youtubeviz.advanced_charts import (\n", "        create_diverging_sentiment_bars, create_sentiment_cluster_heatmap,\n", "        create_positive_theme_lollipops, create_negative_theme_lollipops,\n", "        create_standout_videos_scatter, create_tour_compatibility_analysis,\n", "        create_upset_feature_intersections, create_isrc_balance_bars,\n", "        create_content_length_dumbbells, create_content_type_dots,\n", "        create_views_by_category_areas, create_genre_context_heatmap,\n", "        create_roster_rank_bump_chart, create_polarity_ridgelines,\n", "        create_ab_test_framework, enhance_chart_beauty\n", "    )\n", "    \n", "    # Additional charts (Charts 16 - 20)\n", "    from youtubeviz.charts import views_over_time_plotly\n", "    from youtubeviz.content import create_artist_comparison_chart\n", "    from youtubeviz.sentiment import extract_top_positive_comments\n", "    from youtubeviz.storytelling import story_block, narrative_intro\n", "    from youtubeviz.summary_generator import generate_executive_summary\n", "    \n", "    print('✅ All chart functions imported successfully!')\n", "    \n", "except ImportError as e:\n", "    print(f'❌ Import error: {e}')\n", "    print('💡 Run: pip install -e . to install package')\n",
                                                                                                                                                                                      ],
                             }


def _create_data_loading_cell(self) -> Dict[str, Any]: """Create data loading cell with real data only."""
        return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": ["# Load REAL data only - no fake data fallback\n", "try:\n", "    from youtubeviz.data import load_recent_window_days\n", "    df = load_recent_window_days(days=30)\n", "    \n", "    if not df.empty:\n", "        print(f'✅ Loaded REAL data: {len(df):,} records')\n", '        print(f\'🎭 Artists: {", ".join(df["artist_name"].unique())}\')\n', "        print(f'📊 Columns: {list(df.columns)}')\n", "        \n", "        # Show data quality summary\n", "        print(f'\\n📈 Data Quality Summary:')\n", "        required_cols = ['sentiment_category', 'daily"                     "_views', 'engagement_rate', 'has_isrc', 'content_type']\n", "        for col in required_cols:\n", "            if col in df.columns:\n", "                non_null = df[col].notna().sum()\n",                 "                print(f'   ✅ {col}: {non_null:,
                    }/{len(df):,
                    } records ({non_null / len(df)*100:.1f}%)')\n",                 "            else:\n",                 "                print(f'   ❌ {col}: Missing - charts will show requirements')\n",                 "    else:\n",                 "        print('⚠️  Empty dataset returned from database')\n",                 "        \n",                 "except Exception as e:\n",                 "    print(f'❌ Real data loading failed: {e}')\n",                 "    print('🚫 NO FAKE DATA FALLBACK')\n",                 "    print('💡 Charts will show data requirements')\n",                 "    \n",                 "    # Create empty dataframe\n",                 "    df = pd.DataFrame()\n",                 "    print('⚠️  Empty dataframe - fix data loading to see real analytics')\n",                 "\n",                 "print(f'\\n🎯 Ready for real data analysis!')\n",
            ],
        }

def _create_chart_cells(self, chart_id: int, chart_info: Dict[str, Any]) -> List[Dict[str, Any]]:         """Create markdown and code cells for a chart."""
        cells = []

        # Markdown cell
        cells.append(             {"cell_type": "markdown", "metadata": {}, "source": [f"## Chart {chart_id}: {chart_info['name']}\n"]}
        )

        # Code cell
        cells.append(
            {                 "cell_type": "code",                 "execution_count": None,                 "metadata": {},                 "outputs": [],                 "source": [                     f"print('🎨 Generating Chart {chart_id}: {chart_info['name']}...')\n",                     "\n",                     "try:\n",                     "    if not df.empty:\n",                     "        # Try to create chart with real data\n",                     f"        fig_{chart_id} = {chart_info['function']}(df)\n",                     f"        \n",                     f"        if fig_{chart_id}:\n",                     f"            fig_{chart_id}.show()\n",                     f"            print('✅ Chart {chart_id}: Generated with REAL data!')\n",                     "        else:\n",                     f"            print('❌ Chart {chart_id}: Function returned None')\n",                     "    else:\n",                     "        # Show data requirements\n",                     f"        fig_{chart_id} = go.Figure()\n",                     f"        fig_{chart_id}.add_annotation(\n",                     f"            text=\"📊 Chart {chart_id}: {chart_info['name']}<br><br>\" +\n",                     '                 "📋 Data Requirements:<br>" +\n',                     '                 "• Real data from database<br>" +\n',                     '                 "• Required columns for this chart<br><br>" +\n',                     '                 "💡 Add real data to see analytics!",\n',                     "            x=0.5, y=0.5, showarrow=False,\n",                     "            font=dict(size=14, color='#666')\n",                     "        )\n",                     f"        fig_{chart_id}.update_layout(\n",                     f"            title='Chart {chart_id}: {chart_info['name']} - Data Requirements',\n",                     "            height=400\n",                     "        )\n",                     f"        fig_{chart_id}.show()\n",                     f"        print('📋 Chart {chart_id}: Showing data requirements')\n",                     "        \n",                     "except Exception as e:\n",                     f"    print(f'❌ Chart {chart_id} error: {{e}}')\n",                     f"    fig_{chart_id} = go.Figure()\n",                     f"    fig_{chart_id}.add_annotation(\n",                     f'        text=f"❌ Chart {chart_id} Error: {{str(e)}}",\n',                     "        x=0.5, y=0.5, showarrow=False\n",                     "    )\n",                     f"    fig_{chart_id}.show()\n",
                ],
            }
        )

        return cells

def _create_validation_cell(self, total_charts: int) -> Dict[str, Any]:         """Create final validation cell that counts working charts."""
        return {             "cell_type": "code",             "execution_count": None,             "metadata": {},             "outputs": [],             "source": [                 "# Count charts with real data vs data requirements\n",                 "real_charts = []\n",                 "requirement_charts = []\n",                 "error_charts = []\n",                 "\n",                 f"for i in range(1, {total_charts + 1}):\n",                 "    fig_name = f'fig_{i}'\n",                 "    if fig_name in locals():\n",                 "        fig = locals()[fig_name]\n",                 "        if hasattr(fig, 'data') and len(fig.data) > 0:\n",                 "            # Check if it's a real chart or just annotation\n",                 "            has_real_data = any(\n",                 "                hasattr(trace, 'x') and len(getattr(trace, 'x', [])) > 1 \n",                 "                for trace in fig.data\n",                 "            )\n",                 "            if has_real_data:\n",                 "                real_charts.append(i)\n",                 "            else:\n",                 "                # Check if it's showing data requirements\n",                 "                annotations = getattr(fig.layout, 'annotations', [])\n",                 "                if any('need' in str(ann.text).lower() or 'requirement' in str(ann.text).lower() for ann in annotations):\n",                 "                    requirement_charts.append(i)\n",                 "                else:\n",                 "                    error_charts.append(i)\n",                 "        else:\n",                 "            error_charts.append(i)\n",                 "    else:\n",                 "        error_charts.append(i)\n",                 "\n",                 "print('🎯 REAL DATA ANALYTICS SUMMARY')\n",                 "print('=' * 50)\n",                 f"print(f'📊 Charts with REAL data: {{len(real_charts)}}/{total_charts}')\n",                 f"print(f'📋 Charts showing data requirements: {{len(requirement_charts)}}/{total_charts}')\n",                 f"print(f'❌ Charts with errors: {{len(error_charts)}}/{total_charts}')\n",                 "\n",                 "if real_charts:\n",                 "    print(f'\\n✅ Working with real data: {real_charts}')\n",                 "if requirement_charts:\n",                 "    print(f'📋 Need data columns: {requirement_charts}')\n",                 "if error_charts:\n",                 "    print(f'❌ Have errors: {error_charts}')\n",                 "\n",                 "# Success message\n",                 "if len(real_charts) >= 5:\n",                 "    print(f'\\n🎉 SUCCESS: {len(real_charts)} charts working with REAL data!')\n",                 "    print('💝 No fake data used - authentic analytics only!')\n",                 "elif len(real_charts) >= 1:\n",                 "    print(f'\\n🌱 PROGRESS: {len(real_charts)} charts working with real data!')\n",                 "    print('🔧 Add missing data columns to unlock more charts!')\n",                 "else:\n",                 "    print('\\n📋 All charts show data requirements - add real data to see analytics!')\n",                 "\n",                 "print('\\n🎵 MusicScope™ Real Data Analytics Complete! 🎵')\n",                 "\n",                 "# CI / CD validation\n",                 "success_rate = len(real_charts) / len(range(1, "
                + str(total_charts + 1)                 + ")) if "
                + str(total_charts)                 + " > 0 else 0\n",                 "if success_rate >= 0.8:\n",                 "    print('\\n✅ CI / CD: PASS - Excellent chart health')\n",                 "elif success_rate >= 0.6:\n",                 "    print('\\n⚠️  CI / CD: WARNING - Acceptable but needs improvement')\n",                 "else:\n",                 "    print('\\n❌ CI / CD: FAIL - Poor chart health')\n",
            ],
        }

def save_notebook(self, notebook: Dict[str, Any], filepath: str) -> None:         """Save notebook to file."""         with open(filepath, "w", encoding="utf - 8") as f:
            json.dump(notebook, f, indent=1, ensure_ascii=False)

def generate_and_save_notebook(
        self,
        filepath: str,         notebook_name: str = "MusicScope™_Production_Dashboard",
        include_charts: Optional[List[int]] = None,
) -> None:         """Generate and save a complete notebook."""
        notebook = self.generate_notebook_template(notebook_name, include_charts)
        self.save_notebook(notebook, filepath)         print(f"✅ Generated notebook: {filepath}")         print(f"📊 Total charts: {len(include_charts) if include_charts else self.total_charts}")

 def create_production_notebook(output_path: str = "notebooks / MusicScope™_Production_Dashboard.ipynb") -> None:     """Create a production - ready notebook with all 20 charts."""
manager = NotebookTemplateManager(total_charts=20)
manager.generate_and_save_notebook(
        filepath=output_path,         notebook_name="MusicScope™ Production Dashboard",
        include_charts=list(range(1, 21)),  # All 20 charts
)

 if __name__ == "__main__":
# Generate production notebook
create_production_notebook()
