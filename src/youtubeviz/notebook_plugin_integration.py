"""Notebook generation with plugin system integration."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


from .plugin_integration import get_plugin_manager


class NotebookPluginIntegrationError(Exception):
    """Raised when notebook plugin integration fails."""

    pass


class PluginEnhancedNotebookGenerator:
    """Notebook generator with plugin system integration."""

    def __init__(self, enable_plugins: bool = True):
        """Initialize plugin-enhanced notebook generator."""
        self._logger = logging.getLogger(__name__)
        self._enable_plugins = enable_plugins
        self._plugin_manager = None

        if self._enable_plugins:
            try:
                self._plugin_manager = get_plugin_manager(enable_storage=True)
                if not self._plugin_manager._initialized:
                    self._plugin_manager.initialize()
                self._logger.info("Plugin system initialized for notebook generation")
            except Exception as e:
                self._logger.warning(f"Failed to initialize plugin system: {e}")
                self._enable_plugins = False

    def create_plugin_enhanced_notebook(
        self,
        title: str = "MusicScope™ Plugin-Enhanced Analytics",
        artists: Optional[List[str]] = None,
        algorithms: Optional[List[str]] = None,
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create notebook with plugin-generated content."""
        try:
            # Initialize notebook structure
            notebook = self._create_base_notebook_structure(title)

            # Add plugin system overview
            notebook["cells"].extend(self._create_plugin_overview_cells())

            # Add plugin-based analysis cells
            if self._enable_plugins and self._plugin_manager:
                plugin_cells = self._create_plugin_analysis_cells(artists, algorithms)
                notebook["cells"].extend(plugin_cells)
            else:
                notebook["cells"].extend(self._create_fallback_analysis_cells())

            # Add plugin comparison and insights
            notebook["cells"].extend(self._create_plugin_insights_cells())

            # Save notebook
            output_file = self._save_notebook(notebook, output_path)

            return {
                "success": True,
                "notebook_path": output_file,
                "cells_created": len(notebook["cells"]),
                "plugins_used": self._get_used_algorithms() if self._enable_plugins else [],
                "title": title,
            }

        except Exception as e:
            self._logger.error(f"Plugin-enhanced notebook creation failed: {e}")
            raise NotebookPluginIntegrationError(f"Notebook creation failed: {e}")

    def _create_base_notebook_structure(self, title: str) -> Dict[str, Any]:
        """Create base notebook structure."""
        return {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [
                        f"# {title}\n",
                        "\n",
                        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
                        "\n",
                        "This notebook demonstrates the integration of the plugin system with YouTube analytics.\n",
                        "It showcases multiple scoring algorithms and their insights on music industry data.\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "source": [
                        "# Import required libraries\n",
                        "import pandas as pd\n",
                        "import plotly.express as px\n",
                        "import plotly.graph_objects as go\n",
                        "from plotly.subplots import make_subplots\n",
                        "\n",
                        "# Import youtubeviz package with plugin support\n",
                        "import sys\n",
                        "sys.path.append('.')\n",
                        "import youtubeviz\n",
                        "from src.youtubeviz.plugin_integration import (\n",
                        "    get_plugin_manager,\n",
                        "    initialize_plugins,\n",
                        "    get_available_algorithms,\n",
                        "    execute_scoring\n",
                        ")\n",
                        "\n",
                        "# Configure display options\n",
                        "pd.set_option('display.max_columns', None)\n",
                        "pd.set_option('display.width', None)\n",
                        "\n",
                        "print('📊 MusicScope™ Plugin System Loaded Successfully!')\n",
                    ],
                },
            ],
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "language_info": {"name": "python", "version": "3.8.0"},
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        }

    def _create_plugin_overview_cells(self) -> List[Dict[str, Any]]:
        """Create cells that overview the plugin system."""
        cells = [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 🔌 Plugin System Overview\n",
                    "\n",
                    "The MusicScope™ platform integrates a powerful plugin system that allows for:\n",
                    "\n",
                    "- **Multiple Scoring Algorithms**: Different approaches to analyzing artist performance\n",
                    "- **Extensible Architecture**: Easy addition of new analysis methods\n",
                    "- **Comparative Analysis**: Side-by-side comparison of different scoring approaches\n",
                    "- **Real-time Processing**: Dynamic scoring with live data\n",
                    "\n",
                    "Let's explore what plugins are available and how they work.\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Initialize plugin system\n",
                    "print('🚀 Initializing Plugin System...')\n",
                    "status = initialize_plugins(auto_discover=True, enable_storage=True)\n",
                    "\n",
                    "print(f'✅ Plugin System Status:')\n",
                    "for key, value in status.items():\n",
                    "    print(f'   {key}: {value}')\n",
                    "\n",
                    "# Get available algorithms\n",
                    "algorithms = get_available_algorithms()\n",
                    "print(f'\\n🎯 Available Algorithms: {len(algorithms)}')\n",
                    "for i, alg in enumerate(algorithms, 1):\n",
                    "    print(f'   {i}. {alg}')\n",
                ],
            },
        ]

        return cells

    def _create_plugin_analysis_cells(
        self, artists: Optional[List[str]] = None, algorithms: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Create cells with plugin-based analysis."""
        cells = []

        # Data preparation cell
        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 📊 Data Preparation for Plugin Analysis\n",
                    "\n",
                    "Let's prepare some sample data to demonstrate the plugin system capabilities.\n",
                    "In a real scenario, this would come from your YouTube analytics database.\n",
                ],
            }
        )

        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Create sample artist data for demonstration\n",
                    "# In production, this would come from your database\n",
                    "\n",
                    "sample_artists = [\n",
                    "    'Taylor Swift', 'Drake', 'Bad Bunny', 'The Weeknd', 'Ariana Grande',\n",
                    "    'Ed Sheeran', 'Post Malone', 'Billie Eilish', 'Dua Lipa', 'Justin Bieber'\n",
                    "]\n",
                    "\n",
                    "# Generate realistic sample data\n",
                    "import numpy as np\n",
                    "np.random.seed(42)  # For reproducible results\n",
                    "\n",
                    "artist_data = pd.DataFrame({\n",
                    "    'entity_id': sample_artists,\n",
                    "    'video_count': np.random.randint(5, 50, len(sample_artists)),\n",
                    "    'avg_views': np.random.randint(1000000, 100000000, len(sample_artists)),\n",
                    "    'avg_likes': np.random.randint(10000, 1000000, len(sample_artists)),\n",
                    "    'avg_comments': np.random.randint(1000, 100000, len(sample_artists)),\n",
                    "    'avg_sentiment': np.random.uniform(-0.5, 0.8, len(sample_artists)),\n",
                    "    'total_views': np.random.randint(10000000, 1000000000, len(sample_artists)),\n",
                    "    'total_likes': np.random.randint(100000, 10000000, len(sample_artists)),\n",
                    "    'total_comments': np.random.randint(10000, 1000000, len(sample_artists)),\n",
                    "    'recent_growth_rate': np.random.uniform(-10, 50, len(sample_artists))\n",
                    "})\n",
                    "\n",
                    "print('🎵 Sample Artist Data Created:')\n",
                    "print(artist_data.head())\n",
                    "print(f'\\n📈 Data Shape: {artist_data.shape}')\n",
                ],
            }
        )

        # Plugin execution cells
        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 🎯 Plugin-Based Scoring Analysis\n",
                    "\n",
                    "Now let's run our available algorithms on the artist data and compare their results.\n",
                ],
            }
        )

        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Execute scoring with all available algorithms\n",
                    "scoring_results = {}\n",
                    "algorithm_info = {}\n",
                    "\n",
                    "for algorithm in algorithms:\n",
                    "    try:\n",
                    "        print(f'🔄 Running {algorithm}...')\n",
                    "        \n",
                    "        # Execute scoring\n",
                    "        scores = execute_scoring(\n",
                    "            algorithm_name=algorithm,\n",
                    "            data=artist_data,\n",
                    "            entity_type='artist'\n",
                    "        )\n",
                    "        \n",
                    "        scoring_results[algorithm] = scores\n",
                    "        \n",
                    "        # Get algorithm info\n",
                    "        from src.youtubeviz.plugin_integration import get_algorithm_info\n",
                    "        info = get_algorithm_info(algorithm)\n",
                    "        algorithm_info[algorithm] = info\n",
                    "        \n",
                    "        print(f'   ✅ Generated {len(scores)} scores')\n",
                    "        \n",
                    "    except Exception as e:\n",
                    "        print(f'   ❌ Failed: {e}')\n",
                    "        continue\n",
                    "\n",
                    "print(f'\\n🎉 Successfully executed {len(scoring_results)} algorithms!')\n",
                ],
            }
        )

        # Results visualization cells
        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 📈 Plugin Results Visualization\n",
                    "\n",
                    "Let's visualize and compare the results from different scoring algorithms.\n",
                ],
            }
        )

        cells.append(
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Create comparison visualization\n",
                    "if scoring_results:\n",
                    "    # Combine all results for comparison\n",
                    "    comparison_data = []\n",
                    "    \n",
                    "    for algorithm, scores_df in scoring_results.items():\n",
                    "        for _, row in scores_df.iterrows():\n",
                    "            comparison_data.append({\n",
                    "                'artist': row.get('entity_id', 'Unknown'),\n",
                    "                'algorithm': algorithm,\n",
                    "                'score': row.get('score_value', 0),\n",
                    "                'confidence': row.get('confidence', 0)\n",
                    "            })\n",
                    "    \n",
                    "    comparison_df = pd.DataFrame(comparison_data)\n",
                    "    \n",
                    "    # Create interactive comparison chart\n",
                    "    fig = px.bar(\n",
                    "        comparison_df,\n",
                    "        x='artist',\n",
                    "        y='score',\n",
                    "        color='algorithm',\n",
                    "        title='🎵 Artist Scores by Algorithm',\n",
                    "        labels={'score': 'Score Value', 'artist': 'Artist'},\n",
                    "        height=600\n",
                    "    )\n",
                    "    \n",
                    "    fig.update_layout(\n",
                    "        xaxis_tickangle=-45,\n",
                    "        showlegend=True,\n",
                    "        template='plotly_white'\n",
                    "    )\n",
                    "    \n",
                    "    fig.show()\n",
                    "    \n",
                    "    print('📊 Algorithm Comparison Chart Created!')\n",
                    "else:\n",
                    "    print('⚠️  No scoring results available for visualization')\n",
                ],
            }
        )

        return cells

    def _create_fallback_analysis_cells(self) -> List[Dict[str, Any]]:
        """Create fallback analysis cells when plugins are not available."""
        return [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## ⚠️ Plugin System Not Available\n",
                    "\n",
                    "The plugin system is not currently available. This could be due to:\n",
                    "\n",
                    "- Missing dependencies\n",
                    "- Configuration issues\n",
                    "- Database connectivity problems\n",
                    "\n",
                    "Please check the system configuration and try again.\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Fallback analysis without plugins\n",
                    "print('🔧 Plugin system not available-using fallback analysis')\n",
                    "\n",
                    "# Create basic sample data\n",
                    "sample_data = pd.DataFrame({\n",
                    "    'artist': ['Artist A', 'Artist B', 'Artist C'],\n",
                    "    'views': [1000000, 2000000, 1500000],\n",
                    "    'likes': [50000, 100000, 75000]\n",
                    "})\n",
                    "\n",
                    "print('Sample data created:')\n",
                    "print(sample_data)\n",
                ],
            },
        ]

    def _create_plugin_insights_cells(self) -> List[Dict[str, Any]]:
        """Create cells with plugin insights and recommendations."""
        return [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 🧠 Plugin System Insights\n",
                    "\n",
                    "The plugin system provides several key advantages for music industry analytics:\n",
                    "\n",
                    "### 🎯 Algorithm Diversity\n",
                    "Different algorithms capture different aspects of artist performance:\n",
                    "- **Momentum Scoring**: Focuses on growth trends and trajectory\n",
                    "- **Engagement Scoring**: Emphasizes audience interaction quality\n",
                    "- **Custom Algorithms**: Tailored to specific business needs\n",
                    "\n",
                    "### 📊 Comparative Analysis\n",
                    "By running multiple algorithms, we can:\n",
                    "- Identify consensus picks (artists scored highly by multiple algorithms)\n",
                    "- Spot algorithmic disagreements that warrant further investigation\n",
                    "- Build ensemble models that combine multiple approaches\n",
                    "\n",
                    "### 🔄 Extensibility\n",
                    "The plugin architecture allows for:\n",
                    "- Easy addition of new scoring methods\n",
                    "- A / B testing of different approaches\n",
                    "- Integration with external data sources\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Generate insights summary\n",
                    "if 'comparison_df' in locals() and not comparison_df.empty:\n",
                    "    print('🎯 Plugin System Analysis Summary:')\n",
                    "    print(f'   📊 Total Algorithms Used: {comparison_df[\"algorithm\"].nunique()}')\n",
                    "    print(f'   🎵 Artists Analyzed: {comparison_df[\"artist\"].nunique()}')\n",
                    "    print(f'   📈 Total Scores Generated: {len(comparison_df)}')\n",
                    "    \n",
                    "    # Find consensus picks\n",
                    "    avg_scores = comparison_df.groupby('artist')['score'].mean().sort_values(ascending=False)\n",
                    "    print(f'\\n🏆 Top Artists (Average Score):')\n",
                    "    for i, (artist, score) in enumerate(avg_scores.head(3).items(), 1):\n",
                    "        print(f'   {i}. {artist}: {score:.3f}')\n",
                    "    \n",
                    "    # Algorithm agreement analysis\n",
                    "    score_std = comparison_df.groupby('artist')['score'].std().sort_values()\n",
                    "    print(f'\\n🤝 Most Consistent Scores (Low Std Dev):')\n",
                    "    for i, (artist, std) in enumerate(score_std.head(3).items(), 1):\n",
                    "        print(f'   {i}. {artist}: {std:.3f} std dev')\n",
                    "        \n",
                    "else:\n",
                    "    print('📋 Plugin system analysis completed-check individual algorithm results above')\n",
                    "\n",
                    "print('\\n✨ Plugin-Enhanced Analysis Complete!')\n",
                ],
            },
        ]

    def _get_used_algorithms(self) -> List[str]:
        """Get list of algorithms that were used."""
        if self._plugin_manager:
            return self._plugin_manager.get_available_algorithms()
        return []

    def _save_notebook(self, notebook: Dict[str, Any], output_path: Optional[str] = None) -> str:
        """Save notebook to file."""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
            output_path = f"notebooks / MusicScope™_Plugin_Enhanced_{timestamp}.ipynb"

        # Ensure directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Save notebook
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(notebook, f, indent=2, ensure_ascii=False)

        self._logger.info(f"Notebook saved to: {output_path}")
        return output_path

    def create_plugin_comparison_notebook(
        self, algorithms: Optional[List[str]] = None, output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a notebook specifically for comparing plugin algorithms."""
        try:
            # Get available algorithms
            available_algorithms = []
            if self._enable_plugins and self._plugin_manager:
                available_algorithms = self._plugin_manager.get_available_algorithms()

            if algorithms:
                # Filter to requested algorithms
                available_algorithms = [alg for alg in algorithms if alg in available_algorithms]

            # Create comparison-focused notebook
            notebook = self._create_base_notebook_structure("MusicScope™ Plugin Algorithm Comparison")

            # Add algorithm comparison cells
            comparison_cells = self._create_algorithm_comparison_cells(available_algorithms)
            notebook["cells"].extend(comparison_cells)

            # Save notebook
            if output_path is None:
                timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
                output_path = f"notebooks / MusicScope™_Algorithm_Comparison_{timestamp}.ipynb"

            output_file = self._save_notebook(notebook, output_path)

            return {
                "success": True,
                "notebook_path": output_file,
                "algorithms_compared": available_algorithms,
                "cells_created": len(notebook["cells"]),
            }

        except Exception as e:
            self._logger.error(f"Plugin comparison notebook creation failed: {e}")
            raise NotebookPluginIntegrationError(f"Comparison notebook creation failed: {e}")

    def _create_algorithm_comparison_cells(self, algorithms: List[str]) -> List[Dict[str, Any]]:
        """Create cells for detailed algorithm comparison."""
        cells = [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 🔍 Detailed Algorithm Comparison\n",
                    "\n",
                    "This notebook provides an in-depth comparison of available scoring algorithms.\n",
                    f"We'll analyze {len(algorithms)} algorithms and their characteristics.\n",
                ],
            }
        ]

        # Add cells for each algorithm
        for algorithm in algorithms:
            cells.extend(
                [
                    {
                        "cell_type": "markdown",
                        "metadata": {},
                        "source": [
                            f"### 🎯 Algorithm: {algorithm}\n",
                            "\n",
                            f"Detailed analysis of the {algorithm} scoring algorithm.\n",
                        ],
                    },
                    {
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "source": [
                            f"# Analyze {algorithm} algorithm\n",
                            f"try:\n",
                            f"    from src.youtubeviz.plugin_integration import get_algorithm_info\n",
                            f"    info = get_algorithm_info('{algorithm}')\n",
                            f"    print(f'📊 Algorithm: {algorithm}')\n",
                            f'    print(f\'   Version: {{info.get("version", "Unknown")}}\')\n',
                            f'    print(f\'   Author: {{info.get("author", "Unknown")}}\')\n',
                            f'    print(f\'   Description: {{info.get("description", "No description")}}\')\n',
                            f"    \n",
                            f"    # Show parameters\n",
                            f"    params = info.get('parameters', {{}})\n",
                            f"    if params:\n",
                            f"        print(f'   Parameters:')\n",
                            f"        for key, value in params.items():\n",
                            f"            print(f'     - {{key}}: {{value}}')\n",
                            f"except Exception as e:\n",
                            f"    print(f'❌ Error getting info for {algorithm}: {{e}}')\n",
                        ],
                    },
                ]
            )

        return cells


# Convenience functions
def create_plugin_enhanced_notebook(
    title: str = "MusicScope™ Plugin-Enhanced Analytics",
    artists: Optional[List[str]] = None,
    algorithms: Optional[List[str]] = None,
    output_path: Optional[str] = None,
    enable_plugins: bool = True,
) -> Dict[str, Any]:
    """Create a plugin-enhanced notebook."""
    generator = PluginEnhancedNotebookGenerator(enable_plugins=enable_plugins)
    return generator.create_plugin_enhanced_notebook(
        title=title, artists=artists, algorithms=algorithms, output_path=output_path
    )


def create_algorithm_comparison_notebook(
    algorithms: Optional[List[str]] = None, output_path: Optional[str] = None, enable_plugins: bool = True
) -> Dict[str, Any]:
    """Create an algorithm comparison notebook."""
    generator = PluginEnhancedNotebookGenerator(enable_plugins=enable_plugins)
    return generator.create_plugin_comparison_notebook(algorithms=algorithms, output_path=output_path)
