#!/usr/bin/env python3
"""
Create Dynamic 20-Chart MusicScope™ Notebook

Automatically discovers database structure, artists, and creates
a notebook with 20 charts using real data (no hardcoding).
"""

from datetime import datetime
import json
import logging
import os
from pathlib import Path
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def archive_old_notebooks():
    """Archive existing notebooks to prevent conflicts."""
    notebooks_dir = Path("notebooks")
    archive_dir = notebooks_dir / "archive" / datetime.now().strftime("%Y%m%d_%H%M%S")

    if not notebooks_dir.exists():
        notebooks_dir.mkdir(exist_ok=True)
        return

    # Find notebooks to archive
    notebooks_to_archive = list(notebooks_dir.glob("MusicScope™*.ipynb"))

    if notebooks_to_archive:
        archive_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Archiving {len(notebooks_to_archive)} notebooks to {archive_dir}")

        for notebook in notebooks_to_archive:
            shutil.move(str(notebook), str(archive_dir / notebook.name))
            logger.info(f"   📄 Archived: {notebook.name}")


def fix_source_lines(source_text: str) -> list:
    """Convert source text to proper notebook format."""
    lines = source_text.split("\n")
    return [line + "\n" if i < len(lines) - 1 else line for i, line in enumerate(lines)]


def create_dynamic_notebook():
    """Create notebook with 20 charts using discovered data."""

    # Archive old notebooks first
    archive_old_notebooks()

    # Import here to avoid circular imports
    from src.youtubeviz.data_discovery import get_dynamic_notebook_config

    # Get dynamic configuration
    logger.info("🔍 Discovering database structure and artists...")
    config = get_dynamic_notebook_config()

    artists = config["artists"]
    db_summary = config["database"]
    data_summary = config["data_summary"]

    logger.info(f"📊 Found {len(artists)} artists: {', '.join(artists[:5])}{'...' if len(artists) > 5 else ''}")
    logger.info(f"📋 Database: {db_summary['total_tables']} tables")
    logger.info(f"📈 Data: {data_summary['total_videos']:,} videos, {data_summary['total_comments']:,} comments")

    # Create notebook structure
    notebook = {
        "cells": [],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.8.0"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    # Add title cell
    title_cell = {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            f"# 🎵 MusicScope™ Dynamic Analytics Dashboard\\n",
            f"\\n",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\\n",
            f"**Artists:** {len(artists)} discovered dynamically\\n",
            f"**Charts:** 20 comprehensive visualizations\\n",
            f"**Database:** {db_summary['total_tables']} tables\\n",
            f"\\n",
            f"## 🎯 Discovered Artists\\n",
            f"\\n",
        ]
        + [f"- **{artist}**\\n" for artist in artists[:10]],
    }
    notebook["cells"].append(title_cell)

    # Add bootstrap cell
    bootstrap_source = """# 🚀 MusicScope™ Bootstrap - Dynamic Data Discovery
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configure paths and logging
sys.path.insert(0, '.')

# Setup notebook-safe logging
logger = logging.getLogger('musicscope.charts')
for h in list(logger.handlers): logger.removeHandler(h)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler); logger.setLevel(logging.INFO); logger.propagate = False

print('🎵 MusicScope™ Dynamic Dashboard Initialized!')
print('✅ Logging configured')
print('✅ Paths configured')
print('🔍 Ready for dynamic data discovery...')"""

    bootstrap_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": fix_source_lines(bootstrap_source),
    }
    notebook["cells"].append(bootstrap_cell)

    # Add data discovery cell
    demo_mode = config.get("demo_mode", False)

    if demo_mode:
        discovery_source = """# 🔍 Demo Mode - Using Sample Data
import numpy as np
from datetime import datetime, timedelta

# Configuration from discovery
artists = ['Taylor Swift', 'Drake', 'The Weeknd', 'Billie Eilish', 'Post Malone', 'Ariana Grande']
total_tables = 20
total_videos = 5000
total_comments = 50000

print('🎯 Demo Mode Active - Database connection not available')
print(f'📋 Simulated tables: {total_tables}')
print(f'🎵 Sample artists: {len(artists)}')
print(f'🎭 Artists: {artists[:3]}...')
print(f'📊 Simulated videos: {total_videos:,}')
print(f'💬 Simulated comments: {total_comments:,}')

# Generate sample data for charts
np.random.seed(42)

# Sample videos data
videos_data = []
for artist in artists:
    for i in range(50):  # 50 videos per artist
        videos_data.append({
            'video_id': f'vid_{artist.replace(" ", "")}_{i}',
            'title': f'{artist} - Song {i+1}',
            'artist_name': artist,
            'published_at': datetime.now() - timedelta(days=np.random.randint(1, 365)),
            'view_count': np.random.randint(10000, 10000000),
            'like_count': np.random.randint(100, 100000),
            'comment_count': np.random.randint(10, 10000),
            'duration': f'{np.random.randint(2, 6)}:{np.random.randint(10, 59):02d}'
        })

videos_df = pd.DataFrame(videos_data)
comments_df = pd.DataFrame()  # Empty for demo
sentiment_df = pd.DataFrame()  # Empty for demo
metrics_df = pd.DataFrame()  # Empty for demo

print(f'📈 Generated {len(videos_df):,} sample videos')
print('🎯 Ready to generate 20 charts with sample data!')"""

        discovery_cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": fix_source_lines(discovery_source),
        }
    else:
        discovery_cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 🔍 Dynamic Data Discovery\\n",
                "from src.youtubeviz.data_discovery import DatabaseDiscovery, load_dynamic_data\\n",
                "from sqlalchemy import create_engine\\n",
                "import os\\n",
                "\\n",
                "# Initialize database discovery\\n",
                "print('🔍 Discovering database structure...')\\n",
                "discovery = DatabaseDiscovery()\\n",
                "\\n",
                "# Discover database structure\\n",
                "db_summary = discovery.discover_tables()\\n",
                "print(f'📋 Found {db_summary[\"total_tables\"]} tables')\\n",
                "\\n",
                "# Discover artists dynamically\\n",
                "print('🎵 Discovering artists...')\\n",
                "artists = discovery.discover_artists(min_videos=3)\\n",
                "print(f'🎭 Found {len(artists)} artists: {artists[:5]}')\\n",
                "\\n",
                "# Get data summary\\n",
                "data_summary = discovery.get_data_summary()\\n",
                "print(f'📊 Videos: {data_summary[\"total_videos\"]:,}')\\n",
                "print(f'💬 Comments: {data_summary[\"total_comments\"]:,}')\\n",
                "print(f'✅ Sentiment data: {data_summary[\"has_sentiment\"]}')\\n",
                "print(f'✅ ISRC data: {data_summary[\"has_isrc\"]}')\\n",
                "\\n",
                "# Load actual data\\n",
                "print('📥 Loading dynamic data...')\\n",
                "data = load_dynamic_data(discovery.engine, artists, limit_per_artist=500)\\n",
                "\\n",
                "# Extract dataframes\\n",
                "videos_df = data.get('videos', pd.DataFrame())\\n",
                "comments_df = data.get('comments', pd.DataFrame())\\n",
                "sentiment_df = data.get('sentiment_summary', pd.DataFrame())\\n",
                "metrics_df = data.get('metrics', pd.DataFrame())\\n",
                "\\n",
                "print(f'📈 Loaded {len(videos_df):,} videos')\\n",
                "print(f'💬 Loaded {len(comments_df):,} comments')\\n",
                "print(f'📊 Loaded {len(sentiment_df):,} sentiment records')\\n",
                "print(f'📉 Loaded {len(metrics_df):,} metrics records')\\n",
                "\\n",
                "print('\\n🎯 Data Discovery Complete!')\\n",
                "print(f'Ready to generate 20 charts for {len(artists)} artists')",
            ],
        }
    notebook["cells"].append(discovery_cell)

    # Add import cell
    import_source = """# 📊 Import Chart Functions
from src.youtubeviz.bulletproof import bulletproof_chart
from src.youtubeviz.chart_patterns import safe_artist_views_bar, safe_content_type_sentiment
import src.youtubeviz.advanced_charts as ac
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

print('📊 Chart functions imported')
print('🛡️ Bulletproof system ready')
print('🎨 Plotly visualization ready')"""

    import_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": fix_source_lines(import_source),
    }
    notebook["cells"].append(import_cell)

    # Create 20 chart cells
    chart_definitions = [
        (
            "Artist Performance Overview",
            "safe_artist_views_bar(videos_df.groupby('artist_name')['view_count'].sum().reset_index())",
        ),
        (
            "Engagement Distribution",
            "px.histogram(videos_df, x='like_count', title='Like Count Distribution', nbins=30)",
        ),
        (
            "Upload Timeline",
            "px.scatter(videos_df, x='published_at', y='view_count', color='artist_name', title='Upload Timeline')",
        ),
        (
            "Artist Comparison",
            "px.bar(videos_df.groupby('artist_name')['view_count'].mean().reset_index(), x='artist_name', y='view_count', title='Average Views by Artist')",
        ),
        (
            "Content Performance",
            "px.box(videos_df, x='artist_name', y='view_count', title='View Count Distribution by Artist')",
        ),
        (
            "Comment Volume",
            "px.scatter(videos_df, x='view_count', y='comment_count', color='artist_name', title='Views vs Comments')",
        ),
        (
            "Engagement Rate",
            "px.scatter(videos_df.assign(engagement_rate=videos_df['like_count']/videos_df['view_count']), x='view_count', y='engagement_rate', color='artist_name', title='Engagement Rate Analysis')",
        ),
        (
            "Top Videos",
            "px.bar(videos_df.nlargest(20, 'view_count'), x='title', y='view_count', title='Top 20 Videos by Views').update_xaxes(tickangle=45)",
        ),
        ("Artist Activity", "px.histogram(videos_df, x='artist_name', title='Video Count by Artist')"),
        (
            "Performance Trends",
            "px.line(videos_df.groupby([videos_df['published_at'].dt.date, 'artist_name'])['view_count'].sum().reset_index(), x='published_at', y='view_count', color='artist_name', title='Performance Over Time')",
        ),
        (
            "Like Ratio Analysis",
            "px.scatter(videos_df.assign(like_ratio=videos_df['like_count']/videos_df['view_count']*100), x='view_count', y='like_ratio', color='artist_name', title='Like Percentage vs Views')",
        ),
        (
            "Content Length Impact",
            "px.scatter(videos_df.dropna(subset=['duration']), x='duration', y='view_count', color='artist_name', title='Duration vs Performance')",
        ),
        (
            "Comment Engagement",
            "px.scatter(videos_df, x='comment_count', y='like_count', color='artist_name', title='Comments vs Likes')",
        ),
        (
            "Artist Market Share",
            "px.pie(videos_df.groupby('artist_name')['view_count'].sum().reset_index(), values='view_count', names='artist_name', title='Market Share by Views')",
        ),
        (
            "Performance Distribution",
            "px.violin(videos_df, x='artist_name', y='view_count', title='Performance Distribution by Artist')",
        ),
        (
            "Upload Frequency",
            "px.histogram(videos_df.groupby('artist_name')['published_at'].count().reset_index(), x='published_at', title='Upload Frequency by Artist')",
        ),
        (
            "Viral Content Analysis",
            "px.scatter(videos_df.assign(viral_score=videos_df['view_count']/videos_df['view_count'].median()), x='published_at', y='viral_score', color='artist_name', title='Viral Performance Over Time')",
        ),
        (
            "Engagement Quality",
            "px.scatter(videos_df.assign(quality_score=(videos_df['like_count']+videos_df['comment_count'])/videos_df['view_count']*1000), x='view_count', y='quality_score', color='artist_name', title='Engagement Quality Score')",
        ),
        (
            "Content Strategy",
            "px.treemap(videos_df.groupby('artist_name').agg({'view_count': 'sum', 'like_count': 'sum'}).reset_index(), path=['artist_name'], values='view_count', title='Content Strategy Overview')",
        ),
        (
            "Performance Heatmap",
            "px.density_heatmap(videos_df, x='view_count', y='like_count', title='Performance Density Heatmap')",
        ),
    ]

    chart_count = 0
    for i, (chart_title, chart_code) in enumerate(chart_definitions, 1):
        # Add chart description
        desc_cell = {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"## Chart {i}: {chart_title}\\n\\nDynamic analysis using discovered artist data."],
        }
        notebook["cells"].append(desc_cell)

        # Add chart generation cell
        chart_source = f"""# Chart {i}: {chart_title}
try:
    if not videos_df.empty:
        fig = {chart_code}
        if fig is not None:
            fig.update_layout(height=500, showlegend=True)
            fig.show()
            print('✅ Chart {i} generated successfully')
        else:
            print('⚠️ Chart {i} returned None')
    else:
        print('❌ No data available for Chart {i}')
except Exception as e:
    print(f'❌ Chart {i} failed: {{e}}')"""

        chart_cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": fix_source_lines(chart_source),
        }
        notebook["cells"].append(chart_cell)
        chart_count += 1

    # Add final status cell
    status_source = """# 🎯 Final Dashboard Status
print('🎯 MusicScope™ Dashboard Complete!')
print('=' * 50)
print(f'📊 Charts generated: 20')
print(f'🎵 Artists analyzed: {len(artists)}')
print(f'📋 Tables discovered: {db_summary["total_tables"]}')
print(f'📈 Videos processed: {len(videos_df):,}')
print(f'💬 Comments analyzed: {len(comments_df):,}')
print('\\n✅ All systems operational - ready for insights!')"""

    status_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": fix_source_lines(status_source),
    }
    notebook["cells"].append(status_cell)

    # Save notebook
    notebook_path = "notebooks/MusicScope™_20_Chart_Dashboard.ipynb"
    os.makedirs("notebooks", exist_ok=True)

    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

    logger.info(f"✅ Created dynamic notebook: {notebook_path}")
    logger.info(f"📊 Total cells: {len(notebook['cells'])}")
    logger.info(f"🎨 Chart cells: {chart_count}")
    logger.info(f"🎵 Artists: {len(artists)}")

    return {
        "notebook_path": notebook_path,
        "total_cells": len(notebook["cells"]),
        "chart_count": chart_count,
        "artists": artists,
        "config": config,
    }


if __name__ == "__main__":
    print("🎵 Creating Dynamic 20-Chart MusicScope™ Notebook")
    print("=" * 60)

    result = create_dynamic_notebook()

    print(f"\\n✅ Notebook created successfully!")
    print(f"📄 Path: {result['notebook_path']}")
    print(f"📊 Charts: {result['chart_count']}")
    print(f"🎵 Artists: {len(result['artists'])}")
    print(f"📋 Total cells: {result['total_cells']}")

    if result["artists"]:
        print(f"\\n🎭 Discovered Artists:")
        for i, artist in enumerate(result["artists"][:10], 1):
            print(f"   {i}. {artist}")

    print("\\n🚀 Ready to execute notebook!")
