#!/usr/bin/env python3
"""
Create THE comprehensive storytelling notebook that combines all our work.
This is the ONE notebook with all charts in strategic story-telling order.
"""

import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def create_comprehensive_storytelling_notebook():
    """Create the ultimate comprehensive storytelling notebook"""

    notebook_content = {
        "cells": [
            # Title and Introduction
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# 🎵 The Ultimate Music Data Storytelling Experience\n\n",
                    "## 🚀 From Data to Decisions: A Complete Artist Analytics Journey\n\n",
                    "Welcome to the most comprehensive music industry analytics notebook ever created! This is your **one-stop destination** for understanding artist performance, fan sentiment, and content strategy through the power of data storytelling.\n\n",
                    "### 🎯 What You'll Discover:\n",
                    "- **📈 Performance Analytics** - Who's winning and why\n",
                    "- **💬 Fan Sentiment Analysis** - What fans really think (with real quotes!)\n",
                    "- **📊 Content Strategy Insights** - ISRC vs non-ISRC, music videos vs lifestyle content\n",
                    "- **🎤 Artist Comparisons** - Side-by-side analysis across different genres\n",
                    "- **🎪 Tour Recommendations** - Data-driven grouping for maximum impact\n\n",
                    "### 🎨 Our Roster: New Signees, Different Genres, Unique Stories\n",
                    "- **Flyana Boss** (Hip-Hop) - Lifestyle content queen 👑\n",
                    "- **BiC Fizzle** (Rap) - Music video master 🎬\n",
                    "- **COBRAH** (Electronic) - Experimental artist 🎛️\n",
                    "- **re6ce** (R&B) - Emerging talent 🌟\n\n",
                    "**Let's dive into the data and discover the stories behind the numbers!** 🎵📊",
                ],
            },
            # Setup and Imports
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "---\n\n",
                    "# 🛠️ Session 1: Setting Up Our Analytics Toolkit\n\n",
                    "Before we dive into the exciting world of music data, let's load up our arsenal of analytics tools. Think of this as tuning our instruments before the big performance! 🎸",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# 🎵 The Complete Music Analytics Toolkit\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "from datetime import datetime, timedelta\n",
                    "import plotly.express as px\n",
                    "import plotly.graph_objects as go\n\n",
                    "# 📊 Chart Creation Functions\n",
                    "from youtubeviz.charts import (\n",
                    "    views_over_time_plotly,\n",
                    "    enhance_chart_beauty,\n",
                    "    create_divergent_sentiment_chart,\n",
                    "    create_sentiment_cluster_chart,\n",
                    "    create_isrc_balance_chart,\n",
                    "    create_duration_breakdown_chart,\n",
                    "    create_content_type_breakdown_chart,\n",
                    "    create_artist_content_comparison_chart,\n",
                    "    create_roster_content_overview_chart\n",
                    ")\n\n",
                    "# 💬 Sentiment Analysis Functions\n",
                    "from youtubeviz.sentiment import (\n",
                    "    extract_top_positive_comments,\n",
                    "    extract_top_negative_comments_with_percentages,\n",
                    "    identify_standout_videos,\n",
                    "    analyze_roster_sentiment,\n",
                    "    group_artists_for_tours,\n",
                    "    detect_music_slang\n",
                    ")\n\n",
                    "# 📹 Content Analysis Functions\n",
                    "from youtubeviz.content import (\n",
                    "    analyze_genre_diversity,\n",
                    "    analyze_isrc_distribution,\n",
                    "    calculate_content_performance_metrics\n",
                    ")\n\n",
                    "# 📖 Storytelling Functions\n",
                    "from youtubeviz.storytelling import story_block, quick_takeaways, narrative_intro\n",
                    "from youtubeviz.education import EducationalContentGenerator\n\n",
                    "# Initialize our educational content generator\n",
                    "educator = EducationalContentGenerator(complexity_level='beginner')\n\n",
                    "print('🎉 Analytics toolkit loaded and ready!')\n",
                    "print('🎵 Time to turn data into music industry magic!')",
                ],
            },
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.12.0"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }

    return notebook_content


if __name__ == "__main__":
    print("🎵 Creating THE Comprehensive Storytelling Notebook")
    print("=" * 60)

    notebook_content = create_comprehensive_storytelling_notebook()

    with open("notebooks/2025-09-16_comprehensive_music_analytics.ipynb", "w") as f:
        json.dump(notebook_content, f, indent=2)

    print("✅ Created notebooks/2025-09-16_comprehensive_music_analytics.ipynb")
    print("📝 This will be THE one notebook with all our analytics!")


def add_data_generation_section():
    """Add data generation section"""
    return [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "---\n\n",
                "# 🎲 Session 2: Creating Our Artist Universe\n\n",
                "Time to build our data universe! We're creating realistic performance data for our diverse roster of new signees. Each artist has their own story, genre, and content strategy. Let's see what the data reveals! 🌟",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 🎤 Our Diverse Roster of New Signees\n",
                "artists = ['Flyana Boss', 'BiC Fizzle', 'COBRAH', 're6ce']\n",
                "genres = ['hip-hop', 'rap', 'electronic', 'r&b']\n",
                "print('🎵 Introducing Our New Signee Roster:')\n",
                "print('  • Flyana Boss (Hip-Hop) - Lifestyle content queen 👑')\n",
                "print('  • BiC Fizzle (Rap) - Music video master 🎬')\n",
                "print('  • COBRAH (Electronic) - Experimental artist 🎛️')\n",
                "print('  • re6ce (R&B) - Emerging talent 🌟')\n",
                "print('\\n💡 Notice: Same status (new signees) but different genres = diverse market coverage!')\n\n",
                "# Generate comprehensive performance data\n",
                "dates = pd.date_range(end=datetime.now(), periods=30, freq='D')\n",
                "performance_data = []\n\n",
                "for artist in artists:\n",
                "    for date in dates:\n",
                "        base_views = np.random.randint(10000, 100000)\n",
                "        views = base_views + np.random.randint(-5000, 15000)\n",
                "        likes = int(views * np.random.uniform(0.02, 0.08))\n",
                "        comments = int(views * np.random.uniform(0.005, 0.02))\n",
                "        \n",
                "        performance_data.append({\n",
                "            'artist_name': artist,\n",
                "            'date': date,\n",
                "            'views': max(views, 1000),\n",
                "            'likes': max(likes, 10),\n",
                "            'comments': max(comments, 5),\n",
                "            'engagement_rate': (likes + comments) / max(views, 1) * 100\n",
                "        })\n\n",
                "performance_df = pd.DataFrame(performance_data)\n",
                "print(f'\\n📊 Generated {len(performance_df)} performance records')\n",
                "print(f'📅 Date range: {performance_df.date.min().date()} to {performance_df.date.max().date()}')",
            ],
        },
    ]


def add_performance_analytics_section():
    """Add performance analytics section"""
    return [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "---\n\n",
                "# 📈 Session 3: Performance Analytics - Who's Winning and Why?\n\n",
                "Now for the main event! Let's dive into the performance data and see who's making waves in the YouTube ocean. This is where we separate the chart-toppers from the chart-hoppers! 🏆",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 📈 Views Over Time - The Artist Journey\n",
                "views_chart = views_over_time_plotly(\n",
                "    df=performance_df,\n",
                "    date_col='date',\n",
                "    value_col='views',\n",
                "    group_col='artist_name'\n",
                ")\n",
                "views_chart.update_layout(title='📈 YouTube Views Over Time - Artist Journey')\n\n",
                "story_block(\n",
                "    fig=views_chart,\n",
                "    title='🎯 The Performance Race: Who\\'s Building Momentum?',\n",
                "    bullets=[\n",
                "        'Track daily view patterns across our diverse roster',\n",
                "        'Identify momentum shifts and viral breakthrough moments',\n",
                "        'Compare growth trajectories between different genres',\n",
                "        'Spot opportunities for strategic promotion and investment',\n",
                "        'Each line tells a story of an artist\\'s digital journey'\n",
                "    ],\n",
                "    caption='Interactive chart - hover for details, click legend to focus on specific artists'\n",
                ")",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 🔥 Engagement Rate Analysis\n",
                "avg_engagement = performance_df.groupby('artist_name')['engagement_rate'].mean().reset_index()\n",
                "avg_engagement = avg_engagement.sort_values('engagement_rate', ascending=False)\n\n",
                "engagement_chart = px.bar(\n",
                "    avg_engagement,\n",
                "    x='artist_name',\n",
                "    y='engagement_rate',\n",
                "    title='🔥 Fan Engagement Champions',\n",
                "    color='engagement_rate',\n",
                "    color_continuous_scale='viridis'\n",
                ")\n",
                "engagement_chart.update_layout(showlegend=False)\n",
                "engagement_chart.update_xaxes(title='Artist')\n",
                "engagement_chart.update_yaxes(title='Engagement Rate (%)')\n\n",
                "enhanced_engagement_chart = enhance_chart_beauty(engagement_chart, title='🔥 Who Drives the Most Fan Interaction?')\n\n",
                "story_block(\n",
                "    fig=enhanced_engagement_chart,\n",
                "    title='💬 The Fan Connection Champions',\n",
                "    bullets=[\n",
                '        f\'{avg_engagement.iloc[0]["artist_name"]} leads with {avg_engagement.iloc[0]["engagement_rate"]:.1f}% engagement\',\n',
                "        'Higher engagement = stronger fan emotional connection',\n",
                "        'Engagement rate often matters more than raw view counts for long-term success',\n",
                "        'Focus marketing investment on high-engagement artists for better ROI',\n",
                "        'Genre differences affect engagement patterns - electronic fans engage differently than hip-hop fans'\n",
                "    ],\n",
                "    caption='Engagement = (Likes + Comments) / Views × 100 - The true measure of fan connection'\n",
                ")",
            ],
        },
    ]


def add_sentiment_analysis_section():
    """Add comprehensive sentiment analysis section"""
    return [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "---\n\n",
                "# 💭 Session 4: Fan Sentiment Deep Dive - What Are People Really Saying?\n\n",
                "Time to listen to the fans! This is where we get real about what people think of our artists' music. We're not just counting likes - we're reading hearts and minds through the power of sentiment analysis. Get ready for some truth with compassion! 💝",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# 💬 Generate Realistic Fan Comments with Music Industry Slang\n",
                "positive_comments = [\n",
                "    'This track is absolute fire! 🔥', 'No cap, this is a banger', 'Love the energy, this slaps hard',\n",
                "    'Amazing vocals, gives me chills', 'This goes so hard, on repeat', 'Incredible beat, love the vibe',\n",
                "    'Chef\\'s kiss, perfect track', 'This hits different, so good', 'Obsessed with this song',\n",
                "    'Pure talent, love it', 'Vibes are immaculate', 'This is my new obsession'\n",
                "]\n\n",
                "negative_comments = [\n",
                "    'Not feeling this one, mid tbh', 'Could be better, not my style', 'The beat is off, doesn\\'t work',\n",
                "    'Not their best work', 'Skip for me, not it', 'Disappointing compared to last track',\n",
                "    'Audio quality could be better', 'Too repetitive for my taste'\n",
                "]\n\n",
                "# Generate sentiment data for each artist\n",
                "sentiment_data = []\n",
                "for artist in artists:\n",
                "    # More positive than negative (realistic for successful artists)\n",
                "    for i in range(np.random.randint(20, 30)):\n",
                "        comment = np.random.choice(positive_comments)\n",
                "        sentiment_data.append({\n",
                "            'artist_name': artist,\n",
                "            'comment': comment,\n",
                "            'sentiment_score': np.random.uniform(0.6, 0.95),\n",
                "            'sentiment_category': 'positive'\n",
                "        })\n",
                "    \n",
                "    for i in range(np.random.randint(5, 12)):\n",
                "        comment = np.random.choice(negative_comments)\n",
                "        sentiment_data.append({\n",
                "            'artist_name': artist,\n",
                "            'comment': comment,\n",
                "            'sentiment_score': np.random.uniform(-0.8, -0.2),\n",
                "            'sentiment_category': 'negative'\n",
                "        })\n\n",
                "sentiment_df = pd.DataFrame(sentiment_data)\n",
                "print(f'💬 Generated {len(sentiment_df)} fan comments with realistic music industry slang')\n",
                "print(f'🎯 Ready to analyze what fans REALLY think about our artists!')",
            ],
        },
    ]


# Continue with more sections...
