#!/usr/bin/env python3
"""
Demo script showing the storytelling notebook system in action.

This demonstrates how to use the new storytelling framework to create
engaging, educational notebooks for music industry analytics.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from src.youtubeviz.education import EducationalContentGenerator
from src.youtubeviz.notebook_templates import NotebookConfig, StorytellingNotebook

# Import our storytelling modules
from src.youtubeviz.storytelling import (
    chart_context,
    educational_sidebar,
    narrative_intro,
    section_transition,
)


def demo_storytelling_functions():
    """Demonstrate the storytelling functions."""
    print("🎵 STORYTELLING FRAMEWORK DEMO")
    print("=" * 50)

    # Demo 1: Narrative Introduction
    print("\n📖 1. NARRATIVE INTRODUCTION")
    print("-" * 30)
    intro = narrative_intro("artist_comparison", context={"artists": ["Taylor Swift", "Drake", "The Weeknd"]})
    print(intro[:200] + "...")

    # Demo 2: Educational Sidebar
    print("\n📚 2. EDUCATIONAL CONTENT")
    print("-" * 30)
    education = educational_sidebar("engagement_rate", "beginner")
    print(education[:200] + "...")

    # Demo 3: Section Transition
    print("\n🔄 3. SECTION TRANSITIONS")
    print("-" * 30)
    transition = section_transition(
        "overview", "comparison", key_insight="Engagement rates vary significantly across artists"
    )
    print(transition[:150] + "...")

    # Demo 4: Chart Context
    print("\n📊 4. CHART READING GUIDE")
    print("-" * 30)
    context = chart_context(
        "bar_chart",
        ["Compare heights", "Look for patterns", "Identify outliers"],
        ["Invest in top performers", "Analyze success factors"],
    )
    print(context[:200] + "...")


def demo_educational_generator():
    """Demonstrate the educational content generator."""
    print("\n\n🎓 EDUCATIONAL CONTENT GENERATOR")
    print("=" * 50)

    educator = EducationalContentGenerator("intermediate")

    # Demo different concepts
    concepts = ["youtube_metrics", "music_industry_economics", "data_science_concepts"]

    for concept in concepts:
        print(f"\n📖 {concept.replace('_', ' ').title()}")
        print("-" * 30)
        explanation = educator.explain_concept(concept, include_business_context=False)
        print(explanation[:150] + "...")


def demo_notebook_template():
    """Demonstrate the notebook template system."""
    print("\n\n📓 NOTEBOOK TEMPLATE SYSTEM")
    print("=" * 50)

    # Create configuration
    config = NotebookConfig(
        title="Demo Artist Analysis",
        artists=["Artist A", "Artist B", "Artist C"],
        complexity_level="beginner",
        narrative_style="engaging",
    )

    print(f"✅ Created config: {config.title}")
    print(f"   Artists: {', '.join(config.artists)}")
    print(f"   Style: {config.narrative_style}")
    print(f"   Complexity: {config.complexity_level}")

    # Create notebook template
    notebook = StorytellingNotebook.create_artist_comparison_template(config)

    print(f"\n📋 Generated notebook with {len(notebook.sections)} sections:")
    for section in notebook.sections:
        print(f"   • {section['title']} ({section['type']})")

    # Add some content
    notebook.current_section = "introduction"
    notebook.add_markdown_cell("Welcome to our storytelling demo!")

    print(f"\n✨ Added content to {notebook.current_section} section")


def demo_complete_workflow():
    """Demonstrate a complete storytelling workflow."""
    print("\n\n🚀 COMPLETE STORYTELLING WORKFLOW")
    print("=" * 50)

    # Step 1: Create sample data
    print("📊 1. Creating sample data...")
    np.random.seed(42)
    artists = ["Demo Artist A", "Demo Artist B"]
    dates = pd.date_range(start=datetime.now() - timedelta(days=7), periods=7)

    data = []
    for artist in artists:
        for date in dates:
            data.append(
                {
                    "date": date,
                    "artist": artist,
                    "views": np.random.randint(10000, 50000),
                    "engagement_rate": np.random.uniform(2.0, 6.0),
                }
            )

    df = pd.DataFrame(data)
    print(f"   Generated {len(df)} data points")

    # Step 2: Generate narrative content
    print("\n📝 2. Generating narrative content...")
    intro = narrative_intro("artist_comparison", {"artists": artists})
    print(f"   Introduction: {len(intro)} characters")

    # Step 3: Create educational content
    print("\n🎓 3. Creating educational content...")
    educator = EducationalContentGenerator()
    education = educator.explain_concept("engagement_rate")
    print(f"   Educational content: {len(education)} characters")

    # Step 4: Generate insights
    print("\n💡 4. Generating insights...")
    top_artist = df.groupby("artist")["views"].sum().idxmax()
    avg_engagement = df.groupby("artist")["engagement_rate"].mean()

    insights = [
        f"{top_artist} leads in total views",
        f"Average engagement: {avg_engagement.mean():.1f}%",
        "Weekend patterns visible in data",
    ]

    print(f"   Generated {len(insights)} key insights")

    print("\n✅ Complete storytelling workflow demonstrated!")
    print("   Ready to create engaging, educational notebooks!")


if __name__ == "__main__":
    demo_storytelling_functions()
    demo_educational_generator()
    demo_notebook_template()
    demo_complete_workflow()

    print("\n\n🎉 DEMO COMPLETE!")
    print("=" * 50)
    print("The storytelling framework is ready to transform your")
    print("music industry analytics into engaging, educational narratives!")
    print("\nNext steps:")
    print("• Run the new notebook: notebooks/executed/02_artist_comparison-executed.ipynb")
    print("• Customize the storytelling functions for your specific needs")
    print("• Create new notebook templates for different analysis types")
    print("• Integrate with your real YouTube analytics data")
