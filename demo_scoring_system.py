#!/usr/bin/env python3
"""
Demonstration of the plugin-based scoring system architecture.

This script shows how to:
1. Create and register scoring plugins
2. Execute scoring algorithms with different parameters
3. Handle plugin validation and error cases
4. Export and work with scoring results
"""

import pandas as pd

from src.data_organization.example_plugins import EngagementScoringPlugin, MomentumScoringPlugin, SimpleTestPlugin
from src.data_organization.scoring_engine import ScoringEngine


def main():
    """Demonstrate the scoring system capabilities."""
    print("🚀 Plugin-Based Scoring System Demo")
    print("=" * 50)

    # Initialize the scoring engine
    engine = ScoringEngine()

    # Register example plugins
    print("\n📦 Registering plugins...")
    plugins = [MomentumScoringPlugin(), EngagementScoringPlugin(), SimpleTestPlugin()]

    for plugin in plugins:
        engine.register_plugin(plugin)
        print(f"  ✅ Registered: {plugin.get_name()} v{plugin.get_version()}")

    # Show available algorithms
    print(f"\n🔍 Available algorithms: {engine.get_available_algorithms()}")

    # Demonstrate momentum scoring
    print("\n📊 Testing Momentum Scoring...")
    momentum_data = pd.DataFrame(
        {
            "artist_name": ["Rising Star", "Established Artist", "Viral Hit"],
            "video_count": [15, 45, 8],
            "total_views": [500000, 2500000, 1200000],
            "total_likes": [25000, 125000, 80000],
            "total_comments": [5000, 15000, 12000],
            "avg_views_per_video": [33333, 55555, 150000],
            "recent_growth_rate": [85.5, 15.2, 200.8],
        }
    )

    momentum_result = engine.execute_scoring("momentum_scorer", momentum_data)
    print("Momentum Scoring Results:")
    print(momentum_result.entity_scores[["entity_id", "score_value", "momentum_category", "confidence"]])

    # Demonstrate engagement scoring
    print("\n💬 Testing Engagement Scoring...")
    engagement_data = pd.DataFrame(
        {
            "entity_id": ["video_1", "video_2", "video_3"],
            "total_views": [100000, 50000, 200000],
            "total_likes": [8000, 2500, 15000],
            "total_comments": [1200, 300, 2500],
            "subscriber_count": [50000, 25000, 100000],
        }
    )

    engagement_result = engine.execute_scoring("engagement_scorer", engagement_data)
    print("Engagement Scoring Results:")
    print(engagement_result.entity_scores[["entity_id", "score_value", "confidence", "like_rate", "comment_rate"]])

    # Demonstrate custom parameters
    print("\n⚙️  Testing Custom Parameters...")
    custom_params = {"growth_weight": 0.8, "engagement_weight": 0.2, "min_videos_required": 5}

    custom_result = engine.execute_scoring("momentum_scorer", momentum_data, custom_params)
    print("Custom Parameters Results:")
    print(custom_result.entity_scores[["entity_id", "score_value", "momentum_category"]])
    print(f"Parameters used: {custom_result.metadata['parameters']}")

    # Demonstrate plugin metadata
    print("\n📋 Plugin Metadata Example...")
    metadata = engine.get_plugin_metadata("momentum_scorer")
    print(f"Plugin: {metadata['name']}")
    print(f"Version: {metadata['version']}")
    print(f"Parameters: {list(metadata['parameters'].keys())}")
    print(f"Input Requirements: {metadata['input_requirements']}")

    # Demonstrate system status
    print("\n🔧 System Status...")
    status = engine.get_system_status()
    print(f"Loaded plugins: {status['loaded_plugins']}")
    print(f"Isolation enabled: {status['isolation_enabled']}")
    print(f"Max execution time: {status['max_execution_time']}s")

    # Demonstrate result export
    print("\n💾 Exporting Results...")
    db_records = momentum_result.to_database_records()
    print(f"Generated {len(db_records)} database records")
    print("Sample record keys:", list(db_records[0].keys()))

    # Export to CSV
    momentum_result.export_to_csv("momentum_scores.csv")
    print("✅ Results exported to momentum_scores.csv")

    # Demonstrate validation
    print("\n✅ Plugin Validation...")
    validation_results = engine.validate_all_plugins()
    for plugin_name, result in validation_results.items():
        status = "✅ Valid" if result.is_valid else "❌ Invalid"
        print(f"  {plugin_name}: {status}")
        if result.warnings:
            for warning in result.warnings:
                print(f"    ⚠️  {warning}")

    print("\n🎉 Demo completed successfully!")
    print("\nKey Features Demonstrated:")
    print("  • Plugin registration and discovery")
    print("  • Multiple scoring algorithms")
    print("  • Custom parameter configuration")
    print("  • Input validation and error handling")
    print("  • Result export and database conversion")
    print("  • Plugin isolation and system monitoring")


if __name__ == "__main__":
    main()
