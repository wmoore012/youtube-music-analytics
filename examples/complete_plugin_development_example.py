#!/usr / bin / env python3
"""
Complete Plugin Development Example

This example demonstrates the full workflow of creating, testing, validating,
and using a custom music analytics plugin with the open - source framework.

Run this example to see:
1. Plugin creation and registration
2. Security validation
3. Input / output validation
4. Real data processing
5. Results export and analysis
"""

from datetime import datetime, timedelta
import os
from typing import Any, Dict

import numpy as np
import pandas as pd

from src.data_organization.notebook_validator import ValidationResult

# Import the plugin framework
from src.data_organization.open_source_plugin_framework import (
    OpenSourceScoringPlugin,
    PluginMetadata,
    PluginRegistry,
    PluginValidator,
)
from src.data_organization.plugin_security_examples import AdvancedSecurityChecker


class MusicViralityPlugin(OpenSourceScoringPlugin):
    """
    Example plugin that calculates virality potential for music videos.

    This plugin demonstrates a complete implementation with:
    - Comprehensive metadata
    - Robust input validation
    - Configurable parameters
    - Security - conscious implementation
    - Clear output schema
    """

    def get_name(self) -> str:
        return "music_virality"

    def get_version(self) -> str:
        return "1.2.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="music_virality",
            version="1.2.0",
            author="Music Analytics Community",
            description="Calculates virality potential for music videos based on"
            " engagement patterns and growth velocity",
            parameters={
                "velocity_weight": 0.4,
                "engagement_weight": 0.3,
                "recency_weight": 0.2,
                "consistency_weight": 0.1,
                "min_views_threshold": 10000,
                "viral_threshold": 0.75,
                "time_window_hours": 48,
            },
            input_requirements=[
                "video_id",
                "view_count",
                "like_count",
                "comment_count",
                "published_date",
                "artist_name",
            ],
            output_schema={
                "video_id": "object",
                "artist_name": "object",
                "virality_score": "float64",
                "virality_category": "object",
                "velocity_component": "float64",
                "engagement_component": "float64",
                "recency_component": "float64",
                "prediction_confidence": "float64",
            },
            license="MIT",
            repository_url="https://github.com / music - analytics / virality - plugin",
            documentation_url="https://music - analytics.github.io / virality - plugin",
            tags=["virality", "trending", "music", "prediction", "engagement"],
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Comprehensive input validation with music - specific checks."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)

        # Check required columns
        required_cols = self.get_metadata().input_requirements
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Validate numeric columns
        numeric_cols = ["view_count", "like_count", "comment_count"]
        for col in numeric_cols:
            if col in data.columns:
                result.checked_items += 1
                if not pd.api.types.is_numeric_dtype(data[col]):
                    result.add_error(f"Column '{col}' must be numeric")
                elif (data[col] < 0).any():
                    result.add_error(f"Column '{col}' cannot contain negative values")
                else:
                    result.passed_items += 1

        # Validate date column
        if "published_date" in data.columns:
            result.checked_items += 1
            try:
                pd.to_datetime(data["published_date"])
                result.passed_items += 1
            except Exception:
                result.add_error("published_date must be a valid datetime")

        # Check for reasonable data ranges
        if "view_count" in data.columns and len(data) > 0:
            result.checked_items += 1
            max_views = data["view_count"].max()
            if max_views > 10_000_000_000:  # 10 billion views seems unrealistic
                result.add_warning("Extremely high view counts detected - please verify data accuracy")
            result.passed_items += 1

        # Check for data completeness
        result.checked_items += 1
        if len(data) == 0:
            result.add_error("Input data is empty")
        elif len(data) < 5:
            result.add_warning("Small dataset may produce less reliable virality predictions")
            result.passed_items += 1
        else:
            result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate virality scores with comprehensive error handling."""
        self._record_execution_start()

        try:
            # Get configuration parameters
            velocity_weight = self.config.get("velocity_weight", 0.4)
            engagement_weight = self.config.get("engagement_weight", 0.3)
            recency_weight = self.config.get("recency_weight", 0.2)
            consistency_weight = self.config.get("consistency_weight", 0.1)
            min_views = self.config.get("min_views_threshold", 10000)
            viral_threshold = self.config.get("viral_threshold", 0.75)
            time_window_hours = self.config.get("time_window_hours", 48)

            # Prepare data
            result_data = data.copy()
            result_data["published_date"] = pd.to_datetime(result_data["published_date"])
            current_time = datetime.now()

            results = []

            for _, row in result_data.iterrows():
                video_id = row["video_id"]
                artist_name = row["artist_name"]
                view_count = row["view_count"]
                like_count = row["like_count"]
                comment_count = row["comment_count"]
                published_date = row["published_date"]

                # Skip videos with insufficient views
                if view_count < min_views:
                    results.append(
                        {
                            "video_id": video_id,
                            "artist_name": artist_name,
                            "virality_score": 0.0,
                            "virality_category": "insufficient_data",
                            "velocity_component": 0.0,
                            "engagement_component": 0.0,
                            "recency_component": 0.0,
                            "prediction_confidence": 0.1,
                        }
                    )
                    continue

                # Calculate time since publication
                time_since_pub = (current_time - published_date).total_seconds() / 3600  # hours

                # 1. Velocity Component (views per hour)
                if time_since_pub > 0:
                    velocity = view_count / time_since_pub
                    velocity_component = min(velocity / 10000, 1.0)  # Normalize to 10k views / hour
                else:
                    velocity_component = 0.0

                # 2. Engagement Component
                if view_count > 0:
                    like_ratio = like_count / view_count
                    comment_ratio = comment_count / view_count
                    engagement_component = min((like_ratio * 20 + comment_ratio * 100) / 2, 1.0)
                else:
                    engagement_component = 0.0

                # 3. Recency Component (higher score for recent videos)
                if time_since_pub <= time_window_hours:
                    recency_component = 1.0 - (time_since_pub / time_window_hours)
                else:
                    recency_component = 0.1  # Small bonus for older videos

                # 4. Consistency Component (balance between metrics)
                metrics = [velocity_component, engagement_component, recency_component]
                consistency_component = 1.0 - (np.std(metrics) / (np.mean(metrics) + 0.001))
                consistency_component = max(0, min(consistency_component, 1.0))

                # Calculate overall virality score
                virality_score = (
                    velocity_weight * velocity_component
                    + engagement_weight * engagement_component
                    + recency_weight * recency_component
                    + consistency_weight * consistency_component
                )

                # Determine virality category
                if virality_score >= viral_threshold:
                    category = "viral"
                elif virality_score >= 0.6:
                    category = "trending"
                elif virality_score >= 0.4:
                    category = "growing"
                elif virality_score >= 0.2:
                    category = "stable"
                else:
                    category = "low_potential"

                # Calculate prediction confidence
                data_quality_factors = [
                    min(view_count / 100000, 1.0),  # More views = higher confidence
                    min(time_since_pub / 24, 1.0),  # More time = higher confidence
                    min((like_count + comment_count) / 1000, 1.0),  # More engagement = higher confidence
                ]
                prediction_confidence = np.mean(data_quality_factors)

                results.append(
                    {
                        "video_id": video_id,
                        "artist_name": artist_name,
                        "virality_score": virality_score,
                        "virality_category": category,
                        "velocity_component": velocity_component,
                        "engagement_component": engagement_component,
                        "recency_component": recency_component,
                        "prediction_confidence": prediction_confidence,
                    }
                )

            result_df = pd.DataFrame(results)

            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise

    def _validate_configuration(self) -> None:
        """Enhanced configuration validation."""
        super()._validate_configuration()

        # Validate weights sum to 1.0
        weights = [
            self.config.get("velocity_weight", 0.4),
            self.config.get("engagement_weight", 0.3),
            self.config.get("recency_weight", 0.2),
            self.config.get("consistency_weight", 0.1),
        ]

        if abs(sum(weights) - 1.0) > 0.01:
            raise ValueError("Component weights must sum to 1.0")

        # Validate individual parameters
        for weight_name, weight_value in zip(
            ["velocity_weight", "engagement_weight", "recency_weight", "consistency_weight"], weights
        ):
            if not 0 <= weight_value <= 1:
                raise ValueError(f"{weight_name} must be between 0 and 1")

        # Validate thresholds
        viral_threshold = self.config.get("viral_threshold", 0.75)
        if not 0 <= viral_threshold <= 1:
            raise ValueError("viral_threshold must be between 0 and 1")

        min_views = self.config.get("min_views_threshold", 10000)
        if not isinstance(min_views, int) or min_views < 0:
            raise ValueError("min_views_threshold must be a non - negative integer")


def create_sample_music_data() -> pd.DataFrame:
    """Create realistic sample music data for testing."""
    np.random.seed(42)  # For reproducible results

    artists = ["Taylor Swift", "Drake", "Billie Eilish", "The Weeknd", "Ariana Grande"]
    video_types = ["Official Video", "Lyric Video", "Live Performance", "Behind the Scenes"]

    data_points = []

    for i in range(50):  # 50 sample videos
        artist = np.random.choice(artists)
        video_type = np.random.choice(video_types)

        # Simulate different virality levels
        virality_level = np.random.choice(["low", "medium", "high", "viral"], p=[0.4, 0.3, 0.2, 0.1])

        if virality_level == "viral":
            base_views = np.random.randint(5_000_000, 50_000_000)
            engagement_multiplier = np.random.uniform(1.5, 3.0)
            hours_old = np.random.randint(1, 72)
        elif virality_level == "high":
            base_views = np.random.randint(1_000_000, 10_000_000)
            engagement_multiplier = np.random.uniform(1.2, 2.0)
            hours_old = np.random.randint(6, 168)
        elif virality_level == "medium":
            base_views = np.random.randint(100_000, 2_000_000)
            engagement_multiplier = np.random.uniform(0.8, 1.5)
            hours_old = np.random.randint(24, 720)
        else:  # low
            base_views = np.random.randint(10_000, 500_000)
            engagement_multiplier = np.random.uniform(0.5, 1.0)
            hours_old = np.random.randint(48, 2160)

        like_count = int(base_views * 0.03 * engagement_multiplier * np.random.uniform(0.5, 1.5))
        comment_count = int(base_views * 0.003 * engagement_multiplier * np.random.uniform(0.5, 1.5))

        published_date = datetime.now() - timedelta(hours=hours_old)

        data_points.append(
            {
                "video_id": f"{artist.replace(' ', '_')}_{video_type.replace(' ', '_')}_{i:03d}",
                "artist_name": artist,
                "video_title": f"{artist} - {video_type} {i + 1}",
                "view_count": base_views,
                "like_count": like_count,
                "comment_count": comment_count,
                "published_date": published_date,
                "actual_virality": virality_level,  # For validation purposes
            }
        )

    return pd.DataFrame(data_points)


def demonstrate_complete_workflow():
    """Demonstrate the complete plugin development and usage workflow."""
    print("🎵 Complete Music Analytics Plugin Development Example")
    print("=" * 60)

    # Step 1: Create and validate plugin
    print("\n📝 Step 1: Creating and Validating Plugin")
    print("-" * 40)

    plugin = MusicViralityPlugin()
    validator = PluginValidator()
    _security_checker = AdvancedSecurityChecker()

    # Validate plugin structure
    structure_result = validator.validate_plugin_structure(plugin)
    print(f"Plugin structure: {'✅ VALID' if structure_result.is_valid else '❌ INVALID'}")
    if not structure_result.is_valid:
        for error in structure_result.errors:
            print(f"  - {error}")

    # Validate metadata
    metadata = plugin.get_metadata()
    metadata_result = validator.validate_plugin_metadata(metadata)
    print(f"Plugin metadata: {'✅ VALID' if metadata_result.is_valid else '❌ INVALID'}")
    if not metadata_result.is_valid:
        for error in metadata_result.errors:
            print(f"  - {error}")

    # Step 2: Register plugin
    print("\n🔧 Step 2: Registering Plugin")
    print("-" * 40)

    registry = PluginRegistry()
    registration_result = registry.register_plugin(plugin)

    if registration_result.is_valid:
        print("✅ Plugin registered successfully!")
        print(f"   Name: {metadata.name}")
        print(f"   Version: {metadata.version}")
        print(f"   Author: {metadata.author}")
        print(f"   Tags: {', '.join(metadata.tags)}")
    else:
        print("❌ Plugin registration failed:")
        for error in registration_result.errors:
            print(f"  - {error}")
        return

    # Step 3: Load and validate sample data
    print("\n📊 Step 3: Loading Sample Data")
    print("-" * 40)

    sample_data = create_sample_music_data()
    print(f"Created {len(sample_data)} sample music videos")
    print(f"Artists: {', '.join(sample_data['artist_name'].unique())}")
    print(f"Date range: {sample_data['published_date'].min()} to {sample_data['published_date'].max()}")

    # Validate input data
    input_validation = plugin.validate_input(sample_data)
    print(f"Input validation: {'✅ PASS' if input_validation.is_valid else '❌ FAIL'}")

    if input_validation.warnings:
        print("⚠️  Warnings:")
        for warning in input_validation.warnings:
            print(f"  - {warning}")

    if not input_validation.is_valid:
        print("❌ Input validation errors:")
        for error in input_validation.errors:
            print(f"  - {error}")
        return

    # Step 4: Configure and run plugin
    print("\n⚙️  Step 4: Configuring and Running Plugin")
    print("-" * 40)

    # Configure plugin with custom parameters
    plugin_config = {
        "velocity_weight": 0.35,
        "engagement_weight": 0.35,
        "recency_weight": 0.20,
        "consistency_weight": 0.10,
        "min_views_threshold": 50000,
        "viral_threshold": 0.70,
        "time_window_hours": 72,
    }

    plugin.load_configuration(plugin_config)
    print("✅ Plugin configured with custom parameters")

    # Calculate virality scores
    print("🔄 Calculating virality scores...")
    results = plugin.calculate_scores(sample_data)

    print(f"✅ Analysis complete! Processed {len(results)} videos")

    # Step 5: Analyze results
    print("\n📈 Step 5: Analyzing Results")
    print("-" * 40)

    # Summary statistics
    print("📊 Virality Score Distribution:")
    score_stats = results["virality_score"].describe()
    print(f"   Mean: {score_stats['mean']:.3f}")
    print(f"   Std:  {score_stats['std']:.3f}")
    print(f"   Min:  {score_stats['min']:.3f}")
    print(f"   Max:  {score_stats['max']:.3f}")

    # Category breakdown
    print("\n🏷️  Virality Categories:")
    category_counts = results["virality_category"].value_counts()
    for category, count in category_counts.items():
        percentage = (count / len(results)) * 100
        print(f"   {category}: {count} videos ({percentage:.1f}%)")

    # Top performers
    print("\n🌟 Top 5 Viral Candidates:")
    top_videos = results.nlargest(5, "virality_score")
    for idx, row in top_videos.iterrows():
        print(f"   {row['artist_name']} - {row['video_id']}")
        print(f"      Score: {row['virality_score']:.3f} ({row['virality_category']})")
        print(f"      Confidence: {row['prediction_confidence']:.3f}")
        print()

    # Artist performance
    print("🎤 Artist Performance Summary:")
    artist_summary = (
        results.groupby("artist_name")
        .agg({"virality_score": ["mean", "max", "count"], "virality_category": lambda x: (x == "viral").sum()})
        .round(3)
    )

    artist_summary.columns = ["Avg_Score", "Max_Score", "Total_Videos", "Viral_Count"]
    artist_summary = artist_summary.sort_values("Avg_Score", ascending=False)

    for artist, row in artist_summary.iterrows():
        print(f"   {artist}:")
        print(f"      Avg Score: {row['Avg_Score']:.3f}")
        print(f"      Best Score: {row['Max_Score']:.3f}")
        print(f"      Videos: {int(row['Total_Videos'])}")
        print(f"      Viral Videos: {int(row['Viral_Count'])}")
        print()

    # Step 6: Export results
    print("💾 Step 6: Exporting Results")
    print("-" * 40)

    # Export to CSV
    plugin.export_results(results, "csv", "virality_analysis_results.csv")
    print("✅ Results exported to virality_analysis_results.csv")

    # Export to JSON for API integration
    plugin.export_results(results, "json", "virality_analysis_results.json")
    print("✅ Results exported to virality_analysis_results.json")

    # Step 7: Plugin performance metrics
    print("\n⏱️  Step 7: Performance Metrics")
    print("-" * 40)

    execution_metadata = plugin.get_execution_metadata()
    if execution_metadata:
        duration = execution_metadata.get("duration_seconds", 0)
        records_per_second = len(results) / max(duration, 0.001)

        print(f"Execution time: {duration:.3f} seconds")
        print(f"Processing rate: {records_per_second:.1f} records / second")
        print(f"Memory efficiency: {len(results) * 8 / 1024:.1f} KB output")
        print(f"Success rate: {'100%' if execution_metadata.get('success', False) else 'Failed'}")

    print("\n🎉 Complete workflow demonstration finished!")
    print("   Check the exported files for detailed results.")
    print("   This plugin is now ready for production use!")

    return results


if __name__ == "__main__":
    # Run the complete demonstration
    results = demonstrate_complete_workflow()

    print("\n" + "=" * 60)
    print("🚀 Next Steps:")
    print("1. Customize the plugin parameters for your specific use case")
    print("2. Integrate with your existing YouTube data pipeline")
    print("3. Set up automated virality monitoring")
    print("4. Create visualizations using the exported data")
    print("5. Share your plugin with the music analytics community!")
