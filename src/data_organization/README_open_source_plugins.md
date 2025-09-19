# Open-Source Music Analytics Plugin Framework

A comprehensive framework for music data researchers to create, share, and benchmark custom scoring algorithms for YouTube music analytics.

## Overview

This framework enables the music data community on GitHub to:

- **Create custom scoring algorithms** for music analytics
- **Share plugins** with the research community
- **Benchmark performance** against real YouTube data
- **Ensure security** through validation and sandboxing
- **Collaborate** on music data analysis methodologies

## Quick Start

### Creating Your First Plugin

```python
from src.data_organization.open_source_plugin_framework import (
    OpenSourceScoringPlugin, PluginMetadata
)

class MyMusicPlugin(OpenSourceScoringPlugin):
    def get_name(self) -> str:
        return "my_music_algorithm"
    
    def get_version(self) -> str:
        return "1.0.0"
    
    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="my_music_algorithm",
            version="1.0.0",
            author="Your Name",
            description="Custom algorithm for music analytics",
            parameters={"threshold": 0.5},
            input_requirements=["view_count", "like_count"],
            output_schema={"score": "float64"},
            tags=["music", "engagement", "custom"]
        )
    
    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        # Validate your input requirements
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0)
        
        for col in ["view_count", "like_count"]:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' missing")
            else:
                result.passed_items += 1
        
        return result
    
    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        # Your music analytics algorithm here
        threshold = self.config.get('threshold', 0.5)
        
        # Example: Simple engagement score
        engagement_score = (data['like_count'] / data['view_count']) * threshold
        
        return data.assign(
            engagement_score=engagement_score,
            score_category=pd.cut(engagement_score, 
                                bins=[0, 0.02, 0.05, 1.0], 
                                labels=['low', 'medium', 'high'])
        )
```

### Registering and Using Plugins

```python
from src.data_organization.open_source_plugin_framework import PluginRegistry

# Create registry and register your plugin
registry = PluginRegistry()
plugin = MyMusicPlugin()
result = registry.register_plugin(plugin)

if result.is_valid:
    print("Plugin registered successfully!")
    
    # Use the plugin
    plugin.load_configuration({"threshold": 0.75})
    scores = plugin.calculate_scores(your_youtube_data)
    print(scores.head())
```

## Framework Components

### 1. OpenSourceScoringPlugin (Base Class)

The abstract base class that all plugins must inherit from.

**Required Methods:**
- `get_name()` - Unique plugin identifier
- `get_version()` - Semantic version (e.g., "1.0.0")
- `get_metadata()` - Complete plugin information
- `calculate_scores()` - Main algorithm implementation
- `validate_input()` - Input data validation

**Built-in Features:**
- Configuration management
- Result export (CSV, JSON, Parquet)
- Execution metadata tracking
- Error handling

### 2. PluginMetadata

Comprehensive metadata system for plugin discovery and documentation.

```python
metadata = PluginMetadata(
    name="view_velocity",
    version="1.0.0",
    author="Music Analytics Community",
    description="Calculates view velocity based on growth patterns",
    parameters={
        "time_window_days": 7,
        "velocity_weight": 0.7
    },
    input_requirements=["video_id", "view_count", "analytics_date"],
    output_schema={
        "video_id": "object",
        "velocity_score": "float64",
        "velocity_category": "object"
    },
    license="MIT",
    repository_url="https://github.com/your-repo/music-plugins",
    tags=["velocity", "trending", "growth"]
)
```

### 3. PluginValidator

Ensures plugins meet quality and compatibility standards.

**Validation Checks:**
- Plugin structure and required methods
- Metadata completeness and format
- Input/output schema compliance
- Data type validation

### 4. PluginSecurityChecker

Security validation to protect against malicious code.

**Security Features:**
- Dangerous import detection
- Function call analysis
- File operation monitoring
- Resource limit checking
- Permission validation

### 5. PluginRegistry

Central registry for plugin management and discovery.

**Registry Features:**
- Plugin registration and validation
- Search and discovery
- Metadata management
- Import/export functionality

## Example Plugins

The framework includes several example plugins that demonstrate common music analytics patterns:

### ViewVelocityPlugin

Analyzes how quickly videos gain views over time.

**Use Cases:**
- Identifying trending content
- Viral potential assessment
- Growth pattern analysis

**Input:** `video_id`, `view_count`, `published_date`, `analytics_date`
**Output:** `view_velocity_score`, `daily_view_rate`, `velocity_category`

### EngagementQualityPlugin

Analyzes engagement quality beyond simple ratios.

**Use Cases:**
- Audience connection assessment
- Content quality evaluation
- Engagement sustainability analysis

**Input:** `video_id`, `view_count`, `like_count`, `comment_count`
**Output:** `engagement_quality_score`, `sentiment_factor`, `quality_category`

### CrossPlatformMomentumPlugin

Platform-agnostic momentum calculation for cross-platform analysis.

**Use Cases:**
- Comparing artists across platforms
- Standardized momentum metrics
- Multi-platform research

**Input:** `entity_id`, `metric_value`, `metric_date`, `platform`
**Output:** `momentum_score`, `growth_velocity`, `momentum_category`

### GenreSpecificScoringPlugin

Genre-aware scoring that accounts for different engagement patterns.

**Use Cases:**
- Fair genre comparisons
- Genre-specific benchmarking
- Music industry research

**Input:** `entity_id`, `genre`, `view_count`, `like_count`, `comment_count`
**Output:** `genre_adjusted_score`, `performance_vs_genre`

## Benchmarking System

The framework includes a comprehensive benchmarking system to test plugins against real YouTube data.

### Running Benchmarks

```bash
# Benchmark specific plugin
python tools/benchmark_plugins.py --plugin view_velocity --data-size 1000

# Benchmark all plugins
python tools/benchmark_plugins.py --all-plugins --export-results

# Custom benchmark parameters
python tools/benchmark_plugins.py --plugin engagement_quality \
    --data-size 5000 --days-back 60 --export-results
```

### Benchmark Metrics

**Performance Metrics:**
- Execution time
- Memory usage
- Records processed per second
- Scalability analysis

**Quality Metrics:**
- Input/output validation
- Score distribution analysis
- Error rate tracking
- Data quality assessment

### Sample Benchmark Output

```
MUSIC ANALYTICS PLUGIN BENCHMARK REPORT
================================================================================
Benchmark Date: 2024-01-15T10:30:00
Total Plugins Tested: 4
Data Size: 1000 records
Successful Plugins: 4
Failed Plugins: 0

PERFORMANCE SUMMARY
----------------------------------------
Average Execution Time: 0.245 seconds
Total Execution Time: 0.980 seconds
Average Memory Usage: 12.5 MB
Fastest Plugin: view_velocity
Slowest Plugin: genre_specific_scoring

PLUGIN: view_velocity
----------------------------------------
✅ Execution Time: 0.180s
   Memory Used: 8.2 MB
   Records/Second: 5556
   Output Records: 847
   Input Validation: ✅ PASS
   Output Validation: ✅ PASS
   Score Distribution:
     view_velocity_score: mean=0.342, std=0.198
```

## Development Guidelines

### Plugin Development Best Practices

1. **Clear Documentation**
   - Comprehensive docstrings
   - Usage examples
   - Parameter explanations

2. **Robust Input Validation**
   - Check required columns
   - Validate data types
   - Handle edge cases

3. **Meaningful Output**
   - Descriptive column names
   - Categorical interpretations
   - Confidence metrics

4. **Performance Optimization**
   - Efficient algorithms
   - Memory management
   - Scalable implementations

5. **Testing**
   - Unit tests for all methods
   - Integration tests
   - Edge case handling

### Security Guidelines

**Allowed Operations:**
- Data manipulation with pandas/numpy
- Mathematical calculations
- Statistical analysis
- Data visualization preparation

**Prohibited Operations:**
- File system access (except through framework)
- Network requests
- System command execution
- Dynamic code execution
- Dangerous imports (os, subprocess, etc.)

### Configuration Standards

```python
# Good configuration example
parameters = {
    "time_window_days": 7,        # Clear parameter names
    "min_views_threshold": 1000,  # Descriptive thresholds
    "velocity_weight": 0.7,       # Normalized weights (0-1)
    "enable_normalization": True  # Boolean flags
}

# Configuration validation
def _validate_configuration(self):
    if self.config.get('velocity_weight', 0) < 0 or self.config.get('velocity_weight', 0) > 1:
        raise ValueError("velocity_weight must be between 0 and 1")
```

## Integration with Existing System

### Database Integration

Plugins work seamlessly with existing YouTube analytics tables:

```python
# Example: Loading data for plugin
query = """
SELECT video_id, view_count, like_count, comment_count, published_date
FROM youtube_videos 
WHERE published_date >= %s
"""
data = pd.read_sql(query, engine, params=[start_date])

# Run plugin
plugin = ViewVelocityPlugin()
results = plugin.calculate_scores(data)
```

### Notebook Integration

```python
# In Jupyter notebooks
from src.data_organization.open_source_plugin_framework import PluginRegistry

registry = PluginRegistry()
# ... register plugins ...

# Use in analysis
velocity_plugin = registry.get_plugin("view_velocity")
velocity_scores = velocity_plugin.calculate_scores(youtube_data)

# Visualize results
import plotly.express as px
fig = px.scatter(velocity_scores, x='daily_view_rate', y='view_velocity_score',
                color='velocity_category', title='View Velocity Analysis')
fig.show()
```

## Contributing to the Framework

### Adding New Example Plugins

1. Create plugin class inheriting from `OpenSourceScoringPlugin`
2. Implement all required methods
3. Add comprehensive tests
4. Update documentation
5. Submit pull request

### Improving the Framework

**Areas for Contribution:**
- Additional validation checks
- Performance optimizations
- New security features
- Enhanced benchmarking
- Documentation improvements

### Community Guidelines

- Use descriptive variable names
- Follow existing code style
- Include comprehensive tests
- Document all public methods
- Respect security guidelines

## License and Usage

This framework is released under the MIT License, encouraging open collaboration in the music data research community.

**Commercial Use:** Permitted with attribution
**Modification:** Encouraged for research purposes
**Distribution:** Share improvements with the community
**Attribution:** Credit original authors and contributors

## Support and Community

- **GitHub Issues:** Report bugs and request features
- **Discussions:** Share research findings and methodologies
- **Wiki:** Community-maintained documentation
- **Examples:** Real-world usage patterns and case studies

---

*Built for the music data research community to advance understanding of music analytics and audience engagement patterns.*