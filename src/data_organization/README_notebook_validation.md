# Notebook Validation and Output Explanation System

A comprehensive validation system for notebook cell outputs, metric explanations, and chart data validation to ensure data quality and provide clear explanations for scoring metrics.

## Overview

The notebook validation system provides three main components:

1. **NotebookValidator**: Main validator for notebook outputs and schema validation
2. **MetricExplainer**: Provides clear explanations for scoring metrics
3. **OutputValidator**: Validates data types, ranges, and chart requirements

## Features

### Data Validation
- **Data Type Validation**: Ensures DataFrame columns have expected data types
- **Score Range Validation**: Validates that scores are within specified ranges
- **Missing Value Detection**: Checks for missing values in required columns
- **Chart Requirements**: Validates data meets requirements for specific chart types

### Metric Explanations
- **Clear Definitions**: Human-readable explanations for scoring metrics
- **Contextual Tooltips**: Generate tooltips for chart elements
- **Legend Generation**: Create comprehensive legends for dashboards
- **Range Interpretation**: Explains what different score ranges mean

### Schema Validation
- **Cell Output Validation**: Validates notebook cell outputs against expected schemas
- **Notebook Structure**: Validates overall notebook file structure
- **Error Reporting**: Detailed error messages with actionable debugging information

## Quick Start

```python
from src.data_organization.notebook_validator import (
    NotebookValidator,
    MetricExplainer,
    OutputValidator
)

# Initialize validators
notebook_validator = NotebookValidator()
output_validator = OutputValidator()
metric_explainer = MetricExplainer()

# Validate DataFrame output
data = pd.DataFrame({
    'artist_name': ['Artist_A', 'Artist_B'],
    'momentum_score': [0.85, 0.72]
})

# Validate data types
expected_types = {
    'artist_name': 'object',
    'momentum_score': 'float64'
}
result = output_validator.validate_data_types(data, expected_types)

# Validate score ranges
score_result = output_validator.validate_score_range(
    data['momentum_score'], 0.0, 1.0
)

# Generate metric explanations
explanation = metric_explainer.explain_momentum_score(0.85)
print(explanation)
# Output: "Momentum Score: 0.85 - Exceptional momentum - rapid acceleration..."
```

## Components

### OutputValidator

Validates data types, ranges, and chart requirements for notebook outputs.

#### Methods

- `validate_score_range(scores, min_val, max_val)`: Validate score ranges
- `validate_data_types(data, expected_types)`: Validate DataFrame column types
- `check_missing_values(data, required_columns)`: Check for missing values
- `validate_chart_requirements(data, chart_type)`: Validate chart data requirements

#### Example

```python
validator = OutputValidator()

# Validate score range
scores = pd.Series([0.1, 0.5, 0.9])
result = validator.validate_score_range(scores, 0.0, 1.0)

if result.is_valid:
    print("✅ All scores are within valid range")
else:
    for error in result.errors:
        print(f"❌ {error}")
```

### MetricExplainer

Provides clear explanations for scoring metrics and generates tooltips.

#### Supported Metrics

- **momentum_score**: Measures recent growth trajectory and engagement trends
- **engagement_rate**: Ratio of interactions to total views
- **growth_potential**: Predicted likelihood of future growth

#### Methods

- `explain_momentum_score(score)`: Generate momentum score explanation
- `explain_engagement_rate(rate)`: Generate engagement rate explanation
- `explain_growth_potential(potential)`: Generate growth potential explanation
- `generate_tooltip_text(metric_name, value)`: Create tooltip text
- `create_legend_definitions(metrics)`: Generate legend definitions

#### Example

```python
explainer = MetricExplainer()

# Generate individual explanations
momentum_explanation = explainer.explain_momentum_score(0.75)
engagement_explanation = explainer.explain_engagement_rate(0.045)

# Create tooltips for charts
tooltip = explainer.generate_tooltip_text('momentum_score', 0.75)

# Generate legends for dashboard
metrics = ['momentum_score', 'engagement_rate', 'growth_potential']
legends = explainer.create_legend_definitions(metrics)
```

### NotebookValidator

Main validator for notebook outputs with schema validation and error reporting.

#### Methods

- `validate_cell_output(cell_output, expected_schema)`: Validate cell output
- `validate_chart_data(chart_data)`: Validate chart data
- `generate_metric_explanations(metrics)`: Generate metric explanations
- `create_validation_report(notebook_path)`: Create notebook validation report

#### Schema Definition

```python
expected_schema = {
    'type': 'dataframe',  # 'dataframe', 'series', 'dict', 'list'
    'columns': {
        'artist_name': 'object',
        'momentum_score': 'float64'
    },
    'min_rows': 1,
    'required_columns': ['artist_name', 'momentum_score']
}
```

#### Example

```python
validator = NotebookValidator()

# Validate cell output
output = pd.DataFrame({'artist': ['A'], 'score': [0.5]})
schema = {
    'type': 'dataframe',
    'columns': {'artist': 'object', 'score': 'float64'},
    'min_rows': 1
}

result = validator.validate_cell_output(output, schema)
if result.is_valid:
    print("✅ Cell output is valid")
```

## Integration with Existing Workflows

### Decorator for Automatic Validation

```python
def notebook_cell_validation_decorator(expected_schema):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            validator = NotebookValidator()
            validation_result = validator.validate_cell_output(result, expected_schema)
            
            if not validation_result.is_valid:
                print("⚠️  Cell output validation failed:")
                for error in validation_result.errors:
                    print(f"   - {error}")
            
            return result
        return wrapper
    return decorator

# Usage
@notebook_cell_validation_decorator({
    'type': 'dataframe',
    'columns': {'artist_name': 'object', 'score': 'float64'},
    'min_rows': 1
})
def calculate_scores():
    return pd.DataFrame({'artist_name': ['A'], 'score': [0.5]})
```

### Integration with Analytics Pipeline

```python
def validate_analytics_output(data):
    """Comprehensive validation for analytics data."""
    validator = OutputValidator()
    results = []
    
    # Data type validation
    expected_types = {
        'artist_name': 'object',
        'momentum_score': 'float64',
        'engagement_rate': 'float64'
    }
    results.append(validator.validate_data_types(data, expected_types))
    
    # Score range validations
    results.append(validator.validate_score_range(data['momentum_score'], 0.0, 1.0))
    results.append(validator.validate_score_range(data['engagement_rate'], 0.0, 0.2))
    
    # Missing values check
    required_columns = ['artist_name', 'momentum_score']
    results.append(validator.check_missing_values(data, required_columns))
    
    # Merge all results
    final_result = results[0]
    for result in results[1:]:
        final_result = final_result.merge(result)
    
    return final_result
```

### Chart Integration

```python
def create_validated_chart(data, chart_type='scatter'):
    """Create chart with validated data and explanations."""
    validator = OutputValidator()
    explainer = MetricExplainer()
    
    # Validate chart data
    chart_result = validator.validate_chart_requirements(data, chart_type)
    if not chart_result.is_valid:
        raise ValueError(f"Data not suitable for {chart_type} chart")
    
    # Generate tooltips
    tooltips = {}
    for _, row in data.iterrows():
        artist = row['artist_name']
        tooltip = explainer.generate_tooltip_text('momentum_score', row['momentum_score'])
        tooltips[artist] = tooltip
    
    # Generate legends
    metrics = ['momentum_score', 'engagement_rate']
    legends = explainer.create_legend_definitions(metrics)
    
    return {
        'data': data,
        'tooltips': tooltips,
        'legends': legends,
        'validation_passed': True
    }
```

## Error Handling

The system provides comprehensive error handling with detailed messages:

### ValidationResult

```python
@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    checked_items: int
    passed_items: int
    metadata: Dict[str, Any]
    
    def add_error(self, error: str) -> None
    def add_warning(self, warning: str) -> None
    def merge(self, other: 'ValidationResult') -> 'ValidationResult'
```

### Custom Exceptions

- `ValidationError`: Base exception for validation errors
- `SchemaValidationError`: Raised when schema validation fails
- `OutputValidationError`: Raised when output validation fails

## Testing

The system includes comprehensive tests:

```bash
# Run all validation tests
python -m pytest tests/test_notebook_validator.py -v

# Run integration tests
python -m pytest tests/test_notebook_validation_integration.py -v

# Run demo
python demo_notebook_validation_system.py

# Run example
python examples/notebook_validation_example.py
```

## Configuration

### Metric Definitions

The system includes predefined explanations for common metrics. You can extend these by modifying the `metric_definitions` in `MetricExplainer`:

```python
self.metric_definitions = {
    'custom_metric': {
        'name': 'Custom Metric',
        'description': 'Description of your custom metric',
        'range': '0.0 to 1.0',
        'interpretation': {
            (0.0, 0.5): 'Low value interpretation',
            (0.5, 1.0): 'High value interpretation'
        }
    }
}
```

### Chart Requirements

Chart requirements can be customized by modifying the `min_requirements` in `validate_chart_requirements`:

```python
min_requirements = {
    'custom_chart': {'min_rows': 5, 'min_cols': 3}
}
```

## Best Practices

1. **Always validate before visualization**: Use validation before creating charts
2. **Provide clear error messages**: Include specific information about what failed
3. **Use comprehensive schemas**: Define complete schemas for cell outputs
4. **Generate explanations**: Always provide metric explanations for users
5. **Handle edge cases**: Validate for empty data, infinite values, etc.
6. **Performance considerations**: Use efficient validation for large datasets

## Requirements

- pandas >= 2.2.0
- numpy >= 1.24.0
- Python >= 3.8

## License

This module is part of the YouTube ETL and Analytics platform and follows the same licensing terms.