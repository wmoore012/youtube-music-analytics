"""
Notebook validation and output explanation system.

This module provides comprehensive validation for notebook cell outputs,
metric explanations, and chart data validation to ensure data quality
and provide clear explanations for scoring metrics.

Components:
- NotebookValidator: Main validator for notebook outputs and schema validation
- MetricExplainer: Provides clear explanations for scoring metrics
- OutputValidator: Validates data types, ranges, and chart requirements
- ValidationResult: Data class for validation results
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Base exception for validation errors."""

    pass


class SchemaValidationError(ValidationError):
    """Raised when schema validation fails."""

    pass


class OutputValidationError(ValidationError):
    """Raised when output validation fails."""

    pass


@dataclass
class ValidationResult:
    """
    Result of validation operation with detailed information.

    Attributes:
        is_valid: Whether validation passed
        errors: List of error messages
        warnings: List of warning messages
        checked_items: Number of items checked
        passed_items: Number of items that passed validation
        metadata: Additional metadata about validation
    """

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checked_items: int = 0
    passed_items: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, error: str) -> None:
        """Add an error and mark validation as failed."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning (doesn't affect validity)."""
        self.warnings.append(warning)

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge two validation results."""
        # Handle metadata merging more carefully to avoid overwrites
        merged_metadata = {}

        # Add metadata from self
        for key, value in self.metadata.items():
            if key in merged_metadata:
                # If key exists, create a list or extend existing list
                if isinstance(merged_metadata[key], list):
                    merged_metadata[key].append(value)
                else:
                    merged_metadata[key] = [merged_metadata[key], value]
            else:
                merged_metadata[key] = value

        # Add metadata from other
        for key, value in other.metadata.items():
            if key in merged_metadata:
                # If key exists, create a list or extend existing list
                if isinstance(merged_metadata[key], list):
                    merged_metadata[key].append(value)
                else:
                    merged_metadata[key] = [merged_metadata[key], value]
            else:
                merged_metadata[key] = value

        return ValidationResult(
            is_valid=self.is_valid and other.is_valid,
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
            checked_items=self.checked_items + other.checked_items,
            passed_items=self.passed_items + other.passed_items,
            metadata=merged_metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "checked_items": self.checked_items,
            "passed_items": self.passed_items,
            "metadata": self.metadata,
        }


class OutputValidator:
    """
    Validates data types, ranges, and chart requirements for notebook outputs.

    This class provides methods to validate DataFrame outputs, score ranges,
    data types, missing values, and chart data requirements.
    """

    def validate_score_range(self, scores: pd.Series, min_val: float, max_val: float) -> ValidationResult:
        """
        Validate that scores are within the specified range.

        Args:
            scores: Series of score values to validate
            min_val: Minimum allowed value
            max_val: Maximum allowed value

        Returns:
            ValidationResult with range validation details
        """
        result = ValidationResult(is_valid=True, checked_items=len(scores), passed_items=0)

        for idx, score in scores.items():
            if pd.isna(score):
                result.add_warning(f"Score at index {idx} is NaN")
                continue

            if score < min_val or score > max_val:
                result.add_error(f"Score {score} at index {idx} is outside valid range [{min_val}, {max_val}]")
            else:
                result.passed_items += 1

        result.metadata = {
            "min_value": min_val,
            "max_value": max_val,
            "actual_min": scores.min() if not scores.empty else None,
            "actual_max": scores.max() if not scores.empty else None,
        }

        return result

    def validate_data_types(self, data: pd.DataFrame, expected_types: Dict[str, str]) -> ValidationResult:
        """
        Validate that DataFrame columns have expected data types.

        Args:
            data: DataFrame to validate
            expected_types: Dictionary mapping column names to expected dtypes

        Returns:
            ValidationResult with data type validation details
        """
        result = ValidationResult(is_valid=True, checked_items=len(expected_types), passed_items=0)

        for column, expected_type in expected_types.items():
            if column not in data.columns:
                result.add_error(f"Required column '{column}' is missing from data")
                continue

            actual_type = str(data[column].dtype)

            # Handle common type variations
            if self._types_compatible(actual_type, expected_type):
                result.passed_items += 1
            else:
                result.add_error(f"Column '{column}' has type '{actual_type}', expected '{expected_type}'")

        result.metadata = {
            "expected_types": expected_types,
            "actual_types": {col: str(data[col].dtype) for col in data.columns},
        }

        return result

    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if actual and expected types are compatible."""
        # Handle common type compatibility cases
        type_mappings = {
            "int64": ["int64", "int32", "int"],
            "float64": ["float64", "float32", "float"],
            "object": ["object", "string"],
            "bool": ["bool", "boolean"],
        }

        if actual == expected:
            return True

        for base_type, compatible_types in type_mappings.items():
            if expected == base_type and actual in compatible_types:
                return True

        return False

    def check_missing_values(self, data: pd.DataFrame, required_columns: List[str]) -> ValidationResult:
        """
        Check for missing values in required columns.

        Args:
            data: DataFrame to check
            required_columns: List of columns that cannot have missing values

        Returns:
            ValidationResult with missing value validation details
        """
        result = ValidationResult(is_valid=True, checked_items=len(required_columns), passed_items=0)

        missing_info = {}

        for column in required_columns:
            if column not in data.columns:
                result.add_error(f"Required column '{column}' is missing from data")
                continue

            missing_count = data[column].isna().sum()
            missing_info[column] = missing_count

            if missing_count > 0:
                result.add_error(f"Column '{column}' has {missing_count} missing values out of {len(data)} rows")
            else:
                result.passed_items += 1

        result.metadata = {"missing_counts": missing_info, "total_rows": len(data)}

        return result

    def validate_chart_requirements(self, data: pd.DataFrame, chart_type: str) -> ValidationResult:
        """
        Validate that data meets requirements for specific chart types.

        Args:
            data: DataFrame to validate
            chart_type: Type of chart ('scatter', 'bar', 'line', etc.)

        Returns:
            ValidationResult with chart requirement validation details
        """
        result = ValidationResult(is_valid=True, checked_items=1, passed_items=0)

        # Minimum data requirements by chart type
        min_requirements = {
            "scatter": {"min_rows": 2, "min_cols": 2},
            "bar": {"min_rows": 1, "min_cols": 2},
            "line": {"min_rows": 2, "min_cols": 2},
            "histogram": {"min_rows": 1, "min_cols": 1},
            "heatmap": {"min_rows": 2, "min_cols": 2},
        }

        requirements = min_requirements.get(chart_type, {"min_rows": 1, "min_cols": 1})

        if len(data) < requirements["min_rows"]:
            result.add_error(
                f"Insufficient data for {chart_type} chart: {len(data)} rows, "
                f"minimum {requirements['min_rows']} required"
            )
        elif len(data.columns) < requirements["min_cols"]:
            result.add_error(
                f"Insufficient columns for {chart_type} chart: {len(data.columns)} columns, "
                f"minimum {requirements['min_cols']} required"
            )
        else:
            result.passed_items = 1

        result.metadata = {"chart_type": chart_type, "data_shape": data.shape, "requirements": requirements}

        return result


class MetricExplainer:
    """
    Provides clear explanations for scoring metrics and generates tooltips.

    This class creates human-readable explanations for various scoring metrics
    used in the analytics system, helping users understand what each score means.
    """

    def __init__(self):
        """Initialize MetricExplainer with predefined explanations."""
        self.metric_definitions = {
            "momentum_score": {
                "name": "Momentum Score",
                "description": "Measures recent growth trajectory and engagement trends",
                "range": "0.0 to 1.0",
                "interpretation": {
                    (0.0, 0.3): "Low momentum-limited recent growth",
                    (0.3, 0.6): "Moderate momentum-steady growth pattern",
                    (0.6, 0.8): "High momentum-strong growth trajectory",
                    (0.8, 1.0): "Exceptional momentum-rapid acceleration",
                },
            },
            "engagement_rate": {
                "name": "Engagement Rate",
                "description": "Ratio of interactions (likes, comments) to total views",
                "range": "0.0 to 1.0 (typically 0.01 to 0.10)",
                "interpretation": {
                    (0.0, 0.02): "Low engagement-audience not actively participating",
                    (0.02, 0.04): "Average engagement-typical for most content",
                    (0.04, 0.08): "High engagement-strong audience connection",
                    (0.08, 1.0): "Exceptional engagement-viral-level interaction",
                },
            },
            "growth_potential": {
                "name": "Growth Potential",
                "description": "Predicted likelihood of future growth based on current trends",
                "range": "0.0 to 1.0",
                "interpretation": {
                    (0.0, 0.3): "Limited potential-may need strategic changes",
                    (0.3, 0.6): "Moderate potential-steady growth expected",
                    (0.6, 0.8): "High potential-strong growth indicators",
                    (0.8, 1.0): "Exceptional potential-prime for investment",
                },
            },
        }

    def explain_momentum_score(self, score: float) -> str:
        """
        Generate explanation for momentum score.

        Args:
            score: Momentum score value (0.0 to 1.0)

        Returns:
            Human-readable explanation of the score
        """
        metric_info = self.metric_definitions["momentum_score"]
        interpretation = self._get_interpretation(score, metric_info["interpretation"])

        return (
            f"Momentum Score: {score:.2f} - {interpretation}. "
            f"This metric {metric_info['description'].lower()} over recent time periods. "
            f"Scores range from {metric_info['range']}."
        )

    def explain_engagement_rate(self, rate: float) -> str:
        """
        Generate explanation for engagement rate.

        Args:
            rate: Engagement rate value (typically 0.0 to 0.1)

        Returns:
            Human-readable explanation of the rate
        """
        metric_info = self.metric_definitions["engagement_rate"]
        interpretation = self._get_interpretation(rate, metric_info["interpretation"])
        percentage = rate * 100

        return (
            f"Engagement Rate: {percentage:.2f}% ({rate:.4f}) - {interpretation}. "
            f"This represents the {metric_info['description'].lower()}. "
            f"Typical range is {metric_info['range']}."
        )

    def explain_growth_potential(self, potential: float) -> str:
        """
        Generate explanation for growth potential.

        Args:
            potential: Growth potential score (0.0 to 1.0)

        Returns:
            Human-readable explanation of the potential
        """
        metric_info = self.metric_definitions["growth_potential"]
        interpretation = self._get_interpretation(potential, metric_info["interpretation"])

        return (
            f"Growth Potential: {potential:.2f} - {interpretation}. "
            f"This score represents the {metric_info['description'].lower()}. "
            f"Scores range from {metric_info['range']}."
        )

    def generate_tooltip_text(self, metric_name: str, value: float) -> str:
        """
        Generate tooltip text for chart elements.

        Args:
            metric_name: Name of the metric
            value: Value of the metric

        Returns:
            Formatted tooltip text
        """
        if metric_name in self.metric_definitions:
            metric_info = self.metric_definitions[metric_name]
            interpretation = self._get_interpretation(value, metric_info["interpretation"])

            return f"{metric_info['name']}: {value:.3f}<br>{interpretation}"
        else:
            # Generic tooltip for unknown metrics
            return f"{metric_name.replace('_', ' ').title()}: {value:.3f}"

    def create_legend_definitions(self, metrics: List[str]) -> Dict[str, str]:
        """
        Create legend definitions for multiple metrics.

        Args:
            metrics: List of metric names to create legends for

        Returns:
            Dictionary mapping metric names to their definitions
        """
        legends = {}

        for metric in metrics:
            if metric in self.metric_definitions:
                metric_info = self.metric_definitions[metric]
                legends[metric] = (
                    f"{metric_info['name']}: {metric_info['description']}. " f"Range: {metric_info['range']}"
                )
            else:
                # Generic definition for unknown metrics
                legends[metric] = f"{metric.replace('_', ' ').title()}: Custom metric"

        return legends

    def _get_interpretation(self, value: float, interpretation_ranges: Dict) -> str:
        """Get interpretation text for a value based on defined ranges."""
        for (min_val, max_val), interpretation in interpretation_ranges.items():
            if min_val <= value < max_val:
                return interpretation

        # Fallback for values outside defined ranges
        return "Value outside typical range"


class NotebookValidator:
    """
    Main validator for notebook outputs with schema validation and error reporting.

    This class orchestrates validation of notebook cell outputs, provides
    metric explanations, and generates comprehensive validation reports.
    """

    def __init__(self):
        """Initialize NotebookValidator with component validators."""
        self.output_validator = OutputValidator()
        self.metric_explainer = MetricExplainer()

    def validate_cell_output(self, cell_output: Any, expected_schema: Dict[str, Any]) -> ValidationResult:
        """
        Validate notebook cell output against expected schema.

        Args:
            cell_output: The actual output from a notebook cell
            expected_schema: Schema definition for validation

        Returns:
            ValidationResult with detailed validation information
        """
        result = ValidationResult(is_valid=True, checked_items=1, passed_items=0)

        try:
            # Validate output type
            expected_type = expected_schema.get("type", "unknown")

            if expected_type == "dataframe":
                if not isinstance(cell_output, pd.DataFrame):
                    result.add_error(f"Expected DataFrame, got {type(cell_output).__name__}")
                    return result

                # Validate DataFrame schema
                df_result = self._validate_dataframe_schema(cell_output, expected_schema)
                result = result.merge(df_result)

            elif expected_type == "series":
                if not isinstance(cell_output, pd.Series):
                    result.add_error(f"Expected Series, got {type(cell_output).__name__}")
                    return result

            elif expected_type == "dict":
                if not isinstance(cell_output, dict):
                    result.add_error(f"Expected dict, got {type(cell_output).__name__}")
                    return result

            elif expected_type == "list":
                if not isinstance(cell_output, list):
                    result.add_error(f"Expected list, got {type(cell_output).__name__}")
                    return result

            if result.is_valid:
                result.passed_items = 1

        except Exception as e:
            result.add_error(f"Validation error: {str(e)}")

        result.metadata = {"expected_schema": expected_schema, "actual_type": type(cell_output).__name__}

        return result

    def _validate_dataframe_schema(self, df: pd.DataFrame, schema: Dict[str, Any]) -> ValidationResult:
        """Validate DataFrame against schema definition."""
        result = ValidationResult(is_valid=True, checked_items=0, passed_items=0)

        # Check minimum rows
        min_rows = schema.get("min_rows", 0)
        if len(df) < min_rows:
            result.add_error(f"DataFrame has {len(df)} rows, minimum {min_rows} required")

        # Check columns
        if "columns" in schema:
            column_result = self.output_validator.validate_data_types(df, schema["columns"])
            result = result.merge(column_result)

        # Check required columns
        if "required_columns" in schema:
            missing_result = self.output_validator.check_missing_values(df, schema["required_columns"])
            result = result.merge(missing_result)

        return result

    def validate_chart_data(self, chart_data: pd.DataFrame) -> ValidationResult:
        """
        Validate data intended for chart generation.

        Args:
            chart_data: DataFrame containing chart data

        Returns:
            ValidationResult with chart data validation details
        """
        result = ValidationResult(is_valid=True, checked_items=1, passed_items=0)

        if chart_data.empty:
            result.add_error("Chart data is empty")
            return result

        # Check for basic chart requirements
        if len(chart_data) < 1:
            result.add_error("Chart data must have at least 1 row")
        elif len(chart_data.columns) < 1:
            result.add_error("Chart data must have at least 1 column")
        else:
            result.passed_items = 1

        # Check for infinite or extremely large values
        numeric_columns = chart_data.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if np.isinf(chart_data[col]).any():
                result.add_warning(f"Column '{col}' contains infinite values")

            if (chart_data[col].abs() > 1e10).any():
                result.add_warning(f"Column '{col}' contains extremely large values")

        result.metadata = {
            "data_shape": chart_data.shape,
            "numeric_columns": list(numeric_columns),
            "data_types": {col: str(dtype) for col, dtype in chart_data.dtypes.items()},
        }

        return result

    def generate_metric_explanations(self, metrics: List[str]) -> Dict[str, str]:
        """
        Generate explanations for a list of metrics.

        Args:
            metrics: List of metric names

        Returns:
            Dictionary mapping metric names to explanations
        """
        return self.metric_explainer.create_legend_definitions(metrics)

    def create_validation_report(self, notebook_path: str) -> ValidationResult:
        """
        Create comprehensive validation report for a notebook file.

        Args:
            notebook_path: Path to the notebook file

        Returns:
            ValidationResult with notebook validation details
        """
        result = ValidationResult(is_valid=True, checked_items=0, passed_items=0)

        try:
            notebook_path = Path(notebook_path)

            if not notebook_path.exists():
                result.add_error(f"Notebook file not found: {notebook_path}")
                return result

            # Load and parse notebook
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook_content = json.load(f)

            # Basic notebook structure validation
            if "cells" not in notebook_content:
                result.add_error("Notebook missing 'cells' key")
                return result

            cells = notebook_content["cells"]
            result.checked_items = len(cells)

            # Validate each cell
            for i, cell in enumerate(cells):
                cell_result = self._validate_cell_structure(cell, i)
                result = result.merge(cell_result)

                if cell_result.is_valid:
                    result.passed_items += 1

            result.metadata = {
                "notebook_path": str(notebook_path),
                "total_cells": len(cells),
                "notebook_format": notebook_content.get("nbformat", "unknown"),
            }

        except json.JSONDecodeError as e:
            result.add_error(f"Invalid JSON in notebook: {str(e)}")
        except Exception as e:
            result.add_error(f"Error reading notebook: {str(e)}")

        return result

    def _validate_cell_structure(self, cell: Dict[str, Any], cell_index: int) -> ValidationResult:
        """Validate individual cell structure."""
        result = ValidationResult(is_valid=True, checked_items=1, passed_items=0)

        # Check required cell fields
        required_fields = ["cell_type"]
        for field in required_fields:  # noqa: F402
            if field not in cell:
                result.add_error(f"Cell {cell_index} missing required field: {field}")

        # Validate cell type
        valid_cell_types = ["code", "markdown", "raw"]
        cell_type = cell.get("cell_type")
        if cell_type not in valid_cell_types:
            result.add_error(f"Cell {cell_index} has invalid cell_type: {cell_type}")

        if result.is_valid:
            result.passed_items = 1

        return result
