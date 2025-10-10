"""
Tests for notebook validation and output explanation system.

This module tests the NotebookValidator, MetricExplainer, and OutputValidator
components that ensure notebook outputs are valid and provide clear explanations.
"""

import json
import os
import tempfile
from typing import Any, Dict, List
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.data_organization.notebook_validator import (
    MetricExplainer,
    NotebookValidator,
    OutputValidationError,
    OutputValidator,
    SchemaValidationError,
    ValidationError,
    ValidationResult,
)


class TestValidationResult:
    """Test ValidationResult data class."""

    def test_validation_result_creation(self):
        """Test creating a ValidationResult."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Minor warning"],
            checked_items=10,
            passed_items=9,
            metadata={"test": "data"},
        )

        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == ["Minor warning"]
        assert result.checked_items == 10
        assert result.passed_items == 9
        assert result.metadata == {"test": "data"}

    def test_add_error(self):
        """Test adding errors to ValidationResult."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0, metadata={})

        result.add_error("Test error")
        assert result.errors == ["Test error"]
        assert result.is_valid is False

    def test_add_warning(self):
        """Test adding warnings to ValidationResult."""
        result = ValidationResult(is_valid=True, errors=[], warnings=[], checked_items=0, passed_items=0, metadata={})

        result.add_warning("Test warning")
        assert result.warnings == ["Test warning"]
        assert result.is_valid is True

    def test_merge_results(self):
        """Test merging two ValidationResults."""
        result1 = ValidationResult(
            is_valid=True,
            errors=["Error 1"],
            warnings=["Warning 1"],
            checked_items=5,
            passed_items=4,
            metadata={"key1": "value1"},
        )

        result2 = ValidationResult(
            is_valid=False,
            errors=["Error 2"],
            warnings=["Warning 2"],
            checked_items=3,
            passed_items=2,
            metadata={"key2": "value2"},
        )

        merged = result1.merge(result2)

        assert merged.is_valid is False  # False if any result is invalid
        assert merged.errors == ["Error 1", "Error 2"]
        assert merged.warnings == ["Warning 1", "Warning 2"]
        assert merged.checked_items == 8
        assert merged.passed_items == 6
        assert merged.metadata == {"key1": "value1", "key2": "value2"}


class TestOutputValidator:
    """Test OutputValidator for data type and range checking."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = OutputValidator()

    def test_validate_score_range_valid(self):
        """Test validating scores within valid range."""
        scores = pd.Series([0.1, 0.5, 0.9, 0.3])
        result = self.validator.validate_score_range(scores, 0.0, 1.0)

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert result.checked_items == 4
        assert result.passed_items == 4

    def test_validate_score_range_invalid(self):
        """Test validating scores outside valid range."""
        scores = pd.Series([0.1, 1.5, -0.2, 0.3])
        result = self.validator.validate_score_range(scores, 0.0, 1.0)

        assert result.is_valid is False
        assert len(result.errors) == 2
        assert "Score 1.5 at index 1 is outside valid range [0.0, 1.0]" in result.errors[0]
        assert "Score -0.2 at index 2 is outside valid range [0.0, 1.0]" in result.errors[1]

    def test_validate_data_types_valid(self):
        """Test validating DataFrame with correct data types."""
        data = pd.DataFrame({"artist_name": ["Artist 1", "Artist 2"], "score": [0.5, 0.8], "count": [100, 200]})

        expected_types = {"artist_name": "object", "score": "float64", "count": "int64"}

        result = self.validator.validate_data_types(data, expected_types)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_data_types_invalid(self):
        """Test validating DataFrame with incorrect data types."""
        data = pd.DataFrame(
            {
                "artist_name": ["Artist 1", "Artist 2"],
                "score": ["0.5", "0.8"],  # Should be float
                "count": [100.5, 200.5],  # Should be int
            }
        )

        expected_types = {"artist_name": "object", "score": "float64", "count": "int64"}

        result = self.validator.validate_data_types(data, expected_types)

        assert result.is_valid is False
        assert len(result.errors) == 2

    def test_check_missing_values_valid(self):
        """Test checking for missing values in required columns."""
        data = pd.DataFrame({"artist_name": ["Artist 1", "Artist 2"], "score": [0.5, 0.8], "optional": [None, "value"]})

        required_columns = ["artist_name", "score"]
        result = self.validator.check_missing_values(data, required_columns)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_check_missing_values_invalid(self):
        """Test checking for missing values with missing required data."""
        data = pd.DataFrame({"artist_name": ["Artist 1", None], "score": [0.5, np.nan], "optional": [None, "value"]})

        required_columns = ["artist_name", "score"]
        result = self.validator.check_missing_values(data, required_columns)

        assert result.is_valid is False
        assert len(result.errors) == 2

    def test_validate_chart_requirements_scatter(self):
        """Test validating data for scatter plot requirements."""
        data = pd.DataFrame({"x": [1, 2, 3, 4], "y": [2, 4, 6, 8], "artist": ["A", "B", "C", "D"]})

        result = self.validator.validate_chart_requirements(data, "scatter")

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_chart_requirements_insufficient_data(self):
        """Test validating data with insufficient rows for charting."""
        data = pd.DataFrame({"x": [1], "y": [2]})

        result = self.validator.validate_chart_requirements(data, "scatter")

        assert result.is_valid is False
        assert "Insufficient data for scatter chart" in result.errors[0]


class TestMetricExplainer:
    """Test MetricExplainer for clear scoring metric definitions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.explainer = MetricExplainer()

    def test_explain_momentum_score(self):
        """Test momentum score explanation."""
        explanation = self.explainer.explain_momentum_score(0.75)

        assert "momentum" in explanation.lower()
        assert "0.75" in explanation
        assert len(explanation) > 20  # Should be descriptive

    def test_explain_engagement_rate(self):
        """Test engagement rate explanation."""
        explanation = self.explainer.explain_engagement_rate(0.05)

        assert "engagement" in explanation.lower()
        assert "5%" in explanation or "0.05" in explanation
        assert len(explanation) > 20

    def test_explain_growth_potential(self):
        """Test growth potential explanation."""
        explanation = self.explainer.explain_growth_potential(0.85)

        assert "growth" in explanation.lower()
        assert "potential" in explanation.lower()
        assert "0.85" in explanation

    def test_generate_tooltip_text(self):
        """Test tooltip text generation."""
        tooltip = self.explainer.generate_tooltip_text("momentum_score", 0.65)

        assert "momentum_score" in tooltip.lower() or "momentum" in tooltip.lower()
        assert "0.65" in tooltip
        assert len(tooltip) > 10

    def test_create_legend_definitions(self):
        """Test legend definitions creation."""
        metrics = ["momentum_score", "engagement_rate", "growth_potential"]
        legends = self.explainer.create_legend_definitions(metrics)

        assert len(legends) == 3
        assert "momentum_score" in legends
        assert "engagement_rate" in legends
        assert "growth_potential" in legends

        for metric, definition in legends.items():
            assert len(definition) > 10
            assert isinstance(definition, str)


class TestNotebookValidator:
    """Test NotebookValidator for schema validation and error reporting."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = NotebookValidator()

    def test_validate_cell_output_valid_dataframe(self):
        """Test validating valid DataFrame cell output."""
        output = pd.DataFrame({"artist_name": ["Artist 1", "Artist 2"], "score": [0.5, 0.8]})

        expected_schema = {"type": "dataframe", "columns": {"artist_name": "object", "score": "float64"}, "min_rows": 1}

        result = self.validator.validate_cell_output(output, expected_schema)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_cell_output_invalid_schema(self):
        """Test validating cell output with schema mismatch."""
        output = pd.DataFrame({"artist_name": ["Artist 1", "Artist 2"], "wrong_column": [0.5, 0.8]})

        expected_schema = {"type": "dataframe", "columns": {"artist_name": "object", "score": "float64"}, "min_rows": 1}

        result = self.validator.validate_cell_output(output, expected_schema)

        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_validate_chart_data_valid(self):
        """Test validating valid chart data."""
        chart_data = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [2, 4, 6, 8, 10], "category": ["A", "B", "A", "B", "A"]})

        result = self.validator.validate_chart_data(chart_data)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_chart_data_empty(self):
        """Test validating empty chart data."""
        chart_data = pd.DataFrame()

        result = self.validator.validate_chart_data(chart_data)

        assert result.is_valid is False
        assert "Chart data is empty" in result.errors[0]

    def test_generate_metric_explanations(self):
        """Test generating metric explanations."""
        metrics = ["momentum_score", "engagement_rate"]
        explanations = self.validator.generate_metric_explanations(metrics)

        assert len(explanations) == 2
        assert "momentum_score" in explanations
        assert "engagement_rate" in explanations

        for metric, explanation in explanations.items():
            assert len(explanation) > 10
            assert isinstance(explanation, str)

    def test_create_validation_report(self):
        """Test creating validation report for notebook."""
        # Create a temporary notebook file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            notebook_content = {
                "cells": [{"cell_type": "code", "source": ["import pandas as pd"], "outputs": []}],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 4,
            }
            json.dump(notebook_content, f)
            temp_path = f.name

        try:
            result = self.validator.create_validation_report(temp_path)

            assert isinstance(result, ValidationResult)
            assert result.metadata.get("notebook_path") == temp_path

        finally:
            os.unlink(temp_path)


class TestValidationErrors:
    """Test custom validation exception classes."""

    def test_validation_error(self):
        """Test ValidationError exception."""
        with pytest.raises(ValidationError) as exc_info:
            raise ValidationError("Test validation error")

        assert str(exc_info.value) == "Test validation error"

    def test_schema_validation_error(self):
        """Test SchemaValidationError exception."""
        with pytest.raises(SchemaValidationError) as exc_info:
            raise SchemaValidationError("Schema mismatch")

        assert str(exc_info.value) == "Schema mismatch"

    def test_output_validation_error(self):
        """Test OutputValidationError exception."""
        with pytest.raises(OutputValidationError) as exc_info:
            raise OutputValidationError("Output validation failed")

        assert str(exc_info.value) == "Output validation failed"


class TestIntegrationScenarios:
    """Test integration scenarios combining all validators."""

    def setup_method(self):
        """Set up test fixtures."""
        self.notebook_validator = NotebookValidator()
        self.output_validator = OutputValidator()
        self.metric_explainer = MetricExplainer()

    def test_complete_validation_workflow(self):
        """Test complete validation workflow for notebook output."""
        # Simulate notebook output data
        output_data = pd.DataFrame(
            {
                "artist_name": ["Taylor Swift", "Ed Sheeran", "Billie Eilish"],
                "momentum_score": [0.85, 0.72, 0.91],
                "engagement_rate": [0.045, 0.038, 0.052],
                "growth_potential": [0.78, 0.65, 0.89],
            }
        )

        # Validate data types
        expected_types = {
            "artist_name": "object",
            "momentum_score": "float64",
            "engagement_rate": "float64",
            "growth_potential": "float64",
        }

        type_result = self.output_validator.validate_data_types(output_data, expected_types)
        assert type_result.is_valid is True

        # Validate score ranges
        momentum_result = self.output_validator.validate_score_range(output_data["momentum_score"], 0.0, 1.0)
        assert momentum_result.is_valid is True

        # Generate explanations
        metrics = ["momentum_score", "engagement_rate", "growth_potential"]
        explanations = self.metric_explainer.create_legend_definitions(metrics)
        assert len(explanations) == 3

        # Validate chart requirements
        chart_result = self.output_validator.validate_chart_requirements(output_data, "scatter")
        assert chart_result.is_valid is True

    def test_validation_with_errors(self):
        """Test validation workflow with various errors."""
        # Create problematic data
        output_data = pd.DataFrame(
            {
                "artist_name": ["Taylor Swift", None, "Billie Eilish"],  # Missing value
                "momentum_score": [0.85, 1.5, -0.1],  # Out of range values
                "engagement_rate": ["high", "medium", "low"],  # Wrong data type
                "growth_potential": [0.78, 0.65, np.nan],  # Missing value
            }
        )

        # Validate data types-should fail
        expected_types = {
            "artist_name": "object",
            "momentum_score": "float64",
            "engagement_rate": "float64",
            "growth_potential": "float64",
        }

        type_result = self.output_validator.validate_data_types(output_data, expected_types)
        assert type_result.is_valid is False

        # Check missing values-should fail
        required_columns = ["artist_name", "growth_potential"]
        missing_result = self.output_validator.check_missing_values(output_data, required_columns)
        assert missing_result.is_valid is False

        # Validate score ranges-should fail for momentum_score
        momentum_result = self.output_validator.validate_score_range(output_data["momentum_score"], 0.0, 1.0)
        assert momentum_result.is_valid is False
