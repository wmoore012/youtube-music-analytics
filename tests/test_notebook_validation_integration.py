"""
Integration tests for notebook validation system.

This module tests the integration of the notebook validation system
with existing analytics components and workflows.
"""

import json
import os
import tempfile
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.data_organization.notebook_validator import (
    MetricExplainer,
    NotebookValidator,
    OutputValidator,
    ValidationResult,
)


class TestNotebookValidationIntegration:
    """Test integration with existing analytics workflows."""

    def setup_method(self):
        """Set up test fixtures."""
        self.notebook_validator = NotebookValidator()
        self.output_validator = OutputValidator()
        self.metric_explainer = MetricExplainer()

    def test_integration_with_analytics_data(self):
        """Test validation of typical analytics data output."""
        # Simulate data that would come from existing analytics
        analytics_output = pd.DataFrame(
            {
                "artist_name": ["Taylor Swift", "Ed Sheeran", "Billie Eilish"],
                "channel_id": ["UC1234567890", "UC0987654321", "UC1122334455"],
                "momentum_score": [0.85, 0.72, 0.91],
                "engagement_rate": [0.045, 0.038, 0.052],
                "growth_potential": [0.78, 0.65, 0.89],
                "view_count": [15000000, 8500000, 12000000],
                "subscriber_count": [50000000, 30000000, 45000000],
                "video_count": [150, 200, 80],
            }
        )

        # Define expected schema for analytics output
        expected_schema = {
            "type": "dataframe",
            "columns": {
                "artist_name": "object",
                "channel_id": "object",
                "momentum_score": "float64",
                "engagement_rate": "float64",
                "growth_potential": "float64",
                "view_count": "int64",
                "subscriber_count": "int64",
                "video_count": "int64",
            },
            "min_rows": 1,
            "required_columns": ["artist_name", "momentum_score", "engagement_rate"],
        }

        # Validate the output
        result = self.notebook_validator.validate_cell_output(analytics_output, expected_schema)

        assert result.is_valid is True
        assert result.passed_items == 1
        assert len(result.errors) == 0

    def test_integration_with_chart_data_preparation(self):
        """Test validation of data prepared for chart generation."""
        # Simulate chart data preparation workflow
        raw_data = pd.DataFrame(
            {
                "artist": ["Artist A", "Artist B", "Artist C", "Artist D"],
                "momentum": [0.8, 0.6, 0.9, 0.7],
                "engagement": [0.04, 0.03, 0.05, 0.035],
                "views": [1000000, 500000, 1500000, 800000],
            }
        )

        # Validate chart requirements for different chart types
        scatter_result = self.output_validator.validate_chart_requirements(raw_data, "scatter")
        assert scatter_result.is_valid is True

        bar_result = self.output_validator.validate_chart_requirements(raw_data, "bar")
        assert bar_result.is_valid is True

        line_result = self.output_validator.validate_chart_requirements(raw_data, "line")
        assert line_result.is_valid is True

        # Validate data types for charting
        expected_types = {"artist": "object", "momentum": "float64", "engagement": "float64", "views": "int64"}

        type_result = self.output_validator.validate_data_types(raw_data, expected_types)
        assert type_result.is_valid is True

    def test_integration_with_scoring_system(self):
        """Test validation of scoring system outputs."""
        # Simulate scoring system output
        scoring_results = pd.DataFrame(
            {
                "entity_id": ["artist_1", "artist_2", "artist_3"],
                "entity_type": ["artist", "artist", "artist"],
                "momentum_score": [0.75, 0.82, 0.68],
                "engagement_score": [0.65, 0.71, 0.59],
                "growth_score": [0.88, 0.76, 0.92],
                "composite_score": [0.76, 0.76, 0.73],
                "confidence_level": [0.95, 0.87, 0.91],
            }
        )

        # Validate all score ranges
        score_columns = ["momentum_score", "engagement_score", "growth_score", "composite_score", "confidence_level"]

        for col in score_columns:
            result = self.output_validator.validate_score_range(scoring_results[col], 0.0, 1.0)
            assert result.is_valid is True, f"Score validation failed for {col}"

        # Validate required columns
        required_columns = ["entity_id", "entity_type", "momentum_score"]
        missing_result = self.output_validator.check_missing_values(scoring_results, required_columns)
        assert missing_result.is_valid is True

    def test_metric_explanations_for_dashboard(self):
        """Test metric explanations for dashboard tooltips and legends."""
        # Simulate dashboard data
        dashboard_metrics = {
            "momentum_score": 0.78,
            "engagement_rate": 0.042,
            "growth_potential": 0.85,
            "viral_coefficient": 0.23,
            "retention_rate": 0.67,
        }

        # Generate explanations for known metrics
        known_metrics = ["momentum_score", "engagement_rate", "growth_potential"]
        explanations = self.metric_explainer.create_legend_definitions(known_metrics)

        assert len(explanations) == 3
        for metric in known_metrics:
            assert metric in explanations
            assert len(explanations[metric]) > 20  # Should be descriptive

        # Generate tooltips for all metrics
        tooltips = {}
        for metric, value in dashboard_metrics.items():
            tooltip = self.metric_explainer.generate_tooltip_text(metric, value)
            tooltips[metric] = tooltip
            assert len(tooltip) > 10  # Should be informative

        # Known metrics should have detailed tooltips
        assert "momentum" in tooltips["momentum_score"].lower()
        assert "engagement" in tooltips["engagement_rate"].lower()
        assert "growth" in tooltips["growth_potential"].lower()

    def test_notebook_cell_validation_workflow(self):
        """Test complete notebook cell validation workflow."""
        # Simulate multiple cell outputs from a notebook
        cell_outputs = [
            # Cell 1: Data loading
            pd.DataFrame({"artist": ["A", "B", "C"], "views": [1000, 2000, 1500]}),
            # Cell 2: Score calculation
            pd.DataFrame(
                {"artist": ["A", "B", "C"], "momentum_score": [0.7, 0.8, 0.6], "engagement_rate": [0.03, 0.04, 0.025]}
            ),
            # Cell 3: Summary statistics
            {"total_artists": 3, "avg_momentum": 0.7, "max_engagement": 0.04},
        ]

        # Define schemas for each cell
        schemas = [
            {"type": "dataframe", "columns": {"artist": "object", "views": "int64"}, "min_rows": 1},
            {
                "type": "dataframe",
                "columns": {"artist": "object", "momentum_score": "float64", "engagement_rate": "float64"},
                "min_rows": 1,
            },
            {"type": "dict"},
        ]

        # Validate each cell output
        all_valid = True
        for i, (output, schema) in enumerate(zip(cell_outputs, schemas)):
            result = self.notebook_validator.validate_cell_output(output, schema)
            if not result.is_valid:
                all_valid = False
                print(f"Cell {i} validation failed: {result.errors}")

        assert all_valid is True

    def test_error_handling_and_reporting(self):
        """Test comprehensive error handling and reporting."""
        # Create problematic data
        problematic_data = pd.DataFrame(
            {
                "artist_name": ["Artist 1", None, "Artist 3"],  # Missing value
                "momentum_score": [0.5, 1.5, -0.1],  # Out of range
                "engagement_rate": ["high", 0.03, 0.04],  # Wrong type
                "view_count": [1000, 2000, np.nan],  # Missing numeric value
            }
        )

        # Collect all validation errors
        all_errors = []
        all_warnings = []

        # Data type validation
        expected_types = {
            "artist_name": "object",
            "momentum_score": "float64",
            "engagement_rate": "float64",
            "view_count": "int64",
        }

        type_result = self.output_validator.validate_data_types(problematic_data, expected_types)
        all_errors.extend(type_result.errors)
        all_warnings.extend(type_result.warnings)

        # Score range validation
        try:
            # This will fail due to string in momentum_score
            range_result = self.output_validator.validate_score_range(problematic_data["momentum_score"], 0.0, 1.0)
            all_errors.extend(range_result.errors)
            all_warnings.extend(range_result.warnings)
        except Exception as e:
            all_errors.append(f"Score range validation failed: {str(e)}")

        # Missing values check
        required_columns = ["artist_name", "view_count"]
        missing_result = self.output_validator.check_missing_values(problematic_data, required_columns)
        all_errors.extend(missing_result.errors)
        all_warnings.extend(missing_result.warnings)

        # Should have collected multiple errors
        assert len(all_errors) > 0
        print(f"Collected {len(all_errors)} errors and {len(all_warnings)} warnings")

    def test_performance_with_large_datasets(self):
        """Test validation performance with larger datasets."""
        # Create a larger dataset
        n_rows = 10000
        large_dataset = pd.DataFrame(
            {
                "artist_id": [f"artist_{i}" for i in range(n_rows)],
                "momentum_score": np.random.uniform(0, 1, n_rows),
                "engagement_rate": np.random.uniform(0, 0.1, n_rows),
                "growth_potential": np.random.uniform(0, 1, n_rows),
                "view_count": np.random.randint(1000, 10000000, n_rows),
            }
        )

        # Time the validation operations
        import time

        start_time = time.time()

        # Data type validation
        expected_types = {
            "artist_id": "object",
            "momentum_score": "float64",
            "engagement_rate": "float64",
            "growth_potential": "float64",
            "view_count": "int64",
        }

        type_result = self.output_validator.validate_data_types(large_dataset, expected_types)

        # Score range validations
        momentum_result = self.output_validator.validate_score_range(large_dataset["momentum_score"], 0.0, 1.0)

        engagement_result = self.output_validator.validate_score_range(large_dataset["engagement_rate"], 0.0, 0.2)

        # Chart requirements
        chart_result = self.output_validator.validate_chart_requirements(large_dataset, "scatter")

        end_time = time.time()
        validation_time = end_time-start_time

        # Validation should complete in reasonable time (< 5 seconds for 10k rows)
        assert validation_time < 5.0, f"Validation took too long: {validation_time:.2f} seconds"

        # All validations should pass
        assert type_result.is_valid is True
        assert momentum_result.is_valid is True
        assert engagement_result.is_valid is True
        assert chart_result.is_valid is True

        print(f"Validated {n_rows} rows in {validation_time:.2f} seconds")

    def test_integration_with_existing_notebooks(self):
        """Test integration with existing notebook structure."""
        # Create a mock notebook that resembles existing analytics notebooks
        notebook_content = {
            "cells": [
                {"cell_type": "markdown", "source": ["# Music Analytics Dashboard"]},
                {
                    "cell_type": "code",
                    "source": [
                        "import pandas as pd",
                        "from youtubeviz.utils import filter_artists",
                        "from youtubeviz.charts import views_over_time_plotly",
                    ],
                    "outputs": [],
                },
                {
                    "cell_type": "code",
                    "source": ["# Load analytics data", "df = pd.read_sql('SELECT * FROM artist_analytics', engine)"],
                    "outputs": [
                        {
                            "output_type": "execute_result",
                            "data": {"text / html": ["<div>DataFrame with 100 rows</div>"]},
                        }
                    ],
                },
                {
                    "cell_type": "code",
                    "source": ["# Generate scoring metrics", "scoring_results = calculate_momentum_scores(df)"],
                    "outputs": [],
                },
            ],
            "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Create temporary notebook file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            json.dump(notebook_content, f)
            temp_path = f.name

        try:
            # Validate notebook structure
            result = self.notebook_validator.create_validation_report(temp_path)

            assert result.is_valid is True
            assert result.metadata["total_cells"] == 4
            assert result.metadata["notebook_format"] == 4

            # Should validate all cells successfully
            assert result.passed_items == result.checked_items

        finally:
            os.unlink(temp_path)


class TestValidationResultMerging:
    """Test merging of validation results from multiple sources."""

    def test_merge_multiple_validation_results(self):
        """Test merging validation results from different validators."""
        # Create multiple validation results
        result1 = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Minor warning 1"],
            checked_items=5,
            passed_items=5,
            metadata={"validator_type": "type_check", "step": 1},
        )

        result2 = ValidationResult(
            is_valid=False,
            errors=["Range error"],
            warnings=["Minor warning 2"],
            checked_items=3,
            passed_items=2,
            metadata={"validator_type": "range_check", "step": 2},
        )

        result3 = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            checked_items=2,
            passed_items=2,
            metadata={"validator_type": "missing_check", "step": 3},
        )

        # Merge all results
        merged = result1.merge(result2).merge(result3)

        # Merged result should reflect combined state
        assert merged.is_valid is False  # False because result2 failed
        assert len(merged.errors) == 1
        assert len(merged.warnings) == 2
        assert merged.checked_items == 10  # 5 + 3 + 2
        assert merged.passed_items == 9  # 5 + 2 + 2
        assert len(merged.metadata) == 2  # All unique metadata keys merged
        assert "validator_type" in merged.metadata
        assert "step" in merged.metadata
        # Should have all validator types in a list
        assert merged.metadata["validator_type"] == ["type_check", "range_check", "missing_check"]
        assert merged.metadata["step"] == [1, 2, 3]

    def test_comprehensive_validation_pipeline(self):
        """Test a complete validation pipeline with multiple steps."""
        # Simulate analytics data
        data = pd.DataFrame(
            {
                "artist_name": ["Taylor Swift", "Ed Sheeran", "Billie Eilish"],
                "momentum_score": [0.85, 0.72, 0.91],
                "engagement_rate": [0.045, 0.038, 0.052],
                "growth_potential": [0.78, 0.65, 0.89],
            }
        )

        validator = OutputValidator()

        # Step 1: Data type validation
        type_result = validator.validate_data_types(
            data,
            {
                "artist_name": "object",
                "momentum_score": "float64",
                "engagement_rate": "float64",
                "growth_potential": "float64",
            },
        )

        # Step 2: Score range validation
        momentum_result = validator.validate_score_range(data["momentum_score"], 0.0, 1.0)
        engagement_result = validator.validate_score_range(data["engagement_rate"], 0.0, 0.2)
        growth_result = validator.validate_score_range(data["growth_potential"], 0.0, 1.0)

        # Step 3: Missing values check
        missing_result = validator.check_missing_values(data, ["artist_name", "momentum_score"])

        # Step 4: Chart requirements
        chart_result = validator.validate_chart_requirements(data, "scatter")

        # Merge all results
        final_result = (
            type_result.merge(momentum_result)
            .merge(engagement_result)
            .merge(growth_result)
            .merge(missing_result)
            .merge(chart_result)
        )

        # Final result should be valid
        assert final_result.is_valid is True
        assert len(final_result.errors) == 0
        assert final_result.checked_items > 0
        assert final_result.passed_items > 0

        # Should have metadata from all validation steps
        assert len(final_result.metadata) > 0
