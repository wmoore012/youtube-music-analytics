"""Tests for scoring plugin base classes and data models."""

from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from src.data_organization.scoring_plugin import PluginMetadata, ScoringPlugin, ScoringResult, ValidationResult


class TestPluginMetadata:
    """Test PluginMetadata class."""

    def test_valid_metadata_creation(self):
        """Test creating valid plugin metadata."""
        metadata = PluginMetadata(
            name="test_plugin",
            version="1.0.0",
            author="Test Author",
            description="Test plugin description",
            parameters={"param1": "value1"},
            input_requirements=["column1", "column2"],
            output_schema={"score": "float"},
        )

        assert metadata.name == "test_plugin"
        assert metadata.version == "1.0.0"
        assert metadata.author == "Test Author"
        assert metadata.description == "Test plugin description"
        assert metadata.parameters == {"param1": "value1"}
        assert metadata.input_requirements == ["column1", "column2"]
        assert metadata.output_schema == {"score": "float"}

    def test_metadata_validation_success(self):
        """Test successful metadata validation."""
        metadata = PluginMetadata(
            name="test_plugin", version="1.0.0", author="Test Author", description="Test plugin description"
        )

        result = metadata.validate()
        assert result.is_valid
        assert len(result.errors) == 0
        assert result.checked_items == 7
        assert result.passed_items == 7

    def test_metadata_validation_failures(self):
        """Test metadata validation with invalid data."""
        metadata = PluginMetadata(
            name="",  # Invalid empty name
            version=None,  # Invalid None version
            author="Test Author",
            description="Test plugin description",
            parameters="not_a_dict",  # Invalid parameters type
            input_requirements="not_a_list",  # Invalid input_requirements type
            output_schema="not_a_dict",  # Invalid output_schema type
        )

        result = metadata.validate()
        assert not result.is_valid
        assert len(result.errors) == 5
        assert "Plugin name must be a non-empty string" in result.errors
        assert "Plugin version must be a non-empty string" in result.errors
        assert "Plugin parameters must be a dictionary" in result.errors
        assert "Input requirements must be a list" in result.errors
        assert "Output schema must be a dictionary" in result.errors

    def test_metadata_to_dict(self):
        """Test converting metadata to dictionary."""
        metadata = PluginMetadata(
            name="test_plugin",
            version="1.0.0",
            author="Test Author",
            description="Test plugin description",
            parameters={"param1": "value1"},
            input_requirements=["column1"],
            output_schema={"score": "float"},
        )

        result_dict = metadata.to_dict()
        expected = {
            "name": "test_plugin",
            "version": "1.0.0",
            "author": "Test Author",
            "description": "Test plugin description",
            "parameters": {"param1": "value1"},
            "input_requirements": ["column1"],
            "output_schema": {"score": "float"},
        }

        assert result_dict == expected


class TestValidationResult:
    """Test ValidationResult class."""

    def test_validation_result_creation(self):
        """Test creating validation result."""
        result = ValidationResult(
            is_valid=True,
            errors=["error1"],
            warnings=["warning1"],
            checked_items=10,
            passed_items=9,
            metadata={"key": "value"},
        )

        assert result.is_valid
        assert result.errors == ["error1"]
        assert result.warnings == ["warning1"]
        assert result.checked_items == 10
        assert result.passed_items == 9
        assert result.metadata == {"key": "value"}

    def test_add_error(self):
        """Test adding error to validation result."""
        result = ValidationResult(is_valid=True)
        result.add_error("test error")

        assert not result.is_valid
        assert "test error" in result.errors

    def test_add_warning(self):
        """Test adding warning to validation result."""
        result = ValidationResult(is_valid=True)
        result.add_warning("test warning")

        assert result.is_valid  # Warnings don't affect validity
        assert "test warning" in result.warnings

    def test_merge_validation_results(self):
        """Test merging validation results."""
        result1 = ValidationResult(
            is_valid=True,
            errors=["error1"],
            warnings=["warning1"],
            checked_items=5,
            passed_items=4,
            metadata={"key1": "value1"},
        )

        result2 = ValidationResult(
            is_valid=False,
            errors=["error2"],
            warnings=["warning2"],
            checked_items=3,
            passed_items=2,
            metadata={"key2": "value2"},
        )

        merged = result1.merge(result2)

        assert not merged.is_valid  # False if any result is invalid
        assert merged.errors == ["error1", "error2"]
        assert merged.warnings == ["warning1", "warning2"]
        assert merged.checked_items == 8
        assert merged.passed_items == 6
        assert merged.metadata == {"key1": "value1", "key2": "value2"}


class TestScoringResult:
    """Test ScoringResult class."""

    def test_scoring_result_creation(self):
        """Test creating scoring result."""
        scores_df = pd.DataFrame(
            {"entity_id": ["entity1", "entity2"], "score_value": [0.8, 0.6], "confidence": [0.9, 0.7]}
        )

        result = ScoringResult(
            algorithm_name="test_algorithm",
            algorithm_version="1.0.0",
            entity_scores=scores_df,
            metadata={"param1": "value1"},
            confidence_metrics={"avg_confidence": 0.8},
        )

        assert result.algorithm_name == "test_algorithm"
        assert result.algorithm_version == "1.0.0"
        assert len(result.entity_scores) == 2
        assert result.metadata == {"param1": "value1"}
        assert result.confidence_metrics == {"avg_confidence": 0.8}
        assert isinstance(result.calculation_timestamp, datetime)

    def test_to_database_records(self):
        """Test converting scoring result to database records."""
        scores_df = pd.DataFrame({"entity_id": ["entity1", "entity2"], "score_value": [0.8, 0.6]})

        result = ScoringResult(
            algorithm_name="test_algorithm",
            algorithm_version="1.0.0",
            entity_scores=scores_df,
            metadata={"param1": "value1"},
        )

        records = result.to_database_records()

        assert len(records) == 2
        assert records[0]["algorithm_name"] == "test_algorithm"
        assert records[0]["algorithm_version"] == "1.0.0"
        assert records[0]["entity_id"] == "entity1"
        assert records[0]["score_value"] == 0.8
        assert records[0]["metadata"] == {"param1": "value1"}

    @patch("pandas.DataFrame.to_csv")
    def test_export_to_csv(self, mock_to_csv):
        """Test exporting scoring result to CSV."""
        scores_df = pd.DataFrame({"entity_id": ["entity1", "entity2"], "score_value": [0.8, 0.6]})

        result = ScoringResult(algorithm_name="test_algorithm", algorithm_version="1.0.0", entity_scores=scores_df)

        result.export_to_csv("test.csv")

        mock_to_csv.assert_called_once_with("test.csv", index=False)

    def test_validate_scores_success(self):
        """Test successful score validation."""
        scores_df = pd.DataFrame({"entity_id": ["entity1", "entity2"], "score_value": [0.8, 0.6]})

        result = ScoringResult(algorithm_name="test_algorithm", algorithm_version="1.0.0", entity_scores=scores_df)

        validation = result.validate_scores()
        assert validation.is_valid
        assert len(validation.errors) == 0

    def test_validate_scores_failures(self):
        """Test score validation with invalid data."""
        # Empty DataFrame
        empty_result = ScoringResult(
            algorithm_name="test_algorithm", algorithm_version="1.0.0", entity_scores=pd.DataFrame()
        )

        validation = empty_result.validate_scores()
        assert not validation.is_valid
        assert "Entity scores DataFrame is empty" in validation.errors

        # Missing required columns
        missing_cols_df = pd.DataFrame({"wrong_column": ["entity1", "entity2"]})

        missing_result = ScoringResult(
            algorithm_name="test_algorithm", algorithm_version="1.0.0", entity_scores=missing_cols_df
        )

        validation = missing_result.validate_scores()
        assert not validation.is_valid
        assert any("Missing required columns" in error for error in validation.errors)

    def test_validate_scores_warnings(self):
        """Test score validation with warning conditions."""
        # Scores with null values and out of range
        scores_df = pd.DataFrame(
            {"entity_id": ["entity1", "entity2", "entity3"], "score_value": [0.8, None, 1.5]}  # null and out of range
        )

        result = ScoringResult(algorithm_name="test_algorithm", algorithm_version="1.0.0", entity_scores=scores_df)

        validation = result.validate_scores()
        assert validation.is_valid  # Warnings don't make it invalid
        assert len(validation.warnings) >= 1
        assert any("null score values" in warning for warning in validation.warnings)
        assert any("scores outside 0-1 range" in warning for warning in validation.warnings)


class MockScoringPlugin(ScoringPlugin):
    """Mock scoring plugin for testing."""

    def get_name(self) -> str:
        return "mock_plugin"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> dict:
        return {"param1": "default_value"}

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {"entity_id": data.index.astype(str), "score_value": [0.5] * len(data), "confidence": [0.8] * len(data)}
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        errors = []
        if data.empty:
            errors.append("Input data is empty")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, checked_items=1, passed_items=1 - len(errors))


class TestScoringPlugin:
    """Test ScoringPlugin abstract base class."""

    def test_plugin_metadata_generation(self):
        """Test automatic metadata generation."""
        plugin = MockScoringPlugin()
        metadata = plugin.get_metadata()

        assert metadata.name == "mock_plugin"
        assert metadata.version == "1.0.0"
        assert metadata.parameters == {"param1": "default_value"}

    def test_plugin_parameter_management(self):
        """Test plugin parameter setting."""
        plugin = MockScoringPlugin()

        new_params = {"param1": "new_value", "param2": "value2"}
        plugin.set_parameters(new_params)

        assert plugin._parameters == new_params

    def test_plugin_execution_success(self):
        """Test successful plugin execution."""
        plugin = MockScoringPlugin()

        test_data = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

        result = plugin.execute(test_data)

        assert isinstance(result, ScoringResult)
        assert result.algorithm_name == "mock_plugin"
        assert result.algorithm_version == "1.0.0"
        assert len(result.entity_scores) == 3

    def test_plugin_execution_with_parameters(self):
        """Test plugin execution with parameters."""
        plugin = MockScoringPlugin()

        test_data = pd.DataFrame({"column1": [1, 2, 3]})

        parameters = {"param1": "test_value"}
        result = plugin.execute(test_data, parameters)

        assert result.metadata["parameters"] == parameters

    def test_plugin_execution_input_validation_failure(self):
        """Test plugin execution with invalid input."""
        plugin = MockScoringPlugin()

        # Empty DataFrame should fail validation
        empty_data = pd.DataFrame()

        with pytest.raises(ValueError, match="Invalid input data"):
            plugin.execute(empty_data)

    def test_plugin_parameter_validation(self):
        """Test plugin parameter validation."""
        plugin = MockScoringPlugin()

        # Default implementation should pass any parameters
        result = plugin.validate_parameters({"any": "parameters"})
        assert result.is_valid

    def test_plugin_input_requirements(self):
        """Test plugin input requirements."""
        plugin = MockScoringPlugin()

        # Default implementation returns empty list
        requirements = plugin.get_input_requirements()
        assert requirements == []

    def test_plugin_output_schema(self):
        """Test plugin output schema."""
        plugin = MockScoringPlugin()

        schema = plugin.get_output_schema()
        expected_schema = {"entity_id": "string", "score_value": "float", "confidence": "float"}
        assert schema == expected_schema
