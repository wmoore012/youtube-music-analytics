"""Abstract base class and data models for scoring plugins."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class PluginMetadata:
    """Metadata for scoring plugins."""

    name: str
    version: str
    author: str
    description: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    input_requirements: List[str] = field(default_factory=list)
    output_schema: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> "ValidationResult":
        """Validate plugin metadata."""
        errors = []
        warnings = []

        if not self.name or not isinstance(self.name, str):
            errors.append("Plugin name must be a non - empty string")

        if not self.version or not isinstance(self.version, str):
            errors.append("Plugin version must be a non - empty string")

        if not self.author or not isinstance(self.author, str):
            errors.append("Plugin author must be a non - empty string")

        if not self.description or not isinstance(self.description, str):
            errors.append("Plugin description must be a non - empty string")

        if not isinstance(self.parameters, dict):
            errors.append("Plugin parameters must be a dictionary")

        if not isinstance(self.input_requirements, list):
            errors.append("Input requirements must be a list")

        if not isinstance(self.output_schema, dict):
            errors.append("Output schema must be a dictionary")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=7,
            passed_items=7 - len(errors),
            metadata={"plugin_name": self.name, "plugin_version": self.version},
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            "name": self.name,
            "version": self.version,
            "author": self.author,
            "description": self.description,
            "parameters": self.parameters,
            "input_requirements": self.input_requirements,
            "output_schema": self.output_schema,
        }


@dataclass
class ValidationResult:
    """Result of validation operations."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checked_items: int = 0
    passed_items: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, error: str) -> None:
        """Add an error to the validation result."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning to the validation result."""
        self.warnings.append(warning)

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge another validation result into this one."""
        return ValidationResult(
            is_valid=self.is_valid and other.is_valid,
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
            checked_items=self.checked_items + other.checked_items,
            passed_items=self.passed_items + other.passed_items,
            metadata={**self.metadata, **other.metadata},
        )


@dataclass
class ScoringResult:
    """Result of scoring calculation."""

    algorithm_name: str
    algorithm_version: str
    entity_scores: pd.DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)
    calculation_timestamp: datetime = field(default_factory=datetime.now)
    confidence_metrics: Optional[Dict[str, float]] = None

    def to_database_records(self) -> List[Dict[str, Any]]:
        """Convert scoring result to database records."""
        records = []

        for _, row in self.entity_scores.iterrows():
            record = {
                "algorithm_name": self.algorithm_name,
                "algorithm_version": self.algorithm_version,
                "calculation_timestamp": self.calculation_timestamp,
                "metadata": self.metadata,
            }

            # Add all columns from the DataFrame as fields
            for col, value in row.items():
                record[col] = value

            records.append(record)

        return records

    def export_to_csv(self, file_path: str) -> None:
        """Export scoring results to CSV file."""
        # Add metadata columns to the DataFrame
        export_df = self.entity_scores.copy()
        export_df["algorithm_name"] = self.algorithm_name
        export_df["algorithm_version"] = self.algorithm_version
        export_df["calculation_timestamp"] = self.calculation_timestamp

        export_df.to_csv(file_path, index=False)

    def validate_scores(self) -> ValidationResult:
        """Validate scoring results."""
        errors = []
        warnings = []

        if self.entity_scores.empty:
            errors.append("Entity scores DataFrame is empty")

        # Check for required columns
        required_columns = ["entity_id", "score_value"]
        missing_columns = [col for col in required_columns if col not in self.entity_scores.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Check for null values in score columns
        if "score_value" in self.entity_scores.columns:
            null_scores = self.entity_scores["score_value"].isnull().sum()
            if null_scores > 0:
                warnings.append(f"Found {null_scores} null score values")

        # Check score value ranges (assuming scores should be between 0 and 1)
        if "score_value" in self.entity_scores.columns:
            score_col = self.entity_scores["score_value"]
            if score_col.dtype in ["float64", "float32", "int64", "int32"]:
                out_of_range = ((score_col < 0) | (score_col > 1)).sum()
                if out_of_range > 0:
                    warnings.append(f"Found {out_of_range} scores outside 0 - 1 range")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(self.entity_scores),
            passed_items=len(self.entity_scores) - len(errors),
            metadata={"algorithm_name": self.algorithm_name, "record_count": len(self.entity_scores)},
        )


class ScoringPlugin(ABC):
    """Abstract base class for scoring plugins."""

    def __init__(self):
        """Initialize the scoring plugin."""
        self._metadata: Optional[PluginMetadata] = None
        self._parameters: Dict[str, Any] = {}

    @abstractmethod
    def get_name(self) -> str:
        """Get the plugin name."""
        pass

    @abstractmethod
    def get_version(self) -> str:
        """Get the plugin version."""
        pass

    @abstractmethod
    def get_parameters(self) -> Dict[str, Any]:
        """Get the plugin parameters."""
        pass

    @abstractmethod
    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate scores for the given data."""
        pass

    @abstractmethod
    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data before processing."""
        pass

    def get_metadata(self) -> PluginMetadata:
        """Get plugin metadata."""
        if self._metadata is None:
            self._metadata = PluginMetadata(
                name=self.get_name(),
                version=self.get_version(),
                author="Unknown",
                description="No description provided",
                parameters=self.get_parameters(),
                input_requirements=self.get_input_requirements(),
                output_schema=self.get_output_schema(),
            )
        return self._metadata

    def get_input_requirements(self) -> List[str]:
        """Get list of required input columns."""
        return []

    def get_output_schema(self) -> Dict[str, Any]:
        """Get expected output schema."""
        return {"entity_id": "string", "score_value": "float", "confidence": "float"}

    def set_parameters(self, parameters: Dict[str, Any]) -> None:
        """Set plugin parameters."""
        self._parameters = parameters.copy()

    def validate_parameters(self, parameters: Dict[str, Any]) -> ValidationResult:
        """Validate plugin parameters."""
        # Default implementation - plugins can override for custom validation
        return ValidationResult(
            is_valid=True,
            checked_items=len(parameters),
            passed_items=len(parameters),
            metadata={"parameter_count": len(parameters)},
        )

    def execute(self, data: pd.DataFrame, parameters: Optional[Dict[str, Any]] = None) -> ScoringResult:
        """Execute the scoring plugin with validation."""
        # Set parameters if provided
        if parameters:
            param_validation = self.validate_parameters(parameters)
            if not param_validation.is_valid:
                raise ValueError(f"Invalid parameters: {param_validation.errors}")
            self.set_parameters(parameters)

        # Validate input data
        input_validation = self.validate_input(data)
        if not input_validation.is_valid:
            raise ValueError(f"Invalid input data: {input_validation.errors}")

        # Calculate scores
        try:
            scores_df = self.calculate_scores(data)
        except Exception as e:
            raise RuntimeError(f"Error calculating scores: {str(e)}")

        # Create and validate result
        result = ScoringResult(
            algorithm_name=self.get_name(),
            algorithm_version=self.get_version(),
            entity_scores=scores_df,
            metadata={
                "parameters": self._parameters,
                "input_record_count": len(data),
                "output_record_count": len(scores_df),
            },
        )

        result_validation = result.validate_scores()
        if not result_validation.is_valid:
            raise ValueError(f"Invalid scoring results: {result_validation.errors}")

        return result
