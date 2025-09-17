"""Example scoring plugins demonstrating the plugin system."""

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from .scoring_plugin import ScoringPlugin, ValidationResult


class MomentumScoringPlugin(ScoringPlugin):
    """Example plugin for calculating artist momentum scores."""

    def get_name(self) -> str:
        return "momentum_scorer"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {"time_window_days": 30, "growth_weight": 0.6, "engagement_weight": 0.4, "min_videos_required": 3}

    def get_input_requirements(self) -> List[str]:
        return [
            "artist_name",
            "video_count",
            "total_views",
            "total_likes",
            "total_comments",
            "avg_views_per_video",
            "recent_growth_rate",
        ]

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "entity_id": "string",
            "score_value": "float",
            "confidence": "float",
            "momentum_category": "string",
            "growth_component": "float",
            "engagement_component": "float",
        }

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for momentum scoring."""
        errors = []
        warnings = []

        if data.empty:
            errors.append("Input data is empty")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings, checked_items=1, passed_items=0)

        # Check required columns
        required_columns = self.get_input_requirements()
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Check for null values in critical columns
        critical_columns = ["artist_name", "video_count", "total_views"]
        for col in critical_columns:
            if col in data.columns:
                null_count = data[col].isnull().sum()
                if null_count > 0:
                    warnings.append(f"Found {null_count} null values in {col}")

        # Check data types
        numeric_columns = ["video_count", "total_views", "total_likes", "total_comments"]
        for col in numeric_columns:
            if col in data.columns and not pd.api.types.is_numeric_dtype(data[col]):
                errors.append(f"Column {col} must be numeric")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(data),
            passed_items=len(data) - len(errors),
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate momentum scores for artists."""
        params = self._parameters if hasattr(self, "_parameters") and self._parameters else self.get_parameters()

        results = []

        for _, row in data.iterrows():
            # Skip artists with insufficient data
            if row.get("video_count", 0) < params["min_videos_required"]:
                continue

            # Calculate growth component (0-1 scale)
            growth_rate = row.get("recent_growth_rate", 0)
            growth_component = min(max(growth_rate / 100, 0), 1)  # Normalize to 0-1

            # Calculate engagement component
            total_views = row.get("total_views", 1)
            total_engagement = row.get("total_likes", 0) + row.get("total_comments", 0)
            engagement_rate = total_engagement / total_views if total_views > 0 else 0
            engagement_component = min(engagement_rate * 1000, 1)  # Scale and cap at 1

            # Weighted final score
            momentum_score = (
                growth_component * params["growth_weight"] + engagement_component * params["engagement_weight"]
            )

            # Determine momentum category
            if momentum_score >= 0.8:
                category = "high_momentum"
            elif momentum_score >= 0.5:
                category = "moderate_momentum"
            elif momentum_score >= 0.2:
                category = "low_momentum"
            else:
                category = "declining"

            # Calculate confidence based on data completeness
            confidence = self._calculate_confidence(row)

            results.append(
                {
                    "entity_id": str(row.get("artist_name", f"artist_{len(results)}")),
                    "score_value": round(momentum_score, 4),
                    "confidence": round(confidence, 4),
                    "momentum_category": category,
                    "growth_component": round(growth_component, 4),
                    "engagement_component": round(engagement_component, 4),
                }
            )

        return pd.DataFrame(results)

    def _calculate_confidence(self, row: pd.Series) -> float:
        """Calculate confidence score based on data completeness and quality."""
        confidence_factors = []

        # Video count factor
        video_count = row.get("video_count", 0)
        if video_count >= 10:
            confidence_factors.append(1.0)
        elif video_count >= 5:
            confidence_factors.append(0.8)
        elif video_count >= 3:
            confidence_factors.append(0.6)
        else:
            confidence_factors.append(0.3)

        # Data completeness factor
        required_fields = ["total_views", "total_likes", "total_comments", "recent_growth_rate"]
        complete_fields = sum(1 for field in required_fields if pd.notna(row.get(field)))
        completeness_factor = complete_fields / len(required_fields)
        confidence_factors.append(completeness_factor)

        # Views reliability factor (higher view counts = more reliable)
        total_views = row.get("total_views", 0)
        if total_views >= 100000:
            confidence_factors.append(1.0)
        elif total_views >= 10000:
            confidence_factors.append(0.8)
        elif total_views >= 1000:
            confidence_factors.append(0.6)
        else:
            confidence_factors.append(0.4)

        return np.mean(confidence_factors)


class EngagementScoringPlugin(ScoringPlugin):
    """Example plugin for calculating engagement scores."""

    def get_name(self) -> str:
        return "engagement_scorer"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {"like_weight": 0.4, "comment_weight": 0.6, "view_threshold": 1000, "engagement_rate_cap": 0.1}

    def get_input_requirements(self) -> List[str]:
        return ["entity_id", "total_views", "total_likes", "total_comments", "subscriber_count"]

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for engagement scoring."""
        errors = []
        warnings = []

        if data.empty:
            errors.append("Input data is empty")

        required_columns = self.get_input_requirements()
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Check for negative values
        numeric_columns = ["total_views", "total_likes", "total_comments", "subscriber_count"]
        for col in numeric_columns:
            if col in data.columns:
                negative_count = (data[col] < 0).sum()
                if negative_count > 0:
                    warnings.append(f"Found {negative_count} negative values in {col}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(data),
            passed_items=len(data) - len(errors),
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate engagement scores."""
        params = self._parameters if hasattr(self, "_parameters") and self._parameters else self.get_parameters()

        results = []

        for _, row in data.iterrows():
            total_views = max(row.get("total_views", 0), 1)  # Avoid division by zero
            total_likes = row.get("total_likes", 0)
            total_comments = row.get("total_comments", 0)

            # Calculate engagement rates
            like_rate = total_likes / total_views
            comment_rate = total_comments / total_views

            # Apply caps to prevent outliers
            like_rate = min(like_rate, params["engagement_rate_cap"])
            comment_rate = min(comment_rate, params["engagement_rate_cap"])

            # Weighted engagement score
            engagement_score = like_rate * params["like_weight"] + comment_rate * params["comment_weight"]

            # Normalize to 0-1 scale
            engagement_score = engagement_score / params["engagement_rate_cap"]
            engagement_score = min(max(engagement_score, 0), 1)

            # Calculate confidence
            confidence = 1.0 if total_views >= params["view_threshold"] else 0.5

            results.append(
                {
                    "entity_id": str(row.get("entity_id", f"entity_{len(results)}")),
                    "score_value": round(engagement_score, 4),
                    "confidence": confidence,
                    "like_rate": round(like_rate, 6),
                    "comment_rate": round(comment_rate, 6),
                    "total_engagement": total_likes + total_comments,
                }
            )

        return pd.DataFrame(results)


class SimpleTestPlugin(ScoringPlugin):
    """Simple test plugin for demonstration and testing."""

    def get_name(self) -> str:
        return "simple_test"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {"base_score": 0.5, "random_factor": 0.1}

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Simple validation - just check if data is not empty."""
        if data.empty:
            return ValidationResult(is_valid=False, errors=["Input data is empty"], checked_items=0, passed_items=0)

        return ValidationResult(is_valid=True, checked_items=len(data), passed_items=len(data))

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate simple test scores."""
        params = self._parameters if hasattr(self, "_parameters") and self._parameters else self.get_parameters()

        # Generate simple scores based on row index
        scores = []
        for i, _ in enumerate(data.iterrows()):
            base_score = params["base_score"]
            random_component = np.random.uniform(-params["random_factor"], params["random_factor"])
            score = max(0, min(1, base_score + random_component))

            scores.append({"entity_id": f"entity_{i}", "score_value": round(score, 4), "confidence": 0.8})

        return pd.DataFrame(scores)
