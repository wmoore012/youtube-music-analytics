"""YouTube analytics scoring plugins that work with existing database tables."""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd


from .scoring_plugin import ScoringPlugin, ValidationResult


class ArtistMomentumScoringPlugin(ScoringPlugin):
    """
    Scoring plugin for calculating artist momentum using YouTube videos and metrics.

    Uses existing database tables:
    - youtube_videos: video metadata and publication dates
    - youtube_metrics: view counts, likes, comments over time
    - songs: artist name mapping (if available)
    """

    def get_name(self) -> str:
        return "artist_momentum_scorer"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {
            "momentum_window_days": 30,  # Days to look back for momentum calculation
            "view_growth_weight": 0.4,  # Weight for view growth component
            "engagement_weight": 0.3,  # Weight for engagement rate component
            "consistency_weight": 0.3,  # Weight for posting consistency
            "min_videos_required": 2,  # Minimum videos needed for reliable scoring
            "growth_threshold": 0.1,  # Minimum growth rate to be considered "growing"
        }

    def get_input_requirements(self) -> List[str]:
        return [
            "artist_name",
            "video_id",
            "published_at",
            "view_count",
            "like_count",
            "comment_count",
            "channel_title",
            "metrics_date",
        ]

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "entity_id": "string",
            "score_value": "float",
            "confidence": "float",
            "momentum_category": "string",
            "view_growth_rate": "float",
            "engagement_rate": "float",
            "posting_consistency": "float",
            "total_videos": "int",
            "recent_videos": "int",
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
        critical_columns = ["artist_name", "video_id", "view_count"]
        for col in critical_columns:
            if col in data.columns:
                null_count = data[col].isnull().sum()
                if null_count > 0:
                    warnings.append(f"Found {null_count} null values in {col}")

        # Check data types
        numeric_columns = ["view_count", "like_count", "comment_count"]
        for col in numeric_columns:
            if col in data.columns and not pd.api.types.is_numeric_dtype(data[col]):
                errors.append(f"Column {col} must be numeric")

        # Check date columns
        date_columns = ["published_at", "metrics_date"]
        for col in date_columns:
            if col in data.columns:
                try:
                    pd.to_datetime(data[col])
                except Exception:
                    errors.append(f"Column {col} must be convertible to datetime")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(data),
            passed_items=len(data) - len(errors),
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate momentum scores for artists using YouTube data."""
        # Merge custom parameters with defaults
        default_params = self.get_parameters()
        if hasattr(self, "_parameters") and self._parameters:
            params = {**default_params, **self._parameters}
        else:
            params = default_params

        # Ensure datetime columns
        data = data.copy()
        if "published_at" in data.columns:
            data["published_at"] = pd.to_datetime(data["published_at"])
        if "metrics_date" in data.columns:
            data["metrics_date"] = pd.to_datetime(data["metrics_date"])

        results = []
        cutoff_date = datetime.now() - timedelta(days=params["momentum_window_days"])

        for artist_name, artist_data in data.groupby("artist_name"):
            # Skip invalid artist names
            if (
                pd.isna(artist_name)
                or artist_name is None
                or str(artist_name).strip() == ""
                or str(artist_name).lower() == "none"
            ):
                continue

            # Skip artists with insufficient data
            if len(artist_data) < params["min_videos_required"]:
                continue

            # Calculate view growth rate
            view_growth_rate = self._calculate_view_growth_rate(artist_data, cutoff_date)

            # Calculate engagement rate
            engagement_rate = self._calculate_engagement_rate(artist_data)

            # Calculate posting consistency
            posting_consistency = self._calculate_posting_consistency(artist_data, params["momentum_window_days"])

            # Calculate weighted momentum score
            momentum_score = (
                view_growth_rate * params["view_growth_weight"]
                + engagement_rate * params["engagement_weight"]
                + posting_consistency * params["consistency_weight"]
            )

            # Determine momentum category
            momentum_category = self._categorize_momentum(momentum_score, view_growth_rate, params["growth_threshold"])

            # Calculate confidence based on data quality
            confidence = self._calculate_confidence(artist_data, params["min_videos_required"])

            # Count videos
            total_videos = artist_data["video_id"].nunique()
            recent_videos = artist_data[artist_data["published_at"] >= cutoff_date]["video_id"].nunique()

            results.append(
                {
                    "entity_id": str(artist_name),
                    "score_value": round(momentum_score, 4),
                    "confidence": round(confidence, 4),
                    "momentum_category": momentum_category,
                    "view_growth_rate": round(view_growth_rate, 4),
                    "engagement_rate": round(engagement_rate, 4),
                    "posting_consistency": round(posting_consistency, 4),
                    "total_videos": total_videos,
                    "recent_videos": recent_videos,
                }
            )

        return pd.DataFrame(results)

    def _calculate_view_growth_rate(self, artist_data: pd.DataFrame, cutoff_date: datetime) -> float:
        """Calculate view growth rate for an artist."""
        # Sort by date to get proper time series
        sorted_data = artist_data.sort_values("published_at")

        if len(sorted_data) < 2:
            return 0.0

        # Calculate trend using total views over time
        # Use cumulative views to see growth trajectory
        sorted_data = sorted_data.copy()
        sorted_data["cumulative_views"] = sorted_data["view_count"].cumsum()

        # Get first and last periods for growth calculation
        first_quarter = sorted_data.head(max(1, len(sorted_data) // 4))
        last_quarter = sorted_data.tail(max(1, len(sorted_data) // 4))

        first_avg = first_quarter["view_count"].mean()
        last_avg = last_quarter["view_count"].mean()

        if first_avg == 0:
            return 1.0 if last_avg > 0 else 0.0

        # Calculate growth rate and normalize to 0-1 scale
        growth_rate = (last_avg-first_avg) / first_avg

        # Normalize: -50% to +200% growth maps to 0-1 scale
        normalized_growth = (growth_rate + 0.5) / 2.5  # Maps -0.5 to 2.0 range to 0-1
        return min(max(normalized_growth, 0.0), 1.0)

    def _calculate_engagement_rate(self, artist_data: pd.DataFrame) -> float:
        """Calculate engagement rate for an artist."""
        total_views = artist_data["view_count"].sum()
        total_likes = artist_data["like_count"].fillna(0).sum()
        total_comments = artist_data["comment_count"].fillna(0).sum()

        if total_views == 0:
            return 0.0

        # Calculate raw engagement rate (likes + comments / views)
        raw_engagement_rate = (total_likes + total_comments) / total_views

        # Normalize to 0-1 scale using realistic engagement benchmarks
        # Typical YouTube engagement rates: 1-3% is good, 3-6% is excellent
        # We'll use 5% as the maximum for normalization
        max_expected_engagement = 0.05  # 5%
        normalized_rate = min(raw_engagement_rate / max_expected_engagement, 1.0)

        return normalized_rate

    def _calculate_posting_consistency(self, artist_data: pd.DataFrame, window_days: int) -> float:
        """Calculate posting consistency score."""
        if "published_at" not in artist_data.columns:
            return 0.5  # Neutral score if no date data

        # Get unique videos and their publication dates
        videos = artist_data.drop_duplicates("video_id")[["video_id", "published_at"]].sort_values("published_at")

        if len(videos) < 2:
            return 0.3  # Low consistency for single video

        # Calculate days between posts
        videos["days_since_last"] = videos["published_at"].diff().dt.days

        # Remove first video (no previous post)
        gaps = videos["days_since_last"].dropna()

        if len(gaps) == 0:
            return 0.3

        # Consistency is higher when gaps are more regular (lower std dev relative to mean)
        mean_gap = gaps.mean()
        std_gap = gaps.std()

        if mean_gap == 0:
            return 1.0

        # Coefficient of variation (lower is more consistent)
        cv = std_gap / mean_gap if mean_gap > 0 else 1.0
        consistency = max(0.0, 1.0-cv)

        return min(consistency, 1.0)

    def _categorize_momentum(self, momentum_score: float, growth_rate: float, growth_threshold: float) -> str:
        """Categorize momentum based on score and growth rate."""
        # Use more realistic thresholds for momentum categories
        if momentum_score >= 0.75:
            return "high_momentum"
        elif momentum_score >= 0.6:
            return "moderate_momentum"
        elif momentum_score >= 0.4:
            return "stable"
        elif momentum_score >= 0.25:
            return "low_momentum"
        else:
            return "declining"

    def _calculate_confidence(self, artist_data: pd.DataFrame, min_videos: int) -> float:
        """Calculate confidence score based on data quality and completeness."""
        confidence_factors = []

        # Video count factor
        video_count = artist_data["video_id"].nunique()
        if video_count >= min_videos * 3:
            confidence_factors.append(1.0)
        elif video_count >= min_videos * 2:
            confidence_factors.append(0.8)
        elif video_count >= min_videos:
            confidence_factors.append(0.6)
        else:
            confidence_factors.append(0.3)

        # Data completeness factor
        required_fields = ["view_count", "like_count", "comment_count"]
        completeness = sum(1 for field in required_fields if artist_data[field].notna().any()) / len(required_fields)
        confidence_factors.append(completeness)

        # Recency factor (more recent data = higher confidence)
        if "published_at" in artist_data.columns:
            latest_video = artist_data["published_at"].max()
            days_since_latest = (datetime.now() - latest_video).days
            if days_since_latest <= 30:
                confidence_factors.append(1.0)
            elif days_since_latest <= 90:
                confidence_factors.append(0.8)
            elif days_since_latest <= 180:
                confidence_factors.append(0.6)
            else:
                confidence_factors.append(0.4)
        else:
            confidence_factors.append(0.5)

        return np.mean(confidence_factors)


class EngagementScoringPlugin(ScoringPlugin):
    """
    Scoring plugin for calculating engagement scores using comments and sentiment data.

    Uses existing database tables:
    - youtube_videos: video metadata
    - youtube_metrics: engagement metrics (likes, comments)
    - youtube_sentiment_summary: sentiment analysis results
    """

    def get_name(self) -> str:
        return "engagement_scorer"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {
            "like_weight": 0.3,  # Weight for like rate in engagement score
            "comment_weight": 0.4,  # Weight for comment rate in engagement score
            "sentiment_weight": 0.3,  # Weight for sentiment in engagement score
            "min_views_threshold": 100,  # Minimum views for reliable engagement calculation
            "sentiment_boost_factor": 0.2,  # How much positive sentiment boosts score
        }

    def get_input_requirements(self) -> List[str]:
        return ["video_id", "view_count", "like_count", "comment_count", "avg_sentiment", "sentiment_magnitude"]

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "entity_id": "string",
            "score_value": "float",
            "confidence": "float",
            "engagement_rate": "float",
            "like_rate": "float",
            "comment_rate": "float",
            "sentiment_boost": "float",
            "total_engagement": "int",
        }

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for engagement scoring."""
        errors = []
        warnings = []

        if data.empty:
            errors.append("Input data is empty")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings, checked_items=1, passed_items=0)

        # Check required columns
        required_columns = ["video_id", "view_count", "like_count", "comment_count"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        # Check for negative values
        numeric_columns = ["view_count", "like_count", "comment_count"]
        for col in numeric_columns:
            if col in data.columns:
                negative_count = (data[col] < 0).sum()
                if negative_count > 0:
                    warnings.append(f"Found {negative_count} negative values in {col}")

        # Check sentiment columns if present
        sentiment_columns = ["avg_sentiment", "sentiment_magnitude"]
        for col in sentiment_columns:
            if col in data.columns:
                # Sentiment should be between -1 and 1
                out_of_range = ((data[col] < -1) | (data[col] > 1)).sum()
                if out_of_range > 0:
                    warnings.append(f"Found {out_of_range} sentiment values outside -1 to 1 range in {col}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(data),
            passed_items=len(data) - len(errors),
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate engagement scores for videos."""
        # Merge custom parameters with defaults
        default_params = self.get_parameters()
        if hasattr(self, "_parameters") and self._parameters:
            params = {**default_params, **self._parameters}
        else:
            params = default_params

        results = []

        for _, row in data.iterrows():
            video_id = str(row.get("video_id", f"video_{len(results)}"))
            view_count = max(row.get("view_count", 0), 1)  # Avoid division by zero
            like_count = row.get("like_count", 0)
            comment_count = row.get("comment_count", 0)
            avg_sentiment = row.get("avg_sentiment", 0.0)
            sentiment_magnitude = row.get("sentiment_magnitude", 0.0)

            # Calculate engagement rates
            like_rate = like_count / view_count
            comment_rate = comment_count / view_count

            # Base engagement rate (likes + comments per view)
            base_engagement_rate = like_rate + comment_rate

            # Calculate sentiment boost / penalty
            sentiment_boost = 0.0
            if pd.notna(avg_sentiment) and pd.notna(sentiment_magnitude):
                # Positive sentiment with high magnitude boosts engagement score
                sentiment_boost = avg_sentiment * sentiment_magnitude * params["sentiment_boost_factor"]

            # Weighted engagement score
            engagement_score = (
                like_rate * params["like_weight"]
                + comment_rate * params["comment_weight"]
                + sentiment_boost * params["sentiment_weight"]
            )

            # Normalize to 0-1 scale (engagement rates are typically very small)
            engagement_score = min(engagement_score * 1000, 1.0)  # Scale up and cap

            # Calculate confidence based on view count
            confidence = min(view_count / params["min_views_threshold"], 1.0)

            # Total engagement count
            total_engagement = like_count + comment_count

            results.append(
                {
                    "entity_id": video_id,
                    "score_value": round(engagement_score, 4),
                    "confidence": round(confidence, 4),
                    "engagement_rate": round(base_engagement_rate, 6),
                    "like_rate": round(like_rate, 6),
                    "comment_rate": round(comment_rate, 6),
                    "sentiment_boost": round(sentiment_boost, 4),
                    "total_engagement": int(total_engagement),
                }
            )

        return pd.DataFrame(results)


class GrowthPotentialScoringPlugin(ScoringPlugin):
    """
    Scoring plugin for calculating growth potential using historical performance data.

    Uses existing database tables:
    - youtube_metrics: time series metrics data
    - youtube_videos: video metadata for artist grouping
    """

    def get_name(self) -> str:
        return "growth_potential_scorer"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {
            "trend_window_days": 60,  # Days to analyze for trend calculation
            "velocity_weight": 0.5,  # Weight for growth velocity component
            "acceleration_weight": 0.3,  # Weight for growth acceleration component
            "volatility_weight": 0.2,  # Weight for volatility (lower is better)
            "min_data_points": 5,  # Minimum data points needed for trend analysis
        }

    def get_input_requirements(self) -> List[str]:
        return ["artist_name", "video_id", "metrics_date", "view_count", "like_count", "comment_count"]

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "entity_id": "string",
            "score_value": "float",
            "confidence": "float",
            "growth_velocity": "float",
            "growth_acceleration": "float",
            "volatility_score": "float",
            "trend_direction": "string",
            "data_points": "int",
        }

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for growth potential scoring."""
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

        # Check date column
        if "metrics_date" in data.columns:
            try:
                pd.to_datetime(data["metrics_date"])
            except Exception:
                errors.append("Column metrics_date must be convertible to datetime")

        # Check numeric columns
        numeric_columns = ["view_count", "like_count", "comment_count"]
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
        """Calculate growth potential scores using time series analysis."""
        # Merge custom parameters with defaults
        default_params = self.get_parameters()
        if hasattr(self, "_parameters") and self._parameters:
            params = {**default_params, **self._parameters}
        else:
            params = default_params

        # Ensure datetime column
        data = data.copy()
        data["metrics_date"] = pd.to_datetime(data["metrics_date"])

        results = []
        cutoff_date = datetime.now() - timedelta(days=params["trend_window_days"])

        for artist_name, artist_data in data.groupby("artist_name"):
            # Filter to trend window
            trend_data = artist_data[artist_data["metrics_date"] >= cutoff_date].copy()

            if len(trend_data) < params["min_data_points"]:
                # Insufficient data-return low confidence score
                results.append(
                    {
                        "entity_id": str(artist_name),
                        "score_value": 0.3,  # Neutral-low score
                        "confidence": 0.2,  # Low confidence
                        "growth_velocity": 0.0,
                        "growth_acceleration": 0.0,
                        "volatility_score": 0.5,
                        "trend_direction": "insufficient_data",
                        "data_points": len(trend_data),
                    }
                )
                continue

            # Aggregate metrics by date (sum across videos for multi-video artists)
            daily_metrics = (
                trend_data.groupby("metrics_date")
                .agg({"view_count": "sum", "like_count": "sum", "comment_count": "sum"})
                .reset_index()
                .sort_values("metrics_date")
            )

            # Calculate growth metrics
            growth_velocity = self._calculate_growth_velocity(daily_metrics)
            growth_acceleration = self._calculate_growth_acceleration(daily_metrics)
            volatility_score = self._calculate_volatility_score(daily_metrics)

            # Determine trend direction
            trend_direction = self._determine_trend_direction(growth_velocity, growth_acceleration)

            # Calculate weighted growth potential score
            growth_potential = (
                growth_velocity * params["velocity_weight"]
                + growth_acceleration * params["acceleration_weight"]
                + (1-volatility_score) * params["volatility_weight"]  # Lower volatility is better
            )

            # Normalize to 0-1 scale
            growth_potential = max(0.0, min(1.0, growth_potential))

            # Calculate confidence based on data quality
            confidence = min(len(daily_metrics) / (params["min_data_points"] * 2), 1.0)

            results.append(
                {
                    "entity_id": str(artist_name),
                    "score_value": round(growth_potential, 4),
                    "confidence": round(confidence, 4),
                    "growth_velocity": round(growth_velocity, 4),
                    "growth_acceleration": round(growth_acceleration, 4),
                    "volatility_score": round(volatility_score, 4),
                    "trend_direction": trend_direction,
                    "data_points": len(daily_metrics),
                }
            )

        return pd.DataFrame(results)

    def _calculate_growth_velocity(self, daily_metrics: pd.DataFrame) -> float:
        """Calculate growth velocity (first derivative of views over time)."""
        if len(daily_metrics) < 2:
            return 0.0

        # Calculate daily view changes
        daily_metrics = daily_metrics.copy()
        daily_metrics["view_change"] = daily_metrics["view_count"].diff()

        # Average daily growth rate
        mean_change = daily_metrics["view_change"].mean()
        mean_views = daily_metrics["view_count"].mean()

        if mean_views == 0:
            return 0.0

        # Normalize by average views to get relative growth rate
        velocity = mean_change / mean_views
        return min(max(velocity, -1.0), 1.0)  # Cap between -100% and 100%

    def _calculate_growth_acceleration(self, daily_metrics: pd.DataFrame) -> float:
        """Calculate growth acceleration (second derivative of views over time)."""
        if len(daily_metrics) < 3:
            return 0.0

        # Calculate daily view changes
        daily_metrics = daily_metrics.copy()
        daily_metrics["view_change"] = daily_metrics["view_count"].diff()
        daily_metrics["acceleration"] = daily_metrics["view_change"].diff()

        # Average acceleration
        mean_acceleration = daily_metrics["acceleration"].mean()
        mean_views = daily_metrics["view_count"].mean()

        if mean_views == 0:
            return 0.0

        # Normalize by average views
        acceleration = mean_acceleration / mean_views
        return min(max(acceleration, -1.0), 1.0)  # Cap between -100% and 100%

    def _calculate_volatility_score(self, daily_metrics: pd.DataFrame) -> float:
        """Calculate volatility score (coefficient of variation of view changes)."""
        if len(daily_metrics) < 2:
            return 0.5  # Neutral volatility

        daily_metrics = daily_metrics.copy()
        daily_metrics["view_change"] = daily_metrics["view_count"].diff()

        changes = daily_metrics["view_change"].dropna()
        if len(changes) == 0:
            return 0.5

        # Coefficient of variation (std dev / mean)
        mean_change = changes.mean()
        std_change = changes.std()

        if mean_change == 0:
            return 1.0 if std_change > 0 else 0.0

        cv = abs(std_change / mean_change)
        # Convert to 0-1 scale where 0 is low volatility, 1 is high volatility
        volatility = min(cv / 2.0, 1.0)  # Divide by 2 to scale typical CV values

        return volatility

    def _determine_trend_direction(self, velocity: float, acceleration: float) -> str:
        """Determine trend direction based on velocity and acceleration."""
        if velocity > 0.1 and acceleration > 0.05:
            return "accelerating"
        elif velocity > 0.1:
            return "growing"
        elif velocity > -0.1:
            return "stable"
        elif acceleration < -0.05:
            return "declining"
        else:
            return "stagnant"
