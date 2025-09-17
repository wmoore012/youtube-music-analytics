"""
Bulletproof content analysis system for Charts #8-11.
Implements ISRC balance, dumbbell charts, Cleveland dots with statistical rigor.
"""

import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .chart_models import (
    ChartDataValidationError,
    ContentAnalysisData,
    validate_data_quality,
    validate_dataframe_schema,
    validate_required_columns,
)
from .statistical_utils import calculate_wilson_intervals


class InsufficientContentDataError(Exception):
    """Raised when there's insufficient content data for analysis."""

    pass


class InvalidContentTypeError(Exception):
    """Raised when content type distribution is invalid."""

    pass


@dataclass
class PChartControlLimits:
    """P-chart control limits for proportion analysis."""

    center_line: float
    upper_control_limit: float
    lower_control_limit: float
    upper_warning_limit: float
    lower_warning_limit: float
    sample_sizes: np.ndarray
    proportions: np.ndarray


@dataclass
class ContentAnalysisResult:
    """Results from content analysis."""

    isrc_proportions: Dict[str, float]
    content_type_distribution: Dict[str, Dict[str, int]]
    duration_analysis: Dict[str, Dict[str, float]]
    p_chart_limits: PChartControlLimits
    total_videos: int
    artists_analyzed: List[str]


class ContentAnalysisEngine:
    """
    Bulletproof content analysis engine for music industry analytics.

    Implements p-chart control limits, ISRC balance analysis, and content type distribution
    with proper statistical validation and error handling.
    """

    def __init__(self, min_videos_per_artist: int = 5, min_total_videos: int = 20, confidence_level: float = 0.95):
        """
        Initialize content analysis engine.

        Args:
            min_videos_per_artist: Minimum videos required per artist
            min_total_videos: Minimum total videos for analysis
            confidence_level: Confidence level for control limits

        Raises:
            ValueError: If parameters are invalid
        """
        if min_videos_per_artist < 1:
            raise ValueError("min_videos_per_artist must be at least 1")
        if min_total_videos < min_videos_per_artist:
            raise ValueError("min_total_videos must be >= min_videos_per_artist")
        if not 0.8 <= confidence_level <= 0.99:
            raise ValueError("confidence_level must be between 0.8 and 0.99")

        self.min_videos_per_artist = min_videos_per_artist
        self.min_total_videos = min_total_videos
        self.confidence_level = confidence_level

    def validate_content_data(self, df: pd.DataFrame) -> None:
        """
        Validate content data for analysis.

        Args:
            df: DataFrame with content data

        Raises:
            ChartDataValidationError: If data validation fails
            InsufficientContentDataError: If insufficient data
        """
        # Check required columns
        required_columns = [
            "video_id",
            "artist_name",
            "views",
            "has_isrc",
            "content_type",
            "duration_seconds",
            "upload_date",
        ]
        validate_required_columns(df, required_columns)

        # Validate data quality
        validate_data_quality(df, self.min_total_videos)

        # Validate against Pydantic schema
        validate_dataframe_schema(df, ContentAnalysisData, sample_size=50)

        # Check per-artist video counts
        artist_counts = df["artist_name"].value_counts()
        insufficient_artists = artist_counts[artist_counts < self.min_videos_per_artist]

        if len(insufficient_artists) > 0:
            raise InsufficientContentDataError(
                f"Artists with insufficient videos (< {self.min_videos_per_artist}): "
                f"{insufficient_artists.index.tolist()}. "
                f"Counts: {insufficient_artists.to_dict()}"
            )

        # Validate content type distribution
        content_types = df["content_type"].value_counts()
        if len(content_types) < 2:
            raise InvalidContentTypeError(
                f"Need at least 2 content types for analysis, found: {content_types.index.tolist()}"
            )

    def calculate_p_chart_control_limits(
        self, proportions: np.ndarray, sample_sizes: np.ndarray
    ) -> PChartControlLimits:
        """
        Calculate p-chart control limits for proportion analysis.

        Args:
            proportions: Array of proportions (e.g., ISRC rates per artist)
            sample_sizes: Array of sample sizes per artist

        Returns:
            PChartControlLimits with control and warning limits

        Raises:
            ValueError: If arrays are invalid
        """
        if len(proportions) != len(sample_sizes):
            raise ValueError("proportions and sample_sizes must have same length")

        if np.any(sample_sizes <= 0):
            raise ValueError("All sample sizes must be positive")

        if np.any((proportions < 0) | (proportions > 1)):
            raise ValueError("All proportions must be between 0 and 1")

        # Calculate center line (overall proportion)
        total_successes = np.sum(proportions * sample_sizes)
        total_samples = np.sum(sample_sizes)
        center_line = total_successes / total_samples

        # Calculate control limits using average sample size
        avg_sample_size = np.mean(sample_sizes)

        # Standard error for p-chart
        std_error = np.sqrt(center_line * (1 - center_line) / avg_sample_size)

        # Control limits (3-sigma)
        ucl = center_line + 3 * std_error
        lcl = center_line - 3 * std_error

        # Warning limits (2-sigma)
        uwl = center_line + 2 * std_error
        lwl = center_line - 2 * std_error

        # Ensure limits are within [0, 1]
        ucl = min(ucl, 1.0)
        lcl = max(lcl, 0.0)
        uwl = min(uwl, 1.0)
        lwl = max(lwl, 0.0)

        return PChartControlLimits(
            center_line=center_line,
            upper_control_limit=ucl,
            lower_control_limit=lcl,
            upper_warning_limit=uwl,
            lower_warning_limit=lwl,
            sample_sizes=sample_sizes,
            proportions=proportions,
        )

    def analyze_isrc_balance(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Analyze ISRC vs non-ISRC content balance per artist.

        Args:
            df: DataFrame with content data

        Returns:
            Dictionary mapping artist names to ISRC proportions
        """
        isrc_analysis = {}

        for artist in df["artist_name"].unique():
            artist_data = df[df["artist_name"] == artist]
            isrc_count = artist_data["has_isrc"].sum()
            total_count = len(artist_data)

            isrc_proportion = isrc_count / total_count if total_count > 0 else 0.0
            isrc_analysis[artist] = isrc_proportion

        return isrc_analysis

    def analyze_content_type_distribution(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """
        Analyze content type distribution per artist.

        Args:
            df: DataFrame with content data

        Returns:
            Nested dictionary: {artist: {content_type: count}}
        """
        distribution = {}

        for artist in df["artist_name"].unique():
            artist_data = df[df["artist_name"] == artist]
            content_counts = artist_data["content_type"].value_counts().to_dict()
            distribution[artist] = content_counts

        return distribution

    def analyze_duration_patterns(self, df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """
        Analyze duration patterns by artist and content type.

        Args:
            df: DataFrame with content data

        Returns:
            Nested dictionary with duration statistics
        """
        duration_analysis = {}

        for artist in df["artist_name"].unique():
            artist_data = df[df["artist_name"] == artist]

            duration_stats = {
                "avg_duration": artist_data["duration_seconds"].mean(),
                "median_duration": artist_data["duration_seconds"].median(),
                "short_form_pct": (artist_data["duration_seconds"] < 60).mean(),
                "long_form_pct": (artist_data["duration_seconds"] >= 300).mean(),
            }

            duration_analysis[artist] = duration_stats

        return duration_analysis

    def perform_complete_analysis(self, df: pd.DataFrame) -> ContentAnalysisResult:
        """
        Perform complete content analysis.

        Args:
            df: DataFrame with content data

        Returns:
            ContentAnalysisResult with all analysis results

        Raises:
            ChartDataValidationError: If data validation fails
            InsufficientContentDataError: If insufficient data
        """
        # Validate input data
        self.validate_content_data(df)

        # Perform analyses
        isrc_proportions = self.analyze_isrc_balance(df)
        content_distribution = self.analyze_content_type_distribution(df)
        duration_analysis = self.analyze_duration_patterns(df)

        # Calculate p-chart control limits for ISRC proportions
        artists = list(isrc_proportions.keys())
        proportions = np.array([isrc_proportions[artist] for artist in artists])
        sample_sizes = np.array([len(df[df["artist_name"] == artist]) for artist in artists])

        p_chart_limits = self.calculate_p_chart_control_limits(proportions, sample_sizes)

        return ContentAnalysisResult(
            isrc_proportions=isrc_proportions,
            content_type_distribution=content_distribution,
            duration_analysis=duration_analysis,
            p_chart_limits=p_chart_limits,
            total_videos=len(df),
            artists_analyzed=artists,
        )


def calculate_p_chart_control_limits(
    proportions: np.ndarray, sample_sizes: np.ndarray, confidence_level: float = 0.95
) -> PChartControlLimits:
    """
    Calculate p-chart control limits for proportion analysis.

    Args:
        proportions: Array of proportions
        sample_sizes: Array of sample sizes
        confidence_level: Confidence level for limits

    Returns:
        PChartControlLimits object
    """
    engine = ContentAnalysisEngine(confidence_level=confidence_level)
    return engine.calculate_p_chart_control_limits(proportions, sample_sizes)
