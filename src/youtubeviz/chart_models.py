"""
Pydantic models for chart data validation and type safety.
Ensures bulletproof data handling with clear error messages.
"""

from datetime import datetime
from typing import List, Literal, Optional

import pandas as pd
from pydantic import BaseModel, Field, validator


class ChartDataValidationError(Exception):
    """Custom exception for chart data validation failures."""

    pass


class SentimentData(BaseModel):
    """Validated sentiment data for charts."""

    artist_name: str = Field(..., min_length=1, description="Artist name cannot be empty")
    sentiment_category: Literal["positive", "negative", "neutral"] = Field(..., description="Must be valid sentiment")
    video_id: str = Field(..., min_length=1, description="Video ID cannot be empty")
    comment_count: Optional[int] = Field(default=1, ge=1, description="Comment count must be positive")

    @validator("artist_name", "video_id")
    def validate_non_empty_strings(cls, v):
        if not v or v.strip() == "":
            raise ValueError("String fields cannot be empty or whitespace")
        return v.strip()


class ThemeData(BaseModel):
    """Validated theme data for lollipop charts."""

    artist_name: str = Field(..., min_length=1)
    theme: str = Field(..., min_length=1, description="Theme cannot be empty")
    sentiment_category: Literal["positive", "negative"] = Field(..., description="Must be positive or negative")
    comment_text: str = Field(..., min_length=10, description="Comment must have substance")
    timestamp: str = Field(..., pattern=r"^\d{1,2}:\d{2}$", description="Timestamp must be MM:SS format")
    video_id: str = Field(..., min_length=1)

    @validator("comment_text")
    def validate_comment_quality(cls, v):
        if len(v.strip()) < 10:
            raise ValueError("Comments must be at least 10 characters for meaningful analysis")
        return v.strip()


class VideoPerformanceData(BaseModel):
    """Validated video performance data for scatter plots."""

    video_id: str = Field(..., min_length=1)
    artist_name: str = Field(..., min_length=1)
    views: int = Field(..., ge=1, description="Views must be positive")
    positive_rate: float = Field(..., ge=0.0, le=1.0, description="Rate must be between 0 and 1")
    positive_comments: int = Field(..., ge=0)
    total_comments: int = Field(..., ge=1, description="Must have at least 1 comment")
    upload_date: datetime = Field(..., description="Upload date required for analysis")

    @validator("positive_comments")
    def validate_comment_consistency(cls, v, values):
        if "total_comments" in values and v > values["total_comments"]:
            raise ValueError("Positive comments cannot exceed total comments")
        return v

    @validator("positive_rate")
    def validate_rate_consistency(cls, v, values):
        if "positive_comments" in values and "total_comments" in values:
            expected_rate = values["positive_comments"] / values["total_comments"]
            if abs(v - expected_rate) > 0.01:  # Allow small floating point differences
                raise ValueError(f"Positive rate {v} inconsistent with comment counts")
        return v


class FeatureData(BaseModel):
    """Validated feature data for UpSet plots."""

    video_id: str = Field(..., min_length=1)
    artist_name: str = Field(..., min_length=1)
    views: int = Field(..., ge=1)
    engagement_rate: float = Field(..., ge=0.0, le=1.0)

    # Feature flags
    has_isrc: bool = Field(..., description="Whether video has ISRC code")
    short_form: bool = Field(..., description="Whether video is short-form content")
    visualizer: bool = Field(..., description="Whether video is a visualizer")
    teaser: bool = Field(..., description="Whether video is a teaser")
    music_video: bool = Field(..., description="Whether video is a music video")

    @validator("engagement_rate")
    def validate_realistic_engagement(cls, v):
        if v > 0.2:  # 20% engagement is extremely high
            raise ValueError(f"Engagement rate {v:.1%} seems unrealistic-check data quality")
        return v


class UMAPClusteringData(BaseModel):
    """Validated data for UMAP clustering analysis."""

    artist_name: str = Field(..., min_length=1)
    video_id: str = Field(..., min_length=1)
    comment_text: str = Field(..., min_length=20, description="Need substantial text for embedding")
    sentiment_category: Literal["positive", "negative", "neutral"]
    content_type: Literal["music_video", "lyric_video", "visualizer", "other"] = Field(
        ..., description="Content type for shape encoding"
    )
    views: int = Field(..., ge=1)
    engagement_rate: float = Field(..., ge=0.0, le=1.0)

    @validator("comment_text")
    def validate_text_quality(cls, v):
        # Check for meaningful content (not just repeated characters)
        if len(set(v.lower().replace(" ", ""))) < 5:
            raise ValueError("Comment text lacks diversity-may be spam or low quality")
        return v.strip()


class ContentAnalysisData(BaseModel):
    """Validated content analysis data for Charts #8-11."""

    video_id: str = Field(..., min_length=1)
    artist_name: str = Field(..., min_length=1)
    views: int = Field(..., ge=1, description="Views must be positive")
    has_isrc: bool = Field(..., description="Whether video has ISRC code")
    content_type: Literal["music_video", "lyric_video", "visualizer", "other"] = Field(
        ..., description="Content type classification"
    )
    duration_seconds: int = Field(..., ge=1, le=3600, description="Duration in seconds (max 1 hour)")
    upload_date: str = Field(..., description="Upload date in YYYY-MM-DD format")

    # Optional fields for enhanced analysis
    short_form: Optional[bool] = Field(default=None, description="Whether video is short-form (<60s)")
    engagement_rate: Optional[float] = Field(default=None, ge=0.0, le=1.0)

    @validator("duration_seconds")
    def validate_realistic_duration(cls, v, values):
        """Validate duration is realistic for content type."""
        if "content_type" in values:
            content_type = values["content_type"]

            # Music videos typically 2-6 minutes
            if content_type == "music_video" and (v < 30 or v > 600):
                raise ValueError(f"Duration {v}s seems unrealistic for music video (expect 30-600s)")

            # Lyric videos can be longer
            if content_type == "lyric_video" and (v < 60 or v > 900):
                raise ValueError(f"Duration {v}s seems unrealistic for lyric video (expect 60-900s)")

            # Visualizers are typically shorter
            if content_type == "visualizer" and (v < 15 or v > 300):
                raise ValueError(f"Duration {v}s seems unrealistic for visualizer (expect 15-300s)")

        return v

    @validator("short_form", always=True)
    def auto_detect_short_form(cls, v, values):
        """Auto-detect short form based on duration if not provided."""
        if v is None and "duration_seconds" in values:
            return values["duration_seconds"] < 60
        return v

    @validator("upload_date")
    def validate_upload_date_format(cls, v):
        """Validate upload date format."""
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Upload date must be in YYYY-MM-DD format, got: {v}")
        return v


class ChartConfiguration(BaseModel):
    """Configuration for chart generation with validation."""

    chart_type: Literal[
        "diverging_sentiment_bars",
        "sentiment_cluster_heatmap",
        "positive_theme_lollipops",
        "negative_theme_lollipops",
        "standout_videos_scatter",
        "umap_clustering",
        "upset_plot",
    ]

    # Statistical options
    use_wilson_intervals: bool = Field(default=True, description="Apply Wilson confidence intervals")
    use_bayesian_shrinkage: bool = Field(default=True, description="Apply Bayesian shrinkage for small samples")
    min_comments_threshold: int = Field(default=20, ge=5, le=100, description="Minimum comments for reliable analysis")
    confidence_level: float = Field(default=0.95, ge=0.8, le=0.99, description="Confidence level for intervals")

    # Visual options
    color_scheme: Literal["professional", "academic", "presentation"] = Field(default="professional")
    show_uncertainty: bool = Field(default=True, description="Show uncertainty indicators")
    max_items_displayed: int = Field(default=20, ge=5, le=50, description="Maximum items to display")

    # Interactive options
    enable_cross_filtering: bool = Field(default=True, description="Enable cross-chart filtering")
    include_hover_details: bool = Field(default=True, description="Include detailed hover information")


def validate_dataframe_schema(df: pd.DataFrame, model_class: BaseModel, sample_size: int = 100) -> None:
    """
    Validate DataFrame against Pydantic model schema.

    Args:
        df: DataFrame to validate
        model_class: Pydantic model class for validation
        sample_size: Number of rows to validate (for performance)

    Raises:
        ChartDataValidationError: If validation fails
    """
    if df.empty:
        raise ChartDataValidationError("DataFrame cannot be empty")

    # Sample data for validation (don't validate every row for performance)
    sample_df = df.sample(min(sample_size, len(df)), random_state=42)

    validation_errors = []

    for idx, row in sample_df.iterrows():
        try:
            # Convert row to dict and validate
            row_dict = row.to_dict()
            model_class(**row_dict)
        except Exception as e:
            validation_errors.append(f"Row {idx}: {str(e)}")

    if validation_errors:
        error_summary = f"Data validation failed for {len(validation_errors)} rows:\n"
        error_summary += "\n".join(validation_errors[:5])  # Show first 5 errors
        if len(validation_errors) > 5:
            error_summary += f"\n... and {len(validation_errors) - 5} more errors"

        raise ChartDataValidationError(error_summary)


def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> None:
    """
    Validate that DataFrame has required columns.

    Args:
        df: DataFrame to check
        required_columns: List of required column names

    Raises:
        ChartDataValidationError: If required columns are missing
    """
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        available_columns = list(df.columns)
        raise ChartDataValidationError(
            f"Missing required columns: {missing_columns}. " f"Available columns: {available_columns}"
        )


def validate_data_quality(df: pd.DataFrame, min_rows: int = 10) -> None:
    """
    Validate basic data quality requirements.

    Args:
        df: DataFrame to validate
        min_rows: Minimum number of rows required

    Raises:
        ChartDataValidationError: If data quality is insufficient
    """
    if len(df) < min_rows:
        raise ChartDataValidationError(f"Insufficient data: {len(df)} rows, minimum {min_rows} required")

    # Check for excessive null values
    null_percentages = df.isnull().sum() / len(df)
    high_null_columns = null_percentages[null_percentages > 0.5].index.tolist()

    if high_null_columns:
        raise ChartDataValidationError(
            f"Columns with >50% null values: {high_null_columns}. " "Data quality insufficient for reliable analysis."
        )
