#!/usr / bin / env python3
"""
Pydantic Models for YouTube ETL Pipeline

This module provides comprehensive data models with validation for:
- YouTube API responses
- Database records
- ETL pipeline configuration
- Processing results and metrics

All models use Pydantic for strict validation and fail - fast error handling.
Natural keys are used throughout for better debugging and maintainability.
"""

from datetime import datetime
from enum import Enum
import os
import re
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class VideoStatus(str, Enum):
    """Video processing status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    FILTERED = "filtered"  # Filtered out due to quality issues


class SentimentMethod(str, Enum):
    """Sentiment analysis methods."""

    VADER = "vader"
    TEXTBLOB = "textblob"
    SIMPLE = "simple"
    AUTO = "auto"


class BotRiskLevel(str, Enum):
    """Bot detection risk levels."""

    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


# YouTube API Models
class YouTubeVideo(BaseModel):
    """YouTube video data from API with validation."""

    video_id: str = Field(..., pattern=r"^[a - zA - Z0 - 9_-]{11}$", description="YouTube video ID")
    title: str = Field(..., min_length=1, max_length=500, description="Video title")
    channel_id: str = Field(..., pattern=r"^UC[a - zA - Z0 - 9_-]{22}$", description="YouTube channel ID")
    channel_title: str = Field(..., min_length=1, max_length=255, description="Channel name")
    published_at: datetime = Field(..., description="Video publication timestamp")
    duration: Optional[str] = Field(None, pattern=r"^PT(\d + H)?(\d + M)?(\d + S)?$", description="ISO 8601 duration")
    view_count: int = Field(0, ge=0, description="Total view count")
    like_count: int = Field(0, ge=0, description="Total like count")
    comment_count: int = Field(0, ge=0, description="Total comment count")
    isrc: Optional[str] = Field(
        None, pattern=r"^[A - Z]{2}[A - Z0 - 9]{3}[0 - 9]{2}[0 - 9]{5}$", description="ISRC code")

    @field_validator("title")
    @classmethod
    def validate_title(cls, v):
        """Validate and clean video title."""
        if not v or not v.strip():
            raise ValueError("Video title cannot be empty")
        # Remove excessive whitespace
        cleaned = " ".join(v.strip().split())
        if len(cleaned) < 1:
            raise ValueError("Video title cannot be empty after cleaning")
        return cleaned

    @field_validator("channel_title")
    @classmethod
    def validate_channel_title(cls, v):
        """Validate and clean channel title."""
        if not v or not v.strip():
            raise ValueError("Channel title cannot be empty")
        return v.strip()

    @field_validator("isrc")
    @classmethod
    def validate_isrc_format(cls, v):
        """Validate ISRC format if provided."""
        if v is None:
            return v
        # Convert to uppercase and validate format
        isrc_upper = v.upper().strip()
        if not re.match(r"^[A - Z]{2}[A - Z0 - 9]{3}[0 - 9]{2}[0 - 9]{5}$", isrc_upper):
            raise ValueError(f"Invalid ISRC format: {v}")
        return isrc_upper

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True


class YouTubeComment(BaseModel):
    """YouTube comment data with validation."""

    comment_id: str = Field(..., min_length=1, max_length=100, description="Unique comment ID")
    video_id: str = Field(..., pattern=r"^[a - zA - Z0 - 9_-]{11}$", description="Associated video ID")
    author_name: str = Field(..., min_length=1, max_length=255, description="Comment author name")
    comment_text: str = Field(..., min_length=1, max_length=10000, description="Comment content")
    like_count: int = Field(0, ge=0, description="Comment like count")
    published_at: datetime = Field(..., description="Comment publication timestamp")
    parent_id: Optional[str] = Field(None, description="Parent comment ID for replies")

    @field_validator("comment_text")
    @classmethod
    def validate_comment_text(cls, v):
        """Validate and clean comment text."""
        if not v or not v.strip():
            raise ValueError("Comment text cannot be empty")
        # Basic cleaning while preserving emojis and formatting
        cleaned = v.strip()
        if len(cleaned) < 1:
            raise ValueError("Comment text cannot be empty after cleaning")
        return cleaned

    @field_validator("author_name")
    @classmethod
    def validate_author_name(cls, v):
        """Validate author name."""
        if not v or not v.strip():
            raise ValueError("Author name cannot be empty")
        return v.strip()

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


# Processing Result Models
class SentimentResult(BaseModel):
    """Sentiment analysis result with validation."""

    comment_id: str = Field(..., min_length=1, description="Comment ID")
    video_id: str = Field(..., pattern=r"^[a - zA - Z0 - 9_-]{11}$", description="Video ID")
    sentiment_score: float = Field(..., ge=-1.0, le=1.0, description="Sentiment score (-1 to 1)")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0 to 1)")
    method: SentimentMethod = Field(..., description="Analysis method used")
    processed_at: datetime = Field(default_factory=datetime.utcnow, description="Processing timestamp")

    @field_validator("sentiment_score")
    @classmethod
    def validate_sentiment_range(cls, v):
        """Ensure sentiment score is in valid range."""
        if not -1.0 <= v <= 1.0:
            raise ValueError(f"Sentiment score must be between -1.0 and 1.0, got {v}")
        return round(v, 3)  # Round to 3 decimal places

    @field_validator("confidence_score")
    @classmethod
    def validate_confidence_range(cls, v):
        """Ensure confidence score is in valid range."""
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"Confidence score must be between 0.0 and 1.0, got {v}")
        return round(v, 3)  # Round to 3 decimal places

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True


class BotDetectionResult(BaseModel):
    """Bot detection result with validation."""

    comment_id: str = Field(..., min_length=1, description="Comment ID")
    video_id: str = Field(..., pattern=r"^[a - zA - Z0 - 9_-]{11}$", description="Video ID")
    author_name: str = Field(..., min_length=1, description="Comment author")
    bot_score: float = Field(..., ge=0.0, le=100.0, description="Bot suspicion score (0 - 100)")
    bot_risk_level: BotRiskLevel = Field(..., description="Risk level classification")
    duplicate_count_local: int = Field(0, ge=0, description="Similar comments in same video")
    duplicate_count_global: int = Field(0, ge=0, description="Similar comments across videos")
    burst_score: float = Field(0.0, ge=0.0, le=1.0, description="Timing burst score")
    author_repetition_score: float = Field(0.0, ge=0.0, le=1.0, description="Author repetition score")
    engagement_score: float = Field(0.0, ge=0.0, le=1.0, description="Low engagement score")
    emoji_count: int = Field(0, ge=0, description="Number of emojis in comment")
    is_whitelisted: bool = Field(False, description="Contains whitelisted phrases")
    analyzed_at: datetime = Field(default_factory=datetime.utcnow, description="Analysis timestamp")

    @field_validator("bot_score")
    @classmethod
    def validate_bot_score_range(cls, v):
        """Ensure bot score is in valid range."""
        if not 0.0 <= v <= 100.0:
            raise ValueError(f"Bot score must be between 0.0 and 100.0, got {v}")
        return round(v, 2)  # Round to 2 decimal places

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True


# Configuration Models
class ETLConfig(BaseModel):
    """ETL pipeline configuration with validation."""

    # Database settings
    database_url: str = Field(..., min_length=1, description="Database connection URL")

    # API settings
    youtube_api_key: str = Field(..., min_length=1, description="YouTube API key")
    comments_per_video: int = Field(80, ge=1, le=1000, description="Comments to fetch per video")

    # Processing settings
    batch_size: int = Field(200, ge=1, le=10000, description="Processing batch size")
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")
    timeout_seconds: int = Field(300, ge=30, le=3600, description="Operation timeout")

    # Feature flags
    enable_bot_detection: bool = Field(True, description="Enable bot detection")
    enable_sentiment_analysis: bool = Field(True, description="Enable sentiment analysis")
    enable_data_quality_checks: bool = Field(True, description="Enable data quality validation")

    # Quality thresholds
    quality_threshold: float = Field(80.0, ge=0.0, le=100.0, description="Minimum quality score")
    sentiment_confidence_threshold: float = Field(0.7, ge=0.0, le=1.0, description="Minimum sentiment confidence")
    bot_detection_threshold: float = Field(0.85, ge=0.0, le=1.0, description="Bot detection threshold")

    # Logging
    log_level: str = Field("INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$", description="Logging level")

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v):
        """Validate database URL format."""
        if not v.startswith(("mysql + pymysql://", "postgresql://", "sqlite:///")):
            raise ValueError("Database URL must start with supported scheme")
        return v

    @field_validator("youtube_api_key")
    @classmethod
    def validate_api_key(cls, v):
        """Validate YouTube API key format."""
        if not v or len(v) < 10:  # Relaxed for testing
            raise ValueError("YouTube API key appears to be invalid")
        return v.strip()

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class DataQualityIssue(BaseModel):
    """Data quality issue with details."""

    severity: str = Field(..., pattern=r"^(CRITICAL|HIGH|MEDIUM|LOW)$", description="Issue severity")
    category: str = Field(..., min_length=1, description="Issue category")
    description: str = Field(..., min_length=1, description="Issue description")
    affected_records: int = Field(0, ge=0, description="Number of affected records")
    suggested_fix: str = Field(..., min_length=1, description="Suggested resolution")
    table_name: Optional[str] = Field(None, description="Affected database table")

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class DataQualityReport(BaseModel):
    """Comprehensive data quality assessment."""

    overall_score: float = Field(..., ge=0.0, le=100.0, description="Overall quality score")
    total_records: int = Field(0, ge=0, description="Total records analyzed")
    issues: List[DataQualityIssue] = Field(default_factory=list, description="Quality issues found")
    recommendations: List[str] = Field(default_factory=list, description="Improvement recommendations")
    statistics: Dict[str, Any] = Field(default_factory=dict, description="Quality statistics")
    generated_at: datetime = Field(default_factory=datetime.utcnow, description="Report generation time")

    @field_validator("overall_score")
    @classmethod
    def validate_score_range(cls, v):
        """Ensure quality score is in valid range."""
        if not 0.0 <= v <= 100.0:
            raise ValueError(f"Quality score must be between 0.0 and 100.0, got {v}")
        return round(v, 1)

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


# Pipeline Result Models
class StageResult(BaseModel):
    """Result of individual pipeline stage."""

    stage_name: str = Field(..., min_length=1, description="Stage name")
    status: str = Field(..., pattern=r"^(SUCCESS|FAILED|PARTIAL|SKIPPED)$", description="Stage status")
    start_time: datetime = Field(..., description="Stage start time")
    end_time: datetime = Field(..., description="Stage end time")
    duration_seconds: float = Field(..., ge=0.0, description="Stage duration")
    records_processed: int = Field(0, ge=0, description="Records processed")
    records_failed: int = Field(0, ge=0, description="Records failed")
    errors: List[str] = Field(default_factory=list, description="Error messages")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Stage metrics")

    @model_validator(mode="before")
    @classmethod
    def validate_times(cls, values):
        """Validate that end_time is after start_time."""
        start_time = values.get("start_time")
        end_time = values.get("end_time")
        if start_time and end_time and end_time < start_time:
            raise ValueError("End time must be after start time")

        # Calculate duration if not provided
        if start_time and end_time:
            calculated_duration = (end_time - start_time).total_seconds()
            values["duration_seconds"] = calculated_duration

        return values

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class ETLResult(BaseModel):
    """Complete ETL pipeline execution result."""

    pipeline_id: str = Field(..., min_length=1, description="Unique pipeline execution ID")
    status: str = Field(..., pattern=r"^(SUCCESS|FAILED|PARTIAL)$", description="Overall status")
    start_time: datetime = Field(..., description="Pipeline start time")
    end_time: datetime = Field(..., description="Pipeline end time")
    duration_seconds: float = Field(..., ge=0.0, description="Total duration")
    stages: List[StageResult] = Field(default_factory=list, description="Stage results")
    total_records_processed: int = Field(0, ge=0, description="Total records processed")
    total_errors: int = Field(0, ge=0, description="Total errors encountered")
    quality_score: Optional[float] = Field(None, ge=0.0, le=100.0, description="Data quality score")
    summary: Dict[str, Any] = Field(default_factory=dict, description="Execution summary")

    @model_validator(mode="before")
    @classmethod
    def validate_pipeline_result(cls, values):
        """Validate pipeline result consistency."""
        start_time = values.get("start_time")
        end_time = values.get("end_time")

        if start_time and end_time and end_time < start_time:
            raise ValueError("Pipeline end time must be after start time")

        # Calculate duration
        if start_time and end_time:
            calculated_duration = (end_time - start_time).total_seconds()
            values["duration_seconds"] = calculated_duration

        # Validate stage consistency
        stages = values.get("stages", [])
        failed_stages = [s for s in stages if s.status == "FAILED"]

        if failed_stages and values.get("status") == "SUCCESS":
            raise ValueError("Pipeline cannot be SUCCESS with failed stages")

        return values

    def get_stage_by_name(self, stage_name: str) -> Optional[StageResult]:
        """Get stage result by name."""
        for stage in self.stages:
            if stage.stage_name == stage_name:
                return stage
        return None

    def get_failed_stages(self) -> List[StageResult]:
        """Get list of failed stages."""
        return [s for s in self.stages if s.status == "FAILED"]

    def get_success_rate(self) -> float:
        """Calculate success rate of stages."""
        if not self.stages:
            return 0.0
        successful = len([s for s in self.stages if s.status == "SUCCESS"])
        return (successful / len(self.stages)) * 100.0

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


# Video Filtering Models
class VideoFilter(BaseModel):
    """Configuration for filtering problematic videos."""

    blocked_video_ids: List[str] = Field(default_factory=list, description="Specific video IDs to block")
    blocked_channel_ids: List[str] = Field(default_factory=list, description="Channel IDs to block")
    blocked_title_patterns: List[str] = Field(default_factory=list, description="Title patterns to block")
    min_duration_seconds: Optional[int] = Field(None, ge=0, description="Minimum video duration")
    max_duration_seconds: Optional[int] = Field(None, ge=0, description="Maximum video duration")
    require_isrc: bool = Field(False, description="Require ISRC code for inclusion")

    @field_validator("blocked_video_ids")
    @classmethod
    def validate_video_ids(cls, v):
        """Validate video ID format."""
        for video_id in v:
            if not re.match(r"^[a - zA - Z0 - 9_-]{11}$", video_id):
                raise ValueError(f"Invalid video ID format: {video_id}")
        return v

    @field_validator("blocked_channel_ids")
    @classmethod
    def validate_channel_ids(cls, v):
        """Validate channel ID format."""
        for channel_id in v:
            if not re.match(r"^UC[a - zA - Z0 - 9_-]{22}$", channel_id):
                raise ValueError(f"Invalid channel ID format: {channel_id}")
        return v

    @model_validator(mode="before")
    @classmethod
    def validate_duration_range(cls, values):
        """Validate duration range."""
        min_dur = values.get("min_duration_seconds")
        max_dur = values.get("max_duration_seconds")

        if min_dur is not None and max_dur is not None and min_dur > max_dur:
            raise ValueError("Minimum duration cannot be greater than maximum duration")

        return values

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
