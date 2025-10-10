#!/usr / bin / env python3
"""
ML-Ready Data Models with Pydantic Validation

Provides type-safe data models for machine learning preprocessing and training.
Ensures data quality and consistency across the ML pipeline.
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class SentimentLabel(str, Enum):
    """Standardized sentiment labels for ML training."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class DataSplit(str, Enum):
    """Data split types for ML training."""

    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"
    UNLABELED = "unlabeled"


class MusicDomain(str, Enum):
    """Music domain categories for filtering."""

    MUSIC_VIDEO = "music_video"
    LIVE_PERFORMANCE = "live_performance"
    MUSIC_DISCUSSION = "music_discussion"
    ARTIST_CONTENT = "artist_content"
    GENERAL = "general"


class CommentMetadata(BaseModel):
    """Metadata for individual comments."""

    comment_id: str = Field(..., description="Unique comment identifier")
    video_id: str = Field(..., description="YouTube video ID")
    channel_title: Optional[str] = Field(None, description="Channel name")
    like_count: int = Field(0, ge=0, description="Number of likes")
    published_at: Optional[datetime] = Field(None, description="Comment publication time")
    reply_count: int = Field(0, ge=0, description="Number of replies")

    # Music-specific metadata
    artist_name: Optional[str] = Field(None, description="Associated artist name")
    music_domain: MusicDomain = Field(MusicDomain.GENERAL, description="Music domain category")
    contains_music_slang: bool = Field(False, description="Contains music slang terms")
    slang_terms: List[str] = Field(default_factory=list, description="Identified slang terms")

    # Quality indicators
    is_spam: bool = Field(False, description="Flagged as spam")
    is_bot: bool = Field(False, description="Flagged as bot comment")
    language_code: str = Field("en", description="Detected language code")

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}


class MLComment(BaseModel):
    """ML-ready comment with preprocessing and validation."""

    # Core data
    text: str = Field(..., min_length=1, max_length=2000, description="Comment text")
    normalized_text: str = Field(..., description="Normalized text for ML processing")

    # Labels (optional for unlabeled data)
    sentiment_label: Optional[SentimentLabel] = Field(None, description="Sentiment label")
    confidence_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Label confidence")

    # ML preprocessing
    token_count: int = Field(..., ge=0, description="Number of tokens after preprocessing")
    contains_emoji: bool = Field(False, description="Contains emoji characters")
    emoji_count: int = Field(0, ge=0, description="Number of emoji characters")

    # Data management
    data_split: DataSplit = Field(DataSplit.UNLABELED, description="Data split assignment")
    unique_hash: str = Field(..., description="Unique hash for deduplication")

    # Metadata
    metadata: CommentMetadata = Field(..., description="Comment metadata")

    # Processing timestamps
    created_at: datetime = Field(default_factory=datetime.now, description="Record creation time")
    updated_at: datetime = Field(default_factory=datetime.now, description="Last update time")

    @field_validator("normalized_text")
    @classmethod
    def validate_normalized_text(cls, v: str) -> str:
        """Ensure normalized text is not empty."""
        if not v.strip():
            raise ValueError("Normalized text cannot be empty")
        return v.strip()

    @field_validator("unique_hash")
    @classmethod
    def validate_unique_hash(cls, v: str) -> str:
        """Ensure unique hash is valid."""
        if len(v) < 8:
            raise ValueError("Unique hash must be at least 8 characters")
        return v

    def to_training_dict(self) -> Dict[str, Union[str, float, int]]:
        """Convert to dictionary format for ML training."""
        return {
            "text": self.normalized_text,
            "label": self.sentiment_label.value if self.sentiment_label else None,
            "confidence": self.confidence_score,
            "video_id": self.metadata.video_id,
            "artist_name": self.metadata.artist_name,
            "music_domain": self.metadata.music_domain.value,
            "contains_music_slang": self.metadata.contains_music_slang,
            "token_count": self.token_count,
            "contains_emoji": self.contains_emoji,
        }


class MLDataset(BaseModel):
    """Collection of ML-ready comments with metadata."""

    dataset_id: str = Field(default_factory=lambda: str(uuid4()), description="Unique dataset ID")
    name: str = Field(..., description="Dataset name")
    description: str = Field("", description="Dataset description")
    version: str = Field("1.0", description="Dataset version")

    # Data
    comments: List[MLComment] = Field(default_factory=list, description="List of ML comments")

    # Statistics
    total_comments: int = Field(0, ge=0, description="Total number of comments")
    labeled_comments: int = Field(0, ge=0, description="Number of labeled comments")
    unique_videos: int = Field(0, ge=0, description="Number of unique videos")
    unique_artists: int = Field(0, ge=0, description="Number of unique artists")

    # Split information
    train_count: int = Field(0, ge=0, description="Training set size")
    validation_count: int = Field(0, ge=0, description="Validation set size")
    test_count: int = Field(0, ge=0, description="Test set size")

    # Quality metrics
    avg_confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Average label confidence")
    sentiment_distribution: Dict[str, int] = Field(default_factory=dict, description="Sentiment label distribution")

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now, description="Dataset creation time")
    updated_at: datetime = Field(default_factory=datetime.now, description="Last update time")

    def add_comment(self, comment: MLComment) -> None:
        """Add a comment to the dataset and update statistics."""
        self.comments.append(comment)
        self._update_statistics()

    def get_split(self, split: DataSplit) -> List[MLComment]:
        """Get comments for a specific data split."""
        return [c for c in self.comments if c.data_split == split]

    def get_labeled_comments(self) -> List[MLComment]:
        """Get only labeled comments."""
        return [c for c in self.comments if c.sentiment_label is not None]

    def get_music_domain_comments(self, domain: MusicDomain) -> List[MLComment]:
        """Get comments from a specific music domain."""
        return [c for c in self.comments if c.metadata.music_domain == domain]

    def _update_statistics(self) -> None:
        """Update dataset statistics."""
        self.total_comments = len(self.comments)
        self.labeled_comments = len(self.get_labeled_comments())

        # Count unique values
        unique_videos = set()
        unique_artists = set()

        # Count splits
        split_counts = {split: 0 for split in DataSplit}
        sentiment_counts = {label: 0 for label in SentimentLabel}
        confidences = []

        for comment in self.comments:
            unique_videos.add(comment.metadata.video_id)
            if comment.metadata.artist_name:
                unique_artists.add(comment.metadata.artist_name)

            split_counts[comment.data_split] += 1

            if comment.sentiment_label:
                sentiment_counts[comment.sentiment_label] += 1

            if comment.confidence_score is not None:
                confidences.append(comment.confidence_score)

        self.unique_videos = len(unique_videos)
        self.unique_artists = len(unique_artists)

        self.train_count = split_counts[DataSplit.TRAIN]
        self.validation_count = split_counts[DataSplit.VALIDATION]
        self.test_count = split_counts[DataSplit.TEST]

        self.sentiment_distribution = {k.value: v for k, v in sentiment_counts.items() if v > 0}
        self.avg_confidence = sum(confidences) / len(confidences) if confidences else None

        self.updated_at = datetime.now()


class TransformerConfig(BaseModel):
    """Configuration for transformer model preprocessing."""

    model_config = {"protected_namespaces": ()}  # Allow model_ prefix

    model_name: str = Field(..., description="Transformer model name")
    max_length: int = Field(512, ge=1, le=2048, description="Maximum sequence length")
    truncation: bool = Field(True, description="Enable truncation")
    padding: str = Field("max_length", description="Padding strategy")

    # Tokenization options
    add_special_tokens: bool = Field(True, description="Add special tokens")
    return_attention_mask: bool = Field(True, description="Return attention mask")
    return_token_type_ids: bool = Field(False, description="Return token type IDs")

    # Music-specific preprocessing
    preserve_music_slang: bool = Field(True, description="Preserve music slang terms")
    normalize_emoji: bool = Field(False, description="Normalize emoji to text")
    handle_mentions: bool = Field(True, description="Handle @mentions")

    @field_validator("model_name")
    @classmethod
    def validate_model_name(cls, v: str) -> str:
        """Validate transformer model name."""
        if not v.strip():
            raise ValueError("Model name cannot be empty")
        return v.strip()


class MLExportFormat(BaseModel):
    """Export format specification for ML data."""

    format_type: str = Field(..., description="Export format (csv, jsonl, parquet)")
    include_metadata: bool = Field(True, description="Include metadata in export")
    include_splits: bool = Field(True, description="Include data split information")

    # Column selection
    text_column: str = Field("text", description="Text column name")
    label_column: str = Field("label", description="Label column name")
    confidence_column: str = Field("confidence", description="Confidence column name")

    # Filtering options
    min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Minimum confidence threshold")
    include_unlabeled: bool = Field(False, description="Include unlabeled data")
    music_domains: Optional[List[MusicDomain]] = Field(None, description="Filter by music domains")

    @field_validator("format_type")
    @classmethod
    def validate_format_type(cls, v: str) -> str:
        """Validate export format type."""
        valid_formats = ["csv", "jsonl", "parquet", "json"]
        if v.lower() not in valid_formats:
            raise ValueError(f"Format must be one of: {valid_formats}")
        return v.lower()


class DataQualityReport(BaseModel):
    """Data quality assessment report."""

    dataset_id: str = Field(..., description="Dataset ID")
    report_timestamp: datetime = Field(default_factory=datetime.now, description="Report generation time")

    # Quality metrics
    total_samples: int = Field(..., ge=0, description="Total number of samples")
    valid_samples: int = Field(..., ge=0, description="Number of valid samples")
    invalid_samples: int = Field(..., ge=0, description="Number of invalid samples")

    # Validation issues
    empty_text_count: int = Field(0, ge=0, description="Comments with empty text")
    duplicate_count: int = Field(0, ge=0, description="Duplicate comments")
    low_confidence_count: int = Field(0, ge=0, description="Low confidence labels")

    # Distribution checks
    label_imbalance_score: float = Field(..., ge=0.0, le=1.0, description="Label distribution balance score")
    avg_text_length: float = Field(..., ge=0.0, description="Average text length")

    # Recommendations
    recommendations: List[str] = Field(default_factory=list, description="Data quality recommendations")

    def add_recommendation(self, recommendation: str) -> None:
        """Add a data quality recommendation."""
        self.recommendations.append(recommendation)

    def is_high_quality(self, min_valid_ratio: float = 0.95, max_imbalance: float = 0.8) -> bool:
        """Check if dataset meets high quality standards."""
        valid_ratio = self.valid_samples / self.total_samples if self.total_samples > 0 else 0
        return (
            valid_ratio >= min_valid_ratio and self.label_imbalance_score <= max_imbalance and self.duplicate_count == 0
        )
