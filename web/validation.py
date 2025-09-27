#!/usr / bin / env python3
"""
Validation Utilities for YouTube ETL Pipeline

This module provides comprehensive validation functions using Pydantic models.
All validation follows fail - fast principles with clear error messages.

Key Features:
- Input validation at API boundaries
- Data integrity validation for database operations
- Configuration validation with detailed error reporting
- Batch validation for performance
"""

import re
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from pydantic import ValidationError as PydanticValidationError

from web.error_handling import ErrorContext, ValidationError, get_error_handler
from web.models import (
    BotDetectionResult,
    ETLConfig,
    SentimentResult,
    VideoFilter,
    YouTubeComment,
    YouTubeVideo,
)


class DataValidator:
    """Comprehensive data validation using Pydantic models."""

    def __init__(self):
        self.error_handler = get_error_handler()

    def validate_youtube_video(self, video_data: Dict[str, Any]) -> YouTubeVideo:
        """
        Validate YouTube video data and return validated model.

        Args:
            video_data: Raw video data dictionary

        Returns:
            Validated YouTubeVideo model

        Raises:
            ValidationError: If validation fails
        """
        try:
            return YouTubeVideo(**video_data)
        except PydanticValidationError as e:
            context = ErrorContext(
                component="data_validator",
                operation="validate_youtube_video",
                user_data={"video_id": video_data.get("video_id", "unknown"), "validation_errors": str(e)},
            )
            raise ValidationError(
                f"YouTube video validation failed: {str(e)}",
                field="video_data",
                value=video_data,
                context=context,
                original_error=e,
            )

    def validate_youtube_videos_batch(
        self, videos_data: List[Dict[str, Any]]
    ) -> Tuple[List[YouTubeVideo], List[Dict[str, Any]]]:
        """
        Validate a batch of YouTube videos, returning valid and invalid separately.

        Args:
            videos_data: List of raw video data dictionaries

        Returns:
            Tuple of (valid_videos, invalid_videos_with_errors)
        """
        valid_videos = []
        invalid_videos = []

        for i, video_data in enumerate(videos_data):
            try:
                valid_video = self.validate_youtube_video(video_data)
                valid_videos.append(valid_video)
            except ValidationError as e:
                invalid_videos.append(
                    {"index": i, "data": video_data, "error": str(e), "video_id": video_data.get("video_id", "unknown")}
                )

                # Log validation error but don't stop processing
                self.error_handler.handle_error(e, should_raise=False)

        return valid_videos, invalid_videos

    def validate_youtube_comment(self, comment_data: Dict[str, Any]) -> YouTubeComment:
        """
        Validate YouTube comment data and return validated model.

        Args:
            comment_data: Raw comment data dictionary

        Returns:
            Validated YouTubeComment model

        Raises:
            ValidationError: If validation fails
        """
        try:
            return YouTubeComment(**comment_data)
        except PydanticValidationError as e:
            context = ErrorContext(
                component="data_validator",
                operation="validate_youtube_comment",
                user_data={
                    "comment_id": comment_data.get("comment_id", "unknown"),
                    "video_id": comment_data.get("video_id", "unknown"),
                    "validation_errors": str(e),
                },
            )
            raise ValidationError(
                f"YouTube comment validation failed: {str(e)}",
                field="comment_data",
                value=comment_data,
                context=context,
                original_error=e,
            )

    def validate_youtube_comments_batch(
        self, comments_data: List[Dict[str, Any]]
    ) -> Tuple[List[YouTubeComment], List[Dict[str, Any]]]:
        """
        Validate a batch of YouTube comments, returning valid and invalid separately.

        Args:
            comments_data: List of raw comment data dictionaries

        Returns:
            Tuple of (valid_comments, invalid_comments_with_errors)
        """
        valid_comments = []
        invalid_comments = []

        for i, comment_data in enumerate(comments_data):
            try:
                valid_comment = self.validate_youtube_comment(comment_data)
                valid_comments.append(valid_comment)
            except ValidationError as e:
                invalid_comments.append(
                    {
                        "index": i,
                        "data": comment_data,
                        "error": str(e),
                        "comment_id": comment_data.get("comment_id", "unknown"),
                    }
                )

                # Log validation error but don't stop processing
                self.error_handler.handle_error(e, should_raise=False)

        return valid_comments, invalid_comments

    def validate_sentiment_result(self, result_data: Dict[str, Any]) -> SentimentResult:
        """
        Validate sentiment analysis result.

        Args:
            result_data: Raw sentiment result dictionary

        Returns:
            Validated SentimentResult model

        Raises:
            ValidationError: If validation fails
        """
        try:
            return SentimentResult(**result_data)
        except PydanticValidationError as e:
            context = ErrorContext(
                component="data_validator",
                operation="validate_sentiment_result",
                user_data={
                    "comment_id": result_data.get("comment_id", "unknown"),
                    "sentiment_score": result_data.get("sentiment_score"),
                    "validation_errors": str(e),
                },
            )
            raise ValidationError(
                f"Sentiment result validation failed: {str(e)}",
                field="sentiment_result",
                value=result_data,
                context=context,
                original_error=e,
            )

    def validate_bot_detection_result(self, result_data: Dict[str, Any]) -> BotDetectionResult:
        """
        Validate bot detection result.

        Args:
            result_data: Raw bot detection result dictionary

        Returns:
            Validated BotDetectionResult model

        Raises:
            ValidationError: If validation fails
        """
        try:
            return BotDetectionResult(**result_data)
        except PydanticValidationError as e:
            context = ErrorContext(
                component="data_validator",
                operation="validate_bot_detection_result",
                user_data={
                    "comment_id": result_data.get("comment_id", "unknown"),
                    "bot_score": result_data.get("bot_score"),
                    "validation_errors": str(e),
                },
            )
            raise ValidationError(
                f"Bot detection result validation failed: {str(e)}",
                field="bot_result",
                value=result_data,
                context=context,
                original_error=e,
            )

    def validate_etl_config(self, config_data: Dict[str, Any]) -> ETLConfig:
        """
        Validate ETL configuration.

        Args:
            config_data: Raw configuration dictionary

        Returns:
            Validated ETLConfig model

        Raises:
            ValidationError: If validation fails
        """
        try:
            return ETLConfig(**config_data)
        except PydanticValidationError as e:
            context = ErrorContext(
                component="data_validator",
                operation="validate_etl_config",
                user_data={"config_keys": list(config_data.keys()), "validation_errors": str(e)},
            )
            raise ValidationError(
                f"ETL configuration validation failed: {str(e)}",
                field="etl_config",
                value=config_data,
                context=context,
                original_error=e,
            )

    def validate_video_filter(self, filter_data: Dict[str, Any]) -> VideoFilter:
        """
        Validate video filter configuration.

        Args:
            filter_data: Raw filter configuration dictionary

        Returns:
            Validated VideoFilter model

        Raises:
            ValidationError: If validation fails
        """
        try:
            return VideoFilter(**filter_data)
        except PydanticValidationError as e:
            context = ErrorContext(
                component="data_validator",
                operation="validate_video_filter",
                user_data={"filter_keys": list(filter_data.keys()), "validation_errors": str(e)},
            )
            raise ValidationError(
                f"Video filter validation failed: {str(e)}",
                field="video_filter",
                value=filter_data,
                context=context,
                original_error=e,
            )


class DatabaseValidator:
    """Database - specific validation utilities."""

    def __init__(self):
        self.error_handler = get_error_handler()

    def validate_video_id_format(self, video_id: str) -> bool:
        """
        Validate YouTube video ID format.

        Args:
            video_id: Video ID to validate

        Returns:
            True if valid format

        Raises:
            ValidationError: If format is invalid
        """
        if not video_id or not isinstance(video_id, str):
            raise ValidationError("Video ID must be a non - empty string", field="video_id", value=video_id)

        if not re.match(r"^[a - zA - Z0 - 9_-]{11}$", video_id):
            raise ValidationError(f"Invalid YouTube video ID format: {video_id}", field="video_id", value=video_id)

        return True

    def validate_isrc_format(self, isrc: str) -> bool:
        """
        Validate ISRC code format.

        Args:
            isrc: ISRC code to validate

        Returns:
            True if valid format

        Raises:
            ValidationError: If format is invalid
        """
        if not isrc or not isinstance(isrc, str):
            raise ValidationError("ISRC must be a non - empty string", field="isrc", value=isrc)

        isrc_upper = isrc.upper().strip()
        if not re.match(r"^[A - Z]{2}[A - Z0 - 9]{3}[0 - 9]{2}[0 - 9]{5}$", isrc_upper):
            raise ValidationError(
                f"Invalid ISRC format: {isrc}. Expected format: CC - XXX - YY - NNNNN", field="isrc", value=isrc
            )

        return True

    def validate_dataframe_structure(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate DataFrame has required columns and basic structure.

        Args:
            df: DataFrame to validate
            required_columns: List of required column names

        Returns:
            True if valid structure

        Raises:
            ValidationError: If structure is invalid
        """
        if df is None:
            raise ValidationError("DataFrame cannot be None", field="dataframe", value=None)

        if df.empty:
            raise ValidationError("DataFrame cannot be empty", field="dataframe", value="empty_dataframe")

        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValidationError(
                f"DataFrame missing required columns: {sorted(missing_columns)}",
                field="dataframe_columns",
                value=list(df.columns),
            )

        return True

    def validate_sentiment_scores(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Validate sentiment scores in DataFrame.

        Args:
            df: DataFrame with sentiment scores

        Returns:
            Tuple of (valid_df, validation_errors)
        """
        validation_errors = []

        if "sentiment_score" not in df.columns:
            raise ValidationError(
                "DataFrame must contain 'sentiment_score' column", field="dataframe_columns", value=list(df.columns)
            )

        # Check for valid range
        invalid_scores = df[
            (df["sentiment_score"] < -1.0) | (df["sentiment_score"] > 1.0) | df["sentiment_score"].isna()
        ]

        if not invalid_scores.empty:
            validation_errors.append(f"Found {len(invalid_scores)} invalid sentiment scores (must be -1.0 to 1.0)")

        # Check confidence scores if present
        if "confidence_score" in df.columns:
            invalid_confidence = df[
                (df["confidence_score"] < 0.0) | (df["confidence_score"] > 1.0) | df["confidence_score"].isna()
            ]

            if not invalid_confidence.empty:
                validation_errors.append(
                    f"Found {len(invalid_confidence)} invalid confidence scores (must be 0.0 to 1.0)"
                )

        # Return valid rows only
        valid_df = df[(df["sentiment_score"] >= -1.0) & (df["sentiment_score"] <= 1.0) & df["sentiment_score"].notna()]

        if "confidence_score" in df.columns:
            valid_df = valid_df[
                (valid_df["confidence_score"] >= 0.0)
                & (valid_df["confidence_score"] <= 1.0)
                & valid_df["confidence_score"].notna()
            ]

        return valid_df, validation_errors


def validate_required_environment_variables(required_vars: List[str]) -> Dict[str, str]:
    """
    Validate that required environment variables are set and not empty.

    Args:
        required_vars: List of required environment variable names

    Returns:
        Dictionary of validated environment variables

    Raises:
        ValidationError: If any required variables are missing or empty
    """
    import os

    missing_vars = []
    empty_vars = []
    valid_vars = {}

    for var_name in required_vars:
        value = os.getenv(var_name)

        if value is None:
            missing_vars.append(var_name)
        elif not value.strip():
            empty_vars.append(var_name)
        else:
            valid_vars[var_name] = value.strip()

    if missing_vars or empty_vars:
        error_parts = []
        if missing_vars:
            error_parts.append(f"Missing variables: {', '.join(missing_vars)}")
        if empty_vars:
            error_parts.append(f"Empty variables: {', '.join(empty_vars)}")

        context = ErrorContext(
            component="environment_validator",
            operation="validate_required_environment_variables",
            user_data={"missing_vars": missing_vars, "empty_vars": empty_vars, "required_vars": required_vars},
        )

        raise ValidationError(
            f"Environment validation failed: {'; '.join(error_parts)}",
            field="environment_variables",
            value={"missing": missing_vars, "empty": empty_vars},
            context=context,
        )

    return valid_vars


def validate_database_connection_string(connection_string: str) -> bool:
    """
    Validate database connection string format.

    Args:
        connection_string: Database connection string

    Returns:
        True if valid format

    Raises:
        ValidationError: If format is invalid
    """
    if not connection_string or not connection_string.strip():
        raise ValidationError(
            "Database connection string cannot be empty", field="database_url", value=connection_string
        )

    # Check for supported database schemes
    supported_schemes = ["mysql + pymysql://", "postgresql://", "sqlite:///"]

    if not any(connection_string.startswith(scheme) for scheme in supported_schemes):
        raise ValidationError(
            f"Database connection string must start with one of: {supported_schemes}",
            field="database_url",
            value=connection_string,
        )

    # Basic format validation for MySQL
    if connection_string.startswith("mysql + pymysql://"):
        # Expected format: mysql + pymysql://user:password@host:port / database
        pattern = r"^mysql\+pymysql://[^:]+:[^@]+@[^:]+:\d+/[^/]+$"
        if not re.match(pattern, connection_string):
            raise ValidationError(
                "Invalid MySQL connection string format. Expected: mysql +"
                " pymysql://user:password@host:port / database",
                field="database_url",
                value=connection_string,
            )

    return True


# Global validator instances
_data_validator = DataValidator()
_database_validator = DatabaseValidator()


def get_data_validator() -> DataValidator:
    """Get the global data validator instance."""
    return _data_validator


def get_database_validator() -> DatabaseValidator:
    """Get the global database validator instance."""
    return _database_validator
