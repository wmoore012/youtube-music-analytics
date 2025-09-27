#!/usr / bin / env python3
"""
Video Filtering System for YouTube ETL Pipeline

This module provides comprehensive video filtering at the API level to prevent
problematic videos from entering the database. It implements configuration - driven
filtering rules and logs all filtering decisions with clear reasoning.

Key Features:
- Filter videos before database insertion (fail - fast approach)
- Configuration - driven filtering rules from .env and config files
- Comprehensive logging of filtering decisions
- Support for multiple filtering criteria (video IDs, channel IDs, patterns, etc.)
- Personal issue handling (as mentioned in requirements)
- Validation using Pydantic models

Design Principles:
- Filter at API boundary, not in database
- Log all filtering decisions with clear reasoning
- Use natural keys and meaningful filter names
- Fail loudly when configuration is invalid
- Support both blacklist and whitelist approaches
"""

from datetime import datetime
from enum import Enum
import json
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field, field_validator

from web.error_handling import ErrorContext, ValidationError, get_error_handler
from web.models import VideoFilter, YouTubeVideo


class FilterReason(str, Enum):
    """Reasons why a video was filtered."""

    BLOCKED_VIDEO_ID = "blocked_video_id"
    BLOCKED_CHANNEL_ID = "blocked_channel_id"
    BLOCKED_TITLE_PATTERN = "blocked_title_pattern"
    DURATION_TOO_SHORT = "duration_too_short"
    DURATION_TOO_LONG = "duration_too_long"
    MISSING_ISRC_REQUIRED = "missing_isrc_required"
    PERSONAL_ISSUE = "personal_issue"  # For the 4 problematic videos mentioned
    QUALITY_ISSUE = "quality_issue"
    INVALID_DATA = "invalid_data"


class FilterResult(BaseModel):
    """Result of video filtering operation."""

    video_id: str = Field(..., description="YouTube video ID")
    is_filtered: bool = Field(..., description="Whether video was filtered out")
    reason: Optional[FilterReason] = Field(None, description="Reason for filtering")
    details: str = Field("", description="Additional details about filtering decision")
    filter_rule: Optional[str] = Field(None, description="Name of filter rule that matched")

    class Config:
        """Pydantic configuration."""

        use_enum_values = True


class FilterStats(BaseModel):
    """Statistics about filtering operation."""

    total_videos: int = Field(0, ge=0, description="Total videos processed")
    filtered_videos: int = Field(0, ge=0, description="Videos filtered out")
    passed_videos: int = Field(0, ge=0, description="Videos that passed filtering")
    filter_reasons: Dict[str, int] = Field(default_factory=dict, description="Count by filter reason")

    @property
    def filter_rate(self) -> float:
        """Calculate filtering rate as percentage."""
        if self.total_videos == 0:
            return 0.0
        return (self.filtered_videos / self.total_videos) * 100.0

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class VideoFilterEngine:
    """
    Main video filtering engine that applies multiple filtering rules.

    This class implements the filtering logic and maintains statistics
    about filtering decisions.
    """

    def __init__(self, filter_config: VideoFilter):
        """
        Initialize the filter engine with configuration.

        Args:
            filter_config: Video filter configuration
        """
        self.config = filter_config
        self.error_handler = get_error_handler()
        self.stats = FilterStats()

        # Load personal issue video IDs (the 4 problematic videos mentioned)
        self.personal_issue_videos = self._load_personal_issue_videos()

        # Compile regex patterns for efficiency
        self.title_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in filter_config.blocked_title_patterns]

    def _load_from_config_file(self, config_file: str) -> Optional[Set[str]]:
        """Load video IDs from configuration file."""
        if not os.path.exists(config_file):
            return None

        try:
            with open(config_file, "r") as f:
                data = json.load(f)
                video_ids = set(data.get("video_ids", []))
                if video_ids:
                    print(f"📋 Loaded {len(video_ids)} personal issue video IDs from config file")
                    return video_ids
        except Exception as e:
            self.error_handler.handle_error(
                ValidationError(
                    f"Failed to load personal issue videos config: {str(e)}",
                    field="config_file",
                    value=config_file,
                    context=ErrorContext(component="video_filter", operation="load_personal_issue_videos"),
                ),
                should_raise=False,
            )
        return None

    def _load_from_environment(self) -> Optional[Set[str]]:
        """Load video IDs from environment variable."""
        env_ids = os.getenv("PERSONAL_ISSUE_VIDEO_IDS", "")
        if env_ids:
            video_ids = set(vid.strip() for vid in env_ids.split(",") if vid.strip())
            if video_ids:
                print(f"📋 Loaded {len(video_ids)} personal issue video IDs from environment")
                return video_ids
        return None

    def _get_default_issue_videos(self) -> Set[str]:
        """Get default problematic video IDs as fallback."""
        default_ids = {
            "dQw4w9WgXcQ",  # Example - replace with actual problematic video IDs
            "oHg5SJYRHA0",  # Example - replace with actual problematic video IDs
            "kJQP7kiw5Fk",  # Example - replace with actual problematic video IDs
            "fC7oUOUEEi4",  # Example - replace with actual problematic video IDs
        }
        print(f"⚠️ Using default personal issue video IDs - configure actual"
            " IDs in config / personal_issue_videos.json")  # noqa: E128
        return default_ids

    def _load_personal_issue_videos(self) -> Set[str]:
        """
        Load the list of problematic video IDs that need to be filtered.

        These are the "4 videos that need to be deleted every time we run ETL"
        mentioned in the requirements. We filter them at the API level instead.

        Returns:
            Set of video IDs to filter out
        """
        # Try config file first
        config_file = "config / personal_issue_videos.json"
        video_ids = self._load_from_config_file(config_file)
        if video_ids:
            return video_ids

        # Fallback to environment variable
        video_ids = self._load_from_environment()
        if video_ids:
            return video_ids

        # Use defaults as last resort
        return self._get_default_issue_videos()

    def _parse_duration(self, duration_str: str) -> Optional[int]:
        """
        Parse ISO 8601 duration string to seconds.

        Args:
            duration_str: Duration in ISO 8601 format (e.g., "PT4M13S")

        Returns:
            Duration in seconds, or None if parsing fails
        """
        if not duration_str:
            return None

        try:
            # Parse ISO 8601 duration format: PT4M13S
            match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_str)
            if not match:
                return None

            hours = int(match.group(1) or 0)
            minutes = int(match.group(2) or 0)
            seconds = int(match.group(3) or 0)

            return hours * 3600 + minutes * 60 + seconds
        except (ValueError, AttributeError):
            return None

    def _check_personal_issues(self, video: YouTubeVideo) -> Optional[FilterResult]:
        """Check if video is in personal issue list."""
        if video.video_id in self.personal_issue_videos:
            return FilterResult(
                video_id=video.video_id,
                is_filtered=True,
                reason=FilterReason.PERSONAL_ISSUE,
                details="Video ID is in personal issue list - known problematic video",
                filter_rule="personal_issue_videos",
            )
        return None

    def _check_blocked_ids(self, video: YouTubeVideo) -> Optional[FilterResult]:
        """Check if video or channel ID is blocked."""
        # Check blocked video IDs
        if video.video_id in self.config.blocked_video_ids:
            return FilterResult(
                video_id=video.video_id,
                is_filtered=True,
                reason=FilterReason.BLOCKED_VIDEO_ID,
                details=f"Video ID {video.video_id} is in blocked list",
                filter_rule="blocked_video_ids",
            )

        # Check blocked channel IDs
        if video.channel_id in self.config.blocked_channel_ids:
            return FilterResult(
                video_id=video.video_id,
                is_filtered=True,
                reason=FilterReason.BLOCKED_CHANNEL_ID,
                details=f"Channel ID {video.channel_id} is in blocked list",
                filter_rule="blocked_channel_ids",
            )
        return None

    def _check_title_patterns(self, video: YouTubeVideo) -> Optional[FilterResult]:
        """Check if video title matches blocked patterns."""
        for i, pattern in enumerate(self.title_patterns):
            if pattern.search(video.title):
                return FilterResult(
                    video_id=video.video_id,
                    is_filtered=True,
                    reason=FilterReason.BLOCKED_TITLE_PATTERN,
                    details=f"Title matches blocked pattern: {self.config.blocked_title_patterns[i]}",
                    filter_rule=f"blocked_title_pattern_{i}",
                )
        return None

    def _check_duration_constraints(self, video: YouTubeVideo) -> Optional[FilterResult]:
        """Check if video duration meets constraints."""
        if not video.duration:
            return None

        duration_seconds = self._parse_duration(video.duration)
        if duration_seconds is None:
            return None

        if self.config.min_duration_seconds and duration_seconds < self.config.min_duration_seconds:
            return FilterResult(
                video_id=video.video_id,
                is_filtered=True,
                reason=FilterReason.DURATION_TOO_SHORT,
                details=f"Duration {duration_seconds}s is less than minimum {self.config.min_duration_seconds}s",
                filter_rule="min_duration_seconds",
            )

        if self.config.max_duration_seconds and duration_seconds > self.config.max_duration_seconds:
            return FilterResult(
                video_id=video.video_id,
                is_filtered=True,
                reason=FilterReason.DURATION_TOO_LONG,
                details=f"Duration {duration_seconds}s exceeds maximum {self.config.max_duration_seconds}s",
                filter_rule="max_duration_seconds",
            )
        return None

    def _check_isrc_requirement(self, video: YouTubeVideo) -> Optional[FilterResult]:
        """Check if ISRC requirement is met."""
        if self.config.require_isrc and not video.isrc:
            return FilterResult(
                video_id=video.video_id,
                is_filtered=True,
                reason=FilterReason.MISSING_ISRC_REQUIRED,
                details="ISRC is required but not present",
                filter_rule="require_isrc",
            )
        return None

    def filter_video(self, video: YouTubeVideo) -> FilterResult:
        """
        Apply all filtering rules to a single video.

        Args:
            video: YouTube video to filter

        Returns:
            FilterResult indicating whether video was filtered and why
        """
        # Apply filters in priority order
        filter_checks = [
            self._check_personal_issues,
            self._check_blocked_ids,
            self._check_title_patterns,
            self._check_duration_constraints,
            self._check_isrc_requirement,
        ]

        for check in filter_checks:
            result = check(video)
            if result:
                return result

        # Video passed all filters
        return FilterResult(video_id=video.video_id, is_filtered=False, details="Video passed all filtering rules")

    def _process_filter_result(
        self, video: YouTubeVideo, result: FilterResult, passed_videos: List[YouTubeVideo]
    ) -> None:
        """Process a successful filter result and update statistics."""
        if result.is_filtered:
            self.stats.filtered_videos += 1
            reason = result.reason or "unknown"
            self.stats.filter_reasons[reason] = self.stats.filter_reasons.get(reason, 0) + 1
            print(f"🚫 Filtered video {video.video_id}: {result.details}")
        else:
            passed_videos.append(video)
            self.stats.passed_videos += 1

        self.stats.total_videos += 1

    def _handle_filter_error(self, video: YouTubeVideo, error: Exception) -> FilterResult:
        """Handle errors during video filtering and update statistics."""
        error_result = FilterResult(
            video_id=video.video_id,
            is_filtered=True,
            reason=FilterReason.INVALID_DATA,
            details=f"Error during filtering: {str(error)}",
            filter_rule="error_handler",
        )

        # Update statistics
        self.stats.filtered_videos += 1
        self.stats.total_videos += 1
        self.stats.filter_reasons[FilterReason.INVALID_DATA] = (
            self.stats.filter_reasons.get(FilterReason.INVALID_DATA, 0) + 1
        )

        # Log error but don't stop processing
        self.error_handler.handle_error(
            ValidationError(
                f"Error filtering video {video.video_id}: {str(error)}",
                field="video_filtering",
                value=video.video_id,
                context=ErrorContext(
                    component="video_filter", operation="filter_videos", user_data={"video_id": video.video_id}
                ),
                original_error=error,
            ),
            should_raise=False,
        )

        return error_result

    def filter_videos(self, videos: List[YouTubeVideo]) -> Tuple[List[YouTubeVideo], List[FilterResult]]:
        """
        Filter a list of videos and return passed videos and filter results.

        Args:
            videos: List of YouTube videos to filter

        Returns:
            Tuple of (passed_videos, all_filter_results)
        """
        passed_videos = []
        filter_results = []

        for video in videos:
            try:
                result = self.filter_video(video)
                filter_results.append(result)
                self._process_filter_result(video, result, passed_videos)

            except Exception as e:
                error_result = self._handle_filter_error(video, e)
                filter_results.append(error_result)

        return passed_videos, filter_results

    def get_stats(self) -> FilterStats:
        """Get current filtering statistics."""
        return self.stats

    def reset_stats(self) -> None:
        """Reset filtering statistics."""
        self.stats = FilterStats()

    def log_summary(self) -> None:
        """Log a summary of filtering results."""
        stats = self.get_stats()

        print(f"\n📊 Video Filtering Summary:")
        print(f"   Total videos processed: {stats.total_videos:,}")
        print(f"   Videos passed: {stats.passed_videos:,}")
        print(f"   Videos filtered: {stats.filtered_videos:,}")
        print(f"   Filter rate: {stats.filter_rate:.1f}%")

        if stats.filter_reasons:
            print(f"   Filter reasons:")
            for reason, count in sorted(stats.filter_reasons.items()):
                print(f"     {reason}: {count:,}")


def _load_env_list(env_var: str, separator: str = ",") -> List[str]:
    """Load and parse a list from environment variable."""
    env_value = os.getenv(env_var, "")
    if not env_value:
        return []
    return [item.strip() for item in env_value.split(separator) if item.strip()]


def _load_env_int(env_var: str) -> Optional[int]:
    """Load and parse an integer from environment variable."""
    value = os.getenv(env_var)
    return int(value) if value else None


def _load_env_bool(env_var: str, default: bool = False) -> bool:
    """Load and parse a boolean from environment variable."""
    value = os.getenv(env_var, str(default)).lower()
    return value in ("true", "1", "yes")


def _load_config_from_env() -> Dict[str, Any]:
    """Load video filter configuration from environment variables."""
    return {
        "blocked_video_ids": _load_env_list("BLOCKED_VIDEO_IDS"),
        "blocked_channel_ids": _load_env_list("BLOCKED_CHANNEL_IDS"),
        "blocked_title_patterns": _load_env_list("BLOCKED_TITLE_PATTERNS", "|"),
        "min_duration_seconds": _load_env_int("MIN_VIDEO_DURATION_SECONDS"),
        "max_duration_seconds": _load_env_int("MAX_VIDEO_DURATION_SECONDS"),
        "require_isrc": _load_env_bool("REQUIRE_ISRC_FOR_VIDEOS"),
    }


def _merge_file_config(config_data: Dict[str, Any], config_file: str) -> None:
    """Merge configuration from file into existing config data."""
    if not os.path.exists(config_file):
        return

    try:
        with open(config_file, "r") as f:
            file_config = json.load(f)

        for key, value in file_config.items():
            if key in config_data and value:
                if isinstance(config_data[key], list):
                    config_data[key].extend(value)
                else:
                    config_data[key] = value

        print(f"📋 Loaded additional filter configuration from {config_file}")
    except Exception as e:
        print(f"⚠️ Warning: Could not load filter config file {config_file}: {e}")


def load_filter_config() -> VideoFilter:
    """
    Load video filter configuration from environment and config files.

    Returns:
        VideoFilter configuration object

    Raises:
        ValidationError: If configuration is invalid
    """
    try:
        # Load base configuration from environment
        config_data = _load_config_from_env()

        # Merge additional configuration from file
        _merge_file_config(config_data, "config / video_filter.json")

        # Create and validate configuration
        return VideoFilter(**config_data)

    except Exception as e:
        raise ValidationError(
            f"Failed to load video filter configuration: {str(e)}",
            field="filter_config",
            value=config_data if "config_data" in locals() else None,
            context=ErrorContext(component="video_filter", operation="load_filter_config"),
            original_error=e,
        )


def create_filter_engine() -> VideoFilterEngine:
    """
    Create a video filter engine with loaded configuration.

    Returns:
        Configured VideoFilterEngine instance
    """
    config = load_filter_config()
    return VideoFilterEngine(config)


# Global filter engine instance
_filter_engine: Optional[VideoFilterEngine] = None


def get_filter_engine() -> VideoFilterEngine:
    """Get the global filter engine instance."""
    global _filter_engine
    if _filter_engine is None:
        _filter_engine = create_filter_engine()
    return _filter_engine


def filter_videos_at_api_level(videos: List[YouTubeVideo]) -> Tuple[List[YouTubeVideo], List[FilterResult]]:
    """
    Convenience function to filter videos at API level.

    This is the main entry point for video filtering in the ETL pipeline.

    Args:
        videos: List of YouTube videos from API

    Returns:
        Tuple of (filtered_videos, filter_results)
    """
    filter_engine = get_filter_engine()
    passed_videos, filter_results = filter_engine.filter_videos(videos)

    # Log summary if any videos were filtered
    if filter_engine.get_stats().filtered_videos > 0:
        filter_engine.log_summary()

    return passed_videos, filter_results
