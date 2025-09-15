"""
Bot Detection System for YouTube Comments

This module provides a production-ready bot suspicion scoring system that:
- Detects near-duplicate comments using TF-IDF + cosine similarity
- Identifies burst patterns in comment timing
- Analyzes author behavior patterns
- Provides interpretable 0-100 bot suspicion scores
- Respects human dignity with constructive language

Key Features:
- Fail-fast design with explicit error handling
- Unicode-safe text normalization
- Configurable whitelist for legitimate fan expressions
- Detailed feature breakdown for audit trails
- Optimized for production use with clear scaling paths
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, PositiveInt, field_validator
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import text


class BotDetectionConfig(BaseModel):
    """Configuration for bot detection system with validation."""

    # Legitimate fan expressions that shouldn't be flagged as suspicious
    whitelist_phrases: frozenset[str] = Field(
        default=frozenset(
            {
                "love this",
                "dope",
                "this is dope",
                "great song",
                "love u",
                "🔥",
                "🔥🔥",
                "🔥🔥🔥",
                "🔥🔥🔥🔥",
                "🔥🔥🔥🔥🔥",
                "🌊",
                "🌊🌊",
                "🌊🌊🌊",
                "🌊🌊🌊🌊",
                "🌊🌊🌊🌊🌊",
                "fire",
                "waves",
                "wavy",
                "this waves",
                "crazy",
                "this crazy",
                "this is crazy",
                "fye",
                "this beat is fye",
                "hard",
                "so hard",
                "too hard",
                "this hard",
                "this fire",
                "straight fire",
                "banger",
                "slaps",
                "goated",
                "amazing",
                "incredible",
                "beautiful",
                "perfect",
                "masterpiece",
            }
        )
    )

    # Similarity threshold for near-duplicate detection
    near_dupe_threshold: float = Field(default=0.90, ge=0.5, le=0.999)

    # Minimum cluster size to consider as suspicious
    min_dupe_cluster: PositiveInt = Field(default=3)

    # Time window for burst detection (seconds)
    burst_window_seconds: PositiveInt = Field(default=30)

    # Maximum emoji bonus to prevent over-adjustment
    emoji_max_weight: float = Field(default=0.15, ge=0.0, le=0.5)

    # Feature weights for final score calculation
    w_dupe_local: float = 0.40  # Same text within video
    w_dupe_global: float = 0.20  # Same text across videos
    w_burstiness: float = 0.20  # Timing patterns
    w_author_diversity: float = 0.15  # Author repetition
    w_low_engagement: float = 0.05  # Engagement patterns

    # Character n-gram range for similarity detection
    ngram_min: PositiveInt = 3
    ngram_max: PositiveInt = 5

    @field_validator("near_dupe_threshold")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if v < 0.5 or v >= 1.0:
            raise ValueError("near_dupe_threshold must be in [0.5, 1.0)")
        return v


# Text processing utilities
_whitespace_re = re.compile(r"\s+", flags=re.UNICODE)
_emoji_re = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+", flags=re.UNICODE)


def _normalize_text(text: str) -> str:
    """
    Unicode-safe text normalization for consistent comparison.

    Uses NFKC normalization + casefold for proper Unicode handling.
    Preserves emojis for separate analysis.
    """
    if not text:
        return ""

    # Unicode normalization and case folding
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.casefold()

    # Collapse whitespace
    normalized = _whitespace_re.sub(" ", normalized).strip()

    return normalized


def _strip_emojis(text: str) -> str:
    """Remove emojis from text for content analysis."""
    # Remove emojis but preserve surrounding spaces
    result = _emoji_re.sub("", text)
    return result.strip()


def _count_emojis(text: str) -> int:
    """Count emoji characters in text."""
    return len("".join(_emoji_re.findall(text)))


def _clamp_01(value):
    """Clamp value(s) to [0, 1] range. Works with scalars and pandas Series."""
    if hasattr(value, "clip"):  # pandas Series
        return value.clip(0.0, 1.0)
    else:  # scalar
        return min(max(value, 0.0), 1.0)


@dataclass(frozen=True)
class BotDetector:
    """
    Production-ready bot detection system for YouTube comments.

    Provides interpretable bot suspicion scores based on:
    - Text similarity patterns
    - Timing analysis
    - Author behavior
    - Engagement patterns
    """

    config: BotDetectionConfig

    def analyze_comments(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze comments and return bot suspicion scores.

        Args:
            df: DataFrame with columns: comment_id, video_id, comment_text,
                author_name, like_count, published_at

        Returns:
            DataFrame with bot_score (0-100) and interpretable features

        Raises:
            ValueError: If required columns are missing or data is invalid
        """
        required_cols = {"comment_id", "video_id", "comment_text", "author_name", "like_count", "published_at"}

        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {sorted(missing_cols)}")

        if df.empty:
            raise ValueError("Input DataFrame is empty")

        # Create working copy
        analysis_df = df.copy()

        # Validate and normalize timestamps
        analysis_df["published_at"] = pd.to_datetime(analysis_df["published_at"], utc=True, errors="coerce")

        invalid_timestamps = analysis_df["published_at"].isna()
        if invalid_timestamps.any():
            invalid_ids = analysis_df.loc[invalid_timestamps, "comment_id"].head(5).tolist()
            raise ValueError(f"Invalid timestamps found in comments: {invalid_ids}")

        # Text normalization
        analysis_df["text_normalized"] = analysis_df["comment_text"].astype(str).apply(_normalize_text)

        analysis_df["text_no_emoji"] = analysis_df["text_normalized"].apply(_strip_emojis)

        analysis_df["emoji_count"] = analysis_df["text_normalized"].apply(_count_emojis)

        # Whitelist detection - check if any whitelist phrase is contained in the text
        whitelist = self.config.whitelist_phrases

        def _contains_whitelist_phrase(text: str) -> bool:
            """Check if text contains any whitelisted phrase."""
            if not text:
                return False
            return any(phrase in text for phrase in whitelist)

        analysis_df["is_whitelisted"] = analysis_df["text_normalized"].apply(_contains_whitelist_phrase) | analysis_df[
            "text_no_emoji"
        ].apply(_contains_whitelist_phrase)

        # Feature engineering
        analysis_df = self._add_similarity_features(analysis_df)
        analysis_df = self._add_timing_features(analysis_df)
        analysis_df = self._add_author_features(analysis_df)
        analysis_df = self._add_engagement_features(analysis_df)

        # Calculate final bot score
        analysis_df = self._calculate_bot_score(analysis_df)

        # Return interpretable results
        result_cols = [
            "comment_id",
            "video_id",
            "author_name",
            "comment_text",
            "bot_score",
            "bot_risk_level",
            "duplicate_count_local",
            "duplicate_count_global",
            "burst_score",
            "author_repetition_score",
            "engagement_score",
            "emoji_count",
            "is_whitelisted",
        ]

        return analysis_df[result_cols].sort_values("bot_score", ascending=False)

    def _add_similarity_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add near-duplicate detection features using TF-IDF similarity."""

        def _count_similar_comments(group: pd.DataFrame) -> pd.Series:
            """Count similar comments within a group using TF-IDF + cosine similarity."""
            texts = group["text_no_emoji"].tolist()

            if len(texts) <= 1:
                return pd.Series([1], index=group.index)

            # Use character n-grams for short comment similarity
            vectorizer = TfidfVectorizer(
                analyzer="char",
                ngram_range=(self.config.ngram_min, self.config.ngram_max),
                max_features=10000,  # Limit for performance
            )

            try:
                tfidf_matrix = vectorizer.fit_transform(texts)
                similarity_matrix = cosine_similarity(tfidf_matrix, dense_output=False)

                # Count neighbors above threshold (including self)
                counts = []
                threshold = self.config.near_dupe_threshold

                for i in range(similarity_matrix.shape[0]):
                    row = similarity_matrix[i]
                    similar_count = int((row.data >= threshold).sum())
                    counts.append(similar_count)

                return pd.Series(counts, index=group.index)

            except ValueError:
                # Handle edge cases (empty texts, etc.)
                return pd.Series([1] * len(group), index=group.index)

        # Local similarity (within video)
        local_counts = []
        for video_id, group in df.groupby("video_id"):
            counts = _count_similar_comments(group)
            local_counts.append(counts)

        if local_counts:
            df["duplicate_count_local"] = pd.concat(local_counts).reindex(df.index).fillna(1)
        else:
            df["duplicate_count_local"] = 1

        # Global similarity (across videos, bucketed for performance)
        df["_text_bucket"] = df["text_no_emoji"].apply(lambda x: (x[:1], len(x) // 5) if x else ("", 0))

        def _count_similar_global(group: pd.DataFrame) -> pd.Series:
            if len(group) > 5000:  # Prevent quadratic explosion
                raise RuntimeError(
                    f"Text bucket too large ({len(group)} items). " "Consider implementing LSH for scale."
                )
            return _count_similar_comments(group)

        global_counts = []
        for text_bucket, group in df.groupby("_text_bucket"):
            counts = _count_similar_global(group)
            global_counts.append(counts)
        if global_counts:
            df["duplicate_count_global"] = pd.concat(global_counts).reindex(df.index).fillna(1)
        else:
            df["duplicate_count_global"] = 1

        # Filter out small clusters (but keep pairs as they're still suspicious)
        df.loc[df["duplicate_count_local"] < 2, "duplicate_count_local"] = 0
        df.loc[df["duplicate_count_global"] < 2, "duplicate_count_global"] = 0

        df.drop(columns=["_text_bucket"], inplace=True)
        return df

    def _add_timing_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add burst detection features based on comment timing."""

        def _calculate_burst_score(group: pd.DataFrame) -> pd.Series:
            """Calculate burst score for comments with same text."""
            if len(group) <= 1:
                return pd.Series([0.0], index=group.index)

            # Sort by timestamp
            sorted_group = group.sort_values("published_at")
            timestamps = sorted_group["published_at"].values

            window = timedelta(seconds=self.config.burst_window_seconds)
            burst_scores = []

            for i, current_time in enumerate(timestamps):
                # Count comments within time window
                try:
                    window_start = current_time - window
                    window_count = sum(1 for t in timestamps if window_start <= t <= current_time)
                except TypeError:
                    # Handle timezone-naive timestamps
                    current_ts = pd.Timestamp(current_time)
                    window_start = current_ts - window
                    window_count = sum(
                        1 for t in timestamps if pd.Timestamp(t) >= window_start and pd.Timestamp(t) <= current_ts
                    )

                # Normalize to 0-1 scale (10+ comments in window = max burst)
                burst_score = min((window_count - 1) / 9.0, 1.0)
                burst_scores.append(max(burst_score, 0.0))

            return pd.Series(burst_scores, index=sorted_group.index)

        burst_scores = []
        for (video_id, text_content), group in df.groupby(["video_id", "text_no_emoji"]):
            scores = _calculate_burst_score(group)
            burst_scores.append(scores)
        if burst_scores:
            df["burst_score"] = pd.concat(burst_scores).reindex(df.index).fillna(0.0)
        else:
            df["burst_score"] = 0.0

        return df

    def _add_author_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add author behavior analysis features."""

        # Calculate text diversity per author
        author_stats = df.groupby("author_name").agg({"text_no_emoji": ["nunique", "count"]}).round(3)

        author_stats.columns = ["unique_texts", "total_comments"]
        author_stats["text_diversity"] = author_stats["unique_texts"] / author_stats["total_comments"]

        # Map back to original dataframe
        df["author_repetition_score"] = (1.0 - df["author_name"].map(author_stats["text_diversity"])).fillna(0.0)

        return df

    def _add_engagement_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add engagement-based features."""

        # Normalize like counts (low engagement can be suspicious)
        like_counts = df["like_count"].fillna(0).astype(float)
        df["engagement_score"] = 1.0 - np.tanh(like_counts / 3.0)

        return df

    def _calculate_bot_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate final bot suspicion score with interpretable components."""

        # Component scores (0-1 scale)
        def _duplicate_component(count_series: pd.Series, is_whitelisted: pd.Series) -> pd.Series:
            """Convert duplicate counts to suspicion score."""
            base_score = np.clip((count_series - self.config.min_dupe_cluster) / 7.0, 0.0, 1.0)
            # Reduce suspicion for whitelisted phrases
            return base_score * np.where(is_whitelisted, 0.15, 1.0)

        f_dupe_local = _duplicate_component(df["duplicate_count_local"], df["is_whitelisted"])
        f_dupe_global = _duplicate_component(df["duplicate_count_global"], df["is_whitelisted"])
        f_burst = _clamp_01(df["burst_score"])
        f_author = _clamp_01(df["author_repetition_score"])
        f_engagement = _clamp_01(df["engagement_score"])

        # Emoji bonus (expressive comments are more human-like)
        emoji_bonus = np.minimum(df["emoji_count"] / 5.0, self.config.emoji_max_weight)

        # Weighted combination
        cfg = self.config
        raw_score = (
            cfg.w_dupe_local * f_dupe_local
            + cfg.w_dupe_global * f_dupe_global
            + cfg.w_burstiness * f_burst
            + cfg.w_author_diversity * f_author
            + cfg.w_low_engagement * f_engagement
            - emoji_bonus * 0.15  # Small deduction for emoji use
        )

        # Normalize to 0-100 scale
        if raw_score.max() > raw_score.min():
            normalized_score = (raw_score - raw_score.min()) / (raw_score.max() - raw_score.min())
        else:
            normalized_score = pd.Series([0.0] * len(raw_score), index=raw_score.index)

        df["bot_score"] = (normalized_score * 100.0).round(2)

        # Risk level categorization
        df["bot_risk_level"] = pd.cut(
            df["bot_score"], bins=[0, 30, 70, 100], labels=["Low", "Medium", "High"], include_lowest=True
        )

        return df


def load_recent_comments(engine, days: int = 30) -> pd.DataFrame:
    """
    Load recent comments from database for bot analysis.

    Args:
        engine: SQLAlchemy engine
        days: Number of days to look back

    Returns:
        DataFrame with comment data ready for bot analysis
    """
    cutoff_date = pd.Timestamp.utcnow() - pd.Timedelta(days=days)

    query = """
        SELECT
            yc.comment_id,
            yc.video_id,
            yc.comment_text,
            yc.author_name,
            yc.like_count,
            yc.published_at,
            yv.title as video_title,
            yv.channel_title
        FROM youtube_comments yc
        JOIN youtube_videos yv ON yc.video_id = yv.video_id
        WHERE yc.published_at >= :cutoff_date
        AND yc.comment_text IS NOT NULL
        AND yc.comment_text != ''
        ORDER BY yc.published_at DESC
    """

    return pd.read_sql(text(query), engine, params={"cutoff_date": cutoff_date})


def analyze_bot_patterns(engine, config: Optional[BotDetectionConfig] = None, days: int = 30) -> pd.DataFrame:
    """
    Convenience function to load and analyze comments for bot patterns.

    Args:
        engine: SQLAlchemy engine
        config: Bot detection configuration (uses defaults if None)
        days: Days of comment history to analyze

    Returns:
        DataFrame with bot analysis results
    """
    if config is None:
        config = BotDetectionConfig()

    # Load recent comments
    comments_df = load_recent_comments(engine, days=days)

    if comments_df.empty:
        raise ValueError(f"No comments found in the last {days} days")

    # Analyze for bot patterns
    detector = BotDetector(config=config)
    results = detector.analyze_comments(comments_df)

    return results


def store_bot_analysis(engine, analysis_df: pd.DataFrame, table_name: str = "comment_bot_analysis") -> None:
    """
    Store bot analysis results to database.

    Args:
        engine: SQLAlchemy engine
        analysis_df: Results from bot analysis
        table_name: Target table name
    """
    # Prepare data for storage
    storage_df = analysis_df.copy()
    storage_df["analyzed_at"] = pd.Timestamp.utcnow()

    # Store results
    storage_df.to_sql(table_name, engine, if_exists="replace", index=False, method="multi")

    print(f"✅ Stored {len(storage_df)} bot analysis records to {table_name}")
