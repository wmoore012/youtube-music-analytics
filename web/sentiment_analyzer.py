#!/usr / bin / env python3
"""
Enhanced Sentiment Analysis System for YouTube Comments

This module provides a robust, bulletproof sentiment analysis system with:
- Pydantic models for data validation
- Comprehensive error handling and recovery
- Batch processing with progress tracking
- Performance benchmarking and timeout handling
- Multiple sentiment analysis methods with fallbacks
- Confidence threshold validation
- Detailed logging and monitoring

Key Features:
- Fail - fast validation using Pydantic models
- Graceful degradation when libraries are unavailable
- Batch processing with configurable batch sizes
- Progress tracking and performance metrics
- Comprehensive error handling with retry mechanisms
- Confidence - based result filtering
"""

from datetime import datetime, timedelta
from enum import Enum
import time
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.engine import Engine

from web.error_handling import (
    ErrorCategory,
    ErrorContext,
    ErrorSeverity,
    ETLError,
    get_error_handler,
    retry_with_backoff,
)
from web.models import SentimentMethod, SentimentResult, YouTubeComment


class SentimentAnalysisConfig(BaseModel):
    """Configuration for sentiment analysis with validation."""

    # Processing settings
    batch_size: int = Field(100, ge=1, le=10000, description="Batch size for processing")
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")
    timeout_seconds: int = Field(300, ge=1, le=3600, description="Processing timeout")

    # Quality thresholds
    confidence_threshold: float = Field(0.7, ge=0.0, le=1.0, description="Minimum confidence threshold")
    min_text_length: int = Field(3, ge=1, le=100, description="Minimum text length for analysis")
    max_text_length: int = Field(10000, ge=100, le=50000, description="Maximum text length for analysis")

    # Method preferences
    preferred_method: SentimentMethod = Field(SentimentMethod.AUTO, description="Preferred analysis method")
    enable_fallback: bool = Field(True, description="Enable fallback to simpler methods")

    # Performance settings
    enable_benchmarking: bool = Field(True, description="Enable performance benchmarking")
    progress_reporting: bool = Field(True, description="Enable progress reporting")

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True


class SentimentBenchmark(BaseModel):
    """Performance benchmark results for sentiment analysis."""

    total_comments: int = Field(0, ge=0, description="Total comments processed")
    processing_time_seconds: float = Field(0.0, ge=0.0, description="Total processing time")
    comments_per_second: float = Field(0.0, ge=0.0, description="Processing throughput")
    average_time_per_comment: float = Field(0.0, ge=0.0, description="Average time per comment")
    method_used: SentimentMethod = Field(..., description="Analysis method used")
    confidence_distribution: Dict[str, int] = Field(default_factory=dict, description="Confidence score distribution")
    error_count: int = Field(0, ge=0, description="Number of errors encountered")

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True


class SentimentAnalysisResult(BaseModel):
    """Complete result of sentiment analysis operation."""

    successful_results: List[SentimentResult] = Field(
        default_factory=list, description="Successfully processed results"
    )
    failed_comments: List[Dict[str, str]] = Field(default_factory=list, description="Failed comment processing")
    benchmark: SentimentBenchmark = Field(..., description="Performance benchmark")
    config: SentimentAnalysisConfig = Field(..., description="Configuration used")

    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        total = len(self.successful_results) + len(self.failed_comments)
        if total == 0:
            return 0.0
        return (len(self.successful_results) / total) * 100.0

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class SentimentAnalyzer:
    """
    Enhanced sentiment analyzer with robust error handling and validation.

    This class provides bulletproof sentiment analysis with:
    - Multiple analysis methods with automatic fallback
    - Comprehensive error handling and recovery
    - Performance benchmarking and monitoring
    - Batch processing with progress tracking
    - Confidence - based result validation
    """

    def __init__(self, config: Optional[SentimentAnalysisConfig] = None):
        """
        Initialize sentiment analyzer with configuration.

        Args:
            config: Sentiment analysis configuration (uses defaults if None)
        """
        self.config = config or SentimentAnalysisConfig()
        self.error_handler = get_error_handler()

        # Initialize available methods
        self._initialize_methods()

        # Performance tracking
        self._reset_benchmark()

    def _initialize_methods(self):
        """Initialize available sentiment analysis methods."""
        self.available_methods = {}

        # Try to initialize TextBlob
        try:
            from textblob import TextBlob

            self.available_methods[SentimentMethod.TEXTBLOB] = self._analyze_with_textblob
            print("✅ TextBlob sentiment analysis available")
        except ImportError:
            print("⚠️ TextBlob not available. Install with: pip install textblob")

        # Try to initialize VADER
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

            self._vader_analyzer = SentimentIntensityAnalyzer()
            self.available_methods[SentimentMethod.VADER] = self._analyze_with_vader
            print("✅ VADER sentiment analysis available")
        except ImportError:
            print("⚠️ VADER not available. Install with: pip install vaderSentiment")

        # Simple method is always available
        self.available_methods[SentimentMethod.SIMPLE] = self._analyze_with_simple
        print("✅ Simple rule - based sentiment analysis available")

        if not self.available_methods:
            raise ETLError(
                "No sentiment analysis methods available",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                context=ErrorContext(component="sentiment_analyzer", operation="initialize_methods"),
            )

    def _reset_benchmark(self):
        """Reset benchmark tracking."""
        self._benchmark_start_time = None
        self._benchmark_data = {
            "total_comments": 0,
            "processing_time_seconds": 0.0,
            "method_used": SentimentMethod.AUTO,
            "confidence_distribution": {"high": 0, "medium": 0, "low": 0},
            "error_count": 0,
        }

    def _get_best_available_method(self) -> SentimentMethod:
        """Get the best available sentiment analysis method."""
        if self.config.preferred_method != SentimentMethod.AUTO:
            if self.config.preferred_method in self.available_methods:
                return self.config.preferred_method
            elif not self.config.enable_fallback:
                raise ETLError(
                    f"Preferred method {self.config.preferred_method} not available and fallback disabled",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.CONFIGURATION,
                    context=ErrorContext(
                        component="sentiment_analyzer",
                        operation="get_best_available_method",
                        user_data={"preferred_method": self.config.preferred_method},
                    ),
                )

        # Auto - select best available method
        method_priority = [SentimentMethod.VADER, SentimentMethod.TEXTBLOB, SentimentMethod.SIMPLE]

        for method in method_priority:
            if method in self.available_methods:
                return method

        raise ETLError(
            "No sentiment analysis methods available",
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CONFIGURATION,
            context=ErrorContext(component="sentiment_analyzer", operation="get_best_available_method"),
        )

    def _validate_comment_text(self, text: str) -> bool:
        """Validate comment text for sentiment analysis."""
        if not text or not isinstance(text, str):
            return False

        text_clean = text.strip()

        if len(text_clean) < self.config.min_text_length:
            return False

        if len(text_clean) > self.config.max_text_length:
            return False

        return True

    def _analyze_with_textblob(self, text: str) -> Tuple[float, float]:
        """Analyze sentiment using TextBlob."""
        from textblob import TextBlob

        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # -1 to 1
        confidence = abs(blob.sentiment.subjectivity)  # 0 to 1
        return sentiment, confidence

    def _analyze_with_vader(self, text: str) -> Tuple[float, float]:
        """Analyze sentiment using VADER."""
        scores = self._vader_analyzer.polarity_scores(text)
        sentiment = scores["compound"]  # -1 to 1
        confidence = max(scores["pos"], scores["neg"], scores["neu"])
        return sentiment, confidence

    def _analyze_with_simple(self, text: str) -> Tuple[float, float]:
        """Analyze sentiment using simple rule - based approach."""
        text_lower = text.lower()

        # Positive words and phrases
        positive_words = [
            "love",
            "amazing",
            "great",
            "awesome",
            "fantastic",
            "excellent",
            "perfect",
            "beautiful",
            "wonderful",
            "incredible",
            "best",
            "good",
            "like",
            "enjoy",
            "happy",
            "excited",
            "fire",
            "🔥",
            "❤️",
            "😍",
            "👏",
            "dope",
            "slaps",
            "banger",
            "goated",
            "waves",
            "wavy",
            "hard",
            "fye",
        ]

        # Negative words and phrases
        negative_words = [
            "hate",
            "terrible",
            "awful",
            "horrible",
            "worst",
            "bad",
            "sucks",
            "boring",
            "stupid",
            "trash",
            "garbage",
            "disappointed",
            "angry",
            "sad",
            "annoying",
            "cringe",
            "😡",
            "😢",
            "👎",
            "💩",
        ]

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        # Calculate sentiment score
        if positive_count > negative_count:
            sentiment = min(0.8, positive_count * 0.2)
        elif negative_count > positive_count:
            sentiment = max(-0.8, negative_count * -0.2)
        else:
            sentiment = 0.0

        # Simple confidence based on word count
        total_words = len(text_lower.split())
        confidence = min(0.8, (positive_count + negative_count) / max(total_words, 1))

        return sentiment, confidence

    def _analyze_single_comment_with_retry(self, comment: YouTubeComment, method: SentimentMethod) -> SentimentResult:
        """
        Analyze sentiment for a single comment with selective retry logic.

        Args:
            comment: YouTube comment to analyze
            method: Sentiment analysis method to use

        Returns:
            SentimentResult with analysis results

        Raises:
            ETLError: If analysis fails after retries
        """
        try:
            return self._analyze_single_comment(comment, method)
        except ETLError as e:
            # Don't retry validation or data quality errors - they won't improve
            if e.category in [ErrorCategory.VALIDATION, ErrorCategory.DATA_QUALITY]:
                raise
            # Retry other errors with backoff
            return self._analyze_single_comment_with_backoff(comment, method)

    @retry_with_backoff(max_retries=3, base_delay=1.0)
    def _analyze_single_comment_with_backoff(self, comment: YouTubeComment, method: SentimentMethod) -> SentimentResult:
        """Analyze single comment with retry backoff for retryable errors only."""
        return self._analyze_single_comment(comment, method)

    def _analyze_single_comment(self, comment: YouTubeComment, method: SentimentMethod) -> SentimentResult:
        """
        Analyze sentiment for a single comment with error handling.

        Args:
            comment: YouTube comment to analyze
            method: Sentiment analysis method to use

        Returns:
            SentimentResult with analysis results

        Raises:
            ETLError: If analysis fails
        """
        # Validate comment text
        if not self._validate_comment_text(comment.comment_text):
            raise ETLError(
                f"Invalid comment text for analysis: length={len(comment.comment_text or '')}",
                severity=ErrorSeverity.MEDIUM,
                category=ErrorCategory.VALIDATION,
                context=ErrorContext(
                    component="sentiment_analyzer",
                    operation="analyze_single_comment",
                    user_data={"comment_id": comment.comment_id, "text_length": len(comment.comment_text or "")},
                ),
            )

        try:
            # Get analysis method
            analysis_func = self.available_methods[method]

            # Perform analysis
            sentiment_score, confidence_score = analysis_func(comment.comment_text)

            # Validate results
            if not (-1.0 <= sentiment_score <= 1.0):
                raise ValueError(f"Invalid sentiment score: {sentiment_score}")

            if not (0.0 <= confidence_score <= 1.0):
                raise ValueError(f"Invalid confidence score: {confidence_score}")

            # Check confidence threshold - don't retry these as they won't improve
            if confidence_score < self.config.confidence_threshold:
                raise ETLError(
                    f"Confidence {confidence_score:.3f} below threshold {self.config.confidence_threshold}",
                    severity=ErrorSeverity.MEDIUM,
                    category=ErrorCategory.DATA_QUALITY,
                    context=ErrorContext(
                        component="sentiment_analyzer",
                        operation="analyze_single_comment",
                        user_data={
                            "comment_id": comment.comment_id,
                            "confidence": confidence_score,
                            "threshold": self.config.confidence_threshold,
                        },
                    ),
                )

            # Create result
            return SentimentResult(
                comment_id=comment.comment_id,
                video_id=comment.video_id,
                sentiment_score=round(sentiment_score, 3),
                confidence_score=round(confidence_score, 3),
                method=method,
                processed_at=datetime.utcnow(),
            )

        except Exception as e:
            # Convert to ETL error for consistent handling
            if not isinstance(e, ETLError):
                raise ETLError(
                    f"Sentiment analysis failed: {str(e)}",
                    severity=ErrorSeverity.HIGH,
                    category=ErrorCategory.PROCESSING,
                    context=ErrorContext(
                        component="sentiment_analyzer",
                        operation="analyze_single_comment",
                        user_data={"comment_id": comment.comment_id, "method": method, "error": str(e)},
                    ),
                    original_error=e,
                )
            else:
                raise

    def analyze_comments_batch(self, comments: List[YouTubeComment]) -> SentimentAnalysisResult:
        """
        Analyze sentiment for a batch of comments with comprehensive error handling.

        Args:
            comments: List of YouTube comments to analyze

        Returns:
            SentimentAnalysisResult with results and benchmarks
        """
        if not comments:
            return SentimentAnalysisResult(
                benchmark=SentimentBenchmark(method_used=SentimentMethod.AUTO), config=self.config
            )

        # Reset benchmark tracking
        self._reset_benchmark()
        self._benchmark_start_time = time.time()

        # Get best available method
        method = self._get_best_available_method()
        self._benchmark_data["method_used"] = method

        print(f"🧠 Analyzing {len(comments)} comments using {method} method")

        successful_results = []
        failed_comments = []
        timeout_occurred = False

        # Process comments in batches
        batch_size = self.config.batch_size
        total_batches = (len(comments) + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            if timeout_occurred:
                break
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(comments))
            batch = comments[start_idx:end_idx]

            if self.config.progress_reporting:
                print(f"  Processing batch {batch_idx + 1}/{total_batches} ({len(batch)} comments)")

            # Process batch with timeout
            batch_start_time = time.time()

            for comment in batch:
                try:
                    # Check timeout
                    elapsed_time = time.time() - self._benchmark_start_time
                    if elapsed_time > self.config.timeout_seconds:
                        raise ETLError(
                            f"Processing timeout after {elapsed_time:.1f} seconds",
                            severity=ErrorSeverity.HIGH,
                            category=ErrorCategory.TIMEOUT,
                            context=ErrorContext(
                                component="sentiment_analyzer",
                                operation="analyze_comments_batch",
                                user_data={
                                    "timeout_seconds": self.config.timeout_seconds,
                                    "elapsed_seconds": elapsed_time,
                                    "processed_count": len(successful_results),
                                },
                            ),
                        )

                    # Analyze comment
                    result = self._analyze_single_comment_with_retry(comment, method)
                    successful_results.append(result)

                    # Track confidence distribution
                    if result.confidence_score >= 0.8:
                        self._benchmark_data["confidence_distribution"]["high"] += 1
                    elif result.confidence_score >= 0.6:
                        self._benchmark_data["confidence_distribution"]["medium"] += 1
                    else:
                        self._benchmark_data["confidence_distribution"]["low"] += 1

                except ETLError as e:
                    # Handle timeout errors by breaking out of processing
                    if e.category == ErrorCategory.TIMEOUT:
                        self.error_handler.handle_error(e, should_raise=False)
                        print(
                            f"⏰ Processing stopped due to timeout after {len(successful_results)} successful results"
                        )
                        timeout_occurred = True
                        break

                    # Log other errors but continue processing
                    self.error_handler.handle_error(e, should_raise=False)
                    failed_comments.append(
                        {
                            "comment_id": comment.comment_id,
                            "video_id": comment.video_id,
                            "error": str(e),
                            "error_category": e.category.value,
                        }
                    )
                    self._benchmark_data["error_count"] += 1

                except Exception as e:
                    # Handle unexpected errors
                    error = ETLError(
                        f"Unexpected error processing comment {comment.comment_id}: {str(e)}",
                        severity=ErrorSeverity.MEDIUM,
                        category=ErrorCategory.PROCESSING,
                        context=ErrorContext(
                            component="sentiment_analyzer",
                            operation="analyze_comments_batch",
                            user_data={"comment_id": comment.comment_id},
                        ),
                        original_error=e,
                    )
                    self.error_handler.handle_error(error, should_raise=False)
                    failed_comments.append(
                        {
                            "comment_id": comment.comment_id,
                            "video_id": comment.video_id,
                            "error": str(e),
                            "error_category": "unexpected",
                        }
                    )
                    self._benchmark_data["error_count"] += 1

            batch_time = time.time() - batch_start_time
            if self.config.progress_reporting:
                batch_rate = len(batch) / batch_time if batch_time > 0 else 0
                print(f"    Batch completed in {batch_time:.2f}s ({batch_rate:.1f} comments / sec)")

        # Calculate final benchmark
        total_time = time.time() - self._benchmark_start_time
        total_processed = len(successful_results) + len(failed_comments)

        benchmark = SentimentBenchmark(
            total_comments=total_processed,
            processing_time_seconds=round(total_time, 3),
            comments_per_second=round(total_processed / total_time if total_time > 0 else 0, 2),
            average_time_per_comment=round(total_time / total_processed if total_processed > 0 else 0, 4),
            method_used=method,
            confidence_distribution=self._benchmark_data["confidence_distribution"],
            error_count=self._benchmark_data["error_count"],
        )

        # Create result
        result = SentimentAnalysisResult(
            successful_results=successful_results,
            failed_comments=failed_comments,
            benchmark=benchmark,
            config=self.config,
        )

        # Log summary
        print(f"✅ Sentiment analysis completed:")
        print(f"   Successful: {len(successful_results)}")
        print(f"   Failed: {len(failed_comments)}")
        print(f"   Success rate: {result.success_rate:.1f}%")
        print(f"   Processing rate: {benchmark.comments_per_second:.1f} comments / sec")

        return result


def load_comments_for_sentiment_analysis(
    engine: Engine, limit: Optional[int] = None, video_id: Optional[str] = None
) -> List[YouTubeComment]:
    """
    Load comments that need sentiment analysis from database.

    Args:
        engine: Database engine
        limit: Maximum number of comments to load
        video_id: Optional video ID to filter comments

    Returns:
        List of YouTubeComment objects ready for analysis
    """
    # Build query
    query = """
        SELECT c.comment_id, c.video_id, c.comment_text, c.author_name,
               c.like_count, c.published_at
        FROM youtube_comments c
        LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
        WHERE cs.comment_id IS NULL
        AND c.comment_text IS NOT NULL
        AND c.comment_text != ''
    """

    params = {}

    if video_id:
        query += " AND c.video_id = :video_id"
        params["video_id"] = video_id

    query += " ORDER BY c.published_at DESC"

    if limit:
        query += " LIMIT :limit"
        params["limit"] = limit

    # Execute query and convert to YouTubeComment objects
    with engine.connect() as conn:
        result = conn.execute(text(query), params)

        comments = []
        for row in result:
            try:
                comment = YouTubeComment(
                    comment_id=row.comment_id,
                    video_id=row.video_id,
                    author_name=row.author_name,
                    comment_text=row.comment_text,
                    like_count=row.like_count or 0,
                    published_at=row.published_at,
                )
                comments.append(comment)
            except Exception as e:
                print(f"⚠️ Skipping invalid comment {row.comment_id}: {e}")

        return comments


def store_sentiment_results(engine: Engine, results: List[SentimentResult]) -> int:
    """
    Store sentiment analysis results to database.

    Args:
        engine: Database engine
        results: List of sentiment results to store

    Returns:
        Number of results stored
    """
    if not results:
        return 0

    stored_count = 0

    with engine.connect() as conn:
        for result in results:
            try:
                # Try INSERT first
                try:
                    conn.execute(
                        text(
                            """
                        INSERT INTO comment_sentiment
                        (comment_id, video_id, comment_text, sentiment_score, confidence_score,
                         processed_at, confidence)
                        VALUES
                        (:comment_id, :video_id, '', :sentiment_score, :confidence_score,
                         :processed_at, :confidence)
                    """
                        ),
                        {
                            "comment_id": result.comment_id,
                            "video_id": result.video_id,
                            "sentiment_score": result.sentiment_score,
                            "confidence_score": result.confidence_score,
                            "processed_at": result.processed_at,
                            "confidence": result.confidence_score,
                        },
                    )
                    stored_count += 1
                except Exception:
                    # If INSERT fails (duplicate), try UPDATE
                    conn.execute(
                        text(
                            """
                        UPDATE comment_sentiment
                        SET sentiment_score = :sentiment_score,
                            confidence_score = :confidence_score,
                            processed_at = :processed_at,
                            confidence = :confidence
                        WHERE comment_id = :comment_id
                    """
                        ),
                        {
                            "comment_id": result.comment_id,
                            "sentiment_score": result.sentiment_score,
                            "confidence_score": result.confidence_score,
                            "processed_at": result.processed_at,
                            "confidence": result.confidence_score,
                        },
                    )
                    stored_count += 1
            except Exception as e:
                print(f"⚠️ Failed to store result for comment {result.comment_id}: {e}")

        conn.commit()

    return stored_count
