#!/usr/bin/env python3
"""
Tests for Enhanced Sentiment Analysis System

This module provides comprehensive tests for the sentiment analysis system:
- Configuration validation and error handling
- Multiple analysis methods with fallbacks
- Batch processing with progress tracking
- Performance benchmarking and timeout handling
- Confidence threshold validation
- Database integration and result storage
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest

from tests.conftest import get_table_count, insert_test_comment
from web.error_handling import ErrorCategory, ErrorSeverity, ETLError
from web.models import SentimentMethod, SentimentResult, YouTubeComment
from web.sentiment_analyzer import (
    SentimentAnalysisConfig,
    SentimentAnalysisResult,
    SentimentAnalyzer,
    SentimentBenchmark,
    load_comments_for_sentiment_analysis,
    store_sentiment_results,
)


class TestSentimentAnalysisConfig:
    """Test sentiment analysis configuration validation."""

    def test_config_creation_with_defaults(self):
        """Test creating config with default values."""
        config = SentimentAnalysisConfig()

        assert config.batch_size == 100
        assert config.confidence_threshold == 0.7
        assert config.preferred_method == SentimentMethod.AUTO
        assert config.enable_fallback is True
        assert config.enable_benchmarking is True

    def test_config_creation_with_custom_values(self):
        """Test creating config with custom values."""
        config = SentimentAnalysisConfig(
            batch_size=50,
            confidence_threshold=0.8,
            preferred_method=SentimentMethod.VADER,
            enable_fallback=False,
            timeout_seconds=600,
        )

        assert config.batch_size == 50
        assert config.confidence_threshold == 0.8
        assert config.preferred_method == SentimentMethod.VADER
        assert config.enable_fallback is False
        assert config.timeout_seconds == 600

    def test_config_validation_errors(self):
        """Test config validation with invalid values."""
        with pytest.raises(Exception):  # Should raise validation error
            SentimentAnalysisConfig(
                batch_size=-1,  # Invalid: negative
                confidence_threshold=1.5,  # Invalid: > 1.0
                timeout_seconds=10,  # Invalid: < 30
            )


class TestSentimentBenchmark:
    """Test sentiment benchmark model."""

    def test_benchmark_creation(self):
        """Test creating benchmark with valid data."""
        benchmark = SentimentBenchmark(
            total_comments=100,
            processing_time_seconds=10.5,
            comments_per_second=9.52,
            average_time_per_comment=0.105,
            method_used=SentimentMethod.VADER,
            confidence_distribution={"high": 60, "medium": 30, "low": 10},
            error_count=5,
        )

        assert benchmark.total_comments == 100
        assert benchmark.processing_time_seconds == 10.5
        assert benchmark.method_used == SentimentMethod.VADER
        assert benchmark.error_count == 5

    def test_benchmark_validation(self):
        """Test benchmark validation with invalid values."""
        with pytest.raises(Exception):  # Should raise validation error
            SentimentBenchmark(
                total_comments=-1,  # Invalid: negative
                processing_time_seconds=-5.0,  # Invalid: negative
                method_used=SentimentMethod.VADER,
            )


class TestSentimentAnalyzer:
    """Test sentiment analyzer functionality."""

    def test_analyzer_initialization_with_config(self):
        """Test analyzer initialization with custom config."""
        config = SentimentAnalysisConfig(batch_size=50, confidence_threshold=0.8)

        analyzer = SentimentAnalyzer(config)

        assert analyzer.config.batch_size == 50
        assert analyzer.config.confidence_threshold == 0.8
        assert len(analyzer.available_methods) >= 1  # At least simple method

    def test_analyzer_initialization_without_config(self):
        """Test analyzer initialization with default config."""
        analyzer = SentimentAnalyzer()

        assert analyzer.config.batch_size == 100
        assert analyzer.config.confidence_threshold == 0.7
        assert SentimentMethod.SIMPLE in analyzer.available_methods

    def test_get_best_available_method(self):
        """Test method selection logic."""
        analyzer = SentimentAnalyzer()

        # Should return a valid method
        method = analyzer._get_best_available_method()
        assert method in analyzer.available_methods
        assert isinstance(method, SentimentMethod)

    def test_validate_comment_text(self):
        """Test comment text validation."""
        config = SentimentAnalysisConfig(min_text_length=3, max_text_length=1000)
        analyzer = SentimentAnalyzer(config)

        # Valid text
        assert analyzer._validate_comment_text("This is a good comment") is True

        # Invalid: too short
        assert analyzer._validate_comment_text("Hi") is False

        # Invalid: empty
        assert analyzer._validate_comment_text("") is False
        assert analyzer._validate_comment_text(None) is False

        # Invalid: too long
        long_text = "x" * 2000
        assert analyzer._validate_comment_text(long_text) is False

    def test_simple_sentiment_analysis(self):
        """Test simple rule-based sentiment analysis."""
        analyzer = SentimentAnalyzer()

        # Positive sentiment
        sentiment, confidence = analyzer._analyze_with_simple("This is amazing! I love it! 🔥")
        assert sentiment > 0
        assert 0 <= confidence <= 1

        # Negative sentiment
        sentiment, confidence = analyzer._analyze_with_simple("This is terrible and awful")
        assert sentiment < 0
        assert 0 <= confidence <= 1

        # Neutral sentiment
        sentiment, confidence = analyzer._analyze_with_simple("This is a normal comment")
        assert sentiment == 0.0
        assert 0 <= confidence <= 1

    def test_analyze_single_comment_success(self, test_data_factory):
        """Test successful single comment analysis."""
        analyzer = SentimentAnalyzer()
        comment = test_data_factory.create_youtube_comment(comment_text="This is an amazing video! Love it!")

        result = analyzer._analyze_single_comment(comment, SentimentMethod.SIMPLE)

        assert isinstance(result, SentimentResult)
        assert result.comment_id == comment.comment_id
        assert result.video_id == comment.video_id
        assert -1.0 <= result.sentiment_score <= 1.0
        assert 0.0 <= result.confidence_score <= 1.0
        assert result.method == SentimentMethod.SIMPLE

    def test_analyze_single_comment_validation_error(self, test_data_factory):
        """Test single comment analysis with validation error."""
        analyzer = SentimentAnalyzer()
        comment = test_data_factory.create_youtube_comment(comment_text="Hi")  # Too short

        with pytest.raises(ETLError) as exc_info:
            analyzer._analyze_single_comment(comment, SentimentMethod.SIMPLE)

        assert exc_info.value.category == ErrorCategory.VALIDATION

    def test_analyze_single_comment_confidence_threshold(self, test_data_factory):
        """Test single comment analysis with confidence threshold."""
        config = SentimentAnalysisConfig(confidence_threshold=0.9)  # Very high threshold
        analyzer = SentimentAnalyzer(config)

        comment = test_data_factory.create_youtube_comment(comment_text="This is okay I guess")  # Low confidence text

        with pytest.raises(ETLError) as exc_info:
            analyzer._analyze_single_comment(comment, SentimentMethod.SIMPLE)

        assert exc_info.value.category == ErrorCategory.DATA_QUALITY

    def test_analyze_comments_batch_success(self, test_data_factory):
        """Test successful batch comment analysis."""
        analyzer = SentimentAnalyzer()

        comments = [
            test_data_factory.create_youtube_comment(
                comment_id=f"comment_{i}", comment_text=f"This is test comment {i} and it's great!"
            )
            for i in range(5)
        ]

        result = analyzer.analyze_comments_batch(comments)

        assert isinstance(result, SentimentAnalysisResult)
        assert len(result.successful_results) > 0
        assert result.benchmark.total_comments == 5
        assert result.benchmark.method_used in analyzer.available_methods
        assert result.success_rate > 0

    def test_analyze_comments_batch_empty(self):
        """Test batch analysis with empty comment list."""
        analyzer = SentimentAnalyzer()

        result = analyzer.analyze_comments_batch([])

        assert isinstance(result, SentimentAnalysisResult)
        assert len(result.successful_results) == 0
        assert len(result.failed_comments) == 0
        assert result.benchmark.total_comments == 0

    def test_analyze_comments_batch_with_failures(self, test_data_factory):
        """Test batch analysis with some failing comments."""
        config = SentimentAnalysisConfig(confidence_threshold=0.9)  # High threshold
        analyzer = SentimentAnalyzer(config)

        comments = [
            test_data_factory.create_youtube_comment(
                comment_id="good_comment", comment_text="This is absolutely amazing and fantastic! 🔥🔥🔥"
            ),
            test_data_factory.create_youtube_comment(comment_id="bad_comment", comment_text="Hi"),  # Too short
            test_data_factory.create_youtube_comment(
                comment_id="low_confidence", comment_text="This is okay"  # Low confidence
            ),
        ]

        result = analyzer.analyze_comments_batch(comments)

        assert isinstance(result, SentimentAnalysisResult)
        assert len(result.failed_comments) > 0
        assert result.benchmark.error_count > 0
        assert result.success_rate < 100.0

    def test_analyze_comments_batch_timeout(self, test_data_factory):
        """Test batch analysis with timeout."""
        config = SentimentAnalysisConfig(timeout_seconds=0.1)  # Very short timeout
        analyzer = SentimentAnalyzer(config)

        # Create many comments to trigger timeout
        comments = [
            test_data_factory.create_youtube_comment(
                comment_id=f"comment_{i}", comment_text=f"This is test comment {i}"
            )
            for i in range(100)
        ]

        # Should handle timeout gracefully
        result = analyzer.analyze_comments_batch(comments)

        assert isinstance(result, SentimentAnalysisResult)
        # Should have processed some but not all due to timeout
        assert result.benchmark.total_comments < len(comments)

    def test_analyze_comments_batch_progress_reporting(self, test_data_factory, capsys):
        """Test batch analysis with progress reporting."""
        config = SentimentAnalysisConfig(batch_size=2, progress_reporting=True)
        analyzer = SentimentAnalyzer(config)

        comments = [
            test_data_factory.create_youtube_comment(
                comment_id=f"comment_{i}", comment_text=f"This is test comment {i} and it's great!"
            )
            for i in range(5)
        ]

        result = analyzer.analyze_comments_batch(comments)

        # Check that progress was reported
        captured = capsys.readouterr()
        assert "Processing batch" in captured.out
        assert "Batch completed" in captured.out

        assert isinstance(result, SentimentAnalysisResult)
        assert len(result.successful_results) == 5


class TestDatabaseIntegration:
    """Test database integration for sentiment analysis."""

    def test_load_comments_for_sentiment_analysis(self, test_engine, test_data_factory):
        """Test loading comments that need sentiment analysis."""
        # Insert test comments
        video = test_data_factory.create_youtube_video()
        comments = test_data_factory.create_test_comments_batch(video.video_id, 3)

        for comment in comments:
            insert_test_comment(test_engine, comment)

        # Load comments for analysis
        loaded_comments = load_comments_for_sentiment_analysis(test_engine, limit=10)

        assert len(loaded_comments) == 3
        for comment in loaded_comments:
            assert isinstance(comment, YouTubeComment)
            assert comment.video_id == video.video_id

    def test_load_comments_with_video_filter(self, test_engine, test_data_factory):
        """Test loading comments filtered by video ID."""
        # Insert comments for multiple videos
        video1 = test_data_factory.create_youtube_video(video_id="dQw4w9WgXcQ")
        video2 = test_data_factory.create_youtube_video(video_id="oHg5SJYRHA0")

        comments1 = test_data_factory.create_test_comments_batch(video1.video_id, 2)
        comments2 = test_data_factory.create_test_comments_batch(video2.video_id, 3)

        for comment in comments1 + comments2:
            insert_test_comment(test_engine, comment)

        # Load comments for specific video
        loaded_comments = load_comments_for_sentiment_analysis(test_engine, video_id=video1.video_id)

        assert len(loaded_comments) == 2
        for comment in loaded_comments:
            assert comment.video_id == video1.video_id

    def test_store_sentiment_results(self, test_engine, test_data_factory):
        """Test storing sentiment analysis results."""
        # Create test results
        results = [
            SentimentResult(
                comment_id=f"comment_{i}",
                video_id="dQw4w9WgXcQ",
                sentiment_score=0.5 + (i * 0.1),
                confidence_score=0.8,
                method=SentimentMethod.SIMPLE,
            )
            for i in range(3)
        ]

        # Store results
        stored_count = store_sentiment_results(test_engine, results)

        assert stored_count == 3
        assert get_table_count(test_engine, "comment_sentiment") == 3

    def test_store_sentiment_results_empty(self, test_engine):
        """Test storing empty results list."""
        stored_count = store_sentiment_results(test_engine, [])

        assert stored_count == 0
        assert get_table_count(test_engine, "comment_sentiment") == 0


class TestSentimentAnalysisResult:
    """Test sentiment analysis result model."""

    def test_result_creation(self):
        """Test creating sentiment analysis result."""
        config = SentimentAnalysisConfig()
        benchmark = SentimentBenchmark(
            total_comments=10, processing_time_seconds=5.0, method_used=SentimentMethod.SIMPLE
        )

        result = SentimentAnalysisResult(successful_results=[], failed_comments=[], benchmark=benchmark, config=config)

        assert result.benchmark.total_comments == 10
        assert result.config.batch_size == 100
        assert result.success_rate == 0.0  # No results

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        config = SentimentAnalysisConfig()
        benchmark = SentimentBenchmark(
            total_comments=10, processing_time_seconds=5.0, method_used=SentimentMethod.SIMPLE
        )

        # Create some successful and failed results
        successful_results = [
            SentimentResult(
                comment_id=f"success_{i}",
                video_id="dQw4w9WgXcQ",
                sentiment_score=0.5,
                confidence_score=0.8,
                method=SentimentMethod.SIMPLE,
            )
            for i in range(7)
        ]

        failed_comments = [{"comment_id": f"failed_{i}", "error": "test error"} for i in range(3)]

        result = SentimentAnalysisResult(
            successful_results=successful_results, failed_comments=failed_comments, benchmark=benchmark, config=config
        )

        assert result.success_rate == 70.0  # 7 out of 10


class TestErrorHandling:
    """Test error handling in sentiment analysis."""

    def test_analyzer_with_no_methods_available(self):
        """Test analyzer initialization when no methods are available."""
        with patch.object(SentimentAnalyzer, "_initialize_methods") as mock_init:
            mock_init.side_effect = ETLError(
                "No sentiment analysis methods available",
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
            )

            with pytest.raises(ETLError) as exc_info:
                SentimentAnalyzer()

            assert exc_info.value.severity == ErrorSeverity.CRITICAL
            assert exc_info.value.category == ErrorCategory.CONFIGURATION

    def test_preferred_method_not_available(self):
        """Test when preferred method is not available."""
        config = SentimentAnalysisConfig(
            preferred_method=SentimentMethod.TEXTBLOB, enable_fallback=False  # May not be available
        )

        analyzer = SentimentAnalyzer(config)

        # If TextBlob is not available and fallback is disabled, should raise error
        if SentimentMethod.TEXTBLOB not in analyzer.available_methods:
            with pytest.raises(ETLError) as exc_info:
                analyzer._get_best_available_method()

            assert exc_info.value.category == ErrorCategory.CONFIGURATION


class TestPerformanceBenchmarking:
    """Test performance benchmarking functionality."""

    def test_benchmark_tracking(self, test_data_factory):
        """Test that benchmarking tracks performance correctly."""
        config = SentimentAnalysisConfig(enable_benchmarking=True)
        analyzer = SentimentAnalyzer(config)

        comments = [
            test_data_factory.create_youtube_comment(
                comment_id=f"perf_comment_{i}", comment_text=f"This is performance test comment {i} and it's great!"
            )
            for i in range(10)
        ]

        result = analyzer.analyze_comments_batch(comments)

        # Verify benchmark data
        assert result.benchmark.total_comments == 10
        assert result.benchmark.processing_time_seconds > 0
        assert result.benchmark.comments_per_second > 0
        assert result.benchmark.average_time_per_comment > 0
        assert isinstance(result.benchmark.confidence_distribution, dict)

    def test_confidence_distribution_tracking(self, test_data_factory):
        """Test confidence distribution tracking in benchmarks."""
        analyzer = SentimentAnalyzer()

        # Create comments with different expected confidence levels
        comments = [
            test_data_factory.create_youtube_comment(
                comment_id="high_conf",
                comment_text="This is absolutely amazing and fantastic! I love it so much! 🔥🔥🔥",
            ),
            test_data_factory.create_youtube_comment(comment_id="medium_conf", comment_text="This is pretty good"),
            test_data_factory.create_youtube_comment(comment_id="low_conf", comment_text="This is okay"),
        ]

        result = analyzer.analyze_comments_batch(comments)

        # Should have confidence distribution data
        assert "high" in result.benchmark.confidence_distribution
        assert "medium" in result.benchmark.confidence_distribution
        assert "low" in result.benchmark.confidence_distribution


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
