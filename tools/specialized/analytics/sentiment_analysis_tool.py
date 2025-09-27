#!/usr / bin / env python3
"""
🧠 Sentiment Analysis Tool for YouTube Comments

Consolidated sentiment analysis tool that provides:
- Multiple sentiment analysis methods (TextBlob, VADER, rule - based)
- Batch processing of comments
- Comprehensive sentiment statistics and reporting
- Integration with the YouTube analytics pipeline

Usage:
    python tools / specialized / analytics / sentiment_analysis_tool.py                # Process all comments
    python tools / specialized / analytics / sentiment_analysis_tool.py --limit 100   # Process 100 comments
    python tools / specialized / analytics / sentiment_analysis_tool.py --method vader # Use VADER method
    python tools / specialized / analytics / sentiment_analysis_tool.py --stats        # Show statistics only
"""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import (
    ConfigurationError,
    ExecutionError,
    ToolBase,
    ToolConfig,
    ValidationError,
    register_tool,
)


class SentimentAnalysisTool(ToolBase):
    """
    Unified sentiment analysis tool for YouTube comments.

    This tool provides comprehensive sentiment analysis capabilities:
    - Multiple analysis methods (TextBlob, VADER, rule - based fallback)
    - Batch processing with progress tracking
    - Sentiment statistics and reporting
    - Integration with ETL pipeline logging
    - Sample data generation for testing
    """

    def __init__(self):
        super().__init__(name="sentiment - analysis", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        # Initialize sentiment analysis capabilities
        self.available_methods = self._check_available_methods()
        self.processing_stats = {
            "processed_count": 0,
            "error_count": 0,
            "start_time": None,
            "end_time": None,
        }

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return ["DB_HOST", "DB_USER", "DB_NAME"]

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="sentiment - analysis",
            version="1.0.0",
            description="Unified sentiment analysis tool for YouTube comments",
            dependencies=[
                "python>=3.8",
                "pymysql",
                "sqlalchemy",
                "pandas",
                "textblob",  # Optional
                "vaderSentiment",  # Optional
            ],
            environment_vars=[
                "DB_HOST",
                "DB_USER",
                "DB_NAME",
            ],
            usage_examples=[
                "python tools / specialized / analytics / sentiment_analysis_tool.py",
                "python tools / specialized / analytics / sentiment_analysis_tool.py --limit 100",
                "python tools / specialized / analytics / sentiment_analysis_tool.py --method vader",
            ],
            category="specialized",
        )

    def run(self) -> None:
        """Main execution method - should not be called directly, use specific analysis methods."""
        self.log_progress("Use specific analysis methods like process_comments() or get_statistics()")

    def _check_available_methods(self) -> Dict[str, bool]:
        """Check which sentiment analysis methods are available."""
        methods = {
            "textblob": False,
            "vader": False,
            "simple": True,  # Always available as fallback
        }

        try:
            from textblob import TextBlob

            methods["textblob"] = True
            self.log_progress("✅ TextBlob sentiment analysis available")
        except ImportError:
            self.log_progress("⚠️ TextBlob not available. Install with: pip install textblob", level="WARNING")

        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

            methods["vader"] = True
            self.log_progress("✅ VADER sentiment analysis available")
        except ImportError:
            self.log_progress("⚠️ VADER not available. Install with: pip install vaderSentiment", level="WARNING")

        return methods

    def analyze_sentiment(self, text: str, method: str = "auto") -> Tuple[float, float]:
        """
        Analyze sentiment of text using specified method.

        Args:
            text: Text to analyze
            method: Analysis method ("textblob", "vader", "simple", "auto")

        Returns:
            Tuple of (sentiment_score, confidence_score)
        """
        if not text or not text.strip():
            return 0.0, 0.0

        # Clean and limit text
        text_clean = text.strip()[:1000]

        try:
            if method == "textblob" and self.available_methods["textblob"]:
                return self._analyze_textblob(text_clean)
            elif method == "vader" and self.available_methods["vader"]:
                return self._analyze_vader(text_clean)
            elif method == "simple":
                return self._analyze_simple(text_clean)
            elif method == "auto":
                # Use best available method
                if self.available_methods["textblob"]:
                    return self._analyze_textblob(text_clean)
                elif self.available_methods["vader"]:
                    return self._analyze_vader(text_clean)
                else:
                    return self._analyze_simple(text_clean)
            else:
                raise ValidationError(f"Unknown or unavailable sentiment method: {method}")

        _exc_ept Exc_eption as _e:  # noqa: E999
            self.log_progress(f"Sentiment analysis failed for method {method}, falling back to simple", level="WARNING")
            return self._analyze_simple(text_clean)

    def _analyze_textblob(self, text: str) -> Tuple[float, float]:
        """Analyze sentiment using TextBlob."""
        from textblob import TextBlob

        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # -1 to 1
        confidence = abs(blob.sentiment.subjectivity)  # 0 to 1
        return sentiment, confidence

    def _analyze_vader(self, text: str) -> Tuple[float, float]:
        """Analyze sentiment using VADER."""
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(text)
        sentiment = scores["compound"]  # -1 to 1
        confidence = max(scores["pos"], scores["neg"], scores["neu"])
        return sentiment, confidence

    def _analyze_simple(self, text: str) -> Tuple[float, float]:
        """Simple rule - based sentiment analysis as fallback."""
        if not text:
            return 0.0, 0.0

        text_lower = text.lower()

        # Positive indicators
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
            "brilliant",
            "outstanding",
        ]

        # Negative indicators
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
            "disgusting",
            "pathetic",
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
        confidence = min(0.6, (positive_count + negative_count) * 0.1)

        return sentiment, confidence

    def process_comments(self, limit: Optional[int] = None, method: str = "auto") -> Dict[str, Any]:
        """
        Process sentiment analysis for comments that haven't been analyzed.

        Args:
            limit: Maximum number of comments to process
            method: Sentiment analysis method to use

        Returns:
            Dictionary with processing results
        """
        self.log_progress(f"🧠 Processing sentiment analysis (method: {method})")

        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            self.processing_stats["start_time"] = datetime.now()

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "process_comments",
                "method": method,
                "limit": limit,
                "processed_count": 0,
                "error_count": 0,
                "processing_time_seconds": 0,
            }

            with engine.begin() as conn:
                # Get comments that need sentiment analysis
                sql = """
                    SELECT c.comment_id, c.video_id, c.comment_text
                    FROM youtube_comments c
                    LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
                    WHERE cs.comment_id IS NULL
                    AND c.comment_text IS NOT NULL
                    AND TRIM(c.comment_text) != ''
                """

                if limit:
                    sql += f" LIMIT {limit}"

                result = conn.execute(text(sql))
                comments = result.fetchall()

                if not comments:
                    self.log_progress("📭 No comments need sentiment analysis")
                    return results

                self.log_progress(f"📊 Processing sentiment for {len(comments)} comments")

                for i, comment in enumerate(comments):
                    try:
                        sentiment_score, confidence_score = self.analyze_sentiment(comment.comment_text, method)

                        # Insert sentiment result
                        conn.execute(
                            text(
                                """
                                INSERT INTO comment_sentiment
                                (comment_id, video_id, comment_text, sentiment_score, confidence_score, processed_at)
                                VALUES (:comment_id, :video_id, :text, :sentiment, :confidence, :processed)
                            """
                            ),
                            {
                                "comment_id": comment.comment_id,
                                "video_id": comment.video_id,
                                "text": comment.comment_text[:500],  # Truncate for storage
                                "sentiment": sentiment_score,
                                "confidence": confidence_score,
                                "processed": datetime.now(),
                            },
                        )

                        results["processed_count"] += 1
                        self.processing_stats["processed_count"] += 1

                        # Progress reporting
                        if (i + 1) % 50 == 0:
                            self.log_progress(f"  📊 Processed {i + 1}/{len(comments)} comments")

                    _exc_ept Exc_eption as _e:
                        self.log_progress(f"❌ Error processing comment {comment.comment_id}: {e}", level="ERROR")
                        results["error_count"] += 1
                        self.processing_stats["error_count"] += 1
                        continue

            # Calculate processing time
            self.processing_stats["end_time"] = datetime.now()
            processing_time = (self.processing_stats["end_time"] - self.processing_stats["start_time"]).total_seconds()
            results["processing_time_seconds"] = processing_time

            self.log_progress(
                f"✅ Processed sentiment for {results['processed_count']} comments in {processing_time:.1f}s"
            )

            return results

        _exc_ept Exc_eption as _e:
            self.handle_error(e, "comment processing")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "process_comments",
                "status": "ERROR",
                "error": str(e),
            }

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive sentiment analysis statistics.

        Returns:
            Dictionary with sentiment statistics
        """
        self.log_progress("📊 Generating sentiment analysis statistics")

        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "get_statistics",
                "overall_stats": {},
                "sentiment_distribution": {},
                "confidence_stats": {},
                "recent_activity": {},
            }

            with engine.connect() as conn:
                # Overall statistics
                overall_result = conn.execute(
                    text(
                        """
                    SELECT
                        COUNT(*) as total_comments,
                        AVG(sentiment_score) as avg_sentiment,
                        AVG(confidence_score) as avg_confidence,
                        MIN(sentiment_score) as min_sentiment,
                        MAX(sentiment_score) as max_sentiment,
                        STD(sentiment_score) as sentiment_std
                    FROM comment_sentiment
                """
                    )
                ).fetchone()

                if overall_result and overall_result.total_comments > 0:
                    results["overall_stats"] = {
                        "total_comments": overall_result.total_comments,
                        "avg_sentiment": float(overall_result.avg_sentiment or 0),
                        "avg_confidence": float(overall_result.avg_confidence or 0),
                        "min_sentiment": float(overall_result.min_sentiment or 0),
                        "max_sentiment": float(overall_result.max_sentiment or 0),
                        "sentiment_std": float(overall_result.sentiment_std or 0),
                    }

                    # Sentiment distribution
                    distribution_result = conn.execute(
                        text(
                            """
                        SELECT
                            SUM(CASE WHEN sentiment_score > 0.1 THEN 1 ELSE 0 END) as positive,
                            SUM(CASE WHEN sentiment_score < -0.1 THEN 1 ELSE 0 END) as negative,
                            SUM(CASE WHEN sentiment_score BETWEEN -0.1 AND 0.1 THEN 1 ELSE 0 END) as neutral
                        FROM comment_sentiment
                    """
                        )
                    ).fetchone()

                    if distribution_result:
                        total = overall_result.total_comments
                        results["sentiment_distribution"] = {
                            "positive": {
                                "count": distribution_result.positive,
                                "percentage": (distribution_result.positive / total * 100) if total > 0 else 0,
                            },
                            "negative": {
                                "count": distribution_result.negative,
                                "percentage": (distribution_result.negative / total * 100) if total > 0 else 0,
                            },
                            "neutral": {
                                "count": distribution_result.neutral,
                                "percentage": (distribution_result.neutral / total * 100) if total > 0 else 0,
                            },
                        }

                    # Confidence statistics
                    confidence_result = conn.execute(
                        text(
                            """
                        SELECT
                            SUM(CASE WHEN confidence_score > 0.7 THEN 1 ELSE 0 END) as high_confidence,
                            SUM(CASE WHEN confidence_score BETWEEN 0.4 AND 0.7 THEN 1 ELSE 0 END) as medium_confidence,
                            SUM(CASE WHEN confidence_score < 0.4 THEN 1 ELSE 0 END) as low_confidence
                        FROM comment_sentiment
                    """
                        )
                    ).fetchone()

                    if confidence_result:
                        results["confidence_stats"] = {
                            "high_confidence": confidence_result.high_confidence,
                            "medium_confidence": confidence_result.medium_confidence,
                            "low_confidence": confidence_result.low_confidence,
                        }

                    # Recent activity (last 7 days)
                    recent_result = conn.execute(
                        text(
                            """
                        SELECT
                            COUNT(*) as recent_count,
                            AVG(sentiment_score) as recent_avg_sentiment
                        FROM comment_sentiment
                        WHERE processed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                    """
                        )
                    ).fetchone()

                    if recent_result:
                        results["recent_activity"] = {
                            "last_7_days_count": recent_result.recent_count,
                            "last_7_days_avg_sentiment": float(recent_result.recent_avg_sentiment or 0),
                        }
                else:
                    results["overall_stats"] = {"total_comments": 0}
                    self.log_progress("📭 No sentiment data found")

            return results

        _exc_ept Exc_eption as _e:
            self.handle_error(e, "statistics generation")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "get_statistics",
                "status": "ERROR",
                "error": str(e),
            }

    def create_sample_data(self, count: int = 10) -> Dict[str, Any]:
        """
        Create sample comments for testing sentiment analysis.

        Args:
            count: Number of sample comments to create

        Returns:
            Dictionary with creation results
        """
        self.log_progress(f"📝 Creating {count} sample comments for testing")

        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "create_sample_data",
                "requested_count": count,
                "created_count": 0,
            }

            # Sample comments with expected sentiment ranges
            sample_comments = [
                ("This song is absolutely amazing! Love it so much! 🔥❤️", 0.8),
                ("Great music, keep it up! Really enjoying this track", 0.6),
                ("Not bad, pretty good actually. Nice work!", 0.3),
                ("Meh, it's okay I guess. Could be better", 0.0),
                ("This is terrible, worst song ever 😡 Complete trash", -0.8),
                ("I don't like this at all, very disappointing", -0.5),
                ("Fire track! This is going to be huge! 🚀 Incredible!", 0.9),
                ("Beautiful lyrics, touched my heart ❤️ So emotional", 0.7),
                ("Boring and repetitive, skip this one 👎", -0.4),
                ("Perfect for my playlist, love the vibe 😍", 0.6),
                ("Outstanding performance! Best I've heard in years!", 0.9),
                ("Absolutely horrible. Can't believe this got released", -0.7),
                ("Decent track, nothing special but listenable", 0.2),
                ("Masterpiece! This artist is incredibly talented 🎵", 0.8),
                ("Waste of time. Completely overrated garbage", -0.6),
            ]

            with engine.begin() as conn:
                # Get some video IDs
                video_result = conn.execute(text("SELECT video_id FROM youtube_videos LIMIT 5"))
                video_ids = [row[0] for row in video_result]

                if not video_ids:
                    raise ExecutionError("No videos found to create sample comments")

                # Create sample comments
                for i in range(min(count, len(sample_comments))):
                    comment_text, expected_sentiment = sample_comments[i]
                    video_id = video_ids[i % len(video_ids)]
                    comment_id = f"SAMPLE_SENTIMENT_{datetime.now().strftime('%Y % m%d_ % H%M % S')}_{i + 1:03d}"

                    # Insert into youtube_comments
                    conn.execute(
                        text(
                            """
                            INSERT IGNORE INTO youtube_comments
                            (comment_id, video_id, author_name, comment_text, like_count, published_at, created_at)
                            VALUES (:comment_id, :video_id, :author, :text, :likes, :published, :created)
                        """
                        ),
                        {
                            "comment_id": comment_id,
                            "video_id": video_id,
                            "author": f"SampleUser{i + 1}",
                            "text": comment_text,
                            "likes": i + 1,
                            "published": datetime.now() - timedelta(days=i),
                            "created": datetime.now(),
                        },
                    )
                    results["created_count"] += 1

            self.log_progress(f"✅ Created {results['created_count']} sample comments")
            return results

        _exc_ept Exc_eption as _e:
            self.handle_error(e, "sample data creation")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "create_sample_data",
                "status": "ERROR",
                "error": str(e),
            }

    def cleanup_resources(self) -> None:
        """Clean up any resources used during sentiment analysis."""
        # No persistent resources to clean up
        pass


def main():
    """Main entry point for the sentiment analysis tool."""
    parser = argparse.ArgumentParser(
        description="Unified Sentiment Analysis Tool for YouTube Comments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / specialized / analytics / sentiment_analysis_tool.py                # Process all comments
  python tools / specialized / analytics / sentiment_analysis_tool.py --limit 100   # Process 100 comments
  python tools / specialized / analytics / sentiment_analysis_tool.py --method vader # Use VADER method
  python tools / specialized / analytics / sentiment_analysis_tool.py --stats        # Show statistics only
        """,
    )

    # Analysis operations
    parser.add_argument("--process", action="store_true", help="Process comments for sentiment analysis")
    parser.add_argument("--stats", action="store_true", help="Show sentiment analysis statistics")
    parser.add_argument("--create - samples", action="store_true", help="Create sample comments for testing")

    # Options
    parser.add_argument("--limit", type=int, help="Maximum number of comments to process")
    parser.add_argument(
        "--method", choices=["textblob", "vader", "simple", "auto"], default="auto", help="Sentiment analysis method"
    )
    parser.add_argument("--sample - count", type=int, default=10, help="Number of sample comments to create")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Create sentiment analysis tool instance
    with SentimentAnalysisTool() as sentiment_tool:
        try:
            if args.stats:
                result = sentiment_tool.get_statistics()
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    stats = result.get("overall_stats", {})
                    if stats.get("total_comments", 0) > 0:
                        print(f"📊 Sentiment Analysis Statistics:")
                        print(f"   Total comments: {stats['total_comments']:,}")
                        print(f"   Average sentiment: {stats['avg_sentiment']:.3f}")
                        print(f"   Average confidence: {stats['avg_confidence']:.3f}")

                        dist = result.get("sentiment_distribution", {})
                        if dist:
                            print(f"   Positive: {dist['positive']['count']} ({dist['positive']['percentage']:.1f}%)")
                            print(f"   Negative: {dist['negative']['count']} ({dist['negative']['percentage']:.1f}%)")
                            print(f"   Neutral: {dist['neutral']['count']} ({dist['neutral']['percentage']:.1f}%)")
                    else:
                        print("📭 No sentiment data found")
                return 0
            elif args.create_samples:
                result = sentiment_tool.create_sample_data(count=args.sample_count)
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    print(f"✅ Created {result.get('created_count', 0)} sample comments")
                return 0
            elif args.process:
                result = sentiment_tool.process_comments(limit=args.limit, method=args.method)
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    print(f"✅ Processed {result.get('processed_count', 0)} comments")
                    if result.get("error_count", 0) > 0:
                        print(f"⚠️ {result['error_count']} errors encountered")
                return 0
            else:
                # Default: process comments
                result = sentiment_tool.process_comments(limit=args.limit, method=args.method)
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    print(f"✅ Processed {result.get('processed_count', 0)} comments using {args.method} method")
                return 0

        except KeyboardInterrupt:
            sentiment_tool.log_progress("Sentiment analysis cancelled by user")
            return 1
        _exc_ept Exc_eption as _e:
            sentiment_tool.handle_error(e, "main execution")
            return 1


if __name__ == "__main__":
    sys.exit(main())
