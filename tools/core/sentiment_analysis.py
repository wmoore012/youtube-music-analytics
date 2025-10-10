#!/usr/bin/env python3
"""
Sentiment Analysis Module for YouTube ETL Pipeline

Provides a clean interface to the sentiment scoring functionality
used by run_focused_etl.py and run_comprehensive_etl.py.

This module wraps the production-ready YouTubeCommentSentimentJob
from web/sentiment_job.py with a simple function interface.
"""

from typing import Optional

from sqlalchemy.engine import Engine

from web.sentiment_job import YouTubeCommentSentimentJob


def process_sentiment_analysis(engine: Engine, limit: Optional[int] = None) -> int:
    """
    Process sentiment analysis for unscored YouTube comments.

    This is the main function called by ETL pipelines to score comments
    that don't yet have sentiment scores.

    Args:
        engine: SQLAlchemy engine (not used - job creates its own connection)
        limit: Maximum number of comments to process in this batch

    Returns:
        int: Number of comments successfully processed

    Example:
        >>> from web.etl_helpers import get_engine
        >>> engine = get_engine()
        >>> processed = process_sentiment_analysis(engine, limit=500)
        >>> print(f"Processed {processed} comments")
    """
    # Create sentiment job instance
    job = YouTubeCommentSentimentJob()

    # Process batch of comments
    batch_size = limit if limit else 500
    stats = job.score_batch(limit=batch_size)

    # Return number of successfully updated comments
    return stats.updated


def get_sentiment_statistics(engine: Engine) -> dict:
    """
    Get summary statistics about sentiment analysis coverage.

    Args:
        engine: SQLAlchemy engine for database queries

    Returns:
        dict: Statistics including total comments, scored comments, etc.
    """
    from sqlalchemy import text

    with engine.connect() as conn:
        # Get total comments
        result = conn.execute(text("SELECT COUNT(*) FROM youtube_comments"))
        total_comments = result.fetchone()[0]

        # Get scored comments
        result = conn.execute(
            text(
                """
            SELECT COUNT(*)
            FROM youtube_comments
            WHERE sentiment_score IS NOT NULL
        """
            )
        )
        scored_comments = result.fetchone()[0]

        # Get unscored comments
        unscored = total_comments - scored_comments

        # Get average sentiment
        result = conn.execute(
            text(
                """
            SELECT AVG(sentiment_score) as avg_sentiment
            FROM youtube_comments
            WHERE sentiment_score IS NOT NULL
        """
            )
        )
        avg_sentiment = result.fetchone()[0] or 0.0

    return {
        "total_comments": total_comments,
        "scored_comments": scored_comments,
        "unscored_comments": unscored,
        "coverage_percentage": (scored_comments / total_comments * 100) if total_comments > 0 else 0.0,
        "average_sentiment": float(avg_sentiment),
    }


if __name__ == "__main__":
    """
    Standalone execution for testing or manual sentiment processing.
    """
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    from web.etl_helpers import get_engine

    print("🧠 Sentiment Analysis Module")
    print("=" * 50)

    engine = get_engine()

    # Get current statistics
    stats = get_sentiment_statistics(engine)
    print(f"\n📊 Current Status:")
    print(f"   Total comments: {stats['total_comments']:,}")
    print(f"   Scored comments: {stats['scored_comments']:,}")
    print(f"   Unscored comments: {stats['unscored_comments']:,}")
    print(f"   Coverage: {stats['coverage_percentage']:.1f}%")
    print(f"   Average sentiment: {stats['average_sentiment']:.3f}")

    if stats["unscored_comments"] > 0:
        print(f"\n🔄 Processing {min(stats['unscored_comments'], 500)} comments...")
        processed = process_sentiment_analysis(engine, limit=500)
        print(f"✅ Processed {processed} comments")

        # Show updated statistics
        stats = get_sentiment_statistics(engine)
        print(f"\n📊 Updated Status:")
        print(f"   Scored comments: {stats['scored_comments']:,}")
        print(f"   Coverage: {stats['coverage_percentage']:.1f}%")
    else:
        print("\n✅ All comments are already scored!")

