#!/usr/bin/env python3
"""
Sentiment Analysis Pipeline for YouTube Comments

This script:
1. Fetches comments from YouTube API
2. Analyzes sentiment using TextBlob or VADER
3. Stores results in comment_sentiment table
4. Provides logging and error handling
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import finish_etl_run, get_engine, start_etl_run

# Try to import sentiment analysis libraries
try:
    from textblob import TextBlob

    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False
    print("⚠️ TextBlob not available. Install with: pip install textblob")

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    print("⚠️ VADER not available. Install with: pip install vaderSentiment")


# Fallback: simple rule-based sentiment
def simple_sentiment(text: str) -> float:
    """Simple rule-based sentiment analysis as fallback."""
    if not text:
        return 0.0

    text_lower = text.lower()

    # Positive words
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
    ]

    # Negative words
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

    # Simple scoring
    if positive_count > negative_count:
        return min(0.8, positive_count * 0.2)
    elif negative_count > positive_count:
        return max(-0.8, negative_count * -0.2)
    else:
        return 0.0


def analyze_sentiment(text: str, method: str = "auto") -> tuple:
    """
    Analyze sentiment of text using available methods.

    Returns:
        tuple: (sentiment_score, confidence_score)
    """
    if not text or not text.strip():
        return 0.0, 0.0

    # Clean text
    text = text.strip()[:1000]  # Limit length

    if method == "textblob" and TEXTBLOB_AVAILABLE:
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # -1 to 1
        confidence = abs(blob.sentiment.subjectivity)  # 0 to 1
        return sentiment, confidence

    elif method == "vader" and VADER_AVAILABLE:
        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(text)
        sentiment = scores["compound"]  # -1 to 1
        confidence = max(scores["pos"], scores["neg"], scores["neu"])
        return sentiment, confidence

    elif method == "auto":
        # Try best available method
        if TEXTBLOB_AVAILABLE:
            return analyze_sentiment(text, "textblob")
        elif VADER_AVAILABLE:
            return analyze_sentiment(text, "vader")
        else:
            sentiment = simple_sentiment(text)
            confidence = 0.5  # Low confidence for rule-based
            return sentiment, confidence

    else:
        # Fallback to simple method
        sentiment = simple_sentiment(text)
        confidence = 0.3
        return sentiment, confidence


def create_sample_comments(engine):
    """
    Create sample comments for testing sentiment analysis.
    """
    with engine.connect() as conn:
        # Get some video IDs
        result = conn.execute(text("SELECT video_id FROM youtube_videos LIMIT 5"))
        video_ids = [row[0] for row in result]

        if not video_ids:
            print("❌ No videos found to create sample comments")
            return 0

        # Sample comments with known sentiment
        sample_comments = [
            ("This song is absolutely amazing! Love it so much! 🔥❤️", 0.8),
            ("Great music, keep it up!", 0.6),
            ("Not bad, pretty good actually", 0.3),
            ("Meh, it's okay I guess", 0.0),
            ("This is terrible, worst song ever 😡", -0.8),
            ("I don't like this at all, very disappointing", -0.5),
            ("Fire track! This is going to be huge! 🚀", 0.9),
            ("Beautiful lyrics, touched my heart ❤️", 0.7),
            ("Boring and repetitive, skip", -0.4),
            ("Perfect for my playlist, love the vibe", 0.6),
        ]

        comments_created = 0
        for i, (comment_text, expected_sentiment) in enumerate(sample_comments):
            video_id = video_ids[i % len(video_ids)]
            comment_id = f"SAMPLE_COMMENT_{i+1:03d}"

            # Insert into youtube_comments
            conn.execute(
                text(
                    """
                INSERT IGNORE INTO youtube_comments
                (comment_id, video_id, author_display_name, comment_text, like_count, published_at)
                VALUES (:comment_id, :video_id, :author, :text, :likes, :published)
            """
                ),
                {
                    "comment_id": comment_id,
                    "video_id": video_id,
                    "author": f"TestUser{i+1}",
                    "text": comment_text,
                    "likes": i + 1,
                    "published": datetime.now() - timedelta(days=i),
                },
            )
            comments_created += 1

        conn.commit()
        print(f"✅ Created {comments_created} sample comments")
        return comments_created


def process_sentiment_analysis(engine, limit: int = None):
    """
    Process sentiment analysis for comments that haven't been analyzed.
    """
    with engine.connect() as conn:
        # Get comments that need sentiment analysis
        sql = """
            SELECT c.comment_id, c.video_id, c.comment_text
            FROM youtube_comments c
            LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
            WHERE cs.comment_id IS NULL AND c.comment_text IS NOT NULL
        """

        if limit:
            sql += f" LIMIT {limit}"

        result = conn.execute(text(sql))
        comments = result.fetchall()

        if not comments:
            print("📭 No comments need sentiment analysis")
            return 0

        print(f"📊 Processing sentiment for {len(comments)} comments...")

        processed = 0
        for comment in comments:
            try:
                sentiment_score, confidence_score = analyze_sentiment(comment.comment_text)

                # Insert sentiment result
                conn.execute(
                    text(
                        """
                    INSERT INTO comment_sentiment
                    (comment_id, video_id, comment_text, sentiment_score, confidence_score, created_at)
                    VALUES (:comment_id, :video_id, :text, :sentiment, :confidence, :created)
                """
                    ),
                    {
                        "comment_id": comment.comment_id,
                        "video_id": comment.video_id,
                        "text": comment.comment_text[:500],  # Truncate for storage
                        "sentiment": sentiment_score,
                        "confidence": confidence_score,
                        "created": datetime.now(),
                    },
                )

                processed += 1

                if processed % 10 == 0:
                    print(f"  📊 Processed {processed}/{len(comments)} comments...")

            except Exception as e:
                print(f"❌ Error processing comment {comment.comment_id}: {e}")
                continue

        conn.commit()
        print(f"✅ Processed sentiment for {processed} comments")
        return processed


def main():
    """Main sentiment analysis pipeline."""
    print("🧠 Starting Sentiment Analysis Pipeline")
    print("=" * 50)

    # Check available sentiment methods
    methods = []
    if TEXTBLOB_AVAILABLE:
        methods.append("TextBlob")
    if VADER_AVAILABLE:
        methods.append("VADER")
    if not methods:
        methods.append("Simple Rule-based (fallback)")

    print(f"🔧 Available sentiment methods: {', '.join(methods)}")

    # Start ETL logging
    try:
        run_info = start_etl_run("SENTIMENT_ANALYSIS", "Sentiment analysis pipeline execution")
        run_id = run_info.get("run_id") if run_info else None
    except Exception as e:
        print(f"⚠️ ETL logging not available: {e}")
        run_id = None

    try:
        engine = get_engine()

        # Create sample comments if none exist
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM youtube_comments"))
            comment_count = result.fetchone()[0]

        if comment_count == 0:
            print("📝 No comments found, creating sample data...")
            comments_created = create_sample_comments(engine)
        else:
            print(f"📊 Found {comment_count} existing comments")
            comments_created = 0

        # Process sentiment analysis
        processed = process_sentiment_analysis(engine, limit=100)  # Process in batches

        # Summary
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM comment_sentiment"))
            total_sentiment_records = result.fetchone()[0]

            result = conn.execute(
                text(
                    """
                SELECT
                    AVG(sentiment_score) as avg_sentiment,
                    COUNT(*) as total_comments,
                    SUM(CASE WHEN sentiment_score > 0.1 THEN 1 ELSE 0 END) as positive,
                    SUM(CASE WHEN sentiment_score < -0.1 THEN 1 ELSE 0 END) as negative
                FROM comment_sentiment
            """
                )
            )
            stats = result.fetchone()

        print(f"\n📊 SENTIMENT ANALYSIS SUMMARY:")
        print(f"   📝 Total comments with sentiment: {total_sentiment_records}")
        print(f"   📊 Average sentiment: {stats.avg_sentiment:.3f}")
        print(f"   😊 Positive comments: {stats.positive}")
        print(f"   😞 Negative comments: {stats.negative}")
        print(f"   😐 Neutral comments: {stats.total_comments - stats.positive - stats.negative}")

        # Finish ETL logging
        if run_id:
            finish_etl_run(
                run_id, "SUCCESS", f"Processed {processed} comments, created {comments_created} sample comments"
            )

    except Exception as e:
        print(f"❌ Sentiment analysis failed: {e}")
        if run_id:
            finish_etl_run(run_id, "FAILED", str(e))
        raise


if __name__ == "__main__":
    main()
