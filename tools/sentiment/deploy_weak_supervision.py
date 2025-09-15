#!/usr/bin/env python3
"""
Deploy Weak Supervision Sentiment Analysis to Production

This script:
1. Trains the weak supervision model on existing comments
2. Updates the comment_sentiment table with new analysis
3. Populates beat_appreciation flags
4. Validates the deployment
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine
from youtubeviz.weak_supervision_sentiment import WeakSupervisionSentimentAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_comments_for_training():
    """Load all comments from database for training."""
    engine = get_engine()

    query = """
    SELECT 
        yc.comment_id,
        yc.comment_text,
        yc.video_id,
        yv.channel_title as artist_name
    FROM youtube_comments yc
    JOIN youtube_videos yv ON yc.video_id = yv.video_id
    WHERE yc.comment_text IS NOT NULL 
    AND LENGTH(yc.comment_text) > 0
    ORDER BY yc.comment_id
    """

    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} comments for training")
    return df


def train_and_deploy_model():
    """Train the weak supervision model and deploy to production."""
    logger.info("🚀 Starting weak supervision sentiment deployment...")

    # Load training data
    comments_df = load_comments_for_training()
    if len(comments_df) < 100:
        raise ValueError(f"Insufficient training data: {len(comments_df)} comments")

    # Initialize and train analyzer
    analyzer = WeakSupervisionSentimentAnalyzer()

    # Train on comment texts
    training_texts = comments_df["comment_text"].tolist()
    metrics = analyzer.train_classifier(training_texts)

    logger.info("✅ Model trained successfully:")
    logger.info("   - Training size: %s", metrics["training_size"])
    logger.info("   - Macro-F1: %.3f", metrics["macro_f1"])
    logger.info("   - Label distribution: %s", metrics["label_distribution"])

    # Save model
    model_path = "models/weak_supervision_sentiment.joblib"
    os.makedirs("models", exist_ok=True)
    analyzer.save_model(model_path)

    return analyzer, comments_df


def update_sentiment_table(analyzer, comments_df):
    """Update the comment_sentiment table with new analysis."""
    logger.info("📊 Updating comment_sentiment table...")

    engine = get_engine()

    # Clear existing sentiment data
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM comment_sentiment"))
        conn.commit()
        logger.info("   - Cleared existing sentiment data")

    # Process comments in batches
    batch_size = 1000
    total_processed = 0
    beat_appreciation_count = 0

    for i in range(0, len(comments_df), batch_size):
        batch = comments_df.iloc[i : i + batch_size]

        sentiment_records = []
        for _, row in batch.iterrows():
            try:
                result = analyzer.predict(row["comment_text"])

                # Determine beat appreciation
                beat_appreciation = (
                    "beat" in row["comment_text"].lower()
                    or "production" in row["comment_text"].lower()
                    or "producer" in row["comment_text"].lower()
                )

                if beat_appreciation:
                    beat_appreciation_count += 1

                sentiment_records.append(
                    {
                        "comment_id": row["comment_id"],
                        "video_id": row["video_id"],
                        "comment_text": row["comment_text"],
                        "sentiment_score": result["sentiment_score"],
                        "confidence_score": result["confidence"],
                        "confidence": result["confidence"],
                        "beat_appreciation": 1 if beat_appreciation else 0,
                        "created_at": pd.Timestamp.now(),
                        "processed_at": pd.Timestamp.now(),
                    }
                )

            except Exception as e:
                logger.warning(f"Failed to analyze comment {row['comment_id']}: {e}")
                continue

        # Insert batch
        if sentiment_records:
            sentiment_df = pd.DataFrame(sentiment_records)
            sentiment_df.to_sql("comment_sentiment", engine, if_exists="append", index=False)
            total_processed += len(sentiment_records)

        if (i // batch_size + 1) % 10 == 0:
            logger.info(f"   - Processed {total_processed} comments...")

    logger.info(f"✅ Updated sentiment for {total_processed} comments")
    logger.info(f"🎵 Beat appreciation detected in {beat_appreciation_count} comments")

    return total_processed, beat_appreciation_count


def validate_deployment():
    """Validate the sentiment deployment."""
    logger.info("🔍 Validating deployment...")

    engine = get_engine()

    # Check coverage
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT 
                COUNT(DISTINCT yc.comment_id) as total_comments,
                COUNT(DISTINCT cs.comment_id) as analyzed_comments,
                AVG(cs.sentiment_score) as avg_sentiment,
                SUM(CASE WHEN cs.beat_appreciation THEN 1 ELSE 0 END) as beat_appreciation_count
            FROM youtube_comments yc
            LEFT JOIN comment_sentiment cs ON yc.comment_id = cs.comment_id
        """
            )
        ).fetchone()

        total_comments = result[0]
        analyzed_comments = result[1]
        avg_sentiment = result[2] or 0
        beat_count = result[3] or 0

        coverage = (analyzed_comments / total_comments * 100) if total_comments > 0 else 0

        logger.info(f"📊 Validation Results:")
        logger.info(f"   - Total comments: {total_comments}")
        logger.info(f"   - Analyzed comments: {analyzed_comments}")
        logger.info(f"   - Coverage: {coverage:.1f}%")
        logger.info(f"   - Average sentiment: {avg_sentiment:.3f}")
        logger.info(f"   - Beat appreciation: {beat_count}")

        if coverage >= 95:
            logger.info("✅ Deployment validation PASSED")
            return True
        else:
            logger.error("❌ Deployment validation FAILED - insufficient coverage")
            return False


def main():
    """Main deployment function."""
    try:
        # Train and deploy model
        analyzer, comments_df = train_and_deploy_model()

        # Update sentiment table
        total_processed, beat_count = update_sentiment_table(analyzer, comments_df)

        # Validate deployment
        if validate_deployment():
            logger.info("🎉 Weak supervision sentiment analysis deployed successfully!")
            logger.info(f"📊 Final Stats:")
            logger.info(f"   - Comments processed: {total_processed}")
            logger.info(f"   - Beat appreciation detected: {beat_count}")
            return True
        else:
            logger.error("❌ Deployment failed validation")
            return False

    except Exception as e:
        logger.error(f"❌ Deployment failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
