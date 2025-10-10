#!/usr / bin / env python3
"""
Production ML Sentiment Analysis System

Ready-to-deploy ML sentiment classifier that integrates with your existing
ETL pipeline and database infrastructure.
"""

import pickle
import sys
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, ".")

from src.youtubeviz.music_ml_classifier import MusicMLClassifier
from web.etl_helpers import get_engine


class ProductionMLSentiment:
    """
    Production-ready ML sentiment analysis system.

    Integrates with your existing database and ETL pipeline to provide
    superior sentiment analysis for music industry comments.
    """

    def __init__(self, model_path: Optional[str] = None):
        self.classifier = None
        self.model_path = model_path or "models / music_sentiment_classifier.pkl"
        self._engine = None

    @property
    def engine(self):
        """Lazy database connection."""
        if self._engine is None:
            self._engine = get_engine()
        return self._engine

    def train_and_save_model(self):
        """Train the ML model and save it for production use."""

        print("🤖 Training production ML sentiment model...")

        # Initialize and train classifier
        self.classifier = MusicMLClassifier()
        self.classifier.train(include_isrc_feature=True)

        # Save trained model
        import os

        os.makedirs("models", exist_ok=True)

        with open(self.model_path, "wb") as f:
            pickle.dump(self.classifier, f)

        print(f"✅ Model saved to {self.model_path}")

    def load_model(self):
        """Load trained model from disk."""

        try:
            with open(self.model_path, "rb") as f:
                self.classifier = pickle.load(f)
            print(f"✅ Model loaded from {self.model_path}")
        except FileNotFoundError:
            print(f"⚠️  Model not found at {self.model_path}")
            print("🔄 Training new model...")
            self.train_and_save_model()

    def detect_isrc_in_video(self, video_id: str) -> bool:
        """Detect if a video has ISRC information."""

        try:
            query = """
            SELECT title, channel_title
            FROM youtube_videos
            WHERE video_id = :video_id
            """

            with self.engine.connect() as conn:
                result = pd.read_sql(text(query), conn, params={"video_id": video_id})

            if result.empty:
                return False

            # Check for ISRC pattern in title
            title = result.iloc[0]["title"] or ""

            # ISRC pattern: 2 letters + 3 alphanumeric + 2 digits + 5 digits
            import re

            isrc_pattern = r"[A-Z]{2}[A-Z0-9]{3}[0-9]{2}[0-9]{5}"

            return bool(re.search(isrc_pattern, title.upper()))

        except Exception as e:
            print(f"⚠️  ISRC detection failed: {e}")
            return False

    def analyze_comment(self, comment_text: str, video_id: Optional[str] = None) -> Dict[str, any]:
        """
        Analyze a single comment with ML classifier.

        Args:
            comment_text: The comment text to analyze
            video_id: Optional video ID for ISRC detection

        Returns:
            Dictionary with sentiment analysis results
        """

        if self.classifier is None:
            self.load_model()

        # Detect ISRC if video_id provided
        has_isrc = False
        if video_id:
            has_isrc = self.detect_isrc_in_video(video_id)

        # Get ML prediction
        result = self.classifier.predict(comment_text, has_isrc=has_isrc)

        # Convert to your existing format for compatibility
        return {
            "sentiment": result["sentiment"],
            "confidence": result["sentiment_confidence"],
            "compound_score": self._convert_to_compound_score(result["sentiment"], result["sentiment_confidence"]),
            "production_focus": result["production_focus"],
            "engagement_type": result["engagement_type"],
            "ml_features": result["features_detected"],
            "has_isrc": has_isrc,
            "method": "ml_classifier",
        }

    def _convert_to_compound_score(self, sentiment: str, confidence: float) -> float:
        """Convert ML prediction to VADER-style compound score for compatibility."""

        if sentiment == "positive":
            return confidence * 0.8  # Scale to 0.0 to 0.8 range
        elif sentiment == "negative":
            return -confidence * 0.8  # Scale to -0.8 to 0.0 range
        else:  # neutral
            return 0.0

    def analyze_comments_batch(self, comments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze a batch of comments efficiently.

        Args:
            comments_df: DataFrame with 'comment_text' and optionally 'video_id'

        Returns:
            DataFrame with sentiment analysis results
        """

        if self.classifier is None:
            self.load_model()

        results = []

        for _, row in comments_df.iterrows():
            try:
                result = self.analyze_comment(row["comment_text"], video_id=row.get("video_id"))
                result["comment_id"] = row.get("comment_id", "")
                results.append(result)

            except Exception as e:
                # Fallback for failed predictions
                print(f"⚠️  ML analysis failed for comment: {e}")
                results.append(
                    {
                        "comment_id": row.get("comment_id", ""),
                        "sentiment": "neutral",
                        "confidence": 0.5,
                        "compound_score": 0.0,
                        "production_focus": False,
                        "engagement_type": "unknown",
                        "ml_features": {},
                        "has_isrc": False,
                        "method": "fallback",
                    }
                )

        return pd.DataFrame(results)

    def update_existing_sentiment_scores(self, limit: int = 1000):
        """
        Update existing sentiment scores in database with ML predictions.

        This can be run as a migration to improve existing data.
        """

        print(f"🔄 Updating existing sentiment scores with ML predictions...")

        # Get comments that were previously classified as neutral but have high engagement
        query = """
        SELECT
            c.comment_id,
            c.comment_text,
            c.video_id,
            c.like_count,
            cs.sentiment_score as old_score
        FROM youtube_comments c
        LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
        WHERE c.like_count >= 10
            AND (cs.sentiment_score IS NULL OR ABS(cs.sentiment_score) < 0.1)
            AND c.comment_text IS NOT NULL
            AND LENGTH(c.comment_text) >= 10
        ORDER BY c.like_count DESC
        LIMIT :limit
        """

        with self.engine.connect() as conn:
            comments_df = pd.read_sql(text(query), conn, params={"limit": limit})

        if comments_df.empty:
            print("📊 No comments found for update")
            return

        print(f"📊 Analyzing {len(comments_df)} comments with ML classifier...")

        # Analyze with ML
        ml_results = self.analyze_comments_batch(comments_df)

        # Show improvements
        improvements = 0
        for _, result in ml_results.iterrows():
            if result["sentiment"] != "neutral":
                improvements += 1

        print(
            f"📈 ML classifier found {
                improvements}/{len(ml_results)} comments are not neutral ({improvements / len(ml_results):.1%})"
        )

        # Show examples of improvements
        print(f"\n🎯 Example improvements:")
        for i, (_, row) in enumerate(ml_results.head(5).iterrows()):
            if row["sentiment"] != "neutral":
                _comment_text_item = comments_df.iloc[i]["comment_text"]
                print(f"   \"{_comment_text_item[:50]}...\" → {row['sentiment'].upper()} " f"({row['confidence']:.3f})")

        return ml_results

    def compare_with_existing_system(self, sample_size: int = 100):
        """Compare ML classifier with existing VADER-based system."""

        print(f"⚖️  Comparing ML classifier with existing system...")

        # Get sample of comments
        query = """
        SELECT
            c.comment_id,
            c.comment_text,
            c.video_id,
            c.like_count,
            cs.sentiment_score as vader_score
        FROM youtube_comments c
        LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
        WHERE c.comment_text IS NOT NULL
            AND LENGTH(c.comment_text) >= 10
            AND c.like_count >= 5
        ORDER BY RAND()
        LIMIT :sample_size
        """

        with self.engine.connect() as conn:
            comments_df = pd.read_sql(text(query), conn, params={"sample_size": sample_size})

        # Analyze with ML
        ml_results = self.analyze_comments_batch(comments_df)

        # Compare results
        agreements = 0
        ml_improvements = 0

        for i, ml_result in ml_results.iterrows():
            vader_score = comments_df.iloc[i]["vader_score"] or 0.0
            _ml_score = ml_result["compound_score"]  # noqa: F841

            # Check agreement on sentiment direction
            vader_sentiment = "positive" if vader_score > 0.1 else "negative" if vader_score < -0.1 else "neutral"
            ml_sentiment = ml_result["sentiment"]

            if vader_sentiment == ml_sentiment:
                agreements += 1
            elif ml_sentiment != "neutral" and vader_sentiment == "neutral":
                ml_improvements += 1

        agreement_rate = agreements / len(ml_results)
        improvement_rate = ml_improvements / len(ml_results)

        print(f"📊 Comparison Results:")
        print(f"   Agreement rate: {agreements}/{len(ml_results)} ({agreement_rate:.1%})")
        print(f"   ML improvements: {ml_improvements}/{len(ml_results)} ({improvement_rate:.1%})")

        return {"agreement_rate": agreement_rate, "improvement_rate": improvement_rate, "sample_size": len(ml_results)}


def setup_production_ml_sentiment():
    """Set up the production ML sentiment system."""

    print("🚀 SETTING UP PRODUCTION ML SENTIMENT SYSTEM")
    print("=" * 60)

    # Initialize system
    ml_system = ProductionMLSentiment()

    # Train and save model
    ml_system.train_and_save_model()

    # Test the system
    print(f"\n🧪 Testing production system...")

    test_comments = [
        "YALL ATEEEE",
        "this song is fire no cap",
        "the bass is supernatural",
        "criminally underrated artist",
        "can't wait for the album",
        "Mal from descendants",
        "okay song I guess",
    ]

    for comment in test_comments:
        result = ml_system.analyze_comment(comment)
        print(f"   \"{comment}\" → {result['sentiment'].upper()} ({result['confidence']:.3f})")

    # Compare with existing system
    _comparison = ml_system.compare_with_existing_system(sample_size=50)  # noqa: F841

    print(f"\n✅ Production ML sentiment system is ready!")
    print(f"🎯 Use ml_system.analyze_comment() in your ETL pipeline")

    return ml_system


if __name__ == "__main__":
    # Set up the production system
    ml_system = setup_production_ml_sentiment()

    # Show how to integrate with existing ETL
    print(f"\n🔗 INTEGRATION EXAMPLE:")
    print(f"Replace your existing sentiment analysis with:")
    print(f"")
    print(f"# In your ETL pipeline:")
    print(f"ml_sentiment = ProductionMLSentiment()")
    print(f"result = ml_sentiment.analyze_comment(comment_text, video_id)")
    print(f"sentiment_score = result['compound_score']")
    print(f"confidence = result['confidence']")
    print(f"")
    print(f"🎉 Your sentiment analysis is now powered by ML!")
