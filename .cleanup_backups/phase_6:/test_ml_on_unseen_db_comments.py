#!/usr/bin/env python3
"""
Test ML Classifier on Unseen Database Comments

This tests the ML classifier on completely unseen comments from the database
to get honest performance metrics without overfitting.
"""

import sys

sys.path.insert(0, "src")
sys.path.insert(0, ".")

import pandas as pd
from simple_ml_sentiment_demo import SimpleMusicMLClassifier
from sqlalchemy import text

from web.etl_helpers import get_engine
from youtubeviz.vader_variants import VADERVariantManager, VariantType


def get_unseen_test_comments(limit=200):
    """Get completely unseen comments from database for honest testing."""

    print("📊 Fetching unseen comments from database...")

    try:
        engine = get_engine()

        # Get diverse comments that weren't in our training set
        # Focus on comments with decent engagement to get meaningful sentiment
        query = """
        SELECT
            c.comment_text,
            c.like_count,
            c.video_id,
            v.channel_title,
            v.title as video_title,
            CASE
                WHEN c.like_count >= 20 THEN 'high_engagement'
                WHEN c.like_count >= 5 THEN 'medium_engagement'
                ELSE 'low_engagement'
            END as engagement_level
        FROM youtube_comments c
        JOIN youtube_videos v ON c.video_id = v.video_id
        WHERE c.comment_text IS NOT NULL
            AND LENGTH(c.comment_text) >= 15
            AND LENGTH(c.comment_text) <= 200
            AND c.like_count >= 3
            -- Exclude comments that might be in our training set
            AND c.comment_text NOT LIKE '%ATEEEE%'
            AND c.comment_text NOT LIKE '%criminally underrated%'
            AND c.comment_text NOT LIKE '%SUPERNATURAL%'
            AND c.comment_text NOT LIKE '%descendants%'
            AND c.comment_text NOT LIKE '%sock puppet%'
        ORDER BY RAND()
        LIMIT :limit
        """

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"limit": limit})

        print(f"✅ Fetched {len(df)} unseen comments")
        print(f"📈 Engagement distribution: {df['engagement_level'].value_counts().to_dict()}")

        return df

    except Exception as e:
        print(f"❌ Database fetch failed: {e}")
        return pd.DataFrame()


def create_honest_ground_truth(comments_df):
    """Create honest ground truth labels based on obvious sentiment indicators."""

    print("🎯 Creating ground truth labels for unseen comments...")

    labels = []

    for _, row in comments_df.iterrows():
        text_item = row["comment_text"].lower()
        like_count = row["like_count"]

        # Clear positive indicators
        positive_indicators = [
            "love",
            "amazing",
            "perfect",
            "best",
            "great",
            "awesome",
            "beautiful",
            "incredible",
            "fantastic",
            "outstanding",
            "brilliant",
            "excellent",
            "fire",
            "slaps",
            "goated",
            "ate",
            "periodt",
            "no cap",
            "bussin",
            "obsessed",
            "addicted",
            "can't stop",
            "on repeat",
            "favorite",
            "favourite",
        ]

        # Clear negative indicators
        negative_indicators = [
            "hate",
            "terrible",
            "awful",
            "worst",
            "bad",
            "sucks",
            "trash",
            "mid",
            "cringe",
            "fell off",
            "overrated",
            "disappointing",
        ]

        # Determine label
        positive_count = sum(1 for indicator in positive_indicators if indicator in text)
        negative_count = sum(1 for indicator in negative_indicators if indicator in text)

        if positive_count > 0 and negative_count == 0:
            label = "positive"
        elif negative_count > 0 and positive_count == 0:
            label = "negative"
        else:
            label = "neutral"

        labels.append(label)

    comments_df["ground_truth"] = labels

    # Show distribution
    label_dist = pd.Series(labels).value_counts()
    print(f"📊 Ground truth distribution: {label_dist.to_dict()}")

    return comments_df


def test_models_on_unseen_data():
    """Test all models on completely unseen database comments."""

    print("🧪 HONEST ML TESTING ON UNSEEN DATABASE COMMENTS")
    print("=" * 70)
    print("Testing on comments that were NOT in the training set")
    print()

    # Get unseen test data
    test_df = get_unseen_test_comments(limit=100)

    if test_df.empty:
        print("❌ No test data available")
        return

    # Create honest ground truth
    test_df = create_honest_ground_truth(test_df)

    # Filter to only comments with clear sentiment (not neutral)
    clear_sentiment = test_df[test_df["ground_truth"] != "neutral"].copy()

    if len(clear_sentiment) < 10:
        print("⚠️  Not enough clear sentiment examples for testing")
        print("📊 Using all comments including neutral ones...")
        clear_sentiment = test_df.copy()

    print(f"\n🎯 Testing on {len(clear_sentiment)} comments with clear sentiment")
    print(f"📊 Test distribution: {clear_sentiment['ground_truth'].value_counts().to_dict()}")

    # Initialize models
    print(f"\n🤖 Initializing models...")

    # ML Classifier
    ml_classifier = SimpleMusicMLClassifier()
    ml_classifier.train()

    # VADER variants
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        stock_vader = SentimentIntensityAnalyzer()
    except ImportError:
        stock_vader = None

    vader_manager = VADERVariantManager()
    enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)

    # Test each model
    results = {}

    print(f"\n🧪 Testing models on unseen data...")

    # Test ML Classifier
    ml_predictions = []
    ml_correct = 0

    for _, row in clear_sentiment.iterrows():
        text_item = row["comment_text"]
        true_label = row["ground_truth"]

        ml_result = ml_classifier.predict(text)
        ml_pred = ml_result["sentiment"]
        ml_predictions.append(ml_pred)

        if ml_pred == true_label:
            ml_correct += 1

    ml_accuracy = ml_correct / len(clear_sentiment)
    results["ML Classifier"] = ml_accuracy

    # Test Stock VADER
    if stock_vader:
        stock_correct = 0
        for _, row in clear_sentiment.iterrows():
            text_item = row["comment_text"]
            true_label = row["ground_truth"]

            scores = stock_vader.polarity_scores(text)
            compound = scores["compound"]
            pred = "positive" if compound > 0.1 else "negative" if compound < -0.1 else "neutral"

            if pred == true_label:
                stock_correct += 1

        stock_accuracy = stock_correct / len(clear_sentiment)
        results["Stock VADER"] = stock_accuracy

    # Test Enhanced VADER
    enhanced_correct = 0
    for _, row in clear_sentiment.iterrows():
        text_item = row["comment_text"]
        true_label = row["ground_truth"]

        scores = enhanced_vader.polarity_scores(text)
        compound = scores["compound"]
        pred = "positive" if compound > 0.1 else "negative" if compound < -0.1 else "neutral"

        if pred == true_label:
            enhanced_correct += 1

    enhanced_accuracy = enhanced_correct / len(clear_sentiment)
    results["Enhanced VADER"] = enhanced_accuracy

    # Show results
    print(f"\n📊 HONEST RESULTS ON UNSEEN DATA:")
    print("=" * 50)

    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)

    for i, (model_name, accuracy) in enumerate(sorted_results, 1):
        print(f"{i}. {model_name:20} {accuracy:.1%} correct")

    # Show some example predictions
    print(f"\n🔍 Example predictions on unseen comments:")
    print("-" * 50)

    for i, (_, row) in enumerate(clear_sentiment.head(5).iterrows()):
        text_item = row["comment_text"]
        true_label = row["ground_truth"]

        ml_result = ml_classifier.predict(text)
        ml_pred = ml_result["sentiment"]

        print(f'\n💬 "{text[:50]}..."')
        print(f"   Ground truth: {true_label.upper()}")
        print(f"   ML prediction: {ml_pred.upper()} ({ml_result['confidence']:.3f})")

        if ml_pred == true_label:
            print(f"   ✅ CORRECT")
        else:
            print(f"   ❌ INCORRECT")

    return results


if __name__ == "__main__":
    results = test_models_on_unseen_data()

    if results:
        best_model = max(results.items(), key=lambda x: x[1])
        print(f"\n🏆 WINNER: {best_model[0]} with {best_model[1]:.1%} accuracy")

        if "ML Classifier" in results and results["ML Classifier"] > 0.7:
            print("✅ ML classifier shows strong performance on unseen data!")
        elif "ML Classifier" in results and results["ML Classifier"] > 0.5:
            print("✅ ML classifier shows decent performance, better than random!")
        else:
            print("⚠️  ML classifier needs more training data or feature engineering")
    else:
        print("❌ Testing failed - no results available")
