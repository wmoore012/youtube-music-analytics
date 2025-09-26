#!/usr/bin/env python3
"""
Benchmark Real Comments with User Feedback

Uses your actual database comments and incorporates your feedback
about which comments are positive vs neutral.
"""

import sys

sys.path.insert(0, "src")

from youtubeviz.proprietary_sentiment_formula import ProprietarySentimentEnhancer
from youtubeviz.unique_comment_manager import get_unique_comments_for_benchmark
from youtubeviz.vader_variants import VADERVariantManager, VariantType


def get_real_comments_sample(count: int = 300):
    """Get real comments from your database."""

    print(f"🗄️  Fetching {count} real comments from your database...")

    comments_data = get_unique_comments_for_benchmark("comment_benchmark", count)

    if not comments_data:
        print("❌ No comments available from database")
        return []

    # Extract just the comment text and metadata
    comments = []
    for comment_data in comments_data:
        comments.append(
            {
                "text": comment_data["comment_text"],
                "like_count": comment_data.get("like_count", 0),
                "channel": comment_data.get("channel_title", "Unknown"),
            }
        )

    print(f"✅ Got {len(comments)} real comments from your database")
    return comments


def analyze_with_current_systems(comments):
    """Analyze comments with current sentiment systems."""

    print(f"\n🧪 ANALYZING {len(comments)} REAL COMMENTS")
    print("=" * 60)

    # Initialize systems
    vader_manager = VADERVariantManager()
    enhancer = ProprietarySentimentEnhancer()

    # Get different VADER variants
    stock_vader = vader_manager.create_variant(VariantType.MINIMAL)  # Closest to stock
    enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)

    results = []

    for i, comment in enumerate(comments, 1):
        text_item = comment["text"]
        like_count = comment["like_count"]

        # Stock VADER
        stock_scores = stock_vader.polarity_scores(text)
        stock_sentiment = classify_sentiment(stock_scores["compound"])

        # Enhanced VADER
        enhanced_scores = enhanced_vader.polarity_scores(text)
        enhanced_sentiment = classify_sentiment(enhanced_scores["compound"])

        # Proprietary enhancement
        try:
            from textblob import TextBlob

            textblob_score = TextBlob(text).sentiment.polarity

            proprietary_score, proprietary_confidence = enhancer.enhance_sentiment_score(
                enhanced_scores["compound"], textblob_score, text
            )
            proprietary_sentiment = classify_sentiment(proprietary_score)
        except Exception:
            proprietary_score = enhanced_scores["compound"]
            proprietary_confidence = 0.5
            proprietary_sentiment = enhanced_sentiment

        results.append(
            {
                "text": text,
                "like_count": like_count,
                "channel": comment["channel"],
                "stock_vader": {"score": stock_scores["compound"], "sentiment": stock_sentiment},
                "enhanced_vader": {"score": enhanced_scores["compound"], "sentiment": enhanced_sentiment},
                "proprietary": {
                    "score": proprietary_score,
                    "confidence": proprietary_confidence,
                    "sentiment": proprietary_sentiment,
                },
            }
        )

        if i % 50 == 0:
            print(f"   Processed {i}/{len(comments)} comments...")

    return results


def classify_sentiment(score):
    """Classify sentiment based on compound score."""
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"


def show_problematic_neutrals(results):
    """Show comments classified as neutral that might be wrong."""

    print(f"\n🔍 COMMENTS CLASSIFIED AS 'NEUTRAL' - DO THEY LOOK NEUTRAL?")
    print("=" * 80)

    # Find comments classified as neutral by any system
    neutral_comments = []

    for result in results:
        # Check if any system classified it as neutral
        if (
            result["stock_vader"]["sentiment"] == "neutral"
            or result["enhanced_vader"]["sentiment"] == "neutral"
            or result["proprietary"]["sentiment"] == "neutral"
        ):

            neutral_comments.append(result)

    # Show top 20 neutral classifications
    neutral_comments = sorted(neutral_comments, key=lambda x: x["like_count"], reverse=True)

    for i, comment in enumerate(neutral_comments[:20], 1):
        text_item = comment["text"]
        like_count = comment["like_count"]

        # Show which systems called it neutral
        neutral_systems = []
        if comment["stock_vader"]["sentiment"] == "neutral":
            neutral_systems.append("Stock VADER")
        if comment["enhanced_vader"]["sentiment"] == "neutral":
            neutral_systems.append("Enhanced VADER")
        if comment["proprietary"]["sentiment"] == "neutral":
            neutral_systems.append("Proprietary")

        print(f'{i:2d}. "{text}" (👍 {like_count})')
        print(f"    Classified as NEUTRAL by: {', '.join(neutral_systems)}")
        print(
            f"    Stock: {comment['stock_vader']['score']:.3f} | Enhanced: {comment['enhanced_vader']['score']:.3f} | Proprietary: {comment['proprietary']['score']:.3f}"
        )
        print()


def show_system_comparison(results):
    """Show comparison between different systems."""

    print(f"\n📊 SYSTEM COMPARISON SUMMARY")
    print("=" * 50)

    # Count classifications by each system
    stock_counts = {"positive": 0, "negative": 0, "neutral": 0}
    enhanced_counts = {"positive": 0, "negative": 0, "neutral": 0}
    proprietary_counts = {"positive": 0, "negative": 0, "neutral": 0}

    for result in results:
        stock_counts[result["stock_vader"]["sentiment"]] += 1
        enhanced_counts[result["enhanced_vader"]["sentiment"]] += 1
        proprietary_counts[result["proprietary"]["sentiment"]] += 1

    total = len(results)

    print(f"Stock VADER:")
    for sentiment, count in stock_counts.items():
        pct = (count / total) * 100
        print(f"  {sentiment}: {count} ({pct:.1f}%)")

    print(f"\nEnhanced VADER:")
    for sentiment, count in enhanced_counts.items():
        pct = (count / total) * 100
        print(f"  {sentiment}: {count} ({pct:.1f}%)")

    print(f"\nProprietary System:")
    for sentiment, count in proprietary_counts.items():
        pct = (count / total) * 100
        print(f"  {sentiment}: {count} ({pct:.1f}%)")

    # Show agreement between systems
    agreements = 0
    for result in results:
        if (
            result["stock_vader"]["sentiment"]
            == result["enhanced_vader"]["sentiment"]
            == result["proprietary"]["sentiment"]
        ):
            agreements += 1

    agreement_pct = (agreements / total) * 100
    print(f"\nSystem Agreement: {agreements}/{total} ({agreement_pct:.1f}%)")


def incorporate_user_feedback():
    """Incorporate the user's feedback about positive comments."""

    print(f"\n👤 INCORPORATING YOUR FEEDBACK")
    print("=" * 40)

    # Comments you identified as positive (not neutral)
    user_positive_examples = [
        "I don't care what know one say this song 🔥 🔥 love pulling playing this",
        "done sold an o before I even had me sum sexx⛽️",
        "Dude love this. Where can I listen to the full song??",
        "get my son on trending",
        "I relate to this so much ❤️",
        "One of the dopest artists out.Modern beauty with a vintage voice perfect balance...",
        "Why this not on Spotify",
        "The outfits!!!",
    ]

    print("✅ Your feedback: These should be POSITIVE, not neutral:")

    # Test these with current systems
    vader_manager = VADERVariantManager()
    enhancer = ProprietarySentimentEnhancer()

    stock_vader = vader_manager.create_variant(VariantType.MINIMAL)
    enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)

    for i, text in enumerate(user_positive_examples, 1):
        print(f'\n{i}. "{text}"')

        # Test with systems
        stock_scores = stock_vader.polarity_scores(text)
        enhanced_scores = enhanced_vader.polarity_scores(text)

        try:
            from textblob import TextBlob

            textblob_score = TextBlob(text).sentiment.polarity
            proprietary_score, _ = enhancer.enhance_sentiment_score(enhanced_scores["compound"], textblob_score, text)
        except Exception:
            proprietary_score = enhanced_scores["compound"]

        stock_sentiment = classify_sentiment(stock_scores["compound"])
        enhanced_sentiment = classify_sentiment(enhanced_scores["compound"])
        proprietary_sentiment = classify_sentiment(proprietary_score)

        print(f"   Stock VADER: {stock_sentiment} ({stock_scores['compound']:.3f})")
        print(f"   Enhanced VADER: {enhanced_sentiment} ({enhanced_scores['compound']:.3f})")
        print(f"   Proprietary: {proprietary_sentiment} ({proprietary_score:.3f})")

        # Check if any system got it wrong
        if stock_sentiment != "positive":
            print(f"   ❌ Stock VADER missed this positive comment")
        if enhanced_sentiment != "positive":
            print(f"   ❌ Enhanced VADER missed this positive comment")
        if proprietary_sentiment != "positive":
            print(f"   ❌ Proprietary system missed this positive comment")


def main():
    """Run the real comment benchmark with user feedback."""

    print("🎯 REAL COMMENT BENCHMARK WITH USER FEEDBACK")
    print("=" * 80)
    print("Analyzing your actual database comments and incorporating your feedback")
    print("about which comments should be positive vs neutral.")
    print()

    # First, test the user's specific feedback
    incorporate_user_feedback()

    # Then analyze a larger sample
    print(f"\n🗄️  ANALYZING LARGER SAMPLE FROM YOUR DATABASE")
    print("=" * 60)

    comments = get_real_comments_sample(300)

    if not comments:
        print("❌ Could not get comments from database")
        return

    results = analyze_with_current_systems(comments)

    show_system_comparison(results)
    show_problematic_neutrals(results)

    print(f"\n🎯 NEXT STEPS:")
    print("1. Review the 'neutral' comments above - are they really neutral?")
    print("2. The systems need improvement to catch positive sentiment better")
    print("3. Your feedback shows the proprietary system needs tuning")
    print("4. We should train the ML classifier on your manual classifications")


if __name__ == "__main__":
    main()
