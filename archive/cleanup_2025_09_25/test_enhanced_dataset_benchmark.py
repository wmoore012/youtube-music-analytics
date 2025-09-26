#!/usr/bin/env python3
"""
Test Enhanced Dataset Against Baseline Models

This tests our enhanced dataset entries against both VADER and
the current advanced model to see if our improvements work.
"""

from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset


def test_enhanced_dataset_with_models():
    """Test enhanced dataset entries against available models."""

    print("🧪 TESTING ENHANCED DATASET AGAINST MODELS")
    print("=" * 60)

    # Load enhanced dataset
    dataset = get_enhanced_music_dataset()
    print(f"📊 Loaded {len(dataset.entries)} enhanced entries")

    # Test with VADER
    print("\n🔍 Testing with Stock VADER...")
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()

        vader_correct = 0
        vader_total = 0

        print("\nVADER Results:")
        print("-" * 40)

        for entry in dataset.entries:
            scores = vader.polarity_scores(entry.phrase)
            compound = scores["compound"]

            # Classify
            if compound >= 0.05:
                predicted = "positive"
            elif compound <= -0.05:
                predicted = "negative"
            else:
                predicted = "neutral"

            expected = entry.sentiment.value
            correct = predicted == expected

            if correct:
                vader_correct += 1
            vader_total += 1

            status = "✅" if correct else "❌"
            print(f"{status} {entry.phrase[:30]:30} | Expected: {expected:8} | Got: {predicted:8} | {compound:+.3f}")

        vader_accuracy = vader_correct / vader_total if vader_total > 0 else 0
        print(f"\n📊 VADER Accuracy: {vader_correct}/{vader_total} ({vader_accuracy:.1%})")

    except ImportError:
        print("❌ VADER not available")
        vader_accuracy = 0

    # Test with Advanced Model
    print("\n🔍 Testing with Current Advanced Model...")
    try:
        from src.youtubeviz.advanced_music_sentiment import AdvancedMusicSentimentAnalyzer

        analyzer = AdvancedMusicSentimentAnalyzer()

        advanced_correct = 0
        advanced_total = 0

        print("\nAdvanced Model Results:")
        print("-" * 40)

        for entry in dataset.entries:
            analysis = analyzer.analyze_comment(entry.phrase)
            predicted = analysis.sentiment.value
            expected = entry.sentiment.value
            correct = predicted == expected

            if correct:
                advanced_correct += 1
            advanced_total += 1

            status = "✅" if correct else "❌"
            print(
                f"{status} {entry.phrase[:30]:30} | Expected: {expected:8} | Got: {predicted:8} | Conf: {analysis.confidence:.2f}"
            )

        advanced_accuracy = advanced_correct / advanced_total if advanced_total > 0 else 0
        print(f"\n📊 Advanced Model Accuracy: {advanced_correct}/{advanced_total} ({advanced_accuracy:.1%})")

    except ImportError:
        print("❌ Advanced model not available")
        advanced_accuracy = 0

    # Summary
    print("\n🎯 ENHANCED DATASET PERFORMANCE SUMMARY")
    print("=" * 60)
    if vader_accuracy > 0:
        print(f"Stock VADER:      {vader_accuracy:.1%}")
    if advanced_accuracy > 0:
        print(f"Current Advanced: {advanced_accuracy:.1%}")

    if vader_accuracy > 0 and advanced_accuracy > 0:
        improvement = advanced_accuracy - vader_accuracy
        print(f"Improvement:      {improvement:+.1%}")

    # Identify problem areas
    print("\n🔧 AREAS NEEDING VADER ENHANCEMENT:")
    print("-" * 40)

    if vader_accuracy > 0:
        try:
            problem_phrases = []
            for entry in dataset.entries:
                scores = vader.polarity_scores(entry.phrase)
                compound = scores["compound"]

                if compound >= 0.05:
                    predicted = "positive"
                elif compound <= -0.05:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                if predicted != entry.sentiment.value:
                    problem_phrases.append(
                        {
                            "phrase": entry.phrase,
                            "expected": entry.sentiment.value,
                            "predicted": predicted,
                            "category": entry.category.value,
                            "compound": compound,
                        }
                    )

            # Group by category
            categories = {}
            for p in problem_phrases:
                cat = p["category"]
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(p)

            for category, phrases in categories.items():
                print(f"\n{category}:")
                for p in phrases[:3]:  # Show top 3 per category
                    print(f"  • '{p['phrase']}' → {p['predicted']} (should be {p['expected']})")

        except Exception:
            pass

    print(f"\n✅ Enhanced dataset testing complete!")
    print(f"🎯 Target: Improve VADER from {vader_accuracy:.1%} to 85%+ with enhancements")


if __name__ == "__main__":
    test_enhanced_dataset_with_models()
