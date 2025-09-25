#!/usr/bin/env python3
"""
Test Transformer Models on Problematic "Neutral" Comments

Test the new transformer models on comments that were incorrectly labeled as neutral
to see if they can correctly identify them as positive.
"""

import sys

sys.path.insert(0, "src")

from youtubeviz.music_ml_classifier import MusicSentimentTransformer, create_transformer_models


def test_transformer_on_neutral_comments():
    """Test transformer models on the problematic neutral comments."""

    print("🎯 TESTING TRANSFORMERS ON PROBLEMATIC 'NEUTRAL' COMMENTS")
    print("=" * 70)
    print("These comments were labeled as 'neutral' but are clearly positive!")
    print()

    # The problematic "neutral" comments from the benchmark results
    problematic_comments = [
        "JUNT ON REPEAT MY BOY 🔥🔥🔥🔥",
        "can't wait for the full song.  this is the biggest bop right now! <3",
        "Bumpin this in da charger 🔥",
        "I feel like a collab with Die Antwoord would suit you, just a thought to give yo...",
        "THIS IS PHENOMENAL!!! Just a whole Masterpiece!!",
        "You are my favorite new artist and I'm here for ALL of it! 💗",
        "Get off that contract with WMG Atlantic. They aren't pushing you like they shoul...",
        "I have to listen to this song at least 5 times a day",
        "The outfits!!!",
    ]

    # Test with different transformer models
    try:
        # Create transformer models
        print("🤖 Initializing transformer models...")
        transformers = create_transformer_models()

        if not transformers:
            print("❌ No transformer models available. Installing transformers...")
            return

        print(f"✅ Loaded {len(transformers)} transformer models")
        print()

        # Test each comment
        for i, comment in enumerate(problematic_comments, 1):
            print(f'💬 {i}. "{comment}"')
            print("   Expected: POSITIVE (clearly enthusiastic/positive)")
            print("   Transformer Results:")

            for model_name, transformer in transformers.items():
                try:
                    result = transformer.predict(comment, has_isrc=False)
                    sentiment = result["sentiment"].upper()
                    confidence = result["sentiment_confidence"]

                    # Color code the results
                    if sentiment == "POSITIVE":
                        status = "✅ CORRECT"
                    elif sentiment == "NEGATIVE":
                        status = "❌ WRONG (negative)"
                    else:
                        status = "⚠️  WRONG (neutral)"

                    print(f"     {model_name:25s}: {sentiment:8s} ({confidence:.3f}) {status}")

                except Exception as e:
                    print(f"     {model_name:25s}: ERROR - {e}")

            print()

        # Summary statistics
        print("📊 SUMMARY ANALYSIS")
        print("-" * 40)

        total_tests = len(problematic_comments) * len(transformers)
        correct_predictions = 0

        for comment in problematic_comments:
            for model_name, transformer in transformers.items():
                try:
                    result = transformer.predict(comment, has_isrc=False)
                    if result["sentiment"] == "positive":
                        correct_predictions += 1
                except:
                    pass

        accuracy = correct_predictions / total_tests if total_tests > 0 else 0
        print(f"Transformer Accuracy on 'Neutral' Comments: {correct_predictions}/{total_tests} ({accuracy:.1%})")

        if accuracy > 0.7:
            print("🎉 EXCELLENT! Transformers are correctly identifying positive comments!")
        elif accuracy > 0.5:
            print("👍 GOOD! Transformers are better than the baseline at identifying positive comments!")
        else:
            print("😞 Still struggling. Need to enhance the music domain logic further.")

    except Exception as e:
        print(f"❌ Error testing transformers: {e}")
        import traceback

        traceback.print_exc()


def test_specific_music_terms():
    """Test transformer models on specific music slang terms."""

    print("\n🎵 TESTING ON SPECIFIC MUSIC SLANG")
    print("=" * 50)

    music_slang_tests = [
        ("This song slaps!", "positive"),
        ("This is fire 🔥", "positive"),
        ("Artist is goated fr", "positive"),
        ("This beat goes hard", "positive"),
        ("PERIODT! This ate!", "positive"),
        ("This is mid tbh", "negative"),
        ("Song is trash", "negative"),
        ("Okay song I guess", "neutral"),
        ("Reminds me of someone", "neutral"),
    ]

    try:
        # Test with Twitter RoBERTa (best for social media)
        transformer = MusicSentimentTransformer("cardiffnlp/twitter-roberta-base-sentiment-latest")

        print("Testing with Twitter RoBERTa (best for social media slang):")
        print()

        correct = 0
        total = len(music_slang_tests)

        for text, expected in music_slang_tests:
            result = transformer.predict(text, has_isrc=False)
            predicted = result["sentiment"]
            confidence = result["sentiment_confidence"]

            if predicted == expected:
                status = "✅ CORRECT"
                correct += 1
            else:
                status = "❌ WRONG"

            print(
                f'   "{text:20s}" → {predicted.upper():8s} ({confidence:.3f}) | Expected: {expected.upper():8s} {status}'
            )

        accuracy = correct / total
        print(f"\nMusic Slang Accuracy: {correct}/{total} ({accuracy:.1%})")

        if accuracy >= 0.8:
            print("🎉 EXCELLENT music slang understanding!")
        elif accuracy >= 0.6:
            print("👍 GOOD music slang understanding!")
        else:
            print("😞 Need to improve music slang detection.")

    except Exception as e:
        print(f"❌ Error testing music slang: {e}")


if __name__ == "__main__":
    test_transformer_on_neutral_comments()
    test_specific_music_terms()
