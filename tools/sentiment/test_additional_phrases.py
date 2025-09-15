#!/usr/bin/env python3
"""
Test Additional Music Slang Phrases

Test the additional phrases mentioned in the user request to ensure
comprehensive coverage of music industry sentiment.
"""

import os
import sys

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from src.youtubeviz.enhanced_music_sentiment import ComprehensiveMusicSentimentAnalyzer


def test_additional_user_phrases():
    """Test additional phrases mentioned by the user."""

    analyzer = ComprehensiveMusicSentimentAnalyzer()

    # Additional positive phrases from user request
    additional_positive = [
        # From user's examples
        "for the culture",
        "for the culture fr",
        "I stan",
        "I stan a queen",
        "goated song",
        "this on repeat all day",
        "been on repeat since it dropped",
        "car test passed",
        "summer anthem",
        "this saved my life",
        "no skips",
        "front to back, no skips",
        "SOTY",
        "AOTY",
        "went platinum in my car",
        "went platinum in my headphones",
        "went platinum in my room",
        "real music is back",
        "for the girls",
        "ate and left no crumbs",
        # Beat/production appreciation (should also trigger beat_appreciation)
        "this will go crazy in the club",
        "this just passed the car test",
        "on my gym playlist",
        "drop the album already",
        "we need the album now",
        "who mixed this?",
        "need the instrumental",
        "drop the visuals",
        "clean version pls",
        "what's the sample?",
        # Additional variations user mentioned
        "this hard af",
        "this hard as shit",
        "Bro this crazy",
        "the beat though!",
        "the beat tho!",
        "who made this beat bro?!",
        "you slid",
        "this fye my boi",
        "sheeeesh",
    ]

    # Additional negative phrases
    additional_negative = [
        "this ain't it chief",
        "mid",
        "this is basura",
        "this went double wood",
        "who approved this?",
        "turn it off",
        "nobody asked for this",
        "overrated",
        "fell off",
        "industry plant",
        "switch up the flow",
        "flow is repetitive",
        "mix sounds chaotic",
        "this song is so overrated bro",
        "flop — turn this off",
        "who asked for this remix",
        "album rollout ain't rollouting",
        "this ain't real hip-hop",
        "sounds the same every track",
        "skip",
    ]

    # Test positive phrases
    print("🎵 Testing Additional Positive Phrases:")
    print("=" * 70)

    positive_correct = 0
    beat_appreciation_detected = 0

    for phrase in additional_positive:
        result = analyzer.analyze_comment(phrase)
        score = result["sentiment_score"]
        confidence = result["confidence"]
        beat_love = result["beat_appreciation"]

        is_positive = score > 0.1
        if is_positive:
            status = "✅ POSITIVE"
            positive_correct += 1
        else:
            status = "❌ NOT POSITIVE"

        if beat_love:
            beat_appreciation_detected += 1
            beat_emoji = "🎵"
        else:
            beat_emoji = "⚪"

        print(f"{phrase:35} | {status} | {score:+.2f} | conf: {confidence:.2f} | {beat_emoji}")

    positive_accuracy = positive_correct / len(additional_positive) * 100

    print(
        f"\n📊 Additional Positive Phrases Accuracy: {positive_accuracy:.1f}% ({positive_correct}/{len(additional_positive)})"
    )
    print(f"🎵 Beat Appreciation Detected: {beat_appreciation_detected} phrases")

    # Test negative phrases
    print(f"\n❌ Testing Additional Negative Phrases:")
    print("=" * 70)

    negative_correct = 0

    for phrase in additional_negative:
        result = analyzer.analyze_comment(phrase)
        score = result["sentiment_score"]
        confidence = result["confidence"]

        is_negative = score < -0.1
        if is_negative:
            status = "✅ NEGATIVE"
            negative_correct += 1
        else:
            status = "❌ NOT NEGATIVE"

        print(f"{phrase:35} | {status} | {score:+.2f} | conf: {confidence:.2f}")

    negative_accuracy = negative_correct / len(additional_negative) * 100

    print(
        f"\n📊 Additional Negative Phrases Accuracy: {negative_accuracy:.1f}% ({negative_correct}/{len(additional_negative)})"
    )

    # Overall results
    total_correct = positive_correct + negative_correct
    total_phrases = len(additional_positive) + len(additional_negative)
    overall_accuracy = total_correct / total_phrases * 100

    print(f"\n🎯 Overall Additional Phrases Results:")
    print(f"   Total phrases tested: {total_phrases}")
    print(f"   Correctly classified: {total_correct}")
    print(f"   Overall accuracy: {overall_accuracy:.1f}%")

    if overall_accuracy >= 95:
        print("🎉 Excellent! Enhanced model handles additional phrases very well!")
    elif overall_accuracy >= 85:
        print("✅ Good performance on additional phrases!")
    else:
        print("🔧 Need to improve handling of additional phrases.")

    return {
        "positive_accuracy": positive_accuracy,
        "negative_accuracy": negative_accuracy,
        "overall_accuracy": overall_accuracy,
        "beat_appreciation_count": beat_appreciation_detected,
    }


def test_original_problem_cases():
    """Test the original problem cases that were failing."""

    analyzer = ComprehensiveMusicSentimentAnalyzer()

    original_cases = [
        "Hottie, Baddie, Maddie",
        "Part two pleaseee wtfff",
        "Cuz I willie 😖😚💕",
        "sheeeeesh my nigga snapped 🔥🔥🔥🔥",
        "my legs are spread!!",
        "Bestie goals fr 🤞",
        "🔥🔥🔥",
        "🌊🌊🌊🌊",
        "💯💯💯",
    ]

    print(f"\n🎯 Testing Original Problem Cases:")
    print("=" * 70)

    correct = 0

    for comment in original_cases:
        result = analyzer.analyze_comment(comment)
        score = result["sentiment_score"]
        confidence = result["confidence"]
        beat_love = result["beat_appreciation"]

        is_positive = score > 0.1
        if is_positive:
            status = "✅ POSITIVE"
            correct += 1
        else:
            status = "❌ NOT POSITIVE"

        beat_emoji = "🎵" if beat_love else "⚪"
        print(f"{comment:35} | {status} | {score:+.2f} | conf: {confidence:.2f} | {beat_emoji}")

    accuracy = correct / len(original_cases) * 100
    print(f"\n📊 Original Problem Cases Accuracy: {accuracy:.1f}% ({correct}/{len(original_cases)})")

    return accuracy


if __name__ == "__main__":
    print("🎵 Comprehensive Additional Phrases Test")
    print("=" * 70)

    # Test original problem cases
    original_accuracy = test_original_problem_cases()

    # Test additional phrases
    additional_results = test_additional_user_phrases()

    print(f"\n🏆 FINAL RESULTS:")
    print(f"   Original problem cases: {original_accuracy:.1f}%")
    print(f"   Additional positive phrases: {additional_results['positive_accuracy']:.1f}%")
    print(f"   Additional negative phrases: {additional_results['negative_accuracy']:.1f}%")
    print(f"   Overall additional phrases: {additional_results['overall_accuracy']:.1f}%")
    print(f"   Beat appreciation detected: {additional_results['beat_appreciation_count']} phrases")

    if original_accuracy == 100 and additional_results["overall_accuracy"] >= 95:
        print("\n🎉 SUCCESS! Enhanced model ready for production deployment!")
    else:
        print("\n🔧 Some phrases need improvement before production deployment.")
