#!/usr/bin/env python3
"""
Sentiment Model Comparison Test
Test different sentiment models against music slang and Gen Z language
"""

import os
from typing import Dict, List, Tuple

import pandas as pd


def get_comprehensive_music_slang_test_cases() -> List[Tuple[str, str, str]]:
    """
    Get comprehensive test cases for music slang and Gen Z language.

    Returns:
        List of (comment, expected_sentiment, category) tuples
    """

    test_cases = [
        # Original examples that were failing
        ("Hottie, Baddie, Maddie", "positive", "compliment"),
        ("Part two pleaseee wtfff", "positive", "excitement"),
        ("Cuz I willie 😖😚💕", "positive", "emoji_positive"),
        ("sheeeeesh my nigga snapped 🔥🔥🔥🔥", "positive", "hype"),
        ("my legs are spread!!", "positive", "excitement"),
        ("Bestie goals fr 🤞", "positive", "friendship"),
        # Fire and wave emojis
        ("🔥🔥🔥", "positive", "emoji_fire"),
        ("🌊🌊🌊🌊", "positive", "emoji_wave"),
        ("💯💯💯", "positive", "emoji_hundred"),
        ("😍", "positive", "emoji_love"),
        ("😍😍😍", "positive", "emoji_love"),
        # "This is sick" variations
        ("this is sick", "positive", "sick_positive"),
        ("this sick", "positive", "sick_positive"),
        ("so sick", "positive", "sick_positive"),
        ("that's sick", "positive", "sick_positive"),
        ("sick beat", "positive", "sick_positive"),
        ("sick flow", "positive", "sick_positive"),
        # "Fucking queen" variations
        ("fucking queen!", "positive", "queen_praise"),
        ("queen!", "positive", "queen_praise"),
        ("yes queen", "positive", "queen_praise"),
        ("queen energy", "positive", "queen_praise"),
        ("hot bish", "positive", "queen_praise"),
        ("bad bish", "positive", "queen_praise"),
        ("YES MOTHER!", "positive", "queen_praise"),
        # "Go off" variations
        ("go off king", "positive", "go_off"),
        ("go off queen", "positive", "go_off"),
        ("go off", "positive", "go_off"),
        ("go off bestie", "positive", "go_off"),
        # Excitement expressions
        ("oh my!", "positive", "excitement"),
        ("oh my yes!", "positive", "excitement"),
        ("yessir!", "positive", "excitement"),
        ("yessuh", "positive", "excitement"),
        ("fuck it up", "positive", "excitement"),
        ("Bitchhh!", "positive", "excitement"),
        ("Bitch, it's givinnnng!", "positive", "excitement"),
        # Rating expressions
        ("10/10", "positive", "rating"),
        ("100!", "positive", "rating"),
        ("100/10", "positive", "rating"),
        ("11/10", "positive", "rating"),
        # Need/want expressions
        ("I need the lyrics", "positive", "need_want"),
        ("friday can't come sooner", "positive", "need_want"),
        ("please come to atlanta", "positive", "need_want"),
        ("please come to chicago", "positive", "need_want"),
        ("come to my city", "positive", "need_want"),
        # Hard/crazy variations
        ("this hard af", "positive", "hard_crazy"),
        ("this hard as shit", "positive", "hard_crazy"),
        ("this hard", "positive", "hard_crazy"),
        ("goes hard", "positive", "hard_crazy"),
        ("Bro this crazy", "positive", "hard_crazy"),
        ("this crazy", "positive", "hard_crazy"),
        ("absolutely crazy", "positive", "hard_crazy"),
        # Beat appreciation
        ("the beat though!", "positive", "beat_love"),
        ("the beat tho!", "positive", "beat_love"),
        ("who made this beat bro?!", "positive", "beat_love"),
        ("beat goes hard", "positive", "beat_love"),
        ("this beat is fire", "positive", "beat_love"),
        ("beat slaps", "positive", "beat_love"),
        # New Gen Z slang additions
        ("you slid", "positive", "gen_z_praise"),
        ("this fye my boi", "positive", "gen_z_praise"),
        ("sheeeesh", "positive", "gen_z_hype"),
        ("sheeesh", "positive", "gen_z_hype"),
        ("no cap", "positive", "gen_z_truth"),
        ("periodt", "positive", "gen_z_emphasis"),
        ("period", "positive", "gen_z_emphasis"),
        ("slay", "positive", "gen_z_praise"),
        ("slayed", "positive", "gen_z_praise"),
        ("slaying", "positive", "gen_z_praise"),
        ("ate that", "positive", "gen_z_praise"),
        ("ate and left no crumbs", "positive", "gen_z_praise"),
        ("devoured", "positive", "gen_z_praise"),
        ("served", "positive", "gen_z_praise"),
        ("understood the assignment", "positive", "gen_z_praise"),
        ("main character energy", "positive", "gen_z_praise"),
        ("it's giving", "positive", "gen_z_vibe"),
        ("it's giving main character", "positive", "gen_z_vibe"),
        ("vibes", "positive", "gen_z_vibe"),
        ("good vibes", "positive", "gen_z_vibe"),
        ("immaculate vibes", "positive", "gen_z_vibe"),
        ("chef's kiss", "positive", "gen_z_praise"),
        ("hits different", "positive", "gen_z_praise"),
        ("different breed", "positive", "gen_z_praise"),
        ("built different", "positive", "gen_z_praise"),
        ("that's it that's the tweet", "positive", "gen_z_emphasis"),
        ("we stan", "positive", "gen_z_support"),
        ("stan forever", "positive", "gen_z_support"),
        ("iconic", "positive", "gen_z_praise"),
        ("legendary", "positive", "gen_z_praise"),
        ("rent free", "positive", "gen_z_impact"),
        ("living in my head rent free", "positive", "gen_z_impact"),
        ("obsessed", "positive", "gen_z_love"),
        ("lowkey obsessed", "positive", "gen_z_love"),
        ("highkey obsessed", "positive", "gen_z_love"),
        ("not me crying", "positive", "gen_z_emotional"),
        ("I'm deceased", "positive", "gen_z_funny"),
        ("I'm dead", "positive", "gen_z_funny"),
        ("sent me", "positive", "gen_z_funny"),
        ("I can't even", "positive", "gen_z_overwhelmed"),
        ("I have no words", "positive", "gen_z_overwhelmed"),
        ("speechless", "positive", "gen_z_overwhelmed"),
        ("talent", "positive", "gen_z_praise"),
        ("pure talent", "positive", "gen_z_praise"),
        ("the talent jumped out", "positive", "gen_z_praise"),
        # Neutral examples (for balance)
        ("okay", "neutral", "neutral"),
        ("alright", "neutral", "neutral"),
        ("fine", "neutral", "neutral"),
        ("whatever", "neutral", "neutral"),
        # Negative examples (for balance)
        ("this sucks", "negative", "negative"),
        ("terrible", "negative", "negative"),
        ("hate this", "negative", "negative"),
        ("worst song ever", "negative", "negative"),
    ]

    return test_cases


def test_vader_model() -> Dict:
    """Test VADER sentiment model on music slang."""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()

        test_cases = get_comprehensive_music_slang_test_cases()
        results = []

        for comment, expected, category in test_cases:
            scores = vader.polarity_scores(comment)
            compound = scores["compound"]

            # VADER classification
            if compound >= 0.05:
                predicted = "positive"
            elif compound <= -0.05:
                predicted = "negative"
            else:
                predicted = "neutral"

            correct = predicted == expected

            results.append(
                {
                    "comment": comment,
                    "expected": expected,
                    "predicted": predicted,
                    "compound_score": compound,
                    "correct": correct,
                    "category": category,
                }
            )

        return {"model": "VADER", "results": results, "available": True}

    except ImportError:
        return {"model": "VADER", "results": [], "available": False, "error": "VADER not installed"}


def test_textblob_model() -> Dict:
    """Test TextBlob sentiment model on music slang."""
    try:
        from textblob import TextBlob

        test_cases = get_comprehensive_music_slang_test_cases()
        results = []

        for comment, expected, category in test_cases:
            blob = TextBlob(comment)
            polarity = blob.sentiment.polarity

            # TextBlob classification
            if polarity > 0.1:
                predicted = "positive"
            elif polarity < -0.1:
                predicted = "negative"
            else:
                predicted = "neutral"

            correct = predicted == expected

            results.append(
                {
                    "comment": comment,
                    "expected": expected,
                    "predicted": predicted,
                    "polarity_score": polarity,
                    "correct": correct,
                    "category": category,
                }
            )

        return {"model": "TextBlob", "results": results, "available": True}

    except ImportError:
        return {"model": "TextBlob", "results": [], "available": False, "error": "TextBlob not installed"}


def test_music_industry_model() -> Dict:
    """Test our enhanced Music Industry sentiment model."""
    try:
        from src.youtubeviz.music_sentiment import MusicIndustrySentimentAnalyzer

        analyzer = MusicIndustrySentimentAnalyzer()
        test_cases = get_comprehensive_music_slang_test_cases()
        results = []

        for comment, expected, category in test_cases:
            result = analyzer.analyze_comment(comment)
            score = result["sentiment_score"]

            # Our model classification
            if score > 0.1:
                predicted = "positive"
            elif score < -0.1:
                predicted = "negative"
            else:
                predicted = "neutral"

            correct = predicted == expected

            results.append(
                {
                    "comment": comment,
                    "expected": expected,
                    "predicted": predicted,
                    "sentiment_score": score,
                    "confidence": result["confidence"],
                    "beat_appreciation": result["beat_appreciation"],
                    "correct": correct,
                    "category": category,
                }
            )

        return {"model": "MusicIndustry", "results": results, "available": True}

    except ImportError as e:
        return {
            "model": "MusicIndustry",
            "results": [],
            "available": False,
            "error": f"Music Industry model not available: {e}",
        }


def run_model_comparison():
    """Run comprehensive model comparison and save results."""

    print("🎵 Running Comprehensive Sentiment Model Comparison")
    print("=" * 70)

    # Test all models
    models = [test_vader_model(), test_textblob_model(), test_music_industry_model()]

    # Calculate accuracies
    for model_result in models:
        if model_result["available"]:
            results = model_result["results"]
            total = len(results)
            correct = sum(1 for r in results if r["correct"])
            accuracy = correct / total * 100 if total > 0 else 0

            print(f"\n📊 {model_result['model']} Model:")
            print(f"   Total tests: {total}")
            print(f"   Correct: {correct}")
            print(f"   Accuracy: {accuracy:.1f}%")

            # Category breakdown
            categories = {}
            for result in results:
                cat = result["category"]
                if cat not in categories:
                    categories[cat] = {"total": 0, "correct": 0}
                categories[cat]["total"] += 1
                if result["correct"]:
                    categories[cat]["correct"] += 1

            print(f"   Category breakdown:")
            for cat, stats in categories.items():
                cat_accuracy = stats["correct"] / stats["total"] * 100
                print(f"     {cat}: {cat_accuracy:.1f}% ({stats['correct']}/{stats['total']})")

        else:
            print(f"\n❌ {model_result['model']} Model: {model_result.get('error', 'Not available')}")

    # Save detailed results
    all_results = []
    for model_result in models:
        if model_result["available"]:
            for result in model_result["results"]:
                result["model"] = model_result["model"]
                all_results.append(result)

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv("sentiment_model_comparison_results.csv", index=False)
        print(f"\n💾 Detailed results saved to: sentiment_model_comparison_results.csv")

    # Recommend best model
    best_model = None
    best_accuracy = 0

    for model_result in models:
        if model_result["available"]:
            results = model_result["results"]
            if results:
                accuracy = sum(1 for r in results if r["correct"]) / len(results) * 100
                if accuracy > best_accuracy:
                    best_accuracy = accuracy
                    best_model = model_result["model"]

    if best_model:
        print(f"\n🏆 RECOMMENDED MODEL: {best_model} ({best_accuracy:.1f}% accuracy)")

        if best_model == "MusicIndustry":
            print("✅ Our enhanced Music Industry model performs best!")
            print("   This justifies our custom implementation for music slang.")
        else:
            print(f"⚠️  {best_model} performs better than our custom model.")
            print("   Consider enhancing our Music Industry model further.")

    return models


if __name__ == "__main__":
    run_model_comparison()
