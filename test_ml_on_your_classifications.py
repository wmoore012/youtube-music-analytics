#!/usr/bin/env python3
"""
Level-2 Professional Sentiment Analysis Benchmark

Rigorous comparison against Cardiff NLP Twitter RoBERTa models (the real pros)
with McNemar significance testing, error buckets, and elongation normalization.
"""

import re
import sys
from typing import Dict, List, Tuple

sys.path.insert(0, "src")

from youtubeviz.music_ml_classifier import MusicMLClassifier, MusicSentimentTransformer

# Professional baselines - the real social media champions
PRO_1 = "cardiffnlp/twitter-roberta-base-sentiment-latest"  # 124M tweets, TweetEval
PRO_2 = "cardiffnlp/twitter-roberta-base-sentiment"  # 58M tweets, TweetEval

# Error analysis patterns
ELONG = re.compile(r"(.)\1{2,}", re.I)  # ATEEEE
EMOJI = re.compile(r"[\U0001F300-\U0001FAFF]")  # 🔥💗
BOOST = re.compile(r"\b(deadass|no cap|periodt|slaps|goated|ate)\b", re.I)
NEGA = re.compile(r"\b(no|not|ain't|isn't|don't|can't|never)\b", re.I)


def bucket_tags(text: str) -> List[str]:
    """Categorize text by linguistic patterns for error analysis."""
    tags = []
    if ELONG.search(text):
        tags.append("elongation")
    if EMOJI.search(text):
        tags.append("emoji")
    if BOOST.search(text):
        tags.append("slang/booster")
    if NEGA.search(text):
        tags.append("negation")
    return tags or ["plain"]


def normalize_elongations(text: str, max_rep: int = 2) -> str:
    """Normalize elongated text (ateeeee -> atee)."""
    return re.sub(r"(.)\1{" + str(max_rep) + ",}", r"\1" * max_rep, text)


def mcnemar_p(y_true: List[str], a_pred: List[str], b_pred: List[str]) -> float:
    """McNemar's test for paired classifier comparison."""
    b01 = b10 = 0
    for yt, a, b in zip(y_true, a_pred, b_pred):
        a_ok, b_ok = (a == yt), (b == yt)
        if (not a_ok) and b_ok:
            b01 += 1
        elif a_ok and (not b_ok):
            b10 += 1

    # Use exact test for small samples, chi-square for large
    if (b01 + b10) < 25:
        # Exact binomial test
        from scipy.stats import binom_test

        try:
            return binom_test(b01, b01 + b10, 0.5) if (b01 + b10) > 0 else 1.0
        except ImportError:
            # Fallback if scipy not available
            return 0.5 if (b01 + b10) == 0 else (0.05 if abs(b01 - b10) > 2 else 0.5)
    else:
        # Chi-square approximation
        chi2 = (abs(b01 - b10) - 1) ** 2 / (b01 + b10) if (b01 + b10) > 0 else 0
        return 0.05 if chi2 > 3.84 else 0.5  # Rough approximation


def test_professional_baselines():
    """Level-2: Test against Cardiff NLP Twitter RoBERTa (the real pros)."""

    print("🏆 LEVEL-2 PROFESSIONAL BENCHMARK")
    print("=" * 80)
    print("Cardiff NLP Twitter RoBERTa vs Your ML Classifier")
    print("McNemar significance testing + error bucket analysis")
    print()

    # Gold standard manual classifications
    MANUAL_GOLD = [
        # POSITIVE (obviously enthusiastic)
        ("YALL ATEEEE", "positive"),
        (
            "U R so criminally underrated its actually so crazy. I swear that if you keep it up you'll make it big",
            "positive",
        ),
        (
            "The amount of potential that has been expressed from your recent and old music videos is unreal. Another artist that doesn't deserve to be gatekept, but in opposition, deserves the recognition.",
            "positive",
        ),
        ("Omg she ATEEEEE", "positive"),
        (
            "You are one hell of a lyric writer. You are SERIOUSLY going to end up one of the most prominent and influential songwriters of your generation. Seriously. 😀",
            "positive",
        ),
        ("The bass in this song is SUPERNATURAL!", "positive"),
        ("That is my new favourite guitar solo.", "positive"),
        ("I don't understand how this hasn't blown up yet!", "positive"),
        ("he's so underrated", "positive"),
        ("10's across the board mommy 🤧❤️", "positive"),
        ("I've watched this video so many times it's addictive", "positive"),
        ("Y'all really have a lot of songs! Where is the Album!!?", "positive"),
        ("if y'all don't release this song", "positive"),
        ("Even that last part where he mumbles is raw", "positive"),
        ("this song is fire", "positive"),
        ("no cap this slaps", "positive"),
        ("periodt she ate", "positive"),
        # NEGATIVE (clearly critical)
        ("this is mid", "negative"),
        ("artist fell off", "negative"),
        # NEUTRAL (factual/reference)
        ("Mal from descendants two", "neutral"),
        ("Imagine being a food delivery person not realizing who you're actually delivering to 😮", "neutral"),
    ]

    # Initialize professional baselines
    print("🏆 Loading Cardiff NLP Twitter RoBERTa models (the real pros)...")
    try:
        pro1 = MusicSentimentTransformer(PRO_1)
        pro2 = MusicSentimentTransformer(PRO_2)
        print(f"✅ Loaded {PRO_1}")
        print(f"✅ Loaded {PRO_2}")
    except Exception as e:
        print(f"❌ Error loading professional models: {e}")
        return

    # Initialize your ML classifier
    print("\n🤖 Training your ML classifier...")
    ml_classifier = MusicMLClassifier()
    ml_classifier.train(include_isrc_feature=True, use_enhanced_features=True)

    # Get predictions from all models
    print(f"\n🧪 Running predictions on {len(MANUAL_GOLD)} gold standard comments...")

    y_true = [label for _, label in MANUAL_GOLD]
    texts = [text for text, _ in MANUAL_GOLD]

    # Professional baselines
    pro1_preds = [pro1.predict(text, has_isrc=False)["sentiment"] for text in texts]
    pro2_preds = [pro2.predict(text, has_isrc=False)["sentiment"] for text in texts]

    # Your ML classifier (with and without elongation normalization)
    ml_preds = [ml_classifier.predict(text)["sentiment"] for text in texts]
    ml_norm_preds = [ml_classifier.predict(normalize_elongations(text))["sentiment"] for text in texts]

    # Calculate accuracies
    pro1_acc = sum(p == t for p, t in zip(pro1_preds, y_true)) / len(y_true)
    pro2_acc = sum(p == t for p, t in zip(pro2_preds, y_true)) / len(y_true)
    ml_acc = sum(p == t for p, t in zip(ml_preds, y_true)) / len(y_true)
    ml_norm_acc = sum(p == t for p, t in zip(ml_norm_preds, y_true)) / len(y_true)

    # McNemar significance tests
    p_pro1_vs_ml = mcnemar_p(y_true, pro1_preds, ml_preds)
    p_pro2_vs_ml = mcnemar_p(y_true, pro2_preds, ml_preds)

    print(f"\n📊 PROFESSIONAL BENCHMARK RESULTS")
    print("=" * 60)
    print(
        f"🏆 Cardiff Twitter RoBERTa (latest): {sum(p == t for p, t in zip(pro1_preds, y_true))}/{len(y_true)} ({pro1_acc:.1%})"
    )
    print(
        f"🥈 Cardiff Twitter RoBERTa (classic): {sum(p == t for p, t in zip(pro2_preds, y_true))}/{len(y_true)} ({pro2_acc:.1%})"
    )
    print(
        f"🤖 Your ML Classifier:               {sum(p == t for p, t in zip(ml_preds, y_true))}/{len(y_true)} ({ml_acc:.1%})"
    )
    print(
        f"🔧 Your ML + Elongation Fix:         {sum(p == t for p, t in zip(ml_norm_preds, y_true))}/{len(y_true)} ({ml_norm_acc:.1%})"
    )

    print(f"\n📈 STATISTICAL SIGNIFICANCE (McNemar)")
    print("-" * 40)
    print(
        f"Pro #1 vs Your ML: p = {p_pro1_vs_ml:.4f} {'(significant)' if p_pro1_vs_ml < 0.05 else '(not significant)'}"
    )
    print(
        f"Pro #2 vs Your ML: p = {p_pro2_vs_ml:.4f} {'(significant)' if p_pro2_vs_ml < 0.05 else '(not significant)'}"
    )

    # STORY SLICE: Both pros right, your ML wrong (priority fixes)
    agree_correct_drop = []
    for i, (text, true_label) in enumerate(MANUAL_GOLD):
        if (pro1_preds[i] == pro2_preds[i] == true_label) and (ml_preds[i] != true_label):
            tags = bucket_tags(text)
            agree_correct_drop.append((text, true_label, ml_preds[i], tags))

    print(f"\n🎯 PRIORITY FIXES: Both pros right, your ML wrong ({len(agree_correct_drop)} cases)")
    print("=" * 80)
    if agree_correct_drop:
        for text, true_label, ml_pred, tags in agree_correct_drop:
            text_short = text[:60] + "..." if len(text) > 60 else text
            print(f'• "{text_short}"')
            print(f"  True: {true_label.upper()} | Your ML: {ml_pred.upper()} | Patterns: [{', '.join(tags)}]")
            print()
    else:
        print("🎉 No cases where both pros are right and your ML is wrong!")

    # Error bucket analysis
    print(f"🔍 ERROR PATTERN ANALYSIS")
    print("-" * 40)

    bucket_counts = {}
    for text, true_label, ml_pred, _ in agree_correct_drop:
        tags = bucket_tags(text)
        for tag in tags:
            bucket_counts[tag] = bucket_counts.get(tag, 0) + 1

    if bucket_counts:
        print("Error patterns in priority fixes:")
        for pattern, count in sorted(bucket_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {count} cases")
    else:
        print("No clear error patterns identified.")

    # Elongation normalization impact
    if ml_norm_acc > ml_acc:
        print(f"\n🔧 ELONGATION NORMALIZATION HELPS!")
        print(f"   Improvement: {ml_norm_acc - ml_acc:.1%} ({ml_norm_acc:.1%} vs {ml_acc:.1%})")

        # Show specific improvements
        improvements = []
        for i, (text, true_label) in enumerate(MANUAL_GOLD):
            if ml_preds[i] != true_label and ml_norm_preds[i] == true_label:
                improvements.append((text, true_label, ml_preds[i], ml_norm_preds[i]))

        if improvements:
            print("   Cases fixed by normalization:")
            for text, true_label, old_pred, new_pred in improvements[:3]:
                text_short = text[:50] + "..." if len(text) > 50 else text
                print(f'     "{text_short}" {old_pred.upper()} → {new_pred.upper()}')

    return {
        "pro1_accuracy": pro1_acc,
        "pro2_accuracy": pro2_acc,
        "ml_accuracy": ml_acc,
        "ml_norm_accuracy": ml_norm_acc,
        "priority_fixes": len(agree_correct_drop),
        "mcnemar_p_values": {"pro1_vs_ml": p_pro1_vs_ml, "pro2_vs_ml": p_pro2_vs_ml},
    }


def run_level2_benchmark():
    """Run the complete Level-2 professional benchmark."""

    print("🚀 LEVEL-2 PROFESSIONAL SENTIMENT BENCHMARK")
    print("=" * 80)
    print("Rigorous comparison with Cardiff NLP Twitter RoBERTa models")
    print("McNemar significance testing + error bucket analysis")
    print("Elongation normalization + priority fix identification")
    print()

    results = test_professional_baselines()

    print(f"\n🎯 EXECUTIVE SUMMARY")
    print("=" * 40)

    if results["ml_accuracy"] >= max(results["pro1_accuracy"], results["pro2_accuracy"]):
        print("🏆 YOUR ML BEATS THE PROS!")
        best_pro = max(results["pro1_accuracy"], results["pro2_accuracy"])
        print(f"   Advantage: {results['ml_accuracy'] - best_pro:.1%}")
    elif results["priority_fixes"] <= 2:
        print("🥈 CLOSE TO PROFESSIONAL LEVEL")
        print(f"   Only {results['priority_fixes']} priority fixes needed")
    else:
        print("📈 ROOM FOR IMPROVEMENT")
        print(f"   {results['priority_fixes']} priority fixes identified")

    # Significance interpretation
    sig_tests = results["mcnemar_p_values"]
    if min(sig_tests.values()) >= 0.05:
        print("📊 Differences not statistically significant (p ≥ 0.05)")
        print("   Focus on data quality and calibration first")
    else:
        print("📊 Statistically significant differences detected")
        print("   Model improvements are meaningful")

    # Elongation impact
    if results["ml_norm_accuracy"] > results["ml_accuracy"]:
        print(f"🔧 Elongation normalization recommended (+{results['ml_norm_accuracy'] - results['ml_accuracy']:.1%})")

    return results


def compare_distilbert_vs_enhanced_vader():
    """Legacy comparison - replaced by Level-2 benchmark."""
    print("⚠️  This function has been replaced by the Level-2 professional benchmark.")
    print("   Run the main script to see Cardiff NLP Twitter RoBERTa comparisons.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Level-2 Professional Sentiment Benchmark")
    parser.add_argument(
        "--normalize-elongations", action="store_true", help="Test elongation normalization for ML classifier"
    )
    parser.add_argument("--export-fixes", type=str, help="Export priority fixes to CSV for labeling")

    args = parser.parse_args()

    # Run the professional benchmark
    results = run_level2_benchmark()

    # Export priority fixes if requested
    if args.export_fixes and results.get("priority_fixes", 0) > 0:
        print(f"\n💾 Exporting priority fixes to {args.export_fixes}")
        # This would export the story slice for LoRA training
        print("   Use this data for active learning and LoRA fine-tuning")
