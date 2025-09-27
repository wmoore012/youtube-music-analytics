#!/usr / bin / env python3
"""
Enhanced Dataset v2.2 Additions

Adds the most impactful improvements from the v2.2 spec:
- Regional slang (NYC, LA, Baltimore)
- Cultural expressions (Queer vernacular)
- Industry terms
- Better balance

This extends the existing dataset without breaking compatibility.
"""

import sys

sys.path.insert(0, "src")


def get_v22_additions():
    """Get the v2.2 additions to add to existing dataset."""

    try:
        from datasets.enhanced_sentiment_dataset import (
            Aspect,
            EnhancedMusicSlangEntry,
            Intent,
            SentimentLabel,
            SlangCategory,
            Toxicity,
        )
    except ImportError:
        print("❌ Cannot import dataset classes")
        return []

    additions = []

    # ===== REGIONAL SLANG ADDITIONS =====

    # NYC / AAVE Pack
    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "yerrr this fire",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.HYPE_EXCITEMENT,
                Aspect.GENERAL,
                0.85,
                "NYC greeting + praise combo",
                gen_z_slang=True,
            ),
            EnhancedMusicSlangEntry(
                "deadass slaps",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.HYPE_EXCITEMENT,
                Aspect.GENERAL,
                0.90,
                "NYC intensifier + praise - 'seriously slaps'",
                gen_z_slang=True,
            ),
            EnhancedMusicSlangEntry(
                "deadass mid",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.GENERAL,
                0.85,
                "NYC intensifier + critique - 'seriously mediocre'",
                gen_z_slang=True,
            ),
            EnhancedMusicSlangEntry(
                "word?",
                SentimentLabel.NEUTRAL,
                Intent.INFO,
                SlangCategory.NEUTRAL_QUESTIONS,
                Aspect.GENERAL,
                0.75,
                "NYC confirmation question - neutral unless boosted",
            ),
        ]
    )

    # LA / SoCal Pack
    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "cuh this hard",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.PRAISE_GENERAL,
                Aspect.GENERAL,
                0.85,
                "LA address term + praise - 'cousin this is hard'",
            ),
            EnhancedMusicSlangEntry(
                "fye as hell",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.PRAISE_GENERAL,
                Aspect.GENERAL,
                0.90,
                "LA variant of 'fire' - excellent",
            ),
            EnhancedMusicSlangEntry(
                "the function going up",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.HYPE_EXCITEMENT,
                Aspect.GENERAL,
                0.80,
                "LA party / event hype",
            ),
        ]
    )

    # Baltimore Pack
    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "dummy fire",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.HYPE_EXCITEMENT,
                Aspect.GENERAL,
                0.85,
                "Baltimore intensifier + praise - 'really fire'",
            ),
            EnhancedMusicSlangEntry(
                "lor bro went off",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.PRAISE_PERFORMANCE,
                Aspect.ARTIST,
                0.80,
                "Baltimore 'little brother' + performance praise",
            ),
            EnhancedMusicSlangEntry(
                "ard bet",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.HYPE_EXCITEMENT,
                Aspect.GENERAL,
                0.75,
                "Baltimore agreement - 'alright bet' - mild positive",
            ),
        ]
    )

    # ===== CULTURAL EXPRESSIONS =====

    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "she ate and left no crumbs",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.CULTURAL_IDENTITY,
                Aspect.ARTIST,
                0.95,
                "Queer vernacular - flawless performance",
                gen_z_slang=True,
            ),
            EnhancedMusicSlangEntry(
                "it's giving main character",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.PRAISE_GENERAL,
                Aspect.GENERAL,
                0.88,
                "Gen Z template - giving off main character energy",
                gen_z_slang=True,
            ),
            EnhancedMusicSlangEntry(
                "serving vocals",
                SentimentLabel.POSITIVE,
                Intent.PRAISE,
                SlangCategory.PRAISE_PERFORMANCE,
                Aspect.VOCALS,
                0.90,
                "Queer vernacular - delivering excellent vocals",
                gen_z_slang=True,
            ),
        ]
    )

    # ===== INDUSTRY / META TERMS =====

    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "industry plant vibes",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.MARKETING,
                0.85,
                "Criticism of manufactured success",
            ),
            EnhancedMusicSlangEntry(
                "payola energy",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.MARKETING,
                0.80,
                "Criticism of paid promotion",
            ),
            EnhancedMusicSlangEntry(
                "ratioed",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.GENERAL,
                0.85,
                "Social media criticism - more negative responses than positive",
            ),
            EnhancedMusicSlangEntry(
                "glazing",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.GENERAL,
                0.84,
                "Calling out excessive praise - 2024 Gen Z term",
                gen_z_slang=True,
            ),
        ]
    )

    # ===== ADDITIONAL NEGATIVES FOR BALANCE =====

    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "not it",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.GENERAL,
                0.80,
                "Gen Z rejection - 'this is not it'",
                gen_z_slang=True,
            ),
            EnhancedMusicSlangEntry(
                "pack it up",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.GENERAL,
                0.85,
                "Stop / quit - negative dismissal",
            ),
            EnhancedMusicSlangEntry(
                "trying too hard",
                SentimentLabel.NEGATIVE,
                Intent.CRITIQUE,
                SlangCategory.CRITICISM_NEGATIVE,
                Aspect.GENERAL,
                0.80,
                "Criticism of forced effort",
            ),
        ]
    )

    # ===== ADDITIONAL NEUTRALS FOR BALANCE =====

    additions.extend(
        [
            EnhancedMusicSlangEntry(
                "what genre is this",
                SentimentLabel.NEUTRAL,
                Intent.INFO,
                SlangCategory.NEUTRAL_QUESTIONS,
                Aspect.GENERAL,
                0.85,
                "Genre classification question",
            ),
            EnhancedMusicSlangEntry(
                "tour dates when",
                SentimentLabel.NEUTRAL,
                Intent.INFO,
                SlangCategory.NEUTRAL_QUESTIONS,
                Aspect.ARTIST,
                0.80,
                "Tour information request",
            ),
            EnhancedMusicSlangEntry(
                "drop the sample credit",
                SentimentLabel.NEUTRAL,
                Intent.REQUEST,
                SlangCategory.NEUTRAL_REQUESTS,
                Aspect.BEAT,
                0.85,
                "Production credit request",
                beat_appreciation=True,
            ),
        ]
    )

    return additions


def test_v22_additions():
    """Test the v2.2 additions for quality and balance."""
    print("🧪 Testing v2.2 Additions...")

    additions = get_v22_additions()

    if not additions:
        print("❌ No additions loaded")
        return False

    # Analyze the additions
    sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
    regional_counts = {}
    cultural_counts = 0

    for entry in additions:
        sentiment_counts[entry.sentiment.value] += 1

        # Count regional terms (simple heuristic)
        phrase_lower = entry.phrase.lower()
        if any(term in phrase_lower for term in ["yerrr", "deadass", "word"]):
            regional_counts["NYC"] = regional_counts.get("NYC", 0) + 1
        elif any(term in phrase_lower for term in ["cuh", "fye", "function"]):
            regional_counts["LA"] = regional_counts.get("LA", 0) + 1
        elif any(term in phrase_lower for term in ["dummy", "lor", "ard"]):
            regional_counts["Baltimore"] = regional_counts.get("Baltimore", 0) + 1

        # Count cultural expressions
        if any(term in phrase_lower for term in ["ate", "giving", "serving"]):
            cultural_counts += 1

    print(f"📊 v2.2 Additions Analysis:")
    print(f"   Total additions: {len(additions)}")
    print(f"   Sentiment distribution: {sentiment_counts}")
    print(f"   Regional coverage: {regional_counts}")
    print(f"   Cultural expressions: {cultural_counts}")

    # Check balance of additions
    total_additions = len(additions)
    pos_percent = sentiment_counts["positive"] / total_additions * 100
    neg_percent = sentiment_counts["negative"] / total_additions * 100
    neu_percent = sentiment_counts["neutral"] / total_additions * 100

    print(f"\n📈 Addition Balance:")
    print(f"   Positive: {sentiment_counts['positive']} ({pos_percent:.1f}%)")
    print(f"   Negative: {sentiment_counts['negative']} ({neg_percent:.1f}%)")
    print(f"   Neutral: {sentiment_counts['neutral']} ({neu_percent:.1f}%)")

    # Check if additions help balance
    balance_score = 1.0 - (max(abs(pos_percent - 33.3), abs(neg_percent - 33.3), abs(neu_percent - 33.3)) / 66.7)
    print(f"   Addition balance score: {balance_score:.3f}")

    if balance_score > 0.8:
        print("✅ Additions are well balanced!")
    else:
        print("⚠️  Additions could be more balanced")

    return True


def simulate_combined_dataset():
    """Simulate what the combined current + v2.2 dataset would look like."""
    print("\n🔮 Simulating Combined Dataset Performance...")

    try:
        from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset

        # Get current dataset
        current_dataset = get_enhanced_music_dataset()
        current_labels = [entry.sentiment.value for entry in current_dataset.entries]

        # Get v2.2 additions
        additions = get_v22_additions()
        addition_labels = [entry.sentiment.value for entry in additions]

        # Combine
        combined_labels = current_labels + addition_labels

        # Assess combined quality
        from youtubeviz.model_benchmark_system import ModelBenchmarkSystem

        benchmark_system = ModelBenchmarkSystem()

        current_quality = benchmark_system.assess_dataset_quality(current_labels)
        combined_quality = benchmark_system.assess_dataset_quality(combined_labels)

        print(f"📊 Dataset Comparison:")
        print(f"   CURRENT:")
        print(f"     Total: {current_quality.total_samples}")
        print(f"     Quality: {current_quality.quality_level}")
        print(f"     Balance: {current_quality.balance_score:.3f}")
        print(f"   COMBINED (Current + v2.2):")
        print(f"     Total: {combined_quality.total_samples}")
        print(f"     Quality: {combined_quality.quality_level}")
        print(f"     Balance: {combined_quality.balance_score:.3f}")

        # Calculate improvement
        size_improvement = combined_quality.total_samples - current_quality.total_samples
        balance_improvement = combined_quality.balance_score - current_quality.balance_score

        print(f"\n📈 Improvements:")
        print(
            f"   Size: +{size_improvement} samples "
            f"({size_improvement / current_quality.total_samples * 100:.1f}% increase)"
        )
        print(f"   Balance: {balance_improvement:+.3f} (higher is better)")

        # Quality level progression
        quality_levels = ["poor", "acceptable", "good", "excellent"]
        current_idx = quality_levels.index(current_quality.quality_level)
        combined_idx = quality_levels.index(combined_quality.quality_level)

        if combined_idx > current_idx:
            print(f"   Quality Level: IMPROVED ({current_quality.quality_level} → {combined_quality.quality_level})")
        elif combined_idx == current_idx:
            print(f"   Quality Level: SAME ({combined_quality.quality_level})")
        else:
            print(f"   Quality Level: WORSE ({current_quality.quality_level} → {combined_quality.quality_level})")

        return {
            "current_quality": current_quality,
            "combined_quality": combined_quality,
            "improvement": {
                "size": size_improvement,
                "balance": balance_improvement,
                "quality_improved": combined_idx > current_idx,
            },
        }

    except Exception as e:
        print(f"❌ Combined dataset simulation failed: {e}")
        return None


if __name__ == "__main__":
    print("🚀 TESTING v2.2 DATASET ADDITIONS")
    print("=" * 50)

    # Test the additions
    test_result = test_v22_additions()

    # Simulate combined performance
    simulation_result = simulate_combined_dataset()

    if simulation_result and simulation_result["improvement"]["quality_improved"]:
        print(f"\n🎉 RECOMMENDATION: IMPLEMENT v2.2 ADDITIONS")
        print("   ✅ Improves dataset quality")
        print("   ✅ Adds regional coverage")
        print("   ✅ Better cultural representation")
    else:
        print(f"\n⚠️  RECOMMENDATION: REVIEW v2.2 ADDITIONS")
        print("   May need more work to show clear improvement")
