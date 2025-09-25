#!/usr/bin/env python3
"""
Test Dataset v2.2 Improvements

Systematically test whether the enhanced v2.2 dataset with regional slang,
NSFW handling, and cultural awareness actually improves sentiment classification
compared to the current dataset.
"""

import sys

sys.path.insert(0, "src")


def test_current_dataset_baseline():
    """Test current dataset performance on challenging examples."""
    print("🧪 Testing Current Dataset Baseline...")

    try:
        from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset

        dataset = get_enhanced_music_dataset()

        # Test challenging examples that should expose gaps
        challenging_examples = [
            # Regional slang (should be missing)
            ("yerrr this slaps", "positive", "NYC greeting + praise"),
            ("deadass this mid", "negative", "NYC intensifier + critique"),
            ("cuh this hard", "positive", "LA address + praise"),
            ("dummy fire", "positive", "Baltimore intensifier + praise"),
            # Cultural expressions (may be missing)
            ("she ate and left no crumbs", "positive", "Queer vernacular performance praise"),
            ("it's giving main character energy", "positive", "Gen Z template praise"),
            ("serving vocals", "positive", "Queer vernacular performance praise"),
            # NSFW/reclaimed (should be missing or poorly handled)
            ("BIIIITCH this is fire", "positive", "Reclaimed hype expression"),
            # Booster patterns (may not be handled well)
            ("this song 🔥🔥🔥", "positive", "Emoji spam intensity"),
            ("GOOOOES HARD", "positive", "Elongation intensity"),
            # Industry/meta terms (likely missing)
            ("industry plant vibes", "negative", "Industry criticism"),
            ("payola energy", "negative", "Industry criticism"),
            ("ratioed in the comments", "negative", "Social media criticism"),
        ]

        # Check coverage in current dataset
        current_phrases = {entry.phrase.lower() for entry in dataset.entries}

        coverage_count = 0
        missing_examples = []

        for example, expected_sentiment, description in challenging_examples:
            # Check if we have exact or similar coverage
            found = False
            for phrase in current_phrases:
                if any(word in phrase for word in example.lower().split()[:2]):  # Check first 2 words
                    found = True
                    break

            if found:
                coverage_count += 1
            else:
                missing_examples.append((example, expected_sentiment, description))

        print(f"📊 Current Dataset Coverage:")
        print(f"   Total test cases: {len(challenging_examples)}")
        print(f"   Covered cases: {coverage_count}")
        print(f"   Missing cases: {len(missing_examples)}")
        print(f"   Coverage rate: {coverage_count/len(challenging_examples)*100:.1f}%")

        if missing_examples:
            print(f"\n❌ Missing Examples:")
            for example, sentiment, desc in missing_examples[:5]:  # Show first 5
                print(f"   '{example}' ({sentiment}) - {desc}")

        return {
            "total_cases": len(challenging_examples),
            "covered_cases": coverage_count,
            "missing_cases": len(missing_examples),
            "coverage_rate": coverage_count / len(challenging_examples),
            "missing_examples": missing_examples,
        }

    except Exception as e:
        print(f"❌ Current dataset test failed: {e}")
        return None


def test_regional_slang_coverage():
    """Test how well we handle regional slang variations."""
    print("\n🗺️ Testing Regional Slang Coverage...")

    regional_tests = {
        "NYC/AAVE": ["yerrr", "deadass", "word?", "that's how we feelin", "no cap", "fr", "periodt"],
        "LA/SoCal": ["cuh", "the 405", "WeHo", "fye", "the function"],
        "Baltimore": ["dummy", "lor bro", "ard", "yo snapped", "geeking", "irky"],
        "Queer Vernacular": ["she ate", "serving", "it's giving", "YES MOTHER", "slay"],
        "Gen Z Internet": ["mid", "cringe", "glazing", "no cap", "periodt", "chef's kiss"],
    }

    try:
        from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset

        dataset = get_enhanced_music_dataset()
        current_phrases = {entry.phrase.lower() for entry in dataset.entries}

        regional_coverage = {}

        for region, terms in regional_tests.items():
            covered = 0
            for term in terms:
                # Check if term appears in any phrase
                if any(term.lower() in phrase for phrase in current_phrases):
                    covered += 1

            coverage_rate = covered / len(terms)
            regional_coverage[region] = {"covered": covered, "total": len(terms), "rate": coverage_rate}

            print(f"   {region}: {covered}/{len(terms)} ({coverage_rate*100:.1f}%)")

        return regional_coverage

    except Exception as e:
        print(f"❌ Regional slang test failed: {e}")
        return None


def test_sentiment_classification_accuracy():
    """Test sentiment classification accuracy on tricky examples."""
    print("\n🎯 Testing Sentiment Classification Accuracy...")

    # Tricky examples that often get misclassified
    tricky_examples = [
        # Positive slang that might be missed
        ("this is sick", "positive", "Slang positive"),
        ("goes hard", "positive", "Slang positive"),
        ("she ate", "positive", "Cultural positive"),
        ("no cap this slaps", "positive", "Intensified positive"),
        # Negative slang that might be missed
        ("this ain't it chief", "negative", "Polite negative"),
        ("mid", "negative", "Gen Z negative"),
        ("cringe", "negative", "Gen Z negative"),
        # Neutral that might be misclassified
        ("who produced this", "neutral", "Info request"),
        ("what's the sample", "neutral", "Info request"),
        ("I need the lyrics", "neutral", "Content request"),
        # Context-dependent (challenging)
        ("fuck it up", "positive", "Hype imperative - should be positive"),
        ("the outfits", "neutral", "Should be neutral unless boosted"),
    ]

    try:
        from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset

        dataset = get_enhanced_music_dataset()

        # Create a simple lookup for testing
        phrase_sentiments = {}
        for entry in dataset.entries:
            phrase_sentiments[entry.phrase.lower()] = entry.sentiment.value

        correct = 0
        total = len(tricky_examples)
        misclassified = []

        for phrase, expected, description in tricky_examples:
            # Check exact match first
            actual = phrase_sentiments.get(phrase.lower())

            if actual == expected:
                correct += 1
            else:
                # Check for partial matches
                found_match = False
                for dataset_phrase, sentiment in phrase_sentiments.items():
                    if phrase.lower() in dataset_phrase or dataset_phrase in phrase.lower():
                        if sentiment == expected:
                            correct += 1
                            found_match = True
                            break

                if not found_match:
                    misclassified.append((phrase, expected, actual, description))

        accuracy = correct / total

        print(f"📊 Classification Accuracy:")
        print(f"   Correct: {correct}/{total}")
        print(f"   Accuracy: {accuracy*100:.1f}%")

        if misclassified:
            print(f"\n❌ Misclassified Examples:")
            for phrase, expected, actual, desc in misclassified[:5]:
                print(f"   '{phrase}' - Expected: {expected}, Got: {actual or 'MISSING'} ({desc})")

        return {"accuracy": accuracy, "correct": correct, "total": total, "misclassified": misclassified}

    except Exception as e:
        print(f"❌ Classification accuracy test failed: {e}")
        return None


def test_dataset_balance_and_quality():
    """Test dataset balance and quality metrics."""
    print("\n⚖️ Testing Dataset Balance and Quality...")

    try:
        from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset

        from youtubeviz.model_benchmark_system import ModelBenchmarkSystem

        dataset = get_enhanced_music_dataset()
        labels = [entry.sentiment.value for entry in dataset.entries]

        # Use our quality assessment system
        benchmark_system = ModelBenchmarkSystem()
        quality_metrics = benchmark_system.assess_dataset_quality(labels)

        print(f"📊 Dataset Quality Metrics:")
        print(f"   Total samples: {quality_metrics.total_samples}")
        print(f"   Balance score: {quality_metrics.balance_score:.3f}")
        print(f"   Quality level: {quality_metrics.quality_level}")
        print(f"   Imbalance ratio: {quality_metrics.imbalance_ratio:.2f}x")

        # Check distribution
        print(f"\n📈 Distribution:")
        print(f"   Positive: {quality_metrics.positive_count} ({quality_metrics.positive_percent:.1f}%)")
        print(f"   Negative: {quality_metrics.negative_count} ({quality_metrics.negative_percent:.1f}%)")
        print(f"   Neutral: {quality_metrics.neutral_count} ({quality_metrics.neutral_percent:.1f}%)")

        return quality_metrics

    except Exception as e:
        print(f"❌ Quality assessment failed: {e}")
        return None


def simulate_v22_improvements():
    """Simulate what v2.2 dataset would improve."""
    print("\n🚀 Simulating v2.2 Dataset Improvements...")

    # Based on the provided v2.2 spec, estimate improvements
    v22_additions = {
        "Regional Coverage": {
            "NYC/AAVE": ["yerrr", "deadass", "word?", "fye", "no cap"],
            "LA/SoCal": ["cuh", "the 405", "WeHo", "function"],
            "Baltimore": ["dummy", "lor", "ard", "geeking", "irky"],
        },
        "Cultural Expressions": ["she ate", "it's giving", "serving vocals", "YES MOTHER"],
        "NSFW/Reclaimed": ["BIIIITCH (hype)", "masked n-word handling"],
        "Industry Terms": ["industry plant", "payola", "ratioed", "glazing"],
        "Booster Handling": ["elongation detection", "emoji spam", "ALL-CAPS", "punctuation runs"],
    }

    print("🎯 Expected Improvements with v2.2:")

    total_new_coverage = 0
    for category, items in v22_additions.items():
        if isinstance(items, dict):
            # Regional coverage
            for region, terms in items.items():
                total_new_coverage += len(terms)
                print(f"   {region}: +{len(terms)} terms")
        else:
            # Other categories
            total_new_coverage += len(items)
            print(f"   {category}: +{len(items)} terms/features")

    print(f"\n📊 Estimated Impact:")
    print(f"   New terms/features: ~{total_new_coverage}")
    print(f"   Regional coverage: 5 regions (NYC, LA, Baltimore, Queer, Gen Z)")
    print(f"   NSFW safety: Masked token handling")
    print(f"   Booster detection: 4 types of intensity markers")
    print(f"   Cultural sensitivity: Proper register labeling")

    return v22_additions


def main():
    """Run comprehensive dataset improvement testing."""
    print("🚀 TESTING DATASET v2.2 IMPROVEMENTS")
    print("=" * 60)
    print("Comparing current dataset vs proposed v2.2 enhancements")
    print()

    # Test current baseline
    baseline_results = test_current_dataset_baseline()

    # Test regional coverage
    regional_results = test_regional_slang_coverage()

    # Test classification accuracy
    accuracy_results = test_sentiment_classification_accuracy()

    # Test quality metrics
    quality_results = test_dataset_balance_and_quality()

    # Simulate v2.2 improvements
    v22_improvements = simulate_v22_improvements()

    # Summary
    print(f"\n📋 IMPROVEMENT ANALYSIS SUMMARY")
    print("=" * 50)

    if baseline_results:
        print(f"Current Coverage: {baseline_results['coverage_rate']*100:.1f}% of challenging examples")
        print(f"Missing Cases: {baseline_results['missing_cases']} critical examples")

    if accuracy_results:
        print(f"Classification Accuracy: {accuracy_results['accuracy']*100:.1f}%")
        print(f"Misclassified: {len(accuracy_results['misclassified'])} examples")

    if quality_results:
        print(f"Dataset Quality: {quality_results.quality_level} (balance: {quality_results.balance_score:.3f})")

    print(f"\n💡 RECOMMENDATIONS:")

    if baseline_results and baseline_results["coverage_rate"] < 0.7:
        print("   ✅ v2.2 upgrade RECOMMENDED - Low coverage of modern slang")

    if accuracy_results and accuracy_results["accuracy"] < 0.8:
        print("   ✅ v2.2 upgrade RECOMMENDED - Classification accuracy needs improvement")

    if regional_results:
        low_coverage_regions = [region for region, data in regional_results.items() if data["rate"] < 0.5]
        if low_coverage_regions:
            print(f"   ✅ Regional expansion NEEDED - Poor coverage: {', '.join(low_coverage_regions)}")

    print(f"\n🎯 NEXT STEPS:")
    print("   1. Implement v2.2 dataset with regional slang")
    print("   2. Add booster detection system")
    print("   3. Implement NSFW/cultural safety measures")
    print("   4. Test performance improvements on real data")
    print("   5. Benchmark against current system")


if __name__ == "__main__":
    main()
