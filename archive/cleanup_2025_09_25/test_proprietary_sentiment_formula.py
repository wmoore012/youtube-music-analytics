#!/usr/bin/env python3
"""
Test the proprietary sentiment enhancement formula.

This test verifies that our secret sauce works correctly and provides
meaningful improvements over baseline sentiment analysis.
"""

import os
import sys

# Add src to path
sys.path.insert(0, "src")

from youtubeviz.proprietary_sentiment_formula import (
    ProprietarySentimentEnhancer,
    get_proprietary_enhancement_formula,
    parse_proprietary_formula,
)
from youtubeviz.sentiment_config import get_sentiment_config


def test_proprietary_formula_basic():
    """Test basic functionality of the proprietary formula."""

    print("🧪 Testing Proprietary Sentiment Formula")
    print("=" * 60)

    enhancer = ProprietarySentimentEnhancer()

    # Test cases that should show improvement
    test_cases = [
        {
            "text": "this song is absolutely fire no cap 🔥🔥",
            "vader_score": 0.6,
            "textblob_score": 0.4,
            "expected_improvement": True,
            "description": "Gen Z slang with emoji",
        },
        {
            "text": "the production on this track is insane, the beat goes hard",
            "vader_score": 0.5,
            "textblob_score": 0.3,
            "expected_improvement": True,
            "description": "Music production praise",
        },
        {
            "text": "can't wait for the next album to drop, this artist is goated",
            "vader_score": 0.4,
            "textblob_score": 0.2,
            "expected_improvement": True,
            "description": "Anticipation and praise",
        },
        {
            "text": "this is mid, artist fell off",
            "vader_score": -0.3,
            "textblob_score": -0.1,
            "expected_improvement": True,
            "description": "Negative Gen Z slang",
        },
        {
            "text": "okay song I guess",
            "vader_score": 0.1,
            "textblob_score": 0.0,
            "expected_improvement": False,
            "description": "Neutral comment (should not over-enhance)",
        },
    ]

    improvements = 0
    total_tests = len(test_cases)

    for i, case in enumerate(test_cases, 1):
        print(f"\n🔍 Test {i}: {case['description']}")
        print(f"   Text: '{case['text']}'")

        # Get baseline average
        baseline = (case["vader_score"] + case["textblob_score"]) / 2

        # Apply enhancement
        enhanced_score, enhanced_confidence = enhancer.enhance_sentiment_score(
            case["vader_score"], case["textblob_score"], case["text"]
        )

        # Calculate improvement
        improvement = abs(enhanced_score) - abs(baseline)

        print(f"   Baseline: {baseline:.3f}")
        print(f"   Enhanced: {enhanced_score:.3f} (confidence: {enhanced_confidence:.3f})")
        print(f"   Improvement: {improvement:.3f}")

        if case["expected_improvement"]:
            if improvement > 0.05:  # Meaningful improvement threshold
                print(f"   ✅ Expected improvement achieved")
                improvements += 1
            else:
                print(f"   ⚠️  Expected improvement not achieved")
        else:
            if abs(improvement) < 0.1:  # Should not over-enhance neutral
                print(f"   ✅ Correctly avoided over-enhancement")
                improvements += 1
            else:
                print(f"   ⚠️  Over-enhanced neutral sentiment")

    success_rate = improvements / total_tests
    print(f"\n📊 Results: {improvements}/{total_tests} tests passed ({success_rate:.1%})")

    if success_rate >= 0.8:
        print("✅ Proprietary formula is working correctly!")
        return True
    else:
        print("❌ Proprietary formula needs adjustment")
        return False


def test_formula_string_parsing():
    """Test that the formula string can be parsed correctly."""

    print("\n🔧 Testing Formula String Parsing")
    print("=" * 40)

    # Get the formula string
    formula = get_proprietary_enhancement_formula()
    print(f"Formula: {formula}")

    # Parse it
    config = parse_proprietary_formula(formula)
    print(f"Parsed config: {config}")

    # Verify expected components
    expected_components = ["CSA", "DERW", "MMSF", "TSDM", "SIGMOID"]

    success = True
    for component in expected_components:
        if component in config:
            print(f"✅ {component}: {config[component]}")
        else:
            print(f"❌ Missing component: {component}")
            success = False

    return success


def test_config_integration():
    """Test integration with the configuration system."""

    print("\n⚙️  Testing Configuration Integration")
    print("=" * 40)

    # Set test environment variables
    test_env = {
        "SENTIMENT_PROPRIETARY_ENABLED": "true",
        "SENTIMENT_SECRET_FORMULA": get_proprietary_enhancement_formula(),
        "SENTIMENT_CONFIDENCE_THRESHOLD": "0.5",
    }

    # Store original values
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        # Load config
        config = get_sentiment_config()

        print(f"Proprietary enabled: {config.proprietary_lexicon_enabled}")
        print(f"Secret formula: {config.secret_enhancement_formula[:50]}...")
        print(f"Confidence threshold: {config.confidence_threshold}")

        # Verify configuration
        success = (
            config.proprietary_lexicon_enabled
            and config.secret_enhancement_formula
            and config.confidence_threshold == 0.5
        )

        if success:
            print("✅ Configuration integration working")
        else:
            print("❌ Configuration integration failed")

        return success

    finally:
        # Restore original environment
        for key, original_value in original_env.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value


def main():
    """Run all tests."""

    print("🔒 PROPRIETARY SENTIMENT FORMULA TEST SUITE")
    print("=" * 80)

    tests = [
        ("Basic Formula Functionality", test_proprietary_formula_basic),
        ("Formula String Parsing", test_formula_string_parsing),
        ("Configuration Integration", test_config_integration),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        print("-" * 60)

        try:
            if test_func():
                print(f"✅ {test_name} PASSED")
                passed += 1
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"💥 {test_name} ERROR: {e}")
            import traceback

            traceback.print_exc()

    print(f"\n📊 FINAL RESULTS: {passed}/{total} tests passed ({passed/total:.1%})")

    if passed == total:
        print("🎉 All tests passed! Proprietary formula is ready for production.")
        return True
    else:
        print("⚠️  Some tests failed. Review and fix before deployment.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
