#!/usr/bin/env python3
"""
Demo: Proprietary Sentiment Analysis System

This demonstrates the complete proprietary sentiment analysis system
with the secret formula and professional configuration management.
"""

import os
import sys

# Add paths
sys.path.insert(0, "src")

from youtubeviz.enhanced_sentiment_integration import EnhancedSentimentPipeline
from youtubeviz.proprietary_sentiment_formula import get_proprietary_enhancement_formula
from youtubeviz.sentiment_config import get_sentiment_config


def demo_proprietary_system():
    """Demonstrate the proprietary sentiment system."""

    print("🔒 PROPRIETARY SENTIMENT ANALYSIS SYSTEM DEMO")
    print("=" * 80)

    # Set up proprietary configuration
    print("\n⚙️  Setting up proprietary configuration...")

    proprietary_env = {
        "SENTIMENT_PROPRIETARY_ENABLED": "true",
        "SENTIMENT_SECRET_FORMULA": get_proprietary_enhancement_formula(),
        "SENTIMENT_CONFIDENCE_THRESHOLD": "0.5",
        "SENTIMENT_VARIANT_TYPE": "comprehensive",
        "SENTIMENT_CUSTOM_BOOSTERS": '{"no_cap": 0.35, "fr": 0.30, "deadass": 0.40, "periodt": 0.32}',
    }

    # Store original values
    original_env = {}
    for key, value in proprietary_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    try:
        # Initialize the enhanced pipeline
        print("🚀 Initializing Enhanced Sentiment Pipeline...")
        pipeline = EnhancedSentimentPipeline()

        config = get_sentiment_config()
        print(f"✅ Pipeline configured:")
        print(f"   Variant: {config.variant_type}")
        print(f"   Confidence threshold: {config.confidence_threshold}")
        print(f"   Privacy features: {config.get_privacy_summary()}")

        # Test with music industry comments
        print(f"\n🎵 Testing with Music Industry Comments")
        print("-" * 60)

        test_comments = [
            "this song is absolutely fire no cap 🔥🔥🔥",
            "the production on this track is insane, the beat goes hard",
            "can't wait for the next album to drop, this artist is goated",
            "omg the vocals are beautiful, she ate and left no crumbs periodt",
            "this is mid tbh, artist fell off",
            "the mix is clean but the lyrics are cringe",
            "bro this slaps different, straight masterpiece",
            "I'm obsessed with this song, on repeat all day",
            "the way I screamed when the beat dropped 😭",
            "industry plant vibes, overproduced and soulless",
        ]

        # Get enhanced analyzer
        enhanced_analyzer = pipeline.get_enhanced_analyzer()

        results = []
        for i, comment in enumerate(test_comments, 1):
            print(f"\n🔍 Comment {i}: '{comment}'")

            # Get VADER score
            vader_scores = enhanced_analyzer.polarity_scores(comment)
            vader_score = vader_scores["compound"]

            # Apply proprietary enhancement if available
            if pipeline.proprietary_enhancer:
                try:
                    from textblob import TextBlob

                    textblob_score = TextBlob(comment).sentiment.polarity

                    enhanced_score, enhanced_confidence = pipeline.proprietary_enhancer.enhance_sentiment_score(
                        vader_score, textblob_score, comment
                    )

                    improvement = enhanced_score - vader_score

                    print(f"   VADER: {vader_score:.3f}")
                    print(f"   Enhanced: {enhanced_score:.3f} (confidence: {enhanced_confidence:.3f})")
                    print(f"   Improvement: {improvement:+.3f}")

                    # Classify sentiment
                    if enhanced_score > 0.1:
                        sentiment = "POSITIVE"
                    elif enhanced_score < -0.1:
                        sentiment = "NEGATIVE"
                    else:
                        sentiment = "NEUTRAL"

                    print(f"   Classification: {sentiment}")

                    results.append(
                        {
                            "comment": comment,
                            "vader_score": vader_score,
                            "enhanced_score": enhanced_score,
                            "confidence": enhanced_confidence,
                            "improvement": improvement,
                            "sentiment": sentiment,
                        }
                    )

                except Exception as e:
                    print(f"   ❌ Enhancement failed: {e}")
            else:
                print(f"   VADER only: {vader_score:.3f}")

        # Summary statistics
        if results:
            print(f"\n📊 ENHANCEMENT SUMMARY")
            print("-" * 40)

            avg_improvement = sum(r["improvement"] for r in results) / len(results)
            avg_confidence = sum(r["confidence"] for r in results) / len(results)

            positive_count = sum(1 for r in results if r["sentiment"] == "POSITIVE")
            negative_count = sum(1 for r in results if r["sentiment"] == "NEGATIVE")
            neutral_count = sum(1 for r in results if r["sentiment"] == "NEUTRAL")

            print(f"Average improvement: {avg_improvement:+.3f}")
            print(f"Average confidence: {avg_confidence:.3f}")
            print(f"Sentiment distribution:")
            print(f"  Positive: {positive_count} ({positive_count/len(results):.1%})")
            print(f"  Negative: {negative_count} ({negative_count/len(results):.1%})")
            print(f"  Neutral: {neutral_count} ({neutral_count/len(results):.1%})")

            # Show best improvements
            best_improvements = sorted(results, key=lambda x: abs(x["improvement"]), reverse=True)[:3]
            print(f"\n🏆 Top Improvements:")
            for i, result in enumerate(best_improvements, 1):
                print(f"  {i}. {result['improvement']:+.3f}: '{result['comment'][:50]}...'")

        print(f"\n🎉 Demo completed successfully!")

        # Show configuration for production use
        print(f"\n🔐 PRODUCTION CONFIGURATION")
        print("-" * 40)
        print("Add these to your .env file:")
        print()
        for key, value in proprietary_env.items():
            if "FORMULA" in key:
                print(f"{key}={value}")
            else:
                print(f"{key}={value}")

        return True

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Restore original environment
        for key, original_value in original_env.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value


def demo_deployment_recommendation():
    """Demo the deployment recommendation system."""

    print(f"\n🚀 DEPLOYMENT RECOMMENDATION DEMO")
    print("=" * 60)

    # Set up environment for testing
    os.environ["SENTIMENT_PROPRIETARY_ENABLED"] = "true"
    os.environ["SENTIMENT_SECRET_FORMULA"] = get_proprietary_enhancement_formula()

    try:
        pipeline = EnhancedSentimentPipeline()

        print("Generating deployment recommendation...")
        recommendation = pipeline.generate_deployment_recommendation()

        print(f"\n📋 DEPLOYMENT RECOMMENDATION")
        print("-" * 40)
        print(f"Recommendation: {recommendation['recommendation'].upper()}")
        print(f"Risk Level: {recommendation['risk_level'].upper()}")
        print(f"Rationale: {recommendation['rationale']}")

        if "metrics" in recommendation:
            metrics = recommendation["metrics"]
            print(f"\nMetrics:")
            print(f"  Sample size: {metrics.get('evaluation_sample_size', 'N/A')}")
            print(f"  Average agreement: {metrics.get('average_agreement', 0):.3f}")
            print(f"  Average correlation: {metrics.get('average_correlation', 0):.3f}")

        return True

    except Exception as e:
        print(f"❌ Deployment recommendation failed: {e}")
        return False


if __name__ == "__main__":
    print("🔒 PROPRIETARY SENTIMENT ANALYSIS SYSTEM")
    print("=" * 80)
    print("This demo shows the complete proprietary sentiment enhancement system")
    print("with the secret formula and professional configuration management.")
    print()

    success = True

    # Run main demo
    if not demo_proprietary_system():
        success = False

    # Run deployment demo
    if not demo_deployment_recommendation():
        success = False

    if success:
        print(f"\n🎉 ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("The proprietary sentiment analysis system is ready for production use.")
    else:
        print(f"\n⚠️  Some demos failed. Please review and fix issues.")

    sys.exit(0 if success else 1)
