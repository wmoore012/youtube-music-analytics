#!/usr/bin/env python3
"""
Test ML Classifier on YOUR Manual Classifications

This tests the ML classifier on the exact comments you said were
misclassified as neutral when they're obviously positive.
"""

import sys

sys.path.insert(0, "src")

from youtubeviz.music_ml_classifier import MusicMLClassifier

from youtubeviz.vader_variants import VADERVariantManager, VariantType


def test_on_your_problem_comments():
    """Test on the exact comments you identified as problems."""

    print("🎯 TESTING ON YOUR MANUAL CLASSIFICATIONS")
    print("=" * 60)
    print("These are the comments you said were obviously positive but")
    print("being classified as neutral by VADER systems.")
    print()

    # Your manual classifications from the benchmark analysis
    your_classifications = [
        # POSITIVE (you said these are obviously positive)
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
        # NEUTRAL (you said these should stay neutral)
        ("Mal from descendants two", "neutral"),
        ("Imagine being a food delivery person not realizing who you're actually delivering to 😮", "neutral"),
        # Additional obvious cases
        ("this song is fire", "positive"),
        ("no cap this slaps", "positive"),
        ("periodt she ate", "positive"),
        ("this is mid", "negative"),
        ("artist fell off", "negative"),
    ]

    # Initialize models
    print("🤖 Initializing ML classifier...")
    ml_classifier = MusicMLClassifier()
    ml_classifier.train(include_isrc_feature=True, use_enhanced_features=True)

    print("🎛️  Initializing VADER variants...")
    vader_manager = VADERVariantManager()
    stock_vader = vader_manager.vader_manager if hasattr(vader_manager, "vader_manager") else None

    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        stock_vader = SentimentIntensityAnalyzer()
    except ImportError:
        stock_vader = None

    enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)

    print(f"\n🧪 Testing {len(your_classifications)} manually classified comments:")
    print("-" * 80)

    # Track results
    ml_correct = 0
    stock_correct = 0
    enhanced_correct = 0

    for comment, true_label in your_classifications:
        print(f"\n💬 \"{comment[:60]}{'...' if len(comment) > 60 else ''}\"")
        print(f"   🎯 Your classification: {true_label.upper()}")

        # Test ML classifier
        ml_result = ml_classifier.predict(comment)
        ml_pred = ml_result["sentiment"]
        ml_conf = ml_result.get("confidence", ml_result.get("sentiment_confidence", 0.5))

        if ml_pred == true_label:
            print(f"   🤖 ML Classifier: {ml_pred.upper()} ({ml_conf:.3f}) ✅")
            ml_correct += 1
        else:
            print(f"   🤖 ML Classifier: {ml_pred.upper()} ({ml_conf:.3f}) ❌")

        # Test Stock VADER
        if stock_vader:
            stock_scores = stock_vader.polarity_scores(comment)
            stock_compound = stock_scores["compound"]
            stock_pred = "positive" if stock_compound > 0.1 else "negative" if stock_compound < -0.1 else "neutral"

            if stock_pred == true_label:
                print(f"   📊 Stock VADER: {stock_pred.upper()} ({stock_compound:.3f}) ✅")
                stock_correct += 1
            else:
                print(f"   📊 Stock VADER: {stock_pred.upper()} ({stock_compound:.3f}) ❌")

        # Test Enhanced VADER
        enhanced_scores = enhanced_vader.polarity_scores(comment)
        enhanced_compound = enhanced_scores["compound"]
        enhanced_pred = "positive" if enhanced_compound > 0.1 else "negative" if enhanced_compound < -0.1 else "neutral"

        if enhanced_pred == true_label:
            print(f"   🎵 Enhanced VADER: {enhanced_pred.upper()} ({enhanced_compound:.3f}) ✅")
            enhanced_correct += 1
        else:
            print(f"   🎵 Enhanced VADER: {enhanced_pred.upper()} ({enhanced_compound:.3f}) ❌")

    # Calculate accuracies
    total = len(your_classifications)
    ml_accuracy = ml_correct / total
    stock_accuracy = stock_correct / total if stock_vader else 0
    enhanced_accuracy = enhanced_correct / total

    print(f"\n📊 RESULTS ON YOUR MANUAL CLASSIFICATIONS:")
    print("=" * 60)
    print(f"🤖 ML Classifier:     {ml_correct}/{total} ({ml_accuracy:.1%}) correct")
    if stock_vader:
        print(f"📊 Stock VADER:       {stock_correct}/{total} ({stock_accuracy:.1%}) correct")
    else:
        print(f"📊 Stock VADER:       N/A (not available)")
    print(f"🎵 Enhanced VADER:    {enhanced_correct}/{total} ({enhanced_accuracy:.1%}) correct")

    print(f"\n🏆 WINNER: ", end="")
    if ml_accuracy > stock_accuracy and ml_accuracy > enhanced_accuracy:
        print("ML CLASSIFIER! 🤖")
        print(f"   ML classifier is {ml_accuracy - max(stock_accuracy, enhanced_accuracy):.1%} better!")
    elif enhanced_accuracy > stock_accuracy:
        print("ENHANCED VADER! 🎵")
    else:
        print("STOCK VADER! 📊")

    # Show specific improvements
    print(f"\n🔍 WHERE ML CLASSIFIER EXCELS:")
    for comment, true_label in your_classifications:
        if true_label == "positive":
            ml_result = ml_classifier.predict(comment)
            if ml_result["sentiment"] == "positive" and ml_result["confidence"] > 0.8:
                print(f"   ✅ \"{comment[:40]}...\" → {ml_result['sentiment'].upper()} ({ml_result['confidence']:.3f})")

    return {
        "ml_accuracy": ml_accuracy,
        "stock_accuracy": stock_accuracy,
        "enhanced_accuracy": enhanced_accuracy,
        "total_tests": total,
    }


if __name__ == "__main__":
    results = test_on_your_problem_comments()

    print(f"\n🎯 CONCLUSION:")
    if results["ml_accuracy"] > 0.8:
        print("✅ ML classifier is working great on your manual classifications!")
        print("   The benchmark failure was due to dataset mismatch, not model failure.")
    elif results["ml_accuracy"] > 0.6:
        print("✅ ML classifier shows clear improvement over rule-based systems!")
    else:
        print("⚠️  ML classifier needs more work on your specific examples.")

    print(f"\n💡 The benchmark failed because it used 100% neutral comments,")
    print(f"   but your ML classifier was trained to distinguish positive/negative!")


def compare_distilbert_vs_enhanced_vader():
    """Compare DistilBERT transformer vs Enhanced VADER on your classifications."""
    
    print("\n" + "=" * 80)
    print("🔍 DISTILBERT vs ENHANCED VADER COMPARISON")
    print("=" * 80)
    print("Showing cases where DistilBERT and Enhanced VADER disagree")
    print()
    
    # Initialize models
    from youtubeviz.music_ml_classifier import MusicSentimentTransformer
    from youtubeviz.vader_variants import VADERVariantManager, VariantType
    
    try:
        distilbert = MusicSentimentTransformer("distilbert-base-uncased")
        vader_manager = VADERVariantManager()
        enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)
        
        print("✅ Loaded DistilBERT and Enhanced VADER")
    except Exception as e:
        print(f"❌ Error loading models: {e}")
        return
    
    # Test comments
    test_comments = [
        ("YALL ATEEEE", "positive"),
        ("U R so criminally underrated its actually so crazy. I swear that if you keep it up you'll make it big", "positive"),
        ("Omg she ATEEEEE", "positive"),
        ("The bass in this song is SUPERNATURAL!", "positive"),
        ("he's so underrated", "positive"),
        ("10's across the board mommy 🤧❤️", "positive"),
        ("I've watched this video so many times it's addictive", "positive"),
        ("Y'all really have a lot of songs! Where is the Album!!?", "positive"),
        ("this song is fire", "positive"),
        ("no cap this slaps", "positive"),
        ("periodt she ate", "positive"),
        ("this is mid", "negative"),
        ("artist fell off", "negative"),
        ("Mal from descendants two", "neutral"),
        ("Imagine being a food delivery person not realizing who you're actually delivering to 😮", "neutral"),
    ]
    
    disagreements = []
    agreements = []
    
    for comment, true_label in test_comments:
        # Get DistilBERT prediction
        distilbert_result = distilbert.predict(comment, has_isrc=False)
        distilbert_pred = distilbert_result["sentiment"]
        distilbert_conf = distilbert_result["sentiment_confidence"]
        
        # Get Enhanced VADER prediction
        vader_scores = enhanced_vader.polarity_scores(comment)
        vader_compound = vader_scores["compound"]
        if vader_compound > 0.1:
            vader_pred = "positive"
        elif vader_compound < -0.1:
            vader_pred = "negative"
        else:
            vader_pred = "neutral"
        
        # Check if they disagree
        if distilbert_pred != vader_pred:
            disagreements.append({
                "comment": comment,
                "true_label": true_label,
                "distilbert": distilbert_pred,
                "distilbert_conf": distilbert_conf,
                "vader": vader_pred,
                "vader_score": vader_compound
            })
        else:
            agreements.append({
                "comment": comment,
                "true_label": true_label,
                "prediction": distilbert_pred
            })
    
    # Show disagreements
    print(f"🥊 DISAGREEMENTS ({len(disagreements)} cases):")
    print("-" * 80)
    
    for i, case in enumerate(disagreements, 1):
        comment = case["comment"][:60] + "..." if len(case["comment"]) > 60 else case["comment"]
        
        # Determine who's right
        distilbert_correct = "✅" if case["distilbert"] == case["true_label"] else "❌"
        vader_correct = "✅" if case["vader"] == case["true_label"] else "❌"
        
        print(f"{i}. \"{comment}\"")
        print(f"   True Label: {case['true_label'].upper()}")
        print(f"   🤖 DistilBERT: {case['distilbert'].upper()} ({case['distilbert_conf']:.3f}) {distilbert_correct}")
        print(f"   🎵 Enhanced VADER: {case['vader'].upper()} ({case['vader_score']:.3f}) {vader_correct}")
        print()
    
    # Show agreements
    print(f"🤝 AGREEMENTS ({len(agreements)} cases):")
    print("-" * 40)
    
    for case in agreements:
        comment = case["comment"][:50] + "..." if len(case["comment"]) > 50 else case["comment"]
        correct = "✅" if case["prediction"] == case["true_label"] else "❌"
        print(f"   \"{comment}\" → {case['prediction'].upper()} {correct}")
    
    # Summary
    distilbert_wins = sum(1 for case in disagreements if case["distilbert"] == case["true_label"])
    vader_wins = sum(1 for case in disagreements if case["vader"] == case["true_label"])
    
    print(f"\n📊 DISAGREEMENT SUMMARY:")
    print(f"   🤖 DistilBERT wins: {distilbert_wins}/{len(disagreements)} disagreements")
    print(f"   🎵 Enhanced VADER wins: {vader_wins}/{len(disagreements)} disagreements")
    
    if distilbert_wins > vader_wins:
        print(f"   🏆 DistilBERT is better at handling music slang!")
    elif vader_wins > distilbert_wins:
        print(f"   🏆 Enhanced VADER is better at music sentiment!")
    else:
        print(f"   🤝 They're equally good on disagreements!")


def test_combined_distilbert_enhanced_vader():
    """Test a combined model using both DistilBERT and Enhanced VADER."""
    
    print("\n" + "=" * 80)
    print("🚀 COMBINED MODEL: DistilBERT + Enhanced VADER")
    print("=" * 80)
    print("Testing a hybrid approach that uses both models together")
    print()
    
    # Initialize models
    from youtubeviz.music_ml_classifier import MusicSentimentTransformer
    from youtubeviz.vader_variants import VADERVariantManager, VariantType
    
    try:
        distilbert = MusicSentimentTransformer("distilbert-base-uncased")
        vader_manager = VADERVariantManager()
        enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)
        
        print("✅ Loaded both models for combination")
    except Exception as e:
        print(f"❌ Error loading models: {e}")
        return
    
    def combined_predict(comment):
        """Combine DistilBERT and Enhanced VADER predictions."""
        
        # Get DistilBERT prediction
        distilbert_result = distilbert.predict(comment, has_isrc=False)
        distilbert_pred = distilbert_result["sentiment"]
        distilbert_conf = distilbert_result["sentiment_confidence"]
        
        # Get Enhanced VADER prediction
        vader_scores = enhanced_vader.polarity_scores(comment)
        vader_compound = vader_scores["compound"]
        if vader_compound > 0.1:
            vader_pred = "positive"
        elif vader_compound < -0.1:
            vader_pred = "negative"
        else:
            vader_pred = "neutral"
        
        # Combination logic: 
        # If both agree, use that prediction with high confidence
        # If they disagree, use DistilBERT but with lower confidence
        # If VADER is very confident (>0.5 or <-0.5), give it more weight
        
        if distilbert_pred == vader_pred:
            # Both agree - high confidence
            combined_pred = distilbert_pred
            combined_conf = min(0.95, (distilbert_conf + abs(vader_compound)) / 2)
        elif abs(vader_compound) > 0.5:
            # VADER is very confident - trust it more
            combined_pred = vader_pred
            combined_conf = abs(vader_compound) * 0.8
        else:
            # Disagreement with low VADER confidence - trust DistilBERT
            combined_pred = distilbert_pred
            combined_conf = distilbert_conf * 0.7  # Lower confidence due to disagreement
        
        return {
            "sentiment": combined_pred,
            "confidence": combined_conf,
            "distilbert_pred": distilbert_pred,
            "distilbert_conf": distilbert_conf,
            "vader_pred": vader_pred,
            "vader_score": vader_compound
        }
    
    # Test on your classifications
    test_comments = [
        ("YALL ATEEEE", "positive"),
        ("U R so criminally underrated its actually so crazy", "positive"),
        ("Omg she ATEEEEE", "positive"),
        ("The bass in this song is SUPERNATURAL!", "positive"),
        ("periodt she ate", "positive"),
        ("this is mid", "negative"),
        ("artist fell off", "negative"),
        ("Mal from descendants two", "neutral"),
    ]
    
    print("🧪 Testing Combined Model:")
    print("-" * 60)
    
    combined_correct = 0
    
    for comment, true_label in test_comments:
        result = combined_predict(comment)
        
        correct = "✅" if result["sentiment"] == true_label else "❌"
        if result["sentiment"] == true_label:
            combined_correct += 1
        
        comment_short = comment[:50] + "..." if len(comment) > 50 else comment
        
        print(f"\n💬 \"{comment_short}\"")
        print(f"   🎯 True: {true_label.upper()}")
        print(f"   🤖 DistilBERT: {result['distilbert_pred'].upper()} ({result['distilbert_conf']:.3f})")
        print(f"   🎵 Enhanced VADER: {result['vader_pred'].upper()} ({result['vader_score']:.3f})")
        print(f"   🚀 COMBINED: {result['sentiment'].upper()} ({result['confidence']:.3f}) {correct}")
    
    accuracy = combined_correct / len(test_comments)
    print(f"\n📊 COMBINED MODEL RESULTS:")
    print(f"   Accuracy: {combined_correct}/{len(test_comments)} ({accuracy:.1%})")
    
    if accuracy > 0.8:
        print(f"   🎉 Excellent! The combined approach works great!")
    elif accuracy > 0.6:
        print(f"   👍 Good! The combination shows promise!")
    else:
        print(f"   😞 The combination needs more work.")


def compare_top_models_vs_enhanced_vader():
    """Compare top 2 transformers vs Enhanced VADER - simple table format."""
    
    print("\n" + "=" * 80)
    print("🏆 TOP 2 TRANSFORMERS vs ENHANCED VADER")
    print("=" * 80)
    
    # Initialize models
    from youtubeviz.music_ml_classifier import MusicSentimentTransformer
    from youtubeviz.vader_variants import VADERVariantManager, VariantType
    
    try:
        distilbert = MusicSentimentTransformer("distilbert-base-uncased")
        emotion_ai = MusicSentimentTransformer("j-hartmann/emotion-english-distilroberta-base")
        vader_manager = VADERVariantManager()
        enhanced_vader = vader_manager.create_variant(VariantType.COMPREHENSIVE)
        
        print("✅ Loaded DistilBERT, Emotion AI, and Enhanced VADER")
    except Exception as e:
        print(f"❌ Error loading models: {e}")
        return
    
    # Test comments - expanded to find differences
    test_comments = [
        ("YALL ATEEEE", "positive"),
        ("Omg she ATEEEEE", "positive"),
        ("he's so underrated", "positive"),
        ("this song is fire", "positive"),
        ("periodt she ate", "positive"),
        ("this is mid", "negative"),
        ("artist fell off", "negative"),
        ("Mal from descendants two", "neutral"),
        # Additional tests to find differences
        ("The bass in this song is SUPERNATURAL!", "positive"),
        ("I've watched this video so many times it's addictive", "positive"),
        ("Y'all really have a lot of songs! Where is the Album!!?", "positive"),
        ("if y'all don't release this song", "positive"),
        ("Even that last part where he mumbles is raw", "positive"),
        ("10's across the board mommy 🤧❤️", "positive"),
        ("That is my new favourite guitar solo.", "positive"),
        ("I don't understand how this hasn't blown up yet!", "positive"),
        ("Imagine being a food delivery person not realizing who you're actually delivering to 😮", "neutral"),
        ("okay song I guess", "neutral"),
        ("not bad", "neutral"),
        ("overrated", "negative"),
        ("fell off", "negative"),
    ]
    
    print(f"\n{'Comment':<40} {'True':<8} {'DistilBERT':<12} {'Emotion AI':<12} {'Enhanced VADER':<15}")
    print("-" * 95)
    
    distilbert_correct = 0
    emotion_correct = 0
    vader_correct = 0
    
    for comment, true_label in test_comments:
        # Get predictions
        distilbert_result = distilbert.predict(comment, has_isrc=False)
        distilbert_pred = distilbert_result["sentiment"]
        
        emotion_result = emotion_ai.predict(comment, has_isrc=False)
        emotion_pred = emotion_result["sentiment"]
        
        vader_scores = enhanced_vader.polarity_scores(comment)
        vader_compound = vader_scores["compound"]
        if vader_compound > 0.1:
            vader_pred = "positive"
        elif vader_compound < -0.1:
            vader_pred = "negative"
        else:
            vader_pred = "neutral"
        
        # Check correctness
        distilbert_mark = "✅" if distilbert_pred == true_label else "❌"
        emotion_mark = "✅" if emotion_pred == true_label else "❌"
        vader_mark = "✅" if vader_pred == true_label else "❌"
        
        if distilbert_pred == true_label:
            distilbert_correct += 1
        if emotion_pred == true_label:
            emotion_correct += 1
        if vader_pred == true_label:
            vader_correct += 1
        
        # Format comment
        comment_short = comment[:35] + "..." if len(comment) > 35 else comment
        
        print(f"{comment_short:<40} {true_label:<8} {distilbert_pred + ' ' + distilbert_mark:<12} {emotion_pred + ' ' + emotion_mark:<12} {vader_pred + ' ' + vader_mark:<15}")
    
    print("-" * 95)
    total_tests = len(test_comments)
    print(f"{'ACCURACY:':<40} {'':<8} {distilbert_correct}/{total_tests} ({distilbert_correct/total_tests:.1%})   {emotion_correct}/{total_tests} ({emotion_correct/total_tests:.1%})   {vader_correct}/{total_tests} ({vader_correct/total_tests:.1%})")
    
    # Show where they differ
    print(f"\n🔍 WHERE TOP 2 MODELS DIFFER:")
    print("-" * 50)
    
    for comment, true_label in test_comments:
        distilbert_result = distilbert.predict(comment, has_isrc=False)
        emotion_result = emotion_ai.predict(comment, has_isrc=False)
        
        if distilbert_result["sentiment"] != emotion_result["sentiment"]:
            comment_short = comment[:40] + "..." if len(comment) > 40 else comment
            print(f"💬 \"{comment_short}\"")
            print(f"   🤖 DistilBERT: {distilbert_result['sentiment'].upper()}")
            print(f"   🧠 Emotion AI: {emotion_result['sentiment'].upper()}")
            print(f"   🎯 Correct: {true_label.upper()}")
            print()


if __name__ == "__main__":
    # Run original test
    results = test_on_your_problem_comments()
    
    # Run simple comparison
    compare_top_models_vs_enhanced_vader()
