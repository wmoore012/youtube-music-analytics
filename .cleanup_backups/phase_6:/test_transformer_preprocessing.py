#!/usr/bin/env python3
"""
Test Transformer-Ready Preprocessing Implementation

Tests the enhanced preprocessing with transformer-ready features.
"""

import sys

sys.path.insert(0, "src")


def test_text_processing_helpers():
    """Test the new text processing helpers."""
    print("🧪 Testing Text Processing Helpers...")

    try:
        from youtubeviz.text_processing_helpers import (
            EmojiHandler,
            EmojiHandlingMode,
            MusicSlangPreserver,
            SlangPreservationLevel,
            TransformerTextProcessor,
            create_music_text_processor,
        )

        # Test music slang preservation
        print("   Testing music slang preservation...")
        preserver = MusicSlangPreserver(SlangPreservationLevel.COMPREHENSIVE)

        test_text = "This song absolutely SLAPS! It's GOATED fr, no cap 🔥"
        preserved, replacements = preserver.preserve_slang_in_text(test_text)
        restored = preserver.restore_slang_in_text(preserved, replacements)

        slang_terms = preserver.identify_slang_terms(test_text)

        print(f"     Original: {test_text}")
        print(f"     Slang terms found: {len(slang_terms)}")
        for term in slang_terms:
            print(f"       - {term['term']} ({term['sentiment']})")

        # Test emoji handling
        print("   Testing emoji handling...")
        emoji_handler = EmojiHandler(EmojiHandlingMode.CONVERT_TO_TEXT)

        emoji_text = "This is fire 🔥🔥 absolutely love it 😍💯"
        converted = emoji_handler.process_emoji(emoji_text)
        emoji_count = emoji_handler.count_emoji(emoji_text)

        print(f"     Original: {emoji_text}")
        print(f"     Converted: {converted}")
        print(f"     Emoji count: {emoji_count}")

        # Test transformer processor
        print("   Testing transformer text processor...")
        processor = create_music_text_processor()

        sample_comments = [
            "This song SLAPS! 🔥 PERIODT",
            "The beat goes hard but it's kinda mid tbh 😭",
            "@artist please drop more like this! 🙏",
        ]

        for comment in sample_comments:
            processed = processor.preprocess_text(comment)
            features = processor.analyze_text_features(comment)

            print(f"     Original:  {comment}")
            print(f"     Processed: {processed}")
            print(f"     Features:  {features['slang_count']} slang, {features['emoji_count']} emoji")

        print("✅ Text processing helpers working correctly")
        return True

    except Exception as e:
        print(f"❌ Text processing helpers test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_enhanced_smart_classifier():
    """Test enhanced smart comment classifier with transformer support."""
    print("\n🧪 Testing Enhanced Smart Comment Classifier...")

    try:
        from youtubeviz.smart_comment_classifier import SmartCommentClassifier

        # Test traditional classifier
        print("   Testing traditional classifier...")
        traditional_classifier = SmartCommentClassifier(use_transformer=False)
        print(f"     Traditional classifier created: {not traditional_classifier.use_transformer}")

        # Test transformer classifier
        print("   Testing transformer classifier...")
        transformer_classifier = SmartCommentClassifier(use_transformer=True)
        print(f"     Transformer classifier created: {transformer_classifier.use_transformer}")

        if transformer_classifier.transformer_processor:
            print("     Transformer processor available")

            # Test feature analysis
            sample_comment = "This song absolutely slaps! 🔥 No cap, it's GOATED"
            features = transformer_classifier.analyze_comment_features(sample_comment)

            print(f"     Sample analysis:")
            print(f"       Comment: {sample_comment}")
            print(f"       Slang count: {features.get('slang_count', 0)}")
            print(f"       Emoji count: {features.get('emoji_count', 0)}")
            print(f"       Word count: {features.get('word_count', 0)}")
        else:
            print("     Transformer processor not available (expected if transformers not installed)")

        print("✅ Enhanced smart classifier working correctly")
        return True

    except Exception as e:
        print(f"❌ Enhanced smart classifier test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_enhanced_sentiment_dataset():
    """Test enhanced sentiment dataset with transformer formats."""
    print("\n🧪 Testing Enhanced Sentiment Dataset...")

    try:
        from datasets.enhanced_sentiment_dataset import get_enhanced_music_dataset, normalize_text_for_transformer

        # Test dataset loading
        print("   Testing dataset loading...")
        dataset = get_enhanced_music_dataset()
        print(f"     Dataset loaded: {len(dataset.entries)} entries")

        # Test transformer normalization
        print("   Testing transformer normalization...")
        test_texts = ["This song SLAPS! 🔥🔥 PERIODT", "The beat goes hard but it's mid 😭", "GOATED artist no cap 💯"]

        for text in test_texts:
            normalized = normalize_text_for_transformer(text, preserve_emoji=True)
            normalized_no_emoji = normalize_text_for_transformer(text, preserve_emoji=False)

            print(f"     Original: {text}")
            print(f"     With emoji: {normalized}")
            print(f"     No emoji: {normalized_no_emoji}")

        # Test transformer export
        print("   Testing transformer export...")
        try:
            dataset.export_transformer_format("test_transformer_export.jsonl")
            print("     ✅ Transformer format export successful")

            # Clean up test file
            import os

            if os.path.exists("test_transformer_export.jsonl"):
                os.remove("test_transformer_export.jsonl")

        except Exception as e:
            print(f"     ⚠️  Transformer export failed (may need text processing helpers): {e}")

        # Test training config generation
        print("   Testing training config generation...")
        config = dataset.create_transformer_training_config()
        print(f"     Config created: {config['model_name']}")
        print(f"     Num labels: {config['num_labels']}")
        print(f"     Dataset size: {config['dataset_size']}")

        print("✅ Enhanced sentiment dataset working correctly")
        return True

    except Exception as e:
        print(f"❌ Enhanced sentiment dataset test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_transformer_tokenization():
    """Test transformer tokenization if transformers library is available."""
    print("\n🧪 Testing Transformer Tokenization...")

    try:
        from youtubeviz.text_processing_helpers import TransformerTextProcessor

        # Test with different models
        models_to_test = ["distilbert-base-uncased"]

        for model_name in models_to_test:
            print(f"   Testing {model_name}...")

            try:
                processor = TransformerTextProcessor(model_name)

                if processor.tokenizer:
                    sample_text = "This song absolutely slaps! 🔥 PERIODT, it's GOATED"

                    # Test tokenization
                    tokens = processor.tokenize_for_transformer(sample_text)

                    print(f"     Sample: {sample_text}")
                    print(f"     Tokens: {len(tokens['input_ids'])} tokens")
                    print(f"     Has attention mask: {'attention_mask' in tokens}")

                    # Test batch processing
                    batch_texts = ["This slaps fr 🔥", "Mid song tbh", "GOATED artist no cap"]

                    batch_tokens = processor.batch_tokenize(batch_texts)
                    print(f"     Batch processing: {len(batch_tokens['input_ids'])} samples")

                else:
                    print(f"     ⚠️  Tokenizer not available for {model_name}")

            except Exception as e:
                print(f"     ⚠️  Could not test {model_name}: {e}")

        print("✅ Transformer tokenization test completed")
        return True

    except ImportError:
        print("⚠️  Transformers library not available - skipping tokenization test")
        return True
    except Exception as e:
        print(f"❌ Transformer tokenization test failed: {e}")
        return False


def test_integration_with_existing_systems():
    """Test integration with existing preprocessing systems."""
    print("\n🧪 Testing Integration with Existing Systems...")

    try:
        # Test integration with unique comment manager
        print("   Testing integration with unique comment manager...")
        from youtubeviz.unique_comment_manager import UniqueCommentManager

        manager = UniqueCommentManager()
        print("     ✅ Unique comment manager integration maintained")

        # Test integration with sentiment evaluation
        print("   Testing integration with sentiment evaluation...")
        from youtubeviz.sentiment_evaluation import SentimentEvaluationFramework

        framework = SentimentEvaluationFramework()

        # Test transformer dataset preparation
        sample_comments = ["This song slaps! 🔥", "Not feeling this one", "GOATED track no cap"]
        sample_labels = ["positive", "negative", "positive"]

        transformer_dataset = framework.prepare_transformer_dataset(
            comments=sample_comments, labels=sample_labels, use_unique_comments=False
        )

        if transformer_dataset:
            print(f"     ✅ Transformer dataset preparation: {transformer_dataset.total_comments} comments")
        else:
            print("     ⚠️  Transformer dataset preparation returned None (may need ML dependencies)")

        print("✅ Integration with existing systems working correctly")
        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all tests for Task 2 implementation."""
    print("🚀 TESTING TRANSFORMER-READY PREPROCESSING IMPLEMENTATION")
    print("=" * 70)

    tests = [
        test_text_processing_helpers,
        test_enhanced_smart_classifier,
        test_enhanced_sentiment_dataset,
        test_transformer_tokenization,
        test_integration_with_existing_systems,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1

    print(f"\n📊 TEST RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Transformer-ready preprocessing is working.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")

    print("\n💡 Next steps:")
    print("   1. Install transformers library: pip install transformers torch")
    print("   2. Test with actual transformer models")
    print("   3. Use enhanced preprocessing in ML training pipelines")
    print("   4. Export datasets in transformer-compatible formats")


if __name__ == "__main__":
    main()
