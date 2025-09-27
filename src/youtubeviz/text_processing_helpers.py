#!/usr / bin / env python3
"""
Text Processing Helpers for Music Domain

Provides specialized text processing utilities for music industry content,
including music slang preservation, emoji handling, and transformer - ready preprocessing.
"""

from dataclasses import dataclass
from enum import Enum
import re
from typing import Dict, List, Optional, Set, Tuple
import unicodedata

# Import transformer support if available
try:
    from transformers import AutoTokenizer

    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False


class SlangPreservationLevel(str, Enum):
    """Levels of music slang preservation."""

    NONE = "none"
    BASIC = "basic"
    COMPREHENSIVE = "comprehensive"


class EmojiHandlingMode(str, Enum):
    """Modes for handling emoji in text."""

    PRESERVE = "preserve"
    NORMALIZE = "normalize"
    REMOVE = "remove"
    CONVERT_TO_TEXT = "convert_to_text"


@dataclass
class TextProcessingConfig:
    """Configuration for text processing operations."""

    # Basic normalization
    lowercase: bool = True
    remove_extra_whitespace: bool = True
    normalize_unicode: bool = True

    # Music - specific processing
    slang_preservation: SlangPreservationLevel = SlangPreservationLevel.COMPREHENSIVE
    preserve_case_for_slang: bool = True

    # Emoji handling
    emoji_mode: EmojiHandlingMode = EmojiHandlingMode.PRESERVE

    # Transformer - specific
    max_length: Optional[int] = None
    add_special_tokens: bool = True

    # Advanced options
    handle_mentions: bool = True
    handle_hashtags: bool = True
    preserve_punctuation: bool = True


class MusicSlangPreserver:
    """Preserves music slang terms during text processing."""

    # Comprehensive music slang dictionary
    MUSIC_SLANG_TERMS = {
        # Positive slang (case - sensitive preservation)
        "GOATED": {"variants": ["goated", "GOAT"], "sentiment": "positive", "preserve_case": True},
        "PERIODT": {"variants": ["periodt", "period"], "sentiment": "positive", "preserve_case": True},
        "SLAY": {"variants": ["slay", "slaying"], "sentiment": "positive", "preserve_case": True},
        "fire": {"variants": ["fire", "🔥"], "sentiment": "positive", "preserve_case": False},
        "slaps": {"variants": ["slap", "slapping"], "sentiment": "positive", "preserve_case": False},
        "banger": {"variants": ["bangers"], "sentiment": "positive", "preserve_case": False},
        "hits different": {"variants": ["hit different"], "sentiment": "positive", "preserve_case": False},
        "goes hard": {"variants": ["go hard"], "sentiment": "positive", "preserve_case": False},
        "chef's kiss": {"variants": ["chefs kiss"], "sentiment": "positive", "preserve_case": False},
        "no cap": {"variants": ["nocap"], "sentiment": "positive", "preserve_case": False},
        "frfr": {"variants": ["fr", "for real"], "sentiment": "neutral", "preserve_case": False},
        "deadass": {"variants": ["dead ass"], "sentiment": "neutral", "preserve_case": False},
        "lowkey": {"variants": ["low key"], "sentiment": "neutral", "preserve_case": False},
        "highkey": {"variants": ["high key"], "sentiment": "neutral", "preserve_case": False},
        # Negative slang
        "mid": {"variants": [], "sentiment": "negative", "preserve_case": False},
        "trash": {"variants": ["garbage"], "sentiment": "negative", "preserve_case": False},
        "cringe": {"variants": ["cringey"], "sentiment": "negative", "preserve_case": False},
        "ain't it": {"variants": ["aint it"], "sentiment": "negative", "preserve_case": False},
        # Cultural expressions
        "queen": {"variants": ["QUEEN"], "sentiment": "positive", "preserve_case": False},
        "king": {"variants": ["KING"], "sentiment": "positive", "preserve_case": False},
        "mother": {"variants": ["MOTHER"], "sentiment": "positive", "preserve_case": False},
        "ate": {"variants": ["ate that"], "sentiment": "positive", "preserve_case": False},
        "served": {"variants": ["serve"], "sentiment": "positive", "preserve_case": False},
        # Production / technical slang
        "clean": {"variants": [], "sentiment": "positive", "preserve_case": False},
        "crisp": {"variants": [], "sentiment": "positive", "preserve_case": False},
        "tight": {"variants": [], "sentiment": "positive", "preserve_case": False},
        "smooth": {"variants": [], "sentiment": "positive", "preserve_case": False},
    }

    def __init__(self, preservation_level: SlangPreservationLevel = SlangPreservationLevel.COMPREHENSIVE):
        self.preservation_level = preservation_level
        self._build_patterns()

    def _build_patterns(self):
        """Build regex patterns for slang detection."""
        if self.preservation_level == SlangPreservationLevel.NONE:
            self.patterns = {}
            return

        self.patterns = {}
        for term, info in self.MUSIC_SLANG_TERMS.items():
            # Create pattern for the main term and variants
            all_terms = [term] + info["variants"]
            # Escape special regex characters
            escaped_terms = [re.escape(t) for t in all_terms]
            pattern = r"\b(?:" + "|".join(escaped_terms) + r")\b"

            if self.preservation_level == SlangPreservationLevel.BASIC:
                # Only preserve high - confidence positive / negative terms
                if info["sentiment"] in ["positive", "negative"]:
                    self.patterns[term] = {
                        "pattern": re.compile(pattern, re.IGNORECASE),
                        "preserve_case": info["preserve_case"],
                        "sentiment": info["sentiment"],
                    }
            else:  # COMPREHENSIVE
                self.patterns[term] = {
                    "pattern": re.compile(pattern, re.IGNORECASE),
                    "preserve_case": info["preserve_case"],
                    "sentiment": info["sentiment"],
                }

    def preserve_slang_in_text(self, text: str) -> str:
        """Preserve music slang terms during text processing."""
        if self.preservation_level == SlangPreservationLevel.NONE:
            return text

        preserved_text_item = text
        replacements = {}

        # Find and mark slang terms for preservation
        for term, info in self.patterns.items():
            matches = info["pattern"].finditer(preserved_text)
            for match in matches:
                original = match.group()
                if info["preserve_case"]:
                    # Keep original case
                    placeholder = f"__SLANG_{len(replacements)}__"
                    replacements[placeholder] = original
                    preserved_text = preserved_text.replace(original, placeholder, 1)
                else:
                    # Normalize to lowercase but mark as slang
                    placeholder = f"__SLANG_{len(replacements)}__"
                    replacements[placeholder] = original.lower()
                    preserved_text = preserved_text.replace(original, placeholder, 1)

        return preserved_text, replacements

    def restore_slang_in_text(self, text: str, replacements: Dict[str, str]) -> str:
        """Restore preserved slang terms after processing."""
        restored_text = text
        for placeholder, original in replacements.items():
            restored_text = restored_text.replace(placeholder, original)
        return restored_text

    def identify_slang_terms(self, text: str) -> List[Dict[str, str]]:
        """Identify music slang terms in text."""
        identified = []

        for term, info in self.patterns.items():
            matches = info["pattern"].finditer(text)
            for match in matches:
                identified.append(
                    {
                        "term": match.group(),
                        "canonical": term,
                        "sentiment": info["sentiment"],
                        "start": match.start(),
                        "end": match.end(),
                    }
                )

        return identified


class EmojiHandler:
    """Handles emoji processing for music domain text."""

    # Music - related emoji mappings
    MUSIC_EMOJI_MAP = {
        "🔥": " fire ",
        "💯": " hundred ",
        "😍": " love ",
        "😭": " crying ",
        "👑": " crown ",
        "🎵": " music ",
        "🎶": " musical_note ",
        "🎤": " microphone ",
        "🎧": " headphones ",
        "🎸": " guitar ",
        "🥁": " drums ",
        "🎹": " piano ",
        "💿": " cd ",
        "📀": " dvd ",
        "🎺": " trumpet ",
        "🎷": " saxophone ",
        "🎻": " violin ",
        "⛽": " gas ",  # Often used with "gas" slang
        "💎": " diamond ",
        "✨": " sparkles ",
        "🌟": " star ",
        "⭐": " star ",
        "🚀": " rocket ",
        "💫": " dizzy ",
        "🔊": " loud ",
        "📢": " megaphone ",
        "🎯": " target ",
        "💪": " strong ",
        "👏": " clap ",
        "🙌": " praise ",
        "👌": " ok ",
        "💘": " heart ",
        "❤️": " heart ",
        "💖": " heart ",
        "💕": " hearts ",
        "💓": " heartbeat ",
    }

    # Emoji patterns
    EMOJI_PATTERN = re.compile(
        r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
        r"\U0001F1E0-\U0001F1FF\U00002700-\U000027BF\U0001F900-\U0001F9FF"
        r"\U0001FA70-\U0001FAFF\U00002600-\U000026FF]+",
        re.UNICODE,
    )

    def __init__(self, mode: EmojiHandlingMode = EmojiHandlingMode.PRESERVE):
        self.mode = mode

    def process_emoji(self, text: str) -> str:
        """Process emoji according to the configured mode."""
        if self.mode == EmojiHandlingMode.PRESERVE:
            return text
        elif self.mode == EmojiHandlingMode.REMOVE:
            return self.EMOJI_PATTERN.sub("", text)
        elif self.mode == EmojiHandlingMode.NORMALIZE:
            return self._normalize_emoji(text)
        elif self.mode == EmojiHandlingMode.CONVERT_TO_TEXT:
            return self._convert_emoji_to_text(text)
        else:
            return text

    def _normalize_emoji(self, text: str) -> str:
        """Normalize emoji to consistent representations."""
        # Replace multiple consecutive emoji with single instances
        normalized = re.sub(r"([\U0001F600-\U0001F64F])\1+", r"\1", text)
        normalized = re.sub(r"([\U0001F300-\U0001F5FF])\1+", r"\1", normalized)
        return normalized

    def _convert_emoji_to_text(self, text: str) -> str:
        """Convert emoji to text representations."""
        converted = text
        for emoji, text_repr in self.MUSIC_EMOJI_MAP.items():
            converted = converted.replace(emoji, text_repr)

        # Remove any remaining emoji
        converted = self.EMOJI_PATTERN.sub(" ", converted)
        return converted

    def count_emoji(self, text: str) -> int:
        """Count emoji characters in text."""
        return len(self.EMOJI_PATTERN.findall(text))

    def extract_emoji(self, text: str) -> List[str]:
        """Extract all emoji from text."""
        return self.EMOJI_PATTERN.findall(text)

    def has_music_emoji(self, text: str) -> bool:
        """Check if text contains music - related emoji."""
        for emoji in self.MUSIC_EMOJI_MAP.keys():
            if emoji in text:
                return True
        return False


class TransformerTextProcessor:
    """Transformer - ready text processing with music domain awareness."""

    def __init__(self, model_name: str = "distilbert - base - uncased", config: Optional[TextProcessingConfig] = None):
        self.model_name = model_name
        self.config = config or TextProcessingConfig()

        # Initialize components
        self.slang_preserver = MusicSlangPreserver(self.config.slang_preservation)
        self.emoji_handler = EmojiHandler(self.config.emoji_mode)

        # Initialize tokenizer if available
        self.tokenizer = None
        if TRANSFORMERS_AVAILABLE:
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            except Exception as e:
                print(f"⚠️  Could not load tokenizer for {model_name}: {e}")

    def preprocess_text(self, text: str) -> str:
        """Comprehensive text preprocessing for transformers."""
        processed = text.strip()

        # Step 1: Preserve music slang
        slang_replacements = {}
        if self.config.slang_preservation != SlangPreservationLevel.NONE:
            processed, slang_replacements = self.slang_preserver.preserve_slang_in_text(processed)

        # Step 2: Handle emoji
        processed = self.emoji_handler.process_emoji(processed)

        # Step 3: Handle mentions and hashtags
        if self.config.handle_mentions:
            processed = re.sub(r"@\w+", "[USER]", processed)

        if self.config.handle_hashtags:
            processed = re.sub(r"#\w+", "[HASHTAG]", processed)

        # Step 4: Unicode normalization
        if self.config.normalize_unicode:
            processed = unicodedata.normalize("NFKC", processed)

        # Step 5: Basic text cleaning
        if self.config.remove_extra_whitespace:
            processed = re.sub(r"\s+", " ", processed)

        # Step 6: Case handling (after slang preservation)
        if self.config.lowercase and not self.config.preserve_case_for_slang:
            processed = processed.lower()

        # Step 7: Restore preserved slang
        if slang_replacements:
            processed = self.slang_preserver.restore_slang_in_text(processed, slang_replacements)

        # Step 8: Final cleanup
        processed = processed.strip()

        # Step 9: Length truncation if specified
        if self.config.max_length and len(processed) > self.config.max_length:
            processed = processed[: self.config.max_length].rsplit(" ", 1)[0]  # Truncate at word boundary

        return processed

    def tokenize_for_transformer(self, text: str) -> Dict[str, List[int]]:
        """Tokenize text for transformer models."""
        if not self.tokenizer:
            raise RuntimeError("Tokenizer not available. Install transformers library.")

        # Preprocess text first
        processed_text_item = self.preprocess_text(text)

        # Tokenize
        tokens = self.tokenizer(
            processed_text,
            add_special_tokens=self.config.add_special_tokens,
            max_length=self.config.max_length or 512,
            truncation=True,
            padding="max_length",
            return_attention_mask=True,
            return_tensors=None,  # Return lists, not tensors
        )

        return tokens

    def batch_preprocess(self, texts: List[str]) -> List[str]:
        """Preprocess a batch of texts."""
        return [self.preprocess_text(text) for text_item in texts]

    def batch_tokenize(self, texts: List[str]) -> Dict[str, List[List[int]]]:
        """Tokenize a batch of texts."""
        if not self.tokenizer:
            raise RuntimeError("Tokenizer not available. Install transformers library.")

        # Preprocess all texts first
        processed_texts = self.batch_preprocess(texts)

        # Batch tokenize
        tokens = self.tokenizer(
            processed_texts,
            add_special_tokens=self.config.add_special_tokens,
            max_length=self.config.max_length or 512,
            truncation=True,
            padding="max_length",
            return_attention_mask=True,
            return_tensors=None,  # Return lists, not tensors
        )

        return tokens

    def analyze_text_features(self, text: str) -> Dict[str, any]:
        """Analyze text features for ML processing."""
        processed = self.preprocess_text(text)

        # Basic features
        features = {
            "original_length": len(text),
            "processed_length": len(processed),
            "word_count": len(processed.split()),
            "char_count": len(processed),
            "avg_word_length": sum(len(word) for word in processed.split()) / max(len(processed.split()), 1),
        }

        # Music - specific features
        slang_terms = self.slang_preserver.identify_slang_terms(text)
        features.update(
            {
                "slang_count": len(slang_terms),
                "slang_terms": [term["canonical"] for term in slang_terms],
                "positive_slang_count": len([t for t in slang_terms if t["sentiment"] == "positive"]),
                "negative_slang_count": len([t for t in slang_terms if t["sentiment"] == "negative"]),
            }
        )

        # Emoji features
        features.update(
            {
                "emoji_count": self.emoji_handler.count_emoji(text),
                "has_music_emoji": self.emoji_handler.has_music_emoji(text),
                "emoji_list": self.emoji_handler.extract_emoji(text),
            }
        )

        # Text quality features
        features.update(
            {
                "has_mentions": "@" in text,
                "has_hashtags": "#" in text,
                "has_urls": "http" in text.lower(),
                "all_caps_ratio": sum(1 for c in text if c.isupper()) / max(len(text), 1),
                "punctuation_ratio": sum(1 for c in text if not c.isalnum() and not c.isspace()) / max(len(text), 1),
            }
        )

        return features


# Convenience functions
def create_music_text_processor(
    model_name: str = "distilbert - base - uncased",
    slang_preservation: SlangPreservationLevel = SlangPreservationLevel.COMPREHENSIVE,
    emoji_mode: EmojiHandlingMode = EmojiHandlingMode.PRESERVE,
) -> TransformerTextProcessor:
    """Create a configured text processor for music domain."""
    config = TextProcessingConfig(
        slang_preservation=slang_preservation,
        emoji_mode=emoji_mode,
        preserve_case_for_slang=True,
        handle_mentions=True,
        handle_hashtags=True,
    )

    return TransformerTextProcessor(model_name, config)


def quick_preprocess_for_music(text: str, preserve_slang: bool = True) -> str:
    """Quick preprocessing for music domain text."""
    processor = create_music_text_processor(
        slang_preservation=SlangPreservationLevel.COMPREHENSIVE if preserve_slang else SlangPreservationLevel.NONE
    )
    return processor.preprocess_text(text)


if __name__ == "__main__":
    # Demo the text processing helpers
    print("🎵 MUSIC TEXT PROCESSING HELPERS DEMO")
    print("=" * 50)

    # Sample music comments
    sample_texts = [
        "This song absolutely SLAPS! 🔥🔥 No cap, it's GOATED fr",
        "The beat goes hard but the vocals are mid tbh 😭",
        "PERIODT! This is straight fire, my queen ate and left no crumbs ✨",
        "@artist please drop the full album ASAP! 🙏",
        "This ain't it chief... kinda cringe ngl",
    ]

    # Test different processing configurations
    configs = [
        ("Basic Processing", SlangPreservationLevel.BASIC, EmojiHandlingMode.PRESERVE),
        ("Comprehensive + Emoji to Text", SlangPreservationLevel.COMPREHENSIVE, EmojiHandlingMode.CONVERT_TO_TEXT),
        ("No Slang Preservation", SlangPreservationLevel.NONE, EmojiHandlingMode.REMOVE),
    ]

    for config_name, slang_level, emoji_mode in configs:
        print(f"\n📋 {config_name}")
        print("-" * 30)

        processor = create_music_text_processor(slang_preservation=slang_level, emoji_mode=emoji_mode)

        for text_item in sample_texts[:2]:  # Show first 2 examples
            processed = processor.preprocess_text(text)
            features = processor.analyze_text_features(text)

            print(f"Original:  {text}")
            print(f"Processed: {processed}")
            print(f"Features:  {features['slang_count']} slang, {features['emoji_count']} emoji")
            print()

    print("✅ Text processing helpers demo complete!")
