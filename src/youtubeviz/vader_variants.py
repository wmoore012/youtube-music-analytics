#!/usr/bin/env python3
"""
VADER Enhancement Variants for Music Domain

Implements multiple VADER enhancement approaches using VADER's official extension points:
- Lexicon: word / phrase → valence on −4…+4 scale
- SPECIAL_CASE_IDIOMS: multi-word phrases scored as units
- BOOSTER_DICT: intensifiers / dampeners using VADER's B_INCR = 0.293

Based on expert analysis of music YouTube comments and VADER's documented extension patterns.
"""

import logging
import re

logger = logging.getLogger(__name__)
from enum import Enum
from hashlib import md5
from typing import Dict, Tuple

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    logger.warning("VADER not available—install with: pip install vaderSentiment")


class VariantType(Enum):
    """Types of VADER enhancement variants."""

    MINIMAL = "minimal"
    MODERATE = "moderate"
    COMPREHENSIVE = "comprehensive"
    AGGRESSIVE = "aggressive"
    HYBRID = "hybrid"


class VADERVariantManager:
    """Manages multiple VADER enhancement variants for comparative evaluation."""

    def __init__(self):
        if not VADER_AVAILABLE:
            raise ImportError("VADER not available-install with: pip install vaderSentiment")

    def create_variant(self, variant_type: VariantType) -> SentimentIntensityAnalyzer:
        """Create a VADER variant with specified enhancement level."""

        analyzer = SentimentIntensityAnalyzer()

        if variant_type == VariantType.MINIMAL:
            self._apply_minimal_enhancements(analyzer)
        elif variant_type == VariantType.MODERATE:
            self._apply_moderate_enhancements(analyzer)
        elif variant_type == VariantType.COMPREHENSIVE:
            self._apply_comprehensive_enhancements(analyzer)
        elif variant_type == VariantType.AGGRESSIVE:
            self._apply_aggressive_enhancements(analyzer)
        elif variant_type == VariantType.HYBRID:
            self._apply_hybrid_enhancements(analyzer)

        return analyzer

    def get_all_variants(self) -> Dict[str, SentimentIntensityAnalyzer]:
        """Get all VADER variants for comparison."""

        variants = {}

        # Stock VADER (baseline)
        variants["stock_vader"] = SentimentIntensityAnalyzer()

        # Enhanced variants
        for variant_type in VariantType:
            variants[f"enhanced_{variant_type.value}"] = self.create_variant(variant_type)

        return variants

    def _apply_minimal_enhancements(self, analyzer: SentimentIntensityAnalyzer) -> None:
        """Apply minimal, high-confidence enhancements only."""

        # Only the most obvious music slang terms
        analyzer.lexicon.update(
            {
                "slaps": 2.6,
                "banger": 2.7,
                "fire": 2.7,
                "goated": 2.9,
                "mid": -1.8,
                "cringe": -1.8,
            }
        )

        # Essential boosters-modify the module-level BOOSTER_DICT
        import vaderSentiment.vaderSentiment as vader

        B_INCR = 0.293  # VADER's official booster increment
        vader.BOOSTER_DICT.update(
            {
                "no_cap": B_INCR,
                "fr": B_INCR,
                "frfr": B_INCR,
            }
        )

    def _apply_moderate_enhancements(self, analyzer: SentimentIntensityAnalyzer) -> None:
        """Apply balanced enhancements with curated terms."""

        # Core music slang + Gen Z terms
        analyzer.lexicon.update(
            {
                # Music slang (positive)
                "slaps": 2.6,
                "banger": 2.7,
                "bop": 2.4,
                "goated": 2.9,
                "bussin": 2.6,
                "fire": 2.7,
                "iconic": 2.1,
                "anthem": 2.1,
                "goes_hard": 2.6,
                "hits_different": 2.2,
                "chef's_kiss": 2.6,
                # Production terms
                "mix_is_clean": 2.0,
                "production_is_clean": 2.0,
                # Gen Z positive
                "ate": 2.3,
                "served": 2.0,
                "devoured": 2.3,
                "periodt": 2.0,
                # Negative terms
                "mid": -1.8,
                "flop": -2.4,
                "cringe": -1.8,
                "overrated": -1.8,
                "fell_off": -2.0,
                "this_ain't_it": -2.4,
                # Emoji (key ones)
                "🔥": 2.7,
                "💯": 2.6,
                "👑": 2.2,
                "😭": 1.2,
                "💀": 1.2,
            }
        )

        # Modern boosters-modify the module-level BOOSTER_DICT
        import vaderSentiment.vaderSentiment as vader

        B_INCR = 0.293
        vader.BOOSTER_DICT.update(
            {
                "no_cap": B_INCR,
                "fr": B_INCR,
                "frfr": B_INCR,
                "af": B_INCR,
                "asf": B_INCR,
                "deadass": B_INCR,
                "lowkey": B_INCR * 0.7,  # Mild intensifier
                "highkey": B_INCR,
            }
        )

        # Key multi-word idioms-add to lexicon as underscored phrases
        analyzer.lexicon.update(
            {
                "this_is_sick": 2.3,
                "no_cap_this_slaps": 3.0,
                "im_obsessed": 2.0,
                "the_vocals_are_insane": 2.0,
                "vocals_are_insane": 2.0,
            }
        )

    def _apply_comprehensive_enhancements(self, analyzer: SentimentIntensityAnalyzer) -> None:
        """Apply comprehensive enhancements from expert feedback."""

        # Full music domain lexicon
        analyzer.lexicon.update(
            {
                # Positive music slang
                "slaps": 2.6,
                "banger": 2.7,
                "bop": 2.4,
                "goated": 2.9,
                "bussin": 2.6,
                "fire": 2.7,
                "gas": 2.7,  # Gas = fire / good, often with ⛽️
                "iconic": 2.1,
                "anthem": 2.1,
                "goes_hard": 2.6,
                "hits_different": 2.2,
                "chef's_kiss": 2.6,
                "no_skips": 2.8,
                "on_repeat": 2.5,
                "sick": 2.3,
                "dopest": 2.4,  # "dopest artists out"
                "dope": 2.2,
                # Production & performance
                "mix_is_clean": 2.0,
                "production_is_clean": 2.0,
                "insane": 2.0,  # In music context
                "crazy": 1.8,  # In music context
                "obsessed": 2.0,  # Override VADER's negative
                # Gen Z expressions
                "ate": 2.3,
                "left_no_crumbs": 2.6,
                "ate_and_left_no_crumbs": 3.0,
                "served": 2.0,
                "devoured": 2.3,
                "periodt": 2.0,
                "slay": 2.2,
                "understood_the_assignment": 2.4,
                "snapped": 2.1,
                # Cultural expressions
                "queen": 1.8,
                "king": 1.8,
                "mother": 1.5,
                "bestie": 1.2,
                "my_son": 1.6,  # "get my son on trending"
                "relate": 1.8,  # "I relate to this so much"
                "balance": 1.4,  # "perfect balance"
                "vintage_voice": 2.0,  # "vintage voice"
                "modern_beauty": 1.8,  # "modern beauty"
                # Negative terms
                "mid": -1.8,
                "flop": -2.4,
                "cringe": -1.8,
                "trash": -2.2,
                "overrated": -1.8,
                "fell_off": -2.0,
                "this_ain't_it": -2.4,
                "overproduced": -1.4,
                "mix_is_muddy": -1.8,
                "too_much_autotune": -1.4,
                "industry_plant": -1.8,
                "no_replay_value": -2.0,
                # Emoji
                "🔥": 2.7,
                "💯": 2.6,
                "👑": 2.2,
                "🎶": 1.5,
                "🎧": 1.2,
                "😭": 1.2,
                "💀": 1.2,
                "🥵": 1.4,
                "😍": 2.0,
                "⛽️": 2.7,  # Gas emoji = fire / good
                "❤️": 1.8,  # Heart emoji for "I relate to this so much ❤️"
            }
        )

        # Full booster set-modify the module-level BOOSTER_DICT
        import vaderSentiment.vaderSentiment as vader

        B_INCR = 0.293
        vader.BOOSTER_DICT.update(
            {
                "no_cap": B_INCR,
                "fr": B_INCR,
                "frfr": B_INCR,
                "af": B_INCR,
                "asf": B_INCR,
                "deadass": B_INCR,
                "lowkey": B_INCR * 0.7,
                "highkey": B_INCR,
                "literally": B_INCR * 0.8,
                "actually": B_INCR * 0.6,
            }
        )

        # Comprehensive idioms-add to lexicon as underscored phrases
        analyzer.lexicon.update(
            {
                "this_is_sick": 2.3,
                "this_slaps": 2.6,
                "straight_fire": 2.7,
                "fuck_it_up": 2.1,
                "bad_bish": 2.0,
                "go_off": 2.0,
                "no_cap_this_slaps": 3.0,
                "im_obsessed": 2.0,
                "lowkey_fire": 2.0,
                "highkey_obsessed": 2.2,
                "the_vocals_are_insane": 2.0,
                "vocals_are_insane": 2.0,
                "this_song_is_unmatched": 2.2,
                "the_way_i_screamed": 1.8,
                "bitch_its_giving": 2.4,
                "its_giving": 1.6,
                "left_no_crumbs": 2.6,
                "ate_and_left_no_crumbs": 3.0,
                "no_skips": 2.8,
                "on_repeat": 2.5,
                "goes_hard": 2.6,
                "mix_is_clean": 2.0,
                "production_is_clean": 2.0,
                "this_aint_it": -2.4,
                "fell_off": -2.0,
                "mix_is_muddy": -1.8,
                # New phrases based on your feedback
                "get_my_son_on_trending": 2.4,  # Playful support
                "i_relate_to_this": 2.2,  # Personal connection
                "dopest_artists_out": 2.6,  # High praise
                "modern_beauty": 2.0,  # Sophisticated praise
                "vintage_voice": 2.2,  # Vocal appreciation
                "perfect_balance": 2.0,  # Artistic appreciation
                "why_not_on_spotify": 1.8,  # Desire for access (positive)
                "where_can_i_listen": 1.8,  # Seeking behavior (positive)
                "the_outfits": 1.6,  # Visual appreciation
            }
        )

    def _apply_aggressive_enhancements(self, analyzer: SentimentIntensityAnalyzer) -> None:
        """Apply aggressive enhancements with experimental weights."""

        # Start with comprehensive
        self._apply_comprehensive_enhancements(analyzer)

        # Boost weights for music context
        music_boost_terms = ["slaps", "banger", "goated", "fire", "sick", "obsessed"]
        for term in music_boost_terms:
            if term in analyzer.lexicon:
                analyzer.lexicon[term] = min(analyzer.lexicon[term] * 1.2, 4.0)

        # Add more experimental terms
        analyzer.lexicon.update(
            {
                "vibes": 1.8,
                "vibe": 1.5,
                "energy": 1.6,
                "mood": 1.4,
                "talent": 2.0,
                "gifted": 2.2,
                "skilled": 1.8,
                "masterpiece": 2.8,
                "perfection": 2.6,
                "flawless": 2.4,
                "addicted": 1.8,
                "hooked": 1.6,
                "obsession": 1.8,
            }
        )

        # Stronger boosters-modify the module-level BOOSTER_DICT
        import vaderSentiment.vaderSentiment as vader

        B_INCR = 0.293
        vader.BOOSTER_DICT.update(
            {
                "absolutely": B_INCR * 1.2,
                "completely": B_INCR * 1.1,
                "totally": B_INCR * 1.1,
                "genuinely": B_INCR * 0.9,
            }
        )

    def _apply_hybrid_enhancements(self, analyzer: SentimentIntensityAnalyzer) -> None:
        """Apply hybrid approach combining rule-based and contextual adjustments."""

        # Start with moderate base
        self._apply_moderate_enhancements(analyzer)

        # Add context-sensitive terms (positive in music, negative elsewhere)
        analyzer.lexicon.update(
            {
                "sick": 2.3,  # Override VADER's negative in music context
                "insane": 2.0,  # Override VADER's negative in music context
                "crazy": 1.8,  # Override VADER's negative in music context
                "mad": 1.5,  # "mad good" in music context
                "wild": 1.6,  # "this is wild" in music context
            }
        )

        # Cultural sensitivity adjustments
        analyzer.lexicon.update(
            {
                "bitch": 0.0,  # Neutralize-context dependent
                "shit": 0.0,  # Neutralize - "this shit slaps"
                "damn": 0.0,  # Neutralize - "damn this is good"
            }
        )


class MusicVADERNormalizer:
    """Normalizes text for music-domain VADER analysis."""

    def __init__(self):
        # Multi-word phrase patterns to join before VADER processing
        self.phrase_patterns = [
            (re.compile(r"\bthis\s+is\s+sick\b", re.I), "this_is_sick"),
            (re.compile(r"\bthis\s+slaps\b", re.I), "this_slaps"),
            (re.compile(r"\bstraight\s+fire\b", re.I), "straight_fire"),
            (re.compile(r"\bfuck\s+it\s+up\b", re.I), "fuck_it_up"),
            (re.compile(r"\bbad\s+bish\b", re.I), "bad_bish"),
            (re.compile(r"\bgo\s+off\b", re.I), "go_off"),
            (re.compile(r"\bno\s+cap\b", re.I), "no_cap"),
            (re.compile(r"\bno\s+cap\s+this\s+slaps\b", re.I), "no_cap_this_slaps"),
            (re.compile(r"\bi'?m\s+obsessed\b", re.I), "im_obsessed"),
            (re.compile(r"\blowkey\s+fire\b", re.I), "lowkey_fire"),
            (re.compile(r"\bhighkey\s+obsessed\b", re.I), "highkey_obsessed"),
            (re.compile(r"\b(the\s+)?vocals\s+are\s+insane\b", re.I), "vocals_are_insane"),
            (re.compile(r"\bthis\s+song\s+is\s+unmatched\b", re.I), "this_song_is_unmatched"),
            (re.compile(r"\bthe\s+way\s+i\s+screamed\b", re.I), "the_way_i_screamed"),
            (re.compile(r"\bit'?s\s+giv(?:in + g|in')\b", re.I), "its_giving"),
            (re.compile(r"\bbitch[,!]?\s+it'?s\s+giv(?:in + g|in')\b", re.I), "bitch_its_giving"),
            (re.compile(r"\bleft\s+no\s+crumbs\b", re.I), "left_no_crumbs"),
            (re.compile(r"\bate\s+and\s+left\s+no\s+crumbs\b", re.I), "ate_and_left_no_crumbs"),
            (re.compile(r"\bno\s+skips\b", re.I), "no_skips"),
            (re.compile(r"\bon\s+repeat\b", re.I), "on_repeat"),
            (re.compile(r"\bgoes\s+hard\b", re.I), "goes_hard"),
            (re.compile(r"\bmix\s+is\s+clean\b", re.I), "mix_is_clean"),
            (re.compile(r"\bproduction\s+is\s+clean\b", re.I), "production_is_clean"),
            (re.compile(r"\bthis\s+ain'?t\s+it\b", re.I), "this_aint_it"),
            (re.compile(r"\bfell\s+off\b", re.I), "fell_off"),
            (re.compile(r"\bmix\s+is\s+muddy\b", re.I), "mix_is_muddy"),
            # New patterns based on your feedback
            (re.compile(r"\bget\s+(my\s+)?son\s+on\s+trending\b", re.I), "get_my_son_on_trending"),
            (re.compile(r"\bi\s+relate\s+to\s+this\b", re.I), "i_relate_to_this"),
            (re.compile(r"\bdopest\s+artists?\s+out\b", re.I), "dopest_artists_out"),
            (re.compile(r"\bmodern\s+beauty\b", re.I), "modern_beauty"),
            (re.compile(r"\bvintage\s+voice\b", re.I), "vintage_voice"),
            (re.compile(r"\bperfect\s+balance\b", re.I), "perfect_balance"),
            (re.compile(r"\bwhy\s+.*not\s+on\s+spotify\b", re.I), "why_not_on_spotify"),
            (re.compile(r"\bwhere\s + can\s + i\s + listen\b", re.I), "where_can_i_listen"),
            (re.compile(r"\bthe\s + outfits?\b", re.I), "the_outfits"),
        ]

    def normalize_for_vader(self, text: str) -> str:
        """Normalize text for VADER processing."""

        # Handle elongated words (givinnnng -> giving)
        text = re.sub(r"\bgivin + n+g\b", "giving", text, flags=re.I)
        text = re.sub(r"\bslaaaaaps\b", "slaps", text, flags=re.I)
        text = re.sub(r"\bfiiiire\b", "fire", text, flags=re.I)

        # Apply phrase joining
        for pattern, replacement in self.phrase_patterns:
            text = pattern.sub(replacement, text)

        return text


def get_patch_id(variant_type: VariantType) -> str:
    """Generate patch ID for traceability."""

    # Create hash of variant configuration for reproducibility
    config_str = f"{variant_type.value}_v1.0"
    return md5(config_str.encode()).hexdigest()[:8]


def create_music_vader(
    variant_type: VariantType = VariantType.COMPREHENSIVE,
) -> Tuple[SentimentIntensityAnalyzer, MusicVADERNormalizer, str]:
    """
    Create music-enhanced VADER analyzer with normalizer.

    Returns:
        Tuple of (analyzer, normalizer, patch_id)
    """

    if not VADER_AVAILABLE:
        raise ImportError("VADER not available-install with: pip install vaderSentiment")

    manager = VADERVariantManager()
    analyzer = manager.create_variant(variant_type)
    normalizer = MusicVADERNormalizer()
    patch_id = get_patch_id(variant_type)

    return analyzer, normalizer, patch_id


def score_with_music_vader(text: str, variant_type: VariantType = VariantType.COMPREHENSIVE) -> Dict:
    """Score text with music-enhanced VADER."""

    analyzer, normalizer, patch_id = create_music_vader(variant_type)
    normalized_text = normalizer.normalize_for_vader(text)
    scores = analyzer.polarity_scores(normalized_text)

    # Add metadata
    scores["patch_id"] = patch_id
    scores["variant_type"] = variant_type.value
    scores["normalized_text"] = normalized_text

    return scores


# Convenience functions for testing
def test_all_variants_on_phrase(phrase: str) -> Dict[str, Dict]:
    """Test a phrase against all VADER variants."""

    if not VADER_AVAILABLE:
        return {"error": "VADER not available"}

    results = {}
    manager = VADERVariantManager()
    normalizer = MusicVADERNormalizer()

    # Stock VADER
    stock = SentimentIntensityAnalyzer()
    results["stock"] = stock.polarity_scores(phrase)

    # Enhanced variants
    for variant_type in VariantType:
        analyzer = manager.create_variant(variant_type)
        normalized = normalizer.normalize_for_vader(phrase)
        scores = analyzer.polarity_scores(normalized)
        scores["normalized_text"] = normalized
        results[f"enhanced_{variant_type.value}"] = scores

    return results


if __name__ == "__main__":
    # Quick test
    test_phrases = [
        "this is sick",
        "I'm obsessed",
        "no cap this slaps",
        "the vocals are insane",
        "mid",
        "this ain't it chief",
    ]

    print("🧪 TESTING VADER VARIANTS")
    print("=" * 50)

    for phrase in test_phrases:
        print(f"\n📝 '{phrase}':")
        results = test_all_variants_on_phrase(phrase)

        if "error" in results:
            print(f"   ❌ {results['error']}")
            continue

        for variant, scores in results.items():
            compound = scores["compound"]
            sentiment = "POS" if compound >= 0.05 else "NEG" if compound <= -0.05 else "NEU"
            print(f"   {variant:20} | {sentiment} | {compound:+.3f}")
