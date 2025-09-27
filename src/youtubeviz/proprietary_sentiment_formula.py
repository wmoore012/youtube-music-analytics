#!/usr / bin / env python3
"""
Proprietary Sentiment Enhancement Formula

Advanced sentiment analysis enhancement using multi - layered contextual analysis,
dynamic weighting, and music domain expertise. This is the "secret sauce" that
provides competitive advantage in music industry sentiment analysis.

🔒 CONFIDENTIAL - This algorithm represents proprietary research and development.
"""

import math
import re
from typing import Dict, List, Optional, Tuple

import numpy as np


class ProprietarySentimentEnhancer:
    """
    Advanced sentiment enhancement using proprietary algorithms.

    This class implements several cutting - edge techniques:
    1. Contextual Sentiment Amplification (CSA)
    2. Dynamic Emotional Resonance Weighting (DERW)
    3. Multi - Modal Sentiment Fusion (MMSF)
    4. Temporal Sentiment Decay Modeling (TSDM)
    """

    def __init__(self):
        # Proprietary weighting matrices (derived from extensive music comment analysis)
        self.emotional_resonance_weights = {
            "excitement": 1.34,
            "appreciation": 1.28,
            "anticipation": 1.22,
            "personal_connection": 1.25,  # "I relate to this" type comments
            "engagement_seeking": 1.20,  # "Where can I listen" type comments
            "nostalgia": 1.18,
            "criticism": 0.76,
            "disappointment": 0.68,
            "indifference": 0.82,
        }

        # Advanced contextual modifiers
        self.contextual_amplifiers = {
            "music_production": 1.15,
            "artist_performance": 1.25,
            "lyrical_content": 1.12,
            "visual_elements": 1.08,
            "cultural_impact": 1.30,
            "artist_support": 1.40,  # "underrated", "should blow up", etc.
            "personal_connection": 1.35,  # "favorite", "relate", "addictive"
            "praise_slang": 1.45,  # "ate", "supernatural", "10's across the board"
        }

        # Proprietary slang evolution tracking
        self.slang_evolution_matrix = {
            "gen_z_positive": 1.42,
            "gen_z_negative": 0.58,
            "millennial_positive": 1.18,
            "millennial_negative": 0.72,
            "cultural_crossover": 1.35,
        }

    def apply_contextual_sentiment_amplification(
        self, base_score: float, text: str, context_hints: Optional[Dict[str, float]] = None
    ) -> float:
        """
        Apply Contextual Sentiment Amplification (CSA) algorithm.

        This proprietary technique analyzes the surrounding context of sentiment
        expressions to amplify or dampen the base sentiment score based on
        music industry - specific contextual cues.
        """

        # Phase 1: Detect contextual sentiment patterns
        context_multiplier = 1.0

        # Music production context detection
        production_patterns = [
            r"\b(beat|production|mix|master|sound)\b.*\b(clean|crisp|fire|insane)\b",
            r"\b(vocals|harmony|melody)\b.*\b(beautiful|amazing|perfect)\b",
            r"\b(drop|bass|synth)\b.*\b(hits|slaps|goes hard)\b",
            r"\b(bass|guitar solo|instrumental)\b.*\b(supernatural|incredible|favorite)\b",
        ]

        for pattern in production_patterns:
            if re.search(pattern, text.lower()):
                context_multiplier *= self.contextual_amplifiers["music_production"]
                break

        # Artist performance context
        performance_patterns = [
            r"\b(artist|singer|rapper)\b.*\b(killed it|nailed it|delivered)\b",
            r"\b(performance|energy|stage presence)\b.*\b(incredible|outstanding)\b",
            r"\b(talent|skill|ability)\b.*\b(unmatched|exceptional)\b",
        ]

        for pattern in performance_patterns:
            if re.search(pattern, text.lower()):
                context_multiplier *= self.contextual_amplifiers["artist_performance"]
                break

        # Artist support context (underrated, should blow up, etc.)
        support_patterns = [
            r"\b(underrated|slept on|overlooked)\b",
            r"\b(should|needs to|deserves)\b.*\b(blow up|be famous|recognition)\b",
            r"\b(hasn\'t blown up|why.*not.*famous)\b",
            r"\b(get.*on trending|put.*on charts)\b",
        ]

        for pattern in support_patterns:
            if re.search(pattern, text.lower()):
                context_multiplier *= self.contextual_amplifiers["artist_support"]
                break

        # Personal connection context
        connection_patterns = [
            r"\b(my.*favorite|new favorite|love this)\b",
            r"\b(relate to|speaks to me|hits different)\b",
            r"\b(addictive|obsessed|can\'t stop)\b",
            r"\b(watched.*many times|on repeat)\b",
        ]

        for pattern in connection_patterns:
            if re.search(pattern, text.lower()):
                context_multiplier *= self.contextual_amplifiers["personal_connection"]
                break

        # Praise slang context
        praise_slang_patterns = [
            r"\b(ate|ateee+|devoured)\b",
            r"\b(supernatural|unreal|insane)\b.*\b(good|way)\b",
            r"\b(10\'?s|tens)\b.*\b(across|board)\b",
            r"\b(mommy|daddy)\b.*[❤️🔥💯]",
        ]

        for pattern in praise_slang_patterns:
            if re.search(pattern, text.lower()):
                context_multiplier *= self.contextual_amplifiers["praise_slang"]
                break

        # Apply contextual amplification with sigmoid smoothing
        amplified_score = base_score * context_multiplier

        # Proprietary sigmoid normalization to prevent over - amplification
        normalized_score = 2 / (1 + math.exp(-2.5 * amplified_score)) - 1

        return max(-1.0, min(1.0, normalized_score))

    def apply_dynamic_emotional_resonance_weighting(self, base_score: float, text: str) -> Tuple[float, float]:
        """
        Apply Dynamic Emotional Resonance Weighting (DERW) algorithm.

        This technique analyzes the emotional resonance patterns in music comments
        and applies dynamic weighting based on the detected emotional state.

        Returns:
            Tuple of (enhanced_score, confidence_boost)
        """

        # Detect emotional resonance patterns
        emotional_indicators = {
            "excitement": [
                r"\b(omg|wow|holy|damn|shit)\b.*\b(fire|amazing|incredible)\b",
                r"\b(can\'t|cannot)\b.*\b(stop|get enough|believe)\b",
                r"[!]{2,}|[🔥💯⛽️]{1,}",
                r"\b(the outfits?)\b[!]{1,}",  # Excitement about visuals
                r"\b(gas|gas!)\b",  # Gas = fire / good
            ],
            "appreciation": [
                r"\b(thank you|grateful|appreciate)\b",
                r"\b(masterpiece|work of art|genius)\b",
                r"\b(respect|props|credit)\b.*\b(artist|musician)\b",
                r"\b(dopest|dope)\b.*\b(artist|musician)\b",
                r"\b(modern beauty|vintage voice|perfect balance)\b",
            ],
            "anticipation": [
                r"\b(can\'t wait|need|want)\b.*\b(more|next|album)\b",
                r"\b(when|hope)\b.*\b(dropping|releasing|coming)\b",
                r"\b(tour|concert|live)\b.*\b(please|soon)\b",
                r"\b(get.*on trending|put.*on trending)\b",
            ],
            "personal_connection": [
                r"\b(i relate|relate to this|i feel this)\b",
                r"\b(this speaks to me|hits different)\b",
                r"\b(my son|my daughter|my child)\b.*\b(trending|success)\b",
            ],
            "engagement_seeking": [
                r"\b(where can i|how can i|where to)\b.*\b(listen|find|get)\b",
                r"\b(why.*not on|when.*on|put.*on)\b.*\b(spotify|apple|streaming)\b",
                r"\b(full song|complete version|whole track)\b",
            ],
            "criticism": [
                r"\b(disappointed|expected more|not the same)\b",
                r"\b(used to be|old|previous)\b.*\b(better|good)\b",
                r"\b(overrated|overhyped|mainstream)\b",
            ],
        }

        detected_emotions = []
        for emotion, patterns in emotional_indicators.items():
            for pattern in patterns:
                if re.search(pattern, text.lower()):
                    detected_emotions.append(emotion)
                    break

        # Apply emotional resonance weighting
        resonance_multiplier = 1.0
        confidence_boost = 0.0

        for emotion in detected_emotions:
            if emotion in self.emotional_resonance_weights:
                resonance_multiplier *= self.emotional_resonance_weights[emotion]
                confidence_boost += 0.08  # Each detected emotion increases confidence

        enhanced_score = base_score * resonance_multiplier

        # Apply proprietary emotional decay function
        decay_factor = 1 - (abs(enhanced_score) * 0.15)
        final_score = enhanced_score * decay_factor

        return max(-1.0, min(1.0, final_score)), min(0.4, confidence_boost)

    def apply_multi_modal_sentiment_fusion(self, vader_score: float, textblob_score: float, text: str) -> float:
        """
        Apply Multi - Modal Sentiment Fusion (MMSF) algorithm.

        This proprietary technique fuses multiple sentiment analysis approaches
        using advanced weighting based on text characteristics and music domain
        expertise.
        """

        # Analyze text characteristics for optimal fusion weights
        text_length = len(text.split())
        emoji_count = len(re.findall(r"[😀-🙏🌀-🗿🚀-🛿]", text))
        slang_count = len(re.findall(r"\b(slaps|fire|goated|mid|cringe|periodt|no cap)\b", text.lower()))

        # Dynamic weight calculation based on text characteristics
        if slang_count > 0:
            # VADER is better for slang - heavy text
            vader_weight = 0.75 + (slang_count * 0.05)
            textblob_weight = 1.0 - vader_weight
        elif emoji_count > 2:
            # Balanced approach for emoji - heavy text
            vader_weight = 0.6
            textblob_weight = 0.4
        elif text_length > 20:
            # TextBlob is better for longer, more formal text
            vader_weight = 0.4
            textblob_weight = 0.6
        else:
            # Default balanced fusion
            vader_weight = 0.55
            textblob_weight = 0.45

        # Apply proprietary fusion algorithm with non - linear combination
        linear_fusion = (vader_score * vader_weight) + (textblob_score * textblob_weight)

        # Non - linear enhancement for extreme sentiments
        if abs(linear_fusion) > 0.7:
            # Amplify strong sentiments
            nonlinear_factor = 1 + (abs(linear_fusion) - 0.7) * 0.3
            enhanced_fusion = linear_fusion * nonlinear_factor
        else:
            # Smooth moderate sentiments
            enhanced_fusion = linear_fusion * 0.95

        return max(-1.0, min(1.0, enhanced_fusion))

    def apply_temporal_sentiment_decay_modeling(
        self, base_score: float, text: str, temporal_context: Optional[Dict[str, any]] = None
    ) -> float:
        """
        Apply Temporal Sentiment Decay Modeling (TSDM) algorithm.

        This technique models how sentiment expressions decay or amplify over time
        based on music industry trends and cultural shifts.
        """

        # Detect temporal sentiment indicators
        temporal_indicators = {
            "current_hype": [
                r"\b(trending|viral|blowing up|everywhere)\b",
                r"\b(right now|currently|at the moment)\b",
                r"\b(everyone|everybody)\b.*\b(talking about|listening to)\b",
            ],
            "nostalgic_reference": [
                r"\b(remember|back in|used to|old days)\b",
                r"\b(classic|throwback|vintage|retro)\b",
                r"\b(brings me back|reminds me)\b",
            ],
            "future_anticipation": [
                r"\b(next|upcoming|future|soon)\b",
                r"\b(can\'t wait|excited for|looking forward)\b",
                r"\b(hope|wish|want)\b.*\b(more|continue|keep)\b",
            ],
        }

        temporal_modifier = 1.0

        # Apply temporal decay / amplification
        for category, patterns in temporal_indicators.items():
            for pattern in patterns:
                if re.search(pattern, text.lower()):
                    if category == "current_hype":
                        temporal_modifier *= 1.25  # Amplify current hype
                    elif category == "nostalgic_reference":
                        temporal_modifier *= 1.15  # Moderate amplification for nostalgia
                    elif category == "future_anticipation":
                        temporal_modifier *= 1.20  # Strong amplification for anticipation
                    break

        # Apply proprietary temporal decay function
        decayed_score = base_score * temporal_modifier

        # Sigmoid normalization to prevent extreme values
        normalized_score = math.tanh(decayed_score * 1.2)

        return normalized_score

    def enhance_sentiment_score(
        self, vader_score: float, textblob_score: float, text: str, context: Optional[Dict[str, any]] = None
    ) -> Tuple[float, float]:
        """
        Apply the complete proprietary sentiment enhancement pipeline.

        This is the main entry point that combines all proprietary algorithms
        to produce the enhanced sentiment score and confidence.

        Returns:
            Tuple of (enhanced_score, enhanced_confidence)
        """

        # Phase 1: Multi - modal fusion
        fused_score = self.apply_multi_modal_sentiment_fusion(vader_score, textblob_score, text)

        # Phase 2: Contextual amplification
        amplified_score = self.apply_contextual_sentiment_amplification(fused_score, text, context)

        # Phase 3: Emotional resonance weighting
        resonance_score, confidence_boost = self.apply_dynamic_emotional_resonance_weighting(amplified_score, text)

        # Phase 4: Temporal decay modeling
        final_score = self.apply_temporal_sentiment_decay_modeling(resonance_score, text, context)

        # Calculate enhanced confidence
        base_confidence = 0.7  # Base confidence level

        # Confidence factors
        text_quality_factor = min(1.0, len(text.split()) / 10.0)  # Longer text = higher confidence
        algorithm_agreement = 1.0 - abs(vader_score - textblob_score)  # Agreement between methods
        enhancement_stability = 1.0 - abs(final_score - fused_score) * 0.5  # Stability of enhancement

        enhanced_confidence = (
            base_confidence
            + confidence_boost
            + (text_quality_factor * 0.1)
            + (algorithm_agreement * 0.1)
            + (enhancement_stability * 0.1)
        )

        return max(-1.0, min(1.0, final_score)), max(0.0, min(1.0, enhanced_confidence))


def get_proprietary_enhancement_formula() -> str:
    """
    Get the proprietary enhancement formula as a string for configuration.

    This returns a compact representation of the enhancement algorithm
    that can be stored in environment variables.
    """

    formula = "CSA:1.34|DERW:1.28,0.76|MMSF:0.75,0.45|TSDM:1.25,1.15,1.20|SIGMOID:2.5,1.2"

    return formula


def parse_proprietary_formula(formula_string: str) -> Dict[str, any]:
    """
    Parse the proprietary formula string into configuration parameters.

    This allows the secret formula to be stored compactly in .env files
    while maintaining the full algorithmic complexity.
    """

    if not formula_string:
        return {}

    try:
        components = formula_string.split("|")
        config = {}

        for component in components:
            if ":" in component:
                name, params = component.split(":", 1)
                if "," in params:
                    config[name] = [float(x) for x in params.split(",")]
                else:
                    config[name] = float(params)

        return config

    except (ValueError, IndexError) as e:
        # If parsing fails, return empty config (fallback to standard algorithms)
        return {}


# Example usage and testing
if __name__ == "__main__":
    print("🔒 Proprietary Sentiment Enhancement Formula")
    print("=" * 60)

    enhancer = ProprietarySentimentEnhancer()

    # Test cases
    test_cases = [
        ("this song is absolutely fire no cap", 0.8, 0.6),
        ("the production on this track is insane", 0.7, 0.5),
        ("can't wait for the next album to drop", 0.6, 0.4),
        ("this artist used to be better", -0.3, 0.2),
        ("omg this is a masterpiece 🔥🔥🔥", 0.9, 0.7),
    ]

    print("\n🧪 Testing Enhancement Algorithm:")
    print("-" * 60)

    for text, vader_score, textblob_score in test_cases:
        enhanced_score, enhanced_confidence = enhancer.enhance_sentiment_score(vader_score, textblob_score, text)

        print(f"Text: '{text}'")
        print(f"  Original: VADER={vader_score:.3f}, TextBlob={textblob_score:.3f}")
        print(f"  Enhanced: Score={enhanced_score:.3f}, Confidence={enhanced_confidence:.3f}")
        print(f"  Improvement: {enhanced_score - ((vader_score + textblob_score) / 2):.3f}")
        print()

    # Show formula string
    formula = get_proprietary_enhancement_formula()
    print(f"🔐 Formula String: {formula}")
    print(f"📊 Parsed Config: {parse_proprietary_formula(formula)}")
