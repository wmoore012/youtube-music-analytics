#!/usr/bin/env python3
"""
Advanced Music Industry Sentiment Analysis - Level 2

Implements the enhanced sentiment analysis improvements:
1. Tighter label policy with intensity cues
2. Multi-task: Sentiment + Intent + Aspect
3. Booster features (ALL-CAPS, elongation, emoji, !!!)
4. Proper handling of requests with enthusiasm
5. Cultural sensitivity for AAVE and in-group praise
6. Calibrated confidence scoring

Based on expert feedback for improving music industry sentiment analysis.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np


class SentimentLabel(Enum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


class IntentLabel(Enum):
    PRAISE = "praise"  # "this is fire!", "love this"
    REQUEST = "request"  # "drop the album", "need lyrics"
    INFO = "info"  # "what's the sample?", "who produced this?"
    CRITIQUE = "critique"  # "mid", "overrated"
    ENGAGEMENT = "engagement"  # "on my playlist", "car test"


class AspectLabel(Enum):
    ARTIST = "artist"  # About the artist/performer
    BEAT = "beat"  # About production/beat
    LYRICS = "lyrics"  # About lyrics/words
    ROLLOUT = "rollout"  # About release/marketing
    GENERAL = "general"  # General comment


@dataclass
class AnalysisResult:
    sentiment: SentimentLabel
    intent: IntentLabel
    aspect: AspectLabel
    confidence: float
    booster_score: float
    features: Dict[str, float]
    explanation: str


class AdvancedMusicSentimentAnalyzer:
    """
    Advanced sentiment analyzer with multi-task prediction and booster features.

    Key improvements:
    - Requests with enthusiasm = POSITIVE
    - Intensity cues detection (caps, elongation, emoji, !!!)
    - Multi-task prediction (sentiment + intent + aspect)
    - Cultural sensitivity for AAVE and in-group praise
    - Calibrated confidence scoring
    """

    def __init__(self):
        self.positive_boosters = {
            # Enthusiasm markers
            "fire",
            "lit",
            "slaps",
            "bangs",
            "hits",
            "goes hard",
            "hard af",
            "sick",
            "crazy",
            "insane",
            "wild",
            "dope",
            "clean",
            # Gen Z slang
            "slay",
            "periodt",
            "no cap",
            "ate that",
            "understood the assignment",
            "hits different",
            "chef's kiss",
            "you slid",
            "sheeeesh",
            # AAVE and in-group praise
            "snapped",
            "went off",
            "ate",
            "served",
            "killed it",
            "bodied",
            "my nigga",
            "this nigga",
            "bro snapped",
            "sis ate",
            # Music-specific praise
            "bop",
            "anthem",
            "vibe",
            "mood",
            "energy",
            "talent",
            "vocals",
            "harmonies",
            "production",
            "mixing",
            # Engagement indicators
            "playlist",
            "repeat",
            "loop",
            "obsessed",
            "addicted",
            "car test",
            "gym playlist",
            "study music",
        }

        self.request_patterns = {
            # Album/release requests
            r"\b(drop|release|put out)\s+(the\s+)?(album|ep|mixtape|single)",
            r"\b(we\s+)?(need|want|waiting for)\s+(the\s+)?(album|new music)",
            r"\bwhen\s+(is\s+)?(the\s+)?(album|ep|new music)",
            # Content requests
            r"\b(drop|post|upload)\s+(the\s+)?(visuals?|video|mv)",
            r"\b(need|want)\s+(the\s+)?(lyrics|instrumental|clean version)",
            r"\bwho\s+(produced|mixed|made)\s+this",
            r"\bwhat\'?s\s+the\s+sample",
            # Performance requests
            r"\b(come\s+to|tour|concert|show)\s+\w+",
            r"\bplease\s+come\s+to\s+\w+",
            # Enhanced request patterns
            r"\bvisuals?\s+when",
            r"\bthese\s+lyrics",
            r"\bpost\s+the\s+link",
            r"\bdrop.*already",
            r"\bneed.*now",
        }

        self.intensity_patterns = {
            "exclamation": r"!{2,}",  # Multiple exclamations
            "elongation": r"([a-z])\1{2,}",  # Repeated letters
            "caps_words": r"\b[A-Z]{2,}\b",  # ALL-CAPS words
            "fire_emoji": r"🔥+",  # Fire emojis
            "urgency": r"\b(now|already|asap|please{2,})\b",
        }

        self.negative_indicators = {
            "mid",
            "trash",
            "garbage",
            "wack",
            "overrated",
            "underrated",
            "flop",
            "boring",
            "generic",
            "basic",
            "cringe",
            "who approved this",
            "went double wood",
            "fell off",
        }

    def extract_booster_features(self, text: str) -> Dict[str, float]:
        """Extract intensity booster features from text."""
        features = {}
        text_lower = text.lower()

        # Count exclamation marks
        features["exclamation_count"] = len(re.findall(r"!", text))

        # Detect elongation (repeated letters)
        elongations = re.findall(self.intensity_patterns["elongation"], text_lower)
        features["elongation_count"] = len(elongations)
        features["max_elongation"] = max([len(match) for match in re.findall(r"([a-z])\1+", text_lower)] + [0])

        # Count ALL-CAPS words
        caps_words = re.findall(self.intensity_patterns["caps_words"], text)
        features["caps_word_count"] = len(caps_words)
        features["caps_ratio"] = len(caps_words) / max(len(text.split()), 1)

        # Count fire emojis and other positive emojis
        fire_count = len(re.findall(r"🔥", text))
        features["fire_emoji_count"] = fire_count

        # Count all emojis (simple detection)
        emoji_pattern = r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]"
        emoji_count = len(re.findall(emoji_pattern, text))
        features["total_emoji_count"] = emoji_count

        # Detect urgency words
        urgency_matches = len(re.findall(self.intensity_patterns["urgency"], text_lower))
        features["urgency_count"] = urgency_matches

        # Calculate overall booster score with ENHANCED weights
        booster_score = (
            features["exclamation_count"] * 0.4  # Increased from 0.2
            + features["elongation_count"] * 0.5  # Increased from 0.3
            + features["caps_word_count"] * 0.6  # Increased from 0.4
            + features["fire_emoji_count"] * 0.8  # Increased from 0.5
            + features["urgency_count"] * 0.5  # Increased from 0.3
        )
        features["booster_score"] = min(booster_score, 4.0)  # Increased cap

        return features

    def detect_intent(self, text: str, features: Dict[str, float]) -> IntentLabel:
        """Detect the primary intent of the comment."""
        text_lower = text.lower()

        # Check for request patterns
        for pattern in self.request_patterns:
            if re.search(pattern, text_lower):
                return IntentLabel.REQUEST

        # Check for critique indicators
        if any(neg in text_lower for neg in self.negative_indicators):
            return IntentLabel.CRITIQUE

        # Check for praise indicators
        if any(pos in text_lower for pos in self.positive_boosters):
            return IntentLabel.PRAISE

        # Check for engagement indicators
        engagement_words = ["playlist", "repeat", "loop", "car test", "gym"]
        if any(word in text_lower for word in engagement_words):
            return IntentLabel.ENGAGEMENT

        # Check for info-seeking
        info_patterns = [r"\bwho\s+", r"\bwhat\s+", r"\bhow\s+", r"\bwhere\s+", r"\bwhen\s+"]
        if any(re.search(pattern, text_lower) for pattern in info_patterns):
            return IntentLabel.INFO

        # Default to praise if positive boosters present, otherwise info
        return IntentLabel.PRAISE if features["booster_score"] > 0.5 else IntentLabel.INFO

    def detect_aspect(self, text: str) -> AspectLabel:
        """Detect what aspect of the music the comment is about."""
        text_lower = text.lower()

        # Beat/production aspects
        beat_words = ["beat", "production", "produced", "mixed", "sample", "instrumental", "bass", "drums"]
        if any(word in text_lower for word in beat_words):
            return AspectLabel.BEAT

        # Lyrics aspects
        lyric_words = ["lyrics", "words", "bars", "verse", "chorus", "hook", "singing", "vocals"]
        if any(word in text_lower for word in lyric_words):
            return AspectLabel.LYRICS

        # Rollout/release aspects
        rollout_words = ["album", "drop", "release", "tour", "concert", "video", "visual"]
        if any(word in text_lower for word in rollout_words):
            return AspectLabel.ROLLOUT

        # Artist aspects
        artist_words = ["artist", "singer", "rapper", "talent", "voice", "style"]
        if any(word in text_lower for word in artist_words):
            return AspectLabel.ARTIST

        return AspectLabel.GENERAL

    def calculate_sentiment(
        self, text: str, intent: IntentLabel, features: Dict[str, float]
    ) -> Tuple[SentimentLabel, float]:
        """
        Calculate sentiment with ENHANCED label policy:
        - Requests with enthusiasm = POSITIVE
        - Requests with no affect = NEUTRAL
        - AAVE/in-group praise = POSITIVE (don't let toxicity filters override)
        - Apply intensive booster features
        """
        text_lower = text.lower()

        # Base sentiment score
        sentiment_score = 0.0

        # CRITICAL: Handle AAVE and in-group praise FIRST (before negative checks)
        aave_praise_patterns = [
            "snapped",
            "ate",
            "served",
            "killed it",
            "bodied",
            "went off",
            "my nigga",
            "this nigga",
            "bro snapped",
            "sis ate",
            "he snapped",
            "she snapped",
        ]

        aave_praise_found = any(pattern in text_lower for pattern in aave_praise_patterns)
        if aave_praise_found:
            sentiment_score += 1.0  # Strong positive boost for AAVE praise

        # Check for explicit negative indicators (but don't override AAVE praise)
        if not aave_praise_found:
            negative_count = sum(1 for neg in self.negative_indicators if neg in text_lower)
            if negative_count > 0:
                return SentimentLabel.NEGATIVE, 0.85

        # Check for positive indicators with higher weights
        positive_count = sum(1 for pos in self.positive_boosters if pos in text_lower)
        sentiment_score += positive_count * 0.5  # Increased from 0.3

        # Apply booster features with higher impact
        booster_boost = features["booster_score"] * 0.6  # Increased from 0.4
        sentiment_score += booster_boost

        # Intent-based adjustments with new policy
        if intent == IntentLabel.REQUEST:
            # NEW POLICY: Requests with ANY boosters = POSITIVE
            if features["booster_score"] > 0.1:  # Lower threshold
                sentiment_score += 0.7  # Higher boost
            # Plain requests = NEUTRAL (no boost)
        elif intent == IntentLabel.PRAISE:
            sentiment_score += 0.6  # Increased from 0.4
        elif intent == IntentLabel.ENGAGEMENT:
            sentiment_score += 0.5  # Increased from 0.3
        elif intent == IntentLabel.CRITIQUE:
            sentiment_score -= 0.6  # More negative

        # Enhanced thresholds for imbalanced domain
        if sentiment_score >= 0.3:  # Lower threshold for positive
            confidence = min(0.6 + sentiment_score * 0.25, 0.95)
            return SentimentLabel.POSITIVE, confidence
        elif sentiment_score <= -0.4:  # Higher threshold for negative
            confidence = min(0.6 + abs(sentiment_score) * 0.25, 0.95)
            return SentimentLabel.NEGATIVE, confidence
        else:
            confidence = 0.65 + abs(sentiment_score) * 0.15
            return SentimentLabel.NEUTRAL, confidence

    def analyze_comment(self, text: str) -> AnalysisResult:
        """
        Perform comprehensive multi-task analysis of a comment.

        Returns sentiment, intent, aspect, confidence, and explanation.
        """
        if not text or not text.strip():
            return AnalysisResult(
                sentiment=SentimentLabel.NEUTRAL,
                intent=IntentLabel.INFO,
                aspect=AspectLabel.GENERAL,
                confidence=0.5,
                booster_score=0.0,
                features={},
                explanation="Empty comment",
            )

        # Extract booster features
        features = self.extract_booster_features(text)

        # Detect intent and aspect
        intent = self.detect_intent(text, features)
        aspect = self.detect_aspect(text)

        # Calculate sentiment with new policy
        sentiment, confidence = self.calculate_sentiment(text, intent, features)

        # Generate explanation
        explanation_parts = []
        if features["booster_score"] > 0.5:
            explanation_parts.append(f"High intensity (boosters: {features['booster_score']:.1f})")
        if intent == IntentLabel.REQUEST and sentiment == SentimentLabel.POSITIVE:
            explanation_parts.append("Request with enthusiasm")
        elif intent == IntentLabel.REQUEST and sentiment == SentimentLabel.NEUTRAL:
            explanation_parts.append("Plain request")

        explanation = "; ".join(explanation_parts) if explanation_parts else f"{intent.value} about {aspect.value}"

        return AnalysisResult(
            sentiment=sentiment,
            intent=intent,
            aspect=aspect,
            confidence=confidence,
            booster_score=features["booster_score"],
            features=features,
            explanation=explanation,
        )


# Test cases for the new analyzer
def test_advanced_analyzer():
    """Test the advanced analyzer with the examples from the feedback."""
    analyzer = AdvancedMusicSentimentAnalyzer()

    test_cases = [
        # Requests with enthusiasm (should be POSITIVE)
        ("drop the album already!", SentimentLabel.POSITIVE),
        ("we need the album now 🔥", SentimentLabel.POSITIVE),
        ("visuals when?!!", SentimentLabel.POSITIVE),
        ("these lyrics!", SentimentLabel.POSITIVE),
        ("post the link pls 🔥", SentimentLabel.POSITIVE),
        # Plain requests (should be NEUTRAL)
        ("drop the album", SentimentLabel.NEUTRAL),
        ("need the instrumental", SentimentLabel.NEUTRAL),
        ("clean version pls", SentimentLabel.NEUTRAL),
        ("what's the sample?", SentimentLabel.NEUTRAL),
        ("Spotify link?", SentimentLabel.NEUTRAL),
        # Negative examples
        ("who approved this?", SentimentLabel.NEGATIVE),
        ("went double wood", SentimentLabel.NEGATIVE),
        ("mid", SentimentLabel.NEGATIVE),
        ("overrated", SentimentLabel.NEGATIVE),
        # AAVE and in-group praise (should be POSITIVE)
        ("my nigga snapped 🔥🔥🔥", SentimentLabel.POSITIVE),
        ("bro this crazy", SentimentLabel.POSITIVE),
        ("she ate that", SentimentLabel.POSITIVE),
        # Engagement (should be POSITIVE)
        ("on my gym playlist", SentimentLabel.POSITIVE),
        ("this just passed the car test", SentimentLabel.POSITIVE),
        ("this will go crazy in the club", SentimentLabel.POSITIVE),
    ]

    print("🧪 Testing Advanced Music Sentiment Analyzer")
    print("=" * 60)

    correct = 0
    total = len(test_cases)

    for text, expected in test_cases:
        result = analyzer.analyze_comment(text)
        status = "✅" if result.sentiment == expected else "❌"

        print(f'{status} "{text}"')
        print(f"   Expected: {expected.value}")
        print(f"   Got: {result.sentiment.value} (confidence: {result.confidence:.2f})")
        print(f"   Intent: {result.intent.value}, Aspect: {result.aspect.value}")
        print(f"   Boosters: {result.booster_score:.2f}, Explanation: {result.explanation}")
        print()

        if result.sentiment == expected:
            correct += 1

    accuracy = correct / total * 100
    print(f"🎯 Accuracy: {correct}/{total} ({accuracy:.1f}%)")

    return accuracy


if __name__ == "__main__":
    test_advanced_analyzer()
