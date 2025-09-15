#!/usr/bin/env python3
"""
Comprehensive Music Slang Dictionary and Classification System

This module provides a scientifically classified dictionary of music industry slang
with proper categorization and sentiment labels for testing sentiment models.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Set, Tuple


class SentimentLabel(Enum):
    """Sentiment classification labels."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class SlangCategory(Enum):
    """Categories of music slang."""

    PRAISE_GENERAL = "praise_general"  # General positive expressions
    PRAISE_PERFORMANCE = "praise_performance"  # Performance-specific praise
    PRAISE_PRODUCTION = "praise_production"  # Beat/production praise
    HYPE_EXCITEMENT = "hype_excitement"  # Excitement and hype
    CULTURAL_IDENTITY = "cultural_identity"  # Cultural expressions
    ENGAGEMENT_BEHAVIORAL = "engagement_behavioral"  # Listening behavior
    ANTICIPATION_DEMAND = "anticipation_demand"  # Wanting more content
    CRITICISM_NEGATIVE = "criticism_negative"  # Negative criticism
    CRITICISM_CONSTRUCTIVE = "criticism_constructive"  # Constructive feedback
    NEUTRAL_REQUESTS = "neutral_requests"  # Information requests
    NEUTRAL_QUESTIONS = "neutral_questions"  # Questions without opinion


@dataclass
class SlangEntry:
    """A classified music slang entry."""

    phrase: str
    sentiment: SentimentLabel
    category: SlangCategory
    confidence: float  # How confident we are in this classification (0-1)
    context_notes: str = ""
    variations: List[str] = None

    def __post_init__(self):
        if self.variations is None:
            self.variations = []


class MusicSlangDictionary:
    """Comprehensive classified music slang dictionary."""

    def __init__(self):
        """Initialize with comprehensive classified music slang."""
        self.entries = self._build_dictionary()

    def _build_dictionary(self) -> List[SlangEntry]:
        """Build the comprehensive classified dictionary."""

        entries = []

        # POSITIVE - PRAISE GENERAL
        entries.extend(
            [
                SlangEntry(
                    "this is sick",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Classic music slang - 'sick' means awesome/cool",
                    ["so sick", "that's sick", "sick track", "sick song"],
                ),
                SlangEntry(
                    "this hard",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "'Hard' means impressive/good in music context",
                    ["goes hard", "hard af", "hard as shit", "so hard", "too hard"],
                ),
                SlangEntry(
                    "this crazy",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "'Crazy' means amazing in music context",
                    ["so crazy", "bro this crazy", "absolutely crazy", "way crazy"],
                ),
                SlangEntry(
                    "fire",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Universal music praise term",
                    ["this fire", "straight fire", "pure fire"],
                ),
                SlangEntry(
                    "slaps",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "Song hits hard/sounds great",
                    ["this slaps", "song slaps", "beat slaps"],
                ),
                SlangEntry(
                    "banger", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Great song that hits hard"
                ),
                SlangEntry(
                    "goated",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "Greatest of all time",
                    ["goated song", "goated artist"],
                ),
            ]
        )

        # POSITIVE - PRAISE PERFORMANCE
        entries.extend(
            [
                SlangEntry(
                    "fucking queen",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "High praise for female artist",
                    ["queen", "yes queen", "queen energy"],
                ),
                SlangEntry(
                    "go off king",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "Praise for male artist performance",
                    ["go off queen", "go off", "go off bestie"],
                ),
                SlangEntry(
                    "ate that",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "Performed excellently",
                    ["ate and left no crumbs", "devoured", "served"],
                ),
                SlangEntry(
                    "understood the assignment",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Did exactly what was needed",
                ),
                SlangEntry(
                    "snapped",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Performed exceptionally well",
                    ["my nigga snapped", "artist snapped", "they snapped"],
                ),
                SlangEntry(
                    "you slid",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "You delivered/performed well",
                    ["slid on this", "slid"],
                ),
            ]
        )

        # POSITIVE - PRAISE PRODUCTION
        entries.extend(
            [
                SlangEntry(
                    "the beat though",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.95,
                    "Appreciation for the beat/production",
                    ["the beat tho", "beat goes hard", "beat is fire"],
                ),
                SlangEntry(
                    "who made this beat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.90,
                    "Asking about producer - shows appreciation",
                    ["who produced this", "who made this beat bro"],
                ),
                SlangEntry(
                    "car test passed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.85,
                    "Mix sounds good in car speakers",
                    ["passed the car test", "this just passed the car test"],
                ),
                SlangEntry(
                    "mix sounds",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.70,
                    "Neutral unless qualified with positive/negative",
                    ["mix sounds good", "mix sounds clean"],
                ),
            ]
        )

        # POSITIVE - HYPE EXCITEMENT
        entries.extend(
            [
                SlangEntry(
                    "sheeeesh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.90,
                    "Expression of amazement/approval",
                    ["sheeesh", "sheesh"],
                ),
                SlangEntry(
                    "oh my",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.80,
                    "Excitement/surprise",
                    ["oh my god", "oh my yes"],
                ),
                SlangEntry(
                    "fuck it up",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Encouragement to perform well",
                ),
                SlangEntry(
                    "yessir",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Affirmative excitement",
                    ["yessuh", "yes sir"],
                ),
                SlangEntry(
                    "bitchhh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.80,
                    "Extended exclamation of excitement",
                    ["bitch it's giving", "bitch it's givinnnng"],
                ),
            ]
        )

        # POSITIVE - CULTURAL IDENTITY
        entries.extend(
            [
                SlangEntry(
                    "for the culture",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.90,
                    "Supporting cultural representation",
                    ["for the culture fr"],
                ),
                SlangEntry(
                    "for the girls",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.85,
                    "Supporting female empowerment",
                ),
                SlangEntry(
                    "real music is back",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.80,
                    "Appreciation for authentic music",
                ),
            ]
        )

        # POSITIVE - ENGAGEMENT BEHAVIORAL
        entries.extend(
            [
                SlangEntry(
                    "on repeat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Playing song repeatedly",
                    ["this on repeat", "been on repeat", "on repeat all day"],
                ),
                SlangEntry(
                    "no skips",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.90,
                    "Every song is good",
                    ["front to back no skips", "album no skips"],
                ),
                SlangEntry(
                    "went platinum in my",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.85,
                    "Personal heavy rotation",
                    ["went platinum in my car", "went platinum in my headphones", "went platinum in my room"],
                ),
                SlangEntry(
                    "saved my life",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.90,
                    "Song had major emotional impact",
                    ["this saved my life"],
                ),
                SlangEntry(
                    "gym playlist",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.80,
                    "Adding to workout playlist",
                    ["on my gym playlist", "workout playlist"],
                ),
            ]
        )

        # POSITIVE - ANTICIPATION DEMAND
        entries.extend(
            [
                SlangEntry(
                    "drop the album",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.85,
                    "Wanting more content",
                    ["drop the album already", "we need the album", "album when"],
                ),
                SlangEntry(
                    "need the lyrics",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Wanting to engage more with song",
                    ["I need the lyrics", "where are the lyrics"],
                ),
                SlangEntry(
                    "please come to",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.75,
                    "Wanting live performance",
                    ["please come to atlanta", "come to my city"],
                ),
                SlangEntry(
                    "friday can't come sooner",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Anticipating release",
                ),
            ]
        )

        # POSITIVE - GEN Z SLANG
        entries.extend(
            [
                SlangEntry(
                    "slay",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Perform excellently",
                    ["slayed", "slaying"],
                ),
                SlangEntry(
                    "periodt",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Period with emphasis - end of discussion",
                    ["period"],
                ),
                SlangEntry(
                    "no cap",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.80,
                    "No lie/for real - agreement",
                ),
                SlangEntry(
                    "hits different",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.85,
                    "Uniquely good/special",
                ),
                SlangEntry(
                    "chef's kiss", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Perfect/excellent"
                ),
                SlangEntry(
                    "we stan",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "We support/love this artist",
                    ["I stan", "stan forever"],
                ),
                SlangEntry(
                    "iconic", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.85, "Legendary/memorable"
                ),
                SlangEntry(
                    "obsessed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.80,
                    "Really love this",
                    ["lowkey obsessed", "highkey obsessed"],
                ),
            ]
        )

        # POSITIVE - RATINGS
        entries.extend(
            [
                SlangEntry(
                    "10/10",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Perfect score",
                    ["100/10", "11/10"],
                ),
                SlangEntry("SOTY", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Song of the year"),
                SlangEntry("AOTY", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Album of the year"),
            ]
        )

        # NEGATIVE - CRITICISM
        entries.extend(
            [
                SlangEntry(
                    "this ain't it chief",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.95,
                    "This is not good",
                ),
                SlangEntry(
                    "mid",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.90,
                    "Mediocre/average in a bad way",
                ),
                SlangEntry(
                    "basura", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "Trash (Spanish)"
                ),
                SlangEntry(
                    "went double wood",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.80,
                    "Sold very poorly (opposite of platinum)",
                ),
                SlangEntry(
                    "who approved this",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.85,
                    "Questioning quality control",
                ),
                SlangEntry(
                    "turn it off", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.90, "Stop playing this"
                ),
                SlangEntry(
                    "nobody asked for this",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.85,
                    "Unwanted content",
                ),
                SlangEntry(
                    "overrated",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.80,
                    "Gets more praise than deserved",
                ),
                SlangEntry(
                    "fell off", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "Quality declined"
                ),
                SlangEntry(
                    "industry plant",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.75,
                    "Artificially promoted artist",
                ),
                SlangEntry(
                    "flop",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.90,
                    "Commercial/artistic failure",
                ),
                SlangEntry(
                    "skip", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "Not worth listening to"
                ),
            ]
        )

        # NEGATIVE - CONSTRUCTIVE CRITICISM
        entries.extend(
            [
                SlangEntry(
                    "switch up the flow",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.70,
                    "Suggestion for improvement",
                ),
                SlangEntry(
                    "flow is repetitive",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.75,
                    "Technical criticism",
                ),
                SlangEntry(
                    "mix sounds chaotic",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.70,
                    "Production feedback",
                ),
            ]
        )

        # NEUTRAL - REQUESTS
        entries.extend(
            [
                SlangEntry(
                    "need the instrumental",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_REQUESTS,
                    0.90,
                    "Request for content without opinion",
                ),
                SlangEntry(
                    "drop the visuals",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_REQUESTS,
                    0.85,
                    "Request for music video",
                ),
                SlangEntry(
                    "clean version pls", SentimentLabel.NEUTRAL, SlangCategory.NEUTRAL_REQUESTS, 0.80, "Format request"
                ),
            ]
        )

        # NEUTRAL - QUESTIONS
        entries.extend(
            [
                SlangEntry(
                    "who mixed this",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    0.85,
                    "Information request without explicit opinion",
                ),
                SlangEntry(
                    "what's the sample",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    0.90,
                    "Information request",
                ),
            ]
        )

        # COMPLIMENTS AND IDENTITY
        entries.extend(
            [
                SlangEntry(
                    "hottie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.80,
                    "Attractive person compliment",
                    ["baddie", "hot bish", "bad bish"],
                ),
                SlangEntry(
                    "bestie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.75,
                    "Friend/supportive term",
                    ["bestie goals"],
                ),
                SlangEntry(
                    "YES MOTHER",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "High praise, especially for female artists",
                    ["mother", "mom"],
                ),
            ]
        )

        return entries

    def get_all_phrases(self) -> List[str]:
        """Get all phrases including variations."""
        phrases = []
        for entry in self.entries:
            phrases.append(entry.phrase)
            phrases.extend(entry.variations)
        return phrases

    def get_by_sentiment(self, sentiment: SentimentLabel) -> List[SlangEntry]:
        """Get entries by sentiment label."""
        return [entry for entry in self.entries if entry.sentiment == sentiment]

    def get_by_category(self, category: SlangCategory) -> List[SlangEntry]:
        """Get entries by category."""
        return [entry for entry in self.entries if entry.category == category]

    def get_test_cases(self) -> List[Tuple[str, str, str, float]]:
        """
        Get test cases for sentiment model evaluation.

        Returns:
            List of (phrase, sentiment, category, confidence) tuples
        """
        test_cases = []

        for entry in self.entries:
            # Add main phrase
            test_cases.append((entry.phrase, entry.sentiment.value, entry.category.value, entry.confidence))

            # Add variations
            for variation in entry.variations:
                test_cases.append((variation, entry.sentiment.value, entry.category.value, entry.confidence))

        return test_cases

    def get_beat_appreciation_phrases(self) -> List[str]:
        """Get phrases that should trigger beat appreciation detection."""
        beat_phrases = []

        production_entries = self.get_by_category(SlangCategory.PRAISE_PRODUCTION)
        for entry in production_entries:
            beat_phrases.append(entry.phrase)
            beat_phrases.extend(entry.variations)

        # Add additional beat-related phrases
        beat_phrases.extend(
            [
                "the beat",
                "beat goes hard",
                "beat slaps",
                "production",
                "instrumental",
                "who produced",
                "producer",
                "mix sounds good",
                "car test",
            ]
        )

        return beat_phrases

    def print_statistics(self):
        """Print dictionary statistics."""
        total_entries = len(self.entries)
        total_phrases = len(self.get_all_phrases())

        sentiment_counts = {}
        category_counts = {}

        for entry in self.entries:
            sentiment = entry.sentiment.value
            category = entry.category.value

            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1

        print("🎵 Music Slang Dictionary Statistics")
        print("=" * 50)
        print(f"Total entries: {total_entries}")
        print(f"Total phrases (with variations): {total_phrases}")

        print(f"\nSentiment distribution:")
        for sentiment, count in sentiment_counts.items():
            percentage = count / total_entries * 100
            print(f"  {sentiment}: {count} ({percentage:.1f}%)")

        print(f"\nCategory distribution:")
        for category, count in sorted(category_counts.items()):
            percentage = count / total_entries * 100
            print(f"  {category}: {count} ({percentage:.1f}%)")


def get_music_slang_dictionary() -> MusicSlangDictionary:
    """Get the comprehensive music slang dictionary."""
    return MusicSlangDictionary()


if __name__ == "__main__":
    # Print dictionary statistics
    dictionary = get_music_slang_dictionary()
    dictionary.print_statistics()

    print(f"\n📝 Sample test cases:")
    test_cases = dictionary.get_test_cases()[:10]
    for phrase, sentiment, category, confidence in test_cases:
        print(f"  '{phrase}' → {sentiment} ({category}, conf: {confidence})")

    print(f"\n🎵 Beat appreciation phrases:")
    beat_phrases = dictionary.get_beat_appreciation_phrases()[:10]
    for phrase in beat_phrases:
        print(f"  '{phrase}'")
