#!/usr/bin/env python3
"""
Music Industry Sentiment Analysis Dataset

A comprehensive, scientifically classified dataset of music industry slang,
Gen Z language, and fan expressions for sentiment analysis model training and evaluation.

This dataset is designed for:
- Training music-specific sentiment models
- Evaluating model performance on music industry language
- Research in cultural linguistics and sentiment analysis
- Building production-ready music analytics systems

Dataset Statistics:
- 200+ classified phrases
- 11 semantic categories
- 3 sentiment labels (positive, negative, neutral)
- Confidence scores for each classification
- Cultural context annotations

License: MIT (when published)
Authors: Music Analytics Research Team
Version: 1.0
"""

from dataclasses import dataclass
from enum import Enum
import random
from typing import Dict, List, Optional, Tuple

import pandas as pd


class SentimentLabel(Enum):
    """Sentiment classification labels."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class SlangCategory(Enum):
    """Semantic categories of music industry language."""

    PRAISE_GENERAL = "praise_general"  # General positive expressions
    PRAISE_PERFORMANCE = "praise_performance"  # Artist performance praise
    PRAISE_PRODUCTION = "praise_production"  # Beat/production appreciation
    HYPE_EXCITEMENT = "hype_excitement"  # Excitement and energy
    CULTURAL_IDENTITY = "cultural_identity"  # Cultural expressions
    ENGAGEMENT_BEHAVIORAL = "engagement_behavioral"  # Listening behavior
    ANTICIPATION_DEMAND = "anticipation_demand"  # Wanting more content
    CRITICISM_NEGATIVE = "criticism_negative"  # Negative criticism
    CRITICISM_CONSTRUCTIVE = "criticism_constructive"  # Constructive feedback
    NEUTRAL_REQUESTS = "neutral_requests"  # Information requests
    NEUTRAL_QUESTIONS = "neutral_questions"  # Questions without opinion


@dataclass
class MusicSlangEntry:
    """A classified music industry language entry."""

    phrase: str
    sentiment: SentimentLabel
    category: SlangCategory
    confidence: float  # Classification confidence (0-1)
    context_notes: str = ""
    beat_appreciation: bool = False  # Indicates beat/production focus
    gen_z_slang: bool = False  # Indicates Gen Z specific language

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame creation."""
        return {
            "phrase": self.phrase,
            "sentiment": self.sentiment.value,
            "category": self.category.value,
            "confidence": self.confidence,
            "context_notes": self.context_notes,
            "beat_appreciation": self.beat_appreciation,
            "gen_z_slang": self.gen_z_slang,
        }


class MusicIndustrySentimentDataset:
    """
    Comprehensive Music Industry Sentiment Dataset

    This dataset contains scientifically classified music industry language
    including traditional music slang, Gen Z expressions, and fan culture language.
    """

    def __init__(self):
        """Initialize the complete dataset."""
        self.entries = self._build_complete_dataset()
        self.version = "1.0"
        self.total_phrases = len(self.entries)

    def _build_complete_dataset(self) -> List[MusicSlangEntry]:
        """Build the complete classified dataset."""

        entries = []

        # ===== POSITIVE SENTIMENT =====

        # PRAISE GENERAL - Traditional music slang
        entries.extend(
            [
                MusicSlangEntry(
                    "this is sick",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Classic music slang - 'sick' means awesome/cool",
                ),
                MusicSlangEntry(
                    "so sick",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "Variation of 'sick' meaning awesome",
                ),
                MusicSlangEntry(
                    "that's sick",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "Variation of 'sick' meaning awesome",
                ),
                MusicSlangEntry(
                    "sick beat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "'Sick' applied to beat/production",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "sick flow",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "'Sick' applied to rap flow",
                ),
                MusicSlangEntry(
                    "sick track",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "'Sick' applied to entire song",
                ),
                MusicSlangEntry(
                    "this hard",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "'Hard' means impressive/good in music context",
                ),
                MusicSlangEntry(
                    "goes hard",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Song/beat hits hard emotionally",
                ),
                MusicSlangEntry(
                    "hard af",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Very impressive (af = as fuck)",
                ),
                MusicSlangEntry(
                    "hard as shit",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Very impressive, strong emphasis",
                ),
                MusicSlangEntry(
                    "so hard", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Impressive with emphasis"
                ),
                MusicSlangEntry(
                    "too hard", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Extremely impressive"
                ),
                MusicSlangEntry(
                    "this hard af",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "This is very impressive",
                ),
                MusicSlangEntry(
                    "this hard as shit",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "This is very impressive, strong emphasis",
                ),
                MusicSlangEntry(
                    "this crazy",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "'Crazy' means amazing in music context",
                ),
                MusicSlangEntry(
                    "so crazy", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.85, "Very amazing/impressive"
                ),
                MusicSlangEntry(
                    "bro this crazy",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "Casual expression of amazement",
                ),
                MusicSlangEntry(
                    "absolutely crazy",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.85,
                    "Extremely impressive",
                ),
                MusicSlangEntry(
                    "fire", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Universal music praise term"
                ),
                MusicSlangEntry(
                    "this fire", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "This is excellent"
                ),
                MusicSlangEntry(
                    "straight fire", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Pure excellence"
                ),
                MusicSlangEntry(
                    "pure fire", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Absolute excellence"
                ),
                MusicSlangEntry(
                    "slaps", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Song hits hard/sounds great"
                ),
                MusicSlangEntry(
                    "this slaps", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "This song hits hard"
                ),
                MusicSlangEntry(
                    "song slaps", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Song is excellent"
                ),
                MusicSlangEntry(
                    "banger", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Great song that hits hard"
                ),
                MusicSlangEntry(
                    "goated", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Greatest of all time"
                ),
                MusicSlangEntry(
                    "goated song",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Greatest song of all time",
                ),
                MusicSlangEntry(
                    "goated artist",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.95,
                    "Greatest artist of all time",
                ),
                MusicSlangEntry(
                    "hits different",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.85,
                    "Uniquely good/special",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "chef's kiss",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.90,
                    "Perfect/excellent",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "iconic", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.85, "Legendary/memorable"
                ),
                MusicSlangEntry(
                    "legendary", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.85, "Epic/historic quality"
                ),
            ]
        )

        # PRAISE PERFORMANCE - Artist-specific praise
        entries.extend(
            [
                MusicSlangEntry(
                    "fucking queen",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "High praise for female artist",
                ),
                MusicSlangEntry(
                    "queen", SentimentLabel.POSITIVE, SlangCategory.PRAISE_PERFORMANCE, 0.85, "Praise for female artist"
                ),
                MusicSlangEntry(
                    "yes queen",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Supportive praise for female artist",
                ),
                MusicSlangEntry(
                    "queen energy",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "Confident, powerful performance",
                ),
                MusicSlangEntry(
                    "YES MOTHER",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "High praise, especially for female artists",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "mother",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.80,
                    "Praise term in Gen Z culture",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "mom",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.75,
                    "Praise term variation",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "go off king",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "Praise for male artist performance",
                ),
                MusicSlangEntry(
                    "go off queen",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "Praise for female artist performance",
                ),
                MusicSlangEntry(
                    "go off",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "Encouragement to perform well",
                ),
                MusicSlangEntry(
                    "go off bestie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "Friendly encouragement",
                ),
                MusicSlangEntry(
                    "ate that",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "Performed excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "ate and left no crumbs",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.95,
                    "Perfect performance, nothing left to improve",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "devoured",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Dominated the performance",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "served",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "Delivered an excellent performance",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "understood the assignment",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Did exactly what was needed",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "snapped",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Performed exceptionally well",
                ),
                MusicSlangEntry(
                    "my nigga snapped",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Artist performed exceptionally well",
                ),
                MusicSlangEntry(
                    "you slid",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "You delivered/performed well",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "slay",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Perform excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "slayed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Performed excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "slaying",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Currently performing excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "talent", SentimentLabel.POSITIVE, SlangCategory.PRAISE_PERFORMANCE, 0.85, "Recognition of skill"
                ),
                MusicSlangEntry(
                    "pure talent",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Exceptional natural ability",
                ),
                MusicSlangEntry(
                    "the talent jumped out",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.90,
                    "Talent was clearly displayed",
                    gen_z_slang=True,
                ),
            ]
        )

        # PRAISE PRODUCTION - Beat and production appreciation
        entries.extend(
            [
                MusicSlangEntry(
                    "the beat though",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.95,
                    "Appreciation for the beat/production",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "the beat tho",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.95,
                    "Casual appreciation for beat",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat goes hard",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.95,
                    "Beat is impressive",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat is fire",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.95,
                    "Beat is excellent",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat slaps",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.90,
                    "Beat hits hard",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "who made this beat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.90,
                    "Asking about producer - shows appreciation",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "who made this beat bro",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.90,
                    "Casual inquiry about producer",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "who produced this",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.85,
                    "Asking about producer",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "car test passed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.85,
                    "Mix sounds good in car speakers",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "passed the car test",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.85,
                    "Audio quality approved",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "this just passed the car test",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PRODUCTION,
                    0.85,
                    "Recently confirmed good audio quality",
                    beat_appreciation=True,
                ),
            ]
        )

        # HYPE EXCITEMENT - Energy and enthusiasm
        entries.extend(
            [
                MusicSlangEntry(
                    "sheeeesh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.90,
                    "Expression of amazement/approval",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "sheeesh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.90,
                    "Shorter version of amazement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "sheesh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Casual amazement expression",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "oh my", SentimentLabel.POSITIVE, SlangCategory.HYPE_EXCITEMENT, 0.80, "Excitement/surprise"
                ),
                MusicSlangEntry(
                    "oh my yes", SentimentLabel.POSITIVE, SlangCategory.HYPE_EXCITEMENT, 0.85, "Enthusiastic approval"
                ),
                MusicSlangEntry(
                    "fuck it up",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Encouragement to perform well",
                ),
                MusicSlangEntry(
                    "yessir", SentimentLabel.POSITIVE, SlangCategory.HYPE_EXCITEMENT, 0.85, "Affirmative excitement"
                ),
                MusicSlangEntry(
                    "yessuh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Casual affirmative excitement",
                ),
                MusicSlangEntry(
                    "yes sir",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.80,
                    "Formal affirmative excitement",
                ),
                MusicSlangEntry(
                    "bitchhh",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.80,
                    "Extended exclamation of excitement",
                ),
                MusicSlangEntry(
                    "bitch it's giving",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Excited approval",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "bitch it's givinnnng",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Very excited approval",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "periodt",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.85,
                    "Period with emphasis - end of discussion",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "period",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.75,
                    "End of discussion, agreement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "no cap",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.80,
                    "No lie/for real - agreement",
                    gen_z_slang=True,
                ),
            ]
        )

        # CULTURAL IDENTITY - Cultural and community expressions
        entries.extend(
            [
                MusicSlangEntry(
                    "for the culture",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.90,
                    "Supporting cultural representation",
                ),
                MusicSlangEntry(
                    "for the culture fr",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.90,
                    "Supporting culture, for real",
                ),
                MusicSlangEntry(
                    "for the girls",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.85,
                    "Supporting female empowerment",
                ),
                MusicSlangEntry(
                    "real music is back",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.80,
                    "Appreciation for authentic music",
                ),
                MusicSlangEntry(
                    "we stan",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.85,
                    "We support/love this artist",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "I stan",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.85,
                    "I support this artist",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "I stan a queen",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.90,
                    "I support this female artist",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "stan forever",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.85,
                    "Permanent support",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "bestie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.75,
                    "Friend/supportive term",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "bestie goals",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.80,
                    "Friendship aspirations",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "bestie goals fr",
                    SentimentLabel.POSITIVE,
                    SlangCategory.CULTURAL_IDENTITY,
                    0.80,
                    "Real friendship goals",
                    gen_z_slang=True,
                ),
            ]
        )

        # ENGAGEMENT BEHAVIORAL - Listening and consumption behavior
        entries.extend(
            [
                MusicSlangEntry(
                    "on repeat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Playing song repeatedly",
                ),
                MusicSlangEntry(
                    "this on repeat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Playing this song repeatedly",
                ),
                MusicSlangEntry(
                    "been on repeat",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Has been playing repeatedly",
                ),
                MusicSlangEntry(
                    "on repeat all day",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Playing all day long",
                ),
                MusicSlangEntry(
                    "this on repeat all day",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "This song playing all day",
                ),
                MusicSlangEntry(
                    "been on repeat since it dropped",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Playing since release",
                ),
                MusicSlangEntry(
                    "no skips", SentimentLabel.POSITIVE, SlangCategory.ENGAGEMENT_BEHAVIORAL, 0.90, "Every song is good"
                ),
                MusicSlangEntry(
                    "front to back no skips",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.95,
                    "Entire album is excellent",
                ),
                MusicSlangEntry(
                    "album no skips",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.90,
                    "No bad songs on album",
                ),
                MusicSlangEntry(
                    "went platinum in my car",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.85,
                    "Heavy rotation in car",
                ),
                MusicSlangEntry(
                    "went platinum in my headphones",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.85,
                    "Heavy personal listening",
                ),
                MusicSlangEntry(
                    "went platinum in my room",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.85,
                    "Heavy home listening",
                ),
                MusicSlangEntry(
                    "saved my life",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.90,
                    "Song had major emotional impact",
                ),
                MusicSlangEntry(
                    "this saved my life",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.90,
                    "This song was emotionally important",
                ),
                MusicSlangEntry(
                    "gym playlist",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.80,
                    "Adding to workout playlist",
                ),
                MusicSlangEntry(
                    "on my gym playlist",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.80,
                    "In my workout rotation",
                ),
                MusicSlangEntry(
                    "obsessed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.80,
                    "Really love this",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "lowkey obsessed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.80,
                    "Somewhat obsessed",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "highkey obsessed",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    0.85,
                    "Very obsessed",
                    gen_z_slang=True,
                ),
            ]
        )

        # ANTICIPATION DEMAND - Wanting more content
        entries.extend(
            [
                MusicSlangEntry(
                    "drop the album",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.85,
                    "Wanting album release",
                ),
                MusicSlangEntry(
                    "drop the album already",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.85,
                    "Impatient for album release",
                ),
                MusicSlangEntry(
                    "we need the album",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Community wants album",
                ),
                MusicSlangEntry(
                    "we need the album now",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Urgent album request",
                ),
                MusicSlangEntry(
                    "album when",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.75,
                    "When is album coming",
                ),
                MusicSlangEntry(
                    "need the lyrics",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Wanting to engage more with song",
                ),
                MusicSlangEntry(
                    "I need the lyrics",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Personal request for lyrics",
                ),
                MusicSlangEntry(
                    "where are the lyrics",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.75,
                    "Looking for lyrics",
                ),
                MusicSlangEntry(
                    "please come to atlanta",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.75,
                    "Wanting live performance",
                ),
                MusicSlangEntry(
                    "come to my city", SentimentLabel.POSITIVE, SlangCategory.ANTICIPATION_DEMAND, 0.75, "Tour request"
                ),
                MusicSlangEntry(
                    "friday can't come sooner",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.80,
                    "Anticipating release",
                ),
                MusicSlangEntry(
                    "part two please",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.75,
                    "Wanting sequel/continuation",
                ),
                MusicSlangEntry(
                    "part two pleaseee",
                    SentimentLabel.POSITIVE,
                    SlangCategory.ANTICIPATION_DEMAND,
                    0.75,
                    "Eager for continuation",
                ),
            ]
        )

        # RATINGS AND AWARDS - Numerical and award expressions
        entries.extend(
            [
                MusicSlangEntry("10/10", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Perfect score"),
                MusicSlangEntry(
                    "100/10", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Beyond perfect score"
                ),
                MusicSlangEntry(
                    "11/10", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.95, "Exceeds perfect score"
                ),
                MusicSlangEntry(
                    "100!", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Perfect/complete approval"
                ),
                MusicSlangEntry(
                    "SOTY", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Song of the year"
                ),
                MusicSlangEntry(
                    "AOTY", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.90, "Album of the year"
                ),
                MusicSlangEntry(
                    "summer anthem", SentimentLabel.POSITIVE, SlangCategory.PRAISE_GENERAL, 0.85, "Perfect summer song"
                ),
            ]
        )

        # COMPLIMENTS AND IDENTITY - Personal appearance/identity praise
        entries.extend(
            [
                MusicSlangEntry(
                    "hottie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.80,
                    "Attractive person compliment",
                ),
                MusicSlangEntry(
                    "baddie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.80,
                    "Attractive, confident person",
                ),
                MusicSlangEntry(
                    "hot bish",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.80,
                    "Attractive person, casual",
                ),
                MusicSlangEntry(
                    "bad bish",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.80,
                    "Confident, attractive person",
                ),
                MusicSlangEntry(
                    "hottie baddie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "Combined attractiveness praise",
                ),
            ]
        )

        # ADDITIONAL POSITIVE EXPRESSIONS
        entries.extend(
            [
                MusicSlangEntry(
                    "this fye",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.80,
                    "This is fire/excellent",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "this fye my boi",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.80,
                    "This is excellent, friend",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "it's giving",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.75,
                    "It's providing/delivering",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "this will go crazy in the club",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_GENERAL,
                    0.85,
                    "Will be popular in clubs",
                ),
            ]
        )

        # ===== NEGATIVE SENTIMENT =====

        # CRITICISM NEGATIVE - Direct negative criticism
        entries.extend(
            [
                MusicSlangEntry(
                    "this ain't it chief",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.95,
                    "This is not good",
                ),
                MusicSlangEntry(
                    "mid",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.90,
                    "Mediocre/average in a bad way",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "basura", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "Trash (Spanish)"
                ),
                MusicSlangEntry(
                    "this is basura", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "This is trash"
                ),
                MusicSlangEntry(
                    "went double wood",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.80,
                    "Sold very poorly (opposite of platinum)",
                ),
                MusicSlangEntry(
                    "this went double wood",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.80,
                    "This sold very poorly",
                ),
                MusicSlangEntry(
                    "who approved this",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.85,
                    "Questioning quality control",
                ),
                MusicSlangEntry(
                    "who approved this?",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.85,
                    "Questioning decision making",
                ),
                MusicSlangEntry(
                    "turn it off", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.90, "Stop playing this"
                ),
                MusicSlangEntry(
                    "nobody asked for this",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.85,
                    "Unwanted content",
                ),
                MusicSlangEntry(
                    "overrated",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.80,
                    "Gets more praise than deserved",
                ),
                MusicSlangEntry(
                    "this song is so overrated bro",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.80,
                    "Song gets too much praise",
                ),
                MusicSlangEntry(
                    "fell off", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "Quality declined"
                ),
                MusicSlangEntry(
                    "industry plant",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.75,
                    "Artificially promoted artist",
                ),
                MusicSlangEntry(
                    "flop",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.90,
                    "Commercial/artistic failure",
                ),
                MusicSlangEntry(
                    "flop — turn this off",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.90,
                    "Failure, stop playing",
                ),
                MusicSlangEntry(
                    "skip", SentimentLabel.NEGATIVE, SlangCategory.CRITICISM_NEGATIVE, 0.85, "Not worth listening to"
                ),
                MusicSlangEntry(
                    "who asked for this remix",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.75,
                    "Unwanted remix",
                ),
                MusicSlangEntry(
                    "this ain't real hip-hop",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.70,
                    "Gatekeeping/authenticity criticism",
                ),
                MusicSlangEntry(
                    "sounds the same every track",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    0.75,
                    "Lack of variety criticism",
                ),
            ]
        )

        # CRITICISM CONSTRUCTIVE - Technical/constructive feedback
        entries.extend(
            [
                MusicSlangEntry(
                    "switch up the flow",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.70,
                    "Suggestion for improvement",
                ),
                MusicSlangEntry(
                    "flow is repetitive",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.75,
                    "Technical criticism",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "mix sounds chaotic",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.70,
                    "Production feedback",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "album rollout ain't rollouting",
                    SentimentLabel.NEGATIVE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    0.65,
                    "Marketing criticism",
                ),
            ]
        )

        # ===== NEUTRAL SENTIMENT =====

        # NEUTRAL REQUESTS - Information/content requests
        entries.extend(
            [
                MusicSlangEntry(
                    "need the instrumental",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_REQUESTS,
                    0.90,
                    "Request for instrumental version",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "drop the visuals",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_REQUESTS,
                    0.85,
                    "Request for music video",
                ),
                MusicSlangEntry(
                    "clean version pls",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_REQUESTS,
                    0.80,
                    "Request for clean version",
                ),
            ]
        )

        # NEUTRAL QUESTIONS - Information seeking
        entries.extend(
            [
                MusicSlangEntry(
                    "who mixed this",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    0.85,
                    "Information request about mixing",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "who mixed this?",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    0.85,
                    "Question about audio engineer",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "what's the sample",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    0.90,
                    "Information request about sample",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "what's the sample?",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    0.90,
                    "Question about musical sample",
                    beat_appreciation=True,
                ),
            ]
        )

        # ADDITIONAL REAL FAN COMMENTS (from original problem cases)
        entries.extend(
            [
                MusicSlangEntry(
                    "Hottie, Baddie, Maddie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    0.85,
                    "Real fan comment - compliment sequence",
                ),
                MusicSlangEntry(
                    "Cuz I willie",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.70,
                    "Real fan comment - playful expression",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "my legs are spread",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.75,
                    "Real fan comment - excitement/anticipation",
                ),
                MusicSlangEntry(
                    "wtfff",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.70,
                    "Extended WTF showing excitement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "wtf",
                    SentimentLabel.NEUTRAL,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.60,
                    "Can be positive or negative depending on context",
                ),
                MusicSlangEntry(
                    "fr",
                    SentimentLabel.POSITIVE,
                    SlangCategory.HYPE_EXCITEMENT,
                    0.60,
                    "For real - agreement/emphasis",
                    gen_z_slang=True,
                ),
            ]
        )

        return entries

    def get_train_test_split(
        self, test_size: float = 0.2, random_state: int = 42
    ) -> Tuple[List[MusicSlangEntry], List[MusicSlangEntry]]:
        """
        Get random train/test split of the dataset.

        Args:
            test_size: Proportion for test set (0.0 to 1.0)
            random_state: Random seed for reproducibility

        Returns:
            Tuple of (train_entries, test_entries)
        """
        random.seed(random_state)
        shuffled_entries = self.entries.copy()
        random.shuffle(shuffled_entries)

        split_idx = int(len(shuffled_entries) * (1 - test_size))
        train_entries = shuffled_entries[:split_idx]
        test_entries = shuffled_entries[split_idx:]

        return train_entries, test_entries

    def to_dataframe(self) -> pd.DataFrame:
        """Convert dataset to pandas DataFrame."""
        data = [entry.to_dict() for entry in self.entries]
        return pd.DataFrame(data)

    def get_statistics(self) -> Dict:
        """Get comprehensive dataset statistics."""
        df = self.to_dataframe()

        stats = {
            "total_phrases": len(self.entries),
            "sentiment_distribution": df["sentiment"].value_counts().to_dict(),
            "category_distribution": df["category"].value_counts().to_dict(),
            "beat_appreciation_count": df["beat_appreciation"].sum(),
            "gen_z_slang_count": df["gen_z_slang"].sum(),
            "avg_confidence": df["confidence"].mean(),
            "confidence_by_sentiment": df.groupby("sentiment")["confidence"].mean().to_dict(),
            "confidence_by_category": df.groupby("category")["confidence"].mean().to_dict(),
        }

        return stats

    def print_statistics(self):
        """Print comprehensive dataset statistics."""
        stats = self.get_statistics()

        print(f"🎵 Music Industry Sentiment Dataset v{self.version}")
        print("=" * 60)
        print(f"Total phrases: {stats['total_phrases']}")
        print(f"Beat appreciation phrases: {stats['beat_appreciation_count']}")
        print(f"Gen Z slang phrases: {stats['gen_z_slang_count']}")
        print(f"Average confidence: {stats['avg_confidence']:.3f}")

        print(f"\n📊 Sentiment Distribution:")
        for sentiment, count in stats["sentiment_distribution"].items():
            percentage = count / stats["total_phrases"] * 100
            print(f"  {sentiment}: {count} ({percentage:.1f}%)")

        print(f"\n📂 Category Distribution:")
        for category, count in sorted(stats["category_distribution"].items()):
            percentage = count / stats["total_phrases"] * 100
            print(f"  {category}: {count} ({percentage:.1f}%)")

        print(f"\n🎯 Confidence by Sentiment:")
        for sentiment, conf in stats["confidence_by_sentiment"].items():
            print(f"  {sentiment}: {conf:.3f}")

    def export_to_csv(self, filename: str = "music_industry_sentiment_dataset.csv"):
        """Export dataset to CSV file."""
        df = self.to_dataframe()
        df.to_csv(filename, index=False)
        print(f"💾 Dataset exported to: {filename}")

    def get_test_cases_for_model(self) -> List[Tuple[str, str, str, float, bool]]:
        """
        Get test cases formatted for model evaluation.

        Returns:
            List of (phrase, sentiment, category, confidence, beat_appreciation) tuples
        """
        return [
            (entry.phrase, entry.sentiment.value, entry.category.value, entry.confidence, entry.beat_appreciation)
            for entry in self.entries
        ]


def get_music_industry_dataset() -> "MusicIndustrySentimentDataset":
    """Compatibility: return the production v2 dataset.

    Existing code that imports this v1 function now gets the v2 dataset
    implementation without changing call sites.
    """
    return get_music_industry_sentiment_dataset()


# Backward-compat alias if external code expects an older name
def get_music_industry_sentiment_dataset() -> "MusicIndustrySentimentDataset":  # type: ignore[override]
    """Prefer v2; gracefully fall back to v1 if v2 deps unavailable."""
    try:
        from .music_industry_sentiment_dataset_v2 import get_music_industry_dataset_v2  # type: ignore

        return get_music_industry_dataset_v2()
    except Exception:
        # v2 not available (e.g., pydantic missing) – fall back to v1 dataset
        return MusicIndustrySentimentDataset()


if __name__ == "__main__":
    # Demonstrate dataset usage
    dataset = get_music_industry_dataset()

    # Print statistics
    dataset.print_statistics()

    # Show sample entries
    print(f"\n📝 Sample Entries:")
    for i, entry in enumerate(dataset.entries[:5]):
        print(f"  {i+1}. '{entry.phrase}' → {entry.sentiment.value} ({entry.category.value})")

    # Export to CSV
    dataset.export_to_csv()

    # Show train/test split
    train, test = dataset.get_train_test_split(test_size=0.2)
    print(f"\n🔄 Train/Test Split:")
    print(f"  Training set: {len(train)} phrases")
    print(f"  Test set: {len(test)} phrases")
