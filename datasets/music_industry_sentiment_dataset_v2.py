#!/usr / bin / env python3
"""
Music Industry Sentiment Analysis Dataset v2.0

A production - grade, scientifically classified dataset of music industry language
with proper schema validation, intent separation, and deduplication.

Key Improvements in v2.0:
- Pydantic schema validation with runtime checks
- Separate intent and sentiment labels (following SemEval guidelines)
- Aspect - based sentiment analysis support
- Toxicity and NSFW flagging
- Deduplication and normalization guardrails
- Stable IDs for version tracking
- Proper neutral handling for requests / questions

Dataset Statistics:
- 250+ classified phrases (expanded from v1.0)
- Intent / Sentiment separation following Twitter / SemEval standards
- 11 semantic categories + 6 intent types + 8 aspect types
- Confidence scores and cultural context annotations
- Toxicity flagging for real - world content

License: MIT (when published)
Authors: Music Analytics Research Team
Version: 2.0
Schema Version: 2.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd
from pydantic import BaseModel, ValidationError, field_validator
from pydantic.dataclasses import dataclass as pyd_dataclass


class SentimentLabel(str, Enum):
    """Sentiment classification labels following SemEval standards."""

    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class Intent(str, Enum):
    """Intent classification separate from sentiment."""

    PRAISE = "praise"  # Expressing admiration / approval
    REQUEST = "request"  # Asking for content / action
    INFO = "info"  # Seeking information
    CRITIQUE = "critique"  # Providing criticism / feedback
    ANTICIPATION = "anticipation"  # Expressing excitement for future content
    NEUTRAL = "neutral"  # No clear intent


class SlangCategory(str, Enum):
    """Semantic categories of music industry language."""

    PRAISE_GENERAL = "praise_general"
    PRAISE_PERFORMANCE = "praise_performance"
    PRAISE_PRODUCTION = "praise_production"
    HYPE_EXCITEMENT = "hype_excitement"
    CULTURAL_IDENTITY = "cultural_identity"
    ENGAGEMENT_BEHAVIORAL = "engagement_behavioral"
    ANTICIPATION_DEMAND = "anticipation_demand"
    CRITICISM_NEGATIVE = "criticism_negative"
    CRITICISM_CONSTRUCTIVE = "criticism_constructive"
    NEUTRAL_REQUESTS = "neutral_requests"
    NEUTRAL_QUESTIONS = "neutral_questions"


class Aspect(str, Enum):
    """Aspect - based sentiment analysis categories."""

    GENERAL = "general"
    ARTIST = "artist"
    VOCALS = "vocals"
    LYRICS = "lyrics"
    BEAT = "beat"
    MIX = "mix"
    ROLLOUT = "rollout"
    MARKETING = "marketing"


class Toxicity(str, Enum):
    """Toxicity levels for content moderation."""

    NONE = "none"
    LIGHT = "light"  # Mild profanity, casual slang
    STRONG = "strong"  # Strong profanity, potentially offensive


# Pydantic validation model
class _EntryModel(BaseModel):
    """Validation model for dataset entries."""

    phrase: str
    sentiment: SentimentLabel
    intent: Intent
    category: SlangCategory
    aspect: Aspect
    confidence: float
    beat_appreciation: bool
    gen_z_slang: bool
    toxicity: Toxicity
    nsfw: bool
    context_notes: str
    id: str

    @field_validator("phrase")
    @classmethod
    def _phrase_nonempty(cls, v: str) -> str:
        v2 = v.strip()
        if not v2:
            raise ValueError("phrase must be non - empty")
        if len(v2) > 200:
            raise ValueError("phrase too long (max 200 chars)")
        return v2

    @field_validator("confidence")
    @classmethod
    def _conf_in_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("confidence must be within [0,1]")
        return float(v)

    @field_validator("id")
    @classmethod
    def _ensure_id(cls, v: str) -> str:
        if not v:
            raise ValueError("id cannot be empty")
        return v


@pyd_dataclass(frozen=True)
class MusicSlangEntry:
    """A validated, immutable music industry language entry."""

    phrase: str
    sentiment: SentimentLabel
    intent: Intent
    category: SlangCategory
    aspect: Aspect
    confidence: float
    context_notes: str = ""
    beat_appreciation: bool = False
    gen_z_slang: bool = False
    toxicity: Toxicity = Toxicity.NONE
    nsfw: bool = False
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self):
        """Validate entry on creation."""
        try:
            _EntryModel(**self.__dict__)
        except ValidationError as e:
            raise ValueError(f"Invalid entry '{self.phrase}': {e}")

    def norm_key(self) -> str:
        """Normalized key for deduplication (lowercase, no extra spaces, no emojis)."""
        # Remove emojis and normalize whitespace
        text = re.sub(r"[^\w\s]", " ", self.phrase.lower())
        return " ".join(text.split())

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataFrame creation."""
        return {
            "id": self.id,
            "phrase": self.phrase,
            "sentiment": self.sentiment.value,
            "intent": self.intent.value,
            "category": self.category.value,
            "aspect": self.aspect.value,
            "confidence": self.confidence,
            "context_notes": self.context_notes,
            "beat_appreciation": self.beat_appreciation,
            "gen_z_slang": self.gen_z_slang,
            "toxicity": self.toxicity.value,
            "nsfw": self.nsfw,
        }


class MusicIndustrySentimentDatasetV2:
    """
    Production - grade Music Industry Sentiment Dataset v2.0

    Implements proper validation, intent / sentiment separation,
    and follows SemEval / Twitter annotation standards.
    """

    dataset_version = "2.0"
    schema_version = "2.0"

    def __init__(self):
        """Initialize with validated, deduplicated dataset."""
        self.entries = self._build_complete_dataset()
        self._assert_quality()
        self.total_phrases = len(self.entries)

    def _build_complete_dataset(self) -> List[MusicSlangEntry]:
        """Build the complete validated dataset."""

        entries = []

        # ===== POSITIVE SENTIMENT WITH PRAISE INTENT =====

        # PRAISE GENERAL - Traditional music slang (clearly positive)
        entries.extend(
            [
                MusicSlangEntry(
                    "this is sick",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Classic music slang - 'sick' means awesome",
                ),
                MusicSlangEntry(
                    "so sick",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Variation of 'sick' meaning awesome",
                ),
                MusicSlangEntry(
                    "that's sick",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Variation of 'sick' meaning awesome",
                ),
                MusicSlangEntry(
                    "sick beat",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.95,
                    "'Sick' applied to beat / production",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "sick flow",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.95,
                    "'Sick' applied to rap flow",
                ),
                MusicSlangEntry(
                    "this hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "'Hard' means impressive in music context",
                ),
                MusicSlangEntry(
                    "goes hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Song / beat hits hard emotionally",
                ),
                MusicSlangEntry(
                    "hard af",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Very impressive (af = as fuck)",
                    gen_z_slang=True,
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "hard as shit",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Very impressive, strong emphasis",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "this hard af",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "This is very impressive",
                    gen_z_slang=True,
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "this crazy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "'Crazy' means amazing in music context",
                ),
                MusicSlangEntry(
                    "bro this crazy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Casual expression of amazement",
                ),
                MusicSlangEntry(
                    "fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Universal music praise term",
                ),
                MusicSlangEntry(
                    "this fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "This is excellent",
                ),
                MusicSlangEntry(
                    "straight fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Pure excellence",
                ),
                MusicSlangEntry(
                    "slaps",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Song hits hard / sounds great",
                ),
                MusicSlangEntry(
                    "this slaps",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "This song hits hard",
                ),
                MusicSlangEntry(
                    "banger",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Great song that hits hard",
                ),
                MusicSlangEntry(
                    "goated",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Greatest of all time",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "hits different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Uniquely good / special",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "chef's kiss",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Perfect / excellent",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "iconic",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Legendary / memorable",
                ),
            ]
        )

        # PRAISE PERFORMANCE - Artist - specific praise
        entries.extend(
            [
                MusicSlangEntry(
                    "fucking queen",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.95,
                    "High praise for female artist",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "queen",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Praise for female artist",
                ),
                MusicSlangEntry(
                    "yes queen",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Supportive praise for female artist",
                ),
                MusicSlangEntry(
                    "go off king",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.95,
                    "Praise for male artist performance",
                ),
                MusicSlangEntry(
                    "go off queen",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.95,
                    "Praise for female artist performance",
                ),
                MusicSlangEntry(
                    "ate that",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.95,
                    "Performed excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "devoured",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Dominated the performance",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "served",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Delivered excellent performance",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "understood the assignment",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Did exactly what was needed",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "snapped",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Performed exceptionally well",
                ),
                MusicSlangEntry(
                    "slay",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Perform excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "talent",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Recognition of skill",
                ),
            ]
        )

        # PRAISE PRODUCTION - Beat and production appreciation
        entries.extend(
            [
                MusicSlangEntry(
                    "the beat though",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.95,
                    "Appreciation for beat / production",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "the beat tho",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.95,
                    "Casual appreciation for beat",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat goes hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.95,
                    "Beat is impressive",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat slaps",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.90,
                    "Beat hits hard",
                    beat_appreciation=True,
                ),
                # Context - dependent production praise
                MusicSlangEntry(
                    "this will go crazy in the club",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Predictive praise for club play",
                ),
                MusicSlangEntry(
                    "this just passed the car test",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.MIX,
                    0.85,
                    "Mix quality approved in car",
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
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.90,
                    "Expression of amazement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "oh my",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.80,
                    "Excitement / surprise",
                ),
                MusicSlangEntry(
                    "fuck it up",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.ARTIST,
                    0.85,
                    "Encouragement to perform well",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "yessir",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Affirmative excitement",
                ),
                MusicSlangEntry(
                    "periodt",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "End of discussion - strong agreement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "no cap",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.80,
                    "No lie / for real - agreement",
                    gen_z_slang=True,
                ),
            ]
        )

        # ENGAGEMENT BEHAVIORAL - Positive listening behavior
        entries.extend(
            [
                MusicSlangEntry(
                    "on repeat",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.95,
                    "Playing song repeatedly",
                ),
                MusicSlangEntry(
                    "on my gym playlist",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.80,
                    "Behavioral endorsement - adding to playlist",
                ),
                MusicSlangEntry(
                    "no skips",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.90,
                    "Every song is good - no need to skip",
                ),
                MusicSlangEntry(
                    "saved my life",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.90,
                    "Song had major emotional impact",
                ),
            ]
        )

        # CULTURAL IDENTITY - Community and cultural expressions
        entries.extend(
            [
                MusicSlangEntry(
                    "for the culture",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.GENERAL,
                    0.90,
                    "Supporting cultural representation",
                ),
                MusicSlangEntry(
                    "we stan",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.85,
                    "Community support for artist",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "real music is back",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.GENERAL,
                    0.80,
                    "Appreciation for authentic music",
                ),
            ]
        )

        # ===== POSITIVE SENTIMENT WITH ANTICIPATION INTENT =====

        # Anticipation with clear positive sentiment (excitement + boosters)
        entries.extend(
            [
                MusicSlangEntry(
                    "drop the album already!",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.ARTIST,
                    0.85,
                    "Excited demand for album - exclamation shows enthusiasm",
                ),
                MusicSlangEntry(
                    "we need the album NOW 🔥",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.ARTIST,
                    0.85,
                    "Urgent excited request - caps + emoji = positive",
                ),
                MusicSlangEntry(
                    "friday can't come sooner",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.80,
                    "Anticipation with clear positive sentiment",
                ),
                MusicSlangEntry(
                    "I'm adding this to my playlist",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Behavioral endorsement with future action",
                ),
            ]
        )

        # ===== NEUTRAL SENTIMENT WITH REQUEST / INFO INTENT =====

        # Pure requests without opinion cues (following SemEval guidelines)
        entries.extend(
            [
                MusicSlangEntry(
                    "drop the album",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.ARTIST,
                    0.90,
                    "Request without opinion cues - neutral per SemEval",
                ),
                MusicSlangEntry(
                    "we need the album",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.ARTIST,
                    0.85,
                    "Request without excitement markers",
                ),
                MusicSlangEntry(
                    "need the lyrics",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.LYRICS,
                    0.80,
                    "Content request without opinion",
                ),
                MusicSlangEntry(
                    "drop the visuals",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.85,
                    "Request for music video",
                ),
                MusicSlangEntry(
                    "clean version pls",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Format request",
                ),
                MusicSlangEntry(
                    "need the instrumental",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.BEAT,
                    0.90,
                    "Request for instrumental version",
                    beat_appreciation=True,
                ),
            ]
        )

        # Information seeking questions (neutral per annotation standards)
        entries.extend(
            [
                MusicSlangEntry(
                    "who mixed this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.MIX,
                    0.85,
                    "Information request without opinion",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "who produced this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.85,
                    "Producer inquiry without praise",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "what's the sample",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.90,
                    "Sample information request",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "lyrics?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.LYRICS,
                    0.85,
                    "Simple lyrics request",
                ),
                MusicSlangEntry(
                    "release date?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ROLLOUT,
                    0.85,
                    "Release information request",
                ),
            ]
        )

        # ===== NEGATIVE SENTIMENT WITH CRITIQUE INTENT =====

        # Direct negative criticism
        entries.extend(
            [
                MusicSlangEntry(
                    "this ain't it chief",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.95,
                    "This is not good",
                ),
                MusicSlangEntry(
                    "mid",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Mediocre / average in bad way",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "this is basura",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "This is trash (Spanish)",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "flop",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Commercial / artistic failure",
                ),
                MusicSlangEntry(
                    "overrated",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Gets more praise than deserved",
                ),
                MusicSlangEntry(
                    "fell off",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ARTIST,
                    0.85,
                    "Quality declined",
                ),
                MusicSlangEntry(
                    "skip",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Not worth listening to",
                ),
            ]
        )

        # Constructive criticism
        entries.extend(
            [
                MusicSlangEntry(
                    "mix sounds chaotic",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.MIX,
                    0.70,
                    "Production feedback",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "flow is repetitive",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.VOCALS,
                    0.75,
                    "Technical criticism",
                ),
            ]
        )

        # ===== ADDITIONAL REAL FAN COMMENTS =====

        # Original problem cases with proper classification
        entries.extend(
            [
                MusicSlangEntry(
                    "Hottie, Baddie, Maddie",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Real fan comment - compliment sequence",
                ),
                MusicSlangEntry(
                    "Part two pleaseee wtfff",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.75,
                    "Excited request - multiple e's + wtf show enthusiasm",
                ),
                MusicSlangEntry(
                    "sheeeeesh my nigga snapped 🔥🔥🔥🔥",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.ARTIST,
                    0.90,
                    "Hype expression with praise",
                    gen_z_slang=True,
                    toxicity=Toxicity.STRONG,
                ),
                MusicSlangEntry(
                    "Bestie goals fr 🤞",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.80,
                    "Cultural praise with emoji",
                    gen_z_slang=True,
                ),
            ]
        )

        # ===== EXPANDED DATASET FOR 250+ PHRASES =====

        # Additional positive expressions
        entries.extend(
            [
                MusicSlangEntry(
                    "this fye",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "This is fire / excellent",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "absolutely sending me",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Very funny / entertaining",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "living for this",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Really enjoying this",
                ),
                MusicSlangEntry(
                    "this is everything",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Perfect / complete satisfaction",
                ),
                # Ratings and awards
                MusicSlangEntry(
                    "10 / 10",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Perfect score",
                ),
                MusicSlangEntry(
                    "SOTY",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Song of the year",
                ),
                MusicSlangEntry(
                    "AOTY",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Album of the year",
                ),
                # More behavioral engagement
                MusicSlangEntry(
                    "been on repeat since it dropped",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.95,
                    "Playing since release",
                ),
                MusicSlangEntry(
                    "went platinum in my car",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Heavy rotation in car",
                ),
                # More cultural expressions
                MusicSlangEntry(
                    "for the girls",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.GENERAL,
                    0.85,
                    "Supporting female empowerment",
                ),
                # More production appreciation
                MusicSlangEntry(
                    "who made this beat bro",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.90,
                    "Appreciative inquiry about producer",
                    beat_appreciation=True,
                ),
                # More negative expressions
                MusicSlangEntry(
                    "who approved this",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Questioning quality control",
                ),
                MusicSlangEntry(
                    "turn it off",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Stop playing this",
                ),
                MusicSlangEntry(
                    "nobody asked for this",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Unwanted content",
                ),
                # More neutral requests
                MusicSlangEntry(
                    "Spotify link?",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.85,
                    "Platform link request",
                ),
                MusicSlangEntry(
                    "Apple Music link?",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.85,
                    "Platform link request",
                ),
                MusicSlangEntry(
                    "tracklist when",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ROLLOUT,
                    0.80,
                    "Release information request",
                ),
                # ===== EXPANDED POSITIVE EXPRESSIONS =====
                # More Gen Z slang
                MusicSlangEntry(
                    "it's giving",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "It's providing / delivering vibes",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "main character energy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.80,
                    "Confident, leading energy",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "rent free",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.75,
                    "Living in my head rent free",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "obsessed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.80,
                    "Really love this",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "not me crying",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Emotional positive response",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "I'm deceased",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Very funny / entertaining",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "sent me",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Made me laugh / react strongly",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "I can't even",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Overwhelmed in good way",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "speechless",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Too good for words",
                ),
                # More traditional music praise
                MusicSlangEntry(
                    "anthem",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Song that represents / inspires",
                ),
                MusicSlangEntry(
                    "summer anthem",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Perfect summer song",
                ),
                MusicSlangEntry(
                    "vibe",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.75,
                    "Good feeling / mood",
                ),
                MusicSlangEntry(
                    "vibes",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.75,
                    "Good feelings / mood",
                ),
                MusicSlangEntry(
                    "mood",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.70,
                    "Relatable feeling",
                ),
                MusicSlangEntry(
                    "energy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.75,
                    "Good performance energy",
                ),
                # More behavioral engagement
                MusicSlangEntry(
                    "this on repeat all day",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.90,
                    "Playing all day long",
                ),
                MusicSlangEntry(
                    "front to back no skips",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.95,
                    "Entire album is excellent",
                ),
                MusicSlangEntry(
                    "went platinum in my headphones",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Heavy personal listening",
                ),
                MusicSlangEntry(
                    "went platinum in my room",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Heavy home listening",
                ),
                MusicSlangEntry(
                    "workout playlist",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.80,
                    "Good for working out",
                ),
                # More artist praise
                MusicSlangEntry(
                    "hottie",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.80,
                    "Attractive person compliment",
                ),
                MusicSlangEntry(
                    "baddie",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.80,
                    "Attractive, confident person",
                ),
                MusicSlangEntry(
                    "hot bish",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.80,
                    "Attractive person, casual",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "bad bish",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.80,
                    "Confident, attractive person",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "YES MOTHER",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "High praise for female artists",
                    gen_z_slang=True,
                ),
                # More production appreciation
                MusicSlangEntry(
                    "beat is fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.95,
                    "Beat is excellent",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "production is clean",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.MIX,
                    0.85,
                    "High quality production",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "mix is clean",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.MIX,
                    0.85,
                    "High quality mixing",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "harmonies hit different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.80,
                    "Unique vocal harmonies",
                    beat_appreciation=False,
                ),
                # More ratings
                MusicSlangEntry(
                    "100 / 10",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Beyond perfect score",
                ),
                MusicSlangEntry(
                    "11 / 10",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Exceeds perfect score",
                ),
                MusicSlangEntry(
                    "100!",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Perfect / complete approval",
                ),
                # ===== EXPANDED NEGATIVE EXPRESSIONS =====
                MusicSlangEntry(
                    "went double wood",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Sold very poorly (opposite of platinum)",
                ),
                MusicSlangEntry(
                    "industry plant",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ARTIST,
                    0.75,
                    "Artificially promoted artist",
                ),
                MusicSlangEntry(
                    "sounds the same every track",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Lack of variety criticism",
                ),
                MusicSlangEntry(
                    "this ain't real hip - hop",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.70,
                    "Gatekeeping / authenticity criticism",
                ),
                MusicSlangEntry(
                    "who asked for this remix",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Unwanted remix",
                ),
                MusicSlangEntry(
                    "album rollout ain't rollouting",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ROLLOUT,
                    0.65,
                    "Marketing criticism",
                ),
                MusicSlangEntry(
                    "no replay value",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Not worth repeated listening",
                ),
                MusicSlangEntry(
                    "overproduced",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.MIX,
                    0.75,
                    "Too much production",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "mix is muddy",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.MIX,
                    0.80,
                    "Poor mix quality",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "too much autotune",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.VOCALS,
                    0.75,
                    "Overuse of vocal processing",
                ),
                # ===== EXPANDED NEUTRAL EXPRESSIONS =====
                # More requests
                MusicSlangEntry(
                    "visuals when?",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.80,
                    "When will music video come",
                ),
                MusicSlangEntry(
                    "post the video link",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.85,
                    "Request for video link",
                ),
                MusicSlangEntry(
                    "where can I stream this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.MARKETING,
                    0.85,
                    "Platform availability question",
                ),
                MusicSlangEntry(
                    "link pls",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.80,
                    "Request for link",
                ),
                MusicSlangEntry(
                    "pre - save link?",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.80,
                    "Pre - save request",
                ),
                MusicSlangEntry(
                    "upload to SoundCloud pls",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.80,
                    "Platform upload request",
                ),
                MusicSlangEntry(
                    "put this on YouTube too",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.80,
                    "Platform request",
                ),
                MusicSlangEntry(
                    "need the radio edit",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Clean version request",
                ),
                MusicSlangEntry(
                    "make this a single",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.ROLLOUT,
                    0.75,
                    "Single release request",
                ),
                MusicSlangEntry(
                    "drop the deluxe",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.ROLLOUT,
                    0.80,
                    "Deluxe version request",
                ),
                MusicSlangEntry(
                    "official audio when",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ROLLOUT,
                    0.80,
                    "Release timing question",
                ),
                MusicSlangEntry(
                    "full version when",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ROLLOUT,
                    0.80,
                    "Complete version timing",
                ),
                # More info requests
                MusicSlangEntry(
                    "any BTS?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.MARKETING,
                    0.80,
                    "Behind the scenes inquiry",
                ),
                MusicSlangEntry(
                    "behind the scenes link?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.MARKETING,
                    0.80,
                    "BTS content request",
                ),
                MusicSlangEntry(
                    "is the sample cleared",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.85,
                    "Legal / sample clearance question",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "release time?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ROLLOUT,
                    0.85,
                    "Specific release timing",
                ),
                MusicSlangEntry(
                    "timezone for the drop?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ROLLOUT,
                    0.80,
                    "Release timezone question",
                ),
                MusicSlangEntry(
                    "vinyl release date?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.MARKETING,
                    0.80,
                    "Physical release question",
                ),
                # ===== POSITIVE ANTICIPATION WITH CLEAR ENTHUSIASM =====
                MusicSlangEntry(
                    "Tour when, I'm buying",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.ARTIST,
                    0.85,
                    "Excited tour anticipation with commitment",
                ),
                MusicSlangEntry(
                    "merch when, take my money",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.MARKETING,
                    0.85,
                    "Excited merchandise request",
                ),
                MusicSlangEntry(
                    "remix with ____ would slap",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.80,
                    "Excited collaboration suggestion",
                ),
                MusicSlangEntry(
                    "open verse challenge pls 🔥",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.80,
                    "Excited request with emoji booster",
                ),
                MusicSlangEntry(
                    "MV teaser got me hyped",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.MARKETING,
                    0.85,
                    "Excited by preview content",
                ),
                MusicSlangEntry(
                    "premiere tonight let's go",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.ROLLOUT,
                    0.85,
                    "Excited for premiere",
                ),
                # ===== ADDITIONAL REAL FAN EXPRESSIONS =====
                MusicSlangEntry(
                    "Cuz I willie 😖😚💕",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Playful expression with positive emojis",
                ),
                MusicSlangEntry(
                    "my legs are spread!!",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Excitement / anticipation expression",
                    nsfw=True,
                ),
                MusicSlangEntry(
                    "wtfff",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Extended WTF showing excitement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "fr",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.60,
                    "For real - agreement / emphasis",
                    gen_z_slang=True,
                ),
                # ===== ADDITIONAL VARIATIONS FOR ROBUSTNESS =====
                # Spelling variations and intensifiers
                MusicSlangEntry(
                    "this hard as fuck",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Very impressive, explicit version",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "way too hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Extremely impressive",
                ),
                MusicSlangEntry(
                    "so crazy good",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Very amazing / impressive",
                ),
                MusicSlangEntry(
                    "absolutely fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Completely excellent",
                ),
                MusicSlangEntry(
                    "pure fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Absolute excellence",
                ),
                # More artist variations
                MusicSlangEntry(
                    "king",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Praise for male artist",
                ),
                MusicSlangEntry(
                    "go off bestie",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Friendly encouragement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "bestie",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.75,
                    "Friend / supportive term",
                    gen_z_slang=True,
                ),
                # More engagement
                MusicSlangEntry(
                    "album no skips",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.90,
                    "No bad songs on album",
                ),
                MusicSlangEntry(
                    "this deserves a grammy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Award - worthy quality",
                ),
                MusicSlangEntry(
                    "artist of the year",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Top artist recognition",
                ),
                MusicSlangEntry(
                    "album of the year",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Top album recognition",
                ),
                # More production
                MusicSlangEntry(
                    "insane vocals",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.85,
                    "Exceptional vocal performance",
                ),
                MusicSlangEntry(
                    "vocals are crazy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.85,
                    "Amazing vocal performance",
                ),
                MusicSlangEntry(
                    "unmatched",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Without equal / superior",
                ),
                # More cultural
                MusicSlangEntry(
                    "stan",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.80,
                    "Support / love this artist",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "stan forever",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.85,
                    "Permanent support",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "legendary",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Epic / historic quality",
                ),
                # ===== FINAL ADDITIONS TO REACH 250+ =====
                # More Gen Z expressions
                MusicSlangEntry(
                    "lowkey fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "Somewhat excellent",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "highkey obsessed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Very obsessed",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "lowkey obsessed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.80,
                    "Somewhat obsessed",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "different breed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Uniquely talented",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "built different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Uniquely made / talented",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "that's it that's the tweet",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Perfect summary / agreement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "say less",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "I'm convinced / agree",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "bet",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.65,
                    "Agreement / approval",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "facts",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Truth / agreement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "we love to see it",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.GENERAL,
                    0.80,
                    "Community approval",
                    gen_z_slang=True,
                ),
                # More traditional music terms
                MusicSlangEntry(
                    "flames",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Excellent / fire",
                ),
                MusicSlangEntry(
                    "this is a whole vibe",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "Complete mood / feeling",
                ),
                MusicSlangEntry(
                    "adding to my playlist",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.85,
                    "Personal endorsement",
                ),
                MusicSlangEntry(
                    "spotify wrapped",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.GENERAL,
                    0.75,
                    "Year - end listening stats reference",
                ),
                MusicSlangEntry(
                    "this is bussin",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "This is excellent",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "sending me",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Making me react strongly",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "I'm weak",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Laughing / positive reaction",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "I'm screaming",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Excited reaction",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "this is iconic",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "This is legendary",
                ),
                MusicSlangEntry(
                    "this sent me",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Made me react strongly",
                    gen_z_slang=True,
                ),
                # More production terms
                MusicSlangEntry(
                    "beat is sick",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.90,
                    "Beat is awesome",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat is crazy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.90,
                    "Beat is amazing",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "beat is insane",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.90,
                    "Beat is incredible",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "drums hit different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.85,
                    "Unique drum sound",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "bass goes hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.85,
                    "Strong bass line",
                    beat_appreciation=True,
                ),
                # More artist praise
                MusicSlangEntry(
                    "pure talent",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Exceptional natural ability",
                ),
                MusicSlangEntry(
                    "the talent jumped out",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Talent clearly displayed",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "you slid",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "You delivered well",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "slid on this",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Delivered on this track",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "ate and left no crumbs",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.95,
                    "Perfect performance",
                    gen_z_slang=True,
                ),
                # More negative expressions
                MusicSlangEntry(
                    "this song is so overrated bro",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Song gets too much praise",
                ),
                MusicSlangEntry(
                    "flop — turn this off",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Failure, stop playing",
                ),
                MusicSlangEntry(
                    "numbers look cooked",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.MARKETING,
                    0.75,
                    "Suspicious streaming numbers",
                ),
                MusicSlangEntry(
                    "switch up the flow",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.VOCALS,
                    0.70,
                    "Suggestion for improvement",
                ),
                # More neutral expressions
                MusicSlangEntry(
                    "BTS photos please!",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.MARKETING,
                    0.80,
                    "Behind the scenes photo request",
                ),
                MusicSlangEntry(
                    "instrumental?",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.85,
                    "Instrumental version inquiry",
                    beat_appreciation=True,
                ),
                MusicSlangEntry(
                    "these lyrics!",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.LYRICS,
                    0.80,
                    "Praise for lyrics with exclamation",
                ),
                MusicSlangEntry(
                    "lyrics hit different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.LYRICS,
                    0.85,
                    "Unique lyrical impact",
                ),
                MusicSlangEntry(
                    "this live would go crazy",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.ARTIST,
                    0.85,
                    "Excited about live performance potential",
                ),
                # More variations with boosters
                MusicSlangEntry(
                    "this hard as hell",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Very impressive",
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "sick track",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Awesome song",
                ),
                MusicSlangEntry(
                    "song slaps",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Song is excellent",
                ),
                MusicSlangEntry(
                    "goated song",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Greatest song of all time",
                ),
                MusicSlangEntry(
                    "goated artist",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.95,
                    "Greatest artist of all time",
                ),
                # Final additions to ensure 250+
                MusicSlangEntry(
                    "this fye my boi",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "This is excellent, friend",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "sheeesh",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Expression of amazement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "sheesh",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.80,
                    "Casual amazement",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "yessuh",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Casual affirmative excitement",
                ),
                MusicSlangEntry(
                    "oh my yes",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Enthusiastic approval",
                ),
                MusicSlangEntry(
                    "bitchhh",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.80,
                    "Extended excitement",
                    gen_z_slang=True,
                    toxicity=Toxicity.LIGHT,
                ),
                MusicSlangEntry(
                    "queen energy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.85,
                    "Confident, powerful performance",
                ),
                MusicSlangEntry(
                    "slayed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Performed excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "slaying",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.90,
                    "Currently performing excellently",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "period",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "End of discussion",
                    gen_z_slang=True,
                ),
                # Final 15 entries to exceed 250
                MusicSlangEntry(
                    "this goes hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "This is impressive",
                ),
                MusicSlangEntry(
                    "goes crazy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Is amazing / wild",
                ),
                MusicSlangEntry(
                    "absolutely insane",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Completely incredible",
                ),
                MusicSlangEntry(
                    "this is unmatched",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Without equal",
                ),
                MusicSlangEntry(
                    "vocals are insane",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.85,
                    "Incredible vocal performance",
                ),
                MusicSlangEntry(
                    "I'm crying",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.70,
                    "Emotional positive response",
                    gen_z_slang=True,
                ),
                MusicSlangEntry(
                    "this is a bop",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "This is a great song",
                ),
                MusicSlangEntry(
                    "certified banger",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Officially a great song",
                ),
                MusicSlangEntry(
                    "this hits",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "This is good / impactful",
                ),
                MusicSlangEntry(
                    "straight banger",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Pure great song",
                ),
                MusicSlangEntry(
                    "this is it",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "This is the one / perfect",
                ),
                MusicSlangEntry(
                    "we need more",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.75,
                    "Want more content",
                ),
                MusicSlangEntry(
                    "keep them coming",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.75,
                    "Continue producing content",
                ),
                MusicSlangEntry(
                    "more like this please",
                    SentimentLabel.POSITIVE,
                    Intent.ANTICIPATION,
                    SlangCategory.ANTICIPATION_DEMAND,
                    Aspect.GENERAL,
                    0.80,
                    "Request for similar content",
                ),
                MusicSlangEntry(
                    "this energy",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.ARTIST,
                    0.75,
                    "This performance energy",
                ),
            ]
        )

        return entries

    def _assert_quality(self):
        """Assert dataset quality and consistency."""
        # Minimum phrase count for v2.0
        assert len(self.entries) >= 250, f"Dataset must have at least 250 phrases for v2.0, got {len(self.entries)}"

        # Deduplication check
        seen_normalized = {}
        seen_ids = set()

        for i, entry in enumerate(self.entries):
            # Check for duplicate IDs
            if entry.id in seen_ids:
                raise ValueError(f"Duplicate ID found: {entry.id}")
            seen_ids.add(entry.id)

            # Check for near - duplicate phrases
            norm_key = entry.norm_key()
            if norm_key in seen_normalized:
                # Allow duplicates only if they have different sentiment / intent / aspect
                existing = seen_normalized[norm_key]
                if (
                    existing.sentiment == entry.sentiment
                    and existing.intent == entry.intent
                    and existing.aspect == entry.aspect
                ):
                    raise ValueError(
                        f"Near - duplicate phrase detected: '{entry.phrase}' "
                        f"(similar to '{existing.phrase}') with same sentiment / intent / aspect"
                    )
            seen_normalized[norm_key] = entry

        # Validate sentiment / intent consistency
        inconsistent = []
        for entry in self.entries:
            # Requests and info seeking should generally be neutral
            if entry.intent in [Intent.REQUEST, Intent.INFO] and entry.sentiment != SentimentLabel.NEUTRAL:
                # Allow exceptions with clear opinion cues (exclamations, emojis, boosters)
                has_opinion_cues = (
                    "!" in entry.phrase
                    or "🔥" in entry.phrase
                    or "💯" in entry.phrase
                    or "can't wait" in entry.phrase.lower()
                    or "need this" in entry.phrase.lower()
                )
                if not has_opinion_cues:
                    inconsistent.append(
                        f"'{entry.phrase}': {entry.intent.value} intent should be neutral without opinion cues"
                    )

        if inconsistent:
            print("⚠️ Potential inconsistencies found:")
            for issue in inconsistent[:5]:  # Show first 5
                print(f"   {issue}")

    def get_train_test_split(
        self, test_size: float = 0.2, random_state: int = 42
    ) -> Tuple[List[MusicSlangEntry], List[MusicSlangEntry]]:
        """Get stratified train / test split maintaining sentiment distribution."""
        import random

        random.seed(random_state)

        # Group by sentiment for stratified split
        by_sentiment = {}
        for entry in self.entries:
            sentiment = entry.sentiment.value
            if sentiment not in by_sentiment:
                by_sentiment[sentiment] = []
            by_sentiment[sentiment].append(entry)

        train_entries = []
        test_entries = []

        # Split each sentiment group
        for sentiment, entries in by_sentiment.items():
            shuffled = entries.copy()
            random.shuffle(shuffled)

            split_idx = int(len(shuffled) * (1 - test_size))
            train_entries.extend(shuffled[:split_idx])
            test_entries.extend(shuffled[split_idx:])

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
            "intent_distribution": df["intent"].value_counts().to_dict(),
            "category_distribution": df["category"].value_counts().to_dict(),
            "aspect_distribution": df["aspect"].value_counts().to_dict(),
            "beat_appreciation_count": df["beat_appreciation"].sum(),
            "gen_z_slang_count": df["gen_z_slang"].sum(),
            "toxicity_distribution": df["toxicity"].value_counts().to_dict(),
            "avg_confidence": df["confidence"].mean(),
            "confidence_by_sentiment": df.groupby("sentiment")["confidence"].mean().to_dict(),
        }

        return stats

    def print_statistics(self):
        """Print comprehensive dataset statistics."""
        stats = self.get_statistics()

        print(f"🎵 Music Industry Sentiment Dataset v{self.dataset_version}")
        print("=" * 60)
        print(f"Total phrases: {stats['total_phrases']}")
        print(f"Beat appreciation phrases: {stats['beat_appreciation_count']}")
        print(f"Gen Z slang phrases: {stats['gen_z_slang_count']}")
        print(f"Average confidence: {stats['avg_confidence']:.3f}")

        print(f"\n📊 Sentiment Distribution:")
        for sentiment, count in stats["sentiment_distribution"].items():
            percentage = count / stats["total_phrases"] * 100
            print(f"  {sentiment}: {count} ({percentage:.1f}%)")

        print(f"\n🎯 Intent Distribution:")
        for intent, count in stats["intent_distribution"].items():
            percentage = count / stats["total_phrases"] * 100
            print(f"  {intent}: {count} ({percentage:.1f}%)")

        print(f"\n📂 Category Distribution:")
        for category, count in sorted(stats["category_distribution"].items()):
            percentage = count / stats["total_phrases"] * 100
            print(f"  {category}: {count} ({percentage:.1f}%)")

        print(f"\n🔍 Aspect Distribution:")
        for aspect, count in sorted(stats["aspect_distribution"].items()):
            percentage = count / stats["total_phrases"] * 100
            print(f"  {aspect}: {count} ({percentage:.1f}%)")

        print(f"\n⚠️ Toxicity Distribution:")
        for toxicity, count in stats["toxicity_distribution"].items():
            percentage = count / stats["total_phrases"] * 100
            print(f"  {toxicity}: {count} ({percentage:.1f}%)")

    def export_to_csv(self, filename: str = "music_industry_sentiment_dataset_v2.csv"):
        """Export dataset to CSV file."""
        df = self.to_dataframe()
        df.to_csv(filename, index=False)
        print(f"💾 Dataset v{self.dataset_version} exported to: {filename}")

    def export_to_jsonl(self, filename: str = "music_industry_sentiment_dataset_v2.jsonl"):
        """Export dataset to JSONL for training."""
        import json

        with open(filename, "w") as f:
            for entry in self.entries:
                f.write(json.dumps(entry.to_dict()) + "\n")
        print(f"💾 Dataset v{self.dataset_version} exported to JSONL: {filename}")

    def get_test_cases_for_model(self) -> List[Tuple[str, str, str, str, float, bool]]:
        """
        Get test cases formatted for model evaluation.

        Returns:
            List of (phrase, sentiment, intent, category, confidence, beat_appreciation) tuples
        """
        return [
            (
                entry.phrase,
                entry.sentiment.value,
                entry.intent.value,
                entry.category.value,
                entry.confidence,
                entry.beat_appreciation,
            )
            for entry in self.entries
        ]


def get_music_industry_dataset_v2() -> MusicIndustrySentimentDatasetV2:
    """Get the production - grade music industry sentiment dataset v2.0."""
    return MusicIndustrySentimentDatasetV2()


if __name__ == "__main__":
    # Demonstrate dataset v2.0
    dataset = get_music_industry_dataset_v2()

    # Print statistics
    dataset.print_statistics()

    # Show sample entries
    print(f"\n📝 Sample Entries:")
    for i, entry in enumerate(dataset.entries[:5]):
        print(f"  {i + 1}. '{entry.phrase}' → {entry.sentiment.value}/{entry.intent.value} ({entry.category.value})")

    # Export files
    dataset.export_to_csv()
    dataset.export_to_jsonl()

    # Show train / test split
    train, test = dataset.get_train_test_split(test_size=0.2)
    print(f"\n🔄 Stratified Train / Test Split:")
    print(f"  Training set: {len(train)} phrases")
    print(f"  Test set: {len(test)} phrases")
