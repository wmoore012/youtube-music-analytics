#!/usr / bin / env python3
"""
Enhanced Music Industry Sentiment Dataset with Deterministic Generation

Production-grade dataset with:
- Deterministic UUID-5 IDs for reproducibility
- Unicode-aware text normalization
- SemEval-aligned quality controls
- Pydantic v2 schema validation
"""

from __future__ import annotations

import json
import re
import sys
import threading
import unicodedata
from dataclasses import field
from enum import Enum
from functools import cached_property
from hashlib import sha256
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from pydantic import BaseModel, ValidationError, field_validator
from pydantic.dataclasses import dataclass as pyd_dataclass

# --------------------------- Schema Version Management ---------------------------

SCHEMA_VERSION = "2.1"
DATASET_VERSION = "2.1"
_NAMESPACE = uuid5(NAMESPACE_URL, f"https://project.local/sentiment/{SCHEMA_VERSION}")


# --------------------------- SemEval-aligned enums ---------------------------


class SentimentLabel(str, Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


class Intent(str, Enum):
    PRAISE = "praise"
    REQUEST = "request"
    INFO = "info"
    CRITIQUE = "critique"
    ANTICIPATION = "anticipation"
    NEUTRAL = "neutral"


class SlangCategory(str, Enum):
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
    GENERAL = "general"
    ARTIST = "artist"
    VOCALS = "vocals"
    LYRICS = "lyrics"
    BEAT = "beat"
    MIX = "mix"
    ROLLOUT = "rollout"
    MARKETING = "marketing"


class Toxicity(str, Enum):
    NONE = "none"
    LIGHT = "light"
    STRONG = "strong"


# --------------------------- Opinion Boosters (SemEval-style) ---------------------------

# Opinion boosters for SemEval-style "not-neutral" cues for requests / info
OPINION_BOOSTERS: Tuple[str, ...] = (
    "!",
    "🔥",
    "💯",
    "😍",
    "😭",
    "CAN'T WAIT",
    "NOW",
    "IMMEDIATELY",
    " ASAP ",
    "!!",
    "!!!",
    "PLEASE",
    "NEED",
    "WANT",
    "LOVE",
    "AMAZING",
    "PERFECT",
)


# --------------------------- Unicode Normalization ---------------------------

# Minimal emoji pattern (for production, use maintained UTS #51 pattern)
_EMOJI_SUBSET = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0001F1E6-\U0001F1FF]+", flags=re.UNICODE)

_WS = re.compile(r"\s+")


def normalize_text_for_key(text: str) -> str:
    """
    Unicode-aware normalization for dedup keys & stable IDs.
    - NFKC normalize
    - casefold
    - strip emoji subset
    - collapse whitespace
    """
    s0 = unicodedata.normalize("NFKC", text)
    s1 = _EMOJI_SUBSET.sub(" ", s0)
    s2 = s1.casefold()
    s3 = _WS.sub(" ", s2).strip()
    return s3


def normalize_text_for_transformer(text: str, preserve_emoji: bool = True) -> str:
    """
    Enhanced normalization for transformer models with emoji handling.
    - NFKC normalize
    - Optionally preserve emoji
    - Collapse whitespace
    - Preserve music slang case
    """
    # Music slang terms that should preserve case
    CASE_SENSITIVE_SLANG = ["GOATED", "PERIODT", "SLAY", "QUEEN", "KING", "MOTHER"]

    # Step 1: NFKC normalization
    s0 = unicodedata.normalize("NFKC", text)

    # Step 2: Handle emoji
    if preserve_emoji:
        # Keep emoji but normalize multiple consecutive ones
        s1 = re.sub(r"([\U0001F300-\U0001FAFF])\1{2,}", r"\1\1", s0)  # Max 2 consecutive
    else:
        s1 = _EMOJI_SUBSET.sub(" ", s0)

    # Step 3: Preserve case for specific slang terms
    preserved_terms = {}
    for term in CASE_SENSITIVE_SLANG:
        if term in s1.upper():
            # Find and preserve the term
            pattern = re.compile(re.escape(term), re.IGNORECASE)
            matches = pattern.finditer(s1)
            for match in matches:
                placeholder = f"__PRESERVE_{len(preserved_terms)}__"
                preserved_terms[placeholder] = term
                s1 = s1[: match.start()] + placeholder + s1[match.end() :]

    # Step 4: Collapse whitespace
    s2 = _WS.sub(" ", s1).strip()

    # Step 5: Restore preserved terms
    for placeholder, term in preserved_terms.items():
        s2 = s2.replace(placeholder, term)

    return s2


# --------------------------- Deterministic ID Generation ---------------------------


def generate_stable_id(phrase: str, sentiment: SentimentLabel, intent: Intent, aspect: Aspect) -> str:
    """Generate deterministic UUID-5 based ID for reproducible joins."""
    normalized_phrase = normalize_text_for_key(phrase)
    name = f"{normalized_phrase}|{sentiment.value}|{intent.value}|{aspect.value}"
    return str(uuid5(_NAMESPACE, name))


# --------------------------- Pydantic Validation Model ---------------------------


class _EntryModel(BaseModel):
    """Validation model for dataset entries (authoritative schema)."""

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

    model_config = {"extra": "forbid"}  # Fail on unknown fields

    @field_validator("phrase")
    @classmethod
    def _phrase_nonempty(cls, v: str) -> str:
        v2 = v.strip()
        if not v2:
            raise ValueError("phrase must be non-empty")
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


# --------------------------- Enhanced Dataset Entry ---------------------------


@pyd_dataclass(frozen=True)
class EnhancedMusicSlangEntry:
    """Enhanced, immutable music industry language entry with deterministic ID."""

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
    id: str = field(default="")  # Computed if empty

    def __post_init__(self) -> None:  # type: ignore[override]
        """Compute deterministic ID if absent, then validate via Pydantic."""
        if not self.id:
            stable_id = generate_stable_id(self.phrase, self.sentiment, self.intent, self.aspect)
            object.__setattr__(self, "id", stable_id)

        try:
            _EntryModel(**self.__dict__)
        except ValidationError as e:
            raise ValueError(f"Invalid entry '{self.phrase}': {e}")

    def norm_key(self) -> str:
        """Normalized key for deduplication using Unicode-aware normalization."""
        return normalize_text_for_key(self.phrase)

    def to_dict(self) -> Dict[str, object]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "id": self.id,
            "phrase": self.phrase,
            "sentiment": self.sentiment.value,
            "intent": self.intent.value,
            "category": self.category.value,
            "aspect": self.aspect.value,
            "confidence": float(self.confidence),
            "context_notes": self.context_notes,
            "beat_appreciation": bool(self.beat_appreciation),
            "gen_z_slang": bool(self.gen_z_slang),
            "toxicity": self.toxicity.value,
            "nsfw": bool(self.nsfw),
        }


# --------------------------- Enhanced Dataset Class ---------------------------


class EnhancedMusicSentimentDatasetV2:
    """
    Production-grade dataset with deterministic generation, quality controls,
    and comprehensive validation following SemEval standards.
    """

    dataset_version: str = DATASET_VERSION
    schema_version: str = SCHEMA_VERSION

    def __init__(self) -> None:
        self.entries: List[EnhancedMusicSlangEntry] = self._build_complete_dataset()
        self._assert_quality()

    def _build_complete_dataset(self) -> List[EnhancedMusicSlangEntry]:
        """Build the complete enhanced dataset with improved entries."""

        entries: List[EnhancedMusicSlangEntry] = []

        # ===== POSITIVE SENTIMENT WITH PRAISE INTENT =====

        # Music slang (addressing baseline gaps)
        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "this is sick",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Classic music slang - 'sick' means awesome / cool",
                ),
                EnhancedMusicSlangEntry(
                    "this slaps",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Song hits hard / sounds great",
                ),
                EnhancedMusicSlangEntry(
                    "straight fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Pure excellence",
                ),
                EnhancedMusicSlangEntry(
                    "goes hard",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Song / beat hits hard emotionally",
                ),
                EnhancedMusicSlangEntry(
                    "banger",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.95,
                    "Great song that hits hard",
                ),
                EnhancedMusicSlangEntry(
                    "goated",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Greatest of all time",
                    gen_z_slang=True,
                ),
                # New entries based on your feedback
                EnhancedMusicSlangEntry(
                    "gas",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Gas / gas! means fire / excellent-often with ⛽️ emoji",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "get my son on trending",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.85,
                    "Playful support-calling favorite artist 'son' while rooting for success",
                ),
                EnhancedMusicSlangEntry(
                    "I relate to this so much",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.LYRICS,
                    0.80,
                    "Personal connection and emotional resonance with content",
                ),
                EnhancedMusicSlangEntry(
                    "dopest artists out",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.ARTIST,
                    0.90,
                    "High praise - 'dopest' means best / coolest",
                ),
                EnhancedMusicSlangEntry(
                    "modern beauty with vintage voice",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.85,
                    "Sophisticated praise combining contemporary and classic elements",
                ),
                EnhancedMusicSlangEntry(
                    "perfect balance",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.80,
                    "Appreciation for artistic harmony and composition",
                ),
                EnhancedMusicSlangEntry(
                    "the outfits",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.75,
                    "Excitement about visual presentation-often with multiple exclamation marks",
                ),
            ]
        )

        # Gen Z positive expressions (100% accuracy target)
        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "no cap this slaps",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.90,
                    "No lie, this is excellent-Gen Z enthusiasm",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "periodt",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Period with emphasis-end of discussion, strong agreement",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "hits different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.85,
                    "Uniquely good / special",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "chef's kiss",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_GENERAL,
                    Aspect.GENERAL,
                    0.90,
                    "Perfect / excellent",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "I'm obsessed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Strong positive attachment-Gen Z usage",
                    gen_z_slang=True,
                ),
            ]
        )

        # Cultural positive expressions (needs improvement)
        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "fucking queen",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.95,
                    "High praise for female artist",
                    toxicity=Toxicity.LIGHT,
                ),
                EnhancedMusicSlangEntry(
                    "go off king",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.95,
                    "Praise for male artist performance",
                ),
                EnhancedMusicSlangEntry(
                    "bad bish",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.90,
                    "Positive term for confident / attractive person",
                ),
                EnhancedMusicSlangEntry(
                    "YES MOTHER",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.90,
                    "High praise, especially for female artists",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "my son",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.CULTURAL_IDENTITY,
                    Aspect.ARTIST,
                    0.85,
                    "Playful endearment for favorite artist-shows protective support",
                ),
                EnhancedMusicSlangEntry(
                    "get him on trending",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.ENGAGEMENT_BEHAVIORAL,
                    Aspect.ARTIST,
                    0.80,
                    "Active support for artist success and visibility",
                ),
            ]
        )

        # Enthusiasm expressions (needs improvement)
        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "fuck it up",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.ARTIST,
                    0.85,
                    "Encouragement to perform excellently",
                    toxicity=Toxicity.LIGHT,
                ),
                EnhancedMusicSlangEntry(
                    "the way I screamed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Expression of excitement / amazement",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "lowkey fire",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.80,
                    "Understated praise - 'lowkey' + positive term",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "highkey obsessed",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.HYPE_EXCITEMENT,
                    Aspect.GENERAL,
                    0.85,
                    "Openly / obviously obsessed-strong positive",
                    gen_z_slang=True,
                ),
            ]
        )

        # Production praise (maintain 75%+ accuracy)
        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "the vocals are insane",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.90,
                    "'Insane' in positive context means amazing",
                ),
                EnhancedMusicSlangEntry(
                    "production is clean",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.MIX,
                    0.85,
                    "High-quality production / mixing",
                    beat_appreciation=True,
                ),
                EnhancedMusicSlangEntry(
                    "harmonies hit different",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PERFORMANCE,
                    Aspect.VOCALS,
                    0.85,
                    "Harmonies are uniquely good",
                ),
                EnhancedMusicSlangEntry(
                    "the beat though",
                    SentimentLabel.POSITIVE,
                    Intent.PRAISE,
                    SlangCategory.PRAISE_PRODUCTION,
                    Aspect.BEAT,
                    0.90,
                    "Appreciation for beat / production",
                    beat_appreciation=True,
                ),
            ]
        )

        # ===== NEGATIVE SENTIMENT (improve to 90%+) =====

        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "this ain't it chief",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Polite way of saying something is bad",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "mid",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Mediocre / average in a disappointing way",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "cringe",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Embarrassing / awkward in a negative way",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "trash",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Very poor quality",
                ),
                # ADD MORE NEGATIVE EXAMPLES TO BALANCE DATASET
                EnhancedMusicSlangEntry(
                    "this sucks",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Direct negative criticism",
                ),
                EnhancedMusicSlangEntry(
                    "terrible",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Very bad quality",
                ),
                EnhancedMusicSlangEntry(
                    "boring af",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Extremely boring-Gen Z expression",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "skip this",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Recommendation to avoid",
                ),
                EnhancedMusicSlangEntry(
                    "not it",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Gen Z way of saying something is bad",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "sounds awful",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.90,
                    "Direct negative assessment",
                ),
                EnhancedMusicSlangEntry(
                    "can't stand this",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Strong dislike expression",
                ),
                EnhancedMusicSlangEntry(
                    "overrated",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Criticism of hype vs quality",
                ),
                EnhancedMusicSlangEntry(
                    "disappointing",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Failed expectations",
                ),
                EnhancedMusicSlangEntry(
                    "weak",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Lacking strength or impact",
                ),
                EnhancedMusicSlangEntry(
                    "not feeling it",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Polite way to express dislike",
                ),
                EnhancedMusicSlangEntry(
                    "meh",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.70,
                    "Indifferent / unimpressed response",
                ),
                EnhancedMusicSlangEntry(
                    "nah fam",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Casual rejection",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "hard pass",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Strong rejection",
                ),
                EnhancedMusicSlangEntry(
                    "yikes",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Expression of dismay",
                    gen_z_slang=True,
                ),
                EnhancedMusicSlangEntry(
                    "off-key",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.VOCALS,
                    0.85,
                    "Vocal performance criticism",
                ),
                EnhancedMusicSlangEntry(
                    "messy production",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.MIX,
                    0.80,
                    "Poor mixing / production quality",
                    beat_appreciation=True,
                ),
                EnhancedMusicSlangEntry(
                    "generic",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Lacks originality",
                ),
                EnhancedMusicSlangEntry(
                    "try again",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.GENERAL,
                    0.70,
                    "Constructive but negative feedback",
                ),
                EnhancedMusicSlangEntry(
                    "not your best work",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_CONSTRUCTIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Polite criticism comparing to previous work",
                ),
                EnhancedMusicSlangEntry(
                    "sounds like everyone else",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Criticism of lack of uniqueness",
                ),
                EnhancedMusicSlangEntry(
                    "outdated",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.75,
                    "Criticism of being behind trends",
                ),
                EnhancedMusicSlangEntry(
                    "trying too hard",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.80,
                    "Criticism of forced effort",
                ),
                EnhancedMusicSlangEntry(
                    "lost their touch",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ARTIST,
                    0.80,
                    "Artist has declined in quality",
                ),
                EnhancedMusicSlangEntry(
                    "what happened to them",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ARTIST,
                    0.75,
                    "Disappointment in artist's direction",
                ),
                EnhancedMusicSlangEntry(
                    "used to be better",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ARTIST,
                    0.80,
                    "Comparison to better past work",
                ),
                EnhancedMusicSlangEntry(
                    "sellout",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.ARTIST,
                    0.85,
                    "Criticism of commercialization",
                ),
                EnhancedMusicSlangEntry(
                    "no soul",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.GENERAL,
                    0.85,
                    "Lacks emotional depth",
                ),
                EnhancedMusicSlangEntry(
                    "sounds robotic",
                    SentimentLabel.NEGATIVE,
                    Intent.CRITIQUE,
                    SlangCategory.CRITICISM_NEGATIVE,
                    Aspect.VOCALS,
                    0.80,
                    "Lacks human emotion in vocals",
                ),
            ]
        )

        # ===== NEUTRAL SENTIMENT (maintain accuracy) =====

        # Pure requests without opinion cues
        entries.extend(
            [
                EnhancedMusicSlangEntry(
                    "I need the lyrics",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.LYRICS,
                    0.90,
                    "Request without opinion markers-neutral per SemEval",
                ),
                EnhancedMusicSlangEntry(
                    "who produced this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.85,
                    "Information seeking question",
                    beat_appreciation=True,
                ),
                EnhancedMusicSlangEntry(
                    "what's the sample",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.85,
                    "Information seeking about production",
                    beat_appreciation=True,
                ),
                EnhancedMusicSlangEntry(
                    "clean version pls",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Format request without opinion",
                ),
                # ADD MORE NEUTRAL EXAMPLES TO BALANCE DATASET
                EnhancedMusicSlangEntry(
                    "what genre is this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Genre classification question",
                ),
                EnhancedMusicSlangEntry(
                    "release date",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.90,
                    "Factual information request",
                ),
                EnhancedMusicSlangEntry(
                    "album name",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Album identification request",
                ),
                EnhancedMusicSlangEntry(
                    "available on spotify",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.80,
                    "Platform availability question",
                ),
                EnhancedMusicSlangEntry(
                    "how long is this song",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Duration information request",
                ),
                EnhancedMusicSlangEntry(
                    "instrumental version",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Request for instrumental version",
                ),
                EnhancedMusicSlangEntry(
                    "artist name",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ARTIST,
                    0.90,
                    "Artist identification request",
                ),
                EnhancedMusicSlangEntry(
                    "song title",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Song identification request",
                ),
                EnhancedMusicSlangEntry(
                    "where can I buy this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.80,
                    "Purchase information request",
                ),
                EnhancedMusicSlangEntry(
                    "concert dates",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ARTIST,
                    0.85,
                    "Tour information request",
                ),
                EnhancedMusicSlangEntry(
                    "music video link",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Video link request",
                ),
                EnhancedMusicSlangEntry(
                    "what key is this in",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Musical theory question",
                ),
                EnhancedMusicSlangEntry(
                    "bpm",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.BEAT,
                    0.80,
                    "Beats per minute information",
                    beat_appreciation=True,
                ),
                EnhancedMusicSlangEntry(
                    "chord progression",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Musical structure question",
                ),
                EnhancedMusicSlangEntry(
                    "similar artists",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ARTIST,
                    0.80,
                    "Recommendation request",
                ),
                EnhancedMusicSlangEntry(
                    "first time hearing this",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.75,
                    "Neutral discovery statement",
                ),
                EnhancedMusicSlangEntry(
                    "remix version",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Remix version request",
                ),
                EnhancedMusicSlangEntry(
                    "acoustic version",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Acoustic version request",
                ),
                EnhancedMusicSlangEntry(
                    "live performance",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Live version request",
                ),
                EnhancedMusicSlangEntry(
                    "radio edit",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.80,
                    "Radio version request",
                ),
                EnhancedMusicSlangEntry(
                    "full album",
                    SentimentLabel.NEUTRAL,
                    Intent.REQUEST,
                    SlangCategory.NEUTRAL_REQUESTS,
                    Aspect.GENERAL,
                    0.85,
                    "Complete album request",
                ),
                EnhancedMusicSlangEntry(
                    "track listing",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Album contents request",
                ),
                EnhancedMusicSlangEntry(
                    "featuring who",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ARTIST,
                    0.80,
                    "Collaboration information request",
                ),
                EnhancedMusicSlangEntry(
                    "record label",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.ARTIST,
                    0.85,
                    "Label information request",
                ),
                EnhancedMusicSlangEntry(
                    "copyright info",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.80,
                    "Legal information request",
                ),
                EnhancedMusicSlangEntry(
                    "streaming numbers",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.80,
                    "Statistics information request",
                ),
                EnhancedMusicSlangEntry(
                    "chart position",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.85,
                    "Chart performance question",
                ),
                EnhancedMusicSlangEntry(
                    "music theory analysis",
                    SentimentLabel.NEUTRAL,
                    Intent.INFO,
                    SlangCategory.NEUTRAL_QUESTIONS,
                    Aspect.GENERAL,
                    0.80,
                    "Technical analysis request",
                ),
            ]
        )

        return entries

    # --------------------------- Derived Data & Exports ---------------------------

    @cached_property
    def df(self) -> pd.DataFrame:
        """Cached DataFrame for performance optimization."""
        return pd.DataFrame([e.to_dict() for e in self.entries])

    def get_statistics(self) -> Dict[str, object]:
        """Get comprehensive dataset statistics."""
        df = self.df
        return {
            "total_phrases": len(self.entries),
            "sentiment_distribution": df["sentiment"].value_counts().to_dict(),
            "intent_distribution": df["intent"].value_counts().to_dict(),
            "category_distribution": df["category"].value_counts().to_dict(),
            "aspect_distribution": df["aspect"].value_counts().to_dict(),
            "beat_appreciation_count": int(df["beat_appreciation"].sum()),
            "gen_z_slang_count": int(df["gen_z_slang"].sum()),
            "toxicity_distribution": df["toxicity"].value_counts().to_dict(),
            "avg_confidence": float(df["confidence"].mean()),
            "confidence_by_sentiment": df.groupby("sentiment")["confidence"].mean().to_dict(),
        }

    def fingerprint(self) -> str:
        """Stable build fingerprint for CI provenance."""
        h = sha256()
        for e in sorted(self.entries, key=lambda x: x.id):
            h.update(json.dumps(e.to_dict(), sort_keys=True, ensure_ascii=False).encode("utf-8"))
        return h.hexdigest()

    @staticmethod
    def json_schema() -> Dict[str, object]:
        """Exportable JSON Schema for entries."""
        return _EntryModel.model_json_schema()

    def export_json_schema(self, path: str = "enhanced_music_slang_entry.schema.json") -> None:
        """Export JSON Schema for downstream validation."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.json_schema(), f, ensure_ascii=False, indent=2)
        print(f"🧾 JSON Schema written: {path}")

    # --------------------------- Quality & Invariants ---------------------------

    def _assert_quality(self) -> None:
        """Assert dataset quality with comprehensive checks."""
        assert len(self.entries) >= 30, f"Dataset must have >=30 phrases, got {len(self.entries)}"

        seen_norm: Dict[str, EnhancedMusicSlangEntry] = {}
        seen_ids: set[str] = set()
        inconsistencies: List[str] = []

        for e in self.entries:
            # IDs unique and deterministic
            expected_id = generate_stable_id(e.phrase, e.sentiment, e.intent, e.aspect)
            if e.id != expected_id:
                raise AssertionError(f"Non-deterministic ID for '{e.phrase}': {e.id} != {expected_id}")
            if e.id in seen_ids:
                raise AssertionError(f"Duplicate ID found: {e.id}")
            seen_ids.add(e.id)

            # Dedup by normalized phrase + same label triple
            nk = e.norm_key()
            if nk in seen_norm:
                prev = seen_norm[nk]
                if (prev.sentiment, prev.intent, prev.aspect) == (e.sentiment, e.intent, e.aspect):
                    raise AssertionError(
                        f"Near-duplicate phrase detected: '{e.phrase}' ~ '{prev.phrase}' "
                        f"with same sentiment / intent / aspect"
                    )
            else:
                seen_norm[nk] = e

            # SemEval neutrality convention for REQUEST / INFO (allow with boosters)
            if e.intent in (Intent.REQUEST, Intent.INFO) and e.sentiment != SentimentLabel.NEUTRAL:
                phrase_up = e.phrase.upper()
                if not any(b in phrase_up for b in OPINION_BOOSTERS):
                    inconsistencies.append(f"{e.intent.value} should be neutral unless boosted → '{e.phrase}'")

            # Simple moderation sanity
            if e.nsfw and e.toxicity == Toxicity.NONE:
                inconsistencies.append(f"NSFW true but toxicity NONE → '{e.phrase}'")

        if inconsistencies:
            # Fail loud with succinct preview
            preview = "\n  - ".join(inconsistencies[:10])
            raise AssertionError(f"Inconsistencies detected ({len(inconsistencies)}):\n  - {preview}")

    # --------------------------- Timeout Utilities ---------------------------

    @staticmethod
    def _with_timeout(fn, timeout_s: Optional[int]) -> None:
        """Execute function with optional timeout control."""
        if timeout_s is None:
            fn()
            return

        result = {"err": None}

        def _runner() -> None:
            try:
                fn()
            except BaseException as e:
                result["err"] = e

        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        t.join(timeout=timeout_s)

        if t.is_alive():
            raise TimeoutError(f"Operation timed out after {timeout_s}s")
        if result["err"] is not None:
            raise result["err"]

    def export_to_csv(
        self, filename: str = "enhanced_music_sentiment_dataset.csv", timeout_s: Optional[int] = None
    ) -> None:
        """Export to CSV with timeout control."""
        self._with_timeout(lambda: self.df.to_csv(filename, index=False), timeout_s)
        print(f"💾 CSV exported: {filename}")

    def export_to_jsonl(
        self, filename: str = "enhanced_music_sentiment_dataset.jsonl", timeout_s: Optional[int] = None
    ) -> None:
        """Export to JSONL with timeout control."""

        def _dump() -> None:
            with open(filename, "w", encoding="utf-8") as f:
                for e in self.entries:
                    f.write(json.dumps(e.to_dict(), ensure_ascii=False) + "\n")

        self._with_timeout(_dump, timeout_s)
        print(f"💾 JSONL exported: {filename}")

    def export_transformer_format(
        self,
        filename: str = "enhanced_music_sentiment_transformer.jsonl",
        model_name: str = "distilbert-base-uncased",
        timeout_s: Optional[int] = None,
    ) -> None:
        """Export in transformer-compatible format with preprocessing."""

        def _dump_transformer() -> None:
            try:
                # Import text processing helpers
                from youtubeviz.text_processing_helpers import create_music_text_processor

                processor = create_music_text_processor(model_name=model_name)

                with open(filename, "w", encoding="utf-8") as f:
                    for e in self.entries:
                        # Create transformer-ready entry
                        transformer_entry = {
                            "id": e.id,
                            "text": e.phrase,
                            "processed_text": processor.preprocess_text(e.phrase),
                            "label": e.sentiment.value,
                            "confidence": e.confidence,
                            # Transformer-specific fields
                            "features": processor.analyze_text_features(e.phrase),
                            # Original metadata
                            "intent": e.intent.value,
                            "category": e.category.value,
                            "aspect": e.aspect.value,
                            "context_notes": e.context_notes,
                            "gen_z_slang": e.gen_z_slang,
                            "beat_appreciation": e.beat_appreciation,
                            "toxicity": e.toxicity.value,
                            # Processing metadata
                            "model_name": model_name,
                            "processing_version": "1.0",
                        }

                        f.write(json.dumps(transformer_entry, ensure_ascii=False) + "\n")

            except ImportError:
                print("⚠️  Text processing helpers not available, using basic format")
                # Fallback to basic format
                with open(filename, "w", encoding="utf-8") as f:
                    for e in self.entries:
                        basic_entry = {
                            "id": e.id,
                            "text": e.phrase,
                            "label": e.sentiment.value,
                            "confidence": e.confidence,
                        }
                        f.write(json.dumps(basic_entry, ensure_ascii=False) + "\n")

        self._with_timeout(_dump_transformer, timeout_s)
        print(f"💾 Transformer format exported: {filename}")

    def export_huggingface_format(
        self,
        output_dir: str = "enhanced_music_sentiment_hf",
        test_size: float = 0.2,
        val_size: float = 0.1,
        timeout_s: Optional[int] = None,
    ) -> None:
        """Export in HuggingFace datasets format with train / val / test splits."""

        def _dump_hf() -> None:
            import os

            from sklearn.model_selection import train_test_split

            os.makedirs(output_dir, exist_ok=True)

            # Prepare data
            data = []
            for e in self.entries:
                data.append({"text": e.phrase, "label": e.sentiment.value, "confidence": e.confidence, "id": e.id})

            # Create splits
            train_data, temp_data = train_test_split(
                data, test_size=(test_size + val_size), random_state=42, stratify=[d["label"] for d in data]
            )

            if val_size > 0:
                val_data, test_data = train_test_split(
                    temp_data,
                    test_size=(test_size / (test_size + val_size)),
                    random_state=42,
                    stratify=[d["label"] for d in temp_data],
                )
            else:
                test_data = temp_data
                val_data = []

            # Export splits
            splits = [("train", train_data), ("test", test_data)]
            if val_data:
                splits.append(("validation", val_data))

            for split_name, split_data in splits:
                split_file = os.path.join(output_dir, f"{split_name}.jsonl")
                with open(split_file, "w", encoding="utf-8") as f:
                    for item in split_data:
                        f.write(json.dumps(item, ensure_ascii=False) + "\n")
                print(f"   {split_name}: {len(split_data)} samples")

            # Create dataset info
            info = {
                "dataset_name": "enhanced_music_sentiment",
                "version": self.dataset_version,
                "description": "Enhanced music industry sentiment dataset with music slang and cultural expressions",
                "splits": {split: len(data) for split, data in splits},
                "labels": list(set(d["label"] for d in data)),
                "features": {"text": "string", "label": "string", "confidence": "float", "id": "string"},
            }

            with open(os.path.join(output_dir, "dataset_info.json"), "w") as f:
                json.dump(info, f, indent=2, ensure_ascii=False)

        self._with_timeout(_dump_hf, timeout_s)
        print(f"💾 HuggingFace format exported: {output_dir}/")

    def create_transformer_training_config(self, model_name: str = "distilbert-base-uncased") -> Dict[str, any]:
        """Create training configuration for transformer fine-tuning."""

        # Analyze dataset characteristics
        stats = self.get_statistics()

        config = {
            "model_name": model_name,
            "task": "text-classification",
            "num_labels": len(stats["sentiment_distribution"]),
            "label2id": {label: i for i, label in enumerate(stats["sentiment_distribution"].keys())},
            "id2label": {i: label for i, label in enumerate(stats["sentiment_distribution"].keys())},
            # Training parameters optimized for music domain
            "learning_rate": 2e-5,
            "num_train_epochs": 3,
            "per_device_train_batch_size": 16,
            "per_device_eval_batch_size": 16,
            "warmup_steps": 500,
            "weight_decay": 0.01,
            "logging_dir": "./logs",
            # Music-specific settings
            "max_length": 128,  # Music comments are typically short
            "truncation": True,
            "padding": "max_length",
            # Dataset info
            "dataset_size": stats["total_phrases"],
            "class_distribution": stats["sentiment_distribution"],
            "avg_confidence": stats["avg_confidence"],
            # Preprocessing settings
            "preserve_music_slang": True,
            "handle_emoji": True,
            "normalize_unicode": True,
        }

        return config


# --------------------------- Convenience API ---------------------------


def get_enhanced_music_dataset() -> EnhancedMusicSentimentDatasetV2:
    """Get enhanced dataset instance."""
    return EnhancedMusicSentimentDatasetV2()


# --------------------------- CLI ---------------------------


def _cli(argv: Optional[Iterable[str]] = None) -> int:
    """Command-line interface for dataset operations."""
    import argparse

    p = argparse.ArgumentParser(description="Enhanced Music Industry Sentiment Dataset v2.1")
    p.add_argument("--stats", action="store_true", help="print dataset stats")
    p.add_argument("--export-csv", metavar="PATH", help="export CSV to PATH")
    p.add_argument("--export-jsonl", metavar="PATH", help="export JSONL to PATH")
    p.add_argument("--schema", metavar="PATH", help="export JSON Schema to PATH")
    p.add_argument("--timeout", type=int, default=None, help="timeout seconds for exports")
    args = p.parse_args(list(argv) if argv is not None else None)

    ds = get_enhanced_music_dataset()

    if args.stats:
        stats = ds.get_statistics()
        print(f"🎵 Enhanced Music Industry Sentiment Dataset v{ds.dataset_version}")
        print("=" * 60)
        print(f"Total phrases: {stats['total_phrases']}")
        print(f"Beat appreciation phrases: {stats['beat_appreciation_count']}")
        print(f"Gen Z slang phrases: {stats['gen_z_slang_count']}")
        print(f"Average confidence: {stats['avg_confidence']:.3f}\n")

        for label, dist in [
            ("📊 Sentiment Distribution:", stats["sentiment_distribution"]),
            ("🎯 Intent Distribution:", stats["intent_distribution"]),
            ("📂 Category Distribution:", dict(sorted(stats["category_distribution"].items()))),
            ("🔍 Aspect Distribution:", dict(sorted(stats["aspect_distribution"].items()))),
            ("⚠️ Toxicity Distribution:", stats["toxicity_distribution"]),
        ]:
            print(label)
            total = stats["total_phrases"]
            for k, v in dist.items():
                pct = (v / total) * 100 if total else 0
                print(f"  {k}: {v} ({pct:.1f}%)")
            print()

    if args.schema:
        ds.export_json_schema(args.schema)

    if args.export_csv:
        ds.export_to_csv(args.export_csv, timeout_s=args.timeout)

    if args.export_jsonl:
        ds.export_to_jsonl(args.export_jsonl, timeout_s=args.timeout)

    if not (args.stats or args.schema or args.export_csv or args.export_jsonl):
        p.print_help()
    else:
        print(f"🔐 fingerprint: {ds.fingerprint()}")

    return 0


if __name__ == "__main__":
    sys.exit(_cli())
