#!/usr/bin/env python3
"""
TDD for YouTube Bot Detection — Real-world behaviors baked in.

Ground rules this suite enforces:
- Normal hype ≠ bot: fire-emoji spam, a single wave 👋, "Hey <artist>", "CRAZY!!" are *often legit fan energy*.
- Bot red flags: WhatsApp/Telegram lures, phone-number obfuscations, timestamp+link bait, URL shorteners,
  bursty near-duplicates across users/videos, Unicode tricks (homoglyphs, zero-width chars).
- Scores are trend-based: we compare groups (benign vs bot-pattern) instead of pinning exact thresholds.

Red → Green → Refactor.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pandas as pd
import pytest

# System under test
from src.youtubeviz.bot_detection import (
    BotDetectionConfig,
    BotDetector,
    _clamp_01,
    _count_emojis,
    _normalize_text,
    _strip_emojis,
    analyze_bot_patterns,
    load_recent_comments,
)

# ------------------------------ Utility tests ------------------------------ #


class TestTextUtilsRealWorld:
    def test_normalize_and_strip_handles_zero_width_and_case(self):
        # Zero-width space between letters (adversarial obfuscation)
        zws = "\u200b"
        s = f"W{zws}h{zws}a{zws}t{zws}s{zws}A{zws}p{zws}p"
        norm = _normalize_text(s)
        # Normalization must collapse superfluous whitespace & lowercase
        assert "whatsapp" in norm.replace(zws, "")
        # Emoji stripping shouldn't nuke letters
        assert _strip_emojis("CRAZY!! 🔥🔥") == "CRAZY!! "

    def test_count_emojis_handles_mixed_praise(self):
        assert _count_emojis("temazo 🔥🔥🔥") == 3
        assert _count_emojis("👋 hey artist") == 1
        assert _count_emojis("no emojis") == 0

    def test_clamp_behaves(self):
        assert _clamp_01(-1.2) == 0.0
        assert _clamp_01(0.25) == 0.25
        assert _clamp_01(3.14) == 1.0


# ------------------------------ Config tests ------------------------------- #


class TestConfigBoundaries:
    def test_config_defaults_reasonable(self):
        cfg = BotDetectionConfig()
        # Sanity on core knobs
        assert 0.75 <= cfg.near_dupe_threshold <= 0.95
        assert cfg.min_dupe_cluster >= 3
        assert 10 <= cfg.burst_window_seconds <= 120
        assert 0.05 <= cfg.emoji_max_weight <= 0.3

    def test_config_rejects_extremes(self):
        with pytest.raises(ValueError):
            BotDetectionConfig(near_dupe_threshold=0.49)
        with pytest.raises(ValueError):
            BotDetectionConfig(near_dupe_threshold=1.00)


# ------------------------------ Detector tests ----------------------------- #


@pytest.fixture()
def detector():
    # Whitelist common benign hype so fans aren't punished
    wl = frozenset(
        {"love this", "fire", "🔥", "crazy!!", "temazo", "hey", "first", "lets go", "vamos", "banger", "on repeat"}
    )
    return BotDetector(config=BotDetectionConfig(whitelist_phrases=wl))


def _now(n: int = 0):
    return datetime.now(timezone.utc) - timedelta(seconds=n)


@pytest.fixture()
def df_realistic():
    """
    Build a mixed bag of legit fan comments and classic bot lures.
    Notes:
    - "CRAZY!!" all caps is common legit hype.
    - One wave emoji/Hey <artist> is benign.
    - Fire-emoji floods are common on music; shouldn't be auto-botty.
    - Timestamp spam + links, WhatsApp/Telegram lures, phone #'s → strong bot signals.
    - Duplicate/generic praise across users in a short burst → suspicious.
    """
    zws = "\u200b"
    rows = [
        # ---- Benign hype (should lean LOW risk) ----
        ("c1", "v1", "This is CRAZY!! 🔥🔥", "fan_1", 7, _now(10)),
        ("c2", "v1", "👋 Hey Artist we love you!", "fan_2", 1, _now(20)),
        ("c3", "v2", "temazo 🔥🔥🔥", "fan_es", 3, _now(30)),
        ("c4", "v2", "fire fire fire", "fan_3", 0, _now(40)),
        ("c5", "v3", "First!", "fan_4", 0, _now(50)),
        # ---- Generic praise near-duplicate but not coordinated (spread out) ----
        ("c6", "v4", "amazing track", "fan_5", 0, _now(4000)),
        ("c7", "v5", "amazing track", "fan_6", 1, _now(3200)),
        # ---- Bot patterns: timestamp bait + link/shortener ----
        ("c8", "v6", "0:59 this part tho https://bit.ly/xyz", "susp_1", 0, _now(5)),
        # ---- WhatsApp/Telegram lure with number obfuscation + ZWSP ----
        ("c9", "v6", f"DM me on What{zws}sApp +1(415)-555-0199 for promo", "imp_1", 0, _now(4)),
        ("c10", "v6", "PROMOTE IT ON @tеlegram  @ChannelPromo", "imp_2", 0, _now(3)),  # note homoglyph 'e'
        # ---- Burst duplicates across users/videos within short window ----
        ("c11", "v7", "check my channel for free beats", "bot_a", 0, _now(12)),
        ("c12", "v8", "check my channel for free beats", "bot_b", 0, _now(13)),
        ("c13", "v9", "check my channel for free beats", "bot_c", 0, _now(14)),
    ]
    return pd.DataFrame(
        rows, columns=["comment_id", "video_id", "comment_text", "author_name", "like_count", "published_at"]
    )


def test_detector_structure_and_ranges(detector, df_realistic):
    out = detector.analyze_comments(df_realistic)
    assert len(out) == len(df_realistic)
    # Must include key fields
    for col in ["bot_score", "bot_risk_level", "duplicate_count_local", "emoji_count", "is_whitelisted"]:
        assert col in out.columns
    # Scores are within [0,100]
    assert out["bot_score"].between(0, 100).all()
    # Risk labels limited set
    assert set(out["bot_risk_level"].unique()) <= {"Low", "Medium", "High"}


def test_benign_hype_not_overpenalized(detector, df_realistic):
    out = detector.analyze_comments(df_realistic)
    benign = out[out["comment_id"].isin({"c1", "c2", "c3", "c4", "c5"})]
    assert len(benign) == 5
    # Trend: benign hype tends toward lower scores than bot-lure cohort
    bots = out[out["comment_id"].isin({"c8", "c9", "c10", "c11", "c12", "c13"})]
    assert benign["bot_score"].mean() <= bots["bot_score"].mean() - 10  # meaningful gap
    # Fire emoji flood should NOT push to High on its own
    assert (benign.loc[benign["comment_id"] == "c3", "bot_risk_level"].iloc[0]) in {"Low", "Medium"}
    # "CRAZY!!" should not be flagged purely for caps/punctuation
    assert (benign.loc[benign["comment_id"] == "c1", "bot_risk_level"].iloc[0]) in {"Low", "Medium"}


def test_timestamp_link_and_whatsapp_telegram_lures(detector, df_realistic):
    out = detector.analyze_comments(df_realistic)
    ts = out[out["comment_id"] == "c8"].iloc[0]
    wa = out[out["comment_id"] == "c9"].iloc[0]
    tg = out[out["comment_id"] == "c10"].iloc[0]
    # These should lean HIGH
    assert ts["bot_risk_level"] == "High"
    assert wa["bot_risk_level"] == "High"
    assert tg["bot_risk_level"] == "High"
    # They should also outrank normal hype by a clear margin
    benign_max = out[out["comment_id"].isin({"c1", "c2", "c3", "c4", "c5"})]["bot_score"].max()
    assert ts["bot_score"] > benign_max
    assert wa["bot_score"] > benign_max
    assert tg["bot_score"] > benign_max


def test_burst_near_duplicates_across_users_and_videos(detector, df_realistic):
    out = detector.analyze_comments(df_realistic)
    burst = out[out["comment_text"].str.contains("check my channel", na=False)]
    assert len(burst) == 3
    # Expect local duplicate counts >= cluster size
    assert (burst["duplicate_count_local"] >= 2).all()
    # Burst should escalate risk
    assert (burst["bot_risk_level"] != "Low").all()
    assert burst["bot_score"].mean() >= out["bot_score"].mean()


def test_whitelist_softens_legit_phrases(detector, df_realistic):
    out = detector.analyze_comments(df_realistic)
    wl = out[out["is_whitelisted"]]
    nwl = out[~out["is_whitelisted"]]
    if not wl.empty and not nwl.empty:
        assert wl["bot_score"].mean() <= nwl["bot_score"].mean() + 5  # small cushion allowed


# ------------------------- DB integration shims --------------------------- #


class TestDBEntryPoints:
    @patch("src.youtubeviz.bot_detection.pd.read_sql")
    def test_load_recent_comments_min_columns(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame(
            {
                "comment_id": ["x1"],
                "video_id": ["v1"],
                "comment_text": ["nice 🔥"],
                "author_name": ["fan"],
                "like_count": [0],
                "published_at": [datetime.now(timezone.utc)],
                "video_title": ["Song"],
                "channel_title": ["Artist"],
            }
        )
        eng = Mock()
        df = load_recent_comments(eng, days=3)
        assert not df.empty and {"comment_id", "comment_text"} <= set(df.columns)

    @patch("src.youtubeviz.bot_detection.load_recent_comments")
    def test_analyze_bot_patterns_happy(self, mock_load):
        mock_load.return_value = pd.DataFrame(
            {
                "comment_id": ["a", "b"],
                "video_id": ["v1", "v1"],
                "comment_text": ["check my channel", "temazo 🔥"],
                "author_name": ["u1", "u2"],
                "like_count": [0, 1],
                "published_at": [datetime.now(timezone.utc)] * 2,
            }
        )
        eng = Mock()
        cfg = BotDetectionConfig()
        out = analyze_bot_patterns(eng, config=cfg, days=7)
        assert len(out) == 2 and "bot_score" in out

    @patch("src.youtubeviz.bot_detection.load_recent_comments")
    def test_analyze_bot_patterns_empty(self, mock_load):
        mock_load.return_value = pd.DataFrame()
        with pytest.raises(ValueError):
            analyze_bot_patterns(Mock(), days=30)


# ------------------------------ Edge cases -------------------------------- #


class TestEdgeAndPerf:
    def test_empty_text_and_only_emojis(self, detector):
        df = pd.DataFrame(
            {
                "comment_id": ["e1", "e2", "e3"],
                "video_id": ["v1", "v1", "v1"],
                "comment_text": ["", "🔥🔥🔥", None],
                "author_name": ["u1", "u2", "u3"],
                "like_count": [0, 3, 0],
                "published_at": [datetime.now(timezone.utc)] * 3,
            }
        )
        out = detector.analyze_comments(df)
        assert len(out) == 3
        # Pure emoji praise shouldn't auto-trigger High
        assert (out.loc[out["comment_id"] == "e2", "bot_risk_level"].iloc[0]) in {"Low", "Medium"}

    def test_scale_reasonably(self, detector):
        n = 400
        df = pd.DataFrame(
            {
                "comment_id": [f"c{i}" for i in range(n)],
                "video_id": [f"v{i//20}" for i in range(n)],
                "comment_text": ["amazing track"] * (n // 2) + [f"unique {i}" for i in range(n - n // 2)],
                "author_name": [f"user{i//5}" for i in range(n)],
                "like_count": [0] * n,
                "published_at": [datetime.now(timezone.utc) - timedelta(seconds=i) for i in range(n)],
            }
        )
        out = detector.analyze_comments(df)
        assert len(out) == n
        # Heavy duplicate half should lift average above a pure-unique baseline
        assert out[out["comment_text"] == "amazing track"]["bot_score"].mean() >= out["bot_score"].mean() - 5


# ------------------------- Legacy compatibility --------------------------- #


class TestLegacyCompatibility:
    """Maintain compatibility with existing bot detection interface."""

    def test_basic_bot_detector_interface(self):
        """Test basic BotDetector interface works."""
        detector = BotDetector()

        sample_data = pd.DataFrame(
            {
                "comment_id": ["c1", "c2"],
                "video_id": ["v1", "v1"],
                "comment_text": ["Great song!", "Check my channel"],
                "author_name": ["fan", "spammer"],
                "like_count": [5, 0],
                "published_at": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            }
        )

        result = detector.analyze_comments(sample_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "bot_score" in result.columns
        assert "bot_risk_level" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
