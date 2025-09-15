#!/usr/bin/env python3
"""
Deploy Enhanced Bot Detection System

This script implements sophisticated bot vs. fan detection that:
1. Distinguishes between bot behavior and enthusiastic fan behavior
2. Uses temporal patterns and engagement authenticity scoring
3. Maintains whitelist for legitimate fan expressions
4. Populates is_bot_suspected column in youtube_comments table
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import logging
import re
from collections import Counter
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedBotDetector:
    """
    Enhanced bot detection that balances bot identification with fan engagement preservation.
    """

    def __init__(self):
        self.fan_whitelist = self._create_fan_whitelist()
        self.bot_patterns = self._create_bot_patterns()

    def _create_fan_whitelist(self):
        """Create whitelist for legitimate fan expressions."""
        return {
            # Enthusiastic fan expressions (NEVER flag as bot)
            "aave_praise": [
                r"\b(snapped|ate|served|killed it|bodied|went off)\b",
                r"\b(slay|periodt|no cap|ate that)\b",
                r"\b(understood the assignment|hits different)\b",
            ],
            "music_enthusiasm": [
                r"\b(fire|lit|slaps|bangs|goes hard|hard af)\b",
                r"\b(banger|absolute banger|this is it)\b",
                r"\b(on repeat|can\'t stop|playing this)\b",
            ],
            "engagement_indicators": [
                r"\b(playlist|repeat|loop|obsessed|addicted)\b",
                r"\b(car test|gym playlist|study music)\b",
                r"\b(need this on spotify|when on apple music)\b",
            ],
            "fan_requests": [
                r"\b(drop|release).*\b(already|now|please)\b",
                r"\b(visuals when|music video when|tour when)\b",
                r"\b(lyrics|clean version|instrumental)\b",
            ],
        }

    def _create_bot_patterns(self):
        """Create patterns that indicate bot behavior."""
        return {
            "generic_praise": [
                r"^(nice|good|great|awesome|amazing|cool)!*$",
                r"^(love it|like it|love this|like this)!*$",
                r"^(fire|lit|dope|sick)!*$",
            ],
            "spam_indicators": [
                r"(check out my|subscribe to my|follow me)",
                r"(buy followers|get views|increase subscribers)",
                r"(click here|link in bio|dm me)",
            ],
            "repetitive_patterns": [
                r"^(.)\1{10,}$",  # Same character repeated 10+ times
                r"^(..)\1{5,}$",  # Same 2 chars repeated 5+ times
                r"^\d+$",  # Only numbers
            ],
            "low_effort": [
                r'^[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]*$',  # Only symbols
                r"^.{1,2}$",  # Very short (1-2 chars)
                r"^(first|second|third|early|late)!*$",
            ],
        }

    def is_whitelisted_fan(self, comment_text: str) -> bool:
        """Check if comment matches fan whitelist patterns."""
        text_lower = comment_text.lower()

        for category, patterns in self.fan_whitelist.items():
            for pattern in patterns:
                if re.search(pattern, text_lower, re.IGNORECASE):
                    return True
        return False

    def calculate_bot_score(self, comment_data: dict) -> float:
        """Calculate bot probability score (0.0 = human, 1.0 = bot)."""
        score = 0.0
        text = comment_data["comment_text"]

        # If whitelisted as fan, very low bot score
        if self.is_whitelisted_fan(text):
            return 0.1

        # Check bot patterns
        for category, patterns in self.bot_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    if category == "spam_indicators":
                        score += 0.8  # High penalty for spam
                    elif category == "generic_praise":
                        score += 0.3  # Medium penalty for generic
                    elif category == "repetitive_patterns":
                        score += 0.5  # High penalty for repetitive
                    elif category == "low_effort":
                        score += 0.4  # Medium-high penalty for low effort

        # Length-based scoring
        text_length = len(text.strip())
        if text_length <= 2:
            score += 0.6
        elif text_length <= 5:
            score += 0.3
        elif text_length > 200:
            score += 0.2  # Very long comments can be spam

        # Emoji ratio (too many emojis can indicate bot)
        emoji_count = len(re.findall(r"[😀-🙏🌀-🗿🚀-🛿🇦-🇿]", text))
        word_count = len(text.split())
        if word_count > 0:
            emoji_ratio = emoji_count / word_count
            if emoji_ratio > 0.5:  # More than 50% emojis
                score += 0.3

        # Caps ratio (ALL CAPS can indicate bot)
        caps_chars = sum(1 for c in text if c.isupper())
        total_chars = sum(1 for c in text if c.isalpha())
        if total_chars > 0:
            caps_ratio = caps_chars / total_chars
            if caps_ratio > 0.8:  # More than 80% caps
                score += 0.2

        return min(score, 1.0)  # Cap at 1.0

    def analyze_temporal_patterns(self, user_comments: pd.DataFrame) -> float:
        """Analyze temporal patterns for bot-like behavior."""
        if len(user_comments) < 2:
            return 0.0

        # Convert to datetime if needed
        if "created_at" in user_comments.columns:
            timestamps = pd.to_datetime(user_comments["created_at"])
        else:
            return 0.0

        # Check for rapid-fire commenting (bot indicator)
        time_diffs = timestamps.diff().dropna()
        rapid_comments = sum(1 for diff in time_diffs if diff.total_seconds() < 60)  # < 1 minute apart

        if len(time_diffs) > 0:
            rapid_ratio = rapid_comments / len(time_diffs)
            if rapid_ratio > 0.5:  # More than 50% rapid comments
                return 0.4

        return 0.0

    def calculate_engagement_authenticity(self, comment_data: dict) -> float:
        """Calculate engagement authenticity score (higher = more authentic)."""
        text = comment_data["comment_text"]

        # Authentic engagement indicators
        authenticity_score = 0.0

        # Personal references increase authenticity
        personal_patterns = [
            r"\b(i|me|my|mine|myself)\b",
            r"\b(this reminds me|makes me feel|when i)\b",
            r"\b(my favorite|i love how|i can\'t)\b",
        ]

        for pattern in personal_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                authenticity_score += 0.2

        # Specific references increase authenticity
        specific_patterns = [
            r"\b(at \d+:\d+|the part where|that beat)\b",
            r"\b(the lyrics|the chorus|the bridge)\b",
            r"\b(reminds me of|sounds like|similar to)\b",
        ]

        for pattern in specific_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                authenticity_score += 0.3

        # Questions increase authenticity
        if "?" in text:
            authenticity_score += 0.1

        # Emotional expressions increase authenticity
        emotional_patterns = [
            r"\b(crying|tears|emotional|chills|goosebumps)\b",
            r"\b(obsessed|addicted|can\'t stop)\b",
            r"\b(amazing|incredible|beautiful|perfect)\b",
        ]

        for pattern in emotional_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                authenticity_score += 0.2

        return min(authenticity_score, 1.0)


def load_comments_for_analysis():
    """Load all comments with metadata for bot analysis."""
    engine = get_engine()

    query = """
    SELECT
        yc.comment_id,
        yc.comment_text,
        yc.video_id,
        yc.created_at,
        yc.author_name,
        yv.channel_title as artist_name
    FROM youtube_comments yc
    JOIN youtube_videos yv ON yc.video_id = yv.video_id
    WHERE yc.comment_text IS NOT NULL
    AND LENGTH(yc.comment_text) > 0
    ORDER BY yc.created_at DESC
    """

    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} comments for bot analysis")
    return df


def analyze_duplicate_patterns(comments_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze duplicate comment patterns."""
    # Find exact duplicates
    duplicate_texts = comments_df["comment_text"].value_counts()
    duplicates = duplicate_texts[duplicate_texts > 1]

    logger.info(f"Found {len(duplicates)} duplicate comment texts")

    # Mark comments with duplicate text
    comments_df["has_duplicate_text"] = comments_df["comment_text"].isin(duplicates.index)

    return comments_df


def deploy_bot_detection():
    """Deploy enhanced bot detection system."""
    logger.info("🤖 Starting enhanced bot detection deployment...")

    # Load comments
    comments_df = load_comments_for_analysis()

    # Analyze duplicate patterns
    comments_df = analyze_duplicate_patterns(comments_df)

    # Initialize bot detector
    detector = EnhancedBotDetector()

    # Process comments in batches
    batch_size = 1000
    total_processed = 0
    bot_suspected_count = 0

    # Add is_bot_suspected column if it doesn't exist
    engine = get_engine()
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE youtube_comments ADD COLUMN is_bot_suspected BOOLEAN DEFAULT FALSE"))
            conn.commit()
            logger.info("   - Added is_bot_suspected column")
        except Exception:
            logger.info("   - is_bot_suspected column already exists")

    # Process comments
    for i in range(0, len(comments_df), batch_size):
        batch = comments_df.iloc[i : i + batch_size]

        bot_updates = []
        for _, row in batch.iterrows():
            try:
                comment_data = {
                    "comment_text": row["comment_text"],
                    "created_at": row["created_at"],
                    "author_name": row["author_name"],
                }

                # Calculate bot score
                bot_score = detector.calculate_bot_score(comment_data)

                # Calculate engagement authenticity
                authenticity_score = detector.calculate_engagement_authenticity(comment_data)

                # Adjust bot score based on authenticity
                final_bot_score = bot_score * (1 - authenticity_score * 0.5)

                # Additional penalty for duplicate text
                if row["has_duplicate_text"]:
                    final_bot_score += 0.3

                # Determine if bot suspected (threshold: 0.7)
                is_bot_suspected = final_bot_score > 0.7

                if is_bot_suspected:
                    bot_suspected_count += 1

                bot_updates.append(
                    {
                        "comment_id": row["comment_id"],
                        "is_bot_suspected": is_bot_suspected,
                        "bot_score": final_bot_score,
                    }
                )

            except Exception as e:
                logger.warning(f"Failed to analyze comment {row['comment_id']}: {e}")
                continue

        # Update database
        if bot_updates:
            with engine.connect() as conn:
                for update in bot_updates:
                    conn.execute(
                        text(
                            """
                        UPDATE youtube_comments
                        SET is_bot_suspected = :is_bot_suspected
                        WHERE comment_id = :comment_id
                    """
                        ),
                        {"comment_id": update["comment_id"], "is_bot_suspected": update["is_bot_suspected"]},
                    )
                conn.commit()

            total_processed += len(bot_updates)

        if (i // batch_size + 1) % 10 == 0:
            logger.info(f"   - Processed {total_processed} comments...")

    logger.info(f"✅ Bot detection complete:")
    logger.info(f"   - Comments analyzed: {total_processed}")
    logger.info(f"   - Bot suspected: {bot_suspected_count}")
    logger.info(f"   - Bot rate: {bot_suspected_count/total_processed*100:.1f}%")

    return total_processed, bot_suspected_count


def validate_bot_detection():
    """Validate bot detection deployment."""
    logger.info("🔍 Validating bot detection...")

    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT
                COUNT(*) as total_comments,
                SUM(CASE WHEN is_bot_suspected THEN 1 ELSE 0 END) as bot_suspected,
                AVG(CASE WHEN is_bot_suspected THEN 1.0 ELSE 0.0 END) as bot_rate
            FROM youtube_comments
            WHERE comment_text IS NOT NULL
        """
            )
        ).fetchone()

        total_comments = result[0]
        bot_suspected = result[1] or 0
        bot_rate = result[2] or 0

        logger.info(f"📊 Bot Detection Results:")
        logger.info(f"   - Total comments: {total_comments}")
        logger.info(f"   - Bot suspected: {bot_suspected}")
        logger.info(f"   - Bot rate: {bot_rate*100:.1f}%")

        # Sample some bot-suspected comments for review
        sample_bots = conn.execute(
            text(
                """
            SELECT comment_text
            FROM youtube_comments
            WHERE is_bot_suspected = TRUE
            LIMIT 5
        """
            )
        ).fetchall()

        logger.info("🤖 Sample bot-suspected comments:")
        for i, (comment,) in enumerate(sample_bots, 1):
            logger.info(f"   {i}. '{comment[:50]}{'...' if len(comment) > 50 else ''}'")

        return True


def main():
    """Main deployment function."""
    try:
        # Deploy bot detection
        total_processed, bot_count = deploy_bot_detection()

        # Validate deployment
        if validate_bot_detection():
            logger.info("🎉 Enhanced bot detection deployed successfully!")
            return True
        else:
            logger.error("❌ Bot detection validation failed")
            return False

    except Exception as e:
        logger.error(f"❌ Bot detection deployment failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
