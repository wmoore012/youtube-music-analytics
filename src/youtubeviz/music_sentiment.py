"""
Enhanced Music Industry Sentiment Analysis

This module provides music industry-specific sentiment analysis that understands
slang, AAVE, and cultural context in music fan comments.
"""

import re as regex_module
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd


@dataclass
class MusicSentimentConfig:
    """Configuration for music industry sentiment analysis."""

    # Positive expressions in music context
    positive_patterns = {
        # VADER missed these - need custom rules
        r"\bthis is sick\b": 0.8,
        r"\bfuck it up\b": 0.8,
        r"\bbad bish\b": 0.8,
        r"\bbitch,?\s*it\'?s givinnnng\b": 0.8,
        r"\bno cap.*slaps\b": 0.8,
        r"\bthe way i screamed\b": 0.7,
        r"\bi\'?m obsessed\b": 0.7,
        r"\blowkey fire\b": 0.7,
        r"\bhighkey obsessed\b": 0.8,
        r"\bthis song is unmatched\b": 0.8,
        r"\bthe vocals are insane\b": 0.8,
        # Original patterns that VADER handles poorly
        r"\bfucking queen\b": 0.9,
        r"\bgo off (king|queen)\b": 0.8,
        r"\boh my god?\b": 0.7,
        r"\boh my yes\b": 0.8,
        # Engagement indicators
        r"\bi need the lyrics\b": 0.6,
        r"\bwhere are the lyrics\b": 0.6,
        r"\blyrics please\b": 0.6,
        # Affirmative responses
        r"\byessir!?\b": 0.7,
        r"\byessuh\b": 0.7,
        r"\b10/10\b": 0.9,
        r"\b100/10\b": 0.9,
        r"\b100!\b": 0.8,
        # Queen/royalty references (positive in music context)
        r"\bqueen\b": 0.7,
        r"\bking\b": 0.7,
        r"\bhot bish\b": 0.8,
        r"\byes mother!?\b": 0.9,
        # Anticipation and excitement
        r"\bfriday can\'?t come sooner\b": 0.7,
        r"\bcan\'?t wait\b": 0.6,
        # Enthusiastic expressions
        r"\bbitchhh+!?\b": 0.7,  # Extended "bitch" as excitement
        # Tour/location requests (positive engagement)
        r"\bplease come to \w+\b": 0.6,
        r"\bcome to my city\b": 0.6,
        r"\btour dates\b": 0.5,
        # Gen Z slang that VADER missed
        r"\bno cap\b": 0.6,
        r"\bperiodt\b": 0.7,
        r"\bit\'?s giving\b": 0.6,
        r"\bthis is bussin\b": 0.8,
        r"\babsolutely sending me\b": 0.7,
        r"\bi\'?m deceased\b": 0.7,  # Positive in Gen Z context
        r"\bnot me crying\b": 0.6,  # Emotional positive response
        r"\bthis hits different\b": 0.7,
        r"\bliving for this\b": 0.8,
        r"\bi can\'?t even\b": 0.6,
        r"\bthis is everything\b": 0.8,
        r"\bsay less\b": 0.6,
        r"\bbet\b": 0.5,
        r"\bfacts\b": 0.5,
        # Music-specific positive terms
        r"\bbanger\b": 0.8,
        r"\bslaps\b": 0.8,
        r"\bfire\b": 0.7,
        r"\bflames?\b": 0.7,
        r"\bvibes?\b": 0.6,
        r"\bmood\b": 0.6,
        r"\benergy\b": 0.6,
        r"\bthis is a whole vibe\b": 0.7,
        r"\badding to my playlist\b": 0.7,
        r"\bspotify wrapped\b": 0.6,
        r"\bthis deserves a grammy\b": 0.8,
        r"\bartist of the year\b": 0.8,
        r"\balbum of the year\b": 0.8,
        r"\bproduction is clean\b": 0.7,
        r"\bharmonies hit different\b": 0.7,
        # Additional Gen Z expressions
        r"\bsending me\b": 0.6,
        r"\bI\'?m weak\b": 0.6,  # Laughing/positive
        r"\bI\'?m crying\b": 0.5,  # Can be positive emotional response
        r"\bthis sent me\b": 0.6,
        r"\bI\'?m screaming\b": 0.6,
        r"\bthis is iconic\b": 0.8,
        r"\bmain character energy\b": 0.7,
        r"\bwe love to see it\b": 0.7,
        r"\bchef\'?s kiss\b": 0.8,
        # Fan expressions from real comments (these were missed!)
        r"\bhottie,?\s*baddie,?\s*\w+\b": 0.8,  # "Hottie, Baddie, Maddie"
        r"\bpart two please+\b": 0.7,  # "Part two pleaseee"
        r"\bwtf+\b": 0.6,  # "wtfff" - excitement in music context
        r"\bcuz i \w+\b": 0.6,  # "Cuz I willie" - playful expression
        r"\bsheee+sh\b": 0.8,  # "sheeeeesh" - excitement/approval
        r"\bmy \w+ snapped\b": 0.8,  # "my nigga snapped" - praise
        r"\bsnapped\b": 0.8,  # General praise for performance
        r"\bmy legs are spread\b": 0.7,  # Excitement/anticipation
        r"\bbestie goals\b": 0.7,  # "Bestie goals fr"
        r"\bfr\b": 0.5,  # "for real" - agreement/emphasis
        # New additions from your examples
        r"\bthis hard af\b": 0.8,  # "this hard af"
        r"\bhard af\b": 0.8,  # "hard as fuck" - very positive
        r"\bthis hard as shit\b": 0.8,  # "this hard as shit"
        r"\bhard as shit\b": 0.8,  # Strong positive expression
        r"\bbro this crazy\b": 0.8,  # "Bro this crazy"
        r"\bthis crazy bro\b": 0.8,  # Variation
        r"\bthe beat though!?\b": 0.8,  # "the beat though!"
        r"\bthe beat tho!?\b": 0.8,  # "the beat tho!"
        r"\bwho made this beat\b": 0.7,  # "who made this beat bro?!" - appreciation
        r"\bwho made this beat bro\b": 0.7,  # Full phrase
        # Variations and related expressions
        r"\bthis crazy\b": 0.7,  # General "this crazy"
        r"\bcrazy bro\b": 0.7,  # "crazy bro"
        r"\bbro\b": 0.4,  # "bro" - friendly/positive context
        r"\bthe beat\b": 0.5,  # Beat references are generally positive
        r"\bbeat\s+(though|tho)\b": 0.7,  # "beat though/tho"
        r"\bwho made\b": 0.6,  # Asking about producer - appreciation
        r"\bproducer\b": 0.5,  # Producer mentions - appreciation
        # Extended patterns for "hard" (positive in music)
        r"\bthis\s+hard\b": 0.7,  # "this hard"
        r"\bgoes?\s+hard\b": 0.8,  # "goes hard" / "go hard"
        r"\bso\s+hard\b": 0.7,  # "so hard"
        r"\btoo\s+hard\b": 0.8,  # "too hard" - very positive
        r"\bway\s+too\s+hard\b": 0.9,  # "way too hard" - extremely positive
        # "Crazy" in positive music context
        r"\bthis\s+crazy\b": 0.7,  # "this crazy"
        r"\bso\s+crazy\b": 0.7,  # "so crazy"
        r"\btoo\s+crazy\b": 0.8,  # "too crazy"
        r"\bway\s+crazy\b": 0.8,  # "way crazy"
        # Beat appreciation with variations
        r"\bbeat\s+is\s+(fire|sick|hard|crazy|insane)\b": 0.8,
        r"\bbeat\s+(fire|sick|hard|crazy|insane)\b": 0.8,
        r"\b(fire|sick|hard|crazy|insane)\s+beat\b": 0.8,
        # Variations and extensions
        r"\bhottie\b": 0.7,
        r"\bbaddie\b": 0.7,
        r"\bplease+\b": 0.6,  # Multiple e's show excitement
        r"\bwtf\b": 0.5,  # In music context, often positive surprise
        r"\bsheesh\b": 0.7,  # Approval/amazement
        r"\bsnap\b": 0.6,  # "that snaps" = that's good
        r"\bbestie\b": 0.6,  # Friendly/positive
        r"\bgoals\b": 0.6,  # Aspirational/positive
        # Extended letter patterns (show excitement)
        r"\bplease{3,}\b": 0.7,  # "pleaseee" etc.
        r"\bwtf{2,}\b": 0.6,  # "wtfff" etc.
        r"\bshe{3,}sh\b": 0.8,  # "sheeeeesh" etc.
        # Music appreciation
        r"\binsane vocals\b": 0.8,
        r"\bvocals are crazy\b": 0.8,
        r"\bbeat goes hard\b": 0.8,
        r"\bthis goes hard\b": 0.8,
        r"\bunmatched\b": 0.7,
        r"\bhits different\b": 0.7,
    }

    # Emoji sentiment mapping
    emoji_sentiment = {
        "😍": 0.8,
        "🔥": 0.7,
        "💯": 0.8,
        "🌊": 0.6,
        "👑": 0.7,
        "💖": 0.8,
        "❤️": 0.7,
        "🎵": 0.6,
        "🎶": 0.6,
        "🙌": 0.7,
        "👏": 0.6,
        "😖": 0.5,  # Can be positive in music context (emotional response)
        "😚": 0.7,  # Kiss emoji - positive
        "💕": 0.8,  # Love/hearts - positive
        "🤞": 0.6,  # Crossed fingers - hopeful/positive
    }

    # Emoji multiplier patterns (multiple emojis = more positive)
    emoji_multiplier_patterns = [
        (r"🔥{2,}", 0.8),  # Multiple fire emojis - "🔥🔥🔥"
        (r"🌊{2,}", 0.7),  # Multiple wave emojis - "🌊🌊🌊🌊"
        (r"💯{2,}", 0.9),  # Multiple 100 emojis - "💯💯💯"
        (r"❤️{2,}", 0.8),  # Multiple hearts
        (r"😍{2,}", 0.9),  # Multiple heart eyes
        (r"👑{2,}", 0.8),  # Multiple crowns
        (r"🎵{2,}", 0.7),  # Multiple music notes
        (r"🎶{2,}", 0.7),  # Multiple music notes
        (r"🙌{2,}", 0.8),  # Multiple praise hands
    ]

    # Special emoji-only comment patterns
    emoji_only_patterns = [
        (r"^🔥+$", 0.8),  # Only fire emojis
        (r"^🌊+$", 0.7),  # Only wave emojis
        (r"^💯+$", 0.9),  # Only 100 emojis
        (r"^❤️+$", 0.8),  # Only hearts
        (r"^😍+$", 0.9),  # Only heart eyes
    ]

    # Beat appreciation indicators
    beat_appreciation_patterns = [
        r"\bbeat\b.*\b(fire|sick|hard|crazy|insane)\b",
        r"\b(fire|sick|hard|crazy|insane)\b.*\bbeat\b",
        r"\bproduction\b.*\b(fire|sick|hard|crazy|insane)\b",
        r"\binstrumental\b.*\b(fire|sick|hard|crazy|insane)\b",
        r"\bdrums?\b.*\b(fire|sick|hard|crazy|insane)\b",
        r"\bbass\b.*\b(fire|sick|hard|crazy|insane)\b",
    ]


class MusicIndustrySentimentAnalyzer:
    """Enhanced sentiment analyzer for music industry comments."""

    def __init__(self, config: MusicSentimentConfig = None):
        self.config = config or MusicSentimentConfig()

    def analyze_comment(self, comment_text: str) -> Dict[str, float]:
        """
        Analyze sentiment of a music industry comment.

        Returns:
            Dict with sentiment_score, confidence, beat_appreciation
        """
        if not comment_text or pd.isna(comment_text):
            return {"sentiment_score": 0.0, "confidence": 0.0, "beat_appreciation": False}

        comment_lower = comment_text.lower().strip()

        # Calculate base sentiment from patterns
        sentiment_score = 0.0
        pattern_matches = 0

        # Check positive patterns
        for pattern, score in self.config.positive_patterns.items():
            if regex_module.search(pattern, comment_lower, regex_module.IGNORECASE):
                sentiment_score += score
                pattern_matches += 1

        # Check emoji sentiment
        emoji_score = 0.0
        emoji_count = 0

        # Check individual emojis
        for emoji, score in self.config.emoji_sentiment.items():
            count = comment_text.count(emoji)
            if count > 0:
                emoji_count += count
                emoji_score += score * count  # Multiple emojis = more positive

        # Check emoji multiplier patterns (multiple same emojis)
        import re

        for pattern, bonus_score in self.config.emoji_multiplier_patterns:
            matches = regex_module.findall(pattern, comment_text)
            if matches:
                # Bonus for multiple same emojis
                emoji_score += bonus_score * len(matches)

        # Check emoji-only patterns (comments that are just emojis)
        for pattern, score in self.config.emoji_only_patterns:
            if regex_module.match(pattern, comment_text.strip()):
                emoji_score = max(emoji_score, score)  # Use highest score for emoji-only

        # Combine pattern and emoji scores
        if pattern_matches > 0:
            sentiment_score = sentiment_score / pattern_matches

        if emoji_count > 0:
            emoji_score = emoji_score / emoji_count
            # Weight emoji score based on frequency
            emoji_weight = min(0.3, emoji_count * 0.1)
            sentiment_score = (sentiment_score * (1 - emoji_weight)) + (emoji_score * emoji_weight)

        # Normalize to [-1, 1] range
        sentiment_score = max(-1.0, min(1.0, sentiment_score))

        # Calculate confidence based on pattern matches and length
        confidence = min(1.0, (pattern_matches + emoji_count) * 0.2 + len(comment_text) * 0.001)

        # Check for beat appreciation
        beat_appreciation = self._detect_beat_appreciation(comment_lower)

        return {
            "sentiment_score": round(sentiment_score, 3),
            "confidence": round(confidence, 3),
            "beat_appreciation": beat_appreciation,
        }

    def _detect_beat_appreciation(self, comment_lower: str) -> bool:
        """Detect if comment shows appreciation for the beat/production."""
        for pattern in self.config.beat_appreciation_patterns:
            if regex_module.search(pattern, comment_lower, regex_module.IGNORECASE):
                return True
        return False

    def analyze_batch(self, comments: List[str]) -> pd.DataFrame:
        """Analyze a batch of comments and return results as DataFrame."""
        results = []

        for comment in comments:
            analysis = self.analyze_comment(comment)
            results.append(analysis)

        return pd.DataFrame(results)


def update_comment_sentiment_table(engine, batch_size: int = 1000):
    """
    Update the comment_sentiment table with enhanced music industry sentiment analysis.

    This function:
    1. Backs up existing sentiment data
    2. Analyzes all comments with enhanced music sentiment
    3. Updates the comment_sentiment table
    4. Populates beat_appreciation and confidence fields
    """
    from sqlalchemy import text

    analyzer = MusicIndustrySentimentAnalyzer()

    print("🎵 Starting enhanced music industry sentiment analysis...")

    # Get total comment count
    with engine.connect() as conn:
        total_comments = conn.execute(text("SELECT COUNT(*) FROM youtube_comments")).fetchone()[0]
        print(f"📊 Total comments to analyze: {total_comments:,}")

    # Process in batches
    processed = 0

    with engine.connect() as conn:
        # Create backup table
        print("💾 Creating backup of existing sentiment data...")
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS comment_sentiment_backup AS
            SELECT * FROM comment_sentiment
        """
            )
        )

        # Clear existing sentiment data
        print("🗑️ Clearing existing sentiment data...")
        conn.execute(text("DELETE FROM comment_sentiment"))

        # Process comments in batches
        offset = 0
        while offset < total_comments:
            # Get batch of comments
            comments_batch = pd.read_sql(
                text(
                    """
                SELECT comment_id, comment_text, video_id
                FROM youtube_comments
                WHERE comment_text IS NOT NULL
                ORDER BY comment_id
                LIMIT :batch_size OFFSET :offset
            """
                ),
                conn,
                params={"batch_size": batch_size, "offset": offset},
            )

            if len(comments_batch) == 0:
                break

            # Analyze sentiment for batch
            sentiment_results = []
            for _, row in comments_batch.iterrows():
                analysis = analyzer.analyze_comment(row["comment_text"])
                sentiment_results.append(
                    {
                        "comment_id": row["comment_id"],
                        "video_id": row["video_id"],
                        "sentiment_score": analysis["sentiment_score"],
                        "confidence": analysis["confidence"],
                        "beat_appreciation": analysis["beat_appreciation"],
                    }
                )

            # Insert batch results
            sentiment_df = pd.DataFrame(sentiment_results)
            sentiment_df.to_sql("comment_sentiment", conn, if_exists="append", index=False)

            # Update youtube_comments table with beat_appreciation
            for result in sentiment_results:
                conn.execute(
                    text(
                        """
                    UPDATE youtube_comments
                    SET beat_appreciation = :beat_appreciation
                    WHERE comment_id = :comment_id
                """
                    ),
                    {"beat_appreciation": result["beat_appreciation"], "comment_id": result["comment_id"]},
                )

            processed += len(comments_batch)
            offset += batch_size

            print(f"✅ Processed {processed:,}/{total_comments:,} comments ({processed/total_comments*100:.1f}%)")

        conn.commit()

    print("🎉 Enhanced sentiment analysis complete!")
    return processed
