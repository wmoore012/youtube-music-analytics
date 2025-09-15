#!/usr/bin/env python3
"""
Test Current Sentiment Model Performance

GOAL: Find out what our current sentiment analysis already recognizes correctly
so we don't add unnecessary rules. We'll test music slang, Gen Z language, and
fan expressions to see where we need improvements.

This will help us build a smarter, not bigger, sentiment analyzer!
"""

import sys
from pathlib import Path

from dotenv import load_dotenv

# Setup
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
load_dotenv()

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine


def test_current_vader_sentiment():
    """Test what VADER already recognizes correctly."""

    # Test phrases you mentioned plus Gen Z additions
    test_phrases = [
        # Your original list
        "this is sick",
        "fucking queen!",
        "go off king",
        "oh my!",
        "oh my yes!",
        "fuck it up",
        "I need the lyrics",
        "yessir!",
        "yessuh",
        "10/10",
        "100!",
        "😍",
        "100/10",
        "queen",
        "hot bish",
        "bad bish",
        "YES MOTHER!",
        "friday can't come sooner",
        "Bitchhh!",
        "Bitch, it's givinnnng!",
        "please come to atlanta",
        # Gen Z additions
        "no cap this slaps",
        "periodt",
        "it's giving main character energy",
        "this is bussin",
        "absolutely sending me",
        "I'm deceased 💀",
        "not me crying",
        "the way I screamed",
        "I'm obsessed",
        "this hits different",
        "chef's kiss 👌",
        "living for this",
        "I can't even",
        "this is everything",
        "we love to see it",
        "say less",
        "bet",
        "facts",
        "lowkey fire",
        "highkey obsessed",
        "this ain't it chief",  # Should be negative
        "mid",  # Should be negative/neutral
        "cringe",  # Should be negative
        # Music specific Gen Z
        "this song is unmatched",
        "the vocals are insane",
        "harmonies hit different",
        "production is clean",
        "this is a whole vibe",
        "adding to my playlist rn",
        "spotify wrapped gonna be interesting",
        "this deserves a grammy",
        "artist of the year behavior",
        "album of the year vibes",
    ]

    print("🧪 Testing Current VADER Sentiment Model")
    print("=" * 60)
    print("GOAL: See what our current model already handles correctly")
    print("so we only add rules for what's actually missing!\n")

    # Test with VADER (current model)
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        vader = SentimentIntensityAnalyzer()

        results = []

        for phrase in test_phrases:
            scores = vader.polarity_scores(phrase)
            compound = scores["compound"]

            # Classify sentiment
            if compound >= 0.05:
                classification = "POSITIVE"
                emoji = "✅" if phrase not in ["this ain't it chief", "mid", "cringe"] else "❌"
            elif compound <= -0.05:
                classification = "NEGATIVE"
                emoji = "✅" if phrase in ["this ain't it chief", "mid", "cringe"] else "❌"
            else:
                classification = "NEUTRAL"
                emoji = "⚪"

            results.append(
                {
                    "phrase": phrase,
                    "compound": compound,
                    "classification": classification,
                    "correct": emoji,
                    "pos": scores["pos"],
                    "neg": scores["neg"],
                    "neu": scores["neu"],
                }
            )

            print(f"{emoji} {phrase:30} | {classification:8} | {compound:+.3f}")

        # Summary
        correct_count = sum(1 for r in results if r["correct"] == "✅")
        total_count = len(results)
        accuracy = correct_count / total_count * 100

        print(f"\n📊 VADER Performance Summary:")
        print(f"   Correct: {correct_count}/{total_count} ({accuracy:.1f}%)")

        # Show what needs improvement
        needs_work = [r for r in results if r["correct"] == "❌"]
        if needs_work:
            print(f"\n🔧 Phrases needing improvement:")
            for item in needs_work:
                print(f"   • '{item['phrase']}' → {item['classification']} (should be opposite)")

        return results

    except ImportError:
        print("❌ VADER not installed. Install with: pip install vaderSentiment")
        return []


def test_current_database_sentiment():
    """Test sentiment analysis on actual comments in our database."""

    print("\n🗄️ Testing Current Database Comments")
    print("=" * 60)

    try:
        engine = get_engine()

        # Get sample comments with existing sentiment scores
        with engine.connect() as conn:
            sample_comments = pd.read_sql(
                text(
                    """
                SELECT c.comment_text, cs.sentiment_score, v.channel_title
                FROM youtube_comments c
                JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
                JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE c.comment_text IS NOT NULL
                AND LENGTH(c.comment_text) > 5
                ORDER BY RAND()
                LIMIT 20
            """
                ),
                conn,
            )

        if len(sample_comments) > 0:
            print("Sample of current sentiment analysis:")
            for _, row in sample_comments.iterrows():
                comment = row["comment_text"][:50] + "..." if len(row["comment_text"]) > 50 else row["comment_text"]
                score = row["sentiment_score"]
                artist = row["channel_title"]

                sentiment_label = "POSITIVE" if score > 0.1 else "NEGATIVE" if score < -0.1 else "NEUTRAL"
                print(f"   {sentiment_label:8} | {score:+.2f} | {artist:15} | {comment}")
        else:
            print("   No comments with sentiment scores found in database")

    except Exception as e:
        print(f"   ❌ Database test failed: {e}")


def check_artist_comment_coverage():
    """Check if all configured artists have comments in the database."""

    print("\n🎤 Checking Artist Comment Coverage")
    print("=" * 60)

    try:
        engine = get_engine()

        with engine.connect() as conn:
            # Get artist comment counts
            artist_comments = pd.read_sql(
                text(
                    """
                SELECT v.channel_title, COUNT(c.comment_id) as comment_count
                FROM youtube_videos v
                LEFT JOIN youtube_comments c ON v.video_id = c.video_id
                GROUP BY v.channel_title
                ORDER BY comment_count DESC
            """
                ),
                conn,
            )

        print("Artist comment coverage:")
        total_comments = 0
        artists_with_comments = 0

        for _, row in artist_comments.iterrows():
            artist = row["channel_title"]
            count = row["comment_count"]
            total_comments += count

            if count > 0:
                artists_with_comments += 1
                status = "✅"
            else:
                status = "❌ NO COMMENTS"

            print(f"   {status} {artist:20} | {count:,} comments")

        print(f"\nSummary: {artists_with_comments}/{len(artist_comments)} artists have comments")
        print(f"Total comments: {total_comments:,}")

        return artist_comments

    except Exception as e:
        print(f"   ❌ Coverage check failed: {e}")
        return pd.DataFrame()


def main():
    """Run all current model tests."""

    print("🎯 CURRENT SENTIMENT MODEL ANALYSIS")
    print("=" * 80)
    print("We're testing what works NOW so we can be smart about improvements!")
    print("=" * 80)

    # Test 1: VADER on music slang
    vader_results = test_current_vader_sentiment()

    # Test 2: Database sentiment
    test_current_database_sentiment()

    # Test 3: Artist coverage
    artist_coverage = check_artist_comment_coverage()

    # Recommendations
    print("\n💡 SMART IMPROVEMENT RECOMMENDATIONS:")
    print("=" * 60)

    if vader_results:
        needs_work = [r for r in vader_results if r["correct"] == "❌"]
        if needs_work:
            print("🔧 Add custom rules for these phrases:")
            for item in needs_work:
                print(f"   • '{item['phrase']}'")
        else:
            print("✅ VADER handles most music slang well! Minimal custom rules needed.")

    if len(artist_coverage) > 0:
        no_comments = artist_coverage[artist_coverage["comment_count"] == 0]
        if len(no_comments) > 0:
            print(f"\n🚨 {len(no_comments)} artists need comment data - run ETL!")
        else:
            print("\n✅ All artists have comment data")

    print("\n🎉 Analysis complete! Now we know exactly what to improve.")


if __name__ == "__main__":
    main()
