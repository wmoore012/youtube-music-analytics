#!/usr/bin/env python3
"""
Enhanced Sentiment Analysis Update Tool

This tool updates the sentiment analysis for all comments using our enhanced
music industry sentiment analyzer that understands Gen Z slang and music context.
"""

import os
import sys
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from src.youtubeviz.enhanced_music_sentiment import ComprehensiveMusicSentimentAnalyzer


def check_database_schema(engine):
    """Check if required database columns exist and create them if needed."""

    print("🔍 Checking database schema...")

    with engine.connect() as conn:
        # Check if beat_appreciation column exists in youtube_comments
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'youtube_comments' 
            AND column_name = 'beat_appreciation'
        """
            )
        )

        if result.fetchone()[0] == 0:
            print("   Adding beat_appreciation column to youtube_comments...")
            conn.execute(
                text(
                    """
                ALTER TABLE youtube_comments 
                ADD COLUMN beat_appreciation BOOLEAN DEFAULT FALSE
            """
                )
            )

        # Check if is_bot_suspected column exists in youtube_comments
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'youtube_comments' 
            AND column_name = 'is_bot_suspected'
        """
            )
        )

        if result.fetchone()[0] == 0:
            print("   Adding is_bot_suspected column to youtube_comments...")
            conn.execute(
                text(
                    """
                ALTER TABLE youtube_comments 
                ADD COLUMN is_bot_suspected BOOLEAN DEFAULT FALSE
            """
                )
            )

        # Check if channel_title column exists in youtube_videos
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'youtube_videos' 
            AND column_name = 'channel_title'
        """
            )
        )

        if result.fetchone()[0] == 0:
            print("   Adding channel_title column to youtube_videos...")
            conn.execute(
                text(
                    """
                ALTER TABLE youtube_videos 
                ADD COLUMN channel_title VARCHAR(255)
            """
                )
            )

            # Update channel_title from existing data if possible
            print("   Updating channel_title from existing data...")
            conn.execute(
                text(
                    """
                UPDATE youtube_videos v
                SET channel_title = (
                    SELECT channel_title 
                    FROM youtube_videos_raw r 
                    WHERE r.video_id = v.video_id 
                    AND JSON_EXTRACT(r.raw_data, '$.snippet.channelTitle') IS NOT NULL
                    LIMIT 1
                )
                WHERE channel_title IS NULL
            """
                )
            )

        # Ensure comment_sentiment table exists
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS comment_sentiment (
                comment_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255) NOT NULL,
                sentiment_score DECIMAL(5,3) NOT NULL,
                confidence DECIMAL(5,3) NOT NULL,
                beat_appreciation BOOLEAN DEFAULT FALSE,
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_video_id (video_id),
                INDEX idx_sentiment_score (sentiment_score),
                FOREIGN KEY (comment_id) REFERENCES youtube_comments(comment_id) ON DELETE CASCADE,
                FOREIGN KEY (video_id) REFERENCES youtube_videos(video_id) ON DELETE CASCADE
            )
        """
            )
        )

        conn.commit()
        print("✅ Database schema verified and updated")


def get_comment_statistics(engine) -> Dict:
    """Get statistics about comments in the database."""

    with engine.connect() as conn:
        # Total comments
        result = conn.execute(text("SELECT COUNT(*) FROM youtube_comments"))
        total_comments = result.fetchone()[0]

        # Comments with sentiment analysis
        result = conn.execute(text("SELECT COUNT(*) FROM comment_sentiment"))
        analyzed_comments = result.fetchone()[0]

        # Comments by channel
        result = conn.execute(
            text(
                """
            SELECT v.channel_title, COUNT(c.comment_id) as comment_count
            FROM youtube_comments c
            JOIN youtube_videos v ON c.video_id = v.video_id
            WHERE v.channel_title IS NOT NULL
            GROUP BY v.channel_title
            ORDER BY comment_count DESC
        """
            )
        )

        channel_stats = {row[0]: row[1] for row in result.fetchall()}

        return {
            "total_comments": total_comments,
            "analyzed_comments": analyzed_comments,
            "channel_stats": channel_stats,
        }


def update_sentiment_analysis(engine, batch_size: int = 1000) -> Dict:
    """
    Update sentiment analysis for all comments using enhanced analyzer.

    Returns:
        Dict with processing statistics
    """

    analyzer = ComprehensiveMusicSentimentAnalyzer()

    print("🎵 Starting enhanced sentiment analysis update...")

    # Get statistics
    stats = get_comment_statistics(engine)
    total_comments = stats["total_comments"]

    print(f"📊 Processing {total_comments:,} total comments")
    print(f"📊 Currently analyzed: {stats['analyzed_comments']:,}")

    if stats["channel_stats"]:
        print("\n📺 Comments by channel:")
        for channel, count in stats["channel_stats"].items():
            print(f"   {channel}: {count:,} comments")

    # Create backup of existing sentiment data
    print("\n💾 Creating backup of existing sentiment data...")
    with engine.connect() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS comment_sentiment_backup AS 
            SELECT * FROM comment_sentiment
        """
            )
        )

        # Clear existing sentiment data for fresh analysis
        print("🗑️ Clearing existing sentiment data for fresh analysis...")
        conn.execute(text("DELETE FROM comment_sentiment"))
        conn.commit()

    # Process comments in batches
    processed = 0
    positive_count = 0
    beat_appreciation_count = 0

    print(f"\n🔄 Processing comments in batches of {batch_size:,}...")

    with engine.connect() as conn:
        offset = 0

        while offset < total_comments:
            # Get batch of comments
            comments_batch = pd.read_sql(
                text(
                    """
                SELECT c.comment_id, c.comment_text, c.video_id, v.channel_title
                FROM youtube_comments c
                JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE c.comment_text IS NOT NULL
                ORDER BY c.comment_id
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
            batch_positive = 0
            batch_beat_appreciation = 0

            for _, row in comments_batch.iterrows():
                analysis = analyzer.analyze_comment(row["comment_text"])

                # Count positive sentiments and beat appreciation
                if analysis["sentiment_score"] > 0.1:
                    batch_positive += 1
                if analysis["beat_appreciation"]:
                    batch_beat_appreciation += 1

                sentiment_results.append(
                    {
                        "comment_id": row["comment_id"],
                        "video_id": row["video_id"],
                        "sentiment_score": analysis["sentiment_score"],
                        "confidence": analysis["confidence"],
                        "beat_appreciation": analysis["beat_appreciation"],
                    }
                )

            # Insert batch results into comment_sentiment table
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

            # Update counters
            processed += len(comments_batch)
            positive_count += batch_positive
            beat_appreciation_count += batch_beat_appreciation
            offset += batch_size

            # Progress update
            progress = processed / total_comments * 100
            print(
                f"   ✅ Processed {processed:,}/{total_comments:,} ({progress:.1f}%) | "
                f"Positive: {batch_positive} | Beat Love: {batch_beat_appreciation}"
            )

        conn.commit()

    # Final statistics
    positive_rate = positive_count / processed * 100 if processed > 0 else 0
    beat_rate = beat_appreciation_count / processed * 100 if processed > 0 else 0

    results = {
        "total_processed": processed,
        "positive_comments": positive_count,
        "positive_rate": positive_rate,
        "beat_appreciation_comments": beat_appreciation_count,
        "beat_appreciation_rate": beat_rate,
    }

    print(f"\n🎉 Sentiment analysis update completed!")
    print(f"📊 Total processed: {processed:,}")
    print(f"😊 Positive comments: {positive_count:,} ({positive_rate:.1f}%)")
    print(f"🎵 Beat appreciation: {beat_appreciation_count:,} ({beat_rate:.1f}%)")

    return results


def verify_sentiment_update(engine) -> bool:
    """Verify that sentiment update was successful."""

    print("\n🔍 Verifying sentiment analysis update...")

    with engine.connect() as conn:
        # Check that all comments have sentiment analysis
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) as missing_sentiment
            FROM youtube_comments c
            LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
            WHERE c.comment_text IS NOT NULL AND cs.comment_id IS NULL
        """
            )
        )

        missing_sentiment = result.fetchone()[0]

        if missing_sentiment > 0:
            print(f"⚠️ {missing_sentiment:,} comments missing sentiment analysis")
            return False

        # Check sentiment score distribution
        result = conn.execute(
            text(
                """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN sentiment_score > 0.1 THEN 1 ELSE 0 END) as positive,
                SUM(CASE WHEN sentiment_score < -0.1 THEN 1 ELSE 0 END) as negative,
                SUM(CASE WHEN beat_appreciation = TRUE THEN 1 ELSE 0 END) as beat_love
            FROM comment_sentiment
        """
            )
        )

        row = result.fetchone()
        total, positive, negative, beat_love = row
        neutral = total - positive - negative

        print(f"✅ Sentiment distribution:")
        print(f"   😊 Positive: {positive:,} ({positive/total*100:.1f}%)")
        print(f"   😐 Neutral: {neutral:,} ({neutral/total*100:.1f}%)")
        print(f"   😞 Negative: {negative:,} ({negative/total*100:.1f}%)")
        print(f"   🎵 Beat appreciation: {beat_love:,} ({beat_love/total*100:.1f}%)")

        return True


def main():
    """Main function to update sentiment analysis."""

    print("🎵 Enhanced Sentiment Analysis Update Tool")
    print("=" * 60)

    # Load environment
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL not found in environment variables")
        sys.exit(1)

    engine = create_engine(database_url)

    try:
        # Step 1: Check and update database schema
        check_database_schema(engine)

        # Step 2: Update sentiment analysis
        results = update_sentiment_analysis(engine)

        # Step 3: Verify update
        if verify_sentiment_update(engine):
            print("\n✅ Sentiment analysis update completed successfully!")
        else:
            print("\n⚠️ Sentiment analysis update completed with warnings.")

        print("\nNext steps:")
        print("   1. Run data quality checks")
        print("   2. Update analytics notebooks")
        print("   3. Generate sentiment analysis charts")

    except Exception as e:
        print(f"\n❌ Error during sentiment update: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
