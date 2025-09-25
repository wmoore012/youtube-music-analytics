#!/usr/bin/env python3
"""
Benchmark Against Real Database Comments

Test VADER variants against actual YouTube comments in our database
to avoid overfitting and get realistic performance metrics.
"""

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine


def fetch_real_comments_sample(limit: int = 200) -> pd.DataFrame:
    """Fetch real comments from database for benchmarking."""

    try:
        engine = get_engine()

        with engine.connect() as conn:
            # Get diverse sample of comments across artists and videos
            comments_df = pd.read_sql(
                text(
                    """
                SELECT
                    c.comment_id,
                    c.comment_text,
                    c.video_id,
                    v.channel_title as artist,
                    c.like_count,
                    c.published_at
                FROM youtube_comments c
                JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE c.comment_text IS NOT NULL
                AND LENGTH(c.comment_text) BETWEEN 10 AND 200
                AND c.comment_text NOT LIKE '%http%'
                ORDER BY RAND()
                LIMIT :limit
            """
                ),
                conn,
                params={"limit": limit},
            )

        print(f"✅ Fetched {len(comments_df)} real comments from database")
        return comments_df

    except Exception as e:
        print(f"❌ Failed to fetch comments: {e}")
        return pd.DataFrame()


def benchmark_vader_on_real_comments():
    """Benchmark stock VADER vs enhanced VADER on real comments."""

    print("🎯 BENCHMARKING VADER ON REAL DATABASE COMMENTS")
    print("=" * 60)

    # Fetch real comments
    comments_df = fetch_real_comments_sample(200)
    if len(comments_df) == 0:
        print("❌ No comments available for benchmarking")
        return

    # Test stock VADER
    print("\n🧪 Testing Stock VADER...")
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        stock_vader = SentimentIntensityAnalyzer()

        stock_results = []
        for _, row in comments_df.iterrows():
            comment = row["comment_text"]
            scores = stock_vader.polarity_scores(comment)

            stock_results.append(
                {
                    "comment_id": row["comment_id"],
                    "comment_text": comment,
                    "artist": row["artist"],
                    "compound": scores["compound"],
                    "pos": scores["pos"],
                    "neg": scores["neg"],
                    "neu": scores["neu"],
                }
            )

        stock_df = pd.DataFrame(stock_results)

        # Classify sentiments
        stock_df["sentiment"] = stock_df["compound"].apply(
            lambda x: "positive" if x >= 0.05 else "negative" if x <= -0.05 else "neutral"
        )

        print(f"📊 Stock VADER Results:")
        print(f"   Positive: {(stock_df['sentiment'] == 'positive').sum()}")
        print(f"   Negative: {(stock_df['sentiment'] == 'negative').sum()}")
        print(f"   Neutral:  {(stock_df['sentiment'] == 'neutral').sum()}")

        # Show some examples
        print(f"\n📝 Sample Classifications:")
        for sentiment in ["positive", "negative", "neutral"]:
            sample = stock_df[stock_df["sentiment"] == sentiment].head(2)
            print(f"\n{sentiment.upper()}:")
            for _, row in sample.iterrows():
                comment_short = (
                    row["comment_text"][:60] + "..." if len(row["comment_text"]) > 60 else row["comment_text"]
                )
                print(f"   {row['compound']:+.3f} | {row['artist']:15} | {comment_short}")

    except ImportError:
        print("❌ VADER not available")
        return

    print(f"\n✅ Real comment benchmark complete!")
    print(f"🎯 Next: Implement enhanced VADER and compare performance")

    return stock_df


if __name__ == "__main__":
    benchmark_vader_on_real_comments()
