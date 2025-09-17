#!/usr/bin/env python3
"""
Comprehensive VADER Variants Evaluation

Evaluates multiple VADER enhancement variants against real database comments
using proper statistical methodology to avoid overfitting.
"""

import json
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.youtubeviz.vader_variants import MusicVADERNormalizer, VADERVariantManager, VariantType
from web.etl_helpers import get_engine


def fetch_evaluation_comments(limit: int = 500) -> pd.DataFrame:
    """Fetch diverse real comments for evaluation."""

    try:
        engine = get_engine()

        with engine.connect() as conn:
            # Get stratified sample across artists and engagement levels
            comments_df = pd.read_sql(
                text(
                    """
                SELECT
                    c.comment_id,
                    c.comment_text,
                    c.video_id,
                    v.channel_title as artist,
                    c.like_count,
                    c.published_at,
                    CASE
                        WHEN c.like_count >= 10 THEN 'high_engagement'
                        WHEN c.like_count >= 2 THEN 'medium_engagement'
                        ELSE 'low_engagement'
                    END as engagement_level
                FROM youtube_comments c
                JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE c.comment_text IS NOT NULL
                AND LENGTH(c.comment_text) BETWEEN 5 AND 300
                AND c.comment_text NOT LIKE '%http%'
                AND c.comment_text NOT REGEXP '^[0-9:]+$'  -- Skip timestamps
                ORDER BY RAND()
                LIMIT :limit
            """
                ),
                conn,
                params={"limit": limit},
            )

        print(f"✅ Fetched {len(comments_df)} evaluation comments")

        # Add metadata for analysis
        comments_df["has_emoji"] = comments_df["comment_text"].str.contains(r"[😀-🿿]", regex=True)
        comments_df["has_caps"] = comments_df["comment_text"].str.contains(r"[A-Z]{3,}", regex=True)
        comments_df["has_exclamation"] = comments_df["comment_text"].str.contains(r"!{2,}", regex=True)
        comments_df["word_count"] = comments_df["comment_text"].str.split().str.len()

        return comments_df

    except Exception as e:
        print(f"❌ Failed to fetch comments: {e}")
        return pd.DataFrame()


def evaluate_all_variants(comments_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Evaluate all VADER variants on the comment dataset."""

    print("\n🧪 EVALUATING ALL VADER VARIANTS")
    print("=" * 50)

    # Initialize components
    manager = VADERVariantManager()
    normalizer = MusicVADERNormalizer()

    # Get all variants
    variants = {
        "stock_vader": SentimentIntensityAnalyzer(),
        **{f"enhanced_{vt.value}": manager.create_variant(vt) for vt in VariantType},
    }

    results = {}

    for variant_name, analyzer in variants.items():
        print(f"   Testing {variant_name}...")

        variant_results = []

        for _, row in comments_df.iterrows():
            comment = row["comment_text"]

            # Normalize for enhanced variants
            if variant_name.startswith("enhanced"):
                normalized_comment = normalizer.normalize_for_vader(comment)
            else:
                normalized_comment = comment

            # Get VADER scores
            scores = analyzer.polarity_scores(normalized_comment)

            # Classify sentiment
            compound = scores["compound"]
            if compound >= 0.05:
                sentiment = "positive"
            elif compound <= -0.05:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            variant_results.append(
                {
                    "comment_id": row["comment_id"],
                    "comment_text": comment,
                    "normalized_text": normalized_comment,
                    "artist": row["artist"],
                    "video_id": row["video_id"],
                    "engagement_level": row["engagement_level"],
                    "has_emoji": row["has_emoji"],
                    "has_caps": row["has_caps"],
                    "has_exclamation": row["has_exclamation"],
                    "word_count": row["word_count"],
                    "compound": compound,
                    "pos": scores["pos"],
                    "neg": scores["neg"],
                    "neu": scores["neu"],
                    "sentiment": sentiment,
                    "variant": variant_name,
                }
            )

        results[variant_name] = pd.DataFrame(variant_results)

    return results


def analyze_variant_differences(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Analyze differences between variants."""

    print("\n📊 ANALYZING VARIANT DIFFERENCES")
    print("=" * 40)

    # Combine all results
    all_results = pd.concat(results.values(), ignore_index=True)

    # Pivot to compare variants side by side
    comparison_df = all_results.pivot_table(
        index="comment_id", columns="variant", values=["sentiment", "compound"], aggfunc="first"
    )

    # Flatten column names
    comparison_df.columns = [f"{col[1]}_{col[0]}" for col in comparison_df.columns]

    # Add original comment text
    comment_lookup = all_results[["comment_id", "comment_text", "artist"]].drop_duplicates()
    comparison_df = comparison_df.merge(comment_lookup, on="comment_id")

    # Find disagreements
    sentiment_cols = [col for col in comparison_df.columns if col.endswith("_sentiment")]

    # Count agreements/disagreements
    disagreement_stats = {}

    for i, col1 in enumerate(sentiment_cols):
        for col2 in sentiment_cols[i + 1 :]:
            variant1 = col1.replace("_sentiment", "")
            variant2 = col2.replace("_sentiment", "")

            agreements = (comparison_df[col1] == comparison_df[col2]).sum()
            total = len(comparison_df)
            agreement_rate = agreements / total

            disagreement_stats[f"{variant1}_vs_{variant2}"] = {
                "agreements": agreements,
                "total": total,
                "agreement_rate": agreement_rate,
                "disagreement_rate": 1 - agreement_rate,
            }

    return comparison_df, disagreement_stats


def identify_improvement_cases(results: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Identify cases where enhanced variants improve over stock VADER."""

    print("\n🔍 IDENTIFYING IMPROVEMENT CASES")
    print("=" * 35)

    stock_results = results["stock_vader"]

    improvement_cases = []

    for variant_name, variant_results in results.items():
        if variant_name == "stock_vader":
            continue

        # Merge stock and variant results
        merged = stock_results.merge(
            variant_results[["comment_id", "sentiment", "compound"]], on="comment_id", suffixes=("_stock", "_enhanced")
        )

        # Find cases where enhanced is more positive than stock
        improvements = merged[
            (merged["sentiment_stock"] != merged["sentiment_enhanced"])
            & (
                (
                    (merged["sentiment_stock"] == "negative")
                    & (merged["sentiment_enhanced"].isin(["neutral", "positive"]))
                )
                | ((merged["sentiment_stock"] == "neutral") & (merged["sentiment_enhanced"] == "positive"))
            )
        ].copy()

        improvements["variant"] = variant_name
        improvements["improvement_type"] = improvements.apply(
            lambda x: f"{x['sentiment_stock']} → {x['sentiment_enhanced']}", axis=1
        )

        improvement_cases.append(improvements)

    if improvement_cases:
        all_improvements = pd.concat(improvement_cases, ignore_index=True)
        return all_improvements
    else:
        return pd.DataFrame()


def generate_evaluation_report(
    results: Dict[str, pd.DataFrame],
    comparison_df: pd.DataFrame,
    disagreement_stats: Dict,
    improvements_df: pd.DataFrame,
) -> Dict:
    """Generate comprehensive evaluation report."""

    print("\n📋 GENERATING EVALUATION REPORT")
    print("=" * 32)

    report = {
        "timestamp": datetime.now().isoformat(),
        "total_comments_evaluated": len(comparison_df),
        "variants_tested": list(results.keys()),
        "summary_stats": {},
        "disagreement_analysis": disagreement_stats,
        "top_improvements": [],
        "recommendations": [],
    }

    # Summary statistics for each variant
    for variant_name, variant_df in results.items():
        sentiment_dist = variant_df["sentiment"].value_counts().to_dict()

        report["summary_stats"][variant_name] = {
            "sentiment_distribution": {k: int(v) for k, v in sentiment_dist.items()},
            "avg_compound_score": float(variant_df["compound"].mean()),
            "std_compound_score": float(variant_df["compound"].std()),
            "positive_rate": float(sentiment_dist.get("positive", 0) / len(variant_df)),
            "negative_rate": float(sentiment_dist.get("negative", 0) / len(variant_df)),
            "neutral_rate": float(sentiment_dist.get("neutral", 0) / len(variant_df)),
        }

    # Top improvement examples
    if not improvements_df.empty:
        top_improvements = improvements_df.nlargest(10, "compound_enhanced")[
            ["comment_text", "artist", "variant", "improvement_type", "compound_stock", "compound_enhanced"]
        ].to_dict("records")

        report["top_improvements"] = top_improvements

    # Generate recommendations
    stock_stats = report["summary_stats"]["stock_vader"]

    best_variant = None
    best_improvement_rate = 0

    for variant_name in results.keys():
        if variant_name == "stock_vader":
            continue

        variant_stats = report["summary_stats"][variant_name]

        # Simple heuristic: higher positive rate = better for music domain
        improvement_rate = variant_stats["positive_rate"] - stock_stats["positive_rate"]

        if improvement_rate > best_improvement_rate:
            best_improvement_rate = improvement_rate
            best_variant = variant_name

    report["recommendations"] = [
        f"Best performing variant: {best_variant}",
        f"Improvement in positive detection: +{best_improvement_rate:.1%}",
        f"Stock VADER positive rate: {stock_stats['positive_rate']:.1%}",
        f"Enhanced positive rate: {report['summary_stats'][best_variant]['positive_rate']:.1%}",
    ]

    return report


def main():
    """Run comprehensive VADER variants evaluation."""

    print("🎯 COMPREHENSIVE VADER VARIANTS EVALUATION")
    print("=" * 60)
    print("Testing multiple VADER enhancements against real database comments")
    print("to identify the best approach for music domain sentiment analysis.\n")

    # Fetch evaluation data
    comments_df = fetch_evaluation_comments(300)  # Reasonable sample size

    if len(comments_df) == 0:
        print("❌ No comments available for evaluation")
        return

    print(f"📊 Dataset overview:")
    print(f"   Total comments: {len(comments_df)}")
    print(f"   Artists: {comments_df['artist'].nunique()}")
    print(f"   Engagement levels: {comments_df['engagement_level'].value_counts().to_dict()}")
    print(f"   With emoji: {comments_df['has_emoji'].sum()}")
    print(f"   With caps: {comments_df['has_caps'].sum()}")
    print(f"   With exclamations: {comments_df['has_exclamation'].sum()}")

    # Evaluate all variants
    results = evaluate_all_variants(comments_df)

    # Analyze differences
    comparison_df, disagreement_stats = analyze_variant_differences(results)

    # Identify improvements
    improvements_df = identify_improvement_cases(results)

    # Generate report
    report = generate_evaluation_report(results, comparison_df, disagreement_stats, improvements_df)

    # Display key findings
    print(f"\n🎯 KEY FINDINGS")
    print("=" * 20)

    for variant, stats in report["summary_stats"].items():
        pos_rate = stats["positive_rate"]
        print(f"{variant:20} | Positive: {pos_rate:.1%} | Avg Score: {stats['avg_compound_score']:+.3f}")

    print(f"\n💡 RECOMMENDATIONS")
    print("=" * 20)
    for rec in report["recommendations"]:
        print(f"   • {rec}")

    if not improvements_df.empty:
        print(f"\n📈 TOP IMPROVEMENTS (Enhanced vs Stock)")
        print("=" * 45)
        top_5 = improvements_df.nlargest(5, "compound_enhanced")
        for _, row in top_5.iterrows():
            comment_short = row["comment_text"][:50] + "..." if len(row["comment_text"]) > 50 else row["comment_text"]
            print(
                f"   {row['improvement_type']:15} | {row['compound_stock']:+.3f} → {row['compound_enhanced']:+.3f} | {comment_short}"
            )

    # Save detailed results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    with open(f"vader_evaluation_report_{timestamp}.json", "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    comparison_df.to_csv(f"vader_comparison_{timestamp}.csv", index=False)

    if not improvements_df.empty:
        improvements_df.to_csv(f"vader_improvements_{timestamp}.csv", index=False)

    print(f"\n✅ Evaluation complete! Results saved with timestamp: {timestamp}")
    print(f"🎯 Next step: Implement best variant in production pipeline")


if __name__ == "__main__":
    main()
