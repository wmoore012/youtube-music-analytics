#!/usr/bin/env python3
"""
Focused ETL Pipeline - Production Ready

This script runs essential data processing tasks:
1. Sentiment analysis for new comments
2. Data quality validation
3. Notebook execution

Designed to be robust, fast, and fail-safe.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from tools.etl.sentiment_analysis import process_sentiment_analysis
from web.etl_helpers import get_engine


def run_sentiment_analysis(engine) -> dict:
    """Run sentiment analysis on new comments."""
    print("🧠 Running sentiment analysis...")

    # Check for unprocessed comments
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) as unprocessed_count
            FROM youtube_comments yc
            LEFT JOIN comment_sentiment cs ON yc.comment_id = cs.comment_id
            WHERE cs.comment_id IS NULL
            AND yc.comment_text IS NOT NULL
            AND yc.comment_text != ''
        """
            )
        )

        unprocessed_count = result.fetchone()[0]
        print(f"📊 Found {unprocessed_count:,} unprocessed comments")

        if unprocessed_count == 0:
            print("✅ No new comments to process")
            return {"processed": 0, "status": "up_to_date"}

    # Process in reasonable batches
    total_processed = 0
    batch_size = 200
    max_batches = 10  # Reasonable limit

    for batch_num in range(max_batches):
        processed = process_sentiment_analysis(engine, limit=batch_size)
        total_processed += processed

        if processed == 0:
            break

        print(f"  Batch {batch_num + 1}: Processed {processed} comments")

        if processed < batch_size:
            break

    print(f"✅ Sentiment analysis complete: {total_processed:,} comments processed")
    return {"processed": total_processed, "status": "success"}


def validate_data_quality(engine) -> dict:
    """Run focused data quality checks."""
    print("🔍 Running data quality validation...")

    quality_issues = []

    with engine.connect() as conn:
        # Essential data quality checks
        checks = [
            ("Missing video titles", "SELECT COUNT(*) FROM youtube_videos WHERE title IS NULL OR title = ''"),
            (
                "Missing artist names",
                "SELECT COUNT(*) FROM youtube_videos WHERE channel_title IS NULL OR channel_title = ''",
            ),
            (
                "Comments without text",
                "SELECT COUNT(*) FROM youtube_comments WHERE comment_text IS NULL OR comment_text = ''",
            ),
            (
                "Comments without authors",
                "SELECT COUNT(*) FROM youtube_comments WHERE author_name IS NULL OR author_name = ''",
            ),
            ("Sentiment without confidence", "SELECT COUNT(*) FROM comment_sentiment WHERE confidence_score IS NULL"),
            ("Future published dates", "SELECT COUNT(*) FROM youtube_videos WHERE published_at > NOW()"),
            (
                "Negative metrics",
                "SELECT COUNT(*) FROM youtube_metrics WHERE view_count < 0 OR like_count < 0 OR comment_count < 0",
            ),
        ]

        for check_name, query in checks:
            try:
                result = conn.execute(text(query))
                count = result.fetchone()[0]

                if count > 0:
                    quality_issues.append(f"{check_name}: {count:,} records")
                    print(f"⚠️ {check_name}: {count:,} records")
                else:
                    print(f"✅ {check_name}: OK")
            except Exception as e:
                print(f"❌ {check_name}: Check failed - {e}")
                quality_issues.append(f"{check_name}: Check failed")

        # Overall data statistics
        try:
            result = conn.execute(
                text(
                    """
                SELECT
                    (SELECT COUNT(*) FROM youtube_videos) as total_videos,
                    (SELECT COUNT(*) FROM youtube_comments) as total_comments,
                    (SELECT COUNT(*) FROM comment_sentiment) as total_sentiment,
                    (SELECT COUNT(DISTINCT channel_title) FROM youtube_videos WHERE channel_title IS NOT NULL) as total_artists
            """
                )
            )

            stats = result.fetchone()

            print(f"\n📊 Data Overview:")
            print(f"   Videos: {stats.total_videos:,}")
            print(f"   Comments: {stats.total_comments:,}")
            print(f"   Sentiment records: {stats.total_sentiment:,}")
            print(f"   Artists: {stats.total_artists:,}")

            # Calculate sentiment coverage
            sentiment_coverage = (stats.total_sentiment / stats.total_comments * 100) if stats.total_comments > 0 else 0
            print(f"   Sentiment coverage: {sentiment_coverage:.1f}%")

        except Exception as e:
            print(f"❌ Error getting data overview: {e}")
            stats = None
            sentiment_coverage = 0

    quality_score = max(0, 100 - len(quality_issues) * 5)  # Deduct 5% per issue

    print(f"\n🏆 Overall Data Quality Score: {quality_score:.1f}%")

    return {
        "quality_score": quality_score,
        "issues": quality_issues,
        "stats": {
            "videos": stats.total_videos if stats else 0,
            "comments": stats.total_comments if stats else 0,
            "sentiment_records": stats.total_sentiment if stats else 0,
            "artists": stats.total_artists if stats else 0,
            "sentiment_coverage": sentiment_coverage,
        },
    }


def run_notebooks(notebook_list: list) -> dict:
    """Execute analysis notebooks."""
    print(f"\n📓 Running {len(notebook_list)} notebooks...")

    import subprocess
    import sys

    results = {"executed": [], "failed": []}

    for notebook in notebook_list:
        try:
            print(f"  Executing {notebook}...")

            result = subprocess.run(
                [sys.executable, "tools/run_notebooks.py", notebook],
                capture_output=True,
                text=True,
                cwd=".",
            )

            if result.returncode == 0:
                results["executed"].append(notebook)
                print(f"  ✅ {notebook} completed")
            else:
                results["failed"].append(notebook)
                print(f"  ❌ {notebook} failed")

        except Exception as e:
            results["failed"].append(notebook)
            print(f"  ❌ {notebook} error: {e}")

    return results


def main():
    """Run the focused ETL pipeline."""
    print("🚀 Starting Focused ETL Pipeline")
    print("=" * 50)

    try:
        engine = get_engine()

        # Step 1: Run sentiment analysis
        sentiment_results = run_sentiment_analysis(engine)

        # Step 2: Validate data quality
        quality_results = validate_data_quality(engine)

        # Step 3: Run analysis notebooks (organized under notebooks/analysis and notebooks/quality)
        notebooks_to_run = [
            "notebooks/analysis/02_artist_comparison.ipynb",
            "notebooks/quality/03_appendix_data_quality.ipynb",
        ]
        notebook_results = run_notebooks(notebooks_to_run)

        # Summary report
        print("\n" + "=" * 50)
        print("🎉 FOCUSED ETL PIPELINE COMPLETE")
        print("=" * 50)

        print(f"📊 Sentiment Analysis:")
        print(f"   Processed: {sentiment_results.get('processed', 0):,} comments")

        print(f"🔍 Data Quality:")
        print(f"   Quality Score: {quality_results['quality_score']:.1f}%")
        print(f"   Issues Found: {len(quality_results['issues'])}")

        print(f"📓 Notebooks:")
        print(f"   Executed: {len(notebook_results['executed'])}")
        print(f"   Failed: {len(notebook_results['failed'])}")

        # Determine overall status
        critical_failures = [
            sentiment_results.get("status") == "failed",
            quality_results["quality_score"] < 80,
            len(notebook_results["failed"]) > 0,
        ]

        if any(critical_failures):
            status = "COMPLETED_WITH_ISSUES"
            print(f"\n⚠️ Overall Status: {status}")
            return 1
        else:
            status = "SUCCESS"
            print(f"\n🏆 Overall Status: {status}")
            return 0

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
