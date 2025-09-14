#!/usr/bin/env python3
"""
Comprehensive ETL Pipeline

This script runs the complete data pipeline:
1. Sentiment analysis for new comments
2. Bot detection and scoring
3. Data quality validation
4. Performance metrics update
5. Notebook execution

Features:
- Fail-fast error handling
- Comprehensive logging
- Progress tracking
- Data validation at each step
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from icatalogviz.bot_detection import BotDetectionConfig, analyze_bot_patterns, store_bot_analysis
from tools.etl.sentiment_analysis import process_sentiment_analysis
from web.etl_helpers import finish_etl_run, get_engine, start_etl_run


def validate_database_schema(engine) -> bool:
    """Validate that all required tables exist with proper structure."""
    print("🔍 Validating database schema...")

    required_tables = {
        "youtube_videos": ["video_id", "channel_title", "title", "published_at"],
        "youtube_comments": ["comment_id", "video_id", "comment_text", "author_name", "published_at"],
        "comment_sentiment": ["comment_id", "video_id", "sentiment_score", "confidence_score"],
        "youtube_etl_runs": ["channel_id", "status", "started_at"],
    }

    with engine.connect() as conn:
        for table_name, required_cols in required_tables.items():
            try:
                # Check table exists
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                existing_cols = {row.Field for row in result}

                # Check required columns exist
                missing_cols = set(required_cols) - existing_cols
                if missing_cols:
                    print(f"❌ Table {table_name} missing columns: {missing_cols}")
                    return False

                print(f"✅ Table {table_name} validated")

            except Exception as e:
                print(f"❌ Table {table_name} validation failed: {e}")
                return False

    print("✅ Database schema validation complete")
    return True


def run_sentiment_analysis(engine) -> dict:
    """Run sentiment analysis on new comments."""
    print("\n🧠 Running sentiment analysis...")

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

    # Process in batches
    total_processed = 0
    batch_size = 500
    max_batches = 20  # Prevent runaway processing

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


def run_bot_detection(engine) -> dict:
    """Run bot detection analysis on recent comments."""
    print("\n🤖 Running bot detection analysis...")

    try:
        # Configure bot detection with music industry specific settings
        config = BotDetectionConfig(
            whitelist_phrases=frozenset(
                {
                    # Music-specific legitimate expressions
                    "love this",
                    "dope",
                    "this is dope",
                    "great song",
                    "love u",
                    "🔥",
                    "fire",
                    "hard",
                    "so hard",
                    "too hard",
                    "this hard",
                    "this fire",
                    "straight fire",
                    "banger",
                    "slaps",
                    "goated",
                    "amazing",
                    "incredible",
                    "beautiful",
                    "perfect",
                    "masterpiece",
                    "vibes",
                    "mood",
                    "hits different",
                    "on repeat",
                    "can't stop listening",
                    "talent",
                    "gifted",
                    "underrated",
                    "deserves more",
                    "next level",
                }
            ),
            near_dupe_threshold=0.88,  # Slightly lower for music comments
            min_dupe_cluster=3,
            burst_window_seconds=45,  # Longer window for organic fan reactions
        )

        # Analyze recent comments (last 30 days)
        analysis_results = analyze_bot_patterns(engine, config=config, days=30)

        # Store results
        store_bot_analysis(engine, analysis_results)

        # Summary statistics
        total_comments = len(analysis_results)
        high_risk = len(analysis_results[analysis_results["bot_risk_level"] == "High"])
        medium_risk = len(analysis_results[analysis_results["bot_risk_level"] == "Medium"])
        low_risk = len(analysis_results[analysis_results["bot_risk_level"] == "Low"])

        print(f"📊 Bot Detection Summary:")
        print(f"   Total comments analyzed: {total_comments:,}")
        print(f"   🔴 High risk (likely bots): {high_risk:,} ({high_risk/total_comments*100:.1f}%)")
        print(f"   🟡 Medium risk: {medium_risk:,} ({medium_risk/total_comments*100:.1f}%)")
        print(f"   🟢 Low risk (likely human): {low_risk:,} ({low_risk/total_comments*100:.1f}%)")

        return {
            "analyzed": total_comments,
            "high_risk": high_risk,
            "medium_risk": medium_risk,
            "low_risk": low_risk,
            "status": "success",
        }

    except Exception as e:
        print(f"❌ Bot detection failed: {e}")
        return {"status": "failed", "error": str(e)}


def validate_data_quality(engine) -> dict:
    """Run comprehensive data quality checks."""
    print("\n🔍 Running data quality validation...")

    quality_issues = []

    with engine.connect() as conn:
        # Check for missing critical data
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
            result = conn.execute(text(query))
            count = result.fetchone()[0]

            if count > 0:
                quality_issues.append(f"{check_name}: {count:,} records")
                print(f"⚠️ {check_name}: {count:,} records")
            else:
                print(f"✅ {check_name}: OK")

        # Overall data statistics
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

    quality_score = max(0, 100 - len(quality_issues) * 5)  # Deduct 5% per issue

    print(f"\n🏆 Overall Data Quality Score: {quality_score:.1f}%")

    return {
        "quality_score": quality_score,
        "issues": quality_issues,
        "stats": {
            "videos": stats.total_videos,
            "comments": stats.total_comments,
            "sentiment_records": stats.total_sentiment,
            "artists": stats.total_artists,
            "sentiment_coverage": sentiment_coverage,
        },
    }


def update_performance_metrics(engine) -> dict:
    """Update performance tracking metrics."""
    print("\n📈 Updating performance metrics...")

    try:
        with engine.connect() as conn:
            # Update artist performance summary
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS artist_performance_summary (
                    artist_name VARCHAR(255) PRIMARY KEY,
                    total_videos INT DEFAULT 0,
                    total_views BIGINT DEFAULT 0,
                    total_comments INT DEFAULT 0,
                    avg_sentiment DECIMAL(5,3) DEFAULT 0,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
                )
            )

            # Refresh performance data
            conn.execute(
                text(
                    """
                INSERT INTO artist_performance_summary
                (artist_name, total_videos, total_views, total_comments, avg_sentiment)
                SELECT
                    yv.channel_title as artist_name,
                    COUNT(DISTINCT yv.video_id) as total_videos,
                    COALESCE(SUM(ym.views), 0) as total_views,
                    COUNT(yc.comment_id) as total_comments,
                    COALESCE(AVG(cs.sentiment_score), 0) as avg_sentiment
                FROM youtube_videos yv
                LEFT JOIN youtube_metrics ym ON yv.video_id = ym.video_id
                LEFT JOIN youtube_comments yc ON yv.video_id = yc.video_id
                LEFT JOIN comment_sentiment cs ON yc.comment_id = cs.comment_id
                WHERE yv.channel_title IS NOT NULL
                GROUP BY yv.channel_title
                ON DUPLICATE KEY UPDATE
                    total_videos = VALUES(total_videos),
                    total_views = VALUES(total_views),
                    total_comments = VALUES(total_comments),
                    avg_sentiment = VALUES(avg_sentiment),
                    last_updated = CURRENT_TIMESTAMP
            """
                )
            )

            conn.commit()

            # Get summary
            result = conn.execute(text("SELECT COUNT(*) FROM artist_performance_summary"))
            artist_count = result.fetchone()[0]

            print(f"✅ Updated performance metrics for {artist_count} artists")

            return {"updated_artists": artist_count, "status": "success"}

    except Exception as e:
        print(f"❌ Performance metrics update failed: {e}")
        return {"status": "failed", "error": str(e)}


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
                [sys.executable, "tools/run_notebooks.py", notebook, "--allow-errors"],
                capture_output=True,
                text=True,
                cwd=".",
            )

            if result.returncode == 0:
                results["executed"].append(notebook)
                print(f"  ✅ {notebook} completed")
            else:
                results["failed"].append(notebook)
                print(f"  ❌ {notebook} failed: {result.stderr}")

        except Exception as e:
            results["failed"].append(notebook)
            print(f"  ❌ {notebook} error: {e}")

    return results


def main():
    """Run the comprehensive ETL pipeline."""
    print("🚀 Starting Comprehensive ETL Pipeline")
    print("=" * 60)

    # Start ETL run tracking (skip if not available)
    run_info = None
    try:
        run_info = start_etl_run("COMPREHENSIVE_ETL", "Full pipeline: sentiment, bot detection, quality checks")
    except Exception as e:
        print(f"⚠️ ETL run tracking not available: {e}")
        run_info = {"run_id": None}

    try:
        engine = get_engine()

        # Step 1: Validate database schema
        if not validate_database_schema(engine):
            raise RuntimeError("Database schema validation failed")

        # Step 2: Run sentiment analysis
        sentiment_results = run_sentiment_analysis(engine)

        # Step 3: Run bot detection
        bot_results = run_bot_detection(engine)

        # Step 4: Validate data quality
        quality_results = validate_data_quality(engine)

        # Step 5: Update performance metrics
        performance_results = update_performance_metrics(engine)

        # Step 6: Run analysis notebooks
        notebooks_to_run = ["notebooks/02_artist_comparison.ipynb", "notebooks/03_appendix_data_quality.ipynb"]
        notebook_results = run_notebooks(notebooks_to_run)

        # Summary report
        print("\n" + "=" * 60)
        print("🎉 ETL PIPELINE COMPLETE - SUMMARY REPORT")
        print("=" * 60)

        print(f"📊 Sentiment Analysis:")
        print(f"   Processed: {sentiment_results.get('processed', 0):,} comments")

        if bot_results.get("status") == "success":
            print(f"🤖 Bot Detection:")
            print(f"   Analyzed: {bot_results.get('analyzed', 0):,} comments")
            print(f"   High risk: {bot_results.get('high_risk', 0):,}")
            print(f"   Medium risk: {bot_results.get('medium_risk', 0):,}")
            print(f"   Low risk: {bot_results.get('low_risk', 0):,}")

        print(f"🔍 Data Quality:")
        print(f"   Quality Score: {quality_results['quality_score']:.1f}%")
        print(f"   Issues Found: {len(quality_results['issues'])}")

        print(f"📈 Performance Metrics:")
        print(f"   Artists Updated: {performance_results.get('updated_artists', 0)}")

        print(f"📓 Notebooks:")
        print(f"   Executed: {len(notebook_results['executed'])}")
        print(f"   Failed: {len(notebook_results['failed'])}")

        # Determine overall status
        critical_failures = [
            sentiment_results.get("status") == "failed",
            bot_results.get("status") == "failed",
            quality_results["quality_score"] < 80,
            performance_results.get("status") == "failed",
        ]

        if any(critical_failures):
            status = "COMPLETED_WITH_ISSUES"
            message = "Pipeline completed but some issues detected"
        else:
            status = "SUCCESS"
            message = "All pipeline steps completed successfully"

        print(f"\n🏆 Overall Status: {status}")

        # Finish ETL run tracking
        if run_info and run_info.get("run_id"):
            finish_etl_run(run_info["run_id"], status, message)

        return 0 if status == "SUCCESS" else 1

    except Exception as e:
        error_msg = f"Pipeline failed: {e}"
        print(f"\n❌ {error_msg}")

        if run_info and run_info.get("run_id"):
            finish_etl_run(run_info["run_id"], "FAILED", error_msg)

        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
