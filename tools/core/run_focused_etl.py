#!/usr / bin / env python3
"""
Focused ETL Pipeline-Production Ready

This script runs essential data processing tasks:
1. Sentiment analysis for new comments
2. Data quality validation
3. Notebook execution

Designed to be robust, fast, and fail-safe.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Preflight utilities
import os
import subprocess

from dotenv import load_dotenv
from sqlalchemy import text

from tools.core.sentiment_analysis import process_sentiment_analysis
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
                    (SELECT COUNT(DISTINCT channel_title) FROM youtube_videos WHERE channel_title IS NOT NULL) as total_artists  # noqa: E501
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

    quality_score = max(0, 100-len(quality_issues) * 5)  # Deduct 5% per issue

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
                [sys.executable, "tools / run_notebooks.py", notebook],
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


def run_bot_detection(engine) -> dict:
    """Run bot detection on recent comments."""
    print("🤖 Running bot detection analysis...")

    try:
        import os

        from src.youtubeviz.bot_detection import (
            BotDetectionConfig,
            analyze_bot_patterns,
        )

        # Check if bot detection is enabled
        bot_detection_enabled = os.getenv("BOT_DETECTION_ENABLED", "false").lower() == "true"

        if not bot_detection_enabled:
            print("⚠️ Bot detection is disabled-focusing on core analytics")
            return {"processed": 0, "status": "disabled"}

        # Configure bot detection
        config = BotDetectionConfig()

        # Run bot detection on recent comments
        lookback_days = int(os.getenv("BOT_DETECTION_DAYS_LOOKBACK", "30"))
        bot_results = analyze_bot_patterns(engine, config=config, days=lookback_days)

        if len(bot_results) > 0:
            high_risk_count = len(bot_results[bot_results["bot_risk_level"] == "High"])
            medium_risk_count = len(bot_results[bot_results["bot_risk_level"] == "Medium"])

            print(f"📊 Bot Detection Results:")
            print(f"   Total comments analyzed: {len(bot_results):,}")
            print(f"   High risk bots detected: {high_risk_count:,}")
            print(f"   Medium risk bots detected: {medium_risk_count:,}")

            # Store bot detection results (would save to database in real implementation)
            bot_percentage = (high_risk_count / len(bot_results) * 100) if len(bot_results) > 0 else 0
            print(f"   Bot percentage: {bot_percentage:.1f}%")

            return {
                "processed": len(bot_results),
                "high_risk_bots": high_risk_count,
                "medium_risk_bots": medium_risk_count,
                "bot_percentage": bot_percentage,
                "status": "success",
            }
        else:
            print("✅ No comments found for bot detection")
            return {"processed": 0, "status": "no_data"}

    except ImportError as e:
        print(f"⚠️ Bot detection module not available: {str(e)}")
        print("   Bot detection will be skipped-install youtubeviz package to enable")
        return {"processed": 0, "status": "module_unavailable"}
    except Exception as e:
        print(f"❌ Bot detection failed: {str(e)}")
        return {"processed": 0, "status": "failed", "error": str(e)}


def preflight_setup() -> dict:  # noqa: C901
    """Ensure environment, tables, optional seed load, normalization, and quick DQ summary.

    Returns a dict with simple metrics to include in the final summary.
    """
    print("🧰 Preflight: environment, tables, and normalization")
    # 1) Load .env (best-effort; non-destructive)
    try:
        repo_root = Path(__file__).resolve().parents[2]
        load_dotenv(dotenv_path=repo_root / ".env", override=False)
        print("   ✅ .env loaded (if present)")
    except Exception:
        print("   ⚠️ Could not load .env (continuing)")

    # 2) Ensure tables exist (call script to avoid import path / package name clashes)
    try:
        result = subprocess.run([sys.executable, "tools / setup / create_tables.py"], capture_output=True, text=True)
        if result.returncode == 0:
            print("   ✅ Tables ready")
        else:
            print("   ⚠️ Table creation script returned non-zero exit code")
            if result.stdout:
                print("      ├─ stdout:")
                print("\n".join(["      │ " + line for line in result.stdout.strip().splitlines()[-10:]]))
            if result.stderr:
                print("      ├─ stderr:")
                print("\n".join(["      │ " + line for line in result.stderr.strip().splitlines()[-10:]]))
    except Exception as e:
        print(f"   ❌ Failed ensuring tables: {e}")

    # 3) Optional: seed songs from CSV via env var AUTO_LOAD_SONGS_CSV
    songs_inserted = 0
    songs_rejected = 0
    csv_path = os.getenv("AUTO_LOAD_SONGS_CSV")
    if csv_path:
        try:
            # Use subprocess to avoid import collisions on 'scripts' package name
            result = subprocess.run(
                [sys.executable, "scripts / load_songs_csv.py", csv_path], capture_output=True, text=True
            )
            if result.returncode == 0:
                # Try to parse summary line
                line = (result.stdout or "").strip().splitlines()[-1] if (result.stdout or "").strip() else ""
                print(f"   🎵 {line or 'Songs CSV loaded'}")
            else:
                print("   ⚠️ Songs CSV loader returned non-zero exit code")
                if result.stdout:
                    print("      ├─ stdout:")
                    print("\n".join(["      │ " + l for l in result.stdout.strip().splitlines()[-10:]]))  # noqa: E741
                if result.stderr:
                    print("      ├─ stderr:")
                    print("\n".join(["      │ " + l for l in result.stderr.strip().splitlines()[-10:]]))  # noqa: E741
        except Exception as e:
            print(f"   ❌ Failed loading songs CSV '{csv_path}': {e}")

    # 4) Run normalization to populate music_videos_normalized
    normalized = 0
    try:
        # Lazy import to reduce top-level import fragility
        from src.youtubeviz.normalization import run_normalization

        normalized = run_normalization()
        print(f"   ✅ Normalized rows upserted: {normalized}")
    except Exception as e:
        print(f"   ❌ Normalization failed: {e}")

    # 5) Quick DQ: count null ISRCs in normalized + songs count
    try:
        eng = get_engine()
        with eng.connect() as conn:
            songs_cnt = int(conn.execute(text("SELECT COUNT(*) FROM songs")).scalar() or 0)
            norm_cnt = int(conn.execute(text("SELECT COUNT(*) FROM music_videos_normalized")).scalar() or 0)
            isrc_nulls = int(
                conn.execute(
                    text("SELECT COUNT(*) FROM music_videos_normalized WHERE isrc IS NULL OR TRIM(isrc) = ''")
                ).scalar()
                or 0
            )
        print(
            f"   📈 DQ snapshot -> songs: {songs_cnt}, normalized: {
                norm_cnt}, normalized.isrc NULL / blank: {isrc_nulls}"
        )
    except Exception as e:
        songs_cnt = norm_cnt = isrc_nulls = 0
        print(f"   ⚠️ DQ snapshot failed: {e}")

    return {
        "songs_inserted": songs_inserted,
        "songs_rejected": songs_rejected,
        "normalized_upserts": normalized,
        "songs_count": songs_cnt,
        "normalized_count": norm_cnt,
        "normalized_isrc_nulls": isrc_nulls,
    }


def main():
    """Run the focused ETL pipeline."""
    print("🚀 Starting Focused ETL Pipeline")
    print("=" * 50)

    try:
        # Run preflight once to make onboarding / first-run smooth
        _pre = preflight_setup()  # noqa: F841

        engine = get_engine()

        # Step 1: Run bot detection (before sentiment analysis)
        bot_results = run_bot_detection(engine)

        # Step 2: Run sentiment analysis (after bot detection)
        sentiment_results = run_sentiment_analysis(engine)

        # Step 3: Validate data quality
        quality_results = validate_data_quality(engine)

        # Step 3: Run analysis notebooks (organized under notebooks / analysis and notebooks / quality)
        notebooks_to_run = [
            "notebooks / MusicScope™_Professional_Dashboard.ipynb",
        ]
        notebook_results = run_notebooks(notebooks_to_run)

        # Summary report
        print("\n" + "=" * 50)
        print("🎉 FOCUSED ETL PIPELINE COMPLETE")
        print("=" * 50)

        print(f"🤖 Bot Detection:")
        print(f"   Comments analyzed: {bot_results.get('processed', 0):,}")
        print(f"   High risk bots: {bot_results.get('high_risk_bots', 0):,}")
        print(f"   Bot percentage: {bot_results.get('bot_percentage', 0):.1f}%")

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
            bot_results.get("status") == "failed",
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
