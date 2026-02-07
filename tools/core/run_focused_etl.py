#!/usr/bin/env python3
"""
Focused ETL Pipeline-Production Ready

This script runs essential data processing tasks:
1. Sentiment analysis for new comments
2. Data quality validation
3. Notebook execution

Designed to be robust, fast, and fail-safe.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TypedDict, cast

from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import text
from sqlalchemy.engine import Engine

from tools.core.sentiment_analysis import process_sentiment_analysis
from web.etl_helpers import get_engine

# ============================================================================
# IMPORTANT / DO NOT REGRESS (USER REQUEST)
# ----------------------------------------------------------------------------
# The focused ETL gate is for mission-critical freshness + data quality.
# Notebook execution is useful but non-critical by default; notebook failures
# should be surfaced as warnings, not hard failures, unless explicitly enabled
# via FOCUSED_ETL_NOTEBOOKS_REQUIRED=true.
# ============================================================================


sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class BotDetectionResults(TypedDict, total=False):
    processed: int
    status: str
    high_risk_bots: int
    medium_risk_bots: int
    bot_percentage: float
    error: str


class SentimentRunResults(TypedDict):
    processed: int
    status: str


class DataQualityStats(TypedDict):
    videos: int
    comments: int
    sentiment_records: int
    artists: int
    sentiment_coverage: float


class DataQualityResults(TypedDict):
    quality_score: float
    issues: list[str]
    stats: DataQualityStats


class NotebookRunResults(TypedDict):
    executed: list[str]
    failed: list[str]


class PreflightResults(TypedDict):
    songs_inserted: int
    songs_rejected: int
    normalized_upserts: int
    songs_count: int
    normalized_count: int
    normalized_isrc_nulls: int


def _read_bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean environment flag with explicit accepted truthy values."""

    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def determine_pipeline_status(
    bot_results: BotDetectionResults,
    sentiment_results: SentimentRunResults,
    quality_results: DataQualityResults,
    notebook_results: NotebookRunResults,
) -> tuple[str, int]:
    """Classify focused ETL run status and exit code.

    LOUD NOTE (user-requested guardrail):
    Notebook execution is an auxiliary reporting step. Core ETL freshness,
    sentiment processing, and data quality checks are mission-critical. By
    default, notebook failures are warnings and do not block the pipeline.
    Set FOCUSED_ETL_NOTEBOOKS_REQUIRED=true to make notebook failures blocking.
    """

    notebook_failures = len(notebook_results.get("failed", [])) > 0
    notebook_gate_enabled = _read_bool_env("FOCUSED_ETL_NOTEBOOKS_REQUIRED", default=False)

    core_failure = any(
        [
            bot_results.get("status") == "failed",
            sentiment_results.get("status") == "failed",
            float(quality_results.get("quality_score", 0.0)) < 80.0,
        ]
    )
    if notebook_gate_enabled and notebook_failures:
        core_failure = True

    if core_failure:
        return "COMPLETED_WITH_ISSUES", 1
    if notebook_failures:
        return "SUCCESS_WITH_WARNINGS", 0
    return "SUCCESS", 0


def run_sentiment_analysis(engine: Engine) -> SentimentRunResults:
    """Run sentiment analysis on new comments."""
    print("🧠 Running sentiment analysis...")

    # Check for unprocessed comments
    with engine.connect() as conn:
        unprocessed_count_raw = conn.execute(text("""
                SELECT COUNT(*) as unprocessed_count
                FROM youtube_comments yc
                LEFT JOIN comment_sentiment cs ON yc.comment_id = cs.comment_id
                WHERE cs.comment_id IS NULL
                AND yc.comment_text IS NOT NULL
                AND yc.comment_text != ''
            """)).scalar()
        unprocessed_count = int(unprocessed_count_raw or 0)
        print(f"📊 Found {unprocessed_count:,} unprocessed comments")

        if unprocessed_count == 0:
            print("✅ No new comments to process")
            return {"processed": 0, "status": "up_to_date"}

    # Process in reasonable batches
    total_processed = 0
    batch_size = 200
    max_batches = 10  # Reasonable limit

    for batch_num in range(max_batches):
        processed = int(process_sentiment_analysis(engine, limit=batch_size) or 0)
        total_processed += processed

        if processed == 0:
            break

        print(f"  Batch {batch_num + 1}: Processed {processed} comments")

        if processed < batch_size:
            break

    print(f"✅ Sentiment analysis complete: {total_processed:,} comments processed")
    return {"processed": total_processed, "status": "success"}


def validate_data_quality(engine: Engine) -> DataQualityResults:
    """Run focused data quality checks."""
    print("🔍 Running data quality validation...")

    quality_issues: list[str] = []
    overview_stats: dict[str, int] = {
        "total_videos": 0,
        "total_comments": 0,
        "total_sentiment": 0,
        "total_artists": 0,
    }
    sentiment_coverage = 0.0

    with engine.connect() as conn:
        # Essential data quality checks
        checks: list[tuple[str, str]] = [
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
                count_raw = conn.execute(text(query)).scalar()
                count = int(count_raw or 0)

                if count > 0:
                    quality_issues.append(f"{check_name}: {count:,} records")
                    print(f"⚠️ {check_name}: {count:,} records")
                else:
                    print(f"✅ {check_name}: OK")
            except Exception as exc:
                print(f"❌ {check_name}: Check failed - {exc}")
                quality_issues.append(f"{check_name}: Check failed")

        # Overall data statistics
        try:
            stats_row = conn.execute(text("""
                    SELECT
                        (SELECT COUNT(*) FROM youtube_videos) as total_videos,
                        (SELECT COUNT(*) FROM youtube_comments) as total_comments,
                        (SELECT COUNT(*) FROM comment_sentiment) as total_sentiment,
                        (SELECT COUNT(DISTINCT channel_title) FROM youtube_videos WHERE channel_title IS NOT NULL) as total_artists  # noqa: E501
                """)).mappings().one()
            overview_stats = {
                "total_videos": int(stats_row["total_videos"] or 0),
                "total_comments": int(stats_row["total_comments"] or 0),
                "total_sentiment": int(stats_row["total_sentiment"] or 0),
                "total_artists": int(stats_row["total_artists"] or 0),
            }

            print("\n📊 Data Overview:")
            print(f"   Videos: {overview_stats['total_videos']:,}")
            print(f"   Comments: {overview_stats['total_comments']:,}")
            print(f"   Sentiment records: {overview_stats['total_sentiment']:,}")
            print(f"   Artists: {overview_stats['total_artists']:,}")

            # Calculate sentiment coverage
            if overview_stats["total_comments"] > 0:
                sentiment_coverage = (overview_stats["total_sentiment"] / overview_stats["total_comments"]) * 100.0
            else:
                sentiment_coverage = 0.0
            print(f"   Sentiment coverage: {sentiment_coverage:.1f}%")

        except Exception as exc:
            print(f"❌ Error getting data overview: {exc}")
            sentiment_coverage = 0

    quality_score = max(0, 100 - len(quality_issues) * 5)  # Deduct 5% per issue

    print(f"\n🏆 Overall Data Quality Score: {quality_score:.1f}%")

    return {
        "quality_score": float(quality_score),
        "issues": quality_issues,
        "stats": {
            "videos": overview_stats["total_videos"],
            "comments": overview_stats["total_comments"],
            "sentiment_records": overview_stats["total_sentiment"],
            "artists": overview_stats["total_artists"],
            "sentiment_coverage": float(sentiment_coverage),
        },
    }


def run_notebooks(notebook_list: list[str]) -> NotebookRunResults:
    """Execute analysis notebooks."""
    print(f"\n📓 Running {len(notebook_list)} notebooks...")

    results: NotebookRunResults = {"executed": [], "failed": []}

    for notebook in notebook_list:
        notebook_path = str(notebook)
        try:
            print(f"  Executing {notebook_path}...")

            result = subprocess.run(
                [sys.executable, "tools/development/run_notebooks.py", notebook_path],
                capture_output=True,
                text=True,
                cwd=".",
            )

            if result.returncode == 0:
                results["executed"].append(notebook_path)
                print(f"  ✅ {notebook_path} completed")
            else:
                results["failed"].append(notebook_path)
                print(f"  ❌ {notebook_path} failed")

        except Exception as exc:
            results["failed"].append(notebook_path)
            print(f"  ❌ {notebook_path} error: {exc}")

    return results


def run_bot_detection(engine: Engine) -> BotDetectionResults:
    """Run bot detection on recent comments."""
    print("🤖 Running bot detection analysis...")

    try:
        from src.youtubeviz.bot_detection import (
            BotDetectionConfig,
            analyze_bot_patterns,  # pyright: ignore[reportUnknownVariableType]
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
        total_analyzed = len(bot_results.index)

        if total_analyzed > 0:
            high_risk_rows = cast(DataFrame, bot_results[bot_results["bot_risk_level"] == "High"])
            medium_risk_rows = cast(DataFrame, bot_results[bot_results["bot_risk_level"] == "Medium"])
            high_risk_count = high_risk_rows.shape[0]
            medium_risk_count = medium_risk_rows.shape[0]

            print("📊 Bot Detection Results:")
            print(f"   Total comments analyzed: {total_analyzed:,}")
            print(f"   High risk bots detected: {high_risk_count:,}")
            print(f"   Medium risk bots detected: {medium_risk_count:,}")

            # Store bot detection results (would save to database in real implementation)
            bot_percentage = (high_risk_count / total_analyzed * 100.0) if total_analyzed > 0 else 0.0
            print(f"   Bot percentage: {bot_percentage:.1f}%")

            return {
                "processed": total_analyzed,
                "high_risk_bots": high_risk_count,
                "medium_risk_bots": medium_risk_count,
                "bot_percentage": float(bot_percentage),
                "status": "success",
            }
        else:
            print("✅ No comments found for bot detection")
            return {"processed": 0, "status": "no_data"}

    except ImportError as exc:
        print(f"⚠️ Bot detection module not available: {str(exc)}")
        print("   Bot detection will be skipped-install youtubeviz package to enable")
        return {"processed": 0, "status": "module_unavailable"}
    except Exception as exc:
        print(f"❌ Bot detection failed: {str(exc)}")
        return {"processed": 0, "status": "failed", "error": str(exc)}


def preflight_setup() -> PreflightResults:  # noqa: C901
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
        result = subprocess.run([sys.executable, "tools/core/create_tables.py"], capture_output=True, text=True)
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
    except Exception as exc:
        print(f"   ❌ Failed ensuring tables: {exc}")

    # 3) Optional: seed songs from CSV via env var AUTO_LOAD_SONGS_CSV
    songs_inserted = 0
    songs_rejected = 0
    csv_path = os.getenv("AUTO_LOAD_SONGS_CSV")
    if csv_path:
        try:
            # Use subprocess to avoid import collisions on 'scripts' package name
            result = subprocess.run(
                [sys.executable, "scripts/load_songs_csv.py", csv_path], capture_output=True, text=True
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
        except Exception as exc:
            print(f"   ❌ Failed loading songs CSV '{csv_path}': {exc}")

    # 4) Run normalization to populate music_videos_normalized
    normalized = 0
    try:
        # Lazy import to reduce top-level import fragility
        from src.youtubeviz.normalization import run_normalization

        normalized = int(run_normalization() or 0)
        print(f"   ✅ Normalized rows upserted: {normalized}")
    except Exception as exc:
        print(f"   ❌ Normalization failed: {exc}")

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
            f"   📈 DQ snapshot -> songs: {songs_cnt}, normalized: {norm_cnt}, normalized.isrc NULL / blank: {isrc_nulls}"
        )
    except Exception as exc:
        songs_cnt = norm_cnt = isrc_nulls = 0
        print(f"   ⚠️ DQ snapshot failed: {exc}")

    return {
        "songs_inserted": songs_inserted,
        "songs_rejected": songs_rejected,
        "normalized_upserts": normalized,
        "songs_count": songs_cnt,
        "normalized_count": norm_cnt,
        "normalized_isrc_nulls": isrc_nulls,
    }


def main() -> int:
    """Run the focused ETL pipeline."""
    print("🚀 Starting Focused ETL Pipeline")
    print("=" * 50)

    try:
        # Run preflight once to make onboarding / first-run smooth
        preflight_setup()

        engine = get_engine()

        # Step 1: Run bot detection (before sentiment analysis)
        bot_results = run_bot_detection(engine)

        # Step 2: Run sentiment analysis (after bot detection)
        sentiment_results = run_sentiment_analysis(engine)

        # Step 3: Validate data quality
        quality_results = validate_data_quality(engine)

        # Step 3: Run analysis notebooks (organized under notebooks / analysis and notebooks / quality)
        notebooks_to_run = [
            "notebooks/MusicScope™_Professional_Dashboard.ipynb",
        ]
        notebook_results = run_notebooks(notebooks_to_run)

        # Summary report
        print("\n" + "=" * 50)
        print("🎉 FOCUSED ETL PIPELINE COMPLETE")
        print("=" * 50)

        print("🤖 Bot Detection:")
        print(f"   Comments analyzed: {bot_results.get('processed', 0):,}")
        print(f"   High risk bots: {bot_results.get('high_risk_bots', 0):,}")
        print(f"   Bot percentage: {bot_results.get('bot_percentage', 0):.1f}%")

        print("📊 Sentiment Analysis:")
        print(f"   Processed: {sentiment_results.get('processed', 0):,} comments")

        print("🔍 Data Quality:")
        print(f"   Quality Score: {quality_results['quality_score']:.1f}%")
        print(f"   Issues Found: {len(quality_results['issues'])}")

        print("📓 Notebooks:")
        print(f"   Executed: {len(notebook_results['executed'])}")
        print(f"   Failed: {len(notebook_results['failed'])}")

        status, exit_code = determine_pipeline_status(
            bot_results=bot_results,
            sentiment_results=sentiment_results,
            quality_results=quality_results,
            notebook_results=notebook_results,
        )
        if status == "SUCCESS_WITH_WARNINGS":
            print(
                "\n⚠️ Notebook execution had failures but core ETL freshness/quality passed. "
                "Set FOCUSED_ETL_NOTEBOOKS_REQUIRED=true to make notebook failures blocking."
            )
        elif status == "COMPLETED_WITH_ISSUES":
            print("\n⚠️ One or more mission-critical checks failed.")

        badge = "🏆" if exit_code == 0 else "⚠️"
        print(f"\n{badge} Overall Status: {status}")
        return exit_code

    except Exception as exc:
        print(f"\n❌ Pipeline failed: {exc}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
