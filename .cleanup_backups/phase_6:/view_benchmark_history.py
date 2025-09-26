#!/usr/bin/env python3
"""
View Benchmark History

Query and display benchmark results from the project_benchmarks and
project_benchmark_models tables.
"""

from datetime import datetime
import json

import pandas as pd
from sqlalchemy import text

from web.etl_helpers import get_engine


def view_benchmark_summary():
    """Display summary of all benchmark runs."""

    print("📊 BENCHMARK HISTORY SUMMARY")
    print("=" * 40)

    engine = get_engine()

    with engine.connect() as conn:
        # Get benchmark summary
        summary_query = text(
            """
            SELECT
                benchmark_id,
                benchmark_date,
                total_records,
                unique_videos,
                unique_channels,
                throughput_rows_per_sec,
                sentiment_available,
                sentiment_throughput,
                sentiment_comments_tested,
                comment_count
            FROM project_benchmarks
            ORDER BY benchmark_date DESC
            LIMIT 10
        """
        )

        benchmarks_df = pd.read_sql(summary_query, conn)

        if benchmarks_df.empty:
            print("No benchmark records found.")
            return

        print(f"Found {len(benchmarks_df)} benchmark runs:")
        print()

        for _, row in benchmarks_df.iterrows():
            print(f"🎯 {row['benchmark_id']}")
            print(f"   📅 Date: {row['benchmark_date']}")
            print(f"   📊 Records: {row['total_records']:,}")
            print(f"   🎬 Videos: {row['unique_videos']:,}")
            print(f"   🎤 Channels: {row['unique_channels']}")
            print(f"   🚀 Throughput: {row['throughput_rows_per_sec']:.1f} rows/sec")
            if row["sentiment_available"] == "available":
                print(
                    f"   🧠 Sentiment: {row['sentiment_throughput']:.1f} comments/sec ({row['sentiment_comments_tested']} tested)"
                )
            print()


def view_model_performance(benchmark_id: str = None):
    """Display model performance for a specific benchmark or latest."""

    print("🧠 MODEL PERFORMANCE COMPARISON")
    print("=" * 35)

    engine = get_engine()

    with engine.connect() as conn:
        if benchmark_id is None:
            # Get latest benchmark ID
            latest_query = text("SELECT benchmark_id FROM project_benchmarks ORDER BY benchmark_date DESC LIMIT 1")
            result = conn.execute(latest_query).fetchone()
            if not result:
                print("No benchmark records found.")
                return
            benchmark_id = result[0]

        print(f"Benchmark ID: {benchmark_id}")
        print()

        # Get model performance
        models_query = text(
            """
            SELECT model_name, accuracy_pct, created_at
            FROM project_benchmark_models
            WHERE benchmark_id = :benchmark_id
            ORDER BY accuracy_pct DESC
        """
        )

        models_df = pd.read_sql(models_query, conn, params={"benchmark_id": benchmark_id})

        if models_df.empty:
            print(f"No model records found for benchmark {benchmark_id}")
            return

        print("Model Performance Rankings:")
        for i, (_, row) in enumerate(models_df.iterrows(), 1):
            emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "📊"
            print(f"   {emoji} {row['model_name']}: {row['accuracy_pct']:.1f}% accuracy")

        print()
        print(f"Best performing model: {models_df.iloc[0]['model_name']} ({models_df.iloc[0]['accuracy_pct']:.1f}%)")

        # Show improvement over baseline
        if len(models_df) > 1:
            baseline = models_df[models_df["model_name"] == "stock_vader"]
            if not baseline.empty:
                baseline_acc = baseline.iloc[0]["accuracy_pct"]
                best_acc = models_df.iloc[0]["accuracy_pct"]
                if models_df.iloc[0]["model_name"] != "stock_vader":
                    improvement = best_acc - baseline_acc
                    print(f"Improvement over baseline: +{improvement:.1f} percentage points")


def view_performance_trends():
    """Show performance trends over time."""

    print("📈 PERFORMANCE TRENDS")
    print("=" * 25)

    engine = get_engine()

    with engine.connect() as conn:
        trends_query = text(
            """
            SELECT
                benchmark_date,
                throughput_rows_per_sec,
                sentiment_throughput,
                total_records,
                comment_count
            FROM project_benchmarks
            ORDER BY benchmark_date ASC
        """
        )

        trends_df = pd.read_sql(trends_query, conn)

        if len(trends_df) < 2:
            print("Need at least 2 benchmark runs to show trends.")
            return

        print("Throughput Trends:")
        for _, row in trends_df.iterrows():
            date_str = row["benchmark_date"].strftime("%Y-%m-%d %H:%M")
            print(f"   {date_str}: {row['throughput_rows_per_sec']:.1f} rows/sec")
            if pd.notna(row["sentiment_throughput"]):
                print(f"                    {row['sentiment_throughput']:.1f} sentiment/sec")

        # Calculate trend
        if len(trends_df) >= 2:
            first_throughput = trends_df.iloc[0]["throughput_rows_per_sec"]
            last_throughput = trends_df.iloc[-1]["throughput_rows_per_sec"]
            change = ((last_throughput - first_throughput) / first_throughput) * 100

            trend_emoji = "📈" if change > 0 else "📉" if change < 0 else "➡️"
            print(f"\n{trend_emoji} Overall trend: {change:+.1f}% change in throughput")


def view_detailed_benchmark(benchmark_id: str):
    """Show detailed information for a specific benchmark."""

    print(f"🔍 DETAILED BENCHMARK: {benchmark_id}")
    print("=" * 50)

    engine = get_engine()

    with engine.connect() as conn:
        # Get detailed benchmark info
        detail_query = text(
            """
            SELECT * FROM project_benchmarks
            WHERE benchmark_id = :benchmark_id
        """
        )

        result = conn.execute(detail_query, {"benchmark_id": benchmark_id}).fetchone()

        if not result:
            print(f"Benchmark {benchmark_id} not found.")
            return

        # Display all metrics
        print("System Metrics:")
        print(f"   📅 Date: {result.benchmark_date}")
        print(f"   📊 Total Records: {result.total_records:,}")
        print(f"   🎬 Unique Videos: {result.unique_videos:,}")
        print(f"   🎤 Unique Channels: {result.unique_channels}")
        print(f"   📅 Date Range: {result.date_range_days} days ({result.date_range_years:.1f} years)")
        print(f"   🚀 Load Time: {result.load_time_seconds:.3f} seconds")
        print(f"   ⚡ Throughput: {result.throughput_rows_per_sec:.1f} rows/sec")
        print(f"   ❌ Null Percentage: {result.null_percentage:.1f}%")
        print(f"   💬 Comments: {result.comment_count:,}")

        if result.sentiment_available == "available":
            print(f"\nSentiment Analysis:")
            print(f"   🧠 Available: Yes")
            print(f"   ⏱️  Avg Time: {result.sentiment_avg_time:.6f} seconds/comment")
            print(f"   📊 P95 Time: {result.sentiment_p95_time:.6f} seconds/comment")
            print(f"   🚀 Throughput: {result.sentiment_throughput:.1f} comments/sec")
            print(f"   🧪 Comments Tested: {result.sentiment_comments_tested:,}")

        # Show existing model benchmarks if available
        if result.existing_model_benchmarks:
            try:
                benchmarks_data = json.loads(result.existing_model_benchmarks)
                print(f"\nPerformance Test Results:")
                if "performance_tests" in benchmarks_data:
                    for test in benchmarks_data["performance_tests"]:
                        print(f"   📊 Size {test['sample_size']}: {test['throughput_rows_per_sec']:.1f} rows/sec")
            except Exception:
                pass

        if result.notes:
            print(f"\nNotes: {result.notes}")


def main():
    """Display benchmark information."""

    print("🎯 COMMENT FETCHING SYSTEM BENCHMARKS")
    print("=" * 45)
    print()

    try:
        # Show summary
        view_benchmark_summary()

        # Show latest model performance
        view_model_performance()

        # Show trends if multiple benchmarks exist
        view_performance_trends()

        print("\n💡 To view detailed benchmark info:")
        print("   python view_benchmark_history.py <benchmark_id>")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Show detailed view for specific benchmark
        benchmark_id = sys.argv[1]
        view_detailed_benchmark(benchmark_id)
    else:
        # Show summary view
        main()
