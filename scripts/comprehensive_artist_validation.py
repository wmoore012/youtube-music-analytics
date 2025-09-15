#!/usr/bin/env python3
"""
🎤 COMPREHENSIVE ARTIST VALIDATION
=================================

Checks EVERY SINGLE TABLE AND NOTEBOOK CHART to ensure all 6 artists appear correctly.
This is the definitive validation that catches any missing artist issues.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

# Add current directory to path for imports
sys.path.insert(0, ".")

EXPECTED_ARTISTS = {"BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"}
EXPECTED_COUNT = 6


def check_database_tables():
    """Check all database tables for correct artist count."""
    print("🗄️  CHECKING DATABASE TABLES")
    print("=" * 40)

    try:
        from src.youtubeviz.data import _get_engine, load_youtube_data

        engine = _get_engine()

        # Check main data table
        df = load_youtube_data()
        db_artists = set(df["artist_name"].unique())
        db_artists = {artist for artist in db_artists if artist and str(artist) != "nan"}

        print(f"📊 Main Data Table:")
        print(f"   Found: {len(db_artists)} artists")
        print(f"   Expected: {EXPECTED_COUNT} artists")

        missing = EXPECTED_ARTISTS - db_artists
        unexpected = db_artists - EXPECTED_ARTISTS

        if missing:
            print(f"   ❌ Missing: {missing}")
            return False
        if unexpected:
            print(f"   ⚠️  Unexpected: {unexpected}")

        print(f"   ✅ All expected artists found: {sorted(db_artists)}")

        # Check individual tables
        tables_to_check = ["youtube_videos", "youtube_metrics", "youtube_comments"]

        for table in tables_to_check:
            try:
                query = f"SELECT DISTINCT channel_title FROM {table} WHERE channel_title IS NOT NULL"
                table_df = pd.read_sql(query, engine)
                table_artists = set(table_df["channel_title"].unique())
                table_artists = {artist for artist in table_artists if artist and str(artist) != "nan"}

                print(f"📋 {table}:")
                print(f"   Artists: {len(table_artists)} - {sorted(table_artists)}")

                if len(table_artists) != EXPECTED_COUNT:
                    print(f"   ❌ Expected {EXPECTED_COUNT}, found {len(table_artists)}")
                    return False
                else:
                    print(f"   ✅ Correct count")

            except Exception as e:
                print(f"   ⚠️  Could not check {table}: {e}")

        return True

    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return False


def check_csv_tables():
    """Check all CSV analysis tables for correct artist count."""
    print("\n📁 CHECKING CSV ANALYSIS TABLES")
    print("=" * 40)

    csv_files = [
        "music_analysis_tables/normalized_music_videos.csv",
        "music_analysis_tables/artist_music_summary.csv",
        "time_series_tracking/complete_time_series.csv",
        "time_series_tracking/artist_performance_over_time.csv",
    ]

    all_passed = True

    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            print(f"⚠️  {csv_file}: File not found")
            continue

        try:
            df = pd.read_csv(csv_file)

            # Find artist column (could be artist_name, channel_title, etc.)
            artist_columns = [col for col in df.columns if "artist" in col.lower() or "channel" in col.lower()]

            if not artist_columns:
                print(f"⚠️  {csv_file}: No artist column found")
                continue

            artist_col = artist_columns[0]
            csv_artists = set(df[artist_col].unique())
            csv_artists = {artist for artist in csv_artists if artist and str(artist) != "nan"}

            print(f"📊 {csv_file}:")
            print(f"   Column: {artist_col}")
            print(f"   Artists: {len(csv_artists)} - {sorted(csv_artists)}")

            if len(csv_artists) != EXPECTED_COUNT:
                print(f"   ❌ Expected {EXPECTED_COUNT}, found {len(csv_artists)}")
                all_passed = False
            else:
                print(f"   ✅ Correct count")

        except Exception as e:
            print(f"   ❌ Error reading {csv_file}: {e}")
            all_passed = False

    return all_passed


def check_notebook_outputs():
    """Check all notebook outputs for correct artist count."""
    print("\n📓 CHECKING NOTEBOOK OUTPUTS")
    print("=" * 40)

    scripts_to_check = [
        ("execute_music_analytics.py", "Music Analytics"),
        ("execute_data_quality.py", "Data Quality"),
        ("execute_artist_comparison.py", "Artist Comparison"),
    ]

    all_passed = True

    for script, name in scripts_to_check:
        if not os.path.exists(script):
            print(f"⚠️  {name}: Script not found")
            continue

        try:
            print(f"🔄 Running {name}...")
            result = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=120)

            if result.returncode != 0:
                print(f"   ❌ {name}: Failed to execute")
                print(f"   Error: {result.stderr[:200]}")
                all_passed = False
                continue

            output = result.stdout

            # Count artist mentions in output
            artist_mentions = {}
            for artist in EXPECTED_ARTISTS:
                count = output.count(artist)
                artist_mentions[artist] = count

            # Check for "Artists: X" pattern
            import re

            artist_count_matches = re.findall(r"Artists?:\s*(\d+)", output)

            print(f"📊 {name}:")

            # Check explicit artist count
            if artist_count_matches:
                found_count = int(artist_count_matches[0])
                print(f"   Reported count: {found_count}")
                if found_count != EXPECTED_COUNT:
                    print(f"   ❌ Expected {EXPECTED_COUNT}, found {found_count}")
                    all_passed = False
                else:
                    print(f"   ✅ Correct reported count")

            # Check individual artist mentions
            missing_artists = []
            for artist, count in artist_mentions.items():
                if count == 0:
                    missing_artists.append(artist)
                else:
                    print(f"   ✅ {artist}: {count} mentions")

            if missing_artists:
                print(f"   ❌ Missing artists in output: {missing_artists}")
                all_passed = False
            else:
                print(f"   ✅ All {EXPECTED_COUNT} artists mentioned in output")

        except subprocess.TimeoutExpired:
            print(f"   ⏰ {name}: Timed out")
            all_passed = False
        except Exception as e:
            print(f"   ❌ {name}: Error - {e}")
            all_passed = False

    return all_passed


def check_chart_data():
    """Check that chart data includes all artists."""
    print("\n📈 CHECKING CHART DATA")
    print("=" * 40)

    try:
        # Test chart generation functions
        from src.youtubeviz.charts import views_over_time_plotly
        from src.youtubeviz.data import load_youtube_data

        df = load_youtube_data()

        # Test views over time chart
        try:
            fig = views_over_time_plotly(df)

            # Extract artist data from chart
            chart_artists = set()
            if hasattr(fig, "data"):
                for trace in fig.data:
                    if hasattr(trace, "name") and trace.name:
                        chart_artists.add(trace.name)

            print(f"📊 Views Over Time Chart:")
            print(f"   Artists in chart: {len(chart_artists)} - {sorted(chart_artists)}")

            if len(chart_artists) != EXPECTED_COUNT:
                print(f"   ❌ Expected {EXPECTED_COUNT}, found {len(chart_artists)}")
                return False
            else:
                print(f"   ✅ All artists in chart")

        except Exception as e:
            print(f"   ⚠️  Could not test chart: {e}")

        return True

    except Exception as e:
        print(f"❌ Error checking chart data: {e}")
        return False


def main():
    """Run comprehensive artist validation."""

    print("🎤 COMPREHENSIVE ARTIST VALIDATION")
    print("=" * 50)
    print(f"Expected Artists ({EXPECTED_COUNT}): {sorted(EXPECTED_ARTISTS)}")
    print("=" * 50)

    # Run all checks
    checks = [
        ("Database Tables", check_database_tables),
        ("CSV Analysis Tables", check_csv_tables),
        ("Notebook Outputs", check_notebook_outputs),
        ("Chart Data", check_chart_data),
    ]

    results = {}

    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            print(f"\n❌ {check_name} failed with error: {e}")
            results[check_name] = False

    # Final report
    print("\n" + "=" * 60)
    print("🏆 COMPREHENSIVE VALIDATION REPORT")
    print("=" * 60)

    all_passed = True
    for check_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{status}: {check_name}")
        if not passed:
            all_passed = False

    print(f"\nExpected Artists: {EXPECTED_COUNT}")
    print(f"Artists: {', '.join(sorted(EXPECTED_ARTISTS))}")

    if all_passed:
        print(f"\n🎉 ALL VALIDATIONS PASSED!")
        print(f"✅ All {EXPECTED_COUNT} artists appear correctly in EVERY table and chart")
        return 0
    else:
        print(f"\n🚫 VALIDATION FAILED!")
        print("❌ Some tables/charts are missing artists")
        print("💡 Run ETL to refresh data or check configuration")
        return 1


if __name__ == "__main__":
    sys.exit(main())
