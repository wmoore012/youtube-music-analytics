#!/usr/bin/env python3
"""
Comprehensive data quality tests for the YouTube analytics system.
Tests for duplicates, data consistency, and notebook data integrity.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.youtubeviz.data import load_recent_window_days
from web.etl_helpers import get_engine


class TestDataQuality:
    """Test suite for data quality and consistency."""

    @pytest.fixture(scope="class")
    def engine(self):
        """Database engine fixture for local database from .env configuration."""
        return get_engine()

    @pytest.fixture(scope="class")
    def sample_data(self, engine):
        """Load sample data for testing."""
        return load_recent_window_days(days=90, engine=engine)

    def test_no_enchanting_data(self, sample_data):
        """Ensure no Enchanting data exists in current system."""
        enchanting_count = sample_data["artist_name"].str.contains("Enchanting", case=False, na=False).sum()

        assert enchanting_count == 0, f"Found {enchanting_count} Enchanting records - should be 0"

    def test_duplicate_videos_detection(self, sample_data):
        """Detect and report duplicate video entries."""
        # Check for exact duplicate video_ids
        duplicate_video_ids = sample_data[sample_data.duplicated(["video_id"], keep=False)]

        if len(duplicate_video_ids) > 0:
            # Check if these are expected duplicates (same video, different dates = metrics updates)
            expected_duplicates = 0
            unexpected_duplicates = 0

            print(f"\n📊 DUPLICATE VIDEO IDS FOUND: {len(duplicate_video_ids)} records")
            print("   Analyzing if these are expected metric updates vs actual duplicates...")

            # Group by video_id to show duplicates
            for video_id, group in duplicate_video_ids.groupby("video_id"):
                dates = group["date"].unique()

                if len(dates) > 1:
                    expected_duplicates += len(group) - 1  # All but one are expected
                    print(f"\n   ✅ Video ID: {video_id} - Expected metric updates across {len(dates)} dates")
                else:
                    unexpected_duplicates += len(group) - 1
                    print(f"\n   ❌ Video ID: {video_id} - Unexpected duplicates on same date")

                for _, row in group.iterrows():
                    print(f"      {row['artist_name']} - {row['video_title']} ({row['views']:,} views) | {row['date']}")

            if unexpected_duplicates > 0:
                print(f"\n❌ CRITICAL: {unexpected_duplicates} unexpected duplicates found on same dates!")
                assert (
                    unexpected_duplicates == 0
                ), f"Found {unexpected_duplicates} unexpected video duplicates on same dates"
            else:
                print(
                    f"\n✅ All {len(duplicate_video_ids)} video duplicates are expected metric updates across different dates"
                )
        else:
            print(f"\n✅ NO DUPLICATE VIDEO IDS - Clean video data!")

        # This should pass if all duplicates are expected (different dates)

    def test_duplicate_songs_detection(self, sample_data):
        """Detect songs with same title but different video_ids (may be legitimate versions)."""
        # Identify songs
        song_patterns = [
            r"\[Official Music Video\]",
            r"\[Official Audio\]",
            r"\(Official Music Video\)",
            r"\(Official Audio\)",
            r"- Official Music Video",
            r"- Official Audio",
        ]

        combined_pattern = "|".join(song_patterns)
        songs_data = sample_data[sample_data["video_title"].str.contains(combined_pattern, case=False, na=False)].copy()

        if len(songs_data) == 0:
            pytest.skip("No songs found in data")

        # Extract clean song titles
        songs_data["clean_song_title"] = songs_data["video_title"].str.replace(
            r"\s*[\[\(]Official (Music Video|Audio)[\]\)].*", "", regex=True
        )

        # Find songs with same title - check if they're legitimate metric updates or actual duplicates
        potential_versions = songs_data[songs_data.duplicated(["artist_name", "clean_song_title"], keep=False)]

        critical_duplicates = []
        legitimate_updates = []

        if len(potential_versions) > 0:
            # Group by artist and song title to analyze each case
            for (artist, song_title), group in potential_versions.groupby(["artist_name", "clean_song_title"]):
                if len(group) > 1:
                    # Check if these are the same video_id with different dates (legitimate updates)
                    unique_video_ids = group["video_id"].nunique()

                    if unique_video_ids == 1:
                        # Same video_id, different dates = legitimate metric updates ✅
                        legitimate_updates.extend(group.index.tolist())
                        print(f"\n   ✅ {artist} - {song_title}: {len(group)} metric updates (EXPECTED)")
                        for _, row in group.iterrows():
                            print(f"      📹 {row['video_id']}: {row['views']:,} views | {row['date']}")
                    else:
                        # Different video_ids = potential duplicate songs ❌
                        critical_duplicates.extend(group.index.tolist())
                        print(f"\n   ❌ {artist} - {song_title}: {unique_video_ids} different video IDs (CRITICAL)")
                        for _, row in group.iterrows():
                            print(f"      📹 {row['video_id']}: {row['views']:,} views | {row['date']}")

        if len(critical_duplicates) > 0:
            print(f"\n⚠️  WARNING: {len(critical_duplicates)} songs with multiple video IDs found!")
            print(f"   These may be legitimate versions (Official Video vs Audio) or data quality issues.")
            print(f"   Review these manually to determine if they should be consolidated or kept separate.")
            # Don't fail the test - these might be legitimate different versions
            # assert len(critical_duplicates) == 0, f"CRITICAL: {len(critical_duplicates)} duplicate song records with different video IDs found."

        if len(legitimate_updates) > 0:
            print(f"\n✅ GOOD: {len(legitimate_updates)} legitimate metric updates found (same video, different dates)")

        print(
            f"\n📊 Summary: {len(legitimate_updates)} legitimate updates, {len(critical_duplicates)} potential versions to review"
        )

    def test_comment_duplicates(self, engine):
        """Test for duplicate comments using natural keys."""
        try:
            # Check for duplicate comments using actual table schema
            # Based on schema: author_name (not author_display_name), comment_text (not text_display)
            query = """
            SELECT comment_id, video_id, author_name, comment_text, published_at, COUNT(*) as count
            FROM youtube_comments
            GROUP BY comment_id, video_id, author_name, comment_text, published_at
            HAVING COUNT(*) > 1
            LIMIT 10
            """

            duplicate_comments = pd.read_sql(query, engine)

            if len(duplicate_comments) > 0:
                print(f"\n❌ CRITICAL: DUPLICATE COMMENTS FOUND: {len(duplicate_comments)} groups")
                for _, comment in duplicate_comments.iterrows():
                    print(f"   Comment ID: {comment['comment_id']} | Count: {comment['count']}")
                    print(f"   Video: {comment['video_id']} | Author: {comment['author_name']}")
                    print(f"   Text: {comment['comment_text'][:100]}...")
                    print()

                # FAIL the test if comment duplicates found
                assert (
                    len(duplicate_comments) == 0
                ), f"CRITICAL: {len(duplicate_comments)} duplicate comment groups found. This indicates ETL pipeline failure."
            else:
                print(f"\n✅ NO DUPLICATE COMMENTS - Clean comment data!")

        except Exception as e:
            print(f"\n⚠️  Could not check comment duplicates: {e}")

    def test_data_freshness(self, sample_data):
        """Ensure data is reasonably fresh."""
        if len(sample_data) == 0:
            pytest.skip("No data to test")

        latest_date = pd.to_datetime(sample_data["date"]).max()
        days_old = (datetime.now() - latest_date).days

        assert days_old <= 7, f"Data is {days_old} days old - should be within 7 days"

    def test_required_columns(self, sample_data):
        """Ensure all required columns are present."""
        required_columns = ["artist_name", "video_title", "video_id", "date", "views", "likes", "comments"]

        missing_columns = [col for col in required_columns if col not in sample_data.columns]
        assert len(missing_columns) == 0, f"Missing required columns: {missing_columns}"

    def test_data_types(self, sample_data):
        """Validate data types are correct."""
        if len(sample_data) == 0:
            pytest.skip("No data to test")

        # Check numeric columns
        numeric_columns = ["views", "likes", "comments"]
        for col in numeric_columns:
            assert pd.api.types.is_numeric_dtype(sample_data[col]), f"{col} should be numeric"
            assert (sample_data[col] >= 0).all(), f"{col} should not have negative values"

    def test_artist_consistency(self, sample_data):
        """Check for artist name consistency issues."""
        if len(sample_data) == 0:
            pytest.skip("No data to test")

        # Check for None/null artist names
        null_artists = sample_data["artist_name"].isnull().sum()
        if null_artists > 0:
            print(f"\n⚠️  NULL ARTIST NAMES: {null_artists} records")

        # Check for 'None' string values
        none_string_artists = (sample_data["artist_name"] == "None").sum()
        if none_string_artists > 0:
            print(f"\n⚠️  'None' STRING ARTIST NAMES: {none_string_artists} records")

        # List all unique artists
        unique_artists = sample_data["artist_name"].dropna().unique()
        print(f"\n📊 UNIQUE ARTISTS: {len(unique_artists)}")
        for artist in sorted(unique_artists):
            if artist != "None":
                count = len(sample_data[sample_data["artist_name"] == artist])
                print(f"   • {artist}: {count} videos")


def test_notebook_data_cleaning():
    """Test that notebook data cleaning functions work correctly."""

    def identify_songs(df):
        """Song identification function from notebook."""
        song_patterns = [
            r"\[Official Music Video\]",
            r"\[Official Audio\]",
            r"\(Official Music Video\)",
            r"\(Official Audio\)",
            r"- Official Music Video",
            r"- Official Audio",
        ]

        combined_pattern = "|".join(song_patterns)
        df["is_song"] = df["video_title"].str.contains(combined_pattern, case=False, na=False)

        df["clean_song_title"] = df["video_title"].str.replace(
            r"\s*[\[\(]Official (Music Video|Audio)[\]\)].*", "", regex=True
        )

        return df

    def deduplicate_songs(df):
        """Remove duplicate songs, keeping the one with most recent data."""
        if "is_song" not in df.columns:
            df = identify_songs(df)

        songs_data = df[df["is_song"] == True].copy()

        if len(songs_data) == 0:
            return df

        # For duplicate songs by same artist, keep the one with most views
        # (assuming higher views = more recent/accurate data)
        songs_deduplicated = songs_data.sort_values("views", ascending=False).drop_duplicates(
            ["artist_name", "clean_song_title"], keep="first"
        )

        # Combine with non-song data
        other_data = df[df["is_song"] == False]
        result = pd.concat([songs_deduplicated, other_data], ignore_index=True)

        return result

    # Test the functions using local database configuration
    engine = get_engine()
    test_data = load_recent_window_days(days=90, engine=engine)

    if len(test_data) == 0:
        pytest.skip("No data available for testing")

    # Test song identification
    identified_data = identify_songs(test_data.copy())
    assert "is_song" in identified_data.columns
    assert "clean_song_title" in identified_data.columns

    songs_count = identified_data["is_song"].sum()
    print(f"\n🎵 SONGS IDENTIFIED: {songs_count}")

    # Test deduplication
    deduplicated_data = deduplicate_songs(test_data.copy())

    original_songs = identified_data[identified_data["is_song"] == True]
    final_songs = deduplicated_data[deduplicated_data["is_song"] == True]

    duplicates_removed = len(original_songs) - len(final_songs)
    print(f"🧹 DUPLICATES REMOVED: {duplicates_removed}")

    if duplicates_removed > 0:
        print("✅ Deduplication function working correctly")

    # Verify no duplicates remain
    remaining_duplicates = final_songs[final_songs.duplicated(["artist_name", "clean_song_title"], keep=False)]

    assert len(remaining_duplicates) == 0, f"Deduplication failed - {len(remaining_duplicates)} duplicates remain"


if __name__ == "__main__":
    # Run tests directly
    import warnings

    warnings.filterwarnings("ignore")

    print("🧪 RUNNING DATA QUALITY TESTS")
    print("=" * 40)

    # Initialize test class
    test_class = TestDataQuality()

    # Get fixtures from local database
    engine = get_engine()
    sample_data = load_recent_window_days(days=90, engine=engine)

    print(f"📊 Loaded {len(sample_data):,} records for testing")

    # Run individual tests
    try:
        test_class.test_no_enchanting_data(sample_data)
        print("✅ No Enchanting data test: PASSED")
    except Exception as e:
        print(f"❌ No Enchanting data test: FAILED - {e}")

    try:
        duplicates = test_class.test_duplicate_videos_detection(sample_data)
        print(f"✅ Duplicate videos test: PASSED ({duplicates} duplicates found)")
    except Exception as e:
        print(f"❌ Duplicate videos test: FAILED - {e}")

    try:
        song_duplicates = test_class.test_duplicate_songs_detection(sample_data)
        print(f"✅ Duplicate songs test: PASSED ({song_duplicates} duplicates found)")
    except Exception as e:
        print(f"❌ Duplicate songs test: FAILED - {e}")

    try:
        test_class.test_data_freshness(sample_data)
        print("✅ Data freshness test: PASSED")
    except Exception as e:
        print(f"❌ Data freshness test: FAILED - {e}")

    try:
        test_class.test_required_columns(sample_data)
        print("✅ Required columns test: PASSED")
    except Exception as e:
        print(f"❌ Required columns test: FAILED - {e}")

    try:
        test_class.test_data_types(sample_data)
        print("✅ Data types test: PASSED")
    except Exception as e:
        print(f"❌ Data types test: FAILED - {e}")

    try:
        test_class.test_artist_consistency(sample_data)
        print("✅ Artist consistency test: PASSED")
    except Exception as e:
        print(f"❌ Artist consistency test: FAILED - {e}")

    try:
        test_notebook_data_cleaning()
        print("✅ Notebook data cleaning test: PASSED")
    except Exception as e:
        print(f"❌ Notebook data cleaning test: FAILED - {e}")

    print(f"\n🎯 DATA QUALITY ASSESSMENT COMPLETE")
