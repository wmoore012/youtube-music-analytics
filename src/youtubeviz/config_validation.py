"""
Configuration validation utilities for YouTube ETL and analytics.
Automatically calculates artist counts and validates data consistency.
"""

import os
import re
from typing import Dict, List, Tuple

from dotenv import load_dotenv
import pandas as pd


def get_artists_from_env() -> Tuple[List[str], int]:
    """
    Extract artist names and count from .env file configuration.

    Returns:
        Tuple of (artist_names_list, artist_count)
    """
    load_dotenv()

    # Pattern to match YT_ARTISTNAME_YT variables
    artist_pattern = r"^YT_([A-Z0-9_]+)_YT="

    artists = []

    # Read .env file directly to get all artist configurations
    env_file_path = ".env"
    if os.path.exists(env_file_path):
        with open(env_file_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    match = re.match(artist_pattern, line)
                    if match:
                        artist_name = match.group(1)
                        # Convert from env format to display format
                        # BICFIZZLE -> BiC Fizzle, FLYANABOSS -> Flyana Boss, etc.
                        display_name = format_artist_name(artist_name)
                        artists.append(display_name)

    return sorted(artists), len(artists)


def format_artist_name(env_name: str) -> str:
    """
    Convert environment variable artist name to display format.

    Args:
        env_name: Artist name from env (e.g., 'BICFIZZLE', 'FLYANABOSS')

    Returns:
        Formatted display name (e.g., 'BiC Fizzle', 'Flyana Boss')
    """
    # Special cases for known artists
    name_mapping = {
        "BICFIZZLE": "BiC Fizzle",
        "COBRAH": "COBRAH",
        "COROOK": "Corook",
        "RAICHE": "Raiche",
        "RE6CE": "re6ce",
        "FLYANABOSS": "Flyana Boss",
    }

    return name_mapping.get(env_name, env_name.title())


def validate_artist_count_in_data(df: pd.DataFrame, artist_col: str = "artist_name") -> Dict[str, any]:
    """
    Validate that the number of artists in data matches .env configuration.

    Args:
        df: DataFrame with artist data
        artist_col: Column name containing artist names

    Returns:
        Dictionary with validation results
    """
    expected_artists, expected_count = get_artists_from_env()

    if artist_col not in df.columns:
        return {
            "valid": False,
            "error": f"Column '{artist_col}' not found in data",
            "expected_count": expected_count,
            "actual_count": 0,
            "expected_artists": expected_artists,
            "actual_artists": [],
        }

    actual_artists = sorted(df[artist_col].unique().tolist())
    actual_count = len(actual_artists)

    # Check if counts match
    count_match = actual_count == expected_count

    # Check if artist names match (case-insensitive)
    expected_lower = [name.lower() for name in expected_artists]
    actual_lower = [name.lower() for name in actual_artists]
    names_match = set(expected_lower) == set(actual_lower)

    return {
        "valid": count_match and names_match,
        "count_match": count_match,
        "names_match": names_match,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "expected_artists": expected_artists,
        "actual_artists": actual_artists,
        "missing_artists": [name for name in expected_artists if name.lower() not in actual_lower],
        "extra_artists": [name for name in actual_artists if name.lower() not in expected_lower],
    }


def validate_database_artist_count(engine=None) -> Dict[str, any]:
    """
    Validate artist count in database matches .env configuration.

    Args:
        engine: SQLAlchemy engine (optional, will create if not provided)

    Returns:
        Dictionary with validation results
    """
    try:
        if engine is None:
            from youtubeviz.data import get_engine

            engine = get_engine()

        # Query unique artists from database
        query = """
        SELECT DISTINCT artist_name
        FROM youtube_videos
        WHERE artist_name IS NOT NULL
        ORDER BY artist_name
        """

        df = pd.read_sql(query, engine)
        return validate_artist_count_in_data(df, "artist_name")

    except Exception as e:
        expected_artists, expected_count = get_artists_from_env()
        return {
            "valid": False,
            "error": f"Database validation failed: {str(e)}",
            "expected_count": expected_count,
            "actual_count": 0,
            "expected_artists": expected_artists,
            "actual_artists": [],
        }


def print_validation_results(validation_result: Dict[str, any], loud: bool = True) -> None:
    """
    Print validation results with optional loud warnings.

    Args:
        validation_result: Result from validate_artist_count_in_data()
        loud: If True, print loud warnings for mismatches
    """
    if validation_result["valid"]:
        print(f"✅ Artist validation PASSED: {validation_result['actual_count']} artists found as expected")
        print(f"   Artists: {', '.join(validation_result['actual_artists'])}")
        return

    # Print loud warnings for mismatches
    if loud:
        print("\n" + "=" * 80)
        print("🚨 LOUD WARNING: ARTIST COUNT MISMATCH DETECTED! 🚨")
        print("=" * 80)

    print(f"❌ Expected {validation_result['expected_count']} artists, found {validation_result['actual_count']}")

    if not validation_result.get("count_match", True):
        print(f"   Expected: {validation_result['expected_artists']}")
        print(f"   Found:    {validation_result['actual_artists']}")

    if validation_result.get("missing_artists"):
        print(f"   Missing:  {validation_result['missing_artists']}")

    if validation_result.get("extra_artists"):
        print(f"   Extra:    {validation_result['extra_artists']}")

    if "error" in validation_result:
        print(f"   Error:    {validation_result['error']}")

    if loud:
        print("\n🔧 RECOMMENDED ACTIONS:")
        if validation_result["actual_count"] == 0:
            print("   1. Run ETL pipeline to populate database: python tools / etl / run_focused_etl.py")
            print("   2. Check database connection and table structure")
        elif validation_result.get("missing_artists"):
            print("   1. Run ETL pipeline to fetch missing artist data")
            print("   2. Check .env file for correct artist channel URLs")
        elif validation_result.get("extra_artists"):
            print("   1. Review database for unexpected artist entries")
            print("   2. Run data cleanup if needed")
        print("=" * 80 + "\n")


def get_expected_artist_count() -> int:
    """
    Get the expected number of artists from .env configuration.

    Returns:
        Number of artists configured in .env
    """
    _, count = get_artists_from_env()
    return count


# Constants for notebook use
EXPECTED_ARTIST_COUNT = get_expected_artist_count()
EXPECTED_ARTISTS, _ = get_artists_from_env()
