#!/usr/bin/env python3
"""
🎤 Artist Data Validation Tool
=============================

Validates that all expected artists appear in the database and notebooks.
Works both locally (using .env) and in CI/CD (using config/expected_artists.json).

This ensures data consistency and prevents missing artist issues.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Set


def load_expected_artists_from_config() -> Dict[str, Any]:
    """Load expected artists from config file (for CI/CD)."""
    config_path = Path("config/expected_artists.json")

    if not config_path.exists():
        raise FileNotFoundError(
            "config/expected_artists.json not found. " "This file is required for CI/CD validation."
        )

    with open(config_path, "r") as f:
        return json.load(f)


def load_expected_artists_from_env() -> Set[str]:
    """Load expected artists from .env file (for local development)."""
    from dotenv import load_dotenv

    load_dotenv()

    # Mapping from .env names to expected database channel titles
    artist_mapping = {
        "BICFIZZLE": "BiC Fizzle",
        "COBRAH": "COBRAH",
        "COROOK": "Corook",
        "RAICHE": "Raiche",
        "RE6CE": "re6ce",
        "FLYANABOSS": "Flyana Boss",
    }

    configured = set()
    for key, value in os.environ.items():
        if key.startswith("YT_") and key.endswith("_YT") and value.strip():
            # Handle both main channels and topic channels
            artist_name = key[3:-3]  # Remove YT_ prefix and _YT suffix

            # Remove _TOPIC suffix if present
            if artist_name.endswith("_TOPIC"):
                artist_name = artist_name[:-6]

            if artist_name in artist_mapping:
                configured.add(artist_mapping[artist_name])

    return configured


def get_expected_artists() -> Set[str]:
    """Get expected artists, preferring .env if available, falling back to config."""

    # Try .env first (local development)
    try:
        env_artists = load_expected_artists_from_env()
        if env_artists:
            print(f"📋 Using artists from .env: {len(env_artists)} artists")
            return env_artists
    except Exception as e:
        print(f"⚠️  Could not load from .env: {e}")

    # Fall back to config file (CI/CD)
    try:
        config = load_expected_artists_from_config()
        config_artists = set(config["expected_artists"])
        print(f"📋 Using artists from config: {len(config_artists)} artists")
        return config_artists
    except Exception as e:
        print(f"❌ Could not load from config: {e}")
        raise


def get_database_artists() -> Set[str]:
    """Get artists currently in the database."""
    try:
        sys.path.insert(0, ".")
        from src.youtubeviz.data import load_youtube_data

        df = load_youtube_data()
        db_artists = set(df["artist_name"].unique())

        # Filter out None/NaN values
        db_artists = {artist for artist in db_artists if artist and str(artist) != "nan"}

        return db_artists

    except Exception as e:
        print(f"❌ Error loading database artists: {e}")
        return set()


def validate_artist_consistency() -> Dict[str, Any]:
    """Validate artist consistency between expected and actual data."""

    print("🎤 ARTIST DATA VALIDATION")
    print("=" * 40)

    # Get expected and actual artists
    expected_artists = get_expected_artists()
    database_artists = get_database_artists()

    # Calculate differences
    missing_artists = expected_artists - database_artists
    unexpected_artists = database_artists - expected_artists

    # Validation results
    results = {
        "expected_count": len(expected_artists),
        "database_count": len(database_artists),
        "expected_artists": sorted(list(expected_artists)),
        "database_artists": sorted(list(database_artists)),
        "missing_artists": sorted(list(missing_artists)),
        "unexpected_artists": sorted(list(unexpected_artists)),
        "validation_passed": len(missing_artists) == 0 and len(unexpected_artists) == 0,
    }

    # Print results
    print(f"\n📊 Artist Count Comparison:")
    print(f"   Expected: {results['expected_count']}")
    print(f"   Database: {results['database_count']}")

    print(f"\n🎵 Expected Artists:")
    for artist in results["expected_artists"]:
        status = "✅" if artist in database_artists else "❌"
        print(f"   {status} {artist}")

    if results["missing_artists"]:
        print(f"\n❌ Missing Artists ({len(results['missing_artists'])}):")
        for artist in results["missing_artists"]:
            print(f"   - {artist}")
        print("\n💡 Fix: Check .env configuration and run ETL to collect missing data")

    if results["unexpected_artists"]:
        print(f"\n⚠️  Unexpected Artists ({len(results['unexpected_artists'])}):")
        for artist in results["unexpected_artists"]:
            print(f"   - {artist}")
        print("\n💡 Fix: Update config/expected_artists.json or clean database")

    if results["validation_passed"]:
        print(f"\n🎉 VALIDATION PASSED!")
        print("✅ All expected artists found in database")
        print("✅ No unexpected artists found")
    else:
        print(f"\n🚫 VALIDATION FAILED!")
        print("❌ Artist data inconsistency detected")

    return results


def validate_notebook_outputs() -> bool:
    """Validate that notebook outputs show the correct number of artists."""

    print("\n📓 NOTEBOOK OUTPUT VALIDATION")
    print("=" * 40)

    expected_artists = get_expected_artists()
    expected_count = len(expected_artists)

    validation_passed = True

    # Test execute scripts
    scripts_to_test = [
        ("execute_music_analytics.py", "Music Analytics"),
        ("execute_data_quality.py", "Data Quality"),
    ]

    for script, name in scripts_to_test:
        if not os.path.exists(script):
            print(f"⚠️  {name}: Script not found: {script}")
            continue

        try:
            import subprocess

            result = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                output = result.stdout

                # Check for artist count in output
                artist_count_found = False
                for line in output.split("\n"):
                    if "Artists:" in line or "artists:" in line.lower():
                        # Extract number from line
                        import re

                        numbers = re.findall(r"\d+", line)
                        if numbers:
                            found_count = int(numbers[0])
                            if found_count == expected_count:
                                print(f"✅ {name}: Shows {found_count} artists (correct)")
                                artist_count_found = True
                            else:
                                print(f"❌ {name}: Shows {found_count} artists, expected {expected_count}")
                                validation_passed = False
                            break

                if not artist_count_found:
                    print(f"⚠️  {name}: Could not find artist count in output")

            else:
                print(f"❌ {name}: Script failed to execute")
                validation_passed = False

        except subprocess.TimeoutExpired:
            print(f"⏰ {name}: Script timed out")
        except Exception as e:
            print(f"❌ {name}: Error executing script: {e}")
            validation_passed = False

    return validation_passed


def update_config_from_env():
    """Update config file from current .env (for maintenance)."""

    print("\n🔄 UPDATING CONFIG FROM .ENV")
    print("=" * 40)

    try:
        env_artists = load_expected_artists_from_env()

        if not env_artists:
            print("❌ No artists found in .env")
            return False

        config = {
            "expected_artists": sorted(list(env_artists)),
            "minimum_artists": len(env_artists),
            "last_updated": "2025-09-15",
            "description": "Expected artists configuration for CI/CD validation. Update this when adding/removing artists from .env",
        }

        config_path = Path("config/expected_artists.json")
        config_path.parent.mkdir(exist_ok=True)

        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        print(f"✅ Updated config with {len(env_artists)} artists:")
        for artist in config["expected_artists"]:
            print(f"   - {artist}")

        return True

    except Exception as e:
        print(f"❌ Error updating config: {e}")
        return False


def main():
    """Main validation function."""

    import argparse

    parser = argparse.ArgumentParser(description="Validate artist data consistency")
    parser.add_argument("--update-config", action="store_true", help="Update config file from current .env")
    parser.add_argument("--notebooks-only", action="store_true", help="Only validate notebook outputs")

    args = parser.parse_args()

    if args.update_config:
        success = update_config_from_env()
        sys.exit(0 if success else 1)

    # Run validation
    if not args.notebooks_only:
        artist_results = validate_artist_consistency()
        artist_validation_passed = artist_results["validation_passed"]
    else:
        artist_validation_passed = True

    notebook_validation_passed = validate_notebook_outputs()

    # Final result
    print("\n" + "=" * 60)
    print("🏆 ARTIST VALIDATION REPORT")
    print("=" * 60)

    if artist_validation_passed and notebook_validation_passed:
        print("🎉 ALL VALIDATIONS PASSED!")
        print("✅ Artist data is consistent")
        print("✅ Notebook outputs are correct")
        sys.exit(0)
    else:
        print("🚫 VALIDATION FAILED!")
        if not artist_validation_passed:
            print("❌ Artist data inconsistency")
        if not notebook_validation_passed:
            print("❌ Notebook output issues")
        print("\n💡 Run with --update-config to sync config with .env")
        sys.exit(1)


if __name__ == "__main__":
    main()
