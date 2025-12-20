#!/usr / bin / env python3
"""
🎤 Quick Artist Count Check
==========================

Simple validation that the right number of artists appear in outputs.
"""

import re
import subprocess
import sys


def check_artist_count():
    """Check that outputs show the expected number of artists."""

    expected_count = 6  # Update this when adding / removing artists

    print("🎤 QUICK ARTIST COUNT CHECK")
    print("=" * 30)

    # Check music analytics
    try:
        result = subprocess.run(
            [sys.executable, "scripts/execute_music_analytics.py"], capture_output=True, text=True, timeout=60
        )

        if result.returncode == 0:
            # Look for "Artists: X" in output
            for line in result.stdout.split("\n"):
                if "Artists:" in line:
                    numbers = re.findall(r"\d+", line)
                    if numbers:
                        found_count = int(numbers[0])
                        if found_count == expected_count:
                            print(f"✅ Music Analytics: {found_count} artists (correct)")
                        else:
                            print(f"❌ Music Analytics: {found_count} artists, expected {expected_count}")
                            return False
                        break
        else:
            print("❌ Music Analytics failed to run")
            return False
    except Exception as e:
        print(f"❌ Error checking music analytics: {e}")
        return False

    # Check data quality
    try:
        result = subprocess.run([sys.executable, "scripts/execute_data_quality.py"], capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            # Look for "Artists: X" in output
            for line in result.stdout.split("\n"):
                if "Artists:" in line:
                    numbers = re.findall(r"\d+", line)
                    if numbers:
                        found_count = int(numbers[0])
                        if found_count == expected_count:
                            print(f"✅ Data Quality: {found_count} artists (correct)")
                        else:
                            print(f"❌ Data Quality: {found_count} artists, expected {expected_count}")
                            return False
                        break
        else:
            print("❌ Data Quality failed to run")
            return False
    except Exception as e:
        print(f"❌ Error checking data quality: {e}")
        return False

    print(f"\n🎉 All outputs show {expected_count} artists correctly!")
    return True


if __name__ == "__main__":
    success = check_artist_count()
    sys.exit(0 if success else 1)
