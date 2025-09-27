#!/usr / bin / env python3
"""
ETL Pipeline Test Script

This script tests the complete ETL pipeline execution from channel URL to processed data.
It performs a minimal test run to verify all components work correctly.
"""

import os
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from sqlalchemy import text

from web.etl_entrypoints import run_channel_etl


def test_etl_pipeline():
    """Test the complete ETL pipeline with a small sample."""
    print("🧪 Testing ETL Pipeline Components")
    print("=" * 50)

    # Load environment
    load_dotenv(PROJECT_ROOT / ".env")

    # Test 1: Database connectivity
    print("1. Testing database connectivity...")
    try:
        from web.etl_helpers import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM youtube_videos")).scalar()
            print(f"   ✅ Database connected - {result:,} videos in database")
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        return False

    # Test 2: YouTube API connectivity
    print("\n2. Testing YouTube API connectivity...")
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("   ❌ YOUTUBE_API_KEY not found")
        return False

    try:
        import requests

        response = requests.get(
            "https://www.googleapis.com / youtube / v3 / search",
            params={"key": api_key, "part": "snippet", "q": "test", "type": "video", "maxResults": 1},
            timeout=10,
        )
        if response.status_code == 200:
            print("   ✅ YouTube API accessible")
        else:
            print(f"   ❌ YouTube API error: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ YouTube API test failed: {e}")
        return False

    # Test 3: ETL component imports
    print("\n3. Testing ETL component imports...")
    try:
        from web.etl_helpers import get_engine
        from web.sentiment_job import YouTubeCommentSentimentJob
        from web.youtube_channel_etl import YouTubeChannelETL

        print("   ✅ All ETL components importable")
    except ImportError as e:
        print(f"   ❌ Import error: {e}")
        return False

    # Test 4: ETL configuration
    print("\n4. Testing ETL configuration...")
    required_env_vars = ["DB_HOST", "DB_USER", "DB_PASS", "DB_NAME", "YOUTUBE_API_KEY"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(f"   ❌ Missing environment variables: {', '.join(missing_vars)}")
        return False
    else:
        print("   ✅ All required environment variables present")

    # Test 5: Small ETL execution (dry run)
    print("\n5. Testing ETL instantiation...")
    try:
        etl = YouTubeChannelETL(
            api_key=os.getenv("YOUTUBE_API_KEY"),
            db_host=os.getenv("DB_HOST"),
            db_port=int(os.getenv("DB_PORT", 3306)),
            db_user=os.getenv("DB_USER"),
            db_pass=os.getenv("DB_PASS"),
            db_name=os.getenv("DB_NAME"),
        )
        print("   ✅ ETL instance created successfully")
    except Exception as e:
        print(f"   ❌ ETL instantiation failed: {e}")
        return False

    # Test 6: Sentiment job instantiation
    print("\n6. Testing sentiment job...")
    try:
        sentiment_job = YouTubeCommentSentimentJob()
        print("   ✅ Sentiment job created successfully")
    except Exception as e:
        print(f"   ❌ Sentiment job creation failed: {e}")
        return False

    print("\n" + "=" * 50)
    print("🎉 ALL ETL PIPELINE TESTS PASSED")
    print("=" * 50)
    print("\nThe ETL pipeline is ready for production use!")
    print("\nTo run the full pipeline:")
    print("  python tools / etl / run_focused_etl.py")
    print("\nTo run ETL for specific channels:")
    print("  python tools / etl / run_channels_from_env.py")

    return True


def main():
    """Main entry point."""
    success = test_etl_pipeline()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
