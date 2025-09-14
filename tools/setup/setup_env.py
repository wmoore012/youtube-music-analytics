#!/usr/bin/env python3
"""
Environment setup script for YouTube ETL pipeline.

This script helps you set up your .env file with the required configuration
for the YouTube ETL pipeline.
"""

import os
from pathlib import Path


def setup_env_file():
    """Guide the user through setting up the .env file."""

    env_file = Path(".env")

    print("🎬 YouTube ETL Pipeline - Environment Setup")
    print("=" * 50)

    if env_file.exists():
        print("✅ .env file already exists!")
        print("📝 Current configuration:")

        with open(env_file, "r") as f:
            content = f.read()
            print(content)

        response = input("\n❓ Do you want to update the configuration? (y/n): ").lower().strip()
        if response != "y":
            print("✅ Keeping existing configuration.")
            return

    print("\n📝 Let's set up your .env file:")
    print("You'll need:")
    print("1. YouTube Data API v3 key (from Google Cloud Console)")
    print("2. MySQL database credentials")
    print("3. Artist channel information (already configured)")
    print()

    # Get YouTube API key
    api_key = input("🔑 Enter your YouTube Data API v3 key: ").strip()
    if not api_key:
        print("❌ YouTube API key is required!")
        return

    # Get database configuration
    print("\n🗄️  Database Configuration:")
    db_host = input("Host (default: 127.0.0.1): ").strip() or "127.0.0.1"
    db_port = input("Port (default: 3306): ").strip() or "3306"
    db_user = input("Username: ").strip()
    db_pass = input("Password: ").strip()
    db_name = input("Database name (default: icatalog): ").strip() or "icatalog"

    # Create .env content
    env_content = f"""# YouTube Data API Configuration
# Get your API key from: https://console.developers.google.com/
YOUTUBE_API_KEY={api_key}

# Database Configuration
DB_HOST={db_host}
DB_PORT={db_port}
DB_USER={db_user}
DB_PASS={db_pass}
DB_NAME_PRIVATE={db_name}
DB_NAME_PUBLIC={db_name}_public

# Artist YouTube Channels
# BicFizzle
BICDIZZLE_CHANNEL_ID=UCZcMK-f8loeOkk3GX3hsmtQ
BICDIZZLE_SPOTIFY_URL=https://open.spotify.com/artist/55zZKMiLQNwu6unkKc8J4y

# Cobrah
COBRAH_CHANNEL_ID=@COBRAH
COBRAH_SPOTIFY_URL=https://open.spotify.com/artist/1AHswQqsDNmu1xaE8KpBne

# Corook
COROOK_CHANNEL_ID=@hicorook
COROOK_SPOTIFY_URL=https://open.spotify.com/artist/1rNVlQNJSIESCd5mixdqMt

# Enchanting
ENCHANTING_CHANNEL_ID=@Enchanting
ENCHANTING_SPOTIFY_URL=https://open.spotify.com/artist/26XGM4cZDcTgrXo1nis5HT

# Flyana Boss
FLYANA_BOSS_CHANNEL_ID=@FlyanaBoss
FLYANA_BOSS_SPOTIFY_URL=https://open.spotify.com/artist/0CLW5934vy2XusynS1px1S

# YouTube API Quota Settings
YOUTUBE_QUOTA_LIMIT=10000
YOUTUBE_MAX_RETRIES=3
YOUTUBE_BATCH_SIZE=50

# ETL Configuration
ETL_MAX_VIDEOS_PER_ARTIST=100
ETL_CACHE_THRESHOLD_HOURS=24
ETL_DEVELOPMENT_MODE=false

# Logging
LOG_LEVEL=INFO
LOG_FILE=youtube_etl.log
"""

    # Write to .env file
    with open(env_file, "w") as f:
        f.write(env_content)

    print(f"\n✅ .env file created/updated successfully!")
    print(f"📁 Location: {env_file.absolute()}")
    print("\n🎯 Next Steps:")
    print("1. Verify your YouTube API key is working")
    print("2. Test database connection")
    print("3. Run the ETL notebook: jupyter notebook ETL.ipynb")
    print("4. Execute the pipeline and analyze results!")

    # Offer to test the configuration
    test_config = input("\n❓ Do you want to test the configuration now? (y/n): ").lower().strip()
    if test_config == "y":
        test_configuration()


def test_configuration():
    """Test the configuration to ensure everything is working."""
    print("\n🧪 Testing Configuration...")

    # Test .env file loading
    try:
        from dotenv import load_dotenv

        load_dotenv()

        api_key = os.getenv("YOUTUBE_API_KEY")
        if api_key:
            print("✅ YouTube API key loaded")
        else:
            print("❌ YouTube API key not found")
            return

        db_host = os.getenv("DB_HOST")
        if db_host:
            print("✅ Database configuration loaded")
        else:
            print("❌ Database configuration incomplete")
            return

        print("✅ Configuration test passed!")
        print("\n🚀 Ready to run the ETL pipeline!")

    except ImportError:
        print("⚠️  python-dotenv not installed. Install with: pip install python-dotenv")
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")


if __name__ == "__main__":
    setup_env_file()
