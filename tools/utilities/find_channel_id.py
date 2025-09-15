#!/usr/bin/env python3
"""
🔍 YouTube Channel ID Finder Utility
===================================

Helps users find YouTube Channel IDs from various input formats.
Supports channel URLs, usernames, and handles.

Usage:
    python tools/utilities/find_channel_id.py @username
    python tools/utilities/find_channel_id.py "https://youtube.com/@artist"
    python tools/utilities/find_channel_id.py "Artist Name - Topic"
"""

import logging
import os
import re
import sys
from typing import Optional

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_youtube_api_key() -> Optional[str]:
    """Get YouTube API key from environment."""
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error("❌ YOUTUBE_API_KEY not found in environment")
        logger.error("   Add your API key to .env file: YOUTUBE_API_KEY=your_key_here")
        return None
    return api_key


def extract_channel_info_from_url(url: str) -> Optional[dict]:
    """Extract channel information from various YouTube URL formats."""

    # Channel ID pattern (UC followed by 22 characters)
    channel_id_pattern = r"UC[a-zA-Z0-9_-]{22}"

    # Handle direct channel ID
    if re.match(channel_id_pattern, url):
        return {"type": "channel_id", "value": url}

    # Handle @username format
    if url.startswith("@"):
        return {"type": "handle", "value": url[1:]}

    # Handle full URLs
    if "youtube.com" in url:
        # Extract channel ID from URL
        channel_id_match = re.search(channel_id_pattern, url)
        if channel_id_match:
            return {"type": "channel_id", "value": channel_id_match.group()}

        # Extract @handle from URL
        handle_match = re.search(r"@([a-zA-Z0-9_-]+)", url)
        if handle_match:
            return {"type": "handle", "value": handle_match.group(1)}

        # Extract username from old format
        username_match = re.search(r"/user/([a-zA-Z0-9_-]+)", url)
        if username_match:
            return {"type": "username", "value": username_match.group(1)}

    # Assume it's a search term for Topic channels
    return {"type": "search", "value": url}


def search_channel_by_name(api_key: str, search_term: str) -> Optional[dict]:
    """Search for channels by name (useful for Topic channels)."""

    url = "https://www.googleapis.com/youtube/v3/search"
    params = {"key": api_key, "part": "snippet", "type": "channel", "q": search_term, "maxResults": 5}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("items"):
            results = []
            for item in data["items"]:
                channel_info = {
                    "channel_id": item["id"]["channelId"],
                    "title": item["snippet"]["title"],
                    "description": (
                        item["snippet"]["description"][:100] + "..."
                        if len(item["snippet"]["description"]) > 100
                        else item["snippet"]["description"]
                    ),
                }
                results.append(channel_info)
            return results

    except requests.RequestException as e:
        logger.error(f"❌ API request failed: {e}")

    return None


def get_channel_by_handle_or_username(api_key: str, handle: str) -> Optional[dict]:
    """Get channel information by handle or username."""

    url = "https://www.googleapis.com/youtube/v3/channels"

    # Try with handle first (modern format)
    params = {
        "key": api_key,
        "part": "snippet,statistics",
        "forHandle": handle if not handle.startswith("@") else handle,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("items"):
            item = data["items"][0]
            return {
                "channel_id": item["id"],
                "title": item["snippet"]["title"],
                "description": (
                    item["snippet"]["description"][:100] + "..."
                    if len(item["snippet"]["description"]) > 100
                    else item["snippet"]["description"]
                ),
                "subscriber_count": item["statistics"].get("subscriberCount", "Hidden"),
                "video_count": item["statistics"].get("videoCount", "0"),
            }

    except requests.RequestException as e:
        logger.warning(f"⚠️ Handle lookup failed: {e}")

    # Try with username (legacy format)
    params = {"key": api_key, "part": "snippet,statistics", "forUsername": handle}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("items"):
            item = data["items"][0]
            return {
                "channel_id": item["id"],
                "title": item["snippet"]["title"],
                "description": (
                    item["snippet"]["description"][:100] + "..."
                    if len(item["snippet"]["description"]) > 100
                    else item["snippet"]["description"]
                ),
                "subscriber_count": item["statistics"].get("subscriberCount", "Hidden"),
                "video_count": item["statistics"].get("videoCount", "0"),
            }

    except requests.RequestException as e:
        logger.error(f"❌ Username lookup failed: {e}")

    return None


def get_channel_by_id(api_key: str, channel_id: str) -> Optional[dict]:
    """Get channel information by channel ID."""

    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {"key": api_key, "part": "snippet,statistics", "id": channel_id}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("items"):
            item = data["items"][0]
            return {
                "channel_id": item["id"],
                "title": item["snippet"]["title"],
                "description": (
                    item["snippet"]["description"][:100] + "..."
                    if len(item["snippet"]["description"]) > 100
                    else item["snippet"]["description"]
                ),
                "subscriber_count": item["statistics"].get("subscriberCount", "Hidden"),
                "video_count": item["statistics"].get("videoCount", "0"),
            }

    except requests.RequestException as e:
        logger.error(f"❌ Channel ID lookup failed: {e}")

    return None


def format_subscriber_count(count_str: str) -> str:
    """Format subscriber count for display."""
    if count_str == "Hidden":
        return "Hidden"

    try:
        count = int(count_str)
        if count >= 1_000_000:
            return f"{count / 1_000_000:.1f}M"
        elif count >= 1_000:
            return f"{count / 1_000:.1f}K"
        else:
            return str(count)
    except (ValueError, TypeError):
        return count_str


def main():
    """Main function to find YouTube channel IDs."""

    if len(sys.argv) != 2:
        print("🔍 YouTube Channel ID Finder")
        print("=" * 40)
        print("Usage:")
        print("  python tools/utilities/find_channel_id.py '@username'")
        print("  python tools/utilities/find_channel_id.py 'https://youtube.com/@artist'")
        print("  python tools/utilities/find_channel_id.py 'Artist Name - Topic'")
        print("  python tools/utilities/find_channel_id.py 'UCxxxxxxxxxxxxxxxxxxxxx'")
        print("")
        print("Examples:")
        print("  python tools/utilities/find_channel_id.py '@BicFizzle'")
        print("  python tools/utilities/find_channel_id.py 'BiC Fizzle - Topic'")
        print("  python tools/utilities/find_channel_id.py 'UC-9-kyTW8ZkZNDHQJ6FgpwQ'")
        sys.exit(1)

    input_value = sys.argv[1]
    api_key = get_youtube_api_key()

    if not api_key:
        sys.exit(1)

    print(f"🔍 Searching for: {input_value}")
    print("=" * 50)

    # Extract channel information
    channel_info = extract_channel_info_from_url(input_value)

    if channel_info["type"] == "channel_id":
        # Direct channel ID lookup
        result = get_channel_by_id(api_key, channel_info["value"])
        if result:
            print("✅ Channel Found!")
            print(f"📺 Title: {result['title']}")
            print(f"🆔 Channel ID: {result['channel_id']}")
            print(f"👥 Subscribers: {format_subscriber_count(result['subscriber_count'])}")
            print(f"🎬 Videos: {result['video_count']}")
            print(f"📝 Description: {result['description']}")
            print("")
            print("💡 Add to .env file:")
            print(f"YT_ARTISTNAME_YT={result['channel_id']}")
        else:
            print("❌ Channel not found or invalid Channel ID")

    elif channel_info["type"] in ["handle", "username"]:
        # Handle or username lookup
        result = get_channel_by_handle_or_username(api_key, channel_info["value"])
        if result:
            print("✅ Channel Found!")
            print(f"📺 Title: {result['title']}")
            print(f"🆔 Channel ID: {result['channel_id']}")
            print(f"👥 Subscribers: {format_subscriber_count(result['subscriber_count'])}")
            print(f"🎬 Videos: {result['video_count']}")
            print(f"📝 Description: {result['description']}")
            print("")
            print("💡 Add to .env file:")
            print(f"YT_ARTISTNAME_YT={result['channel_id']}")
        else:
            print(f"❌ Channel not found for handle/username: {channel_info['value']}")

    elif channel_info["type"] == "search":
        # Search by name (useful for Topic channels)
        results = search_channel_by_name(api_key, channel_info["value"])
        if results:
            print(f"✅ Found {len(results)} channels:")
            print("")
            for i, result in enumerate(results, 1):
                print(f"{i}. 📺 {result['title']}")
                print(f"   🆔 Channel ID: {result['channel_id']}")
                print(f"   📝 Description: {result['description']}")
                print(f"   💡 Add to .env: YT_ARTISTNAME_YT={result['channel_id']}")
                print("")
        else:
            print(f"❌ No channels found for search term: {channel_info['value']}")

    print("🎯 Pro Tips:")
    print("• Use Channel IDs instead of URLs for better performance")
    print("• Topic channels usually end with '- Topic' in the name")
    print("• Main artist channels have @handles, Topic channels don't")
    print("• Always verify the channel content before adding to .env")


if __name__ == "__main__":
    main()
