from __future__ import annotations

import os
from googleapiclient.discovery import build


def get_youtube_client():
    """
    Get an authenticated YouTube API client.

    Returns:
        googleapiclient.discovery.Resource: YouTube API client
    """
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY environment variable not set")

    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

