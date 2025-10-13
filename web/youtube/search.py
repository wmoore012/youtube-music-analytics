from __future__ import annotations

import logging
from typing import Any, Dict, List

from googleapiclient.errors import HttpError

from web.youtube.constants import QUOTA_COST
from web.youtube.quota import quota_tracker

logger = logging.getLogger(__name__)


def search_youtube_videos(youtube, query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    """
    Search for YouTube videos matching the query.

    Args:
        youtube: YouTube API client
        query (str): Search query
        max_results (int): Maximum number of results to return

    Returns:
        List[Dict[str, Any]]: List of video search results
    """
    # Get the required quota units
    required_units = QUOTA_COST.get("search.list", 100)

    # Check if we've hit the quota limit
    if not quota_tracker.check_quota(required_units):
        logger.warning(f"Quota limit nearly exhausted ({quota_tracker.get_usage_str()}). Skipping search.")
        return []

    try:
        # Execute the search
        search_response = (
            youtube.search().list(q=query, part="id,snippet", maxResults=max_results, type="video").execute()
        )

        # Track quota usage (search is expensive!)
        quota_tracker.increment(required_units)
        logger.info(f"Quota usage: {quota_tracker.get_usage_str()}")

        return search_response.get("items", [])
    except HttpError as e:
        if "quotaExceeded" in str(e):
            logger.error(f"YouTube API quota exceeded. Current usage: {quota_tracker.get_usage_str()}")
        else:
            logger.error(f"Error searching YouTube: {e}")
        return []

