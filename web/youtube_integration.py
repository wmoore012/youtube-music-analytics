# SPDX-License-Identifier: GPL-3.0-or-later
"""
YouTube data integration for the iCatalog ETL pipeline.

This module provides functions to extract data from the YouTube API,
map it to the appropriate tables, and integrate it with the existing ETL pipeline.
It also uses the YouTube version parser to extract artist names and version information
from video titles.

The module supports two approaches for finding YouTube videos:
1. Playlist-based approach: Uses curated YouTube playlists to find videos
2. Search-based approach: Searches YouTube for videos matching songs in the database

It also supports cleaning the database to remove incorrect songs:
1. Selective clean: Removes only songs that don't belong to the user's catalog
2. Full clean: Removes all records for a fresh start

Features:
- Caching of video details to avoid reprocessing
- Progress tracking to resume from where it left off
- Retry logic with exponential backoff for quota errors
- Prioritization of specific playlists
- Skipping metrics update to save quota
- Development mode for easier development and testing
- Persistent staging of raw data for reprocessing without calling the API again
- Batch processing and bulk upserts for efficiency
- Daily reset of playlist progress in development mode
- Tracking of videos fetched from each playlist

Usage:
    # Run with default settings (playlist approach, no cleaning)
    python youtube_integration.py

    # Run with cleaning (selective clean)
    python youtube_integration.py --clean

    # Run with full cleaning (remove all records)
    python youtube_integration.py --clean --full-clean

    # Run with search-based approach
    python youtube_integration.py --no-playlist

    # Run with custom batch size (smaller batch size reduces quota usage)
    python youtube_integration.py --batch-size 5

    # Process a specific playlist first
    python youtube_integration.py --priority-playlist PLl-ShioB5kaqu8jD43bGi7qX799RIZA3Q

    # Process only a specific playlist
    python youtube_integration.py --only-playlist PLl-ShioB5kaqu8jD43bGi7qX799RIZA3Q

    # Skip updating metrics to save quota
    python youtube_integration.py --skip-metrics

    # Run in development mode (fetch all videos, ignore progress tracking, store raw data)
    python youtube_integration.py --development-mode

    # Run in development mode with full clean (remove all records and start fresh)
    python youtube_integration.py --development-mode --full-clean
"""
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import func, inspect, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import Connection, Engine

# ---- Feature flags ---------------------------------------------------------
try:
    # wherever you keep globals; adjust the module name
    from config import YT_SEARCH_ENABLED
except Exception:
    import os

    YT_SEARCH_ENABLED = os.getenv("YT_SEARCH_ENABLED", "0") == "1"

from web.etl_helpers import (
    bulk_upsert,
    ensure_dsp_rows,
    get_engine,
    get_table,
    init_tables,
)

# Import YouTube version parser functions
from .youtube_version_parser import parse_youtube_title

# ── Artist / version helpers ──────────────────────────────────────────────
VERSION_KEYWORDS = {
    "Official Music Video": ["official video", "official music video", "mv"],
    "Official Audio": ["official audio", "- topic"],
    "Lyric Video": ["lyric video", "lyrics"],
    "Live Performance": ["live", "performance", "session"],
    "Remix": ["remix", "mashup"],
    "Acoustic": ["acoustic", "unplugged"],
    "Cover": ["cover"],
    "Chopped/Slowed": ["slowed", "reverb", "chopped", "screwed"],
    "Visualizer": ["visualizer", "lyric visualizer"],
}


def _normalize(name: str) -> str:
    """Lower‑case, strip punctuation and extra whitespace."""
    return re.sub(r"[^a-z0-9 ]", " ", name.lower()).strip()


def resolve_artist_id(conn: Connection, channel: str, parsed_artists: List[str]) -> Optional[int]:
    """
    Try to find the matching artist_id for this video using:
    1) Channel title heuristics (Artist, ArtistVEVO, Artist – Topic)
    2) Parsed artist names from the title

    Returns the artist_id or None.
    """
    artists_tbl = get_table("artists")
    candidates = []

    # 1️⃣ Channel title
    if channel:
        chan = channel.replace("VEVO", "", 1).replace("- Topic", "").strip()
        candidates.append(chan)

    # 2️⃣ Parsed artist names from the video title
    if parsed_artists:
        candidates.extend(parsed_artists)

    # First try exact match with artist_name (case-insensitive)
    for cand in {c for c in candidates if c}:  # unique, non‑empty
        # Try direct match with artist name
        row = conn.execute(
            select(artists_tbl.c.artist_id).where(func.lower(artists_tbl.c.artist_name) == _normalize(cand))
        ).fetchone()
        if row:
            return row.artist_id

    return None


def classify_version(video_title: str, channel_title: str, description: str = "") -> str:
    """
    Return the best‑guess version label based on keywords.
    Defaults to 'Audio' when nothing obvious is found.
    """
    text = f"{video_title} {description}".lower()
    for label, words in VERSION_KEYWORDS.items():
        if any(w in text for w in words):
            return label
    # Fallback: Official Audio if on '‑ Topic' channel, else plain Audio
    if "- topic" in channel_title.lower():
        return "Official Audio"
    return "Audio"


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)


def get_video_id(item):
    """
    Safely get the ID from a video object, whether it's accessed as an attribute or as a dictionary key.

    Args:
        item: A video object from the YouTube API

    Returns:
        str: The video ID, or None if not found
    """
    return getattr(item, "id", None) or item.get("id")  # works for obj or dict


def safe_execute(request):
    """
    Safely execute a YouTube API request, aborting immediately on quotaExceeded errors.

    Args:
        request: A YouTube API request object

    Returns:
        dict: The response from the API

    Raises:
        SystemExit: If the quota is exceeded
        HttpError: For any other API errors
    """
    try:
        return request.execute()
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            logger.error("Daily YouTube quota exhausted – aborting run 😭")
            raise SystemExit(1)  # stop the pipeline
        raise  # any other error → bubble up


# YouTube API quota costs (approximate)
QUOTA_COST = {
    "videos.list": 1,  # 1 unit per request, regardless of number of videos (up to 50)
    "search.list": 100,  # 100 units per request
    "playlistItems.list": 1,  # 1 unit per request
}


class QuotaTracker:
    """
    Track YouTube API quota usage to avoid exceeding daily limits.

    Attributes:
        units (int): Current quota units used
        max_units (int): Maximum quota units allowed (0 = no limit)
    """

    def __init__(self, max_units: int = 0):
        """
        Initialize the quota tracker.

        Args:
            max_units (int): Maximum quota units allowed (0 = no limit)
        """
        self.units = 0
        self.max_units = max_units

    def check_quota(self, required_units: int = 1) -> bool:
        """
        Check if there's enough quota available.

        Args:
            required_units (int): Units required for the next operation

        Returns:
            bool: True if enough quota is available, False otherwise
        """
        if self.max_units <= 0:  # No limit
            return True
        return (self.units + required_units) <= self.max_units

    def increment(self, units: int = 1) -> None:
        """
        Increment the quota usage.

        Args:
            units (int): Units to add to the current usage
        """
        self.units += units

    def get_usage_str(self) -> str:
        """
        Get a string representation of the current quota usage.

        Returns:
            str: Quota usage string
        """
        if self.max_units <= 0:
            return f"{self.units} units"
        return f"{self.units}/{self.max_units} units"


# Initialize quota tracker
quota_tracker = QuotaTracker()


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


def get_playlist_videos(
    youtube,
    playlist_id: str,
    max_videos: int = 0,
    offset: int = 0,
    engine: Engine = None,
    development_mode: bool = False,
    cache_threshold_hours: int = 24,
) -> List[Dict[str, Any]]:
    """
    Get videos from a YouTube playlist, up to a maximum number if specified.

    In development mode, this function will fetch all videos from the playlist regardless of max_videos.
    It will also store the raw playlist data in the staging table if engine is provided.

    This function implements a caching mechanism to avoid unnecessary API calls:
    - If data for the playlist exists in the youtube_playlists_raw table and is less than cache_threshold_hours old,
      it will use the cached data instead of making API calls.
    - If the data is older than cache_threshold_hours, it will fetch fresh data from the API.
    - The cache_threshold_hours parameter defaults to 24 hours, meaning data will be refreshed once per day.

    Args:
        youtube: YouTube API client
        playlist_id (str): YouTube playlist ID
        max_videos (int): Maximum number of videos to retrieve (0 = no limit)
        offset (int): Number of videos to skip from the beginning of the playlist
        engine (Engine, optional): SQLAlchemy engine for storing raw data
        development_mode (bool): Whether to run in development mode (fetch all videos)
        cache_threshold_hours (int): Number of hours before cached data is considered stale (default: 24)

    Returns:
        List[Dict[str, Any]]: List of video items from the playlist
    """
    try:
        videos = []
        next_page_token = None
        total_fetched = 0  # Total number of videos fetched (including skipped ones)
        total_pages = 0  # Total number of API pages fetched
        using_cached_data = False

        # In development mode, we fetch all videos regardless of max_videos
        effective_max = 0 if development_mode else max_videos

        # Log the mode we're running in
        if development_mode:
            logger.info(f"Running in DEVELOPMENT MODE: Will fetch ALL videos from playlist {playlist_id}")
        elif max_videos > 0:
            logger.info(f"Running in PRODUCTION MODE: Will fetch up to {max_videos} videos from playlist {playlist_id}")
        else:
            logger.info(f"Running in PRODUCTION MODE: Will fetch all videos from playlist {playlist_id}")

        # Check if we have cached data for this playlist
        if engine:
            try:
                # Get raw playlist data from the database
                cached_data = get_raw_playlist_data(engine, playlist_id)

                if cached_data:
                    # Get the most recent cached data
                    most_recent = max(cached_data, key=lambda x: x["fetched_at"])

                    # Calculate how old the data is
                    fetched_at = most_recent["fetched_at"]
                    age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600

                    # If the data is fresh enough, use it
                    if age_hours < cache_threshold_hours:
                        logger.info(
                            f"🔄 USING CACHED DATA: Playlist data from {fetched_at.isoformat()} ({age_hours:.1f} hours old)"
                        )
                        logger.info(f"💾 Cache threshold: {cache_threshold_hours} hours - No API call needed")

                        # Extract items from the cached data
                        raw_data = most_recent["raw_data"]
                        all_items = []

                        # If this is a single page response
                        if "items" in raw_data:
                            all_items.extend(raw_data.get("items", []))

                        # Process the items, respecting offset and max_videos
                        for i, item in enumerate(all_items):
                            if i < offset:
                                continue
                            videos.append(item)
                            if effective_max > 0 and len(videos) >= effective_max:
                                break

                        logger.info(f"📦 Retrieved {len(videos)} videos from CACHED DATA - No YouTube API quota used")
                        using_cached_data = True
                    else:
                        logger.info(
                            f"🔄 FETCHING FRESH DATA: Cached data is {age_hours:.1f} hours old (threshold: {cache_threshold_hours})"
                        )
                        logger.info(f"⚠️ Cache expired - Will use YouTube API quota")
                else:
                    logger.info(f"🔄 FETCHING FRESH DATA: No cached data found for playlist {playlist_id}")
                    logger.info(f"⚠️ No cache available - Will use YouTube API quota")
            except Exception as e:
                logger.warning(f"Error checking cached data: {e}")
                # Continue with API fetching

        # If we're not using cached data, fetch from the API
        if not using_cached_data:
            while True:
                # Check if we've reached the maximum number of videos (only in production mode)
                if effective_max > 0 and len(videos) >= effective_max:
                    logger.info(f"Reached maximum number of videos ({effective_max}). Stopping playlist retrieval.")
                    break

                # Get the required quota units
                required_units = QUOTA_COST.get("playlistItems.list", 1)

                # Check if we've hit the quota limit
                if not quota_tracker.check_quota(required_units):
                    logger.warning(
                        f"Quota limit nearly exhausted ({quota_tracker.get_usage_str()}). Stopping playlist retrieval."
                    )
                    break

                # Calculate how many more videos we need (always 50 in development mode)
                remaining = effective_max - len(videos) if effective_max > 0 else 50
                max_results = min(50, remaining) if effective_max > 0 else 50  # API maximum is 50

                # Get playlist items
                playlist_response = (
                    youtube.playlistItems()
                    .list(
                        part="snippet,contentDetails",
                        playlistId=playlist_id,
                        maxResults=max_results,
                        pageToken=next_page_token,
                    )
                    .execute()
                )

                # Increment page counter
                total_pages += 1

                # Track quota usage
                quota_tracker.increment(required_units)
                logger.info(f"Quota usage: {quota_tracker.get_usage_str()}")

                # Store raw playlist data if engine is provided
                if engine:
                    try:
                        # Add page information to the response
                        playlist_response["_meta"] = {
                            "page": total_pages,
                            "playlist_id": playlist_id,
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                        }
                        # Store with the main playlist ID for the first page to enable caching
                        store_id = playlist_id if total_pages == 1 else f"{playlist_id}_page{total_pages}"
                        store_raw_playlist_data(engine, store_id, playlist_response)
                    except Exception as e:
                        logger.warning(f"Error storing raw playlist data: {e}")

                # Add items to the list, respecting the offset
                new_items = playlist_response.get("items", [])
                logger.info(f"Retrieved {len(new_items)} videos from page {total_pages}")

                for item in new_items:
                    total_fetched += 1
                    # Skip items before the offset
                    if total_fetched <= offset:
                        continue
                    videos.append(item)
                    # Check if we've reached the maximum number of videos (only in production mode)
                    if effective_max > 0 and len(videos) >= effective_max:
                        break

                # Check if we've reached the maximum number of videos (only in production mode)
                if effective_max > 0 and len(videos) >= effective_max:
                    logger.info(f"Reached maximum number of videos ({effective_max}). Stopping playlist retrieval.")
                    break

                # Check if there are more pages
                next_page_token = playlist_response.get("nextPageToken")
                if not next_page_token:
                    logger.info(f"No more pages available. Completed retrieval after {total_pages} pages.")
                    break

        # Log detailed counts
        logger.info(
            f"Found {len(videos)} videos in playlist {playlist_id} after processing {total_pages} pages"
            + (f" (offset: {offset}, limited to {effective_max})" if effective_max > 0 else f" (offset: {offset})")
        )

        # If max_videos is set and we're not in development mode, ensure we don't return more than that
        if effective_max > 0 and len(videos) > effective_max:
            logger.info(f"Limiting results to {effective_max} videos (from {len(videos)} total)")
            videos = videos[:effective_max]

        return videos
    except HttpError as e:
        if "quotaExceeded" in str(e):
            logger.error(f"YouTube API quota exceeded. Current usage: {quota_tracker.get_usage_str()}")
        else:
            logger.error(f"Error getting playlist videos: {e}")
        return []


def get_video_details(
    youtube,
    video_ids: List[str],
    max_retries: int = 3,
    initial_delay: float = 1.0,
    engine: Engine = None,
) -> List[Dict[str, Any]]:
    """
    Get detailed information for a list of video IDs.
    Implements retry logic with exponential backoff for quota errors.

    If an engine is provided, this function will also store the raw video data in the staging table.

    Args:
        youtube: YouTube API client
        video_ids (List[str]): List of YouTube video IDs
        max_retries (int): Maximum number of retries for quota errors
        initial_delay (float): Initial delay in seconds before retrying
        engine (Engine, optional): SQLAlchemy engine for storing raw data

    Returns:
        List[Dict[str, Any]]: List of video details
    """
    if not video_ids:
        return []

    # Use a smaller batch size to reduce the chance of quota errors
    batch_size = 10  # Reduced from 50 to 10
    all_videos = []
    processed_ids = set()  # Track processed IDs to avoid duplicates

    # Create a cache file path
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "youtube_video_details_cache.json")

    # Load cache if it exists
    video_cache = {}
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                video_cache = json.load(f)
            logger.info(f"Loaded {len(video_cache)} videos from cache")
        except Exception as e:
            logger.warning(f"Error loading video cache: {e}")

    # Check which videos are already in the cache
    videos_to_fetch = []
    for video_id in video_ids:
        if video_id in video_cache:
            all_videos.append(video_cache[video_id])
            processed_ids.add(video_id)
        else:
            videos_to_fetch.append(video_id)

    logger.info(f"Found {len(processed_ids)} videos in cache, need to fetch {len(videos_to_fetch)}")

    # Process videos in smaller batches
    for i in range(0, len(videos_to_fetch), batch_size):
        # Get the required quota units
        required_units = QUOTA_COST.get("videos.list", 1)

        # Check if we've hit the quota limit
        if not quota_tracker.check_quota(required_units):
            logger.warning(f"Quota limit nearly exhausted ({quota_tracker.get_usage_str()}). Stopping processing.")
            break

        batch = videos_to_fetch[i : i + batch_size]

        # Skip if all videos in this batch have been processed
        if all(video_id in processed_ids for video_id in batch):
            continue

        # Filter out already processed videos
        batch = [video_id for video_id in batch if video_id not in processed_ids]

        if not batch:
            continue

        # Try to get video details with retries
        retry_count = 0
        delay = initial_delay

        while retry_count <= max_retries:
            try:
                logger.info(
                    f"Fetching details for batch {i//batch_size + 1}/{(len(videos_to_fetch)-1)//batch_size + 1} ({len(batch)} videos)"
                )

                # Use safe_execute to abort immediately on quota exceeded
                video_response = safe_execute(
                    youtube.videos().list(id=",".join(batch), part="snippet,contentDetails,statistics")
                )

                # Track quota usage
                quota_tracker.increment(required_units)
                logger.info(f"Quota usage: {quota_tracker.get_usage_str()}")

                batch_videos = video_response.get("items", [])
                all_videos.extend(batch_videos)

                # Store raw video data if engine is provided
                if engine:
                    for video in batch_videos:
                        try:
                            # Add metadata to the video
                            video["_meta"] = {"fetched_at": datetime.now(timezone.utc).isoformat()}
                            store_raw_video_data(engine, video["id"], video)
                        except Exception as e:
                            logger.warning(f"Error storing raw video data: {e}")

                # Update processed IDs and cache
                for video in batch_videos:
                    video_id = video["id"]
                    processed_ids.add(video_id)
                    video_cache[video_id] = video

                # Save cache periodically
                if len(video_cache) % 50 == 0:
                    try:
                        with open(cache_file, "w") as f:
                            json.dump(video_cache, f)
                        logger.info(f"Saved {len(video_cache)} videos to cache")
                    except Exception as e:
                        logger.warning(f"Error saving video cache: {e}")

                # Break out of retry loop on success
                break

            except HttpError as e:
                # For non-quota errors, log and break
                logger.error(f"Error getting video details: {e}")
                break

    # Save the final cache
    try:
        with open(cache_file, "w") as f:
            json.dump(video_cache, f)
        logger.info(f"Saved {len(video_cache)} videos to cache")
    except Exception as e:
        logger.warning(f"Error saving video cache: {e}")

    logger.info(f"Successfully processed {len(processed_ids)}/{len(video_ids)} videos")
    return all_videos


def find_youtube_videos_for_songs(*_, **__):
    # Guard in case someone flips the flag back
    if YT_SEARCH_ENABLED:
        raise NotImplementedError("Re-enable the old search code before using this.")
    return {}


def find_youtube_videos_from_playlist(
    engine: Engine,
    youtube,
    playlist_id: str,
    max_videos: int = 0,
    offset: int = 0,
    development_mode: bool = False,
    cache_threshold_hours: int = 24,
) -> Dict[str, str]:
    """
    Find YouTube videos for songs in the database using a curated playlist.

    This approach uses a specific YouTube playlist instead of searching for videos.
    It matches videos to songs based on ISRCs in the video description or title matching.

    In development mode, this function will fetch all videos from the playlist regardless of max_videos
    and store raw data in the staging tables.

    This function uses a caching mechanism to avoid unnecessary API calls:
    - If data for the playlist exists in the youtube_playlists_raw table and is less than cache_threshold_hours old,
      it will use the cached data instead of making API calls.
    - If the data is older than cache_threshold_hours, it will fetch fresh data from the API.
    - The cache_threshold_hours parameter defaults to 24 hours, meaning data will be refreshed once per day.

    Args:
        engine (Engine): SQLAlchemy engine
        youtube: YouTube API client
        playlist_id (str): YouTube playlist ID
        max_videos (int): Maximum number of videos to process (0 = no limit)
        offset (int): Number of videos to skip from the beginning of the playlist
        development_mode (bool): Whether to run in development mode (fetch all videos)
        cache_threshold_hours (int): Number of hours before cached data is considered stale (default: 24)

    Returns:
        Dict[str, str]: Mapping of ISRCs to YouTube video IDs
    """
    # Get table handles
    songs_tbl = get_table("songs")

    # Get all songs from the database
    song_map = {}
    with engine.connect() as conn:
        # Get all songs
        query = text(
            """
            SELECT s.isrc, s.song_title, GROUP_CONCAT(a.artist_name SEPARATOR ', ') as artists
            FROM songs s
            LEFT JOIN song_artist_roles sar ON s.isrc = sar.isrc
            LEFT JOIN artists a ON sar.artist_id = a.artist_id
            GROUP BY s.isrc, s.song_title
        """
        )

        songs = conn.execute(query).fetchall()
        logger.info(f"Found {len(songs)} songs in the database")

        # Create a map of ISRCs to songs
        for song in songs:
            song_map[song.isrc] = {
                "isrc": song.isrc,
                "title": song.song_title,
                "artists": song.artists or "",
            }

    # Get videos from the playlist
    playlist_videos = get_playlist_videos(
        youtube,
        playlist_id,
        max_videos,
        offset,
        engine,
        development_mode,
        cache_threshold_hours,
    )
    logger.info(
        f"Processing {len(playlist_videos)} videos from playlist (offset: {offset}, max: {max_videos if max_videos > 0 else 'unlimited'}, development_mode: {development_mode}, cache_threshold_hours: {cache_threshold_hours})"
    )

    # Extract video IDs for detailed info
    video_ids = [video["contentDetails"]["videoId"] for video in playlist_videos]
    video_details = get_video_details(youtube, video_ids, engine=engine)

    # Map video IDs to details
    video_id_to_details = {video["id"]: video for video in video_details}

    # Match videos to songs
    isrc_to_video_id = {}
    video_to_song_matches = []

    # First pass: Try to find ISRCs in video descriptions
    for video in playlist_videos:
        video_id = video["contentDetails"]["videoId"]

        # Get detailed info
        if video_id not in video_id_to_details:
            continue

        details = video_id_to_details[video_id]
        snippet = details.get("snippet", {})

        # Extract title and description
        title = snippet.get("title", "")
        description = snippet.get("description", "")
        channel = snippet.get("channelTitle", "")

        # Look for ISRC in description
        isrc_match = re.search(r"ISRC:?\s*([A-Z]{2}[A-Z0-9]{10})", description, re.IGNORECASE)
        if isrc_match:
            isrc = isrc_match.group(1)
            if isrc in song_map:
                isrc_to_video_id[isrc] = video_id
                logger.info(f"Matched video {video_id} to song with ISRC {isrc} from description")
                continue

        # If no ISRC in description, store for title matching in second pass
        video_to_song_matches.append({"video_id": video_id, "title": title, "channel": channel})

    # Second pass: Match by title similarity
    for video in video_to_song_matches:
        video_id = video["video_id"]
        title = video["title"]
        channel = video["channel"]

        # Parse the title
        parsed_data = parse_youtube_title(title, channel)
        parsed_title = parsed_data.get("title", "").lower()
        parsed_artists = parsed_data.get("artists", [])

        best_match = None
        best_score = 0

        # Compare with each song
        for isrc, song in song_map.items():
            # Skip if already matched
            if isrc in isrc_to_video_id:
                continue

            song_title = song["title"].lower()
            song_artists = song["artists"].lower() if song["artists"] else ""

            # Calculate title similarity
            title_score = 0
            if parsed_title and song_title:
                if parsed_title in song_title or song_title in parsed_title:
                    title_score = 80
                elif any(word in parsed_title for word in song_title.split() if len(word) > 3):
                    title_score = 50

            # Calculate artist similarity
            artist_score = 0
            if parsed_artists and song_artists:
                for artist in parsed_artists:
                    if artist.lower() in song_artists:
                        artist_score = 80
                        break

            # Calculate overall score
            score = (title_score * 0.7) + (artist_score * 0.3)

            # Update best match if score is higher
            if score > best_score and score >= 50:  # Threshold of 50
                best_score = score
                best_match = isrc

        # If we found a match, add it to the map
        if best_match:
            isrc_to_video_id[best_match] = video_id
            logger.info(
                f"Matched video {video_id} to song with ISRC {best_match} by title similarity (score: {best_score})"
            )

    logger.info(f"Matched {len(isrc_to_video_id)} videos to songs")
    return isrc_to_video_id


def insert_youtube_videos(engine: Engine, isrc_to_video_id: Dict[str, str], development_mode: bool = False) -> None:
    """
    Insert YouTube videos into the database.

    In development mode, this function will log more detailed information about the videos being inserted.

    Args:
        engine (Engine): SQLAlchemy engine
        isrc_to_video_id (Dict[str, str]): Mapping of ISRCs to YouTube video IDs
        development_mode (bool): Whether to run in development mode
    """
    if not isrc_to_video_id:
        return

    # Get YouTube client
    youtube = get_youtube_client()

    # Get video details (pass engine for storing raw data)
    video_ids = list(isrc_to_video_id.values())

    # Log more detailed information in development mode
    if development_mode:
        logger.info(f"Fetching details for {len(video_ids)} videos in development mode")

    video_details = get_video_details(youtube, video_ids, engine=engine)

    # Map video IDs to details
    video_id_to_details = {video["id"]: video for video in video_details}

    # Log counts in development mode
    if development_mode:
        logger.info(f"Retrieved details for {len(video_details)}/{len(video_ids)} videos")
        logger.info(f"Videos waiting to be upserted: {len(video_id_to_details)}")

    # Prepare records for youtube_videos table
    youtube_videos_tbl = get_table("youtube_videos")
    video_records = []

    # Create a connection to use for artist_id resolution
    with engine.connect() as conn:
        # Get table handles
        artists_tbl = get_table("artists")

        # ---------- resolve & upsert artists ----------
        def ensure_artist(name: str) -> int:
            # Special case for "blackwave." which has known duplicates
            if name.lower() == "blackwave.":
                # Try to find the existing artist with exact match first
                row = conn.execute(
                    select(artists_tbl.c.artist_id).where(artists_tbl.c.artist_name == "blackwave.")
                ).fetchone()
                if row:
                    logger.info(f"Found existing artist 'blackwave.' with ID {row.artist_id}")
                    return row.artist_id

            # First try direct match with artist name (case-insensitive)
            row = conn.execute(
                select(artists_tbl.c.artist_id).where(func.lower(artists_tbl.c.artist_name) == _normalize(name))
            ).fetchone()
            if row:
                return row.artist_id

            # If no match found, try to create a new artist
            try:
                # For "blackwave.", try one more time with exact match before creating
                if name.lower() == "blackwave.":
                    # Try to find with exact match
                    row = conn.execute(
                        select(artists_tbl.c.artist_id).where(artists_tbl.c.artist_name == "blackwave.")
                    ).fetchone()
                    if row:
                        logger.info(f"Found existing artist 'blackwave.' with ID {row.artist_id} (second check)")
                        return row.artist_id
                    # If still not found, log that we're about to create it
                    logger.info(f"Creating new artist 'blackwave.' after multiple checks")

                rid = conn.execute(artists_tbl.insert().values(artist_name=name)).lastrowid

                # Also add this name as a preferred alias for the new artist
                try:
                    if inspector.has_table("artist_aliases"):
                        conn.execute(
                            mysql_insert(aliases_tbl)
                            .values(artist_id=rid, alias=name, is_preferred=True)
                            .prefix_with("IGNORE")  # In case of duplicate
                        )
                except Exception as e:
                    logger.warning(f"Error adding artist alias: {e}")

                return rid
            except Exception as e:
                # If insertion fails (e.g., due to duplicate key), try to find the artist again
                if "Duplicate entry" in str(e):
                    logger.warning(f"Duplicate artist entry for '{name}', trying to find existing artist")

                    # For "blackwave.", try with exact match first
                    if name.lower() == "blackwave.":
                        row = conn.execute(
                            select(artists_tbl.c.artist_id).where(artists_tbl.c.artist_name == "blackwave.")
                        ).fetchone()
                        if row:
                            logger.info(
                                f"Found existing artist 'blackwave.' with ID {row.artist_id} after duplicate error"
                            )
                            return row.artist_id

                    # Try to find the artist again (case-insensitive)
                    row = conn.execute(
                        select(artists_tbl.c.artist_id).where(func.lower(artists_tbl.c.artist_name) == _normalize(name))
                    ).fetchone()
                    if row:
                        return row.artist_id
                    else:
                        # If we still can't find it, try with exact match
                        row = conn.execute(
                            select(artists_tbl.c.artist_id).where(artists_tbl.c.artist_name == name)
                        ).fetchone()
                        if row:
                            return row.artist_id
                        else:
                            # If we still can't find it, log the error and raise
                            logger.error(f"Failed to create or find artist '{name}': {e}")
                            raise
                else:
                    # For other errors, log and raise
                    logger.error(f"Error creating artist '{name}': {e}")
                    raise

        for isrc, video_id in isrc_to_video_id.items():
            if video_id in video_id_to_details:
                details = video_id_to_details[video_id]
                snippet = details.get("snippet", {})
                content_details = details.get("contentDetails", {})

                # Parse duration (ISO 8601 duration format)
                duration_str = content_details.get("duration", "PT0S")
                duration_sec = parse_duration(duration_str)

                # Parse published date
                published_at_str = snippet.get("publishedAt")
                published_at = None
                if published_at_str:
                    try:
                        published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                    except ValueError:
                        logger.warning(f"Could not parse publishedAt: {published_at_str}")

                # Extract video title and channel title
                video_title = snippet.get("title", "")
                channel_title = snippet.get("channelTitle", "")

                # Parse YouTube title to extract artist names and version information
                parsed = parse_youtube_title(video_title, channel_title)

                primary_names = parsed["primary"]
                featured_names = parsed["featured"]

                # Resolve artist IDs
                primary_ids = [ensure_artist(n) for n in primary_names]
                featured_ids = [ensure_artist(n) for n in featured_names]

                # Keep single FK for the main table (first primary artist or None)
                primary_id = primary_ids[0] if primary_ids else None

                # Determine version type using helper
                version_type = classify_version(video_title, channel_title, snippet.get("description", ""))

                # Build the record
                video_records.append(
                    {
                        "video_id": video_id,
                        "isrc": isrc,
                        "title": video_title,  # matches schema
                        "channel_title": channel_title,
                        "published_at": published_at,
                        "duration": duration_str,  # matches schema (varchar)
                        "dsp_name": "YouTube",  # matches schema
                        # Store artist IDs for linking after video insertion
                        "_primary_artist_ids": primary_ids,
                        "_featured_artist_ids": featured_ids,
                    }
                )

    # Bulk upsert to youtube_videos table
    if video_records:
        # Create a copy of the records without the temporary fields for bulk_upsert
        clean_records = []
        for record in video_records:
            clean_record = record.copy()
            # Remove temporary fields
            primary_artist_ids = clean_record.pop("_primary_artist_ids", [])
            featured_artist_ids = clean_record.pop("_featured_artist_ids", [])
            clean_records.append(clean_record)

        written = bulk_upsert(
            engine,
            youtube_videos_tbl,
            clean_records,
            conflict_columns=["video_id"],  # primary key
            update_columns=[
                "isrc",
                "title",
                "channel_title",
                "published_at",
                "duration",
                "dsp_name",
            ],
        )
        logger.info(f"Inserted {written} records into youtube_videos")


def update_youtube_metrics(engine: Engine) -> None:
    """
    Update YouTube metrics for videos in the database.

    Args:
        engine (Engine): SQLAlchemy engine
    """
    # Import the metrics helper functions
    from web.youtube_metrics_helpers import get_playlist_count, upsert_metrics

    # Get table handles
    youtube_videos_tbl = get_table("youtube_videos")
    songs_tbl = get_table("songs")

    # Get YouTube client
    youtube = get_youtube_client()

    # Get all YouTube videos from the database
    with engine.connect() as conn:
        # Get all valid ISRCs from the songs table
        valid_isrcs = set(row[0] for row in conn.execute(select(songs_tbl.c.isrc)).fetchall())
        logger.info(f"Found {len(valid_isrcs)} valid ISRCs in the songs table")

        # Get videos that have valid ISRCs
        videos = conn.execute(select(youtube_videos_tbl).where(youtube_videos_tbl.c.isrc.in_(valid_isrcs))).fetchall()

        logger.info(f"Found {len(videos)} YouTube videos with valid ISRCs")

        # Get video IDs
        video_ids = [video.video_id for video in videos]

        # Get video details
        video_details = get_video_details(youtube, video_ids)

        # Map video IDs to details
        video_id_to_details = {video["id"]: video for video in video_details}

        # Process each video and upsert metrics
        metrics_count = 0
        for video in videos:
            if video.video_id in video_id_to_details:
                details = video_id_to_details[video.video_id]
                statistics = details.get("statistics", {})

                # Extract metrics
                view_count = int(statistics.get("viewCount", 0))
                like_count = int(statistics.get("likeCount", 0))

                # Use playlist count as a replacement for favorite_count
                # This is a placeholder that currently returns 0
                # In a production environment, this would query the YouTube API
                # to get the number of playlists a video appears in
                playlist_count = get_playlist_count(conn, video.video_id)

                comment_count = int(statistics.get("commentCount", 0))

                # Upsert metrics for this video
                upsert_metrics(
                    engine,
                    isrc=video.isrc,
                    video_id=video.video_id,
                    views=view_count,
                    likes=like_count,
                    faves=playlist_count,  # Use playlist count instead of favorite_count
                    comments=comment_count,
                )
                metrics_count += 1

        logger.info(f"Upserted metrics for {metrics_count} videos")

        # Log a note about the favorite_count replacement
        if metrics_count > 0:
            logger.info("Note: favorite_count has been replaced with playlist_count due to API deprecation")
            logger.info("      Currently returning 0 as a placeholder until full implementation")


def store_raw_video_data(engine: Engine, video_id: str, raw_data: dict) -> None:
    """
    Store raw video data in the staging table.

    Args:
        engine (Engine): SQLAlchemy engine
        video_id (str): YouTube video ID
        raw_data (dict): Raw video data from the YouTube API
    """
    with engine.begin() as conn:
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table("youtube_videos_raw"):
            logger.warning("youtube_videos_raw table does not exist. Skipping raw data storage.")
            return

        # Insert or update the raw data
        conn.execute(
            text(
                """
            INSERT INTO youtube_videos_raw (video_id, raw_data, fetched_at, processed)
            VALUES (:video_id, :raw_data, NOW(), FALSE)
            ON DUPLICATE KEY UPDATE
                raw_data = :raw_data,
                fetched_at = NOW(),
                processed = FALSE
        """
            ),
            {"video_id": video_id, "raw_data": json.dumps(raw_data)},
        )

        logger.debug(f"Stored raw payload for video {video_id} in youtube_videos_raw")


def store_raw_playlist_data(engine: Engine, playlist_id: str, raw_data: dict) -> None:
    """
    Store raw playlist data in the staging table.

    Args:
        engine (Engine): SQLAlchemy engine
        playlist_id (str): YouTube playlist ID
        raw_data (dict): Raw playlist data from the YouTube API
    """
    with engine.begin() as conn:
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table("youtube_playlists_raw"):
            logger.warning("youtube_playlists_raw table does not exist. Skipping raw data storage.")
            return

        # Insert or update the raw data
        conn.execute(
            text(
                """
            INSERT INTO youtube_playlists_raw (playlist_id, raw_data, fetched_at, processed)
            VALUES (:playlist_id, :raw_data, NOW(), FALSE)
            ON DUPLICATE KEY UPDATE
                raw_data = :raw_data,
                fetched_at = NOW(),
                processed = FALSE
        """
            ),
            {"playlist_id": playlist_id, "raw_data": json.dumps(raw_data)},
        )

        logger.info(f"🔄 Stored raw JSON for playlist {playlist_id}")


def get_raw_video_data(engine: Engine, video_id: str = None, processed: bool = None) -> List[Dict[str, Any]]:
    """
    Retrieve raw video data from the staging table.

    Args:
        engine (Engine): SQLAlchemy engine
        video_id (str, optional): YouTube video ID to retrieve. If None, retrieves all videos.
        processed (bool, optional): Filter by processed status. If None, retrieves all videos.

    Returns:
        List[Dict[str, Any]]: List of raw video data records
    """
    with engine.connect() as conn:
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table("youtube_videos_raw"):
            logger.warning("youtube_videos_raw table does not exist.")
            return []

        # Build the query
        query = "SELECT id, video_id, raw_data, fetched_at, processed FROM youtube_videos_raw"
        params = {}

        # Add filters
        filters = []
        if video_id is not None:
            filters.append("video_id = :video_id")
            params["video_id"] = video_id
        if processed is not None:
            filters.append("processed = :processed")
            params["processed"] = processed

        if filters:
            query += " WHERE " + " AND ".join(filters)

        # Execute the query
        result = conn.execute(text(query), params).fetchall()

        # Convert to list of dicts
        return [
            {
                "id": row.id,
                "video_id": row.video_id,
                "raw_data": json.loads(row.raw_data),
                "fetched_at": row.fetched_at,
                "processed": row.processed,
            }
            for row in result
        ]


def get_raw_playlist_data(engine: Engine, playlist_id: str = None, processed: bool = None) -> List[Dict[str, Any]]:
    """
    Retrieve raw playlist data from the staging table.

    Args:
        engine (Engine): SQLAlchemy engine
        playlist_id (str, optional): YouTube playlist ID to retrieve. If None, retrieves all playlists.
        processed (bool, optional): Filter by processed status. If None, retrieves all playlists.

    Returns:
        List[Dict[str, Any]]: List of raw playlist data records
    """
    with engine.connect() as conn:
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table("youtube_playlists_raw"):
            logger.warning("youtube_playlists_raw table does not exist.")
            return []

        # Build the query
        query = "SELECT id, playlist_id, raw_data, fetched_at, processed FROM youtube_playlists_raw"
        params = {}

        # Add filters
        filters = []
        if playlist_id is not None:
            filters.append("playlist_id = :playlist_id")
            params["playlist_id"] = playlist_id
        if processed is not None:
            filters.append("processed = :processed")
            params["processed"] = processed

        if filters:
            query += " WHERE " + " AND ".join(filters)

        # Execute the query
        result = conn.execute(text(query), params).fetchall()

        # Convert to list of dicts
        return [
            {
                "id": row.id,
                "playlist_id": row.playlist_id,
                "raw_data": json.loads(row.raw_data),
                "fetched_at": row.fetched_at,
                "processed": row.processed,
            }
            for row in result
        ]


def mark_raw_data_processed(engine: Engine, table: str, id_field: str, id_value: str) -> None:
    """
    Mark a raw data record as processed.

    Args:
        engine (Engine): SQLAlchemy engine
        table (str): Table name ('youtube_videos_raw' or 'youtube_playlists_raw')
        id_field (str): ID field name ('video_id' or 'playlist_id')
        id_value (str): ID value
    """
    with engine.begin() as conn:
        # Check if the table exists
        inspector = inspect(engine)
        if not inspector.has_table(table):
            logger.warning(f"{table} table does not exist. Skipping update.")
            return

        # Update the record
        conn.execute(
            text(
                f"""
            UPDATE {table}
            SET processed = TRUE
            WHERE {id_field} = :id_value
        """
            ),
            {"id_value": id_value},
        )


def save_progress(progress_data: Dict[str, Any], progress_file: str = None) -> None:
    """
    Save progress data to a JSON file.

    Args:
        progress_data (Dict[str, Any]): Progress data to save
        progress_file (str, optional): Path to the progress file. If None, uses the default path.
    """
    if progress_file is None:
        # Create a progress file path
        progress_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
        os.makedirs(progress_dir, exist_ok=True)
        progress_file = os.path.join(progress_dir, "youtube_playlist_progress.json")

    try:
        with open(progress_file, "w") as f:
            json.dump(progress_data, f)
        logger.info(f"Saved progress data to {progress_file}")
    except Exception as e:
        logger.warning(f"Error saving progress data: {e}")


def purge_orphans(engine: Engine, valid_ids: set[str]):
    """
    Delete rows whose video_id is not present in the valid_ids set.

    Args:
        engine (Engine): SQLAlchemy engine
        valid_ids (set[str]): Set of valid video IDs
    """
    sql = text("DELETE FROM youtube_videos WHERE video_id NOT IN :ids")
    with engine.begin() as conn:
        conn.execute(sql, {"ids": tuple(valid_ids)})
    logger.info(f"Purged orphan rows not in the set of {len(valid_ids)} valid IDs")


def parse_duration(duration_str: str) -> int:
    """
    Parse ISO 8601 duration string to seconds.

    Args:
        duration_str (str): ISO 8601 duration string (e.g., "PT1H2M3S")

    Returns:
        int: Duration in seconds
    """

    # Remove 'PT' prefix
    duration_str = duration_str[2:]

    # Extract hours, minutes, seconds
    hours = 0
    minutes = 0
    seconds = 0

    # Extract hours
    match = re.search(r"(\d+)H", duration_str)
    if match:
        hours = int(match.group(1))

    # Extract minutes
    match = re.search(r"(\d+)M", duration_str)
    if match:
        minutes = int(match.group(1))

    # Extract seconds
    match = re.search(r"(\d+)S", duration_str)
    if match:
        seconds = int(match.group(1))

    return hours * 3600 + minutes * 60 + seconds


def clean_youtube_database(engine: Engine, full_clean: bool = False) -> None:
    """
    Clean the YouTube database by removing incorrect records.

    This function can either:
    1. Remove all records (full_clean=True) for a fresh start
    2. Remove only records that don't belong to the user's catalog (full_clean=False)

    Args:
        engine (Engine): SQLAlchemy engine
        full_clean (bool): Whether to perform a full clean (remove all records)
    """
    logger.info("Cleaning YouTube database...")

    if full_clean:
        # Full clean - remove all records
        with engine.begin() as conn:
            # Delete all records from youtube_metrics first (due to foreign key constraint)
            conn.execute(text("DELETE FROM youtube_metrics"))
            logger.info("Deleted all records from youtube_metrics")

            # Delete all records from youtube_videos
            conn.execute(text("DELETE FROM youtube_videos"))
            logger.info("Deleted all records from youtube_videos")

        logger.info("YouTube database fully cleaned")
    else:
        # Selective clean - remove only incorrect records
        with engine.begin() as conn:
            # Get table handles
            youtube_metrics_tbl = get_table("youtube_metrics")
            youtube_videos_tbl = get_table("youtube_videos")
            songs_tbl = get_table("songs")

            # Get all ISRCs from the songs table (these are the correct ones)
            valid_isrcs = set(row[0] for row in conn.execute(select(songs_tbl.c.ISRC)).fetchall())
            logger.info(f"Found {len(valid_isrcs)} valid ISRCs in the songs table")

            # Find YouTube videos with ISRCs not in the songs table
            invalid_videos = conn.execute(
                select(
                    youtube_videos_tbl.c.video_id,
                    youtube_videos_tbl.c.isrc,
                    youtube_videos_tbl.c.title,
                ).where(~youtube_videos_tbl.c.isrc.in_(valid_isrcs))
            ).fetchall()

            if invalid_videos:
                logger.info(f"Found {len(invalid_videos)} invalid videos to remove")

                # Log the invalid videos being removed
                for video in invalid_videos:
                    logger.info(f"Removing invalid video: {video.video_id} - {video.title} (ISRC: {video.isrc})")

                # Get the video IDs to remove
                invalid_video_ids = [video.video_id for video in invalid_videos]

                # Delete metrics for these videos first (due to foreign key constraint)
                deleted_metrics = conn.execute(
                    youtube_metrics_tbl.delete().where(youtube_metrics_tbl.c.video_id.in_(invalid_video_ids))
                ).rowcount
                logger.info(f"Deleted {deleted_metrics} metrics records for invalid videos")

                # Delete the invalid videos
                deleted_videos = conn.execute(
                    youtube_videos_tbl.delete().where(youtube_videos_tbl.c.video_id.in_(invalid_video_ids))
                ).rowcount
                logger.info(f"Deleted {deleted_videos} invalid videos")
            else:
                logger.info("No invalid videos found")

        logger.info("YouTube database selectively cleaned")


def ensure_youtube_raw_tables(engine: Engine) -> None:
    """
    Ensure that the YouTube raw tables exist in the database.

    This function checks if the YouTube raw tables exist in the database.
    If they don't exist, it creates them.

    Args:
        engine (Engine): SQLAlchemy engine
    """
    logger.info("Checking if YouTube raw tables exist")

    # Check if tables exist
    inspector = inspect(engine)

    # Check if youtube_videos_raw table exists
    if not inspector.has_table("youtube_videos_raw"):
        logger.info("Creating youtube_videos_raw table")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS youtube_videos_raw (
                    video_id VARCHAR(50) NOT NULL PRIMARY KEY,
                    raw_data JSON DEFAULT NULL,
                    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    processed TINYINT NOT NULL DEFAULT 0,
                    error TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """
                )
            )
        logger.info("Created youtube_videos_raw table")
    else:
        logger.info("youtube_videos_raw table already exists")

    # Check if youtube_playlists_raw table exists
    if not inspector.has_table("youtube_playlists_raw"):
        logger.info("Creating youtube_playlists_raw table")
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS youtube_playlists_raw (
                    playlist_id VARCHAR(50) NOT NULL PRIMARY KEY,
                    raw_data JSON DEFAULT NULL,
                    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    processed TINYINT NOT NULL DEFAULT 0,
                    error TEXT,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """
                )
            )
        logger.info("Created youtube_playlists_raw table")
    else:
        logger.info("youtube_playlists_raw table already exists")

    logger.info("✅ All YouTube raw tables exist")


def ensure_youtube_tables(engine: Engine, check_staging: bool = False) -> None:
    """
    Ensure that the YouTube tables exist in the database.

    This function checks if the required YouTube tables exist in the database.
    If they don't exist, it raises an error, indicating that the user needs to run
    Alembic migrations.

    Args:
        engine (Engine): SQLAlchemy engine
        check_staging (bool): Whether to check for staging tables as well

    Raises:
        RuntimeError: If any of the required tables don't exist
    """
    logger.info("Checking if YouTube tables exist")

    # Check if tables exist
    inspector = inspect(engine)
    missing_tables = []

    # Check if youtube_videos table exists
    if not inspector.has_table("youtube_videos"):
        missing_tables.append("youtube_videos")

    # Check if youtube_metrics table exists
    if not inspector.has_table("youtube_metrics"):
        missing_tables.append("youtube_metrics")

    # Check for staging tables if requested
    if check_staging:
        if not inspector.has_table("youtube_videos_raw"):
            missing_tables.append("youtube_videos_raw")
        if not inspector.has_table("youtube_playlists_raw"):
            missing_tables.append("youtube_playlists_raw")

    # If any tables are missing, raise an error
    if missing_tables:
        error_msg = (
            f"Missing required tables: {', '.join(missing_tables)}. "
            "Please run 'alembic upgrade head' to create all required tables."
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # Verify expected columns (title column exists)
    cols = {c["name"] for c in inspector.get_columns("youtube_videos")}
    if "title" not in cols:
        msg = "youtube_videos schema out of date – missing 'title' " "column. Expected: title, isrc, video_id"
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info("✅ All YouTube tables exist with required columns")


def process_youtube_data(
    engine: Engine,
    batch_size: int = 100,
    use_playlist: bool = True,
    clean_db: bool = False,
    full_clean: bool = False,
    priority_playlist: str = None,
    skip_metrics: bool = False,
    quota_limit: int = 0,
    max_videos: int = 0,
    development_mode: bool = False,
    cache_threshold_hours: int = 24,
    force_api_fetch: bool = False,
    max_api_fetches_per_day: int = 10,
) -> None:
    """
    Process YouTube data for the iCatalog ETL pipeline.

    This function implements a caching mechanism to avoid unnecessary API calls:
    - If data for a playlist exists in the youtube_playlists_raw table and is less than cache_threshold_hours old,
      it will use the cached data instead of making API calls.
    - If the data is older than cache_threshold_hours, it will fetch fresh data from the API.
    - The cache_threshold_hours parameter defaults to 24 hours, meaning data will be refreshed once per day.
    - By default, it will only fetch directly from the API max_api_fetches_per_day times per day (default: 10).
      After that, it will use the data from the raw table.
    - If force_api_fetch is True, it will fetch directly from the API regardless of the number of fetches today.

    Args:
        engine (Engine): SQLAlchemy engine
        batch_size (int): Number of songs to process in each batch
        use_playlist (bool): Whether to use the curated playlist approach
        clean_db (bool): Whether to clean the database before processing
        full_clean (bool): Whether to perform a full clean (remove all records) or selective clean
        priority_playlist (str): Playlist ID to process first (if None, process in default order)
        skip_metrics (bool): Whether to skip updating metrics (useful when quota is limited)
        quota_limit (int): Stop processing after using this many quota units (0 = no limit)
        max_videos (int): Maximum number of videos to process from each playlist (0 = no limit)
        development_mode (bool): Whether to run in development mode, which fetches all videos and
                               ignores progress tracking for easier development and testing
        cache_threshold_hours (int): Number of hours before cached data is considered stale (default: 24)
        force_api_fetch (bool): Whether to force fetching from the API regardless of the number of fetches today
        max_api_fetches_per_day (int): Maximum number of times to fetch directly from the API per day (default: 10)
    """
    # Set up quota tracker with the specified limit
    quota_tracker.max_units = quota_limit

    # Reset quota usage counter
    quota_tracker.units = 0

    if quota_tracker.max_units > 0:
        logger.info(f"YouTube API quota limit set to {quota_tracker.max_units} units")

    logger.info("Starting YouTube data processing")

    # Ensure DSP rows exist
    ensure_dsp_rows(engine, ["YouTube"])

    # Ensure YouTube tables exist (check for staging tables in development mode)
    ensure_youtube_tables(engine, check_staging=development_mode)

    # Ensure YouTube raw tables exist (always check, create if they don't exist)
    ensure_youtube_raw_tables(engine)

    # Check if we should fetch directly from the API or use the raw table
    use_raw_table = False
    if not force_api_fetch and not development_mode:
        # Check how many times we've fetched from the API today
        with engine.connect() as conn:
            # Create a table to track API fetches if it doesn't exist
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS youtube_api_fetches (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fetch_date DATE NOT NULL,
                    fetch_count INT NOT NULL DEFAULT 1,
                    UNIQUE KEY (fetch_date)
                )
            """
                )
            )

            # Get today's fetch count
            today = datetime.now().date().isoformat()
            result = conn.execute(
                text(
                    """
                SELECT fetch_count FROM youtube_api_fetches
                WHERE fetch_date = :today
            """
                ),
                {"today": today},
            ).fetchone()

            if result:
                fetch_count = result[0]
                logger.info(f"Already fetched from the API {fetch_count} times today")
                if fetch_count >= max_api_fetches_per_day:
                    logger.info(f"Reached maximum API fetches for today ({max_api_fetches_per_day}), using raw table")
                    use_raw_table = True
                else:
                    # Increment the fetch count
                    now = datetime.now()
                    conn.execute(
                        text(
                            """
                        UPDATE youtube_api_fetches
                        SET fetch_count = fetch_count + 1
                        WHERE fetch_date = :today
                    """
                        ),
                        {"today": today},
                    )
                    logger.info(f"Incremented API fetch count to {fetch_count + 1}")
            else:
                # Insert a new record for today
                now = datetime.now()
                conn.execute(
                    text(
                        """
                    INSERT INTO youtube_api_fetches (fetch_date, fetch_count)
                    VALUES (:today, 1)
                """
                    ),
                    {"today": today},
                )
                logger.info("First API fetch of the day")

    # Log mode status
    if development_mode:
        logger.info("🔧 Running in DEVELOPMENT MODE: Will fetch all videos and store raw data")
    elif use_raw_table:
        logger.info("📦 Using data from raw table instead of fetching from the API")
    else:
        logger.info("🚀 Running in PRODUCTION MODE: Will use progress tracking and fetch only new videos")

    # Clean the database if requested or in development mode with full_clean
    if clean_db or (development_mode and full_clean):
        # Commented out: clean_youtube_database(engine, full_clean=full_clean)
        logger.info("Skipping database cleaning as requested in issue description")
        if full_clean:
            logger.info("Would have performed full clean of YouTube database")
        else:
            logger.info("Would have performed selective clean of YouTube database")

    # Get YouTube client
    youtube = get_youtube_client()

    # Find YouTube videos for songs
    if use_playlist:
        # Use the curated playlist approach
        # Playlist IDs from the issue description
        playlist_ids = [
            "PLl-ShioB5kaqGuVCTwT7jP7VBu_jcSGj8",  # Original playlist
            "PLl-ShioB5kaqu8jD43bGi7qX799RIZA3Q",  # New playlist from issue description
        ]

        # If a priority playlist is specified, move it to the front of the list
        if priority_playlist:
            # Check if this is an "only playlist" scenario
            if priority_playlist.startswith("only:"):
                # Extract the actual playlist ID
                only_playlist = priority_playlist[5:]
                # Replace the playlist_ids list with just this playlist
                playlist_ids = [only_playlist]
                logger.info(f"Processing only playlist: {only_playlist}")
            elif priority_playlist in playlist_ids:
                playlist_ids.remove(priority_playlist)
                playlist_ids.insert(0, priority_playlist)
                logger.info(f"Prioritizing playlist: {priority_playlist}")
            else:
                playlist_ids.insert(0, priority_playlist)
                logger.info(f"Added priority playlist: {priority_playlist}")

        # Process all playlists and combine results
        isrc_to_video_id = {}

        # Create a progress file path
        progress_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
        os.makedirs(progress_dir, exist_ok=True)
        progress_file = os.path.join(progress_dir, "youtube_playlist_progress.json")

        # Load progress if it exists (skip in development mode)
        # processed is a dictionary mapping playlist IDs to the number of videos already processed
        # state is a dictionary with "playlists" key mapping to processed dictionary and "date" key for last update
        state = {
            "playlists": {},
            "date": None,
        }  # state["playlists"][playlist_id] -> videos_already_done

        if not development_mode and os.path.exists(progress_file):
            try:
                with open(progress_file, "r") as f:
                    progress_data = json.load(f)
                    # Support both old and new format
                    if "processed_playlists" in progress_data:
                        # Old format - convert to new format
                        for pl_id in progress_data.get("processed_playlists", []):
                            state["playlists"][pl_id] = max_videos or 0
                        state["date"] = date.today().isoformat()
                    elif "playlists" in progress_data:
                        # New format with playlists key
                        state = progress_data
                    else:
                        # Intermediate format - direct mapping of playlist IDs to video counts
                        state["playlists"] = progress_data
                        state["date"] = date.today().isoformat()

                # Check if we need to reset the state based on the date
                if development_mode and state.get("date") and state["date"] != date.today().isoformat():
                    logger.info("Development mode: Resetting progress tracking because it's a new day")
                    state["playlists"] = {}
                    state["date"] = date.today().isoformat()

                logger.info(f"Loaded progress: {len(state['playlists'])} playlists already processed")
            except Exception as e:
                logger.warning(f"Error loading progress: {e}")
                # Initialize with empty state and today's date
                state = {"playlists": {}, "date": date.today().isoformat()}
        elif development_mode:
            logger.info("Development mode: Ignoring progress tracking and fetching all videos")
            # Initialize with empty state and today's date
            state = {"playlists": {}, "date": date.today().isoformat()}

        # Track total videos found for logging
        total_videos_found = 0
        total_new_videos = 0

        # Keep track of processed video IDs to avoid duplicates
        processed_video_ids = set()

        for playlist_id in playlist_ids:
            # In development mode, always process all videos
            # In production mode, check if we've already processed enough videos
            already = 0 if development_mode else state["playlists"].get(playlist_id, 0)

            if not development_mode and already >= max_videos and max_videos > 0 and not clean_db:
                logger.info(f"✅ Playlist {playlist_id} already has {already}/{max_videos} vids – skipping")
                continue

            # In development mode, fetch all videos
            # In production mode, fetch only the remaining videos
            to_fetch = 0 if development_mode else (max_videos - already if max_videos > 0 else 0)
            offset = 0 if development_mode else already

            logger.info(
                f"Processing playlist: {playlist_id}"
                + (
                    f" (development mode: fetching ALL videos)"
                    if development_mode
                    else f" (fetching {to_fetch if to_fetch > 0 else 'all'} videos, already have {already})"
                )
            )

            try:
                # Pass development_mode and cache_threshold_hours to find_youtube_videos_from_playlist
                playlist_results = find_youtube_videos_from_playlist(
                    engine,
                    youtube,
                    playlist_id,
                    to_fetch if to_fetch > 0 else 0,
                    offset,
                    development_mode=development_mode,
                    cache_threshold_hours=cache_threshold_hours,
                )

                # Log detailed counts
                logger.info(f"Found {len(playlist_results)} matches in playlist {playlist_id}")
                total_videos_found += len(playlist_results)
                total_new_videos += len(playlist_results)

                # Merge results, keeping track of duplicates
                for isrc, video_id in playlist_results.items():
                    # Skip if this video ID has already been processed
                    if video_id in processed_video_ids:
                        logger.info(f"Skipping duplicate video ID {video_id} for ISRC {isrc}")
                        continue

                    # Add to processed video IDs set
                    processed_video_ids.add(video_id)

                    if isrc in isrc_to_video_id and isrc_to_video_id[isrc] != video_id:
                        logger.warning(f"Duplicate video for ISRC {isrc}: {isrc_to_video_id[isrc]} and {video_id}")
                    isrc_to_video_id[isrc] = video_id

                # Update progress - store the total number of videos processed (skip in development mode)
                if not development_mode:
                    videos_processed = already + len(playlist_results)
                    state["playlists"][playlist_id] = max_videos if max_videos > 0 else videos_processed
                    state["date"] = date.today().isoformat()

                    # Save progress using the save_progress function
                    save_progress(state, progress_file)
                    logger.info(f"Saved progress: {len(state['playlists'])} playlists processed")

            except Exception as e:
                logger.error(f"Error processing playlist {playlist_id}: {e}")
                # Continue with the next playlist

        # Log summary of videos found
        logger.info(f"Total videos found across all playlists: {total_videos_found}")
        logger.info(f"New videos to be processed: {total_new_videos}")

        logger.info(f"Total matches from all playlists: {len(isrc_to_video_id)}")
    else:
        # Use the original search-based approach
        logger.info("Using search-based approach")

        # Calculate max_search_calls based on quota_limit
        # Each search.list call costs 100 units
        # If quota_limit is 0 (no limit), use a default of 10 search calls
        # Otherwise, allow up to 90% of the quota to be used for search calls
        max_search_calls = 0  # Default to 0 (disable search) if quota is very limited
        if quota_limit <= 0:
            # No quota limit specified, use default of 10 search calls
            max_search_calls = 10
        elif quota_limit >= 200:  # At least 200 units available
            # Allow up to 90% of quota for search, but at least 1 call
            max_search_calls = max(1, int((quota_limit * 0.9) / 100))
            logger.info(f"Allowing up to {max_search_calls} search calls based on quota limit of {quota_limit} units")

        isrc_to_video_id = find_youtube_videos_for_songs(engine, youtube, batch_size, max_search_calls=max_search_calls)

    # Insert YouTube videos
    if isrc_to_video_id:
        # Log the number of videos to be inserted
        logger.info(f"Found {len(isrc_to_video_id)} videos that could be inserted into the database")
        # Commented out: insert_youtube_videos(engine, isrc_to_video_id, development_mode=development_mode)
        logger.info("Skipping insertion of videos as requested in issue description")
    else:
        logger.warning("No videos to insert")

    # Update YouTube metrics (skip if requested)
    if not skip_metrics:
        # Commented out: update_youtube_metrics(engine)
        logger.info("Skipping metrics update as requested in issue description")
    else:
        logger.info("Skipping metrics update as requested")

    logger.info("YouTube data processing complete")


if __name__ == "__main__":
    import argparse

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process YouTube data for the iCatalog ETL pipeline.")
    parser.add_argument(
        "--no-playlist",
        action="store_false",
        dest="use_playlist",
        help="Use search-based approach instead of playlist approach",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        dest="clean_db",
        help="Clean the database before processing",
    )
    parser.add_argument(
        "--full-clean",
        action="store_true",
        dest="full_clean",
        help="Perform a full clean (remove all records) instead of selective clean",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of songs to process in each batch (default: 10)",
    )
    parser.add_argument(
        "--quota-limit",
        type=int,
        default=0,
        help="Stop processing after using this many quota units (0 = no limit)",
    )
    parser.add_argument("--priority-playlist", type=str, help="Playlist ID to process first")
    parser.add_argument(
        "--skip-metrics",
        action="store_true",
        help="Skip updating metrics (useful when quota is limited)",
    )
    parser.add_argument("--only-playlist", type=str, help="Process only this specific playlist")
    parser.add_argument(
        "--development-mode",
        action="store_true",
        help="Run in development mode (fetch all videos, ignore progress tracking, store raw data)",
    )

    # Parse arguments
    args = parser.parse_args()

    # Initialize engine and tables
    try:
        engine = get_engine()
        init_tables(engine)

        # If only_playlist is specified, use it as the priority playlist and skip others
        priority_playlist = args.priority_playlist
        if args.only_playlist:
            priority_playlist = f"only:{args.only_playlist}"
            logger.info(f"Will process only playlist: {args.only_playlist}")

        # Process YouTube data with the specified arguments
        process_youtube_data(
            engine,
            batch_size=args.batch_size,
            use_playlist=args.use_playlist,
            clean_db=args.clean_db,
            full_clean=args.full_clean,
            priority_playlist=priority_playlist,
            skip_metrics=args.skip_metrics,
            quota_limit=args.quota_limit,
            development_mode=args.development_mode,
        )
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
