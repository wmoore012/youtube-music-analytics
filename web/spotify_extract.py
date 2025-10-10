from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

from .dsp_cache import read_daily_json, write_daily_json

FRESH_SPOTIFY_PULL: bool = False


def _check_cache_for_playlist(playlist_id: str, force_fresh: bool) -> Tuple[Dict[str, Any] | None, bool]:
    """Check cache for existing playlist data."""
    global FRESH_SPOTIFY_PULL

    if not force_fresh:
        cached, fresh = read_daily_json("spotify_playlists", playlist_id)
        if fresh and isinstance(cached, dict):
            FRESH_SPOTIFY_PULL = False
            os.environ["FRESH_SPOTIFY_PULL"] = "0"
            return cached, True

    return None, False


def _create_spotify_client():
    """Create and configure Spotify API client."""
    try:
        import spotipy  # noqa: F401
        from spotipy import Spotify
        from spotipy.oauth2 import SpotifyClientCredentials
    except Exception as e:  # noqa: BLE001
        raise RuntimeError("spotipy not installed; cannot fetch playlist") from e

    return Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.getenv("SPOTIPY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIPY_CLIENT_SECRET"),
        )
    )


def _extract_track_data(playlist_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract and normalize track data from Spotify playlist response."""
    items = playlist_response.get("tracks", {}).get("items", [])
    tracks = []

    for item in items:
        track = item.get("track") or {}
        album = track.get("album") or {}
        artists = track.get("artists") or []

        tracks.append(
            {
                "id": track.get("id"),
                "name": track.get("name"),
                "isrc": (track.get("external_ids") or {}).get("isrc"),
                "album": album.get("name"),
                "artist": (artists[0]["name"] if artists else None),
                "duration_ms": track.get("duration_ms"),
            }
        )

    return tracks


def fetch_playlist_json(playlist_id: str, *, force_fresh: bool | None = None) -> Tuple[Dict[str, Any], bool]:
    """Fetch Spotify playlist JSON with a 1-day on-disk cache.

    Returns (data, used_cache).

    Environment overrides:
    - SPOTIFY_FORCE_FRESH=1 forces API pull
    - ICATALOG_CACHE_DIR overrides cache base directory
    """
    global FRESH_SPOTIFY_PULL

    # Determine if we should force fresh data
    if force_fresh is None:
        force_fresh = os.getenv("SPOTIFY_FORCE_FRESH", "0").strip() in {"1", "true", "TRUE", "yes"}

    # Check cache first
    cached_data, used_cache = _check_cache_for_playlist(playlist_id, force_fresh)
    if used_cache:
        return cached_data, True

    # Fetch fresh data from Spotify API
    client = _create_spotify_client()
    response = client.playlist(playlist_id)
    tracks = _extract_track_data(response)

    # Build final data structure
    data: Dict[str, Any] = {
        "playlist_id": playlist_id,
        "name": response.get("name"),
        "owner": (response.get("owner") or {}).get("display_name"),
        "track_count": len(tracks),
        "tracks": tracks,
    }

    # Cache the results
    write_daily_json("spotify_playlists", playlist_id, data)
    FRESH_SPOTIFY_PULL = True
    os.environ["FRESH_SPOTIFY_PULL"] = "1"
    return data, False
