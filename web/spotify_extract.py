from __future__ import annotations

import os
from typing import Any, Dict, Tuple

from .dsp_cache import read_daily_json, write_daily_json

FRESH_SPOTIFY_PULL: bool = False


def fetch_playlist_json(playlist_id: str, *, force_fresh: bool | None = None) -> Tuple[Dict[str, Any], bool]:
    """Fetch Spotify playlist JSON with a 1-day on-disk cache.

    Returns (data, used_cache).

    Environment overrides:
    - SPOTIFY_FORCE_FRESH=1 forces API pull
    - ICATALOG_CACHE_DIR overrides cache base directory
    """
    global FRESH_SPOTIFY_PULL
    if force_fresh is None:
        force_fresh = os.getenv("SPOTIFY_FORCE_FRESH", "0").strip() in {"1", "true", "TRUE", "yes"}

    if not force_fresh:
        cached, fresh = read_daily_json("spotify_playlists", playlist_id)
        if fresh and isinstance(cached, dict):
            FRESH_SPOTIFY_PULL = False
            os.environ["FRESH_SPOTIFY_PULL"] = "0"
            return cached, True

    try:
        import spotipy  # noqa: F401
        from spotipy import Spotify
        from spotipy.oauth2 import SpotifyClientCredentials
    except Exception as e:  # noqa: BLE001
        raise RuntimeError("spotipy not installed; cannot fetch playlist") from e

    client = Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=os.getenv("SPOTIPY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIPY_CLIENT_SECRET"),
        )
    )

    resp = client.playlist(playlist_id)
    items = resp.get("tracks", {}).get("items", [])
    tracks = []
    for it in items:
        t = it.get("track") or {}
        album = t.get("album") or {}
        artists = t.get("artists") or []
        tracks.append(
            {
                "id": t.get("id"),
                "name": t.get("name"),
                "isrc": (t.get("external_ids") or {}).get("isrc"),
                "album": album.get("name"),
                "artist": (artists[0]["name"] if artists else None),
                "duration_ms": t.get("duration_ms"),
            }
        )

    data: Dict[str, Any] = {
        "playlist_id": playlist_id,
        "name": resp.get("name"),
        "owner": (resp.get("owner") or {}).get("display_name"),
        "track_count": len(tracks),
        "tracks": tracks,
    }

    write_daily_json("spotify_playlists", playlist_id, data)
    FRESH_SPOTIFY_PULL = True
    os.environ["FRESH_SPOTIFY_PULL"] = "1"
    return data, False
