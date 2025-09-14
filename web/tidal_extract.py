from __future__ import annotations

import os
from typing import Any, Dict, Tuple

from .dsp_cache import read_daily_json, write_daily_json

# Freshness flags exported for observability
FRESH_TIDAL_PULL: bool = False


def fetch_playlist_json(playlist_id: str, *, force_fresh: bool | None = None) -> Tuple[Dict[str, Any], bool]:
    """Fetch Tidal playlist JSON with a 1-day on-disk cache.

    Returns (data, used_cache). Updates module/global env flag FRESH_TIDAL_PULL.

    Environment overrides:
    - TIDAL_FORCE_FRESH=1 forces API pull (skip cache)
    - ICATALOG_CACHE_DIR overrides cache base directory
    """
    global FRESH_TIDAL_PULL
    if force_fresh is None:
        force_fresh = os.getenv("TIDAL_FORCE_FRESH", "0").strip() in {"1", "true", "TRUE", "yes"}

    if not force_fresh:
        cached, fresh = read_daily_json("tidal_playlists", playlist_id)
        if fresh and isinstance(cached, dict):
            FRESH_TIDAL_PULL = False
            os.environ["FRESH_TIDAL_PULL"] = "0"
            return cached, True

    # Fetch via API — no fallbacks; raise if tidalapi not available
    try:
        import tidalapi  # noqa: F401
    except Exception as e:  # noqa: BLE001
        raise RuntimeError("tidalapi not installed; cannot fetch playlist") from e

    from .tidal_auth import get_tidal_session  # type: ignore

    session = get_tidal_session()
    # Minimal, explicit API use; avoid cleverness
    playlist = session.get_playlist(playlist_id)
    items = [t for t in playlist.tracks()]  # list of Track objects
    # Serialize to a compact JSON structure we control
    data: Dict[str, Any] = {
        "playlist_id": playlist_id,
        "name": getattr(playlist, "name", None),
        "owner": getattr(playlist, "creator", None),
        "track_count": len(items),
        "tracks": [
            {
                "id": getattr(t, "id", None),
                "name": getattr(t, "name", None),
                "isrc": getattr(t, "isrc", None),
                "album": getattr(getattr(t, "album", None), "title", None),
                "artist": getattr(getattr(t, "artist", None), "name", None),
                "duration": getattr(t, "duration", None),
            }
            for t in items
        ],
    }

    write_daily_json("tidal_playlists", playlist_id, data)
    FRESH_TIDAL_PULL = True
    os.environ["FRESH_TIDAL_PULL"] = "1"
    return data, False
