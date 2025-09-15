#!/usr/bin/env python3
from __future__ import annotations

import os
from urllib.parse import urlparse

from web.etl_helpers import (
    batch_upsert_song_version,
    flush_song_versions_buffer,
)
from web.etl_helpers import get_engine as get_sa_engine
from web.spotify_extract import fetch_playlist_json as fetch_spotify
from web.tidal_extract import fetch_playlist_json as fetch_tidal


def _tidal_playlist_id(val: str) -> str:
    # Accept full URL or raw ID
    if "://" in val:
        p = urlparse(val)
        return p.path.rstrip("/").split("/")[-1]
    return val


def main() -> None:
    processed = 0
    # Engine used for upserts
    engine = get_sa_engine()

    # Spotify
    sp_pl = os.getenv("SPOTIFY_PLAYLIST_ID")
    if sp_pl:
        try:
            data, from_cache = fetch_spotify(sp_pl)
            for t in data.get("tracks", []):
                isrc = t.get("isrc")
                if not isrc:
                    continue
                batch_upsert_song_version(
                    isrc=isrc,
                    dsp_name="Spotify",
                    dsp_record_id=str(t.get("id")),
                    track_title=t.get("name") or "",
                    album_title=t.get("album") or None,
                )
                processed += 1
        except Exception as e:  # noqa: BLE001
            print(f"Spotify refresh failed: {e}")

    # Tidal
    td_pl = os.getenv("TIDAL_PLAYLIST_URL")
    if td_pl:
        try:
            pid = _tidal_playlist_id(td_pl)
            data, from_cache = fetch_tidal(pid)
            for t in data.get("tracks", []):
                isrc = t.get("isrc")
                if not isrc:
                    continue
                batch_upsert_song_version(
                    isrc=isrc,
                    dsp_name="Tidal",
                    dsp_record_id=str(t.get("id")),
                    track_title=t.get("name") or "",
                    album_title=t.get("album") or None,
                )
                processed += 1
        except Exception as e:  # noqa: BLE001
            print(f"Tidal refresh failed: {e}")

    n = flush_song_versions_buffer(engine)
    print({"tracked_versions": processed, "upserts": n})


if __name__ == "__main__":
    main()
