"""Transform helpers (public namespace)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List

import web.etl_helpers as etl_helpers

# Wrappers so tests can patch via this module path

def get_engine():
    return etl_helpers.get_engine()

def init_tables(engine):
    return etl_helpers.init_tables(engine)

def _assert_tables_exist(engine):
    return etl_helpers._assert_tables_exist(engine)

def get_connection():
    return etl_helpers.get_connection()

def normalize_spotify_track(payload):
    return etl_helpers.normalize_spotify_track(payload)

def normalize_tidal(payload):
    return etl_helpers.normalize_tidal(payload)


@dataclass
class TransformConfig:
    enable_spotify_normalization: bool = True
    enable_tidal_normalization: bool = True
    enable_youtube_normalization: bool = True
    enable_artist_role_extraction: bool = True
    enable_version_detection: bool = True
    batch_size: int = 50


class CompleteTransformer:
    def __init__(self, config: TransformConfig):
        self.config = config
        # Lazy init: do not touch the database in the constructor. Tests should
        # explicitly call _init_database() or patch get_engine/init_tables as needed.
        self.engine = None

    def _init_database(self):
        # Initialize engine and ensure tables exist
        self.engine = get_engine()
        init_tables(self.engine)
        _assert_tables_exist(self.engine)

    def transform_spotify_data(self, raw_tracks: List[Dict]) -> Dict[str, int]:
        success = error = 0
        with get_connection() as conn:
            for item in raw_tracks:
                try:
                    raw = item.get("raw_data")
                    if not raw:
                        raise ValueError("Missing raw data")
                    payload = json.loads(raw)
                    normalize_spotify_track(payload)
                    success += 1
                except Exception:
                    error += 1
        return {"success": success, "error": error, "total": len(raw_tracks)}

    def transform_tidal_data(self, raw_tracks: List[Dict]) -> Dict[str, int]:
        success = error = 0
        with get_connection() as conn:
            for item in raw_tracks:
                try:
                    raw = item.get("raw_data")
                    payload = json.loads(raw) if raw else {}
                    normalize_tidal(payload)
                    success += 1
                except Exception:
                    error += 1
        return {"success": success, "error": error, "total": len(raw_tracks)}

    def transform_youtube_data(self, raw_videos: List[Dict]) -> Dict[str, int]:
        success = error = 0
        for item in raw_videos:
            try:
                raw = item.get("raw_data")
                json.loads(raw)
                success += 1
            except Exception:
                error += 1
        return {"success": success, "error": error, "total": len(raw_videos)}

    def transform_all_data(self, raw: Dict[str, List[Dict]]) -> Dict[str, Dict[str, int]]:  # pragma: no cover (mocked in tests)
        return {
            "youtube": self.transform_youtube_data(raw.get("youtube", [])),
            "spotify": self.transform_spotify_data(raw.get("spotify", [])),
            "tidal": self.transform_tidal_data(raw.get("tidal", [])),
            "artist_roles": {"success": 0, "error": 0, "total": 0},
        }

