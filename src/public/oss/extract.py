"""Extraction helpers (public namespace) used by tests.

Lightweight extraction helpers for the public namespace.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ExtractConfig:
    youtube_playlists: List[Optional[str]] = field(default_factory=list)
    spotify_playlists: List[Optional[str]] = field(default_factory=list)
    tidal_playlists: List[Optional[str]] = field(default_factory=list)
    max_retries: int = 3
    retry_delay: int = 5


class YouTubeExtractor:
    def __init__(self, api_key: str):
        from googleapiclient.discovery import build  # patched in tests

        self.api_key = api_key
        self.youtube = build("youtube", "v3", developerKey=api_key)

    def extract_playlist(self, playlist_id: str):  # pragma: no cover (mocked in tests)
        raise NotImplementedError


class SpotifyExtractor:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret


class TidalExtractor:
    def __init__(self, access_token: str):
        self.access_token = access_token


class ExtractOrchestrator:
    def __init__(self, config: ExtractConfig):
        self.config = config
        self.youtube_extractor = None
        self.spotify_extractor = None
        self.tidal_extractor = None

        # Lazily initialize based on env presence and configured playlists
        if os.getenv("YOUTUBE_API_KEY") and any(self.config.youtube_playlists):
            self.youtube_extractor = YouTubeExtractor(os.environ["YOUTUBE_API_KEY"])
        if os.getenv("SPOTIFY_CLIENT_ID") and os.getenv("SPOTIFY_CLIENT_SECRET") and any(
            self.config.spotify_playlists
        ):
            self.spotify_extractor = SpotifyExtractor(
                os.environ["SPOTIFY_CLIENT_ID"], os.environ["SPOTIFY_CLIENT_SECRET"]
            )
        if os.getenv("TIDAL_ACCESS_TOKEN") and any(self.config.tidal_playlists):
            self.tidal_extractor = TidalExtractor(os.environ["TIDAL_ACCESS_TOKEN"])

    def extract_all_data(self) -> Dict[str, List[Dict]]:
        return {"youtube": [], "spotify": [], "tidal": []}

