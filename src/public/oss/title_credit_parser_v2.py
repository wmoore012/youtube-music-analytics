"""Title/Credit parsing helpers (vendor-neutral) used by tests."""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

_FEAT_PAT = re.compile(r"\((?:feat\.|featuring|ft\.)\s*([^)]*)\)", re.I)
_NOISE_PAT = re.compile(r"\((?:official(?: music)? (?:video|audio)|visualizer)\)", re.I)
_LIVE_VARIANTS = re.compile(r"\((live(?: version)?)\)", re.I)
_REMIX_PAT = re.compile(r"\((?:remix)\)", re.I)
_ACOUSTIC_PAT = re.compile(r"\((?:acoustic)\)", re.I)
_SLOWED_REVERB_PAT = re.compile(
    r"\((?=.*slowed)(?=.*reverb)[^)]*\)", re.I
)


def split_artists_from_title(s: str) -> Tuple[List[str], str]:
    """Split a string of the form "Artist A & Artist B - Title".

    Returns (artists_list, title). If no separator present, artists=[], title=s.
    """
    if "-" in s:
        left, right = s.split("-", 1)
        # split artists by "," or "&"
        parts = [p.strip() for p in re.split(r",\s*|\s*&\s*", left) if p.strip()]
        return parts, right.strip()
    return [], s.strip()


def parse_title_and_credits(title: str, *, normalize_youtube_noise: bool = True) -> Dict[str, object]:
    """Extract features and version tokens from a YouTube-like title string.

    - Removes YouTube noise like (Official Video) by default or when flag is True
    - Extracts (feat. ...) artists
    - Normalizes version tags: Remix, Live/Live Version, Slowed and Reverbed
    """
    if not isinstance(title, str):
        raise ValueError("Title must be string")

    base_title = title
    version = "Original"
    features: List[str] = []

    # features
    m = _FEAT_PAT.search(base_title)
    if m:
        raw = m.group(1)
        parts = re.split(r"\s*&\s*|,\s*", raw)
        features = [p.strip() for p in parts if p.strip()]
        base_title = _FEAT_PAT.sub("", base_title)

    # version: slowed + reverb family first
    if _SLOWED_REVERB_PAT.search(base_title):
        version = "Slowed and Reverbed"
        base_title = _SLOWED_REVERB_PAT.sub("", base_title)

    # remix
    if _REMIX_PAT.search(base_title):
        version = "Remix"
        base_title = _REMIX_PAT.sub("", base_title)

    # acoustic
    if _ACOUSTIC_PAT.search(base_title):
        version = "Acoustic"
        base_title = _ACOUSTIC_PAT.sub("", base_title)

    # live/live version
    m2 = _LIVE_VARIANTS.search(base_title)
    if m2:
        # keep the exact captured variant capitalization as title-case
        raw = m2.group(1)
        version = raw.strip().title()
        base_title = _LIVE_VARIANTS.sub("", base_title)

    # remove generic noise
    if normalize_youtube_noise:
        base_title = _NOISE_PAT.sub("", base_title)

    # cleanup spaces and punctuation leftovers
    base_title = re.sub(r"\s+", " ", base_title).strip()

    return {
        "artist": "",
        "title": base_title,
        "features": features,
        "version": version,
    }

