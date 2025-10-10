"""YouTube helper utilities (vendor-neutral, minimal test-focused API).

This module provides a compact set of helpers sufficient for our tests.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

# ----------------------------- normalization ------------------------------

def normalize_string(text: Optional[str]) -> str:
    if text is None:
        raise ValueError("Text cannot be null")
    if not isinstance(text, str):
        raise ValueError("Text must be string")
    s = text.strip()
    if s == "":
        raise ValueError("Text cannot be empty")
    # Remove punctuation-like chars and collapse spaces
    s = re.sub(r"[()\[\]\-_/|&.,'\"]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


# ----------------------------- titles/channels ----------------------------

def clean_video_title(title: Optional[str], *, remove_youtube_noise: bool = True) -> str:
    if title is None:
        raise ValueError("Title cannot be null")
    if not isinstance(title, str):
        raise ValueError("Title must be string")
    if title == "":
        raise ValueError("Title cannot be empty")
    t = title
    if remove_youtube_noise:
        # Remove common descriptors in parentheses including HD
        t = re.sub(
            r"\((?:official(?: music)? (?:video|audio)|lyrics?|visualizer|live|hd)\)",
            "",
            t,
            flags=re.I,
        )
    # Collapse spaces
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_artist_from_channel(channel_title: Optional[str]) -> str:
    if channel_title is None:
        raise ValueError("Channel title cannot be null")
    if not isinstance(channel_title, str):
        raise ValueError("Channel title must be string")
    s = channel_title.strip()
    if s == "":
        raise ValueError("Channel title cannot be empty")
    # Remove known suffix tokens wherever they appear
    s = re.sub(r"\s+VEVO\b", "", s, flags=re.I)
    s = re.sub(r"-Topic\b", "", s, flags=re.I)
    s = re.sub(r"\s+Official\b", "", s, flags=re.I)
    return s.strip()


# ----------------------------- video id -----------------------------------

def extract_video_id(item: Any) -> str:
    if item is None:
        raise ValueError("Video item cannot be null")
    # dict with nested id
    if isinstance(item, dict):
        if "id" not in item:
            raise KeyError("Video item missing 'id' field")
        vid = item["id"]
        if isinstance(vid, dict):
            if "videoId" not in vid:
                raise KeyError("Video item missing 'videoId' field")
            v = vid["videoId"]
        else:
            v = vid
    else:
        # object with attribute id
        if not hasattr(item, "id"):
            raise KeyError("Video item missing 'id' field")
        v = getattr(item, "id")

    if v is None:
        raise ValueError("Video ID cannot be null")
    if not isinstance(v, str):
        raise ValueError("Video ID must be string")
    if v.strip() == "":
        raise ValueError("Video ID cannot be empty")
    return v.strip()


# ----------------------------- ISO 8601 durations -------------------------

def parse_duration_iso8601(s: Optional[str]) -> int:
    """Parse a subset of ISO8601 durations like PT1H2M3S -> seconds."""
    if s is None:
        raise ValueError("Duration cannot be null")
    if not isinstance(s, str):
        raise ValueError("Invalid duration format")
    if s == "":
        raise ValueError("Duration cannot be empty")
    if not s.startswith("P"):
        raise ValueError("Invalid duration format")
    m = re.fullmatch(r"P(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)", s)
    if not m:
        raise ValueError("Invalid duration format")
    h, m_, s_ = m.groups()
    hours = int(h or 0)
    minutes = int(m_ or 0)
    seconds = int(s_ or 0)
    return hours * 3600 + minutes * 60 + seconds


# ----------------------------- classification -----------------------------

def classify_video_version(title: str, channel_title: str, description: Optional[str] = None) -> str:
    if title is None:
        raise ValueError("Video title cannot be null")
    if channel_title is None:
        raise ValueError("Channel title cannot be null")
    if title == "":
        raise ValueError("Video title cannot be empty")
    if channel_title == "":
        raise ValueError("Channel title cannot be empty")

    t = title.lower()
    d = (description or "").lower()
    ch = channel_title

    if re.search(r"official music video|official video", t):
        return "Official Music Video"
    if re.search(r"\(live\)|\blive\b", t):
        return "Live Performance"
    if "remix" in t:
        return "Remix"
    if "acoustic" in t:
        return "Acoustic"
    if re.search(r"lyric", t) or "lyric" in d:
        return "Lyric Video"
    if re.search(r"-topic$", ch, flags=re.I):
        return "Official Audio"

    return "Original"


# ----------------------------- validation & errors ------------------------

def validate_video_data(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if data is None:
        raise ValueError("Video data cannot be null")
    if not isinstance(data, dict):
        raise ValueError("Video data must be dict")
    errors = []
    if "id" not in data:
        errors.append("Missing video ID")
    elif data.get("id") is None:
        errors.append("Video ID cannot be null")
    snippet = data.get("snippet")
    if snippet is None:
        errors.append("Missing snippet data")
        snippet = {}
    title = snippet.get("title")
    channel = snippet.get("channelTitle")
    if title is None:
        errors.append("Video title cannot be null")
    if channel is None:
        errors.append("Missing channelTitle")
    return {"valid": len(errors) == 0, "errors": errors}


def validate_playlist_data(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if data is None:
        raise ValueError("Playlist data cannot be null")
    if not isinstance(data, dict):
        raise ValueError("Playlist data must be dict")
    errors = []
    if "id" not in data:
        errors.append("Missing playlist ID")
    elif data.get("id") is None:
        errors.append("Playlist ID cannot be null")
    snippet = data.get("snippet")
    if snippet is None:
        errors.append("Missing snippet data")
        snippet = {}
    if snippet.get("title") is None:
        errors.append("Missing title")
    return {"valid": len(errors) == 0, "errors": errors}


def is_quota_exceeded_error(err: Any) -> bool:
    s = str(err).lower()
    # also check common API error shapes
    status = getattr(err, "resp", {}).get("status") if hasattr(err, "resp") else None
    return "quota" in s or "quotaexceeded" in s or status == 403


def handle_api_error(err: Any) -> Dict[str, Any]:
    status = getattr(err, "resp", {}).get("status") if hasattr(err, "resp") else None
    message = str(err) if err is not None else "Unknown error"
    is_quota = is_quota_exceeded_error(err)
    should_retry = (status in (429, 500)) and not is_quota
    return {
        "is_quota_error": is_quota,
        "should_retry": bool(should_retry),
        "status_code": status,
        "message": message,
    }

