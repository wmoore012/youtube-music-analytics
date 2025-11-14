from __future__ import annotations

"""
General data utilities for MusicScope notebooks.
"""
from typing import Optional
import re
import pandas as pd

__all__ = [
    "iso8601_to_seconds",
    "resolve_artist_column",
    "pick_content_column",
    "merge_artist",
    "artist_of",
    "daily_hits",
]


_ISO_RE = re.compile(r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$", re.I)


def iso8601_to_seconds(s: str) -> int:
    """Parse a subset of ISO-8601 duration like 'PT1H30M20S' into seconds."""
    if not s:
        return 0
    m = _ISO_RE.match(str(s).strip())
    if not m:
        return 0
    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def resolve_artist_column(df: pd.DataFrame) -> str:
    """Return the best-guess artist column name present in df."""
    for c in ["artist_name", "artist", "channel_title", "uploader"]:
        if c in df.columns:
            return c
    return "artist_name"  # default; callers should handle absence


def pick_content_column(df: pd.DataFrame) -> str:
    """Return a reasonable content text/title column name if available."""
    for c in ["title", "name", "video_title", "text"]:
        if c in df.columns:
            return c
    return "title"


def merge_artist(df: pd.DataFrame, on: Optional[str] = None) -> pd.DataFrame:
    """Ensure an artist column is present; noop if already present.

    This function is a placeholder for future joins; currently returns df unchanged.
    """
    return df


def artist_of(row: pd.Series, col: str = "artist_name") -> str:
    return str(row.get(col, "")).strip()


def daily_hits(df: pd.DataFrame, date_col: str = "date", count_col: str = "comments") -> pd.DataFrame:
    """Aggregate a simple daily count per date (helper for quick charts)."""
    if date_col not in df.columns:
        return pd.DataFrame({date_col: [], count_col: []})
    d = df.copy()
    d[date_col] = pd.to_datetime(d[date_col]).dt.floor("D")
    out = d.groupby(date_col).size().rename(count_col).reset_index()
    return out

