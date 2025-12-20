from __future__ import annotations

from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

DISPLAY_NAME_OVERRIDES = {
    "hicorook": "Corook",
}

EXPECTED_ARTISTS = {"BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required data file: {path}")
    return pd.read_csv(path)


def load_artist_summary() -> pd.DataFrame:
    df = _read_csv(Path("music_analysis_tables/artist_music_summary.csv"))
    df["display_name"] = df["artist_name"].map(DISPLAY_NAME_OVERRIDES).fillna(df["artist_name"])
    return df


def load_normalized_videos() -> pd.DataFrame:
    df = _read_csv(Path("music_analysis_tables/normalized_music_videos.csv"))
    df["display_name"] = df["artist_name"].map(DISPLAY_NAME_OVERRIDES).fillna(df["artist_name"])
    return df


def format_number(value: float | int) -> str:
    return f"{value:,.0f}"


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_percent(value: float, *, unit: Literal["percent", "fraction"] = "percent") -> str:
    """Format a percentage value with explicit unit handling."""
    if unit == "fraction":
        value *= 100.0
    return f"{value:.2f}%"


def list_artists(values: Iterable[str]) -> list[str]:
    return sorted({val for val in values if val and str(val) != "nan"})
