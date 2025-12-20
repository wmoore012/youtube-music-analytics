#!/usr/bin/env python3
"""
Artist comparison execution summary.

Uses normalized CSV outputs to compare artists and list top-performing videos.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "music_analysis_tables"

DISPLAY_NAME_OVERRIDES = {"hicorook": "Corook"}


def _normalize_artist(name: str) -> str:
    return DISPLAY_NAME_OVERRIDES.get(name, name)


def _load_artist_summary() -> pd.DataFrame:
    path = DATA_DIR / "artist_music_summary.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing artist summary CSV: {path}")
    df = pd.read_csv(path)
    required = ["artist_name", "total_views", "avg_engagement_rate", "revenue_per_video"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"artist_music_summary.csv missing columns: {', '.join(missing)}")

    for col in ["total_views", "avg_engagement_rate", "revenue_per_video"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["artist_name"] = df["artist_name"].astype(str).map(_normalize_artist)
    return df


def _load_normalized_videos() -> pd.DataFrame:
    path = DATA_DIR / "normalized_music_videos.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing normalized videos CSV: {path}")
    df = pd.read_csv(path)
    if "artist_name" in df.columns:
        df["artist_name"] = df["artist_name"].astype(str).map(_normalize_artist)
    return df


def _rank_artists(df: pd.DataFrame, column: str) -> List[str]:
    return df.sort_values(column, ascending=False)["artist_name"].tolist()


def main() -> int:
    summary = _load_artist_summary()
    normalized = _load_normalized_videos()

    artists = sorted({name for name in summary["artist_name"] if name and name != "nan"})

    print("Artist Comparison Metrics")
    print("=" * 35)
    print(f"Artists: {len(artists)}")
    print(f"Roster: {', '.join(artists)}")
    print("")

    print("ARTIST RANKING SUMMARY")
    print("-" * 30)
    print("By total views:", ", ".join(_rank_artists(summary, "total_views")))
    print("By engagement:", ", ".join(_rank_artists(summary, "avg_engagement_rate")))
    print("By revenue per video:", ", ".join(_rank_artists(summary, "revenue_per_video")))
    print("")

    print("Top Performing Videos by Artist")
    print("-" * 35)
    top_videos: Dict[str, List[Dict[str, int | str]]] = {}
    for artist in artists:
        artist_videos = normalized[normalized["artist_name"] == artist]
        if artist_videos.empty:
            continue
        top = artist_videos.sort_values("view_count", ascending=False).head(3)
        print(f"Artist: {artist}")
        top_entries: List[Dict[str, int | str]] = []
        for _, row in top.iterrows():
            title = str(row.get("title", "Unknown title"))
            views = int(row.get("view_count", 0))
            print(f"  - {title} ({views:,} views)")
            top_entries.append({"title": title, "view_count": views})
        top_videos[artist] = top_entries

    payload = {
        "artist_count": len(artists),
        "artists": artists,
        "rankings": {
            "total_views": _rank_artists(summary, "total_views"),
            "avg_engagement_rate": _rank_artists(summary, "avg_engagement_rate"),
            "revenue_per_video": _rank_artists(summary, "revenue_per_video"),
        },
        "top_videos": top_videos,
    }

    print("\nJSON_OUTPUT_START")
    print(json.dumps(payload, indent=2, sort_keys=True))
    print("JSON_OUTPUT_END")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
