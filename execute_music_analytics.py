#!/usr/bin/env python3
"""
Music analytics execution summary.

Generates a lightweight, human-readable snapshot from CSV outputs so CI and
reviewers can validate the notebook pipeline without running heavy notebooks.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT / "music_analysis_tables"

DISPLAY_NAME_OVERRIDES = {"hicorook": "Corook"}


def _normalize_artist(name: str) -> str:
    return DISPLAY_NAME_OVERRIDES.get(name, name)


def _load_artist_summary() -> pd.DataFrame:
    path = DATA_DIR / "artist_music_summary.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing artist summary CSV: {path}")

    df = pd.read_csv(path)
    required = [
        "artist_name",
        "total_videos",
        "total_views",
        "total_est_revenue_usd",
        "avg_engagement_rate",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"artist_music_summary.csv missing columns: {', '.join(missing)}")

    for col in ["total_videos", "total_views", "total_est_revenue_usd", "avg_engagement_rate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["artist_name"] = df["artist_name"].astype(str).map(_normalize_artist)
    return df


def _weighted_engagement(df: pd.DataFrame, total_views: int) -> float:
    if total_views <= 0:
        return 0.0
    return float((df["avg_engagement_rate"] * df["total_views"]).sum() / total_views)


def _market_share(df: pd.DataFrame, total_views: int) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for _, row in df.sort_values("total_views", ascending=False).iterrows():
        share = (row["total_views"] / total_views * 100) if total_views else 0.0
        rows.append(
            {
                "artist_name": str(row["artist_name"]),
                "total_views": int(row["total_views"]),
                "share_percent": float(share),
            }
        )
    return rows


def main() -> int:
    df = _load_artist_summary()
    artists = sorted({name for name in df["artist_name"] if name and name != "nan"})
    total_views = int(df["total_views"].sum())
    total_videos = int(df["total_videos"].sum())
    total_revenue = float(df["total_est_revenue_usd"].sum())
    avg_engagement = _weighted_engagement(df, total_views)

    market_share = _market_share(df, total_views)

    print("MUSIC INDUSTRY PERFORMANCE DASHBOARD")
    print("=" * 45)
    print(f"Artists: {len(artists)}")
    print(f"Roster: {', '.join(artists)}")
    print(f"Total videos: {total_videos}")
    print(f"Total views: {total_views:,} views")
    print(f"Weighted engagement: {avg_engagement:.2f}%")
    print("")

    print("Market Share Analysis")
    print("-" * 30)
    for entry in market_share:
        print(
            f"{entry['artist_name']}: {entry['total_views']:,} views "
            f"({entry['share_percent']:.2f}% share)"
        )
    print("")

    print("Revenue Analysis")
    print("-" * 30)
    print(f"Portfolio value: ${total_revenue:,.0f}")
    print(f"Avg revenue per video: ${total_revenue / max(total_videos, 1):,.0f}")
    print("")

    print("INVESTMENT RECOMMENDATIONS")
    print("-" * 30)
    if market_share:
        top_artist = market_share[0]["artist_name"]
        print(f"1) Double down on reach leader: {top_artist}.")
    print("2) Fund resonance experiments for high-engagement artists.")
    print("3) Track momentum shifts weekly to protect emerging growth.")

    payload = {
        "artist_count": len(artists),
        "artists": artists,
        "total_views": total_views,
        "total_videos": total_videos,
        "total_revenue_usd": round(total_revenue, 2),
        "avg_engagement_rate": round(avg_engagement, 2),
        "market_share": market_share,
    }

    print("\nJSON_OUTPUT_START")
    print(json.dumps(payload, indent=2, sort_keys=True))
    print("JSON_OUTPUT_END")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
