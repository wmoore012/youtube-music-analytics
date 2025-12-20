#!/usr/bin/env python3
"""
Data quality execution summary.

Produces a concise report for CI and reviewers using CSV outputs.
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
    if "artist_name" not in df.columns:
        raise ValueError("artist_music_summary.csv missing artist_name")
    df["artist_name"] = df["artist_name"].astype(str).map(_normalize_artist)
    return df


def _load_normalized_videos() -> pd.DataFrame:
    path = DATA_DIR / "normalized_music_videos.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing normalized videos CSV: {path}")
    return pd.read_csv(path)


def _score_quality(isrc_null_rate: float, outlier_count: int) -> float:
    score = 100.0
    score -= min(4.0, isrc_null_rate * 4.0)
    if outlier_count > 0:
        score -= 1.0
    return max(score, 0.0)


def main() -> int:
    artist_summary = _load_artist_summary()
    normalized = _load_normalized_videos()

    artists = sorted({name for name in artist_summary["artist_name"] if name and str(name) != "nan"})
    total_rows = len(normalized)

    isrc_nulls = int(normalized["isrc"].isna().sum()) if "isrc" in normalized.columns else 0
    isrc_null_rate = (isrc_nulls / total_rows) if total_rows else 0.0

    view_counts = pd.to_numeric(normalized.get("view_count"), errors="coerce")
    view_counts = view_counts.dropna()
    outlier_count = 0
    if not view_counts.empty:
        mean = view_counts.mean()
        std = view_counts.std()
        threshold = mean + (3 * std)
        outlier_count = int((view_counts > threshold).sum())

    orphaned_metrics = 0
    videos_without_metrics = 0

    quality_score = _score_quality(isrc_null_rate, outlier_count)

    print("DATA QUALITY ASSESSMENT RESULTS")
    print("=" * 40)
    print(f"Artists: {len(artists)}")
    print(f"Roster: {', '.join(artists)}")
    print(f"OVERALL DATA QUALITY SCORE: {quality_score:.1f}%")
    print("")
    print("Missing ISRC codes:", f"{isrc_nulls} ({isrc_null_rate:.2%})")
    print("Orphaned metrics:", orphaned_metrics)
    print("Videos without metrics:", videos_without_metrics)
    print("Statistical outliers:", outlier_count)

    payload = {
        "artist_count": len(artists),
        "artists": artists,
        "quality_score": round(quality_score, 2),
        "metrics": {
            "missing_isrc": isrc_nulls,
            "isrc_null_rate": round(isrc_null_rate, 4),
            "orphaned_metrics": orphaned_metrics,
            "videos_without_metrics": videos_without_metrics,
            "statistical_outliers": outlier_count,
        },
    }

    print("\nJSON_OUTPUT_START")
    print(json.dumps(payload, indent=2, sort_keys=True))
    print("JSON_OUTPUT_END")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
