from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd

DISPLAY_NAME_OVERRIDES = {
    "hicorook": "Corook",
}


@dataclass(frozen=True)
class ArtistInsight:
    name: str
    display_name: str
    total_videos: int
    total_views: int
    total_est_revenue_usd: float
    avg_engagement_rate: float
    revenue_per_video: float
    avg_views_per_day: float
    view_share: float


@dataclass(frozen=True)
class VideoTypeInsight:
    video_type: str
    video_count: int
    total_views: int
    avg_views_per_video: float
    total_revenue_usd: float
    avg_engagement_rate: float


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _round(value: float, digits: int = 2) -> float:
    return round(float(value), digits)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing insights source file: {path}")
    return pd.read_csv(path)


def _display_name(name: str) -> str:
    return DISPLAY_NAME_OVERRIDES.get(name, name)


def _compute_views_per_day(normalized_videos: pd.DataFrame) -> pd.DataFrame:
    if "views_per_day" not in normalized_videos.columns or "artist_name" not in normalized_videos.columns:
        raise ValueError("normalized_videos missing required columns: artist_name, views_per_day")

    views_per_day = pd.to_numeric(normalized_videos["views_per_day"], errors="coerce").fillna(0)
    return (
        normalized_videos.assign(views_per_day=views_per_day)
        .groupby("artist_name", dropna=False)
        .agg(avg_views_per_day=("views_per_day", "mean"))
        .reset_index()
    )


def build_insights(
    artist_summary_path: Path,
    normalized_videos_path: Path,
    video_type_path: Path,
    top_artist_count: int = 5,
) -> Dict[str, Any]:
    artist_summary = _read_csv(artist_summary_path)
    normalized_videos = _read_csv(normalized_videos_path)
    video_type = _read_csv(video_type_path)

    views_per_day = _compute_views_per_day(normalized_videos)

    merged = artist_summary.merge(views_per_day, on="artist_name", how="left")

    total_views = _safe_int(merged["total_views"].sum())
    total_videos = _safe_int(merged["total_videos"].sum())
    total_revenue = _round(_safe_float(merged["total_est_revenue_usd"].sum()), 2)
    weighted_engagement = 0.0
    if total_views > 0:
        weighted_engagement = _round(
            (merged["avg_engagement_rate"] * merged["total_views"]).sum() / total_views,
            2,
        )

    artists: list[ArtistInsight] = []
    for _, row in merged.iterrows():
        view_share = _safe_float(row.get("total_views", 0)) / total_views if total_views else 0.0
        artists.append(
            ArtistInsight(
                name=str(row["artist_name"]),
                display_name=_display_name(str(row["artist_name"])),
                total_videos=_safe_int(row.get("total_videos")),
                total_views=_safe_int(row.get("total_views")),
                total_est_revenue_usd=_round(_safe_float(row.get("total_est_revenue_usd")), 2),
                avg_engagement_rate=_round(_safe_float(row.get("avg_engagement_rate")), 2),
                revenue_per_video=_round(_safe_float(row.get("revenue_per_video")), 2),
                avg_views_per_day=_round(_safe_float(row.get("avg_views_per_day")), 2),
                view_share=_round(view_share, 4),
            )
        )

    artists_sorted = sorted(artists, key=lambda item: item.total_views, reverse=True)
    top_artists = [
        {
            "display_name": artist.display_name,
            "total_views": artist.total_views,
            "avg_engagement_rate": artist.avg_engagement_rate,
            "avg_views_per_day": artist.avg_views_per_day,
        }
        for artist in artists_sorted[:top_artist_count]
    ]

    video_types: list[VideoTypeInsight] = []
    for _, row in video_type.iterrows():
        video_types.append(
            VideoTypeInsight(
                video_type=str(row["video_type"]),
                video_count=_safe_int(row.get("video_count")),
                total_views=_safe_int(row.get("total_views")),
                avg_views_per_video=_round(_safe_float(row.get("avg_views_per_video")), 2),
                total_revenue_usd=_round(_safe_float(row.get("total_revenue_usd")), 2),
                avg_engagement_rate=_round(_safe_float(row.get("avg_engagement_rate")), 2),
            )
        )

    isrc_nulls = normalized_videos["isrc"].isna().sum() if "isrc" in normalized_videos.columns else 0
    normalized_total = len(normalized_videos)
    isrc_null_rate = isrc_nulls / normalized_total if normalized_total else 0.0

    return {
        "summary": {
            "artist_count": len(artists),
            "total_views": total_views,
            "total_videos": total_videos,
            "total_est_revenue_usd": total_revenue,
            "avg_engagement_rate": weighted_engagement,
        },
        "artists": [asdict(artist) for artist in artists_sorted],
        "top_artists": top_artists,
        "video_types": [asdict(item) for item in video_types],
        "data_quality": {
            "isrc_null_count": int(isrc_nulls),
            "isrc_null_rate": _round(isrc_null_rate, 4),
            "normalized_rows": normalized_total,
        },
    }


def _write_json(payload: Dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export insights for frontend charts.")
    parser.add_argument(
        "--artist-summary",
        type=Path,
        default=Path("music_analysis_tables/artist_music_summary.csv"),
        help="Path to artist_music_summary.csv",
    )
    parser.add_argument(
        "--normalized-videos",
        type=Path,
        default=Path("music_analysis_tables/normalized_music_videos.csv"),
        help="Path to normalized_music_videos.csv",
    )
    parser.add_argument(
        "--video-type",
        type=Path,
        default=Path("music_analysis_tables/video_type_analysis.csv"),
        help="Path to video_type_analysis.csv",
    )
    parser.add_argument(
        "--top-artists",
        type=int,
        default=5,
        help="Number of top artists to include in highlights",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("web/insights/artist_insights.json"),
        help="Output path for the insights JSON file",
    )

    args = parser.parse_args()

    payload = build_insights(
        artist_summary_path=args.artist_summary,
        normalized_videos_path=args.normalized_videos,
        video_type_path=args.video_type,
        top_artist_count=args.top_artists,
    )
    _write_json(payload, args.output)
    print(f"✅ Wrote frontend insights to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
