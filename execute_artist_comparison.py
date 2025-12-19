#!/usr/bin/env python3
"""
Notebook-style artist comparison snapshot for CI validation.
Outputs ranking summaries and top-performing videos per artist.
"""

from __future__ import annotations

from tools.notebook_output_helpers import (
    format_number,
    format_percent,
    list_artists,
    load_artist_summary,
    load_normalized_videos,
)


def main() -> int:
    summary = load_artist_summary()
    videos = load_normalized_videos()
    artists = list_artists(summary["display_name"])

    print("Artist Comparison Metrics")
    print("=" * 60)
    print(f"Artists: {len(artists)}")
    print("Roster:", ", ".join(artists))
    print()

    print("ARTIST RANKING SUMMARY")
    ranked = summary.sort_values("total_views", ascending=False)
    for _, row in ranked.iterrows():
        print(
            f"- {row['display_name']}: {format_number(row['total_views'])} views | "
            f"Engagement {format_percent(row['avg_engagement_rate'])}"
        )

    print("\nTop Performing Videos by Artist")
    for artist in artists:
        subset = videos[videos["display_name"] == artist]
        top_videos = subset.sort_values("view_count", ascending=False).head(3)
        print(f"\n🎤 {artist}:")
        for _, row in top_videos.iterrows():
            title = str(row.get("title", "Untitled"))
            views = format_number(row.get("view_count", 0))
            print(f"  - {title} ({views} views)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
