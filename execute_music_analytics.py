#!/usr/bin/env python3
"""
Notebook-style summary for CI validation.
This script prints a recruiter-friendly analytics snapshot using the
ETL-generated CSV outputs so notebook tests can verify real signals.
"""

from __future__ import annotations

from tools.notebook_output_helpers import (
    format_currency,
    format_number,
    format_percent,
    list_artists,
    load_artist_summary,
)


def main() -> int:
    summary = load_artist_summary()
    artists = list_artists(summary["display_name"])

    total_views = int(summary["total_views"].sum())
    total_revenue = float(summary["total_est_revenue_usd"].sum())
    weighted_engagement = (
        (summary["avg_engagement_rate"] * summary["total_views"]).sum() / total_views
        if total_views
        else 0.0
    )

    print("MUSIC INDUSTRY PERFORMANCE DASHBOARD")
    print("=" * 60)
    print(f"Artists: {len(artists)}")
    print("Roster:", ", ".join(artists))
    print()

    print("Market Share Analysis")
    for _, row in summary.sort_values("total_views", ascending=False).iterrows():
        share = (row["total_views"] / total_views * 100) if total_views else 0
        print(
            f"- {row['display_name']}: {format_number(row['total_views'])} views | "
            f"Share {share:.1f}%"
        )

    print("\nRevenue Analysis")
    print(f"Total Estimated Revenue (USD): {format_currency(total_revenue)}")
    print(f"Weighted Engagement Rate: {format_percent(weighted_engagement)}")

    print("\nINVESTMENT RECOMMENDATIONS")
    print("- Prioritize artists with high momentum and resonance.")
    print("- Balance reach leaders with community-driven engagement spikes.")
    print("- Use catalog health signals before scaling promotion.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
