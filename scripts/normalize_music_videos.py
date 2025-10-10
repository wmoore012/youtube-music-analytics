#!/usr / bin / env python3
"""
Populate / refresh music_videos_normalized from existing youtube tables quickly.

Usage:
  python scripts / normalize_music_videos.py [--dry-run]

This avoids running the full ETL and reduces nulls by applying aliases and simple revenue estimation.
"""
from __future__ import annotations

import argparse

from dotenv import load_dotenv

from youtubeviz.normalization import run_normalization


def main() -> int:
    # Load environment variables from .env if present
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Plan only (no DB writes)")
    args = parser.parse_args()

    if args.dry_run:
        from web.db_guard import get_engine
        from youtubeviz.normalization import build_normalized_rows

        eng = get_engine(ro=True)
        rows = list(build_normalized_rows(eng))
        print(f"Would upsert {len(rows)} rows")
        return 0

    inserted = run_normalization()
    print(f"✅ Normalized rows upserted: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
