#!/usr/bin/env python3
"""
Run ETL for all YouTube channels defined in .env (keys starting with YT_ and value is a YouTube URL).

Example .env entries picked up:
  YT_COROOK_YT=https://www.youtube.com/@hicorook
  YT_CHANNEL_1=https://www.youtube.com/@someartist

No secrets are hardcoded; values are read from .env at runtime.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List

from dotenv import load_dotenv

from web.etl_entrypoints import run_channel_etl

YOUTUBE_URL_RE = re.compile(r"https?://(www\.)?(youtube\.com|youtu\.be)/", re.IGNORECASE)


def collect_channel_urls_from_env() -> list[tuple[str, str]]:
    """Return [(env_key, url), ...] for all YT_* env vars set to a YouTube URL."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    pairs: List[tuple[str, str]] = []
    for k, v in os.environ.items():
        if not k.startswith("YT_"):
            continue
        if isinstance(v, str) and YOUTUBE_URL_RE.search(v):
            pairs.append((k, v))
    pairs.sort(key=lambda kv: kv[0])
    return pairs


def main() -> int:
    pairs = collect_channel_urls_from_env()
    if not pairs:
        print("No YT_* channel URLs found in .env")
        return 0

    print(f"Found {len(pairs)} channel URLs in env:\n  " + "\n  ".join(f"{k}={v}" for k, v in pairs))
    failures = 0
    for k, url in pairs:
        print(f"\n▶ Running ETL for {k}: {url}")
        try:
            summary = run_channel_etl(url)
            errs = ", ".join(summary.errors) if summary.errors else "none"
            print(
                f"  ✓ channel_id={summary.channel_id or '?'} uploads={summary.uploads_playlist_id or '?'} "
                f"videos={summary.videos_seen} raw_upserts={summary.raw_upserts} "
                f"metrics_upserts={summary.metrics_upserts} errors=[{errs}]"
            )
        except Exception as e:
            failures += 1
            print(f"  ✗ Failed: {e}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
