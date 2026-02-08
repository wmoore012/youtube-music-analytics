#!/usr/bin/env python3
"""
Run ETL for all YouTube channels defined in .env (keys starting with YT_ and value is a YouTube URL).

Example .env entries picked up:
  YT_COROOK_YT=https://www.youtube.com/@hicorook
  YT_CHANNEL_1=https://www.youtube.com/@someartist

No secrets are hardcoded; values are read from .env at runtime.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Sequence

from dotenv import load_dotenv

# Ensure project-root imports (e.g., web.etl_entrypoints) work in CI and cron.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from web.etl_entrypoints import run_channel_etl  # noqa: E402

YOUTUBE_URL_RE = re.compile(r"https?://(www\.)?(youtube\.com|youtu\.be)/", re.IGNORECASE)
DEFAULT_SNAPSHOT_TIMEOUT_SECONDS = 300


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        print(f"WARNING: {name} must be an integer; got {raw!r}. Using {default}.")
        return default
    if value <= 0:
        print(f"WARNING: {name} must be > 0; got {value}. Using {default}.")
        return default
    return value


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


def _refresh_demo_snapshot(timeout_seconds: int) -> bool:
    """Regenerate demo cohort snapshot from latest warehouse rows."""

    print(f"\n▶ Refreshing demo snapshot (timeout: {timeout_seconds}s)")
    try:
        result = subprocess.run(
            [sys.executable, "scripts/refresh_demo_snapshot.py"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        print(f"  ✗ Demo snapshot refresh timed out after {timeout_seconds}s")
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"  ✗ Demo snapshot refresh failed to start: {exc}")
        return False

    if result.returncode != 0:
        print("  ✗ Demo snapshot refresh failed")
        if result.stderr.strip():
            print("  stderr:")
            for line in result.stderr.strip().splitlines()[-8:]:
                print(f"    {line}")
        return False

    stdout = result.stdout.strip()
    if stdout:
        print(f"  ✓ {stdout.splitlines()[-1]}")
    else:
        print("  ✓ Demo snapshot refreshed")
    return True


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run channel ETL for all YT_* URLs in env")
    parser.add_argument(
        "--refresh-demo-snapshot",
        action="store_true",
        help=(
            "Regenerate demo_data/curated_cohort.json after successful ingestion. "
            "Equivalent to REFRESH_DEMO_SNAPSHOT_AFTER_INGEST=1."
        ),
    )
    return parser.parse_args(list(argv))


def main(argv: Sequence[str] | None = None) -> int:
    # IMPORTANT / DO NOT REGRESS (user-required behavior):
    # The Streamlit demo snapshot must be able to refresh daily as part of ETL.
    # Keep this explicit and timeout-guarded so cron jobs never hang silently.
    args = parse_args([] if argv is None else argv)

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

    refresh_snapshot_requested = args.refresh_demo_snapshot or _is_truthy(
        os.getenv("REFRESH_DEMO_SNAPSHOT_AFTER_INGEST"),
    )
    if refresh_snapshot_requested and failures == 0:
        timeout_seconds = _read_positive_int_env(
            "ETL_SNAPSHOT_REFRESH_TIMEOUT_SECONDS",
            DEFAULT_SNAPSHOT_TIMEOUT_SECONDS,
        )
        if not _refresh_demo_snapshot(timeout_seconds):
            failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
