"""ETL data freshness verification for curated YouTube cohort.

This test is *intentionally* integration / manual‑run focused:

- It connects to the real MySQL YouTube analytics database using
  the same DB_*/.env configuration as the ETL.
- It compares current aggregate view counts for a small curated
  artist cohort against a stored JSON baseline.
- It fails *very loudly* when data is stale or the DB is
  unreachable, so you cannot miss it before a live demo.

Usage
-----
Local (with .env + DB_* set):

    RUN_DB_TESTS=1 pytest tests/test_etl_data_freshness.py -q

To update the baseline after a successful fresh ETL run:

    RUN_DB_TESTS=1 pytest tests/test_etl_data_freshness.py -q --update-baseline

In CI this test should only run in a dedicated workflow that
provides real MySQL credentials via GitHub Secrets.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Mapping

import pandas as pd
import pytest
from sqlalchemy import bindparam, text

from web.etl_helpers import get_engine, read_sql_safe


REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = REPO_ROOT / "tests" / "fixtures" / "baseline_play_counts.json"


CURATED_ARTISTS = [
    "BiC Fizzle",
    "COBRAH",
]

DEFAULT_MAX_AGE_DAYS = 7
MAX_AGE_DAYS_ENV = "ETL_FRESHNESS_MAX_AGE_DAYS"


@dataclass
class ArtistSnapshot:
    total_views: int
    video_count: int
    last_updated: datetime


def _is_db_tests_enabled() -> bool:
    """Return True only when RUN_DB_TESTS is explicitly enabled.

    Treats "0", "false", "off" and empty values as disabled so that
    ``RUN_DB_TESTS=0`` behaves the same as the variable being unset.
    """

    raw = os.getenv("RUN_DB_TESTS")
    if raw is None:
        return False
    return raw not in {"0", "", "false", "False", "off", "OFF"}


def _load_current_snapshot() -> Dict[str, ArtistSnapshot]:
    """Query MySQL for the latest aggregate stats per artist.

    The query intentionally mirrors the production aggregates used
    for the Streamlit dashboard and portfolio notebooks.
    """

    # Ensure we have a real database; this should never silently
    # fall back to SQLite. Fail fast if required env vars are missing
    # when RUN_DB_TESTS=1 is set.
    required_env = ("DB_HOST", "DB_USER", "DB_PASS", "DB_NAME")
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        pytest.fail(
            "\n[ETL Data Freshness] ❌ RUN_DB_TESTS=1 but database environment is incomplete.\n"
            f"Missing variables: {', '.join(sorted(missing))}\n"
            "Set these DB_* variables (via .env or CI secrets) before running the "
            "freshness test.",
            pytrace=False,
        )

    engine = get_engine()

    # Expect the production music_videos_normalized table with
    # per-artist aggregates and a last_updated timestamp.
    query = (
        text(
            """
        SELECT
            artist_name,
            SUM(total_views) AS total_views,
            COUNT(*) AS video_count,
            MAX(last_updated) AS last_updated
        FROM music_videos_normalized
        WHERE artist_name IN :artists
        GROUP BY artist_name
    """
        ).bindparams(bindparam("artists", expanding=True))
    )

    try:
        df = read_sql_safe(
            query,
            engine,
            params={"artists": CURATED_ARTISTS},
        )
    except Exception as exc:  # noqa: BLE001
        pytest.fail(
            "\n[ETL Data Freshness] ❌ Failed to query MySQL for current artist stats.\n"
            f"Error: {exc}\n"
            "Check that DB_HOST/DB_USER/DB_PASS/DB_NAME are set correctly and that the "
            "music_analysis_tables schema is up to date before demoing the dashboard.",
            pytrace=False,
        )

    required_cols = {"artist_name", "total_views", "video_count", "last_updated"}
    missing = required_cols - set(df.columns)
    if missing:
        pytest.fail(
            "\n[ETL Data Freshness] ❌ Query returned data but is missing required columns.\n"
            f"Missing columns: {sorted(missing)}\n"
            f"Available columns: {list(df.columns)}\n"
            "This usually means the ETL schema changed without updating the freshness test.",
            pytrace=False,
        )

    snapshot: Dict[str, ArtistSnapshot] = {}
    for _, row in df.iterrows():
        artist = str(row["artist_name"])
        ts = pd.to_datetime(row["last_updated"], utc=True)
        if pd.isna(ts):
            # Treat missing timestamps as extremely old so they fail
            # the freshness check decisively.
            ts = pd.to_datetime("1970-01-01T00:00:00Z", utc=True)
        snapshot[artist] = ArtistSnapshot(
            total_views=int(row["total_views"] or 0),
            video_count=int(row["video_count"] or 0),
            last_updated=ts.to_pydatetime(),
        )

    return snapshot


def _load_baseline() -> Mapping[str, ArtistSnapshot]:
    if not BASELINE_PATH.exists():
        return {}

    with BASELINE_PATH.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    artists = payload.get("artists", {})
    # Optional file-level timestamp for legacy baselines. Per-artist
    # timestamps were added later; both shapes should remain loadable.
    file_level_ts_raw = payload.get("last_updated")
    file_level_ts: datetime | None = None
    if file_level_ts_raw:
        try:
            file_level_ts = pd.to_datetime(file_level_ts_raw, utc=True).to_pydatetime()
        except Exception:  # noqa: BLE001
            file_level_ts = None

    baseline: Dict[str, ArtistSnapshot] = {}
    for name, stats in artists.items():
        # Per-artist last_updated was introduced after the initial
        # baseline schema. Older baselines won't have this field, so we
        # fall back to the file-level timestamp or, as a final fallback,
        # to a very old date. Only the *current* snapshot's timestamps
        # participate in freshness enforcement; baseline timestamps are
        # primarily for operator context.
        artist_ts_raw = stats.get("last_updated") or file_level_ts_raw
        if artist_ts_raw:
            try:
                ts = pd.to_datetime(artist_ts_raw, utc=True).to_pydatetime()
            except Exception:  # noqa: BLE001
                ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        elif file_level_ts is not None:
            ts = file_level_ts
        else:
            ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

        baseline[name] = ArtistSnapshot(
            total_views=int(stats.get("total_views", 0)),
            video_count=int(stats.get("video_count", 0)),
            last_updated=ts,
        )
    return baseline


def _write_baseline(snapshot: Mapping[str, ArtistSnapshot]) -> None:
    payload = {
        "last_updated": datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "artists": {
            name: {
                "total_views": s.total_views,
                "video_count": s.video_count,
            }
            for name, s in snapshot.items()
        },
    }
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with BASELINE_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def _get_max_age_days() -> int:
    """Return the maximum allowed age (in days) for fresh data.

    Controlled via ETL_FRESHNESS_MAX_AGE_DAYS; defaults to 7 days,
    and coerced to at least 1 day if misconfigured.
    """

    try:
        value = int(os.getenv(MAX_AGE_DAYS_ENV, str(DEFAULT_MAX_AGE_DAYS)))
        return max(1, value)
    except ValueError:
        return DEFAULT_MAX_AGE_DAYS


@pytest.mark.database
def test_etl_data_freshness(request: pytest.FixtureRequest) -> None:
    """Verify ETL has produced fresh, non-regressing data for key artists.

    This test is meant to be unmistakable when things are wrong, but
    *opt-in* so normal pytest runs don't require MySQL:

    - Requires RUN_DB_TESTS=1 to be set, otherwise it is skipped.
    - If MySQL is unreachable, it fails with a clear message.
    - If required columns are missing, it calls out the schema issue.
    - If data for curated artists is older than ETL_FRESHNESS_MAX_AGE_DAYS
      (default 7), it fails with a per-artist diff. That failure is a
      signal that the ETL has not run recently enough for demo/production,
      not a bug in the test.
    - View-count regressions relative to the JSON baseline are reported
      as warnings, not hard failures (schema rebuilds/backfills can
      legitimately reset counts).
    """
    if not _is_db_tests_enabled():
        pytest.skip(
            "[ETL Data Freshness] Skipping MySQL-based freshness check "
            "(set RUN_DB_TESTS=1 to enable)."
        )

    update_baseline = bool(request.config.getoption("--update-baseline"))

    current = _load_current_snapshot()
    if not current:
        pytest.fail(
            "\n[ETL Data Freshness] ❌ Query returned no rows for curated artists.\n"
            f"Curated artists: {CURATED_ARTISTS}\n"
            "This usually means the ETL has not populated music_videos_normalized "
            "for these artists yet. Run the ETL before demoing the dashboard.",
            pytrace=False,
        )

    baseline = _load_baseline()

    if not baseline and not update_baseline:
        pytest.fail(
            "\n[ETL Data Freshness] ❌ Baseline file is missing or empty.\n"
            f"Expected JSON at: {BASELINE_PATH}\n"
            "Create it explicitly by running:\n"
            "  RUN_DB_TESTS=1 pytest tests/test_etl_data_freshness.py --update-baseline\n"
            "after a known-good ETL run.",
            pytrace=False,
        )

    if update_baseline:
        _write_baseline(current)
        pytest.skip(
            "[ETL Data Freshness] Baseline updated from current MySQL snapshot; "
            "re-run without --update-baseline to enforce freshness."
        )

    # Freshness and safety checks -------------------------------------------------
    fatal_problems: Dict[str, str] = {}
    regression_warnings: Dict[str, str] = {}

    max_age_days = _get_max_age_days()
    now = datetime.now(timezone.utc)

    for artist in CURATED_ARTISTS:
        cur = current.get(artist)
        if cur is None:
            fatal_problems[artist] = (
                "No current data found in MySQL for this artist. "
                "ETL may not have ingested their channel recently."
            )
            continue

        age = now - cur.last_updated
        if age > timedelta(days=max_age_days):
            fatal_problems[artist] = (
                f"last_updated={cur.last_updated.isoformat()} "
                f"({age.days} days ago) exceeds freshness threshold "
                f"of {max_age_days} days."
            )

    # View-count comparison against baseline is *warning-only*; schema
    # rebuilds or deduplication can legitimately reduce totals.
    for artist in CURATED_ARTISTS:
        cur = current.get(artist)
        base = baseline.get(artist) if baseline else None
        if not cur or not base:
            continue

        diff = cur.total_views - base.total_views
        if diff < 0:
            regression_warnings[artist] = (
                f"total_views decreased from {base.total_views:,} to {cur.total_views:,}."
            )

    if fatal_problems:
        lines = [
            "\n[ETL Data Freshness] ❌ Freshness check failed for curated artists.",
            f"Threshold: max age {max_age_days} days.",
            "",
        ]
        for artist, msg in fatal_problems.items():
            lines.append(f"  • {artist}: {msg}")
        if regression_warnings:
            lines.append("")
            lines.append("Additional non-fatal view-count regressions observed:")
            for artist, msg in regression_warnings.items():
                lines.append(f"  • {artist}: {msg}")
        pytest.fail("\n".join(lines), pytrace=False)

    # If we reach here, all curated artists have data within the freshness window.
    deltas = []
    for artist in CURATED_ARTISTS:
        cur = current.get(artist)
        base = baseline.get(artist) if baseline else None
        if not cur or not base:
            continue
        diff = cur.total_views - base.total_views
        sign = "+" if diff >= 0 else ""
        deltas.append(f"  • {artist}: {sign}{diff:,} views vs baseline")

    # Emit a clear, celebratory message in the test output, with any
    # regressions explicitly highlighted as warnings.
    msg_lines = [
        "[ETL Data Freshness] ✅ All curated artists have data within freshness window.",
        f"Freshness window: last {max_age_days} days (configurable via {MAX_AGE_DAYS_ENV}).",
    ]
    if deltas:
        msg_lines.append("View deltas vs baseline:")
        msg_lines.extend(deltas)
    if regression_warnings:
        msg_lines.append("")
        msg_lines.append(
            "[Warning] Some artists show view-count regression; review ETL/backfill "
            "and update the baseline if this is expected."
        )
        for artist, msg in regression_warnings.items():
            msg_lines.append(f"  • {artist}: {msg}")

    print("\n" + "\n".join(msg_lines))
