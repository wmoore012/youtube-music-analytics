import os
from datetime import datetime, timezone

import pytest

from streamlit_app import get_production_health


def _is_db_tests_enabled() -> bool:
    """Return True when database-backed tests should run.

    Mirrors the semantics used in tests/test_etl_data_freshness.py so a fresh
    clone does not require MySQL by default.
    """

    raw = os.getenv("RUN_DB_TESTS")
    if raw is None:
        return False
    return raw not in {"0", "", "false", "False", "off", "OFF"}


@pytest.mark.database
def test_production_warehouse_health() -> None:
    """Validate production-mode warehouse health for Streamlit.

    This test intentionally mirrors the checks enforced in streamlit_app.py
    and the Diagnostics view:

    - DB is reachable using web.etl_helpers.get_engine()
    - Core tables exist and have data
    - Latest metrics_date is within the configured freshness window
    """

    if not _is_db_tests_enabled():
        pytest.skip(
            "[Production Health] Skipping MySQL-backed checks "
            "(set RUN_DB_TESTS=1 to enable).",
        )

    health = get_production_health()

    assert health["db_reachable"], f"Database not reachable: {health['db_error']}"

    videos_rows = int(health["music_videos_rows"])
    artist_rows = int(health["artist_summary_rows"])
    assert videos_rows > 0, "music_videos_normalized table is empty"
    assert artist_rows > 0, "artist_performance_summary table is empty"

    latest_metrics = health["latest_metrics_date"]
    assert latest_metrics is not None, "latest metrics_date is missing from warehouse"

    value = latest_metrics
    if not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    age_days = (datetime.now(timezone.utc) - value).days

    raw = os.getenv("DATA_FRESHNESS_DAYS")
    if raw is None:
        max_age_days = 30
    else:
        try:
            max_age_days = max(1, int(raw))
        except ValueError:
            max_age_days = 30

    assert (
        age_days <= max_age_days
    ), f"Warehouse metrics are stale: {age_days} days old (limit {max_age_days})"
