"""Tests for persisting operational health snapshots."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from src.youtubeviz.operations_monitor import (
    OperationalHealthSnapshot,
    record_operational_health_snapshot,
)


def _build_sqlite_engine():
    """Create an in-memory SQLite engine with the logging table."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE operational_health_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at DATETIME NOT NULL,
                    source TEXT NOT NULL,
                    lookback_days INTEGER NOT NULL,
                    data_freshness_hours REAL NOT NULL,
                    coverage_ratio REAL NOT NULL,
                    average_daily_views REAL NOT NULL,
                    engagement_rate REAL NOT NULL,
                    reliability_score REAL NOT NULL,
                    stale_channels TEXT NOT NULL,
                    notes TEXT NOT NULL
                )
                """
            )
        )
    return engine


def test_record_operational_health_snapshot_inserts_row():
    """Snapshots should persist with all metrics for downstream reporting."""
    engine = _build_sqlite_engine()
    snapshot = OperationalHealthSnapshot(
        data_freshness_hours=12.5,
        stale_channels=["Channel A", "Channel B"],
        coverage_ratio=75.0,
        average_daily_views=1200.0,
        engagement_rate=9.5,
        reliability_score=81.2,
        notes=["Investigate Channel B"],
    )

    record_operational_health_snapshot(
        snapshot,
        lookback_days=14,
        source="unit-test",
        engine=engine,
        recorded_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT source, lookback_days, data_freshness_hours, coverage_ratio, stale_channels, notes "
                "FROM operational_health_log"
            )
        ).fetchone()

    assert row is not None
    data = row._mapping
    assert data["source"] == "unit-test"
    assert data["lookback_days"] == 14
    assert data["data_freshness_hours"] == pytest.approx(12.5)
    assert data["coverage_ratio"] == pytest.approx(75.0)

    stale_channels = json.loads(data["stale_channels"])
    assert "Channel A" in stale_channels
    assert json.loads(data["notes"])[0].startswith("Investigate")


def test_record_operational_health_snapshot_requires_source():
    """Empty sources should be rejected to avoid ambiguous log entries."""
    engine = _build_sqlite_engine()
    snapshot = OperationalHealthSnapshot()

    with pytest.raises(ValueError):
        record_operational_health_snapshot(
            snapshot,
            lookback_days=7,
            source="  ",
            engine=engine,
        )
