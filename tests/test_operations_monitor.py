"""Tests for operational health analytics."""

from datetime import datetime

import pandas as pd
import pytest

from src.youtubeviz.operations_monitor import analyze_operational_health


def _build_sample_frame() -> pd.DataFrame:
    """Create deterministic sample metrics for testing."""
    return pd.DataFrame(
        [
            {
                "artist_name": "Artist One",
                "metrics_date": datetime(2024, 5, 7, 0, 0),
                "view_count": 1000,
                "like_count": 120,
                "comment_count": 30,
                "fetched_at": datetime(2024, 5, 7, 8, 0),
            },
            {
                "artist_name": "Artist One",
                "metrics_date": datetime(2024, 5, 6, 0, 0),
                "view_count": 900,
                "like_count": 100,
                "comment_count": 25,
                "fetched_at": datetime(2024, 5, 6, 8, 0),
            },
            {
                "artist_name": "Artist Two",
                "metrics_date": datetime(2024, 5, 2, 0, 0),
                "view_count": 400,
                "like_count": 50,
                "comment_count": 10,
                "fetched_at": datetime(2024, 5, 2, 9, 0),
            },
            {
                "artist_name": "Artist Three",
                "metrics_date": datetime(2024, 4, 20, 0, 0),
                "view_count": 200,
                "like_count": 20,
                "comment_count": 5,
                "fetched_at": datetime(2024, 4, 20, 9, 0),
            },
        ]
    )


def test_operational_health_basic_metrics():
    """Operational analytics should compute intuitive metrics."""
    reference_time = datetime(2024, 5, 8, 0, 0)
    snapshot = analyze_operational_health(_build_sample_frame(), reference_time=reference_time, lookback_days=7)

    assert snapshot.data_freshness_hours == pytest.approx(24.0, abs=1e-6)
    assert snapshot.coverage_ratio == pytest.approx(66.6666, rel=1e-4)
    assert snapshot.average_daily_views == pytest.approx(625.0, abs=1e-6)
    assert snapshot.engagement_rate == pytest.approx(14.0972, rel=1e-4)
    assert "Artist Three" in snapshot.stale_channels
    assert snapshot.reliability_score == pytest.approx(72.2, rel=1e-3)
    assert any("stale" in note.lower() for note in snapshot.notes)


def test_operational_health_handles_empty_frame():
    """Empty datasets should return a neutral snapshot with guidance."""
    snapshot = analyze_operational_health(pd.DataFrame())

    assert snapshot.data_freshness_hours == 0.0
    assert snapshot.coverage_ratio == 0.0
    assert snapshot.reliability_score == 0.0
    assert snapshot.notes, "Expected guidance notes when no data is present"
