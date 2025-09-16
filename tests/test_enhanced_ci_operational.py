"""Operational readiness behaviour for the enhanced CI pipeline."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import src.youtubeviz.data as data_module
from scripts.enhanced_ci import EnhancedCI


@pytest.fixture
def ci_runner() -> EnhancedCI:
    """Create an EnhancedCI instance in report-only mode for isolated tests."""

    return EnhancedCI(report_only=True)


def test_operational_loader_requires_live_metrics(monkeypatch, ci_runner: EnhancedCI) -> None:
    """The loader must fail loudly when the warehouse returns no metrics."""

    def _empty_loader(start: date, end: date) -> pd.DataFrame:  # noqa: D401 - inline helper
        return pd.DataFrame()

    monkeypatch.setattr(data_module, "load_artist_daily_metrics", _empty_loader)

    with pytest.raises(RuntimeError, match="No operational metrics"):
        ci_runner._load_operational_dataframe()


def test_operational_loader_surfaces_etl_failures(monkeypatch, ci_runner: EnhancedCI) -> None:
    """Any warehouse failure must raise a descriptive runtime error."""

    def _broken_loader(start: date, end: date) -> pd.DataFrame:  # noqa: D401 - inline helper
        raise RuntimeError("warehouse unavailable")

    monkeypatch.setattr(data_module, "load_artist_daily_metrics", _broken_loader)

    with pytest.raises(RuntimeError, match="Failed to load operational metrics"):
        ci_runner._load_operational_dataframe()
