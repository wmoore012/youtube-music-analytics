"""Operational health analytics for the YouTube analytics platform."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from typing import List

import pandas as pd
from sqlalchemy import text

from web.db_guard import get_engine

__all__ = [
    "OperationalHealthSnapshot",
    "analyze_operational_health",
    "record_operational_health_snapshot",
]


@dataclass
class OperationalHealthSnapshot:
    """Summary metrics describing operational readiness."""

    data_freshness_hours: float = 0.0
    stale_channels: List[str] = field(default_factory=list)
    coverage_ratio: float = 0.0
    average_daily_views: float = 0.0
    engagement_rate: float = 0.0
    reliability_score: float = 0.0
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        """Serialize the snapshot for reporting."""
        return {
            "data_freshness_hours": round(float(self.data_freshness_hours), 2),
            "stale_channels": list(self.stale_channels),
            "coverage_ratio": round(float(self.coverage_ratio), 2),
            "average_daily_views": round(float(self.average_daily_views), 2),
            "engagement_rate": round(float(self.engagement_rate), 2),
            "reliability_score": round(float(self.reliability_score), 2),
            "notes": list(self.notes),
        }


def _prepare_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise the incoming metrics DataFrame."""
    if raw.empty:
        return raw.copy()

    df = raw.copy()

    if "metrics_date" not in df.columns:
        if "date" in df.columns:
            df = df.rename(columns={"date": "metrics_date"})
        else:
            raise ValueError("metrics_date column is required for operational analytics")

    rename_map = {
        "views": "view_count",
        "likes": "like_count",
        "comments": "comment_count",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for column in ("metrics_date", "fetched_at"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], utc=False, errors="coerce")
            if df[column].dt.tz is None:
                df[column] = df[column].dt.tz_localize(timezone.utc)

    for column in ("view_count", "like_count", "comment_count"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    if "fetched_at" not in df.columns:
        df["fetched_at"] = df["metrics_date"]

    required = {"artist_name", "metrics_date", "view_count", "like_count", "comment_count", "fetched_at"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    return df[list(required)]


def analyze_operational_health(
    metrics: pd.DataFrame,
    *,
    reference_time: datetime | None = None,
    lookback_days: int = 7,
) -> OperationalHealthSnapshot:
    """Compute operational readiness metrics from daily YouTube performance."""
    reference_dt = _coerce_to_utc(reference_time)

    if metrics.empty:
        return OperationalHealthSnapshot(
            notes=[
                "No metrics available for analysis. Populate the warehouse before deployment.",
            ]
        )

    df = _prepare_dataframe(metrics)

    latest_timestamp = df["metrics_date"].max()
    if pd.isna(latest_timestamp):
        return OperationalHealthSnapshot(
            notes=["Metrics timestamps are unavailable; investigate ETL ingestion."],
        )

    data_freshness_hours, latest_dt = _evaluate_freshness(latest_timestamp, reference_dt)
    fresh_artists, stale_artists = _partition_artist_freshness(df, reference_dt, lookback_days)
    coverage_ratio = _calculate_coverage_ratio(fresh_artists, stale_artists)

    average_daily_views = _calculate_average_daily_views(df)
    engagement_rate = _calculate_engagement_rate(df)
    reliability_score = _score_reliability(
        data_freshness_hours,
        coverage_ratio,
        engagement_rate,
    )

    notes = _build_operational_notes(
        data_freshness_hours,
        coverage_ratio,
        engagement_rate,
        stale_artists,
    )

    return OperationalHealthSnapshot(
        data_freshness_hours=data_freshness_hours,
        stale_channels=[str(name) for name in sorted(stale_artists.index)],
        coverage_ratio=coverage_ratio,
        average_daily_views=average_daily_views,
        engagement_rate=engagement_rate,
        reliability_score=reliability_score,
        notes=notes,
    )


def record_operational_health_snapshot(
    snapshot: OperationalHealthSnapshot,
    *,
    lookback_days: int,
    source: str = "operations_monitor",
    engine=None,
    recorded_at: datetime | None = None,
) -> None:
    """Persist an operational health snapshot for future reporting."""
    if not source or not source.strip():
        raise ValueError("source must be a non - empty string")

    engine = engine or get_engine()
    recorded_ts = _coerce_to_utc(recorded_at)
    payload = snapshot.to_dict()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO operational_health_log (
                    recorded_at,
                    source,
                    lookback_days,
                    data_freshness_hours,
                    coverage_ratio,
                    average_daily_views,
                    engagement_rate,
                    reliability_score,
                    stale_channels,
                    notes
                )
                VALUES (
                    :recorded_at,
                    :source,
                    :lookback_days,
                    :freshness,
                    :coverage,
                    :average_views,
                    :engagement,
                    :reliability,
                    :stale_channels,
                    :notes
                )
                """
            ),
            {
                "recorded_at": recorded_ts,
                "source": source.strip(),
                "lookback_days": lookback_days,
                "freshness": payload["data_freshness_hours"],
                "coverage": payload["coverage_ratio"],
                "average_views": payload["average_daily_views"],
                "engagement": payload["engagement_rate"],
                "reliability": payload["reliability_score"],
                "stale_channels": json.dumps(payload["stale_channels"]),
                "notes": json.dumps(payload["notes"]),
            },
        )


def _coerce_to_utc(moment: datetime | None) -> datetime:
    """Return an aware UTC timestamp."""
    if moment is None:
        return datetime.now(timezone.utc)
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _evaluate_freshness(
    latest_timestamp: pd.Timestamp,
    reference_dt: datetime,
) -> tuple[float, datetime]:
    latest_dt = latest_timestamp.to_pydatetime()
    if latest_dt.tzinfo is None:
        latest_dt = latest_dt.replace(tzinfo=timezone.utc)
    freshness_delta = reference_dt - latest_dt
    data_freshness_hours = max(freshness_delta.total_seconds() / 3600.0, 0.0)
    return data_freshness_hours, latest_dt


def _partition_artist_freshness(
    df: pd.DataFrame,
    reference_dt: datetime,
    lookback_days: int,
):
    window_start = reference_dt - timedelta(days=lookback_days)
    latest_by_artist = df.groupby("artist_name")["metrics_date"].max()
    fresh_artists = latest_by_artist[latest_by_artist >= window_start]
    stale_artists = latest_by_artist[latest_by_artist < window_start]
    return fresh_artists, stale_artists


def _calculate_coverage_ratio(fresh_artists: pd.Series, stale_artists: pd.Series) -> float:
    total_artists = len(fresh_artists) + len(stale_artists)
    total = max(total_artists, 1)
    return (len(fresh_artists) / total) * 100.0


def _calculate_average_daily_views(df: pd.DataFrame) -> float:
    daily_views = df.groupby(df["metrics_date"].dt.normalize())["view_count"].sum()
    if daily_views.empty:
        return 0.0
    return float(daily_views.mean())


def _calculate_engagement_rate(df: pd.DataFrame) -> float:
    engagement_series = (df["like_count"] + df["comment_count"]) / df["view_count"].replace({0: pd.NA})
    engagement_series = engagement_series.dropna()
    if engagement_series.empty:
        return 0.0
    return float(engagement_series.mean() * 100.0)


def _score_reliability(
    data_freshness_hours: float,
    coverage_ratio: float,
    engagement_rate: float,
) -> float:
    freshness_component = 1.0 if data_freshness_hours == 0 else max(0.0, 1.0 - min(data_freshness_hours, 72.0) / 72.0)
    coverage_component = min(1.0, coverage_ratio / 100.0)
    engagement_component = min(1.0, engagement_rate / 15.0) if engagement_rate else 0.0
    return (0.4 * freshness_component + 0.4 * coverage_component + 0.2 * engagement_component) * 100.0


def _build_operational_notes(
    data_freshness_hours: float,
    coverage_ratio: float,
    engagement_rate: float,
    stale_artists: pd.Series,
) -> List[str]:
    notes: List[str] = []
    if data_freshness_hours > 24.0:
        notes.append("Data freshness exceeds 24 hours; schedule ETL catch - up run.")
    if coverage_ratio < 80.0:
        notes.append("Artist coverage below 80%; some channels may be missing from ingestion.")
    if not stale_artists.empty:
        stale_list = ", ".join(sorted(str(name) for name in stale_artists.index))
        notes.append(f"Stale artist metrics detected: {stale_list}.")
    if engagement_rate < 8.0:
        notes.append("Engagement rate under 8%; investigate audience sentiment and metadata optimisations.")
    return notes
