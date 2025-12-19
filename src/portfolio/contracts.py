from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Schema contracts for notebook -> dashboard exports.
# These guard column presence and block boolean exports (per project rules).
# ---------------------------------------------------------------------------

# Required columns per table; extra columns are allowed and preserved.
SCHEMA_SPECS: Dict[str, Dict[str, object]] = {
    "momentum_insights": {
        "required": [
            "video_id",
            "artist_name",
            "date",
            "momentum_score",
            "state",
            "warning_days_before",
        ],
        "datetime_cols": ["date"],
        "schema_version": "1.0.0",
    },
    "sentiment_insights": {
        "required": [
            "video_id",
            "artist_name",
            "time_bucket",
            "avg_sentiment_score",
            "net_sentiment_score",
            "comment_volume",
            "sentiment_band",
        ],
        "datetime_cols": ["time_bucket"],
        "schema_version": "1.0.0",
    },
    "performance_insights": {
        "required": [
            "video_id",
            "artist_name",
            "views",
            "engagement_score",
            "engagement_rate",
            "hidden_gem_flag",
            "quartile_bucket",
        ],
        "datetime_cols": [],
        "schema_version": "1.0.0",
    },
    "portfolio_highlights": {
        "required": [
            "highlight_id",
            "category",
            "title",
            "why_it_matters",
            "supporting_table",
        ],
        "datetime_cols": [],
        "schema_version": "1.0.0",
    },
}

TABLE_PARSE_DATES: Dict[str, List[str]] = {
    name: spec.get("datetime_cols", []) for name, spec in SCHEMA_SPECS.items()  # type: ignore[arg-type]
}


@dataclass
class ManifestTableEntry:
    name: str
    path: str
    row_count: int
    schema_version: str


@dataclass
class PortfolioManifest:
    run_id: str
    cohort_slug: str
    generated_at: str
    git_commit: Optional[str] = None
    source_notebook: Optional[str] = None
    input_data_window: Optional[dict] = None
    impact_summary: Optional[dict] = None
    tables: List[ManifestTableEntry] = field(default_factory=list)
    status: str = "success"

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "cohort_slug": self.cohort_slug,
            "generated_at": self.generated_at,
            "git_commit": self.git_commit,
            "source_notebook": self.source_notebook,
            "input_data_window": self.input_data_window,
            "impact_summary": self.impact_summary,
            "tables": [entry.__dict__ for entry in self.tables],
            "status": self.status,
        }


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def schema_for(table_name: str) -> Dict[str, object]:
    if table_name not in SCHEMA_SPECS:
        raise ValueError(f"Unknown table contract: {table_name}")
    return SCHEMA_SPECS[table_name]


def ensure_no_boolean_columns(df: pd.DataFrame, table_name: str) -> None:
    bool_cols = [col for col in df.columns if pd.api.types.is_bool_dtype(df[col])]
    if bool_cols:
        raise ValueError(
            f"{table_name} contains boolean columns {bool_cols}. "
            "Convert them to categorical strings (e.g., 'hidden_gem' | 'normal') "
            "per the project rule to avoid booleans in normalized insight tables."
        )


def validate_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    if df is None:
        raise ValueError(f"{table_name} DataFrame is None")
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{table_name} must be a pandas DataFrame")

    spec = schema_for(table_name)
    required_cols: Iterable[str] = spec.get("required", [])  # type: ignore[assignment]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"{table_name} is missing required columns: {missing}. "
            "Do not drop or simplify the table; include the full schema."
        )

    ensure_no_boolean_columns(df, table_name)
    return df


def manifest_from_meta(
    *,
    run_id: str,
    cohort_slug: str,
    table_entries: List[ManifestTableEntry],
    meta: Optional[dict] = None,
) -> PortfolioManifest:
    meta = meta or {}
    generated_at = meta.get("generated_at") or datetime.now(timezone.utc).isoformat()
    return PortfolioManifest(
        run_id=run_id,
        cohort_slug=cohort_slug,
        generated_at=generated_at,
        git_commit=meta.get("git_commit"),
        source_notebook=meta.get("source_notebook"),
        input_data_window=meta.get("input_data_window"),
        impact_summary=meta.get("impact_summary"),
        tables=table_entries,
        status=meta.get("status", "success"),
    )
