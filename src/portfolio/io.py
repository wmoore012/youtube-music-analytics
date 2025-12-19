from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

from portfolio.contracts import (
    ManifestTableEntry,
    PortfolioManifest,
    SCHEMA_SPECS,
    TABLE_PARSE_DATES,
    manifest_from_meta,
    validate_dataframe,
)

# ---------------------------------------------------------------------------
# Export root handling
# ---------------------------------------------------------------------------

EXPORT_ROOT_ENV = "MUSICSCOPE_EXPORT_ROOT"
DEFAULT_EXPORT_ROOT = Path("exports/portfolio")


def get_export_root() -> Path:
    env_path = os.getenv(EXPORT_ROOT_ENV)
    if env_path:
        return Path(env_path).expanduser()
    return DEFAULT_EXPORT_ROOT


def _run_dir(root: Path, cohort_slug: str, run_id: str) -> Path:
    return root / cohort_slug / run_id


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def write_insight_table(
    df: pd.DataFrame,
    *,
    root: Optional[Path],
    cohort_slug: str,
    run_id: str,
    name: str,
) -> Path:
    if name not in SCHEMA_SPECS:
        raise ValueError(f"Unknown insight table: {name}")

    validated = validate_dataframe(df, name)
    export_root = root or get_export_root()
    target_dir = _run_dir(export_root, cohort_slug, run_id)
    target_dir.mkdir(parents=True, exist_ok=True)

    path = target_dir / f"{name}.csv"
    validated.to_csv(path, index=False, encoding="utf-8")
    return path


def write_manifest(
    manifest: PortfolioManifest,
    *,
    root: Optional[Path],
    cohort_slug: str,
    run_id: str,
) -> Path:
    export_root = root or get_export_root()
    target_dir = _run_dir(export_root, cohort_slug, run_id)
    target_dir.mkdir(parents=True, exist_ok=True)

    path = target_dir / "manifest.json"
    with path.open("w", encoding="utf-8") as fh:
        json.dump(manifest.to_dict(), fh, indent=2)
    return path


def write_latest_pointer(
    *,
    root: Optional[Path],
    cohort_slug: str,
    run_id: str,
) -> Path:
    export_root = root or get_export_root()
    target_dir = export_root / cohort_slug
    target_dir.mkdir(parents=True, exist_ok=True)
    latest_path = target_dir / "latest.json"
    payload = {"cohort_slug": cohort_slug, "run_id": run_id}
    with latest_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return latest_path


def export_portfolio_run(
    *,
    cohort_slug: str,
    run_id: str,
    dfs: Dict[str, pd.DataFrame],
    meta: Optional[dict] = None,
    root: Optional[Path] = None,
) -> Dict[str, Path]:
    """Validate and write all provided insight tables plus manifest.

    Parameters
    ----------
    cohort_slug: str
        Cohort identifier (snake_case, no spaces).
    run_id: str
        Unique run identifier (e.g., timestamp or UUID).
    dfs: Dict[str, pd.DataFrame]
        Mapping of table name to DataFrame. Only known tables are written; extra
        entries raise to avoid silent drops.
    meta: dict
        Optional metadata for manifest (source_notebook, input_data_window,
        impact_summary, git_commit, generated_at, status).
    root: Path
        Optional override for export root (defaults to env/constant).
    """

    unknown = [name for name in dfs if name not in SCHEMA_SPECS]
    if unknown:
        raise ValueError(f"Unknown tables in export_portfolio_run: {unknown}")

    export_root = root or get_export_root()
    export_root.mkdir(parents=True, exist_ok=True)

    table_entries: list[ManifestTableEntry] = []
    written_paths: Dict[str, Path] = {}

    for name, df in dfs.items():
        path = write_insight_table(df, root=export_root, cohort_slug=cohort_slug, run_id=run_id, name=name)
        written_paths[name] = path
        spec = SCHEMA_SPECS[name]
        row_count = len(df.index)
        table_entries.append(
            ManifestTableEntry(
                name=name,
                path=str(path.relative_to(export_root)),
                row_count=row_count,
                schema_version=str(spec.get("schema_version", "1.0.0")),
            )
        )

    manifest = manifest_from_meta(run_id=run_id, cohort_slug=cohort_slug, table_entries=table_entries, meta=meta)
    manifest_path = write_manifest(manifest, root=export_root, cohort_slug=cohort_slug, run_id=run_id)
    written_paths["manifest"] = manifest_path

    latest_path = write_latest_pointer(root=export_root, cohort_slug=cohort_slug, run_id=run_id)
    written_paths["latest"] = latest_path

    return written_paths


# ---------------------------------------------------------------------------
# Read helpers (used by Streamlit loaders)
# ---------------------------------------------------------------------------


def read_manifest(base_dir: Path, cohort_slug: str, run_id: str) -> dict:
    path = base_dir / cohort_slug / run_id / "manifest.json"
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def resolve_run_id(base_dir: Path, cohort_slug: str) -> Optional[str]:
    latest_path = base_dir / cohort_slug / "latest.json"
    if latest_path.exists():
        try:
            data = json.loads(latest_path.read_text(encoding="utf-8"))
            return data.get("run_id")
        except Exception:
            pass

    cohort_dir = base_dir / cohort_slug
    if not cohort_dir.exists():
        return None
    candidates = [p.name for p in cohort_dir.iterdir() if p.is_dir()]
    if not candidates:
        return None
    return sorted(candidates, reverse=True)[0]


def load_insight_table(base_dir: Path, cohort_slug: str, run_id: str, name: str) -> pd.DataFrame:
    if name not in SCHEMA_SPECS:
        raise ValueError(f"Unknown insight table: {name}")
    path = base_dir / cohort_slug / run_id / f"{name}.csv"
    parse_dates = TABLE_PARSE_DATES.get(name) or None
    df = pd.read_csv(path, parse_dates=parse_dates)
    return validate_dataframe(df, name)
