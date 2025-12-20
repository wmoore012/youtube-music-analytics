#!/usr/bin/env python3
"""
Structural checks for the MusicScope YouTube dashboard notebook.

These tests validate that key sections and expected cells still exist,
without executing notebook code.
"""

from __future__ import annotations

import json
from pathlib import Path


NOTEBOOK_PATH = Path("notebooks/MusicScope_YouTube_Dashboard.ipynb")


def _load_notebook(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _collect_sources(cells: list[dict]) -> list[str]:
    return ["".join(cell.get("source", [])) for cell in cells]


def test_dashboard_notebook_exists() -> None:
    assert NOTEBOOK_PATH.exists(), f"Notebook not found: {NOTEBOOK_PATH}"


def test_dashboard_notebook_has_cells() -> None:
    nb = _load_notebook(NOTEBOOK_PATH)
    cells = nb.get("cells", [])
    assert cells, "Notebook has no cells"
    code_cells = [c for c in cells if c.get("cell_type") == "code"]
    markdown_cells = [c for c in cells if c.get("cell_type") == "markdown"]
    assert code_cells, "Notebook has no code cells"
    assert markdown_cells, "Notebook has no markdown cells"


def test_dashboard_key_sections_present() -> None:
    nb = _load_notebook(NOTEBOOK_PATH)
    sources = _collect_sources(nb.get("cells", []))

    checks = {
        "Config cell": any("THRESHOLDS" in s and "pre_breakout" in s for s in sources),
        "Data loading": any("videos_df" in s and "comments_df" in s for s in sources),
        "Momentum scoring": any("momentum_score" in s and "s_views" in s for s in sources),
        "Episode detection": any("def _episodes" in s or "episodes =" in s for s in sources),
        "KPI-22 chart": any("KPI 22" in s or "KPI-22" in s for s in sources),
    }

    missing = [name for name, found in checks.items() if not found]
    assert not missing, f"Missing expected sections: {', '.join(missing)}"
