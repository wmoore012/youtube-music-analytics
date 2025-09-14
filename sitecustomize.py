"""
Ensure project root and src/ are on sys.path without manual PYTHONPATH.

Python auto-imports `sitecustomize` if it exists on sys.path. Since pytest runs
from the repo root, this guarantees local packages like `web` and `icatalogviz`
are importable without extra env setup.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"

# Prepend deterministic paths for import resolution
for p in (str(REPO_ROOT), str(SRC_PATH)):
    if p not in sys.path:
        sys.path.insert(0, p)
