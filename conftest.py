"""Pytest configuration hooks.

We ensure project root and src/ are importable during test collection without
requiring users to export PYTHONPATH. This mirrors what sitecustomize.py does
at runtime, but guarantees it applies early for pytest collection too.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"

for p in (str(REPO_ROOT), str(SRC_PATH)):
    if p not in sys.path:
        sys.path.insert(0, p)
