"""Compatibility wrapper for CI workflows.

GitHub Actions expects `execute_*.py` entrypoints at repo root.
The implementation lives in `scripts/execute_artist_comparison.py`.
"""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    target = Path(__file__).resolve().parent / "scripts" / "execute_artist_comparison.py"
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
