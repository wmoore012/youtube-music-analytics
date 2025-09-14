from __future__ import annotations

from pathlib import Path
from typing import Sequence

from run_notebooks import run_notebook


def main(argv: Sequence[str] | None = None) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    editable = repo_root / "notebooks" / "editable"
    executed = repo_root / "notebooks" / "executed"
    editable.mkdir(parents=True, exist_ok=True)

    # Preferred: notebooks in editable/
    nbs = sorted(editable.glob("*.ipynb"))
    source = "editable"

    if not nbs:
        # Next: organized notebooks under analysis/ and quality/
        analysis = repo_root / "notebooks" / "analysis"
        quality = repo_root / "notebooks" / "quality"
        nbs = sorted(analysis.glob("*.ipynb")) + sorted(quality.glob("*.ipynb"))
        source = "analysis+quality"

    if not nbs:
        # Fallback to legacy root location
        legacy = repo_root / "notebooks"
        nbs = [p for p in sorted(legacy.glob("*.ipynb")) if not p.name.endswith("-executed.ipynb")]
        source = "legacy-root"

    if not nbs:
        print("No notebooks found.")
        return

    print(f"Running {len(nbs)} notebooks from: {source}")
    for nb in nbs:
        out = run_notebook(nb, executed_dir=executed)
        print(f"executed: {out}")


if __name__ == "__main__":
    main()
