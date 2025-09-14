from __future__ import annotations

from pathlib import Path
from typing import Sequence

from run_notebooks import run_notebook


def main(argv: Sequence[str] | None = None) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    editable = repo_root / "notebooks" / "editable"
    executed = repo_root / "notebooks" / "executed"
    editable.mkdir(parents=True, exist_ok=True)
    nbs = sorted(editable.glob("*.ipynb"))
    if not nbs:
        # fallback to legacy location
        legacy = repo_root / "notebooks"
        nbs = [p for p in sorted(legacy.glob("*.ipynb")) if not p.name.endswith("-executed.ipynb")]
        if nbs:
            print("No notebooks in notebooks/editable; running notebooks/ root instead…")
        else:
            print("No notebooks found.")
            return
    for nb in nbs:
        out = run_notebook(nb, executed_dir=executed)
        print(f"executed: {out}")


if __name__ == "__main__":
    main()
