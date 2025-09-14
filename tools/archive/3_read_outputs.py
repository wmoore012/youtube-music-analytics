from __future__ import annotations

from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    executed = repo_root / "notebooks" / "executed"
    if not executed.exists():
        print("No executed notebooks found. Run tools/2_run_notebooks.py first.")
        return
    for p in sorted(executed.glob("*-executed.ipynb")):
        print(p)


if __name__ == "__main__":
    main()
