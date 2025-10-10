from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def run_notebook(path: Path, timeout: int = 180, kernel_name: str = "python3") -> Path:
    nb = nbformat.read(str(path), as_version=4)
    ep = ExecutePreprocessor(timeout=timeout, kernel_name=kernel_name)
    ep.preprocess(nb, {"metadata": {"path": str(path.parent)}})
    out_path = path.with_name(path.stem + "-executed" + path.suffix)
    nbformat.write(nb, str(out_path))
    return out_path


def main(argv: Sequence[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Execute notebooks with per-cell timeout")
    ap.add_argument("notebooks", nargs="+", help="Notebook paths to run")
    ap.add_argument("--timeout", type=int, default=180, help="Per-cell timeout in seconds")
    args = ap.parse_args(argv)
    for nb in args.notebooks:
        out = run_notebook(Path(nb), timeout=args.timeout)
        print(f"executed: {out}")


if __name__ == "__main__":
    main()
