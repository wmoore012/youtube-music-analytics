from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional, Sequence

import nbformat
from dotenv import load_dotenv
from nbconvert.preprocessors import ExecutePreprocessor


def run_notebook(
    path: Path,
    timeout: int = 180,
    kernel_name: str = "python3",
    allow_errors: bool = False,
    allow_error_names: tuple[str, ...] | None = None,
    executed_dir: Optional[Path] = None,
) -> Path:
    nb = nbformat.read(str(path), as_version=4)
    # Ensure repo-local imports work inside the spawned kernel and load .env
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(dotenv_path=repo_root / ".env", override=False)
    extra_paths = [str(repo_root), str(repo_root / "src")]
    current_pp = os.environ.get("PYTHONPATH", "")
    parts = [p for p in current_pp.split(os.pathsep) if p]
    for p in extra_paths:
        if p not in parts:
            parts.insert(0, p)
    os.environ["PYTHONPATH"] = os.pathsep.join(parts)
    ep = ExecutePreprocessor(timeout=timeout, kernel_name=kernel_name)
    # Error policy
    ep.allow_errors = allow_errors
    if allow_error_names:
        # nbconvert forwards to nbclient; attribute present in recent versions
        try:
            ep.allow_error_names = list(allow_error_names)
        except Exception:
            pass
    ep.preprocess(nb, {"metadata": {"path": str(path.parent)}})
    # Save executed notebooks under provided dir or alongside source in 'executed/'
    if executed_dir is None:
        # If source under notebooks/editable/, prefer repo_root/notebooks/executed
        repo_root = Path(__file__).resolve().parents[1]
        if path.parts and "notebooks" in path.parts:
            nb_root = Path(*path.parts[: path.parts.index("notebooks") + 1])
            executed_dir = nb_root / "executed"
        else:
            executed_dir = path.parent / "executed"
    executed_dir.mkdir(parents=True, exist_ok=True)
    out_path = executed_dir / (path.stem + "-executed" + path.suffix)
    nbformat.write(nb, str(out_path))
    return out_path


def main(argv: Sequence[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Execute notebooks with per-cell timeout")
    ap.add_argument("notebooks", nargs="+", help="Notebook paths to run")
    ap.add_argument("--timeout", type=int, default=180, help="Per-cell timeout in seconds")
    ap.add_argument("--allow-errors", action="store_true", help="Allow errors in notebook execution")
    ap.add_argument(
        "--allow-error-name",
        action="append",
        default=None,
        help="Error name to allow (can be repeated)",
    )
    ap.add_argument(
        "--executed-dir",
        default=None,
        help="Directory to write executed notebooks (default notebooks/executed)",
    )
    args = ap.parse_args(argv)
    for nb in args.notebooks:
        out = run_notebook(
            Path(nb),
            timeout=args.timeout,
            allow_errors=args.allow_errors,
            allow_error_names=tuple(args.allow_error_name) if args.allow_error_name else None,
            executed_dir=Path(args.executed_dir) if args.executed_dir else None,
        )
        print(f"executed: {out}")


if __name__ == "__main__":
    main()
