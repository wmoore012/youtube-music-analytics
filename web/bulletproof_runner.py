"""Multiprocessing bullet-proof wrapper for notebook cells."""

from __future__ import annotations

import logging
import multiprocessing as mp
import queue
import time
from dataclasses import dataclass
from typing import Any, Callable


# ────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class CellExecutionResult:
    status: str  # "success" | "timeout" | "error"
    execution_time_ms: int
    result: Any | None = None
    error_message: str | None = None


# ────────────────────────────────────────────────────────────────────────────
def _target(fn: Callable[[], Any], q: mp.Queue[CellExecutionResult], mem_mb: int) -> None:  # pragma: no cover
    """Child process: run *fn*, stream result/error back, enforce RSS cap."""
    import resource
    import sys

    # Try to set memory limit, but don't fail if it's not supported
    try:
        if sys.platform != "darwin":  # Skip on macOS due to system limitations
            rss = mem_mb * 1_048_576  # bytes
            resource.setrlimit(resource.RLIMIT_AS, (rss, rss))
    except (ValueError, OSError):
        pass  # Continue without memory limit

    start = time.time_ns()
    status = "success"
    result: Any | None = None
    err: str | None = None
    try:
        result = fn()
    except Exception as exc:  # noqa: BLE001
        status = "error"
        err = str(exc)
    elapsed = int((time.time_ns() - start) / 1_000_000)
    q.put(CellExecutionResult(status=status, execution_time_ms=elapsed, result=result, error_message=err))


def run_cell_bulletproof(fn: Callable[[], Any], *, timeout_s: int = 60, mem_mb: int = 512) -> CellExecutionResult:
    """Run *fn* in a killable subprocess; return structured result."""
    logger = logging.getLogger("bulletproof_runner")
    q: mp.Queue[CellExecutionResult] = mp.Queue(maxsize=1)
    p = mp.Process(target=_target, args=(fn, q, mem_mb), daemon=True)
    p.start()
    p.join(timeout_s)

    if p.is_alive():
        logger.warning("⏰  Timeout — terminating child process")
        p.terminate()
        p.join(2)
        return CellExecutionResult("timeout", timeout_s * 1000)

    try:
        return q.get_nowait()
    except queue.Empty:
        return CellExecutionResult("error", timeout_s * 1000, error_message="No result")
