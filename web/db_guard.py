# web/db_guard.py
from __future__ import annotations

import logging
import os
from functools import wraps
from time import perf_counter
from typing import Any, Callable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


# ── Latency decorator ───────────────────────────────────────────────────────
def latency_warn(ms: int = 500) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def wrapper(*args: Any, **kw: Any) -> Any:
            t0 = perf_counter()
            res = fn(*args, **kw)
            elapsed = (perf_counter() - t0) * 1000
            if elapsed > ms:
                logger.warning(f"🐢  Slow query: {elapsed:.1f} ms (>{ms})")
            return res

        return wrapper

    return deco


# ── Engine factory with kill-switch & RO mode ──────────────────────────────
def get_engine(schema: str, *, ro: bool = False, echo: bool = False) -> Engine:
    """Get database engine with kill-switch and optional read-only mode."""
    # Use existing environment variables
    if schema == "icatalog_public":
        url = os.getenv("DATABASE_URL")
        if not url:
            raise ValueError("DATABASE_URL environment variable not set")
    else:
        # Construct URL from components
        host = os.getenv("DB_HOST", "127.0.0.1")
        # Respect .env; default to standard local MySQL port and root user
        port = os.getenv("DB_PORT", "3306")
        user = os.getenv("DB_USER", "root")
        password = os.getenv("DB_PASS")
        if not password:
            raise ValueError("DB_PASS environment variable not set")
        db_name = os.getenv("DB_NAME", "yt_proj")
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

    if ro:
        # Check if URL already has query parameters
        separator = "&" if "?" in url else "?"
        if "read_timeout" not in url:
            url += f"{separator}read_timeout=15"

    eng = create_engine(url, pool_pre_ping=True, echo=echo)
    with eng.begin() as conn:
        conn.execute(text("SET SESSION MAX_EXECUTION_TIME=5000"))
    return eng
