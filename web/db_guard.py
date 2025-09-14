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
                logger.warning("Slow query: %.1f ms (>%d)", elapsed, ms)
            return res

        return wrapper

    return deco


# ── Engine factory with kill-switch & RO mode ──────────────────────────────
def get_engine(schema: str, *, ro: bool = False, echo: bool = False) -> Engine:
    """Get SQLAlchemy engine with optional read-only flavor and local fallback.

    Behavior
    - If schema == "icatalog_public": use DATABASE_URL as-is (required).
    - Else build MySQL URL from DB_* env vars.
    - If YT_SQLITE_URL is set (e.g., sqlite:///./.yt_local.db), prefer that for local runs.
    - If ro=True, append read_timeout if missing.

    Notes
    - Keep pool_pre_ping to avoid stale connections.
    - Apply a conservative MAX_EXECUTION_TIME where supported.
    """
    # Prefer explicit override for local dev
    sqlite_url = os.getenv("YT_SQLITE_URL")
    if sqlite_url:
        eng = create_engine(sqlite_url, pool_pre_ping=True, echo=echo, future=True)
        return eng

    # Use existing environment variables
    if schema == "icatalog_public":
        url = os.getenv("DATABASE_URL")
        if not url:
            raise ValueError("DATABASE_URL environment variable not set")
    else:
        # Construct URL from components for private schema
        host = os.getenv("DB_HOST", "127.0.0.1")
        # Respect .env; default to standard local MySQL port and root user
        port = os.getenv("DB_PORT", "3306")
        user = os.getenv("DB_USER", "root")
        password = os.getenv("DB_PASS")
        if not password:
            raise ValueError("DB_PASS environment variable not set")
        db_name = os.getenv("DB_NAME_PRIVATE", "icatalog")
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

    if ro:
        # Check if URL already has query parameters
        separator = "&" if "?" in url else "?"
        if "read_timeout" not in url:
            url += f"{separator}read_timeout=15"

    eng = create_engine(url, pool_pre_ping=True, echo=echo, future=True)
    # Try to set a max execution time where supported; ignore if not
    try:
        with eng.begin() as conn:
            conn.execute(text("SET SESSION MAX_EXECUTION_TIME=5000"))
    except Exception:
        # Non-MySQL engines may not support this session variable
        pass
    return eng
