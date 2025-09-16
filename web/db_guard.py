# web/db_guard.py
from __future__ import annotations

import logging
import os
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

from dotenv import load_dotenv
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
def get_engine(schema: str | None = None, *, ro: bool = False, echo: bool = False) -> Engine:
    """Get database engine with kill-switch and optional read-only mode."""
    schema_normalized = (schema or "").strip().lower()
    # Best-effort load of .env from repo root if not already present in env
    try:
        repo_root = Path(__file__).resolve().parents[1]
        load_dotenv(dotenv_path=repo_root / ".env", override=False)
    except Exception:
        pass

    database_url = os.getenv("DATABASE_URL")

    if schema_normalized == "icatalog_public":
        url = database_url
        if not url:
            raise ValueError("DATABASE_URL environment variable not set")
    else:
        # Construct URL from components (default to the local analytics database)
        host = os.getenv("DB_HOST", "127.0.0.1")
        port = os.getenv("DB_PORT", "3306")
        user = os.getenv("DB_USER", "root")
        password = os.getenv("DB_PASS")

        if not password:
            if database_url:
                url = database_url
            else:
                raise ValueError("DB_PASS environment variable not set")
        else:
            db_name = os.getenv("DB_NAME", "yt_proj")
            # Allow explicit schema overrides when provided
            if schema_normalized and schema_normalized not in {"public", "icatalog_public"}:
                db_name = schema
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
