"""Thin, vendor-neutral SQL helper shims used by tests."""
from __future__ import annotations

import contextlib
from contextlib import contextmanager
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Connection, Engine

# Backing implementation comes from our canonical ETL helpers
from web.etl_helpers import read_sql_safe as _read_sql_safe_impl
from web.etl_helpers import get_engine as _get_engine_impl
from web.etl_helpers import ALL_TABLE_NAMES as _ALL_TABLE_NAMES

# Public constant list used by tests during reflection
ALL_TABLE_NAMES = tuple(_ALL_TABLE_NAMES)

# Internal reflection state (tests poke at these via function __globals__)
_TABLES_INITIALIZED = False
_GLOBAL_META: Optional[MetaData] = None
_TABLE_HANDLES: Dict[str, Table] = {}


def read_sql_safe(sql: str, engine: Engine, **kw) -> pd.DataFrame:
    """Compatibility wrapper around the project implementation."""
    return _read_sql_safe_impl(sql, engine, **kw)


def get_engine(*, schema: Optional[str] = None, echo: bool = False) -> Engine:
    """Return a SQLAlchemy Engine.

    The optional `schema` parameter exists for backward-compatibility with tests
    that assert the call signature. It is ignored by the underlying implementation.
    """
    return _get_engine_impl(echo=echo)


@contextmanager
def get_connection(schema: Optional[str] = None) -> Connection:  # type: ignore[override]
    """Context manager returning a SQLAlchemy Connection.

    Tests expect the yielded value to be the direct result of engine.connect(),
    not the __enter__ of a context-managed Connection.
    """
    engine = get_engine() if schema is None else get_engine(schema=schema)
    conn = engine.connect()  # type: ignore[assignment]
    try:
        yield conn
        try:
            conn.commit()  # type: ignore[attr-defined]
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_tables(engine: Engine) -> None:
    """Reflect selected tables once and cache handles in module globals."""
    global _TABLES_INITIALIZED, _GLOBAL_META, _TABLE_HANDLES
    if _TABLES_INITIALIZED:
        return
    meta = MetaData()
    # Tests patch MetaData(...) and assert reflect called with only=ALL_TABLE_NAMES
    meta.reflect(bind=engine, only=ALL_TABLE_NAMES)
    _GLOBAL_META = meta
    _TABLE_HANDLES = dict(meta.tables)
    _TABLES_INITIALIZED = True


def get_table(name: str) -> Table:
    """Return reflected Table by name after init_tables()."""
    if not _TABLES_INITIALIZED or _GLOBAL_META is None:
        raise RuntimeError("init_tables(engine) must be called once at program start")
    try:
        return _TABLE_HANDLES[name]
    except KeyError as e:
        raise KeyError(
            f"Unknown table '{name}'. Check ALL_TABLE_NAMES in sql_helpers_v2.py."
        ) from e

