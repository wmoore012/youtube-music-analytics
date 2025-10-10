"""ETL SQL helpers – vendor-neutral shim for tests."""
from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine

from web.etl_helpers import read_sql_safe as _read_sql_safe_impl


def read_sql_safe(sql: str, engine: Engine, **kw) -> pd.DataFrame:
    return _read_sql_safe_impl(sql, engine, **kw)

