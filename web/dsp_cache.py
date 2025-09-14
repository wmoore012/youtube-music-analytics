from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple


def _cache_root() -> Path:
    root = os.getenv("ICATALOG_CACHE_DIR")
    if root:
        return Path(root)
    # default: repo_root/cache
    here = Path(__file__).resolve().parents[1]
    return here / "cache"


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def cache_path(namespace: str, key: str) -> Path:
    base = _cache_root() / namespace
    base.mkdir(parents=True, exist_ok=True)
    # one file per key; embed freshness inside the JSON payload
    safe_key = "".join(ch for ch in key if ch.isalnum() or ch in ("-", "_"))
    return base / f"{safe_key}.json"


def read_daily_json(namespace: str, key: str) -> Tuple[Any | None, bool]:
    """Return (data, is_fresh_today). If file missing or stale, (None, False)."""
    p = cache_path(namespace, key)
    if not p.exists():
        return None, False
    try:
        obj = json.loads(p.read_text())
        if isinstance(obj, dict) and obj.get("cache_date") == _today_utc():
            return obj.get("data"), True
        return None, False
    except Exception:
        return None, False


def write_daily_json(namespace: str, key: str, data: Any) -> Path:
    p = cache_path(namespace, key)
    payload = {"cache_date": _today_utc(), "data": data}
    p.write_text(json.dumps(payload, ensure_ascii=False))
    return p
