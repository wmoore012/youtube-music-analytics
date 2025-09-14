# web/atomic_cache.py
import json
import os
import tempfile
from pathlib import Path

CACHE_FILE = ".registry_cache.json"


def _atomic_write(path: str, data: str) -> None:
    """Atomically write data to file using temp file + rename and fsync directory entry."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = tempfile.NamedTemporaryFile(delete=False, dir=p.parent, mode="w", encoding="utf-8")
    try:
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())  # Force write to disk
        tmp.close()
        os.replace(tmp.name, p)  # atomic on POSIX/NTFS
        # fsync directory entry for durability (best-effort)
        try:
            dir_fd = os.open(str(p.parent), os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass
    except Exception:
        # Cleanup on failure
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)
        raise


def remember(label: str, obj):
    """Store object in cache with atomic write."""
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                cache = json.load(f)
        except (json.JSONDecodeError, IOError):
            cache = {}

    cache[label] = obj
    _atomic_write(CACHE_FILE, json.dumps(cache, indent=2))


def recall(label: str, default=None):
    """Retrieve object from cache."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                return json.load(f).get(label, default)
        except (json.JSONDecodeError, IOError):
            return default
    return default


def clear_cache():
    """Clear all cached data."""
    if os.path.exists(CACHE_FILE):
        os.unlink(CACHE_FILE)
