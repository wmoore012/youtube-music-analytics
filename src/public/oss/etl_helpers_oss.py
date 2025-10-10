"""Miscellaneous OSS helpers needed by tests."""
from __future__ import annotations

import re
import threading
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

_ISRC_HYPHEN_RE = re.compile(r"\b([A-Z]{2})-?([A-Z0-9]{3})-?(\d{2})-?(\d{5})\b", re.I)


def extract_isrc_from_text(text: Optional[str]) -> Optional[str]:
    """Extract an ISRC code from free text.

    Normalizes hyphenated and compact forms to canonical 12-char uppercase string.
    """
    if not text:
        return None
    m = _ISRC_HYPHEN_RE.search(text)
    if not m:
        return None
    country, registrant, year, designation = m.groups()
    return f"{country}{registrant}{year}{designation}".upper()


def run_with_timeout(func: Callable[..., T], timeout_seconds: float, *args, **kwargs) -> T:
    """Run `func` with a soft timeout; raise TimeoutError if exceeded.

    Implementation uses a thread and join(timeout). Suitable for test usage.
    """
    result: dict[str, T] = {}
    error: list[BaseException] = []

    def _target():
        try:
            result["value"] = func(*args, **kwargs)  # type: ignore[index]
        except BaseException as e:  # propagate later
            error.append(e)

    th = threading.Thread(target=_target, daemon=True)
    th.start()
    th.join(timeout_seconds)
    if th.is_alive():
        raise TimeoutError("operation timed out")
    if error:
        raise error[0]
    return result["value"]  # type: ignore[index]

