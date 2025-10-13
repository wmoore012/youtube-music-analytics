"""Modular YouTube integration components.

This package exposes stable public APIs extracted from the legacy
web.youtube_integration module to enable incremental refactoring while
preserving backward compatibility.
"""

from .quota import QuotaTracker  # re-export
from .client import get_youtube_client  # re-export

