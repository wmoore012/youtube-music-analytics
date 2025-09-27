"""
Comprehensive YouTube Analytics and ML Package for Music Industry Applications.

Public API:
- utils: filter_artists, safe_head, ensure_cols, ArtistFilter
- charts: views_over_time_plotly, artist_compare_altair, linked_scatter_detail_altair
- content: Content categorization and analysis functions
- ml_analytics: Advanced ML analytics and predictive modeling
- data: Data loading and computation helpers (lazy loaded to avoid SQLAlchemy deps)
- bulletproof: Chart execution with timeouts and validation
"""

import logging


# Configure notebook - safe logging
def _setup_package_logger():
    """Set up package logger that won't duplicate in notebooks."""
    logger = logging.getLogger("youtubeviz")
    # Clear existing handlers to prevent duplicates
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


_logger = _setup_package_logger()

from .bulletproof import bulletproof_chart, safe_chart_execution  # noqa: F401

# Core imports that don't pull in heavy dependencies
from .utils import ArtistFilter, ensure_cols, filter_artists, safe_head  # noqa: F401


# Lazy loading for modules with heavy dependencies
def __getattr__(name):
    """Lazy load submodules to avoid importing heavy dependencies like SQLAlchemy."""
    if name == "charts":
        from . import charts

        return charts
    elif name == "data":
        from . import data

        return data
    elif name == "content":
        from . import content

        return content
    elif name == "ml_analytics":
        from . import ml_analytics

        return ml_analytics
    elif name == "advanced_charts":
        from . import advanced_charts

        return advanced_charts
    elif name == "clustering_analysis":
        from . import clustering_analysis

        return clustering_analysis
    elif name == "content_analysis":
        from . import content_analysis

        return content_analysis
    elif name == "plugins":
        from . import plugin_integration

        return plugin_integration
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    # Core utilities (always available)
    "ArtistFilter",
    "ensure_cols",
    "filter_artists",
    "safe_head",
    # Bulletproof execution
    "bulletproof_chart",
    "safe_chart_execution",
    # Lazy - loaded modules (access via youtubeviz.charts, etc.)
    "charts",
    "data",
    "content",
    "ml_analytics",
    "advanced_charts",
    "clustering_analysis",
    "content_analysis",
    "plugins",
]
