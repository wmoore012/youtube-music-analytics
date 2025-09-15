"""
Lightweight helper package for YouTube analytics notebooks.

Public API:
- utils: filter_artists, safe_head, ensure_cols, ArtistFilter
- charts: views_over_time_plotly, artist_compare_altair, linked_scatter_detail_altair
"""

from .charts import (  # noqa: F401
    artist_compare_altair,
    get_artist_color_map,
    linked_scatter_detail_altair,
    views_over_time_plotly,
)
from .data import (  # noqa: F401
    compute_coengagement_matrix,
    compute_estimated_revenue,
    compute_kpis,
    compute_yoy_views,
    detect_outliers_iqr,
    load_comment_examples,
    load_recent_window_days,
    load_sentiment_daily,
    load_sentiment_summary,
    read_rpm_from_env,
)
from .utils import ArtistFilter, ensure_cols, filter_artists, safe_head  # noqa: F401

__all__ = [
    "ArtistFilter",
    "ensure_cols",
    "filter_artists",
    "safe_head",
    "artist_compare_altair",
    "linked_scatter_detail_altair",
    "views_over_time_plotly",
    "get_artist_color_map",
    # data helpers
    "compute_kpis",
    "detect_outliers_iqr",
    "load_recent_window_days",
    "load_sentiment_daily",
    "load_sentiment_summary",
    "compute_estimated_revenue",
    "compute_yoy_views",
    "load_comment_examples",
    "compute_coengagement_matrix",
    "read_rpm_from_env",
]
