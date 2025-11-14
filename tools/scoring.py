from __future__ import annotations

"""
Scoring utilities for MusicScope notebooks.

Contains robust cross-sectional daily scoring used to compute momentum components.
"""
from typing import Optional
import numpy as np
import pandas as pd

__all__ = [
    "score_component_daily",
]


def score_component_daily(df: pd.DataFrame, col: str, clip_low: float = 0.0, clip_high: float = 100.0) -> pd.Series:
    """Cross-sectional robust score in [0,100] using median/MAD at each date.

    Parameters
    - df: DataFrame for a single date (cross-section) or any grouping context
    - col: column to score
    - clip_low/high: bounds for the score

    Returns: pd.Series of scores aligned with df

    Notes:
    - Uses 1.4826 * MAD to approximate std under normality
    - Falls back safely when MAD==0
    """
    x = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    med = float(np.nanmedian(x.values))
    mad = float(np.nanmedian(np.abs(x.values - med)))
    denom = (1.4826 * mad) if mad > 0 else 1.0
    z = (x - med) / denom
    pct = (z.rank(pct=True) * 100.0).clip(clip_low, clip_high)
    return pct

