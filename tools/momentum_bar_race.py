"""Momentum Bar Race color rules and helpers.
This module provides color mapping consistent with the dashboard semantics.
"""
from typing import List

PRE = 55.0
# Align breakout threshold with KPI-22 across dashboards
BRK = 60.0  # breakout when momentum_score >= 60
USE_RED_FOR_BREAKOUT = True

# Palette (avoid red/green pairing)
BLUE = "#1f77b4"
ORNG = "#ff7f0e"
GREY = "#CCCCCC"
RED = "#FF6B6B"


def color_for_score(score: float) -> str:
    if score >= BRK:
        return RED if USE_RED_FOR_BREAKOUT else ORNG
    if score >= PRE:
        return BLUE
    return GREY


def colors_for_scores(scores: List[float]) -> List[str]:
    return [color_for_score(s) for s in scores]

