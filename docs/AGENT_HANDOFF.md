# Agent Handoff - Story Manifest + Tour Pairing v0

This file organizes the next set of work into clean, PR-sized tasks with a
clear contract between schema, charts, and story. It is designed for a
full-repo agent to execute without guesswork.

## Current Decisions (Locked)
- Streamlit cache TTL: keep `CACHE_TTL_SECONDS` defined above decorators and
  parsed via `_read_int_env`. This avoids decorator NameError at import time.
- Frontend titles: use `<Page> | TrackStats YT` for every page.
- Frontend font stack: keep a single stack across all yt_analytics pages. No
  mixed font sets unless the entire site is intentionally redesigned.
- Dependencies (Streamlit Cloud): prefer a single dependency source. Keep
  `requirements.txt` as the deploy source; treat `pyproject.toml` as dev-only
  or remove it in a dedicated cleanup PR.

## Non-Negotiables
- YouTube Data API v3 only (videos.list + commentThreads.list).
- No revenue, no dislikes, no ISRC claims from public API data.
- Progressive disclosure order: A Trust -> B Performance -> C Audience -> D Levers -> E Appendix.
- All time-series charts must include a range slider + range selector buttons.
- Fail fast on missing columns; no silent fallbacks.
- Notebook execution must be deterministic (nbconvert --execute). Run all cells
  and keep outputs before wiring the frontend.

## Status - Recently Fixed
- Divergent sentiment chart: safe engagement fallbacks, include_lowest=True,
  and true horizontal divergence.
- Views-over-time: require Plotly GO, parse dates before sorting, handle
  invalid dates with a clear placeholder.
- Color map: reject string input and warn on invalid color JSON.
- Duration labels: derive labels from `short_form_threshold`.
- Chart titles: avoid double-counting engagement, handle NaN correlation,
  and remove hardcoded year formatting.

## PR Plan (Next Agent)

### PR-1: Metric Contracts (Correctness Gate)
Goal: prevent silent unit drift and view-column mismatch.

Deliverables:
- New `metrics_contracts.py` with:
  - `resolve_view_col`
  - `normalize_engagement_rate` (fraction 0..1)
  - `weighted_mean`
  - `robust_growth_label`
  - `positive_rate`
- Update summary/growth functions to use the contract.
- Tests covering edge cases (0 baseline, insufficient data, view weights).

### PR-2: Plotly Time-Series Standard
Goal: ensure every timeline is usable.

Deliverables:
- New `plotly_controls.py` with `apply_timeseries_controls(fig)`.
- Apply to all time-series charts.
- Test that rangeslider is visible and buttons exist.

### PR-3: Story Manifest (Schema-Backed Story)
Goal: enforce a deterministic story order with minimum data rules.

Deliverables:
- `story_manifest.py` with card specs:
  - business_question
  - decision_owner (artist/manager/label)
  - required_cols
  - min_rows
  - chart_kind
  - fallback_text
- `render_story(manifest, frames)` to render cards or "inconclusive" blocks.

### PR-4: Tour Pairing ML v0 (Cluster -> Demand -> Confidence Gate)
Goal: provide honest pairing signals, not hype.

Deliverables:
- Artist feature vectors from YouTube-only data:
  - content mix distribution
  - upload cadence
  - normalized views/day
  - engagement density
  - sentiment aggregates (if available)
- Similarity clustering and top-K pair suggestions.
- Ticket demand model using user-provided venue sales:
  - gate results if not better than baseline
  - return recommendations for missing data

## Story Cards (Minimum Set)

Avatar: Artist
- A1 Trust & Coverage (KPI indicators)
- A2 Share of attention (bar with highlight)
- A3 Strongest content proof (bar, normalized)
- A4 Audience voice (keyword bar/treemap)
- A5 Actionable levers (stacked content mix)
- A6 Timing mirror (heatmap; include correlation caveat)
- A7 Human context (text-only card)

Avatar: Manager (3 artists)
- B1 Roster pulse (KPI indicators)
- B2 Share of attention (stacked)
- B3 Win condition per artist (grouped bars)
- B4 Cadence risk (bar + concentration %)
- B5 Release windows (time-series + slider)

Avatar: Label Owner
- C1 Portfolio overview (KPI indicators)
- C2 Concentration risk (top1/top3 share)
- C3 Release readiness (rank table + bars)
- C4 Pipeline mix (stacked)
- C5 Brand language overlap (heatmap)

## Schema Views (SQL Targets)
These should be SQL views or pandas builders that mirror the schema:

- vw_artist_name_resolved
- vw_video_daily_deltas (uses LAG for daily deltas)
- vw_artist_daily
- vw_artist_sentiment_daily

## Code Blocks (Copy/Paste Targets)

### metrics_contracts.py
```python
from __future__ import annotations

from typing import Literal, Sequence

import pandas as pd

ViewCol = Literal["daily_views", "views_per_day", "view_count", "views"]

def require_cols(df: pd.DataFrame, cols: Sequence[str], *, where: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{where}] Missing required columns: {missing}. Present: {list(df.columns)}")

def resolve_view_col(df: pd.DataFrame, *, where: str) -> ViewCol:
    for c in ("views_per_day", "daily_views", "view_count", "views"):
        if c in df.columns:
            return c  # type: ignore[return-value]
    raise ValueError(f"[{where}] No view column found (views_per_day/daily_views/view_count/views).")

def normalize_engagement_rate(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return s
    if float(s.dropna().quantile(0.9)) > 1.5:
        s = s / 100.0
    return s.clip(lower=0, upper=1)

def weighted_mean(values: pd.Series, weights: pd.Series, *, where: str) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").clip(lower=0)
    mask = v.notna() & w.notna() & (w > 0)
    if not mask.any():
        raise ValueError(f"[{where}] weighted_mean has no valid rows.")
    return float((v[mask] * w[mask]).sum() / w[mask].sum())

def positive_rate(sentiment_df: pd.DataFrame, *, where: str = "positive_rate") -> float:
    if sentiment_df.empty or "sentiment_category" not in sentiment_df.columns:
        return float("nan")
    s = sentiment_df["sentiment_category"].dropna().astype(str).str.lower()
    return float(s.eq("positive").mean() * 100.0) if not s.empty else float("nan")

def robust_growth_label(series: pd.Series, *, threshold: float, min_n: int = 10) -> tuple[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < min_n:
        return ("insufficient_data", float("nan"))
    n = len(s)
    k = max(1, int(round(n * 0.2)))
    early = float(s.iloc[:k].median())
    late = float(s.iloc[-k:].median())
    if early <= 0:
        return ("growth" if late > 0 else "stable", float("inf") if late > 0 else 0.0)
    rate = (late - early) / early
    if rate > threshold:
        return ("growth", float(rate))
    if rate < -threshold:
        return ("declining", float(rate))
    return ("stable", float(rate))
```

### plotly_controls.py
```python
from __future__ import annotations

import plotly.graph_objects as go

def apply_timeseries_controls(fig: go.Figure) -> go.Figure:
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=[
                dict(count=7, label="7d", step="day", stepmode="backward"),
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all", label="All"),
            ]
        ),
    )
    return fig
```

### Tour Pairing v0 (Demand + Confidence Gate)
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import numpy as np
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import KFold

@dataclass(frozen=True)
class VenueSale:
    venue_cap: int
    tickets_sold: int

@dataclass(frozen=True)
class DemandModelResult:
    expected_tickets: float
    interval_low: float
    interval_high: float
    model_mae: float
    baseline_mae: float
    skill_score: float
    is_inconclusive: bool
    recommendations: List[str]

def _validate_sales(sales: Iterable[VenueSale], *, where: str) -> List[VenueSale]:
    out: List[VenueSale] = []
    for s in sales:
        if s.venue_cap <= 0:
            raise ValueError(f"[{where}] venue_cap must be > 0")
        if s.tickets_sold < 0:
            raise ValueError(f"[{where}] tickets_sold must be >= 0")
        if s.tickets_sold > s.venue_cap:
            raise ValueError(f"[{where}] tickets_sold cannot exceed venue_cap")
        out.append(s)
    if len(out) < 4:
        raise ValueError(f"[{where}] at least 4 venue sales required for modeling")
    if len(out) > 10:
        raise ValueError(f"[{where}] maximum 10 venue sales allowed")
    return out

def _crossval_mae(X: np.ndarray, y: np.ndarray, model, *, folds: int = 5) -> float:
    k = min(folds, len(y))
    if k < 2:
        return float("nan")
    kf = KFold(n_splits=k, shuffle=True, random_state=42)
    maes = []
    for tr, te in kf.split(X):
        model.fit(X[tr], y[tr])
        pred = model.predict(X[te])
        maes.append(mean_absolute_error(y[te], pred))
    return float(np.mean(maes)) if maes else float("nan")
```

## UI Requirement (Time-Series + Confidence Gate)
- Artist selectors for pair evaluation.
- Up to 10 venue rows per artist: capacity + tickets_sold.
- If model is inconclusive, display:
  "NOT BETTER THAN A BASELINE GUESS - DATA INCONCLUSIVE"
- Always show recommendations for what data to collect next.
