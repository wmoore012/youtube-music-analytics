from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

try:
    import plotly.graph_objects as go
except Exception:  # Plotly may be optional in some envs
    go = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Thresholds & palette
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Thresholds:
    pre_breakout: float = 55.0  # blue
    breakout: float = 60.0      # orange/red (configurable)
    legacy: float = 75.0        # historical reference

# Accessible, consistent palette (no red/green pairing)
BLUE = "#1f77b4"   # momentum / scale
ORNG = "#ff7f0e"   # urgency / engagement
PURP = "#7b3294"   # pre-warning window
GREY = "#B0B0B0"   # context
RED  = "#FF6B6B"   # optional hot (avoid green pairing)
TXT  = "#222222"


def state_color(score: float, th: Thresholds, use_red_for_breakout: bool = False) -> str:
    """Map a momentum score to a semantic color.

    - ≥ breakout -> orange (or red if configured)
    - ≥ pre_breakout -> blue
    - otherwise -> grey
    """
    if score >= th.breakout:
        return RED if use_red_for_breakout else ORNG
    if score >= th.pre_breakout:
        return BLUE
    return GREY


# ──────────────────────────────────────────────────────────────────────────────
# Plotly helpers: direct labels & year on axes
# ──────────────────────────────────────────────────────────────────────────────

if go is not None:

    def add_direct_label(fig: go.Figure, trace_idx: int, text: str):
        tr = fig.data[trace_idx]
        if not (hasattr(tr, "x") and hasattr(tr, "y")):
            return fig
        if len(tr.x) == 0:
            return fig
        x_last = tr.x[-1]
        y_last = tr.y[-1]
        fig.add_annotation(
            x=x_last,
            y=y_last,
            text=text,
            showarrow=False,
            xanchor="left",
            yanchor="middle",
            font=dict(color=TXT, size=12),
        )
        return fig

    def ensure_year_on_time_axis(fig: go.Figure, axis: str = "x"):
        fmt = "%b %d %Y"
        fig.update_layout(**{f"{axis}axis": dict(tickformat=fmt)})
        return fig

    def apply_time_format(fig: go.Figure):
        return ensure_year_on_time_axis(fig, "x")


# ──────────────────────────────────────────────────────────────────────────────
# Base-100 normalization & robust outliers (MAD)
# ──────────────────────────────────────────────────────────────────────────────

def base100(series: pd.Series, window: int = 30) -> pd.Series:
    """Return a Base-100 index vs rolling-median baseline.

    - Uses median over a window to be robust to spikes
    - ffill/bfill is intentionally avoided: index is only defined where baseline exists
    - Caps to 99th percentile (min 200) to keep axes readable
    """
    s = pd.to_numeric(series, errors="coerce").astype(float)
    baseline = s.rolling(window, min_periods=7).median().replace(0, np.nan)
    idx = (s / baseline) * 100.0
    if idx.notna().any():
        p99 = float(np.nanpercentile(idx.dropna().values, 99))
    else:
        p99 = 200.0
    return idx.clip(upper=max(p99, 200.0))


def modified_z(series: pd.Series) -> pd.Series:
    """Robust modified Z using median/MAD. |z|>3.5 is a common outlier rule."""
    x = pd.to_numeric(series, errors="coerce")
    med = x.median()
    mad = (x - med).abs().median()
    if mad == 0:
        return pd.Series(0.0, index=x.index)
    return 0.6745 * (x - med) / mad


# ──────────────────────────────────────────────────────────────────────────────
# Diverging 100% bars for sentiment
# ──────────────────────────────────────────────────────────────────────────────

def diverging_sentiment_df(df: pd.DataFrame, pos_col: str = "pos", neu_col: str = "neu", neg_col: str = "neg") -> pd.DataFrame:
    d = df.copy()
    total = d[[pos_col, neu_col, neg_col]].sum(axis=1).replace(0, np.nan)
    d["pos_pct"] = d[pos_col] / total
    d["neg_pct"] = -(d[neg_col] / total)  # negative to the left
    d["neu_pct"] = d[neu_col] / total     # thin grey band in the middle
    return d


def plot_diverging_sentiment(d: pd.DataFrame, y_col: str = "artist_name"):
    if go is None:
        raise RuntimeError("Plotly is required for plot_diverging_sentiment")
    fig = go.Figure()
    fig.add_bar(y=d[y_col], x=d["neg_pct"], name="Negative", orientation="h", marker_color="#8c8c8c")
    fig.add_bar(y=d[y_col], x=d["neu_pct"], name="Neutral",  orientation="h", marker_color="#d9d9d9")
    fig.add_bar(y=d[y_col], x=d["pos_pct"], name="Positive", orientation="h", marker_color=BLUE)
    fig.update_layout(barmode="relative", showlegend=False)
    fig.update_xaxes(tickformat=".0%", zeroline=True)
    return fig


# ──────────────────────────────────────────────────────────────────────────────
# KPI-22 episode engine (contiguity + pre-warning)
# ──────────────────────────────────────────────────────────────────────────────

def compute_kpi22_video_breakouts(md: pd.DataFrame, pre: float = 55, brk: float = 60, cap_hours: int = 720) -> pd.DataFrame:
    """Compute video breakout periods with contiguous pre-warning calculation.

    Parameters
    - md: DataFrame with columns [video_id, date, momentum_score]
    - pre: pre-breakout threshold (default 55)
    - brk: breakout threshold (default 60)
    - cap_hours: cap on pre-warning hours (default 720 = 30 days)

    Returns
    - DataFrame columns: video_id, start, end, duration_days, pre_warning_hours, capped
    """
    md = md.copy()
    md["date"] = pd.to_datetime(md["date"]).dt.floor("D")
    md = md.sort_values(["video_id", "date"])
    out = []
    for vid, g in md.groupby("video_id", sort=False):
        g = g[["date", "momentum_score"]].sort_values("date").set_index("date")
        dates = g.index.tolist()
        in_run, start = False, None
        for i, d in enumerate(dates):
            s = float(g.loc[d, "momentum_score"])
            if s >= brk and not in_run:
                in_run, start = True, d
            elif (s < brk or i == len(dates) - 1) and in_run:
                end = dates[i] if (s >= brk and i == len(dates) - 1) else dates[i - 1]
                # contiguous pre-warning directly before start
                pre_days, step = 0, 1
                while True:
                    prev_day = start - pd.Timedelta(days=step)
                    if prev_day not in g.index:
                        break
                    prev_s = float(g.loc[prev_day, "momentum_score"])
                    if pre <= prev_s < brk:
                        pre_days += 1
                        step += 1
                    else:
                        break
                out.append(
                    {
                        "video_id": vid,
                        "start": start,
                        "end": end,
                        "duration_days": int((end - start).days + 1),
                        "pre_warning_hours": int(min(pre_days * 24, cap_hours)),
                        "capped": pre_days * 24 >= cap_hours,
                    }
                )
                in_run = False
    return pd.DataFrame(out)


def compute_kpi22_episodes(*args, **kwargs):  # deprecated alias
    import warnings
    warnings.warn("Use compute_kpi22_video_breakouts() instead of compute_kpi22_episodes()", DeprecationWarning)
    return compute_kpi22_video_breakouts(*args, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# Matplotlib helpers, accessibility, and preattentive highlighting
# ──────────────────────────────────────────────────────────────────────────────

def ensure_year_on_dates(ax):
    """Ensure year is shown on Matplotlib date axes (x-axis).
    Safe to call repeatedly.
    """
    try:
        from matplotlib.dates import DateFormatter
        ax.xaxis.set_major_formatter(DateFormatter("%b %d %Y"))
    except Exception:
        pass


def label_bars(ax, fmt: str = "{:.0f}", color: Optional[str] = None):
    """Directly label bars on a Matplotlib Axes (horizontal or vertical)."""
    try:
        for p in ax.patches:
            if p.get_width() > 0 and p.get_height() > 0:
                # Heuristic: decide orientation
                horiz = p.get_width() >= p.get_height()
                if horiz:
                    x = p.get_x() + p.get_width()
                    y = p.get_y() + p.get_height() / 2
                    text = fmt.format(p.get_width())
                    ax.text(x, y, f" {text}", va="center", ha="left", color=color or TXT)
                else:
                    x = p.get_x() + p.get_width() / 2
                    y = p.get_y() + p.get_height()
                    text = fmt.format(p.get_height())
                    ax.text(x, y, f" {text}", va="bottom", ha="center", color=color or TXT)
    except Exception:
        pass


def direct_line_label(ax, line, text: str, pad: float = 6):
    """Place a text label at the end of a Matplotlib Line2D."""
    try:
        x = line.get_xdata()[-1]
        y = line.get_ydata()[-1]
        ax.text(x, y, f" {text}", va="center", ha="left", color=TXT, clip_on=False, fontsize=11)
    except Exception:
        pass


# Accessibility helpers (WCAG contrast)

def hex_to_rgb(hex_str: str):
    h = hex_str.lstrip('#')
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def rel_luminance(rgb):
    def _c(c):
        c = c / 255.0
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    r, g, b = rgb
    return 0.2126 * _c(r) + 0.7152 * _c(g) + 0.0722 * _c(b)


def contrast_ratio(fg_hex: str, bg_hex: str) -> float:
    L1 = rel_luminance(hex_to_rgb(fg_hex))
    L2 = rel_luminance(hex_to_rgb(bg_hex))
    L_light, L_dark = (L1, L2) if L1 >= L2 else (L2, L1)
    return (L_light + 0.05) / (L_dark + 0.05)


def assert_contrast(fg_hex: str, bg_hex: str, min_ratio: float = 4.5):
    ratio = contrast_ratio(fg_hex, bg_hex)
    if ratio < min_ratio:
        raise ValueError(f"Contrast {ratio:.2f}:1 is below WCAG AA {min_ratio}:1")


# Titles/text helpers

def clean_title(ax, title: str):
    ax.set_title(title, fontsize=12, color=TXT)
    return ax


def action_title(headline: str, why: Optional[str] = None, action: Optional[str] = None) -> str:
    parts = [headline]
    if why:
        parts.append(f"→ {why}")
    if action:
        parts.append(f"→ {action}")
    return " ".join(parts)


# Preattentive highlighting

def artist_color_map(artists, palette=None):
    palette = palette or [BLUE, ORNG, PURP, "#E6AB02", "#A6761D", "#66A61E", "#7570B3"]
    uniq = list(dict.fromkeys(list(artists)))
    return {a: palette[i % len(palette)] for i, a in enumerate(uniq)}


def highlight_top_n(values, n: int, highlight_color: str = ORNG, context_color: str = GREY):
    if n <= 0:
        return [context_color] * len(values)
    idx = np.argsort(np.array(values))[-n:]
    colors = [context_color] * len(values)
    for i in idx:
        colors[i] = highlight_color
    return colors


def selective_highlight(df, metric_col: str, threshold: float, highlight_color: str = ORNG, context_color: str = GREY):
    vals = df[metric_col].values
    return np.where(vals >= threshold, highlight_color, context_color)

