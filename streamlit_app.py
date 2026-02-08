from __future__ import annotations

import html
import json
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Literal, Mapping

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit_shadcn_ui as ui
from streamlit_echarts import st_echarts
from streamlit_extras.add_vertical_space import add_vertical_space
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_option_menu import option_menu

from web.etl_helpers import get_engine
from youtubeviz.viz_theme import build_color_discrete_map, get_artist_color_palette

# ======================================================================
# IMPORTANT - DO NOT REGRESS (USER-REQUIRED BEHAVIOR)
# ----------------------------------------------------------------------
# 1) In "Production (MySQL)" mode, Streamlit MUST read live MySQL data,
#    not stale CSV snapshots.
# 2) Executive KPI strip must stay operationally actionable (speed + engagement),
#    and must not surface pseudo-finance metrics.
# 3) If short-form/format labels are shown, keep labels simple and derivation explicit.
# 4) Never override artist stylistic casing choices automatically.
# 5) KPI deltas shown in green/red must have matching arithmetic + actions;
#    otherwise hide the delta.
# 6) In Streamlit Cloud, do not auto-enter Production mode from repo/env
#    DB_* values alone; require explicit secrets/session intent.
# 7) Demo cohort JSON must be loaded with json.load (not pandas read_json->to_dict),
#    because pandas reshapes nested JSON and can crash artist/video loops.
# 8) Zero likes/comments with non-zero videos must trigger visible data-check warnings
#    and regression tests before deployment.
# 9) Snapshot freshness in Production must follow ETL heartbeat (youtube_etl_runs):
#    a run completed today can be "fresh" even when no brand-new videos were added.
# 10) Executive rollout view must answer "what do we do today" with
#     last-10 release windows + simple lift arithmetic (official vs other).
# 11) Artist Deep Dive must feel premium: focus artist highlighted with assigned
#     color, benchmark context de-emphasized in grayscale, subtle micro-motion.
# ======================================================================

CACHE_TTL_SECONDS = 900  # 15 minutes
DATA_FRESHNESS_DAYS_ENV = "DATA_FRESHNESS_DAYS"
DEFAULT_DATA_FRESHNESS_DAYS = 30
SHORT_VIDEO_MAX_SECONDS = 60
SHORT_VIDEO_LABEL = "Short video (<60s)"
NEW_ENTRY_VIEWS_PER_DAY_FLOOR = 100.0
DATA_MODE_SETTING_KEYS = ("MUSICSCOPE_DATA_MODE", "TRACKSTATS_DATA_MODE")
OFFICIAL_RELEASE_TYPES = frozenset({"Official Music Video", "Official Audio", "Lyric Video"})
HEX_COLOR_RE = re.compile(r"^#(?:[0-9A-Fa-f]{6}|[0-9A-Fa-f]{3})$")
COMMENT_WATCHLIST_MIN_VIEWS = 500
COMMENT_WATCHLIST_MIN_COMMENTS = 3
COMMENT_WATCHLIST_MIN_LIKES = 3
COMMENT_WATCHLIST_MIN_LIFT = 1.35
LOGGER = logging.getLogger(__name__)
APP_RED_700 = "#7A1F2B"
APP_RED_600 = "#8B2635"
APP_RED_500 = "#A3262A"
APP_RED_100 = "#FBE9E8"
APP_RED_050 = "#FFF5F4"
APP_BENCHMARK_GRAY = "#C7CBD4"
APP_BENCHMARK_GRAY_DARK = "#B4B9C3"
# IMPORTANT / DO NOT REGRESS:
# Keep one global artist palette source for the full app so styling remains
# consistent across Overview, Deep Dive, Velocity, and callout components.
GLOBAL_ARTIST_PALETTE = get_artist_color_palette()


@dataclass(frozen=True)
class CommentSignalSpec:
    """Arithmetic-backed rule used to flag unusual comment behavior."""

    metric_key: str
    reason_key: str
    reason_label: str
    arithmetic_label: str
    min_like_count: int = 0


COMMENT_SIGNAL_SPECS: tuple[CommentSignalSpec, ...] = (
    CommentSignalSpec(
        metric_key="comments_per_1k_views",
        reason_key="comments per 1k views spike",
        reason_label="Comments are unusually high for the view level.",
        arithmetic_label="(comments / views) x 1,000",
    ),
    CommentSignalSpec(
        metric_key="comments_per_like",
        reason_key="comments per like spike",
        reason_label="Comments are unusually high compared with likes.",
        arithmetic_label="comments / likes",
        min_like_count=COMMENT_WATCHLIST_MIN_LIKES,
    ),
    CommentSignalSpec(
        metric_key="views_per_comment",
        reason_key="views per comment spike",
        reason_label="Views are unusually high per comment (investigate passive viewing).",
        arithmetic_label="views / comments",
    ),
)

try:
    # Disable on_hover_tabs due to local loading issues (assets not found)
    # from st_on_hover_tabs import on_hover_tabs
    on_hover_tabs = None
except ModuleNotFoundError:  # pragma: no cover - external dependency guard
    on_hover_tabs = None

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "music_analysis_tables"
DEMO_DATA_PATH = BASE_DIR / "demo_data" / "curated_cohort.json"
EXPECTED_ARTISTS_PATH = BASE_DIR / "config" / "expected_artists.json"
ARTIST_ALIASES_PATH = BASE_DIR / "config" / "artist_aliases.json"
ARTIST_ALIAS_OVERRIDES = {
    "hicorook": "Corook",
    "@hicorook": "Corook",
}


def _cache_key_for_path(path: Path) -> tuple[str, int]:
    """Return a stable cache key that changes when file mtime changes."""

    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        mtime_ns = -1
    return str(path), mtime_ns


def _normalize_optional_text(value: object) -> str:
    """Convert scalar-ish values to text while dropping common missing sentinels."""

    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):  # scalar NaN/NA/NaT
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text.casefold() in {"", "nan", "<na>", "none", "null"}:
        return ""
    return text


@st.cache_data(show_spinner=False)
def _load_expected_artists_cached(path_text: str, _mtime_ns: int) -> list[str]:
    path = Path(path_text)
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except PermissionError as exc:
        LOGGER.warning("Cannot read expected artists config '%s': %s", path.name, exc)
        return []
    except OSError as exc:
        LOGGER.warning("OS error reading expected artists config '%s': %s", path.name, exc)
        return []

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        LOGGER.warning("Could not parse expected artists config '%s': %s", path.name, exc)
        return []

    names = payload.get("expected_artists") if isinstance(payload, dict) else None
    if not isinstance(names, list):
        return []
    return [str(name).strip() for name in names if str(name).strip()]


@st.cache_data(show_spinner=False)
def _load_expected_artists() -> list[str]:
    """Load expected artist roster from config, when available."""

    return _load_expected_artists_cached(*_cache_key_for_path(EXPECTED_ARTISTS_PATH))


@st.cache_data(show_spinner=False)
def _load_artist_aliases_cached(path_text: str, _mtime_ns: int) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    path = Path(path_text)

    payload: object = {}
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raw_text = ""
    except PermissionError as exc:
        LOGGER.warning("Cannot read artist aliases config '%s': %s", path.name, exc)
        raw_text = ""
    except OSError as exc:
        LOGGER.warning("OS error reading artist aliases config '%s': %s", path.name, exc)
        raw_text = ""

    if raw_text:
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            LOGGER.warning("Could not parse artist aliases config '%s': %s", path.name, exc)
            payload = {}

    if isinstance(payload, dict):
        for alias, canonical in payload.items():
            alias_text = str(alias).strip()
            canonical_text = str(canonical).strip()
            if alias_text and canonical_text:
                alias_map[alias_text.casefold()] = canonical_text
    for alias, canonical in ARTIST_ALIAS_OVERRIDES.items():
        alias_map[alias.casefold()] = canonical
    return alias_map


@st.cache_data(show_spinner=False)
def _load_artist_aliases() -> dict[str, str]:
    """Load artist aliases (case-insensitive key map)."""

    return _load_artist_aliases_cached(*_cache_key_for_path(ARTIST_ALIASES_PATH))


def _build_artist_name_index(
    *,
    palette: Mapping[str, str],
    expected_artists: list[str],
    alias_map: Mapping[str, str],
) -> dict[str, str]:
    """Build case-insensitive canonical-name lookup index."""

    index: dict[str, str] = {}
    for artist_name in palette.keys():
        text = str(artist_name).strip()
        if text:
            index[text.casefold()] = text
    for artist_name in alias_map.values():
        text = str(artist_name).strip()
        if text:
            index[text.casefold()] = text
    for artist_name in expected_artists:
        text = str(artist_name).strip()
        if text:
            index[text.casefold()] = text
    return index


def _canonicalize_artist_name(
    value: object,
    *,
    alias_map: Mapping[str, str],
    name_index: Mapping[str, str],
) -> str:
    """Return canonical artist display name while preserving stylistic casing."""

    raw = _normalize_optional_text(value)
    if not raw:
        return ""
    alias_canonical = alias_map.get(raw.casefold(), raw)
    return name_index.get(alias_canonical.casefold(), alias_canonical)


def normalize_artist_dimension(
    df: pd.DataFrame,
    *,
    palette: Mapping[str, str],
    drop_untracked: bool = True,
) -> tuple[pd.DataFrame, list[str]]:
    """Canonicalize artist names and optionally hide untracked labels.

    IMPORTANT / DO NOT REGRESS:
    - Merge case-only splits (e.g., "Cobrah" + "COBRAH") into one canonical name.
    - Keep artist stylistic casing from config.
    - Filter obvious junk labels (e.g., accidental channel names) from dashboard
      without deleting source data from storage.
    """

    if df.empty or "artist_name" not in df.columns:
        return df, []

    expected_artists = _load_expected_artists()
    alias_map = _load_artist_aliases()
    name_index = _build_artist_name_index(
        palette=palette,
        expected_artists=expected_artists,
        alias_map=alias_map,
    )

    typed = df.copy()
    typed["artist_name"] = typed["artist_name"].map(
        lambda name: _canonicalize_artist_name(name, alias_map=alias_map, name_index=name_index)
    )
    typed = typed.loc[typed["artist_name"] != ""].copy()

    tracked_roster = expected_artists if expected_artists else sorted(set(str(name) for name in palette.keys()))
    tracked_keys = {name.casefold() for name in tracked_roster}
    unknown_artists = sorted(
        {
            str(name)
            for name in typed["artist_name"].dropna().astype(str).tolist()
            if str(name).strip() and str(name).casefold() not in tracked_keys
        }
    )

    if drop_untracked and unknown_artists:
        typed = typed.loc[~typed["artist_name"].isin(unknown_artists)].copy()

    return typed, unknown_artists


def _read_int_env(name: str, default: int) -> int:
    """Read an integer from environment, with validation.

    Used for cache TTLs and other simple numeric tuning knobs. Fails fast with
    a clear error message if misconfigured so Cloud logs are actionable.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:  # pragma: no cover - defensive config guard
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise RuntimeError(f"{name} must be >= 0, got {value}")
    return value


def read_int_env(name: str, default: int) -> int:
    """Public typed wrapper for integer env parsing."""

    return _read_int_env(name, default)


def _read_float_env(name: str, default: float) -> float:
    """Read a float from environment, with validation."""

    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:  # pragma: no cover - defensive config guard
        raise RuntimeError(f"{name} must be a float, got {raw!r}") from exc
    if not math.isfinite(value):
        raise RuntimeError(f"{name} must be a finite float, got {raw!r}")
    if value < 0:
        raise RuntimeError(f"{name} must be >= 0, got {value}")
    return value


def read_float_env(name: str, default: float) -> float:
    """Public typed wrapper for float env parsing."""

    return _read_float_env(name, default)


def _sanitize_hex_color(color: str | None, fallback: str = APP_RED_500) -> str:
    """Return a safe CSS hex color or fallback."""

    candidate = (color or "").strip()
    if not HEX_COLOR_RE.fullmatch(candidate):
        return fallback
    if len(candidate) == 4:
        candidate = "#" + "".join(ch * 2 for ch in candidate[1:])
    return candidate.upper()


def sanitize_hex_color(color: str | None, fallback: str = APP_RED_500) -> str:
    """Public wrapper for safe CSS hex colors."""

    return _sanitize_hex_color(color, fallback=fallback)


def _hex_color_to_rgb_csv(color: str) -> str:
    """Convert '#RRGGBB' into 'R, G, B' string for CSS rgba()."""

    safe_color = _sanitize_hex_color(color)
    red = int(safe_color[1:3], 16)
    green = int(safe_color[3:5], 16)
    blue = int(safe_color[5:7], 16)
    return f"{red}, {green}, {blue}"


def hex_color_to_rgb_csv(color: str) -> str:
    """Public wrapper for RGB CSS tuple conversion."""

    return _hex_color_to_rgb_csv(color)


def inject_dashboard_motion_styles() -> None:
    """Inject subtle, premium motion + accent styling for Streamlit widgets."""

    accent_rgb = _hex_color_to_rgb_csv(APP_RED_500)
    accent_rgb_700 = _hex_color_to_rgb_csv(APP_RED_700)
    css = """
        <style>
          :root {
            --yt-red-700: __APP_RED_700__;
            --yt-red-600: __APP_RED_600__;
            --yt-red-500: __APP_RED_500__;
            --yt-red-100: __APP_RED_100__;
            --yt-red-050: __APP_RED_050__;
            --ink-900: #111827;
            --ink-600: #4B5563;
            --benchmark-gray: __APP_BENCHMARK_GRAY__;
          }
          @keyframes focusRise {
            from { opacity: 0; transform: translateY(8px); }
            to { opacity: 1; transform: translateY(0); }
          }
          [data-testid="stSelectbox"] > div[data-baseweb="select"] {
            border: 1px solid rgba(__ACCENT_RGB_700__, 0.40);
            border-radius: 14px;
            transition: border-color .2s ease, box-shadow .2s ease, transform .2s ease;
            background: linear-gradient(180deg, var(--yt-red-050) 0%, #FFFFFF 100%);
          }
          [data-testid="stSelectbox"] > div[data-baseweb="select"]:focus-within {
            border-color: var(--yt-red-500);
            box-shadow: 0 0 0 3px rgba(__ACCENT_RGB__, 0.18);
            transform: translateY(-1px);
          }
          .focus-artist-hero {
            margin: 0 0 12px 0;
            padding: 14px 16px;
            border-radius: 14px;
            border: 1px solid rgba(var(--focus-rgb), 0.45);
            background: linear-gradient(120deg, rgba(var(--focus-rgb), 0.12), rgba(var(--focus-rgb), 0.04));
            animation: focusRise .32s ease-out;
          }
          .focus-artist-label {
            color: var(--ink-600);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }
          .focus-artist-name {
            margin-top: 3px;
            color: var(--focus-color);
            font-size: clamp(1.45rem, 2.4vw, 2rem);
            line-height: 1.1;
            font-weight: 800;
            letter-spacing: 0.03em;
          }
          .focus-artist-color-chip {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            margin-top: 10px;
            color: var(--ink-900);
            font-size: 0.9rem;
            font-weight: 600;
          }
          .focus-artist-color-chip .swatch {
            width: 14px;
            height: 14px;
            border-radius: 999px;
            background: var(--focus-color);
            box-shadow: 0 0 0 2px rgba(var(--focus-rgb), 0.20);
          }
          .benchmark-context {
            margin: 8px 0 14px 0;
            padding: 10px 12px;
            border-left: 4px solid var(--benchmark-gray);
            border-radius: 10px;
            background: linear-gradient(90deg, #F3F4F6 0%, #F9FAFB 100%);
            color: #374151;
            animation: focusRise .28s ease-out;
          }
          [data-testid="stPlotlyChart"] {
            animation: focusRise .36s ease-out;
          }
        </style>
    """
    css = (
        css.replace("__APP_RED_700__", APP_RED_700)
        .replace("__APP_RED_600__", APP_RED_600)
        .replace("__APP_RED_500__", APP_RED_500)
        .replace("__APP_RED_100__", APP_RED_100)
        .replace("__APP_RED_050__", APP_RED_050)
        .replace("__APP_BENCHMARK_GRAY__", APP_BENCHMARK_GRAY)
        .replace("__ACCENT_RGB__", accent_rgb)
        .replace("__ACCENT_RGB_700__", accent_rgb_700)
    )
    st.markdown(
        css,
        unsafe_allow_html=True,
    )


def build_focus_artist_header_html(focus_artist: str, focus_color: str) -> str:
    """Return premium focus-artist header HTML with safe color rendering."""

    safe_artist = html.escape(focus_artist)
    safe_color = _sanitize_hex_color(focus_color)
    rgb_csv = _hex_color_to_rgb_csv(safe_color)
    return (
        f"<div class='focus-artist-hero' style='--focus-color:{safe_color}; --focus-rgb:{rgb_csv};'>"
        "<div class='focus-artist-label'>Focus Artist</div>"
        f"<div class='focus-artist-name'>{safe_artist}</div>"
        f"<div class='focus-artist-color-chip'><span class='swatch'></span>Assigned color: {safe_color}</div>"
        "</div>"
    )


def render_focus_artist_header(focus_artist: str, focus_color: str) -> None:
    """Render focus-artist display in bold, color-coded type."""

    st.markdown(build_focus_artist_header_html(focus_artist, focus_color), unsafe_allow_html=True)


def render_benchmark_context_note(focus_artist: str) -> None:
    """Explain that peers are context, not the primary narrative."""

    safe_artist = html.escape(focus_artist)
    st.markdown(
        (
            "<div class='benchmark-context'>"
            f"<strong>{safe_artist}</strong> is the story. Benchmark artists are context only "
            "and intentionally shown in grayscale."
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _parse_iso8601_duration_seconds(duration: object) -> int | None:
    """Parse YouTube ISO-8601 duration string (e.g. PT3M12S) into seconds."""

    if duration is None or pd.isna(duration):
        return None
    text = str(duration).strip().upper()
    if not text:
        return None

    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", text)
    if match is None:
        return None

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_streamlit_cloud_runtime() -> bool:
    """Best-effort detection of Streamlit Community Cloud runtime.

    Uses known cloud env flags first (robust), then falls back to the historical
    `/mount/src/` working-directory pattern as a compatibility safeguard (brittle).
    """

    if _is_truthy(os.getenv("STREAMLIT_SERVER_RUNNING_IN_CLOUD")):
        return True
    if _is_truthy(os.getenv("STREAMLIT_CLOUD")):
        return True

    cwd = str(Path.cwd())
    return cwd.startswith("/mount/src/")


def _normalize_data_mode(raw_value: str | None) -> Literal["demo", "production"] | None:
    """Normalize optional run-mode text to a supported literal value."""

    if raw_value is None:
        return None
    mode = raw_value.strip().lower()
    if not mode:
        return None
    if mode == "demo":
        return "demo"
    if mode == "production":
        return "production"

    st.error(
        "Invalid data mode value. Use 'demo' or 'production' for MUSICSCOPE_DATA_MODE / TRACKSTATS_DATA_MODE.",
    )
    st.stop()
    return None


def _get_requested_data_mode(*, allow_env: bool) -> Literal["demo", "production"] | None:
    """Read an explicitly requested data mode from settings."""

    for key in DATA_MODE_SETTING_KEYS:
        mode = _normalize_data_mode(_get_db_setting(key, allow_env=allow_env))
        if mode is not None:
            return mode
    return None


def _classify_video_type_from_duration(duration: object) -> str:
    """Classify short-form vs other content from duration only.

    IMPORTANT / DO NOT REGRESS:
    We intentionally label only what duration arithmetic can prove.
    We do not claim YouTube product-level "Shorts" distribution.
    """

    seconds = _parse_iso8601_duration_seconds(duration)
    if seconds is None:
        return "Other Content"
    if seconds <= SHORT_VIDEO_MAX_SECONDS:
        return SHORT_VIDEO_LABEL
    return "Other Content"


def _display_video_type(video_type: object) -> str:
    """Normalize raw type labels to a small, understandable set."""

    if video_type is None:
        return "Other Content"
    text = str(video_type).strip()
    if not text:
        return "Other Content"
    normalized = text.lower()

    if normalized in {"short", "shorts", "short / reel", "reel", "reels", "youtube short"}:
        return SHORT_VIDEO_LABEL
    if normalized in {"official music video", "music video", "mv"}:
        return "Official Music Video"
    if "official audio" in normalized or normalized == "audio":
        return "Official Audio"
    if "lyric" in normalized:
        return "Lyric Video"
    if "live" in normalized:
        return "Live Performance"
    if normalized in {"music content", "video", "unknown", "nan", "none"}:
        return "Other Content"
    return "Other Content"


def _classify_video_type(*, duration: object, title: object, raw_video_type: object = None) -> str:
    """Classify display label using explicit metadata and duration arithmetic."""

    explicit_label = _display_video_type(raw_video_type)
    if explicit_label != "Other Content":
        return explicit_label

    title_text = str(title or "").strip().lower()
    if "official music video" in title_text:
        return "Official Music Video"
    if "official audio" in title_text:
        return "Official Audio"
    if "lyric video" in title_text:
        return "Lyric Video"
    return _classify_video_type_from_duration(duration)


def _coerce_float(value: object, default: float = 0.0) -> float:
    """Return a finite float or *default* for malformed values."""

    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(candidate):
        return default
    return candidate


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    """Return ratio when denominator is positive and finite; otherwise None."""

    if not (math.isfinite(numerator) and math.isfinite(denominator)):
        return None
    if denominator <= 0:
        return None
    value = numerator / denominator
    if not math.isfinite(value):
        return None
    return value


def _youtube_watch_url(video_id: object) -> str:
    """Return canonical YouTube watch URL for a video id, or empty string."""

    video_text = _normalize_optional_text(video_id)
    if not video_text:
        return ""
    return f"https://www.youtube.com/watch?v={video_text}"


def _youtube_thumbnail_url(video_id: object) -> str:
    """Return YouTube HQ thumbnail URL for a video id, or empty string."""

    video_text = _normalize_optional_text(video_id)
    if not video_text:
        return ""
    return f"https://i.ytimg.com/vi/{video_text}/hqdefault.jpg"


def _coerce_timestamp(value: object) -> pd.Timestamp:
    """Parse a timestamp and return timezone-naive UTC; NaT when invalid."""

    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return pd.NaT
    return ts.tz_convert(None)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def _load_csv(path: Path, parse_dates: Iterable[str] | None = None) -> pd.DataFrame:
    """Load a CSV with basic error handling visible in the UI.

    Used when running directly from ETL-exported tables. In demo mode we
    instead hydrate DataFrames from ``demo_data/curated_cohort.json``.
    """
    if not path.exists():
        st.error(f"Missing data file: {path.name}. Run the ETL pipeline to generate it.")
        st.stop()
    try:
        return pd.read_csv(path, parse_dates=list(parse_dates) if parse_dates else None)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not read {path.name}: {exc}")
        st.stop()
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def _load_demo_cohort() -> dict[str, object]:
    """Load curated demo cohort JSON.

    Fails loudly with a friendly message if the file is missing or invalid
    so industry professionals can still understand what's wrong.
    """

    if not DEMO_DATA_PATH.exists():
        st.error(
            "Demo data is missing. Expected curated demo cohort at "
            f"{DEMO_DATA_PATH}. This file should ship with the repository."
        )
        st.stop()
    try:
        with DEMO_DATA_PATH.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except json.JSONDecodeError as exc:
        st.error(f"Demo data is not valid JSON: {exc}")
        st.stop()

    if not isinstance(payload, dict):
        st.error("Demo data must be a JSON object with an 'artists' field.")
        st.stop()
    return payload


def _coerce_record_list(value: object) -> list[dict[str, object]]:
    """Normalize JSON arrays/maps into a list of record dicts.

    Accepts list[dict] and dict[index->dict] layouts to stay backward-compatible
    with older transformed demo payloads while skipping malformed elements.
    """

    source_items: list[object]
    if isinstance(value, list):
        source_items = value
    elif isinstance(value, dict):
        source_items = list(value.values())
    else:
        return []

    records: list[dict[str, object]] = []
    for item in source_items:
        if isinstance(item, dict):
            records.append(item)
    return records


def _get_db_setting(name: str, *, allow_env: bool = True) -> str | None:
    """Lookup a DB_* setting from Streamlit secrets, session_state, or env.

    This keeps resolution consistent anywhere we need database credentials
    and avoids subtle differences between demo and production modes.
    """

    try:
        if name in st.secrets:
            value = st.secrets[name]
            if value:
                return str(value)
    except Exception:  # noqa: BLE001 - gracefully handle missing secrets.toml
        pass

    if name in st.session_state:
        value = st.session_state[name]
        if value:
            return str(value)
    if allow_env:
        env_value = os.getenv(name)
        if env_value:
            return env_value
    return None


def _sync_db_settings_to_env(*, allow_env: bool = False) -> None:
    """Mirror DB settings from Streamlit secrets/session into process env.

    web.etl_helpers.get_engine() reads only os.environ. In Streamlit Cloud,
    secrets may exist in st.secrets without being exported to environment
    variables. This sync keeps engine behavior consistent across local + cloud.
    """

    keys = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME")
    for key in keys:
        value = _get_db_setting(key, allow_env=allow_env)
        if value is None:
            continue
        os.environ[key] = value


def get_data_mode() -> Literal["demo", "production"]:
    """Detect whether to use demo data or production MySQL.

    Behaviour:
    - If **no** DB_* settings are present, stay safely in demo mode.
    - If **some** DB_* settings are present but required ones are missing,
      fail loudly with a clear error instead of silently downgrading.
    - If all required settings are present but the connection fails,
      show a helpful error with details and stop the app.
    """

    from sqlalchemy import text  # Imported lazily to keep imports light

    required_keys = ["DB_HOST", "DB_USER", "DB_PASS", "DB_NAME"]
    # In Streamlit Cloud we ignore raw env DB_* by default because a checked-in
    # .env (or inherited env) can accidentally force broken localhost Production
    # mode. Override with MUSICSCOPE_ALLOW_ENV_DB=1 if needed.
    cloud_runtime = _is_streamlit_cloud_runtime()
    allow_env_db = not cloud_runtime or _is_truthy(os.getenv("MUSICSCOPE_ALLOW_ENV_DB"))
    requested_mode = _get_requested_data_mode(allow_env=allow_env_db)
    if requested_mode == "demo":
        return "demo"

    # Cloud-safe default: unless production is explicitly requested, stay in demo
    # mode even if DB_* is present in inherited environment variables.
    if cloud_runtime and requested_mode is None:
        return "demo"

    any_present = any(_get_db_setting(key, allow_env=allow_env_db) is not None for key in required_keys)
    if not any_present:
        if requested_mode == "production":
            st.error(
                "Production (MySQL) mode was explicitly requested, but DB settings are missing.",
            )
            st.info(
                "Set DB_HOST, DB_USER, DB_PASS, and DB_NAME (via Streamlit secrets "
                "or session state). Then keep MUSICSCOPE_DATA_MODE=production.",
            )
            st.stop()
        # No DB intent configured -> stay in demo mode using curated cohort.
        return "demo"

    missing = [key for key in required_keys if _get_db_setting(key, allow_env=allow_env_db) is None]
    if missing:
        st.error(
            "Production (MySQL) mode was requested, but these settings are missing: " + ", ".join(missing),
        )
        st.info(
            "Set DB_HOST, DB_USER, DB_PASS, and DB_NAME via Streamlit secrets, "
            "environment variables, or st.session_state before running in "
            "Production mode. Without them, MusicScope runs safely in Demo mode.",
        )
        st.stop()

    try:
        _sync_db_settings_to_env(allow_env=allow_env_db)
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return "production"
    except Exception as exc:  # noqa: BLE001
        st.error(
            "MusicScope is configured for Production (MySQL), but the database "
            "connection failed. Double-check DB_HOST/DB_USER/DB_NAME and that "
            "the database is reachable from this app.",
        )
        host = _get_db_setting("DB_HOST") or "(unset)"
        if host in {"localhost", "127.0.0.1", "0.0.0.0"}:
            st.info(
                "DB_HOST is set to localhost/127.0.0.1. In containerized/cloud "
                "runtime this points to the app container itself, not your MySQL "
                "server. Use a reachable DB host or tunnel address.",
            )
        st.caption(f"Connection details: {exc}")
        st.stop()


def load_artist_summary_from_demo() -> pd.DataFrame:
    """Build demo summary from normalized rows, not sparse top-level rollups."""

    videos = load_normalized_videos_from_demo()
    if videos.empty:
        return pd.DataFrame(
            columns=[
                "artist_name",
                "total_videos",
                "total_views",
                "total_likes",
                "total_comments",
                "avg_engagement_rate",
            ]
        )
    return build_artist_summary_from_metrics(videos)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_production_metrics_from_db() -> pd.DataFrame:
    """Load production video metrics directly from MySQL and derive UI-ready fields."""

    from sqlalchemy import text

    sql = text("""
        SELECT
            m.video_id,
            COALESCE(v.channel_title, 'Unknown') AS artist_name,
            COALESCE(v.title, '(untitled)') AS title,
            m.metrics_date,
            m.fetched_at,
            COALESCE(m.view_count, 0) AS view_count,
            COALESCE(m.like_count, 0) AS like_count,
            COALESCE(m.comment_count, 0) AS comment_count,
            v.published_at,
            v.duration
        FROM youtube_metrics AS m
        INNER JOIN youtube_videos AS v
            ON v.video_id = m.video_id
        """)

    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    if df.empty:
        return df

    df["metrics_date"] = pd.to_datetime(df["metrics_date"], errors="coerce")
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["duration_seconds"] = (
        df["duration"].map(_parse_iso8601_duration_seconds).fillna(-1).astype(int) if "duration" in df.columns else -1
    )

    for column in ["view_count", "like_count", "comment_count"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    # IMPORTANT / DO NOT REGRESS:
    # Unknown publish ages must not default to 1 day because that can create
    # fake velocity spikes. Use -1 sentinel for unknown age and set views/day
    # to 0.0 when age is unknown.
    age_days = pd.to_numeric((df["metrics_date"] - df["published_at"]).dt.days, errors="coerce")
    df["age_days"] = age_days.where(age_days >= 1, -1).fillna(-1).astype(int)
    df["views_per_day"] = 0.0
    known_age_mask = df["age_days"] >= 1
    df.loc[known_age_mask, "views_per_day"] = df.loc[known_age_mask, "view_count"] / df.loc[known_age_mask, "age_days"]

    views_nonzero = df["view_count"].astype(float).where(df["view_count"] > 0)
    df["like_rate"] = ((df["like_count"] / views_nonzero) * 100).astype(float).fillna(0.0)
    df["comment_rate"] = ((df["comment_count"] / views_nonzero) * 100).astype(float).fillna(0.0)
    df["engagement_rate"] = df["like_rate"] + df["comment_rate"]
    df["video_type"] = df.apply(
        lambda row: _classify_video_type(
            duration=row.get("duration"),
            title=row.get("title"),
        ),
        axis=1,
    )

    return df


def load_normalized_videos_from_demo() -> pd.DataFrame:
    payload = _load_demo_cohort()
    snapshot_ts = _coerce_timestamp(payload.get("last_updated"))
    records: list[dict[str, object]] = []
    for artist in _coerce_record_list(payload.get("artists")):
        name = artist.get("name")
        for video in _coerce_record_list(artist.get("videos")):
            view_count = max(int(round(_coerce_float(video.get("view_count"), 0.0))), 0)
            duration_seconds = _parse_iso8601_duration_seconds(video.get("duration"))
            like_rate = max(_coerce_float(video.get("like_rate"), 0.0), 0.0)
            comment_rate = max(_coerce_float(video.get("comment_rate"), 0.0), 0.0)
            engagement_rate_raw = _coerce_float(video.get("engagement_rate"), float("nan"))
            engagement_rate = (
                like_rate + comment_rate if not math.isfinite(engagement_rate_raw) else max(engagement_rate_raw, 0.0)
            )
            published_at = _coerce_timestamp(video.get("published_at"))
            metrics_date = snapshot_ts if pd.notna(snapshot_ts) else published_at
            if pd.notna(metrics_date):
                metrics_date = pd.Timestamp(metrics_date).floor("D")
            views_per_day = _coerce_float(video.get("views_per_day"), 0.0)
            age_days = -1
            if pd.notna(metrics_date) and pd.notna(published_at):
                age_days = max(int((metrics_date - published_at).days), 1)
            if age_days >= 1:
                if views_per_day <= 0 and view_count > 0:
                    views_per_day = view_count / age_days
            else:
                views_per_day = 0.0

            video_type = _classify_video_type(
                duration=video.get("duration"),
                title=video.get("title"),
                raw_video_type=video.get("video_type"),
            )
            like_count = int(round(view_count * (like_rate / 100.0)))
            comment_count = int(round(view_count * (comment_rate / 100.0)))

            records.append(
                {
                    "video_id": video.get("video_id"),
                    "artist_name": name,
                    "title": video.get("title"),
                    "published_at": published_at,
                    "metrics_date": metrics_date,
                    "view_count": view_count,
                    "duration_seconds": int(duration_seconds) if duration_seconds is not None else -1,
                    "age_days": age_days,
                    "views_per_day": views_per_day,
                    "engagement_rate": engagement_rate,
                    "like_rate": like_rate,
                    "comment_rate": comment_rate,
                    "like_count": like_count,
                    "comment_count": comment_count,
                    "video_type": video_type,
                }
            )
    return pd.DataFrame(records)


def build_artist_summary_from_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Build artist summary from per-video metrics using latest snapshot per video."""

    if df.empty:
        return pd.DataFrame(
            columns=[
                "artist_name",
                "total_videos",
                "total_views",
                "total_likes",
                "total_comments",
                "avg_engagement_rate",
            ]
        )

    latest = latest_snapshot(df)
    summary = (
        latest.groupby("artist_name", dropna=False)
        .agg(
            total_videos=("video_id", "nunique"),
            total_views=("view_count", "sum"),
            total_likes=("like_count", "sum"),
            total_comments=("comment_count", "sum"),
            avg_engagement_rate=("engagement_rate", "mean"),
        )
        .reset_index()
        .sort_values("total_views", ascending=False)
    )
    return summary


def _get_data_freshness_days() -> int:
    """Return maximum allowed age (in days) for production metrics.

    Controlled via DATA_FRESHNESS_DAYS; defaults to 30 days and coerced to at
    least 1 day if misconfigured.
    """

    # Reuse the same integer parsing semantics we use for CACHE_TTL_SECONDS so
    # Cloud misconfigurations fail fast and loudly in logs.
    value = _read_int_env(DATA_FRESHNESS_DAYS_ENV, DEFAULT_DATA_FRESHNESS_DAYS)
    return max(1, value)


def _coerce_utc_datetime(value: object) -> datetime | None:
    """Return a timezone-aware UTC datetime, or None when invalid."""

    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).to_pydatetime()


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_latest_successful_etl_run_at() -> datetime | None:
    """Return latest completed ETL heartbeat from youtube_etl_runs.

    Uses successful/partial/completed statuses only so a failed or in-flight run
    does not get treated as a fresh snapshot.
    """

    from sqlalchemy import text

    query = text("""
        SELECT MAX(COALESCE(finished_at, started_at)) AS latest_run_at
        FROM youtube_etl_runs
        WHERE LOWER(COALESCE(status, '')) IN ('success', 'partial', 'completed')
        """)

    try:
        engine = get_engine()
        with engine.connect() as conn:
            latest_run_at = conn.execute(query).scalar_one_or_none()
    except Exception:  # noqa: BLE001 - optional diagnostic; fall back to metrics_date
        return None
    return _coerce_utc_datetime(latest_run_at)


def _resolve_freshness_anchor(
    *,
    mode: Literal["demo", "production"] | str | None,
    latest_metrics_at: datetime | None,
    latest_etl_run_at: datetime | None,
) -> tuple[datetime | None, str]:
    """Pick the timestamp used for freshness checks."""

    if mode == "production" and latest_etl_run_at is not None:
        return latest_etl_run_at, "ETL heartbeat"
    if latest_metrics_at is not None:
        return latest_metrics_at, "latest metrics row"
    if latest_etl_run_at is not None:
        return latest_etl_run_at, "ETL heartbeat"
    return None, "snapshot timestamp"


def load_artist_summary(mode: str | None = None) -> pd.DataFrame:
    """Load artist summary for either demo or production mode.

    When *mode* is omitted, this function will auto-detect via get_data_mode().
    Passing the already-computed mode from main() avoids redundant DB
    connection checks on first page load.
    """

    if mode is None:
        mode = get_data_mode()
    if mode == "demo":
        return load_artist_summary_from_demo()
    return build_artist_summary_from_metrics(load_production_metrics_from_db())


def load_normalized_videos(mode: str | None = None) -> pd.DataFrame:
    """Load normalized video metrics for either demo or production mode.

    Accepts an optional *mode* to share detection with callers.
    """

    if mode is None:
        mode = get_data_mode()
    if mode == "demo":
        return load_normalized_videos_from_demo()
    return load_production_metrics_from_db()


def get_production_health() -> dict:
    """Compute production database + analytics health indicators.

    This is intentionally side-effect free so it can be reused by tests and
    any future Diagnostics view without depending on Streamlit runtime.
    """

    from sqlalchemy import text

    engine = get_engine()
    summary: dict[str, object] = {
        "db_reachable": False,
        "db_error": None,
        "db_name": os.getenv("DB_NAME", "yt_proj"),
        "db_host": os.getenv("DB_HOST", "127.0.0.1"),
        "checked_at": datetime.now(timezone.utc),
        "music_videos_rows": 0,
        "artist_summary_rows": 0,
        "latest_metrics_date": None,
        "latest_metrics_age_days": None,
    }

    try:
        with engine.connect() as conn:
            # Basic connectivity
            conn.execute(text("SELECT 1"))
            summary["db_reachable"] = True

            # Table row counts
            videos_count = conn.execute(
                text("SELECT COUNT(*) FROM music_videos_normalized"),
            ).scalar_one()
            artists_count = conn.execute(
                text("SELECT COUNT(*) FROM artist_performance_summary"),
            ).scalar_one()

            summary["music_videos_rows"] = int(videos_count or 0)
            summary["artist_summary_rows"] = int(artists_count or 0)

            # Data freshness from warehouse metrics_date
            latest_metrics = conn.execute(
                text("SELECT MAX(metrics_date) FROM music_videos_normalized"),
            ).scalar_one()
            if latest_metrics is not None:
                # Normalise to timezone-aware datetime for consistent age
                # calculations.
                if not isinstance(latest_metrics, datetime):
                    latest_metrics = datetime.combine(
                        latest_metrics,
                        datetime.min.time(),
                    )
                if latest_metrics.tzinfo is None:
                    latest_metrics = latest_metrics.replace(tzinfo=timezone.utc)

                summary["latest_metrics_date"] = latest_metrics

                age_days = (datetime.now(timezone.utc) - latest_metrics).days
                summary["latest_metrics_age_days"] = age_days
    except Exception as exc:  # noqa: BLE001
        summary["db_error"] = str(exc)

    return summary


def format_number(value: float) -> str:
    return f"{value:,.0f}"


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "–"
    return f"{value:.2f}%"


def _mask_host(host: str) -> str:
    """Mask database host for display purposes (basic privacy).

    Examples::

        "127.0.0.1" -> "127.*"
        "db.example.com" -> "db.*"
    """

    if not host:
        return "(unknown)"
    if "." in host:
        prefix = host.split(".")[0]
        return f"{prefix}.*"
    if len(host) > 3:
        return host[:3] + "*"
    return "*"


def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Take the most recent metrics_date per video to avoid double counting."""
    if df.empty or "metrics_date" not in df.columns:
        return df
    return df.sort_values("metrics_date").drop_duplicates(subset="video_id", keep="last")


def resolve_metrics_date_window(df: pd.DataFrame) -> tuple[date, date]:
    """Return min/max metrics dates with explicit validation."""

    if "metrics_date" not in df.columns:
        raise ValueError(
            "Video metrics are missing the metrics_date column. Run ETL to regenerate music_videos_normalized.",
        )
    metrics_dates = pd.to_datetime(df["metrics_date"], errors="coerce").dt.date
    if metrics_dates.isna().all():
        raise ValueError(
            "metrics_date values are missing or invalid across all rows. Run ETL to regenerate fresh metrics windows.",
        )

    min_date = metrics_dates.min()
    max_date = metrics_dates.max()
    if min_date is None or max_date is None:
        raise ValueError("Could not resolve metrics date window from loaded rows.")
    return min_date, max_date


def select_metrics_window(
    *,
    min_date: date,
    max_date: date,
    selected_window: tuple[date, date] | None = None,
) -> tuple[date, date]:
    """Normalize and validate a date window used for metrics filtering.

    LOUD GUARDRAIL: single-day snapshots (`min_date == max_date`) must
    short-circuit here and in `main()` so Streamlit never receives an invalid
    range slider with identical bounds.
    """

    if min_date > max_date:
        raise ValueError("min_date cannot be after max_date.")
    if min_date == max_date or selected_window is None:
        return min_date, max_date

    start_date, end_date = selected_window
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    normalized_start = max(min_date, min(start_date, max_date))
    normalized_end = max(min_date, min(end_date, max_date))
    return normalized_start, normalized_end


def filter_by_artists(df: pd.DataFrame, artists: list[str]) -> pd.DataFrame:
    if not artists:
        return df.iloc[0:0]
    return df[df["artist_name"].isin(artists)]


def filter_by_date_window(df: pd.DataFrame, window: tuple[date, date]) -> pd.DataFrame:
    if df.empty or "metrics_date" not in df.columns:
        return df
    start, end = window
    mask = df["metrics_date"].dt.date.between(start, end)
    return df.loc[mask]


def ensure_columns(df: pd.DataFrame, columns: Iterable[str], context: str) -> bool:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        st.warning(f"{context} is missing columns: {', '.join(missing)}")
        return False
    return True


def compute_pct_delta(current: float, baseline: float, threshold: float = 0.1) -> float | None:
    """Return percentage delta, or None when baseline is invalid / change is tiny."""

    if not all(math.isfinite(v) for v in (current, baseline, threshold)):
        return None
    if threshold < 0:
        return None
    if baseline <= 0:
        return None
    change = (current / baseline - 1.0) * 100.0
    if not math.isfinite(change):
        return None
    if abs(change) < threshold:
        return None
    return change


def format_delta_value(change: float | None) -> str | None:
    if change is None:
        return None
    return f"{change:+.1f}%"


def compute_delta_display(
    *,
    current: float,
    baseline: float,
    new_entry_floor: float | None = None,
) -> str | None:
    """Return user-facing delta text with optional NEW ENTRY handling.

    Use NEW ENTRY when baseline is effectively near-zero and current is now
    materially positive; this avoids misleading huge percentages.
    """

    if new_entry_floor is not None and baseline < new_entry_floor <= current and current > baseline:
        return "NEW ENTRY"
    return format_delta_value(compute_pct_delta(current, baseline))


def build_delta_signal_rows(
    *,
    views_per_artist: float,
    roster_views_per_artist: float,
    videos_per_artist: float,
    roster_videos_per_artist: float,
    likes_per_artist: float,
    roster_likes_per_artist: float,
    comments_per_artist: float,
    roster_comments_per_artist: float,
    overall_engagement_rate: float,
    roster_overall_engagement_rate: float,
    avg_views_per_day: float,
    roster_avg_views_per_day: float,
) -> pd.DataFrame:
    """Build arithmetic-backed rows for every displayed KPI delta."""

    specs = [
        (
            "Total views",
            views_per_artist,
            roster_views_per_artist,
            "Scale the format mix that is already pulling reach.",
            None,
        ),
        (
            "Videos analyzed",
            videos_per_artist,
            roster_videos_per_artist,
            "Tune release cadence to match capacity.",
            1.0,
        ),
        (
            "Total likes",
            likes_per_artist,
            roster_likes_per_artist,
            "Double down on hooks/creative that drives positive reactions.",
            None,
        ),
        (
            "Total comments",
            comments_per_artist,
            roster_comments_per_artist,
            "Prioritize call-to-action formats that trigger conversation.",
            None,
        ),
        (
            "Overall engagement rate",
            overall_engagement_rate,
            roster_overall_engagement_rate,
            "Replicate the top engagement format with tighter iteration loops.",
            None,
        ),
        (
            "Avg views/day",
            avg_views_per_day,
            roster_avg_views_per_day,
            "Use current acceleration leaders as the rollout priority this week.",
            NEW_ENTRY_VIEWS_PER_DAY_FLOOR,
        ),
    ]
    rows: list[dict[str, str]] = []
    for name, current, baseline, action, new_entry_floor in specs:
        delta_text = compute_delta_display(
            current=current,
            baseline=baseline,
            new_entry_floor=new_entry_floor,
        )
        if delta_text is None:
            continue
        arithmetic = (
            f"{current:,.2f} vs {baseline:,.2f} baseline (new entry floor: {new_entry_floor:,.1f})"
            if delta_text == "NEW ENTRY"
            else f"(({current:,.2f} / {baseline:,.2f}) - 1) x 100"
        )
        rows.append(
            {
                "KPI": name,
                "Delta": delta_text,
                "Arithmetic": arithmetic,
                "Action": action,
            }
        )
    return pd.DataFrame(rows)


def build_kpi_context(
    summary: pd.DataFrame,
    artists: list[str],
    videos: pd.DataFrame | None = None,
    roster_videos: pd.DataFrame | None = None,
) -> dict[str, float | int]:
    """Build KPI totals and roster baselines from filtered videos when available."""

    filtered_summary = filter_by_artists(summary, artists)
    if filtered_summary.empty:
        return {}

    use_video_math = videos is not None and not videos.empty
    if use_video_math:
        selected_rows = filter_by_artists(videos, artists)
        if selected_rows.empty:
            use_video_math = False
        else:
            total_views = int(selected_rows["view_count"].sum())
            total_videos = int(selected_rows["video_id"].nunique())
            total_likes = int(selected_rows["like_count"].sum()) if "like_count" in selected_rows.columns else 0
            total_comments = (
                int(selected_rows["comment_count"].sum()) if "comment_count" in selected_rows.columns else 0
            )
            avg_engagement = (
                float(selected_rows["engagement_rate"].mean()) if "engagement_rate" in selected_rows.columns else 0.0
            )
            selected_artist_count = max(int(selected_rows["artist_name"].nunique()), 1)

    if not use_video_math:
        total_views = int(filtered_summary["total_views"].sum())
        total_videos = int(filtered_summary["total_videos"].sum())
        total_likes = int(filtered_summary["total_likes"].sum())
        total_comments = int(filtered_summary["total_comments"].sum())
        avg_engagement = float(filtered_summary["avg_engagement_rate"].mean())
        selected_artist_count = max(len(filtered_summary), 1)

    if roster_videos is not None and not roster_videos.empty:
        roster_rows = roster_videos
        roster_artist_count = max(int(roster_rows["artist_name"].nunique()), 1)
        roster_views_per_artist = float(roster_rows["view_count"].sum()) / roster_artist_count
        roster_videos_per_artist = float(roster_rows["video_id"].nunique()) / roster_artist_count
        roster_likes_per_artist = (
            float(roster_rows["like_count"].sum()) / roster_artist_count if "like_count" in roster_rows.columns else 0.0
        )
        roster_comments_per_artist = (
            float(roster_rows["comment_count"].sum()) / roster_artist_count
            if "comment_count" in roster_rows.columns
            else 0.0
        )
        if "engagement_rate" in roster_rows.columns:
            per_artist_engagement = roster_rows.groupby("artist_name")["engagement_rate"].mean()
            roster_avg_engagement = float(per_artist_engagement.mean()) if not per_artist_engagement.empty else 0.0
        else:
            roster_avg_engagement = 0.0
    else:
        roster_views_per_artist = summary["total_views"].sum() / max(len(summary), 1)
        roster_videos_per_artist = summary["total_videos"].sum() / max(len(summary), 1)
        roster_likes_per_artist = summary["total_likes"].sum() / max(len(summary), 1)
        roster_comments_per_artist = summary["total_comments"].sum() / max(len(summary), 1)
        roster_avg_engagement = float(summary["avg_engagement_rate"].mean())

    return {
        "total_views": total_views,
        "total_videos": total_videos,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "avg_engagement": avg_engagement,
        "selected_artist_count": selected_artist_count,
        "roster_views_per_artist": roster_views_per_artist,
        "roster_videos_per_artist": roster_videos_per_artist,
        "roster_likes_per_artist": roster_likes_per_artist,
        "roster_comments_per_artist": roster_comments_per_artist,
        "roster_avg_engagement": roster_avg_engagement,
    }


def build_kpi_red_flags(
    *,
    total_videos: int,
    total_likes: int,
    total_comments: int,
    latest_metrics_date: date | None,
    mode: Literal["demo", "production"] | str | None,
    latest_etl_run_date: date | None = None,
    reference_date: date | None = None,
) -> list[str]:
    """Return user-visible RED FLAG messages for suspicious KPI conditions."""

    flags: list[str] = []
    is_production_mode = mode == "production"
    if total_videos > 0 and total_likes == 0:
        flags.append(
            "Likes are zero across analyzed videos. Check whether like_count "
            "ingestion or demo derivation is missing.",
        )
    if total_videos > 0 and total_comments == 0:
        flags.append(
            "Comments are zero across analyzed videos. Check whether comment_count "
            "ingestion or demo derivation is missing.",
        )

    is_etl_heartbeat_source = is_production_mode and latest_etl_run_date is not None
    freshness_reference_date = latest_etl_run_date if is_etl_heartbeat_source else latest_metrics_date
    if freshness_reference_date is None:
        return flags

    age_days = (reference_date or date.today()) - freshness_reference_date
    freshness_days = _get_data_freshness_days()
    if age_days.days <= freshness_days:
        return flags

    if is_production_mode:
        source_text = "successful ETL heartbeat is" if is_etl_heartbeat_source else "metrics are"
        flags.append(
            f"Latest {source_text} {age_days.days} days old "
            f"(limit: {freshness_days}). Run ETL before trusting KPI cards.",
        )
    else:
        flags.append(
            f"Demo snapshot is {age_days.days} days old (latest: {freshness_reference_date.isoformat()}). "
            "Use Production mode for fresh daily ETL KPIs.",
        )
    return flags


def build_artist_content_action_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return artist-level action rows from content-type KPIs."""

    if df.empty:
        return pd.DataFrame(columns=["Artist", "Best Reach Format", "Best Engagement Format", "Action Plan"])

    typed = df.copy()
    typed["video_type_label"] = typed["video_type"].map(_display_video_type)
    mix = (
        typed.groupby(["artist_name", "video_type_label"], dropna=False)
        .agg(
            videos=("video_id", "nunique"),
            total_views=("view_count", "sum"),
            avg_views_per_day=("views_per_day", "mean"),
            avg_engagement=("engagement_rate", "mean"),
        )
        .reset_index()
    )
    rows: list[dict[str, str]] = []
    for artist_name, artist_mix in mix.groupby("artist_name"):
        best_reach = artist_mix.sort_values(["avg_views_per_day", "total_views"], ascending=False).iloc[0]
        best_engagement = artist_mix.sort_values(["avg_engagement", "videos"], ascending=False).iloc[0]

        reach_format = str(best_reach["video_type_label"])
        engagement_format = str(best_engagement["video_type_label"])

        if reach_format == engagement_format:
            action = (
                f"Primary bet: {reach_format}. Keep >=70% of next releases in this format; "
                "A/B test titles and openings."
            )
        else:
            action = (
                f"Use 70/30 split: 70% {reach_format} for reach and 30% {engagement_format} for deeper fan response."
            )
        rows.append(
            {
                "Artist": str(artist_name),
                "Best Reach Format": f"{reach_format} ({best_reach['avg_views_per_day']:,.1f} views/day)",
                "Best Engagement Format": f"{engagement_format} ({best_engagement['avg_engagement']:.2f}%)",
                "Action Plan": action,
            }
        )
    return pd.DataFrame(rows)


def _prepare_rollout_frame(videos: pd.DataFrame) -> pd.DataFrame:
    """Build standardized frame used by rollout charts/tables."""

    required = {"artist_name", "video_id", "title", "video_type", "view_count", "views_per_day", "engagement_rate"}
    if videos.empty or not required.issubset(videos.columns):
        return pd.DataFrame()

    typed = videos.copy()
    typed["video_type_label"] = typed["video_type"].map(_display_video_type)
    typed["published_at"] = pd.to_datetime(typed.get("published_at"), errors="coerce")
    typed["metrics_date"] = pd.to_datetime(typed.get("metrics_date"), errors="coerce")
    typed["activity_date"] = typed["published_at"].fillna(typed["metrics_date"])
    typed["view_count"] = pd.to_numeric(typed["view_count"], errors="coerce")
    typed["views_per_day"] = pd.to_numeric(typed["views_per_day"], errors="coerce")
    typed["engagement_rate"] = pd.to_numeric(typed["engagement_rate"], errors="coerce")
    typed = typed[typed["artist_name"].notna() & typed["video_id"].notna()]
    typed = typed.sort_values(
        ["artist_name", "activity_date", "view_count"],
        ascending=[True, False, False],
        na_position="last",
    )
    return typed


def _prepare_recent_release_windows(
    videos: pd.DataFrame,
    *,
    per_artist_limit: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return per-artist recent windows for official releases and other content."""

    typed = _prepare_rollout_frame(videos)
    if typed.empty:
        return pd.DataFrame(), pd.DataFrame()

    official_recent = (
        typed.loc[typed["video_type_label"].isin(OFFICIAL_RELEASE_TYPES)]
        .groupby("artist_name", group_keys=False)
        .head(per_artist_limit)
        .reset_index(drop=True)
    )
    other_recent = (
        typed.loc[~typed["video_type_label"].isin(OFFICIAL_RELEASE_TYPES)]
        .groupby("artist_name", group_keys=False)
        .head(per_artist_limit)
        .reset_index(drop=True)
    )
    return official_recent, other_recent


def prepare_recent_release_windows(
    videos: pd.DataFrame,
    *,
    per_artist_limit: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Public wrapper for recent official/other release windows."""

    return _prepare_recent_release_windows(videos, per_artist_limit=per_artist_limit)


def _mean_or_none(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.mean())


def _median_release_gap_days(official_rows: pd.DataFrame) -> float | None:
    if "activity_date" not in official_rows.columns:
        return None
    dates = pd.to_datetime(official_rows["activity_date"], errors="coerce").dropna().drop_duplicates()
    if len(dates) < 2:
        return None
    sorted_dates = dates.sort_values(ascending=False).to_list()
    day_gaps = [
        float((sorted_dates[idx] - sorted_dates[idx + 1]).days)
        for idx in range(len(sorted_dates) - 1)
        if (sorted_dates[idx] - sorted_dates[idx + 1]).days > 0
    ]
    if not day_gaps:
        return None
    return float(pd.Series(day_gaps).median())


def _safe_lift_pct(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    if not (math.isfinite(current) and math.isfinite(baseline)):
        return None
    if baseline <= 0:
        return None
    return (current / baseline - 1.0) * 100.0


def _build_today_action(
    *,
    official_count: int,
    other_count: int,
    official_vs_other_lift_pct: float | None,
    mv_vs_other_official_lift_pct: float | None,
    short_video_share_pct: float | None,
) -> str:
    """Return a plain-language rollout action for today's planning."""

    if official_count == 0:
        return (
            "No recent official releases in window. Schedule one official release with "
            "short-video (<60s) support this week."
        )
    if mv_vs_other_official_lift_pct is not None and mv_vs_other_official_lift_pct >= 20.0:
        return "Official music videos are leading. Prioritize next budget for an Official Music Video rollout."
    if official_vs_other_lift_pct is not None and official_vs_other_lift_pct < 0:
        return "Other content is outperforming official releases. Rework official rollout hooks and thumbnails."
    if short_video_share_pct is not None and short_video_share_pct < 40.0:
        return (
            "Increase short-video (<60s) support around each official release to improve discovery and follow-through."
        )
    if other_count == 0:
        return "Add short-form support posts around each official release to create follow-on audience lift."
    return "Mix looks healthy. Keep cadence steady and run one controlled title/thumbnail experiment this week."


def build_release_strategy_board(
    videos: pd.DataFrame,
    *,
    per_artist_limit: int = 10,
) -> pd.DataFrame:
    """Build per-artist rollout KPIs from latest official vs other content windows."""

    typed = _prepare_rollout_frame(videos)
    official_recent, other_recent = _prepare_recent_release_windows(
        videos,
        per_artist_limit=per_artist_limit,
    )
    if typed.empty or (official_recent.empty and other_recent.empty):
        return pd.DataFrame(
            columns=[
                "Artist",
                "Official release count",
                "Other content count",
                "Official avg views/day",
                "Other avg views/day",
                "Official vs Other lift (%)",
                "MV vs other official lift (%)",
                "Short video (<60s) share in other content (%)",
                "Official cadence (days)",
                "Today action",
            ]
        )

    official_pool = typed.loc[typed["video_type_label"].isin(OFFICIAL_RELEASE_TYPES)]
    other_pool = typed.loc[~typed["video_type_label"].isin(OFFICIAL_RELEASE_TYPES)]
    artists = sorted(
        set(official_pool.get("artist_name", pd.Series(dtype=str)).tolist())
        | set(other_pool.get("artist_name", pd.Series(dtype=str)).tolist())
    )
    rows: list[dict[str, float | int | str]] = []
    for artist_name in artists:
        official_rows = official_recent.loc[official_recent["artist_name"] == artist_name]
        other_rows = other_recent.loc[other_recent["artist_name"] == artist_name]
        official_full_rows = official_pool.loc[official_pool["artist_name"] == artist_name]
        other_full_rows = other_pool.loc[other_pool["artist_name"] == artist_name]

        official_avg_views_per_day = _mean_or_none(official_rows["views_per_day"])
        other_avg_views_per_day = _mean_or_none(other_rows["views_per_day"])
        official_vs_other_lift = _safe_lift_pct(official_avg_views_per_day, other_avg_views_per_day)

        mv_rows = official_rows.loc[official_rows["video_type_label"] == "Official Music Video"]
        non_mv_official_rows = official_rows.loc[official_rows["video_type_label"] != "Official Music Video"]
        mv_avg_views_per_day = _mean_or_none(mv_rows["views_per_day"])
        non_mv_official_avg_views_per_day = _mean_or_none(non_mv_official_rows["views_per_day"])
        mv_lift = _safe_lift_pct(mv_avg_views_per_day, non_mv_official_avg_views_per_day)

        short_video_rows = other_full_rows.loc[other_full_rows["video_type_label"] == SHORT_VIDEO_LABEL]
        short_video_share_pct = None
        if len(other_full_rows) > 0:
            short_video_share_pct = float(len(short_video_rows) / len(other_full_rows) * 100.0)

        official_count = int(official_full_rows["video_id"].nunique()) if not official_full_rows.empty else 0
        other_count = int(other_full_rows["video_id"].nunique()) if not other_full_rows.empty else 0
        cadence_days = _median_release_gap_days(official_full_rows)

        rows.append(
            {
                "Artist": str(artist_name),
                "Official release count": official_count,
                "Other content count": other_count,
                "Official avg views/day": official_avg_views_per_day or 0.0,
                "Other avg views/day": other_avg_views_per_day or 0.0,
                "Official vs Other lift (%)": official_vs_other_lift,
                "MV vs other official lift (%)": mv_lift,
                "Short video (<60s) share in other content (%)": short_video_share_pct,
                "Official cadence (days)": cadence_days,
                "Today action": _build_today_action(
                    official_count=official_count,
                    other_count=other_count,
                    official_vs_other_lift_pct=official_vs_other_lift,
                    mv_vs_other_official_lift_pct=mv_lift,
                    short_video_share_pct=short_video_share_pct,
                ),
            }
        )

    result = pd.DataFrame(rows)
    return result.sort_values("MV vs other official lift (%)", ascending=False, na_position="last").reset_index(
        drop=True
    )


def _build_focus_artist_color_map(
    artists: list[str],
    *,
    focus_artist: str,
    base_color_map: dict[str, str],
) -> dict[str, str]:
    """Highlight one artist and mute all benchmark artists to grayscale."""

    color_map: dict[str, str] = {}
    for artist in artists:
        if artist == focus_artist:
            color_map[artist] = base_color_map.get(artist, APP_RED_500)
        else:
            color_map[artist] = APP_BENCHMARK_GRAY
    return color_map


def build_focus_artist_scorecard(board: pd.DataFrame, focus_artist: str) -> pd.DataFrame:
    """Build one-artist KPI comparisons against benchmark artist averages."""

    columns = ["Metric", "Focus value", "Benchmark avg", "Lift vs benchmark (%)", "Interpretation"]
    if board.empty or "Artist" not in board.columns:
        return pd.DataFrame(columns=columns)

    focus_rows = board.loc[board["Artist"] == focus_artist]
    if focus_rows.empty:
        return pd.DataFrame(columns=columns)

    focus = focus_rows.iloc[0]
    peers = board.loc[board["Artist"] != focus_artist]

    def _peer_mean(column: str) -> float | None:
        if peers.empty or column not in peers.columns:
            return None
        values = pd.to_numeric(peers[column], errors="coerce").dropna()
        if values.empty:
            return None
        return float(values.mean())

    specs = [
        ("Official avg views/day", "Official avg views/day", "Higher means official drops are compounding faster."),
        (
            "Other avg views/day",
            "Other avg views/day",
            "Tracks discovery speed from short-video (<60s) and other support content.",
        ),
        (
            "Official vs Other lift (%)",
            "Official vs Other lift (%)",
            "Positive means official releases beat other content.",
        ),
        (
            "MV vs other official lift (%)",
            "MV vs other official lift (%)",
            "Positive means music videos beat other official formats.",
        ),
        (
            "Short video (<60s) share in other content (%)",
            "Short video (<60s) share in other content (%)",
            "Shows how much support content is short-form by duration (<60s).",
        ),
        ("Official cadence (days)", "Official cadence (days)", "Lower means more frequent official release cadence."),
    ]

    rows: list[dict[str, float | str | None]] = []
    for label, column, interpretation in specs:
        focus_value = pd.to_numeric(pd.Series([focus.get(column)]), errors="coerce").iloc[0]
        focus_float = float(focus_value) if pd.notna(focus_value) else None
        benchmark_avg = _peer_mean(column)
        lift_pct = _safe_lift_pct(focus_float, benchmark_avg)
        rows.append(
            {
                "Metric": label,
                "Focus value": focus_float,
                "Benchmark avg": benchmark_avg,
                "Lift vs benchmark (%)": lift_pct,
                "Interpretation": interpretation,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def build_focus_trend_frame(df: pd.DataFrame, focus_artist: str) -> pd.DataFrame:
    """Return focus-vs-benchmark trend frame (views/day) for charting."""

    required = {"artist_name", "metrics_date", "views_per_day"}
    if df.empty or not required.issubset(df.columns):
        return pd.DataFrame(columns=["metrics_date", "Series", "views_per_day"])

    typed = df.copy()
    typed["metrics_date"] = pd.to_datetime(typed["metrics_date"], errors="coerce")
    typed["views_per_day"] = pd.to_numeric(typed["views_per_day"], errors="coerce")
    typed = typed.dropna(subset=["metrics_date", "views_per_day"])
    if typed.empty:
        return pd.DataFrame(columns=["metrics_date", "Series", "views_per_day"])

    artist_day = (
        typed.groupby(["metrics_date", "artist_name"], dropna=False)
        .agg(views_per_day=("views_per_day", "mean"))
        .reset_index()
    )
    focus = artist_day.loc[artist_day["artist_name"] == focus_artist, ["metrics_date", "views_per_day"]].copy()
    if focus.empty:
        return pd.DataFrame(columns=["metrics_date", "Series", "views_per_day"])
    focus["Series"] = "Focus artist"

    peers = artist_day.loc[artist_day["artist_name"] != focus_artist]
    if peers.empty:
        return focus.loc[:, ["metrics_date", "Series", "views_per_day"]]

    benchmark = peers.groupby("metrics_date", dropna=False).agg(views_per_day=("views_per_day", "mean")).reset_index()
    benchmark["Series"] = "Benchmark average"
    return pd.concat(
        [
            focus.loc[:, ["metrics_date", "Series", "views_per_day"]],
            benchmark.loc[:, ["metrics_date", "Series", "views_per_day"]],
        ],
        ignore_index=True,
    ).sort_values(["metrics_date", "Series"])


def build_focus_format_lift_table(df: pd.DataFrame, focus_artist: str) -> pd.DataFrame:
    """Compare focus artist format performance against benchmark averages."""

    required = {"artist_name", "video_type", "video_id", "views_per_day"}
    if df.empty or not required.issubset(df.columns):
        return pd.DataFrame(
            columns=[
                "Format",
                "Focus avg views/day",
                "Benchmark avg views/day",
                "Lift vs benchmark (%)",
            ]
        )

    typed = df.copy()
    typed["video_type_label"] = typed["video_type"].map(_display_video_type)
    typed["views_per_day"] = pd.to_numeric(typed["views_per_day"], errors="coerce")
    typed = typed.dropna(subset=["views_per_day"])
    if typed.empty:
        return pd.DataFrame(
            columns=[
                "Format",
                "Focus avg views/day",
                "Benchmark avg views/day",
                "Lift vs benchmark (%)",
            ]
        )

    focus_mix = (
        typed.loc[typed["artist_name"] == focus_artist]
        .groupby("video_type_label", dropna=False)
        .agg(focus_views_per_day=("views_per_day", "mean"))
    )
    if focus_mix.empty:
        return pd.DataFrame(
            columns=[
                "Format",
                "Focus avg views/day",
                "Benchmark avg views/day",
                "Lift vs benchmark (%)",
            ]
        )

    benchmark_mix = (
        typed.loc[typed["artist_name"] != focus_artist]
        .groupby("video_type_label", dropna=False)
        .agg(benchmark_views_per_day=("views_per_day", "mean"))
    )
    merged = focus_mix.join(benchmark_mix, how="left").reset_index()
    merged["Lift vs benchmark (%)"] = merged.apply(
        lambda row: _safe_lift_pct(
            float(row["focus_views_per_day"]) if pd.notna(row["focus_views_per_day"]) else None,
            float(row["benchmark_views_per_day"]) if pd.notna(row["benchmark_views_per_day"]) else None,
        ),
        axis=1,
    )
    merged = merged.rename(
        columns={
            "video_type_label": "Format",
            "focus_views_per_day": "Focus avg views/day",
            "benchmark_views_per_day": "Benchmark avg views/day",
        }
    )
    return merged.sort_values("Focus avg views/day", ascending=False).reset_index(drop=True)


def render_kpis(
    summary: pd.DataFrame,
    artists: list[str],
    videos: pd.DataFrame | None = None,
    roster_videos: pd.DataFrame | None = None,
    mode: Literal["demo", "production"] | str | None = None,
    latest_etl_run_date: date | None = None,
) -> None:
    """Render KPI cards with directional deltas vs roster averages.

    This keeps the demo honest: deltas are always computed from the same
    underlying data the cards display, and work in both demo and production
    modes without any hidden heuristics.
    """

    filtered = filter_by_artists(summary, artists)
    if filtered.empty:
        st.warning("No data found for the selected artists.")
        return

    context = build_kpi_context(summary, artists, videos=videos, roster_videos=roster_videos)
    if not context:
        st.warning("No KPI context is available for the selected filters.")
        return

    total_views = int(context["total_views"])
    total_videos = int(context["total_videos"])
    total_likes = int(context["total_likes"])
    total_comments = int(context["total_comments"])
    selected_artist_count = int(context["selected_artist_count"])

    # Roster-wide baselines for directional context
    roster_views_per_artist = float(context["roster_views_per_artist"])
    roster_videos_per_artist = float(context["roster_videos_per_artist"])
    roster_likes_per_artist = float(context["roster_likes_per_artist"])
    roster_comments_per_artist = float(context["roster_comments_per_artist"])
    views_per_artist = total_views / selected_artist_count
    videos_per_artist = total_videos / selected_artist_count
    likes_per_artist = total_likes / selected_artist_count
    comments_per_artist = total_comments / selected_artist_count

    overall_engagement_rate = ((total_likes + total_comments) / total_views * 100.0) if total_views > 0 else 0.0
    roster_overall_engagement_rate = (
        ((roster_likes_per_artist + roster_comments_per_artist) / roster_views_per_artist * 100.0)
        if roster_views_per_artist > 0
        else 0.0
    )

    selected_avg_views_per_day = (
        _mean_or_none(filter_by_artists(videos, artists)["views_per_day"])
        if videos is not None and not videos.empty and "views_per_day" in videos.columns
        else None
    )
    roster_avg_views_per_day = (
        _mean_or_none(roster_videos["views_per_day"])
        if roster_videos is not None and not roster_videos.empty and "views_per_day" in roster_videos.columns
        else None
    )
    avg_views_per_day = selected_avg_views_per_day or 0.0
    baseline_views_per_day = roster_avg_views_per_day or 0.0

    views_delta = compute_delta_display(current=views_per_artist, baseline=roster_views_per_artist)
    videos_delta = compute_delta_display(
        current=videos_per_artist,
        baseline=roster_videos_per_artist,
        new_entry_floor=1.0,
    )
    likes_delta = compute_delta_display(current=likes_per_artist, baseline=roster_likes_per_artist)
    comments_delta = compute_delta_display(current=comments_per_artist, baseline=roster_comments_per_artist)
    engagement_delta = compute_delta_display(
        current=overall_engagement_rate,
        baseline=roster_overall_engagement_rate,
    )
    views_per_day_delta = compute_delta_display(
        current=avg_views_per_day,
        baseline=baseline_views_per_day,
        new_entry_floor=NEW_ENTRY_VIEWS_PER_DAY_FLOOR,
    )

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric(
        "Total views",
        format_number(total_views),
        delta=views_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Per-artist views vs roster-wide average",
    )
    c2.metric(
        "Videos analyzed",
        format_number(total_videos),
        delta=videos_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Per-artist video count vs roster-wide average",
    )
    c3.metric(
        "Total likes",
        format_number(total_likes),
        delta=likes_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Per-artist likes vs roster-wide average",
    )
    c4.metric(
        "Total comments",
        format_number(total_comments),
        delta=comments_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Per-artist comments vs roster-wide average",
    )
    c5.metric(
        "Overall engagement rate",
        format_percent(overall_engagement_rate),
        delta=engagement_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="((Likes + Comments) / Views) x 100, compared to roster average",
    )
    c6.metric(
        "Avg views/day",
        f"{avg_views_per_day:,.1f}",
        delta=views_per_day_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Average daily velocity vs roster baseline",
    )

    # Apply modern, card-like styling to the KPI strip
    style_metric_cards(
        background_color="#F0F2F6",
        border_left_color=APP_RED_500,
        border_radius_px=8,
        box_shadow=True,
    )

    # Celebrate big wins in a fun, notebook-consistent way
    # if total_views >= 10_000_000:
    #     rain(
    #         emoji="🎉",
    #         font_size=40,
    #         falling_speed=5,
    #         animation_length=2,
    #     )

    latest_metrics_date: date | None = None
    if videos is not None and not videos.empty and "metrics_date" in videos.columns:
        latest_value = pd.to_datetime(videos["metrics_date"], errors="coerce").max()
        if not pd.isna(latest_value):
            latest_metrics_date = pd.Timestamp(latest_value).date()

    red_flags = build_kpi_red_flags(
        total_videos=total_videos,
        total_likes=total_likes,
        total_comments=total_comments,
        latest_metrics_date=latest_metrics_date,
        latest_etl_run_date=latest_etl_run_date,
        mode=mode,
    )
    for flag in red_flags:
        st.warning(f"Data check: {flag}")

    delta_rows = build_delta_signal_rows(
        views_per_artist=views_per_artist,
        roster_views_per_artist=roster_views_per_artist,
        videos_per_artist=videos_per_artist,
        roster_videos_per_artist=roster_videos_per_artist,
        likes_per_artist=likes_per_artist,
        roster_likes_per_artist=roster_likes_per_artist,
        comments_per_artist=comments_per_artist,
        roster_comments_per_artist=roster_comments_per_artist,
        overall_engagement_rate=overall_engagement_rate,
        roster_overall_engagement_rate=roster_overall_engagement_rate,
        avg_views_per_day=avg_views_per_day,
        roster_avg_views_per_day=baseline_views_per_day,
    )
    if not delta_rows.empty:
        st.markdown("##### KPI delta arithmetic (shown percentages only)")
        st.dataframe(
            delta_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "KPI": st.column_config.TextColumn("KPI"),
                "Delta": st.column_config.TextColumn("Delta"),
                "Arithmetic": st.column_config.TextColumn("Arithmetic"),
                "Action": st.column_config.TextColumn("Action"),
            },
        )


def build_release_anchor_trend_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build release-anchored cumulative view curves per artist.

    This is used when only one metrics snapshot date is available, so a regular
    time-series chart would collapse to one point per artist.
    """

    required = {"artist_name", "published_at", "view_count"}
    if df.empty or not required.issubset(df.columns):
        return pd.DataFrame(columns=["artist_name", "day_since_first_release", "cumulative_views"])

    typed = df.copy()
    typed["published_at"] = pd.to_datetime(typed["published_at"], errors="coerce")
    typed["view_count"] = pd.to_numeric(typed["view_count"], errors="coerce")
    typed = typed.dropna(subset=["artist_name", "published_at", "view_count"])
    if typed.empty:
        return pd.DataFrame(columns=["artist_name", "day_since_first_release", "cumulative_views"])

    if "video_id" in typed.columns and "metrics_date" in typed.columns:
        typed["metrics_date"] = pd.to_datetime(typed["metrics_date"], errors="coerce")
        typed = typed.sort_values("metrics_date").drop_duplicates(subset="video_id", keep="last")

    releases = (
        typed.groupby(["artist_name", "published_at"], dropna=False)
        .agg(view_count=("view_count", "sum"))
        .reset_index()
        .sort_values(["artist_name", "published_at"])
    )

    rows: list[dict[str, float | int | str]] = []
    for artist_name, artist_rows in releases.groupby("artist_name"):
        first_release = artist_rows["published_at"].min()
        if pd.isna(first_release):
            continue
        running = 0.0
        rows.append(
            {
                "artist_name": str(artist_name),
                "day_since_first_release": 0,
                "cumulative_views": 0.0,
            }
        )
        for _, release in artist_rows.iterrows():
            running += float(release["view_count"])
            day_offset = int((release["published_at"] - first_release).days)
            rows.append(
                {
                    "artist_name": str(artist_name),
                    "day_since_first_release": max(0, day_offset),
                    "cumulative_views": running,
                }
            )
    return pd.DataFrame(rows)


def render_trend_chart(df: pd.DataFrame, color_map: dict[str, str]) -> None:
    if df.empty:
        st.info("No time-series data available for this selection.")
        return
    if not ensure_columns(df, ["metrics_date", "artist_name", "view_count", "views_per_day"], "Trend chart"):
        return

    typed = df.copy()
    typed["metrics_date"] = pd.to_datetime(typed["metrics_date"], errors="coerce").dt.floor("D")
    trend = (
        typed.groupby(["metrics_date", "artist_name"])
        .agg(view_count=("view_count", "sum"), views_per_day=("views_per_day", "mean"))
        .reset_index()
        .sort_values("metrics_date")
    )
    if trend.empty:
        st.info("No valid metrics dates are available for this selection.")
        return

    if trend["metrics_date"].nunique() <= 1:
        release_anchored = build_release_anchor_trend_frame(typed)
        if not release_anchored.empty:
            fig = px.line(
                release_anchored,
                x="day_since_first_release",
                y="cumulative_views",
                color="artist_name",
                color_discrete_map=color_map,
                markers=True,
                title="Portfolio growth since first release (day-0 anchored)",
            )
            fig.update_layout(
                hovermode="x unified",
                legend_title_text="Artist",
                xaxis_title="Days since first release",
                yaxis_title="Cumulative views (latest snapshot)",
            )
            st.plotly_chart(fig, use_container_width=True, height=380)
            st.caption(
                "Day 0 is the artist's first release in this filtered set. "
                "Curve uses latest snapshot views as a release-anchored portfolio trajectory."
            )
            return

    fig = px.line(
        trend,
        x="metrics_date",
        y="view_count",
        color="artist_name",
        color_discrete_map=color_map,
        markers=True,
        title="View growth over time",
    )
    fig.update_layout(hovermode="x unified", legend_title_text="Artist")
    st.plotly_chart(fig, use_container_width=True, height=380)


def render_velocity_scatter(df: pd.DataFrame, color_map: dict[str, str]) -> None:
    if df.empty:
        st.info("No videos available to plot engagement velocity.")
        return
    if not ensure_columns(
        df,
        ["views_per_day", "engagement_rate", "view_count", "artist_name", "title"],
        "Velocity chart",
    ):
        return

    fig = px.scatter(
        df,
        x="views_per_day",
        y="engagement_rate",
        color="artist_name",
        size="view_count",
        hover_name="title",
        hover_data={
            "artist_name": True,
            "view_count": ":,.0f",
            "views_per_day": ":,.1f",
            "engagement_rate": ":.2f",
        },
        color_discrete_map=color_map,
        title="Engagement vs. daily velocity (latest metrics)",
    )
    fig.update_layout(legend_title_text="Artist")
    st.plotly_chart(fig, use_container_width=True, height=380)


def render_content_mix(df: pd.DataFrame) -> None:
    if df.empty or "video_type" not in df.columns:
        st.info("Content mix unavailable for the selected filters.")
        return
    if not ensure_columns(df, ["video_type", "video_id", "view_count", "engagement_rate"], "Content mix chart"):
        return

    typed = df.copy()
    typed["video_type_label"] = typed["video_type"].map(_display_video_type)
    mix = (
        typed.groupby("video_type_label")
        .agg(
            video_count=("video_id", "nunique"),
            total_views=("view_count", "sum"),
            avg_engagement=("engagement_rate", "mean"),
        )
        .reset_index()
        .sort_values("total_views", ascending=False)
    )

    # Use an ECharts bar chart for a slightly more dynamic content-mix view
    options = {
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": mix["video_type_label"].tolist()},
        "yAxis": {"type": "value"},
        "series": [
            {
                "name": "Total views",
                "type": "bar",
                "data": mix["total_views"].tolist(),
                "itemStyle": {"color": APP_RED_500},
            }
        ],
    }
    st_echarts(options=options, height="400px")


def render_artist_content_mix(df: pd.DataFrame) -> None:
    """Render artist-by-format mix and a concrete action board."""

    if df.empty:
        st.info("Per-artist content mix unavailable for the selected filters.")
        return
    required = ["artist_name", "video_type", "video_id", "view_count", "views_per_day", "engagement_rate"]
    if not ensure_columns(df, required, "Artist content mix chart"):
        return

    typed = df.copy()
    typed["video_type_label"] = typed["video_type"].map(_display_video_type)
    mix = (
        typed.groupby(["artist_name", "video_type_label"], dropna=False)
        .agg(
            video_count=("video_id", "nunique"),
            total_views=("view_count", "sum"),
            avg_views_per_day=("views_per_day", "mean"),
            avg_engagement=("engagement_rate", "mean"),
        )
        .reset_index()
    )
    artist_order = (
        mix.groupby("artist_name", dropna=False)["video_count"]
        .sum()
        .sort_values(ascending=True)
        .index.astype(str)
        .tolist()
    )

    fig = px.bar(
        mix,
        x="video_count",
        y="artist_name",
        color="video_type_label",
        orientation="h",
        title="Video content mix by artist (horizontal count by format)",
        barmode="stack",
        category_orders={"artist_name": artist_order},
        hover_data={
            "total_views": ":,.0f",
            "avg_views_per_day": ":,.1f",
            "avg_engagement": ":.2f",
            "video_type_label": False,
        },
    )
    fig.update_layout(
        xaxis_title="Videos",
        yaxis_title="Artist",
        legend_title_text="Format",
        hovermode="y unified",
    )
    st.plotly_chart(fig, use_container_width=True, height=420)

    action_rows = build_artist_content_action_rows(df)
    if not action_rows.empty:
        st.markdown("##### Label Action Board (KPI-driven)")
        st.dataframe(
            action_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Artist": st.column_config.TextColumn("Artist"),
                "Best Reach Format": st.column_config.TextColumn("Best Reach Format"),
                "Best Engagement Format": st.column_config.TextColumn("Best Engagement Format"),
                "Action Plan": st.column_config.TextColumn("Action Plan", width="large"),
            },
        )


def _prepare_release_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "artist_name",
        "title",
        "video_type_label",
        "activity_date",
        "view_count",
        "views_per_day",
        "engagement_rate",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    table = df.copy()
    table["activity_date"] = pd.to_datetime(table["activity_date"], errors="coerce").dt.date
    return table.loc[:, columns].rename(
        columns={
            "artist_name": "Artist",
            "title": "Title",
            "video_type_label": "Format",
            "activity_date": "Release date",
            "view_count": "Views",
            "views_per_day": "Views/day",
            "engagement_rate": "Engagement %",
        }
    )


def render_executive_action_center(df: pd.DataFrame, *, palette: Mapping[str, str]) -> None:
    """Render high-signal planning view for rollout meetings."""

    st.markdown("### Executive Action Center")
    st.caption(
        "Official releases = Official Music Video, Official Audio, Lyric Video. "
        "Other content = short videos (<60s) + everything else. "
        "Counts use the full filtered window; lift averages use the latest 10 releases per artist."
    )

    official_recent, other_recent = _prepare_recent_release_windows(df, per_artist_limit=10)
    board = build_release_strategy_board(df, per_artist_limit=10)
    if board.empty:
        st.info("Not enough data to build executive rollout actions yet.")
        return

    chart_rows = board.dropna(subset=["MV vs other official lift (%)"])
    if not chart_rows.empty:
        chart_rows = chart_rows.sort_values("MV vs other official lift (%)", ascending=True)
        lift_chart = px.bar(
            chart_rows,
            x="MV vs other official lift (%)",
            y="Artist",
            orientation="h",
            color="MV vs other official lift (%)",
            color_continuous_scale=[(0.0, "#B91C1C"), (0.5, "#F59E0B"), (1.0, "#15803D")],
            title="Music video lift vs other official formats (views/day, latest 10 official releases)",
        )
        lift_chart.add_vline(x=0.0, line_dash="dot", line_color="#6B7280")
        lift_chart.update_layout(coloraxis_colorbar_title="Lift %", yaxis_title="Artist", xaxis_title="Lift %")
        st.plotly_chart(lift_chart, use_container_width=True, height=360)

    st.markdown("##### Today-first rollout KPIs")
    st.dataframe(
        board,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Official release count": st.column_config.NumberColumn("Official count (window)", format="%d"),
            "Other content count": st.column_config.NumberColumn("Other count (window)", format="%d"),
            "Official avg views/day": st.column_config.NumberColumn("Official avg views/day", format="%.1f"),
            "Other avg views/day": st.column_config.NumberColumn("Other avg views/day", format="%.1f"),
            "Official vs Other lift (%)": st.column_config.NumberColumn("Official vs Other lift %", format="%.1f"),
            "MV vs other official lift (%)": st.column_config.NumberColumn(
                "MV vs other official lift %", format="%.1f"
            ),
            "Short video (<60s) share in other content (%)": st.column_config.NumberColumn(
                "Short-video share %", format="%.1f"
            ),
            "Official cadence (days)": st.column_config.NumberColumn("Official cadence (days)", format="%.0f"),
            "Today action": st.column_config.TextColumn("Today's move", width="large"),
        },
    )

    st.markdown("##### Rollout Meeting Talking Points")
    points = board.to_dict(orient="records")
    point_columns = st.columns(2)
    for idx, row in enumerate(points):
        artist_name = str(row["Artist"])
        accent = sanitize_hex_color(palette.get(artist_name), fallback=APP_RED_500)
        body = html.escape(str(row["Today action"]))
        with point_columns[idx % 2]:
            st.markdown(
                (
                    "<div style='margin-bottom:10px; padding:10px 12px; border-radius:12px; "
                    f"border-left:6px solid {accent}; background:linear-gradient(135deg,#FFFFFF,#F8FAFC); "
                    "box-shadow:0 2px 10px rgba(15,23,42,0.06);'>"
                    f"<div style='font-weight:800; color:{accent}; letter-spacing:0.2px;'>{html.escape(artist_name)}</div>"
                    f"<div style='margin-top:6px; color:#1F2937; font-weight:600; font-size:0.95rem;'>{body}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

    official_table = _prepare_release_table_for_display(official_recent)
    other_table = _prepare_release_table_for_display(other_recent)

    left, right = st.columns(2)
    with left:
        st.markdown("##### Last 10 official releases per artist")
        st.dataframe(
            official_table,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Views": st.column_config.NumberColumn(format="%d"),
                "Views/day": st.column_config.NumberColumn(format="%.1f"),
                "Engagement %": st.column_config.NumberColumn(format="%.2f"),
            },
        )
    with right:
        st.markdown("##### Last 10 other content posts per artist")
        st.dataframe(
            other_table,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Views": st.column_config.NumberColumn(format="%d"),
                "Views/day": st.column_config.NumberColumn(format="%.1f"),
                "Engagement %": st.column_config.NumberColumn(format="%.2f"),
            },
        )


def build_comment_watchlist(videos: pd.DataFrame, *, per_artist_limit: int = 2) -> pd.DataFrame:
    """Return up to N videos per artist with unusual comment arithmetic."""

    columns = [
        "Artist",
        "Thumbnail",
        "Video",
        "Why this is flagged",
        "Reason key",
        "Quick arithmetic",
        "Comments",
        "Likes",
        "Views",
        "Watch",
    ]
    required = {"artist_name", "video_id", "title", "view_count", "like_count", "comment_count"}
    if per_artist_limit < 1 or videos.empty or not required.issubset(videos.columns):
        return pd.DataFrame(columns=columns)

    typed = videos.copy()
    typed["artist_name"] = typed["artist_name"].map(_normalize_optional_text)
    typed["video_id"] = typed["video_id"].map(_normalize_optional_text)
    typed["title"] = typed["title"].map(_normalize_optional_text).replace("", "(untitled video)")
    for metric in ("view_count", "like_count", "comment_count"):
        typed[metric] = pd.to_numeric(typed[metric], errors="coerce").fillna(0.0).clip(lower=0.0)
    if "metrics_date" in typed.columns:
        typed["metrics_date"] = pd.to_datetime(typed["metrics_date"], errors="coerce")
        typed = typed.sort_values("metrics_date", ascending=False, na_position="last")
    typed = typed[typed["artist_name"].ne("") & typed["video_id"].ne("")].copy()
    typed = typed.drop_duplicates(subset=["artist_name", "video_id"], keep="first")
    typed = typed.loc[
        (typed["view_count"] >= COMMENT_WATCHLIST_MIN_VIEWS)
        & (typed["comment_count"] >= COMMENT_WATCHLIST_MIN_COMMENTS)
    ].copy()
    if typed.empty:
        return pd.DataFrame(columns=columns)

    typed["comments_per_1k_views"] = (
        typed["comment_count"] / typed["view_count"].where(typed["view_count"] > 0) * 1000.0
    ).astype(float)
    typed["comments_per_like"] = (typed["comment_count"] / typed["like_count"].where(typed["like_count"] > 0)).astype(
        float
    )
    typed["views_per_comment"] = (
        typed["view_count"] / typed["comment_count"].where(typed["comment_count"] > 0)
    ).astype(float)

    rows: list[dict[str, object]] = []
    for artist_name, artist_rows in typed.groupby("artist_name", dropna=False):
        comments_per_1k_median = float(artist_rows["comments_per_1k_views"].dropna().median())
        comments_per_like_median_series = artist_rows.loc[
            artist_rows["like_count"] >= COMMENT_WATCHLIST_MIN_LIKES,
            "comments_per_like",
        ].dropna()
        comments_per_like_median = (
            float(comments_per_like_median_series.median()) if not comments_per_like_median_series.empty else None
        )
        views_per_comment_median = float(artist_rows["views_per_comment"].dropna().median())
        metric_medians: dict[str, float | None] = {
            "comments_per_1k_views": comments_per_1k_median if comments_per_1k_median > 0 else None,
            "comments_per_like": (
                comments_per_like_median if comments_per_like_median and comments_per_like_median > 0 else None
            ),
            "views_per_comment": views_per_comment_median if views_per_comment_median > 0 else None,
        }

        for row in artist_rows.to_dict(orient="records"):
            best_reason: dict[str, object] | None = None
            likes = int(round(_coerce_float(row.get("like_count"), 0.0)))
            for spec in COMMENT_SIGNAL_SPECS:
                if likes < spec.min_like_count:
                    continue
                median_value = metric_medians.get(spec.metric_key)
                metric_value = _coerce_float(row.get(spec.metric_key), float("nan"))
                if median_value is None or not math.isfinite(metric_value) or metric_value <= 0:
                    continue
                lift = _safe_ratio(metric_value, median_value)
                if lift is None or lift < COMMENT_WATCHLIST_MIN_LIFT:
                    continue
                if best_reason is None or lift > float(best_reason["signal_lift"]):
                    best_reason = {
                        "reason_key": spec.reason_key,
                        "reason_label": spec.reason_label,
                        "math": (
                            f"{spec.arithmetic_label}: {metric_value:,.2f} vs artist median "
                            f"{median_value:,.2f} ({lift:.1f}x)"
                        ),
                        "signal_lift": lift,
                    }

            if best_reason is None:
                continue

            video_id = row.get("video_id")
            rows.append(
                {
                    "Artist": str(artist_name),
                    "Thumbnail": _youtube_thumbnail_url(video_id),
                    "Video": str(row.get("title") or "(untitled video)"),
                    "Why this is flagged": str(best_reason["reason_label"]),
                    "Reason key": str(best_reason["reason_key"]),
                    "Quick arithmetic": str(best_reason["math"]),
                    "Comments": int(round(_coerce_float(row.get("comment_count"), 0.0))),
                    "Likes": likes,
                    "Views": int(round(_coerce_float(row.get("view_count"), 0.0))),
                    "Watch": _youtube_watch_url(video_id),
                    "signal_lift": float(best_reason["signal_lift"]),
                }
            )

    if not rows:
        return pd.DataFrame(columns=columns)

    watchlist = (
        pd.DataFrame(rows)
        .sort_values(["Artist", "signal_lift", "Views"], ascending=[True, False, False], na_position="last")
        .groupby("Artist", group_keys=False)
        .head(per_artist_limit)
        .reset_index(drop=True)
    )
    return watchlist.loc[:, columns]


def render_comment_watchlist(videos: pd.DataFrame, *, per_artist_limit: int = 2, title: str) -> None:
    """Render manager-ready links for unusual comment behavior investigation."""

    st.markdown(f"### {title}")
    st.caption(
        "Two videos per artist that show unusual comment arithmetic. Use the YouTube link "
        "to quickly inspect what people are saying."
    )
    watchlist = build_comment_watchlist(videos, per_artist_limit=per_artist_limit)
    if watchlist.empty:
        st.info("No videos met the minimum thresholds for comment outlier review in this filter window.")
        return

    st.dataframe(
        watchlist,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Thumbnail": st.column_config.ImageColumn(
                "Thumbnail",
                help="YouTube preview image for quick context before opening the video.",
                width="small",
            ),
            "Video": st.column_config.TextColumn("Video", width="large"),
            "Why this is flagged": st.column_config.TextColumn("Why this is flagged", width="medium"),
            "Reason key": st.column_config.TextColumn("Reason key", width="medium"),
            "Quick arithmetic": st.column_config.TextColumn("Quick arithmetic", width="large"),
            "Comments": st.column_config.NumberColumn("Comments", format="%,d"),
            "Likes": st.column_config.NumberColumn("Likes", format="%,d"),
            "Views": st.column_config.NumberColumn("Views", format="%,d"),
            "Watch": st.column_config.LinkColumn(
                "Watch on YouTube",
                help="Open the video to investigate comment threads and audience context.",
                validate=r"^https://www\.youtube\.com/watch\?v=.+$",
                display_text="Open video",
            ),
        },
    )
    st.caption(
        "Arithmetic used: comments per 1K views = (comments / views) x 1,000; "
        "comments per like = comments / likes; views per comment = views / comments."
    )


def render_artist_focus_dashboard(
    *,
    latest: pd.DataFrame,
    normalized_filtered: pd.DataFrame,
    selected_artists: list[str],
    focus_artist: str,
    base_color_map: dict[str, str],
) -> None:
    """Render one-artist coaching dashboard with grayscale benchmarks."""

    focus_color_map = _build_focus_artist_color_map(
        selected_artists,
        focus_artist=focus_artist,
        base_color_map=base_color_map,
    )
    focus_color = focus_color_map.get(focus_artist, APP_RED_500)

    st.markdown("### Artist Coaching View")
    render_focus_artist_header(focus_artist, focus_color)
    render_benchmark_context_note(focus_artist)

    board = build_release_strategy_board(latest, per_artist_limit=10)
    scorecard = build_focus_artist_scorecard(board, focus_artist)

    focus_row = board.loc[board["Artist"] == focus_artist]
    if not focus_row.empty:
        today_action = str(focus_row.iloc[0]["Today action"])
        st.markdown(
            (
                "<div style='padding:14px 16px; border-radius:14px; "
                f"border:1px solid {focus_color}; background:linear-gradient(120deg,#FFFFFF,{APP_RED_050}); "
                "animation: focusRise .28s ease-out;'>"
                f"<div style='font-size:1.02rem; font-weight:700; color:{APP_RED_700};'>Rollout Priority Today</div>"
                f"<div style='margin-top:8px; color:#1F2937; font-weight:600;'>{html.escape(today_action)}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    if not scorecard.empty:
        st.markdown("##### Focus KPI snapshot (vs benchmark artists)")
        st.dataframe(
            scorecard,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Focus value": st.column_config.NumberColumn("Focus value", format="%.2f"),
                "Benchmark avg": st.column_config.NumberColumn("Benchmark avg", format="%.2f"),
                "Lift vs benchmark (%)": st.column_config.NumberColumn("Lift %", format="%.1f"),
                "Interpretation": st.column_config.TextColumn("Interpretation", width="large"),
            },
        )

    trend_df = build_focus_trend_frame(normalized_filtered, focus_artist)
    format_lift_df = build_focus_format_lift_table(latest, focus_artist)

    left, right = st.columns(2)
    with left:
        if trend_df.empty:
            st.info("Not enough trend points to compare focus artist vs benchmarks.")
        else:
            trend_fig = px.line(
                trend_df,
                x="metrics_date",
                y="views_per_day",
                color="Series",
                markers=True,
                color_discrete_map={"Focus artist": focus_color, "Benchmark average": APP_BENCHMARK_GRAY_DARK},
                title="Daily velocity trend: focus artist vs benchmark average",
            )
            trend_fig.update_layout(hovermode="x unified", legend_title_text="")
            st.plotly_chart(trend_fig, use_container_width=True, height=360)
    with right:
        scatter_df = latest.copy()
        if scatter_df.empty:
            st.info("No recent videos available for benchmark comparison.")
        else:
            scatter_df["Role"] = scatter_df["artist_name"].apply(
                lambda name: "Focus artist" if str(name) == focus_artist else "Benchmark artists"
            )
            scatter_fig = px.scatter(
                scatter_df,
                x="views_per_day",
                y="engagement_rate",
                color="Role",
                size="view_count",
                hover_name="title",
                hover_data={
                    "artist_name": True,
                    "view_count": ":,.0f",
                    "views_per_day": ":,.1f",
                    "engagement_rate": ":.2f",
                },
                color_discrete_map={"Focus artist": focus_color, "Benchmark artists": APP_BENCHMARK_GRAY},
                title="Latest videos: focus highlight with benchmark context",
            )
            for trace in scatter_fig.data:
                if trace.name == "Benchmark artists":
                    trace.marker.opacity = 0.35
                else:
                    trace.marker.opacity = 0.95
                    trace.marker.line = {"width": 1.5, "color": "#0F172A"}
            st.plotly_chart(scatter_fig, use_container_width=True, height=360)

    if not format_lift_df.empty:
        st.markdown("##### Format lift board (focus artist)")
        st.dataframe(
            format_lift_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Focus avg views/day": st.column_config.NumberColumn(format="%.1f"),
                "Benchmark avg views/day": st.column_config.NumberColumn(format="%.1f"),
                "Lift vs benchmark (%)": st.column_config.NumberColumn(format="%.1f"),
            },
        )

    focus_watchlist_source = latest.loc[latest["artist_name"] == focus_artist].copy()
    render_comment_watchlist(
        focus_watchlist_source,
        per_artist_limit=2,
        title=f"{focus_artist}: comment thread investigation",
    )

    official_recent, other_recent = _prepare_recent_release_windows(latest, per_artist_limit=10)
    official_focus = _prepare_release_table_for_display(
        official_recent.loc[official_recent["artist_name"] == focus_artist]
    )
    other_focus = _prepare_release_table_for_display(other_recent.loc[other_recent["artist_name"] == focus_artist])
    col_official, col_other = st.columns(2)
    with col_official:
        st.markdown(f"##### {focus_artist}: last 10 official releases")
        st.dataframe(
            official_focus,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Views": st.column_config.NumberColumn(format="%d"),
                "Views/day": st.column_config.NumberColumn(format="%.1f"),
                "Engagement %": st.column_config.NumberColumn(format="%.2f"),
            },
        )
    with col_other:
        st.markdown(f"##### {focus_artist}: last 10 other-content posts")
        st.dataframe(
            other_focus,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Views": st.column_config.NumberColumn(format="%d"),
                "Views/day": st.column_config.NumberColumn(format="%.1f"),
                "Engagement %": st.column_config.NumberColumn(format="%.2f"),
            },
        )


def render_top_videos(df: pd.DataFrame, limit: int) -> None:
    if df.empty:
        st.info("No videos match the current filters.")
        return
    required_cols = [
        "title",
        "artist_name",
        "view_count",
        "views_per_day",
        "engagement_rate",
        "like_rate",
        "comment_rate",
    ]
    if not ensure_columns(df, required_cols, "Top videos table"):
        return

    top_videos = (
        df.sort_values("view_count", ascending=False)
        .head(limit)
        .loc[
            :,
            [
                "title",
                "artist_name",
                "view_count",
                "views_per_day",
                "engagement_rate",
                "like_rate",
                "comment_rate",
            ],
        ]
    )

    renamed = top_videos.rename(
        columns={
            "artist_name": "Artist",
            "title": "Video",
            "view_count": "Views",
            "views_per_day": "Views/Day",
            "engagement_rate": "Engagement %",
            "like_rate": "Like %",
            "comment_rate": "Comment %",
        }
    )

    st.dataframe(
        renamed,
        column_config={
            "Views": st.column_config.NumberColumn(format="%,.0f"),
            "Views/Day": st.column_config.NumberColumn(format="%,.1f"),
            "Engagement %": st.column_config.NumberColumn(format="%.2f"),
            "Like %": st.column_config.NumberColumn(format="%.2f"),
            "Comment %": st.column_config.NumberColumn(format="%.2f"),
        },
        use_container_width=True,
        hide_index=True,
        height=360,
        placeholder="—",
    )

    def _to_csv() -> bytes:
        return renamed.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="📥 Download current view (CSV)",
        data=_to_csv,
        file_name=f"musicscope_top_videos_{date.today().isoformat()}.csv",
        mime="text/csv",
        type="primary",
        use_container_width=False,
        help="Export the currently filtered top videos table.",
    )


def main() -> None:
    """Entry point for the MusicScope Streamlit dashboard.

    The app supports two data modes:

    - **Demo Mode** (default for new users): loads a small curated cohort from
      ``demo_data/curated_cohort.json`` with no database or API setup.
    - **Production (MySQL)**: uses the analytics warehouse via DB_* settings.
      In Streamlit Cloud, this mode requires explicit intent via
      ``MUSICSCOPE_DATA_MODE=production`` (or ``TRACKSTATS_DATA_MODE=production``)
      to avoid accidental localhost DB failures.
    """

    st.set_page_config(page_title="TrackStats YT™", layout="wide")
    inject_dashboard_motion_styles()

    col_title, col_mode = st.columns([4, 1])
    with col_title:
        st.title("TrackStats YT™ live roster snapshot")

    mode = get_data_mode()
    with col_mode:
        label = "Demo Mode" if mode == "demo" else "Production (MySQL)"
        st.badge(label)

    palette = GLOBAL_ARTIST_PALETTE.copy()
    normalized_videos = load_normalized_videos(mode)
    normalized_videos, excluded_artists = normalize_artist_dimension(
        normalized_videos,
        palette=palette,
        drop_untracked=True,
    )
    if normalized_videos.empty:
        st.error("Normalized video metrics are empty. Run the ETL to refresh inputs.")
        st.stop()
    artist_summary = build_artist_summary_from_metrics(normalized_videos)
    if artist_summary.empty:
        st.error("Artist summary is empty. Run the ETL to generate fresh aggregates.")
        st.stop()

    if mode == "demo":
        st.info(
            f"📊 **Data Source: Demo Mode** — curated cohort of {len(artist_summary):d} artists "
            "with realistic metrics. No database setup required."
        )
    else:
        st.success("🔗 **Data Source: Production (MySQL)** — live data from the YouTube analytics warehouse.")

    if excluded_artists:
        st.warning(
            "Data check: Excluded untracked artist labels from dashboard view: "
            + ", ".join(excluded_artists)
            + ". Source data remains intact.",
        )

    latest_metrics_at: datetime | None = None
    if "metrics_date" in normalized_videos.columns:
        latest_metrics_at = _coerce_utc_datetime(normalized_videos["metrics_date"].max())
    latest_etl_run_at = load_latest_successful_etl_run_at() if mode == "production" else None
    latest_etl_run_date = latest_etl_run_at.date() if latest_etl_run_at is not None else None

    # In production mode, freshness is based on ETL heartbeat when available.
    if mode == "production":
        freshness_days = _get_data_freshness_days()
        freshness_anchor_at, freshness_source = _resolve_freshness_anchor(
            mode=mode,
            latest_metrics_at=latest_metrics_at,
            latest_etl_run_at=latest_etl_run_at,
        )
        if freshness_anchor_at is None:
            st.error(
                "Production (MySQL) mode has no freshness timestamp "
                "(no metrics_date and no completed ETL run). Run ETL before demoing.",
            )
            st.stop()

        age_days = (datetime.now(timezone.utc) - freshness_anchor_at).days
        if age_days > freshness_days:
            st.error(
                f"Production (MySQL) data freshness is stale: {freshness_source} "
                f"is {age_days} days old (limit: {freshness_days}). Run ETL before demoing.",
            )
            st.stop()

        if (
            latest_etl_run_at is not None
            and latest_metrics_at is not None
            and latest_etl_run_at.date() > latest_metrics_at.date()
        ):
            st.info(
                "Fresh snapshot confirmed by ETL heartbeat "
                f"({latest_etl_run_at.date().isoformat()}). No new video metric rows "
                f"were added after {latest_metrics_at.date().isoformat()}.",
            )

    available_artists = sorted(artist_summary["artist_name"].unique().tolist())

    # Hover-based sidebar navigation with filters to keep the main canvas clean
    with st.sidebar:
        if on_hover_tabs is None:
            # st.warning("⚠️ Hover tabs unavailable; using fallback navigation.")
            tabs = option_menu(
                menu_title=None,
                options=["Filters", "About"],
                icons=["filter", "info-circle"],
                default_index=0,
                orientation="horizontal",
            )
        else:
            tabs = on_hover_tabs(
                tabName=["Filters", "About"],
                iconName=["filter", "info-circle"],
                default_choice=0,
            )

        if tabs == "Filters":
            st.header("Filters")
        else:
            st.header("About this demo")
            st.markdown(
                "This hover sidebar keeps the main canvas focused on storytelling "
                "while still giving you quick control over artist and date filters."
            )
            add_vertical_space(2)

        selected_artists = st.multiselect("Artists", available_artists, default=available_artists)
    if not selected_artists:
        st.warning("Select at least one artist to explore the dashboard.")
        st.stop()

    try:
        min_date, max_date = resolve_metrics_date_window(normalized_videos)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    # LOUD GUARDRAIL: Streamlit range slider crashes if min == max.
    # Keep single-day snapshots usable by bypassing slider rendering.
    if min_date == max_date:
        st.sidebar.info(f"Single metrics snapshot date: {min_date.isoformat()}")
        start_date, end_date = select_metrics_window(min_date=min_date, max_date=max_date)
    else:
        # Use a slider for date range selection, defaulting to the full history.
        date_selection = st.sidebar.slider(
            "Metrics window",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD",
        )
        # st.slider with a tuple value always returns a tuple of 2.
        start_date, end_date = select_metrics_window(
            min_date=min_date,
            max_date=max_date,
            selected_window=date_selection,
        )

    top_n = st.sidebar.slider("Show top videos", min_value=5, max_value=25, value=10, step=1)
    st.sidebar.metric("Latest metrics date", max_date.isoformat())
    if mode == "production" and latest_etl_run_date is not None:
        st.sidebar.metric("Latest ETL heartbeat", latest_etl_run_date.isoformat())
    st.sidebar.metric("Rows loaded", f"{len(normalized_videos):,}")

    window_filtered_all = filter_by_date_window(normalized_videos, (start_date, end_date))
    normalized_filtered = filter_by_artists(window_filtered_all, selected_artists)
    latest_roster = latest_snapshot(window_filtered_all)
    latest = filter_by_artists(latest_roster, selected_artists)
    color_map = build_color_discrete_map(selected_artists, palette)

    # Top-level navigation for different storytelling modes
    selected_view = option_menu(
        menu_title=None,
        options=["Overview", "Artist Deep Dive", "Velocity Analysis"],
        icons=["bar-chart-fill", "person-lines-fill", "lightning-fill"],
        orientation="horizontal",
    )

    # Layout and content depend slightly on the selected high-level view,
    # but always keep the story action-oriented and insight-first.
    if selected_view == "Overview":
        render_kpis(
            artist_summary,
            selected_artists,
            latest,
            latest_roster,
            mode=mode,
            latest_etl_run_date=latest_etl_run_date,
        )

        col1, col2 = st.columns(2)
        with col1:
            render_trend_chart(normalized_filtered, color_map)
        with col2:
            render_velocity_scatter(latest, color_map)

        st.markdown("### Content strategy signals")
        render_content_mix(latest)
        render_artist_content_mix(latest)
        render_executive_action_center(latest, palette=palette)
        render_comment_watchlist(latest, per_artist_limit=2, title="Comment thread investigation")

        ui.card(
            content=(
                "**So what?** Use this view to spot which artists are quietly "
                "compounding views and engagement. Big bubbles high and to the "
                "right are your next breakout campaigns waiting to happen."
            ),
        )

        st.markdown(f"### Top {top_n} performing videos (latest metrics)")
        render_top_videos(latest, limit=top_n)

    elif selected_view == "Artist Deep Dive":
        focus_artist = st.selectbox(
            "Focus artist",
            selected_artists,
            index=0,
            format_func=lambda artist_name: f"{artist_name} • {sanitize_hex_color(color_map.get(artist_name))}",
            help=(
                "Choose one artist to coach. Dropdown includes assigned palette color; "
                "peers stay on screen as grayscale benchmarks."
            ),
        )
        focus_artist_color = sanitize_hex_color(color_map.get(focus_artist), fallback=APP_RED_500)
        st.markdown(
            (
                "<div style='margin-top:2px; margin-bottom:8px; font-weight:900; "
                f"font-size:1.3rem; letter-spacing:0.3px; color:{focus_artist_color};'>"
                f"{html.escape(focus_artist)}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        render_kpis(
            artist_summary,
            [focus_artist],
            latest,
            latest,
            mode=mode,
            latest_etl_run_date=latest_etl_run_date,
        )
        render_artist_focus_dashboard(
            latest=latest,
            normalized_filtered=normalized_filtered,
            selected_artists=selected_artists,
            focus_artist=focus_artist,
            base_color_map=color_map,
        )

    else:  # "Velocity Analysis"
        st.markdown("### Velocity & momentum")
        render_velocity_scatter(latest, color_map)
        ui.card(
            content=(
                "**So what?** High views/day with strong engagement tells you "
                "where fan energy is peaking *right now* so you can time your "
                "next release, sync, or tour push."
            ),
        )


if __name__ == "__main__":
    import sys

    from streamlit.web import cli as stcli

    if st.runtime.exists():
        main()
    else:
        sys.argv = ["streamlit", "run", sys.argv[0]]
        sys.exit(stcli.main())
