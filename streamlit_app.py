from __future__ import annotations

import math
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Literal, Tuple

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
# 2) Revenue KPI must show explicit TOS-safe arithmetic in plain language:
#    Estimated revenue (USD) = (Total views / 1,000) x RPM (USD per 1,000 views).
# 3) If Shorts/video length affects estimates, explain this very simply.
# 4) Never override artist stylistic casing choices automatically.
# 5) KPI deltas shown in green/red must have matching arithmetic + actions;
#    otherwise hide the delta.
# 6) In Streamlit Cloud, do not auto-enter Production mode from repo/env
#    DB_* values alone; require explicit secrets/session intent.
# ======================================================================

CACHE_TTL_SECONDS = 900  # 15 minutes
DATA_FRESHNESS_DAYS_ENV = "DATA_FRESHNESS_DAYS"
DEFAULT_DATA_FRESHNESS_DAYS = 30
RPM_VARIATION_TOLERANCE = 0.05
REVENUE_RPM_DEFAULT_ENV = "REVENUE_RPM_DEFAULT"
DEFAULT_REVENUE_RPM_USD = 2.5
SHORTS_MAX_SECONDS = 60
DATA_MODE_SETTING_KEYS = ("MUSICSCOPE_DATA_MODE", "TRACKSTATS_DATA_MODE")

try:
    # Disable on_hover_tabs due to local loading issues (assets not found)
    # from st_on_hover_tabs import on_hover_tabs
    on_hover_tabs = None
except ModuleNotFoundError:  # pragma: no cover - external dependency guard
    on_hover_tabs = None

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "music_analysis_tables"
DEMO_DATA_PATH = BASE_DIR / "demo_data" / "curated_cohort.json"


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

    Uses known cloud env flags first, then falls back to the historical
    `/mount/src/` working-directory pattern as a compatibility safeguard.
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
        "Invalid data mode value. Use 'demo' or 'production' for "
        "MUSICSCOPE_DATA_MODE / TRACKSTATS_DATA_MODE.",
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
    """Classify video type from duration with a simple Shorts cutoff."""

    seconds = _parse_iso8601_duration_seconds(duration)
    if seconds is None:
        return "Video"
    if seconds <= SHORTS_MAX_SECONDS:
        return "Short"
    return "Official Music Video"


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
def _load_demo_cohort() -> dict:
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
        return pd.read_json(DEMO_DATA_PATH).to_dict()  # type: ignore[no-any-return]
    except ValueError:
        # Fall back to manual JSON load if structure is not tabular
        import json

        with DEMO_DATA_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)


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
    payload = _load_demo_cohort()
    rows = []
    for artist in payload.get("artists", []):
        metrics = artist.get("metrics", {})
        rows.append(
            {
                "artist_name": artist.get("name"),
                "total_views": metrics.get("total_views", 0),
                "total_videos": metrics.get("total_videos", 0),
                "total_likes": 0,
                "total_comments": 0,
                "total_est_revenue_usd": metrics.get("total_views", 0) * 0.0015,
                "avg_engagement_rate": metrics.get("engagement_rate", 0.0),
            }
        )
    return pd.DataFrame(rows)


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

    for column in ["view_count", "like_count", "comment_count"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    rpm_usd = _read_float_env(REVENUE_RPM_DEFAULT_ENV, DEFAULT_REVENUE_RPM_USD)
    df["est_revenue_usd"] = (df["view_count"] / 1000.0) * rpm_usd

    age_days = (df["metrics_date"] - df["published_at"]).dt.days
    df["days_since_publish"] = age_days.fillna(1).clip(lower=1)
    df["views_per_day"] = df["view_count"] / df["days_since_publish"]

    views_nonzero = df["view_count"].astype(float).where(df["view_count"] > 0)
    df["like_rate"] = ((df["like_count"] / views_nonzero) * 100).astype(float).fillna(0.0)
    df["comment_rate"] = ((df["comment_count"] / views_nonzero) * 100).astype(float).fillna(0.0)
    df["engagement_rate"] = df["like_rate"] + df["comment_rate"]
    df["video_type"] = df["duration"].map(_classify_video_type_from_duration)

    return df


def load_normalized_videos_from_demo() -> pd.DataFrame:
    payload = _load_demo_cohort()
    records = []
    for artist in payload.get("artists", []):
        name = artist.get("name")
        for video in artist.get("videos", []):
            records.append(
                {
                    "video_id": video.get("video_id"),
                    "artist_name": name,
                    "title": video.get("title"),
                    "metrics_date": pd.to_datetime(video.get("published_at")),
                    "view_count": video.get("view_count", 0),
                    "views_per_day": video.get("views_per_day", 0.0),
                    "engagement_rate": video.get("engagement_rate", 0.0),
                    "like_rate": video.get("like_rate", 0.0),
                    "comment_rate": video.get("comment_rate", 0.0),
                    "video_type": video.get("video_type", "Official Music Video"),
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
                "total_est_revenue_usd",
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
            total_est_revenue_usd=("est_revenue_usd", "sum"),
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


def filter_by_artists(df: pd.DataFrame, artists: list[str]) -> pd.DataFrame:
    if not artists:
        return df.iloc[0:0]
    return df[df["artist_name"].isin(artists)]


def filter_by_date_window(df: pd.DataFrame, window: Tuple[date, date]) -> pd.DataFrame:
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
    avg_engagement: float,
    roster_avg_engagement: float,
    revenue_per_artist: float,
    roster_revenue_per_artist: float,
) -> pd.DataFrame:
    """Build arithmetic-backed rows for every displayed KPI delta."""

    specs = [
        (
            "Total views",
            views_per_artist,
            roster_views_per_artist,
            "Scale the format mix that is already pulling reach.",
        ),
        ("Videos analyzed", videos_per_artist, roster_videos_per_artist, "Tune release cadence to match capacity."),
        (
            "Total likes",
            likes_per_artist,
            roster_likes_per_artist,
            "Double down on hooks/creative that drives positive reactions.",
        ),
        (
            "Total comments",
            comments_per_artist,
            roster_comments_per_artist,
            "Prioritize call-to-action formats that trigger conversation.",
        ),
        (
            "Avg engagement rate",
            avg_engagement,
            roster_avg_engagement,
            "Replicate the top engagement format with tighter iteration loops.",
        ),
        (
            "Est. revenue (USD)",
            revenue_per_artist,
            roster_revenue_per_artist,
            "Allocate budget toward the highest-yield format first.",
        ),
    ]
    rows: list[dict[str, str]] = []
    for name, current, baseline, action in specs:
        change = compute_pct_delta(current, baseline)
        delta_text = format_delta_value(change)
        if delta_text is None:
            continue
        rows.append(
            {
                "KPI": name,
                "Delta": delta_text,
                "Arithmetic": f"(({current:,.2f} / {baseline:,.2f}) - 1) x 100",
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
            if "est_revenue_usd" in selected_rows.columns:
                total_revenue = float(selected_rows["est_revenue_usd"].sum())
            else:
                rpm = _read_float_env(REVENUE_RPM_DEFAULT_ENV, DEFAULT_REVENUE_RPM_USD)
                total_revenue = float(total_views / 1000.0 * rpm)
            avg_engagement = (
                float(selected_rows["engagement_rate"].mean()) if "engagement_rate" in selected_rows.columns else 0.0
            )
            selected_artist_count = max(int(selected_rows["artist_name"].nunique()), 1)

    if not use_video_math:
        total_views = int(filtered_summary["total_views"].sum())
        total_videos = int(filtered_summary["total_videos"].sum())
        total_likes = int(filtered_summary["total_likes"].sum())
        total_comments = int(filtered_summary["total_comments"].sum())
        total_revenue = float(filtered_summary["total_est_revenue_usd"].sum())
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
        if "est_revenue_usd" in roster_rows.columns:
            roster_revenue_per_artist = float(roster_rows["est_revenue_usd"].sum()) / roster_artist_count
        else:
            rpm = _read_float_env(REVENUE_RPM_DEFAULT_ENV, DEFAULT_REVENUE_RPM_USD)
            roster_revenue_per_artist = float(roster_rows["view_count"].sum()) / 1000.0 * rpm / roster_artist_count
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
        roster_revenue_per_artist = summary["total_est_revenue_usd"].sum() / max(len(summary), 1)
        roster_avg_engagement = float(summary["avg_engagement_rate"].mean())

    return {
        "total_views": total_views,
        "total_videos": total_videos,
        "total_likes": total_likes,
        "total_comments": total_comments,
        "total_revenue": total_revenue,
        "avg_engagement": avg_engagement,
        "selected_artist_count": selected_artist_count,
        "roster_views_per_artist": roster_views_per_artist,
        "roster_videos_per_artist": roster_videos_per_artist,
        "roster_likes_per_artist": roster_likes_per_artist,
        "roster_comments_per_artist": roster_comments_per_artist,
        "roster_revenue_per_artist": roster_revenue_per_artist,
        "roster_avg_engagement": roster_avg_engagement,
    }


def build_artist_content_action_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return artist-level action rows from content-type KPIs."""

    if df.empty:
        return pd.DataFrame(columns=["Artist", "Best Reach Format", "Best Engagement Format", "Action Plan"])

    mix = (
        df.groupby(["artist_name", "video_type"], dropna=False)
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

        reach_format = "Short / Reel" if str(best_reach["video_type"]) == "Short" else str(best_reach["video_type"])
        engagement_format = (
            "Short / Reel" if str(best_engagement["video_type"]) == "Short" else str(best_engagement["video_type"])
        )

        if reach_format == engagement_format:
            action = (
                f"Primary bet: {reach_format}. Keep >=70% of next releases in this format; "
                "A/B test titles and openings."
            )
        else:
            action = (
                f"Use 70/30 split: 70% {reach_format} for reach and 30% "
                f"{engagement_format} for deeper fan response."
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


def _compute_rpm_by_video_type(videos: pd.DataFrame | None) -> dict[str, float]:
    """Compute RPM (USD per 1,000 views) by video type from video rows."""

    if videos is None or videos.empty:
        return {}
    required = {"video_type", "view_count", "est_revenue_usd"}
    if not required.issubset(videos.columns):
        return {}

    typed = videos[list(required)].copy()
    typed["view_count"] = pd.to_numeric(typed["view_count"], errors="coerce")
    typed["est_revenue_usd"] = pd.to_numeric(typed["est_revenue_usd"], errors="coerce")
    typed = typed[(typed["view_count"] > 0) & typed["est_revenue_usd"].notna()]
    if typed.empty:
        return {}

    grouped = typed.groupby("video_type", dropna=False).agg(
        total_views=("view_count", "sum"),
        total_revenue=("est_revenue_usd", "sum"),
    )
    grouped = grouped[grouped["total_views"] > 0]
    if grouped.empty:
        return {}

    grouped["rpm"] = (grouped["total_revenue"] / grouped["total_views"]) * 1000.0
    rpm_by_type: dict[str, float] = {}
    for idx, row in grouped.iterrows():
        label = str(idx).strip()
        label = label if label and label.lower() != "nan" else "Unknown"
        rpm_by_type[label] = float(row["rpm"])
    return rpm_by_type


def build_revenue_formula_context(
    summary_filtered: pd.DataFrame,
    videos_filtered: pd.DataFrame | None = None,
) -> dict[str, str]:
    """Build explicit formula text for the estimated revenue KPI."""

    total_views = float(summary_filtered["total_views"].sum()) if "total_views" in summary_filtered.columns else 0.0
    total_revenue = (
        float(summary_filtered["total_est_revenue_usd"].sum())
        if "total_est_revenue_usd" in summary_filtered.columns
        else 0.0
    )
    blended_rpm = (total_revenue / total_views) * 1000.0 if total_views > 0 else 0.0

    equation = "Estimated revenue (USD) = (Total views / 1,000) x RPM (USD per 1,000 views)"
    worked_example = (
        f"Current selection: ({format_number(total_views)} / 1,000) x "
        f"${blended_rpm:.2f} = {format_currency(total_revenue)}"
    )

    has_video_rows = videos_filtered is not None and not videos_filtered.empty
    rpm_by_type = _compute_rpm_by_video_type(videos_filtered)
    if not has_video_rows or not rpm_by_type:
        type_note = (
            "Shorts vs music videos: no type-level revenue rows are available for "
            "the current filters. Video length is not in this formula."
        )
    elif len(rpm_by_type) < 2:
        type_note = (
            "Shorts vs music videos: this model uses one RPM value for all video "
            "types. Video length is not in this formula."
        )
    else:
        rpm_values = list(rpm_by_type.values())
        spread = max(rpm_values) - min(rpm_values)
        if spread <= RPM_VARIATION_TOLERANCE:
            type_note = (
                "Shorts vs music videos: this model is effectively using the same RPM "
                "across video types in this view. Video length is not in this formula."
            )
        else:
            top_types = sorted(rpm_by_type.items(), key=lambda item: item[1], reverse=True)
            sample = "; ".join(f"{name}: ${rpm:.2f}" for name, rpm in top_types[:4])
            type_note = (
                "Shorts vs music videos: this view uses different RPM values by video type. "
                "Per-video arithmetic is still: (video views / 1,000) x type RPM. "
                f"Current type RPMs (USD per 1,000 views): {sample}."
            )

    return {
        "equation": equation,
        "worked_example": worked_example,
        "type_note": type_note,
    }


def render_kpis(
    summary: pd.DataFrame,
    artists: list[str],
    videos: pd.DataFrame | None = None,
    roster_videos: pd.DataFrame | None = None,
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
    total_revenue = float(context["total_revenue"])
    avg_engagement = float(context["avg_engagement"])
    selected_artist_count = int(context["selected_artist_count"])

    # Roster-wide baselines for directional context
    roster_views_per_artist = float(context["roster_views_per_artist"])
    roster_videos_per_artist = float(context["roster_videos_per_artist"])
    roster_likes_per_artist = float(context["roster_likes_per_artist"])
    roster_comments_per_artist = float(context["roster_comments_per_artist"])
    roster_revenue_per_artist = float(context["roster_revenue_per_artist"])
    roster_avg_engagement = float(context["roster_avg_engagement"])
    views_per_artist = total_views / selected_artist_count
    videos_per_artist = total_videos / selected_artist_count
    likes_per_artist = total_likes / selected_artist_count
    comments_per_artist = total_comments / selected_artist_count
    revenue_per_artist = total_revenue / selected_artist_count

    views_delta = format_delta_value(compute_pct_delta(views_per_artist, roster_views_per_artist))
    videos_delta = format_delta_value(compute_pct_delta(videos_per_artist, roster_videos_per_artist))
    likes_delta = format_delta_value(compute_pct_delta(likes_per_artist, roster_likes_per_artist))
    comments_delta = format_delta_value(compute_pct_delta(comments_per_artist, roster_comments_per_artist))
    revenue_delta = format_delta_value(compute_pct_delta(revenue_per_artist, roster_revenue_per_artist))
    engagement_delta = format_delta_value(compute_pct_delta(avg_engagement, roster_avg_engagement))

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
        "Avg engagement rate",
        format_percent(avg_engagement),
        delta=engagement_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Engagement rate vs roster-wide average",
    )
    c6.metric(
        "Est. revenue (USD)",
        format_currency(total_revenue),
        delta=revenue_delta,
        delta_color="normal",
        delta_arrow="auto",
        help=(
            "Per-artist estimated revenue vs roster-wide average. "
            "Formula: (Total views / 1,000) x RPM (USD per 1,000 views)."
        ),
    )

    # Apply modern, card-like styling to the KPI strip
    style_metric_cards(
        background_color="#F0F2F6",
        border_left_color="#FF4B4B",
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

    formula_summary = pd.DataFrame(
        [{"total_views": total_views, "total_est_revenue_usd": total_revenue}],
    )
    formula_context = build_revenue_formula_context(formula_summary, videos)
    st.markdown("##### Estimated revenue arithmetic (TOS-safe explicit formula)")
    st.code(formula_context["equation"], language="text")
    st.caption(formula_context["worked_example"])
    st.caption("No hidden score is used. RPM means explicit USD per 1,000 views arithmetic.")
    st.caption("This is a directional estimate from public metrics, not a YouTube payout statement.")
    st.caption(formula_context["type_note"])

    delta_rows = build_delta_signal_rows(
        views_per_artist=views_per_artist,
        roster_views_per_artist=roster_views_per_artist,
        videos_per_artist=videos_per_artist,
        roster_videos_per_artist=roster_videos_per_artist,
        likes_per_artist=likes_per_artist,
        roster_likes_per_artist=roster_likes_per_artist,
        comments_per_artist=comments_per_artist,
        roster_comments_per_artist=roster_comments_per_artist,
        avg_engagement=avg_engagement,
        roster_avg_engagement=roster_avg_engagement,
        revenue_per_artist=revenue_per_artist,
        roster_revenue_per_artist=roster_revenue_per_artist,
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


def render_trend_chart(df: pd.DataFrame, color_map: dict[str, str]) -> None:
    if df.empty:
        st.info("No time-series data available for this selection.")
        return
    if not ensure_columns(df, ["metrics_date", "artist_name", "view_count", "views_per_day"], "Trend chart"):
        return

    trend = (
        df.groupby(["metrics_date", "artist_name"])
        .agg(view_count=("view_count", "sum"), views_per_day=("views_per_day", "mean"))
        .reset_index()
        .sort_values("metrics_date")
    )
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

    mix = (
        df.groupby("video_type")
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
        "xAxis": {"type": "category", "data": mix["video_type"].tolist()},
        "yAxis": {"type": "value"},
        "series": [
            {
                "name": "Total views",
                "type": "bar",
                "data": mix["total_views"].tolist(),
                "itemStyle": {"color": "#FF4B4B"},
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

    mix = (
        df.groupby(["artist_name", "video_type"], dropna=False)
        .agg(
            video_count=("video_id", "nunique"),
            total_views=("view_count", "sum"),
            avg_views_per_day=("views_per_day", "mean"),
            avg_engagement=("engagement_rate", "mean"),
        )
        .reset_index()
    )
    mix["video_type_label"] = mix["video_type"].replace({"Short": "Short / Reel"})

    fig = px.bar(
        mix,
        x="artist_name",
        y="video_count",
        color="video_type_label",
        title="Video content mix by artist (counts by format)",
        barmode="stack",
        hover_data={
            "total_views": ":,.0f",
            "avg_views_per_day": ":,.1f",
            "avg_engagement": ":.2f",
            "video_type": False,
        },
    )
    fig.update_layout(xaxis_title="Artist", yaxis_title="Videos", legend_title_text="Format")
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

    col_title, col_mode = st.columns([4, 1])
    with col_title:
        st.title("TrackStats YT™ live roster snapshot")

    mode = get_data_mode()
    with col_mode:
        label = "Demo Mode" if mode == "demo" else "Production (MySQL)"
        st.badge(label)

    if mode == "demo":
        st.info(
            "📊 **Data Source: Demo Mode** — curated cohort of 5 artists with "
            "realistic metrics. No database setup required."
        )
    else:
        st.success("🔗 **Data Source: Production (MySQL)** — live data from the YouTube analytics warehouse.")

    artist_summary = load_artist_summary(mode)
    normalized_videos = load_normalized_videos(mode)
    if artist_summary.empty:
        st.error("Artist summary is empty. Run the ETL to generate fresh aggregates.")
        st.stop()
    if normalized_videos.empty:
        st.error("Normalized video metrics are empty. Run the ETL to refresh inputs.")
        st.stop()

    # In production mode, enforce a freshness window against normalized_videos.
    if mode == "production" and "metrics_date" in normalized_videos.columns:
        freshness_days = _get_data_freshness_days()
        latest_metrics_value = normalized_videos["metrics_date"].max()
        if not pd.isna(latest_metrics_value):
            latest_dt = pd.to_datetime(latest_metrics_value).to_pydatetime()
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - latest_dt).days
            if age_days > freshness_days:
                st.error(
                    "Production (MySQL) data is older than the configured "
                    f"freshness window ({freshness_days} days). Run the ETL "
                    "pipeline before demoing this dashboard.",
                )
                st.stop()

    palette = get_artist_color_palette()

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

    metrics_dates = normalized_videos["metrics_date"].dt.date
    min_date = metrics_dates.min()
    max_date = metrics_dates.max()

    # Use a slider for date range selection, defaulting to the full history
    date_selection = st.sidebar.slider(
        "Metrics window", min_value=min_date, max_value=max_date, value=(min_date, max_date), format="YYYY-MM-DD"
    )

    # st.slider with a tuple value always returns a tuple of 2
    start_date, end_date = date_selection

    top_n = st.sidebar.slider("Show top videos", min_value=5, max_value=25, value=10, step=1)
    st.sidebar.metric("Latest metrics date", max_date.isoformat())
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
        render_kpis(artist_summary, selected_artists, latest, latest_roster)

        col1, col2 = st.columns(2)
        with col1:
            render_trend_chart(normalized_filtered, color_map)
        with col2:
            render_velocity_scatter(latest, color_map)

        st.markdown("### Content strategy signals")
        render_content_mix(latest)
        render_artist_content_mix(latest)

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
        render_kpis(artist_summary, selected_artists, latest, latest_roster)
        st.markdown("Dive into per-artist performance and content mix to understand why certain videos overperform.")
        render_content_mix(latest)
        render_artist_content_mix(latest)

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
