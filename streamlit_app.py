from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Literal, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from portfolio.io import get_export_root, load_insight_table, read_manifest, resolve_run_id
from streamlit_echarts import st_echarts
from streamlit_extras.add_vertical_space import add_vertical_space
from streamlit_extras.let_it_rain import rain
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_on_Hover_tabs import on_hover_tabs
from streamlit_option_menu import option_menu
import streamlit_shadcn_ui as ui

from youtubeviz.viz_theme import build_color_discrete_map, get_artist_color_palette
from web.etl_helpers import get_engine, read_sql_safe

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "music_analysis_tables"
DEMO_DATA_PATH = BASE_DIR / "demo_data" / "curated_cohort.json"

# Cache + freshness configuration
CACHE_TTL_SECONDS = int(os.getenv("MUSICSCOPE_CACHE_TTL_SECONDS", "3600"))
DATA_FRESHNESS_DAYS_ENV = "DATA_FRESHNESS_DAYS"
DEFAULT_DATA_FRESHNESS_DAYS = 30


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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
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


def _get_db_setting(name: str) -> str | None:
    """Lookup a DB_* setting from Streamlit secrets, session_state, or env.

    This keeps resolution consistent anywhere we need database credentials
    and avoids subtle differences between demo and production modes.
    """

    if name in st.secrets:
        value = st.secrets[name]
        if value:
            return str(value)
    if name in st.session_state:
        value = st.session_state[name]
        if value:
            return str(value)
    env_value = os.getenv(name)
    if env_value:
        return env_value
    return None


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

    any_present = any(_get_db_setting(key) is not None for key in required_keys)
    if not any_present:
        # No DB intent configured → stay in demo mode using curated cohort.
        return "demo"

    missing = [key for key in required_keys if _get_db_setting(key) is None]
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
        st.caption(f"Connection details: {exc}")
        st.stop()


def _get_data_freshness_days() -> int:
    """Return maximum allowed age (in days) for production metrics.

    Controlled via DATA_FRESHNESS_DAYS; defaults to 30 days and coerced to at
    least 1 day if misconfigured.
    """

    raw = os.getenv(DATA_FRESHNESS_DAYS_ENV)
    if raw is None:
        return DEFAULT_DATA_FRESHNESS_DAYS
    try:
        value = int(raw)
        return max(1, value)
    except ValueError:
        return DEFAULT_DATA_FRESHNESS_DAYS


# Portfolio export helpers
def _get_exports_dir() -> Path:
    """Return the base directory for notebook exports (env overrideable)."""

    return get_export_root()


def list_portfolio_cohorts() -> list[str]:
    exports_dir = _get_exports_dir()
    if not exports_dir.exists():
        return []
    return sorted([p.name for p in exports_dir.iterdir() if p.is_dir()])


def _load_portfolio_manifest(cohort_slug: str) -> tuple[dict | None, str | None]:
    base_dir = _get_exports_dir()
    run_id = resolve_run_id(base_dir, cohort_slug)
    if not run_id:
        return None, None
    try:
        manifest = read_manifest(base_dir, cohort_slug, run_id)
        return manifest, run_id
    except FileNotFoundError:
        return None, run_id
    except Exception as exc:  # noqa: BLE001
        st.warning(f"Could not read manifest for {cohort_slug} (run {run_id}): {exc}")
        return None, run_id


def load_portfolio_table(name: str, cohort_slug: str, run_id: str) -> pd.DataFrame:
    base_dir = _get_exports_dir()
    try:
        return load_insight_table(base_dir, cohort_slug, run_id, name)
    except FileNotFoundError:
        st.warning(f"Missing {name}.csv for cohort {cohort_slug} (run {run_id}).")
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed to load {name}.csv for cohort {cohort_slug}: {exc}")
    return pd.DataFrame()


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
                "total_est_revenue_usd": metrics.get("total_views", 0) * 0.0015,
                "avg_engagement_rate": metrics.get("engagement_rate", 0.0),
            }
        )
    return pd.DataFrame(rows)


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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_artist_summary(mode: str | None = None) -> pd.DataFrame:
    """Load artist summary for either demo or production mode.

    When *mode* is omitted, this function will auto-detect via get_data_mode().
    Passing the already-computed mode from main() avoids redundant DB
    connection checks on first page load.
    """

    if mode is None:
        mode = get_data_mode()
    if mode == "demo":
        df = load_artist_summary_from_demo()
    else:
        df = _load_csv(DATA_DIR / "artist_music_summary.csv")

    # Stamp cache metadata for diagnostics
    df.attrs["cache_refreshed_at"] = datetime.now(timezone.utc)
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_normalized_videos(mode: str | None = None) -> pd.DataFrame:
    """Load normalized video metrics for either demo or production mode.

    Accepts an optional *mode* to share detection with callers.
    """

    if mode is None:
        mode = get_data_mode()
    if mode == "demo":
        df = load_normalized_videos_from_demo()
    else:
        df = _load_csv(
            DATA_DIR / "normalized_music_videos.csv",
            parse_dates=["published_at", "metrics_date", "fetched_at"],
        )

    df.attrs["cache_refreshed_at"] = datetime.now(timezone.utc)
    return df


def get_production_health() -> dict:
    """Compute production database + analytics health indicators.

    This is intentionally side-effect free so it can be reused by tests and
    the Diagnostics view without depending on Streamlit runtime.
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


def render_kpis(summary: pd.DataFrame, artists: list[str]) -> None:
    """Render KPI cards with directional deltas vs roster averages.

    This keeps the demo honest: deltas are always computed from the same
    underlying data the cards display, and work in both demo and production
    modes without any hidden heuristics.
    """

    filtered = filter_by_artists(summary, artists)
    if filtered.empty:
        st.warning("No data found for the selected artists.")
        return

    total_views = int(filtered["total_views"].sum())
    total_videos = int(filtered["total_videos"].sum())
    total_revenue = float(filtered["total_est_revenue_usd"].sum())
    avg_engagement = float(filtered["avg_engagement_rate"].mean())

    # Roster-wide baselines for directional context
    roster_views_per_artist = summary["total_views"].sum() / max(len(summary), 1)
    roster_videos_per_artist = summary["total_videos"].sum() / max(len(summary), 1)
    roster_revenue_per_artist = summary["total_est_revenue_usd"].sum() / max(len(summary), 1)
    roster_avg_engagement = float(summary["avg_engagement_rate"].mean())

    selected_artist_count = max(len(filtered), 1)
    views_per_artist = total_views / selected_artist_count
    videos_per_artist = total_videos / selected_artist_count
    revenue_per_artist = total_revenue / selected_artist_count

    def _pct_delta(current: float, baseline: float) -> str:
        if baseline == 0:
            return "+0.0%"
        change = (current / baseline - 1.0) * 100.0
        return f"{change:+.1f}%"

    views_delta = _pct_delta(views_per_artist, roster_views_per_artist)
    videos_delta = _pct_delta(videos_per_artist, roster_videos_per_artist)
    revenue_delta = _pct_delta(revenue_per_artist, roster_revenue_per_artist)
    engagement_delta = _pct_delta(avg_engagement, roster_avg_engagement)

    c1, c2, c3, c4 = st.columns(4)
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
        "Avg engagement rate",
        format_percent(avg_engagement),
        delta=engagement_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Engagement rate vs roster-wide average",
    )
    c4.metric(
        "Est. revenue (USD)",
        format_currency(total_revenue),
        delta=revenue_delta,
        delta_color="normal",
        delta_arrow="auto",
        help="Per-artist estimated revenue vs roster-wide average",
    )

    # Apply modern, card-like styling to the KPI strip
    style_metric_cards(
        background_color="#F0F2F6",
        border_left_color="#FF4B4B",
        border_radius_px=8,
        box_shadow=True,
    )

    # Celebrate big wins in a fun, notebook-consistent way
    if total_views >= 10_000_000:
        rain(
            emoji="🎉",
            font_size=40,
            falling_speed=5,
            animation_length=2,
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


def _render_cache_controls(artist_summary: pd.DataFrame, normalized_videos: pd.DataFrame) -> None:
    """Show cache status and provide a force-refresh button.

    This is intentionally lightweight so it can be called from any view.
    """

    cache_timestamp_artist = artist_summary.attrs.get("cache_refreshed_at")
    cache_timestamp_videos = normalized_videos.attrs.get("cache_refreshed_at")

    cache_state = "Warm" if len(artist_summary) > 0 and len(normalized_videos) > 0 else "Cold"

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        if cache_timestamp_artist or cache_timestamp_videos:
            ts = cache_timestamp_artist or cache_timestamp_videos
            st.caption(f"Cache last refreshed: {ts}")
        else:
            st.caption("Cache state: cold (no timestamp recorded yet)")
    with col2:
        st.caption(
            f"Cache state: {cache_state} — artists: {len(artist_summary):,} rows, "
            f"videos: {len(normalized_videos):,} rows",
        )
    with col3:
        if st.button("🔄 Force Refresh Data", use_container_width=True):
            # Clear all cached data and reload the app.
            st.cache_data.clear()
            try:
                st.rerun()
            except AttributeError:  # Streamlit < 1.27 fallback
                st.experimental_rerun()


def _render_diagnostics_view(
    mode: str,
    artist_summary: pd.DataFrame,
    normalized_videos: pd.DataFrame,
) -> None:
    """Diagnostics panel for DB, data freshness, cache, and mode info."""

    st.header("Diagnostics")

    freshness_days = _get_data_freshness_days()
    prod_health = None
    if mode == "production":
        prod_health = get_production_health()

    # Database status
    st.subheader("Database status")
    if mode == "demo":
        st.info("Running in Demo Mode – database checks are skipped.")
    else:
        assert prod_health is not None  # for type checkers
        db_ok = bool(prod_health["db_reachable"])
        db_name = str(prod_health["db_name"])
        db_host = _mask_host(str(prod_health["db_host"]))
        checked_at = prod_health["checked_at"]

        status_icon = "✅" if db_ok else "❌"
        st.markdown(f"{status_icon} **Connection:** {'Connected' if db_ok else 'Unreachable'}")
        st.markdown(f"- **Database:** `{db_name}` on `{db_host}`")
        st.markdown(f"- **Last ping:** `{checked_at}`")
        if not db_ok:
            st.error(f"Database unreachable: {prod_health['db_error']}")

    # Data freshness
    st.subheader("Data freshness")
    if "metrics_date" in normalized_videos.columns:
        latest_metrics_value = normalized_videos["metrics_date"].max()
        if pd.isna(latest_metrics_value):
            st.error("Could not determine latest metrics_date from normalized videos.")
        else:
            latest_dt = pd.to_datetime(latest_metrics_value).to_pydatetime()
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - latest_dt).days
            st.markdown(f"- **Latest metrics_date (from app data):** `{latest_dt}`")
            st.markdown(f"- **Age of latest data:** `{age_days} days`")
            st.markdown(f"- **Target freshness window:** `{freshness_days} days`")
    else:
        st.warning("normalized_videos is missing metrics_date column.")

    if prod_health and prod_health.get("latest_metrics_date") is not None:
        st.markdown("---")
        st.markdown("**Warehouse metrics (from MySQL):**")
        st.markdown(f"- Latest metrics_date: `{prod_health['latest_metrics_date']}`")
        st.markdown(f"- Age of latest data: `{prod_health['latest_metrics_age_days']} days`")

    # Cache status
    st.subheader("Cache status")
    cache_timestamp_artist = artist_summary.attrs.get("cache_refreshed_at")
    cache_timestamp_videos = normalized_videos.attrs.get("cache_refreshed_at")
    cache_state = "Warm" if len(artist_summary) > 0 and len(normalized_videos) > 0 else "Cold"

    st.markdown(f"- **Cache state:** {cache_state}")
    st.markdown(f"- **Artist summary rows:** {len(artist_summary):,}")
    st.markdown(f"- **Normalized videos rows:** {len(normalized_videos):,}")
    st.markdown(
        f"- **Last cache refresh (artist summary):** `{cache_timestamp_artist}`",
    )
    st.markdown(
        f"- **Last cache refresh (normalized videos):** `{cache_timestamp_videos}`",
    )

    # Mode + data source
    st.subheader("Mode & data source")
    if mode == "demo":
        st.markdown("- **Mode:** Demo")
        st.markdown(f"- **Data source:** `{DEMO_DATA_PATH}`")
    else:
        st.markdown("- **Mode:** Production (MySQL)")
        st.markdown("- **Data source:** CSVs from ETL in `music_analysis_tables/`")

    st.info(
        "All diagnostics are read-only and safe to run during demos. Use this "
        "panel as a quick health check before important walkthroughs.",
    )


def main() -> None:
    """Entry point for the MusicScope Streamlit dashboard.

    The app supports two data modes:

    - **Demo Mode** (default for new users): loads a small curated cohort from
      ``demo_data/curated_cohort.json`` with no database or API setup.
    - **Production (MySQL)**: uses the local analytics warehouse via DB_* env
      vars / ``.env`` and web.etl_helpers.get_engine().
    """

    st.set_page_config(page_title="MusicScope Streamlit", layout="wide")

    col_title, col_mode = st.columns([4, 1])
    with col_title:
        st.title("MusicScope™ live roster snapshot")

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
        st.success("🔗 **Data Source: Production (MySQL)** — live data from the " "YouTube analytics warehouse.")

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
    default_range = (metrics_dates.min(), metrics_dates.max())
    date_selection = st.sidebar.date_input("Metrics window", value=default_range)
    if isinstance(date_selection, (list, tuple)):
        start_date, end_date = date_selection
    else:
        start_date = end_date = date_selection

    top_n = st.sidebar.slider("Show top videos", min_value=5, max_value=25, value=10, step=1)
    st.sidebar.metric("Latest metrics date", default_range[1].isoformat())
    st.sidebar.metric("Rows loaded", f"{len(normalized_videos):,}")

    summary_filtered = filter_by_artists(artist_summary, selected_artists)
    normalized_filtered = filter_by_date_window(
        filter_by_artists(normalized_videos, selected_artists), (start_date, end_date)
    )
    latest = latest_snapshot(normalized_filtered)
    color_map = build_color_discrete_map(selected_artists, palette)

    # Top-level navigation for different storytelling modes
    selected_view = option_menu(
        menu_title=None,
        options=["Overview", "Artist Deep Dive", "Velocity Analysis", "Portfolio Exports", "Diagnostics"],
        icons=[
            "bar-chart-fill",
            "person-lines-fill",
            "lightning-fill",
            "diagram-2",
            "activity",
        ],
        orientation="horizontal",
    )

    # Layout and content depend slightly on the selected high-level view,
    # but always keep the story action-oriented and insight-first.
    if selected_view == "Overview":
        render_kpis(artist_summary, selected_artists)

        col1, col2 = st.columns(2)
        with col1:
            render_trend_chart(normalized_filtered, color_map)
        with col2:
            render_velocity_scatter(latest, color_map)

        st.markdown("### Content strategy signals")
        render_content_mix(latest)

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
        render_kpis(artist_summary, selected_artists)
        st.markdown("Dive into per-artist performance and content mix to understand " "why certain videos overperform.")
        render_content_mix(latest)

    elif selected_view == "Velocity Analysis":
        st.markdown("### Velocity & momentum")
        render_velocity_scatter(latest, color_map)
        ui.card(
            content=(
                "**So what?** High views/day with strong engagement tells you "
                "where fan energy is peaking *right now* so you can time your "
                "next release, sync, or tour push."
            ),
        )
    elif selected_view == "Portfolio Exports":
        st.markdown("### Notebook exports (momentum, sentiment, performance)")

        cohorts = list_portfolio_cohorts()
        if not cohorts:
            st.warning(
                "No portfolio exports found in exports/portfolio/. Run the portfolio notebooks to generate them."
            )
            st.stop()

        cohort_choice = st.selectbox("Cohort", cohorts, index=0)
        manifest, run_id = _load_portfolio_manifest(cohort_choice)

        if not run_id:
            st.warning("No runs available for this cohort yet. Generate a run to proceed.")
            st.stop()

        st.caption(f"Exports root: {_get_exports_dir().resolve()}")
        st.caption(f"Active run: {run_id} (from latest.json if present)")

        if manifest:
            st.markdown("**Manifest snapshot**")
            st.json(manifest)
        else:
            st.info("Manifest not found; attempting to load tables directly.")

        table_plan = {
            "momentum_insights": "Track breakout timing and warning windows",
            "sentiment_insights": "Understand fan mood and comment volume",
            "performance_insights": "Find hidden gems and efficiency spread",
            "portfolio_highlights": "Curated highlights to explain the why",
        }

        for name, why in table_plan.items():
            st.markdown(f"#### {name} — {why}")
            df_export = load_portfolio_table(name, cohort_choice, run_id)
            if df_export.empty:
                st.info(f"{name} is empty or missing for this run.")
                continue
            st.caption(f"Rows: {len(df_export):,} • Columns: {len(df_export.columns)}")
            st.dataframe(df_export, use_container_width=True, height=420)
    else:  # "Diagnostics"
        _render_diagnostics_view(mode, artist_summary, normalized_videos)
        _render_cache_controls(artist_summary, normalized_videos)


if __name__ == "__main__":
    main()
