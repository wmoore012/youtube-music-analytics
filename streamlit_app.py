from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

from youtubeviz.viz_theme import build_color_discrete_map, get_artist_color_palette

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "music_analysis_tables"


@st.cache_data(show_spinner=False)
def _load_csv(path: Path, parse_dates: Iterable[str] | None = None) -> pd.DataFrame:
    """Load a CSV with basic error handling visible in the UI."""
    if not path.exists():
        st.error(f"Missing data file: {path.name}. Run the ETL pipeline to generate it.")
        st.stop()
    try:
        return pd.read_csv(path, parse_dates=list(parse_dates) if parse_dates else None)
    except Exception as exc:
        st.error(f"Could not read {path.name}: {exc}")
        st.stop()
    return pd.DataFrame()


def load_artist_summary() -> pd.DataFrame:
    return _load_csv(DATA_DIR / "artist_music_summary.csv")


def load_normalized_videos() -> pd.DataFrame:
    return _load_csv(
        DATA_DIR / "normalized_music_videos.csv",
        parse_dates=["published_at", "metrics_date", "fetched_at"],
    )


def format_number(value: float) -> str:
    return f"{value:,.0f}"


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "–"
    return f"{value:.2f}%"


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
    filtered = filter_by_artists(summary, artists)
    if filtered.empty:
        st.warning("No data found for the selected artists.")
        return

    total_views = int(filtered["total_views"].sum())
    total_videos = int(filtered["total_videos"].sum())
    total_revenue = float(filtered["total_est_revenue_usd"].sum())
    avg_engagement = float(filtered["avg_engagement_rate"].mean())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total views", format_number(total_views))
    c2.metric("Videos analyzed", format_number(total_videos))
    c3.metric("Avg engagement rate", format_percent(avg_engagement))
    c4.metric("Est. revenue (USD)", format_currency(total_revenue))


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
    st.plotly_chart(fig, use_container_width=True)


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
    st.plotly_chart(fig, use_container_width=True)


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

    fig = px.bar(
        mix,
        x="video_type",
        y="total_views",
        color="avg_engagement",
        color_continuous_scale="Blues",
        title="Content mix by video type (latest metrics)",
        hover_data={"video_count": True, "avg_engagement": ":.2f"},
    )
    fig.update_layout(xaxis_title="Video type", yaxis_title="Total views", coloraxis_colorbar_title="Avg engagement")
    st.plotly_chart(fig, use_container_width=True)


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
        .loc[:, ["title", "artist_name", "view_count", "views_per_day", "engagement_rate", "like_rate", "comment_rate"]]
    )

    st.dataframe(
        top_videos.rename(
            columns={
                "artist_name": "Artist",
                "title": "Video",
                "view_count": "Views",
                "views_per_day": "Views/Day",
                "engagement_rate": "Engagement %",
                "like_rate": "Like %",
                "comment_rate": "Comment %",
            }
        ),
        column_config={
            "Views": st.column_config.NumberColumn(format="%,.0f"),
            "Views/Day": st.column_config.NumberColumn(format="%,.1f"),
            "Engagement %": st.column_config.NumberColumn(format="%.2f"),
            "Like %": st.column_config.NumberColumn(format="%.2f"),
            "Comment %": st.column_config.NumberColumn(format="%.2f"),
        },
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="MusicScope Streamlit", layout="wide")
    st.title("MusicScope live snapshot")
    st.caption("Streamlit app powered by the ETL outputs in music_analysis_tables/")

    artist_summary = load_artist_summary()
    normalized_videos = load_normalized_videos()
    if artist_summary.empty:
        st.error("Artist summary is empty. Run the ETL to generate fresh aggregates.")
        st.stop()
    if normalized_videos.empty:
        st.error("Normalized video metrics are empty. Run the ETL to refresh inputs.")
        st.stop()
    palette = get_artist_color_palette()

    available_artists = sorted(artist_summary["artist_name"].unique().tolist())

    st.sidebar.header("Filters")
    selected_artists = st.sidebar.multiselect("Artists", available_artists, default=available_artists)
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

    render_kpis(summary_filtered, selected_artists)

    col1, col2 = st.columns(2)
    with col1:
        render_trend_chart(normalized_filtered, color_map)
    with col2:
        render_velocity_scatter(latest, color_map)

    st.markdown("### Content strategy signals")
    render_content_mix(latest)

    st.markdown(f"### Top {top_n} performing videos (latest metrics)")
    render_top_videos(latest, limit=top_n)


if __name__ == "__main__":
    main()
