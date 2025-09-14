from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import bindparam, inspect, text


@dataclass(frozen=True)
class DateRange:
    start: Optional[date] = None
    end: Optional[date] = None


def _get_engine(engine=None):
    if engine is not None:
        return engine
    # Lazily import to avoid hard dependency when not needed
    from web.etl_helpers import get_engine

    return get_engine()  # Use unified env-driven engine (no PUBLIC/PRIVATE schemas)


def load_artist_daily_metrics(
    artists: Iterable[str] | None = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    engine=None,
    chunksize: Optional[int] = None,
) -> pd.DataFrame:
    """
    Load daily YouTube metrics joined to video metadata and song artist names.

    Returns columns:
    - artist_name, video_title, date, views, likes, comments, video_id, isrc, channel_title, published_at

    Notes:
    - The ETL uses channel URLs from your `.env` (e.g. YT_CHANNEL_1=...) to control ingestion. This function does not
      alter those inputs; the `artists` parameter is a read-time filter only (uses `songs.artist` or
      `youtube_videos.channel_title`).
    """
    eng = _get_engine(engine)

    # Detect whether optional 'songs' table exists once
    try:
        has_songs = inspect(eng).has_table("songs")
    except Exception:
        has_songs = False

    conds = []
    params: dict[str, object] = {}
    names: list[str] = []
    if artists:
        names = list(artists)
        if has_songs:
            conds.append("s.artist IN :names")
        else:
            # Fallback: filter by channel when songs table is absent
            conds.append("v.channel_title IN :names")
    if start:
        conds.append("m.metrics_date >= :d0")
        params["d0"] = start
    if end:
        conds.append("m.metrics_date <= :d1")
        params["d1"] = end

    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    artist_sel = "COALESCE(s.artist, v.channel_title)" if has_songs else "v.channel_title"
    join_songs = "LEFT JOIN songs s ON v.isrc = s.isrc" if has_songs else ""

    sql = f"""
        SELECT
            {artist_sel} AS artist_name,
            v.title AS video_title,
            m.metrics_date AS `date`,
            m.view_count AS views,
            m.like_count AS likes,
            m.comment_count AS comments,
            v.video_id,
            v.isrc,
            v.channel_title,
            v.published_at
        FROM youtube_metrics m
        JOIN youtube_videos v ON v.video_id = m.video_id
        {join_songs}
        {where}
    """

    stmt = text(sql)
    if names:
        stmt = stmt.bindparams(bindparam("names", expanding=True))

    with eng.connect() as conn:
        if chunksize and chunksize > 0:
            parts = []
            _params = {**params, "names": names} if names else params
            for ch in pd.read_sql(stmt, conn, params=_params, chunksize=chunksize):
                parts.append(ch)
            df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        else:
            _params = {**params, "names": names} if names else params
            df = pd.read_sql(stmt, conn, params=_params)

    # Ensure types
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])  # keep datetime64 for plotting/slicing
        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"])
    return df


def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate KPI snapshot per artist using per-video rollup:
    - total_views: sum of per-video max views within the window
    - videos: number of unique videos
    - median_views: median of per-video max views
    """
    if df.empty:
        return df
    per_video = df.groupby(["artist_name", "video_id"]).agg(max_views=("views", "max")).reset_index()
    kpis = (
        per_video.groupby("artist_name")
        .agg(
            total_views=("max_views", "sum"),
            videos=("video_id", "nunique"),
            median_views=("max_views", "median"),
            mean_views=("max_views", "mean"),
        )
        .reset_index()
        .sort_values("total_views", ascending=False)
    )
    # Compute mode per artist (may be multimodal – pick first)
    try:
        mode_series = per_video.groupby("artist_name")["max_views"].agg(
            lambda s: s.mode().iloc[0] if not s.mode().empty else pd.NA
        )
        kpis = kpis.merge(mode_series.rename("mode_views"), on="artist_name", how="left")
    except Exception:
        kpis["mode_views"] = pd.NA
    return kpis


def detect_outliers_iqr(
    df: pd.DataFrame, value_col: str, group_col: str | None = None, factor: float = 1.5
) -> pd.DataFrame:
    """Return rows considered outliers by IQR rule.

    If group_col is provided, detect outliers within each group; otherwise overall.
    """
    if df.empty or value_col not in df.columns:
        return df.iloc[0:0]

    def _outliers(sub: pd.DataFrame) -> pd.DataFrame:
        q1 = sub[value_col].quantile(0.25)
        q3 = sub[value_col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        return sub[(sub[value_col] < lower) | (sub[value_col] > upper)]

    if group_col and group_col in df.columns:
        parts = [_outliers(g) for _, g in df.groupby(group_col)]
        return pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0]
    return _outliers(df)


def read_rpm_from_env() -> tuple[float, dict[str, float]]:
    """Read RPM defaults from env: REVENUE_RPM_DEFAULT and REVENUE_RPM_MAP_JSON."""
    try:
        default = float(os.getenv("REVENUE_RPM_DEFAULT", "3.0"))
    except Exception:
        default = 3.0
    mapping: dict[str, float] = {}
    raw = os.getenv("REVENUE_RPM_MAP_JSON")
    if raw:
        try:
            obj = json.loads(raw)
            mapping = {str(k): float(v) for k, v in obj.items()}
        except Exception:
            mapping = {}
    return default, mapping


def compute_estimated_revenue(
    df: pd.DataFrame,
    rpm_usd: float | dict[str, float] | None = None,
    per_video: bool = True,
) -> pd.DataFrame:
    """Estimate revenue from views using RPM (USD per 1,000 views).

    - If `rpm_usd` is a float, apply globally; if dict, map per artist (fallback to global 3.0).
    - Returns per-artist summary; when `per_video=True`, also aggregates by video first using max views.
    Columns: artist_name, total_views, est_revenue_usd, videos, median_views, mean_views
    """
    if df.empty:
        return df.iloc[0:0]
    # Normalize RPM mapping
    default_rpm, rpm_map = read_rpm_from_env()

    def _rpm_for(artist: str) -> float:
        # Priority: explicit mapping arg > arg scalar > env mapping > env default
        if isinstance(rpm_usd, dict):
            return float(rpm_usd.get(artist, default_rpm))
        if isinstance(rpm_usd, (int, float)):
            return float(rpm_usd)
        return float(rpm_map.get(artist, default_rpm))

    if per_video:
        base = df.groupby(["artist_name", "video_id"]).agg(max_views=("views", "max")).reset_index()
    else:
        base = df.rename(columns={"views": "max_views"})

    base["rpm"] = base["artist_name"].map(lambda a: _rpm_for(a))
    base["est_revenue_usd"] = (base["max_views"].fillna(0) / 1000.0) * base["rpm"].fillna(default_rpm)

    out = (
        base.groupby("artist_name")
        .agg(
            total_views=("max_views", "sum"),
            est_revenue_usd=("est_revenue_usd", "sum"),
            videos=("video_id", "nunique"),
            median_views=("max_views", "median"),
            mean_views=("max_views", "mean"),
        )
        .reset_index()
        .sort_values("est_revenue_usd", ascending=False)
    )
    return out


def compute_yoy_views(df: pd.DataFrame) -> pd.DataFrame:
    """Year-over-year total views per artist from daily metrics df (with 'date', 'views')."""
    if df.empty:
        return df.iloc[0:0]
    data = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data["date"] = pd.to_datetime(data["date"])
    data["year"] = data["date"].dt.year
    yoy = data.groupby(["artist_name", "year"], as_index=False)["views"].sum().rename(columns={"views": "year_views"})
    return yoy


def load_comment_examples(
    artists: Iterable[str] | None = None,
    per_artist: int = 3,
    kind: str = "both",
    engine=None,
) -> pd.DataFrame:
    """Load example comments with sentiment scores per artist.

    kind: 'positive', 'negative', or 'both' (returns up to 2*per_artist if both)
    Returns: artist_name, video_title, video_id, sentiment_score, comment_text, comment_time
    """
    eng = _get_engine(engine)
    try:
        has_songs = inspect(eng).has_table("songs")
    except Exception:
        has_songs = False

    # Determine available timestamp column on youtube_comments
    try:
        cols = {c["name"] for c in inspect(eng).get_columns("youtube_comments")}
    except Exception:
        cols = set()
    time_col = (
        "published_at"
        if "published_at" in cols
        else ("fetch_datetime" if "fetch_datetime" in cols else ("created_at" if "created_at" in cols else None))
    )

    conds = ["c.sentiment_score IS NOT NULL"]
    names: list[str] = []
    if artists:
        names = list(artists)
        if has_songs:
            conds.append("sg.artist IN :names")
        else:
            conds.append("v.channel_title IN :names")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    artist_sel = "COALESCE(sg.artist, v.channel_title)" if has_songs else "v.channel_title"
    join_songs = "LEFT JOIN songs sg ON v.isrc = sg.isrc" if has_songs else ""

    # Use window functions if available; fallback to simple LIMIT per group via variables isn't portable.
    # We'll select all and do per-group head in pandas.
    # Choose a stable alias for time column
    time_sel = f", c.{time_col} AS comment_time" if time_col else ""
    sql = f"""
        SELECT {artist_sel} AS artist_name,
               v.video_id,
               v.title AS video_title,
               c.sentiment_score,
               c.comment_text{time_sel}
        FROM youtube_comments c
        JOIN youtube_videos v ON v.video_id = c.video_id
        {join_songs}
        {where}
    """
    stmt = text(sql)
    if names:
        stmt = stmt.bindparams(bindparam("names", expanding=True))
    with eng.connect() as conn:
        _params = {"names": names} if names else {}
        df = pd.read_sql(stmt, conn, params=_params)
    if df.empty:
        return df
    # Convert time
    if "comment_time" in df.columns:
        df["comment_time"] = pd.to_datetime(df["comment_time"])

    dfs: list[pd.DataFrame] = []
    for artist, sub in df.groupby("artist_name"):
        if kind in ("positive", "both"):
            dfs.append(sub.sort_values("sentiment_score", ascending=False).head(per_artist))
        if kind in ("negative", "both"):
            dfs.append(sub.sort_values("sentiment_score", ascending=True).head(per_artist))
    out = pd.concat(dfs, ignore_index=True) if dfs else df.iloc[0:0]
    return out


def compute_coengagement_matrix(artists: Iterable[str] | None = None, engine=None) -> pd.DataFrame:
    """Compute commenter overlap (Jaccard) across artists.

    Requires youtube_comments.author_channel_id to be present; returns empty if not.
    Columns: artist_a, artist_b, commenters_a, commenters_b, overlap, jaccard
    """
    eng = _get_engine(engine)
    try:
        has_songs = inspect(eng).has_table("songs")
    except Exception:
        has_songs = False
    names: list[str] = list(artists) if artists else []
    where = ""
    conds = ["c.author_channel_id IS NOT NULL"]
    if artists:
        if has_songs:
            conds.append("sg.artist IN :names")
        else:
            conds.append("v.channel_title IN :names")
        where = f"WHERE {' AND '.join(conds)}"
    else:
        where = f"WHERE {' AND '.join(conds)}"

    artist_sel = "COALESCE(sg.artist, v.channel_title)" if has_songs else "v.channel_title"
    join_songs = "LEFT JOIN songs sg ON v.isrc = sg.isrc" if has_songs else ""
    sql = f"""
        SELECT {artist_sel} AS artist_name,
               c.author_channel_id
        FROM youtube_comments c
        JOIN youtube_videos v ON v.video_id = c.video_id
        {join_songs}
        {where}
    """
    stmt = text(sql)
    if names:
        stmt = stmt.bindparams(bindparam("names", expanding=True))
    with eng.connect() as conn:
        _params = {"names": names} if names else {}
        df = pd.read_sql(stmt, conn, params=_params)
    if df.empty or "author_channel_id" not in df.columns:
        return df.iloc[0:0]
    # Build sets per artist
    sets = {a: set(s["author_channel_id"].dropna().astype(str)) for a, s in df.groupby("artist_name")}
    rows: list[dict[str, object]] = []
    arts = sorted(sets.keys())
    for i, a in enumerate(arts):
        for b in arts[i:]:
            A = sets[a]
            B = sets[b]
            inter = len(A & B)
            ja = inter / max(1, len(A | B))
            rows.append(
                {
                    "artist_a": a,
                    "artist_b": b,
                    "commenters_a": len(A),
                    "commenters_b": len(B),
                    "overlap": inter,
                    "jaccard": ja,
                }
            )
    return pd.DataFrame(rows)


def load_recent_window_days(
    artists: Iterable[str] | None = None,
    days: int = 90,
    engine=None,
) -> pd.DataFrame:
    """Convenience: load last N days of metrics for selected artists."""
    eng = _get_engine(engine)
    sql = "SELECT MAX(metrics_date) AS maxd FROM youtube_metrics"
    with eng.connect() as conn:
        maxd = pd.read_sql(text(sql), conn).iloc[0]["maxd"]
    if pd.isna(maxd):
        return pd.DataFrame()
    maxd = pd.to_datetime(maxd).date()
    start = maxd - timedelta(days=days)
    return load_artist_daily_metrics(artists=artists, start=start, end=maxd, engine=eng)


def qa_nulls_and_orphans(engine=None) -> dict[str, pd.DataFrame]:
    """Basic QA: null ISRC in videos, metrics without matching video, videos without metrics."""
    eng = _get_engine(engine)
    with eng.connect() as conn:
        null_isrc = pd.read_sql(
            text("SELECT video_id, title, channel_title FROM youtube_videos WHERE isrc IS NULL"), conn
        )
        metrics_orphans = pd.read_sql(
            text(
                "SELECT m.video_id, m.metrics_date\n"
                "FROM youtube_metrics m\n"
                "LEFT JOIN youtube_videos v ON v.video_id = m.video_id\n"
                "WHERE v.video_id IS NULL"
            ),
            conn,
        )
        videos_no_metrics = pd.read_sql(
            text(
                "SELECT v.video_id, v.title\n"
                "FROM youtube_videos v\n"
                "LEFT JOIN youtube_metrics m ON m.video_id = v.video_id\n"
                "WHERE m.video_id IS NULL"
            ),
            conn,
        )
    return {
        "null_isrc": null_isrc,
        "metrics_orphans": metrics_orphans,
        "videos_no_metrics": videos_no_metrics,
    }


def load_sentiment_summary(
    artists: Iterable[str] | None = None,
    engine=None,
) -> pd.DataFrame:
    """Load sentiment summary per artist with smart ISRC detection.

    Returns columns:
    - artist_name, avg_sentiment, sentiment_std, total_comments, positive_comments, negative_comments
    """
    eng = _get_engine(engine)

    # Check if we have songs table with data AND videos with ISRCs
    has_songs_with_isrcs = False
    try:
        if inspect(eng).has_table("songs"):
            with eng.connect() as conn:
                # Check if we have songs data AND videos with ISRCs
                result = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM songs sg
                    JOIN youtube_videos v ON v.isrc = sg.isrc
                    WHERE v.isrc IS NOT NULL
                """
                    )
                )
                has_songs_with_isrcs = result.fetchone()[0] > 0
    except Exception:
        has_songs_with_isrcs = False

    conds = []
    names: list[str] = []
    if artists:
        names = list(artists)
        if has_songs_with_isrcs:
            # Use both song artist names and channel titles for filtering
            conds.append("(sg.artist IN :names OR v.channel_title IN :names)")
        else:
            # Only use channel titles
            conds.append("v.channel_title IN :names")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    # Smart artist selection: prefer song artist name if available, fallback to channel title
    if has_songs_with_isrcs:
        artist_sel = "COALESCE(sg.artist, v.channel_title)"
        join_songs = "LEFT JOIN songs sg ON v.isrc = sg.isrc AND v.isrc IS NOT NULL"
    else:
        artist_sel = "v.channel_title"
        join_songs = ""

    sql = f"""
        SELECT
            {artist_sel} AS artist_name,
            AVG(cs.sentiment_score) AS avg_sentiment,
            STDDEV(cs.sentiment_score) AS sentiment_std,
            COUNT(*) AS total_comments,
            SUM(CASE WHEN cs.sentiment_score > 0.1 THEN 1 ELSE 0 END) AS positive_comments,
            SUM(CASE WHEN cs.sentiment_score < -0.1 THEN 1 ELSE 0 END) AS negative_comments
        FROM comment_sentiment cs
        JOIN youtube_videos v ON v.video_id = cs.video_id
        {join_songs}
        {where}
        GROUP BY {artist_sel}
    """
    stmt = text(sql)
    if names:
        stmt = stmt.bindparams(bindparam("names", expanding=True))
    with eng.connect() as conn:
        _params = {"names": names} if names else {}
        df = pd.read_sql(stmt, conn, params=_params)
    if not df.empty and "last_updated" in df.columns:
        df["last_updated"] = pd.to_datetime(df["last_updated"])
    return df


def load_sentiment_daily(
    artists: Iterable[str] | None = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    engine=None,
) -> pd.DataFrame:
    """Aggregate daily sentiment from youtube_comments joined to videos (+songs if ISRC present).

    Returns columns:
    - date, artist_name, avg_sentiment, comments
    """
    eng = _get_engine(engine)

    # Check if we have songs table with data AND videos with ISRCs
    has_songs_with_isrcs = False
    try:
        if inspect(eng).has_table("songs"):
            with eng.connect() as conn:
                # Check if we have songs data AND videos with ISRCs
                result = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM songs sg
                    JOIN youtube_videos v ON v.isrc = sg.isrc
                    WHERE v.isrc IS NOT NULL
                """
                    )
                )
                has_songs_with_isrcs = result.fetchone()[0] > 0
    except Exception:
        has_songs_with_isrcs = False

    conds = []
    params: dict[str, object] = {}
    names: list[str] = []
    # Determine available timestamp column on youtube_comments for daily rollup
    try:
        c_cols = {c["name"] for c in inspect(eng).get_columns("youtube_comments")}
    except Exception:
        c_cols = set()
    ts_col = "published_at" if "published_at" in c_cols else ("created_at" if "created_at" in c_cols else None)
    date_expr = f"DATE(c.{ts_col})" if ts_col else "DATE(NOW())"  # fallback shouldn't be hit if schema is sane
    if artists:
        names = list(artists)
        if has_songs_with_isrcs:
            # Use both song artist names and channel titles for filtering
            conds.append("(sg.artist IN :names OR v.channel_title IN :names)")
        else:
            # Only use channel titles
            conds.append("v.channel_title IN :names")

    if start:
        conds.append(f"{date_expr} >= :d0")
        params["d0"] = start
    if end:
        conds.append(f"{date_expr} <= :d1")
        params["d1"] = end
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    # Smart artist selection: prefer song artist name if available, fallback to channel title
    if has_songs_with_isrcs:
        artist_sel = "COALESCE(sg.artist, v.channel_title)"
        join_songs = "LEFT JOIN songs sg ON v.isrc = sg.isrc AND v.isrc IS NOT NULL"
    else:
        artist_sel = "v.channel_title"
        join_songs = ""

    sql = f"""
        SELECT
            {date_expr} AS `date`,
            {artist_sel} AS artist_name,
            AVG(cs.sentiment_score) AS avg_sentiment,
            COUNT(*) AS comments
        FROM youtube_comments c
        JOIN youtube_videos v ON v.video_id = c.video_id
        JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
        {join_songs}
        {where}
        GROUP BY {date_expr}, {artist_sel}
        ORDER BY `date` ASC
    """
    stmt = text(sql)
    if names:
        stmt = stmt.bindparams(bindparam("names", expanding=True))
    with eng.connect() as conn:
        _params = {**params, "names": names} if names else params
        df = pd.read_sql(stmt, conn, params=_params)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df
