from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from time import perf_counter
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import bindparam, inspect, select, text

# Import unique comment integration
try:
    from .unique_comment_integration import enforce_real_data_only, ensure_unique_comments
except ImportError:
    # Fallback decorators if unique comment system not available
    def ensure_unique_comments(func_name: str, usage_type: str = "analysis"):
        def decorator(func):
            return func

        return decorator

    def enforce_real_data_only(df: pd.DataFrame, context: str = "unknown") -> pd.DataFrame:
        return df


@dataclass(frozen=True)
class DateRange:
    start: Optional[date] = None
    end: Optional[date] = None


def _get_engine(engine=None):
    if engine is not None:
        return engine
    # Lazily import to avoid hard dependency when not needed
    from web.etl_helpers import get_engine

    return get_engine()  # Use unified .env-driven engine for local YouTube analytics database


def load_youtube_data(
    artists: Iterable[str] | None = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    engine=None,
) -> pd.DataFrame:
    """
    Load YouTube data for analytics and notebooks.

    This is the main data loading function used by notebooks and analytics scripts.

    Returns:
        DataFrame with columns: video_id, title, artist_name, view_count, like_count,
        comment_count, published_at, metrics_date, etc.
    """
    eng = _get_engine(engine)

    # Load data from youtube_metrics joined with youtube_videos
    query = """
    SELECT
        v.video_id,
        v.title,
        v.channel_title as artist_name,
        m.view_count,
        m.like_count,
        m.comment_count,
        v.published_at,
        m.metrics_date,
        m.fetched_at,
        v.duration
    FROM youtube_videos v
    JOIN youtube_metrics m ON v.video_id = m.video_id
    WHERE 1=1
    """

    params = {}

    if artists:
        placeholders = ",".join([f":artist_{i}" for i in range(len(artists))])
        query += f" AND v.channel_title IN ({placeholders})"
        for i, artist in enumerate(artists):
            params[f"artist_{i}"] = artist

    if start:
        query += " AND m.metrics_date >= :start_date"
        params["start_date"] = start

    if end:
        query += " AND m.metrics_date <= :end_date"
        params["end_date"] = end

    query += " ORDER BY m.metrics_date DESC, v.published_at DESC"

    df = pd.read_sql(query, eng, params=params)

    # Convert date columns
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"])
    if "metrics_date" in df.columns:
        df["metrics_date"] = pd.to_datetime(df["metrics_date"])
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"])

    return df


def load_artist_daily_metrics(  # noqa: C901
    artists: Iterable[str] | None = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    engine=None,
    chunksize: Optional[int] = None,
    normalize_aliases: bool = True,
) -> pd.DataFrame:
    """
    Load daily YouTube metrics joined to video metadata and song artist names.

    Returns columns:
    - artist_name, video_title, date, views, likes, comments, video_id, isrc, channel_title, published_at

    Notes:
    - The ETL uses channel URLs from your `.env` (e.g. YT_CHANNEL_1=...) to control ingestion. This function does not
      alter those inputs; the `artists` parameter is a read-time filter only (uses `isrc_recordings.artist_primary` or
      `youtube_videos.channel_title`).
    """
    eng = _get_engine(engine)

    # Detect whether ISRC schema exists (isrc_recordings + video_recording_link)
    try:
        has_isrc_schema = inspect(eng).has_table("isrc_recordings") and inspect(eng).has_table("video_recording_link")
    except Exception:
        has_isrc_schema = False

    conds = []
    params: dict[str, object] = {}
    names: list[str] = []
    if artists:
        names = list(artists)
        if has_isrc_schema:
            conds.append("ir.artist_primary IN :names")
        else:
            # Fallback: filter by channel when ISRC schema is absent
            conds.append("v.channel_title IN :names")
    if start:
        conds.append("m.metrics_date >= :d0")
        params["d0"] = start
    if end:
        conds.append("m.metrics_date <= :d1")
        params["d1"] = end

    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    artist_sel = "COALESCE(ir.artist_primary, v.channel_title)" if has_isrc_schema else "v.channel_title"
    join_isrc = (
        """
        LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
        LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
    """
        if has_isrc_schema
        else ""
    )

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
        {join_isrc}
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
        df["date"] = pd.to_datetime(df["date"])  # keep datetime64 for plotting / slicing
        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"])
    # Normalize aliases if requested to unify names (e.g., artist variations)
    if normalize_aliases and not df.empty:
        alias_map = _build_artist_alias_map(eng)
        if alias_map:

            def _canonical(name: object) -> str:
                raw = str(name)
                if raw in alias_map:
                    return alias_map[raw]
                return alias_map.get(raw.lower(), raw)

            df["artist_name"] = df["artist_name"].map(_canonical)
    return df


def _build_artist_alias_map(eng) -> dict[str, str]:  # noqa: C901
    """Combine alias mapping from DB (artist_aliases → artists) and ENV JSON.

    Returns alias→canonical dict. ENV overrides DB.
    ENV var: ARTIST_ALIASES_JSON (JSON object)
    """
    mapping: dict[str, str] = {}

    # DB mapping if tables exist
    inspector = inspect(eng)
    if inspector.has_table("artist_aliases"):
        # Determine schema shape
        from sqlalchemy import MetaData, Table

        meta = MetaData()
        meta.reflect(bind=eng, only=["artist_aliases"])  # type: ignore[arg-type]
        aliases_tbl: Table = meta.tables["artist_aliases"]  # type: ignore[index]
        cols = set(aliases_tbl.c.keys())
        with eng.connect() as conn:
            if "canonical_name" in cols:
                # Natural-key schema
                res = conn.execute(text("SELECT alias, canonical_name FROM artist_aliases WHERE alias <> ''"))
                for alias, canonical in res:
                    if alias and canonical:
                        mapping[str(alias)] = str(canonical)
            elif inspector.has_table("artists") and "artist_id" in cols:
                # Legacy schema join
                from web.etl_helpers import get_table  # type: ignore

                artists_tbl = get_table("artists")
                stmt = select(aliases_tbl.c.alias, artists_tbl.c.artist_name).select_from(
                    aliases_tbl.join(artists_tbl, aliases_tbl.c.artist_id == artists_tbl.c.artist_id)
                )
                rows = conn.execute(stmt).fetchall()
                for alias, canonical in rows:
                    if alias and canonical:
                        mapping[str(alias)] = str(canonical)

    # Load from config file first
    try:
        from pathlib import Path

        config_path = Path(__file__).parent.parent.parent / "config" / "artist_aliases.json"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
                for k, v in (obj or {}).items():
                    if k and v:
                        mapping[str(k)] = str(v)
    except Exception:
        pass

    # ENV overlay (overrides config file)
    try:
        raw = os.getenv("ARTIST_ALIASES_JSON")
        if raw:
            obj = json.loads(raw)
            for k, v in (obj or {}).items():
                if k and v:
                    mapping[str(k)] = str(v)
    except Exception:
        pass

    # Case-insensitive keys: add lowercase variants for lookups
    lower = {k.lower(): v for k, v in mapping.items()}
    mapping.update(lower)
    return mapping


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
        iqr = q3-q1
        lower = q1-factor * iqr
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


def run_artist_metrics_pipeline(
    *,
    engine=None,
    artists: Iterable[str] | None = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    rpm_usd: float | dict[str, float] | None = None,
) -> dict[str, pd.DataFrame]:
    """Execute an end-to-end pipeline from DB load to revenue summary."""

    daily = load_artist_daily_metrics(artists=artists, start=start, end=end, engine=engine)
    revenue = compute_estimated_revenue(daily, rpm_usd=rpm_usd)
    return {"daily_metrics": daily, "revenue": revenue}


def benchmark_run_artist_metrics_pipeline(
    *, engine=None, iterations: int = 1, **kwargs
) -> dict[str, float | int | pd.DataFrame]:
    """Benchmark the end-to-end pipeline to detect performance regressions."""

    runs = max(1, int(iterations))
    total = 0.0
    last_result: dict[str, pd.DataFrame] | None = None
    for _ in range(runs):
        start_time = perf_counter()
        last_result = run_artist_metrics_pipeline(engine=engine, **kwargs)
        total += perf_counter() - start_time

    rows = len(last_result["daily_metrics"]) if last_result else 0
    avg_duration = total / runs if runs else 0.0
    return {"iterations": runs, "rows": rows, "duration_sec": avg_duration, "last_result": last_result}


@ensure_unique_comments("load_comment_examples", "analysis")
def load_comment_examples(
    artists: Iterable[str] | None = None,
    per_artist: int = 3,
    kind: str = "both",
    engine=None,
) -> pd.DataFrame:
    """Load example comments with sentiment scores per artist.

    kind: 'positive', 'negative', or 'both' (returns up to 2 * per_artist if both)
    Returns: artist_name, video_title, video_id, sentiment_score, comment_text, comment_time
    """
    eng = _get_engine(engine)
    try:
        has_isrc_schema = inspect(eng).has_table("isrc_recordings") and inspect(eng).has_table("video_recording_link")
    except Exception:
        has_isrc_schema = False

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
        if has_isrc_schema:
            conds.append("ir.artist_primary IN :names")
        else:
            conds.append("v.channel_title IN :names")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    artist_sel = "COALESCE(ir.artist_primary, v.channel_title)" if has_isrc_schema else "v.channel_title"
    join_isrc = (
        """
        LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
        LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
    """
        if has_isrc_schema
        else ""
    )

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
        {join_isrc}
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

    # Ensure only real, unique comments are returned
    out = enforce_real_data_only(out, "load_comment_examples")

    return out


def compute_coengagement_matrix(artists: Iterable[str] | None = None, engine=None) -> pd.DataFrame:
    """Compute commenter overlap (Jaccard) across artists.

    Requires youtube_comments.author_channel_id or author_name to be present; returns empty if not.
    Columns: artist_a, artist_b, commenters_a, commenters_b, overlap, jaccard
    """
    eng = _get_engine(engine)

    # Check if author_channel_id column exists, fallback to author_name
    try:
        cols = {c["name"] for c in inspect(eng).get_columns("youtube_comments")}
        if "author_channel_id" in cols:
            author_col = "author_channel_id"
        elif "author_name" in cols:
            author_col = "author_name"
        else:
            # No suitable author column found
            return pd.DataFrame(columns=["artist_a", "artist_b", "commenters_a", "commenters_b", "overlap", "jaccard"])
    except Exception:
        return pd.DataFrame(columns=["artist_a", "artist_b", "commenters_a", "commenters_b", "overlap", "jaccard"])

    try:
        has_isrc_schema = inspect(eng).has_table("isrc_recordings") and inspect(eng).has_table("video_recording_link")
    except Exception:
        has_isrc_schema = False
    names: list[str] = list(artists) if artists else []
    where = ""
    conds = [f"c.{author_col} IS NOT NULL"]
    if artists:
        if has_isrc_schema:
            conds.append("ir.artist_primary IN :names")
        else:
            conds.append("v.channel_title IN :names")
        where = f"WHERE {' AND '.join(conds)}"
    else:
        where = f"WHERE {' AND '.join(conds)}"

    artist_sel = "COALESCE(ir.artist_primary, v.channel_title)" if has_isrc_schema else "v.channel_title"
    join_isrc = (
        """
        LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
        LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
    """
        if has_isrc_schema
        else ""
    )
    sql = f"""
        SELECT {artist_sel} AS artist_name,
               c.{author_col} AS author_identifier
        FROM youtube_comments c
        JOIN youtube_videos v ON v.video_id = c.video_id
        {join_isrc}
        {where}
    """
    stmt = text(sql)
    if names:
        stmt = stmt.bindparams(bindparam("names", expanding=True))
    with eng.connect() as conn:
        _params = {"names": names} if names else {}
        df = pd.read_sql(stmt, conn, params=_params)
    if df.empty or "author_identifier" not in df.columns:
        return df.iloc[0:0]
    # Build sets per artist
    sets = {a: set(s["author_identifier"].dropna().astype(str)) for a, s in df.groupby("artist_name")}
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
    start = maxd-timedelta(days=days)
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

    # Check if we have ISRC schema with data AND videos with recording links
    has_isrc_with_data = False
    try:
        if inspect(eng).has_table("isrc_recordings") and inspect(eng).has_table("video_recording_link"):
            with eng.connect() as conn:
                # Check if we have ISRC data AND videos with recording links
                result = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM isrc_recordings ir
                    JOIN video_recording_link vrl ON ir.isrc = vrl.isrc
                    JOIN youtube_videos v ON vrl.video_id = v.video_id
                """
                    )
                )
                has_isrc_with_data = result.fetchone()[0] > 0
    except Exception:
        has_isrc_with_data = False

    conds = []
    names: list[str] = []
    if artists:
        names = list(artists)
        if has_isrc_with_data:
            # Use both ISRC artist names and channel titles for filtering
            conds.append("(ir.artist_primary IN :names OR v.channel_title IN :names)")
        else:
            # Only use channel titles
            conds.append("v.channel_title IN :names")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    # Smart artist selection: prefer ISRC artist name if available, fallback to channel title
    if has_isrc_with_data:
        artist_sel = "COALESCE(ir.artist_primary, v.channel_title)"
        join_isrc = """
            LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
            LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
        """
    else:
        artist_sel = "v.channel_title"
        join_isrc = ""

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
        {join_isrc}
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
    normalize_aliases: bool = True,
) -> pd.DataFrame:
    """Aggregate daily sentiment from youtube_comments joined to videos (+ISRC if present).

    Returns columns:
    - date, artist_name, avg_sentiment, comments
    """
    eng = _get_engine(engine)

    # Check if we have ISRC schema with data AND videos with recording links
    has_isrc_with_data = False
    try:
        if inspect(eng).has_table("isrc_recordings") and inspect(eng).has_table("video_recording_link"):
            with eng.connect() as conn:
                # Check if we have ISRC data AND videos with recording links
                result = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM isrc_recordings ir
                    JOIN video_recording_link vrl ON ir.isrc = vrl.isrc
                    JOIN youtube_videos v ON vrl.video_id = v.video_id
                """
                    )
                )
                has_isrc_with_data = result.fetchone()[0] > 0
    except Exception:
        has_isrc_with_data = False

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
        if has_isrc_with_data:
            # Use both ISRC artist names and channel titles for filtering
            conds.append("(ir.artist_primary IN :names OR v.channel_title IN :names)")
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

    # Smart artist selection: prefer ISRC artist name if available, fallback to channel title
    if has_isrc_with_data:
        artist_sel = "COALESCE(ir.artist_primary, v.channel_title)"
        join_isrc = """
            LEFT JOIN video_recording_link vrl ON v.video_id = vrl.video_id
            LEFT JOIN isrc_recordings ir ON vrl.isrc = ir.isrc
        """
    else:
        artist_sel = "v.channel_title"
        join_isrc = ""

    sql = f"""
        SELECT
            {date_expr} AS `date`,
            {artist_sel} AS artist_name,
            AVG(cs.sentiment_score) AS avg_sentiment,
            COUNT(*) AS comments
        FROM youtube_comments c
        JOIN youtube_videos v ON v.video_id = c.video_id
        JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
        {join_isrc}
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

    # Normalize aliases if requested to unify names (e.g., artist variations)
    if normalize_aliases and not df.empty:
        alias_map = _build_artist_alias_map(eng)
        if alias_map:
            df["artist_name"] = df["artist_name"].map(lambda n: alias_map.get(str(n), str(n)))

    return df


def qa_artist_consistency_check(days: int = 30, engine=None) -> dict[str, int]:
    """
    Quality assurance check for artist count consistency across all data functions.

    This catches bugs where different functions return different artist counts
    for the same underlying data (like the color mapping bug we found).

    Args:
        days: Number of days to look back for data
        engine: Database engine

    Returns:
        Dictionary with artist counts from each function and consistency status
    """
    from datetime import date, timedelta

    from .charts import get_artist_color_map

    eng = _get_engine(engine)

    try:
        # Load base data
        recent_data = load_recent_window_days(days=days, engine=eng)

        if len(recent_data) == 0:
            return {
                "status": "no_data",
                "message": f"No data found for last {days} days",
                "data_artists": 0,
                "kpi_artists": 0,
                "sentiment_artists": 0,
                "color_artists": 0,
                "revenue_artists": 0,
                "consistent": True,
            }

        # Count artists in base data
        unique_artists = recent_data["artist_name"].dropna().unique()
        data_count = len(unique_artists)

        # Count artists in KPIs
        kpis = compute_kpis(recent_data)
        kpi_count = len(kpis)

        # Count artists in sentiment data
        end_date = date.today()
        start_date = end_date-timedelta(days=days)
        sentiment_data = load_sentiment_daily(start=start_date, end=end_date, engine=eng)
        sentiment_count = sentiment_data["artist_name"].nunique() if len(sentiment_data) > 0 else 0

        # Count artists in color mapping (test correct usage)
        colors = get_artist_color_map(unique_artists)
        color_count = len(colors)

        # Count artists in revenue computation
        revenue = compute_estimated_revenue(recent_data, rpm_usd=1.0)
        revenue_count = len(revenue)

        # Check consistency (sentiment can be 0 if no sentiment data exists for the time period)
        core_counts = [data_count, kpi_count, color_count, revenue_count]
        core_consistent = len(set(core_counts)) == 1

        # Sentiment is consistent if:
        # - It's 0 (no sentiment data for this time period) OR
        # - It matches the core count (sentiment data exists for all artists)
        sentiment_consistent = sentiment_count == 0 or sentiment_count == data_count

        consistent = core_consistent and sentiment_consistent

        # Generate user-friendly explanation
        if consistent:
            if sentiment_count == 0:
                _explanation = f"✅ Consistent: All core functions return {data_count} artists. Sentiment is 0 (no sentiment data for {  # noqa: E501
                                                                                                              days} day period-this is normal if ETL hasn't run recently or comments lack sentiment analysis)."  # noqa: E501  # noqa: E126
            else:
                _explanation = f"✅ Consistent: All functions return {data_count} artists including sentiment data."
        else:
            _explanation = f"❌ Inconsistent: Core functions should all return the same count, but got {  # noqa: F841
                core_counts}. This indicates a bug in the analytics functions."

        # Temporal consistency check-sentiment data should exist for same time period
        temporal_issues = []
        if data_count > 0 and sentiment_count == 0:
            temporal_issues.append(f"No sentiment data found for {days}-day period despite having {data_count} artists")

        # Check sentiment data recency
        if len(sentiment_data) > 0:
            latest_sentiment = sentiment_data["date"].max()
            latest_main_data = recent_data["date"].max()
            date_diff = (latest_main_data-latest_sentiment).days
            if date_diff > 1:  # More than 1 day lag
                temporal_issues.append(f"Sentiment data is {date_diff} days behind main data")

        return {
            "status": "success",
            "data_artists": data_count,
            "kpi_artists": kpi_count,
            "sentiment_artists": sentiment_count,
            "color_artists": color_count,
            "revenue_artists": revenue_count,
            "consistent": consistent,
            "temporal_issues": temporal_issues,
            "message": (
                "All artist counts match"
                if consistent
                else f"Inconsistent counts-Core: {core_counts}, Sentiment: {sentiment_count}"
            ),
            "explanation": f"Core functions: {data_count} artists."
            " Sentiment: {sentiment_count} artists ({'normal-no"
            " sentiment data for this period' if sentiment_count == 0 else 'matches core count'})",
        }

    except Exception as e:
        return {"status": "error", "message": f"Consistency check failed: {str(e)}", "consistent": False}
