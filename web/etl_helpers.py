# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

"""
ETL helper utilities that keep notebooks clean.

Usage
-----
from web.etl_helpers import (
    get_engine, reflect_global_tables, get_or_create,
    normalize_spotify_track, normalize_youtube_video
)
"""

__doctest__ = False  # 🚫 Skip doc‑string doctests when PyCharm / pytest runs the module
import contextlib
import json
import os
import re
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv  # NEW: enables load_dotenv()
from sqlalchemy import MetaData  # fixes NameError in init_tables()
from sqlalchemy import Table  # used for type hints all over the file
from sqlalchemy import create_engine  # used in get_engine()
from sqlalchemy import inspect  # used in bulk_upsert()
from sqlalchemy import select  # already imported later, but safe to add here
from sqlalchemy import text  # used in several on_duplicate_key_update() calls
from sqlalchemy import insert
from sqlalchemy.engine import Connection  # used for type hints
from sqlalchemy.engine import Engine

# --------------------------------------------------------------------------- #
# … other imports / code above stay unchanged …
# --------------------------------------------------------------------------- #


def read_sql_safe(sql: str, engine: Engine, **kw):
    """
    Execute *sql* and return the result as a pandas DataFrame.

    Strategy
    --------
    1. SQLite – pandas still requires a *raw* DB‑API connection.
    2. Other dialects – try the efficient SQLAlchemy Connection first.
       If pandas complains that the object has no ``cursor`` attribute
       (this happens with SQLAlchemy 2.x), transparently fall back to
       the raw DB‑API connection which *does* expose ``cursor()``.

    This keeps the call site identical and guarantees compatibility
    across SQLAlchemy / pandas versions and database back‑ends.
    """
    # --- (1) SQLite always needs the raw DB‑API connection ---------------
    if engine.dialect.name == "sqlite":
        with contextlib.closing(engine.raw_connection()) as raw:
            return pd.read_sql_query(sql, con=raw, **kw)

    # --- (2) Other dialects – optimistic path ----------------------------
    try:
        with engine.connect() as conn:
            return pd.read_sql_query(sql, con=conn, **kw)
    except AttributeError as exc:
        # pandas (< 2.2) tries to call .cursor() on SQLAlchemy‑2.0 Connection
        # objects. If that is the failure, fall back to the raw DB‑API
        # connection which *does* expose .cursor(); otherwise re‑raise.
        if "cursor" not in str(exc):
            raise

    # --- Fallback: use DB‑API connection that *does* have .cursor() ------
    with contextlib.closing(engine.raw_connection()) as raw:
        return pd.read_sql_query(sql, con=raw, **kw)


# ──────────────────────────────────────────────────────────────────
# 0.  ENV + ENGINE
# ──────────────────────────────────────────────────────────────────
# Always load the canonical .env from repo root without overwriting existing env
_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=_REPO_ROOT / ".env", override=False)


def get_engine(*, echo: bool = False, schema: str = "PUBLIC") -> Engine:
    """
    Build a SQLAlchemy engine using DB_* env-vars.

    Args:
        echo: Whether to echo SQL statements
        schema: Which schema to connect to - "PUBLIC" or "PRIVATE"

    Raises:
        ValueError: If schema is not "PUBLIC" or "PRIVATE"

    Works locally, with Cloud-SQL proxy, or with a public-IP instance.
    """
    # Validate schema parameter
    valid_schemas = {"PUBLIC", "PRIVATE"}
    if schema.upper() not in valid_schemas:
        raise ValueError(f"Invalid schema '{schema}'. Must be one of: {valid_schemas}")

    # Determine which database to use
    if schema.upper() == "PRIVATE":
        db_name = os.getenv("DB_NAME_PRIVATE", os.getenv("DB_NAME", "icatalog"))
    else:  # PUBLIC (default)
        db_name = os.getenv("DB_NAME_PUBLIC", "icatalog_public")

    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{os.getenv('DB_HOST', '127.0.0.1')}:{os.getenv('DB_PORT', '3306')}/"
        f"{db_name}?charset=utf8mb4"
    )
    return create_engine(url, echo=echo, pool_pre_ping=True)


@contextmanager
def get_connection(which: str = "PUBLIC"):
    """
    Context manager to get a database connection to either PUBLIC or PRIVATE schema.

    Args:
        which: Either "PUBLIC" (icatalog_public) or "PRIVATE" (icatalog)

    Usage:
        # Read from private database
        with get_connection("PRIVATE") as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM songs LIMIT 5")
            results = cursor.fetchall()

        # Write to public database
        with get_connection("PUBLIC") as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO songs (...) VALUES (...)")
            conn.commit()
    """
    import mysql.connector

    # Determine which database to use
    if which.upper() == "PRIVATE":
        db_name = os.getenv("DB_NAME_PRIVATE", os.getenv("DB_NAME", "icatalog"))
    else:  # PUBLIC (default)
        db_name = os.getenv("DB_NAME_PUBLIC", "icatalog_public")

    conn = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 3306)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        database=db_name,
    )
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────
# 1A.  GLOBAL TABLE REFLECTION + CONVENIENCE HANDLES
# ──────────────────────────────────────────────────────────────────
from typing import Any

# ── 1️⃣  Make sure every table you need is listed ────────────────
ALL_TABLE_NAMES = [
    "album_sentiment_summary",
    "album_type_l10n",
    "albums",
    "artist_aliases",
    "artists",
    "dsp_providers",
    "labels",
    "role_types",
    "song_artist_roles",
    "song_versions",
    "songs",
    "spotify_tracks",
    "tidal_tracks_raw",
    "twitter_cost_tracking",
    "twitter_tweets",
    "youtube_videos",
    "youtube_videos_raw",
]

_GLOBAL_META: MetaData | None = None  # populated by init_tables()
_TABLE_HANDLES: dict[str, Any] = {}
_TABLES_INITIALIZED: bool = False


def init_tables(engine: Engine) -> None:
    """
    Reflect ALL core tables once and expose them as module-level variables.
    """
    global _GLOBAL_META, _TABLE_HANDLES, _TABLES_INITIALIZED

    if _TABLES_INITIALIZED:
        return

    # NEW – fail fast if tables aren’t there
    _assert_tables_exist(engine, ALL_TABLE_NAMES)

    _GLOBAL_META = MetaData()
    _GLOBAL_META.reflect(bind=engine, only=ALL_TABLE_NAMES)

    for name, tbl_obj in _GLOBAL_META.tables.items():
        _TABLE_HANDLES[name] = tbl_obj
        globals()[f"{name}_tbl"] = tbl_obj

    _TABLES_INITIALIZED = True


# convenience re‑exports for type hints / autocompletion — will be set by init_tables()
songs_tbl: Table
dsp_tbl: Table
artists_tbl: Table
# (all others will appear as <name>_tbl after `init_tables` is called)


def get_table(name: str):
    """
    Return a reflected sqlalchemy.Table by its lowercase name.

    Example
    -------
    >>> songs = get_table("songs")  # doctest: +SKIP
    >>> conn.execute(songs.select().limit(5))  # doctest: +SKIP

    NOTE: init_tables(engine) must be called once at program start.
    """
    try:
        return _TABLE_HANDLES[name]
    except AttributeError as exc:
        # pandas (< 2.2) tries to call .cursor() on the object when it is
        # not an Engine. SQLAlchemy 2.x Connection objects deliberately
        # hide that attribute, so we retry with the underlying raw
        # connection only when the AttributeError is about 'cursor'.
        if "cursor" not in str(exc):
            raise

    # --- Fallback: use DB-API connection that *does* have .cursor() ------
    with contextlib.closing(engine.raw_connection()) as raw:
        return pd.read_sql_query(sql, con=raw, **kw)
        raise KeyError(f"Unknown table '{name}'. Check ALL_TABLE_NAMES.") from exc


# ──────────────────────────────────────────────────────────────────
# 2.  GENERIC UPSERT UTILS
# ──────────────────────────────────────────────────────────────────
def get_or_create(conn: Connection, table: Table, **kwargs) -> int:
    """
    Return the PK for the row matching **kwargs.
    Insert if not present and return new PK.

    Assumes single-column integer PK and SQLAlchemy table object.
    """
    row = conn.execute(select(table).filter_by(**kwargs)).first()
    if row:
        return row[0]  # PK is first column
    res = conn.execute(insert(table).values(**kwargs))
    return res.inserted_primary_key[0]


# ──────────────────────────────────────────────────────────────────
# 3.  DOMAIN-SPECIFIC NORMALISATION
# ──────────────────────────────────────────────────────────────────

# ---------------------------------------------------------------------------
# Module-level in-memory caches
# ---------------------------------------------------------------------------
# Having these objects *defined once* on import guarantees they always exist.
label_cache: dict[str, int] = {}
album_cache: dict[tuple[str, Optional[int], Optional[int]], int] = {}
artist_cache: dict[str, int] = {}

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def clear_caches() -> None:
    """Utility for tests – empties all module-level caches."""
    label_cache.clear()
    album_cache.clear()
    artist_cache.clear()


def _split_title(title: str):
    """
    `"Song (Acoustic)"` → ("Song", "Acoustic")
    `"Song"`            → ("Song", "Original")
    """
    m = re.match(r"^(.*?)\s*\(([^)]+)\)", title)
    return (m.group(1).strip(), m.group(2).strip()) if m else (title, "Original")


def detect_version_type(track_title: str, album_title: str = None) -> str:
    """
    Detect the version type based on the track title and album title.

    According to the requirements, tracks are considered "original" versions unless
    their title or album title contains specific keywords like 'Acoustic',
    'Chopped and Screwed', 'Live', 'Original', 'Radio Edit', or 'Remix'.

    Args:
        track_title (str): The title of the track
        album_title (str, optional): The title of the album

    Returns:
        str: The detected version type
    """
    # Define the version types to look for
    version_keywords = {
        "Acoustic": r"acoustic",
        "Chopped and Screwed": r"chopped|screwed|slowed|reverb",
        "Live": r"\blive\b",
        "Original": r"original",
        "Radio Edit": r"radio\s*edit|clean",
        "Remix": r"remix",
    }

    # First, try to extract version from parentheses in the track title
    m = re.match(r"^(.*?)\s*\(([^)]+)\)", track_title)
    if m:
        version_text = m.group(2).strip()
        # Check if the extracted version matches any of our known version types
        for version_type, pattern in version_keywords.items():
            if re.search(pattern, version_text, re.IGNORECASE):
                return version_type

    # If no version found in parentheses, check the full track title
    for version_type, pattern in version_keywords.items():
        if re.search(pattern, track_title, re.IGNORECASE):
            return version_type

    # If still no version found, check the album title if provided
    if album_title:
        for version_type, pattern in version_keywords.items():
            if re.search(pattern, album_title, re.IGNORECASE):
                return version_type

    # Default to "Original" if no version type is detected
    return "Original"


def upsert_song_version(
    conn: Connection,
    isrc: str,
    dsp_name: str,
    dsp_record_id: str,
    track_title: str,
    album_title: str = None,
) -> None:
    """
    Upsert a record into the song_versions table.
    Checks if the ISRC exists in the songs table before inserting.

    Args:
        conn (Connection): The database connection
        isrc (str): The ISRC of the song
        dsp_name (str): The name of the DSP (e.g., "Spotify", "Tidal")
        dsp_record_id (str): The DSP's record ID for the track
        track_title (str): The title of the track
        album_title (str, optional): The title of the album
    """
    # Check if the isrc exists in the songs table
    songs_tbl = get_table("songs")
    exists = conn.execute(select(songs_tbl.c.isrc).where(songs_tbl.c.isrc == isrc)).scalar_one_or_none()

    if not exists:
        print(f"⚠️ Skipping song version for isrc {isrc} - not found in songs table")
        return

    # Detect the version type
    version_type = detect_version_type(track_title, album_title)

    # Get the song_versions table
    song_versions_tbl = get_table("song_versions")

    # Upsert the record
    stmt = mysql_insert(song_versions_tbl).values(
        isrc=isrc,
        dsp_name=dsp_name,
        dsp_record_id=dsp_record_id,
        version_type=version_type,
    )

    # On duplicate key, update the version_type
    stmt = stmt.on_duplicate_key_update(version_type=text("VALUES(version_type)"))

    # Execute the statement
    conn.execute(stmt)


# Global buffer for song versions to be batch upserted
_song_versions_buffer = []


def batch_upsert_song_version(
    isrc: str,
    dsp_name: str,
    dsp_record_id: str,
    track_title: str,
    album_title: str = None,
) -> None:
    """
    Add a song version record to the buffer for batch upserting.

    Args:
        isrc (str): The ISRC of the song
        dsp_name (str): The name of the DSP (e.g., "Spotify", "Tidal")
        dsp_record_id (str): The DSP's record ID for the track
        track_title (str): The title of the track
        album_title (str, optional): The title of the album
    """
    global _song_versions_buffer

    # Detect the version type
    version_type = detect_version_type(track_title, album_title)

    # Add to buffer
    _song_versions_buffer.append(
        {
            "isrc": isrc,
            "dsp_name": dsp_name,
            "dsp_record_id": dsp_record_id,
            "version_type": version_type,
        }
    )


def flush_song_versions_buffer(engine: Engine) -> int:
    """
    Flush the song versions buffer by performing a bulk upsert.
    Ensures that ISRCs exist in the songs table before inserting into song_versions.

    Args:
        engine (Engine): The SQLAlchemy engine

    Returns:
        int: Number of records upserted
    """
    global _song_versions_buffer

    if not _song_versions_buffer:
        return 0

    # Get the song_versions and songs tables
    song_versions_tbl = get_table("song_versions")
    songs_tbl = get_table("songs")

    # Filter out records with isrcs that don't exist in the songs table
    valid_records = []
    skipped_count = 0

    with engine.connect() as conn:
        for record in _song_versions_buffer:
            isrc = record.get("isrc")
            if not isrc:
                print(f"⚠️ Skipping song version record with missing isrc")
                skipped_count += 1
                continue

            # Check if the isrc exists in the songs table
            exists = conn.execute(select(songs_tbl.c.isrc).where(songs_tbl.c.isrc == isrc)).scalar_one_or_none()

            if exists:
                valid_records.append(record)
            else:
                print(f"⚠️ Skipping song version for isrc {isrc} - not found in songs table")
                skipped_count += 1

    if skipped_count > 0:
        print(f"⚠️ Skipped {skipped_count} song version records due to missing isrcs in songs table")

    if not valid_records:
        _song_versions_buffer = []
        return 0

    # Perform bulk upsert with valid records only
    count = bulk_upsert(engine, song_versions_tbl, valid_records)

    # Clear the buffer
    _song_versions_buffer = []

    return count


# Helper function to get disc number from Tidal track
def get_tidal_disc_number(track: object, conn: Connection = None) -> int | None:
    """
    Safely pull the disc/volume number from a Tidal track object
    regardless of which SDK field names are present.

    If disc number is not found in the track object and a connection is provided,
    attempts to look up the disc number from the songs table using the track's ISRC.

    If no disc number is found in either place, returns None (caller should default to 1).

    Args:
        track: Tidal track object
        conn: Optional database connection to look up disc number if not found in track

    Returns:
        int: Disc number if found, None otherwise
    """
    # First try to get disc number from track object
    disc_num = None
    for attr in (
        "disc_num",  # tidalapi
        "discNumber",  # just in case
        "volumeNumber",  # raw JSON / Go SDKs
        "volume_number",
    ):  # defensive
        if hasattr(track, attr):
            disc_num = getattr(track, attr)
            if disc_num is not None:
                return disc_num

    # Fallback for raw-dict tracks
    if isinstance(track, dict):
        disc_num = track.get("volumeNumber") or track.get("discNumber")
        if disc_num is not None:
            return disc_num

    # If we have a connection, try to look up disc number from songs table
    if conn is not None:
        # Get the ISRC from the track
        isrc = None
        if hasattr(track, "isrc"):
            isrc = getattr(track, "isrc")
        elif isinstance(track, dict):
            isrc = track.get("isrc")

        if isrc:
            # Look up disc number from stream_metrics table
            stream_tbl = get_table("stream_metrics")
            disc_num = conn.execute(
                select(stream_tbl.c.disc_number)
                .where(stream_tbl.c.ISRC == isrc, stream_tbl.c.dsp_name != "Tidal")
                .order_by(stream_tbl.c.fetch_datetime.desc())
                .limit(1)
            ).scalar_one_or_none()

            if disc_num is not None:
                return disc_num

    # Default to None (caller should default to 1)
    return None


from typing import Dict

# ⬇️  Just drop this whole function in place of the old one
from sqlalchemy import Table, select
from sqlalchemy.engine import Connection


def normalize_spotify_track(
    track: dict,
    conn: Connection,
    dsp_id: int = None,
    songs_tbl: Table = None,
) -> tuple[dict, tuple, dict, str]:
    """
    Normalize a Spotify track:
      1) Lookup SongID by track ISRC
      2) Clean label from album data
      3) Upsert label & album
      4) Build song_update dict + si_tuple + stream_rec + raw_meta_json

    Similar to normalize_tidal but adapted for Spotify data structure.
    """
    # Extract ISRC from track
    isrc = track.get("external_ids", {}).get("isrc")
    if not isrc:
        raise ValueError("Missing ISRC → skipping this track")

    # 1) SongID lookup
    if songs_tbl is None:
        songs_tbl = get_table("songs")
    # Check if the song exists in the database
    exists = conn.execute(select(songs_tbl.c.ISRC).where(songs_tbl.c.ISRC == isrc)).scalar_one_or_none()
    if exists is None:
        raise ValueError(f"No Songs entry for ISRC={isrc!r}")

    # Use ISRC as the song identifier
    song_id = isrc

    # 2) Get DSP ID if not provided
    if dsp_id is None:
        dsp_providers_tbl = get_table("dsp_providers")
        dsp_id = conn.execute(
            select(dsp_providers_tbl.c.DSPName).where(dsp_providers_tbl.c.DSPName == "Spotify").limit(1)
        ).scalar_one_or_none()
        if dsp_id is None:
            # Ensure the DSP exists
            ensure_dsp_rows(conn.engine, ["Spotify"])
            # Try again
            dsp_id = conn.execute(
                select(dsp_providers_tbl.c.DSPName).where(dsp_providers_tbl.c.DSPName == "Spotify").limit(1)
            ).scalar_one_or_none()
            if dsp_id is None:
                raise ValueError("Could not find or create DSP 'Spotify'")

    # 3) Extract metadata
    title = track.get("name")

    # 4) Album info and artwork
    album = track.get("album") or {}
    album_name = album.get("name")
    album_release_date = album.get("release_date")
    release_year = None
    if album_release_date:
        # Extract year from release date (could be YYYY, YYYY-MM, or YYYY-MM-DD)
        release_year_match = re.match(r"^(\d{4})", album_release_date)
        if release_year_match:
            release_year = int(release_year_match.group(1))

    # Get album artwork
    art_url = None
    if album.get("images") and len(album["images"]) > 0:
        # Find the closest image to 640x640
        target_size = 640
        closest_image = min(
            album["images"],
            key=lambda img: abs(img.get("width", 0) - target_size) + abs(img.get("height", 0) - target_size),
        )
        art_url = closest_image.get("url")

    # 5) Clean label from album data
    raw_label = album.get("label")
    label_id = get_or_create_label(conn, _strip_year_and_suffix(raw_label)) if raw_label else None

    # 6) Create or get album
    album_id = get_or_create_album(conn, album_name, label_id, release_year) if album_name else None

    # 7) Build song_update
    song_update = {
        "song_id": song_id,
        "isrc": isrc,
        "song_title": title,
        "album_title": album_name,
        "artwork_url": art_url,
        "label_id": label_id,
        "album_id": album_id,
    }

    # 8) Identifiers tuple
    dsp_record_id = track.get("id")
    si_tuple = (song_id, dsp_id, dsp_record_id)

    # 9) Raw metadata JSON
    raw_meta = {k: make_json_safe(v) for k, v in track.items()}
    raw_meta_json = json.dumps(raw_meta)

    # 10) Stream metrics
    stream_rec = {
        "isrc": isrc,
        "dsp_name": "Spotify" if dsp_id == "Spotify" else dsp_id,
        "fetch_datetime": datetime.now(timezone.utc),
        "popularity": track.get("popularity"),
        "track_number": track.get("track_number"),
        "disc_number": track.get("disc_number"),
    }

    # 11) Add to song version batch
    batch_upsert_song_version(isrc, "Spotify", dsp_record_id, title, album_name)

    # For backward compatibility with existing code
    # Check if called with the old signature
    import inspect

    frame = inspect.currentframe().f_back
    if frame:
        # Check for the old signature with 'tables' parameter
        if len(frame.f_locals) >= 3 and "tables" in frame.f_locals:
            # If called with old signature, return old format
            old_format = {
                "SongID": song_id,
                "DSPID": dsp_id,
                "VersionTag": "Original",  # Default version
                "TrackTitle": title,
                "AlbumName": album_name,
                "ReleaseDate": album_release_date,
                "RecordLabel": raw_label,
                "Popularity": track.get("popularity"),
                "DurationMS": track.get("duration_ms"),
            }
            return old_format

        # Check for the etl_pipeline.ipynb usage pattern
        # In this case, we're called with just (track, conn)
        if len(frame.f_locals) == 2 and "conn" in frame.f_locals and "t" in frame.f_locals:
            # Return a format compatible with the existing pipeline
            pipeline_format = {
                "SongID": song_id,
                "DSPID": dsp_id,
                "DSPRecordID": dsp_record_id,
                "Title": title,
                "AlbumName": album_name,
                "ReleaseDate": album_release_date,
                "Label": raw_label,
                "Popularity": track.get("popularity"),
                "DurationMS": track.get("duration_ms"),
            }
            return pipeline_format

    return song_update, si_tuple, stream_rec, raw_meta_json


def normalize_youtube_video(
    video: dict,
    conn: Connection,
    tables: Dict[str, Table],
) -> dict:
    """
    Convert one YouTube playlistItem JSON → YoutubeMetrics row.
    Simplified — adapt to your schema.
    """
    youtube_metrics_tbl = tables["YoutubeMetrics"]
    dsp_tbl = tables["DSPProviders"]
    songs_tbl = tables["Songs"]

    video_id = video["contentDetails"]["videoId"]
    snippet = video["snippet"]
    title, version = _split_title(snippet["title"])

    # DSP FK
    dsp_id = conn.execute(select(dsp_tbl.c.DSPID).where(dsp_tbl.c.DSPName == "YouTube").limit(1)).scalar_one_or_none()
    if dsp_id is None:
        # Ensure the DSP exists
        ensure_dsp_rows(conn.engine, ["YouTube"])
        # Try again
        dsp_id = conn.execute(
            select(dsp_tbl.c.DSPID).where(dsp_tbl.c.DSPName == "YouTube").limit(1)
        ).scalar_one_or_none()
        if dsp_id is None:
            raise ValueError("Could not find or create DSP 'YouTube'")

    # Song FK — assume you mapped ISRC elsewhere; for demo fall back to title match
    song_id = conn.execute(select(songs_tbl.c.SongID).where(songs_tbl.c.Title == title)).scalar_one_or_none()
    if song_id is None:
        raise ValueError(f"No song match for YouTube video '{title}'")

    return {
        "SongID": song_id,
        "DSPID": dsp_id,
        "VideoID": video_id,
        "VersionTag": version,
        "PublishedAt": snippet.get("publishedAt"),
        "ChannelTitle": snippet.get("channelTitle"),
    }


# Human-friendly alias for the title splitter
parse_title = _split_title

# Notebook-friendly alias for the richer Spotify normaliser
normalize_spotify = normalize_spotify_track

# Anything you expose here should also be surfaced via __all__
try:
    __all__.extend(["parse_title", "normalize_spotify", "upsert_artist", "get_or_create_artist"])
except NameError:  # __all__ not yet defined → create it
    __all__ = [
        "parse_title",
        "normalize_spotify",
        "upsert_artist",
        "get_or_create_artist",
    ]


# ──────────────────────────────────────────────────────────────────
# 4.  TIDAL HELPERS
# ──────────────────────────────────────────────────────────────────
def clean_label_name(name: str) -> str:
    """
    Clean a label name by removing common suffixes and standardizing format.
    """
    if not name:
        return name

    # Remove common corporate suffixes
    name = re.sub(r",?\s*(Inc\.?|LLC|Ltd\.?)$", "", name, flags=re.IGNORECASE).strip()

    # Remove leading/trailing whitespace and standardize case
    return name.strip()


def _clean_copyright(cp_raw: str) -> str:
    """Strip leading ℗/year and trailing corporate suffixes."""
    current_year = datetime.now().year
    max_year = current_year + 4
    m = re.match(r"^[\s\(\)℗]*(?P<year>[12]\d{3})\s+(?P<rest>.*)", cp_raw.strip(), re.IGNORECASE)
    if m and 1900 <= int(m.group("year")) <= max_year:
        core = m.group("rest").strip()
    else:
        core = re.sub(r"^[\s\(\)℗]*", "", cp_raw.strip())
    return re.sub(r",?\s*(Inc\.?|LLC|Ltd\.?)$", "", core, flags=re.IGNORECASE).strip()


def _strip_year_and_suffix(text: str) -> str:
    if not text:
        return text
    m = re.match(r"^[\s\(\)℗]*(?P<year>[12]\d{3})\s+(?P<name>.*)", text.strip(), re.IGNORECASE)
    if m and 1900 <= int(m.group("year")) <= datetime.now().year + 4:
        core = m.group("name").strip()
    else:
        core = text.strip()
    return re.sub(r",?\s*(Inc\.?|LLC|Ltd\.?)$", "", core, flags=re.IGNORECASE).strip()


def get_or_create_label(conn: Connection, label_name: str) -> Optional[int]:
    """
    Upsert a label and return its ID. Uses caching for performance.
    Returns None if label_name is None or empty.
    """
    if not label_name:
        return None

    # Check cache first
    clean_name = clean_label_name(label_name)
    if clean_name in label_cache:
        return label_cache[clean_name]

    # Get the labels table
    labels_tbl = get_table("labels")

    # Try to find existing label
    # Use first() instead of scalar_one_or_none() to handle potential duplicates
    stmt = select(labels_tbl.c.label_id).where(labels_tbl.c.label_name == clean_name)
    result = conn.execute(stmt).first()
    label_id = result[0] if result else None

    if label_id is None:
        # Insert new label
        stmt = insert(labels_tbl).values(label_name=clean_name)
        result = conn.execute(stmt)
        label_id = result.inserted_primary_key[0]

    # Cache the result
    label_cache[clean_name] = label_id
    return label_id


def get_or_create_album(
    conn: Connection,
    album_title: str,
    label_id: Optional[int] = None,
    release_year: Optional[int] = None,
) -> Optional[int]:
    """
    Upsert an album and return its ID. Uses caching for performance.
    Returns None if album_title is None or empty.
    Verifies that label_id exists in the labels table before inserting.
    """
    if not album_title:
        return None

    # Create a cache key that includes label_id and release_year
    cache_key = (album_title, label_id, release_year)

    # Check cache first
    if cache_key in album_cache:
        return album_cache[cache_key]

    # Get the albums table
    albums_tbl = get_table("albums")

    # Verify that label_id exists in the labels table if provided
    if label_id is not None:
        labels_tbl = get_table("labels")
        # Use first() instead of scalar_one_or_none() to handle potential duplicates
        result = conn.execute(select(labels_tbl.c.label_id).where(labels_tbl.c.label_id == label_id)).first()
        label_exists = result[0] if result else None

        if not label_exists:
            print(f"⚠️ Label ID {label_id} does not exist in the labels table. Using None instead.")
            label_id = None
            # Update cache key since label_id changed
            cache_key = (album_title, label_id, release_year)
            # Check cache again with the new key
            if cache_key in album_cache:
                return album_cache[cache_key]

    # Build the query conditions
    conditions = [albums_tbl.c.album_title == album_title]
    if label_id is not None:
        conditions.append(albums_tbl.c.label_id == label_id)
    if release_year is not None:
        conditions.append(albums_tbl.c.release_year == release_year)

    # Try to find existing album
    # Use first() instead of scalar_one_or_none() to handle potential duplicates
    stmt = select(albums_tbl.c.album_id).where(*conditions)
    result = conn.execute(stmt).first()
    album_id = result[0] if result else None

    if album_id is None:
        # Insert new album
        values = {"album_title": album_title}
        if label_id is not None:
            values["label_id"] = label_id
        if release_year is not None:
            values["release_year"] = release_year

        stmt = insert(albums_tbl).values(**values)
        result = conn.execute(stmt)
        album_id = result.inserted_primary_key[0]

    # Cache the result
    album_cache[cache_key] = album_id
    return album_id


def upsert_artist(conn: Connection, artist_name: str) -> int:
    """
    Upsert an artist and return its primary key (works on MySQL 8.x).

    Parameters
    ----------
    conn : sqlalchemy.engine.Connection
        An open SQLAlchemy connection.
    artist_name : str
        The canonical artist name (case-sensitive).

    Returns
    -------
    int
        The artist_id primary key for *artist_name*.
    """
    artists_tbl = get_table("artists")

    # ── 1. Quick path: does the artist already exist? ──────────────────────
    existing_id = conn.execute(
        select(artists_tbl.c.artist_id).where(artists_tbl.c.artist_name == artist_name)
    ).scalar_one_or_none()
    if existing_id:
        return existing_id

    # ── 2. Upsert (INSERT … ON DUPLICATE KEY UPDATE) ──────────────────────
    stmt = (
        mysql_insert(artists_tbl)
        .values(artist_name=artist_name)
        .on_duplicate_key_update(artist_name=mysql_insert(artists_tbl).inserted.artist_name)
    )
    result = conn.execute(stmt)

    # MySQL returns the new PK in lastrowid only when an INSERT actually
    # happened; if we hit the duplicate key path we need to re-query.
    artist_id = result.lastrowid
    if not artist_id:
        artist_id = conn.execute(
            select(artists_tbl.c.artist_id).where(artists_tbl.c.artist_name == artist_name)
        ).scalar_one()

    return artist_id


def get_or_create_artist(conn: Connection, artist_name: str) -> Optional[int]:
    """
    Upsert an artist and return its ID. Uses caching for performance.
    Returns None if artist_name is None or empty.
    """
    if not artist_name:
        return None

    # Check cache first
    if artist_name in artist_cache:
        return artist_cache[artist_name]

    # Get the artists table
    artists_tbl = get_table("artists")

    # Try to find existing artist
    # Use first() instead of scalar_one_or_none() to handle potential duplicates
    stmt = select(artists_tbl.c.ArtistID).where(artists_tbl.c.artist_name == artist_name)
    result = conn.execute(stmt).first()
    artist_id = result[0] if result else None

    if artist_id is None:
        # Insert new artist
        stmt = insert(artists_tbl).values(artist_name=artist_name)
        result = conn.execute(stmt)
        artist_id = result.inserted_primary_key[0]

    # Cache the result
    artist_cache[artist_name] = artist_id
    return artist_id


def get_spotify_track(conn: Connection, isrc: str) -> Optional[Dict[str, Any]]:
    """
    Get Spotify track data for a given ISRC.

    Args:
        conn (Connection): Database connection
        isrc (str): ISRC code

    Returns:
        Optional[Dict[str, Any]]: Spotify track data or None if not found
    """
    # Get the spotify_track_static table
    spotify_static_tbl = get_table("spotify_track_static")

    # Get the DSP record ID for this ISRC
    dsp_record_id = conn.execute(
        select(spotify_static_tbl.c.DSPRecordID).where(spotify_static_tbl.c.ISRC == isrc).limit(1)
    ).scalar_one_or_none()

    if not dsp_record_id:
        print(f"⚠️ No Spotify track found for ISRC: {isrc}")
        return None

    # Try to get the track from Spotify API
    try:
        import os

        from spotipy import Spotify
        from spotipy.oauth2 import SpotifyClientCredentials

        # Set up Spotify client
        sp = Spotify(
            client_credentials_manager=SpotifyClientCredentials(
                client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
            )
        )

        # Get the track
        track = sp.track(dsp_record_id)
        return track
    except Exception as e:
        print(f"⚠️ Error getting Spotify track: {e}")

        # Fallback: Try to construct a track object from the database
        try:
            # Get track data from spotify_track_static
            track_data = conn.execute(
                select(spotify_static_tbl).where(spotify_static_tbl.c.DSPRecordID == dsp_record_id)
            ).fetchone()

            if not track_data:
                return None

            # Construct a minimal track object
            return {
                "id": dsp_record_id,
                "name": track_data.album_name,
                "album": {
                    "id": track_data.album_id,
                    "name": track_data.album_name,
                    "release_date": (str(track_data.release_date) if track_data.release_date else None),
                    "label": None,  # We don't have the label in the static table
                },
                "artists": [],
                "external_ids": {"isrc": isrc},
            }
        except Exception as e:
            print(f"⚠️ Error constructing Spotify track from database: {e}")
            return None


def get_tidal_track(conn: Connection, isrc: str) -> Optional[Any]:
    """
    Get Tidal track data for a given ISRC from the private schema.

    Args:
        conn (Connection): Database connection (should be connected to private schema)
        isrc (str): ISRC code

    Returns:
        Optional[Any]: Tidal track object or None if not found
    """
    from sqlalchemy import text

    # Query the private schema table directly with the actual snake_case column names
    query = text(
        """
        SELECT dsp_record_id, isrc, track_number, disc_number,
               album_id, release_date, duration_sec, dsp_name,
               created_at, updated_at
        FROM tidal_track_static
        WHERE isrc = :isrc
        LIMIT 1
    """
    )

    result = conn.execute(query, {"isrc": isrc}).fetchone()

    if not result:
        print(f"⚠️ No Tidal track found for ISRC: {isrc}")
        return None

    # Get the DSP record ID for API lookup
    dsp_record_id = result.dsp_record_id

    # Try to get the track from Tidal API
    try:
        import tidalapi

        # Set up Tidal client using our authentication module
        from web.tidal_auth import get_tidal_session

        session = get_tidal_session()  # Will handle authentication and token refresh

        # Get the track using the dsp_record_id (snake_case from migrated table)
        track = session.get_track(dsp_record_id)
        return track
    except Exception as e:
        print(f"⚠️ Error getting Tidal track: {e}")

        # Fallback: Try to construct a track object from the database
        try:
            # Create a simple object with the necessary attributes from the result
            class TidalTrack:
                def __init__(self, isrc, id, album_id, release_date):
                    self.isrc = isrc
                    self.id = id
                    self.album_id = album_id
                    self.release_date = release_date
                    self.copyright = None

            return TidalTrack(
                isrc=isrc,
                id=dsp_record_id,
                album_id=None,  # We'd need to add album_id to query if needed
                release_date=result.release_date,
            )
        except Exception as e:
            print(f"⚠️ Error constructing Tidal track from database: {e}")
            return None


def populate_clean_labels(conn: Connection) -> None:
    """
    Reads every LabelID/Name from labels table, applies clean_label_name(),
    and logs when it's done.

    Note: This function assumes the labels table has a CleanName column.
    If it doesn't exist, you'll need to alter the table first.
    """
    # Get the labels table
    labels_tbl = get_table("labels")

    # Check if CleanName column exists
    if not hasattr(labels_tbl.c, "CleanName"):
        print("⚠️ CleanName column not found in labels table. Skipping population.")
        return

    # Fetch all existing labels
    rows = conn.execute(select(labels_tbl.c.label_id, labels_tbl.c.label_name)).fetchall()

    # Update each one
    for lid, raw in rows:
        cleaned = clean_label_name(raw)
        conn.execute(update(labels_tbl).where(labels_tbl.c.label_id == lid).values(CleanName=cleaned))

    print(f"✅ Populated CleanName for {len(rows)} labels!")


def make_json_safe(obj):
    """
    Return a JSON‑serialisable copy of *obj* (fallback → str()).
    Lists / dicts are walked recursively.
    """
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [make_json_safe(i) for i in obj]
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    return str(obj)


def normalize_tidal(
    track: Any,
    conn: Connection,
    dsp_id: int,
    songs_tbl: Table = None,
) -> tuple[dict, tuple, dict, str]:
    """
    Normalize a Tidal track:
      1) Lookup SongID by track.isrc
      2) Clean copyright ⇒ label/dist
      3) Upsert label & distributor
      4) Upsert album
      5) Build song_update dict + si_tuple + stream_rec + raw_meta_json

    For backward compatibility:
    - If songs_tbl is provided, it will be used instead of looking up the table
    - Returns a tuple of (song_update, stream_record) if called with the old signature
    """
    isrc = getattr(track, "isrc", None)
    if not isrc:
        raise ValueError("Missing ISRC → skipping this track")

    # 1) SongID lookup
    if songs_tbl is None:
        songs_tbl = get_table("songs")
    # Check if the song exists in the database
    exists = conn.execute(select(songs_tbl.c.ISRC).where(songs_tbl.c.ISRC == isrc)).scalar_one_or_none()
    if exists is None:
        raise ValueError(f"No Songs entry for ISRC={isrc!r}")

    # Use ISRC as the song identifier
    song_id = isrc

    # 2) Metadata
    title = getattr(track, "name", None)
    iswc = getattr(track, "iswc", None)
    album_name = getattr(track.album, "title", None) if getattr(track, "album", None) else None

    # Extract release year if available
    release_year = None
    release_date = getattr(track.album, "release_date", None) if getattr(track, "album", None) else None
    if release_date:
        # Extract year from release date (could be YYYY, YYYY-MM, or YYYY-MM-DD)
        release_year_match = re.match(r"^(\d{4})", str(release_date))
        if release_year_match:
            release_year = int(release_year_match.group(1))

    # Optional artwork URL
    art_url = None
    if getattr(track, "album", None) and hasattr(track.album, "picture"):
        try:
            art_url = track.album.picture(640, 640)
        except Exception:
            pass

    # 3) Clean copyright into label/dist
    raw_cp = getattr(track, "copyright", "") or ""
    cp = _clean_copyright(raw_cp)
    if "/" in cp:
        raw_lbl, raw_dist = [p.strip() or None for p in cp.split("/", 1)]
    else:
        raw_lbl, raw_dist = (cp or None, None)
    label_id = get_or_create_label(conn, _strip_year_and_suffix(raw_lbl)) if raw_lbl else None
    distributor_id = get_or_create_label(conn, _strip_year_and_suffix(raw_dist)) if raw_dist else None

    # 4) Create or get album
    album_id = get_or_create_album(conn, album_name, label_id, release_year) if album_name else None

    # 5) Build song_update
    song_update = {
        "song_id": song_id,
        "isrc": isrc,
        "song_title": title,
        "iswc": iswc,
        "album_title": album_name,
        "artwork_url": art_url,
        "label_id": label_id,
        "distributor_id": distributor_id,
        "album_id": album_id,
    }

    # 6) Identifiers tuple
    dsp_record_id = str(getattr(track, "id", None))
    si_tuple = (song_id, dsp_id, dsp_record_id)

    # 7) Raw metadata JSON
    raw_meta = {k: make_json_safe(v) for k, v in track.__dict__.items()}
    raw_meta_json = json.dumps(raw_meta)

    # 8) Stream metrics
    stream_rec = {
        "isrc": isrc,
        "dsp_name": "Tidal" if dsp_id == "Tidal" else dsp_id,
        "fetch_datetime": datetime.now(timezone.utc),
        "popularity": getattr(track, "popularity", None),
        "track_number": getattr(track, "track_num", None),
        "disc_number": getattr(track, "disc_number", None),
    }

    # 9) Add to song version batch
    batch_upsert_song_version(isrc, "Tidal", dsp_record_id, title, album_name)

    # For backward compatibility with existing code
    # Check if called with the old signature (track, conn, tidal_dsp_id)
    import inspect

    frame = inspect.currentframe().f_back
    if frame:
        # Check if we're being called from Cell 7 in etl_pipeline.ipynb
        # In this case, we're called with (t, conn, tidal_dsp_id)
        args = list(frame.f_locals.values())
        if (
            len(args) >= 3
            and isinstance(args[0], object)
            and isinstance(args[1], Connection)
            and isinstance(args[2], int)
        ):
            # Convert to old format for backward compatibility
            old_song_update = {
                "SongID": song_id,
                "ISRC": isrc,
                "Title": title,
                "AlbumName": album_name,
                "ArtworkURL": art_url,
                "label_id": label_id,
                "album_id": album_id,
            }
            old_stream_record = {
                "SongID": song_id,
                "DSPID": dsp_id,
                "DSPRecordID": dsp_record_id,
                "FetchDateTime": datetime.now(timezone.utc),
                "Popularity": getattr(track, "popularity", None),
                "DurationSec": getattr(track, "duration", None),
                "TrackName": title,
                "TrackNumber": getattr(track, "track_num", None),
                "ReplayGain": getattr(track, "replay_gain", None),
                "Peak": getattr(track, "peak", None),
                "AudioQuality": getattr(track, "audio_quality", None),
            }
            return old_song_update, old_stream_record

    # Otherwise, return the new format
    return song_update, si_tuple, stream_rec, raw_meta_json


def upsert_songs(conn_or_records, records_or_conn=None, conn_or_none=None) -> None:
    """
    Bulk‑upsert into Songs using MySQL `ON DUPLICATE KEY UPDATE`.
    No‑ops gracefully if *records* is empty.

    Supports both new and old signatures:
    - New: upsert_songs(conn, records)
    - Old: upsert_songs(records, conn) or upsert_songs(songs_tbl, records, conn)
    """
    # Determine which signature is being used
    if isinstance(conn_or_records, Connection):
        # New signature: upsert_songs(conn, records)
        conn = conn_or_records
        records = records_or_conn
    elif records_or_conn is not None and isinstance(records_or_conn, Connection):
        # Old signature: upsert_songs(records, conn) or upsert_songs(songs_tbl, records, conn)
        records = conn_or_records
        conn = records_or_conn
    elif conn_or_none is not None and isinstance(conn_or_none, Connection):
        # Old signature with table: upsert_songs(songs_tbl, records, conn)
        records = records_or_conn
        conn = conn_or_none
    else:
        raise ValueError("Invalid arguments to upsert_songs")
    if not records:
        return

    songs_tbl = get_table("songs")
    from sqlalchemy.dialects.mysql import insert as mysql_insert  # local import to avoid top‑level duplicate

    stmt = mysql_insert(songs_tbl).values(records)

    # Determine which columns to update based on the first record
    # (assumes all records have the same structure)
    first_record = records[0]
    update_cols = {col: stmt.inserted[col] for col in first_record if col != "isrc"}  # Don't update primary key

    stmt = stmt.on_duplicate_key_update(**update_cols)
    conn.execute(stmt)


import logging

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert


def safe_upsert_legacy(conn, tbl, record: dict) -> None:
    # -------------------------------------------------------------------
    # LEGACY ROW-BY-ROW UPSERT  – kept for reference / small jobs only
    # -------------------------------------------------------------------
    """
    Insert or update a single row.
    • Columns that do not exist on *tbl* are silently dropped.
    • None-values are stripped so you never overwrite real data with NULLs.
    • Primary-key columns are never updated.
    """
    cols = {c.name for c in inspect(tbl).columns}
    clean = {k: v for k, v in record.items() if k in cols and v is not None}
    if not clean:
        return

    stmt = mysql_insert(tbl).values(**clean)

    # Do not touch primary-key columns on update
    pk_cols = {c.name for c in tbl.primary_key.columns}
    update_cols = {k: v for k, v in clean.items() if k not in pk_cols}
    if update_cols:
        stmt = stmt.on_duplicate_key_update(**update_cols)

    conn.execute(stmt)


# FAST MYSQL UPSERT!!!
def bulk_upsert(
    engine: Engine,
    table: Table,
    rows: list[dict],
    conflict_columns=None,  # 👈 add this back
    update_columns=None,
) -> int:
    """
    Insert *rows* into *table*, updating existing rows on conflicts.

    • Works on every MySQL ≥5.7 release. [oai_citation:5‡dev.mysql.com](https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-19.html)
    • UPDATE clause references *only* the columns you actually supplied,
      so “unknown column new.xyz” can’t happen again. [oai_citation:6‡stackoverflow.com](https://stackoverflow.com/questions/68878680/on-duplicate-key-doesnt-work-with-my-insert-into-script)
    """
    conflict_columns = conflict_columns or []
    update_columns = update_columns or []
    if not rows:
        return 0

    insert_stmt = mysql_insert(table).values(rows)  # explicit VALUES list
    pk_cols = {
        c.name for c in inspect(table).primary_key
    }  # fast PK lookup [oai_citation:7‡stackoverflow.com](https://stackoverflow.com/questions/46012899/error-1054-unknown-column-in-field-list?utm_source=chatgpt.com)
    update_map = {c: insert_stmt.inserted[c] for c in rows[0] if c not in pk_cols}

    stmt = insert_stmt.on_duplicate_key_update(
        **update_map
    )  # SQLAlchemy magic [oai_citation:8‡docs.sqlalchemy.org](https://docs.sqlalchemy.org/en/latest/dialects/mysql.html?utm_source=chatgpt.com)
    with engine.begin() as conn:
        return conn.execute(stmt).rowcount


def clean_df_playcounts(df_playcounts):
    df_playcounts.rename(
        columns={
            "dsprecordid": "DSPRecordID",
            "playcount": "Playcount",
            "popularity": "Popularity",
            "trackname": "TrackName",
            "discnumber": "DiscNumber",
            "tracknumber": "TrackNumber",
        },
        inplace=True,
        errors="ignore",
    )
    print("✅ df_playcounts columns:", df_playcounts.columns.tolist())
    return df_playcounts


# ──────────────────────────────────────────────────────────────
# 1D.  Ensure required DSP rows exist
# ──────────────────────────────────────────────────────────────
from sqlalchemy import insert, select

# %%  ← ordinary notebook cell

# utils.py lives in the `etl` package, not in `web`
# from etl.utils import tables_exist  # Commented out - causing MySQL import error


def _assert_tables_exist(engine: Engine, names: List[str]) -> None:
    """
    Ensure that every requested table is present in the target database.

    Raises
    ------
    RuntimeError
        If one or more tables are missing, with a helpful hint on how to fix it.
    """
    # Simple check for table existence without etl.utils dependency
    from sqlalchemy import inspect as sqlalchemy_inspect

    inspector = sqlalchemy_inspect(engine)
    existing_tables = inspector.get_table_names()
    missing_tables = [name for name in names if name not in existing_tables]

    if missing_tables:
        raise RuntimeError(
            f"Missing required tables: {', '.join(missing_tables)}. "
            "Please run 'alembic upgrade head' to create all required tables."
        )


def ensure_dsp_rows(engine: Engine, dsp_names: List[str]) -> None:
    """
    Idempotently insert *dsp_names* into dsp_providers.
    Safe to call at startup of any ETL script.

    Args:
        engine (Engine): SQLAlchemy engine
        dsp_names (List[str]): List of DSP names to ensure exist
    """
    dsp_tbl = get_table("dsp_providers")  # uses handles built by init_tables()
    with engine.begin() as conn:
        present = {name for (name,) in conn.execute(select(dsp_tbl.c.dsp_name))}
        to_add = set(dsp_names) - present
        if to_add:
            conn.execute(
                insert(dsp_tbl),
                [{"dsp_name": n} for n in to_add],
            )


def seed_version_types(engine: Engine) -> None:
    """
    Idempotently insert canonical version types into version_types.
    Safe to call at startup of any ETL script.
    """
    version_types = [
        "Acoustic",
        "Chopped and Screwed",
        "Live",
        "Original",
        "Radio Edit",
        "Remix",
        "Visualizer",
    ]

    version_types_tbl = get_table("version_types")

    with engine.begin() as conn:
        present = {vt for (vt,) in conn.execute(select(version_types_tbl.c.version_type))}
        to_add = set(version_types) - present
        if to_add:
            conn.execute(
                insert(version_types_tbl),
                [{"version_type": vt} for vt in to_add],
            )


def seed_role_types(engine: Engine) -> None:
    """
    Idempotently insert canonical role types into the `role_types` table.
    Safe to call at startup of any ETL script.
    """
    from sqlalchemy import insert, select, text

    # Get the reflected table
    role_types_tbl = get_table("role_types")
    # The list of canonical roles we want present
    canonical = [
        "Primary",
        "Featured",
        "Producer",
        "Composer",
        "Lyricist",
        "Remixer",
    ]

    # Open a transaction-scoped Connection (SQLAlchemy 2.x style)
    with engine.begin() as conn:
        # 1️⃣ Fetch existing role names (each row is a single string)
        existing = {role for (role,) in conn.execute(select(role_types_tbl.c.role_name))}

        # 2️⃣ Determine which roles are missing
        to_insert = [{"role_name": r} for r in canonical if r not in existing]
        if not to_insert:
            return  # All canonical roles already exist

        # Build an INSERT ... ON DUPLICATE KEY UPDATE statement
        stmt = insert(role_types_tbl).on_duplicate_key_update(role_name=text("VALUES(role_name)"))

        # Execute the bulk insert
        conn.execute(stmt, to_insert)


def seed_song_artist_roles(conn: Connection, raw_tracks: list[dict]) -> None:
    """
    Idempotently seed song_artist_roles table with artist role assignments.

    Args:
        conn: SQLAlchemy connection
        raw_tracks: List of raw track dictionaries in Spotify API format

    This function:
    - Processes each track to identify artists and their roles
    - Automatically identifies featured artists based on name patterns
    - Assigns primary/featured roles based on detection
    - Inserts/updates rows in song_artist_roles table
    """
    import re

    # Helper function to detect if an artist is featured based on name
    def is_featured_artist(name: str) -> bool:
        """Return True if the artist name indicates they are featured (contains feat., ft., etc.)"""
        pattern = r"feat\.?|ft\.?|featuring"
        return bool(re.search(pattern, name.lower()))

    # Helper function to extract featured artists from track title
    def extract_featured_from_title(title: str) -> list[str]:
        """
        Extract featured artist names from track title.
        Example: "I'm Good (feat. Erick Lottary)" -> ["Erick Lottary"]
        """
        featured_artists = []
        pattern = r"\((?:feat\.?|ft\.?|featuring)\s+([^)]+)\)"
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            artists_part = match.group(1)
            for artist in re.split(r",\s*|&\s*", artists_part):
                featured_artists.append(artist.strip())
        return featured_artists

    # Get necessary tables
    songs_tbl = get_table("songs")
    roles_tbl = get_table("role_types")
    song_artist_roles_tbl = get_table("song_artist_roles")

    # Get role IDs
    role_id_map = {row.role_name: row.role_id for row in conn.execute(select(roles_tbl))}

    # Get the primary and featured artist role IDs
    primary_role_id = role_id_map.get("Primary")
    featured_role_id = role_id_map.get("Featured")

    if not primary_role_id or not featured_role_id:
        # This should not happen as seed_role_types should be called before this function
        raise ValueError("Role types 'Primary' and 'Featured' must exist before calling seed_song_artist_roles")

    for track in raw_tracks:
        isrc = track.get("external_ids", {}).get("isrc")
        if not isrc:
            continue

        # Check if the song exists
        if conn.execute(select(songs_tbl.c.isrc).where(songs_tbl.c.isrc == isrc)).scalar_one_or_none() is None:
            continue

        # Get all artists for this track and auto-detect featured artists
        artists = []
        auto_featured = []  # Track which artists are automatically detected as featured

        # Extract featured artists from track title
        track_title = track["name"]
        featured_from_title = extract_featured_from_title(track_title)

        for art in track["artists"]:
            name = art["name"]
            # Use get_or_create_artist to ensure the artist exists and get their ID
            aid = get_or_create_artist(conn, name)

            # Check if artist is featured based on their name
            is_featured = is_featured_artist(name)
            artists.append((aid, name))
            if is_featured:
                auto_featured.append(len(artists))  # Store the 1-based index

        # Check if any artists match the featured artists extracted from the title
        if featured_from_title:
            for i, (aid, name) in enumerate(artists, 1):
                for featured_name in featured_from_title:
                    # Simple case-insensitive comparison
                    if featured_name.lower() in name.lower() or name.lower() in featured_name.lower():
                        if i not in auto_featured:
                            auto_featured.append(i)
                            break

        # Determine primary and featured artists
        if len(artists) == 1:
            # If there's only one artist, they're automatically the primary artist
            primary_indices = [1]
        elif auto_featured and len(auto_featured) < len(artists):
            # We have some auto-detected featured artists, but not all artists are featured
            primary_indices = [i for i in range(1, len(artists) + 1) if i not in auto_featured]
        else:
            # Default: first artist is primary, rest are featured
            primary_indices = [1]

        # Insert artist roles into database
        for i, (aid, name) in enumerate(artists, 1):
            is_primary = i in primary_indices
            role_id = primary_role_id if is_primary else featured_role_id

            # Insert into database
            link_payload = {
                "isrc": isrc,
                "artist_id": aid,
                "role_id": role_id,
            }

            # Use MySQL's INSERT ... ON DUPLICATE KEY UPDATE for idempotence
            stmt = mysql_insert(song_artist_roles_tbl).values(link_payload)
            stmt = stmt.on_duplicate_key_update(
                role_id=text("VALUES(role_id)"),
            )
            conn.execute(stmt)


# ── 1) DEBUG logging setup – only affects this notebook session ──────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    force=True,  # override any earlier logging config
)
logger = logging.getLogger("playcount_upsert_debug")

# # ── 2) Reflected table handles & constants ───────────────────────────────────
# SpotifyTracks_tbl        = metadata.tables["SpotifyTracks"]
# spotify_playcounts_tbl   = metadata.tables["spotify_playcounts"]
# SPOTIFY_DSP_ID           = getattr(etl_helpers, "SPOTIFY_DSP_ID", None)
#
# logger.info("🐛 DEBUG: SPOTIFY_DSP_ID            = %r", SPOTIFY_DSP_ID)
# logger.info("🐛 DEBUG: df_playcounts.shape       = %s", df_playcounts.shape)
# logger.info("🐛 DEBUG: spotify_playcounts PK col = %s", list(spotify_playcounts_tbl.primary_key)[0].name)
#
# # ── 3) Wrapper that logs every statement it executes ─────────────────────────
# def debug_upsert_spotify_playcounts(conn, df, pc_tbl, tracks_tbl, dsp_id):
#     logger.info("▶️  START debug_upsert_spotify_playcounts")
#     logger.debug("Tracks cols      : %s", list(tracks_tbl.c.keys()))
#     logger.debug("Playcounts cols  : %s", list(pc_tbl.c.keys()))
#
#     for i, row in enumerate(df.itertuples(index=False), start=1):
#         logger.debug("–––– Row %d ––––", i)
#         logger.debug("DSPRecordID=%s  Playcount=%s  Disc=%s Track=%s",
#                      row.DSPRecordID, row.Playcount, row.DiscNumber, row.TrackNumber)
#
#         # 1️⃣  Verify foreign-key (SpotifyTracks) exists
#         exists = conn.execute(
#             select(func.count())
#             .select_from(tracks_tbl)
#             .where(tracks_tbl.c.DSPID == dsp_id)
#             .where(tracks_tbl.c.DSPRecordID == row.DSPRecordID)
#         ).scalar() or 0
#         logger.debug("exists_in_SpotifyTracks? %d", exists)
#         if exists == 0:
#             logger.warning("SKIP  – no matching SpotifyTracks row")
#             continue
#
#         # 2️⃣  Build INSERT … ON DUPLICATE KEY UPDATE
#         insert_vals = {
#             "TrackID":   row.DSPRecordID,   # PK in spotify_playcounts
#             "Playcount": row.Playcount,
#             "DiscNumber": row.DiscNumber,
#             "TrackNumber": row.TrackNumber,
#             "LastPlaycountUpdate": func.now(),
#         }
#         stmt = mysql_insert(pc_tbl).values(**insert_vals).on_duplicate_key_update(
#             Playcount           = stmt.inserted.Playcount,
#             DiscNumber          = stmt.inserted.DiscNumber,
#             TrackNumber         = stmt.inserted.TrackNumber,
#             LastPlaycountUpdate = func.now(),
#         )
#
#         # 3️⃣  Show fully-rendered SQL (literal values) then execute
#         compiled = stmt.compile(dialect=conn.dialect,
#                                 compile_kwargs={"literal_binds": True})
#         logger.debug("SQL: %s", compiled.string)
#         try:
#             res = conn.execute(stmt)
#             logger.info("UPSERT OK – rowcount=%d", res.rowcount)
#         except Exception:
#             logger.exception("‼️  UPSERT FAILED")
#             raise     # still raise so you see a stack-trace in notebook
#
#     logger.info("⏹️  END debug_upsert_spotify_playcounts")


# ------------------------------------------------------------------------------
# 1) JSON → DataFrame loader for Spotify playcounts (from your `data.json`)
# ------------------------------------------------------------------------------
def load_playcounts(path: str) -> pd.DataFrame:
    """
    Load Spotify playlistV2 JSON from `path` and return a DataFrame with columns:
      DSPRecordID (Spotify track ID),
      Playcount (int),
      Popularity (float/int or None),
      TrackName (str),
      DiscNumber (int or None),
      TrackNumber (int or None)
    """
    raw = json.load(open(path, "r"))
    items = raw.get("data", {}).get("playlistV2", {}).get("content", {}).get("items", [])

    rows = []
    for entry in items:
        d = entry.get("itemV2", {}).get("data", {})
        pc = d.get("playcount")
        if pc is None:
            continue

        uri = d.get("uri", "")
        # Extract the Spotify ID (last segment after colon)
        rec = uri.split(":")[-1] if ":" in uri else uri

        rows.append(
            {
                "DSPRecordID": rec,
                "Playcount": int(pc),
                "Popularity": d.get("popularity"),
                "TrackName": d.get("name"),
                "DiscNumber": d.get("trackNumber") and d.get("discNumber"),
                "TrackNumber": d.get("trackNumber"),
            }
        )

    df = pd.DataFrame(rows)
    return df


# ------------------------------------------------------------------------------
# 2) (Optional) Your existing debug helper, unchanged
# ------------------------------------------------------------------------------
import logging

from sqlalchemy import func


def debug_upsert_spotify_playcounts(conn, df, pc_tbl, tracks_tbl, dsp_id):
    """
    Verbose‐logging wrapper around the playcounts upsert.
    """
    logger = logging.getLogger("playcount_upsert_debug")
    logger.info("▶️  START debug_upsert_spotify_playcounts")
    logger.debug("DSPID = %r", dsp_id)
    logger.debug("Tracks cols: %s", list(tracks_tbl.c.keys()))
    logger.debug("Playcounts cols: %s", list(pc_tbl.c.keys()))

    for i, row in enumerate(df.itertuples(index=False), start=1):
        logger.debug("––– Row %d: %s", i, row._asdict())
        # 1) Verify existence in SpotifyTracks
        exists = (
            conn.execute(
                select(func.count())
                .select_from(tracks_tbl)
                .where(tracks_tbl.c.DSPID == dsp_id)
                .where(tracks_tbl.c.DSPRecordID == row.DSPRecordID)
            ).scalar()
            or 0
        )
        logger.debug("exists_in_SpotifyTracks? %d", exists)
        if not exists:
            logger.warning("SKIP row %d: no matching SpotifyTracks for %r", i, row.DSPRecordID)
            continue

        # 2) Build upsert …
        stmt = (
            mysql_insert(pc_tbl)
            .values(
                TrackID=row.DSPRecordID,
                Playcount=row.Playcount,
                DiscNumber=row.DiscNumber,
                TrackNumber=row.TrackNumber,
                LastPlaycountUpdate=func.now(),
            )
            .on_duplicate_key_update(
                Playcount=stmt.inserted.Playcount,
                DiscNumber=stmt.inserted.DiscNumber,
                TrackNumber=stmt.inserted.TrackNumber,
                LastPlaycountUpdate=func.now(),
            )
        )

        # 3) Log & execute
        sql = stmt.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True}).string
        logger.debug("SQL: %s", sql)
        conn.execute(stmt)
        logger.info("UPSERT OK for %r", row.DSPRecordID)

    logger.info("⏹️  END debug_upsert_spotify_playcounts")


# ---------------------------------------------------------------------------
# Debug helpers
# ---------------------------------------------------------------------------


def _run_debug_upsert() -> None:
    """
    Run the Spotify-play-count upsert in DEBUG mode.
    Invoke with:  python -m web.etl_helpers
    """
    import pandas as pd

    engine = get_engine(echo=False)

    # 1️⃣  Reflect tables locally
    init_tables(engine)
    SpotifyTracks_tbl = get_table("spotify_track_static")
    spotify_playcounts_tbl = get_table("spotify_playcounts")
    dsp_tbl = get_table("dsp_providers")

    # 2️⃣  Look up the Spotify DSP ID once
    with engine.connect() as conn:
        SPOTIFY_DSP_ID = conn.scalar(select(dsp_tbl.c.DSPName).where(dsp_tbl.c.DSPName == "Spotify"))

    # 3️⃣  Load & clean the JSON file you want to upsert
    df_playcounts = clean_df_playcounts(load_playcounts("data/playlist.json"))

    # 4️⃣  Execute the upsert
    print("🚀 Upserting play-counts in DEBUG mode …")
    with engine.begin() as conn:
        debug_upsert_spotify_playcounts(
            conn,
            df_playcounts,
            spotify_playcounts_tbl,
            SpotifyTracks_tbl,
            SPOTIFY_DSP_ID,
        )
    print("✅ All done!")


# ---------------------------------------------------------------------------
# Run the debug helper only when executed directly
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    _run_debug_upsert()
    # ────────────────────────────────────────────────────────────────
    #  ⬇️  Create debug-only handles *after* init_tables() has run
    # ────────────────────────────────────────────────────────────────
    if __name__ == "__main__":
        from sqlalchemy import select

        # 1️⃣  Engine + full reflection
        engine = get_engine(echo=False)
        init_tables(engine)  # ⬅️ _populates _GLOBAL_META

        # 2️⃣  Grab table handles via convenience helper
        SpotifyTracks_tbl = get_table("spotify_track_static")
        spotify_playcounts_tbl = get_table("spotify_playcounts")
        dsp_tbl = get_table("dsp_providers")

        # 3️⃣  Dynamic lookup → “Spotify” primary-key
        with engine.connect() as conn:
            SPOTIFY_DSP_ID = conn.scalar(
                select(dsp_tbl.c.DSPName).where(dsp_tbl.c.DSPName == "Spotify")  # PK column in your schema
            )

        # 4️⃣  Smoke-test
        with engine.connect() as conn:
            any_track = conn.execute(SpotifyTracks_tbl.select().limit(1)).first()

        print("✅ Debug handles ready!")
        print("   • SpotifyTracks rows :", "≥1" if any_track else "0")
        print(
            "   • Playcounts columns :",
            [c.name for c in spotify_playcounts_tbl.columns],
        )
        print("   • SPOTIFY_DSP_ID     =", SPOTIFY_DSP_ID)


# ──────────────────────────────────────────────────────────────────
# ETL RUN LOGGING FUNCTIONS
# ──────────────────────────────────────────────────────────────────

from datetime import datetime

def detect_run_type() -> str:
    """
    Detect if this is a manual run or CRON job based on environment.

    Returns:
        'cron' if running from cron, 'manual' otherwise
    """
    # Check explicit override from .env
    explicit_type = os.getenv('ETL_RUN_TYPE')
    if explicit_type in ['manual', 'cron']:
        return explicit_type

    # Check common CRON environment indicators
    if os.getenv('CRON_JOB') == '1':
        return 'cron'
    if os.getenv('AUTOMATED_RUN') == '1':
        return 'cron'
    if not os.getenv('TERM'):  # No terminal usually means automated
        return 'cron'
    if os.getenv('USER') == 'root':  # Root user often indicates cron
        return 'cron'

    # Check if running in CI/CD
    ci_indicators = ['CI', 'CONTINUOUS_INTEGRATION', 'GITHUB_ACTIONS', 'JENKINS_URL']
    if any(os.getenv(indicator) for indicator in ci_indicators):
        return 'cron'

    return 'manual'


def start_etl_run(channel_id: str, reason: str = None, engine: Engine = None) -> dict:
    """
    Log the start of an ETL run for a specific channel.

    Args:
        channel_id: YouTube channel ID
        reason: Optional reason for the run
        engine: Database engine (will create if None)

    Returns:
        dict with run info for tracking
    """
    if engine is None:
        engine = get_engine()

    run_info = {
        'channel_id': channel_id,
        'run_date': datetime.now().date(),
        'started_at': datetime.now(),
        'run_type': detect_run_type(),
        'reason': reason or f"ETL run for {channel_id}",
        'status': 'running'
    }

    with engine.connect() as conn:
        # Insert or update the run record
        conn.execute(text("""
            INSERT INTO youtube_etl_runs
            (channel_id, run_date, started_at, run_type, reason, status)
            VALUES (:channel_id, :run_date, :started_at, :run_type, :reason, :status)
            ON DUPLICATE KEY UPDATE
            started_at = VALUES(started_at),
            run_type = VALUES(run_type),
            reason = VALUES(reason),
            status = VALUES(status),
            finished_at = NULL,
            error_message = NULL
        """), run_info)
        conn.commit()

    return run_info


def finish_etl_run(run_info: dict, status: str = 'success', error_message: str = None,
                   videos_processed: int = 0, metrics_collected: int = 0, engine: Engine = None):
    """
    Log the completion of an ETL run.

    Args:
        run_info: Run info dict from start_etl_run()
        status: 'success', 'failed', or 'partial'
        error_message: Error details if failed
        videos_processed: Number of videos processed
        metrics_collected: Number of metrics collected
        engine: Database engine (will create if None)
    """
    if engine is None:
        engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE youtube_etl_runs
            SET finished_at = :finished_at,
                status = :status,
                error_message = :error_message,
                videos_processed = :videos_processed,
                metrics_collected = :metrics_collected
            WHERE channel_id = :channel_id AND run_date = :run_date
        """), {
            'channel_id': run_info['channel_id'],
            'run_date': run_info['run_date'],
            'finished_at': datetime.now(),
            'status': status,
            'error_message': error_message,
            'videos_processed': videos_processed,
            'metrics_collected': metrics_collected
        })
        conn.commit()


def get_etl_run_summary(days: int = 7, engine: Engine = None) -> pd.DataFrame:
    """
    Get a summary of ETL runs for the last N days.

    Args:
        days: Number of days to look back
        engine: Database engine (will create if None)

    Returns:
        DataFrame with run summary
    """
    if engine is None:
        engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                channel_id,
                run_date,
                started_at,
                finished_at,
                status,
                run_type,
                reason,
                videos_processed,
                metrics_collected,
                error_message,
                TIMESTAMPDIFF(SECOND, started_at, finished_at) as duration_seconds
            FROM youtube_etl_runs
            WHERE run_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
            ORDER BY run_date DESC, started_at DESC
        """), {'days': days})

        return pd.DataFrame(result.fetchall(), columns=result.keys())


def log_etl_attempt(channel_id: str, success: bool, reason: str = None,
                   error_message: str = None, videos_processed: int = 0,
                   metrics_collected: int = 0, engine: Engine = None):
    """
    Convenience function to log a complete ETL attempt in one call.

    Args:
        channel_id: YouTube channel ID
        success: Whether the run was successful
        reason: Reason for the run
        error_message: Error details if failed
        videos_processed: Number of videos processed
        metrics_collected: Number of metrics collected
        engine: Database engine (will create if None)
    """
    run_info = start_etl_run(channel_id, reason, engine)
    status = 'success' if success else 'failed'
    finish_etl_run(run_info, status, error_message, videos_processed, metrics_collected, engine)
