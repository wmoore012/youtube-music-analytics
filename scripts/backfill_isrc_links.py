#!/usr / bin / env python3
"""
Backfill ISRC - related links using only existing local tables (no external backfill):
- Ensure isrc_recordings contains all ISRCs from songs and youtube_videos
- Create video_recording_link rows for explicit ISRCs from youtube_videos
- Create conservative title / artist exact - match links from youtube_videos to songs
- Then you can run normalize to reduce nulls in music_videos_normalized

This is idempotent and safe to re - run.
"""
from __future__ import annotations

import sys
from typing import Any, List, Set, Tuple

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

from web.db_guard import get_engine
from web.youtube_version_parser import clean_text as yclean
from web.youtube_version_parser import extract_artists_from_title
from youtubeviz.normalization import canonicalize_artist, load_alias_map


def exec_many(conn: Any, stmts: List[str]) -> None:
    for s in stmts:
        conn.execute(text(s))


def backfill_isrc() -> int:
    load_dotenv()
    engine = get_engine()
    affected = 0
    with engine.begin() as conn:
        # 1) Seed isrc_recordings from songs
        stmt1 = """
        INSERT INTO isrc_recordings (isrc, title, artist_primary, source)
        SELECT s.isrc, s.title, s.artist, 'songs_table'
        FROM songs s
        LEFT JOIN isrc_recordings ir ON ir.isrc = s.isrc
        WHERE ir.isrc IS NULL
        """

        # 2) Seed isrc_recordings from youtube_videos where ISRC is present
        stmt2 = """
        INSERT INTO isrc_recordings (isrc, title, artist_primary, source)
        SELECT DISTINCT uv.isrc, uv.title, uv.channel_title, 'youtube_videos'
        FROM youtube_videos uv
        LEFT JOIN isrc_recordings ir ON ir.isrc = uv.isrc
        WHERE uv.isrc IS NOT NULL AND ir.isrc IS NULL
        """

        # 3) Link explicit ISRCs from youtube_videos → video_recording_link
        stmt3 = """
        INSERT INTO video_recording_link (video_id, isrc, match_method, confidence)
        SELECT uv.video_id, uv.isrc, 'explicit_isrc', 1.000
        FROM youtube_videos uv
        LEFT JOIN video_recording_link vrl
          ON vrl.video_id = uv.video_id AND vrl.isrc = uv.isrc
        WHERE uv.isrc IS NOT NULL AND vrl.video_id IS NULL
        """

        # 4) Conservative exact match on title + artist to songs
        #    Use normalized lowercase trimmed strings for equality
        stmt4 = """
        INSERT INTO video_recording_link (video_id, isrc, match_method, confidence)
        SELECT uv.video_id, s.isrc, 'title_parse', 0.950
        FROM youtube_videos uv
        JOIN songs s
          ON LOWER(TRIM(uv.title)) = LOWER(TRIM(s.title))
         AND LOWER(TRIM(uv.channel_title)) = LOWER(TRIM(s.artist))
        LEFT JOIN video_recording_link vrl
          ON vrl.video_id = uv.video_id AND vrl.isrc = s.isrc
        WHERE uv.isrc IS NULL AND vrl.video_id IS NULL
        """

        res1 = conn.execute(text(stmt1))
        res2 = conn.execute(text(stmt2))
        res3 = conn.execute(text(stmt3))
        res4 = conn.execute(text(stmt4))
        # rowcount may be -1 for some drivers; treat None/-1 as 0
        for r in (res1, res2, res3, res4):
            try:
                n = 0 if r.rowcount in (None, -1) else int(r.rowcount)
                affected += n
            except Exception:
                pass
    # Phase 2: Conservative Python - side parsed matches (title + artist)
    # - Use youtube_version_parser to extract artist + cleaned title
    # - Canonicalize artist via alias map
    # - Join to songs on exact cleaned title + canonical artist (lowercased)
    # - Insert new VRL rows for unique matches
    aliases = load_alias_map()
    with engine.connect() as conn:
        # Load YT rows with missing ISRC
        yt = pd.read_sql(  # type: ignore[call - overload]
            """
            SELECT video_id, title, channel_title AS artist_name
            FROM youtube_videos
            WHERE isrc IS NULL OR TRIM(COALESCE(isrc, '')) = ''
            """,
            conn,
        )
        if not yt.empty:
            parsed_artists: List[str] = []
            cleaned_titles: List[str] = []
            for _, r in yt.iterrows():
                title = r.get("title") or ""
                channel = r.get("artist_name") or ""
                artists, cleaned_title = extract_artists_from_title(str(title), str(channel))
                artist = artists[0] if artists else channel
                artist = canonicalize_artist(artist.strip(), aliases)
                parsed_artists.append(artist.strip().lower())
                cleaned_titles.append((cleaned_title or str(title)).strip().lower())
            yt["artist_key"] = parsed_artists
            yt["title_key"] = cleaned_titles

            # Load songs
            songs = pd.read_sql(  # type: ignore[call - overload]
                "SELECT isrc, title, artist FROM songs",
                conn,
            )
            if not songs.empty:
                # Canonicalize artists and clean titles similarly to YT parsing
                songs["artist_key"] = (
                    songs["artist"]
                    .fillna("")
                    .astype(str)
                    .map(lambda s: canonicalize_artist(s, aliases))
                    .str.strip()
                    .str.lower()
                )
                songs["title_key"] = (
                    songs["title"].fillna("").astype(str).map(lambda s: yclean(s)).str.strip().str.lower()
                )

                m = yt.merge(songs, on=["artist_key", "title_key"], how="inner")
                # Deduplicate exact pairs
                pairs = (
                    m[["video_id", "isrc"]]
                    .dropna()  # type: ignore[call - overload]
                    .drop_duplicates()
                    .itertuples(index=False, name=None)
                )

                # Fetch existing VRL pairs to keep idempotent
                existing = pd.read_sql("SELECT video_id, isrc FROM video_recording_link",
                                       conn)  # type: ignore[call - overload]
                existing_set: Set[Tuple[str, str]] = (
                    set((str(r[0]), str(r[1])) for r in existing.itertuples(index=False, name=None))
                    if not existing.empty
                    else set()
                )

                to_insert = [(vid, isrc) for (vid, isrc) in pairs if (str(vid), str(isrc)) not in existing_set]

                if to_insert:
                    with engine.begin() as wconn:
                        ins = text(
                            """
                            INSERT INTO video_recording_link (video_id, isrc, match_method, confidence)
                            VALUES (:video_id, :isrc, 'parsed_exact', 0.920)
                            """
                        )
                        for vid, isrc in to_insert:
                            wconn.execute(ins, {"video_id": vid, "isrc": isrc})
                        affected += len(to_insert)
    return affected


def main(argv: list[str]) -> int:
    total = backfill_isrc()
    print(f"Backfill complete. Rows affected (approx): {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
