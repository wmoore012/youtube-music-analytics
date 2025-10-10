#!/usr / bin / env python3
"""
Extract ISRC codes from youtube_videos_raw.raw_data JSON and create safe links:
- Parse description / title fields for strict ISRC pattern
- Insert into isrc_recordings if missing
- Insert into video_recording_link (match_method='raw_isrc') if not present
- Optionally update youtube_videos.isrc when currently blank (flag)

Idempotent and local-only (no external calls).
"""
from __future__ import annotations

import argparse
import re
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from web.db_guard import get_engine

# Match standard ISRC with optional separators: CC-XXX-YY-NNNNN or without separators
ISRC_FLEX_RE = re.compile(
    r"\b([A-Z]{2})[\s\-]?([A-Z0-9]{3})[\s\-]?(\d{2})[\s\-]?(\d{5})\b",
    re.IGNORECASE,
)


def _extract_isrc_from_raw(raw_text: str) -> Optional[str]:
    if not raw_text:
        return None
    m = ISRC_FLEX_RE.search(raw_text)
    if not m:
        return None
    cc, registrant, year, designation = m.group(1), m.group(2), m.group(3), m.group(4)
    code = f"{cc}{registrant}{year}{designation}".upper()
    return code


def run(update_youtube_videos: bool = False) -> int:
    load_dotenv()
    eng = get_engine()
    inserted_links = 0

    # Load candidate rows: videos with blank ISRC and available raw JSON
    sql = text(
        """
        SELECT r.video_id, CAST(r.raw_data AS CHAR) AS raw_text
        FROM youtube_videos_raw r
        JOIN youtube_videos v ON v.video_id = r.video_id
        WHERE (v.isrc IS NULL OR TRIM(COALESCE(v.isrc,'')) = '')
        """
    )

    df = pd.read_sql(sql, eng)  # type: ignore[call-overload]
    if df.empty:
        return 0

    # Parse ISRCs
    df["isrc_found"] = df["raw_text"].apply(_extract_isrc_from_raw)
    matches = df.dropna(subset=["isrc_found"]).copy()
    if matches.empty:
        return 0

    # Insert into isrc_recordings (minimal stub) and VRL
    with eng.begin() as conn:
        # Ensure recordings exist
        conn.execute(
            text(
                """
                INSERT IGNORE INTO isrc_recordings (isrc, source)
                VALUES (:isrc, 'raw_extract')
                """
            ),
            [{"isrc": isrc} for isrc in matches["isrc_found"].astype(str).str.upper().unique().tolist()],
        )

        # Insert VRL links
        ins_vrl = text(
            """
            INSERT INTO video_recording_link (video_id, isrc, match_method, confidence)
            SELECT :video_id, :isrc, 'raw_isrc', 0.990 FROM DUAL
            WHERE NOT EXISTS (
              SELECT 1 FROM video_recording_link WHERE video_id = :video_id AND isrc = :isrc
            )
            """
        )
        for _, r in matches.iterrows():
            conn.execute(ins_vrl, {"video_id": r["video_id"], "isrc": str(r["isrc_found"]).upper()})
            inserted_links += 1

        if update_youtube_videos:
            upd = text(
                """
                UPDATE youtube_videos v
                JOIN (
                  SELECT video_id, UPPER(isrc_found) AS isrc
                  FROM (
                    SELECT DISTINCT video_id, isrc_found FROM (
                      SELECT :video_id AS video_id, :isrc AS isrc_found
                    ) x
                  ) y
                ) m ON m.video_id = v.video_id
                SET v.isrc = m.isrc
                WHERE v.isrc IS NULL OR TRIM(COALESCE(v.isrc,'')) = ''
                """
            )
            # Batch style update per row to keep it simple and safe
            for _, r in matches.iterrows():
                conn.execute(upd, {"video_id": r["video_id"], "isrc": str(r["isrc_found"]).upper()})

    return inserted_links


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--update-youtube-videos", action="store_true", help="Also set youtube_videos.isrc when blank")
    args = p.parse_args()
    n = run(update_youtube_videos=args.update_youtube_videos)
    print(f"ISRC extraction complete. Links inserted: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
