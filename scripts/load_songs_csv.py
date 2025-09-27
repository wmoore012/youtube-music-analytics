#!/usr / bin / env python3
"""
Load a CSV of songs (isrc,title,artist) into the songs table quickly.

Requirements:
- CSV headers: isrc,title,artist (case - insensitive OK)
- ISRC must match format: ^[A - Z]{2}[A - Z0 - 9]{3}[0 - 9]{2}[0 - 9]{5}$ (case - insensitive accepted; will be uppercased)

Behavior:
- Validates rows and prints a summary of accepted / rejected
- Upsert semantics: INSERT ... ON DUPLICATE KEY UPDATE title / artist
- Optional dry - run mode
"""
from __future__ import annotations

import argparse
import csv
import re
from typing import List, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

from web.db_guard import get_engine

ISRC_RE = re.compile(r"^[A - Z]{2}[A - Z0 - 9]{3}[0 - 9]{2}[0 - 9]{5}$", re.IGNORECASE)


def validate_row(row: dict) -> Tuple[bool, str]:
    isrc = (row.get("isrc") or row.get("ISRC") or "").strip().upper()
    title = (row.get("title") or row.get("Title") or "").strip()
    artist = (row.get("artist") or row.get("Artist") or "").strip()
    if not isrc or not title or not artist:
        return False, "missing required fields"
    if not ISRC_RE.match(isrc):
        return False, f"invalid ISRC format: {isrc}"
    return True, ""


def load_csv(path: str, dry_run: bool = False) -> Tuple[int, int]:
    load_dotenv()
    engine = get_engine()
    ok_rows: List[Tuple[str, str, str]] = []
    bad_rows = 0

    with open(path, newline="", encoding="utf - 8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            valid, reason = validate_row(row)
            if not valid:
                bad_rows += 1
                continue
            isrc = (row.get("isrc") or row.get("ISRC") or "").strip().upper()
            title = (row.get("title") or row.get("Title") or "").strip()
            artist = (row.get("artist") or row.get("Artist") or "").strip()
            ok_rows.append((isrc, title, artist))

    inserted = 0
    if not dry_run and ok_rows:
        sql = text(
            """
            INSERT INTO songs (isrc, title, artist)
            VALUES (:isrc, :title, :artist)
            ON DUPLICATE KEY UPDATE
              title=VALUES(title),
              artist=VALUES(artist)
            """
        )
        with engine.begin() as conn:
            for isrc, title, artist in ok_rows:
                conn.execute(sql, {"isrc": isrc, "title": title, "artist": artist})
                inserted += 1

    return inserted, bad_rows


def main() -> int:
    p = argparse.ArgumentParser(description="Load songs CSV into songs table")
    p.add_argument("csv_path", help="Path to CSV with headers isrc,title,artist")
    p.add_argument("--dry - run", action="store_true", help="Validate only; do not write to DB")
    args = p.parse_args()

    ins, bad = load_csv(args.csv_path, dry_run=args.dry_run)
    mode = "(dry - run) " if args.dry_run else ""
    print(f"{mode}Songs processed: {ins} inserted / updated, {bad} rejected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
