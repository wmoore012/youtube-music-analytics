"""Utilities for maintaining the artist_aliases helper table.

These helpers intentionally use SQLAlchemy Core primitives so they work with
both SQLite (for tests) and MySQL in production without ORM state.  The module
keeps its public surface minimal because several tests rely on deterministic
behaviour for counts, ordering, and case handling.
"""

from __future__ import annotations

from collections import Counter
import json
import os
from pathlib import Path
from typing import Sequence

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Engine

ALIAS_TABLE_NAME = "artist_aliases"


def ensure_alias_table(eng: Engine) -> Table:
    """Create the artist_aliases table if it does not exist and return it."""

    inspector = inspect(eng)
    meta = MetaData()
    if inspector.has_table(ALIAS_TABLE_NAME):
        meta.reflect(bind=eng, only=[ALIAS_TABLE_NAME])  # type: ignore[arg-type]
        return meta.tables[ALIAS_TABLE_NAME]

    Table(
        ALIAS_TABLE_NAME,
        meta,
        Column("alias_id", Integer, primary_key=True, autoincrement=True),
        Column("canonical_name", String(255), nullable=False),
        Column("alias", String(255), nullable=False, server_default=""),
    )
    meta.create_all(eng)
    return meta.tables[ALIAS_TABLE_NAME]


def _existing_aliases(conn, tbl: Table, canonical: str) -> set[str]:
    rows = conn.execute(select(tbl.c.alias).where(tbl.c.canonical_name == canonical)).scalars()
    return {row.lower() for row in rows if row is not None}


def upsert_aliases(eng: Engine, canonical_name: str, aliases: Sequence[str]) -> int:
    """Insert aliases for a canonical artist, returning the number of new rows."""

    canonical = canonical_name.strip()
    if not canonical:
        return 0

    tbl = ensure_alias_table(eng)
    cleaned: dict[str, str] = {}
    for alias in aliases:
        if not alias:
            continue
        trimmed = alias.strip()
        if not trimmed or trimmed.lower() == canonical.lower():
            continue
        key = trimmed.lower()
        cleaned.setdefault(key, trimmed)

    inserted = 0
    with eng.begin() as conn:
        existing = _existing_aliases(conn, tbl, canonical)
        if "" not in existing:
            conn.execute(insert(tbl).values(canonical_name=canonical, alias=""))
            existing.add("")
            inserted += 1
        for key, alias in cleaned.items():
            if key in existing:
                continue
            conn.execute(insert(tbl).values(canonical_name=canonical, alias=alias))
            existing.add(key)
            inserted += 1
    return inserted


def fetch_artists_and_channels(eng: Engine, limit: int | None = None) -> list[tuple[str, int]]:
    """Return aggregated name counts across artists and youtube_videos."""

    inspector = inspect(eng)
    counts: Counter[str] = Counter()
    with eng.connect() as conn:
        if inspector.has_table("artists"):
            for name in conn.execute(text("SELECT artist_name FROM artists")):
                value = (name[0] or "").strip()
                if value:
                    counts[value] += 1
        if inspector.has_table("youtube_videos"):
            for name in conn.execute(text("SELECT channel_title FROM youtube_videos")):
                value = (name[0] or "").strip()
                if value:
                    counts[value] += 1
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0].lower()))
    if limit is not None:
        return ordered[:limit]
    return ordered


def write_aliases_json(eng: Engine, destination: Path | str) -> int:
    """Persist alias→canonical mapping to JSON and return mapping size."""

    path = Path(destination)
    tbl = ensure_alias_table(eng)
    with eng.connect() as conn:
        rows = conn.execute(select(tbl.c.alias, tbl.c.canonical_name).where(tbl.c.alias != "")).all()
    mapping = {alias: canonical for alias, canonical in rows if alias and canonical}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8")
    return len(mapping)


__all__ = [
    "ensure_alias_table",
    "fetch_artists_and_channels",
    "upsert_aliases",
    "write_aliases_json",
]
