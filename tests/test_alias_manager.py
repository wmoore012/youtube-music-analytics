from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text

# Import functions under test
from tools.alias_manager import (
    ensure_alias_table,
    fetch_artists_and_channels,
    upsert_aliases,
    write_aliases_json,
)
from src.icatalogviz.data import _build_artist_alias_map


@pytest.fixture()
def sqlite_engine(tmp_path: Path):
    # Use a file-backed SQLite DB so multiple connections see the same schema
    db_path = tmp_path / "test_aliases.sqlite"
    eng = create_engine(f"sqlite:///{db_path}")
    # Minimal schema for tests (artists + youtube_videos are optional; tool does not require artists)
    meta = MetaData()
    Table(
        "artists",
        meta,
        Column("artist_id", Integer, primary_key=True, autoincrement=True),
        Column("artist_name", String(255), nullable=False, unique=True),
    )
    Table(
        "youtube_videos",
        meta,
        Column("video_id", String(32), primary_key=False),
        Column("channel_title", String(255), nullable=True),
    )
    meta.create_all(eng)
    # Seed some names
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO artists(artist_name) VALUES (:n)"), {"n": "Enchanting"})
        conn.execute(
            text(
                "INSERT INTO youtube_videos(video_id, channel_title) VALUES (:id, :c1), (:id2, :c2), (:id3, :c3)"
            ),
            {"id": "v1", "c1": "LuvEnchantingINC", "id2": "v2", "c2": "@hicorook", "id3": "v3", "c3": None},
        )
    return eng


def test_ensure_alias_table_creates(sqlite_engine):
    # Should create artist_aliases if missing, idempotently
    tbl = ensure_alias_table(sqlite_engine)
    assert tbl is not None
    # Re-run should not raise
    tbl2 = ensure_alias_table(sqlite_engine)
    assert tbl2 is not None


def test_upsert_and_dump_json(sqlite_engine, tmp_path: Path):
    # Create alias table
    ensure_alias_table(sqlite_engine)
    # Upsert a couple of aliases for Enchanting and ensure canonical row exists
    n = upsert_aliases(sqlite_engine, "Enchanting", ["LuvEnchantingINC", "enchanting"])  # case-insensitive allowed
    assert n >= 1
    # Verify rows exist
    with sqlite_engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM artist_aliases")).scalar_one()
        assert total >= 2  # canonical + at least one alias
        canonical_rows = conn.execute(
            text("SELECT COUNT(*) FROM artist_aliases WHERE canonical_name = 'Enchanting' AND alias = ''")
        ).scalar_one()
        assert canonical_rows == 1
    # Dump JSON and verify content
    out = tmp_path / "aliases.json"
    total = write_aliases_json(sqlite_engine, out)
    mapping = json.loads(out.read_text())
    assert total == len(mapping) >= 1
    assert mapping.get("LuvEnchantingINC") == "Enchanting"


def test_build_alias_map_merges_db_and_env(sqlite_engine, monkeypatch):
    # Prepare alias table with one mapping
    ensure_alias_table(sqlite_engine)
    upsert_aliases(sqlite_engine, "Enchanting", ["LuvEnchantingINC"])  # mapping

    # No monkeypatch needed with natural-key schema

    # Overlay ENV mapping
    monkeypatch.setenv(
        "ARTIST_ALIASES_JSON",
        json.dumps({"luvenchantinginc": "Enchanting", "@hicorook": "corook"}),
    )

    # Build mapping
    mapping = _build_artist_alias_map(sqlite_engine)
    # Should include DB mapping and env overrides with lowercase keys too
    assert mapping["LuvEnchantingINC"] == "Enchanting"
    assert mapping["luvenchantinginc"] == "Enchanting"
    assert mapping["@hicorook"] == "corook"
