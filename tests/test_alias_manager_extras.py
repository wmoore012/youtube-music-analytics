from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import text

from tools.alias_manager import (
    ensure_alias_table,
    fetch_artists_and_channels,
    upsert_aliases,
    write_aliases_json,
)


@pytest.fixture()
def sqlite_engine(tmp_path: Path):
    # Create a file-backed SQLite DB so all connections share state
    from sqlalchemy import create_engine

    db_path = tmp_path / "test_aliases.db"
    eng = create_engine(f"sqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS artists (
                artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_name TEXT NOT NULL
            );
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS youtube_videos (
                video_id TEXT PRIMARY KEY,
                channel_title TEXT
            );
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS artist_aliases (
                alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_name TEXT NOT NULL,
                alias TEXT NOT NULL DEFAULT ''
            );
            """
        ))
        # seed some rows
        conn.execute(text("INSERT INTO artists(artist_name) VALUES (:n1), (:n2)"), {"n1": "Enchanting", "n2": "@hicorook"})
        conn.execute(text(
            "INSERT INTO youtube_videos(video_id, channel_title) VALUES (:id1, :c1), (:id2, :c2), (:id3, :c3)"
        ), {"id1": "v1", "c1": "LuvEnchantingINC", "id2": "v2", "c2": "Enchanting", "id3": "v3", "c3": "@hicorook"})
    return eng


def test_fetch_artists_and_channels_dedup_and_limit(sqlite_engine):
    """
    Ensures:
      - names are aggregated across `artists` and `youtube_videos`
      - duplicates are summed correctly
      - limit truncates the list deterministically (by count desc)
    """
    with sqlite_engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO youtube_videos(video_id, channel_title) "
                "VALUES (:id4, :c), (:id5, :c), (:id6, :c)"
            ),
            {"id4": "v4", "id5": "v5", "id6": "v6", "c": "LuvEnchantingINC"},
        )

    items = fetch_artists_and_channels(sqlite_engine)
    names = [n for n, _ in items]

    assert "Enchanting" in names          # from artists table
    assert "LuvEnchantingINC" in names    # from youtube_videos (now with count>1)
    assert "@hicorook" in names

    luv_count = dict(items).get("LuvEnchantingINC")
    assert luv_count is not None and luv_count >= 4

    limited = fetch_artists_and_channels(sqlite_engine, limit=2)
    assert len(limited) == 2
    assert limited[0][1] >= limited[1][1]


def test_upsert_aliases_idempotent_and_skip_canonical(sqlite_engine):
    """
    Ensures:
      - canonical row (alias='') is created once
      - aliases are case-insensitive de-duped
      - canonical-as-alias is ignored
    """
    ensure_alias_table(sqlite_engine)

    n1 = upsert_aliases(sqlite_engine, "Enchanting", ["LuvEnchantingINC", "luvenchantinginc", "Enchanting"])
    assert n1 >= 1

    n2 = upsert_aliases(sqlite_engine, "Enchanting", ["luvENCHANTINGinc"])
    assert n2 == 0

    with sqlite_engine.connect() as conn:
        canon = conn.execute(
            text("SELECT COUNT(*) FROM artist_aliases WHERE canonical_name='Enchanting' AND alias=''")
        ).scalar_one()
        assert canon == 1

        aliases = conn.execute(
            text("SELECT COUNT(*) FROM artist_aliases WHERE canonical_name='Enchanting' AND alias='LuvEnchantingINC'")
        ).scalar_one()
        assert aliases == 1


def test_write_aliases_json_overwrite_valid(sqlite_engine, tmp_path: Path):
    """
    Ensures:
      - write_aliases_json produces valid JSON
      - it overwrites an existing file cleanly (even if the old file was garbage)
    """
    ensure_alias_table(sqlite_engine)
    upsert_aliases(sqlite_engine, "Enchanting", ["LuvEnchantingINC"])

    out = tmp_path / "artist_aliases.json"
    out.write_text("{not: json", encoding="utf-8")

    total = write_aliases_json(sqlite_engine, out)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert total == len(data) >= 1
    assert data["LuvEnchantingINC"] == "Enchanting"
