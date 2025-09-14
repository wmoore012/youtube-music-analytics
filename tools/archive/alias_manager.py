#!/usr/bin/env python3
"""
Artist Alias Manager — human-friendly CLI

Key features:
- Interactive picker (optional) and non-interactive flags for automation
- Safe plan/preview with --dry-run and --yes to auto-confirm
- Explicit commands: ensure-table, list, add, export
- Atomic JSON writes for durability
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import typer
from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.engine import Engine

# Optional niceties (only used when --interactive)
try:  # pragma: no cover
    from InquirerPy import inquirer  # type: ignore
except Exception:  # pragma: no cover
    inquirer = None  # type: ignore


app = typer.Typer(add_completion=False, help="Manage artist aliases without thinking.")

# Ensure repo root on sys.path, then wire engine from project helper
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from web.etl_helpers import get_engine as _get_engine  # noqa: E402


# --- ENV -----------------------------------------------------------------
def _load_env() -> None:
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)


def get_engine_from_env(schema: Optional[str]) -> Engine:
    _load_env()
    schema_env = (schema or os.getenv("ALIAS_MANAGER_SCHEMA") or os.getenv("DB_SCHEMA") or "PUBLIC").upper()
    # get_engine() requires keyword-only args; pass schema explicitly
    return _get_engine(schema=schema_env)


# --- DB ops (reuse existing semantics) -----------------------------------
def ensure_alias_table(engine: Engine) -> None:
    meta = MetaData()
    meta.reflect(bind=engine)
    table_exists = "artist_aliases" in meta.tables

    if not table_exists:
        artist_aliases = Table(
            "artist_aliases",
            meta,
            Column("alias_id", Integer, primary_key=True, autoincrement=True),
            Column("canonical_name", String(255), nullable=False),
            Column("alias", String(255), nullable=False, server_default=text("''")),
            Column("created_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
            Column("updated_at", DateTime, server_default=text("CURRENT_TIMESTAMP")),
        )
        meta.create_all(engine, tables=[artist_aliases])

    with engine.begin() as conn:
        for ddl in (
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_canonical_alias ON artist_aliases(canonical_name, alias)",
            "CREATE INDEX IF NOT EXISTS idx_alias ON artist_aliases(alias)",
            "CREATE INDEX IF NOT EXISTS idx_canonical ON artist_aliases(canonical_name)",
        ):
            try:
                conn.execute(text(ddl))
            except Exception:
                # SQLite may not support IF NOT EXISTS for indexes on older versions; ignore
                pass


def fetch_candidates(engine: Engine, limit: Optional[int] = None) -> List[str]:
    rows: List[tuple[str, int]] = []
    with engine.connect() as conn:
        # From artists
        try:
            for name, c in conn.execute(
                text("SELECT artist_name, COUNT(*) as c FROM artists GROUP BY artist_name ORDER BY c DESC")
            ):
                rows.append((str(name), int(c)))
        except Exception:
            pass
        # From youtube_videos
        try:
            for name, c in conn.execute(
                text(
                    "SELECT channel_title, COUNT(*) as c FROM youtube_videos "
                    "WHERE channel_title IS NOT NULL GROUP BY channel_title ORDER BY c DESC"
                )
            ):
                rows.append((str(name), int(c)))
        except Exception:
            pass
    agg: Dict[str, int] = {}
    for n, c in rows:
        agg[n] = agg.get(n, 0) + c
    items = sorted(agg, key=lambda k: agg[k], reverse=True)
    return items[: limit or len(items)]


# --- Back-compat shims for older tests ----------------------------------
def fetch_artists_and_channels(engine: Engine, limit: Optional[int] = None) -> List[tuple[str, int]]:
    """Compat: return List[(name, count)] for tests that expect counts."""
    rows: Dict[str, int] = {}
    with engine.connect() as conn:
        for sql in (
            "SELECT artist_name AS n, COUNT(*) AS c FROM artists GROUP BY artist_name",
            "SELECT channel_title AS n, COUNT(*) AS c FROM youtube_videos WHERE channel_title IS NOT NULL GROUP BY channel_title",
        ):
            try:
                for n, c in conn.execute(text(sql)):
                    if n:
                        rows[str(n)] = rows.get(str(n), 0) + int(c)
            except Exception:
                continue
    items = sorted(rows.items(), key=lambda kv: kv[1], reverse=True)
    return items[: (limit or len(items))]


def upsert_aliases(engine: Engine, canonical: str, aliases: List[str]) -> int:
    """Ensure canonical row exists (alias=''), then upsert aliases. SQLite uses ON CONFLICT; MySQL uses ON DUPLICATE KEY."""
    n = 0
    canonical = canonical.strip()
    aliases = [a.strip() for a in aliases if a.strip() and a.strip().lower() != canonical.lower()]
    is_sqlite = engine.dialect.name == "sqlite"
    with engine.begin() as conn:
        insp = sa_inspect(engine)
        cols = {c["name"] for c in insp.get_columns("artist_aliases")}
        if "canonical_name" in cols:  # natural-key schema (preferred)
            if is_sqlite:
                conn.execute(
                    text(
                        """
                    INSERT INTO artist_aliases (canonical_name, alias)
                    VALUES (:c, '')
                    ON CONFLICT(canonical_name, alias) DO NOTHING
                    """
                    ),
                    {"c": canonical},
                )
                for a in aliases:
                    # Count only if actually new
                    exists = conn.execute(
                        text(
                            "SELECT 1 FROM artist_aliases WHERE canonical_name=:c AND lower(alias)=lower(:a) LIMIT 1"
                        ),
                        {"c": canonical, "a": a},
                    ).first()
                    if exists:
                        continue
                    conn.execute(
                        text(
                            """
                        INSERT INTO artist_aliases (canonical_name, alias)
                        VALUES (:c, :a)
                        ON CONFLICT(canonical_name, alias) DO NOTHING
                        """
                        ),
                        {"c": canonical, "a": a},
                    )
                    n += 1
            else:
                conn.execute(
                    text(
                        """
                    INSERT INTO artist_aliases (canonical_name, alias)
                    VALUES (:c, '')
                    ON DUPLICATE KEY UPDATE canonical_name=VALUES(canonical_name)
                    """
                    ),
                    {"c": canonical},
                )
                for a in aliases:
                    exists = conn.execute(
                        text(
                            "SELECT 1 FROM artist_aliases WHERE canonical_name=:c AND lower(alias)=lower(:a) LIMIT 1"
                        ),
                        {"c": canonical, "a": a},
                    ).first()
                    if exists:
                        continue
                    conn.execute(
                        text(
                            """
                        INSERT INTO artist_aliases (canonical_name, alias)
                        VALUES (:c, :a)
                        ON DUPLICATE KEY UPDATE canonical_name=VALUES(canonical_name)
                        """
                        ),
                        {"c": canonical, "a": a},
                    )
                    n += 1
        else:  # legacy schema fallback (artist_id)
            # Ensure artists row exists
            r = conn.execute(text("SELECT artist_id FROM artists WHERE artist_name = :n LIMIT 1"), {"n": canonical}).first()
            if r:
                artist_id = int(r[0])
            else:
                res = conn.execute(text("INSERT INTO artists (artist_name) VALUES (:n)"), {"n": canonical})
                artist_id = int(getattr(res, "lastrowid", 0) or 0)
            if is_sqlite:
                for a in aliases:
                    exists = conn.execute(
                        text("SELECT 1 FROM artist_aliases WHERE artist_id=:id AND lower(alias)=lower(:a) LIMIT 1"),
                        {"id": artist_id, "a": a},
                    ).first()
                    if exists:
                        continue
                    conn.execute(
                        text(
                            """
                        INSERT INTO artist_aliases (artist_id, alias)
                        VALUES (:id, :a)
                        ON CONFLICT(artist_id, alias) DO NOTHING
                        """
                        ),
                        {"id": artist_id, "a": a},
                    )
                    n += 1
            else:
                for a in aliases:
                    exists = conn.execute(
                        text("SELECT 1 FROM artist_aliases WHERE artist_id=:id AND lower(alias)=lower(:a) LIMIT 1"),
                        {"id": artist_id, "a": a},
                    ).first()
                    if exists:
                        continue
                    conn.execute(
                        text(
                            """
                        INSERT INTO artist_aliases (artist_id, alias)
                        VALUES (:id, :a)
                        ON DUPLICATE KEY UPDATE alias=VALUES(alias)
                        """
                        ),
                        {"id": artist_id, "a": a},
                    )
                    n += 1
    return n


def export_mapping(engine: Engine, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with engine.connect() as conn:
        insp = sa_inspect(engine)
        cols = {c["name"] for c in insp.get_columns("artist_aliases")}
        if "canonical_name" in cols:
            res = conn.execute(text("SELECT alias, canonical_name FROM artist_aliases WHERE alias <> '' ORDER BY alias"))
            mapping = {row.alias: row.canonical_name for row in res}
        else:
            res = conn.execute(
                text(
                    """
                SELECT aa.alias, a.artist_name
                FROM artist_aliases aa
                JOIN artists a ON a.artist_id = aa.artist_id
                ORDER BY aa.alias
                """
                )
            )
            mapping = {row.alias: row.artist_name for row in res}

    # Atomic + durable write
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, out_path)
    try:  # best-effort directory fsync
        dir_fd = os.open(str(out_path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        pass
    return len(mapping)


# Back-compat alias for tests
def write_aliases_json(engine: Engine, out_path: Path) -> int:
    return export_mapping(engine, out_path)


# --- Commands -------------------------------------------------------------
@app.command("ensure-table")
def cmd_ensure_table(schema: Optional[str] = typer.Option(None, help="DB schema PUBLIC|PRIVATE")):
    """Create the artist_aliases table if missing."""
    eng = get_engine_from_env(schema)
    ensure_alias_table(eng)
    typer.echo("✔ ensured artist_aliases exists")


@app.command("list")
def cmd_list(
    limit: int = typer.Option(100, help="Max names to show"),
    only_new: bool = typer.Option(False, help="Hide names already marked canonical"),
    schema: Optional[str] = typer.Option(None, help="DB schema PUBLIC|PRIVATE"),
):
    """List candidate artist/channel names from your DB."""
    eng = get_engine_from_env(schema)
    items = fetch_candidates(eng, limit=limit)
    if only_new:
        with eng.connect() as conn:
            canon = {
                r[0]
                for r in conn.execute(text("SELECT DISTINCT canonical_name FROM artist_aliases WHERE alias=''"))
            }
        items = [n for n in items if n not in canon]
    for i, name in enumerate(items, 1):
        typer.echo(f"{i:>3}. {name}")


@app.command("add")
def cmd_add(
    canonical: str = typer.Option(..., "--canonical", "-c", help="Canonical artist name"),
    alias: List[str] = typer.Option([], "--alias", "-a", help="Alias (repeatable)"),
    interactive: bool = typer.Option(False, "--interactive", "-i", help="Pick from a menu"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show plan; do not write"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Apply without confirmation"),
    schema: Optional[str] = typer.Option(None, help="DB schema PUBLIC|PRIVATE"),
):
    """Add aliases mapping to a canonical artist."""
    eng = get_engine_from_env(schema)
    ensure_alias_table(eng)
    aliases = list(alias)

    if interactive:
        if inquirer is None:
            typer.secho("Interactive mode requires InquirerPy. Try: pip install InquirerPy", fg=typer.colors.RED)
            raise typer.Exit(code=2)
        pool = fetch_candidates(eng, limit=300)
        choices = [n for n in pool if n.lower() != canonical.lower()]
        picked = inquirer.checkbox(
            message=f"Select aliases for '{canonical}' (space=toggle, enter=confirm):",
            choices=choices,
        ).execute()
        aliases += picked

    # de-dupe + present plan
    aliases = sorted({a.strip() for a in aliases if a.strip() and a.lower() != canonical.lower()})
    if not aliases:
        typer.echo("No aliases to add.")
        raise typer.Exit(code=2)

    typer.echo("\nPlan:")
    typer.echo(f"  Canonical: {canonical}")
    for a in aliases:
        typer.echo(f"  {a}  →  {canonical}")

    if dry_run:
        typer.echo("\n(dry-run) nothing written.")
        raise typer.Exit(code=0)

    if not yes:
        if not typer.confirm("\nApply these changes?", default=False):
            typer.echo("Canceled.")
            raise typer.Exit(code=2)

    n = upsert_aliases(eng, canonical, aliases)
    typer.echo(f"✔ upserted {n} alias rows")


@app.command("export")
def cmd_export(
    output: Path = typer.Option(Path("config/artist_aliases.json"), "--output", "-o", help="Output JSON path"),
    schema: Optional[str] = typer.Option(None, help="DB schema PUBLIC|PRIVATE"),
):
    """Write alias→canonical JSON mapping."""
    eng = get_engine_from_env(schema)
    total = export_mapping(eng, output)
    typer.echo(f"✔ wrote {total} entries → {output}")


if __name__ == "__main__":
    app()
