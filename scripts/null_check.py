#!/usr / bin / env python3
"""
Fast, comprehensive data quality scanner for YouTube ETL database.

Features:
- Scans ALL columns (nullable and NOT NULL) and reports DB - level NULL counts.
- Optional checks for blank strings ("", whitespace - only) on text columns.
- Optional checks for empty JSON values (empty object / array) on JSON columns.
- Filter by table list and choose output format (table, csv, json).

Usage examples:
  - python3 scripts / null_check.py
  - python3 scripts / null_check.py --check - blanks --check - empty - json
  - python3 scripts / null_check.py --tables youtube_videos,artist_aliases --format json
"""

import argparse
import json
import logging
import shutil
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from sqlalchemy import text

from web.db_guard import get_engine

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


TEXT_TYPES = {"varchar", "char", "text", "tinytext", "mediumtext", "longtext"}
JSON_TYPES = {"json"}


def fetch_columns(engine, table_name: str) -> List[Tuple[str, str, str]]:
    """Return list of (name, is_nullable, data_type) for a table."""
    query = text(
        """
        SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name})
        return [(row[0], row[1], row[2]) for row in result]


def count_where(engine, table: str, where_sql: str) -> int:
    """Generic counter with WHERE predicate."""
    query = text(f"SELECT COUNT(*) FROM `{table}` WHERE {where_sql}")
    with engine.connect() as conn:
        return int(conn.execute(query).scalar() or 0)


def scan_table(
    engine,
    table: str,
    check_blanks: bool,
    check_empty_json: bool,
) -> List[Dict[str, object]]:
    """Scan a single table and return per - column metrics."""
    cols = fetch_columns(engine, table)
    logger.info(
        "Table %s: %d total columns, %d nullable",
        table,
        len(cols),
        sum(1 for _, nullable, _ in cols if nullable == "YES"),
    )

    report: List[Dict[str, object]] = []
    for name, _, data_type in cols:
        # Always compute DB - level NULLs (even on NOT NULL columns -> typically 0)
        null_count = count_where(engine, table, f"`{name}` IS NULL")

        entry: Dict[str, object] = {
            "table": table,
            "column": name,
            "data_type": data_type,
            "null_count": null_count,
        }

        # Optional: blanks on text columns
        if check_blanks and data_type.lower() in TEXT_TYPES:
            blank_count = count_where(engine, table, f"TRIM(COALESCE(`{name}`,'')) = ''")
            entry["blank_count"] = blank_count

        # Optional: empty JSON
        if check_empty_json and data_type.lower() in JSON_TYPES:
            empty_obj = count_where(
                engine,
                table,
                f"JSON_TYPE(`{name}`) = 'OBJECT' AND JSON_LENGTH(`{name}`) = 0",
            )
            empty_arr = count_where(
                engine,
                table,
                f"JSON_TYPE(`{name}`) = 'ARRAY' AND JSON_LENGTH(`{name}`) = 0",
            )
            entry["empty_json_count"] = empty_obj + empty_arr

        report.append(entry)

    return report


def _truncate(val: str, width: int) -> str:
    s = str(val)
    if len(s) <= width:
        return s
    if width <= 1:
        return s[:width]
    return s[: max(0, width - 1)] + "…"


def format_table(report: List[Dict[str, object]]) -> str:  # noqa: C901
    """Pretty table format sorted by null_count desc, then blanks / empty JSON."""
    # Determine optional columns present
    has_blanks = any("blank_count" in r for r in report)
    has_empty_json = any("empty_json_count" in r for r in report)

    header_cols = ["Table", "Column", "Type", "Nulls"]
    if has_blanks:
        header_cols.append("Blanks")
    if has_empty_json:
        header_cols.append("EmptyJSON")

    lines = []
    lines.append("\n=== DATA QUALITY REPORT ===")
    lines.append("All columns with counts; sorted by Nulls desc (then Blanks / EmptyJSON)")

    # Sort
    def sort_key(r: Dict[str, object]):
        return (
            int(r.get("null_count", 0)) * -1,
            int(r.get("blank_count", 0)) * -1,
            int(r.get("empty_json_count", 0)) * -1,
        )

    report_sorted = sorted(report, key=sort_key)

    # Desired column widths (pre - fit)
    col_widths = {
        "Table": max(5, min(24, max(len(str(r["table"])) for r in report_sorted))),
        "Column": max(6, min(30, max(len(str(r["column"])) for r in report_sorted))),
        "Type": max(4, min(12, max(len(str(r["data_type"])) for r in report_sorted))),
        "Nulls": 7,
        "Blanks": 7,
        "EmptyJSON": 9,
    }

    # Fit to terminal width by shrinking Table / Column / Type as needed
    term_width = shutil.get_terminal_size((120, 20)).columns

    def current_header_len(include_blanks: bool, include_empty: bool) -> int:
        base = col_widths["Table"] + 2 + col_widths["Column"] + 2 + col_widths["Type"] + 2 + col_widths["Nulls"]
        if include_blanks:
            base += 2 + col_widths["Blanks"]
        if include_empty:
            base += 2 + col_widths["EmptyJSON"]
        return base

    need_shrink = current_header_len(has_blanks, has_empty_json) > term_width
    while need_shrink:
        # Shrink Column first, then Table, then Type
        if col_widths["Column"] > 12:
            col_widths["Column"] -= 1
        elif col_widths["Table"] > 12:
            col_widths["Table"] -= 1
        elif col_widths["Type"] > 6:
            col_widths["Type"] -= 1
        else:
            break
        need_shrink = current_header_len(has_blanks, has_empty_json) > term_width

    # Header
    header = (
        f"{header_cols[0]:<{col_widths['Table']}}  "
        f"{header_cols[1]:<{col_widths['Column']}}  "
        f"{header_cols[2]:<{col_widths['Type']}}  "
        f"{header_cols[3]:>{col_widths['Nulls']}}"
    )
    if has_blanks:
        header += f"  {'Blanks':>{col_widths['Blanks']}}"
    if has_empty_json:
        header += f"  {'EmptyJSON':>{col_widths['EmptyJSON']}}"
    sep = "-" * min(term_width, len(header))
    lines.append(sep)
    lines.append(header)
    lines.append(sep)

    # Rows
    for r in report_sorted:
        row = (
            f"{_truncate(r['table'], col_widths['Table']):<{col_widths['Table']}}  "
            f"{_truncate(r['column'], col_widths['Column']):<{col_widths['Column']}}  "
            f"{_truncate(r['data_type'], col_widths['Type']):<{col_widths['Type']}}  "
            f"{int(r.get('null_count', 0)):>{col_widths['Nulls']}}"
        )
        if has_blanks:
            row += f"  {int(r.get('blank_count', 0)):>{col_widths['Blanks']}}"
        if has_empty_json:
            row += f"  {int(r.get('empty_json_count', 0)):>{col_widths['EmptyJSON']}}"
        lines.append(row)

    # Totals
    total_nulls = sum(int(r.get("null_count", 0)) for r in report)
    total_blanks = sum(int(r.get("blank_count", 0)) for r in report)
    total_empty_json = sum(int(r.get("empty_json_count", 0)) for r in report)
    lines.append(sep)
    tail = f"Totals -> Nulls: {total_nulls}"
    if has_blanks:
        tail += f", Blanks: {total_blanks}"
    if has_empty_json:
        tail += f", EmptyJSON: {total_empty_json}"
    lines.append(tail)
    lines.append("=== END REPORT ===\n")
    return "\n".join(lines)


def format_csv(report: List[Dict[str, object]]) -> str:
    keys = ["table", "column", "data_type", "null_count", "blank_count", "empty_json_count"]
    lines = [",".join(keys)]
    for r in report:
        lines.append(",".join(str(r.get(k, "")) for k in keys))
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Comprehensive data quality scanner")
    parser.add_argument(
        "--tables",
        help="Comma - separated table list to scan. Default scans core YouTube + project tables.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress INFO logs for a cleaner report",
    )
    parser.add_argument(
        "--check - blanks",
        action="store_true",
        help="Also count blank strings on text columns ('' or whitespace - only)",
    )
    parser.add_argument(
        "--check - empty - json",
        action="store_true",
        help="Also count empty JSON values ({} or []) on JSON columns",
    )
    parser.add_argument(
        "--format",
        choices=["table", "csv", "json"],
        default="table",
        help="Output format",
    )

    args = parser.parse_args()

    if args.quiet:
        logger.setLevel(logging.WARNING)
    logger.info("Starting data quality scan...")
    engine = get_engine()

    default_tables = [
        "artist_aliases",
        "artist_performance_summary",
        "comment_bot_analysis",
        "comment_sentiment",
        "comment_sentiment_backup",
        "isrc_artists",
        "isrc_recordings",
        "music_videos_normalized",
        "operational_health_log",
        "project_benchmark_models",
        "project_benchmarks",
        "songs",
        "video_recording_link",
        "youtube_comments",
        "youtube_etl_runs",
        "youtube_metrics",
        "youtube_playlists_raw",
        "youtube_sentiment",
        "youtube_sentiment_by_video",
        "youtube_sentiment_summary",
        "youtube_videos",
        "youtube_videos_raw",
    ]

    tables: List[str] = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else default_tables

    full_report: List[Dict[str, object]] = []
    for table in tables:
        logger.info("Scanning table: %s", table)
        full_report.extend(
            scan_table(engine, table, check_blanks=args.check_blanks, check_empty_json=args.check_empty_json)
        )

    if args.format == "table":
        print(format_table(full_report))
    elif args.format == "csv":
        print(format_csv(full_report))
    else:
        print(json.dumps(full_report, indent=2))

    logger.info("Scan complete.")


if __name__ == "__main__":
    main()
