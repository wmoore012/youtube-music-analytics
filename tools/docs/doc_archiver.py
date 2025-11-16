#!/usr / bin / env python3
"""Automatic Markdown Documentation Archiver

Inspired by the `NotebookArchiver` system. This tool categorizes and time-based
archives non-core Markdown files to keep the root repo clean while preserving
history (important due to shallow git history / churn).

Key Features:
  * Deterministic categorization via pattern heuristics (configurable)
  * Time-based archival (default: > retention_days old)
  * Immediate archival of obviously generated / ephemeral or zero-byte files
  * Safe dry-run mode by default (no changes until --apply)
  * Index generation (docs / archive / README.md) summarizing archived docs
  * Idempotent: re-running won’t duplicate moves; skips already archived paths
  * Configurable via `docs / doc_archive_config.json`

Usage:
  Dry run (recommended first):
    python3 tools / docs / doc_archiver.py

  Apply changes (perform moves + index generation):
    python3 tools / docs / doc_archiver.py --apply

  Regenerate index only:
    python3 tools / docs / doc_archiver.py --regen-index

Exit codes:
  0 success, 1 failure.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "docs" / "doc_archive_config.json"


@dataclass
class DocRecord:
    path: Path
    rel_path: str
    size: int
    mtime: datetime
    category: str
    should_archive: bool
    reason: str


class DocArchiver:
    def __init__(self, config_path: Path = DEFAULT_CONFIG_PATH):
        self.config_path = config_path
        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)

        self.core_docs = {self._norm(p) for p in self.config.get("core_docs", [])}
        self.retention_days = int(self.config.get("retention_days", 7))
        self.archive_root = REPO_ROOT / self.config.get("archive_root", "docs / archive")
        self.keep_recent = int(self.config.get("keep_recent_count_per_category", 50))
        self.category_patterns: Dict[str, List[str]] = self.config.get("category_patterns", {})
        self.now = datetime.now()

        self.archive_root.mkdir(parents=True, exist_ok=True)

    def _norm(self, rel: str) -> str:
        return str(Path(rel)).replace("\\", "/").lstrip("./")

    def _relative(self, path: Path) -> str:
        return self._norm(str(path.relative_to(REPO_ROOT)))

    def discover_markdown_files(self) -> List[Path]:
        files: List[Path] = []
        for p in REPO_ROOT.rglob("*.md"):
            # Skip already archived content
            if "docs / archive" in str(p.as_posix()):
                continue
            # Skip virtual env & vendored & dot-directories
            if any(seg.startswith(".") and seg not in {".kiro"} for seg in p.relative_to(REPO_ROOT).parts):
                # allow .kiro specs; skip others like .venv, .git
                if ".kiro" not in str(p):
                    continue
            files.append(p)
        return files

    def categorize(self, rel_path: str, size: int) -> str:
        low = rel_path.lower()
        if rel_path in self.core_docs:
            return "core"
        # pattern mapping
        for cat, patterns in self.category_patterns.items():
            for pat in patterns:
                if pat.lower() in low:
                    return cat
        # fallback heuristics
        if size == 0:
            return "generated"
        return "uncategorized"

    def should_archive(self, category: str, size: int, mtime: datetime) -> Tuple[bool, str]:
        age_days = (self.now-mtime).days
        if category == "core":
            return False, "core doc"
        if size == 0:
            return True, "zero-byte"
        if category in {"generated"}:
            return True, f"category={category}"
        if category in {"reports", "specs", "tasks", "experiments", "notebooks"} and age_days > self.retention_days:
            return True, f"age>{self.retention_days}d"
        return False, f"age={age_days}d"

    def build_records(self) -> List[DocRecord]:
        records: List[DocRecord] = []
        for path in self.discover_markdown_files():
            rel = self._relative(path)
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            size = stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime)
            category = self.categorize(rel, size)
            archive_flag, reason = self.should_archive(category, size, mtime)
            records.append(
                DocRecord(
                    path=path,
                    rel_path=rel,
                    size=size,
                    mtime=mtime,
                    category=category,
                    should_archive=archive_flag,
                    reason=reason,
                )
            )
        return records

    def archive_record(self, rec: DocRecord, dry_run: bool = True) -> Optional[Path]:
        if not rec.should_archive:
            return None
        # Date folder based on original mtime for historical fidelity
        date_folder = rec.mtime.strftime("%Y % m%d")
        category_folder = rec.category
        archive_dir = self.archive_root / date_folder / category_folder
        archive_dir.mkdir(parents=True, exist_ok=True)
        target = archive_dir / Path(rec.rel_path).name
        if target.exists():
            # Already archived once; skip
            return target
        if dry_run:
            return target
        shutil.move(str(rec.path), str(target))
        return target

    def generate_index(self, records: List[DocRecord]) -> str:
        # Scan archive for existing entries
        rows = []
        for md in self.archive_root.rglob("*.md"):
            rel = self._relative(md)
            stat = md.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime)
            # derive category / date from path parts after archive root
            parts = md.relative_to(self.archive_root).parts
            date_part = parts[0] if parts else ""
            category = parts[1] if len(parts) > 1 else "uncategorized"
            rows.append((rel, category, date_part, stat.st_size, mtime.strftime("%Y-%m-%d")))

        header = "# Documentation Archive Index\n\n"
        header += "Generated by doc_archiver. Do not edit manually.\n\n"
        header += "| File | Category | Date Folder | Size (bytes) | Last Modified |\n"
        header += "|------|----------|-------------|--------------|---------------|\n"
        body = "".join(
            f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} |\n" for r in sorted(rows, key=lambda x: (x[2], x[1], x[0]))
        )
        summary = f"\n\nTotal Archived Files: {len(rows)}\n"
        return header + body + summary

    def write_index(self, content: str, dry_run: bool = True) -> Optional[Path]:
        index_path = self.archive_root / "README.md"
        if dry_run:
            return index_path
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(content)
        return index_path

    def run(self, apply: bool = False, regen_index: bool = False) -> int:
        dry_run = not apply
        records = self.build_records()
        to_archive = [r for r in records if r.should_archive]

        print(f"📄 Discovered markdown files: {len(records)}")
        print(f"📦 Candidates for archival: {len(to_archive)} (retention_days={self.retention_days})")
        print("")
        if not regen_index:
            for rec in sorted(to_archive, key=lambda r: (r.category, r.mtime)):
                action = "MOVE" if apply else "DRY-RUN"
                print(
                    f"{action}: {rec.rel_path} -> docs / archive/{rec.mtime.strftime('%Y % m%d')}/{rec.category}/ (reason={rec.reason})"  # noqa: E501
                )
                self.archive_record(rec, dry_run=dry_run)
        else:
            print("🔄 Regenerating index only (no moves)")

        index_content = self.generate_index(records)
        idx_path = self.write_index(index_content, dry_run=dry_run)
        print(f"🗂️ Archive index {'(dry-run)' if dry_run else 'written'}: {idx_path}")

        if dry_run:
            print("💡 Dry run complete. Re-run with --apply to perform archival.")
        return 0


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Automatic documentation archiver")
    p.add_argument("--config", type=str, default=str(DEFAULT_CONFIG_PATH), help="Path to config JSON")
    p.add_argument("--apply", action="store_true", help="Perform moves (default dry-run)")
    p.add_argument("--regen-index", action="store_true", help="Only regenerate index (no moves)")
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    archiver = DocArchiver(Path(args.config))
    return archiver.run(apply=args.apply, regen_index=args.regen_index)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
