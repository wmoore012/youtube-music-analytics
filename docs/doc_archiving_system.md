# Automatic Documentation Archiving System

This repository now includes an automated archival system for Markdown documentation, inspired by the notebook archiver workflow.

## Goals

1. Keep the project root clean and focused for onboarding.
2. Preserve historical context (important given shallow / rewritten git history).
3. Avoid deleting potentially valuable exploratory or report-style docs.
4. Provide a deterministic, repeatable process (runs locally or in CI).

## How It Works

The script `tools/docs/doc_archiver.py` scans all `*.md` files (excluding already archived ones) and categorizes them via pattern heuristics defined in `docs/doc_archive_config.json`.

It decides to archive a file if:
- It is not a core document AND
  - It is older than the retention window (default 7 days) AND belongs to a report/spec/task/experiment/notebook category, OR
  - It is zero-byte / obviously generated / ephemeral.

Archived files are relocated to:

```
docs/archive/YYYYMMDD/<category>/<original_filename>.md
```

The date folder uses the file's original modification time (historical fidelity).

An index file `docs/archive/README.md` is (re)generated each run with a table of archived files.

## Categories (Heuristics)

Configured via `doc_archive_config.json`:

- core (never archived unless manually removed) — explicit allow list
- specs — design/blueprint/spec/requirements
- reports — report/summary/benchmark/evaluation/quality/validation
- tasks — task/execution/runbook/workflow
- experiments — demo/experiment/prototype/benchmark_
- notebooks — NOTEBOOK_* and notebook related generated docs
- generated — ci_report, coverage, data_quality, success, zero-byte
- uncategorized — anything else (kept in place unless manually curated)

## Running

Dry run (safe):

```
python3 tools/docs/doc_archiver.py
```

Apply (perform moves and write index):

```
python3 tools/docs/doc_archiver.py --apply
```

Regenerate index only:

```
python3 tools/docs/doc_archiver.py --regen-index
```

## CI Integration (Optional)

Add a lightweight CI step:

1. Run dry mode — fail if more than N candidates (signals cleanup needed) OR
2. In a maintenance branch / scheduled workflow run with `--apply`.

## Configuration

Edit `docs/doc_archive_config.json` to adjust:
- `core_docs`
- `retention_days`
- `category_patterns`
- `keep_recent_count_per_category` (reserved for future prune logic)

## Future Enhancements

- Prune logic for extremely old archives beyond a long-tail retention window.
- Optional YAML provenance metadata per archived document.
- Git hook / pre-commit advisory if adding large non-core docs.

## Philosophy

Archive > Delete. The system keeps the working surface minimal while ensuring no knowledge is lost.
