# MusicScope Notebooks - Portfolio Exports

This repo standardizes notebook-to-dashboard handoff via CSV exports and manifests. Use this as a quick reference when running portfolio notebooks.

## Notebook layout
- Source notebooks:
  - `notebooks/MusicScope_YouTube_Dashboard.ipynb`
  - `notebooks/MusicScope™_Professional_Dashboard.ipynb`
- Executed outputs: `notebooks/executed/` (latest runs)
- Archived snapshots: `notebooks/archive/` (historical runs)
- Orphaned recovery: `notebooks/archive/orphaned_executed/` (legacy outputs)
- Scripts: `notebooks/scripts/` (check dependencies, create dashboard, integration, smoke tests)
- Notebook helpers: `tools/notebook_helpers/` (design system + section renderers)
- Collected tables: `notebooks/outputs/` (store intermediate exports for frontend wiring)

## Notebook titles (pre-attentive)
- `MusicScope_YouTube_Dashboard.ipynb`: "MusicScope YouTube Signals - Momentum, Resonance, Risk"
- `MusicScope™_Professional_Dashboard.ipynb`: "MusicScope Professional Signals - Momentum, Resonance, Risk, Performance"

## Export root
- Default: `exports/portfolio/`
- Override with env var `MUSICSCOPE_EXPORT_ROOT`
- Structure: `exports/portfolio/<cohort_slug>/<run_id>/`
  - `manifest.json` (run metadata + table inventory)
  - `momentum_insights.csv`
  - `sentiment_insights.csv`
  - `performance_insights.csv`
  - `portfolio_highlights.csv`
  - `latest.json` lives at `exports/portfolio/<cohort_slug>/latest.json`

## Rules for tables
- CSV is the canonical format for Streamlit; Parquet is optional for ad-hoc analysis only.
- Column names stay snake_case; do not drop or simplify columns.
- Booleans are not allowed in exports. Convert flags to categorical strings (e.g., `hidden_gem` | `normal`).

## How to export from notebooks
1. Build the portfolio insight DataFrames (momentum/sentiment/performance/breakout/highlights).
2. Call `export_portfolio_run(cohort_slug, run_id, dfs, meta)` from `portfolio.io`.
3. `run_id` should be a timestamp or UUID (e.g., `2025-09-26T14-30-00Z`).
4. `meta` can include `source_notebook`, `input_data_window`, `impact_summary`, and `git_commit`.

## How Streamlit consumes exports
- Streamlit reads `latest.json` to pick the active run; if missing, it falls back to the most recent run_id directory.
- Tables load from CSV with schema validation; missing tables surface user-facing warnings.
- Freshness and manifest details are displayed in the Portfolio Exports tab.

## Canonical source tables
- Treat `youtube_videos`, `youtube_metrics`, `youtube_comments`, and `youtube_sentiment_summary` as sources of truth.
- Do not wire the dashboard directly to raw `youtube_*_raw` tables.

## Quick checklist before running notebooks
- Confirm CSV inputs exist (`music_analysis_tables/artist_music_summary.csv`, `music_analysis_tables/normalized_music_videos.csv`).
- Confirm the portfolio insight DataFrames include required columns (see contracts in `src/portfolio/contracts.py`).
- Ensure exports directory is writable and env override is set if needed.
