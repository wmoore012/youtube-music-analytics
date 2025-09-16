# ETL pipeline

This document summarises the operational characteristics of the pipelines that load, transform, and publish YouTube analytics data.

## Components
- **Ingestion** – `web/youtube_channel_etl.py` drives the end-to-end workflow: discover uploads, fetch latest metrics, upsert comment threads, and snapshot results.
- **Helpers** – `web/etl_helpers.py`, `web/spotify_extract.py`, and `web/tidal_extract.py` provide transformation utilities shared by the CLI entry points.
- **Entry points** – orchestrators in `tools/etl/` combine ingestion with validation and downstream analytics.

## Pipeline entry points
| Command | Description |
| --- | --- |
| `python tools/etl/run_focused_etl.py` | Minimal pipeline for local development. Refreshes configured channels, updates metrics, runs lightweight validation, and exports canonical CSVs. |
| `python tools/etl/run_comprehensive_etl.py` | Production pipeline. Adds sentiment refresh, benchmark updates, operational health logging, and performance summaries. |
| `python tools/etl/run_production_pipeline.py` | Multi-stage orchestrator for scheduled deployments. Executes ingestion, notebook validation, and reporting with progress logging. |
| `python web/sentiment_job.py` | Stand-alone comment sentiment scoring job. Useful when sentiment updates should run out-of-band. |

## Scheduling guidelines
- Pipelines read channel configuration from environment variables prefixed with `YT_` (handles or IDs).
- Set `ETL_MAX_WORKERS` to control concurrency when refreshing multiple channels.
- Logs are written under `logs/` with timestamps for audit.
- Use `scripts/automation_manager.py` to generate cron templates; it never applies a schedule without explicit confirmation.

## Data products
- **Relational tables** – defined in `tools/setup/create_tables.py` (`youtube_videos`, `youtube_metrics`, `youtube_comments`, raw mirrors, and supporting lookup tables).
- **Analytics exports** – generated under `music_analysis_tables/` and `time_series_tracking/` for notebooks and dashboards.
- **Health reports** – `system_health_dashboard.json` and outputs from `scripts/benchmark_progress.py` provide high level metrics about freshness and coverage.

## Cache and rate limiting
- DSP fetchers cache playlist JSON on disk (`cache/spotify_playlists/`, `cache/tidal_playlists/`) with daily expiry to limit API usage.
- YouTube calls respect per-channel throttling; set `YOUTUBE_API_SLEEP_SECONDS` if you need to slow down requests.
- Force a refresh by deleting cached files or setting `SPOTIFY_FORCE_FRESH=1` / `TIDAL_FORCE_FRESH=1` for the relevant run.

## Validation
- Each pipeline step collects metrics and raises on failure; there are no silent fallbacks.
- Notebook execution is part of CI via `make test-notebook-execution`.
- Run `make ci-local` before committing to exercise linting, typing, and unit tests.

Keep the information above aligned with any behavioural change: update command examples and configuration guidance whenever new stages are added or defaults change.
