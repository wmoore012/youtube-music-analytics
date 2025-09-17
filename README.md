# YouTube Analytics & Music Industry Intelligence Platform

This repository hosts an end-to-end pipeline that ingests YouTube channel data, stores it in MySQL, and produces music-industry focused analytics. The project emphasises reproducibility: ETL scripts, notebooks, and monitoring utilities all rely on the same modules so that KPIs match across dashboards, tests, and automated reports.

## Quick start
1. Install prerequisites (Python 3.10+, MySQL 8.0+, YouTube Data API key).
2. Clone the repository and install dependencies:
   ```bash
   git clone https://github.com/wmoore012/staging_yt_analytics.git
   cd staging_yt_analytics
   python -m venv .venv
   source .venv/bin/activate
   pip install -U pip
   pip install -r requirements.txt
   pip install -e .
   ```
3. Configure the environment:
   ```bash
   cp .env.example .env
   # populate YouTube API key, database credentials, and artist channels
   ```
4. Provision the database schema:
   ```bash
   python tools/setup/create_tables.py
   ```
5. Execute a pipeline run:
   ```bash
   python tools/etl/run_focused_etl.py            # local smoke test
   # or
   python tools/etl/run_comprehensive_etl.py      # full analytics + benchmarks
   ```
6. Validate results with the standard checks:
   ```bash
   make ci-local
   make test-notebook-execution
   python scripts/benchmark_progress.py
   ```

See [docs/getting-started.md](docs/getting-started.md) for a more detailed walkthrough, including Docker-based database setup.

## Project layout
| Path | Purpose |
| --- | --- |
| `src/youtubeviz/` | Analytics package used by ETL scripts and notebooks (data loading, sentiment, visualisations). |
| `web/` | Extraction and transformation helpers for YouTube and DSP sources, plus database utilities. |
| `tools/etl/` | Command-line entry points for focused, comprehensive, and production pipelines. |
| `tools/setup/` | Schema creation and environment bootstrapping utilities. |
| `scripts/` | Operational tooling (CI, automation management, benchmarks, monitoring). |
| `notebooks/` | Executed notebooks validated in CI to keep story-driven outputs in sync with the warehouse. |
| `music_analysis_tables/` & `time_series_tracking/` | Exported analytics artefacts produced by the ETL. |
| `docs/` | Authoritative documentation for setup, architecture, and operations. |

## Key workflows
### Songs with ISRC (fast path)
If your goal is to quickly see songs with ISRCs (treating `songs` as the ISRC fact table), you can load a small CSV and refresh the normalized view without a full ETL:

1) Prepare a CSV with headers `isrc,title,artist`. A starter template is at `data/songs_template.csv`.

2) Load into the `songs` table:
```bash
make load-songs FILE=data/songs_template.csv
```

3) Normalize and check ISRC coverage:
```bash
make refresh-normalized
```

The normalization step will also consult `video_recording_link` and an optional override mapping at `config/video_isrc_overrides.json` for additional ISRC attribution. To add a few precise links without ETL, put `{ "<video_id>": "<ISRC>" }` pairs in that JSON and re-run the normalization.

### ETL
- **Focused pipeline** – `python tools/etl/run_focused_etl.py`
- **Comprehensive pipeline** – `python tools/etl/run_comprehensive_etl.py`
- **Production orchestrator** – `python tools/etl/run_production_pipeline.py`
- **Sentiment refresh** – `python web/sentiment_job.py`

All pipelines honour environment configuration (YouTube channels, concurrency, cache controls) and write logs under `logs/`.

### Notebook execution
- Use `make test-notebook-execution` to run the notebook validation suite before committing changes.
- Storytelling notebooks read CSV exports from `music_analysis_tables/`; run at least the focused pipeline before executing notebooks to ensure inputs exist.

### Monitoring & reporting
- `scripts/enhanced_ci.py` aggregates linting, notebook execution, duplicate detection, and data-quality checks.
- `scripts/system_health_monitor.py` summarises database freshness and coverage.
- `scripts/benchmark_progress.py` records throughput benchmarks and stores results in `project_benchmarks`.

## Quality gates
- `make ci-local` – wraps formatting, linting, mypy, unit tests, and security scanning.
- `make test-notebook-execution` – executes notebook smoke tests used in CI.
- `python scripts/comprehensive_artist_validation.py` – deep validation of expected artists and exported KPIs.

Run these commands before publishing changes or promoting builds. The repository intentionally fails fast when expected environment variables or tables are missing.

## Documentation
The documentation set is curated under [docs/](docs/README.md):
- [Getting started](docs/getting-started.md)
- [Architecture overview](docs/architecture.md)
- [ETL pipeline reference](docs/etl-pipeline.md)
- [Docker setup](docs/docker_setup_instructions.md)
- [Artist color configuration](docs/ARTIST_COLORS.md)
- [Tech debt workflow](docs/TECH_DEBT.md)

Update the relevant document whenever behaviour, commands, or data outputs change so that scripts, notebooks, and automation stay aligned.

## Contributing
- Use feature branches and keep diffs focused.
- Follow the linting/type-checking guidance in the `Makefile`.
- Add or update tests when changing behaviour.
- Run the quality gates listed above before opening a pull request.

## Support and feedback
Create an issue if you encounter defects or need clarifications. For operational incidents (e.g. database unavailability) include log snippets and the command that was executed.
