# Getting started

This guide walks through running the project on a new workstation. The commands are designed for macOS, Linux, or WSL. Adapt paths as needed for your environment.

## 1. Prerequisites
- Python 3.10 or newer
- MySQL 8.0+ (local instance or network accessible)
- YouTube Data API v3 key with quota for video and channel endpoints
- `pip`, `virtualenv`, and optionally `make`

Optional but recommended:
- Docker Desktop if you plan to use the helper container defined in `docs/docker_setup_instructions.md`
- A dedicated Python virtual environment

## 2. Clone and install dependencies
```bash
git clone https://github.com/wmoore012/staging_yt_analytics.git
cd staging_yt_analytics
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -e .
```

## 3. Configure environment variables
Copy the example file and provide credentials:
```bash
cp .env.example .env
```
Populate the following keys at minimum:
- `YOUTUBE_API_KEY`
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`
- Channel configuration such as `YT_<ARTIST>_YT` pointing to channel handles or IDs

Validation scripts also read `config/expected_artists.json`; update this file if you add or remove tracked artists.

## 4. Initialise the database schema
Ensure the target MySQL instance is reachable, then create tables:
```bash
python tools/setup/create_tables.py
```
If you prefer Docker, follow the dedicated instructions in [docker_setup_instructions.md](docker_setup_instructions.md).

## 5. Run the ETL pipeline
For a local smoke test run the focused pipeline:
```bash
python tools/etl/run_focused_etl.py
```
To execute the full refresh, including performance metrics and sentiment scoring:
```bash
python tools/etl/run_comprehensive_etl.py
```
Both commands respect environment variables for channel selection and logging configuration.

## 6. Validate outputs
Quality gates provide fast feedback:
```bash
make ci-local          # formatting + lint + unit tests
make test-notebook-execution
python scripts/benchmark_progress.py  # optional KPI snapshot
```
Review the generated CSVs under `music_analysis_tables/` and the health metrics in `system_health_dashboard.json` to confirm data freshness.

## 7. Next steps
- Inspect notebooks in `notebooks/` for storytelling outputs.
- Review architecture notes in [architecture.md](architecture.md).
- Set up automation via `scripts/automation_manager.py` when promoting to staging or production.
