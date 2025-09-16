# Documentation Overview

The documentation set focuses on operating the YouTube analytics ETL stack. Start with the guides below depending on the task at hand.

## Quick links
- [Getting started](getting-started.md): install dependencies, configure environment variables, and run the first ETL job.
- [Architecture](architecture.md): data flow, storage model, and how the services interact.
- [ETL pipeline](etl-pipeline.md): command entry points, scheduling expectations, and refresh cadence.
- [Docker setup](docker_setup_instructions.md): bootstrap a local MySQL instance used by the pipelines.
- [Artist color configuration](ARTIST_COLORS.md): configure chart styling for presentations.
- [Tech debt workflow](TECH_DEBT.md): conventions for tracking and resolving code quality work.

## Additional references
- Project level automation is orchestrated through the scripts in `tools/` and `scripts/`; see inline module docstrings for command line options.
- Executed notebooks live in `notebooks/` and are exercised through `make test-notebook-execution`.
- Data artefacts exported by the ETL live under `music_analysis_tables/` and `time_series_tracking/`.

Keep documentation updates in lock-step with behavioural changes so that CLI tools, notebooks, and tests always point to the same sources of truth.
