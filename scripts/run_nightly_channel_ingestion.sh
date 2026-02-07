#!/bin/zsh
set -euo pipefail

# ======================================================================
# IMPORTANT - DO NOT REGRESS (USER-REQUIRED BEHAVIOR)
# ----------------------------------------------------------------------
# This nightly job is intentionally scoped to channel ingestion so the
# project always gets fresh youtube_metrics/youtube_etl_runs entries.
# Keep a hard timeout to prevent cron hangs.
# ======================================================================

PROJECT_ROOT="/Users/jsmash/PycharmProjects/YoutubeETL And Analyis"
LOG_FILE="${PROJECT_ROOT}/logs/nightly_channel_ingestion.log"
TIMEOUT_SECONDS="${ETL_INGEST_TIMEOUT_SECONDS:-600}"

mkdir -p "${PROJECT_ROOT}/logs"
cd "${PROJECT_ROOT}"

{
  echo "==== $(date '+%Y-%m-%d %H:%M:%S') nightly channel ingestion start ===="
  PYTHONPATH=. /opt/homebrew/bin/gtimeout "${TIMEOUT_SECONDS}" .venv/bin/python tools/core/run_channels_from_env.py
  echo "==== $(date '+%Y-%m-%d %H:%M:%S') nightly channel ingestion success ===="
} >> "${LOG_FILE}" 2>&1
