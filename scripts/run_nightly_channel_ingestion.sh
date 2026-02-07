#!/bin/zsh
set -euo pipefail

# ======================================================================
# IMPORTANT - DO NOT REGRESS (USER-REQUIRED BEHAVIOR)
# ----------------------------------------------------------------------
# This nightly job is intentionally scoped to channel ingestion so the
# project always gets fresh youtube_metrics/youtube_etl_runs entries.
# Keep a hard timeout to prevent cron hangs.
# ======================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_FILE="${PROJECT_ROOT}/logs/nightly_channel_ingestion.log"
TIMEOUT_SECONDS="${ETL_INGEST_TIMEOUT_SECONDS:-600}"

if [[ -x "/opt/homebrew/bin/gtimeout" ]]; then
  TIMEOUT_CMD="/opt/homebrew/bin/gtimeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_CMD="$(command -v gtimeout)"
elif command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="$(command -v timeout)"
else
  echo "ERROR: no timeout command found (gtimeout/timeout required)." >&2
  exit 1
fi

mkdir -p "${PROJECT_ROOT}/logs"
cd "${PROJECT_ROOT}"

{
  echo "==== $(date '+%Y-%m-%d %H:%M:%S') nightly channel ingestion start ===="
  PYTHONPATH=. "${TIMEOUT_CMD}" "${TIMEOUT_SECONDS}" .venv/bin/python tools/core/run_channels_from_env.py
  echo "==== $(date '+%Y-%m-%d %H:%M:%S') nightly channel ingestion success ===="
} >> "${LOG_FILE}" 2>&1
