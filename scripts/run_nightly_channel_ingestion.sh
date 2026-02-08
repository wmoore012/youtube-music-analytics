#!/usr/bin/env bash
set -euo pipefail

# ======================================================================
# IMPORTANT - DO NOT REGRESS (USER-REQUIRED BEHAVIOR)
# ----------------------------------------------------------------------
# This nightly job is intentionally scoped to channel ingestion so the
# project always gets fresh youtube_metrics/youtube_etl_runs entries.
# It also forces a demo cohort refresh so Streamlit Demo mode can reflect
# truly fresh daily snapshots even when no brand-new videos were added.
# Keep a hard timeout to prevent cron hangs.
# ======================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_FILE="${PROJECT_ROOT}/logs/nightly_channel_ingestion.log"
TIMEOUT_SECONDS="${ETL_INGEST_TIMEOUT_SECONDS:-600}"
PYTHON_EXEC="${PYTHON_EXEC:-.venv/bin/python}"

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
  PYTHONPATH=. "${TIMEOUT_CMD}" "${TIMEOUT_SECONDS}" "${PYTHON_EXEC}" \
    tools/core/run_channels_from_env.py --refresh-demo-snapshot
  echo "==== $(date '+%Y-%m-%d %H:%M:%S') nightly channel ingestion success ===="
} >> "${LOG_FILE}" 2>&1
