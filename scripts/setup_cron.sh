#!/bin/bash
"""
Production Cron Job Setup Script

This script sets up professional cron jobs for the YouTube Analytics ETL pipeline.
Run this on your production server to enable automated data processing.

Usage:
    chmod +x scripts/setup_cron.sh
    ./scripts/setup_cron.sh
"""

set -e  # Exit on any error

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRON_USER="${CRON_USER:-$(whoami)}"
LOG_DIR="${PROJECT_ROOT}/logs"
VENV_PATH="${PROJECT_ROOT}/.venv"

echo "🚀 Setting up production cron jobs for YouTube Analytics ETL"
echo "=" * 60
echo "Project root: $PROJECT_ROOT"
echo "Cron user: $CRON_USER"
echo "Log directory: $LOG_DIR"

# Create log directory
mkdir -p "$LOG_DIR"

# Create virtual environment activation script
cat > "${PROJECT_ROOT}/scripts/activate_and_run.sh" << 'EOF'
#!/bin/bash
# Activation wrapper for cron jobs

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Change to project directory
cd "$PROJECT_ROOT"

# Run the command passed as arguments
exec "$@"
EOF

chmod +x "${PROJECT_ROOT}/scripts/activate_and_run.sh"

# Create cron job entries
CRON_JOBS=$(cat << EOF
# YouTube Analytics ETL Pipeline - Professional Production Schedule
# Generated on $(date) by setup_cron.sh

# Set environment variables for all cron jobs
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin
MAILTO=${CRON_EMAIL:-admin@yourdomain.com}

# Data Migration Check - Runs before ETL at 1:30 AM
30 1 * * * ${PROJECT_ROOT}/scripts/activate_and_run.sh python ${PROJECT_ROOT}/tools/migration/migrate_artist_data.py --check-only >> ${LOG_DIR}/migration_check.log 2>&1

# Main ETL Pipeline - Runs nightly at 2 AM (after migration check)
0 2 * * * ${PROJECT_ROOT}/scripts/activate_and_run.sh python ${PROJECT_ROOT}/tools/etl/run_production_pipeline.py >> ${LOG_DIR}/nightly_pipeline.log 2>&1

# YouTube TOS Compliance - Runs daily at 4 AM (after ETL, before quality checks)
0 4 * * * ${PROJECT_ROOT}/scripts/activate_and_run.sh python ${PROJECT_ROOT}/tools/maintenance/youtube_tos_compliance.py --execute >> ${LOG_DIR}/tos_compliance.log 2>&1

# Data Quality Monitoring - Runs every 6 hours
0 */6 * * * ${PROJECT_ROOT}/scripts/activate_and_run.sh python ${PROJECT_ROOT}/scripts/run_data_quality_checks.py >> ${LOG_DIR}/quality_monitoring.log 2>&1

# Weekly Deep Quality Analysis - Runs Sundays at 3 AM
0 3 * * 0 ${PROJECT_ROOT}/scripts/activate_and_run.sh python ${PROJECT_ROOT}/scripts/run_data_quality_checks.py --fail-on-duplicates >> ${LOG_DIR}/weekly_quality.log 2>&1

# Monthly Database Cleanup - Runs first day of month at 4 AM
0 4 1 * * ${PROJECT_ROOT}/scripts/activate_and_run.sh python ${PROJECT_ROOT}/tools/cleanup_old_artists.py >> ${LOG_DIR}/monthly_cleanup.log 2>&1

# Log Rotation - Runs daily at 1 AM (before main pipeline)
0 1 * * * find ${LOG_DIR} -name "*.log" -type f -mtime +30 -delete

EOF
)

# Backup existing crontab
echo "📋 Backing up existing crontab..."
crontab -l > "${PROJECT_ROOT}/crontab_backup_$(date +%Y%m%d_%H%M%S).txt" 2>/dev/null || echo "No existing crontab found"

# Install new cron jobs
echo "⚙️  Installing cron jobs..."
echo "$CRON_JOBS" | crontab -

echo "✅ Cron jobs installed successfully!"
echo ""
echo "📋 Installed cron jobs:"
crontab -l | grep -v "^#" | grep -v "^$"

echo ""
echo "📁 Log files will be created in: $LOG_DIR"
echo "   • nightly_pipeline.log - Main ETL pipeline logs"
echo "   • quality_monitoring.log - 6-hourly quality checks"
echo "   • weekly_quality.log - Weekly deep quality analysis"
echo "   • monthly_cleanup.log - Monthly database cleanup"

echo ""
echo "🔧 Configuration:"
echo "   • Main pipeline runs at 2 AM daily"
echo "   • Quality monitoring every 6 hours"
echo "   • Weekly deep analysis on Sundays at 3 AM"
echo "   • Monthly cleanup on 1st of month at 4 AM"
echo "   • Log rotation keeps 30 days of logs"

echo ""
echo "📧 Email notifications:"
echo "   • Set CRON_EMAIL environment variable for notifications"
echo "   • Current MAILTO: ${CRON_EMAIL:-admin@yourdomain.com}"

echo ""
echo "🎯 Next steps:"
echo "   1. Test the pipeline: python tools/etl/run_production_pipeline.py --dry-run"
echo "   2. Monitor logs: tail -f $LOG_DIR/nightly_pipeline.log"
echo "   3. Check cron status: crontab -l"
echo "   4. Remove cron jobs: crontab -r (if needed)"

echo ""
echo "🚀 Production ETL pipeline is now scheduled and ready!"
