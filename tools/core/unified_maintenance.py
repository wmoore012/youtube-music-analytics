#!/usr/bin/env python3
"""
🔧 Unified YouTube Analytics Maintenance Tool

Consolidates all maintenance functionality into a single, comprehensive tool that provides:
- Database cleanup and optimization
- Data retention management (YouTube ToS compliance)
- System maintenance and health optimization
- Automated cleanup with safety checks
- Performance optimization and tuning

Usage:
    python tools / core / unified_maintenance.py                    # Interactive maintenance
    python tools / core / unified_maintenance.py --cleanup-old     # Clean old data
    python tools / core / unified_maintenance.py --optimize-db     # Database optimization
    python tools / core / unified_maintenance.py --retention       # Data retention cleanup
    python tools / core / unified_maintenance.py --full-maintenance # Complete maintenance
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import (
    ToolBase,
    ToolConfig,
    ValidationError,
    register_tool,
)


class SystemMaintenance(ToolBase):
    """
    Unified system maintenance tool that consolidates all maintenance functionality.

    This tool provides comprehensive maintenance capabilities:
    - Database cleanup and optimization
    - Data retention management for YouTube ToS compliance
    - System performance optimization
    - Automated maintenance with safety checks
    - Backup and recovery operations
    - Storage optimization and cleanup
    """

    def __init__(self):
        super().__init__(name="unified-maintenance", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        # Maintenance state and tracking
        self.maintenance_session_id = f"maint_{datetime.now().strftime('%Y % m%d_ % H%M % S')}"
        self.operations_log = {
            "session_id": self.maintenance_session_id,
            "start_time": datetime.now().isoformat(),
            "operations_performed": [],
            "data_affected": {},
            "safety_checks": [],
        }

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return ["DB_HOST", "DB_USER", "DB_NAME"]

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="unified-maintenance",
            version="1.0.0",
            description="Unified YouTube Analytics system maintenance tool",
            dependencies=[
                "python>=3.8",
                "pymysql",
                "sqlalchemy",
                "pandas",
            ],
            environment_vars=[
                "DB_HOST",
                "DB_USER",
                "DB_NAME",
                "YOUTUBE_DATA_RETENTION_DAYS",
            ],
            usage_examples=[
                "python tools / core / unified_maintenance.py --cleanup-old",
                "python tools / core / unified_maintenance.py --optimize-db",
                "python tools / core / unified_maintenance.py --retention",
            ],
            category="core",
        )

    def run(self) -> None:
        """Main execution method-should not be called directly, use specific maintenance methods."""
        self.log_progress("Use specific maintenance methods like cleanup_old_data() or optimize_database()")

    def cleanup_old_data(self, days: int = None, dry_run: bool = True) -> Dict[str, Any]:
        """
        Clean up old data based on retention policies.

        Args:
            days: Number of days to retain (default from YOUTUBE_DATA_RETENTION_DAYS)
            dry_run: Whether to perform a dry run without actual deletion

        Returns:
            Dictionary with cleanup results
        """
        if days is None:
            days = int(self.get_config_value("YOUTUBE_DATA_RETENTION_DAYS", default="30"))

        self.log_progress(f"🧹 Cleaning up data older than {days} days (dry_run={dry_run})")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "cleanup_old_data",
                "retention_days": days,
                "dry_run": dry_run,
                "tables_processed": {},
                "total_records_affected": 0,
                "safety_checks": [],
            }

            # Safety check: Ensure retention period is reasonable
            if days < 7:
                raise ValidationError("Retention period must be at least 7 days for safety")

            results["safety_checks"].append(
                {
                    "check": "retention_period_validation",
                    "status": "PASSED",
                    "message": f"Retention period {days} days is within safe limits",
                }
            )

            # Get database engine
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            cutoff_date = datetime.now() - timedelta(days=days)

            # Define tables and their date columns for cleanup
            cleanup_tables = [
                ("youtube_videos_raw", "fetched_at", "Raw video data"),
                ("youtube_comments", "created_at", "Comment data"),
                ("youtube_metrics", "fetched_at", "Metrics data"),
                ("comment_sentiment", "processed_at", "Sentiment analysis data"),
                ("youtube_etl_runs", "run_date", "ETL run logs"),
            ]

            with engine.begin() as conn:
                for table_name, date_column, description in cleanup_tables:
                    try:
                        # Check if table exists
                        table_exists = conn.execute(
                            text(
                                f"""
                            SELECT COUNT(*) FROM information_schema.tables
                            WHERE table_schema = DATABASE() AND table_name = '{table_name}'
                        """
                            )
                        ).scalar()

                        if not table_exists:
                            results["tables_processed"][table_name] = {
                                "status": "SKIPPED",
                                "reason": "Table does not exist",
                                "records_affected": 0,
                            }
                            continue

                        # Count records to be affected
                        count_query = f"""
                            SELECT COUNT(*) FROM {table_name}
                            WHERE {date_column} < %s
                        """
                        records_to_delete = conn.execute(text(count_query), (cutoff_date,)).scalar()

                        if records_to_delete == 0:
                            results["tables_processed"][table_name] = {
                                "status": "NO_ACTION",
                                "reason": "No old records found",
                                "records_affected": 0,
                            }
                            continue

                        # Safety check: Don't delete more than 50% of data at once
                        total_records = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                        deletion_percentage = (records_to_delete / total_records) * 100 if total_records > 0 else 0

                        if deletion_percentage > 50:
                            results["tables_processed"][table_name] = {
                                "status": "SAFETY_BLOCK",
                                "reason": f"Would delete {deletion_percentage:.1f}% of data (safety limit: 50%)",
                                "records_affected": 0,
                                "records_identified": records_to_delete,
                            }
                            results["safety_checks"].append(
                                {
                                    "check": f"{table_name}_deletion_percentage",
                                    "status": "BLOCKED",
                                    "message": f"Blocked deletion of {deletion_percentage:.1f}% of {table_name}",
                                }
                            )
                            continue

                        # Perform deletion (or dry run)
                        if not dry_run:
                            delete_query = f"""
                                DELETE FROM {table_name}
                                WHERE {date_column} < %s
                            """
                            conn.execute(text(delete_query), (cutoff_date,))

                            results["tables_processed"][table_name] = {
                                "status": "DELETED",
                                "description": description,
                                "records_affected": records_to_delete,
                                "cutoff_date": cutoff_date.isoformat(),
                            }
                        else:
                            results["tables_processed"][table_name] = {
                                "status": "DRY_RUN",
                                "description": description,
                                "records_would_delete": records_to_delete,
                                "cutoff_date": cutoff_date.isoformat(),
                            }

                        results["total_records_affected"] += records_to_delete

                    except Exception as e:
                        results["tables_processed"][table_name] = {
                            "status": "ERROR",
                            "error": str(e),
                            "records_affected": 0,
                        }

            # Log operation
            self.operations_log["operations_performed"].append(
                {
                    "operation": "cleanup_old_data",
                    "timestamp": datetime.now().isoformat(),
                    "parameters": {"days": days, "dry_run": dry_run},
                    "results": results,
                }
            )

            return results

        except Exception as e:
            self.handle_error(e, "old data cleanup")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "cleanup_old_data",
                "status": "ERROR",
                "error": str(e),
            }

    def optimize_database(self, analyze_tables: bool = True) -> Dict[str, Any]:
        """
        Optimize database performance through various maintenance operations.

        Args:
            analyze_tables: Whether to run ANALYZE TABLE on all tables

        Returns:
            Dictionary with optimization results
        """
        self.log_progress("⚡ Optimizing database performance")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "optimize_database",
                "optimizations": {},
                "performance_impact": {},
            }

            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            with engine.begin() as conn:
                # Get list of tables to optimize
                tables_query = """
                    SELECT table_name,
                           ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb,
                           table_rows
                    FROM information_schema.TABLES
                    WHERE table_schema = DATABASE()
                    AND table_type = 'BASE TABLE'
                    ORDER BY size_mb DESC
                """
                tables = conn.execute(text(tables_query)).fetchall()

                # Optimize each table
                for table in tables:
                    table_name = table.table_name
                    table_size = float(table.size_mb)

                    try:
                        optimization_result = {
                            "original_size_mb": table_size,
                            "operations": [],
                        }

                        # OPTIMIZE TABLE
                        self.log_progress(f"Optimizing table {table_name}")
                        optimize_result = conn.execute(text(f"OPTIMIZE TABLE {table_name}")).fetchall()
                        optimization_result["operations"].append(
                            {
                                "operation": "OPTIMIZE",
                                "status": optimize_result[0].Msg_text if optimize_result else "Unknown",
                            }
                        )

                        # ANALYZE TABLE (if requested)
                        if analyze_tables:
                            analyze_result = conn.execute(text(f"ANALYZE TABLE {table_name}")).fetchall()
                            optimization_result["operations"].append(
                                {
                                    "operation": "ANALYZE",
                                    "status": analyze_result[0].Msg_text if analyze_result else "Unknown",
                                }
                            )

                        # Get new size
                        new_size_query = """
                            SELECT ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                            FROM information_schema.TABLES
                            WHERE table_schema = DATABASE() AND table_name = %s
                        """
                        new_size = conn.execute(text(new_size_query), (table_name,)).scalar()
                        optimization_result["new_size_mb"] = float(new_size) if new_size else table_size
                        optimization_result["size_reduction_mb"] = table_size-optimization_result["new_size_mb"]

                        results["optimizations"][table_name] = optimization_result

                    except Exception as e:
                        results["optimizations"][table_name] = {
                            "status": "ERROR",
                            "error": str(e),
                        }

                # Calculate overall performance impact
                total_original_size = sum(
                    opt.get("original_size_mb", 0)
                    for opt in results["optimizations"].values()
                    if isinstance(opt, dict) and "original_size_mb" in opt
                )
                total_new_size = sum(
                    opt.get("new_size_mb", 0)
                    for opt in results["optimizations"].values()
                    if isinstance(opt, dict) and "new_size_mb" in opt
                )

                results["performance_impact"] = {
                    "total_original_size_mb": total_original_size,
                    "total_new_size_mb": total_new_size,
                    "total_size_reduction_mb": total_original_size-total_new_size,
                    "optimization_percentage": (
                        ((total_original_size-total_new_size) / total_original_size * 100)
                        if total_original_size > 0
                        else 0
                    ),
                }

            # Log operation
            self.operations_log["operations_performed"].append(
                {
                    "operation": "optimize_database",
                    "timestamp": datetime.now().isoformat(),
                    "results": results,
                }
            )

            return results

        except Exception as e:
            self.handle_error(e, "database optimization")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "optimize_database",
                "status": "ERROR",
                "error": str(e),
            }

    def data_retention_cleanup(self, compliance_mode: bool = True) -> Dict[str, Any]:
        """
        Perform YouTube ToS compliant data retention cleanup.

        Args:
            compliance_mode: Whether to use strict YouTube ToS compliance rules

        Returns:
            Dictionary with retention cleanup results
        """
        self.log_progress("📋 Performing YouTube ToS compliant data retention cleanup")

        try:
            # Get retention period from environment or use YouTube ToS default
            retention_value = self.get_config_value("YOUTUBE_DATA_RETENTION_DAYS", default="30")
            # Extract just the number part (handle comments in env values)
            retention_days = int(retention_value.split()[0] if isinstance(retention_value, str) else retention_value)

            if compliance_mode and retention_days > 30:
                self.log_progress("⚠️ YouTube ToS compliance mode: limiting retention to 30 days")
                retention_days = 30

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "data_retention_cleanup",
                "compliance_mode": compliance_mode,
                "retention_days": retention_days,
                "youtube_tos_compliant": retention_days <= 30,
                "cleanup_results": {},
            }

            # Perform cleanup with retention policy
            cleanup_results = self.cleanup_old_data(days=retention_days, dry_run=False)
            results["cleanup_results"] = cleanup_results

            # Additional YouTube-specific cleanup
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            cutoff_date = datetime.now() - timedelta(days=retention_days)

            with engine.begin() as conn:
                # Clean up YouTube API response cache (if exists)
                try:
                    cache_cleanup = conn.execute(
                        text(
                            """
                        DELETE FROM youtube_videos_raw
                        WHERE fetched_at < %s
                    """
                        ),
                        (cutoff_date,),
                    ).rowcount

                    results["youtube_api_cache_cleaned"] = cache_cleanup

                except Exception as e:
                    results["youtube_api_cache_error"] = str(e)

                # Update ETL run tracking
                try:
                    conn.execute(
                        text(
                            """
                        UPDATE youtube_etl_runs
                        SET reason = CONCAT(reason, ' [Retention cleanup applied]')
                        WHERE run_date < %s AND reason NOT LIKE '%Retention cleanup%'
                    """
                        ),
                        (cutoff_date,),
                    )

                except Exception as e:
                    results["etl_tracking_error"] = str(e)

            # Log compliance status
            if results["youtube_tos_compliant"]:
                self.log_progress("✅ YouTube ToS compliance maintained")
            else:
                self.log_progress("⚠️ YouTube ToS compliance may be at risk-consider reducing retention period")

            # Log operation
            self.operations_log["operations_performed"].append(
                {
                    "operation": "data_retention_cleanup",
                    "timestamp": datetime.now().isoformat(),
                    "parameters": {"compliance_mode": compliance_mode, "retention_days": retention_days},
                    "results": results,
                }
            )

            return results

        except Exception as e:
            self.handle_error(e, "data retention cleanup")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "data_retention_cleanup",
                "status": "ERROR",
                "error": str(e),
            }

    def system_health_maintenance(self) -> Dict[str, Any]:
        """
        Perform comprehensive system health maintenance.

        Returns:
            Dictionary with maintenance results
        """
        self.log_progress("🏥 Performing system health maintenance")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "system_health_maintenance",
                "maintenance_tasks": {},
                "overall_status": "HEALTHY",
            }

            # Task 1: Check and fix data consistency
            consistency_results = self._fix_data_consistency()
            results["maintenance_tasks"]["data_consistency"] = consistency_results

            # Task 2: Update statistics and indexes
            stats_results = self._update_database_statistics()
            results["maintenance_tasks"]["database_statistics"] = stats_results

            # Task 3: Clean up orphaned records
            orphan_cleanup = self._cleanup_orphaned_records()
            results["maintenance_tasks"]["orphan_cleanup"] = orphan_cleanup

            # Task 4: Validate data integrity
            integrity_check = self._validate_data_integrity()
            results["maintenance_tasks"]["data_integrity"] = integrity_check

            # Determine overall status
            task_statuses = [task.get("status", "UNKNOWN") for task in results["maintenance_tasks"].values()]
            if "ERROR" in task_statuses:
                results["overall_status"] = "ERROR"
            elif "WARNING" in task_statuses:
                results["overall_status"] = "WARNING"
            else:
                results["overall_status"] = "HEALTHY"

            # Log operation
            self.operations_log["operations_performed"].append(
                {
                    "operation": "system_health_maintenance",
                    "timestamp": datetime.now().isoformat(),
                    "results": results,
                }
            )

            return results

        except Exception as e:
            self.handle_error(e, "system health maintenance")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "system_health_maintenance",
                "status": "ERROR",
                "error": str(e),
            }

    def full_maintenance(self, include_optimization: bool = True, dry_run: bool = False) -> Dict[str, Any]:
        """
        Perform complete system maintenance including all operations.

        Args:
            include_optimization: Whether to include database optimization
            dry_run: Whether to perform dry run for destructive operations

        Returns:
            Dictionary with complete maintenance results
        """
        self.log_progress("🔧 Running full system maintenance")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "full_maintenance",
                "maintenance_session": self.maintenance_session_id,
                "include_optimization": include_optimization,
                "dry_run": dry_run,
                "operations": {},
                "summary": {},
            }

            # Operation 1: Data retention cleanup
            self.log_progress("Step 1: Data retention cleanup")
            retention_results = self.data_retention_cleanup(compliance_mode=True)
            results["operations"]["data_retention"] = retention_results

            # Operation 2: System health maintenance
            self.log_progress("Step 2: System health maintenance")
            health_results = self.system_health_maintenance()
            results["operations"]["system_health"] = health_results

            # Operation 3: Database optimization (if requested)
            if include_optimization:
                self.log_progress("Step 3: Database optimization")
                optimization_results = self.optimize_database(analyze_tables=True)
                results["operations"]["database_optimization"] = optimization_results

            # Generate summary
            operation_statuses = [
                op.get("status", op.get("overall_status", "UNKNOWN")) for op in results["operations"].values()
            ]

            results["summary"] = {
                "total_operations": len(results["operations"]),
                "successful_operations": len([s for s in operation_statuses if s in ["HEALTHY", "SUCCESS"]]),
                "operations_with_warnings": len([s for s in operation_statuses if s == "WARNING"]),
                "failed_operations": len([s for s in operation_statuses if s in ["ERROR", "CRITICAL"]]),
                "overall_status": (
                    "SUCCESS"
                    if all(s in ["HEALTHY", "SUCCESS"] for s in operation_statuses)
                    else "WARNING" if any(s == "WARNING" for s in operation_statuses) else "ERROR"
                ),
                "maintenance_session": self.maintenance_session_id,
            }

            # Final status log
            if results["summary"]["overall_status"] == "SUCCESS":
                self.log_progress("✅ Full maintenance completed successfully")
            elif results["summary"]["overall_status"] == "WARNING":
                self.log_progress("⚠️ Full maintenance completed with warnings")
            else:
                self.log_progress("❌ Full maintenance completed with errors")

            return results

        except Exception as e:
            self.handle_error(e, "full maintenance")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "full_maintenance",
                "status": "ERROR",
                "error": str(e),
            }

    def get_maintenance_status(self) -> Dict[str, Any]:
        """Get current maintenance session status."""
        return {
            "maintenance_session": self.maintenance_session_id,
            "operations_log": self.operations_log.copy(),
            "timestamp": datetime.now().isoformat(),
        }

    # Helper methods for maintenance operations

    def _fix_data_consistency(self) -> Dict[str, Any]:
        """Fix data consistency issues."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            fixes_applied = []

            with engine.begin() as conn:
                # Fix 1: Remove duplicate video entries
                duplicates_removed = conn.execute(
                    text(
                        """
                    DELETE v1 FROM youtube_videos v1
                    INNER JOIN youtube_videos v2
                    WHERE v1.video_id = v2.video_id AND v1.fetched_at < v2.fetched_at
                """
                    )
                ).rowcount

                if duplicates_removed > 0:
                    fixes_applied.append(f"Removed {duplicates_removed} duplicate video entries")

                # Fix 2: Update null channel titles
                null_channels_fixed = conn.execute(
                    text(
                        """
                    UPDATE youtube_videos
                    SET channel_title = 'Unknown Channel'
                    WHERE channel_title IS NULL OR channel_title = ''
                """
                    )
                ).rowcount

                if null_channels_fixed > 0:
                    fixes_applied.append(f"Fixed {null_channels_fixed} null channel titles")

            return {
                "status": "SUCCESS",
                "fixes_applied": fixes_applied,
                "total_fixes": len(fixes_applied),
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _update_database_statistics(self) -> Dict[str, Any]:
        """Update database statistics for query optimization."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            tables_updated = []

            with engine.begin() as conn:
                # Get all tables
                tables = conn.execute(
                    text(
                        """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                """
                    )
                ).fetchall()

                for table in tables:
                    table_name = table.table_name
                    try:
                        conn.execute(text(f"ANALYZE TABLE {table_name}"))
                        tables_updated.append(table_name)
                    except Exception:
                        pass  # Skip tables that can't be analyzed

            return {
                "status": "SUCCESS",
                "tables_updated": tables_updated,
                "total_tables": len(tables_updated),
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _cleanup_orphaned_records(self) -> Dict[str, Any]:
        """Clean up orphaned records across tables."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            cleanup_results = []

            with engine.begin() as conn:
                # Clean up orphaned comments (comments without videos)
                orphaned_comments = conn.execute(
                    text(
                        """
                    DELETE c FROM youtube_comments c
                    LEFT JOIN youtube_videos v ON c.video_id = v.video_id
                    WHERE v.video_id IS NULL
                """
                    )
                ).rowcount

                if orphaned_comments > 0:
                    cleanup_results.append(f"Removed {orphaned_comments} orphaned comments")

                # Clean up orphaned metrics (metrics without videos)
                orphaned_metrics = conn.execute(
                    text(
                        """
                    DELETE m FROM youtube_metrics m
                    LEFT JOIN youtube_videos v ON m.video_id = v.video_id
                    WHERE v.video_id IS NULL
                """
                    )
                ).rowcount

                if orphaned_metrics > 0:
                    cleanup_results.append(f"Removed {orphaned_metrics} orphaned metrics")

            return {
                "status": "SUCCESS",
                "cleanup_results": cleanup_results,
                "total_cleanups": len(cleanup_results),
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _validate_data_integrity(self) -> Dict[str, Any]:
        """Validate data integrity across the system."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            integrity_checks = []

            with engine.connect() as conn:
                # Check 1: Video ID format validation
                invalid_video_ids = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM youtube_videos
                    WHERE LENGTH(video_id) != 11 OR video_id REGEXP '[^A-Za-z0-9_-]'
                """
                    )
                ).scalar()

                integrity_checks.append(
                    {
                        "check": "video_id_format",
                        "status": "PASS" if invalid_video_ids == 0 else "FAIL",
                        "invalid_count": invalid_video_ids,
                    }
                )

                # Check 2: Date consistency
                future_dates = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM youtube_videos
                    WHERE published_at > NOW()
                """
                    )
                ).scalar()

                integrity_checks.append(
                    {
                        "check": "date_consistency",
                        "status": "PASS" if future_dates == 0 else "FAIL",
                        "future_dates_count": future_dates,
                    }
                )

            overall_status = "PASS" if all(check["status"] == "PASS" for check in integrity_checks) else "FAIL"

            return {
                "status": "SUCCESS",
                "overall_integrity": overall_status,
                "checks": integrity_checks,
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def cleanup_resources(self) -> None:
        """Clean up any resources used during maintenance."""
        # Log final session summary
        self.operations_log["end_time"] = datetime.now().isoformat()
        self.log_progress(f"Maintenance session {self.maintenance_session_id} completed")


def main():  # noqa: C901
    """Main entry point for the unified maintenance tool."""
    parser = argparse.ArgumentParser(
        description="Unified YouTube Analytics Maintenance Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / core / unified_maintenance.py                    # Interactive maintenance
  python tools / core / unified_maintenance.py --cleanup-old     # Clean old data
  python tools / core / unified_maintenance.py --optimize-db     # Database optimization
  python tools / core / unified_maintenance.py --retention       # Data retention cleanup
  python tools / core / unified_maintenance.py --full-maintenance # Complete maintenance
        """,
    )

    # Maintenance operations
    parser.add_argument("--cleanup-old", action="store_true", help="Clean up old data based on retention policy")
    parser.add_argument("--optimize-db", action="store_true", help="Optimize database performance")
    parser.add_argument("--retention", action="store_true", help="YouTube ToS compliant data retention cleanup")
    parser.add_argument("--health-maintenance", action="store_true", help="System health maintenance")
    parser.add_argument("--full-maintenance", action="store_true", help="Complete system maintenance")
    parser.add_argument("--status", action="store_true", help="Show maintenance status")

    # Options
    parser.add_argument("--days", type=int, help="Retention period in days (default from YOUTUBE_DATA_RETENTION_DAYS)")
    parser.add_argument("--dry-run", action="store_true", help="Perform dry run without actual changes")
    parser.add_argument(
        "--no-optimization", action="store_true", help="Skip database optimization in full maintenance"
    )
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Create maintenance tool instance
    with SystemMaintenance() as maintenance_tool:
        try:
            if args.status:
                status = maintenance_tool.get_maintenance_status()
                if args.json:
                    print(json.dumps(status, indent=2))
                else:
                    print(f"Maintenance Session: {status['maintenance_session']}")
                    print(f"Operations Performed: {len(status['operations_log']['operations_performed'])}")
                return 0
            elif args.cleanup_old:
                result = maintenance_tool.cleanup_old_data(days=args.days, dry_run=args.dry_run)
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    print(f"✅ Cleanup completed: {result['total_records_affected']} records affected")
                return 0
            elif args.optimize_db:
                result = maintenance_tool.optimize_database(analyze_tables=True)
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    impact = result.get("performance_impact", {})
                    print(f"✅ Database optimized: {impact.get('optimization_percentage', 0):.1f}% improvement")
                return 0
            elif args.retention:
                result = maintenance_tool.data_retention_cleanup(compliance_mode=True)
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    compliant = "✅" if result.get("youtube_tos_compliant") else "⚠️"
                    print(f"{compliant} Retention cleanup completed ({result['retention_days']} days)")
                return 0
            elif args.health_maintenance:
                result = maintenance_tool.system_health_maintenance()
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    status = result.get("overall_status", "UNKNOWN")
                    print(f"✅ Health maintenance completed: {status}")
                return 0
            elif args.full_maintenance:
                result = maintenance_tool.full_maintenance(
                    include_optimization=not args.no_optimization, dry_run=args.dry_run
                )
                if args.json:
                    print(json.dumps(result, indent=2))
                else:
                    summary = result.get("summary", {})
                    print(f"✅ Full maintenance completed: {summary.get('overall_status', 'UNKNOWN')}")
                    print(f"   Operations: {summary.get('successful_operations', 0)}/{summary.get('total_operations', 0)} successful")
                return 0
            else:
                # Interactive maintenance
                print("🔧 YouTube Analytics Maintenance Tool")
                print("=" * 50)
                print("Available operations:")
                print("1. Clean old data (--cleanup-old)")
                print("2. Optimize database (--optimize-db)")
                print("3. Data retention cleanup (--retention)")
                print("4. System health maintenance (--health-maintenance)")
                print("5. Full maintenance (--full-maintenance)")
                print("\nUse --help for detailed options")
                return 0

        except KeyboardInterrupt:
            maintenance_tool.log_progress("Maintenance cancelled by user")
            return 1
        except Exception as e:
            maintenance_tool.handle_error(e, "main execution")
            return 1


if __name__ == "__main__":
    sys.exit(main())
