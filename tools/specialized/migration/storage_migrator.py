#!/usr / bin / env python3
"""
🔄 Unified Storage Migrator Tool

Consolidated migration tool that provides comprehensive data migration capabilities:
- Database - to - file migrations with multiple formats (CSV, JSON, Parquet)
- File - to - database migrations with validation and mapping
- Migration validation and rollback capabilities
- Backup and recovery operations
- Storage optimization and cleanup

Usage:
    python tools / specialized / migration / storage_migrator.py --db - to - file --table youtube_videos --format csv
    python tools / specialized / migration / storage_migrator.py --file - to - db --source data / csv --type csv
    python tools / specialized / migration / storage_migrator.py --validate --migration - id 12345
    python tools / specialized / migration / storage_migrator.py --rollback --migration - id 12345
"""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import (
    ConfigurationError,
    ExecutionError,
    ToolBase,
    ToolConfig,
    ValidationError,
    register_tool,
)


class StorageMigrator(ToolBase):
    """
    Unified storage migration tool for database - to - file and file - to - database operations.

    This tool provides comprehensive migration capabilities:
    - Database - to - file migrations (CSV, JSON, Parquet formats)
    - File - to - database migrations with validation
    - Migration validation and integrity checking
    - Rollback and recovery operations
    - Backup creation and management
    - Storage optimization and cleanup
    """

    def __init__(self):
        super().__init__(name="storage - migrator", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        # Migration tracking
        self.migration_session_id = f"migration_{datetime.now().strftime('%Y % m%d_ % H%M % S')}"
        self.migration_log = {
            "session_id": self.migration_session_id,
            "start_time": datetime.now().isoformat(),
            "migrations_performed": [],
            "backups_created": [],
            "validations_run": [],
        }

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return ["DB_HOST", "DB_USER", "DB_NAME"]

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="storage - migrator",
            version="1.0.0",
            description="Unified storage migration tool for database and file operations",
            dependencies=[
                "python>=3.8",
                "pymysql",
                "sqlalchemy",
                "pandas",
                "pyarrow",  # For Parquet support
            ],
            environment_vars=[
                "DB_HOST",
                "DB_USER",
                "DB_NAME",
            ],
            usage_examples=[
                "python tools / specialized / migration / storage_migrator.py --db - to - file --table youtube_videos",
                "python tools / specialized / migration / storage_migrator.py --file - to - db --source data / csv",
                "python tools / specialized / migration / storage_migrator.py --validate --migration - id 12345",
            ],
            category="specialized",
        )

    def run(self) -> None:
        """Main execution method - should not be called directly, use specific migration methods."""
        self.log_progress("Use specific migration methods like migrate_db_to_file() or migrate_file_to_db()")

    def migrate_db_to_file(
        self,
        table_name: str,
        output_format: str = "csv",
        output_dir: str = "data / exports",
        where_clause: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Migrate database table to file format.

        Args:
            table_name: Name of the database table to export
            output_format: Output format (csv, json, parquet)
            output_dir: Directory to save exported files
            where_clause: Optional SQL WHERE clause for filtering
            limit: Optional limit on number of records

        Returns:
            Dictionary with migration results
        """
        self.log_progress(f"🔄 Migrating table '{table_name}' to {output_format.upper()} format")

        try:
            import pandas as pd
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            migration_id = str(uuid.uuid4())

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "db_to_file",
                "migration_id": migration_id,
                "table_name": table_name,
                "output_format": output_format,
                "output_dir": output_dir,
                "records_exported": 0,
                "file_path": None,
                "file_size_bytes": 0,
            }

            # Build SQL query
            sql_query = f"SELECT * FROM {table_name}"
            if where_clause:
                sql_query += f" WHERE {where_clause}"
            if limit:
                sql_query += f" LIMIT {limit}"

            # Execute query and load data
            self.log_progress(f"Executing query: {sql_query}")
            df = pd.read_sql(text(sql_query), engine)

            if df.empty:
                self.log_progress("⚠️ No data found to export", level="WARNING")
                results["records_exported"] = 0
                return results

            # Create output directory
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
            filename = f"{table_name}_{timestamp}.{output_format}"
            file_path = output_path / filename

            # Export data based on format
            if output_format.lower() == "csv":
                df.to_csv(file_path, index=False)
            elif output_format.lower() == "json":
                df.to_json(file_path, orient="records", indent=2)
            elif output_format.lower() == "parquet":
                df.to_parquet(file_path, index=False)
            else:
                raise ValidationError(f"Unsupported output format: {output_format}")

            # Get file size
            file_size = file_path.stat().st_size

            results.update(
                {
                    "records_exported": len(df),
                    "file_path": str(file_path),
                    "file_size_bytes": file_size,
                    "columns_exported": list(df.columns),
                }
            )

            # Log migration
            self.migration_log["migrations_performed"].append(
                {
                    "migration_id": migration_id,
                    "type": "db_to_file",
                    "timestamp": datetime.now().isoformat(),
                    "details": results,
                }
            )

            self.log_progress(f"✅ Exported {len(df):,} records to {file_path} ({file_size:,} bytes)")

            return results

        except Exception as e:
            self.handle_error(e, "database to file migration")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "db_to_file",
                "status": "ERROR",
                "error": str(e),
            }

    def migrate_file_to_db(
        self,
        source_path: str,
        table_name: str,
        file_format: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        create_backup: bool = True,
        validate_after: bool = True,
    ) -> Dict[str, Any]:
        """
        Migrate file data to database table.

        Args:
            source_path: Path to source file or directory
            table_name: Target database table name
            file_format: File format (csv, json, parquet) - auto - detected if None
            column_mapping: Optional mapping of file columns to database columns
            create_backup: Whether to create backup before migration
            validate_after: Whether to validate migration after completion

        Returns:
            Dictionary with migration results
        """
        self.log_progress(f"🔄 Migrating file '{source_path}' to table '{table_name}'")

        try:
            import pandas as pd
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            migration_id = str(uuid.uuid4())
            source_file = Path(source_path)

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "file_to_db",
                "migration_id": migration_id,
                "source_path": source_path,
                "table_name": table_name,
                "records_imported": 0,
                "backup_created": False,
                "validation_passed": False,
            }

            # Validate source file exists
            if not source_file.exists():
                raise ValidationError(f"Source file does not exist: {source_path}")

            # Auto - detect file format if not specified
            if file_format is None:
                file_format = source_file.suffix.lower().lstrip(".")

            # Create backup if requested
            if create_backup:
                backup_result = self._create_table_backup(table_name, migration_id)
                results["backup_created"] = backup_result["success"]
                results["backup_path"] = backup_result.get("backup_path")

            # Load data based on file format
            if file_format == "csv":
                df = pd.read_csv(source_file)
            elif file_format == "json":
                df = pd.read_json(source_file)
            elif file_format == "parquet":
                df = pd.read_parquet(source_file)
            else:
                raise ValidationError(f"Unsupported file format: {file_format}")

            if df.empty:
                self.log_progress("⚠️ No data found in source file", level="WARNING")
                results["records_imported"] = 0
                return results

            # Apply column mapping if provided
            if column_mapping:
                df = df.rename(columns=column_mapping)
                self.log_progress(f"Applied column mapping: {column_mapping}")

            # Import data to database
            _records_imported = df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")

            results["records_imported"] = len(df)
            results["columns_imported"] = list(df.columns)

            # Validate migration if requested
            if validate_after:
                validation_result = self._validate_migration(migration_id, table_name, len(df))
                results["validation_passed"] = validation_result["success"]
                results["validation_details"] = validation_result

            # Log migration
            self.migration_log["migrations_performed"].append(
                {
                    "migration_id": migration_id,
                    "type": "file_to_db",
                    "timestamp": datetime.now().isoformat(),
                    "details": results,
                }
            )

            self.log_progress(f"✅ Imported {len(df):,} records from {source_file} to {table_name}")

            return results

        except Exception as e:
            self.handle_error(e, "file to database migration")
            return {
                "timestamp": datetime.now().isoformat(),
                "operation": "file_to_db",
                "status": "ERROR",
                "error": str(e),
            }

    def validate_migration(self, migration_id: str) -> Dict[str, Any]:
        """
        Validate a completed migration.

        Args:
            migration_id: ID of the migration to validate

        Returns:
            Dictionary with validation results
        """
        self.log_progress(f"🔍 Validating migration {migration_id}")

        try:
            # Find migration in log
            migration_record = None
            for migration in self.migration_log["migrations_performed"]:
                if migration["migration_id"] == migration_id:
                    migration_record = migration
                    break

            if not migration_record:
                raise ValidationError(f"Migration {migration_id} not found in session log")

            results = {
                "timestamp": datetime.now().isoformat(),
                "operation": "validate_migration",
                "migration_id": migration_id,
                "validation_checks": [],
                "overall_success": True,
            }

            migration_details = migration_record["details"]

            # Validation checks based on migration type
            if migration_record["type"] == "db_to_file":
                # Validate file exists and has expected size
                file_path = migration_details.get("file_path")
                if file_path and Path(file_path).exists():
                    actual_size = Path(file_path).stat().st_size
                    expected_size = migration_details.get("file_size_bytes", 0)

                    results["validation_checks"].append(
                        {"check": "file_exists", "status": "PASS", "message": f"Export file exists at {file_path}"}
                    )

                    if actual_size == expected_size:
                        results["validation_checks"].append(
                            {
                                "check": "file_size",
                                "status": "PASS",
                                "message": f"File size matches expected: {actual_size} bytes",
                            }
                        )
                    else:
                        results["validation_checks"].append(
                            {
                                "check": "file_size",
                                "status": "FAIL",
                                "message": f"File size mismatch: expected {expected_size}, got {actual_size}",
                            }
                        )
                        results["overall_success"] = False
                else:
                    results["validation_checks"].append(
                        {"check": "file_exists", "status": "FAIL", "message": f"Export file not found: {file_path}"}
                    )
                    results["overall_success"] = False

            elif migration_record["type"] == "file_to_db":
                # Validate database records
                table_name = migration_details.get("table_name")
                expected_records = migration_details.get("records_imported", 0)

                if table_name:
                    actual_records = self._count_table_records(table_name)

                    results["validation_checks"].append(
                        {"check": "table_exists", "status": "PASS", "message": f"Target table {table_name} exists"}
                    )

                    if actual_records >= expected_records:
                        results["validation_checks"].append(
                            {
                                "check": "record_count",
                                "status": "PASS",
                                "message": f"Table has {actual_records} records (expected at least {expected_records})",
                            }
                        )
                    else:
                        results["validation_checks"].append(
                            {
                                "check": "record_count",
                                "status": "FAIL",
                                "message": f"Record count mismatch: expected {expected
                                                                              _records}"}"}, found {actual_records}",
                        }  # noqa: E999
                        )
                            results["overall_success"] = False

                        # Log validation
                        self.migration_log["validations_run"].append(
                {
                    "migration_id": migration_id,
                    "timestamp": datetime.now().isoformat(),
                    "results": results,
                }
            )

                status = "✅ PASSED" if results["overall_success"] else "❌ FAILED"
                self.log_progress(f"Validation {status} for migration {migration_id}")

                return results

            except Exception as e:
            self.handle_error(e, "migration validation")
            return {
               "timestamp": datetime.now().isoformat(),
                "operation": "validate_migration",
                "migration_id": migration_id,
                "status": "ERROR",
                "error": str(e),
            }

            def rollback_migration(self, migration_id: str) -> Dict[str, Any]:
            """
        Rollback a migration using backup data.

        Args:
            migration_id: ID of the migration to rollback

        Returns:
            Dictionary with rollback results
        """
            self.log_progress(f"🔄 Rolling back migration {migration_id}")

            try:
            # Find migration and backup
            migration_record = None
            for migration in self.migration_log["migrations_performed"]:
            if migration["migration_id"] == migration_id:
            migration_record = migration
                    break

            if not migration_record:
            raise ValidationError(f"Migration {migration_id} not found in session log")

            results = {
               "timestamp": datetime.now().isoformat(),
                "operation": "rollback_migration",
                "migration_id": migration_id,
                "rollback_success": False,
            }

                migration_details = migration_record["details"]

                # Handle rollback based on migration type
                if migration_record["type"] == "file_to_db":
                # For file - to - db, restore from backup
            backup_path = migration_details.get("backup_path")
                table_name = migration_details.get("table_name")

                if backup_path and Path(backup_path).exists() and table_name:
            rollback_result = self._restore_table_from_backup(table_name, backup_path)
                    results["rollback_success"] = rollback_result["success"]
                    results["rollback_details"] = rollback_result
                else:
            raise ExecutionError(f"Backup not found for migration {migration_id}")

                elif migration_record["type"] == "db_to_file":
                # For db - to - file, just delete the exported file
            file_path = migration_details.get("file_path")
                if file_path and Path(file_path).exists():
            Path(file_path).unlink()
                    results["rollback_success"] = True
                    results["rollback_details"] = {"file_deleted": file_path}
                else:
            self.log_progress("⚠️ Export file not found, nothing to rollback", level="WARNING")
                    results["rollback_success"] = True

                status = "✅ SUCCESS" if results["rollback_success"] else "❌ FAILED"
                self.log_progress(f"Rollback {status} for migration {migration_id}")

                return results

            except Exception as e:
            self.handle_error(e, "migration rollback")
            return {
               "timestamp": datetime.now().isoformat(),
                "operation": "rollback_migration",
                "migration_id": migration_id,
                "status": "ERROR",
                "error": str(e),
            }

            def get_migration_status(self) -> Dict[str, Any]:
            """Get current migration session status."""
            return {
            "migration_session": self.migration_session_id,
            "migration_log": self.migration_log.copy(),
            "timestamp": datetime.now().isoformat(),
        }

        # Helper methods for migration operations

        def _create_table_backup(self, table_name: str, migration_id: str) -> Dict[str, Any]:
        """Create backup of table before migration."""
        try:
        from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
            backup_table = f"{table_name}_backup_{timestamp}"
            backup_dir = Path("data / backups")
            backup_dir.mkdir(parents=True, exist_ok=True)

            with engine.begin() as conn:
                # Create backup table
        conn.execute(text(f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}"))

                # Also export to file as additional backup
                backup_file = backup_dir / f"{backup_table}.sql"
                conn.execute(text(f"SELECT * FROM {backup_table} INTO OUTFILE '{backup_file}'"))

            backup_info = {
               "success": True,
                "backup_table": backup_table,
                "backup_path": str(backup_file),
                "migration_id": migration_id,
                "timestamp": datetime.now().isoformat(),
            }

                self.migration_log["backups_created"].append(backup_info)

                return backup_info

            except Exception as e:
            return {
               "success": False,
                "error": str(e),
            }

            def _restore_table_from_backup(self, table_name: str, backup_path: str) -> Dict[str, Any]:
            """Restore table from backup."""
            try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            with engine.begin() as conn:
                # Clear current table
            conn.execute(text(f"DELETE FROM {table_name}"))

                # Restore from backup file
                conn.execute(text(f"LOAD DATA INFILE '{backup_path}' INTO TABLE {table_name}"))

            return {
               "success": True,
                "message": f"Table {table_name} restored from {backup_path}",
            }

            except Exception as e:
            return {
               "success": False,
                "error": str(e),
            }

            def _validate_migration(self, migration_id: str, table_name: str, expected_records: int) -> Dict[str, Any]:
            """Validate migration by checking record counts."""
            try:
            actual_records = self._count_table_records(table_name)

            return {
               "success": actual_records >= expected_records,
                "expected_records": expected_records,
                "actual_records": actual_records,
                "table_name": table_name,
            }

            except Exception as e:
            return {
               "success": False,
                "error": str(e),
            }

    def _count_table_records(self, table_name: str) -> int:
        """Count records in a database table."""
        from sqlalchemy import text

        from web.etl_helpers import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()

    def cleanup_resources(self) -> None:
        """Clean up any resources used during migration."""
        # Log final session summary
        self.migration_log["end_time"] = datetime.now().isoformat()
        self.log_progress(f"Migration session {self.migration_session_id} completed")

  # noqa: C901
def main():
    """Main entry point for the storage migrator tool."""
    parser = argparse.ArgumentParser(
        description = "Unified Storage Migration Tool",
        formatter_class = argparse.RawDescriptionHelpFormatter,
        epilog = """
Examples:
  python tools / specialized / migration / storage_migrator.py --db - to - file --table youtube_videos --format csv
  python tools / specialized / migration / storage_migrator.py --file - to - db --source data / export.csv --table youtube_videos
  python tools / specialized / migration / storage_migrator.py --validate --migration - id abc123
  python tools / specialized / migration / storage_migrator.py --rollback --migration - id abc123
        """,
    )

        # Migration operations
        parser.add_argument("--db - to - file", action="store_true", help="Migrate database table to file")
        parser.add_argument("--file - to - db", action="store_true", help="Migrate file to database table")
        parser.add_argument("--validate", action="store_true", help="Validate a migration")
        parser.add_argument("--rollback", action="store_true", help="Rollback a migration")
        parser.add_argument("--status", action="store_true", help="Show migration session status")

        # Parameters
        parser.add_argument("--table", type=str, help="Database table name")
        parser.add_argument("--source", type=str, help="Source file path for file - to - db migration")
        parser.add_argument(
       "--format", choices = ["csv", "json", "parquet"], default = "csv", help = "File format for db - to - file migration"
    )
        parser.add_argument(
       "--output - dir", type = str, default = "data / exports", help = "Output directory for db - to - file migration"
    )
        parser.add_argument("--migration - id", type=str, help="Migration ID for validation or rollback")
        parser.add_argument("--where", type=str, help="WHERE clause for db - to - file migration")
        parser.add_argument("--limit", type=int, help="Limit number of records for db - to - file migration")
        parser.add_argument("--no - backup", action="store_true", help="Skip backup creation for file - to - db migration")
        parser.add_argument("--no - validate", action="store_true", help="Skip validation after file - to - db migration")

        # Options
        parser.add_argument("--json", action="store_true", help="Output results in JSON format")
        parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

        args = parser.parse_args()

        # Create storage migrator instance
        with StorageMigrator() as migrator:
    try:
    if args.status:
    status = migrator.get_migration_status()
                if args.json:
    print(json.dumps(status, indent=2))
                else:
    print(f"Migration Session: {status['migration_session']}")
                    print(f"Migrations Performed: {len(status['migration_log']['migrations_performed'])}")
                    print(f"Backups Created: {len(status['migration_log']['backups_created'])}")
                return 0
            elif args.db_to_file:
    if not args.table:
    print("❌ --table is required for db - to - file migration")
                    return 1

                result= migrator.migrate_db_to_file(
                   table_name = args.table,
                    output_format = args.format,
                    output_dir = args.output_dir,
                    where_clause = args.where,
                    limit = args.limit,
                )

                    if args.json:
                print(json.dumps(result, indent=2))
                    else:
                if result.get("records_exported", 0) > 0:
                print(f"✅ Exported {result['records_exported']:,} records to {result['file_path']}")
                    else:
                print("⚠️ No records exported")
                    return 0
                elif args.file_to_db:
                if not args.source or not args.table:
                print("❌ --source and --table are required for file - to - db migration")
                    return 1

                result= migrator.migrate_file_to_db(
                   source_path = args.source,
                    table_name = args.table,
                    create_backup = not args.no_backup,
                    validate_after = not args.no_validate,
                )

                    if args.json:
                print(json.dumps(result, indent=2))
                    else:
                if result.get("records_imported", 0) > 0:
                print(f"✅ Imported {result['records_imported']:,} records to {result['table_name']}")
                        if result.get("validation_passed"):
                print("✅ Migration validation passed")
                    else:
                print("⚠️ No records imported")
                    return 0
                elif args.validate:
                if not args.migration_id:
                print("❌ --migration - id is required for validation")
                    return 1

                result= migrator.validate_migration(args.migration_id)

                if args.json:
                print(json.dumps(result, indent=2))
                else:
                status = "✅ PASSED" if result.get("overall_success") else "❌ FAILED"
                    print(f"Validation {status} for migration {args.migration_id}")
                    for check in result.get("validation_checks", []):
                print(f"  {check['status']}: {check['message']}")
                return 0
                elif args.rollback:
                if not args.migration_id:
                print("❌ --migration - id is required for rollback")
                    return 1

                result= migrator.rollback_migration(args.migration_id)

                if args.json:
                print(json.dumps(result, indent=2))
                else:
                status = "✅ SUCCESS" if result.get("rollback_success") else "❌ FAILED"
                    print(f"Rollback {status} for migration {args.migration_id}")
                return 0
                else:
                print("❌ Please specify an operation: --db - to - file, --file - to - db, --validate, --rollback, or --status")
                return 1

                except KeyboardInterrupt:
                migrator.log_progress("Migration cancelled by user")
                return 1
                except Exception as e:
                migrator.handle_error(e, "main execution")
                return 1


                if __name__ == "__main__":
                sys.exit(main())
