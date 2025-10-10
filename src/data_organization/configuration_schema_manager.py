"""
Database schema manager for configuration management system.

This module handles the creation and management of database schema
for the configuration management system.
"""

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ConfigurationSchemaManager:
    """Manages database schema for configuration management system."""

    def __init__(self, database_connection):
        """Initialize with database connection."""
        self.database_connection = database_connection

    def create_configuration_schema(self) -> bool:
        """Create the configuration management database schema."""
        try:
            # Read the SQL schema file
            schema_file = Path(__file__).parent / "configuration_schema.sql"

            if not schema_file.exists():
                logger.error(f"Schema file not found: {schema_file}")
                return False

            with open(schema_file, "r") as f:
                schema_sql = f.read()

            # Split into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

            cursor = self.database_connection.cursor()
            try:
                for statement in statements:
                    if statement:
                        cursor.execute(statement)

                self.database_connection.commit()
                logger.info("Configuration schema created successfully")
                return True

            except Exception as e:
                self.database_connection.rollback()
                logger.error(f"Failed to create configuration schema: {e}")
                return False
            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error reading schema file: {e}")
            return False

    def verify_schema_exists(self) -> bool:
        """Verify that the configuration schema exists."""
        required_tables = [
            "scoring_algorithms",
            "scoring_configurations",
            "configuration_audit_log",
            "environment_settings",
        ]

        cursor = self.database_connection.cursor()
        try:
            for table in required_tables:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if not cursor.fetchone():
                    logger.warning(f"Required table '{table}' not found")
                    return False

            logger.info("Configuration schema verification passed")
            return True

        except Exception as e:
            logger.error(f"Schema verification failed: {e}")
            return False
        finally:
            cursor.close()

    def get_schema_version(self) -> Optional[str]:
        """Get the current schema version."""
        cursor = self.database_connection.cursor()
        try:
            # Check if we have a schema version table
            cursor.execute("SHOW TABLES LIKE 'schema_versions'")
            if not cursor.fetchone():
                return None

            cursor.execute(
                """
                SELECT version FROM schema_versions
                WHERE component = 'configuration_management'
                ORDER BY applied_at DESC LIMIT 1
            """
            )

            result = cursor.fetchone()
            return result["version"] if result else None

        except Exception as e:
            logger.error(f"Failed to get schema version: {e}")
            return None
        finally:
            cursor.close()

    def initialize_default_data(self) -> bool:
        """Initialize default configuration data."""
        try:
            cursor = self.database_connection.cursor()

            # Check if we already have data
            cursor.execute("SELECT COUNT(*) as count FROM scoring_algorithms")
            result = cursor.fetchone()

            if result["count"] > 0:
                logger.info("Default data already exists, skipping initialization")
                return True

            # The default data is already included in the schema SQL file
            # This method can be used for additional initialization if needed

            logger.info("Default configuration data initialized")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize default data: {e}")
            return False
        finally:
            cursor.close()

    def drop_configuration_schema(self) -> bool:
        """Drop the configuration management schema (use with caution)."""
        tables_to_drop = [
            "configuration_audit_log",
            "scoring_configurations",
            "environment_settings",
            "scoring_algorithms",
        ]

        views_to_drop = ["active_algorithm_configs", "recent_config_changes", "environment_settings_summary"]

        cursor = self.database_connection.cursor()
        try:
            # Drop views first
            for view in views_to_drop:
                try:
                    cursor.execute(f"DROP VIEW IF EXISTS {view}")
                except Exception as e:
                    logger.warning(f"Failed to drop view {view}: {e}")

            # Drop tables in reverse order to handle foreign keys
            for table in reversed(tables_to_drop):
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                except Exception as e:
                    logger.warning(f"Failed to drop table {table}: {e}")

            self.database_connection.commit()
            logger.info("Configuration schema dropped successfully")
            return True

        except Exception as e:
            self.database_connection.rollback()
            logger.error(f"Failed to drop configuration schema: {e}")
            return False
        finally:
            cursor.close()

    def backup_configuration_data(self, backup_file: str) -> bool:
        """Backup configuration data to a file."""
        try:
            import json
            from datetime import datetime

            cursor = self.database_connection.cursor()

            backup_data = {
                "backup_timestamp": datetime.now().isoformat(),
                "algorithms": [],
                "configurations": [],
                "audit_log": [],
                "environment_settings": [],
            }

            # Backup algorithms
            cursor.execute("SELECT * FROM scoring_algorithms")
            backup_data["algorithms"] = list(cursor.fetchall())

            # Backup configurations
            cursor.execute("SELECT * FROM scoring_configurations")
            backup_data["configurations"] = list(cursor.fetchall())

            # Backup recent audit log (last 90 days)
            cursor.execute(
                """
                SELECT * FROM configuration_audit_log
                WHERE change_timestamp >= DATE_SUB(NOW(), INTERVAL 90 DAY)
            """
            )
            backup_data["audit_log"] = list(cursor.fetchall())

            # Backup environment settings
            cursor.execute("SELECT * FROM environment_settings")
            backup_data["environment_settings"] = list(cursor.fetchall())

            # Write to file
            with open(backup_file, "w") as f:
                json.dump(backup_data, f, indent=2, default=str)

            logger.info(f"Configuration data backed up to {backup_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to backup configuration data: {e}")
            return False
        finally:
            cursor.close()

    def restore_configuration_data(self, backup_file: str) -> bool:
        """Restore configuration data from a backup file."""
        try:
            import json

            if not os.path.exists(backup_file):
                logger.error(f"Backup file not found: {backup_file}")
                return False

            with open(backup_file, "r") as f:
                backup_data = json.load(f)

            cursor = self.database_connection.cursor()

            # Clear existing data
            cursor.execute("DELETE FROM configuration_audit_log")
            cursor.execute("DELETE FROM scoring_configurations")
            cursor.execute("DELETE FROM environment_settings")
            cursor.execute("DELETE FROM scoring_algorithms")

            # Restore algorithms
            for algorithm in backup_data["algorithms"]:
                cursor.execute(
                    """
                    INSERT INTO scoring_algorithms
                    (algorithm_id, algorithm_name, version, description, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        algorithm["algorithm_id"],
                        algorithm["algorithm_name"],
                        algorithm["version"],
                        algorithm["description"],
                        algorithm.get("status", "active"),  # Handle old backups
                        algorithm["created_at"],
                        algorithm["updated_at"],
                    ),
                )

            # Restore configurations
            for config in backup_data["configurations"]:
                cursor.execute(
                    """
                    INSERT INTO scoring_configurations
                    (config_id, algorithm_id, environment, parameters, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        config["config_id"],
                        config["algorithm_id"],
                        config["environment"],
                        (
                            json.dumps(config["parameters"])
                            if isinstance(config["parameters"], dict)
                            else config["parameters"]
                        ),
                        config.get("status", "active"),  # Handle old backups
                        config["created_at"],
                        config["updated_at"],
                    ),
                )

            # Restore environment settings
            for setting in backup_data["environment_settings"]:
                cursor.execute(
                    """
                    INSERT INTO environment_settings
                    (setting_id, environment, setting_name, setting_value, setting_type,
                     description, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        setting["setting_id"],
                        setting["environment"],
                        setting["setting_name"],
                        setting["setting_value"],
                        setting["setting_type"],
                        setting["description"],
                        setting.get("status", "active"),  # Handle old backups
                        setting["created_at"],
                        setting["updated_at"],
                    ),
                )

            # Restore audit log
            for audit_entry in backup_data["audit_log"]:
                cursor.execute(
                    """
                    INSERT INTO configuration_audit_log
                    (audit_id, algorithm_name, parameter_name, old_value, new_value,
                     changed_by, change_reason, change_timestamp, environment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        audit_entry["audit_id"],
                        audit_entry["algorithm_name"],
                        audit_entry["parameter_name"],
                        audit_entry["old_value"],
                        audit_entry["new_value"],
                        audit_entry["changed_by"],
                        audit_entry["change_reason"],
                        audit_entry["change_timestamp"],
                        audit_entry["environment"],
                    ),
                )

            self.database_connection.commit()
            logger.info(f"Configuration data restored from {backup_file}")
            return True

        except Exception as e:
            self.database_connection.rollback()
            logger.error(f"Failed to restore configuration data: {e}")
            return False
        finally:
            cursor.close()

    def get_configuration_statistics(self) -> dict:
        """Get statistics about the configuration system."""
        cursor = self.database_connection.cursor()
        try:
            stats = {}

            # Algorithm count
            cursor.execute("SELECT COUNT(*) as count FROM scoring_algorithms WHERE status = 'active'")
            stats["active_algorithms"] = cursor.fetchone()["count"]

            # Configuration count by environment
            cursor.execute(
                """
                SELECT environment, COUNT(*) as count
                FROM scoring_configurations
                WHERE status = 'active'
                GROUP BY environment
            """
            )
            stats["configurations_by_environment"] = {row["environment"]: row["count"] for row in cursor.fetchall()}

            # Recent changes count
            cursor.execute(
                """
                SELECT COUNT(*) as count
                FROM configuration_audit_log
                WHERE change_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """
            )
            stats["recent_changes_7_days"] = cursor.fetchone()["count"]

            # Environment settings count
            cursor.execute("SELECT COUNT(*) as count FROM environment_settings WHERE status = 'active'")
            stats["active_environment_settings"] = cursor.fetchone()["count"]

            return stats

        except Exception as e:
            logger.error(f"Failed to get configuration statistics: {e}")
            return {}
        finally:
            cursor.close()
