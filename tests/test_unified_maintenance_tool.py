"""
Tests for tools / core / unified_maintenance.py-unified system maintenance tool.

This test suite validates:
- SystemMaintenance class functionality
- Data cleanup and retention operations
- Database optimization features
- System health maintenance
- Safety checks and validation
- Error handling and recovery
"""

import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from tools.core.unified_maintenance import SystemMaintenance, main


class TestSystemMaintenance:
    """Test SystemMaintenance class functionality."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    def test_tool_initialization(self):
        """Test basic tool initialization."""
        assert self.maintenance_tool.name == "unified-maintenance"
        assert self.maintenance_tool.version == "1.0.0"
        assert self.maintenance_tool.logger is not None

        # Check maintenance session tracking
        assert self.maintenance_tool.maintenance_session_id.startswith("maint_")
        assert "session_id" in self.maintenance_tool.operations_log
        assert "operations_performed" in self.maintenance_tool.operations_log

    def test_get_tool_config(self):
        """Test tool configuration metadata."""
        config = self.maintenance_tool.get_tool_config()

        assert config.name == "unified-maintenance"
        assert config.version == "1.0.0"
        assert config.category == "core"
        assert "python>=3.8" in config.dependencies
        assert "DB_HOST" in config.environment_vars
        assert "YOUTUBE_DATA_RETENTION_DAYS" in config.environment_vars

    def test_get_required_environment_vars(self):
        """Test required environment variables."""
        required_vars = self.maintenance_tool.get_required_environment_vars()
        expected_vars = ["DB_HOST", "DB_USER", "DB_NAME"]

        for var in expected_vars:
            assert var in required_vars


class TestDataCleanup:
    """Test data cleanup functionality."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    @patch("web.etl_helpers.get_engine")
    @patch.dict(os.environ, {"YOUTUBE_DATA_RETENTION_DAYS": "30"})
    def test_cleanup_old_data_dry_run(self, mock_get_engine):
        """Test dry run data cleanup."""
        # Mock database engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock table existence check
        mock_conn.execute.side_effect = [
            MagicMock(scalar=lambda: 1),  # Table exists
            MagicMock(scalar=lambda: 100),  # Records to delete
            MagicMock(scalar=lambda: 1000),  # Total records
        ]

        result = self.maintenance_tool.cleanup_old_data(days=30, dry_run=True)

        assert result["operation"] == "cleanup_old_data"
        assert result["retention_days"] == 30
        assert result["dry_run"] is True
        assert "tables_processed" in result
        assert "safety_checks" in result

    def test_cleanup_old_data_validation_error(self):
        """Test cleanup validation for unsafe retention periods."""
        with pytest.raises(Exception):  # Should raise ValidationError
            self.maintenance_tool.cleanup_old_data(days=3, dry_run=True)

    @patch("web.etl_helpers.get_engine")
    def test_cleanup_old_data_safety_block(self, mock_get_engine):
        """Test safety block for large deletions."""
        # Mock database engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock scenario where deletion would affect >50% of data
        mock_conn.execute.side_effect = [
            MagicMock(scalar=lambda: 1),  # Table exists
            MagicMock(scalar=lambda: 600),  # Records to delete (60% of 1000)
            MagicMock(scalar=lambda: 1000),  # Total records
        ]

        result = self.maintenance_tool.cleanup_old_data(days=30, dry_run=False)

        # Should block deletion due to safety check
        table_result = list(result["tables_processed"].values())[0]
        assert table_result["status"] == "SAFETY_BLOCK"
        assert "50%" in table_result["reason"]

    @patch("web.etl_helpers.get_engine")
    def test_cleanup_old_data_actual_deletion(self, mock_get_engine):
        """Test actual data deletion (not dry run)."""
        # Mock database engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock safe deletion scenario (10% of data)
        mock_conn.execute.side_effect = [
            MagicMock(scalar=lambda: 1),  # Table exists
            MagicMock(scalar=lambda: 100),  # Records to delete
            MagicMock(scalar=lambda: 1000),  # Total records
            MagicMock(),  # DELETE operation
        ]

        result = self.maintenance_tool.cleanup_old_data(days=30, dry_run=False)

        # Should perform actual deletion
        table_result = list(result["tables_processed"].values())[0]
        assert table_result["status"] == "DELETED"
        assert table_result["records_affected"] == 100


class TestDatabaseOptimization:
    """Test database optimization functionality."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    @patch("web.etl_helpers.get_engine")
    def test_optimize_database_success(self, mock_get_engine):
        """Test successful database optimization."""
        # Mock database engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock table list and optimization results
        mock_tables = [
            MagicMock(table_name="youtube_videos", size_mb=100.5, table_rows=1000),
            MagicMock(table_name="youtube_comments", size_mb=50.2, table_rows=5000),
        ]
        # Mock execute to return proper result objects
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_tables

        mock_optimize_result = MagicMock()
        mock_optimize_result.fetchall.return_value = [MagicMock(Msg_text="OK")]

        mock_size_result = MagicMock()
        mock_size_result.scalar.return_value = 95.0

        mock_conn.execute.side_effect = [
            mock_result,  # Table list query
            mock_optimize_result,  # OPTIMIZE result
            mock_optimize_result,  # ANALYZE result
            mock_size_result,  # New size query
            mock_optimize_result,  # OPTIMIZE result
            mock_optimize_result,  # ANALYZE result
            MagicMock(scalar=lambda: 48.0),  # New size query
        ]

        result = self.maintenance_tool.optimize_database(analyze_tables=True)

        assert result["operation"] == "optimize_database"
        assert "optimizations" in result
        assert "performance_impact" in result

        # Check that both tables were optimized
        assert "youtube_videos" in result["optimizations"]
        assert "youtube_comments" in result["optimizations"]

        # Check performance impact calculation
        impact = result["performance_impact"]
        assert impact["total_original_size_mb"] > impact["total_new_size_mb"]
        assert impact["total_size_reduction_mb"] > 0

    @patch("web.etl_helpers.get_engine")
    def test_optimize_database_error_handling(self, mock_get_engine):
        """Test database optimization error handling."""
        # Mock database engine that raises an error
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock table list but optimization fails
        mock_tables = [MagicMock(table_name="youtube_videos", size_mb=100.5)]
        # Mock table list query to succeed, but optimization to fail
        mock_result = MagicMock()
        mock_result.fetchall.return_value = mock_tables

        mock_conn.execute.side_effect = [
            mock_result,  # Table list query succeeds
            Exception("Optimization failed"),  # OPTIMIZE fails
        ]

        result = self.maintenance_tool.optimize_database()

        assert result["operation"] == "optimize_database"
        assert "youtube_videos" in result["optimizations"]
        assert result["optimizations"]["youtube_videos"]["status"] == "ERROR"


class TestDataRetention:
    """Test data retention functionality."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    @patch.object(SystemMaintenance, "cleanup_old_data")
    @patch("web.etl_helpers.get_engine")
    @patch.dict(os.environ, {"YOUTUBE_DATA_RETENTION_DAYS": "45"})
    def test_data_retention_compliance_mode(self, mock_get_engine, mock_cleanup):
        """Test data retention in YouTube ToS compliance mode."""
        # Mock cleanup results
        mock_cleanup.return_value = {"total_records_affected": 1000, "status": "SUCCESS"}

        # Mock database operations
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        mock_conn.execute.return_value.rowcount = 50

        # Force reload config to pick up test environment variable
        self.maintenance_tool.config["YOUTUBE_DATA_RETENTION_DAYS"] = "45"

        result = self.maintenance_tool.data_retention_cleanup(compliance_mode=True)

        # Should enforce 30-day limit in compliance mode
        assert result["retention_days"] == 30
        assert result["youtube_tos_compliant"] is True
        assert result["compliance_mode"] is True

        # Should call cleanup with 30 days, not the configured 45
        mock_cleanup.assert_called_once_with(days=30, dry_run=False)

    @patch.object(SystemMaintenance, "cleanup_old_data")
    @patch("web.etl_helpers.get_engine")
    @patch.dict(os.environ, {"YOUTUBE_DATA_RETENTION_DAYS": "15"})
    def test_data_retention_non_compliance_mode(self, mock_get_engine, mock_cleanup):
        """Test data retention without compliance mode."""
        # Mock cleanup results
        mock_cleanup.return_value = {"total_records_affected": 500, "status": "SUCCESS"}

        # Mock database operations
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Force reload config to pick up test environment variable
        self.maintenance_tool.config["YOUTUBE_DATA_RETENTION_DAYS"] = "15"

        result = self.maintenance_tool.data_retention_cleanup(compliance_mode=False)

        # Should use configured retention period
        assert result["retention_days"] == 15
        assert result["youtube_tos_compliant"] is True  # 15 days is compliant
        assert result["compliance_mode"] is False

        # Should call cleanup with configured days
        mock_cleanup.assert_called_once_with(days=15, dry_run=False)


class TestSystemHealthMaintenance:
    """Test system health maintenance functionality."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    @patch.object(SystemMaintenance, "_fix_data_consistency")
    @patch.object(SystemMaintenance, "_update_database_statistics")
    @patch.object(SystemMaintenance, "_cleanup_orphaned_records")
    @patch.object(SystemMaintenance, "_validate_data_integrity")
    def test_system_health_maintenance_success(self, mock_integrity, mock_orphans, mock_stats, mock_consistency):
        """Test successful system health maintenance."""
        # Mock all maintenance tasks as successful
        mock_consistency.return_value = {"status": "SUCCESS", "fixes_applied": ["Fix 1"]}
        mock_stats.return_value = {"status": "SUCCESS", "tables_updated": ["table1", "table2"]}
        mock_orphans.return_value = {"status": "SUCCESS", "cleanup_results": ["Cleanup 1"]}
        mock_integrity.return_value = {"status": "SUCCESS", "overall_integrity": "PASS"}

        result = self.maintenance_tool.system_health_maintenance()

        assert result["operation"] == "system_health_maintenance"
        assert result["overall_status"] == "HEALTHY"
        assert "maintenance_tasks" in result

        # Check that all tasks were executed
        tasks = result["maintenance_tasks"]
        assert "data_consistency" in tasks
        assert "database_statistics" in tasks
        assert "orphan_cleanup" in tasks
        assert "data_integrity" in tasks

        # Verify all helper methods were called
        mock_consistency.assert_called_once()
        mock_stats.assert_called_once()
        mock_orphans.assert_called_once()
        mock_integrity.assert_called_once()

    @patch.object(SystemMaintenance, "_fix_data_consistency")
    @patch.object(SystemMaintenance, "_update_database_statistics")
    @patch.object(SystemMaintenance, "_cleanup_orphaned_records")
    @patch.object(SystemMaintenance, "_validate_data_integrity")
    def test_system_health_maintenance_with_errors(self, mock_integrity, mock_orphans, mock_stats, mock_consistency):
        """Test system health maintenance with some errors."""
        # Mock mixed results
        mock_consistency.return_value = {"status": "SUCCESS"}
        mock_stats.return_value = {"status": "ERROR", "error": "Stats update failed"}
        mock_orphans.return_value = {"status": "WARNING"}
        mock_integrity.return_value = {"status": "SUCCESS"}

        result = self.maintenance_tool.system_health_maintenance()

        assert result["operation"] == "system_health_maintenance"
        assert result["overall_status"] == "ERROR"  # Should be ERROR due to stats failure


class TestFullMaintenance:
    """Test full maintenance workflow."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    @patch.object(SystemMaintenance, "data_retention_cleanup")
    @patch.object(SystemMaintenance, "system_health_maintenance")
    @patch.object(SystemMaintenance, "optimize_database")
    def test_full_maintenance_success(self, mock_optimize, mock_health, mock_retention):
        """Test successful full maintenance."""
        # Mock all operations as successful
        mock_retention.return_value = {"status": "SUCCESS", "retention_days": 30}
        mock_health.return_value = {"overall_status": "HEALTHY"}
        mock_optimize.return_value = {"status": "SUCCESS"}

        result = self.maintenance_tool.full_maintenance(include_optimization=True, dry_run=False)

        assert result["operation"] == "full_maintenance"
        assert "operations" in result
        assert "summary" in result

        # Check that all operations were executed
        operations = result["operations"]
        assert "data_retention" in operations
        assert "system_health" in operations
        assert "database_optimization" in operations

        # Check summary
        summary = result["summary"]
        assert summary["total_operations"] == 3
        assert summary["overall_status"] == "SUCCESS"

        # Verify all methods were called
        mock_retention.assert_called_once_with(compliance_mode=True)
        mock_health.assert_called_once()
        mock_optimize.assert_called_once_with(analyze_tables=True)

    @patch.object(SystemMaintenance, "data_retention_cleanup")
    @patch.object(SystemMaintenance, "system_health_maintenance")
    def test_full_maintenance_without_optimization(self, mock_health, mock_retention):
        """Test full maintenance without database optimization."""
        # Mock operations
        mock_retention.return_value = {"status": "SUCCESS"}
        mock_health.return_value = {"overall_status": "HEALTHY"}

        result = self.maintenance_tool.full_maintenance(include_optimization=False)

        # Should only have 2 operations (no optimization)
        assert len(result["operations"]) == 2
        assert "database_optimization" not in result["operations"]

        summary = result["summary"]
        assert summary["total_operations"] == 2


class TestHelperMethods:
    """Test helper methods for maintenance operations."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    @patch("web.etl_helpers.get_engine")
    def test_fix_data_consistency(self, mock_get_engine):
        """Test data consistency fixing."""
        # Mock database operations
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock successful fixes
        mock_conn.execute.side_effect = [
            MagicMock(rowcount=5),  # Duplicates removed
            MagicMock(rowcount=3),  # Null channels fixed
        ]

        result = self.maintenance_tool._fix_data_consistency()

        assert result["status"] == "SUCCESS"
        assert result["total_fixes"] == 2
        assert len(result["fixes_applied"]) == 2

    @patch("web.etl_helpers.get_engine")
    def test_cleanup_orphaned_records(self, mock_get_engine):
        """Test orphaned records cleanup."""
        # Mock database operations
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock cleanup results
        mock_conn.execute.side_effect = [
            MagicMock(rowcount=10),  # Orphaned comments
            MagicMock(rowcount=5),  # Orphaned metrics
        ]

        result = self.maintenance_tool._cleanup_orphaned_records()

        assert result["status"] == "SUCCESS"
        assert result["total_cleanups"] == 2
        assert "orphaned comments" in result["cleanup_results"][0]
        assert "orphaned metrics" in result["cleanup_results"][1]

    @patch("web.etl_helpers.get_engine")
    def test_validate_data_integrity(self, mock_get_engine):
        """Test data integrity validation."""
        # Mock database operations
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock validation results (all pass)
        mock_conn.execute.side_effect = [
            MagicMock(scalar=lambda: 0),  # No invalid video IDs
            MagicMock(scalar=lambda: 0),  # No future dates
        ]

        result = self.maintenance_tool._validate_data_integrity()

        assert result["status"] == "SUCCESS"
        assert result["overall_integrity"] == "PASS"
        assert len(result["checks"]) == 2
        assert all(check["status"] == "PASS" for check in result["checks"])


class TestUtilityMethods:
    """Test utility and helper methods."""

    def setup_method(self):
        """Set up fresh maintenance tool for each test."""
        self.maintenance_tool = SystemMaintenance()

    def test_get_maintenance_status(self):
        """Test maintenance status reporting."""
        status = self.maintenance_tool.get_maintenance_status()

        assert "maintenance_session" in status
        assert "operations_log" in status
        assert "timestamp" in status

        # Check operations log structure
        ops_log = status["operations_log"]
        assert "session_id" in ops_log
        assert "start_time" in ops_log
        assert "operations_performed" in ops_log
        assert isinstance(ops_log["operations_performed"], list)

    def test_cleanup_resources(self):
        """Test resource cleanup (should not raise errors)."""
        # Should not raise any exceptions
        self.maintenance_tool.cleanup_resources()

        # Should update operations log
        assert "end_time" in self.maintenance_tool.operations_log


class TestMainFunction:
    """Test main function and CLI interface."""

    @patch("tools.core.unified_maintenance.SystemMaintenance")
    def test_main_cleanup_old_option(self, mock_maintenance_class):
        """Test main function with --cleanup-old option."""
        mock_maintenance = MagicMock()
        mock_maintenance.cleanup_old_data.return_value = {"total_records_affected": 100}
        mock_maintenance_class.return_value.__enter__.return_value = mock_maintenance

        with patch("sys.argv", ["unified_maintenance.py", "--cleanup-old", "--days", "30"]):
            result = main()

        assert result == 0
        mock_maintenance.cleanup_old_data.assert_called_once()

    @patch("tools.core.unified_maintenance.SystemMaintenance")
    def test_main_optimize_db_option(self, mock_maintenance_class):
        """Test main function with --optimize-db option."""
        mock_maintenance = MagicMock()
        mock_maintenance.optimize_database.return_value = {"performance_impact": {"optimization_percentage": 15.5}}
        mock_maintenance_class.return_value.__enter__.return_value = mock_maintenance

        with patch("sys.argv", ["unified_maintenance.py", "--optimize-db"]):
            result = main()

        assert result == 0
        mock_maintenance.optimize_database.assert_called_once_with(analyze_tables=True)

    @patch("tools.core.unified_maintenance.SystemMaintenance")
    def test_main_full_maintenance_option(self, mock_maintenance_class):
        """Test main function with --full-maintenance option."""
        mock_maintenance = MagicMock()
        mock_maintenance.full_maintenance.return_value = {
            "summary": {"overall_status": "SUCCESS", "successful_operations": 3, "total_operations": 3}
        }
        mock_maintenance_class.return_value.__enter__.return_value = mock_maintenance

        with patch("sys.argv", ["unified_maintenance.py", "--full-maintenance"]):
            result = main()

        assert result == 0
        mock_maintenance.full_maintenance.assert_called_once_with(include_optimization=True, dry_run=False)

    @patch("tools.core.unified_maintenance.SystemMaintenance")
    def test_main_json_output(self, mock_maintenance_class):
        """Test main function with JSON output."""
        mock_maintenance = MagicMock()
        mock_maintenance.get_maintenance_status.return_value = {"test": "status"}
        mock_maintenance_class.return_value.__enter__.return_value = mock_maintenance

        with patch("sys.argv", ["unified_maintenance.py", "--status", "--json"]):
            with patch("builtins.print") as mock_print:
                result = main()

        assert result == 0
        mock_maintenance.get_maintenance_status.assert_called_once()
        # Should print JSON
        mock_print.assert_called_once()

    @patch("tools.core.unified_maintenance.SystemMaintenance")
    def test_main_keyboard_interrupt(self, mock_maintenance_class):
        """Test main function handles keyboard interrupt gracefully."""
        mock_maintenance = MagicMock()
        mock_maintenance.cleanup_old_data.side_effect = KeyboardInterrupt()
        mock_maintenance_class.return_value.__enter__.return_value = mock_maintenance

        with patch("sys.argv", ["unified_maintenance.py", "--cleanup-old"]):
            result = main()

        assert result == 1


class TestIntegration:
    """Integration tests for the unified maintenance tool."""

    def test_tool_registration(self):
        """Test that the tool registers itself properly."""
        from tools.shared.common import find_tool

        # Create tool instance (should register itself)
        maintenance_tool = SystemMaintenance()

        # Find the registered tool
        found_tool = find_tool("unified-maintenance")

        assert found_tool is not None
        assert found_tool.name == "unified-maintenance"
        assert found_tool.version == "1.0.0"
        assert found_tool.category == "core"

    def test_context_manager_usage(self):
        """Test tool can be used as context manager."""
        cleanup_called = False

        class TestMaintenance(SystemMaintenance):
            def cleanup_resources(self):
                nonlocal cleanup_called
                cleanup_called = True

        with TestMaintenance() as maintenance_tool:
            assert maintenance_tool.name == "unified-maintenance"

        assert cleanup_called
