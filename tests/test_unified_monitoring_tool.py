"""
Tests for tools / core / unified_monitor.py - unified system monitoring tool.

This test suite validates:
- SystemMonitor class functionality
- Quick health checks and full system monitoring
- Data quality monitoring integration
- ETL health check integration
- Performance monitoring capabilities
- Enterprise monitoring features
- Error handling and recovery
"""

from datetime import datetime, timedelta
import json
import os
from unittest.mock import MagicMock, call, patch

import pytest

from tools.core.unified_monitor import SystemMonitor, main, print_monitoring_report


class TestSystemMonitor:
    """Test SystemMonitor class functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    def test_tool_initialization(self):
        """Test basic tool initialization."""
        assert self.monitor.name == "unified - monitor"
        assert self.monitor.version == "1.0.0"
        assert self.monitor.logger is not None
        assert "monitor_" in self.monitor.monitoring_session_id

        # Check initial metrics
        expected_keys = ["session_id", "start_time", "checks_performed", "issues_found", "overall_status"]
        for key in expected_keys:
            assert key in self.monitor.metrics

    def test_get_tool_config(self):
        """Test tool configuration metadata."""
        config = self.monitor.get_tool_config()

        assert config.name == "unified - monitor"
        assert config.version == "1.0.0"
        assert config.category == "core"
        assert "python>=3.8" in config.dependencies
        assert "DB_HOST" in config.environment_vars

    def test_get_required_environment_vars(self):
        """Test required environment variables."""
        required_vars = self.monitor.get_required_environment_vars()
        expected_vars = ["DB_HOST", "DB_USER", "DB_NAME"]

        for var in expected_vars:
            assert var in required_vars


class TestQuickHealthCheck:
    """Test quick health check functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch.object(SystemMonitor, "_check_database_connectivity")
    @patch.object(SystemMonitor, "_get_basic_data_counts")
    @patch.object(SystemMonitor, "_quick_consistency_check")
    @patch.object(SystemMonitor, "_check_etl_status")
    def test_quick_health_check_healthy(self, mock_etl, mock_consistency, mock_counts, mock_db):
        """Test quick health check with all systems healthy."""
        # Mock all components as healthy
        mock_db.return_value = {"status": "HEALTHY", "message": "DB OK"}
        mock_counts.return_value = {"status": "HEALTHY", "counts": {"videos": 1000}}
        mock_consistency.return_value = {"status": "HEALTHY", "message": "Consistent"}
        mock_etl.return_value = {"status": "HEALTHY", "runs_today": 1}

        result = self.monitor.quick_health_check(days=7)

        assert result["status"] == "HEALTHY"
        assert "components" in result
        assert "summary" in result
        assert result["summary"]["overall_status"] == "HEALTHY"

        # Verify all checks were called
        mock_db.assert_called_once()
        mock_counts.assert_called_once()
        mock_consistency.assert_called_once_with(7)
        mock_etl.assert_called_once()

    @patch.object(SystemMonitor, "_check_database_connectivity")
    @patch.object(SystemMonitor, "_get_basic_data_counts")
    @patch.object(SystemMonitor, "_quick_consistency_check")
    @patch.object(SystemMonitor, "_check_etl_status")
    def test_quick_health_check_critical(self, mock_etl, mock_consistency, mock_counts, mock_db):
        """Test quick health check with critical issues."""
        # Mock database as critical
        mock_db.return_value = {"status": "CRITICAL", "message": "DB Failed"}
        mock_counts.return_value = {"status": "HEALTHY", "counts": {"videos": 1000}}
        mock_consistency.return_value = {"status": "HEALTHY", "message": "Consistent"}
        mock_etl.return_value = {"status": "WARNING", "runs_today": 0}

        result = self.monitor.quick_health_check(days=7)

        assert result["status"] == "CRITICAL"
        assert result["components"]["database"]["status"] == "CRITICAL"

    @patch.object(SystemMonitor, "_check_database_connectivity")
    def test_quick_health_check_error_handling(self, mock_db):
        """Test quick health check error handling."""
        # Mock database check to raise exception
        mock_db.side_effect = Exception("Database connection failed")

        # The method should handle the error and return error result
        try:
            result = self.monitor.quick_health_check()
            # Should not reach here due to exception handling
            assert False, "Expected ExecutionError to be raised"
        except Exception as e:
            # Verify the error was handled properly
            assert "Database connection failed" in str(e)


class TestDataQualityCheck:
    """Test data quality monitoring functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch("tools.core.data_quality_validator.DataQualityValidator")
    def test_data_quality_check_success(self, mock_validator_class):
        """Test successful data quality check."""
        # Mock validator and report
        mock_validator = MagicMock()
        mock_validator_class.return_value = mock_validator

        mock_report = MagicMock()
        mock_report.timestamp = datetime.now()
        mock_report.overall_score = 85.5
        mock_report.issues = []
        mock_report.statistics = {"total_records": 1000}
        mock_report.recommendations = ["Keep monitoring"]

        mock_validator.run_comprehensive_validation.return_value = mock_report

        result = self.monitor.data_quality_check(fix_issues=False)

        assert result["check_type"] == "data_quality"
        assert result["overall_score"] == 85.5
        assert result["status"] == "HEALTHY"  # Score > 80
        assert result["statistics"] == {"total_records": 1000}

        # Verify validator was created correctly
        mock_validator_class.assert_called_once_with(fix_issues=False, report_only=False)

    @patch("tools.core.data_quality_validator.DataQualityValidator")
    def test_data_quality_check_with_issues(self, mock_validator_class):
        """Test data quality check with issues found."""
        # Mock validator with issues
        mock_validator = MagicMock()
        mock_validator_class.return_value = mock_validator

        mock_issue = MagicMock()
        mock_issue.category = "Completeness"
        mock_issue.severity = "HIGH"
        mock_issue.table = "youtube_videos"
        mock_issue.issue_type = "missing_data"
        mock_issue.description = "Missing video titles"
        mock_issue.count = 50
        mock_issue.fix_suggestion = "Investigate data source"
        mock_issue.auto_fixable = False

        mock_report = MagicMock()
        mock_report.timestamp = datetime.now()
        mock_report.overall_score = 65.0
        mock_report.issues = [mock_issue]
        mock_report.statistics = {}
        mock_report.recommendations = ["Fix data issues"]

        mock_validator.run_comprehensive_validation.return_value = mock_report

        result = self.monitor.data_quality_check()

        assert result["overall_score"] == 65.0
        assert result["status"] == "CRITICAL"  # Score < 70
        assert len(result["issues"]) == 1
        assert result["issues"][0]["category"] == "Completeness"
        assert result["issues"][0]["severity"] == "HIGH"

    @patch("tools.core.data_quality_validator.DataQualityValidator")
    def test_data_quality_check_error(self, mock_validator_class):
        """Test data quality check error handling."""
        # Mock validator to raise exception
        mock_validator_class.side_effect = Exception("Validator failed")

        # The method should handle the error and raise ExecutionError
        try:
            result = self.monitor.data_quality_check()
            # Should not reach here due to exception handling
            assert False, "Expected ExecutionError to be raised"
        except Exception as e:
            # Verify the error was handled properly
            assert "Validator failed" in str(e)


class TestETLHealthCheck:
    """Test ETL health monitoring functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch("tools.core.etl_health_check.ETLHealthChecker")
    def test_etl_health_check_success(self, mock_checker_class):
        """Test successful ETL health check."""
        # Mock health checker and report
        mock_checker = MagicMock()
        mock_checker_class.return_value = mock_checker

        mock_result = MagicMock()
        mock_result.component = "Database"
        mock_result.status = "PASS"
        mock_result.message = "Database connection successful"
        mock_result.details = {"host": "localhost"}
        mock_result.recovery_instructions = []

        mock_report = MagicMock()
        mock_report.timestamp = datetime.now()
        mock_report.overall_status = "HEALTHY"
        mock_report.summary = {"total_checks": 5, "passed": 5}
        mock_report.results = [mock_result]

        mock_checker.run_comprehensive_health_check.return_value = mock_report

        result = self.monitor.etl_health_check(verbose=True)

        assert result["check_type"] == "etl_health"
        assert result["overall_status"] == "HEALTHY"
        assert result["status"] == "HEALTHY"
        assert len(result["results"]) == 1
        assert result["results"][0]["component"] == "Database"

        # Verify checker was created correctly
        mock_checker_class.assert_called_once_with(verbose=True, fix_issues=False)

    @patch("tools.core.etl_health_check.ETLHealthChecker")
    def test_etl_health_check_with_failures(self, mock_checker_class):
        """Test ETL health check with failures."""
        # Mock health checker with failures
        mock_checker = MagicMock()
        mock_checker_class.return_value = mock_checker

        mock_result = MagicMock()
        mock_result.component = "YouTube API"
        mock_result.status = "FAIL"
        mock_result.message = "API key invalid"
        mock_result.details = {}
        mock_result.recovery_instructions = ["Check API key"]

        mock_report = MagicMock()
        mock_report.timestamp = datetime.now()
        mock_report.overall_status = "CRITICAL"
        mock_report.summary = {"total_checks": 5, "passed": 3, "failed": 2}
        mock_report.results = [mock_result]

        mock_checker.run_comprehensive_health_check.return_value = mock_report

        result = self.monitor.etl_health_check()

        assert result["overall_status"] == "CRITICAL"
        assert result["status"] == "CRITICAL"
        assert len(result["results"]) == 1
        assert result["results"][0]["status"] == "FAIL"


class TestPerformanceMonitoring:
    """Test performance monitoring functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch.object(SystemMonitor, "_get_database_performance_metrics")
    @patch.object(SystemMonitor, "_get_etl_performance_metrics")
    @patch.object(SystemMonitor, "_analyze_data_growth_trends")
    @patch.object(SystemMonitor, "_get_api_usage_metrics")
    @patch.object(SystemMonitor, "_generate_performance_alerts")
    def test_performance_monitoring_success(self, mock_alerts, mock_api, mock_trends, mock_etl, mock_db):
        """Test successful performance monitoring."""
        # Mock all performance metrics
        mock_db.return_value = {
            "status": "HEALTHY",
            "metrics": {"table_sizes": [{"table": "youtube_videos", "size_mb": 500}]},
        }
        mock_etl.return_value = {"status": "HEALTHY", "metrics": {"avg_processing_time_minutes": 30}}
        mock_trends.return_value = {"status": "HEALTHY", "trends": {"growth_rate_percent": 5.2}}
        mock_api.return_value = {"status": "HEALTHY", "quota_usage": "50%"}
        mock_alerts.return_value = []

        result = self.monitor.performance_monitoring(days=7)

        assert result["check_type"] == "performance"
        assert result["analysis_period_days"] == 7
        assert result["status"] == "HEALTHY"
        assert "metrics" in result
        assert "trends" in result
        assert result["alerts"] == []

        # Verify all methods were called
        mock_db.assert_called_once_with(7)
        mock_etl.assert_called_once_with(7)
        mock_trends.assert_called_once_with(7)
        mock_api.assert_called_once_with(7)

    @patch.object(SystemMonitor, "_get_database_performance_metrics")
    @patch.object(SystemMonitor, "_get_etl_performance_metrics")
    @patch.object(SystemMonitor, "_analyze_data_growth_trends")
    @patch.object(SystemMonitor, "_get_api_usage_metrics")
    @patch.object(SystemMonitor, "_generate_performance_alerts")
    def test_performance_monitoring_with_alerts(self, mock_alerts, mock_api, mock_trends, mock_etl, mock_db):
        """Test performance monitoring with alerts."""
        # Mock metrics with performance issues
        mock_db.return_value = {"status": "HEALTHY", "metrics": {}}
        mock_etl.return_value = {"status": "HEALTHY", "metrics": {}}
        mock_trends.return_value = {"status": "HEALTHY", "trends": {}}
        mock_api.return_value = {"status": "HEALTHY"}

        # Mock critical alert
        mock_alerts.return_value = [
            {
                "severity": "CRITICAL",
                "component": "ETL Performance",
                "message": "ETL processing time is high",
                "recommendation": "Investigate bottlenecks",
            }
        ]

        result = self.monitor.performance_monitoring()

        assert result["status"] == "CRITICAL"  # Due to critical alert
        assert len(result["alerts"]) == 1
        assert result["alerts"][0]["severity"] == "CRITICAL"


class TestEnterpriseMonitoring:
    """Test enterprise monitoring functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch.object(SystemMonitor, "_monitor_service_health")
    @patch.object(SystemMonitor, "_monitor_sla_compliance")
    @patch.object(SystemMonitor, "_generate_executive_summary")
    @patch.object(SystemMonitor, "_generate_enterprise_alerts")
    def test_enterprise_monitoring_success(self, mock_alerts, mock_summary, mock_sla, mock_health):
        """Test successful enterprise monitoring."""
        # Mock enterprise monitoring components
        mock_health.return_value = {"health_score": 95.0, "status": "HEALTHY", "indicators": {}}
        mock_sla.return_value = {"overall_compliance_percent": 98.5, "status": "COMPLIANT", "sla_details": {}}
        mock_summary.return_value = {
            "system_health_score": 95.0,
            "sla_compliance_percent": 98.5,
            "critical_issues": 0,
            "warnings": 0,
            "recommendations": [],
        }
        mock_alerts.return_value = []

        result = self.monitor.enterprise_monitoring(include_sla=True)

        assert result["check_type"] == "enterprise"
        assert result["status"] == "HEALTHY"
        assert "monitoring_session" in result
        assert result["service_health"]["health_score"] == 95.0
        assert result["sla_compliance"]["overall_compliance_percent"] == 98.5
        assert result["executive_summary"]["critical_issues"] == 0

        # Verify all methods were called
        mock_health.assert_called_once()
        mock_sla.assert_called_once()
        mock_summary.assert_called_once()
        mock_alerts.assert_called_once()

    @patch.object(SystemMonitor, "_monitor_service_health")
    @patch.object(SystemMonitor, "_generate_executive_summary")
    @patch.object(SystemMonitor, "_generate_enterprise_alerts")
    def test_enterprise_monitoring_without_sla(self, mock_alerts, mock_summary, mock_health):
        """Test enterprise monitoring without SLA monitoring."""
        # Mock components
        mock_health.return_value = {"health_score": 85.0, "status": "HEALTHY"}
        mock_summary.return_value = {"critical_issues": 0, "warnings": 1}
        mock_alerts.return_value = []

        result = self.monitor.enterprise_monitoring(include_sla=False)

        assert result["check_type"] == "enterprise"
        assert "sla_compliance" not in result or result["sla_compliance"] == {}

        # Verify SLA monitoring was not called
        mock_health.assert_called_once()
        mock_summary.assert_called_once()


class TestFullSystemCheck:
    """Test full system monitoring functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch.object(SystemMonitor, "quick_health_check")
    @patch.object(SystemMonitor, "data_quality_check")
    @patch.object(SystemMonitor, "etl_health_check")
    @patch.object(SystemMonitor, "performance_monitoring")
    @patch.object(SystemMonitor, "sentiment_monitoring")
    @patch.object(SystemMonitor, "_generate_system_recommendations")
    def test_full_system_check_all_healthy(
        self, mock_recommendations, mock_sentiment, mock_performance, mock_etl, mock_quality, mock_health
    ):
        """Test full system check with all components healthy."""
        # Mock all checks as healthy
        mock_health.return_value = {"status": "HEALTHY", "summary": {}}
        mock_quality.return_value = {"status": "HEALTHY", "overall_score": 95}
        mock_etl.return_value = {"status": "HEALTHY", "summary": {}}
        mock_performance.return_value = {"status": "HEALTHY", "metrics": {}}
        mock_sentiment.return_value = {"status": "HEALTHY", "overall_health_score": 85}
        mock_recommendations.return_value = ["System is healthy"]

        result = self.monitor.full_system_check(days=7, fix_issues=False)

        assert result["check_type"] == "full_system"
        assert result["status"] == "HEALTHY"
        assert result["analysis_period_days"] == 7
        assert len(result["components"]) == 5
        assert result["summary"]["total_checks"] == 5
        assert result["summary"]["passed"] == 5
        assert result["summary"]["failed"] == 0

        # Verify all checks were called
        mock_health.assert_called_once_with(7)
        mock_quality.assert_called_once_with(False)
        mock_etl.assert_called_once_with(verbose=False)
        mock_performance.assert_called_once_with(7)
        mock_sentiment.assert_called_once()

    @patch.object(SystemMonitor, "quick_health_check")
    @patch.object(SystemMonitor, "data_quality_check")
    @patch.object(SystemMonitor, "etl_health_check")
    @patch.object(SystemMonitor, "performance_monitoring")
    @patch.object(SystemMonitor, "sentiment_monitoring")
    @patch.object(SystemMonitor, "_generate_system_recommendations")
    def test_full_system_check_with_failures(
        self, mock_recommendations, mock_sentiment, mock_performance, mock_etl, mock_quality, mock_health
    ):
        """Test full system check with some failures."""
        # Mock mixed results
        mock_health.return_value = {"status": "HEALTHY"}
        mock_quality.return_value = {"status": "CRITICAL", "overall_score": 45}
        mock_etl.return_value = {"status": "WARNING"}
        mock_performance.return_value = {"status": "HEALTHY"}
        mock_sentiment.return_value = {"status": "WARNING"}
        mock_recommendations.return_value = ["Fix data quality issues", "Address ETL warnings"]

        result = self.monitor.full_system_check()

        assert result["status"] == "CRITICAL"  # Due to critical data quality
        assert result["summary"]["passed"] == 2  # health and performance
        assert result["summary"]["warnings"] == 2  # etl and sentiment
        assert result["summary"]["failed"] == 1  # data quality
        assert len(result["recommendations"]) == 2

    @patch.object(SystemMonitor, "quick_health_check")
    def test_full_system_check_with_exception(self, mock_health):
        """Test full system check handles exceptions gracefully."""
        # Mock one check to raise exception
        mock_health.side_effect = Exception("Health check failed")

        result = self.monitor.full_system_check()

        assert result["check_type"] == "full_system"
        assert "components" in result
        assert result["components"]["quick_health"]["status"] == "ERROR"


class TestHelperMethods:
    """Test helper methods for monitoring functionality."""

    def setup_method(self):
        """Set up fresh monitor for each test."""
        self.monitor = SystemMonitor()

    @patch("web.etl_helpers.get_engine")
    def test_check_database_connectivity_success(self, mock_get_engine):
        """Test successful database connectivity check."""
        # Mock successful database connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        result = self.monitor._check_database_connectivity()

        assert result["status"] == "HEALTHY"
        assert "Database connection successful" in result["message"]
        mock_conn.execute.assert_called_once()

    @patch("web.etl_helpers.get_engine")
    def test_check_database_connectivity_failure(self, mock_get_engine):
        """Test database connectivity check failure."""
        # Mock database connection failure
        mock_get_engine.side_effect = Exception("Connection failed")

        result = self.monitor._check_database_connectivity()

        assert result["status"] == "CRITICAL"
        assert "Database connection failed" in result["message"]
        assert "error" in result

    def test_score_to_status_conversion(self):
        """Test score to status conversion utility."""
        assert self.monitor._score_to_status(85.0) == "HEALTHY"
        assert self.monitor._score_to_status(75.0) == "WARNING"
        assert self.monitor._score_to_status(65.0) == "CRITICAL"

    def test_convert_sentiment_status(self):
        """Test sentiment health score conversion."""
        assert self.monitor._convert_sentiment_status(85.0) == "HEALTHY"
        assert self.monitor._convert_sentiment_status(70.0) == "WARNING"
        assert self.monitor._convert_sentiment_status(50.0) == "CRITICAL"

    def test_get_monitoring_status(self):
        """Test monitoring status reporting."""
        status = self.monitor.get_monitoring_status()

        assert "monitoring_session" in status
        assert "metrics" in status
        assert "timestamp" in status
        assert status["monitoring_session"] == self.monitor.monitoring_session_id


class TestMainFunction:
    """Test main function and CLI interface."""

    @patch("tools.core.unified_monitor.SystemMonitor")
    def test_main_quick_health_default(self, mock_monitor_class):
        """Test main function with default (quick health) option."""
        mock_monitor = MagicMock()
        mock_monitor.quick_health_check.return_value = {"status": "HEALTHY"}
        mock_monitor_class.return_value.__enter__.return_value = mock_monitor

        with patch("sys.argv", ["unified_monitor.py"]):
            result = main()

        assert result == 0
        mock_monitor.quick_health_check.assert_called_once_with(days=7)

    @patch("tools.core.unified_monitor.SystemMonitor")
    def test_main_full_check_option(self, mock_monitor_class):
        """Test main function with --full - check option."""
        mock_monitor = MagicMock()
        mock_monitor.full_system_check.return_value = {"status": "WARNING"}
        mock_monitor_class.return_value.__enter__.return_value = mock_monitor

        with patch("sys.argv", ["unified_monitor.py", "--full - check", "--days", "14"]):
            result = main()

        assert result == 1  # WARNING status
        mock_monitor.full_system_check.assert_called_once_with(days=14, fix_issues=False)

    @patch("tools.core.unified_monitor.SystemMonitor")
    def test_main_data_quality_option(self, mock_monitor_class):
        """Test main function with --data - quality option."""
        mock_monitor = MagicMock()
        mock_monitor.data_quality_check.return_value = {"status": "CRITICAL"}
        mock_monitor_class.return_value.__enter__.return_value = mock_monitor

        with patch("sys.argv", ["unified_monitor.py", "--data - quality", "--fix - issues"]):
            result = main()

        assert result == 2  # CRITICAL status
        mock_monitor.data_quality_check.assert_called_once_with(fix_issues=True)

    @patch("tools.core.unified_monitor.SystemMonitor")
    def test_main_json_output(self, mock_monitor_class):
        """Test main function with JSON output."""
        mock_monitor = MagicMock()
        mock_monitor.quick_health_check.return_value = {"status": "HEALTHY", "timestamp": "2023 - 01 - 01"}
        mock_monitor_class.return_value.__enter__.return_value = mock_monitor

        with patch("sys.argv", ["unified_monitor.py", "--json"]):
            with patch("builtins.print") as mock_print:
                result = main()

        assert result == 0
        # Should print JSON output
        mock_print.assert_called_once()
        printed_content = mock_print.call_args[0][0]
        assert '"status": "HEALTHY"' in printed_content

    @patch("tools.core.unified_monitor.SystemMonitor")
    def test_main_keyboard_interrupt(self, mock_monitor_class):
        """Test main function handles keyboard interrupt gracefully."""
        mock_monitor = MagicMock()
        mock_monitor.quick_health_check.side_effect = KeyboardInterrupt()
        mock_monitor_class.return_value.__enter__.return_value = mock_monitor

        with patch("sys.argv", ["unified_monitor.py"]):
            result = main()

        assert result == 1


class TestReportPrinting:
    """Test report printing functionality."""

    def test_print_monitoring_report_basic(self):
        """Test basic monitoring report printing."""
        result = {
            "timestamp": "2023 - 01 - 01T12:00:00",
            "check_type": "quick_health",
            "status": "HEALTHY",
            "summary": {"total_checks": 5, "passed": 5, "overall_status": "HEALTHY"},
        }

        with patch("builtins.print") as mock_print:
            print_monitoring_report(result)

        # Verify report sections were printed
        printed_calls = [str(call.args[0]) if call.args else str(call) for call in mock_print.call_args_list]
        printed_content = "\n".join(printed_calls)

        assert "UNIFIED SYSTEM MONITORING REPORT" in printed_content
        assert "Status: HEALTHY" in printed_content
        assert "SUMMARY:" in printed_content

    def test_print_monitoring_report_with_issues(self):
        """Test monitoring report printing with issues."""
        result = {
            "timestamp": "2023 - 01 - 01T12:00:00",
            "check_type": "data_quality",
            "status": "WARNING",
            "issues": [
                {"severity": "HIGH", "description": "Missing video titles", "fix_suggestion": "Investigate data source"}
            ],
            "recommendations": ["Fix data quality issues", "Implement validation"],
        }

        with patch("builtins.print") as mock_print:
            print_monitoring_report(result, verbose=True)

        printed_calls = [str(call.args[0]) if call.args else str(call) for call in mock_print.call_args_list]
        printed_content = "\n".join(printed_calls)

        assert "ISSUES FOUND:" in printed_content
        assert "Missing video titles" in printed_content
        assert "RECOMMENDATIONS:" in printed_content
        assert "Fix data quality issues" in printed_content


class TestIntegration:
    """Integration tests for the unified monitoring tool."""

    def test_tool_registration(self):
        """Test that the tool registers itself properly."""
        from tools.shared.common import find_tool

        # Create tool instance (should register itself)
        monitor = SystemMonitor()

        # Find the registered tool
        found_tool = find_tool("unified - monitor")

        assert found_tool is not None
        assert found_tool.name == "unified - monitor"
        assert found_tool.version == "1.0.0"
        assert found_tool.category == "core"

    def test_context_manager_usage(self):
        """Test tool can be used as context manager."""
        cleanup_called = False

        class TestMonitor(SystemMonitor):
            def cleanup_resources(self):
                nonlocal cleanup_called
                cleanup_called = True

        with TestMonitor() as monitor:
            assert monitor.name == "unified - monitor"

        assert cleanup_called
