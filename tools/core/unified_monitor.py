#!/usr / bin / env python3
"""
🔍 Unified YouTube Analytics Monitoring Tool

Consolidates all monitoring functionality into a single, comprehensive tool that provides:
- Data quality monitoring and validation
- ETL pipeline health checks and status
- System resource monitoring and alerts
- Enterprise-grade monitoring with SLA tracking
- Sentiment analysis monitoring and bot detection
- Performance metrics and trend analysis

Usage:
    python tools / core / unified_monitor.py                    # Quick health check
    python tools / core / unified_monitor.py --full-check       # Complete system check
    python tools / core / unified_monitor.py --data-quality     # Data quality report
    python tools / core / unified_monitor.py --etl-status       # ETL pipeline status
    python tools / core / unified_monitor.py --performance      # Performance metrics
    python tools / core / unified_monitor.py --enterprise       # Enterprise dashboard
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import (
    ToolBase,
    ToolConfig,
    register_tool,
)


class SystemMonitor(ToolBase):
    """
    Unified system monitoring tool that consolidates all monitoring functionality.

    This tool provides comprehensive monitoring capabilities:
    - Data quality validation and reporting
    - ETL pipeline health checks and status tracking
    - System resource monitoring and performance metrics
    - Enterprise-grade SLA monitoring and alerting
    - Sentiment analysis monitoring and bot detection
    - Automated issue detection and recovery suggestions
    """

    def __init__(self):
        super().__init__(name="unified-monitor", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        # Monitoring state and metrics
        self.monitoring_session_id = f"monitor_{datetime.now().strftime('%Y % m%d_ % H%M % S')}"
        self.metrics = {
            "session_id": self.monitoring_session_id,
            "start_time": datetime.now().isoformat(),
            "checks_performed": [],
            "issues_found": [],
            "overall_status": "UNKNOWN",
        }

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return ["DB_HOST", "DB_USER", "DB_NAME"]

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="unified-monitor",
            version="1.0.0",
            description="Unified YouTube Analytics system monitoring tool",
            dependencies=[
                "python>=3.8",
                "pymysql",
                "sqlalchemy",
                "pandas",
                "requests",
            ],
            environment_vars=[
                "DB_HOST",
                "DB_USER",
                "DB_NAME",
                "YOUTUBE_API_KEY",
            ],
            usage_examples=[
                "python tools / core / unified_monitor.py --full-check",
                "python tools / core / unified_monitor.py --data-quality",
                "python tools / core / unified_monitor.py --etl-status",
            ],
            category="core",
        )

    def run(self) -> None:
        """Main execution method-should not be called directly, use specific monitoring methods."""
        self.log_progress("Use specific monitoring methods like quick_health_check() or full_system_check()")

    def quick_health_check(self, days: int = 30) -> Dict[str, Any]:
        """
        Perform quick system health check.

        Args:
            days: Number of days to analyze for trends

        Returns:
            Dictionary with health check results
        """
        self.log_progress("⚡ Running quick health check")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "check_type": "quick_health",
                "status": "HEALTHY",
                "components": {},
                "summary": {},
            }

            # Database connectivity
            db_status = self._check_database_connectivity()
            results["components"]["database"] = db_status

            # Basic data counts
            data_counts = self._get_basic_data_counts()
            results["components"]["data_counts"] = data_counts

            # Quick consistency check
            consistency = self._quick_consistency_check(days)
            results["components"]["consistency"] = consistency

            # ETL status
            etl_status = self._check_etl_status()
            results["components"]["etl_status"] = etl_status

            # Determine overall status
            component_statuses = [comp.get("status", "UNKNOWN") for comp in results["components"].values()]
            if "CRITICAL" in component_statuses:
                results["status"] = "CRITICAL"
            elif "WARNING" in component_statuses:
                results["status"] = "WARNING"
            else:
                results["status"] = "HEALTHY"

            # Generate summary
            results["summary"] = {
                "total_videos": data_counts.get("videos", 0),
                "total_comments": data_counts.get("comments", 0),
                "database_status": db_status.get("status", "UNKNOWN"),
                "etl_runs_today": etl_status.get("runs_today", 0),
                "overall_status": results["status"],
            }

            self.metrics["checks_performed"].append("quick_health")
            self.metrics["overall_status"] = results["status"]

            return results

        except Exception as e:
            self.handle_error(e, "quick health check")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def data_quality_check(self, fix_issues: bool = False) -> Dict[str, Any]:
        """
        Perform comprehensive data quality validation.

        Args:
            fix_issues: Whether to attempt automatic fixes

        Returns:
            Dictionary with data quality results
        """
        self.log_progress("🔍 Running data quality validation")

        try:
            from tools.core.data_quality_validator import DataQualityValidator

            # Create validator instance
            validator = DataQualityValidator(fix_issues=fix_issues, report_only=False)

            # Run comprehensive validation
            report = validator.run_comprehensive_validation()

            # Convert to our format
            results = {
                "timestamp": report.timestamp.isoformat(),
                "check_type": "data_quality",
                "overall_score": report.overall_score,
                "status": self._score_to_status(report.overall_score),
                "issues": [
                    {
                        "category": issue.category,
                        "severity": issue.severity,
                        "table": issue.table,
                        "type": issue.issue_type,
                        "description": issue.description,
                        "count": issue.count,
                        "fix_suggestion": issue.fix_suggestion,
                        "auto_fixable": issue.auto_fixable,
                    }
                    for issue in report.issues
                ],
                "statistics": report.statistics,
                "recommendations": report.recommendations,
            }

            self.metrics["checks_performed"].append("data_quality")
            self.metrics["issues_found"].extend([f"{issue.category}: {issue.description}" for issue in report.issues])

            return results

        except Exception as e:
            self.handle_error(e, "data quality check")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def etl_health_check(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Perform comprehensive ETL system health check.

        Args:
            verbose: Whether to include detailed information

        Returns:
            Dictionary with ETL health results
        """
        self.log_progress("⚙️ Running ETL health check")

        try:
            from tools.core.etl_health_check import ETLHealthChecker

            # Create health checker instance
            checker = ETLHealthChecker(verbose=verbose, fix_issues=False)

            # Run comprehensive health check
            report = checker.run_comprehensive_health_check()

            # Convert to our format
            results = {
                "timestamp": report.timestamp.isoformat(),
                "check_type": "etl_health",
                "overall_status": report.overall_status,
                "status": report.overall_status,
                "summary": report.summary,
                "results": [
                    {
                        "component": result.component,
                        "status": result.status,
                        "message": result.message,
                        "details": result.details,
                        "recovery_instructions": result.recovery_instructions,
                    }
                    for result in report.results
                ],
            }

            self.metrics["checks_performed"].append("etl_health")

            # Add issues to metrics
            failed_checks = [r for r in report.results if r.status == "FAIL"]
            self.metrics["issues_found"].extend([f"ETL {check.component}: {check.message}" for check in failed_checks])

            return results

        except Exception as e:
            self.handle_error(e, "ETL health check")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def performance_monitoring(self, days: int = 7) -> Dict[str, Any]:
        """
        Monitor system performance metrics and trends.

        Args:
            days: Number of days to analyze for trends

        Returns:
            Dictionary with performance metrics
        """
        self.log_progress(f"📊 Monitoring performance metrics ({days} days)")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "check_type": "performance",
                "analysis_period_days": days,
                "status": "HEALTHY",
                "metrics": {},
                "trends": {},
                "alerts": [],
            }

            # Database performance metrics
            db_metrics = self._get_database_performance_metrics(days)
            results["metrics"]["database"] = db_metrics

            # ETL performance metrics
            etl_metrics = self._get_etl_performance_metrics(days)
            results["metrics"]["etl"] = etl_metrics

            # Data growth trends
            growth_trends = self._analyze_data_growth_trends(days)
            results["trends"]["data_growth"] = growth_trends

            # API usage metrics
            api_metrics = self._get_api_usage_metrics(days)
            results["metrics"]["api_usage"] = api_metrics

            # Generate performance alerts
            alerts = self._generate_performance_alerts(results["metrics"], results["trends"])
            results["alerts"] = alerts

            # Determine overall status
            if any(alert["severity"] == "CRITICAL" for alert in alerts):
                results["status"] = "CRITICAL"
            elif any(alert["severity"] == "HIGH" for alert in alerts):
                results["status"] = "WARNING"
            else:
                results["status"] = "HEALTHY"

            self.metrics["checks_performed"].append("performance")

            return results

        except Exception as e:
            self.handle_error(e, "performance monitoring")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def enterprise_monitoring(self, include_sla: bool = True) -> Dict[str, Any]:
        """
        Enterprise-grade monitoring with SLA tracking and alerting.

        Args:
            include_sla: Whether to include SLA compliance monitoring

        Returns:
            Dictionary with enterprise monitoring results
        """
        self.log_progress("🏢 Running enterprise monitoring")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "check_type": "enterprise",
                "monitoring_session": self.monitoring_session_id,
                "status": "HEALTHY",
                "sla_compliance": {},
                "service_health": {},
                "executive_summary": {},
                "alerts": [],
            }

            # Service health monitoring
            service_health = self._monitor_service_health()
            results["service_health"] = service_health

            # SLA compliance monitoring
            if include_sla:
                sla_compliance = self._monitor_sla_compliance()
                results["sla_compliance"] = sla_compliance

            # Generate executive summary
            executive_summary = self._generate_executive_summary(service_health, results.get("sla_compliance", {}))
            results["executive_summary"] = executive_summary

            # Enterprise alerting
            alerts = self._generate_enterprise_alerts(service_health, results.get("sla_compliance", {}))
            results["alerts"] = alerts

            # Determine overall status
            if executive_summary.get("critical_issues", 0) > 0:
                results["status"] = "CRITICAL"
            elif executive_summary.get("warnings", 0) > 0:
                results["status"] = "WARNING"
            else:
                results["status"] = "HEALTHY"

            self.metrics["checks_performed"].append("enterprise")

            return results

        except Exception as e:
            self.handle_error(e, "enterprise monitoring")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def sentiment_monitoring(self) -> Dict[str, Any]:
        """
        Monitor sentiment analysis performance and bot detection.

        Returns:
            Dictionary with sentiment monitoring results
        """
        self.log_progress("💭 Monitoring sentiment analysis")

        try:
            from tools.specialized.analytics.sentiment_monitoring import SentimentMonitor

            # Create sentiment monitor instance
            monitor = SentimentMonitor()

            # Generate health dashboard
            dashboard = monitor.generate_health_dashboard()

            # Convert to our format
            results = {
                "timestamp": dashboard["timestamp"],
                "check_type": "sentiment",
                "status": self._convert_sentiment_status(dashboard.get("overall_health_score", 0)),
                "sentiment_accuracy": dashboard.get("sentiment_accuracy", {}),
                "bot_detection": dashboard.get("bot_detection", {}),
                "data_quality": dashboard.get("data_quality", {}),
                "alerts": dashboard.get("alerts", []),
                "overall_health_score": dashboard.get("overall_health_score", 0),
            }

            self.metrics["checks_performed"].append("sentiment")

            return results

        except Exception as e:
            self.handle_error(e, "sentiment monitoring")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def full_system_check(self, days: int = 7, fix_issues: bool = False) -> Dict[str, Any]:
        """
        Perform comprehensive system monitoring across all components.

        Args:
            days: Number of days to analyze for trends
            fix_issues: Whether to attempt automatic fixes

        Returns:
            Dictionary with complete system status
        """
        self.log_progress("🔬 Running full system check")

        try:
            results = {
                "timestamp": datetime.now().isoformat(),
                "check_type": "full_system",
                "monitoring_session": self.monitoring_session_id,
                "analysis_period_days": days,
                "status": "HEALTHY",
                "components": {},
                "summary": {},
                "recommendations": [],
            }

            # Run all monitoring checks
            checks = [
                ("quick_health", lambda: self.quick_health_check(days)),
                ("data_quality", lambda: self.data_quality_check(fix_issues)),
                ("etl_health", lambda: self.etl_health_check(verbose=False)),
                ("performance", lambda: self.performance_monitoring(days)),
                ("sentiment", lambda: self.sentiment_monitoring()),
            ]

            component_statuses = []

            for check_name, check_func in checks:
                self.log_progress(f"Running {check_name} check")
                try:
                    check_result = check_func()
                    results["components"][check_name] = check_result

                    # Track component status
                    status = check_result.get("status", "UNKNOWN")
                    component_statuses.append(status)

                    if status == "HEALTHY":
                        self.log_progress(f"✅ {check_name}: PASSED")
                    elif status == "WARNING":
                        self.log_progress(f"⚠️ {check_name}: WARNING")
                    else:
                        self.log_progress(f"❌ {check_name}: FAILED")

                except Exception as e:
                    self.log_progress(f"❌ {check_name}: ERROR - {e}", level="ERROR")
                    results["components"][check_name] = {
                        "status": "ERROR",
                        "error": str(e),
                    }
                    component_statuses.append("ERROR")

            # Determine overall status
            if "CRITICAL" in component_statuses or "ERROR" in component_statuses:
                results["status"] = "CRITICAL"
            elif "WARNING" in component_statuses:
                results["status"] = "WARNING"
            else:
                results["status"] = "HEALTHY"

            # Generate summary
            passed = len([s for s in component_statuses if s == "HEALTHY"])
            warnings = len([s for s in component_statuses if s == "WARNING"])
            failed = len([s for s in component_statuses if s in ["CRITICAL", "ERROR"]])

            results["summary"] = {
                "total_checks": len(checks),
                "passed": passed,
                "warnings": warnings,
                "failed": failed,
                "success_rate": f"{(passed / len(checks) * 100):.1f}%",
                "overall_status": results["status"],
            }

            # Generate recommendations
            results["recommendations"] = self._generate_system_recommendations(results["components"])

            # Update metrics
            self.metrics["overall_status"] = results["status"]

            return results

        except Exception as e:
            self.handle_error(e, "full system check")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "ERROR",
                "error": str(e),
            }

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring session status."""
        return {
            "monitoring_session": self.monitoring_session_id,
            "metrics": self.metrics.copy(),
            "timestamp": datetime.now().isoformat(),
        }

    # Helper methods for monitoring functionality

    def _check_database_connectivity(self) -> Dict[str, Any]:
        """Check database connectivity and basic functionality."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            return {
                "status": "HEALTHY",
                "message": "Database connection successful",
                "host": self.get_config_value("DB_HOST", "unknown"),
                "database": self.get_config_value("DB_NAME", "unknown"),
            }

        except Exception as e:
            return {
                "status": "CRITICAL",
                "message": f"Database connection failed: {e}",
                "error": str(e),
            }

    def _get_basic_data_counts(self) -> Dict[str, Any]:
        """Get basic data counts from key tables."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            counts = {}

            tables = {
                "videos": "youtube_videos",
                "comments": "youtube_comments",
                "metrics": "youtube_metrics",
                "sentiment": "comment_sentiment",
            }

            with engine.connect() as conn:
                for name, table in tables.items():
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        counts[name] = count
                    except Exception:
                        counts[name] = 0

            return {
                "status": "HEALTHY" if sum(counts.values()) > 0 else "WARNING",
                "counts": counts,
                "total_records": sum(counts.values()),
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _quick_consistency_check(self, days: int) -> Dict[str, Any]:
        """Perform quick data consistency check."""
        try:
            from src.youtubeviz.data import qa_artist_consistency_check
            from web.etl_helpers import get_engine

            engine = get_engine()
            result = qa_artist_consistency_check(days=days, engine=engine)

            if result["status"] == "success" and result["consistent"]:
                return {
                    "status": "HEALTHY",
                    "message": f"Artist consistency check passed ({result['data_artists']} artists)",
                    "details": result,
                }
            else:
                return {
                    "status": "WARNING",
                    "message": f"Consistency issues detected: {result.get('message', 'Unknown issue')}",
                    "details": result,
                }

        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"Consistency check failed: {e}",
                "error": str(e),
            }

    def _check_etl_status(self) -> Dict[str, Any]:
        """Check ETL pipeline status and recent runs."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            with engine.connect() as conn:
                # Check recent ETL runs
                recent_runs = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) as total_runs,
                           SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs
                    FROM youtube_etl_runs
                    WHERE run_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                """
                    )
                ).fetchone()

                # Check runs today
                runs_today = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM youtube_etl_runs
                    WHERE run_date = CURDATE()
                """
                    )
                ).scalar()

                if recent_runs and recent_runs.total_runs > 0:
                    success_rate = (recent_runs.successful_runs / recent_runs.total_runs) * 100

                    if success_rate >= 90:
                        status = "HEALTHY"
                    elif success_rate >= 70:
                        status = "WARNING"
                    else:
                        status = "CRITICAL"

                    return {
                        "status": status,
                        "message": f"ETL success rate: {success_rate:.1f}% (last 7 days)",
                        "total_runs": recent_runs.total_runs,
                        "successful_runs": recent_runs.successful_runs,
                        "runs_today": runs_today,
                        "success_rate": success_rate,
                    }
                else:
                    return {
                        "status": "WARNING",
                        "message": "No recent ETL runs found",
                        "runs_today": runs_today,
                    }

        except Exception as e:
            return {
                "status": "ERROR",
                "message": f"ETL status check failed: {e}",
                "error": str(e),
            }

    def _get_database_performance_metrics(self, days: int) -> Dict[str, Any]:
        """Get database performance metrics."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            with engine.connect() as conn:
                # Query performance metrics
                metrics = {}

                # Table sizes
                table_sizes = conn.execute(
                    text(
                        """
                    SELECT table_name,
                           ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                    FROM information_schema.TABLES
                    WHERE table_schema = DATABASE()
                    ORDER BY size_mb DESC
                    LIMIT 10
                """
                    )
                ).fetchall()

                metrics["table_sizes"] = [
                    {"table": row.table_name, "size_mb": float(row.size_mb)} for row in table_sizes
                ]

                # Connection count
                connections = conn.execute(text("SHOW STATUS LIKE 'Threads_connected'")).fetchone()
                if connections:
                    metrics["active_connections"] = int(connections.Value)

                return {
                    "status": "HEALTHY",
                    "metrics": metrics,
                }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _get_etl_performance_metrics(self, days: int) -> Dict[str, Any]:
        """Get ETL performance metrics."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            with engine.connect() as conn:
                # ETL performance metrics
                metrics = {}

                # Average processing time
                avg_time = conn.execute(
                    text(
                        f"""
                    SELECT AVG(TIMESTAMPDIFF(MINUTE, started_at, finished_at)) as avg_minutes
                    FROM youtube_etl_runs
                    WHERE started_at IS NOT NULL
                    AND finished_at IS NOT NULL
                    AND run_date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
                """
                    )
                ).scalar()

                if avg_time:
                    metrics["avg_processing_time_minutes"] = float(avg_time)

                # Processing volume
                volume = conn.execute(
                    text(
                        f"""
                    SELECT AVG(videos_processed) as avg_videos,
                           AVG(metrics_collected) as avg_metrics
                    FROM youtube_etl_runs
                    WHERE run_date >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
                """
                    )
                ).fetchone()

                if volume:
                    metrics["avg_videos_processed"] = float(volume.avg_videos or 0)
                    metrics["avg_metrics_collected"] = float(volume.avg_metrics or 0)

                return {
                    "status": "HEALTHY",
                    "metrics": metrics,
                }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _analyze_data_growth_trends(self, days: int) -> Dict[str, Any]:
        """Analyze data growth trends."""
        try:
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()

            with engine.connect() as conn:
                # Daily growth rates
                growth = conn.execute(
                    text(
                        f"""
                    SELECT DATE(fetched_at) as date,
                           COUNT(*) as daily_videos
                    FROM youtube_videos
                    WHERE fetched_at >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
                    GROUP BY DATE(fetched_at)
                    ORDER BY date DESC
                    LIMIT 7
                """
                    )
                ).fetchall()

                trends = {
                    "daily_video_growth": [{"date": row.date.isoformat(), "count": row.daily_videos} for row in growth]
                }

                # Calculate growth rate
                if len(trends["daily_video_growth"]) >= 2:
                    recent = trends["daily_video_growth"][0]["count"]
                    previous = trends["daily_video_growth"][1]["count"]
                    growth_rate = ((recent-previous) / previous * 100) if previous > 0 else 0
                    trends["growth_rate_percent"] = round(growth_rate, 1)

                return {
                    "status": "HEALTHY",
                    "trends": trends,
                }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _get_api_usage_metrics(self, days: int) -> Dict[str, Any]:
        """Get API usage metrics and quota status."""
        try:
            # This would integrate with YouTube API quota monitoring
            # For now, return basic structure
            return {
                "status": "HEALTHY",
                "quota_usage": "Unknown",
                "requests_today": "Unknown",
                "message": "API monitoring not fully implemented",
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _generate_performance_alerts(self, metrics: Dict[str, Any], trends: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate performance-based alerts."""
        alerts = []

        # Check ETL performance
        etl_metrics = metrics.get("etl", {}).get("metrics", {})
        avg_time = etl_metrics.get("avg_processing_time_minutes", 0)

        if avg_time > 60:  # More than 1 hour
            alerts.append(
                {
                    "severity": "HIGH",
                    "component": "ETL Performance",
                    "message": f"ETL processing time is high: {avg_time:.1f} minutes average",
                    "recommendation": "Investigate ETL performance bottlenecks",
                }
            )

        # Check database size
        db_metrics = metrics.get("database", {}).get("metrics", {})
        table_sizes = db_metrics.get("table_sizes", [])

        for table in table_sizes:
            if table["size_mb"] > 1000:  # More than 1GB
                alerts.append(
                    {
                        "severity": "MEDIUM",
                        "component": "Database Size",
                        "message": f"Table {table['table']} is large: {table['size_mb']} MB",
                        "recommendation": "Consider data archiving or optimization",
                    }
                )

        return alerts

    def _monitor_service_health(self) -> Dict[str, Any]:
        """Monitor overall service health."""
        try:
            # Combine multiple health indicators
            health_indicators = {
                "database": self._check_database_connectivity(),
                "data_counts": self._get_basic_data_counts(),
                "etl_status": self._check_etl_status(),
            }

            # Calculate overall health score
            healthy_count = sum(1 for indicator in health_indicators.values() if indicator.get("status") == "HEALTHY")
            total_count = len(health_indicators)
            health_score = (healthy_count / total_count) * 100

            return {
                "health_score": health_score,
                "status": "HEALTHY" if health_score >= 80 else "WARNING" if health_score >= 60 else "CRITICAL",
                "indicators": health_indicators,
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _monitor_sla_compliance(self) -> Dict[str, Any]:
        """Monitor SLA compliance metrics."""
        try:
            # SLA targets (configurable)
            sla_targets = {
                "data_freshness_hours": 24,
                "etl_success_rate_percent": 95,
                "system_uptime_percent": 99.5,
            }

            compliance = {}

            # Check data freshness SLA
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            with engine.connect() as conn:
                latest_data = conn.execute(
                    text(
                        """
                    SELECT MAX(fetched_at) as latest_fetch
                    FROM youtube_videos
                """
                    )
                ).scalar()

                if latest_data:
                    hours_old = (datetime.now() - latest_data).total_seconds() / 3600
                    compliance["data_freshness"] = {
                        "target_hours": sla_targets["data_freshness_hours"],
                        "actual_hours": hours_old,
                        "compliant": hours_old <= sla_targets["data_freshness_hours"],
                    }

            # Check ETL success rate SLA
            etl_status = self._check_etl_status()
            success_rate = etl_status.get("success_rate", 0)
            compliance["etl_success_rate"] = {
                "target_percent": sla_targets["etl_success_rate_percent"],
                "actual_percent": success_rate,
                "compliant": success_rate >= sla_targets["etl_success_rate_percent"],
            }

            # Calculate overall SLA compliance
            compliant_slas = sum(1 for sla in compliance.values() if sla.get("compliant", False))
            total_slas = len(compliance)
            overall_compliance = (compliant_slas / total_slas) * 100 if total_slas > 0 else 0

            return {
                "overall_compliance_percent": overall_compliance,
                "status": (
                    "COMPLIANT"
                    if overall_compliance >= 90
                    else "AT_RISK" if overall_compliance >= 70 else "NON_COMPLIANT"
                ),
                "sla_details": compliance,
            }

        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e),
            }

    def _generate_executive_summary(
        self, service_health: Dict[str, Any], sla_compliance: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate executive summary for enterprise monitoring."""
        try:
            summary = {
                "system_health_score": service_health.get("health_score", 0),
                "sla_compliance_percent": sla_compliance.get("overall_compliance_percent", 0),
                "critical_issues": 0,
                "warnings": 0,
                "recommendations": [],
            }

            # Count issues by severity
            if service_health.get("status") == "CRITICAL":
                summary["critical_issues"] += 1
            elif service_health.get("status") == "WARNING":
                summary["warnings"] += 1

            if sla_compliance.get("status") == "NON_COMPLIANT":
                summary["critical_issues"] += 1
            elif sla_compliance.get("status") == "AT_RISK":
                summary["warnings"] += 1

            # Generate recommendations
            if summary["critical_issues"] > 0:
                summary["recommendations"].append("Immediate attention required for critical system issues")
            if summary["sla_compliance_percent"] < 90:
                summary["recommendations"].append("Review SLA compliance and implement corrective measures")
            if summary["system_health_score"] < 80:
                summary["recommendations"].append("System health monitoring indicates performance degradation")

            return summary

        except Exception as e:
            return {
                "error": str(e),
                "critical_issues": 1,
            }

    def _generate_enterprise_alerts(
        self, service_health: Dict[str, Any], sla_compliance: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate enterprise-level alerts."""
        alerts = []

        # Service health alerts
        if service_health.get("status") == "CRITICAL":
            alerts.append(
                {
                    "severity": "CRITICAL",
                    "component": "System Health",
                    "message": f"System health score is critical: {service_health.get('health_score', 0):.1f}%",
                    "action_required": "Immediate investigation and remediation required",
                }
            )

        # SLA compliance alerts
        if sla_compliance.get("status") == "NON_COMPLIANT":
            alerts.append(
                {
                    "severity": "HIGH",
                    "component": "SLA Compliance",
                    "message": f"SLA compliance below threshold: {sla_compliance.get('overall_compliance_percent',
                        0):.1f}%",  # noqa: E128
                    "action_required": "Review SLA breaches and implement corrective actions",
                }
            )

        return alerts

    def _generate_system_recommendations(self, components: Dict[str, Any]) -> List[str]:
        """Generate system-wide recommendations based on all monitoring results."""
        recommendations = []

        # Analyze component results
        failed_components = []
        warning_components = []

        for component_name, result in components.items():
            status = result.get("status", "UNKNOWN")
            if status in ["CRITICAL", "ERROR"]:
                failed_components.append(component_name)
            elif status == "WARNING":
                warning_components.append(component_name)

        # Generate recommendations based on failures
        if "data_quality" in failed_components:
            recommendations.append("Run data quality fixes and implement validation in ETL pipeline")

        if "etl_health" in failed_components:
            recommendations.append("Address ETL system issues before running production pipelines")

        if "performance" in warning_components:
            recommendations.append("Monitor system performance and consider optimization")

        if len(failed_components) > 2:
            recommendations.append("Multiple system components failing-consider comprehensive system review")

        # Add general recommendations
        if not recommendations:
            recommendations.append("System is healthy-continue regular monitoring")

        return recommendations

    # Utility methods

    def _score_to_status(self, score: float) -> str:
        """Convert numeric score to status string."""
        if score >= 80:
            return "HEALTHY"
        elif score >= 70:
            return "WARNING"
        else:
            return "CRITICAL"

    def _convert_sentiment_status(self, health_score: float) -> str:
        """Convert sentiment health score to status."""
        if health_score >= 80:
            return "HEALTHY"
        elif health_score >= 60:
            return "WARNING"
        else:
            return "CRITICAL"

    def cleanup_resources(self) -> None:
        """Clean up any resources used during monitoring."""
        # No persistent resources to clean up
        pass


def main():
    """Main entry point for the unified monitoring tool."""
    parser = argparse.ArgumentParser(
        description="Unified YouTube Analytics Monitoring Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / core / unified_monitor.py                    # Quick health check
  python tools / core / unified_monitor.py --full-check       # Complete system check
  python tools / core / unified_monitor.py --data-quality     # Data quality report
  python tools / core / unified_monitor.py --etl-status       # ETL pipeline status
  python tools / core / unified_monitor.py --performance      # Performance metrics
  python tools / core / unified_monitor.py --enterprise       # Enterprise dashboard
        """,
    )

    # Monitoring options
    parser.add_argument("--full-check", action="store_true", help="Complete system health check")
    parser.add_argument("--data-quality", action="store_true", help="Data quality validation")
    parser.add_argument("--etl-status", action="store_true", help="ETL pipeline status")
    parser.add_argument("--performance", action="store_true", help="Performance monitoring")
    parser.add_argument("--enterprise", action="store_true", help="Enterprise monitoring dashboard")
    parser.add_argument("--sentiment", action="store_true", help="Sentiment analysis monitoring")

    # Options
    parser.add_argument("--days", type=int, default=7, help="Number of days to analyze (default: 7)")
    parser.add_argument("--fix-issues", action="store_true", help="Attempt to fix issues automatically")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Create monitoring tool instance
    with SystemMonitor() as monitor:
        try:
            # Determine which check to run
            if args.full_check:
                result = monitor.full_system_check(days=args.days, fix_issues=args.fix_issues)
            elif args.data_quality:
                result = monitor.data_quality_check(fix_issues=args.fix_issues)
            elif args.etl_status:
                result = monitor.etl_health_check(verbose=args.verbose)
            elif args.performance:
                result = monitor.performance_monitoring(days=args.days)
            elif args.enterprise:
                result = monitor.enterprise_monitoring()
            elif args.sentiment:
                result = monitor.sentiment_monitoring()
            else:
                # Default: quick health check
                result = monitor.quick_health_check(days=args.days)

            # Output results
            if args.json:
                print(json.dumps(result, indent=2, default=str))
            else:
                # Human-readable output
                print_monitoring_report(result, verbose=args.verbose)

            # Exit with appropriate code
            status = result.get("status", "UNKNOWN")
            exit_code = {
                "HEALTHY": 0,
                "WARNING": 1,
                "CRITICAL": 2,
                "ERROR": 2,
            }.get(status, 2)

            return exit_code

        except KeyboardInterrupt:
            monitor.log_progress("Monitoring cancelled by user")
            return 1
        except Exception as e:
            monitor.handle_error(e, "main execution")
            return 1


def print_monitoring_report(result: Dict[str, Any], verbose: bool = False) -> None:  # noqa: C901
    """Print human-readable monitoring report."""
    print("\n" + "=" * 80)
    print("UNIFIED SYSTEM MONITORING REPORT")
    print("=" * 80)

    # Header information
    print(f"Timestamp: {result.get('timestamp', 'Unknown')}")
    print(f"Check Type: {result.get('check_type', 'Unknown').replace('_', ' ').title()}")
    print(f"Status: {result.get('status', 'Unknown')}")

    if result.get("monitoring_session"):
        print(f"Session: {result['monitoring_session']}")

    print()

    # Summary section
    if "summary" in result:
        summary = result["summary"]
        print("SUMMARY:")
        for key, value in summary.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")
        print()

    # Components section
    if "components" in result:
        print("COMPONENT STATUS:")
        print("-" * 40)

        for component_name, component_result in result["components"].items():
            status = component_result.get("status", "UNKNOWN")
            status_symbol = {
                "HEALTHY": "✅",
                "WARNING": "⚠️",
                "CRITICAL": "❌",
                "ERROR": "❌",
            }.get(status, "❓")

            print(f"{status_symbol} {component_name.replace('_', ' ').title()}: {status}")

            if verbose and "message" in component_result:
                print(f"    {component_result['message']}")
        print()

    # Issues section
    if "issues" in result and result["issues"]:
        print("ISSUES FOUND:")
        print("-" * 40)

        for issue in result["issues"][:10]:  # Show first 10 issues
            severity_symbol = {
                "CRITICAL": "🔴",
                "HIGH": "🟠",
                "MEDIUM": "🟡",
                "LOW": "🟢",
            }.get(issue.get("severity", "UNKNOWN"), "⚪")

            print(f"{severity_symbol} {issue.get('description', 'Unknown issue')}")
            if verbose and issue.get("fix_suggestion"):
                print(f"    Fix: {issue['fix_suggestion']}")

        if len(result["issues"]) > 10:
            print(f"    ... and {len(result['issues']) - 10} more issues")
        print()

    # Recommendations section
    if "recommendations" in result and result["recommendations"]:
        print("RECOMMENDATIONS:")
        print("-" * 40)
        for i, rec in enumerate(result["recommendations"], 1):
            print(f"{i}. {rec}")
        print()

    # Alerts section
    if "alerts" in result and result["alerts"]:
        print("ALERTS:")
        print("-" * 40)
        for alert in result["alerts"]:
            severity = alert.get("severity", "UNKNOWN")
            severity_symbol = {
                "CRITICAL": "🚨",
                "HIGH": "⚠️",
                "MEDIUM": "⚠️",
                "LOW": "ℹ️",
            }.get(severity, "❓")

            print(f"{severity_symbol} {alert.get('message', 'Unknown alert')}")
            if verbose and alert.get("action_required"):
                print(f"    Action: {alert['action_required']}")
        print()

    print("=" * 80)


if __name__ == "__main__":
    sys.exit(main())
