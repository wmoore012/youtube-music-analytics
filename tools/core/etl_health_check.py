#!/usr / bin / env python3
"""
ETL System Health Check - YouTube Analytics Platform

This script performs comprehensive health checks on the ETL pipeline to ensure:
1. Database connectivity and schema validation
2. API credentials and rate limit status
3. Data freshness validation (< 24 hours)
4. System dependencies and configuration
5. Error reporting with clear recovery instructions

Usage:
    python tools / etl / etl_health_check.py
    python tools / etl / etl_health_check.py --verbose
    python tools / etl / etl_health_check.py --fix - issues
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import pymysql
import requests

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from web.etl_helpers import get_connection, get_engine


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""

    component: str
    status: str  # "PASS", "WARN", "FAIL"
    message: str
    details: Dict = field(default_factory=dict)
    recovery_instructions: List[str] = field(default_factory=list)


@dataclass
class SystemHealthReport:
    """Complete system health report."""

    timestamp: datetime
    overall_status: str
    results: List[HealthCheckResult]
    summary: Dict = field(default_factory=dict)


class ETLHealthChecker:
    """Comprehensive ETL system health checker."""

    def __init__(self, verbose: bool = False, fix_issues: bool = False):
        self.verbose = verbose
        self.fix_issues = fix_issues
        self.logger = self._setup_logging()
        self.results: List[HealthCheckResult] = []

        # Load environment variables
        load_dotenv(PROJECT_ROOT / ".env")

    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        level = logging.DEBUG if self.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        return logging.getLogger(__name__)

    def _add_result(
        self, component: str, status: str, message: str, details: Dict = None, recovery_instructions: List[str] = None
    ):
        """Add a health check result."""
        result = HealthCheckResult(
            component=component,
            status=status,
            message=message,
            details=details or {},
            recovery_instructions=recovery_instructions or [],
        )
        self.results.append(result)

        # Log the result
        log_level = {"PASS": logging.INFO, "WARN": logging.WARNING, "FAIL": logging.ERROR}.get(status, logging.INFO)

        self.logger.log(log_level, f"{component}: {message}")

        if self.verbose and details:
            for key, value in details.items():
                self.logger.debug(f"  {key}: {value}")

    def check_environment_variables(self) -> None:
        """Check required environment variables."""
        self.logger.info("Checking environment variables...")

        required_vars = {
            "DB_HOST": "Database host",
            "DB_PORT": "Database port",
            "DB_USER": "Database username",
            "DB_PASS": "Database password",
            "DB_NAME": "Database name",
            "YOUTUBE_API_KEY": "YouTube API key",
        }

        missing_vars = []
        present_vars = {}

        for var, description in required_vars.items():
            value = os.getenv(var)
            if not value:
                missing_vars.append(f"{var} ({description})")
            else:
                # Mask sensitive values for logging
                if "PASS" in var or "KEY" in var:
                    present_vars[var] = f"{'*' * (len(value) - 4)}{value[-4:]}"
                else:
                    present_vars[var] = value

        if missing_vars:
            self._add_result(
                "Environment Variables",
                "FAIL",
                f"Missing required environment variables: {', '.join(missing_vars)}",
                {"missing_count": len(missing_vars), "present_vars": present_vars},
                [
                    "Copy .env.example to .env",
                    "Fill in all required environment variables",
                    "Ensure YOUTUBE_API_KEY is valid and has quota remaining",
                ],
            )
        else:
            self._add_result(
                "Environment Variables",
                "PASS",
                "All required environment variables are present",
                {"present_vars": present_vars},
            )

    def check_database_connectivity(self) -> bool:
        """Check database connection and basic functionality."""
        self.logger.info("Checking database connectivity...")

        try:
            # Test SQLAlchemy engine connection
            engine = get_engine()
            with engine.connect() as conn:
                from sqlalchemy import text

                result = conn.execute(text("SELECT 1 as test")).fetchone()
                if result[0] != 1:
                    raise Exception("Database connection test failed")

            # Test pymysql connection (used by ETL)
            import pymysql

            conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME"),
                charset="utf8mb4",
            )
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
            finally:
                conn.close()

            self._add_result(
                "Database Connectivity",
                "PASS",
                "Database connection successful",
                {"mysql_version": version, "host": os.getenv("DB_HOST"), "database": os.getenv("DB_NAME")},
            )
            return True

        except Exception as e:
            self._add_result(
                "Database Connectivity",
                "FAIL",
                f"Database connection failed: {str(e)}",
                {"error_type": type(e).__name__},
                [
                    "Verify database server is running",
                    "Check DB_HOST, DB_PORT, DB_USER, DB_PASS in .env",
                    "Ensure database exists and user has proper permissions",
                    "Test connection manually: mysql -h HOST -u USER -p DATABASE",
                ],
            )
            return False

    def check_database_schema(self) -> None:
        """Validate database schema and required tables."""
        self.logger.info("Checking database schema...")

        required_tables = [
            "youtube_videos",
            "youtube_videos_raw",
            "youtube_comments",
            "youtube_metrics",
            "youtube_etl_runs",
            "youtube_playlists_raw",
            "comment_sentiment",
            "artist_performance_summary",
        ]

        try:
            import pymysql

            conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME"),
                charset="utf8mb4",
            )
            try:
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                existing_tables = [row[0] for row in cursor.fetchall()]

                missing_tables = [table for table in required_tables if table not in existing_tables]

                if missing_tables:
                    self._add_result(
                        "Database Schema",
                        "FAIL",
                        f"Missing required tables: {', '.join(missing_tables)}",
                        {
                            "missing_tables": missing_tables,
                            "existing_tables": existing_tables,
                            "total_tables": len(existing_tables),
                        },
                        [
                            "Run database schema creation script: mysql < yt_proj.sql",
                            "Or use tools / setup / create_tables.py if available",
                            "Ensure all ETL tables are properly created",
                        ],
                    )
                else:
                    # Check table structures for key columns
                    table_health = {}
                    for table in required_tables:
                        cursor.execute(f"DESCRIBE {table}")
                        columns = [row[0] for row in cursor.fetchall()]
                        table_health[table] = len(columns)

                    self._add_result(
                        "Database Schema",
                        "PASS",
                        f"All {len(required_tables)} required tables exist",
                        {"table_count": len(existing_tables), "table_health": table_health},
                    )
            finally:
                conn.close()

        except Exception as e:
            self._add_result(
                "Database Schema",
                "FAIL",
                f"Schema validation failed: {str(e)}",
                {"error_type": type(e).__name__},
                [
                    "Check database permissions for SHOW TABLES and DESCRIBE",
                    "Verify database schema is properly initialized",
                ],
            )

    def check_youtube_api_credentials(self) -> bool:
        """Check YouTube API credentials and quota status."""
        self.logger.info("Checking YouTube API credentials...")

        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key:
            self._add_result(
                "YouTube API",
                "FAIL",
                "YOUTUBE_API_KEY not found in environment",
                {},
                [
                    "Set YOUTUBE_API_KEY in .env file",
                    "Get API key from Google Cloud Console",
                    "Enable YouTube Data API v3 for your project",
                ],
            )
            return False

        try:
            # Test API with a simple request
            url = "https://www.googleapis.com / youtube / v3 / search"
            params = {"key": api_key, "part": "snippet", "q": "test", "type": "video", "maxResults": 1}

            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                _data = response.json()
                quota_info = response.headers.get("X - RateLimit - Remaining", "Unknown")

                self._add_result(
                    "YouTube API",
                    "PASS",
                    "YouTube API credentials valid and working",
                    {
                        "api_key_suffix": f"...{api_key[-8:]}",
                        "quota_remaining": quota_info,
                        "response_time_ms": response.elapsed.total_seconds() * 1000,
                    },
                )
                return True

            elif response.status_code == 403:
                error_data = response.json()
                error_reason = error_data.get("error", {}).get("errors", [{}])[0].get("reason", "unknown")

                if error_reason == "quotaExceeded":
                    self._add_result(
                        "YouTube API",
                        "WARN",
                        "YouTube API quota exceeded",
                        {"error_reason": error_reason, "status_code": 403},
                        [
                            "Wait for quota reset (daily at midnight Pacific Time)",
                            "Consider requesting quota increase in Google Cloud Console",
                            "Optimize ETL to use fewer API calls",
                        ],
                    )
                else:
                    self._add_result(
                        "YouTube API",
                        "FAIL",
                        f"YouTube API access denied: {error_reason}",
                        {"error_reason": error_reason, "status_code": 403},
                        [
                            "Check API key permissions in Google Cloud Console",
                            "Ensure YouTube Data API v3 is enabled",
                            "Verify API key restrictions (if any) allow your IP / domain",
                        ],
                    )
                return False

            else:
                self._add_result(
                    "YouTube API",
                    "FAIL",
                    f"YouTube API request failed with status {response.status_code}",
                    {"status_code": response.status_code, "response": response.text[:200]},
                    [
                        "Check API key validity",
                        "Verify network connectivity to googleapis.com",
                        "Check for API service outages",
                    ],
                )
                return False

        except Exception as e:
            self._add_result(
                "YouTube API",
                "FAIL",
                f"YouTube API test failed: {str(e)}",
                {"error_type": type(e).__name__},
                [
                    "Check network connectivity",
                    "Verify YOUTUBE_API_KEY format",
                    "Test API manually with curl or browser",
                ],
            )
            return False

    def check_data_freshness(self) -> None:
        """Check if data is fresh (less than 24 hours old)."""
        self.logger.info("Checking data freshness...")

        try:
            import pymysql

            conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME"),
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                cursor = conn.cursor()

                # Check latest video data
                cursor.execute(
                    """
                    SELECT
                        MAX(fetched_at) as latest_fetch,
                        COUNT(*) as total_videos,
                        COUNT(DISTINCT channel_title) as unique_channels
                    FROM youtube_videos
                    WHERE fetched_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                """
                )
                video_stats = cursor.fetchone()

                # Check latest metrics data
                cursor.execute(
                    """
                    SELECT
                        MAX(fetched_at) as latest_metrics,
                        COUNT(*) as total_metrics
                    FROM youtube_metrics
                    WHERE fetched_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                """
                )
                metrics_stats = cursor.fetchone()

                # Check ETL run status
                cursor.execute(
                    """
                    SELECT
                        MAX(finished_at) as latest_run,
                        COUNT(*) as total_runs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs
                    FROM youtube_etl_runs
                    WHERE run_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                """
                )
                etl_stats = cursor.fetchone()

                # Analyze freshness
                now = datetime.now()
                freshness_issues = []

                if video_stats["latest_fetch"]:
                    video_age = now - video_stats["latest_fetch"]
                    if video_age > timedelta(hours=24):
                        freshness_issues.append(f"Video data is {video_age.days} days old")
                else:
                    freshness_issues.append("No video data found")

                if metrics_stats["latest_metrics"]:
                    metrics_age = now - metrics_stats["latest_metrics"]
                    if metrics_age > timedelta(hours=24):
                        freshness_issues.append(f"Metrics data is {metrics_age.days} days old")
                else:
                    freshness_issues.append("No metrics data found")

                if etl_stats["latest_run"]:
                    run_age = now - etl_stats["latest_run"]
                    if run_age > timedelta(hours=24):
                        freshness_issues.append(f"Last ETL run was {run_age.days} days ago")
                else:
                    freshness_issues.append("No ETL runs found")

                details = {"video_stats": video_stats, "metrics_stats": metrics_stats, "etl_stats": etl_stats}

                if freshness_issues:
                    self._add_result(
                        "Data Freshness",
                        "WARN",
                        f"Data freshness issues: {'; '.join(freshness_issues)}",
                        details,
                        [
                            "Run ETL pipeline: python tools / etl / run_focused_etl.py",
                            "Check ETL scheduling (cron jobs, etc.)",
                            "Verify no ETL errors in recent runs",
                            "Check YouTube API quota availability",
                        ],
                    )
                else:
                    self._add_result("Data Freshness", "PASS", "All data is fresh (< 24 hours old)", details)
            finally:
                conn.close()

        except Exception as e:
            self._add_result(
                "Data Freshness",
                "FAIL",
                f"Data freshness check failed: {str(e)}",
                {"error_type": type(e).__name__},
                [
                    "Check database connectivity",
                    "Verify table structures are correct",
                    "Run basic ETL pipeline to populate data",
                ],
            )

    def check_etl_components(self) -> None:
        """Check ETL pipeline components and dependencies."""
        self.logger.info("Checking ETL components...")

        # Check critical ETL files exist
        etl_files = [
            "web / youtube_channel_etl.py",
            "web / etl_helpers.py",
            "web / sentiment_job.py",
            "tools / etl / run_focused_etl.py",
        ]

        missing_files = []
        for file_path in etl_files:
            full_path = PROJECT_ROOT / file_path
            if not full_path.exists():
                missing_files.append(file_path)

        if missing_files:
            self._add_result(
                "ETL Components",
                "FAIL",
                f"Missing ETL files: {', '.join(missing_files)}",
                {"missing_files": missing_files},
                [
                    "Ensure all ETL files are present in the repository",
                    "Check git status for missing or moved files",
                    "Restore missing files from backup or repository",
                ],
            )
        else:
            # Test imports
            try:
                from web.etl_helpers import get_engine
                from web.youtube_channel_etl import YouTubeChannelETL

                self._add_result(
                    "ETL Components",
                    "PASS",
                    "All ETL components present and importable",
                    {"checked_files": len(etl_files)},
                )
            except ImportError as e:
                self._add_result(
                    "ETL Components",
                    "FAIL",
                    f"ETL import error: {str(e)}",
                    {"error_type": type(e).__name__},
                    [
                        "Check Python path and module structure",
                        "Install missing dependencies: pip install -r requirements.txt",
                        "Verify all imports in ETL files are correct",
                    ],
                )

    def check_system_dependencies(self) -> None:
        """Check system dependencies and Python packages."""
        self.logger.info("Checking system dependencies...")

        required_packages = ["pymysql", "requests", "pandas", "sqlalchemy", "plotly", "altair", "python - dotenv"]

        missing_packages = []
        package_versions = {}

        for package in required_packages:
            try:
                module = __import__(package.replace("-", "_"))
                version = getattr(module, "__version__", "unknown")
                package_versions[package] = version
            except ImportError:
                missing_packages.append(package)

        if missing_packages:
            self._add_result(
                "System Dependencies",
                "FAIL",
                f"Missing Python packages: {', '.join(missing_packages)}",
                {"missing_packages": missing_packages, "installed_packages": package_versions},
                [
                    "Install missing packages: pip install " + " ".join(missing_packages),
                    "Or install all requirements: pip install -r requirements.txt",
                    "Consider using virtual environment for isolation",
                ],
            )
        else:
            self._add_result(
                "System Dependencies",
                "PASS",
                f"All {len(required_packages)} required packages installed",
                {"package_versions": package_versions},
            )

    def run_comprehensive_health_check(self) -> SystemHealthReport:
        """Run all health checks and generate comprehensive report."""
        self.logger.info("Starting comprehensive ETL health check...")
        start_time = datetime.now()

        # Run all health checks
        self.check_environment_variables()

        db_connected = self.check_database_connectivity()
        if db_connected:
            self.check_database_schema()
            self.check_data_freshness()

        self.check_youtube_api_credentials()
        self.check_etl_components()
        self.check_system_dependencies()

        # Generate summary
        total_checks = len(self.results)
        passed = len([r for r in self.results if r.status == "PASS"])
        warnings = len([r for r in self.results if r.status == "WARN"])
        failed = len([r for r in self.results if r.status == "FAIL"])

        # Determine overall status
        if failed > 0:
            overall_status = "CRITICAL"
        elif warnings > 0:
            overall_status = "WARNING"
        else:
            overall_status = "HEALTHY"

        summary = {
            "total_checks": total_checks,
            "passed": passed,
            "warnings": warnings,
            "failed": failed,
            "success_rate": f"{(passed / total_checks * 100):.1f}%",
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
        }

        report = SystemHealthReport(
            timestamp=start_time, overall_status=overall_status, results=self.results, summary=summary
        )

        self.logger.info(f"Health check completed: {overall_status}")
        self.logger.info(f"Results: {passed} passed, {warnings} warnings, {failed} failed")

        return report

    def print_detailed_report(self, report: SystemHealthReport) -> None:
        """Print detailed health check report to console."""
        print("\n" + "=" * 80)
        print("ETL SYSTEM HEALTH CHECK REPORT")
        print("=" * 80)
        print(f"Timestamp: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Overall Status: {report.overall_status}")
        print(f"Duration: {report.summary['duration_seconds']:.2f} seconds")
        print()

        # Summary
        print("SUMMARY:")
        print(f"  Total Checks: {report.summary['total_checks']}")
        print(f"  Passed: {report.summary['passed']}")
        print(f"  Warnings: {report.summary['warnings']}")
        print(f"  Failed: {report.summary['failed']}")
        print(f"  Success Rate: {report.summary['success_rate']}")
        print()

        # Detailed results
        print("DETAILED RESULTS:")
        print("-" * 80)

        for result in report.results:
            status_symbol = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(result.status, "❓")

            print(f"{status_symbol} {result.component}: {result.message}")

            if self.verbose and result.details:
                for key, value in result.details.items():
                    print(f"    {key}: {value}")

            if result.recovery_instructions:
                print("    Recovery Instructions:")
                for instruction in result.recovery_instructions:
                    print(f"      • {instruction}")
            print()

        # Final recommendations
        if report.overall_status != "HEALTHY":
            print("RECOMMENDED ACTIONS:")
            print("-" * 80)

            failed_components = [r for r in report.results if r.status == "FAIL"]
            if failed_components:
                print("CRITICAL ISSUES (must fix before running ETL):")
                for result in failed_components:
                    print(f"  • {result.component}: {result.message}")
                    for instruction in result.recovery_instructions[:2]:  # Show top 2
                        print(f"    - {instruction}")
                print()

            warning_components = [r for r in report.results if r.status == "WARN"]
            if warning_components:
                print("WARNINGS (should address soon):")
                for result in warning_components:
                    print(f"  • {result.component}: {result.message}")
                print()

        print("=" * 80)


def main():
    """Main entry point for health check script."""
    import argparse

    parser = argparse.ArgumentParser(description="ETL System Health Check")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument(
        "--fix - issues", action="store_true", help="Attempt to fix issues automatically (future feature)"
    )
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")

    args = parser.parse_args()

    # Run health check
    checker = ETLHealthChecker(verbose=args.verbose, fix_issues=args.fix_issues)
    report = checker.run_comprehensive_health_check()

    if args.json:
        # Output JSON for programmatic use
        report_dict = {
            "timestamp": report.timestamp.isoformat(),
            "overall_status": report.overall_status,
            "summary": report.summary,
            "results": [
                {
                    "component": r.component,
                    "status": r.status,
                    "message": r.message,
                    "details": r.details,
                    "recovery_instructions": r.recovery_instructions,
                }
                for r in report.results
            ],
        }
        print(json.dumps(report_dict, indent=2))
    else:
        # Print human - readable report
        checker.print_detailed_report(report)

    # Exit with appropriate code
    exit_code = {"HEALTHY": 0, "WARNING": 1, "CRITICAL": 2}.get(report.overall_status, 2)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
