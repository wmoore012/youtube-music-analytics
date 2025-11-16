#!/usr/bin/env python3
"""
Data Quality Validation System-YouTube Analytics Platform

This script performs comprehensive data quality checks including:
1. Completeness validation (missing data detection)
2. Consistency validation (referential integrity)
3. Duplicate detection and handling
4. Anomaly detection for unusual data patterns
5. Data freshness and staleness checks

Usage:
    python tools / etl / data_quality_validator.py
    python tools / etl / data_quality_validator.py --fix-duplicates
    python tools / etl / data_quality_validator.py --report-only
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import pymysql

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text

from web.etl_helpers import get_engine


@dataclass
class DataQualityIssue:
    """Represents a data quality issue."""

    category: str
    severity: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    table: str
    issue_type: str
    description: str
    count: int
    sample_records: List[Dict] = field(default_factory=list)
    fix_suggestion: str = ""
    auto_fixable: bool = False


@dataclass
class DataQualityReport:
    """Complete data quality assessment report."""

    timestamp: datetime
    overall_score: float
    issues: List[DataQualityIssue]
    statistics: Dict[str, Any]
    recommendations: List[str]


class DataQualityValidator:
    """Comprehensive data quality validation system."""

    def __init__(self, fix_issues: bool = False, report_only: bool = False):
        self.fix_issues = fix_issues
        self.report_only = report_only
        self.logger = self._setup_logging()
        self.issues: List[DataQualityIssue] = []

        # Load environment
        load_dotenv(PROJECT_ROOT / ".env")

        # Database connection
        self.engine = get_engine()

    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        return logging.getLogger(__name__)

    def _add_issue(
        self,
        category: str,
        severity: str,
        table: str,
        issue_type: str,
        description: str,
        count: int,
        sample_records: List[Dict] = None,
        fix_suggestion: str = "",
        auto_fixable: bool = False,
    ):
        """Add a data quality issue to the report."""
        issue = DataQualityIssue(
            category=category,
            severity=severity,
            table=table,
            issue_type=issue_type,
            description=description,
            count=count,
            sample_records=sample_records or [],
            fix_suggestion=fix_suggestion,
            auto_fixable=auto_fixable,
        )
        self.issues.append(issue)

        # Log the issue
        log_level = {
            "LOW": logging.INFO,
            "MEDIUM": logging.WARNING,
            "HIGH": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }.get(severity, logging.INFO)

        self.logger.log(log_level, f"{table}.{issue_type}: {description} ({count:,} records)")

    def check_completeness(self) -> None:
        """Check for missing or incomplete data."""
        self.logger.info("Checking data completeness...")

        completeness_checks = [
            # YouTube Videos
            {
                "table": "youtube_videos",
                "checks": [
                    ("title", "Missing video titles", "Title is essential for analysis"),
                    ("channel_title", "Missing channel names", "Channel identification required"),
                    ("published_at", "Missing publish dates", "Temporal analysis requires dates"),
                    ("view_count", "Missing view counts", "Core metric for analysis"),
                ],
            },
            # YouTube Comments
            {
                "table": "youtube_comments",
                "checks": [
                    ("comment_text", "Missing comment text", "Text required for sentiment analysis"),
                    ("author_name", "Missing comment authors", "Author tracking important"),
                    ("video_id", "Missing video references", "Comments must link to videos"),
                ],
            },
            # Comment Sentiment
            {
                "table": "comment_sentiment",
                "checks": [
                    ("sentiment_score", "Missing sentiment scores", "Core sentiment analysis output"),
                    ("confidence_score", "Missing confidence scores", "Quality assessment needed"),
                ],
            },
            # YouTube Metrics
            {
                "table": "youtube_metrics",
                "checks": [
                    ("view_count", "Missing view metrics", "Essential performance data"),
                    ("metrics_date", "Missing metric dates", "Temporal tracking required"),
                ],
            },
        ]

        with self.engine.connect() as conn:
            for table_check in completeness_checks:
                table = table_check["table"]

                # Check if table exists
                try:
                    total_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    if total_count == 0:
                        self._add_issue(
                            "Completeness",
                            "HIGH",
                            table,
                            "empty_table",
                            f"Table {table} is empty",
                            0,
                            fix_suggestion="Run ETL pipeline to populate data",
                        )
                        continue
                except Exception as e:
                    self._add_issue(
                        "Completeness",
                        "CRITICAL",
                        table,
                        "missing_table",
                        f"Table {table} does not exist or is inaccessible",
                        0,
                        fix_suggestion="Create table using schema script",
                    )
                    continue

                # Check individual columns
                for column, issue_desc, importance in table_check["checks"]:
                    try:
                        null_count = conn.execute(
                            text(
                                f"""
                            SELECT COUNT(*) FROM {table}
                            WHERE {column} IS NULL OR TRIM({column}) = ''
                        """
                            )
                        ).scalar()

                        if null_count > 0:
                            null_percentage = (null_count / total_count) * 100
                            severity = (
                                "CRITICAL"
                                if null_percentage > 50
                                else "HIGH" if null_percentage > 20 else "MEDIUM" if null_percentage > 5 else "LOW"
                            )

                            # Get sample records
                            sample_query = f"""
                                SELECT * FROM {table}
                                WHERE {column} IS NULL OR TRIM({column}) = ''
                                LIMIT 3
                            """
                            sample_result = conn.execute(text(sample_query))
                            samples = [dict(row._mapping) for row in sample_result]

                            self._add_issue(
                                "Completeness",
                                severity,
                                table,
                                f"missing_{column}",
                                f"{issue_desc} ({null_percentage:.1f}% of records)",
                                null_count,
                                samples,
                                f"Investigate data source for {column} population",
                                auto_fixable=(severity == "LOW"),
                            )
                    except Exception as e:
                        self.logger.warning(f"Could not check {table}.{column}: {e}")

    def check_consistency(self) -> None:
        """Check referential integrity and data consistency."""
        self.logger.info("Checking data consistency...")

        consistency_checks = [
            # Video-Comment relationships
            {
                "name": "orphaned_comments",
                "query": """
                    SELECT COUNT(*) FROM youtube_comments yc
                    LEFT JOIN youtube_videos yv ON yc.video_id = yv.video_id
                    WHERE yv.video_id IS NULL
                """,
                "description": "Comments without corresponding videos",
                "table": "youtube_comments",
                "severity": "HIGH",
                "fix_suggestion": "Remove orphaned comments or ensure video data is complete",
            },
            # Sentiment-Comment relationships
            {
                "name": "orphaned_sentiment",
                "query": """
                    SELECT COUNT(*) FROM comment_sentiment cs
                    LEFT JOIN youtube_comments yc ON cs.comment_id = yc.comment_id
                    WHERE yc.comment_id IS NULL
                """,
                "description": "Sentiment records without corresponding comments",
                "table": "comment_sentiment",
                "severity": "MEDIUM",
                "fix_suggestion": "Clean up orphaned sentiment records",
            },
            # Metrics-Video relationships
            {
                "name": "orphaned_metrics",
                "query": """
                    SELECT COUNT(*) FROM youtube_metrics ym
                    LEFT JOIN youtube_videos yv ON ym.video_id = yv.video_id
                    WHERE yv.video_id IS NULL
                """,
                "description": "Metrics without corresponding videos",
                "table": "youtube_metrics",
                "severity": "HIGH",
                "fix_suggestion": "Ensure video data exists before collecting metrics",
            },
            # Date consistency
            {
                "name": "future_dates",
                "query": """
                    SELECT COUNT(*) FROM youtube_videos
                    WHERE published_at > NOW()
                """,
                "description": "Videos with future publication dates",
                "table": "youtube_videos",
                "severity": "MEDIUM",
                "fix_suggestion": "Verify date parsing and timezone handling",
            },
            # Negative metrics
            {
                "name": "negative_metrics",
                "query": """
                    SELECT COUNT(*) FROM youtube_metrics
                    WHERE view_count < 0 OR like_count < 0 OR comment_count < 0
                """,
                "description": "Negative metric values",
                "table": "youtube_metrics",
                "severity": "HIGH",
                "fix_suggestion": "Investigate data source and parsing logic",
            },
        ]

        with self.engine.connect() as conn:
            for check in consistency_checks:
                try:
                    count = conn.execute(text(check["query"])).scalar()
                    if count > 0:
                        self._add_issue(
                            "Consistency",
                            check["severity"],
                            check["table"],
                            check["name"],
                            check["description"],
                            count,
                            fix_suggestion=check["fix_suggestion"],
                        )
                except Exception as e:
                    self.logger.warning(f"Consistency check {check['name']} failed: {e}")

    def check_duplicates(self) -> None:
        """Detect and optionally handle duplicate records."""
        self.logger.info("Checking for duplicate records...")

        duplicate_checks = [
            # Duplicate videos
            {
                "table": "youtube_videos",
                "key_columns": ["video_id"],
                "description": "Duplicate video records",
                "severity": "HIGH",
            },
            # Duplicate comments
            {
                "table": "youtube_comments",
                "key_columns": ["comment_id"],
                "description": "Duplicate comment records",
                "severity": "MEDIUM",
            },
            # Duplicate sentiment records
            {
                "table": "comment_sentiment",
                "key_columns": ["comment_id"],
                "description": "Duplicate sentiment records",
                "severity": "MEDIUM",
            },
            # Duplicate metrics (same video, same date)
            {
                "table": "youtube_metrics",
                "key_columns": ["video_id", "metrics_date"],
                "description": "Duplicate metric records for same video / date",
                "severity": "LOW",
            },
        ]

        with self.engine.connect() as conn:
            for check in duplicate_checks:
                table = check["table"]
                key_cols = check["key_columns"]
                _key_list = ", ".join(key_cols)

                try:
                    # Find duplicates
                    duplicate_query = f"""
                        SELECT {_key_list}, COUNT(*) as duplicate_count
                        FROM {table}
                        GROUP BY {_key_list}
                        HAVING COUNT(*) > 1
                    """

                    result = conn.execute(text(duplicate_query))
                    duplicates = result.fetchall()

                    if duplicates:
                        total_duplicate_records = sum(row.duplicate_count-1 for row in duplicates)

                        # Get sample duplicates
                        samples = []
                        for row in duplicates[:3]:  # First 3 duplicate groups
                            key_conditions = " AND ".join([f"{col} = '{getattr(row, col)}'" for col in key_cols])
                            sample_query = f"SELECT * FROM {table} WHERE {key_conditions} LIMIT 2"
                            sample_result = conn.execute(text(sample_query))
                            samples.extend([dict(r._mapping) for r in sample_result])

                        self._add_issue(
                            "Duplicates",
                            check["severity"],
                            table,
                            "duplicate_records",
                            f"{check['description']} ({len(duplicates)} groups, {total_duplicate_records} excess records)",
                            total_duplicate_records,
                            samples,
                            f"Remove duplicate records keeping most recent",
                            auto_fixable=True,
                        )

                        # Auto-fix if requested
                        if self.fix_issues and check["severity"] in ["LOW", "MEDIUM"]:
                            self._fix_duplicates(conn, table, key_cols)

                except Exception as e:
                    self.logger.warning(f"Duplicate check for {table} failed: {e}")

    def _fix_duplicates(self, conn, table: str, key_columns: List[str]) -> None:
        """Fix duplicate records by keeping the most recent."""
        self.logger.info(f"Fixing duplicates in {table}...")

        try:
            # Create a temporary table with unique records
            _key_list = ", ".join(key_columns)

            # For tables with timestamps, keep the most recent
            if table in ["youtube_videos", "youtube_comments"]:
                timestamp_col = "fetched_at" if table == "youtube_videos" else "created_at"
                dedupe_query = f"""
                    DELETE t1 FROM {table} t1
                    INNER JOIN {table} t2
                    WHERE t1.{timestamp_col} < t2.{timestamp_col}
                    AND {" AND ".join([f"t1.{col} = t2.{col}" for col in key_columns])}
                """
            else:
                # For other tables, keep any one record (using row comparison)
                dedupe_query = f"""
                    DELETE t1 FROM {table} t1
                    INNER JOIN {table} t2
                    WHERE t1.id > t2.id
                    AND {" AND ".join([f"t1.{col} = t2.{col}" for col in key_columns])}
                """

            result = conn.execute(text(dedupe_query))
            deleted_count = result.rowcount
            conn.commit()

            self.logger.info(f"Removed {deleted_count} duplicate records from {table}")

        except Exception as e:
            self.logger.error(f"Failed to fix duplicates in {table}: {e}")
            conn.rollback()

    def check_anomalies(self) -> None:
        """Detect unusual data patterns and anomalies."""
        self.logger.info("Checking for data anomalies...")

        anomaly_checks = [
            # Extremely high view counts (potential data errors)
            {
                "name": "extreme_view_counts",
                "query": """
                    SELECT COUNT(*) FROM youtube_videos
                    WHERE view_count > 10000000000
                """,
                "description": "Videos with unrealistically high view counts (>10B)",
                "table": "youtube_videos",
                "severity": "MEDIUM",
            },
            # Comments much longer than typical
            {
                "name": "extremely_long_comments",
                "query": """
                    SELECT COUNT(*) FROM youtube_comments
                    WHERE LENGTH(comment_text) > 5000
                """,
                "description": "Unusually long comments (>5000 characters)",
                "table": "youtube_comments",
                "severity": "LOW",
            },
            # Sentiment scores outside expected range
            {
                "name": "invalid_sentiment_scores",
                "query": """
                    SELECT COUNT(*) FROM comment_sentiment
                    WHERE sentiment_score < -1.0 OR sentiment_score > 1.0
                """,
                "description": "Sentiment scores outside valid range (-1 to 1)",
                "table": "comment_sentiment",
                "severity": "HIGH",
            },
            # Videos with zero engagement
            {
                "name": "zero_engagement_videos",
                "query": """
                    SELECT COUNT(*) FROM youtube_videos
                    WHERE view_count > 1000 AND like_count = 0 AND comment_count = 0
                """,
                "description": "Popular videos with no engagement (suspicious)",
                "table": "youtube_videos",
                "severity": "MEDIUM",
            },
            # Burst patterns in comments (potential bot activity)
            {
                "name": "comment_burst_patterns",
                "query": """
                    SELECT COUNT(DISTINCT video_id) FROM (
                        SELECT video_id, DATE(published_at) as comment_date, COUNT(*) as daily_comments
                        FROM youtube_comments
                        WHERE published_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                        GROUP BY video_id, DATE(published_at)
                        HAVING COUNT(*) > 100
                    ) burst_videos
                """,
                "description": "Videos with comment burst patterns (>100 comments / day)",
                "table": "youtube_comments",
                "severity": "MEDIUM",
            },
        ]

        with self.engine.connect() as conn:
            for check in anomaly_checks:
                try:
                    count = conn.execute(text(check["query"])).scalar()
                    if count > 0:
                        self._add_issue(
                            "Anomalies",
                            check["severity"],
                            check["table"],
                            check["name"],
                            check["description"],
                            count,
                            fix_suggestion="Investigate data source and collection process",
                        )
                except Exception as e:
                    self.logger.warning(f"Anomaly check {check['name']} failed: {e}")

    def check_data_freshness(self) -> None:
        """Check data freshness and identify stale data."""
        self.logger.info("Checking data freshness...")

        freshness_checks = [
            {
                "table": "youtube_videos",
                "timestamp_column": "fetched_at",
                "description": "Video data",
                "max_age_hours": 24,
            },
            {
                "table": "youtube_metrics",
                "timestamp_column": "fetched_at",
                "description": "Metrics data",
                "max_age_hours": 24,
            },
            {
                "table": "youtube_comments",
                "timestamp_column": "created_at",
                "description": "Comment data",
                "max_age_hours": 48,
            },
            {
                "table": "comment_sentiment",
                "timestamp_column": "processed_at",
                "description": "Sentiment analysis",
                "max_age_hours": 72,
            },
        ]

        with self.engine.connect() as conn:
            for check in freshness_checks:
                try:
                    # Check for stale data
                    stale_query = f"""
                        SELECT COUNT(*) FROM {check['table']}
                        WHERE {check['timestamp_column']} < DATE_SUB(NOW(), INTERVAL {check['max_age_hours']} HOUR)
                    """
                    stale_count = conn.execute(text(stale_query)).scalar()

                    # Get total count
                    total_count = conn.execute(text(f"SELECT COUNT(*) FROM {check['table']}")).scalar()

                    if total_count > 0:
                        stale_percentage = (stale_count / total_count) * 100

                        if stale_percentage > 80:
                            severity = "HIGH"
                        elif stale_percentage > 50:
                            severity = "MEDIUM"
                        elif stale_percentage > 20:
                            severity = "LOW"
                        else:
                            continue  # Data is fresh enough

                        self._add_issue(
                            "Freshness",
                            severity,
                            check["table"],
                            "stale_data",
                            f"{check['description']} is stale ({stale_percentage:.1f}% older than {check['max_age_hours']}h)",
                            stale_count,
                            fix_suggestion="Run ETL pipeline to refresh data",
                        )

                except Exception as e:
                    self.logger.warning(f"Freshness check for {check['table']} failed: {e}")

    def generate_statistics(self) -> Dict[str, Any]:
        """Generate overall data statistics."""
        self.logger.info("Generating data statistics...")

        stats = {}

        with self.engine.connect() as conn:
            try:
                # Basic counts
                tables = ["youtube_videos", "youtube_comments", "comment_sentiment", "youtube_metrics"]
                for table in tables:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        stats[f"{table}_count"] = count
                    except Exception:
                        stats[f"{table}_count"] = 0

                # Data coverage
                if stats.get("youtube_comments_count", 0) > 0:
                    sentiment_coverage = (
                        stats.get("comment_sentiment_count", 0) / stats["youtube_comments_count"]
                    ) * 100
                    stats["sentiment_coverage_percent"] = round(sentiment_coverage, 1)

                # Unique entities
                try:
                    unique_videos = conn.execute(text("SELECT COUNT(DISTINCT video_id) FROM youtube_videos")).scalar()
                    unique_channels = conn.execute(
                        text("SELECT COUNT(DISTINCT channel_title) FROM youtube_videos WHERE channel_title IS NOT NULL")
                    ).scalar()
                    stats["unique_videos"] = unique_videos
                    stats["unique_channels"] = unique_channels
                except Exception:
                    pass

                # Date ranges
                try:
                    date_range = conn.execute(
                        text(
                            """
                        SELECT
                            MIN(published_at) as earliest_video,
                            MAX(published_at) as latest_video,
                            DATEDIFF(MAX(published_at), MIN(published_at)) as date_span_days
                        FROM youtube_videos
                        WHERE published_at IS NOT NULL
                    """
                        )
                    ).fetchone()

                    if date_range:
                        stats["earliest_video"] = (
                            date_range.earliest_video.isoformat() if date_range.earliest_video else None
                        )
                        stats["latest_video"] = date_range.latest_video.isoformat() if date_range.latest_video else None
                        stats["date_span_days"] = date_range.date_span_days
                except Exception:
                    pass

            except Exception as e:
                self.logger.warning(f"Error generating statistics: {e}")

        return stats

    def calculate_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)."""
        if not self.issues:
            return 100.0

        # Weight issues by severity
        severity_weights = {"CRITICAL": 25, "HIGH": 15, "MEDIUM": 8, "LOW": 3}

        total_deduction = sum(severity_weights.get(issue.severity, 0) for issue in self.issues)

        # Cap at 100 point deduction
        total_deduction = min(total_deduction, 100)

        return max(0.0, 100.0-total_deduction)

    def generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on issues found."""
        recommendations = []

        # Group issues by category
        categories = {}
        for issue in self.issues:
            if issue.category not in categories:
                categories[issue.category] = []
            categories[issue.category].append(issue)

        # Generate category-specific recommendations
        if "Completeness" in categories:
            critical_completeness = [i for i in categories["Completeness"] if i.severity in ["CRITICAL", "HIGH"]]
            if critical_completeness:
                recommendations.append("Run full ETL pipeline to populate missing core data")
                recommendations.append("Investigate data source quality and extraction logic")

        if "Consistency" in categories:
            recommendations.append("Implement referential integrity constraints in database")
            recommendations.append("Add data validation steps to ETL pipeline")

        if "Duplicates" in categories:
            recommendations.append("Implement deduplication logic in ETL pipeline")
            recommendations.append("Add unique constraints to prevent future duplicates")

        if "Anomalies" in categories:
            recommendations.append("Implement data validation rules and bounds checking")
            recommendations.append("Add anomaly detection to ETL monitoring")

        if "Freshness" in categories:
            recommendations.append("Schedule regular ETL runs to maintain data freshness")
            recommendations.append("Implement data freshness monitoring and alerts")

        # Add general recommendations
        if len(self.issues) > 10:
            recommendations.append("Consider implementing automated data quality monitoring")

        return recommendations

    def run_comprehensive_validation(self) -> DataQualityReport:
        """Run all data quality checks and generate comprehensive report."""
        self.logger.info("Starting comprehensive data quality validation...")
        start_time = datetime.now()

        # Run all validation checks
        if not self.report_only:
            self.check_completeness()
            self.check_consistency()
            self.check_duplicates()
            self.check_anomalies()
            self.check_data_freshness()

        # Generate statistics and recommendations
        statistics = self.generate_statistics()
        quality_score = self.calculate_quality_score()
        recommendations = self.generate_recommendations()

        # Create report
        report = DataQualityReport(
            timestamp=start_time,
            overall_score=quality_score,
            issues=self.issues,
            statistics=statistics,
            recommendations=recommendations,
        )

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Data quality validation completed in {duration:.2f} seconds")
        self.logger.info(f"Overall quality score: {quality_score:.1f}/100")
        self.logger.info(f"Issues found: {len(self.issues)}")

        return report

    def print_detailed_report(self, report: DataQualityReport) -> None:
        """Print detailed data quality report to console."""
        print("\n" + "=" * 80)
        print("DATA QUALITY VALIDATION REPORT")
        print("=" * 80)
        print(f"Timestamp: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Overall Quality Score: {report.overall_score:.1f}/100")
        print()

        # Statistics
        print("DATA STATISTICS:")
        print("-" * 40)
        for key, value in report.statistics.items():
            if isinstance(value, (int, float)):
                if isinstance(value, int):
                    print(f"  {key.replace('_', ' ').title()}: {value:,}")
                else:
                    print(f"  {key.replace('_', ' ').title()}: {value:.1f}")
            else:
                print(f"  {key.replace('_', ' ').title()}: {value}")
        print()

        # Issues by category
        if report.issues:
            print("QUALITY ISSUES:")
            print("-" * 40)

            categories = {}
            for issue in report.issues:
                if issue.category not in categories:
                    categories[issue.category] = []
                categories[issue.category].append(issue)

            for category, issues in categories.items():
                print(f"\n{category.upper()}:")
                for issue in sorted(
                    issues, key=lambda x: {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}[x.severity]
                ):
                    severity_symbol = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(
                        issue.severity, "❓"
                    )

                    print(f"  {severity_symbol} {issue.table}.{issue.issue_type}: {issue.description}")
                    if issue.fix_suggestion:
                        print(f"    💡 Fix: {issue.fix_suggestion}")
        else:
            print("✅ NO QUALITY ISSUES FOUND")

        # Recommendations
        if report.recommendations:
            print(f"\nRECOMMENDATIONS:")
            print("-" * 40)
            for i, rec in enumerate(report.recommendations, 1):
                print(f"  {i}. {rec}")

        print("\n" + "=" * 80)


def main():
    """Main entry point for data quality validation."""
    import argparse

    parser = argparse.ArgumentParser(description="Data Quality Validation System")
    parser.add_argument("--fix-duplicates", action="store_true", help="Automatically fix duplicate records")
    parser.add_argument("--report-only", action="store_true",
                        help="Generate report without running validation checks")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")

    args = parser.parse_args()

    # Run validation
    validator = DataQualityValidator(fix_issues=args.fix_duplicates, report_only=args.report_only)
    report = validator.run_comprehensive_validation()

    if args.json:
        # Output JSON for programmatic use
        report_dict = {
            "timestamp": report.timestamp.isoformat(),
            "overall_score": report.overall_score,
            "statistics": report.statistics,
            "issues": [
                {
                    "category": i.category,
                    "severity": i.severity,
                    "table": i.table,
                    "issue_type": i.issue_type,
                    "description": i.description,
                    "count": i.count,
                    "fix_suggestion": i.fix_suggestion,
                    "auto_fixable": i.auto_fixable,
                }
                for i in report.issues
            ],
            "recommendations": report.recommendations,
        }
        print(json.dumps(report_dict, indent=2))
    else:
        # Print human-readable report
        validator.print_detailed_report(report)

    # Exit with appropriate code based on quality score
    if report.overall_score >= 90:
        exit_code = 0  # Excellent
    elif report.overall_score >= 70:
        exit_code = 1  # Good but needs attention
    else:
        exit_code = 2  # Poor quality, needs immediate attention

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
