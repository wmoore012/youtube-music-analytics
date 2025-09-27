#!/usr / bin / env python3
"""
Data Quality Monitoring Script

This script provides continuous monitoring of data quality across the YouTube analytics platform.
It runs comprehensive checks and generates alerts when issues are detected.
"""

from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from web.data_quality import DataQualityValidator
from web.error_handling import ErrorSeverity
from web.etl_helpers import get_engine

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataQualityMonitor:
    """Monitors data quality across the YouTube analytics platform."""

    def __init__(self):
        self.engine = get_engine()
        self.validator = DataQualityValidator(self.engine)
        self.monitoring_config = self._load_monitoring_config()

    def _load_monitoring_config(self) -> Dict[str, Any]:
        """Load monitoring configuration with sensible defaults."""
        return {
            "tables": {
                "youtube_videos": {
                    "required_columns": ["video_id", "title", "channel_title", "published_at"],
                    "timestamp_column": "published_at",
                    "max_age_hours": 168,  # 1 week
                    "min_records_expected": 100,
                    "critical_null_threshold": 5.0,  # 5% nulls is critical
                    "warning_null_threshold": 1.0,  # 1% nulls is warning
                },
                "youtube_metrics": {
                    "required_columns": ["video_id", "metrics_date", "view_count", "like_count", "comment_count"],
                    "timestamp_column": "fetched_at",
                    "max_age_hours": 48,  # 2 days
                    "min_records_expected": 100,
                    "critical_null_threshold": 5.0,
                    "warning_null_threshold": 1.0,
                },
                "youtube_comments": {
                    "required_columns": ["video_id", "comment_id", "comment_text", "author_name"],
                    "min_records_expected": 1000,
                    "critical_null_threshold": 2.0,
                    "warning_null_threshold": 0.5,
                },
                "comment_sentiment": {
                    "required_columns": ["comment_id", "sentiment_score", "confidence_score"],
                    "min_records_expected": 1000,
                    "critical_null_threshold": 1.0,
                    "warning_null_threshold": 0.1,
                },
            },
            "business_rules": {
                "max_like_to_view_ratio": 0.2,  # Likes shouldn't exceed 20% of views
                "max_comment_to_view_ratio": 0.1,  # Comments shouldn't exceed 10% of views
                "min_sentiment_confidence": 0.5,  # Sentiment confidence should be at least 50%
                "max_future_dates_allowed": 0,  # No future dates allowed
            },
        }

    def check_table_record_counts(self) -> Dict[str, Any]:
        """Check that tables have expected minimum record counts."""
        results = {"timestamp": datetime.utcnow().isoformat(), "checks": {}, "alerts": []}

        for table_name, config in self.monitoring_config["tables"].items():
            min_expected = config.get("min_records_expected", 0)

            try:
                with self.engine.connect() as conn:
                    from sqlalchemy import text

                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    actual_count = result.fetchone()[0]

                    check_result = {
                        "actual_count": actual_count,
                        "min_expected": min_expected,
                        "status": "PASS" if actual_count >= min_expected else "FAIL",
                    }

                    if actual_count < min_expected:
                        alert = {
                            "severity": "HIGH",
                            "table": table_name,
                            "issue": f"Record count below threshold: {actual_count:,} < {min_expected:,}",
                            "recommendation": f"Investigate why {table_name} has insufficient records",
                        }
                        results["alerts"].append(alert)

                    results["checks"][f"{table_name}_record_count"] = check_result

            except Exception as e:
                error_result = {"error": str(e), "status": "ERROR"}
                results["checks"][f"{table_name}_record_count"] = error_result
                results["alerts"].append(
                    {
                        "severity": "CRITICAL",
                        "table": table_name,
                        "issue": f"Failed to check record count: {str(e)}",
                        "recommendation": "Check database connectivity and table existence",
                    }
                )

        return results

    def check_business_rule_violations(self) -> Dict[str, Any]:
        """Check for violations of business rules and data consistency."""
        results = {"timestamp": datetime.utcnow().isoformat(), "checks": {}, "alerts": []}

        business_rules = self.monitoring_config["business_rules"]

        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text

                # Check like - to - view ratios
                like_ratio_query = text(
                    """
                    SELECT COUNT(*) as violation_count
                    FROM youtube_metrics
                    WHERE view_count > 0
                    AND (like_count / view_count) > :max_ratio
                """
                )
                result = conn.execute(like_ratio_query, {"max_ratio": business_rules["max_like_to_view_ratio"]})
                like_violations = result.fetchone()[0]

                results["checks"]["like_to_view_ratio"] = {
                    "violations": like_violations,
                    "threshold": business_rules["max_like_to_view_ratio"],
                    "status": "PASS" if like_violations == 0 else "FAIL",
                }

                if like_violations > 0:
                    results["alerts"].append(
                        {
                            "severity": "MEDIUM",
                            "issue": f"{like_violations} videos have suspicious like - to - view ratios",
                            "recommendation": "Review videos with unusually high engagement rates",
                        }
                    )

                # Check comment - to - view ratios
                comment_ratio_query = text(
                    """
                    SELECT COUNT(*) as violation_count
                    FROM youtube_metrics
                    WHERE view_count > 0
                    AND (comment_count / view_count) > :max_ratio
                """
                )
                result = conn.execute(comment_ratio_query, {"max_ratio": business_rules["max_comment_to_view_ratio"]})
                comment_violations = result.fetchone()[0]

                results["checks"]["comment_to_view_ratio"] = {
                    "violations": comment_violations,
                    "threshold": business_rules["max_comment_to_view_ratio"],
                    "status": "PASS" if comment_violations == 0 else "FAIL",
                }

                if comment_violations > 0:
                    results["alerts"].append(
                        {
                            "severity": "MEDIUM",
                            "issue": f"{comment_violations} videos have suspicious comment - to - view ratios",
                            "recommendation": "Review videos with unusually high comment rates",
                        }
                    )

                # Check sentiment confidence scores
                sentiment_confidence_query = text(
                    """
                    SELECT COUNT(*) as low_confidence_count
                    FROM comment_sentiment
                    WHERE confidence_score < :min_confidence
                """
                )
                result = conn.execute(
                    sentiment_confidence_query, {"min_confidence": business_rules["min_sentiment_confidence"]}
                )
                low_confidence_count = result.fetchone()[0]

                results["checks"]["sentiment_confidence"] = {
                    "low_confidence_count": low_confidence_count,
                    "threshold": business_rules["min_sentiment_confidence"],
                    "status": "PASS" if low_confidence_count == 0 else "WARNING",
                }

                if low_confidence_count > 0:
                    results["alerts"].append(
                        {
                            "severity": "LOW",
                            "issue": f"{low_confidence_count} sentiment scores have low confidence",
                            "recommendation": "Consider reprocessing comments with low confidence scores",
                        }
                    )

                # Check for future dates
                future_dates_query = text(
                    """
                    SELECT COUNT(*) as future_date_count
                    FROM youtube_videos
                    WHERE published_at > NOW()
                """
                )
                result = conn.execute(future_dates_query)
                future_dates_count = result.fetchone()[0]

                results["checks"]["future_dates"] = {
                    "future_date_count": future_dates_count,
                    "status": "PASS" if future_dates_count == 0 else "FAIL",
                }

                if future_dates_count > 0:
                    results["alerts"].append(
                        {
                            "severity": "HIGH",
                            "issue": f"{future_dates_count} videos have future publication dates",
                            "recommendation": "Investigate and correct videos with invalid publication dates",
                        }
                    )

        except Exception as e:
            results["checks"]["business_rules_error"] = {"error": str(e), "status": "ERROR"}
            results["alerts"].append(
                {
                    "severity": "CRITICAL",
                    "issue": f"Failed to check business rules: {str(e)}",
                    "recommendation": "Check database connectivity and query syntax",
                }
            )

        return results

    def check_data_consistency(self) -> Dict[str, Any]:
        """Check for data consistency issues across related tables."""
        results = {"timestamp": datetime.utcnow().isoformat(), "checks": {}, "alerts": []}

        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text

                # Check for orphaned metrics (metrics without corresponding videos)
                orphaned_metrics_query = text(
                    """
                    SELECT COUNT(*) as orphaned_count
                    FROM youtube_metrics m
                    LEFT JOIN youtube_videos v ON m.video_id = v.video_id
                    WHERE v.video_id IS NULL
                """
                )
                result = conn.execute(orphaned_metrics_query)
                orphaned_metrics = result.fetchone()[0]

                results["checks"]["orphaned_metrics"] = {
                    "orphaned_count": orphaned_metrics,
                    "status": "PASS" if orphaned_metrics == 0 else "FAIL",
                }

                if orphaned_metrics > 0:
                    results["alerts"].append(
                        {
                            "severity": "HIGH",
                            "issue": f"{orphaned_metrics} metrics records have no corresponding video",
                            "recommendation": "Clean up orphaned metrics or investigate missing videos",
                        }
                    )

                # Check for orphaned comments
                orphaned_comments_query = text(
                    """
                    SELECT COUNT(*) as orphaned_count
                    FROM youtube_comments c
                    LEFT JOIN youtube_videos v ON c.video_id = v.video_id
                    WHERE v.video_id IS NULL
                """
                )
                result = conn.execute(orphaned_comments_query)
                orphaned_comments = result.fetchone()[0]

                results["checks"]["orphaned_comments"] = {
                    "orphaned_count": orphaned_comments,
                    "status": "PASS" if orphaned_comments == 0 else "FAIL",
                }

                if orphaned_comments > 0:
                    results["alerts"].append(
                        {
                            "severity": "MEDIUM",
                            "issue": f"{orphaned_comments} comments have no corresponding video",
                            "recommendation": "Clean up orphaned comments or investigate missing videos",
                        }
                    )

                # Check for sentiment records without comments
                orphaned_sentiment_query = text(
                    """
                    SELECT COUNT(*) as orphaned_count
                    FROM comment_sentiment s
                    LEFT JOIN youtube_comments c ON s.comment_id = c.comment_id
                    WHERE c.comment_id IS NULL
                """
                )
                result = conn.execute(orphaned_sentiment_query)
                orphaned_sentiment = result.fetchone()[0]

                results["checks"]["orphaned_sentiment"] = {
                    "orphaned_count": orphaned_sentiment,
                    "status": "PASS" if orphaned_sentiment == 0 else "FAIL",
                }

                if orphaned_sentiment > 0:
                    results["alerts"].append(
                        {
                            "severity": "MEDIUM",
                            "issue": f"{orphaned_sentiment} sentiment records have no corresponding comment",
                            "recommendation": "Clean up orphaned sentiment records",
                        }
                    )

        except Exception as e:
            results["checks"]["consistency_error"] = {"error": str(e), "status": "ERROR"}
            results["alerts"].append(
                {
                    "severity": "CRITICAL",
                    "issue": f"Failed to check data consistency: {str(e)}",
                    "recommendation": "Check database connectivity and table relationships",
                }
            )

        return results

    def run_comprehensive_monitoring(self) -> Dict[str, Any]:
        """Run all data quality monitoring checks."""
        logger.info("🔍 Starting comprehensive data quality monitoring...")

        monitoring_results = {
            "monitoring_timestamp": datetime.utcnow().isoformat(),
            "overall_status": "PASS",
            "total_alerts": 0,
            "critical_alerts": 0,
            "high_alerts": 0,
            "medium_alerts": 0,
            "low_alerts": 0,
            "checks": {},
        }

        # Run all monitoring checks
        checks_to_run = [
            ("record_counts", self.check_table_record_counts),
            ("business_rules", self.check_business_rule_violations),
            ("data_consistency", self.check_data_consistency),
        ]

        all_alerts = []

        for check_name, check_function in checks_to_run:
            try:
                logger.info(f"Running {check_name} checks...")
                check_results = check_function()
                monitoring_results["checks"][check_name] = check_results

                # Collect alerts
                alerts = check_results.get("alerts", [])
                all_alerts.extend(alerts)

                logger.info(f"✅ {check_name} checks completed: {len(alerts)} alerts")

            except Exception as e:
                logger.error(f"❌ Failed to run {check_name} checks: {str(e)}")
                monitoring_results["checks"][check_name] = {"error": str(e), "status": "ERROR"}
                all_alerts.append(
                    {
                        "severity": "CRITICAL",
                        "issue": f"Failed to run {check_name} monitoring",
                        "recommendation": "Check monitoring system configuration",
                    }
                )

        # Summarize alerts by severity
        monitoring_results["total_alerts"] = len(all_alerts)
        for alert in all_alerts:
            severity = alert.get("severity", "UNKNOWN").lower()
            if severity == "critical":
                monitoring_results["critical_alerts"] += 1
            elif severity == "high":
                monitoring_results["high_alerts"] += 1
            elif severity == "medium":
                monitoring_results["medium_alerts"] += 1
            elif severity == "low":
                monitoring_results["low_alerts"] += 1

        # Determine overall status
        if monitoring_results["critical_alerts"] > 0:
            monitoring_results["overall_status"] = "CRITICAL"
        elif monitoring_results["high_alerts"] > 0:
            monitoring_results["overall_status"] = "HIGH_ISSUES"
        elif monitoring_results["medium_alerts"] > 0:
            monitoring_results["overall_status"] = "MEDIUM_ISSUES"
        elif monitoring_results["low_alerts"] > 0:
            monitoring_results["overall_status"] = "LOW_ISSUES"

        monitoring_results["all_alerts"] = all_alerts

        logger.info(f"🏁 Data quality monitoring completed: {monitoring_results['overall_status']}")

        return monitoring_results

    def generate_monitoring_report(self, results: Dict[str, Any]) -> str:
        """Generate human - readable monitoring report."""
        report_lines = [
            "=" * 60,
            "📊 DATA QUALITY MONITORING REPORT",
            "=" * 60,
            f"🕐 Timestamp: {results['monitoring_timestamp']}",
            f"📈 Overall Status: {results['overall_status']}",
            f"🚨 Total Alerts: {results['total_alerts']}",
            "",
        ]

        # Alert summary
        if results["total_alerts"] > 0:
            report_lines.extend(
                [
                    "Alert Breakdown:",
                    f"  🔴 Critical: {results['critical_alerts']}",
                    f"  🟠 High: {results['high_alerts']}",
                    f"  🟡 Medium: {results['medium_alerts']}",
                    f"  🟢 Low: {results['low_alerts']}",
                    "",
                ]
            )

            # List all alerts
            report_lines.append("Detailed Alerts:")
            for i, alert in enumerate(results.get("all_alerts", []), 1):
                severity_icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(
                    alert.get("severity", "UNKNOWN"), "⚪"
                )

                report_lines.extend(
                    [
                        f"{i}. {severity_icon} {alert.get('severity', 'UNKNOWN')}",
                        f"   Issue: {alert.get('issue', 'Unknown issue')}",
                        f"   Recommendation: {alert.get('recommendation', 'No recommendation')}",
                        "",
                    ]
                )
        else:
            report_lines.append("✅ No data quality issues detected!")

        report_lines.append("=" * 60)

        return "\n".join(report_lines)


def main():
    """Main function for data quality monitoring."""
    monitor = DataQualityMonitor()

    try:
        # Run comprehensive monitoring
        results = monitor.run_comprehensive_monitoring()

        # Generate and print report
        report = monitor.generate_monitoring_report(results)
        print(report)

        # Save results to file
        report_file = Path("data_quality_report.json")
        with open(report_file, "w") as f:
            json.dump(results, f, indent=2)

        print(f"\n📄 Detailed results saved to: {report_file}")

        # Return appropriate exit code
        if results["overall_status"] in ["PASS", "LOW_ISSUES"]:
            return 0
        elif results["overall_status"] in ["MEDIUM_ISSUES", "HIGH_ISSUES"]:
            return 1
        else:  # CRITICAL
            return 2

    except Exception as e:
        logger.error(f"💥 Critical failure in data quality monitoring: {str(e)}")
        return 3


if __name__ == "__main__":
    sys.exit(main())
