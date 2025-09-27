#!/usr / bin / env python3
"""
Enhanced Data Quality Manager

Professional - grade data quality validation and cleanup system with:
- Automatic detection and cleanup of missing critical fields
- Well - formatted cleanup reports with emojis and statistics
- Educational bot analysis display
- Comprehensive audit logging
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class DataIssue:
    """Represents a data quality issue found during validation."""

    issue_type: str
    description: str
    count: int
    severity: str  # 'critical', 'warning', 'info'
    table_name: str
    column_name: Optional[str] = None


@dataclass
class CleanupOperation:
    """Represents a cleanup operation performed on the data."""

    operation_type: str
    description: str
    records_affected: int
    table_name: str
    timestamp: datetime
    sql_query: str


@dataclass
class DataQualityReport:
    """Comprehensive data quality report with cleanup summary."""

    timestamp: datetime
    issues_detected: List[DataIssue]
    cleanup_operations: List[CleanupOperation]
    quality_score: float
    total_records_cleaned: int
    bot_analysis_summary: Dict[str, int]
    recommendations: List[str]


class EnhancedDataQualityManager:
    """
    Professional data quality management system.

    Automatically detects and resolves data quality issues while providing
    comprehensive reporting and educational content.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.cleanup_operations: List[CleanupOperation] = []

    def validate_and_cleanup(self) -> DataQualityReport:
        """
        Run comprehensive data quality validation and cleanup.

        Returns:
            DataQualityReport with complete analysis and cleanup summary
        """
        print("🔍 PROFESSIONAL DATA QUALITY ANALYSIS")
        print("=" * 60)

        # Step 1: Detect all data quality issues
        issues = self.detect_all_data_issues()

        # Step 2: Perform automatic cleanup
        cleanup_ops = self.perform_automatic_cleanup(issues)

        # Step 3: Analyze bot patterns
        bot_summary = self.analyze_and_display_bot_patterns()

        # Step 4: Generate quality score and recommendations
        quality_score = self.calculate_quality_score(issues, cleanup_ops)
        recommendations = self.generate_recommendations(issues, cleanup_ops)

        # Step 5: Create comprehensive report
        report = DataQualityReport(
            timestamp=datetime.now(),
            issues_detected=issues,
            cleanup_operations=cleanup_ops,
            quality_score=quality_score,
            total_records_cleaned=sum(op.records_affected for op in cleanup_ops),
            bot_analysis_summary=bot_summary,
            recommendations=recommendations,
        )

        # Step 6: Display professional summary
        self.display_professional_summary(report)

        return report

    def detect_all_data_issues(self) -> List[DataIssue]:
        """Detect all data quality issues across the database."""
        print("\n🔍 Scanning Database for Quality Issues...")
        print("-" * 40)

        issues = []

        with self.engine.connect() as conn:
            # Critical data validation checks
            validation_checks = [
                {
                    "type": "missing_video_titles",
                    "description": "Videos without titles",
                    "query": "SELECT COUNT(*) FROM youtube_videos WHERE title IS NULL OR title = ''",
                    "severity": "critical",
                    "table": "youtube_videos",
                    "column": "title",
                },
                {
                    "type": "missing_artist_names",
                    "description": "Videos without artist names",
                    "query": "SELECT COUNT(*) FROM youtube_videos WHERE channel_title IS NULL OR channel_title = ''",
                    "severity": "critical",
                    "table": "youtube_videos",
                    "column": "channel_title",
                },
                {
                    "type": "missing_comment_text",
                    "description": "Comments without text content",
                    "query": "SELECT COUNT(*) FROM youtube_comments WHERE comment"
                    "_text IS NULL OR comment_text_item = ''",
                    "severity": "critical",
                    "table": "youtube_comments",
                    "column": "comment_text",
                },
                {
                    "type": "missing_comment_authors",
                    "description": "Comments without author information",
                    "query": "SELECT COUNT(*) FROM youtube_comments WHERE author_name IS NULL OR author_name = ''",
                    "severity": "critical",
                    "table": "youtube_comments",
                    "column": "author_name",
                },
                {
                    "type": "missing_sentiment_confidence",
                    "description": "Sentiment records without confidence scores",
                    "query": "SELECT COUNT(*) FROM comment_sentiment WHERE confidence_score IS NULL",
                    "severity": "warning",
                    "table": "comment_sentiment",
                    "column": "confidence_score",
                },
                {
                    "type": "future_published_dates",
                    "description": "Videos with future publication dates",
                    "query": "SELECT COUNT(*) FROM youtube_videos WHERE published_at > NOW()",
                    "severity": "warning",
                    "table": "youtube_videos",
                    "column": "published_at",
                },
                {
                    "type": "negative_metrics",
                    "description": "Records with negative view / like / comment counts",
                    "query": "SELECT COUNT(*) FROM youtube_metrics WHERE view"
                    "_count < 0 OR like_count < 0 OR comment_count < 0",
                    "severity": "critical",
                    "table": "youtube_metrics",
                    "column": "metrics",
                },
            ]

            for check in validation_checks:
                result = conn.execute(text(check["query"]))
                count = result.fetchone()[0]

                if count > 0:
                    issue = DataIssue(
                        issue_type=check["type"],
                        description=check["description"],
                        count=count,
                        severity=check["severity"],
                        table_name=check["table"],
                        column_name=check["column"],
                    )
                    issues.append(issue)

                    # Display issue with appropriate emoji
                    severity_emoji = "🔴" if check["severity"] == "critical" else "🟡"
                    print(f"{severity_emoji} {check['description']}: {count:,} records")
                else:
                    print(f"✅ {check['description']}: Clean")

        print(f"\n📊 Total Issues Found: {len(issues)}")
        return issues

    def perform_automatic_cleanup(self, issues: List[DataIssue]) -> List[CleanupOperation]:
        """
        Perform automatic cleanup of critical data quality issues.

        Args:
            issues: List of detected data quality issues

        Returns:
            List of cleanup operations performed
        """
        if not issues:
            print("\n✨ No cleanup needed - data is already clean!")
            return []

        print("\n🧹 AUTOMATIC DATA CLEANUP OPERATIONS")
        print("-" * 40)

        cleanup_operations = []

        with self.engine.connect() as conn:
            # Begin transaction for safe cleanup
            trans = conn.begin()

            try:
                for issue in issues:
                    if issue.severity == "critical":
                        cleanup_op = self._cleanup_critical_issue(conn, issue)
                        if cleanup_op:
                            cleanup_operations.append(cleanup_op)

                # Commit all cleanup operations
                trans.commit()
                print(f"\n✅ All cleanup operations completed successfully!")

            except Exception as e:
                trans.rollback()
                print(f"\n❌ Cleanup failed, rolling back: {e}")
                raise

        return cleanup_operations

    def _cleanup_critical_issue(self, conn, issue: DataIssue) -> Optional[CleanupOperation]:
        """Clean up a specific critical data quality issue."""

        cleanup_queries = {
            "missing_video_titles": {
                "query": "DELETE FROM youtube_videos WHERE title IS NULL OR title = ''",
                "description": "Removed videos without titles",
            },
            "missing_artist_names": {
                "query": "DELETE FROM youtube_videos WHERE channel_title IS NULL OR channel_title = ''",
                "description": "Removed videos without artist names",
            },
            "missing_comment_text": {
                "query": "DELETE FROM youtube_comments WHERE comment_text IS NULL OR comment_text_item = ''",
                "description": "Removed comments without text content",
            },
            "missing_comment_authors": {
                "query": "DELETE FROM youtube_comments WHERE author_name IS NULL OR author_name = ''",
                "description": "Removed comments without author information",
            },
            "negative_metrics": {
                "query": "DELETE FROM youtube_metrics WHERE view_count < 0 OR like_count < 0 OR comment_count < 0",
                "description": "Removed records with negative metrics",
            },
        }

        if issue.issue_type not in cleanup_queries:
            print(f"⚠️  No automatic cleanup available for: {issue.description}")
            return None

        cleanup_config = cleanup_queries[issue.issue_type]

        # Execute cleanup query
        result = conn.execute(text(cleanup_config["query"]))
        records_affected = result.rowcount

        # Create cleanup operation record
        cleanup_op = CleanupOperation(
            operation_type="DELETE",
            description=cleanup_config["description"],
            records_affected=records_affected,
            table_name=issue.table_name,
            timestamp=datetime.now(),
            sql_query=cleanup_config["query"],
        )

        # Display cleanup result with emoji
        print(f"🗑️  {cleanup_config['description']}: {records_affected:,} records removed")

        return cleanup_op

    def analyze_and_display_bot_patterns(self) -> Dict[str, int]:
        """
        Analyze bot patterns and display educational examples.

        Returns:
            Dictionary with bot analysis summary statistics
        """
        print("\n🤖 BOT DETECTION ANALYSIS")
        print("-" * 40)

        try:
            # Import bot detection (may not be available in all environments)
            from youtubeviz.bot_detection import BotDetectionConfig, analyze_bot_patterns

            # Configure bot detection for music industry
            config = BotDetectionConfig(
                whitelist_phrases=frozenset(
                    {
                        "love this",
                        "dope",
                        "fire",
                        "banger",
                        "slaps",
                        "goated",
                        "amazing",
                        "incredible",
                        "beautiful",
                        "perfect",
                        "masterpiece",
                        "vibes",
                        "mood",
                        "hits different",
                        "on repeat",
                        "talent",
                    }
                ),
                near_dupe_threshold=0.88,
                min_dupe_cluster=3,
                burst_window_seconds=45,
            )

            # Analyze recent bot patterns
            bot_results = analyze_bot_patterns(self.engine, config=config, days=30)

            if not bot_results.empty:
                # Calculate summary statistics
                total_analyzed = len(bot_results)
                high_risk = len(bot_results[bot_results["bot_risk_level"] == "High"])
                medium_risk = len(bot_results[bot_results["bot_risk_level"] == "Medium"])
                low_risk = len(bot_results[bot_results["bot_risk_level"] == "Low"])

                print(f"📊 Analyzed {total_analyzed:,} recent comments")
                print(f"🔴 High Risk (Likely Bots): {high_risk:,} ({high_risk / total_analyzed * 100:.1f}%)")
                print(f"🟡 Medium Risk: {medium_risk:,} ({medium_risk / total_analyzed * 100:.1f}%)")
                print(f"🟢 Low Risk (Likely Human): {low_risk:,} ({low_risk / total_analyzed * 100:.1f}%)")

                # Display educational examples of high - risk bot comments
                if high_risk > 0:
                    self._display_bot_examples(bot_results)

                return {
                    "total_analyzed": total_analyzed,
                    "high_risk": high_risk,
                    "medium_risk": medium_risk,
                    "low_risk": low_risk,
                }
            else:
                print("📊 No recent comments found for bot analysis")
                return {"total_analyzed": 0, "high_risk": 0, "medium_risk": 0, "low_risk": 0}

        except ImportError:
            print("⚠️  Bot detection module not available")
            return {"total_analyzed": 0, "high_risk": 0, "medium_risk": 0, "low_risk": 0}
        except Exception as e:
            print(f"⚠️  Bot analysis failed: {e}")
            return {"total_analyzed": 0, "high_risk": 0, "medium_risk": 0, "low_risk": 0}

    def _display_bot_examples(self, bot_results: pd.DataFrame) -> None:
        """Display educational examples of bot comments."""
        print("\n🎓 EDUCATIONAL: High - Risk Bot Comment Examples")
        print("-" * 50)

        high_risk_bots = bot_results[bot_results["bot_risk_level"] == "High"].head(5)

        for idx, bot in high_risk_bots.iterrows():
            comment_preview = bot.get("comment_text", "")[:100]
            if len(comment_preview) == 100:
                comment_preview += "..."

            print(f"🤖 Bot Pattern: {bot.get('primary_bot_indicator', 'Unknown')}")
            print(f'   Comment: "{comment_preview}"')
            print(f"   Risk Score: {bot.get('bot_score', 0):.2f}")
            print(f"   Channel: {bot.get('channel_title', 'Unknown')}")
            print()

        print("💡 Why Bot Detection Matters:")
        print("   • Bots can artificially inflate engagement metrics")
        print("   • They may spread spam or misleading information")
        print("   • Accurate sentiment analysis requires filtering out bot comments")
        print("   • Music industry decisions should be based on genuine fan feedback")

    def calculate_quality_score(self, issues: List[DataIssue], cleanup_ops: List[CleanupOperation]) -> float:
        """Calculate overall data quality score (0 - 100)."""
        if not issues:
            return 100.0

        # Deduct points based on issue severity
        total_deduction = 0
        for issue in issues:
            if issue.severity == "critical":
                total_deduction += 10  # 10 points per critical issue
            elif issue.severity == "warning":
                total_deduction += 5  # 5 points per warning

        # Add points back for successful cleanup
        cleanup_bonus = min(len(cleanup_ops) * 5, total_deduction)

        quality_score = max(0, 100 - total_deduction + cleanup_bonus)
        return quality_score

    def generate_recommendations(self, issues: List[DataIssue], cleanup_ops: List[CleanupOperation]) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []

        if cleanup_ops:
            recommendations.append(
                f"✅ Successfully cleaned {sum(op.records_affected for op in cleanup_ops):,} problematic records"
            )

        remaining_issues = [issue for issue in issues if issue.severity == "warning"]
        if remaining_issues:
            recommendations.append(
                f"⚠️  {len(remaining_issues)} warning - level issues remain - consider manual review")

        if not issues:
            recommendations.append("🎉 Excellent! Your data quality is pristine")
        elif len([i for i in issues if i.severity == "critical"]) == 0:
            recommendations.append("👍 Good data quality - only minor issues detected")

        recommendations.append("🔄 Run this analysis regularly to maintain data quality")
        recommendations.append("📊 Monitor bot detection results to ensure authentic engagement metrics")

        return recommendations

    def display_professional_summary(self, report: DataQualityReport) -> None:
        """Display a professional, well - formatted summary report."""
        print("\n" + "=" * 60)
        print("🏆 DATA QUALITY ANALYSIS COMPLETE")
        print("=" * 60)

        print(f"📅 Analysis Date: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 Quality Score: {report.quality_score:.1f}/100")

        if report.total_records_cleaned > 0:
            print(f"🧹 Records Cleaned: {report.total_records_cleaned:,}")

        if report.bot_analysis_summary.get("total_analyzed", 0) > 0:
            bot_stats = report.bot_analysis_summary
            print(f"🤖 Bot Analysis: {bot_stats['high_risk']:,} high - risk bots detected")

        print(f"\n💡 Key Recommendations:")
        for rec in report.recommendations:
            print(f"   {rec}")

        print(f"\n📋 Detailed Operations Log:")
        if report.cleanup_operations:
            for op in report.cleanup_operations:
                print(f"   • {op.description}: {op.records_affected:,} records")
        else:
            print("   • No cleanup operations needed")

        print("\n✨ Your data is now ready for professional analytics!")
        print("=" * 60)


def run_enhanced_data_quality_check(engine: Engine) -> DataQualityReport:
    """
    Convenience function to run complete data quality analysis.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataQualityReport with complete analysis results
    """
    manager = EnhancedDataQualityManager(engine)
    return manager.validate_and_cleanup()


if __name__ == "__main__":
    # Demo the enhanced data quality system
    from web.etl_helpers import get_engine

    print("🚀 ENHANCED DATA QUALITY MANAGER DEMO")
    print("=" * 60)

    try:
        engine = get_engine()
        report = run_enhanced_data_quality_check(engine)

        print(f"\n📊 Final Quality Score: {report.quality_score:.1f}/100")
        print(f"🧹 Total Records Cleaned: {report.total_records_cleaned:,}")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
