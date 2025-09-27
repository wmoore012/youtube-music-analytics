#!/usr / bin / env python3
"""
Bulletproof ETL Runner with Enhanced Error Handling and Data Quality

This module provides a robust ETL execution framework with:
- Comprehensive error handling and recovery
- Data quality validation at each step
- Progress tracking for long - running operations
- Automatic retry logic for transient failures
- Detailed logging and monitoring
"""

from datetime import datetime
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from web.data_quality import DataQualityValidator
from web.error_handling import ErrorCategory, ErrorContext, ErrorSeverity, ETLError
from web.etl_helpers import get_engine
from web.retry_handler import ProgressTracker, retry_database_operation
from web.sentiment_job import YouTubeCommentSentimentJob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bulletproof_etl.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class BulletproofETLRunner:
    """Enhanced ETL runner with bulletproof error handling and data quality validation."""

    def __init__(self):
        self.engine = None
        self.data_validator = None
        self.sentiment_analyzer = None
        self.execution_stats = {
            "start_time": None,
            "end_time": None,
            "videos_processed": 0,
            "comments_processed": 0,
            "sentiment_scores_generated": 0,
            "errors_encountered": 0,
            "data_quality_issues": 0,
        }

    def initialize_components(self) -> None:
        """Initialize database connection and components with error handling."""
        context_item = ErrorContext(component="BulletproofETLRunner", operation="initialize_components")

        try:
            logger.info("🔧 Initializing ETL components...")

            # Initialize database connection with retry
            self.engine = self._initialize_database_connection()

            # Initialize data quality validator
            self.data_validator = DataQualityValidator(self.engine)

            # Initialize sentiment analyzer
            self.sentiment_analyzer = YouTubeCommentSentimentJob()

            logger.info("✅ All ETL components initialized successfully")

        except Exception as e:
            raise ETLError(
                "Failed to initialize ETL components",
                context=context,
                original_error=e,
                severity=ErrorSeverity.CRITICAL,
            )

    @retry_database_operation(max_attempts=3, base_delay=2.0)
    def _initialize_database_connection(self):
        """Initialize database connection with retry logic."""
        logger.info("🗄️ Connecting to database...")
        engine = get_engine()

        # Test connection
        with engine.connect() as conn:
            from sqlalchemy import text

            conn.execute(text("SELECT 1"))

        logger.info("✅ Database connection established")
        return engine

    def validate_system_prerequisites(self) -> None:
        """Validate that system is ready for ETL execution."""
        context_item = ErrorContext(component="BulletproofETLRunner", operation="validate_system_prerequisites")

        try:
            logger.info("🔍 Validating system prerequisites...")

            # Define expected schema for core tables
            tables_config = {
                "youtube_videos": {
                    "required_columns": ["video_id", "title", "channel_title", "published_at"],
                    "timestamp_column": "published_at",
                    "max_age_hours": 168,  # 1 week
                },
                "youtube_metrics": {
                    "required_columns": ["video_id", "metrics_date", "view_count", "like_count", "comment_count"],
                    "timestamp_column": "fetched_at",
                    "max_age_hours": 48,  # 2 days
                },
                "youtube_comments": {
                    "required_columns": ["video_id", "comment_id", "comment_text", "author_name"]
                    # No timestamp validation for comments
                },
            }

            # Run comprehensive validation
            validation_results = self.data_validator.run_comprehensive_validation(tables_config)

            if validation_results["overall_status"] == "FAIL":
                issues = validation_results["issues_found"]
                raise ETLError(
                    f"System prerequisites validation failed: {'; '.join(issues)}",
                    context=context,
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.VALIDATION,
                )

            logger.info("✅ System prerequisites validation passed")

        except ETLError:
            raise
        except Exception as e:
            raise ETLError(
                "Unexpected error during system prerequisites validation",
                context=context,
                original_error=e,
                severity=ErrorSeverity.CRITICAL,
            )

    def process_youtube_data_with_validation(self, channel_ids: List[str]) -> Dict[str, Any]:
        """Process YouTube data with comprehensive validation and error handling."""
        context_item = ErrorContext(
            component="BulletproofETLRunner",
            operation="process_youtube_data_with_validation",
            user_data={"channel_count": len(channel_ids)},
        )

        processing_results = {
            "channels_processed": 0,
            "videos_extracted": 0,
            "comments_extracted": 0,
            "errors": [],
            "warnings": [],
        }

        try:
            logger.info(f"🎬 Processing YouTube data for {len(channel_ids)} channels...")

            with ProgressTracker("YouTube Data Processing", len(channel_ids)) as progress:
                for i, channel_id in enumerate(channel_ids):
                    try:
                        # Validate channel ID format
                        self._validate_channel_id(channel_id)

                        # Process individual channel with error isolation
                        channel_results = self._process_single_channel(channel_id)

                        # Update statistics
                        processing_results["videos_extracted"] += channel_results.get("videos", 0)
                        processing_results["comments_extracted"] += channel_results.get("comments", 0)
                        processing_results["channels_processed"] += 1

                        progress.update()

                    except ETLError as e:
                        error_msg = f"Channel {channel_id}: {str(e)}"
                        processing_results["errors"].append(error_msg)
                        self.execution_stats["errors_encountered"] += 1
                        logger.error(error_msg)

                        # Continue with next channel (error isolation)
                        progress.update()
                        continue

                    except Exception as e:
                        error_msg = f"Unexpected error processing channel {channel_id}: {str(e)}"
                        processing_results["errors"].append(error_msg)
                        self.execution_stats["errors_encountered"] += 1
                        logger.error(error_msg)

                        # Continue with next channel
                        progress.update()
                        continue

            # Update execution statistics
            self.execution_stats["videos_processed"] = processing_results["videos_extracted"]
            self.execution_stats["comments_processed"] = processing_results["comments_extracted"]

            logger.info(
                f"✅ YouTube data processing completed: "
                f"{processing_results['channels_processed']}/{len(channel_ids)} channels, "
                f"{processing_results['videos_extracted']} videos, "
                f"{processing_results['comments_extracted']} comments"
            )

            return processing_results

        except Exception as e:
            raise ETLError(
                "Critical failure in YouTube data processing",
                context=context,
                original_error=e,
                severity=ErrorSeverity.CRITICAL,
            )

    def _validate_channel_id(self, channel_id: str) -> None:
        """Validate YouTube channel ID format."""
        if not channel_id or not isinstance(channel_id, str):
            raise ETLError(
                f"Invalid channel ID: {channel_id}", severity=ErrorSeverity.HIGH, category=ErrorCategory.VALIDATION
            )

        if not channel_id.startswith(("UC", "UU", "UL")):
            raise ETLError(
                f"Channel ID must start with UC, UU, or UL: {channel_id}",
                severity=ErrorSeverity.HIGH,
                category=ErrorCategory.VALIDATION,
            )

    def _process_single_channel(self, channel_id: str) -> Dict[str, int]:
        """Process a single YouTube channel with error handling."""
        # This would integrate with existing YouTube API processing
        # For now, return mock results
        logger.info(f"Processing channel: {channel_id}")

        # Simulate processing with validation
        return {"videos": 10, "comments": 150}  # Mock: 10 videos processed  # Mock: 150 comments extracted

    def run_sentiment_analysis_with_validation(self) -> Dict[str, Any]:
        """Run sentiment analysis with comprehensive validation."""
        context_item = ErrorContext(
            component="BulletproofETLRunner", operation="run_sentiment_analysis_with_validation"
        )

        start_time = datetime.utcnow()

        try:
            self.structured_logger.info(
                "Running sentiment analysis with validation", context={"operation": "sentiment_analysis_start"}
            )

            # Get unprocessed comments count
            with self.engine.connect() as conn:
                from sqlalchemy import text

                result = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM youtube_comments c
                    LEFT JOIN comment_sentiment cs ON c.comment_id = cs.comment_id
                    WHERE cs.comment_id IS NULL
                """
                    )
                )
                unprocessed_count = result.fetchone()[0]

            if unprocessed_count == 0:
                self.structured_logger.info("No unprocessed comments found for sentiment analysis")
                return {"comments_processed": 0, "errors": []}

            self.structured_logger.info(
                "Processing sentiment analysis", context={"unprocessed_comments": unprocessed_count}
            )

            # Run sentiment analysis with progress tracking
            with ProgressTracker("Sentiment Analysis", unprocessed_count, log_interval=500) as progress:
                sentiment_results = self.sentiment_analyzer.score_batch(limit=unprocessed_count)
                progress.processed_items = sentiment_results.processed

            # Log performance metrics
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.performance_logger.log_operation_time(
                "sentiment_analysis", duration, {"comments_processed": sentiment_results.processed}
            )
            self.performance_logger.log_throughput("sentiment_analysis", sentiment_results.processed, duration)

            # Update execution statistics
            self.execution_stats["sentiment_scores_generated"] = sentiment_results.processed

            self.structured_logger.info(
                "Sentiment analysis completed",
                context={"comments_processed": sentiment_results.processed},
                performance_data={
                    "duration_seconds": duration,
                    "throughput_per_second": sentiment_results.processed / duration if duration > 0 else 0,
                },
            )

            return sentiment_results

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.structured_logger.error(
                "Sentiment analysis failed", context={"error": str(e), "duration_seconds": duration}
            )
            raise ETLError(
                "Failed to run sentiment analysis", context=context, original_error=e, severity=ErrorSeverity.HIGH
            )

    def run_final_data_quality_validation(self) -> Dict[str, Any]:
        """Run final comprehensive data quality validation."""
        context_item = ErrorContext(component="BulletproofETLRunner", operation="run_final_data_quality_validation")

        try:
            logger.info("🔍 Running final data quality validation...")

            # Define post - processing validation config
            final_validation_config = {
                "youtube_videos": {
                    "required_columns": ["video_id", "title", "channel_title"],
                },
                "youtube_metrics": {
                    "required_columns": ["video_id", "view_count", "like_count", "comment_count"],
                },
                "youtube_comments": {
                    "required_columns": ["video_id", "comment_text", "author_name"],
                },
                "comment_sentiment": {
                    "required_columns": ["comment_id", "sentiment_score", "confidence_score"],
                },
            }

            validation_results = self.data_validator.run_comprehensive_validation(final_validation_config)

            # Count data quality issues
            self.execution_stats["data_quality_issues"] = len(validation_results.get("issues_found", []))

            if validation_results["overall_status"] == "FAIL":
                logger.warning(f"⚠️ Data quality issues found: {validation_results['issues_found']}")
            else:
                logger.info("✅ Final data quality validation passed")

            return validation_results

        except Exception as e:
            raise ETLError(
                "Failed to run final data quality validation",
                context=context,
                original_error=e,
                severity=ErrorSeverity.MEDIUM,
            )

    def generate_execution_report(self) -> Dict[str, Any]:
        """Generate comprehensive execution report with performance metrics."""
        execution_time = None
        if self.execution_stats["start_time"] and self.execution_stats["end_time"]:
            execution_time = (self.execution_stats["end_time"] - self.execution_stats["start_time"]).total_seconds()

        # Get performance summary
        performance_summary = self.performance_logger.get_performance_summary()

        report = {
            "execution_summary": {
                "start_time": (
                    self.execution_stats["start_time"].isoformat() if self.execution_stats["start_time"] else None
                ),
                "end_time": self.execution_stats["end_time"].isoformat() if self.execution_stats["end_time"] else None,
                "execution_time_seconds": execution_time,
                "overall_status": "SUCCESS" if self.execution_stats["errors_encountered"] == 0 else "PARTIAL_SUCCESS",
            },
            "processing_statistics": {
                "videos_processed": self.execution_stats["videos_processed"],
                "comments_processed": self.execution_stats["comments_processed"],
                "sentiment_scores_generated": self.execution_stats["sentiment_scores_generated"],
                "errors_encountered": self.execution_stats["errors_encountered"],
                "data_quality_issues": self.execution_stats["data_quality_issues"],
            },
            "performance_metrics": performance_summary,
            "system_health": {
                "memory_usage_mb": self._get_memory_usage(),
                "disk_usage_gb": self._get_disk_usage(),
                "database_connections": self._get_db_connection_count(),
            },
            "recommendations": [],
        }

        # Add performance - based recommendations
        if execution_time and execution_time > 3600:  # More than 1 hour
            report["recommendations"].append("Consider optimizing pipeline performance - execution took over 1 hour")

        if self.execution_stats["errors_encountered"] > 0:
            report["recommendations"].append("Review error logs and address recurring issues")

        if self.execution_stats["data_quality_issues"] > 0:
            report["recommendations"].append("Investigate and resolve data quality issues")

        if self.execution_stats["errors_encountered"] == 0 and self.execution_stats["data_quality_issues"] == 0:
            report["recommendations"].append("ETL execution completed successfully with no issues")

        # Log the comprehensive report
        self.structured_logger.info(
            "ETL execution report generated", context=report["execution_summary"], performance_data=performance_summary
        )

        return report

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil

            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0

    def _get_disk_usage(self) -> float:
        """Get current disk usage in GB."""
        try:
            import psutil

            disk_usage = psutil.disk_usage(".")
            return disk_usage.used / 1024 / 1024 / 1024
        except ImportError:
            return 0.0

    def _get_db_connection_count(self) -> int:
        """Get current database connection count."""
        try:
            if self.engine:
                return self.engine.pool.size()
            return 0
        except Exception:
            return 0

    def run_bulletproof_etl(self, channel_ids: List[str]) -> Dict[str, Any]:
        """
        Run complete bulletproof ETL pipeline with comprehensive error handling.

        Args:
            channel_ids: List of YouTube channel IDs to process

        Returns:
            Comprehensive execution report
        """
        self.execution_stats["start_time"] = datetime.utcnow()

        try:
            logger.info("🚀 Starting bulletproof ETL pipeline...")

            # Step 1: Initialize components
            self.initialize_components()

            # Step 2: Validate system prerequisites
            self.validate_system_prerequisites()

            # Step 3: Process YouTube data with validation
            youtube_results = self.process_youtube_data_with_validation(channel_ids)

            # Step 4: Run sentiment analysis with validation
            sentiment_results = self.run_sentiment_analysis_with_validation()

            # Step 5: Final data quality validation
            final_validation = self.run_final_data_quality_validation()

            self.execution_stats["end_time"] = datetime.utcnow()

            # Generate and return comprehensive report
            execution_report = self.generate_execution_report()
            execution_report["youtube_processing"] = youtube_results
            execution_report["sentiment_analysis"] = sentiment_results
            execution_report["final_validation"] = final_validation

            logger.info("🎉 Bulletproof ETL pipeline completed successfully!")

            return execution_report

        except ETLError as e:
            self.execution_stats["end_time"] = datetime.utcnow()
            self.execution_stats["errors_encountered"] += 1

            logger.error(f"💥 ETL pipeline failed: {str(e)}")

            # Generate failure report
            failure_report = self.generate_execution_report()
            failure_report["failure_details"] = e.to_dict()
            failure_report["execution_summary"]["overall_status"] = "FAILED"

            return failure_report

        except Exception as e:
            self.execution_stats["end_time"] = datetime.utcnow()
            self.execution_stats["errors_encountered"] += 1

            logger.error(f"💥 Unexpected ETL pipeline failure: {str(e)}")

            # Generate critical failure report
            failure_report = self.generate_execution_report()
            failure_report["critical_failure"] = str(e)
            failure_report["execution_summary"]["overall_status"] = "CRITICAL_FAILURE"

            return failure_report


def main():
    """Main function for running bulletproof ETL."""
    # Example usage
    runner = BulletproofETLRunner()

    # Example channel IDs (replace with actual channels from .env)
    test_channels = ["UCcomP27Fb7_kqDBPaWKJQzg", "UC - 9-kyTW8ZkZNDHQJ6FgpwQ"]  # Example channel  # Example channel

    try:
        results = runner.run_bulletproof_etl(test_channels)

        # Print summary
        print("\n" + "=" * 60)
        print("📋 BULLETPROOF ETL EXECUTION REPORT")
        print("=" * 60)

        summary = results["execution_summary"]
        stats = results["processing_statistics"]

        print(f"🕐 Execution Time: {summary.get('execution_time_seconds', 0):.1f} seconds")
        print(f"📊 Overall Status: {summary['overall_status']}")
        print(f"🎬 Videos Processed: {stats['videos_processed']:,}")
        print(f"💬 Comments Processed: {stats['comments_processed']:,}")
        print(f"🎭 Sentiment Scores: {stats['sentiment_scores_generated']:,}")
        print(f"❌ Errors: {stats['errors_encountered']}")
        print(f"⚠️  Data Quality Issues: {stats['data_quality_issues']}")

        if results.get("recommendations"):
            print("\n💡 Recommendations:")
            for rec in results["recommendations"]:
                print(f"   • {rec}")

        print("=" * 60)

        return 0 if summary["overall_status"] in ["SUCCESS", "PARTIAL_SUCCESS"] else 1

    except Exception as e:
        logger.error(f"Critical failure in main execution: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
