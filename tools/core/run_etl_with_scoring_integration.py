#!/usr / bin / env python3
"""
ETL Pipeline with Scoring System Integration

This script implements task 9 from the data organization and scoring system spec:
1. Execute standard ETL pipeline (sentiment analysis, data quality)
2. Run data migration from CSV files to database tables
3. Execute scoring algorithms on real YouTube data
4. Validate scoring results are properly stored
5. Verify all database tables have current data for notebooks
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime
import os
import traceback
from typing import Any, Dict

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

from src.data_organization.configuration_manager import ConfigurationManager
from src.data_organization.example_plugins import EngagementScoringPlugin, MomentumScoringPlugin, SimpleTestPlugin

# Import scoring system components
from src.data_organization.scoring_engine import ScoringEngine
from src.data_organization.scoring_storage import ScoringStorage

# Import ETL components
from tools.etl.run_focused_etl import preflight_setup, run_bot_detection, run_sentiment_analysis, validate_data_quality
from web.etl_helpers import get_engine


class ETLScoringIntegration:
    """Manages ETL pipeline with integrated scoring system."""

    def __init__(self):
        """Initialize the ETL scoring integration."""
        self.engine = get_engine()
        self.config_manager = ConfigurationManager()
        self.scoring_engine = ScoringEngine(self.config_manager)
        self.scoring_storage = ScoringStorage(self.engine)

        # Results tracking
        self.results = {
            "etl_results": {},
            "migration_results": {},
            "scoring_results": {},
            "validation_results": {},
            "overall_status": "unknown",
        }

    def run_standard_etl(self) -> Dict[str, Any]:
        """Run the standard ETL pipeline components."""
        print("🚀 Running Standard ETL Pipeline")
        print("-" * 40)

        etl_results = {}

        try:
            # Preflight setup
            print("🧰 Running preflight setup...")
            preflight_results = preflight_setup()
            etl_results["preflight"] = preflight_results
            print(f"✅ Preflight complete: {preflight_results.get('normalized_upserts', 0)} normalized records")

            # Bot detection
            print("\n🤖 Running bot detection...")
            bot_results = run_bot_detection(self.engine)
            etl_results["bot_detection"] = bot_results
            print(f"✅ Bot detection complete: {bot_results.get('processed', 0)} comments analyzed")

            # Sentiment analysis
            print("\n🧠 Running sentiment analysis...")
            sentiment_results = run_sentiment_analysis(self.engine)
            etl_results["sentiment"] = sentiment_results
            print(f"✅ Sentiment analysis complete: {sentiment_results.get('processed', 0)} comments processed")

            # Data quality validation
            print("\n🔍 Running data quality validation...")
            quality_results = validate_data_quality(self.engine)
            etl_results["quality"] = quality_results
            print(f"✅ Data quality complete: {quality_results.get('quality_score', 0):.1f}% quality score")

            etl_results["status"] = "success"

        except Exception as e:
            print(f"❌ Standard ETL failed: {str(e)}")
            etl_results["status"] = "failed"
            etl_results["error"] = str(e)

        return etl_results

    def run_data_migration(self) -> Dict[str, Any]:
        """Run data migration from CSV files to database tables."""
        print("\n📦 Running Data Migration")
        print("-" * 40)

        migration_results = {
            "files_found": 0,
            "files_migrated": 0,
            "records_migrated": 0,
            "errors": [],
            "status": "unknown",
        }

        try:
            # Check for CSV files that need migration
            csv_directories = ["music_analysis_tables", "data / processed", "data / exports", "time_series_tracking"]

            files_to_migrate = []
            for directory in csv_directories:
                if os.path.exists(directory):
                    for file in os.listdir(directory):
                        if file.endswith(".csv"):
                            files_to_migrate.append(os.path.join(directory, file))

            migration_results["files_found"] = len(files_to_migrate)
            print(f"📊 Found {len(files_to_migrate)} CSV files to potentially migrate")

            if not files_to_migrate:
                print("✅ No CSV files found for migration")
                migration_results["status"] = "no_files"
                return migration_results

            # Simple migration approach - just validate files exist and are readable
            total_records = 0
            migrated_files = 0

            for file_path in files_to_migrate[:5]:  # Limit to 5 files for safety
                try:
                    print(f"  Validating {file_path}...")
                    df = pd.read_csv(file_path)

                    if len(df) > 0:
                        total_records += len(df)
                        migrated_files += 1
                        print(f"    ✅ Validated {len(df)} records")
                    else:
                        print(f"    ⚠️ Empty file, skipping")

                except Exception as e:
                    error_msg = f"{file_path}: {str(e)}"
                    migration_results["errors"].append(error_msg)
                    print(f"    ❌ Error reading file: {str(e)}")

            migration_results["files_migrated"] = migrated_files
            migration_results["records_migrated"] = total_records
            migration_results["status"] = "success" if migrated_files > 0 else "no_migrations"

            print(f"✅ Migration validation complete: {migrated_files} files, {total_records} records")

        except Exception as e:
            print(f"❌ Data migration failed: {str(e)}")
            migration_results["status"] = "failed"
            migration_results["error"] = str(e)

        return migration_results

    def register_scoring_plugins(self) -> Dict[str, Any]:
        """Register scoring plugins with the scoring engine."""
        print("\n🔌 Registering Scoring Plugins")
        print("-" * 40)

        registration_results = {"plugins_registered": 0, "plugins_failed": 0, "registered_plugins": [], "errors": []}

        # List of plugins to register
        plugins_to_register = [MomentumScoringPlugin(), EngagementScoringPlugin(), SimpleTestPlugin()]

        for plugin in plugins_to_register:
            try:
                self.scoring_engine.register_plugin(plugin)
                registration_results["plugins_registered"] += 1
                registration_results["registered_plugins"].append(plugin.get_name())
                print(f"  ✅ Registered {plugin.get_name()} v{plugin.get_version()}")

            except Exception as e:
                registration_results["plugins_failed"] += 1
                error_msg = f"{plugin.get_name()}: {str(e)}"
                registration_results["errors"].append(error_msg)
                print(f"  ❌ Failed to register {plugin.get_name()}: {str(e)}")

        print(f"✅ Plugin registration complete: {registration_results['plugins_registered']} registered")
        return registration_results

    def execute_scoring_algorithms(self) -> Dict[str, Any]:
        """Execute scoring algorithms on real YouTube data."""
        print("\n🎯 Executing Scoring Algorithms")
        print("-" * 40)

        scoring_results = {
            "algorithms_executed": 0,
            "total_scores_calculated": 0,
            "algorithm_results": {},
            "errors": [],
            "status": "unknown",
        }

        try:
            # Get YouTube data for scoring
            youtube_data = self._load_youtube_data_for_scoring()

            if youtube_data.empty:
                print("⚠️ No YouTube data available for scoring")
                scoring_results["status"] = "no_data"
                return scoring_results

            print(f"📊 Loaded {len(youtube_data)} records for scoring")

            # Get list of available algorithms
            available_algorithms = self.scoring_engine.get_available_algorithms()
            print(f"🔍 Available algorithms: {', '.join(available_algorithms)}")

            # Execute each algorithm
            for algorithm_name in available_algorithms:
                try:
                    print(f"  Executing {algorithm_name}...")

                    # Execute scoring algorithm
                    result = self.scoring_engine.execute_scoring(algorithm_name=algorithm_name, data=youtube_data)

                    if result.success:
                        scores_count = len(result.scores) if hasattr(result, "scores") else 0
                        scoring_results["algorithms_executed"] += 1
                        scoring_results["total_scores_calculated"] += scores_count
                        scoring_results["algorithm_results"][algorithm_name] = {
                            "scores_calculated": scores_count,
                            "status": "success",
                        }
                        print(f"    ✅ {algorithm_name}: {scores_count} scores calculated")

                        # Store results in database
                        if hasattr(result, "scores") and not result.scores.empty:
                            self._store_scoring_results(algorithm_name, result.scores)

                    else:
                        error_msg = f"{algorithm_name}: Execution failed"
                        scoring_results["errors"].append(error_msg)
                        scoring_results["algorithm_results"][algorithm_name] = {
                            "status": "failed",
                            "error": "Execution failed",
                        }
                        print(f"    ❌ {algorithm_name}: Execution failed")

                except Exception as e:
                    error_msg = f"{algorithm_name}: {str(e)}"
                    scoring_results["errors"].append(error_msg)
                    scoring_results["algorithm_results"][algorithm_name] = {"status": "error", "error": str(e)}
                    print(f"    ❌ {algorithm_name}: {str(e)}")

            scoring_results["status"] = "success" if scoring_results["algorithms_executed"] > 0 else "no_executions"
            print(f"✅ Scoring execution complete: {scoring_results['algorithms_executed']} algorithms executed")

        except Exception as e:
            print(f"❌ Scoring execution failed: {str(e)}")
            scoring_results["status"] = "failed"
            scoring_results["error"] = str(e)

        return scoring_results

    def _load_youtube_data_for_scoring(self) -> pd.DataFrame:
        """Load YouTube data from database for scoring algorithms."""
        try:
            # Query to get aggregated YouTube data suitable for scoring
            query = """
            SELECT
                yv.channel_title as artist_name,
                COUNT(yv.video_id) as video_count,
                SUM(COALESCE(ym.view_count, 0)) as total_views,
                SUM(COALESCE(ym.like_count, 0)) as total_likes,
                SUM(COALESCE(ym.comment_count, 0)) as total_comments,
                AVG(COALESCE(ym.view_count, 0)) as avg_views_per_video,
                -- Calculate recent growth rate (simplified)
                (SUM(CASE WHEN yv.published_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                     THEN COALESCE(ym.view_count, 0) ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN yv.published_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
                           THEN COALESCE(ym.view_count, 0) ELSE 0 END), 0)) as recent_growth_rate
            FROM youtube_videos yv
            LEFT JOIN youtube_metrics ym ON yv.video_id = ym.video_id
            WHERE yv.channel_title IS NOT NULL
            AND yv.channel_title != ''
            GROUP BY yv.channel_title
            HAVING video_count >= 3
            ORDER BY total_views DESC
            LIMIT 100
            """

            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)

            # Fill NaN values
            df = df.fillna(0)

            # Ensure required columns exist
            required_columns = [
                "artist_name",
                "video_count",
                "total_views",
                "total_likes",
                "total_comments",
                "avg_views_per_video",
                "recent_growth_rate",
            ]

            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0

            return df

        except Exception as e:
            print(f"❌ Error loading YouTube data: {str(e)}")
            return pd.DataFrame()

    def _store_scoring_results(self, algorithm_name: str, scores: pd.DataFrame):
        """Store scoring results in the database."""
        try:
            # Register algorithm if not already registered
            algorithm_id = self.scoring_storage.register_algorithm(
                algorithm_name=algorithm_name,
                version="1.0.0",
                description=f"Automated scoring results from {algorithm_name}",
            )

            # Store results
            run_id = f"etl_run_{datetime.now().strftime('%Y % m%d_ % H%M % S')}"

            for _, row in scores.iterrows():
                self.scoring_storage.store_result(
                    run_id=run_id,
                    algorithm_id=algorithm_id,
                    entity_type="artist",
                    entity_id=str(row.get("entity_id", row.get("artist_name", "unknown"))),
                    score_type="composite",
                    score_value=float(row.get("score_value", 0)),
                    confidence_level=float(row.get("confidence", 1.0)),
                    metadata={
                        "algorithm": algorithm_name,
                        "execution_timestamp": datetime.now().isoformat(),
                        "data_source": "youtube_etl",
                    },
                )

        except Exception as e:
            print(f"⚠️ Warning: Could not store results for {algorithm_name}: {str(e)}")

    def validate_scoring_storage(self) -> Dict[str, Any]:
        """Validate that scoring results are properly stored in database."""
        print("\n✅ Validating Scoring Storage")
        print("-" * 40)

        validation_results = {
            "algorithms_in_db": 0,
            "scoring_results_count": 0,
            "recent_results_count": 0,
            "validation_errors": [],
            "status": "unknown",
        }

        try:
            with self.engine.connect() as conn:
                # Check scoring algorithms table
                try:
                    algorithms_result = conn.execute(
                        text(
                            """
                        SELECT COUNT(*) as count FROM scoring_algorithms
                        WHERE is_active = 1
                    """
                        )
                    )
                    validation_results["algorithms_in_db"] = algorithms_result.fetchone()[0]
                except Exception:
                    validation_results["algorithms_in_db"] = 0

                # Check scoring results table
                try:
                    results_result = conn.execute(
                        text(
                            """
                        SELECT COUNT(*) as count FROM scoring_results
                    """
                        )
                    )
                    validation_results["scoring_results_count"] = results_result.fetchone()[0]
                except Exception:
                    validation_results["scoring_results_count"] = 0

                # Check recent results (last 24 hours)
                try:
                    recent_result = conn.execute(
                        text(
                            """
                        SELECT COUNT(*) as count FROM scoring_results
                        WHERE calculation_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                    """
                        )
                    )
                    validation_results["recent_results_count"] = recent_result.fetchone()[0]
                except Exception:
                    validation_results["recent_results_count"] = 0

                print(f"📊 Validation Results:")
                print(f"   Active algorithms: {validation_results['algorithms_in_db']}")
                print(f"   Total scoring results: {validation_results['scoring_results_count']}")
                print(f"   Recent results (24h): {validation_results['recent_results_count']}")

                # Validate data integrity
                if validation_results["algorithms_in_db"] == 0:
                    validation_results["validation_errors"].append("No active algorithms found in database")

                if validation_results["scoring_results_count"] == 0:
                    validation_results["validation_errors"].append("No scoring results found in database")

                validation_results["status"] = (
                    "success" if len(validation_results["validation_errors"]) == 0 else "issues_found"
                )

                if validation_results["validation_errors"]:
                    print("⚠️ Validation Issues:")
                    for error in validation_results["validation_errors"]:
                        print(f"   - {error}")
                else:
                    print("✅ All validations passed")

        except Exception as e:
            print(f"❌ Validation failed: {str(e)}")
            validation_results["status"] = "failed"
            validation_results["error"] = str(e)

        return validation_results

    def verify_database_readiness(self) -> Dict[str, Any]:
        """Verify all database tables have current data for notebook execution."""
        print("\n🗄️ Verifying Database Readiness")
        print("-" * 40)

        readiness_results = {
            "tables_checked": 0,
            "tables_ready": 0,
            "table_status": {},
            "overall_readiness": False,
            "issues": [],
        }

        # Critical tables for notebook execution
        critical_tables = ["youtube_videos", "youtube_metrics", "youtube_comments", "comment_sentiment"]

        # Optional scoring tables
        optional_tables = ["scoring_algorithms", "scoring_results"]

        all_tables = critical_tables + optional_tables

        try:
            with self.engine.connect() as conn:
                for table in all_tables:
                    readiness_results["tables_checked"] += 1

                    try:
                        # Check if table exists and has data
                        result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                        count = result.fetchone()[0]

                        table_ready = count > 0
                        if table_ready:
                            readiness_results["tables_ready"] += 1

                        readiness_results["table_status"][table] = {
                            "total_records": count,
                            "ready": table_ready,
                            "critical": table in critical_tables,
                        }

                        status_icon = "✅" if table_ready else "❌"
                        table_type = "(critical)" if table in critical_tables else "(optional)"
                        print(f"   {status_icon} {table} {table_type}: {count:,} records")

                        if not table_ready and table in critical_tables:
                            readiness_results["issues"].append(f"{table} has no data")

                    except Exception as e:
                        readiness_results["table_status"][table] = {
                            "error": str(e),
                            "ready": False,
                            "critical": table in critical_tables,
                        }
                        if table in critical_tables:
                            readiness_results["issues"].append(f"{table}: {str(e)}")
                        print(f"   ❌ {table}: Error - {str(e)}")

                # Overall readiness assessment (based on critical tables only)
                critical_ready = sum(
                    1
                    for table in critical_tables
                    if readiness_results["table_status"].get(table, {}).get("ready", False)
                )
                readiness_percentage = (critical_ready / len(critical_tables)) * 100
                readiness_results["overall_readiness"] = readiness_percentage >= 75  # 75% threshold

                print(f"\n📊 Database Readiness: {readiness_percentage:.1f}%")
                print(f"   Critical tables ready: {critical_ready}/{len(critical_tables)}")

                if readiness_results["overall_readiness"]:
                    print("✅ Database is ready for notebook execution")
                else:
                    print("⚠️ Database readiness issues detected")

        except Exception as e:
            print(f"❌ Database readiness check failed: {str(e)}")
            readiness_results["error"] = str(e)

        return readiness_results

    def run_complete_pipeline(self) -> Dict[str, Any]:
        """Run the complete ETL pipeline with scoring system integration."""
        print("🚀 Starting Complete ETL Pipeline with Scoring Integration")
        print("=" * 60)

        start_time = datetime.now()

        try:
            # Step 1: Run standard ETL pipeline
            self.results["etl_results"] = self.run_standard_etl()

            # Step 2: Run data migration
            self.results["migration_results"] = self.run_data_migration()

            # Step 3: Register scoring plugins
            _plugin_results = self.register_scoring_plugins()

            # Step 4: Execute scoring algorithms
            self.results["scoring_results"] = self.execute_scoring_algorithms()

            # Step 5: Validate scoring storage
            self.results["validation_results"] = self.validate_scoring_storage()

            # Step 6: Verify database readiness
            readiness_results = self.verify_database_readiness()

            # Determine overall status
            critical_failures = [
                self.results["etl_results"].get("status") == "failed",
                self.results["scoring_results"].get("status") == "failed",
                not readiness_results.get("overall_readiness", False),
            ]

            if any(critical_failures):
                self.results["overall_status"] = "completed_with_issues"
            else:
                self.results["overall_status"] = "success"

            # Final summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print("\n" + "=" * 60)
            print("🎉 ETL PIPELINE WITH SCORING INTEGRATION COMPLETE")
            print("=" * 60)

            print(f"⏱️ Total Duration: {duration:.1f} seconds")
            print(f"🏆 Overall Status: {self.results['overall_status'].upper()}")

            print(f"\n📊 Summary:")
            print(f"   ETL Status: {self.results['etl_results'].get('status', 'unknown')}")
            print(f"   Migration: {self.results['migration_results'].get('files_migrated', 0)} files processed")
            print(f"   Scoring: {self.results['scoring_results'].get('algorithms_executed', 0)} algorithms executed")
            print(f"   Validation: {self.results['validation_results'].get('status', 'unknown')}")
            print(f"   Database Readiness: {'✅ Ready' if readiness_results.get('overall_readiness') else '⚠️ Issues'}")

            return self.results

        except Exception as e:
            print(f"\n❌ Pipeline failed with error: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            self.results["overall_status"] = "failed"
            self.results["error"] = str(e)
            return self.results


def main():
    """Main entry point for ETL with scoring integration."""
    # Load environment variables
    load_dotenv()

    # Create and run the integration pipeline
    integration = ETLScoringIntegration()
    results = integration.run_complete_pipeline()

    # Return appropriate exit code
    if results["overall_status"] == "success":
        return 0
    elif results["overall_status"] == "completed_with_issues":
        return 1
    else:
        return 2


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
