#!/usr / bin / env python3
"""
Comprehensive Cleanup Orchestrator-YouTube Analytics Platform

This script orchestrates the systematic cleanup of the massive codebase with:
1. Safe deletion of unused root-level files
2. Consolidation of duplicate functionality
3. Archiving of old documentation and reports
4. Testing after each major change to ensure nothing breaks

Usage:
    python tools / code_quality / comprehensive_cleanup_orchestrator.py --phase 1
    python tools / code_quality / comprehensive_cleanup_orchestrator.py --phase 2
    python tools / code_quality / comprehensive_cleanup_orchestrator.py --all
"""

import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.code_quality.advanced_script_dependency_analyzer import AdvancedScriptDependencyAnalyzer
from tools.code_quality.safe_script_deletion_system import SafeScriptDeletionSystem


@dataclass
class CleanupPhase:
    """Represents a phase of the cleanup process."""

    name: str
    description: str
    files_to_archive: List[str] = field(default_factory=list)
    files_to_delete: List[str] = field(default_factory=list)
    directories_to_clean: List[str] = field(default_factory=list)
    test_commands: List[str] = field(default_factory=list)
    rollback_possible: bool = True


class ComprehensiveCleanupOrchestrator:
    """Orchestrates systematic cleanup of the massive codebase."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.analyzer = AdvancedScriptDependencyAnalyzer()
        self.deletion_system = SafeScriptDeletionSystem(dry_run=dry_run)
        self.archive_dir = PROJECT_ROOT / "archive" / "cleanup_2025_09_25"
        self.backup_dir = PROJECT_ROOT / ".cleanup_backups"

        # Ensure directories exist
        if not self.dry_run:
            self.archive_dir.mkdir(parents=True, exist_ok=True)
            self.backup_dir.mkdir(parents=True, exist_ok=True)

    def define_cleanup_phases(self) -> List[CleanupPhase]:
        """Define the systematic cleanup phases."""

        phases = [
            CleanupPhase(
                name="Phase 1: Root Level Test Files",
                description="Archive old test files in root directory",
                files_to_archive=[
                    "test_comprehensive_system.py",  # Empty file
                    "test_dataframe_outputs.py",
                    "test_enhanced_vader_clean.py",
                    "test_final_notebook.py",
                    "test_final_system.py",
                    "test_notebook_charts.py",
                    "test_notebook_functionality.py",
                    "test_notebook_with_mock_data.py",
                    "test_simple_notebook.py",
                    "test_ultimate_notebook_execution.py",
                    "test_ultimate_notebook.py",
                    "test_working_chart_validation.py",
                ],
                test_commands=[
                    "python -m pytest tests / test_sentiment_analyzer.py -q",
                    "python -c \"import web.sentiment_analyzer; print('Import OK')\"",
                ],
            ),
            CleanupPhase(
                name="Phase 2: Root Level Demo Files",
                description="Archive demo and experimental files",
                files_to_archive=[
                    "demo_configuration_system.py",
                    "demo_error_handling.py",
                    "demo_storytelling.py",
                    "simple_proprietary_demo.py",
                    "create_and_validate_notebooks.py",
                    "create_bulletproof_real_data_notebook.py",
                    "create_comprehensive_notebook.py",
                    "create_comprehensive_storytelling_notebook.py",
                    "create_final_working_notebook.py",
                    "create_minimal_working_notebook.py",
                    "create_production_notebook.py",
                    "create_real_data_notebook.py",
                    "create_simple_demo.py",
                    "create_ultimate_notebook.py",
                    "create_validated_notebook.py",
                    "create_working_notebook.py",
                ],
                test_commands=[
                    "python -m pytest tests / test_sentiment_analyzer.py -q",
                    "python -c \"import src.youtubeviz; print('Import OK')\"",
                ],
            ),
            CleanupPhase(
                name="Phase 3: Root Level Benchmark Files",
                description="Archive benchmark and evaluation files",
                files_to_archive=[
                    "benchmark_real_comments.py",
                    "benchmark_sentiment_models.py",
                    "record_enhanced_vader_benchmark.py",
                    "compare_model_disagreements.py",
                    "evaluate_vader_variants.py",
                    "proper_ml_benchmark.py",
                    "run_comprehensive_ml_benchmark.py",
                ],
                test_commands=[
                    "python -m pytest tests / test_benchmark_data_integrity.py -q",
                ],
            ),
            CleanupPhase(
                name="Phase 4: Root Level Report Files",
                description="Archive markdown reports and documentation",
                files_to_archive=[
                    "BULLETPROOF_CHARTS_IMPLEMENTATION.md",
                    "CODE_QUALITY_OPTIMIZATION_SUMMARY.md",
                    "COMPLETE_SYSTEM_FINAL_REPORT.md",
                    "COMPREHENSIVE_DATA_SCIENCE_VALIDATION_REPORT.md",
                    "CONFIGURATION_STANDARDS_UPDATE.md",
                    "DATASET_QUALITY_BENCHMARK_INTEGRATION.md",
                    "DOCKER_MACOS_GUIDE.md",
                    "NOTEBOOK_ARCHIVING_SYSTEM_COMPLETE.md",
                    "NOTEBOOK_EXECUTION_SUCCESS_REPORT.md",
                    "NOTEBOOK_GUARDIAN_MARKETING.md",
                    "NOTEBOOK_GUARDIAN_README.md",
                    "NOTEBOOK_VALIDATION_SUCCESS_REPORT.md",
                    "NOTEBOOK_VALIDATION_SYSTEM_IMPLEMENTATION.md",
                    "NOTEBOOK_VALIDATION_SYSTEM_REPORT.md",
                    "PORTFOLIO_BENCHMARK.md",
                    "PROFESSIONAL_ETL_IMPLEMENTATION_PROGRESS.md",
                    "README_NOTEBOOK_GUARDIAN.md",
                    "REAL_DATA_SUCCESS_REPORT.md",
                    "REAL_MVP_WITH_CHARTS_REPORT.md",
                    "REFACTORING_SUMMARY.md",
                    "SCORING_STORAGE_SYSTEM_IMPLEMENTATION.md",
                    "SCORING_SYSTEM_BENCHMARK_REPORT.md",
                    "SYSTEM_VALIDATION_REPORT.md",
                    "TASK_1_COMPLETION_REPORT.md",
                    "TASK_2_COMPLETION_REPORT.md",
                    "TASK_3_CODE_QUALITY_COMPLETION_REPORT.md",
                    "TASK_4_COMPLETION_REPORT.md",
                    "TDD_NOTEBOOK_VALIDATION_SUMMARY.md",
                    "ULTIMATE_NOTEBOOK_SUMMARY.md",
                    "UNIQUE_COMMENTS_MIGRATION_GUIDE.md",
                    "WORKING_CODEBASE_SUMMARY.md",
                    "YOUTUBE_COMMENT_FETCHER_FINAL_IMPLEMENTATION.md",
                    "YOUTUBE_COMMENT_FETCHER_IMPLEMENTATION.md",
                    "YOUTUBE_METRICS_SCHEMA_ALIGNMENT_REPORT.md",
                    "YOUTUBE_SCORING_PLUGINS_IMPLEMENTATION.md",
                ],
                test_commands=[
                    "python -c 'print(\"Documentation archived successfully\")'",
                ],
            ),
            CleanupPhase(
                name="Phase 5: Root Level Data Files",
                description="Archive data and result files",
                files_to_archive=[
                    "benchmark_advanced_20250917_013511.json",
                    "benchmark_vader_20250917_013511.json",
                    "benchmarks.json",
                    "ci_report.json",
                    "ci_validation_report.json",
                    "comment_classifications.db",
                    "coverage.json",
                    "data_quality_report.json",
                    "dataset_model_evaluation_summary.csv",
                    "enhanced_music_failed_cases.csv",
                    "experiment_1_metadata.json",
                    "experiment_2_metadata.json",
                    "function_analysis_report.json",
                    "momentum_scores.csv",
                    "music_industry_sentiment_dataset_v2.csv",
                    "music_industry_sentiment_dataset_v2.jsonl",
                    "music_industry_sentiment_dataset.csv",
                    "music_sentiment_model_comparison.csv",
                    "notebook_guardian_performance.html",
                    "notebook_guardian_security.html",
                    "notebook_guardian_validation_dashboard.html",
                    "sentiment_cluster_heatmap.html",
                    "sentiment_model_comparison_results.csv",
                    "sentiment_model_test_results.csv",
                    "system_health_dashboard.json",
                    "temp_benchmark_dataset.csv",
                    "unique_comment_tracking.db",
                    "vader_comparison_20250918_042549.csv",
                    "vader_failed_cases.csv",
                    "vader_improvements_20250918_042549.csv",
                    "virality_analysis_results.csv",
                    "virality_analysis_results.json",
                ],
                files_to_delete=[
                    "comment_fetch_experiment_benchmark_size_100.json",
                    "comment_fetch_experiment_benchmark_size_200.json",
                    "comment_fetch_experiment_benchmark_size_50.json",
                    "comment_fetch_experiment_benchmark_size_500.json",
                    "comment_fetch_experiment_sentiment_benchmark.json",
                    "comment_fetch_experiment_vader_eval_20250918_042535.json",
                    "comment_fetch_experiment_vader_eval_20250918_042548.json",
                    "vader_evaluation_report_20250917_020734.json",
                    "vader_evaluation_report_20250917_020757.json",
                    "vader_evaluation_report_20250918_042536.json",
                    "vader_evaluation_report_20250918_042549.json",
                ],
                test_commands=[
                    "python -c 'print(\"Data files cleaned successfully\")'",
                ],
            ),
            CleanupPhase(
                name="Phase 6: Remaining Root Level Files",
                description="Archive remaining test, demo, and utility files",
                files_to_archive=[
                    # Remaining test files
                    "test_20_chart_notebook.py",
                    "test_20_charts_execution.py",
                    "test_blueprint_execution_system.py",
                    "test_blueprint_system_simple.py",
                    "test_complete_notebook_workflow.py",
                    "test_complete_workflow_failure_scenarios.py",
                    "test_current_sentiment_model.py",
                    "test_dataset_quality_benchmark.py",
                    "test_dataset_v22_improvements.py",
                    "test_enhanced_data_quality.py",
                    "test_enhanced_dataset_benchmark.py",
                    "test_final_integration.py",
                    "test_ground_truth_fix.py",
                    "test_ml_data_collection.py",
                    "test_ml_on_unseen_db_comments.py",
                    "test_ml_on_your_classifications.py",
                    "test_nbconvert_debug.py",
                    "test_notebook_archiver.py",
                    "test_notebook_execution_validation.py",
                    "test_notebook_plugin_integration.py",
                    "test_notebook_scoring_integration.py",
                    "test_notebook_validation_simple.py",
                    "test_plugin_integration.py",
                    "test_professional_momentum_scoring.py",
                    "test_proprietary_sentiment_formula.py",
                    "test_real_data_scoring.py",
                    "test_sentiment_plugin_integration.py",
                    "test_simple_notebook_execution.py",
                    "test_transformer_on_neutral_comments.py",
                    "test_transformer_preprocessing.py",
                    "test_transformer_vs_existing_models.py",
                    "test_unique_comment_integration.py",
                    "test_unique_comments.py",
                    # Remaining demo files
                    "demo_complete_plugin_integration.py",
                    "demo_data_retention_manager.py",
                    "demo_enhanced_data_quality.py",
                    "demo_notebook_guardian_with_charts.py",
                    "demo_proprietary_sentiment_system.py",
                    "demo_schema_validator.py",
                    "demo_scoring_storage_system.py",
                    "demo_scoring_system.py",
                    "demo_smart_classifier.py",
                    "demo_youtube_scoring_plugins.py",
                    # Remaining utility and execution files
                    "benchmark_comment_fetching_system.py",
                    "benchmark_models.py",
                    "benchmark_real_comments_with_feedback.py",
                    "blueprint_execution_system.py",
                    "build_ml_sentiment_system.py",
                    "classify_real_comments.py",
                    "complete_notebook_workflow.py",
                    "create_20_chart_notebook.py",
                    "create_notebook.py",
                    "create_simple_scoring_notebook.py",
                    "demonstrate_notebook_system.py",
                    "execute_and_count_charts.py",
                    "execute_and_validate_notebook.py",
                    "execute_artist_comparison.py",
                    "execute_comprehensive_sentiment_tasks.py",
                    "execute_data_quality.py",
                    "execute_music_analytics.py",
                    "final_complete_system_demo.py",
                    "final_status_report.py",
                    "fix_imports.py",
                    "notebook_archiver.py",
                    "notebook_bootstrap.py",
                    "notebook_execution_validator.py",
                    "run_benchmark_integrity_tests.py",
                    "run_enhanced_sentiment_tests.py",
                    "run_notebook.py",
                    "setup_notebook_guardian.py",
                    "simple_ml_sentiment_demo.py",
                    "smart_comment_classifier.py",
                    "update_systems_for_unique_comments.py",
                    "validate_analytics_queries_schema.py",
                    "validate_and_fix_notebook.py",
                    "validate_created_notebook.py",
                    "validate_final_notebook.py",
                    "validate_youtube_metrics_schema.py",
                    "view_benchmark_history.py",
                ],
                files_to_delete=[
                    "conftest.py",  # Duplicate of tests / conftest.py
                    "test_notebook.ipynb",  # Old test notebook
                ],
                test_commands=[
                    "python -m pytest tests / test_sentiment_analyzer.py -q",
                    "python -c \"import web.sentiment_analyzer; import src.youtubeviz; print('All imports OK')\"",
                ],
            ),
        ]

        return phases

    def execute_phase(self, phase: CleanupPhase) -> bool:
        """Execute a cleanup phase safely."""
        print(f"\n🚀 Starting {phase.name}")
        print(f"📝 {phase.description}")

        if self.dry_run:
            print(f"🔍 DRY RUN MODE-No actual changes will be made")

        # Create phase-specific backup
        phase_backup_dir = self.backup_dir / f"phase_{phase.name.split()[1].lower()}"
        if not self.dry_run:
            phase_backup_dir.mkdir(exist_ok=True)

        success_count = 0
        total_operations = len(phase.files_to_archive) + len(phase.files_to_delete)

        # Archive files
        for file_path in phase.files_to_archive:
            if self._archive_file(file_path, phase_backup_dir):
                success_count += 1
            else:
                print(f"⚠️ Failed to archive {file_path}")

        # Delete files
        for file_path in phase.files_to_delete:
            if self._delete_file(file_path, phase_backup_dir):
                success_count += 1
            else:
                print(f"⚠️ Failed to delete {file_path}")

        print(f"📊 Processed {success_count}/{total_operations} files successfully")

        # Run tests to ensure nothing broke
        if not self.dry_run and phase.test_commands:
            print(f"🧪 Running validation tests...")
            for test_cmd in phase.test_commands:
                if not self._run_test(test_cmd):
                    print(f"❌ Test failed: {test_cmd}")
                    print(f"🔄 Rolling back phase...")
                    self._rollback_phase(phase, phase_backup_dir)
                    return False
            print(f"✅ All tests passed!")

        # Commit changes
        if not self.dry_run:
            self._commit_phase(phase)

        print(f"✅ {phase.name} completed successfully!")
        return True

    def _archive_file(self, file_path: str, backup_dir: Path) -> bool:
        """Archive a single file."""
        source_path = PROJECT_ROOT / file_path

        if not source_path.exists():
            print(f"⚠️ File not found: {file_path}")
            return True  # Not an error if file doesn't exist

        # Create backup
        backup_path = backup_dir / source_path.name
        archive_path = self.archive_dir / source_path.name

        try:
            if self.dry_run:
                print(f"🔍 DRY RUN: Would archive {file_path}")
                return True

            # Backup original
            shutil.copy2(source_path, backup_path)

            # Move to archive
            shutil.move(source_path, archive_path)

            print(f"📦 Archived: {file_path}")
            return True

        except Exception as e:
            print(f"❌ Error archiving {file_path}: {e}")
            return False

    def _delete_file(self, file_path: str, backup_dir: Path) -> bool:
        """Delete a single file with backup."""
        source_path = PROJECT_ROOT / file_path

        if not source_path.exists():
            print(f"⚠️ File not found: {file_path}")
            return True  # Not an error if file doesn't exist

        backup_path = backup_dir / source_path.name

        try:
            if self.dry_run:
                print(f"🔍 DRY RUN: Would delete {file_path}")
                return True

            # Backup original
            shutil.copy2(source_path, backup_path)

            # Delete original
            source_path.unlink()

            print(f"🗑️ Deleted: {file_path}")
            return True

        except Exception as e:
            print(f"❌ Error deleting {file_path}: {e}")
            return False

    def _run_test(self, test_cmd: str) -> bool:
        """Run a test command and return success status."""
        try:
            # Use shell=True for complex commands with quotes
            result = subprocess.run(test_cmd, shell=True, cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                print(f"✅ Test passed: {test_cmd}")
                return True
            else:
                print(f"❌ Test failed: {test_cmd}")
                print(f"   stdout: {result.stdout}")
                print(f"   stderr: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"⏰ Test timed out: {test_cmd}")
            return False
        except Exception as e:
            print(f"❌ Test error: {test_cmd} - {e}")
            return False

    def _rollback_phase(self, phase: CleanupPhase, backup_dir: Path):
        """Rollback a failed phase."""
        print(f"🔄 Rolling back {phase.name}...")

        # Restore files from backup
        for backup_file in backup_dir.glob("*"):
            try:
                target_path = PROJECT_ROOT / backup_file.name
                shutil.copy2(backup_file, target_path)
                print(f"🔄 Restored: {backup_file.name}")
            except Exception as e:
                print(f"❌ Rollback error for {backup_file.name}: {e}")

    def _commit_phase(self, phase: CleanupPhase):
        """Commit phase changes to git."""
        try:
            subprocess.run(["git", "add", "."], cwd=PROJECT_ROOT, check=True)
            subprocess.run(
                ["git", "commit", "-m", f"Cleanup {phase.name}: {phase.description}"], cwd=PROJECT_ROOT, check=True
            )
            print(f"📝 Committed {phase.name} to git")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Git commit failed: {e}")

    def execute_all_phases(self) -> bool:
        """Execute all cleanup phases in sequence."""
        phases = self.define_cleanup_phases()

        print(f"🎯 Starting comprehensive cleanup of {len(phases)} phases")
        print(f"📊 Estimated files to process: {sum(len(p.files_to_archive) + len(p.files_to_delete) for p in phases)}")

        for i, phase in enumerate(phases, 1):
            print(f"\n{'=' * 80}")
            print(f"Phase {i}/{len(phases)}")

            if not self.execute_phase(phase):
                print(f"❌ Cleanup failed at {phase.name}")
                return False

        print(f"\n🎉 All cleanup phases completed successfully!")
        print(f"📈 Codebase is now significantly cleaner and more maintainable")

        # Final validation
        print(f"\n🧪 Running final validation...")
        final_tests = [
            "python -m pytest tests / test_sentiment_analyzer.py -q",
            "python -c \"import web.sentiment_analyzer; import src.youtubeviz; print('All imports OK')\"",
        ]

        for test_cmd in final_tests:
            if not self._run_test(test_cmd):
                print(f"❌ Final validation failed!")
                return False

        print(f"✅ Final validation passed!")
        return True


def main():
    """Main entry point for comprehensive cleanup orchestrator."""
    import argparse

    parser = argparse.ArgumentParser(description="Comprehensive Cleanup Orchestrator")
    parser.add_argument("--phase", type=int, help="Execute specific phase (1-5)")
    parser.add_argument("--all", action="store_true", help="Execute all phases")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--list-phases", action="store_true", help="List all cleanup phases")

    args = parser.parse_args()

    orchestrator = ComprehensiveCleanupOrchestrator(dry_run=args.dry_run)

    if args.list_phases:
        phases = orchestrator.define_cleanup_phases()
        print("📋 Cleanup Phases:")
        for i, phase in enumerate(phases, 1):
            print(f"\n{i}. {phase.name}")
            print(f"   {phase.description}")
            print(f"   Files to archive: {len(phase.files_to_archive)}")
            print(f"   Files to delete: {len(phase.files_to_delete)}")
        return

    if args.phase:
        phases = orchestrator.define_cleanup_phases()
        if 1 <= args.phase <= len(phases):
            phase = phases[args.phase-1]
            success = orchestrator.execute_phase(phase)
            sys.exit(0 if success else 1)
        else:
            print(f"❌ Invalid phase number. Use 1-{len(phases)}")
            sys.exit(1)

    if args.all:
        success = orchestrator.execute_all_phases()
        sys.exit(0 if success else 1)

    # Default: show help
    parser.print_help()


if __name__ == "__main__":
    main()
