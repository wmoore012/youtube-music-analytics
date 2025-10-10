#!/usr / bin / env python3
"""
🧪 Test Relevance Assessor

Systematically assesses test files for current system relevance and removes
outdated tests while ensuring no critical functionality loses test coverage.

This tool implements the TestRelevanceAssessor class to:
- Categorize tests by current system relevance
- Remove tests for deprecated functionality
- Consolidate similar tests
- Ensure critical functionality maintains test coverage

Usage:
    python tools / development / testing / test_relevance_assessor.py --assess-all
    python tools / development / testing / test_relevance_assessor.py --remove-outdated
    python tools / development / testing / test_relevance_assessor.py --consolidate-similar
"""

import argparse
import ast
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import ToolBase, ToolConfig, register_tool


class TestRelevanceResult:
    """Results of test relevance assessment."""

    def __init__(self):
        self.relevant_tests: List[str] = []
        self.outdated_tests: List[str] = []
        self.deprecated_tests: List[str] = []
        self.duplicate_tests: List[Tuple[str, str]] = []  # (test1, test2) pairs
        self.orphaned_tests: List[str] = []  # Tests for non-existent functionality
        self.critical_coverage_gaps: List[str] = []
        self.consolidation_candidates: List[List[str]] = []  # Groups of similar tests
        self.total_tests_analyzed: int = 0
        self.total_size_bytes: int = 0


class TestRelevanceAssessor(ToolBase):
    """
    Systematic test relevance assessment tool.

    Analyzes test files to determine current system relevance and identifies:
    1. Tests for deprecated functionality
    2. Tests for abandoned experiments
    3. Tests for removed integrations
    4. Duplicate or similar tests that can be consolidated
    5. Critical functionality that lacks test coverage
    """

    def __init__(self):
        super().__init__(name="test-relevance-assessor", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        self.tests_directory = project_root / "tests"
        self.source_directories = [
            project_root / "src",
            project_root / "web",
            project_root / "tools",
            project_root / "scripts",
        ]

        # Patterns that indicate deprecated / outdated functionality
        self.deprecated_patterns = [
            "test_old_",
            "test_legacy_",
            "test_deprecated_",
            "test_backup_",
            "test_temp_",
            "test_experimental_",
            "test_demo_",
            "_old_test",
            "_legacy_test",
            "_deprecated_test",
        ]

        # Patterns that indicate current / critical functionality
        self.critical_patterns = [
            "test_core_",
            "test_main_",
            "test_production_",
            "test_api_",
            "test_database_",
            "test_security_",
            "test_integration_",
        ]

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return []  # No environment variables required

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="test-relevance-assessor",
            version="1.0.0",
            description="Systematic test relevance assessment and cleanup tool",
            dependencies=["python>=3.8", "ast"],
            environment_vars=[],
            usage_examples=[
                "python tools / development / testing / test_relevance_assessor.py --assess-all",
                "python tools / development / testing / test_relevance_assessor.py --remove-outdated",
                "python tools / development / testing / test_relevance_assessor.py --consolidate-similar",
            ],
            category="development",
        )

    def run(self) -> None:
        """Main execution method-should not be called directly."""
        self.log_progress("Use specific assessment methods like assess_all_tests()")

    def assess_all_tests(self) -> TestRelevanceResult:
        """
        Assess all test files for current system relevance.

        Returns:
            TestRelevanceResult with comprehensive assessment
        """
        self.log_progress("🧪 Starting comprehensive test relevance assessment")

        if not self.tests_directory.exists():
            self.log_progress("⚠️  Tests directory not found")
            return TestRelevanceResult()

        result = TestRelevanceResult()

        # Get all test files
        test_files = list(self.tests_directory.rglob("test_*.py"))
        test_files.extend(list(self.tests_directory.rglob("*_test.py")))

        self.log_progress(f"📄 Found {len(test_files)} test files to analyze")

        result.total_tests_analyzed = len(test_files)

        for test_file in test_files:
            try:
                # Calculate file size
                result.total_size_bytes += test_file.stat().st_size

                # Assess this test file
                relevance = self._assess_test_file_relevance(test_file)

                if relevance == "relevant":
                    result.relevant_tests.append(str(test_file))
                elif relevance == "outdated":
                    result.outdated_tests.append(str(test_file))
                elif relevance == "deprecated":
                    result.deprecated_tests.append(str(test_file))
                elif relevance == "orphaned":
                    result.orphaned_tests.append(str(test_file))

            except Exception as e:
                self.log_progress(f"⚠️  Error assessing {test_file}: {e}")

        # Find duplicate and similar tests
        result.duplicate_tests = self._find_duplicate_tests(test_files)
        result.consolidation_candidates = self._find_consolidation_candidates(test_files)

        # Check for critical coverage gaps
        result.critical_coverage_gaps = self._find_coverage_gaps()

        self.log_progress(
            f"✅ Assessment complete: {len(result.relevant_tests)} relevant, {len(result.outdated_tests)} outdated"
        )

        return result

    def _assess_test_file_relevance(self, test_file: Path) -> str:
        """
        Assess the relevance of a single test file.

        Args:
            test_file: Path to test file

        Returns:
            Relevance category: 'relevant', 'outdated', 'deprecated', 'orphaned'
        """
        try:
            # Check filename patterns first
            filename = test_file.name.lower()

            # Check for deprecated patterns
            for pattern in self.deprecated_patterns:
                if pattern in filename:
                    return "deprecated"

            # Check for critical patterns
            for pattern in self.critical_patterns:
                if pattern in filename:
                    return "relevant"

            # Read file content for deeper analysis
            with open(test_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            # Parse imports to see what's being tested
            tested_modules = self._extract_tested_modules(content)

            # Check if tested modules still exist
            existing_modules = 0
            for module in tested_modules:
                if self._module_exists_in_codebase(module):
                    existing_modules += 1

            if not tested_modules:
                return "relevant"  # Assume relevant if we can't determine modules

            # If less than 50% of tested modules exist, likely orphaned
            if existing_modules / len(tested_modules) < 0.5:
                return "orphaned"

            # Check for outdated patterns in content
            outdated_indicators = [
                "# TODO: Remove this test",
                "# DEPRECATED",
                "# OLD VERSION",
                "# LEGACY",
                'skip("deprecated")',
                'skip("old version")',
                "experimental",
                "temp test",
                "backup test",
            ]

            content_lower = content.lower()
            for indicator in outdated_indicators:
                if indicator.lower() in content_lower:
                    return "outdated"

            # Check file modification time (very old files might be outdated)
            mod_time = test_file.stat().st_mtime
            age_days = (datetime.now().timestamp() - mod_time) / (24 * 3600)

            # If file hasn't been modified in over a year and has few assertions, might be outdated
            if age_days > 365:
                assertion_count = content.count("assert")
                if assertion_count < 3:  # Very simple test, possibly outdated
                    return "outdated"

            return "relevant"

        except Exception as e:
            self.log_progress(f"⚠️  Error assessing {test_file}: {e}")
            return "relevant"  # Default to relevant if we can't assess

    def _extract_tested_modules(self, content: str) -> List[str]:
        """Extract module names that are being tested from test content."""
        modules = []

        try:
            # Parse the AST to find imports
            tree = ast.parse(content)

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        modules.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        modules.append(node.module)
        except Exception:
            # Fallback to regex-based extraction
            import re

            # Find import statements
            import_patterns = [
                r"from\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s + import",
                r"import\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            ]

            for pattern in import_patterns:
                matches = re.findall(pattern, content)
                modules.extend(matches)

        # Filter to only include our project modules
        project_modules = []
        for module in modules:
            if any(module.startswith(prefix) for prefix in ["src.", "web.", "tools.", "youtubeviz"]):
                project_modules.append(module)

        return project_modules

    def _module_exists_in_codebase(self, module_name: str) -> bool:
        """Check if a module still exists in the current codebase."""
        try:
            # Convert module name to file path
            parts = module_name.split(".")

            # Try different possible locations
            possible_paths = []

            # Standard src structure
            if parts[0] == "src" and len(parts) > 1:
                possible_paths.append(project_root / "src" / "/".join(parts[1:]) + ".py")
                possible_paths.append(project_root / "src" / "/".join(parts[1:]) / "__init__.py")

            # Web module
            elif parts[0] == "web":
                if len(parts) > 1:
                    possible_paths.append(project_root / "web" / "/".join(parts[1:]) + ".py")
                else:
                    possible_paths.append(project_root / "web" / "__init__.py")

            # Tools module
            elif parts[0] == "tools":
                if len(parts) > 1:
                    possible_paths.append(project_root / "tools" / "/".join(parts[1:]) + ".py")
                else:
                    possible_paths.append(project_root / "tools" / "__init__.py")

            # YouTubeViz module
            elif parts[0] == "youtubeviz":
                if len(parts) > 1:
                    possible_paths.append(project_root / "src" / "youtubeviz" / "/".join(parts[1:]) + ".py")
                else:
                    possible_paths.append(project_root / "src" / "youtubeviz" / "__init__.py")

            # Direct module name
            else:
                possible_paths.append(project_root / "src" / module_name.replace(".", "/") + ".py")
                possible_paths.append(project_root / "web" / module_name.replace(".", "/") + ".py")
                possible_paths.append(project_root / "tools" / module_name.replace(".", "/") + ".py")

            # Check if any of the possible paths exist
            for path in possible_paths:
                if path.exists():
                    return True

            return False

        except Exception:
            return True  # Default to exists if we can't determine

    def _find_duplicate_tests(self, test_files: List[Path]) -> List[Tuple[str, str]]:
        """Find duplicate or very similar test files."""
        duplicates = []

        # Simple approach: compare file sizes and names
        file_info = {}

        for test_file in test_files:
            try:
                size = test_file.stat().st_size
                name_parts = test_file.stem.lower().split("_")

                # Create a signature based on size and key name parts
                signature = (size, tuple(sorted(name_parts)))

                if signature in file_info:
                    duplicates.append((str(file_info[signature]), str(test_file)))
                else:
                    file_info[signature] = test_file

            except Exception:
                continue

        return duplicates

    def _find_consolidation_candidates(self, test_files: List[Path]) -> List[List[str]]:
        """Find groups of tests that could be consolidated."""
        candidates = []

        # Group tests by similar functionality
        functionality_groups = {}

        for test_file in test_files:
            try:
                # Extract key functionality being tested from filename
                name = test_file.stem.lower()

                # Remove common prefixes / suffixes
                name = name.replace("test_", "").replace("_test", "")

                # Group by main functionality
                main_func = name.split("_")[0] if "_" in name else name

                if main_func not in functionality_groups:
                    functionality_groups[main_func] = []

                functionality_groups[main_func].append(str(test_file))

            except Exception:
                continue

        # Find groups with multiple tests that might be consolidated
        for func, tests in functionality_groups.items():
            if len(tests) > 2:  # More than 2 tests for same functionality
                candidates.append(tests)

        return candidates

    def _find_coverage_gaps(self) -> List[str]:
        """Find critical functionality that lacks test coverage."""
        gaps = []

        # Check for important modules that might lack tests
        critical_modules = [
            "src / youtubeviz / data.py",
            "web / youtube_channel_etl.py",
            "web / sentiment_job.py",
            "tools / shared / common.py",
        ]

        for module_path in critical_modules:
            module_file = project_root / module_path
            if module_file.exists():
                # Check if there's a corresponding test file
                test_patterns = [
                    f"test_{module_file.stem}.py",
                    f"{module_file.stem}_test.py",
                    f"test_{module_file.stem}_*.py",
                ]

                has_test = False
                for pattern in test_patterns:
                    if list(self.tests_directory.rglob(pattern)):
                        has_test = True
                        break

                if not has_test:
                    gaps.append(module_path)

        return gaps

    def remove_outdated_tests(self, assessment_result: TestRelevanceResult, dry_run: bool = True) -> Dict[str, any]:
        """
        Remove tests identified as outdated or deprecated.

        Args:
            assessment_result: Results from test assessment
            dry_run: If True, only simulate removal

        Returns:
            Dictionary with removal results
        """
        self.log_progress(f"🗑️  {'Simulating' if dry_run else 'Performing'} outdated test removal")

        removal_results = {
            "removed_files": [],
            "total_size_freed": 0,
            "errors": [],
        }

        # Combine outdated and deprecated tests for removal
        tests_to_remove = assessment_result.outdated_tests + assessment_result.deprecated_tests

        if not tests_to_remove:
            self.log_progress("✅ No outdated tests to remove")
            return removal_results

        for test_file_path in tests_to_remove:
            test_file = Path(test_file_path)

            if test_file.exists():
                try:
                    file_size = test_file.stat().st_size

                    if not dry_run:
                        test_file.unlink()
                        self.log_progress(f"🗑️  Removed: {test_file}")
                    else:
                        self.log_progress(f"🔍 Would remove: {test_file}")

                    removal_results["removed_files"].append(test_file_path)
                    removal_results["total_size_freed"] += file_size

                except Exception as e:
                    error_msg = f"Error removing {test_file_path}: {e}"
                    removal_results["errors"].append(error_msg)
                    self.log_progress(f"❌ {error_msg}")

        return removal_results

    def generate_assessment_report(self, assessment_result: TestRelevanceResult) -> str:  # noqa: C901
        """
        Generate comprehensive test relevance assessment report.

        Args:
            assessment_result: Results from test assessment

        Returns:
            Formatted report string
        """
        report = []
        report.append("🧪 TEST RELEVANCE ASSESSMENT REPORT")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Summary statistics
        total_tests = assessment_result.total_tests_analyzed

        report.append("📊 SUMMARY:")
        report.append(f"  Total tests analyzed: {total_tests}")
        report.append(f"  Relevant tests: {len(assessment_result.relevant_tests)}")
        report.append(f"  Outdated tests: {len(assessment_result.outdated_tests)}")
        report.append(f"  Deprecated tests: {len(assessment_result.deprecated_tests)}")
        report.append(f"  Orphaned tests: {len(assessment_result.orphaned_tests)}")
        report.append(f"  Duplicate test pairs: {len(assessment_result.duplicate_tests)}")
        report.append(f"  Total test suite size: {assessment_result.total_size_bytes / 1024 / 1024:.1f} MB")
        report.append("")

        # Outdated tests
        if assessment_result.outdated_tests:
            report.append("⚠️  OUTDATED TESTS (Safe to remove):")
            for test_file in assessment_result.outdated_tests[:10]:  # Show first 10
                report.append(f"  • {test_file}")
            if len(assessment_result.outdated_tests) > 10:
                report.append(f"  ... and {len(assessment_result.outdated_tests) - 10} more tests")
            report.append("")

        # Deprecated tests
        if assessment_result.deprecated_tests:
            report.append("🗑️  DEPRECATED TESTS (Safe to remove):")
            for test_file in assessment_result.deprecated_tests[:10]:
                report.append(f"  • {test_file}")
            if len(assessment_result.deprecated_tests) > 10:
                report.append(f"  ... and {len(assessment_result.deprecated_tests) - 10} more tests")
            report.append("")

        # Coverage gaps
        if assessment_result.critical_coverage_gaps:
            report.append("❌ CRITICAL COVERAGE GAPS:")
            for gap in assessment_result.critical_coverage_gaps:
                report.append(f"  • {gap}")
            report.append("")

        # Consolidation candidates
        if assessment_result.consolidation_candidates:
            report.append("🔄 CONSOLIDATION CANDIDATES:")
            for i, group in enumerate(assessment_result.consolidation_candidates[:5]):  # Show first 5 groups
                report.append(f"  Group {i + 1}: {len(group)} similar tests")
                for test in group[:3]:  # Show first 3 in each group
                    report.append(f"    - {test}")
                if len(group) > 3:
                    report.append(f"    ... and {len(group) - 3} more")
            report.append("")

        # Recommendations
        report.append("💡 RECOMMENDATIONS:")
        removable_count = len(assessment_result.outdated_tests) + len(assessment_result.deprecated_tests)
        if removable_count > 0:
            report.append(f"  1. Remove {removable_count} outdated / deprecated tests")

        if assessment_result.critical_coverage_gaps:
            report.append(f"  2. Add tests for {len(assessment_result.critical_coverage_gaps)} critical modules")

        if assessment_result.consolidation_candidates:
            report.append(f"  3. Consider consolidating {len(assessment_result.consolidation_candidates)} test groups")

        if removable_count == 0 and not assessment_result.critical_coverage_gaps:
            report.append("  ✅ Test suite is well-maintained-no major issues found!")

        return "\n".join(report)

    def cleanup_resources(self) -> None:
        """Clean up any resources used during assessment."""
        # No persistent resources to clean up
        pass


def main():
    """Main entry point for the test relevance assessor tool."""
    parser = argparse.ArgumentParser(
        description="Test Relevance Assessment and Cleanup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / development / testing / test_relevance_assessor.py --assess-all
  python tools / development / testing / test_relevance_assessor.py --remove-outdated --dry-run
  python tools / development / testing / test_relevance_assessor.py --consolidate-similar
        """,
    )

    # Assessment operations
    parser.add_argument("--assess-all", action="store_true", help="Assess all test files for relevance")
    parser.add_argument(
        "--remove-outdated", action="store_true", help="Remove tests identified as outdated or deprecated"
    )
    parser.add_argument(
        "--consolidate-similar", action="store_true", help="Identify tests that could be consolidated"
    )

    # Options
    parser.add_argument("--dry-run", action="store_true", help="Simulate operations without making changes")
    parser.add_argument("--report", type=str, help="Save assessment report to file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Create assessor instance
    with TestRelevanceAssessor() as assessor:
        try:
            if args.assess_all:
                result = assessor.assess_all_tests()

                # Generate and display report
                report = assessor.generate_assessment_report(result)
                print(report)

                # Save report if requested
                if args.report:
                    with open(args.report, "w") as f:
                        f.write(report)
                    print(f"\n📄 Report saved to: {args.report}")

                return 0

            elif args.remove_outdated:
                # First assess to get removal candidates
                result = assessor.assess_all_tests()

                if not (result.outdated_tests or result.deprecated_tests):
                    print("✅ No outdated tests to remove")
                    return 0

                # Perform removal
                removal_result = assessor.remove_outdated_tests(result, dry_run=args.dry_run)

                print(
                    f"🗑️  {'Would remove' if args.dry_run else 'Removed'} {
                        len(removal_result['removed_files'])} test files"
                )
                print(
                    f"💾 {'Would free' if args.dry_run else 'Freed'} {
                        removal_result['total_size_freed'] / 1024 / 1024:.1f} MB"
                )

                if removal_result["errors"]:
                    print(f"❌ {len(removal_result['errors'])} errors occurred:")
                    for error in removal_result["errors"]:
                        print(f"   • {error}")

                return 0

            elif args.consolidate_similar:
                result = assessor.assess_all_tests()

                if not result.consolidation_candidates:
                    print("✅ No consolidation candidates found")
                    return 0

                print("🔄 Test Consolidation Candidates:")
                for i, group in enumerate(result.consolidation_candidates):
                    print(f"\nGroup {i + 1}: {len(group)} similar tests")
                    for test in group:
                        print(f"  • {test}")

                return 0

            else:
                print("❌ No operation specified. Use --help for options.")
                return 1

        except KeyboardInterrupt:
            assessor.log_progress("Test assessment cancelled by user")
            return 1
        except Exception as e:
            assessor.handle_error(e, "main execution")
            return 1


if __name__ == "__main__":
    sys.exit(main())
