#!/usr / bin / env python3
"""
Safe Script Deletion System-YouTube Analytics Platform

This script provides a safe, reversible system for deleting and consolidating scripts.
It focuses on:

1. Script deletion validation with dependency checking
2. Consolidation engine that extracts reusable functions
3. Reversible deletion with git history preservation
4. Test validation to ensure no functionality is broken

Usage:
    python tools / code_quality / safe_script_deletion_system.py --validate script.py
    python tools / code_quality / safe_script_deletion_system.py --delete script.py
    python tools / code_quality / safe_script_deletion_system.py --consolidate script1.py script2.py
    python tools / code_quality / safe_script_deletion_system.py --extract-helpers script.py
"""

import ast
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.code_quality.advanced_script_dependency_analyzer import AdvancedScriptDependencyAnalyzer, SafetyAssessment


@dataclass
class DeletionPlan:
    """Plan for safely deleting a script."""

    target_file: str
    safety_assessment: SafetyAssessment
    backup_location: Optional[str] = None
    affected_files: List[str] = field(default_factory=list)
    required_updates: List[Tuple[str, str, str]] = field(default_factory=list)  # file, old_import, new_import
    extracted_functions: List[str] = field(default_factory=list)
    rollback_commands: List[str] = field(default_factory=list)


@dataclass
class ConsolidationPlan:
    """Plan for consolidating multiple scripts."""

    source_files: List[str]
    target_helper_module: str
    extracted_functions: Dict[str, str] = field(default_factory=dict)  # function_name -> source_file
    import_updates: List[Tuple[str, str, str]] = field(default_factory=list)  # file, old_import, new_import
    files_to_delete: List[str] = field(default_factory=list)
    backup_locations: Dict[str, str] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of validation after deletion / consolidation."""

    tests_passed: bool = False
    linting_passed: bool = False
    import_errors: List[str] = field(default_factory=list)
    test_failures: List[str] = field(default_factory=list)
    lint_errors: List[str] = field(default_factory=list)
    rollback_required: bool = False


class SafeScriptDeletionSystem:
    """System for safely deleting and consolidating scripts."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.analyzer = AdvancedScriptDependencyAnalyzer()
        self.backup_dir = PROJECT_ROOT / ".cleanup_backups"
        self.helper_modules_dir = PROJECT_ROOT / "src" / "youtubeviz"

        # Ensure backup directory exists
        if not self.dry_run:
            self.backup_dir.mkdir(exist_ok=True)

    def validate_deletion(self, script_path: str) -> SafetyAssessment:
        """Validate that a script can be safely deleted."""
        print(f"🔍 Validating deletion safety for: {script_path}")

        assessment = self.analyzer.validate_safe_deletion(script_path)

        print(f"Safety Assessment:")
        print(f"  Safe to delete: {'✅ YES' if assessment.is_safe_to_delete else '❌ NO'}")
        print(f"  Risk level: {assessment.risk_level}")
        print(f"  Recommended action: {assessment.recommended_action}")
        print(f"  Rationale: {assessment.rationale}")

        if assessment.blocking_dependencies:
            print(f"  Blocking dependencies: {len(assessment.blocking_dependencies)}")
            for dep in assessment.blocking_dependencies[:3]:
                print(f"    - {dep}")
            if len(assessment.blocking_dependencies) > 3:
                print(f"    ... and {len(assessment.blocking_dependencies) - 3} more")

        return assessment

    def create_deletion_plan(self, script_path: str) -> DeletionPlan:
        """Create a detailed plan for deleting a script."""
        assessment = self.validate_deletion(script_path)

        plan = DeletionPlan(target_file=script_path, safety_assessment=assessment)

        if not assessment.is_safe_to_delete:
            print(f"❌ Cannot create deletion plan-script is not safe to delete")
            return plan

        # Create backup location
        script_name = Path(script_path).name
        timestamp = subprocess.run(["date", "+%Y % m%d_ % H%M % S"], capture_output=True, text=True).stdout.strip()
        plan.backup_location = str(self.backup_dir / f"{script_name}.backup.{timestamp}")

        # Find affected files (files that import this script)
        if hasattr(self.analyzer, "dependency_map") and self.analyzer.dependency_map.usage_patterns:
            usage = self.analyzer.dependency_map.usage_patterns.get(str(Path(script_path).resolve()))
            if usage:
                plan.affected_files = usage.imported_by.copy()

        # Create rollback commands
        plan.rollback_commands = [
            f"cp {plan.backup_location} {script_path}",
            f"git add {script_path}",
            f"git commit -m 'Rollback: Restore {script_name}'",
        ]

        return plan

    def execute_deletion(self, plan: DeletionPlan) -> bool:
        """Execute a deletion plan safely."""
        if not plan.safety_assessment.is_safe_to_delete:
            print(f"❌ Deletion aborted-script is not safe to delete")
            return False

        script_path = Path(plan.target_file)

        if self.dry_run:
            print(f"🔍 DRY RUN: Would delete {script_path}")
            print(f"🔍 DRY RUN: Would backup to {plan.backup_location}")
            return True

        try:
            # Create backup
            if plan.backup_location:
                print(f"💾 Creating backup: {plan.backup_location}")
                shutil.copy2(script_path, plan.backup_location)

            # Remove the file
            print(f"🗑️ Deleting: {script_path}")
            script_path.unlink()

            # Commit the deletion
            if self._is_git_repo():
                subprocess.run(["git", "add", str(script_path)], cwd=PROJECT_ROOT, check=True)
                subprocess.run(
                    [
                        "git",
                        "commit",
                        "-m",
                        f"Safe deletion: Remove {script_path.name} (backed up to {plan.backup_location})",
                    ],
                    cwd=PROJECT_ROOT,
                    check=True,
                )

            print(f"✅ Successfully deleted {script_path}")
            return True

        except Exception as e:
            print(f"❌ Error during deletion: {e}")
            # Attempt rollback if backup exists
            if plan.backup_location and Path(plan.backup_location).exists():
                try:
                    shutil.copy2(plan.backup_location, script_path)
                    print(f"🔄 Rollback successful-restored from backup")
                except Exception as rollback_error:
                    print(f"💥 Rollback failed: {rollback_error}")
            return False

    def extract_helper_functions(self, script_path: str, target_module: Optional[str] = None) -> List[str]:
        """Extract reusable functions from a script to a helper module."""
        script_file = Path(script_path)

        if not script_file.exists():
            print(f"❌ Script not found: {script_path}")
            return []

        # Determine target module
        if not target_module:
            target_module = f"{script_file.stem}_helpers.py"

        target_path = self.helper_modules_dir / target_module

        print(f"🔧 Extracting helper functions from {script_path}")
        print(f"📁 Target module: {target_path}")

        try:
            with open(script_file, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content, filename=str(script_file))

            # Find functions that could be extracted
            extractable_functions = []

            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    # Check if function is a good candidate for extraction
                    if self._is_extractable_function(node, content):
                        extractable_functions.append(node.name)

            if not extractable_functions:
                print(f"ℹ️ No extractable functions found in {script_path}")
                return []

            print(f"🎯 Found {len(extractable_functions)} extractable functions:")
            for func_name in extractable_functions:
                print(f"  - {func_name}")

            if self.dry_run:
                print(f"🔍 DRY RUN: Would extract functions to {target_path}")
                return extractable_functions

            # Create helper module with extracted functions
            self._create_helper_module(script_file, target_path, extractable_functions)

            return extractable_functions

        except Exception as e:
            print(f"❌ Error extracting functions: {e}")
            return []

    def _is_extractable_function(self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef], content: str) -> bool:
        """Check if a function is a good candidate for extraction."""
        # Skip private functions
        if node.name.startswith("_"):
            return False

        # Skip very short functions
        if node.end_lineno and node.lineno and (node.end_lineno-node.lineno) < 5:
            return False

        # Skip functions with complex dependencies (simplified check)
        function_content = ast.get_source_segment(content, node)
        if function_content:
            # Skip if it uses many global variables or imports
            if function_content.count("global ") > 2:
                return False
            if function_content.count("import ") > 3:
                return False

        return True

    def _create_helper_module(self, source_file: Path, target_path: Path, function_names: List[str]):
        """Create a helper module with extracted functions."""
        try:
            with open(source_file, "r", encoding="utf-8") as f:
                content = f.read()

            tree = ast.parse(content, filename=str(source_file))

            # Extract function definitions
            extracted_functions = []
            imports = []

            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    imports.append(ast.get_source_segment(content, node))
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if node.name in function_names:
                        func_content = ast.get_source_segment(content, node)
                        if func_content:
                            extracted_functions.append(func_content)

            # Create helper module content
            helper_content = f'''#!/usr / bin / env python3
"""
Helper functions extracted from {source_file.name}

This module contains reusable functions that were extracted during codebase cleanup
to reduce duplication and improve maintainability.

Original source: {source_file}
Extraction date: {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}
"""

{chr(10).join(set(imp for imp in imports if imp is not None)) if imports else ""}

{chr(10).join(extracted_functions)}
'''

            # Write helper module
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(helper_content)

            print(f"✅ Created helper module: {target_path}")

            # Update __init__.py to export the functions
            init_file = target_path.parent / "__init__.py"
            if init_file.exists():
                with open(init_file, "a", encoding="utf-8") as f:
                    f.write(f"\n# Extracted from {source_file.name}\n")
                    for func_name in function_names:
                        f.write(f"from .{target_path.stem} import {func_name}\n")

        except Exception as e:
            print(f"❌ Error creating helper module: {e}")
            raise

    def create_consolidation_plan(self, source_files: List[str]) -> ConsolidationPlan:
        """Create a plan for consolidating multiple scripts."""
        print(f"📋 Creating consolidation plan for {len(source_files)} files")

        # Determine target helper module name
        common_prefix = os.path.commonprefix([Path(f).stem for f in source_files])
        if len(common_prefix) > 3:
            target_module = f"{common_prefix}_consolidated.py"
        else:
            target_module = "consolidated_helpers.py"

        plan = ConsolidationPlan(source_files=source_files, target_helper_module=target_module)

        # Analyze each source file
        for source_file in source_files:
            assessment = self.validate_deletion(source_file)

            if assessment.is_safe_to_delete:
                plan.files_to_delete.append(source_file)

                # Create backup location
                script_name = Path(source_file).name
                timestamp = subprocess.run(
                    ["date", "+%Y % m%d_ % H%M % S"], capture_output=True, text=True
                ).stdout.strip()
                backup_location = str(self.backup_dir / f"{script_name}.consolidation_backup.{timestamp}")
                plan.backup_locations[source_file] = backup_location

                # Extract functions
                extractable_functions = self.extract_helper_functions(source_file, target_module)
                for func_name in extractable_functions:
                    plan.extracted_functions[func_name] = source_file
            else:
                print(f"⚠️ Cannot consolidate {source_file} - not safe to delete")

        return plan

    def execute_consolidation(self, plan: ConsolidationPlan) -> bool:
        """Execute a consolidation plan."""
        if not plan.files_to_delete:
            print(f"❌ No files can be safely consolidated")
            return False

        if self.dry_run:
            print(f"🔍 DRY RUN: Would consolidate {len(plan.files_to_delete)} files")
            print(f"🔍 DRY RUN: Would create helper module: {plan.target_helper_module}")
            return True

        try:
            # Create backups
            for source_file, backup_location in plan.backup_locations.items():
                print(f"💾 Backing up: {source_file} -> {backup_location}")
                shutil.copy2(source_file, backup_location)

            # Create consolidated helper module
            target_path = self.helper_modules_dir / plan.target_helper_module
            self._create_consolidated_module(plan, target_path)

            # Delete source files
            for source_file in plan.files_to_delete:
                print(f"🗑️ Deleting consolidated file: {source_file}")
                Path(source_file).unlink()

            # Commit changes
            if self._is_git_repo():
                subprocess.run(["git", "add", "."], cwd=PROJECT_ROOT, check=True)
                subprocess.run(
                    [
                        "git",
                        "commit",
                        "-m",
                        f"Consolidation: Merge {len(plan.files_to_delete)} files into {plan.target_helper_module}",
                    ],
                    cwd=PROJECT_ROOT,
                    check=True,
                )

            print(f"✅ Successfully consolidated {len(plan.files_to_delete)} files")
            return True

        except Exception as e:
            print(f"❌ Error during consolidation: {e}")
            # Attempt rollback
            self._rollback_consolidation(plan)
            return False

    def _create_consolidated_module(self, plan: ConsolidationPlan, target_path: Path):
        """Create a consolidated helper module from multiple source files."""
        all_imports = set()
        all_functions = []

        for source_file in plan.files_to_delete:
            try:
                with open(source_file, "r", encoding="utf-8") as f:
                    content = f.read()

                tree = ast.parse(content, filename=source_file)

                # Extract imports
                for node in ast.walk(tree):
                    if isinstance(node, (ast.Import, ast.ImportFrom)):
                        import_stmt = ast.get_source_segment(content, node)
                        if import_stmt and not import_stmt.startswith("from ."):  # Skip relative imports
                            all_imports.add(import_stmt)

                # Extract functions that are in the plan
                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if node.name in plan.extracted_functions:
                            func_content = ast.get_source_segment(content, node)
                            if func_content:
                                all_functions.append(f"# From {Path(source_file).name}\n{func_content}")

            except Exception as e:
                print(f"⚠️ Error processing {source_file}: {e}")

        # Create consolidated module content
        consolidated_content = f'''#!/usr / bin / env python3
"""
Consolidated Helper Functions

This module contains functions consolidated from multiple source files during
codebase cleanup to reduce duplication and improve maintainability.

Consolidated from:
{chr(10).join(f"- {Path(f).name}" for f in plan.files_to_delete)}

Consolidation date: {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}
"""

{chr(10).join(sorted(all_imports))}

{chr(10).join(all_functions)}
'''

        # Write consolidated module
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(consolidated_content)

        print(f"✅ Created consolidated module: {target_path}")

    def _rollback_consolidation(self, plan: ConsolidationPlan):
        """Rollback a failed consolidation."""
        print(f"🔄 Rolling back consolidation...")

        try:
            # Restore backed up files
            for source_file, backup_location in plan.backup_locations.items():
                if Path(backup_location).exists():
                    shutil.copy2(backup_location, source_file)
                    print(f"🔄 Restored: {source_file}")

            # Remove consolidated module if it was created
            target_path = self.helper_modules_dir / plan.target_helper_module
            if target_path.exists():
                target_path.unlink()
                print(f"🗑️ Removed failed consolidation: {target_path}")

            print(f"✅ Rollback completed")

        except Exception as e:
            print(f"💥 Rollback failed: {e}")

    def validate_after_changes(self) -> ValidationResult:
        """Validate the codebase after deletion / consolidation changes."""
        print(f"🧪 Validating codebase after changes...")

        result = ValidationResult()

        # Run tests
        try:
            print(f"🧪 Running tests...")
            test_result = subprocess.run(
                ["python", "-m", "pytest", "--tb=short", "-q"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            result.tests_passed = test_result.returncode == 0
            if not result.tests_passed:
                result.test_failures = test_result.stdout.split("\n")
                print(f"❌ Tests failed")
            else:
                print(f"✅ Tests passed")

        except subprocess.TimeoutExpired:
            print(f"⏰ Tests timed out")
            result.test_failures = ["Test execution timed out"]
        except Exception as e:
            print(f"❌ Error running tests: {e}")
            result.test_failures = [str(e)]

        # Run linting
        try:
            print(f"🔍 Running linting...")
            lint_result = subprocess.run(
                ["python", "-m", "flake8", "--max-line-length=120", "--select=E,W,F"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
            )

            result.linting_passed = lint_result.returncode == 0
            if not result.linting_passed:
                result.lint_errors = lint_result.stdout.split("\n")
                print(f"⚠️ Linting issues found")
            else:
                print(f"✅ Linting passed")

        except Exception as e:
            print(f"❌ Error running linting: {e}")
            result.lint_errors = [str(e)]

        # Check for import errors
        try:
            print(f"🔍 Checking for import errors...")
            import_result = subprocess.run(
                ["python", "-c", 'import sys; sys.path.insert(0, "."); import web; import src'],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
            )

            if import_result.returncode != 0:
                result.import_errors = import_result.stderr.split("\n")
                print(f"❌ Import errors found")
            else:
                print(f"✅ No import errors")

        except Exception as e:
            print(f"❌ Error checking imports: {e}")
            result.import_errors = [str(e)]

        # Determine if rollback is required
        result.rollback_required = not (result.tests_passed and result.linting_passed and not result.import_errors)

        return result

    def _is_git_repo(self) -> bool:
        """Check if we're in a git repository."""
        try:
            subprocess.run(["git", "rev-parse", "--git-dir"], cwd=PROJECT_ROOT, check=True, capture_output=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False


def main():
    """Main entry point for safe script deletion system."""
    import argparse

    parser = argparse.ArgumentParser(description="Safe Script Deletion System")
    parser.add_argument("--validate", type=str, help="Validate deletion safety for a script")
    parser.add_argument("--delete", type=str, help="Safely delete a script")
    parser.add_argument("--consolidate", nargs="+", help="Consolidate multiple scripts")
    parser.add_argument("--extract-helpers", type=str, help="Extract helper functions from a script")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--target-module", type=str, help="Target module for extracted functions")

    args = parser.parse_args()

    if not any([args.validate, args.delete, args.consolidate, args.extract_helpers]):
        parser.print_help()
        sys.exit(1)

    # Create deletion system
    system = SafeScriptDeletionSystem(dry_run=args.dry_run)

    # Validate deletion safety
    if args.validate:
        assessment = system.validate_deletion(args.validate)
        sys.exit(0 if assessment.is_safe_to_delete else 1)

    # Delete script
    if args.delete:
        plan = system.create_deletion_plan(args.delete)
        if system.execute_deletion(plan):
            # Validate after deletion
            validation = system.validate_after_changes()
            if validation.rollback_required:
                print(f"⚠️ Validation failed-consider rollback")
                sys.exit(1)
            else:
                print(f"✅ Deletion completed successfully")
                sys.exit(0)
        else:
            sys.exit(1)

    # Extract helper functions
    if args.extract_helpers:
        extracted = system.extract_helper_functions(args.extract_helpers, args.target_module)
        if extracted:
            print(f"✅ Extracted {len(extracted)} functions")
            sys.exit(0)
        else:
            print(f"❌ No functions extracted")
            sys.exit(1)

    # Consolidate scripts
    if args.consolidate:
        consolidation_plan = system.create_consolidation_plan(args.consolidate)
        if system.execute_consolidation(consolidation_plan):
            # Validate after consolidation
            validation = system.validate_after_changes()
            if validation.rollback_required:
                print(f"⚠️ Validation failed-consider rollback")
                sys.exit(1)
            else:
                print(f"✅ Consolidation completed successfully")
                sys.exit(0)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
