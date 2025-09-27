#!/usr / bin / env python3
"""
Advanced Script Dependency Analyzer - YouTube Analytics Platform

This script extends the duplicate code analyzer with dependency mapping and script usage tracking.
It focuses on:

1. Script dependency analysis and import mapping
2. Script usage tracking to identify actively used vs unused scripts
3. Safe deletion assessment for script consolidation
4. Duplicate functionality detection across scripts
5. Helper function extraction recommendations

Usage:
    python tools / code_quality / advanced_script_dependency_analyzer.py --analyze
    python tools / code_quality / advanced_script_dependency_analyzer.py --dependencies
    python tools / code_quality / advanced_script_dependency_analyzer.py --usage
    python tools / code_quality / advanced_script_dependency_analyzer.py --safety - check script_name.py
"""

import ast
from collections import defaultdict
from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Dict, List, Optional, Set, Tuple, Union

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.code_quality.duplicate_code_analyzer import (
    CodeBlock,
    DuplicateCodeAnalyzer,
    DuplicationGroup,
    DuplicationReport,
)


@dataclass
class ScriptDependency:
    """Represents a dependency relationship between scripts."""

    source_file: str
    target_file: str
    import_type: str  # "direct", "from", "dynamic"
    import_statement: str
    line_number: int
    is_external: bool = False


@dataclass
class ScriptUsage:
    """Tracks usage patterns for a script."""

    file_path: str
    is_executable: bool = False
    has_main_block: bool = False
    imported_by: List[str] = field(default_factory=list)
    imports_from: List[str] = field(default_factory=list)
    last_modified: Optional[float] = None
    git_activity: Dict[str, int] = field(default_factory=dict)  # commits, additions, deletions
    complexity_score: int = 0
    function_count: int = 0
    class_count: int = 0


@dataclass
class SafetyAssessment:
    """Assessment of whether a script can be safely deleted or consolidated."""

    file_path: str
    is_safe_to_delete: bool = False
    risk_level: str = "HIGH"  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
    blocking_dependencies: List[str] = field(default_factory=list)
    usage_indicators: List[str] = field(default_factory=list)
    consolidation_candidates: List[str] = field(default_factory=list)
    recommended_action: str = ""
    rationale: str = ""


@dataclass
class DependencyMap:
    """Complete dependency mapping for the codebase."""

    dependencies: List[ScriptDependency] = field(default_factory=list)
    usage_patterns: Dict[str, ScriptUsage] = field(default_factory=dict)
    safety_assessments: Dict[str, SafetyAssessment] = field(default_factory=dict)
    consolidation_opportunities: List[DuplicationGroup] = field(default_factory=list)
    unused_scripts: List[str] = field(default_factory=list)
    high_risk_scripts: List[str] = field(default_factory=list)


class AdvancedScriptDependencyAnalyzer(DuplicateCodeAnalyzer):
    """Advanced analyzer that combines duplication detection with dependency analysis."""

    def __init__(self, extract_mode: bool = False):
        super().__init__(extract_mode)
        self.dependency_map = DependencyMap()

        # Additional configuration for dependency analysis
        self.script_extensions = [".py"]
        self.executable_patterns = [
            r'if\s + __name__\s*==\s*["\']__main__["\']',
            r"#!/usr / bin / env python",
            r"#!/usr / bin / python",
        ]

        # Git analysis configuration
        self.git_available = self._check_git_availability()

    def _check_git_availability(self) -> bool:
        """Check if git is available and we're in a git repository."""
        try:
            result = subprocess.run(["git", "rev - parse", "--git - dir"],
                                    cwd=PROJECT_ROOT, capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False

    def _extract_imports(self, file_path: Path) -> List[ScriptDependency]:
        """Extract import dependencies from a Python file."""
        dependencies = []

        try:
            with open(file_path, "r", encoding="utf - 8") as f:
                content = f.read()

            tree = ast.parse(content, filename=str(file_path))

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        dep = ScriptDependency(
                            source_file=str(file_path),
                            target_file=alias.name,
                            import_type="direct",
                            import_statement=f"import {alias.name}",
                            line_number=node.lineno,
                            is_external=not self._is_local_module(alias.name),
                        )
                        dependencies.append(dep)

                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        dep = ScriptDependency(
                            source_file=str(file_path),
                            target_file=node.module,
                            import_type="from",
                            import_statement=f"from {node.module} import {
                                ', '.join(alias.name for alias in node.names)}",
                            line_number=node.lineno,
                            is_external=not self._is_local_module(node.module),
                        )
                        dependencies.append(dep)

        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"Warning: Could not parse imports from {file_path}: {e}")

        return dependencies

    def _is_local_module(self, module_name: str) -> bool:
        """Check if a module is part of the local codebase."""
        # Check if it's a relative import or matches our project structure
        local_prefixes = ["web", "src", "tools", "scripts", "tests"]

        if module_name.startswith("."):
            return True

        for prefix in local_prefixes:
            if module_name.startswith(prefix):
                return True

        return False

    def _analyze_script_usage(self, file_path: Path) -> ScriptUsage:
        """Analyze usage patterns for a script."""
        usage = ScriptUsage(file_path=str(file_path))

        try:
            with open(file_path, "r", encoding="utf - 8") as f:
                content = f.read()

            # Check if executable
            for pattern in self.executable_patterns:
                if re.search(pattern, content):
                    usage.is_executable = True
                    break

            # Check for main block
            usage.has_main_block = bool(re.search(r'if\s + __name__\s*==\s*["\']__main__["\']', content))

            # Parse AST for complexity analysis
            try:
                tree = ast.parse(content, filename=str(file_path))

                for node in ast.walk(tree):
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        usage.function_count += 1
                    elif isinstance(node, ast.ClassDef):
                        usage.class_count += 1

                # Simple complexity score based on AST nodes
                usage.complexity_score = len(list(ast.walk(tree)))

            except SyntaxError:
                pass

            # File system metadata
            stat = file_path.stat()
            usage.last_modified = stat.st_mtime

            # Git activity analysis
            if self.git_available:
                usage.git_activity = self._get_git_activity(file_path)

        except (IOError, UnicodeDecodeError) as e:
            print(f"Warning: Could not analyze usage for {file_path}: {e}")

        return usage

    def _get_git_activity(self, file_path: Path) -> Dict[str, int]:
        """Get git activity statistics for a file."""
        activity = {"commits": 0, "additions": 0, "deletions": 0}

        try:
            # Get commit count
            result = subprocess.run(
                ["git", "log", "--oneline", str(file_path)], cwd=PROJECT_ROOT, capture_output=True, text=True
            )
            if result.returncode == 0:
                activity["commits"] = len(result.stdout.strip().split("\n")) if result.stdout.strip() else 0

            # Get line change statistics
            result = subprocess.run(
                ["git", "log", "--numstat", "--pretty=format:", str(file_path)],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if line and "\t" in line:
                        parts = line.split("\t")
                        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                            activity["additions"] += int(parts[0])
                            activity["deletions"] += int(parts[1])

        except (subprocess.SubprocessError, ValueError):
            pass

        return activity

    def _assess_deletion_safety(self, file_path: str, usage: ScriptUsage) -> SafetyAssessment:
        """Assess whether a script can be safely deleted."""
        assessment = SafetyAssessment(file_path=file_path)

        # Check for blocking dependencies
        assessment.blocking_dependencies = [
            dep.source_file
            for dep in self.dependency_map.dependencies
            if dep.target_file == file_path or file_path.endswith(dep.target_file.replace(".", "/") + ".py")
        ]

        # Analyze usage indicators
        usage_indicators = []

        if usage.is_executable:
            usage_indicators.append("Executable script (has main block)")

        if usage.imported_by:
            usage_indicators.append(f"Imported by {len(usage.imported_by)} other files")

        if usage.git_activity.get("commits", 0) > 10:
            usage_indicators.append(f"High git activity ({usage.git_activity['commits']} commits)")

        if usage.last_modified and (time.time() - usage.last_modified) < 30 * 24 * 3600:  # 30 days
            usage_indicators.append("Recently modified (within 30 days)")

        if usage.function_count > 5 or usage.class_count > 2:
            usage_indicators.append(
                f"Complex implementation ({usage.function_count} functions, {usage.class_count} classes)"
            )

        assessment.usage_indicators = usage_indicators

        # Determine risk level and safety
        if assessment.blocking_dependencies:
            assessment.risk_level = "CRITICAL"
            assessment.is_safe_to_delete = False
            assessment.recommended_action = "CONSOLIDATE"
            assessment.rationale = f"Script is imported by {len(assessment.blocking_dependencies)} other files"

        elif usage.is_executable and usage.git_activity.get("commits", 0) > 5:
            assessment.risk_level = "HIGH"
            assessment.is_safe_to_delete = False
            assessment.recommended_action = "REVIEW"
            assessment.rationale = "Executable script with significant git history"

        elif len(usage_indicators) >= 3:
            assessment.risk_level = "MEDIUM"
            assessment.is_safe_to_delete = False
            assessment.recommended_action = "REVIEW"
            assessment.rationale = "Multiple usage indicators suggest active use"

        elif usage.git_activity.get("commits", 0) == 0 and not usage.is_executable:
            assessment.risk_level = "LOW"
            assessment.is_safe_to_delete = True
            assessment.recommended_action = "DELETE"
            assessment.rationale = "No git history and not executable - likely unused"

        else:
            assessment.risk_level = "LOW"
            assessment.is_safe_to_delete = True
            assessment.recommended_action = "ARCHIVE"
            assessment.rationale = "Low usage indicators - candidate for archiving"

        return assessment

    def analyze_dependencies(self) -> DependencyMap:
        """Analyze script dependencies across the codebase."""
        print("🔍 Analyzing script dependencies and usage patterns...")

        all_python_files = []

        # Collect all Python files
        for include_dir in self.include_dirs:
            dir_path = PROJECT_ROOT / include_dir
            if dir_path.exists():
                python_files = list(dir_path.glob("**/*.py"))
                all_python_files.extend(python_files)

        # Also check root level scripts
        root_scripts = list(PROJECT_ROOT.glob("*.py"))
        all_python_files.extend(root_scripts)

        print(f"📊 Found {len(all_python_files)} Python files to analyze")

        # Extract dependencies and usage patterns
        all_dependencies = []
        usage_patterns = {}

        for py_file in all_python_files:
            # Extract imports
            dependencies = self._extract_imports(py_file)
            all_dependencies.extend(dependencies)

            # Analyze usage
            usage = self._analyze_script_usage(py_file)
            usage_patterns[str(py_file)] = usage

        # Build reverse dependency mapping
        for dep in all_dependencies:
            if not dep.is_external:
                target_path = self._resolve_import_path(dep.target_file)
                if target_path and target_path in usage_patterns:
                    usage_patterns[target_path].imported_by.append(dep.source_file)

        # Build forward dependency mapping
        for file_path, usage in usage_patterns.items():
            file_deps = [dep for dep in all_dependencies if dep.source_file == file_path]
            usage.imports_from = [dep.target_file for dep in file_deps if not dep.is_external]

        # Assess deletion safety for each script
        safety_assessments = {}
        for file_path, usage in usage_patterns.items():
            assessment = self._assess_deletion_safety(file_path, usage)
            safety_assessments[file_path] = assessment

        # Identify unused and high - risk scripts
        unused_scripts = [
            file_path
            for file_path, assessment in safety_assessments.items()
            if assessment.is_safe_to_delete and assessment.recommended_action == "DELETE"
        ]

        high_risk_scripts = [
            file_path
            for file_path, assessment in safety_assessments.items()
            if assessment.risk_level in ["HIGH", "CRITICAL"]
        ]

        # Update dependency map
        self.dependency_map.dependencies = all_dependencies
        self.dependency_map.usage_patterns = usage_patterns
        self.dependency_map.safety_assessments = safety_assessments
        self.dependency_map.unused_scripts = unused_scripts
        self.dependency_map.high_risk_scripts = high_risk_scripts

        print(f"✅ Analyzed {len(all_dependencies)} dependencies")
        print(f"📈 Found {len(unused_scripts)} potentially unused scripts")
        print(f"⚠️ Identified {len(high_risk_scripts)} high - risk scripts")

        return self.dependency_map

    def _resolve_import_path(self, import_name: str) -> Optional[str]:
        """Resolve an import name to an actual file path."""
        # This is a simplified implementation
        # In practice, you'd need more sophisticated path resolution

        # Try direct mapping
        for include_dir in self.include_dirs:
            dir_path = PROJECT_ROOT / include_dir
            if dir_path.exists():
                # Try as direct file
                potential_file = dir_path / f"{import_name}.py"
                if potential_file.exists():
                    return str(potential_file)

                # Try as module directory
                potential_module = dir_path / import_name / "__init__.py"
                if potential_module.exists():
                    return str(potential_module)

        return None

    def validate_safe_deletion(self, script_path: str) -> SafetyAssessment:
        """Validate that a specific script can be safely deleted."""
        if not self.dependency_map.safety_assessments:
            self.analyze_dependencies()

        abs_path = str(Path(script_path).resolve())

        if abs_path in self.dependency_map.safety_assessments:
            return self.dependency_map.safety_assessments[abs_path]
        else:
            # Analyze this specific file
            file_path = Path(script_path)
            if file_path.exists():
                usage = self._analyze_script_usage(file_path)
                return self._assess_deletion_safety(str(file_path), usage)
            else:
                return SafetyAssessment(
                    file_path=script_path,
                    is_safe_to_delete=False,
                    risk_level="CRITICAL",
                    recommended_action="ERROR",
                    rationale="File does not exist",
                )

    def print_dependency_report(self) -> None:
        """Print detailed dependency analysis report."""
        print("\n" + "=" * 80)
        print("SCRIPT DEPENDENCY ANALYSIS REPORT")
        print("=" * 80)
        print(f"Total Python Files: {len(self.dependency_map.usage_patterns)}")
        print(f"Total Dependencies: {len(self.dependency_map.dependencies)}")
        print(f"Unused Scripts: {len(self.dependency_map.unused_scripts)}")
        print(f"High - Risk Scripts: {len(self.dependency_map.high_risk_scripts)}")
        print()

        # Safe deletion candidates
        safe_deletions = [
            path for path, assessment in self.dependency_map.safety_assessments.items() if assessment.is_safe_to_delete
        ]

        if safe_deletions:
            print("SAFE DELETION CANDIDATES:")
            print("-" * 40)
            for file_path in safe_deletions[:10]:  # Show first 10
                assessment = self.dependency_map.safety_assessments[file_path]
                print(f"🗑️ {Path(file_path).name}")
                print(f"   Path: {file_path}")
                print(f"   Action: {assessment.recommended_action}")
                print(f"   Rationale: {assessment.rationale}")
                print()
            if len(safe_deletions) > 10:
                print(f"   ... and {len(safe_deletions) - 10} more")
            print()

        # High - risk scripts
        if self.dependency_map.high_risk_scripts:
            print("HIGH - RISK SCRIPTS (DO NOT DELETE):")
            print("-" * 40)
            for file_path in self.dependency_map.high_risk_scripts[:10]:
                assessment = self.dependency_map.safety_assessments[file_path]
                usage = self.dependency_map.usage_patterns[file_path]
                print(f"⚠️ {Path(file_path).name}")
                print(f"   Risk Level: {assessment.risk_level}")
                print(f"   Imported by: {len(usage.imported_by)} files")
                print(f"   Git commits: {usage.git_activity.get('commits', 0)}")
                print(f"   Rationale: {assessment.rationale}")
                print()
            if len(self.dependency_map.high_risk_scripts) > 10:
                print(f"   ... and {len(self.dependency_map.high_risk_scripts) - 10} more")
            print()

        # Consolidation opportunities
        consolidation_candidates = [
            path
            for path, assessment in self.dependency_map.safety_assessments.items()
            if assessment.recommended_action == "CONSOLIDATE"
        ]

        if consolidation_candidates:
            print("CONSOLIDATION CANDIDATES:")
            print("-" * 40)
            for file_path in consolidation_candidates:
                assessment = self.dependency_map.safety_assessments[file_path]
                print(f"🔄 {Path(file_path).name}")
                print(f"   Blocking dependencies: {len(assessment.blocking_dependencies)}")
                print(f"   Suggested action: Extract to helper modules")
                print()

        print("=" * 80)


def main():
    """Main entry point for advanced script dependency analyzer."""
    import argparse
    import time

    parser = argparse.ArgumentParser(description="Advanced Script Dependency Analyzer")
    parser.add_argument("--analyze", action="store_true", help="Run full analysis (dependencies + duplicates)")
    parser.add_argument("--dependencies", action="store_true", help="Analyze script dependencies only")
    parser.add_argument("--usage", action="store_true", help="Analyze script usage patterns")
    parser.add_argument("--safety - check", type=str, help="Check if specific script can be safely deleted")
    parser.add_argument("--extract", action="store_true", help="Generate helper function suggestions")

    args = parser.parse_args()

    if not any([args.analyze, args.dependencies, args.usage, args.safety_check, args.extract]):
        args.analyze = True  # Default to full analysis

    # Create analyzer
    analyzer = AdvancedScriptDependencyAnalyzer(extract_mode=args.extract)

    # Safety check for specific script
    if args.safety_check:
        print(f"🔍 Checking deletion safety for: {args.safety_check}")
        assessment = analyzer.validate_safe_deletion(args.safety_check)

        print(f"\nSAFETY ASSESSMENT: {Path(args.safety_check).name}")
        print("-" * 50)
        print(f"Safe to delete: {'✅ YES' if assessment.is_safe_to_delete else '❌ NO'}")
        print(f"Risk level: {assessment.risk_level}")
        print(f"Recommended action: {assessment.recommended_action}")
        print(f"Rationale: {assessment.rationale}")

        if assessment.blocking_dependencies:
            print(f"\nBlocking dependencies ({len(assessment.blocking_dependencies)}):")
            for dep in assessment.blocking_dependencies[:5]:
                print(f"  - {dep}")
            if len(assessment.blocking_dependencies) > 5:
                print(f"  ... and {len(assessment.blocking_dependencies) - 5} more")

        if assessment.usage_indicators:
            print(f"\nUsage indicators:")
            for indicator in assessment.usage_indicators:
                print(f"  - {indicator}")

        sys.exit(0 if assessment.is_safe_to_delete else 1)

    # Dependency analysis
    if args.dependencies or args.usage or args.analyze:
        _dependency_map = analyzer.analyze_dependencies()
        analyzer.print_dependency_report()

    # Duplication analysis
    if args.analyze or args.extract:
        print("\n" + "=" * 80)
        print("RUNNING DUPLICATE CODE ANALYSIS...")
        print("=" * 80)

        _duplication_report = analyzer.analyze_codebase()

        if args.extract:
            helpers = analyzer.generate_helper_functions()
            if helpers:
                print(f"\n🔧 Generated {len(helpers)} helper function suggestions")

        analyzer.print_report()

    print(f"\n✅ Advanced script dependency analysis completed!")
    print("🎉 Task 4: Build Advanced Script Dependency Analyzer - COMPLETED")


if __name__ == "__main__":
    main()
