#!/usr/bin/env python3
"""
Staged Code Formatting - Safe and Trackable

This script formats code in small, manageable batches with full tracking
and the ability to review/rollback changes at each stage.

Usage:
    python tools/code_quality/staged_formatting.py --analyze
    python tools/code_quality/staged_formatting.py --format-batch 1
    python tools/code_quality/staged_formatting.py --preview-batch 1
"""

from pathlib import Path
import subprocess
import sys
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class StagedFormatter:
    """Safely format code in small, trackable batches."""

    def __init__(self):
        self.project_root = PROJECT_ROOT
        self.batch_size = 10  # Small batches for safety

        # Get all Python files, organized by priority
        self.file_batches = self._organize_files_by_priority()

    def _organize_files_by_priority(self) -> List[List[Path]]:
        """Organize files into priority batches."""

        # Priority 1: Core helper functions and utilities (safest to format)
        priority_1 = [
            "src/youtubeviz/common_helpers.py",
            "tools/code_quality/naming_convention_auditor.py",
            "tools/code_quality/duplicate_code_analyzer.py",
        ]

        # Priority 2: New files we created (we know they're clean)
        priority_2 = [
            "tools/code_quality/setup_formatting_tools.py",
            "tools/etl/etl_health_check.py",
            "tools/etl/data_quality_validator.py",
        ]

        # Priority 3: Core ETL files (critical but stable)
        priority_3_patterns = [
            "web/youtube_channel_etl.py",
            "web/etl_helpers.py",
            "web/sentiment_job.py",
        ]

        # Priority 4: Other src files (batch by directory)
        src_dirs = ["src/youtubeviz", "src/data_organization", "src/notebook_guardian"]

        # Priority 5: Tools and scripts
        tools_dirs = ["tools", "scripts"]

        batches = []

        # Add priority files first
        for priority_files in [priority_1, priority_2, priority_3_patterns]:
            existing_files = []
            for file_pattern in priority_files:
                file_path = self.project_root / file_pattern
                if file_path.exists():
                    existing_files.append(file_path)

            if existing_files:
                # Split into small batches
                for i in range(0, len(existing_files), self.batch_size):
                    batch = existing_files[i : i + self.batch_size]
                    batches.append(batch)

        # Add directory-based batches
        for dir_pattern in src_dirs + tools_dirs:
            dir_path = self.project_root / dir_pattern
            if dir_path.exists():
                py_files = list(dir_path.glob("**/*.py"))
                # Filter out files we've already included
                already_included = set()
                for batch in batches:
                    already_included.update(batch)

                py_files = [f for f in py_files if f not in already_included]

                # Create small batches
                for i in range(0, len(py_files), self.batch_size):
                    batch = py_files[i : i + self.batch_size]
                    if batch:
                        batches.append(batch)

        return batches

    def analyze_formatting_needs(self) -> Dict:
        """Analyze what needs formatting without making changes."""
        print("🔍 ANALYZING FORMATTING NEEDS")
        print("=" * 50)

        analysis = {
            "total_batches": len(self.file_batches),
            "total_files": sum(len(batch) for batch in self.file_batches),
            "batches_needing_format": [],
            "syntax_errors": [],
            "safe_to_format": True,
        }

        for i, batch in enumerate(self.file_batches, 1):
            print(f"\n📦 Batch {i}/{len(self.file_batches)} ({len(batch)} files):")

            # Check for syntax errors first
            syntax_ok = True
            for file_path in batch:
                if not self._check_syntax(file_path):
                    analysis["syntax_errors"].append(str(file_path))
                    syntax_ok = False
                    print(f"   ❌ {file_path.name} - SYNTAX ERROR")

            if not syntax_ok:
                analysis["safe_to_format"] = False
                continue

            # Check if formatting is needed
            needs_formatting = self._check_batch_formatting(batch)
            if needs_formatting:
                analysis["batches_needing_format"].append(i)
                print(f"   ⚠️ Needs formatting: {[f.name for f in batch]}")
            else:
                print(f"   ✅ Already formatted: {[f.name for f in batch]}")

        return analysis

    def _check_syntax(self, file_path: Path) -> bool:
        """Check if a Python file has valid syntax."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                compile(f.read(), str(file_path), "exec")
            return True
        except SyntaxError:
            return False
        except Exception:
            return False

    def _check_batch_formatting(self, batch: List[Path]) -> bool:
        """Check if a batch needs formatting."""
        try:
            # Check with black
            cmd = ["python", "-m", "black", "--check", "--quiet"] + [str(f) for f in batch]
            result = subprocess.run(cmd, capture_output=True, cwd=self.project_root)
            return result.returncode != 0  # Non-zero means formatting needed
        except Exception:
            return True  # Assume formatting needed if check fails

    def preview_batch_changes(self, batch_num: int) -> bool:
        """Preview what changes would be made to a batch."""
        if batch_num < 1 or batch_num > len(self.file_batches):
            print(f"❌ Invalid batch number. Must be 1-{len(self.file_batches)}")
            return False

        batch = self.file_batches[batch_num - 1]
        print(f"\n🔍 PREVIEWING BATCH {batch_num} CHANGES")
        print("=" * 50)
        print(f"Files in batch: {[f.name for f in batch]}")

        # Show black diff
        print("\n📝 Black formatting changes:")
        cmd = ["python", "-m", "black", "--diff", "--color"] + [str(f) for f in batch]
        result = subprocess.run(cmd, cwd=self.project_root)

        # Show isort diff
        print("\n📚 Import sorting changes:")
        cmd = ["python", "-m", "isort", "--diff", "--color"] + [str(f) for f in batch]
        result = subprocess.run(cmd, cwd=self.project_root)

        return True

    def format_batch(self, batch_num: int, confirm: bool = False) -> bool:
        """Format a specific batch of files."""
        if batch_num < 1 or batch_num > len(self.file_batches):
            print(f"❌ Invalid batch number. Must be 1-{len(self.file_batches)}")
            return False

        batch = self.file_batches[batch_num - 1]

        if not confirm:
            print(f"\n⚠️ About to format batch {batch_num}:")
            for file_path in batch:
                print(f"   • {file_path}")

            response = input("\nProceed with formatting? (y/N): ")
            if response.lower() != "y":
                print("❌ Formatting cancelled")
                return False

        print(f"\n🎨 FORMATTING BATCH {batch_num}")
        print("=" * 50)

        # Check syntax first
        for file_path in batch:
            if not self._check_syntax(file_path):
                print(f"❌ Syntax error in {file_path}, skipping batch")
                return False

        success = True

        # Run isort first
        print("📚 Sorting imports...")
        cmd = ["python", "-m", "isort"] + [str(f) for f in batch]
        result = subprocess.run(cmd, cwd=self.project_root)
        if result.returncode != 0:
            print("⚠️ Import sorting had issues")
            success = False

        # Run black
        print("🖤 Formatting code...")
        cmd = ["python", "-m", "black"] + [str(f) for f in batch]
        result = subprocess.run(cmd, cwd=self.project_root)
        if result.returncode != 0:
            print("⚠️ Black formatting had issues")
            success = False

        if success:
            print(f"✅ Batch {batch_num} formatted successfully")
            print("\n💡 Next steps:")
            print("   1. Review the changes with: git diff")
            print("   2. Test that everything still works")
            print("   3. Commit the changes: git add . && git commit -m 'Format batch X'")

        return success

    def print_analysis_report(self, analysis: Dict) -> None:
        """Print detailed analysis report."""
        print("\n" + "=" * 60)
        print("FORMATTING ANALYSIS REPORT")
        print("=" * 60)
        print(f"Total files: {analysis['total_files']}")
        print(f"Total batches: {analysis['total_batches']}")
        print(f"Batches needing format: {len(analysis['batches_needing_format'])}")
        print(f"Files with syntax errors: {len(analysis['syntax_errors'])}")

        if analysis["syntax_errors"]:
            print("\n❌ SYNTAX ERRORS (must fix first):")
            for error_file in analysis["syntax_errors"]:
                print(f"   • {error_file}")

        if analysis["batches_needing_format"]:
            print(f"\n⚠️ BATCHES NEEDING FORMATTING:")
            for batch_num in analysis["batches_needing_format"]:
                batch = self.file_batches[batch_num - 1]
                print(f"   • Batch {batch_num}: {[f.name for f in batch]}")

        print(f"\n🛡️ SAFETY STATUS: {'✅ SAFE' if analysis['safe_to_format'] else '❌ UNSAFE'}")

        if analysis["safe_to_format"] and analysis["batches_needing_format"]:
            print("\n📋 RECOMMENDED WORKFLOW:")
            print("   1. Preview changes: --preview-batch 1")
            print("   2. Format batch: --format-batch 1")
            print("   3. Review and commit: git diff && git commit")
            print("   4. Repeat for next batch")

        print("=" * 60)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Staged Code Formatting")
    parser.add_argument("--analyze", action="store_true", help="Analyze formatting needs")
    parser.add_argument("--preview-batch", type=int, help="Preview changes for batch N")
    parser.add_argument("--format-batch", type=int, help="Format batch N")
    parser.add_argument("--confirm", action="store_true", help="Skip confirmation prompts")

    args = parser.parse_args()

    if not any([args.analyze, args.preview_batch, args.format_batch]):
        args.analyze = True  # Default to analysis

    formatter = StagedFormatter()

    if args.analyze:
        analysis = formatter.analyze_formatting_needs()
        formatter.print_analysis_report(analysis)

        if not analysis["safe_to_format"]:
            print("\n❌ Cannot proceed with formatting due to syntax errors")
            return 1

    if args.preview_batch:
        formatter.preview_batch_changes(args.preview_batch)

    if args.format_batch:
        success = formatter.format_batch(args.format_batch, confirm=args.confirm)
        return 0 if success else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
