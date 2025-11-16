#!/usr / bin / env python3
"""
🔍 Backup Verifier

Systematically verifies that cleanup backup files are no longer needed
and can be safely removed from the repository.

This tool implements the BackupVerifier class to:
- Verify files were successfully moved to new locations
- Check that functionality is preserved in current codebase
- Identify unique content that exists only in backups
- Generate removal recommendations with safety checks

Usage:
    python tools / development / code_quality / backup_verifier.py --verify-all
    python tools / development / code_quality / backup_verifier.py --phase 1
    python tools / development / code_quality / backup_verifier.py --remove-verified
"""

import argparse
import hashlib
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import ToolBase, ToolConfig, register_tool


class BackupVerificationResult:
    """Results of backup verification process."""

    def __init__(self):
        self.verified_files: List[str] = []
        self.missing_files: List[str] = []
        self.unique_content: List[str] = []
        self.functionality_preserved: Dict[str, bool] = {}
        self.safe_to_remove: List[str] = []
        self.requires_review: List[str] = []
        self.total_size_bytes: int = 0


class BackupVerifier(ToolBase):
    """
    Systematic backup verification tool.

    Verifies that cleanup backup files are no longer needed by:
    1. Checking if files were successfully moved to new locations
    2. Verifying functionality is preserved in current codebase
    3. Identifying unique content that exists only in backups
    4. Generating safe removal recommendations
    """

    def __init__(self):
        super().__init__(name="backup-verifier", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        self.backup_root = project_root / ".cleanup_backups"
        self.current_codebase = project_root

        # File extensions to analyze
        self.code_extensions = {".py", ".js", ".ts", ".sql", ".md", ".json", ".yaml", ".yml"}

        # Directories to search for moved files
        self.search_directories = [
            project_root / "src",
            project_root / "tools",
            project_root / "web",
            project_root / "scripts",
            project_root / "tests",
            project_root / "docs",
        ]

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        return []  # No environment variables required

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="backup-verifier",
            version="1.0.0",
            description="Systematic backup verification and cleanup tool",
            dependencies=["python>=3.8"],
            environment_vars=[],
            usage_examples=[
                "python tools / development / code_quality / backup_verifier.py --verify-all",
                "python tools / development / code_quality / backup_verifier.py --phase 1",
                "python tools / development / code_quality / backup_verifier.py --remove-verified",
            ],
            category="development",
        )

    def run(self) -> None:
        """Main execution method-should not be called directly."""
        self.log_progress("Use specific verification methods like verify_all_backups()")

    def verify_all_backups(self) -> BackupVerificationResult:
        """
        Verify all backup phases and generate comprehensive report.

        Returns:
            BackupVerificationResult with complete verification status
        """
        self.log_progress("🔍 Starting comprehensive backup verification")

        if not self.backup_root.exists():
            self.log_progress("✅ No backup directory found-cleanup already complete")
            return BackupVerificationResult()

        result = BackupVerificationResult()

        # Get all backup phases
        backup_phases = [d for d in self.backup_root.iterdir() if d.is_dir() and d.name.startswith("phase_")]
        backup_phases.sort()

        self.log_progress(f"📂 Found {len(backup_phases)} backup phases to verify")

        for phase_dir in backup_phases:
            self.log_progress(f"🔍 Verifying {phase_dir.name}")
            phase_result = self.verify_backup_phase(phase_dir)

            # Merge results
            result.verified_files.extend(phase_result.verified_files)
            result.missing_files.extend(phase_result.missing_files)
            result.unique_content.extend(phase_result.unique_content)
            result.functionality_preserved.update(phase_result.functionality_preserved)
            result.safe_to_remove.extend(phase_result.safe_to_remove)
            result.requires_review.extend(phase_result.requires_review)
            result.total_size_bytes += phase_result.total_size_bytes

        # Generate final recommendations
        self._generate_removal_recommendations(result)

        return result

    def verify_backup_phase(self, phase_dir: Path) -> BackupVerificationResult:
        """
        Verify a specific backup phase directory.

        Args:
            phase_dir: Path to backup phase directory

        Returns:
            BackupVerificationResult for this phase
        """
        result = BackupVerificationResult()

        if not phase_dir.exists():
            return result

        # Get all files in this phase
        backup_files = []
        for root, dirs, files in os.walk(phase_dir):
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix in self.code_extensions:
                    backup_files.append(file_path)

        self.log_progress(f"📄 Found {len(backup_files)} files in {phase_dir.name}")

        for backup_file in backup_files:
            # Calculate file size
            try:
                result.total_size_bytes += backup_file.stat().st_size
            except:  # noqa: E722
                pass

            # Check if file was successfully moved
            moved_successfully = self.verify_file_moved_successfully(backup_file)

            if moved_successfully:
                result.verified_files.append(str(backup_file))
                result.safe_to_remove.append(str(backup_file))
            else:
                # Check if functionality is preserved elsewhere
                functionality_preserved = self.check_functionality_preserved(backup_file)
                result.functionality_preserved[str(backup_file)] = functionality_preserved

                if functionality_preserved:
                    result.verified_files.append(str(backup_file))
                    result.safe_to_remove.append(str(backup_file))
                else:
                    # Check if this contains unique content
                    has_unique_content = self.identify_unique_content(backup_file)

                    if has_unique_content:
                        result.unique_content.append(str(backup_file))
                        result.requires_review.append(str(backup_file))
                    else:
                        result.missing_files.append(str(backup_file))
                        result.safe_to_remove.append(str(backup_file))

        return result

    def verify_file_moved_successfully(self, backup_file: Path) -> bool:
        """
        Verify file was successfully moved to new location.

        Args:
            backup_file: Path to backup file

        Returns:
            True if file exists in current codebase with same or similar content
        """
        try:
            # Get relative path from backup root
            relative_path = backup_file.relative_to(self.backup_root)

            # Remove phase directory from path
            path_parts = relative_path.parts[1:]  # Skip phase_X directory
            if not path_parts:
                return False

            # Try to find file in current codebase
            possible_locations = []

            # Direct path match
            direct_path = self.current_codebase / Path(*path_parts)
            if direct_path.exists():
                possible_locations.append(direct_path)

            # Search in common directories
            filename = path_parts[-1]
            for search_dir in self.search_directories:
                if search_dir.exists():
                    for found_file in search_dir.rglob(filename):
                        if found_file.is_file():
                            possible_locations.append(found_file)

            # Check if any location has similar content
            backup_content = self._get_file_content_hash(backup_file)
            if not backup_content:
                return False

            for location in possible_locations:
                current_content = self._get_file_content_hash(location)
                if current_content and self._content_similarity(backup_content, current_content) > 0.8:
                    return True

            return False

        except Exception as e:
            self.log_progress(f"⚠️  Error verifying {backup_file}: {e}")
            return False

    def check_functionality_preserved(self, backup_file: Path) -> bool:
        """
        Verify functionality is preserved in current codebase.

        Args:
            backup_file: Path to backup file

        Returns:
            True if functionality appears to be preserved elsewhere
        """
        try:
            if not backup_file.exists():
                return False

            # Read backup file content
            with open(backup_file, "r", encoding="utf-8", errors="ignore") as f:
                backup_content = f.read()

            # Extract key functions and classes
            key_elements = self._extract_key_elements(backup_content)

            if not key_elements:
                return True  # No significant functionality to preserve

            # Search for these elements in current codebase
            preserved_count = 0

            for element in key_elements:
                if self._search_for_element_in_codebase(element):
                    preserved_count += 1

            # Consider functionality preserved if 80% of elements are found
            preservation_ratio = preserved_count / len(key_elements)
            return preservation_ratio >= 0.8

        except Exception as e:
            self.log_progress(f"⚠️  Error checking functionality for {backup_file}: {e}")
            return False

    def identify_unique_content(self, backup_file: Path) -> bool:
        """
        Find content that exists only in backups.

        Args:
            backup_file: Path to backup file

        Returns:
            True if file contains unique content not found elsewhere
        """
        try:
            if not backup_file.exists():
                return False

            # Read backup file
            with open(backup_file, "r", encoding="utf-8", errors="ignore") as f:
                backup_content = f.read()

            # Skip very small files
            if len(backup_content.strip()) < 100:
                return False

            # Extract significant code blocks
            significant_blocks = self._extract_significant_blocks(backup_content)

            if not significant_blocks:
                return False

            # Search for these blocks in current codebase
            unique_blocks = []

            for block in significant_blocks:
                if not self._search_for_block_in_codebase(block):
                    unique_blocks.append(block)

            # Consider unique if more than 50% of significant blocks are not found
            unique_ratio = len(unique_blocks) / len(significant_blocks)
            return unique_ratio > 0.5

        except Exception as e:
            self.log_progress(f"⚠️  Error identifying unique content in {backup_file}: {e}")
            return False

    def remove_verified_backups(  # noqa: C901
        self, verification_result: BackupVerificationResult, dry_run: bool = True
    ) -> Dict[str, any]:
        """
        Remove backup files that have been verified as safe to remove.

        Args:
            verification_result: Results from backup verification
            dry_run: If True, only simulate removal

        Returns:
            Dictionary with removal results
        """
        self.log_progress(f"🗑️  {'Simulating' if dry_run else 'Performing'} backup removal")

        removal_results = {
            "removed_files": [],
            "removed_directories": [],
            "total_size_freed": 0,
            "errors": [],
        }

        if not verification_result.safe_to_remove:
            self.log_progress("✅ No files marked as safe to remove")
            return removal_results

        # Group files by directory for efficient removal
        directories_to_check = set()

        for file_path in verification_result.safe_to_remove:
            file_path_obj = Path(file_path)

            if file_path_obj.exists():
                try:
                    file_size = file_path_obj.stat().st_size

                    if not dry_run:
                        file_path_obj.unlink()
                        self.log_progress(f"🗑️  Removed: {file_path}")
                    else:
                        self.log_progress(f"🔍 Would remove: {file_path}")

                    removal_results["removed_files"].append(file_path)
                    removal_results["total_size_freed"] += file_size
                    directories_to_check.add(file_path_obj.parent)

                except Exception as e:
                    error_msg = f"Error removing {file_path}: {e}"
                    removal_results["errors"].append(error_msg)
                    self.log_progress(f"❌ {error_msg}")

        # Remove empty directories
        for directory in directories_to_check:
            if directory.exists() and not any(directory.iterdir()):
                try:
                    if not dry_run:
                        directory.rmdir()
                        self.log_progress(f"🗑️  Removed empty directory: {directory}")
                    else:
                        self.log_progress(f"🔍 Would remove empty directory: {directory}")

                    removal_results["removed_directories"].append(str(directory))

                except Exception as e:
                    error_msg = f"Error removing directory {directory}: {e}"
                    removal_results["errors"].append(error_msg)
                    self.log_progress(f"❌ {error_msg}")

        # Check if entire backup root can be removed
        if not dry_run and self.backup_root.exists():
            try:
                remaining_files = list(self.backup_root.rglob("*"))
                if not remaining_files or all(not f.is_file() for f in remaining_files):
                    shutil.rmtree(self.backup_root)
                    self.log_progress(f"🗑️  Removed entire backup directory: {self.backup_root}")
                    removal_results["removed_directories"].append(str(self.backup_root))
            except Exception as e:
                error_msg = f"Error removing backup root: {e}"
                removal_results["errors"].append(error_msg)
                self.log_progress(f"❌ {error_msg}")

        return removal_results

    def generate_verification_report(self, verification_result: BackupVerificationResult) -> str:
        """
        Generate comprehensive verification report.

        Args:
            verification_result: Results from backup verification

        Returns:
            Formatted report string
        """
        report = []
        report.append("🔍 BACKUP VERIFICATION REPORT")
        report.append("=" * 50)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Summary statistics
        total_files = (
            len(verification_result.verified_files)
            + len(verification_result.missing_files)
            + len(verification_result.unique_content)
        )

        report.append("📊 SUMMARY:")
        report.append(f"  Total files analyzed: {total_files}")
        report.append(f"  Files verified as moved: {len(verification_result.verified_files)}")
        report.append(f"  Files with unique content: {len(verification_result.unique_content)}")
        report.append(f"  Files safe to remove: {len(verification_result.safe_to_remove)}")
        report.append(f"  Files requiring review: {len(verification_result.requires_review)}")
        report.append(f"  Total backup size: {verification_result.total_size_bytes / 1024 / 1024:.1f} MB")
        report.append("")

        # Safe to remove
        if verification_result.safe_to_remove:
            report.append("✅ SAFE TO REMOVE:")
            for file_path in verification_result.safe_to_remove[:10]:  # Show first 10
                report.append(f"  • {file_path}")
            if len(verification_result.safe_to_remove) > 10:
                report.append(f"  ... and {len(verification_result.safe_to_remove) - 10} more files")
            report.append("")

        # Requires review
        if verification_result.requires_review:
            report.append("⚠️  REQUIRES MANUAL REVIEW:")
            for file_path in verification_result.requires_review:
                report.append(f"  • {file_path}")
            report.append("")

        # Recommendations
        report.append("💡 RECOMMENDATIONS:")
        if len(verification_result.safe_to_remove) > 0:
            size_mb = (
                sum(Path(f).stat().st_size for f in verification_result.safe_to_remove if Path(f).exists())
                / 1024
                / 1024
            )
            report.append(
                f"  1. Remove {len(verification_result.safe_to_remove)} verified files to free {size_mb:.1f} MB"
            )

        if len(verification_result.requires_review) > 0:
            report.append(f"  2. Manually review {len(verification_result.requires_review)} files with unique content")

        if len(verification_result.safe_to_remove) == total_files:
            report.append("  3. All backup files can be safely removed!")

        return "\n".join(report)

    def _get_file_content_hash(self, file_path: Path) -> Optional[str]:
        """Get hash of file content for comparison."""
        try:
            with open(file_path, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        except:  # noqa: E722
            return None

    def _content_similarity(self, hash1: str, hash2: str) -> float:
        """Calculate similarity between two content hashes."""
        if hash1 == hash2:
            return 1.0
        return 0.0  # For now, only exact matches

    def _extract_key_elements(self, content: str) -> List[str]:
        """Extract key functions and classes from code content."""
        elements = []
        lines = content.split("\n")

        for line in lines:
            line = line.strip()
            if line.startswith("def ") or line.startswith("class "):
                # Extract function / class name
                if "(" in line:
                    name = line.split("(")[0].replace("def ", "").replace("class ", "").strip()
                    if name and not name.startswith("_"):  # Skip private methods
                        elements.append(name)

        return elements

    def _search_for_element_in_codebase(self, element: str) -> bool:
        """Search for a function or class name in current codebase."""
        for search_dir in self.search_directories:
            if not search_dir.exists():
                continue

            for file_path in search_dir.rglob("*.py"):
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        if f"def {element}" in content or f"class {element}" in content:
                            return True
                except:  # noqa: E722
                    continue

        return False

    def _extract_significant_blocks(self, content: str) -> List[str]:
        """Extract significant code blocks from content."""
        blocks = []
        lines = content.split("\n")
        current_block = []

        for line in lines:
            if line.strip():
                current_block.append(line)
            else:
                if len(current_block) > 5:  # Only significant blocks
                    blocks.append("\n".join(current_block))
                current_block = []

        if len(current_block) > 5:
            blocks.append("\n".join(current_block))

        return blocks

    def _search_for_block_in_codebase(self, block: str) -> bool:
        """Search for a code block in current codebase."""
        # Simplified search-look for key lines from the block
        key_lines = [line.strip() for line in block.split("\n") if line.strip() and not line.strip().startswith("#")]

        if not key_lines:
            return True  # Empty block is considered found

        # Search for at least 50% of key lines
        found_lines = 0

        for search_dir in self.search_directories:
            if not search_dir.exists():
                continue

            for file_path in search_dir.rglob("*.py"):
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        for key_line in key_lines:
                            if key_line in content:
                                found_lines += 1
                                break
                except:  # noqa: E722
                    continue

        return found_lines >= len(key_lines) * 0.5

    def _generate_removal_recommendations(self, result: BackupVerificationResult) -> None:
        """Generate final removal recommendations."""
        # This method can be extended to add more sophisticated logic
        # for determining what's safe to remove
        pass

    def cleanup_resources(self) -> None:
        """Clean up any resources used during verification."""
        # No persistent resources to clean up
        pass


def main():
    """Main entry point for the backup verifier tool."""
    parser = argparse.ArgumentParser(
        description="Backup Verification and Cleanup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / development / code_quality / backup_verifier.py --verify-all
  python tools / development / code_quality / backup_verifier.py --phase 1
  python tools / development / code_quality / backup_verifier.py --remove-verified --dry-run
        """,
    )

    # Verification operations
    parser.add_argument("--verify-all", action="store_true", help="Verify all backup phases")
    parser.add_argument("--phase", type=int, help="Verify specific backup phase (1-6)")
    parser.add_argument("--remove-verified", action="store_true", help="Remove files verified as safe to remove")

    # Options
    parser.add_argument("--dry-run", action="store_true", help="Simulate operations without making changes")
    parser.add_argument("--report", type=str, help="Save verification report to file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Create verifier instance
    with BackupVerifier() as verifier:
        try:
            if args.verify_all:
                result = verifier.verify_all_backups()

                # Generate and display report
                report = verifier.generate_verification_report(result)
                print(report)

                # Save report if requested
                if args.report:
                    with open(args.report, "w") as f:
                        f.write(report)
                    print(f"\n📄 Report saved to: {args.report}")

                return 0

            elif args.phase:
                phase_dir = verifier.backup_root / f"phase_{args.phase}"
                if not phase_dir.exists():
                    print(f"❌ Phase {args.phase} backup directory not found")
                    return 1

                result = verifier.verify_backup_phase(phase_dir)
                report = verifier.generate_verification_report(result)
                print(report)

                return 0

            elif args.remove_verified:
                # First verify to get removal candidates
                result = verifier.verify_all_backups()

                if not result.safe_to_remove:
                    print("✅ No files marked as safe to remove")
                    return 0

                # Perform removal
                removal_result = verifier.remove_verified_backups(result, dry_run=args.dry_run)

                print(
                    f"🗑️  {'Would remove' if args.dry_run else 'Removed'} {len(removal_result['removed_files'])} files"
                )
                print(
                    f"📁 {'Would remove' if args.dry_run else 'Removed'} {len(removal_result['removed_directories'])} directories"
                )
                print(
                    f"💾 {'Would free' if args.dry_run else 'Freed'} {removal_result['total_size_freed'] / 1024 / 1024:.1f} MB"
                )

                if removal_result["errors"]:
                    print(f"❌ {len(removal_result['errors'])} errors occurred:")
                    for error in removal_result["errors"]:
                        print(f"   • {error}")

                return 0

            else:
                print("❌ No operation specified. Use --help for options.")
                return 1

        except KeyboardInterrupt:
            verifier.log_progress("Backup verification cancelled by user")
            return 1
        except Exception as e:
            verifier.handle_error(e, "main execution")
            return 1


if __name__ == "__main__":
    sys.exit(main())
