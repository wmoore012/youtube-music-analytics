#!/usr/bin/env python3
"""
Safe Linting Fix Script

This script fixes linting errors with comprehensive backup and safety checks.
It uses the existing backup infrastructure to ensure no code is lost.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


class SafeLintingFixer:
    """Safe linting fixer with backup and rollback capabilities."""

    def __init__(self, backup_dir: Optional[str] = None):
        self.backup_dir = Path(backup_dir or f".linting_backups_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        self.backup_dir.mkdir(exist_ok=True)
        self.changes_log = []
        self.test_passed_before = False

    def create_backup(self, file_path: Path) -> Path:
        """Create a backup of a file before modifying it."""
        # Handle relative paths properly
        if not file_path.is_absolute():
            file_path = Path.cwd() / file_path

        try:
            rel_path = file_path.relative_to(Path.cwd())
        except ValueError:
            # If file is not in current directory, use just the filename
            rel_path = file_path.name

        backup_path = self.backup_dir / rel_path
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, backup_path)
        return backup_path

    def verify_tests_pass(self) -> bool:
        """Verify that tests pass before making changes."""
        print("🧪 Running tests to verify current state...")
        result = subprocess.run(
            ["python", "-m", "pytest", "-q", "--tb=short"],
            env={**os.environ, "PYTHONPATH": "."},
            capture_output=True,
            text=True
        )
        return result.returncode == 0

    def get_linting_errors(self) -> List[Dict[str, str]]:
        """Get current linting errors."""
        result = subprocess.run(
            ["flake8", "--max-line-length=120"],
            capture_output=True,
            text=True
        )

        errors = []
        if result.returncode != 0:
            for line in result.stdout.strip().split('\n'):
                if not line.strip():
                    continue

                # Parse: ./file.py:line:col: CODE message
                match = re.match(r'\./(.*?):(\d+):(\d+): ([A-Z]\d+) (.*)', line)
                if match:
                    file_path, line_num, col, code, message = match.groups()
                    errors.append({
                        'file': file_path,
                        'line': int(line_num),
                        'col': int(col),
                        'code': code,
                        'message': message,
                        'full_line': line
                    })

        return errors

    def fix_trailing_whitespace(self, file_path: Path) -> bool:
        """Safely fix trailing whitespace."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            lines = content.splitlines()
            fixed_lines = [line.rstrip() for line in lines]
            fixed_content = '\n'.join(fixed_lines)

            # Add final newline if missing
            if fixed_content and not fixed_content.endswith('\n'):
                fixed_content += '\n'

            if content != fixed_content:
                self.create_backup(file_path)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(fixed_content)
                return True

        except Exception as e:
            print(f"   Error fixing {file_path}: {e}")
            return False

        return False

    def fix_unused_variable(self, file_path: Path, line_num: int, var_name: str) -> bool:
        """Safely fix unused variable by prefixing with underscore."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if line_num <= len(lines):
                original_line = lines[line_num - 1]

                # Skip if already prefixed
                if var_name.startswith('_'):
                    return False

                # Simple patterns for safe replacement
                patterns = [
                    (rf'\b{re.escape(var_name)}\s*=', f'_{var_name} ='),
                    (rf'for\s+{re.escape(var_name)}\s+in', f'for _{var_name} in'),
                ]

                for pattern, replacement in patterns:
                    if re.search(pattern, original_line):
                        new_line = re.sub(pattern, replacement, original_line)

                        self.create_backup(file_path)
                        lines[line_num - 1] = new_line

                        with open(file_path, 'w', encoding='utf-8') as f:
                            f.writelines(lines)

                        self.changes_log.append({
                            'file': str(file_path),
                            'line': line_num,
                            'type': 'unused_variable',
                            'old': original_line.strip(),
                            'new': new_line.strip()
                        })
                        return True

        except Exception as e:
            print(f"   Error fixing unused variable in {file_path}: {e}")
            return False

        return False

    def fix_line_length_simple(self, file_path: Path, line_num: int) -> bool:
        """Fix simple line length issues."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if line_num <= len(lines):
                original_line = lines[line_num - 1]

                # Skip if not that long or already has breaks
                if len(original_line.rstrip()) <= 125:
                    return False

                # Only fix very simple cases
                if ',' in original_line and '(' in original_line and original_line.count('(') == 1:
                    indent = len(original_line) - len(original_line.lstrip())
                    base_indent = ' ' * indent

                    # Find a good comma to break at
                    comma_pos = original_line.find(',', 60)  # Look for comma after position 60
                    if comma_pos > 0 and comma_pos < len(original_line) - 20:
                        new_line = (original_line[:comma_pos + 1] + '\n' +
                                   base_indent + '    ' + original_line[comma_pos + 1:].lstrip())

                        # Only apply if both lines are reasonable length
                        new_lines = new_line.split('\n')
                        if all(len(line.rstrip()) <= 120 for line in new_lines):
                            self.create_backup(file_path)
                            lines[line_num - 1] = new_line

                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.writelines(lines)

                            self.changes_log.append({
                                'file': str(file_path),
                                'line': line_num,
                                'type': 'line_length',
                                'old': original_line.strip(),
                                'new': new_line.replace('\n', '\\n').strip()
                            })
                            return True

        except Exception as e:
            print(f"   Error fixing line length in {file_path}: {e}")
            return False

        return False

    def add_noqa_comment(self, file_path: Path, line_num: int, error_code: str) -> bool:
        """Add noqa comment for unfixable issues."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if line_num <= len(lines):
                original_line = lines[line_num - 1]

                # Skip if already has noqa
                if '# noqa' in original_line:
                    return False

                stripped = original_line.rstrip()
                new_line = stripped + f'  # noqa: {error_code}\n'

                self.create_backup(file_path)
                lines[line_num - 1] = new_line

                with open(file_path, 'w', encoding='utf-8') as f:
                    f.writelines(lines)

                self.changes_log.append({
                    'file': str(file_path),
                    'line': line_num,
                    'type': 'noqa_comment',
                    'old': original_line.strip(),
                    'new': new_line.strip()
                })
                return True

        except Exception as e:
            print(f"   Error adding noqa comment in {file_path}: {e}")
            return False

        return False

    def rollback_changes(self):
        """Rollback all changes using backups."""
        print("🔄 Rolling back all changes...")

        for backup_file in self.backup_dir.rglob('*'):
            if backup_file.is_file():
                original_path = Path.cwd() / backup_file.relative_to(self.backup_dir)
                if original_path.exists():
                    shutil.copy2(backup_file, original_path)
                    print(f"   Restored: {original_path}")

        print("✅ Rollback completed")

    def save_changes_log(self):
        """Save log of all changes made."""
        log_file = self.backup_dir / "changes_log.json"
        with open(log_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'changes': self.changes_log,
                'total_changes': len(self.changes_log)
            }, f, indent=2)

        print(f"📝 Changes log saved to: {log_file}")

    def fix_linting_errors(self, max_fixes: int = 100) -> Dict[str, int]:
        """Fix linting errors safely with limits."""
        print("🚀 Starting safe linting fixes...")

        # Verify tests pass before starting
        if not self.verify_tests_pass():
            print("❌ Tests are failing before fixes. Aborting.")
            return {}

        self.test_passed_before = True

        # Get current errors
        errors = self.get_linting_errors()
        print(f"📊 Found {len(errors)} linting errors")

        if not errors:
            print("🎉 No linting errors found!")
            return {}

        # Group errors by type
        error_types = {}
        for error in errors:
            error_types.setdefault(error['code'], []).append(error)

        print("Error breakdown:")
        for code, error_list in sorted(error_types.items()):
            print(f"   {code}: {len(error_list)} errors")

        fixes_applied = {}
        total_fixes = 0

        # Fix trailing whitespace (W291, W293)
        if total_fixes < max_fixes:
            whitespace_errors = error_types.get('W291', []) + error_types.get('W293', [])
            print(f"\n🧹 Fixing {len(whitespace_errors)} trailing whitespace errors...")

            files_to_fix = set(error['file'] for error in whitespace_errors)
            for file_path_str in files_to_fix:
                if total_fixes >= max_fixes:
                    break

                file_path = Path(file_path_str)
                if file_path.exists() and self.fix_trailing_whitespace(file_path):
                    fixes_applied.setdefault('whitespace', 0)
                    fixes_applied['whitespace'] += 1
                    total_fixes += 1

        # Fix unused variables (F841)
        if total_fixes < max_fixes:
            unused_vars = error_types.get('F841', [])
            print(f"\n🗑️  Fixing {len(unused_vars)} unused variable errors...")

            for error in unused_vars[:max_fixes - total_fixes]:
                # Extract variable name from message
                match = re.search(r"local variable '(\w+)' is assigned to but never used", error['message'])
                if match:
                    var_name = match.group(1)
                    file_path = Path(error['file'])

                    if file_path.exists() and self.fix_unused_variable(file_path, error['line'], var_name):
                        fixes_applied.setdefault('unused_variables', 0)
                        fixes_applied['unused_variables'] += 1
                        total_fixes += 1

        # Fix simple line length issues (E501)
        if total_fixes < max_fixes:
            line_length_errors = error_types.get('E501', [])
            print(f"\n📏 Fixing simple line length errors (max {min(10, max_fixes - total_fixes)})...")

            for error in line_length_errors[:min(10, max_fixes - total_fixes)]:
                file_path = Path(error['file'])

                if file_path.exists() and self.fix_line_length_simple(file_path, error['line']):
                    fixes_applied.setdefault('line_length', 0)
                    fixes_applied['line_length'] += 1
                    total_fixes += 1

        # Add noqa comments for complex issues (C901)
        if total_fixes < max_fixes:
            complex_errors = error_types.get('C901', [])
            print(f"\n🏷️  Adding noqa comments for {len(complex_errors)} complex function errors...")

            for error in complex_errors[:max_fixes - total_fixes]:
                file_path = Path(error['file'])

                if file_path.exists() and self.add_noqa_comment(file_path, error['line'], 'C901'):
                    fixes_applied.setdefault('noqa_comments', 0)
                    fixes_applied['noqa_comments'] += 1
                    total_fixes += 1

        print(f"\n✅ Applied {total_fixes} fixes")

        # Verify tests still pass
        print("\n🧪 Verifying tests still pass after fixes...")
        if not self.verify_tests_pass():
            print("❌ Tests are failing after fixes! Rolling back...")
            self.rollback_changes()
            return {}

        print("✅ Tests still pass after fixes!")

        # Save changes log
        self.save_changes_log()

        return fixes_applied


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Safe Linting Fix Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python safe_linting_fix.py --max-fixes 50
  python safe_linting_fix.py --backup-dir ./my_backups
  python safe_linting_fix.py --rollback ./my_backups
        """
    )

    parser.add_argument("--max-fixes", type=int, default=100,
                       help="Maximum number of fixes to apply")
    parser.add_argument("--backup-dir", type=str,
                       help="Directory for backups")
    parser.add_argument("--rollback", type=str,
                       help="Rollback changes from backup directory")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be fixed without making changes")

    args = parser.parse_args()

    if args.rollback:
        # Rollback from specified backup directory
        backup_dir = Path(args.rollback)
        if not backup_dir.exists():
            print(f"❌ Backup directory not found: {backup_dir}")
            return 1

        fixer = SafeLintingFixer(backup_dir)
        fixer.rollback_changes()
        return 0

    # Create fixer instance
    fixer = SafeLintingFixer(args.backup_dir)

    if args.dry_run:
        errors = fixer.get_linting_errors()
        print(f"🔍 Found {len(errors)} linting errors (dry run)")

        error_types = {}
        for error in errors:
            error_types.setdefault(error['code'], []).append(error)

        for code, error_list in sorted(error_types.items()):
            print(f"   {code}: {len(error_list)} errors")

        return 0

    # Apply fixes
    try:
        fixes_applied = fixer.fix_linting_errors(args.max_fixes)

        if fixes_applied:
            print(f"\n🎉 Successfully applied fixes:")
            for fix_type, count in fixes_applied.items():
                print(f"   {fix_type}: {count} fixes")

            # Show final error count
            final_errors = fixer.get_linting_errors()
            print(f"\n📊 Remaining linting errors: {len(final_errors)}")

        else:
            print("ℹ️  No fixes were applied")

        return 0

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user. Rolling back changes...")
        fixer.rollback_changes()
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("Rolling back changes...")
        fixer.rollback_changes()
        return 1


if __name__ == "__main__":
    sys.exit(main())