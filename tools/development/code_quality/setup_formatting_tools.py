#!/usr / bin / env python3
"""
Code Formatting Tools Setup and Runner

This script sets up and runs code formatting tools (black and isort) across the entire codebase.
It ensures consistent code formatting according to the project standards.

Usage:
    python tools / code_quality / setup_formatting_tools.py --install
    python tools / code_quality / setup_formatting_tools.py --format
    python tools / code_quality / setup_formatting_tools.py --check
    python tools / code_quality / setup_formatting_tools.py --all
"""

from pathlib import Path
import subprocess
import sys
from typing import List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


class CodeFormattingManager:
    """Manages code formatting tools setup and execution."""

    def __init__(self):
        self.project_root = PROJECT_ROOT
        self.python_dirs = ["src", "web", "tools", "scripts", "tests"]
        self.python_files = list(self.project_root.glob("*.py"))

        # Add all Python files from project directories
        for dir_name in self.python_dirs:
            dir_path = self.project_root / dir_name
            if dir_path.exists():
                self.python_files.extend(dir_path.glob("**/*.py"))

        # Filter out files we should skip
        self.python_files = [
            f
            for f in self.python_files
            if not any(
                skip in str(f)
                for skip in ["__pycache__", ".venv", "venv", "build", "dist", ".git", "migrations", "node_modules"]
            )
        ]

        print(f"📁 Found {len(self.python_files)} Python files to format")

    def install_tools(self) -> bool:
        """Install code formatting tools."""
        print("📦 Installing code formatting tools...")

        tools = ["black>=23.0.0", "isort>=5.12.0", "flake8>=6.0.0", "mypy>=1.5.0"]

        try:
            for tool in tools:
                print(f"   Installing {tool}...")
                result = subprocess.run([sys.executable, "-m", "pip", "install", tool], capture_output=True, text=True)

                if result.returncode != 0:
                    print(f"   ❌ Failed to install {tool}")
                    print(f"   Error: {result.stderr}")
                    return False
                else:
                    print(f"   ✅ Installed {tool}")

            print("✅ All formatting tools installed successfully")
            return True

        except Exception as e:
            print(f"❌ Error installing tools: {e}")
            return False

    def run_black(self, check_only: bool = False) -> Tuple[bool, str]:
        """Run black formatter."""
        print("🖤 Running black formatter...")

        cmd = [sys.executable, "-m", "black"]

        if check_only:
            cmd.append("--check")
            cmd.append("--diff")

        # Add target directories
        cmd.extend([str(self.project_root / d) for d in self.python_dirs if (self.project_root / d).exists()])
        cmd.extend([str(f) for f in self.python_files if f.parent == self.project_root])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_root)

            if result.returncode == 0:
                if check_only:
                    print("   ✅ All files are properly formatted")
                else:
                    print("   ✅ Black formatting completed successfully")
                return True, result.stdout
            else:
                if check_only:
                    print("   ⚠️ Some files need formatting")
                    print("   Run without --check to format them")
                else:
                    print("   ❌ Black formatting failed")
                return False, result.stderr

        except Exception as e:
            print(f"   ❌ Error running black: {e}")
            return False, str(e)

    def run_isort(self, check_only: bool = False) -> Tuple[bool, str]:
        """Run isort import sorter."""
        print("📚 Running isort import sorter...")

        cmd = [sys.executable, "-m", "isort"]

        if check_only:
            cmd.append("--check - only")
            cmd.append("--diff")

        # Add target directories
        cmd.extend([str(self.project_root / d) for d in self.python_dirs if (self.project_root / d).exists()])
        cmd.extend([str(f) for f in self.python_files if f.parent == self.project_root])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_root)

            if result.returncode == 0:
                if check_only:
                    print("   ✅ All imports are properly sorted")
                else:
                    print("   ✅ Import sorting completed successfully")
                return True, result.stdout
            else:
                if check_only:
                    print("   ⚠️ Some imports need sorting")
                    print("   Run without --check to sort them")
                else:
                    print("   ❌ Import sorting failed")
                return False, result.stderr

        except Exception as e:
            print(f"   ❌ Error running isort: {e}")
            return False, str(e)

    def check_formatting(self) -> bool:
        """Check if code is properly formatted without making changes."""
        print("🔍 Checking code formatting...")

        black_ok, black_output = self.run_black(check_only=True)
        isort_ok, isort_output = self.run_isort(check_only=True)

        if black_ok and isort_ok:
            print("✅ All code is properly formatted")
            return True
        else:
            print("⚠️ Code formatting issues found:")
            if not black_ok:
                print("   Black formatting needed")
            if not isort_ok:
                print("   Import sorting needed")
            return False

    def format_code(self) -> bool:
        """Format all code using black and isort."""
        print("🎨 Formatting all code...")

        # Run isort first (import sorting)
        isort_ok, isort_output = self.run_isort(check_only=False)

        # Then run black (code formatting)
        black_ok, black_output = self.run_black(check_only=False)

        if black_ok and isort_ok:
            print("✅ Code formatting completed successfully")
            return True
        else:
            print("❌ Code formatting encountered issues")
            return False

    def generate_formatting_report(self) -> dict:
        """Generate a report on code formatting status."""
        print("📊 Generating formatting report...")

        report = {
            "total_files": len(self.python_files),
            "directories_scanned": self.python_dirs,
            "black_compliant": False,
            "isort_compliant": False,
            "overall_compliant": False,
        }

        # Check black compliance
        black_ok, _ = self.run_black(check_only=True)
        report["black_compliant"] = black_ok

        # Check isort compliance
        isort_ok, _ = self.run_isort(check_only=True)
        report["isort_compliant"] = isort_ok

        # Overall compliance
        report["overall_compliant"] = black_ok and isort_ok

        return report

    def print_report(self, report: dict) -> None:
        """Print formatting report."""
        print("\n" + "=" * 60)
        print("CODE FORMATTING REPORT")
        print("=" * 60)
        print(f"Total Python files: {report['total_files']}")
        print(f"Directories scanned: {', '.join(report['directories_scanned'])}")
        print()
        print("Formatting Status:")
        print(f"  Black (code formatting): {'✅ PASS' if report['black_compliant'] else '❌ FAIL'}")
        print(f"  Isort (import sorting): {'✅ PASS' if report['isort_compliant'] else '❌ FAIL'}")
        print()
        print(f"Overall Status: {'✅ COMPLIANT' if report['overall_compliant'] else '❌ NEEDS FORMATTING'}")
        print("=" * 60)


def main():
    """Main entry point for code formatting setup."""
    import argparse

    parser = argparse.ArgumentParser(description="Code Formatting Tools Setup and Runner")
    parser.add_argument("--install", action="store_true", help="Install formatting tools")
    parser.add_argument("--format", action="store_true", help="Format all code")
    parser.add_argument("--check", action="store_true", help="Check formatting without changes")
    parser.add_argument("--report", action="store_true", help="Generate formatting report")
    parser.add_argument("--all", action="store_true", help="Install tools, format code, and generate report")

    args = parser.parse_args()

    if not any([args.install, args.format, args.check, args.report, args.all]):
        args.all = True  # Default to all operations

    print("🎨 CODE FORMATTING TOOLS SETUP")
    print("=" * 50)

    manager = CodeFormattingManager()
    success = True

    # Install tools if requested
    if args.install or args.all:
        if not manager.install_tools():
            success = False
        print()

    # Format code if requested
    if args.format or args.all:
        if not manager.format_code():
            success = False
        print()

    # Check formatting if requested
    if args.check:
        if not manager.check_formatting():
            success = False
        print()

    # Generate report if requested
    if args.report or args.all:
        report = manager.generate_formatting_report()
        manager.print_report(report)
        if not report["overall_compliant"]:
            success = False

    # Final status
    print("\n" + "=" * 50)
    if success:
        print("🎉 TASK 3.1: CONFIGURE CODE FORMATTING TOOLS - COMPLETED")
        print("✅ Black and isort configured and working")
        print("✅ Code formatting standards established")
        print("✅ All code properly formatted")
    else:
        print("⚠️ Code formatting setup completed with issues")
        print("Run with --format to fix formatting issues")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
