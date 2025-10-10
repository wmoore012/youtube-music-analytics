#!/usr / bin / env python3
"""
Generate comprehensive CI validation report.

This script creates a summary report of all CI checks and system health.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


class CIReportGenerator:
    """Generates comprehensive CI validation reports."""

    def __init__(self):
        self.report = {
            "timestamp": datetime.now().isoformat(),
            "system_info": {},
            "code_quality": {},
            "database_status": {},
            "notebook_status": {},
            "overall_status": "UNKNOWN",
        }

    def get_system_info(self) -> Dict[str, Any]:
        """Get system information."""
        try:
            python_version = sys.version.split()[0]

            # Get git info if available
            git_info = {}
            try:
                git_branch = subprocess.check_output(["git", "branch", "--show-current"], text=True).strip()
                git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()[:8]
                git_info = {"branch": git_branch, "commit": git_commit}
            except Exception:
                git_info = {"branch": "unknown", "commit": "unknown"}

            return {
                "python_version": python_version,
                "platform": sys.platform,
                "git_info": git_info,
                "working_directory": str(Path.cwd()),
            }
        except Exception as e:
            return {"error": str(e)}

    def check_code_quality(self) -> Dict[str, Any]:
        """Check code quality metrics."""
        quality_checks = {
            "black_formatting": self.run_command(["black", "--check", "--line-length=120", "."]),
            "isort_imports": self.run_command(["isort", "--check-only", "--profile", "black", "."]),
            "flake8_linting": self.run_command(
                ["flake8", "--max-line-length=120", "--exclude=.git,__pycache__,notebooks,venv,.venv"]
            ),
            "loc_limits": self.run_command(["python", "scripts / validate_loc_limits.py"]),
        }

        # Count Python files
        python_files = list(Path(".").rglob("*.py"))
        python_files = [
            f for f in python_files if not any(exclude in str(f) for exclude in ["__pycache__", ".git", "venv"])
        ]

        return {
            "checks": quality_checks,
            "python_files_count": len(python_files),
            "all_passed": all(check["success"] for check in quality_checks.values()),
        }

    def check_database_status(self) -> Dict[str, Any]:
        """Check database connectivity and schema."""
        try:
            # Try to import and test database connection
            sys.path.insert(0, ".")
            from sqlalchemy import text

            from web.etl_helpers import get_engine

            engine = get_engine()
            with engine.connect() as conn:
                # Test basic connectivity
                result = conn.execute(text("SELECT 1"))
                connectivity = result.fetchone()[0] == 1

                # Check table counts
                tables_info = {}
                core_tables = ["youtube_videos", "youtube_metrics", "youtube_comments", "comment_sentiment"]

                for table in core_tables:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        count = result.fetchone()[0]
                        tables_info[table] = {"exists": True, "count": count}
                    except Exception as e:
                        tables_info[table] = {"exists": False, "error": str(e)}

                return {
                    "connectivity": connectivity,
                    "tables": tables_info,
                    "schema_validation": self.run_command(["python", "scripts / test_schema_alignment.py"]),
                }

        except Exception as e:
            return {
                "connectivity": False,
                "error": str(e),
                "tables": {},
                "schema_validation": {"success": False, "error": str(e)},
            }

    def check_notebook_status(self) -> Dict[str, Any]:
        """Check notebook validation status."""
        notebook_checks = {
            "syntax_validation": self.run_command(["python", "scripts / validate_notebooks.py"]),
            "outputs_stripped": self.run_command(["python", "scripts / check_notebook_outputs.py"]),
        }

        # Count notebooks
        notebooks_dir = Path("notebooks")
        if notebooks_dir.exists():
            notebook_files = list(notebooks_dir.rglob("*.ipynb"))
            notebook_files = [f for f in notebook_files if ".ipynb_checkpoints" not in str(f)]
            notebook_count = len(notebook_files)
        else:
            notebook_count = 0

        return {
            "checks": notebook_checks,
            "notebook_count": notebook_count,
            "all_passed": all(check["success"] for check in notebook_checks.values()),
        }

    def run_command(self, command: List[str]) -> Dict[str, Any]:
        """Run a command and return result info."""
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=60)
            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "Command timed out", "returncode": -1}
        except Exception as e:
            return {"success": False, "error": str(e), "returncode": -1}

    def generate_report(self) -> Dict[str, Any]:
        """Generate complete CI report."""
        print("🔍 Generating CI validation report...")

        # Collect all information
        self.report["system_info"] = self.get_system_info()
        self.report["code_quality"] = self.check_code_quality()
        self.report["database_status"] = self.check_database_status()
        self.report["notebook_status"] = self.check_notebook_status()

        # Determine overall status
        all_systems_good = all(
            [
                self.report["code_quality"].get("all_passed", False),
                self.report["database_status"].get("connectivity", False),
                self.report["notebook_status"].get("all_passed", False),
            ]
        )

        self.report["overall_status"] = "PASS" if all_systems_good else "FAIL"

        return self.report

    def print_summary(self):
        """Print a human-readable summary of the report."""
        print("\\n" + "=" * 60)
        print("📋 CI VALIDATION REPORT")
        print("=" * 60)

        # System info
        sys_info = self.report["system_info"]
        print(
            f"🖥️  System: Python {sys_info.get('python_version', 'unknown')} on {
              sys_info.get('platform', 'unknown')}"
        )
        if "git_info" in sys_info:
            git = sys_info["git_info"]
            print(f"📝 Git: {git.get('branch', 'unknown')} @ {git.get('commit', 'unknown')}")

        # Code quality
        quality = self.report["code_quality"]
        status = "✅ PASS" if quality.get("all_passed", False) else "❌ FAIL"
        print(f"\\n🔧 Code Quality: {status}")
        print(f"   • Python files: {quality.get('python_files_count', 0)}")

        for check_name, check_result in quality.get("checks", {}).items():
            check_status = "✅" if check_result.get("success", False) else "❌"
            print(f"   • {check_name}: {check_status}")

        # Database status
        db = self.report["database_status"]
        db_status = "✅ CONNECTED" if db.get("connectivity", False) else "❌ DISCONNECTED"
        print(f"\\n🗄️  Database: {db_status}")

        for table_name, table_info in db.get("tables", {}).items():
            if table_info.get("exists", False):
                count = table_info.get("count", 0)
                print(f"   • {table_name}: {count:,} records")
            else:
                print(f"   • {table_name}: ❌ Missing")

        # Notebook status
        notebooks = self.report["notebook_status"]
        nb_status = "✅ VALID" if notebooks.get("all_passed", False) else "❌ ISSUES"
        print(f"\\n📓 Notebooks: {nb_status}")
        print(f"   • Notebook files: {notebooks.get('notebook_count', 0)}")

        # Overall status
        overall = self.report["overall_status"]
        overall_icon = "🎉" if overall == "PASS" else "⚠️"
        print(f"\\n{overall_icon} Overall Status: {overall}")

        if overall == "FAIL":
            print("\\n💡 Issues found. Check individual sections above for details.")
        else:
            print("\\n🚀 System is ready for deployment!")

        print("=" * 60)


def main():
    """Main function."""
    generator = CIReportGenerator()
    report = generator.generate_report()

    # Print summary
    generator.print_summary()

    # Save detailed report
    report_file = Path("ci_report.json")
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\\n📄 Detailed report saved to: {report_file}")

    # Return appropriate exit code
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
