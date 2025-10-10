#!/usr/bin/env python3
"""
Final targeted fix for the remaining 44 flake8 errors.
Focuses on specific, safe fixes for the identified issues.
"""
import subprocess
import re
from pathlib import Path


def run_command(cmd, check=True):
    """Run command safely"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {cmd}")
        return e


def fix_unterminated_strings():
    """Fix the 21 E999 unterminated string errors"""
    print("🔧 Fixing unterminated string literals...")

    fixes = [
        # Fix the specific files mentioned in the error output
        {
            "file": "datasets/music_industry_sentiment_dataset.py",
            "line": 236,
            "description": "Fix unterminated string"
        },
        {
            "file": "fix_youtube_parser.py",
            "line": 23,
            "description": "Fix unterminated string"
        },
        {
            "file": "scripts/benchmark_progress.py",
            "line": 128,
            "description": "Fix unterminated f-string"
        },
        {
            "file": "src/youtubeviz/proprietary_sentiment_formula.py",
            "line": 404,
            "description": "Fix unterminated string"
        },
        {
            "file": "tests/integration/test_data_pipeline.py",
            "line": 27,
            "description": "Fix unterminated string"
        },
        {
            "file": "tests/performance/test_pipeline_performance.py",
            "line": 32,
            "description": "Fix unterminated string"
        },
        {
            "file": "tests/test_normalization.py",
            "line": 70,
            "description": "Fix unterminated string"
        },
        {
            "file": "tests/test_schema_validator.py",
            "line": 37,
            "description": "Fix unterminated string"
        },
        {
            "file": "tools/core/data_quality_validator.py",
            "line": 183,
            "description": "Fix unterminated string"
        },
        {
            "file": "tools/specialized/analytics/sentiment_analysis_tool.py",
            "line": 163,
            "description": "Fix unterminated string"
        },
        {
            "file": "web/youtube_version_parser.py",
            "line": 284,
            "description": "Fix unterminated string"
        }
    ]

    for fix in fixes:
        file_path = Path(fix["file"])
        if file_path.exists():
            try:
                lines = file_path.read_text().splitlines()
                if len(lines) >= fix["line"]:
                    line_content = lines[fix["line"] - 1]

                    # Common patterns for unterminated strings
                    if '"""' in line_content and line_content.count('"""') % 2 == 1:
                        # Add closing triple quote
                        lines[fix["line"] - 1] = line_content + '"""'
                        file_path.write_text('\n'.join(lines) + '\n')
                        print(f"  ✅ Fixed triple quote in {fix['file']}:{fix['line']}")

                    elif '"' in line_content and line_content.count('"') % 2 == 1:
                        # Add closing quote
                        lines[fix["line"] - 1] = line_content + '"'
                        file_path.write_text('\n'.join(lines) + '\n')
                        print(f"  ✅ Fixed quote in {fix['file']}:{fix['line']}")

                    elif 'f"' in line_content and not line_content.rstrip().endswith('"'):
                        # Fix f-string
                        lines[fix["line"] - 1] = line_content.rstrip() + '"'
                        file_path.write_text('\n'.join(lines) + '\n')
                        print(f"  ✅ Fixed f-string in {fix['file']}:{fix['line']}")

                    else:
                        print(f"  ⚠️  Manual review needed for {fix['file']}:{fix['line']}")

            except Exception as e:
                print(f"  ❌ Error fixing {fix['file']}: {e}")


def fix_unused_variables():
    """Remove the 4 unused variables (F841)"""
    print("🗑️  Removing unused variables...")

    # Use ruff to fix F841 safely
    result = run_command("ruff check --select F841 --fix .", check=False)
    if result.returncode == 0:
        print("  ✅ Removed unused variables with ruff")
    else:
        print("  ⚠️  Manual removal needed for unused variables")


def fix_formatting_issues():
    """Fix the remaining formatting issues"""
    print("🎨 Fixing formatting issues...")

    # Fix trailing whitespace
    run_command("ruff check --select W291,W292,W293 --fix .", check=False)

    # Fix blank lines
    run_command("ruff check --select E303 --fix .", check=False)

    # Fix indentation where safe
    run_command("ruff check --select E121,E124,E126,E128 --fix .", check=False)

    print("  ✅ Applied formatting fixes")


def fix_specific_syntax_errors():
    """Fix specific syntax errors that can be automated"""
    print("🔧 Fixing specific syntax errors...")

    # Fix invalid decimal literals (1e-6 should be 1e-6)
    files_to_fix = [
        "tests/test_benchmark_progress.py",
        "tests/test_notebook_validator_comprehensive_data_science.py",
        "tests/test_operations_monitor.py",
        "tests/test_statistical_utils.py"
    ]

    for file_path in files_to_fix:
        path = Path(file_path)
        if path.exists():
            try:
                content = path.read_text()
                # Fix scientific notation with spaces
                content = re.sub(r'(\d+)e\s*-\s*(\d+)', r'\1e-\2', content)
                path.write_text(content)
                print(f"  ✅ Fixed decimal literals in {file_path}")
            except Exception as e:
                print(f"  ❌ Error fixing {file_path}: {e}")


def verify_progress():
    """Check how many errors remain"""
    print("📊 Checking progress...")
    result = run_command("flake8 --count", check=False)
    if result.returncode != 0 and result.stdout:
        lines = result.stdout.strip().split('\n')
        for line in reversed(lines):
            if line.strip().isdigit():
                return int(line.strip())
    return 0


def main():
    print("🎯 FINAL TARGETED FIX")
    print("=" * 30)
    print("Fixing the remaining 44 specific flake8 errors")
    print()

    start_errors = verify_progress()
    print(f"Starting with {start_errors} errors")

    # Fix each category of errors
    fix_unterminated_strings()
    fix_specific_syntax_errors()
    fix_unused_variables()
    fix_formatting_issues()

    # Check final count
    end_errors = verify_progress()
    print(f"\n📊 Results:")
    print(f"  Started: {start_errors} errors")
    print(f"  Ended: {end_errors} errors")

    if end_errors < start_errors:
        print(f"🎉 Fixed {start_errors-end_errors} errors!")

    if end_errors == 0:
        print("🏆 ZERO FLAKE8 ERRORS ACHIEVED!")
    elif end_errors < 10:
        print("🎯 Almost there! Just a few errors left for manual review.")

    # Show remaining errors if any
    if end_errors > 0:
        print("\n📋 Remaining errors:")
        result = run_command("flake8 --count --statistics", check=False)
        if result.stdout:
            print(result.stdout)


if __name__ == "__main__":
    main()
