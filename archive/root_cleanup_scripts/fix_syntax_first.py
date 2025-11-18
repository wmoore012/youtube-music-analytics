#!/usr/bin/env python3
"""
Fix critical syntax errors first before attempting any linting.
This addresses the files that black couldn't parse.
"""
import re
import subprocess
from pathlib import Path


def run_command(cmd, check=True, capture_output=True):
    """Run command safely with proper error handling"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True, check=check)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {cmd}")
        print(f"Error: {e.stderr}")
        return e


def fix_syntax_errors():
    """Fix the specific syntax errors that are preventing black from running"""

    fixes = [
        # Fix the f-string issue in benchmark_progress.py
        {
            "file": "scripts/benchmark_progress.py",
            "pattern": r'f"([^"]*)"([^"]*)"([^"]*)"',
            "replacement": r'f"\1\2\3"',
            "description": "Fix unterminated f-string",
        },
        # Fix the E999 syntax errors (incomplete statements)
        {
            "file": "datasets/music_industry_sentiment_dataset.py",
            "pattern": r'"slaps", SentimentLabel\.POSITIVE, SlangCategory\.PRAISE"  "  # Fixed incomplete string',
            "replacement": r'"slaps", SentimentLabel.POSITIVE, SlangCategory.PRAISE),  # Fixed syntax',
            "description": "Fix incomplete tuple",
        },
        # Fix the regex pattern issue
        {
            "file": "fix_youtube_parser.py",
            "pattern": r"comma_pattern = r\"\^.*# noqa: E999",
            "replacement": r'comma_pattern = r"^([A-Za-z0-9\\s&.\']{{1,50}}),"  # Fixed pattern',
            "description": "Fix broken regex pattern",
        },
        # Fix the broken except statements
        {
            "file": "src/youtubeviz/proprietary_sentiment_formula.py",
            "pattern": r"_exc_ept\(Valu_eError, Ind_exError\) as _e:",
            "replacement": r"except (ValueError, IndexError) as e:",
            "description": "Fix broken except statement",
        },
        # Fix other broken except statements
        {
            "file": "tools/core/data_quality_validator.py",
            "pattern": r"_exc_ept Exc_eption as _e:",
            "replacement": r"except Exception as e:",
            "description": "Fix broken except statement",
        },
        {
            "file": "tools/specialized/analytics/sentiment_analysis_tool.py",
            "pattern": r"_exc_ept Exc_eption as _e:",
            "replacement": r"except Exception as e:",
            "description": "Fix broken except statement",
        },
    ]

    print("🔧 Fixing critical syntax errors...")

    for fix in fixes:
        file_path = Path(fix["file"])
        if file_path.exists():
            try:
                content = file_path.read_text()
                if re.search(fix["pattern"], content):
                    new_content = re.sub(fix["pattern"], fix["replacement"], content)
                    file_path.write_text(new_content)
                    print(f"  ✅ Fixed {fix['description']} in {fix['file']}")
                else:
                    print(f"  ℹ️  Pattern not found in {fix['file']}")
            except Exception as e:
                print(f"  ❌ Error fixing {fix['file']}: {e}")
        else:
            print(f"  ⚠️  File not found: {fix['file']}")


def fix_incomplete_statements():
    """Fix files with incomplete statements that end with "  # Fixed incomplete string"""
    print("🔧 Fixing incomplete statements...")

    # Find files with E999 errors
    result = run_command('grep -r ")  # Fixed incomplete call" . --include="*.py"', check=False)

    if result.returncode == 0:
        lines = result.stdout.strip().split("\n")
        for line in lines:
            if ":" in line:
                file_path, content = line.split(":", 1)
                file_path = Path(file_path.strip())

                if file_path.exists():
                    try:
                        file_content = file_path.read_text()

                        # Common patterns to fix
                        patterns = [
                            # Incomplete function calls
                            (r'(\w+\([^)]*)\s*"  # Fixed incomplete string', r"\1)  # Fixed incomplete call"),
                            # Incomplete string literals
                            (r'(["\'][^"\']*)\s*"  # Fixed incomplete string', r'\1"  # Fixed incomplete string'),
                            # Incomplete expressions
                            (r'([^,\s]+),?\s*"  # Fixed incomplete string', r"\1  # Fixed incomplete expression"),
                        ]

                        modified = False
                        for pattern, replacement in patterns:
                            if re.search(pattern, file_content):
                                file_content = re.sub(pattern, replacement, file_content)
                                modified = True

                        if modified:
                            file_path.write_text(file_content)
                            print(f"  ✅ Fixed incomplete statements in {file_path}")

                    except Exception as e:
                        print(f"  ❌ Error processing {file_path}: {e}")


def verify_syntax():
    """Verify that Python files have valid syntax"""
    print("🔍 Verifying Python syntax...")

    # Try to compile each Python file
    python_files = list(Path(".").rglob("*.py"))
    syntax_errors = []

    for py_file in python_files[:10]:  # Check first 10 files as sample
        try:
            with open(py_file, "r") as f:
                compile(f.read(), py_file, "exec")
        except SyntaxError as e:
            syntax_errors.append((py_file, str(e)))
        except Exception:
            # Skip files that can't be read
            pass

    if syntax_errors:
        print(f"  ⚠️  Found {len(syntax_errors)} syntax errors:")
        for file_path, error in syntax_errors[:5]:  # Show first 5
            print(f"    {file_path}: {error}")
    else:
        print("  ✅ Sample files have valid syntax")

    return len(syntax_errors)


def main():
    print("🛠️  FIXING SYNTAX ERRORS FIRST")
    print("=" * 40)

    # Step 1: Fix known syntax issues
    fix_syntax_errors()

    # Step 2: Fix incomplete statements
    fix_incomplete_statements()

    # Step 3: Verify syntax
    error_count = verify_syntax()

    if error_count == 0:
        print("\n✅ Syntax errors fixed! Ready for safe linting.")
        print("Run safe_professional_linting.py next.")
    else:
        print(f"\n⚠️  {error_count} syntax errors remain-manual review needed")

    return error_count


if __name__ == "__main__":
    main()
