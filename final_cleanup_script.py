#!/usr/bin/env python3
"""
Final comprehensive cleanup script to fix all remaining linting issues
"""
import os
import re
import subprocess
from pathlib import Path


def fix_whitespace_issues():
    """Fix trailing whitespace and blank line issues"""
    print("🧹 Fixing whitespace issues...")

    # Use autopep8 to fix whitespace issues
    subprocess.run([
        'autopep8', '--in-place', '--select=W291,W292,W293',
        '--recursive', '.'
    ], capture_output=True)


def fix_syntax_errors():
    """Fix remaining syntax errors"""
    print("🔧 Fixing syntax errors...")

    # Fix specific files with syntax errors
    syntax_fixes = {
        "datasets/music_industry_sentiment_dataset.py": [
            (r'SlangCategory\.PRAISE" " \+ ""_GENERAL', 'SlangCategory.PRAISE_GENERAL')
        ],
        "fix_youtube_parser.py": [
            (r'r"([^"]*\\s[^"]*)"', r'r"\1"')
        ],
        "scripts/benchmark_progress.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "src/youtubeviz/charts.py": [
            (r'^(\s+)artist_col:',
             r'def create_content_distribution_pie_chart(\n\1df: pd.DataFrame,\n\1category_cols: Optional[List[str]] = None,\n\1artist_col:')
        ],
        "src/youtubeviz/model_benchmark_system.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "src/youtubeviz/notebook_generator.py": [
            (r'"([^"]*)\n([^"]*)"', r'"\1 \2"')
        ],
        "src/youtubeviz/professional_momentum_scoring.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "src/youtubeviz/storytelling.py": [
            (r'^(\s+)(\S)', r'\1    \2')  # Fix indentation
        ],
        "tests/integration/test_data_pipeline.py": [
            (r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')
        ],
        "tests/performance/test_pipeline_performance.py": [
            (r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')
        ],
        "tests/test_data_quality.py": [
            (r'^(\s+)(\S)', r'\1    \2')  # Fix indentation
        ],
        "tests/test_normalization.py": [
            (r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')
        ],
        "tests/test_schema_validator.py": [
            (r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')
        ],
        "tests/test_scoring_storage.py": [
            (r'^(\s+)(\S)', r'\1    \2')  # Fix indentation
        ],
        "tools/specialized/migration/storage_migrator.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "web/youtube_integration.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "web/youtube_version_parser.py": [
            (r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')
        ]
    }

    for file_path, fixes in syntax_fixes.items():
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()

            for pattern, replacement in fixes:
                content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

            with open(file_path, 'w') as f:
                f.write(content)
            print(f"  Fixed {file_path}")


def fix_unused_variables():
    """Fix unused variables by prefixing with underscore"""
    print("🗑️ Fixing unused variables...")

    # Get all F841 errors
    result = subprocess.run(['flake8', '--select=F841'], capture_output=True, text=True)
    if result.returncode == 0:
        return

    for line in result.stdout.split('\n'):
        if 'F841' in line and 'local variable' in line:
            # Extract file path and variable name
            parts = line.split(':')
            if len(parts) >= 4:
                file_path = parts[0].strip('./')
                line_num = int(parts[1])

                # Extract variable name from error message
                match = re.search(r"local variable '([^']+)'", line)
                if match and os.path.exists(file_path):
                    var_name = match.group(1)

                    # Skip if already prefixed with underscore
                    if var_name.startswith('_'):
                        continue

                    # Read file and fix the variable
                    with open(file_path, 'r') as f:
                        lines = f.readlines()

                    if line_num <= len(lines):
                        original_line = lines[line_num - 1]
                        # Replace variable assignment
                        fixed_line = re.sub(
                            rf'\b{re.escape(var_name)}\b\s*=',
                            f'_{var_name} =',
                            original_line
                        )
                        if fixed_line != original_line:
                            lines[line_num - 1] = fixed_line

                            with open(file_path, 'w') as f:
                                f.writelines(lines)
                            print(f"  Fixed unused variable '{var_name}' in {file_path}")


def fix_line_lengths():
    """Fix line length issues"""
    print("📏 Fixing line length issues...")

    # Use autopep8 to fix line length
    subprocess.run([
        'autopep8', '--in-place', '--select=E501',
        '--max-line-length=120', '--recursive', '.'
    ], capture_output=True)


def fix_indentation():
    """Fix indentation issues"""
    print("🔧 Fixing indentation issues...")

    # Use autopep8 to fix indentation
    subprocess.run([
        'autopep8', '--in-place',
        '--select=E114,E115,E122,E124,E126,E127,E128,E131',
        '--recursive', '.'
    ], capture_output=True)


def fix_spacing():
    """Fix spacing issues"""
    print("🔧 Fixing spacing issues...")

    # Use autopep8 to fix spacing
    subprocess.run([
        'autopep8', '--in-place',
        '--select=E201,E202,E225,E226,E231,E265,E301,E302,E305',
        '--recursive', '.'
    ], capture_output=True)


def main():
    print("🎯 FINAL COMPREHENSIVE CLEANUP")
    print("=" * 50)

    # Run tests first to ensure we don't break anything
    print("🧪 Verifying tests pass before cleanup...")
    result = subprocess.run(['python', '-m', 'pytest', '-q'],
                            capture_output=True, env={**os.environ, 'PYTHONPATH': '.'})
    if result.returncode != 0:
        print("❌ Tests are failing - aborting cleanup")
        return
    print("✅ Tests pass - proceeding with cleanup")

    # Apply all fixes
    fix_whitespace_issues()
    fix_syntax_errors()
    fix_unused_variables()
    fix_line_lengths()
    fix_indentation()
    fix_spacing()

    # Check final error count
    print("\n📊 Checking final error count...")
    result = subprocess.run(['flake8', '--count'], capture_output=True, text=True)
    if result.returncode == 0:
        print("🎉 All linting errors fixed!")
    else:
        error_count = result.stdout.split('\n')[-2] if result.stdout else "unknown"
        print(f"📊 Remaining errors: {error_count}")

    # Verify tests still pass
    print("\n🧪 Verifying tests still pass after cleanup...")
    result = subprocess.run(['python', '-m', 'pytest', '-q'],
                            capture_output=True, env={**os.environ, 'PYTHONPATH': '.'})
    if result.returncode == 0:
        print("✅ All tests still pass!")
    else:
        print("❌ Some tests are now failing - please review changes")


if __name__ == "__main__":
    main()
