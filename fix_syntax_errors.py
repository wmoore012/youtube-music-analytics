#!/usr/bin/env python3
"""
Fix all syntax errors in the codebase
"""
import os
import re
import subprocess


def fix_unterminated_strings():
    """Fix unterminated string literals"""
    files_with_errors = [
        "datasets/music_industry_sentiment_dataset.py",
        "src/youtubeviz/model_benchmark_system.py",
        "tests/integration/test_data_pipeline.py",
        "tests/performance/test_pipeline_performance.py",
        "tests/test_normalization.py",
        "tests/test_schema_validator.py",
        "web/sentiment_job.py",
        "web/youtube_channel_etl.py",
        "web/youtube_version_parser.py"
    ]

    for file_path in files_with_errors:
        if os.path.exists(file_path):
            print(f"Fixing unterminated strings in {file_path}")
            with open(file_path, 'r') as f:
                content = f.read()

            # Fix common unterminated string patterns
            # Pattern 1: Multi-line strings that got broken
            content = re.sub(r'(\s+)"([^"]*)\n([^"]*)"', r'\1"\2 \3"', content)

            # Pattern 2: SQL queries split across lines
            content = re.sub(r'"SELECT ([^"]*)\n\s*([^"]*)"', r'"SELECT \1 \2"', content)

            # Pattern 3: Long strings that need proper continuation
            lines = content.split('\n')
            fixed_lines = []
            i = 0
            while i < len(lines):
                line = lines[i]
                # Check if line has unterminated string
                if '"' in line and line.count('"') % 2 == 1:
                    # Look for continuation on next line
                    if i + 1 < len(lines) and '"' in lines[i + 1]:
                        # Combine the lines properly
                        combined = line.rstrip() + ' " + "' + lines[i + 1].strip()
                        fixed_lines.append(combined)
                        i += 2  # Skip next line
                        continue
                fixed_lines.append(line)
                i += 1

            with open(file_path, 'w') as f:
                f.write('\n'.join(fixed_lines))


def fix_f_string_errors():
    """Fix f-string syntax errors"""
    files_with_errors = [
        "src/youtubeviz/professional_momentum_scoring.py",
        "tools/specialized/migration/storage_migrator.py"
    ]

    for file_path in files_with_errors:
        if os.path.exists(file_path):
            print(f"Fixing f-string errors in {file_path}")
            with open(file_path, 'r') as f:
                content = f.read()

            # Fix missing closing braces in f-strings
            content = re.sub(r'f"([^"]*\{[^}]*)"', r'f"\1}"', content)
            content = re.sub(r"f'([^']*\{[^}]*)'", r"f'\1}'", content)

            with open(file_path, 'w') as f:
                f.write(content)


def fix_indentation_errors():
    """Fix indentation errors"""
    files_with_errors = [
        "src/youtubeviz/charts.py",
        "src/youtubeviz/storytelling.py",
        "tests/test_data_quality.py",
        "tests/test_notebook_chart_validation.py"
    ]

    for file_path in files_with_errors:
        if os.path.exists(file_path):
            print(f"Fixing indentation in {file_path}")
            # Use autopep8 to fix indentation
            try:
                subprocess.run(['autopep8', '--in-place', '--select=E111,E112,E113,E114', file_path],
                             check=True, capture_output=True)  # noqa: E128
            except subprocess.CalledProcessError:
                print(f"Could not auto-fix indentation in {file_path}")


def main():
    print("🔧 Fixing all syntax errors...")

    fix_unterminated_strings()
    fix_f_string_errors()
    fix_indentation_errors()

    # Check if syntax errors are fixed
    result = subprocess.run(['flake8', '--select=E999', '--count'],
                          capture_output=True, text=True)  # noqa: E128

    if result.returncode == 0:
        print("✅ All syntax errors fixed!")
    else:
        print("❌ Some syntax errors remain:")
        print(result.stdout)
        print(result.stderr)


if __name__ == "__main__":
    main()  # noqa: W292
