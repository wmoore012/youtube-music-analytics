#!/usr/bin/env python3
"""
Fix critical syntax errors that prevent the code from running
"""
import os
import re


def fix_file_syntax(file_path, fixes):
    """Apply syntax fixes to a file"""
    if not os.path.exists(file_path):
        return

    with open(file_path, 'r') as f:
        content = f.read()

    original_content = content

    for pattern, replacement in fixes:
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE | re.DOTALL)

    if content != original_content:
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Fixed {file_path}")


def main():
    print("🔧 Fixing critical syntax errors...")

    # Fix specific syntax errors
    fixes = {
        "datasets/music_industry_sentiment_dataset.py": [
            (r'SlangCategory\.PRAISE" " \+ ""_GENERAL', 'SlangCategory.PRAISE_GENERAL')
        ],
        "fix_youtube_parser.py": [
            (r'r"([^"]*\\s[^"]*)"([^"]*)', r'r"\1\2"')
        ],
        "scripts/benchmark_progress.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*\n[^}]*)', r'f"\1\2}"')
        ],
        "src/youtubeviz/charts.py": [
            (r'def create_content_distribution_pie_chart\(\n\s+df: pd\.DataFrame,.*?\):',
             'def create_content_distribution_pie_chart(\n    df: pd.DataFrame,\n    category_cols: Optional[List[str]] = None,\n    artist_col: Optional[str] = None,\n    content_type_col: str = "content_type",\n):')
        ],
        "src/youtubeviz/model_benchmark_system.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "src/youtubeviz/notebook_generator.py": [
            (r'^    (\S)', r'\1')  # Fix indentation
        ],
        "src/youtubeviz/professional_momentum_scoring.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "src/youtubeviz/storytelling.py": [
            (r'^    (\S)', r'\1')  # Fix indentation
        ],
        "tests/integration/test_data_pipeline.py": [
            (r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')
        ],
        "tests/performance/test_pipeline_performance.py": [
            (r'^    (\S)', r'\1')  # Fix indentation
        ],
        "tests/test_data_quality.py": [
            (r'^    (\S)', r'\1')  # Fix indentation
        ],
        "tests/test_normalization.py": [
            (r'^        (\S)', r'    \1')  # Fix indentation
        ],
        "tests/test_schema_validator.py": [
            (r'^    (\S)', r'\1')  # Fix indentation
        ],
        "tests/test_scoring_storage.py": [
            (r'^    (\S)', r'\1')  # Fix indentation
        ],
        "tools/specialized/migration/storage_migrator.py": [
            (r'f"([^"]*\{[^}]*)"([^}]*)', r'f"\1\2}"')
        ],
        "web/youtube_version_parser.py": [
            (r'^    (\S)', r'\1')  # Fix indentation at start of file
        ]
    }

    for file_path, file_fixes in fixes.items():
        fix_file_syntax(file_path, file_fixes)

    print("✅ Critical syntax fixes applied")


if __name__ == "__main__":
    main()
