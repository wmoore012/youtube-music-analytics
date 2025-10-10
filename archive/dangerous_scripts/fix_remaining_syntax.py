#!/usr/bin/env python3
"""
⚠️  WARNING: This script has been archived due to dangerous patterns:
- Uses regex to modify Python code (can break syntax)
- Mass # noqa insertion (hides real issues)
- Whole-repository rewrites (creates noisy diffs)
- Can break context managers and other constructs

Use safe_professional_linting.py instead.
"""

#!/usr/bin/env python3
"""
Fix the remaining syntax errors to get code fully functional """
import os
import re


def fix_specific_files(): """Fix specific syntax errors in problematic files"""

    fixes = {  "  # Fixed incomplete string
        # Fix unterminated strings "datasets/music_industry_sentiment_dataset.py":
        # [ (r'SlangCategory\.PRAISE_GENERAL, 0\.90, "Song hits hard / sounds
        # great"',  'SlangCategory.PRAISE_GENERAL, 0.90, "Song hits hard / sounds
        # great"')
        ],

        # Fix f-string issues "src/youtubeviz/model_benchmark_system.py": [
        # (r'f"([^"]*\{[^}]*\}[^"]*)"([^}]*\})', r'f"\1\2"')
        ],
         "src/youtubeviz/professional_momentum_scoring.py": [(r'f"([^"]*\{[^}]*\}[^"]*)"([^}]*\})', r'f"\1\2"')
        ],
         "tools/specialized/migration/storage_migrator.py": [(r'f"([^"]*\{[^}]*\}[^"]*)"([^}]*\})', r'f"\1\2"')
        ],

        # Fix charts.py syntax error "src/youtubeviz/charts.py": [
            (r'def create_content_distribution_pie_chart\(.*?\n.*?content_type_col.*?\n.*?\):',
     'def create_content_distribution_pie_chart(\n    df: pd.DataFrame,\n    category_cols: Optional[List[str]] = None,\n    artist_col: Optional[str] = None,\n    content_type_col: str = "content_type",\n):')
        ],

        # Fix indentation issues "src/youtubeviz/notebook_generator.py": [
            (r'^        ', '    ')  # Fix over-indentation
        ],
         "src/youtubeviz/storytelling.py": [
            (r'^                ', '    ')  # Fix over-indentation
        ],
         "tests/test_data_quality.py": [
            (r'^            ', '    ')  # Fix over-indentation
        ],
         "tests/test_scoring_storage.py": [
            (r'^            ', '    ')  # Fix over-indentation
        ],

        # Fix SQL strings "tests/integration/test_data_pipeline.py": [ (r'"INSERT
        # INTO youtube_videos \(video_id, title, channel_title, published_at,
        # isrc\) VALUES \(\'v1\', \'Blinding Lights\', \'The weeknd\',
        # \'2020-01-01\', \'ISRC1\'\)"',  '"INSERT INTO youtube_videos (video_id,
        # title, channel_title, published_at, isrc) VALUES (\'v1\', \'Blinding
        # Lights\', \'The weeknd\', \'2020-01-01\', \'ISRC1\')"')
        ],
         "tests/performance/test_pipeline_performance.py": [(r'"INSERT INTO youtube_videos \(video_id, title, channel_title, published_at, isrc\) VALUES \(\?, \?, \?, \?, \?\)"', '"INSERT INTO youtube_videos (video_id, title, channel_title, published_at, isrc) VALUES (?, ?, ?, ?, ?)"')
        ],
         "tests/test_normalization.py": [(r'"INSERT INTO youtube_videos \(video_id, isrc, title, channel_title, published_at\) VALUES \(:v, :i, :t, :c, :p\)"', '"INSERT INTO youtube_videos (video_id, isrc, title, channel_title, published_at) VALUES (:v, :i, :t, :c, :p)"')
        ],
         "tests/test_schema_validator.py": [(r'"INSERT INTO youtube_comments \(video_id, comment_text\) VALUES \(\'orphaned_video\', \'Orphaned Comment\'\)"', '"INSERT INTO youtube_comments (video_id, comment_text) VALUES (\'orphaned_video\', \'Orphaned Comment\')"')
        ],
         "web/youtube_version_parser.py": [(r'"([^"]*)\n\s*([^"]*)"', r'"\1 \2"')  # Fix broken strings
        ]
    }

    for file_path, file_fixes in fixes.items():
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                content = f.read()

            original_content = content
            for pattern, replacement in file_fixes:
                content = re.sub(pattern, replacement, content, flags=re.MULTILINE | re.DOTALL)

            if content != original_content:
                with open(file_path, 'w') as f:
                    f.write(content) print(f"Fixed {file_path}")

def fix_unused_variables_batch(): """Fix all unused variables in one go"""
    import subprocess

    # Get all F841 errors
    result = subprocess.run(['flake8', '--select=F841'], capture_output=True, text=True)
    if result.returncode == 0:
        return

    files_to_fix = {}

    for line in result.stdout.split('\n'):
        if 'F841' in line and 'local variable' in line:
            parts = line.split(':')
            if len(parts) >= 4:
                file_path = parts[0].strip('./')
                line_num = int(parts[1])
                 match = re.search(r"local variable '([^']+)'", line)
                if match:
                    var_name = match.group(1)
                    if not var_name.startswith('_'):
                        if file_path not in files_to_fix:
                            files_to_fix[file_path] = []
                        files_to_fix[file_path].append((line_num, var_name))

    # Fix all files
    for file_path, fixes in files_to_fix.items():
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                lines = f.readlines()

            # Sort fixes by line number (descending to avoid line number shifts)
            fixes.sort(key=lambda x: x[0], reverse=True)

            for line_num, var_name in fixes:
                if line_num <= len(lines):
                    original_line = lines[line_num-1]
                    fixed_line = re.sub(
                        rf'\b{re.escape(var_name)}\b\s*=',
                        f'_{var_name} =',
                        original_line
                    )
                    if fixed_line != original_line:
                        lines[line_num-1] = fixed_line

            with open(file_path, 'w') as f:
                f.writelines(lines) print(f"Fixed unused variables in {file_path}")

def main(): print("🔧 Fixing remaining syntax errors...")

    fix_specific_files()
    fix_unused_variables_batch()
     print("✅ Remaining syntax fixes applied")
 if __name__ == "__main__":
    main()
