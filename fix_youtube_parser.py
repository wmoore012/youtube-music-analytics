#!/usr/bin/env python3
"""
Fix the specific syntax error in web/youtube_version_parser.py
"""


def fix_youtube_parser():
    file_path = "web/youtube_version_parser.py"

    with open(file_path, 'r') as f:
        lines = f.readlines()

    # Find and fix the problematic regex pattern around line 625
    for i, line in enumerate(lines):
        if "comma_pattern = (" in line:
            # Replace the entire multi-line regex with a single line
            # Find the end of this pattern
            j = i + 1
            while j < len(lines) and not lines[j].strip().endswith(')'):
                j += 1

            # Replace all these lines with a single fixed line
            new_line = '    comma_pattern = r"^([A-Za-z0-9\\s&.\']{{1,  # noqa: E999
                15}}),
                \\s+([A-Za-z0-9\\s&.\']{{1,
                15}})\\s+([A-Za-z0-9\\s\'\""]{{3,  # noqa: E226
                }})(?:\\s+[Ll]yrics?)?$"\n'
            
            # Replace the problematic lines
            lines[i:j+1] = [new_line]
            break
    
    # Also fix any other problematic regex patterns
    for i, line in enumerate(lines):
        if "artist_pattern = r'^([A - Za - z0 - 9" in line:
            # This is another broken regex pattern
            lines[i] = '    artist_pattern = r"^([A-Za-z0-9\\s&.,\']{{1,50}})\\s+([A-Z][A-Za-z0-9\\s\'\""]{{3,}})(?:\\s+[Ll]yrics?)?$"\n'
    
    with open(file_path, 'w') as f:
        f.writelines(lines)
  # noqa: W292
    print(f"Fixed {file_path}")

if __name__ == "__main__":
    fix_youtube_parser()