#!/usr/bin/env python3
"""
Clean up the dangerous linting scripts that use regex hacks
and could break code. Replace with safe alternatives.
"""
from pathlib import Path


def main():
    print("🧹 Cleaning up dangerous linting scripts...")

    dangerous_scripts = [
        "reduce_to_100_errors.py",
        "push_to_zero_errors.py",
        "final_27_errors.py",
        "careful_final_reduction.py",
        "aggressive_error_reduction.py",
        "targeted_linting_fix.py",
        "fix_remaining_syntax.py",
        "final_zero_linting.py",
        "fix_critical_syntax.py",
        "final_cleanup_script.py",
        "final_linting_cleanup.py",
    ]

    moved_count = 0

    # Create archive directory
    archive_dir = Path("archive/dangerous_scripts")
    archive_dir.mkdir(parents=True, exist_ok=True)

    for script in dangerous_scripts:
        script_path = Path(script)
        if script_path.exists():
            # Move to archive with warning comment
            archive_path = archive_dir / script

            # Add warning header to archived script
            content = script_path.read_text()
            warning = '''#!/usr/bin/env python3
"""
⚠️  WARNING: This script has been archived due to dangerous patterns:
- Uses regex to modify Python code (can break syntax)
- Mass # noqa insertion (hides real issues)
- Whole-repository rewrites (creates noisy diffs)
- Can break context managers and other constructs

Use safe_professional_linting.py instead.
"""

'''

            archive_path.write_text(warning + content)
            script_path.unlink()  # Remove original
            moved_count += 1
            print(f"  📦 Archived {script}")

    # Create a README in the archive
    readme_content = """# Archived Dangerous Linting Scripts

These scripts have been archived because they use dangerous patterns that can break code:

## Problems with these scripts:

1. **Regex-based code modification**: Uses string replacement to modify Python code, which can break syntax
2. **Mass # noqa insertion**: Hides real code quality issues instead of fixing them
3. **Whole-repository rewrites**: Creates noisy diffs and makes it hard to review changes
4. **Context manager breakage**: Can turn `with open() as f:` into `with open() as _:` breaking functionality
5. **Double-underscore variables**: Creates confusing variable names like `__var_name`

## Safe alternatives:

- Use `safe_professional_linting.py` for proper tooling-based approach
- Use `ruff --fix` for safe automatic fixes
- Use `black` for code formatting
- Create baselines for gradual improvement instead of mass # noqa

## Key principles:

- Never use regex to modify Python code
- Use proper AST-aware tools (ruff, black, autoflake)
- Apply fixes incrementally with proper review
- Maintain test coverage throughout the process
"""

    (archive_dir / "README.md").write_text(readme_content)

    print(f"\n✅ Archived {moved_count} dangerous scripts")
    print("📝 Created archive/dangerous_scripts/README.md with explanation")
    print("🛡️  Use safe_professional_linting.py for future linting needs")


if __name__ == "__main__":
    main()
