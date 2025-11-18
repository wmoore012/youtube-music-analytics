#!/usr/bin/env python3
"""
Corrected safe linting with proper ruff syntax and syntax error handling.
"""
import json
import subprocess
import sys
from pathlib import Path


def run_command(cmd, check=True, capture_output=True):
    """Run command safely with proper error handling"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True, check=check)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {cmd}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return e


def verify_tests_pass():
    """Ensure tests pass before making any changes"""
    print("🧪 Verifying tests pass before making changes...")
    result = run_command("PYTHONPATH=. python -m pytest -q --tb=no", check=False)
    if result.returncode != 0:
        print("❌ Tests are failing-aborting to avoid breaking working code")
        return False
    print("✅ Tests pass-safe to proceed")
    return True


def apply_safe_fixes_only():
    """Apply only the safest automatic fixes using correct ruff syntax"""
    print("🔧 Applying ONLY safe automatic fixes...")

    # 1. Format with black (safe, skip files with syntax errors)
    print("  🎨 Formatting with black (skipping syntax errors)...")
    run_command("black --line-length=120 --safe .", check=False)

    # 2. Fix imports with ruff (correct syntax)
    print("  📦 Fixing import order...")
    run_command("ruff check --select I --fix .", check=False)

    # 3. Remove unused imports ONLY (not variables)
    print("  🗑️  Removing unused imports (safe)...")
    run_command("ruff check --select F401 --fix .", check=False)

    # 4. Fix whitespace issues (safe)
    print("  🧹 Fixing whitespace...")
    run_command("ruff check --select W291,W292,W293,E302,E303,E305 --fix .", check=False)

    print("  ✅ Safe fixes applied")


def get_current_error_count():
    """Get current flake8 error count"""
    result = run_command("flake8 --count", check=False)
    if result.returncode != 0 and result.stdout:
        try:
            # Extract number from last line
            lines = result.stdout.strip().split("\n")
            for line in reversed(lines):
                if line.strip().isdigit():
                    return int(line.strip())
        except Exception:
            pass
    return 0


def create_ruff_baseline():
    """Create a proper baseline for gradual improvement"""
    print("📊 Creating ruff baseline...")

    result = run_command("ruff check . --output-format=json", check=False)

    if result.returncode != 0 and result.stdout:
        try:
            violations = json.loads(result.stdout)
            print(f"  📝 Found {len(violations)} ruff violations")

            with open(".ruff-baseline.json", "w") as f:
                json.dump(violations, f, indent=2)

            return len(violations)
        except json.JSONDecodeError:
            print("  ⚠️  Could not parse ruff output")
            return 0
    else:
        print("  ✅ No ruff violations found!")
        return 0


def setup_proper_tooling():
    """Set up proper modern tooling with correct configuration"""
    print("🛠️  Setting up proper tooling...")

    # Update pyproject.toml with working ruff config
    ruff_config = """
[tool.ruff]
# Basic checks that work reliably
select = [
    "E",   # pycodestyle errors
    "F",   # pyflakes  
    "W",   # pycodestyle warnings
    "I",   # isort
]

# Ignore problematic rules for now
ignore = [
    "E501",  # Line too long (handled by black)
    "E999",  # Syntax errors (fix separately)
]

# Only auto-fix safe things
fixable = ["F401", "I001", "W291", "W292", "W293", "E302", "E303", "E305"]
unfixable = ["F841", "E999"]  # Don't auto-remove variables or fix syntax

line-length = 120
target-version = "py38"

[tool.ruff.per-file-ignores]
# Tests can be more flexible
"tests/**/*.py" = ["F401", "F841"]
# Scripts can use print statements
"scripts/**/*.py" = ["T201"]
# Archive can be ignored
"archive/**/*.py" = ["ALL"]
# Virtual env should be ignored
".venv/**/*.py" = ["ALL"]

[tool.black]
line-length = 120
target-version = ['py38']
skip-string-normalization = true
"""

    pyproject_path = Path("pyproject.toml")
    if pyproject_path.exists():
        content = pyproject_path.read_text()
        if "[tool.ruff]" not in content:
            with open("pyproject.toml", "a") as f:
                f.write("\n" + ruff_config)
            print("  ✅ Added ruff config to pyproject.toml")
        else:
            print("  ℹ️  Ruff config already exists")
    else:
        pyproject_path.write_text(ruff_config)
        print("  ✅ Created pyproject.toml with ruff config")


def main():
    print("🛡️  CORRECTED SAFE LINTING")
    print("=" * 40)
    print("Using proper ruff syntax and handling syntax errors")
    print()

    # Get starting error count
    start_errors = get_current_error_count()
    print(f"📊 Starting with {start_errors} flake8 errors")

    # Step 1: Verify tests pass
    if not verify_tests_pass():
        print("Fix tests first, then run linting")
        return 1

    # Step 2: Set up proper tooling
    setup_proper_tooling()

    # Step 3: Create baseline
    ruff_violations = create_ruff_baseline()

    # Step 4: Apply safe fixes
    apply_safe_fixes_only()

    # Step 5: Verify tests still pass
    print("\n🧪 Verifying tests still pass...")
    if not verify_tests_pass():
        print("❌ Tests broken-need manual review")
        return 1

    # Step 6: Show results
    end_errors = get_current_error_count()
    print(f"\n📊 Results:")
    print(f"  Started with: {start_errors} flake8 errors")
    print(f"  Ended with: {end_errors} flake8 errors")
    print(f"  Ruff violations: {ruff_violations}")

    if end_errors < start_errors:
        print(f"🎉 Reduced errors by {start_errors-end_errors}!")

    print(f"\n✅ SAFE APPROACH COMPLETE")
    print("🛡️  Only safe fixes applied")
    print("🧪 Tests verified throughout")
    print("\nNext steps:")
    print("- Fix remaining syntax errors manually")
    print("- Use ruff check --diff to preview changes")
    print("- Apply fixes incrementally with review")

    return 0


if __name__ == "__main__":
    sys.exit(main())
