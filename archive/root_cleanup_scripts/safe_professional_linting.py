#!/usr/bin/env python3
"""
Safe, professional linting solution that avoids the dangerous patterns
identified in the assessment. Uses proper tooling instead of regex hacks.
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
        print(f"Error: {e.stderr}")
        return e


def verify_tests_pass():
    """Ensure tests pass before making any changes"""
    print("🧪 Verifying tests pass before making changes...")
    result = run_command("PYTHONPATH=. python -m pytest -q", check=False)
    if result.returncode != 0:
        print("❌ Tests are failing-aborting to avoid breaking working code")
        print("Fix tests first, then run linting")
        return False
    print("✅ Tests pass-safe to proceed")
    return True


def create_ruff_baseline():
    """Create a proper baseline for gradual improvement"""
    print("📊 Creating ruff baseline for gradual improvement...")

    # Get current violations in JSON format
    result = run_command("ruff . --output-format=json", check=False)

    if result.returncode != 0:
        violations = json.loads(result.stdout) if result.stdout else []

        # Create baseline config that ignores current violations by file
        baseline_config = {"per-file-ignores": {}}

        for violation in violations:
            filename = violation.get("filename", "")
            code = violation.get("code", "")

            if filename and code:
                if filename not in baseline_config["per-file-ignores"]:
                    baseline_config["per-file-ignores"][filename] = []

                if code not in baseline_config["per-file-ignores"][filename]:
                    baseline_config["per-file-ignores"][filename].append(code)

        # Write baseline to file for reference
        with open(".ruff-baseline.json", "w") as f:
            json.dump(baseline_config, f, indent=2)

        print(f"  📝 Created baseline with {len(violations)} violations")
        print("  ℹ️  New code will be held to full standards")
        return len(violations)
    else:
        print("  ✅ No violations found!")
        return 0


def apply_safe_fixes_only():
    """Apply only the safest automatic fixes"""
    print("🔧 Applying ONLY safe automatic fixes...")

    # 1. Format with black (safe)
    print("  🎨 Formatting with black...")
    run_command("black --line-length=120 .")

    # 2. Fix imports with ruff (safe)
    print("  📦 Fixing import order...")
    run_command("ruff --select I --fix .")

    # 3. Remove unused imports ONLY (not variables-those need manual review)
    print("  🗑️  Removing unused imports (safe)...")
    run_command("ruff --select F401 --fix .")

    # 4. Fix whitespace issues (safe)
    print("  🧹 Fixing whitespace...")
    run_command("ruff --select W291,W292,W293,E302,E303,E305 --fix .")

    print("  ✅ Safe fixes applied")


def audit_for_damage():
    """Check for any damage from previous scripts"""
    print("🔍 Auditing for damage from previous scripts...")

    # Look for double underscore variables (from final_27_errors.py)
    result = run_command('grep -r "__.*=" . --include="*.py"', check=False)
    if result.returncode == 0 and result.stdout.strip():
        print("  ⚠️  Found potential double-underscore variables:")
        print(result.stdout[:500] + "..." if len(result.stdout) > 500 else result.stdout)

    # Look for excessive # noqa comments
    result = run_command('grep -r "# noqa" . --include="*.py" | wc -l', check=False)
    if result.returncode == 0:
        noqa_count = int(result.stdout.strip())
        if noqa_count > 20:
            print(f"  ⚠️  Found {noqa_count} # noqa comments-may be excessive")

    # Check for broken with statements
    result = run_command('grep -r "with.*as _" . --include="*.py"', check=False)
    if result.returncode == 0 and result.stdout.strip():
        print("  ⚠️  Found potential broken with statements:")
        print(result.stdout[:300] + "..." if len(result.stdout) > 300 else result.stdout)


def setup_proper_tooling():
    """Set up proper modern tooling"""
    print("🛠️  Setting up proper tooling...")

    # Install if needed
    tools = ["ruff", "black"]
    for tool in tools:
        result = run_command(f"pip show {tool}", check=False, capture_output=True)
        if result.returncode != 0:
            print(f"  Installing {tool}...")
            run_command(f"pip install {tool}")

    # Update pyproject.toml with safe ruff config
    ruff_config = """
[tool.ruff]
# Start with basic checks, expand gradually
select = [
    "E",   # pycodestyle errors
    "F",   # pyflakes
    "W",   # pycodestyle warnings
    "I",   # isort
]

# Ignore current violations (baseline approach)
ignore = [
    "E501",  # Line too long (handled by black)
]

# Safe automatic fixes only
fixable = ["F401", "I", "W291", "W292", "W293", "E302", "E303", "E305"]
unfixable = ["F841"]  # Don't auto-remove unused variables

line-length = 120
target-version = "py38"

[tool.ruff.per-file-ignores]
# Tests can be more flexible
"tests/**/*.py" = ["F401", "F841"]
# Scripts can use print statements
"scripts/**/*.py" = ["T201"]
"""

    # Only add if not already present
    pyproject_path = Path("pyproject.toml")
    if pyproject_path.exists():
        content = pyproject_path.read_text()
        if "[tool.ruff]" not in content:
            with open("pyproject.toml", "a") as f:
                f.write(ruff_config)
            print("  ✅ Added safe ruff config")
    else:
        pyproject_path.write_text(ruff_config)
        print("  ✅ Created pyproject.toml with safe ruff config")


def main():
    print("🛡️  SAFE PROFESSIONAL LINTING")
    print("=" * 40)
    print("Avoiding dangerous regex hacks and mass # noqa insertion")
    print()

    # Step 1: Verify we're starting from a good state
    if not verify_tests_pass():
        return 1

    # Step 2: Audit for existing damage
    audit_for_damage()

    # Step 3: Set up proper tooling
    setup_proper_tooling()

    # Step 4: Create baseline for gradual improvement
    baseline_count = create_ruff_baseline()

    # Step 5: Apply only safe fixes
    apply_safe_fixes_only()

    # Step 6: Verify tests still pass
    print("\n🧪 Verifying tests still pass after changes...")
    if not verify_tests_pass():
        print("❌ Tests broken by changes-need manual review")
        return 1

    # Step 7: Show results
    print("\n📊 Final status:")
    result = run_command("ruff . --statistics", check=False)
    if result.returncode == 0:
        print("🎉 Zero ruff violations!")
    else:
        print("📈 Remaining violations (tracked in baseline):")
        print(result.stdout)

    print(f"\n✅ SAFE APPROACH COMPLETE")
    print(f"📊 Started with {baseline_count} violations")
    print("🛡️  Applied only safe fixes")
    print("📝 Created baseline for gradual improvement")
    print("🧪 Tests verified at each step")
    print("\nNext steps:")
    print("- Review .ruff-baseline.json for improvement opportunities")
    print("- Fix violations gradually with proper code review")
    print("- Never use mass # noqa insertion")

    return 0


if __name__ == "__main__":
    sys.exit(main())
