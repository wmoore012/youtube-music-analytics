#!/usr / bin / env python3
"""
Verify that CI / CD setup is working correctly.
"""

from pathlib import Path
import subprocess
import sys


def run_command(command: str, description: str) -> bool:
    """Run a command and return True if successful."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command.split(), capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            print(f"✅ {description} - PASSED")
            return True
        else:
            print(f"❌ {description} - FAILED")
            print(f"Error: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - TIMEOUT")
        return False
    except Exception as e:
        print(f"💥 {description} - ERROR: {e}")
        return False


def check_file_exists(file_path: str, description: str) -> bool:
    """Check if a file exists."""
    if Path(file_path).exists():
        print(f"✅ {description} - EXISTS")
        return True
    else:
        print(f"❌ {description} - MISSING")
        return False


def main():
    """Main function to verify CI / CD setup."""
    print("🚀 Verifying CI / CD Setup")
    print("=" * 50)

    checks_passed = 0
    total_checks = 0

    # Check configuration files exist
    config_files = [
        (".pre - commit - config.yaml", "Pre - commit configuration"),
        ("pyproject.toml", "Project configuration"),
        (".github / workflows / ci.yml", "GitHub Actions CI workflow"),
    ]

    for file_path, description in config_files:
        total_checks += 1
        if check_file_exists(file_path, description):
            checks_passed += 1

    # Check tools are installed and working
    tool_checks = [
        ("black --version", "Black formatter"),
        ("isort --version", "isort import sorter"),
        ("flake8 --version", "flake8 linter"),
        ("mypy --version", "mypy type checker"),
        ("pre - commit --version", "pre - commit hooks"),
        ("pytest --version", "pytest testing framework"),
    ]

    for command, description in tool_checks:
        total_checks += 1
        if run_command(command, description):
            checks_passed += 1

    # Test pre - commit hooks are installed
    total_checks += 1
    if Path(".git / hooks / pre - commit").exists():
        print("✅ Pre - commit hooks installed - EXISTS")
        checks_passed += 1
    else:
        print("❌ Pre - commit hooks installed - MISSING")
        print("Run 'pre - commit install' to install hooks")

    # Summary
    print("\n" + "=" * 50)
    print(f"📊 CI / CD Setup Summary: {checks_passed}/{total_checks} checks passed")

    if checks_passed == total_checks:
        print("🎉 All CI / CD components are properly configured!")
        return True
    else:
        print("⚠️  Some CI / CD components need attention.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
