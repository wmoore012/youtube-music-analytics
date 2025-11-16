#!/usr/bin/env python3
"""
🔧 Dependency Checker-Check and auto-install required packages

Run this before using the 🚀 play button to make sure everything is ready.
Now with AUTO-INSTALL capability!

Usage:
    python 🔧_CHECK_DEPENDENCIES.py
    python 🔧_CHECK_DEPENDENCIES.py --auto-install
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path to find our modules
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

try:
    from src.youtubeviz.auto_install import AutoInstaller, check_available, ensure

    AUTO_INSTALL_AVAILABLE = True
except ImportError:
    AUTO_INSTALL_AVAILABLE = False


def check_dependency(package_name, import_name=None, auto_install=False):
    """Check if a package is installed and importable, optionally auto-installing."""
    if import_name is None:
        import_name = package_name

    # First check if it's available
    if AUTO_INSTALL_AVAILABLE and check_available(package_name, import_name):
        print(f"✅ {package_name}")
        return True

    # Try regular import
    try:
        __import__(import_name)
        print(f"✅ {package_name}")
        return True
    except ImportError:
        if auto_install and AUTO_INSTALL_AVAILABLE:
            print(f"📦 {package_name} - Installing...")
            module = ensure(package_name, import_name)
            if module:
                print(f"✅ {package_name} - INSTALLED")
                return True
            else:
                print(f"❌ {package_name} - INSTALL FAILED")
                return False
        else:
            print(f"❌ {package_name} - MISSING")
            return False


def main():
    """Check all required dependencies with optional auto-install."""
    parser = argparse.ArgumentParser(description="Check and install MusicScope™ dependencies")
    parser.add_argument("--auto-install", action="store_true", help="Automatically install missing packages")
    args = parser.parse_args()

    print("🔧" + "=" * 60)
    print("�  MusicScope™ Dependency Checker")
    if args.auto_install and AUTO_INSTALL_AVAILABLE:
        print("🚀 AUTO-INSTALL MODE ENABLED")
    print("🔧" + "=" * 60)
    print()

    if not AUTO_INSTALL_AVAILABLE and args.auto_install:
        print("⚠️  Auto-install not available (missing auto_install module)")
        print("    Falling back to check-only mode")
        print()

    auto_install = args.auto_install and AUTO_INSTALL_AVAILABLE

    # Core Python packages (should always be available)
    print("📦 Core Python packages:")
    core_deps = [
        ("sys", "sys"),
        ("os", "os"),
        ("json", "json"),
        ("datetime", "datetime"),
        ("pathlib", "pathlib"),
    ]

    core_ok = all(check_dependency(pkg, imp, auto_install) for pkg, imp in core_deps)
    print()

    # Data science packages
    print("📊 Data science packages:")
    data_deps = [
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("scipy", "scipy"),
    ]

    data_ok = all(check_dependency(pkg, imp, auto_install) for pkg, imp in data_deps)
    print()

    # Visualization packages
    print("📈 Visualization packages:")
    viz_deps = [
        ("plotly", "plotly"),
        ("matplotlib", "matplotlib"),
        ("seaborn", "seaborn"),
    ]

    viz_ok = all(check_dependency(pkg, imp, auto_install) for pkg, imp in viz_deps)
    print()

    # Database packages
    print("🗄️ Database packages:")
    db_deps = [
        ("sqlalchemy", "sqlalchemy"),
        ("pymysql", "pymysql"),
    ]

    db_ok = all(check_dependency(pkg, imp, auto_install) for pkg, imp in db_deps)
    print()

    # Notebook packages
    print("📓 Notebook packages:")
    nb_deps = [
        ("nbconvert", "nbconvert"),
        ("ipywidgets", "ipywidgets"),
        ("tqdm", "tqdm"),
    ]

    nb_ok = all(check_dependency(pkg, imp, auto_install) for pkg, imp in nb_deps)
    print()

    # Optional enhancement packages
    print("✨ Optional enhancements:")
    optional_deps = [
        ("rich", "rich"),
        ("psutil", "psutil"),
        ("memory-profiler", "memory_profiler"),
    ]

    optional_ok = all(check_dependency(pkg, imp, auto_install) for pkg, imp in optional_deps)
    print()

    # Overall status
    essential_ok = core_ok and data_ok and viz_ok and db_ok and nb_ok
    __all_ok = essential_ok and optional_ok  # noqa: F841

    if essential_ok:
        print("🎉" + "=" * 60)
        print("✅ ESSENTIAL DEPENDENCIES READY!")
        if not optional_ok:
            print("⚠️  Some optional packages missing (not critical)")
        print("🎉" + "=" * 60)
        print()
        print("🚀 You can now run: python 🚀_RUN_NOTEBOOK_CREATION.py")
        print()

        if AUTO_INSTALL_AVAILABLE:
            print("💡 Pro tip: Run with --auto-install to install missing packages automatically")
            print("   python 🔧_CHECK_DEPENDENCIES.py --auto-install")
        print()
    else:
        print("🚨" + "=" * 60)
        print("❌ MISSING ESSENTIAL DEPENDENCIES!")
        print("🚨" + "=" * 60)
        print()

        if AUTO_INSTALL_AVAILABLE and not auto_install:
            print("🚀 Quick fix: Run with auto-install")
            print("   python 🔧_CHECK_DEPENDENCIES.py --auto-install")
            print()

        print("🔧 Manual installation:")
        print("   pip install pandas numpy scipy plotly matplotlib seaborn")
        print("   pip install sqlalchemy pymysql nbconvert ipywidgets tqdm")
        print()
        print("   Or install everything at once:")
        print(
            "   pip install pandas numpy scipy plotly matplotlib seaborn"
            " sqlalchemy pymysql nbconvert ipywidgets tqdm rich psutil memory-profiler"
        )
        print()
        sys.exit(1)


if __name__ == "__main__":
    main()
