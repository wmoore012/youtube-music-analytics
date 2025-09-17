#!/usr/bin/env python3
"""
🔧 Dependency Checker - Check if all required packages are installed

Run this before using the 🚀 play button to make sure everything is ready.

Usage:
    python 🔧_CHECK_DEPENDENCIES.py
"""

import sys
from pathlib import Path


def check_dependency(package_name, import_name=None):
    """Check if a package is installed and importable."""
    if import_name is None:
        import_name = package_name

    try:
        __import__(import_name)
        print(f"✅ {package_name}")
        return True
    except ImportError:
        print(f"❌ {package_name} - MISSING")
        return False


def main():
    """Check all required dependencies."""
    print("🔧" + "=" * 50)
    print("🔍 MusicScope™ Dependency Checker")
    print("🔧" + "=" * 50)
    print()

    # Core Python packages
    print("📦 Core Python packages:")
    core_deps = [
        ("sys", "sys"),
        ("os", "os"),
        ("json", "json"),
        ("datetime", "datetime"),
        ("pathlib", "pathlib"),
    ]

    core_ok = all(check_dependency(pkg, imp) for pkg, imp in core_deps)
    print()

    # Data science packages
    print("📊 Data science packages:")
    data_deps = [
        ("pandas", "pandas"),
        ("numpy", "numpy"),
        ("scipy", "scipy"),
    ]

    data_ok = all(check_dependency(pkg, imp) for pkg, imp in data_deps)
    print()

    # Visualization packages
    print("📈 Visualization packages:")
    viz_deps = [
        ("plotly", "plotly"),
        ("plotly.graph_objects", "plotly.graph_objects"),
        ("plotly.express", "plotly.express"),
    ]

    viz_ok = all(check_dependency(pkg, imp) for pkg, imp in viz_deps)
    print()

    # Database packages
    print("🗄️ Database packages:")
    db_deps = [
        ("sqlalchemy", "sqlalchemy"),
        ("pymysql", "pymysql"),
    ]

    db_ok = all(check_dependency(pkg, imp) for pkg, imp in db_deps)
    print()

    # Notebook packages
    print("📓 Notebook packages:")
    nb_deps = [
        ("nbconvert", "nbconvert"),
        ("jupyter", "jupyter"),
    ]

    nb_ok = all(check_dependency(pkg, imp) for pkg, imp in nb_deps)
    print()

    # Overall status
    all_ok = core_ok and data_ok and viz_ok and db_ok and nb_ok

    if all_ok:
        print("🎉" + "=" * 50)
        print("✅ ALL DEPENDENCIES READY!")
        print("🎉" + "=" * 50)
        print()
        print("🚀 You can now run: python 🚀_RUN_NOTEBOOK_CREATION.py")
        print()
    else:
        print("🚨" + "=" * 50)
        print("❌ MISSING DEPENDENCIES!")
        print("🚨" + "=" * 50)
        print()
        print("🔧 To install missing packages:")
        print("   pip install pandas numpy scipy plotly sqlalchemy pymysql nbconvert jupyter")
        print()
        print("   Or if you have a requirements.txt:")
        print("   pip install -r requirements.txt")
        print()
        sys.exit(1)


if __name__ == "__main__":
    main()
