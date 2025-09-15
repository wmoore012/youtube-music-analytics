#!/usr/bin/env python3
"""
🔧 YouTube Analytics Setup Tool

Handles all setup tasks: environment configuration, database creation, and initial setup.
Consolidates setup functionality into one robust tool.

Usage:
    python tools/setup.py                  # Interactive setup
    python tools/setup.py --create-tables  # Create database tables
    python tools/setup.py --full-setup     # Complete automated setup
    python tools/setup.py --check          # Verify setup
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    """Main setup tool with comprehensive options."""
    parser = argparse.ArgumentParser(
        description="YouTube Analytics Setup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/setup.py                   # Interactive setup wizard
  python tools/setup.py --create-tables   # Create database tables only
  python tools/setup.py --full-setup      # Complete automated setup
  python tools/setup.py --check           # Verify current setup
  python tools/setup.py --env-only        # Environment setup only
        """,
    )

    # Setup options
    parser.add_argument("--create-tables", action="store_true", help="Create database tables")
    parser.add_argument("--full-setup", action="store_true", help="Complete automated setup (env + tables)")
    parser.add_argument("--env-only", action="store_true", help="Environment setup only")
    parser.add_argument("--check", action="store_true", help="Verify current setup")

    # Control options
    parser.add_argument("--force", action="store_true", help="Force overwrite existing setup")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    try:
        print("🔧 YouTube Analytics Setup Tool")
        print("=" * 50)

        # Import setup modules
        from tools.setup.create_tables import main as create_tables
        from tools.setup.setup_env import main as setup_env

        if args.check:
            print("🔍 Verifying setup...")
            return verify_setup()
        elif args.create_tables:
            print("📊 Creating database tables...")
            return create_tables()
        elif args.env_only:
            print("🌍 Setting up environment...")
            return setup_env()
        elif args.full_setup:
            print("🚀 Running complete setup...")

            # Step 1: Environment setup
            print("\n📋 Step 1: Environment Configuration")
            result1 = setup_env()
            if result1 != 0:
                print("❌ Environment setup failed")
                return result1

            # Step 2: Database tables
            print("\n📊 Step 2: Database Tables")
            result2 = create_tables()
            if result2 != 0:
                print("❌ Database setup failed")
                return result2

            print("\n✅ Complete setup finished successfully!")
            print("💡 Run 'python tools/etl.py' to start processing data")
            return 0
        else:
            # Interactive setup
            return interactive_setup()

    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure you're running from the project root directory")
        return 1
    except Exception as e:
        print(f"❌ Setup error: {e}")
        return 1


def verify_setup():
    """Verify that setup is complete and working."""
    print("🔍 Checking setup status...")

    issues = []

    # Check environment file
    if not os.path.exists(".env"):
        issues.append("❌ .env file missing")
    else:
        print("✅ .env file exists")

    # Check database connection
    try:
        from web.etl_helpers import get_engine

        engine = get_engine()
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        print("✅ Database connection working")
    except Exception as e:
        issues.append(f"❌ Database connection failed: {e}")

    # Check required tables
    try:
        from sqlalchemy import text

        from web.etl_helpers import get_engine

        engine = get_engine()

        required_tables = ["youtube_videos", "youtube_comments", "youtube_metrics"]
        with engine.connect() as conn:
            for table in required_tables:
                result = conn.execute(text(f"SHOW TABLES LIKE '{table}'"))
                if result.fetchone():
                    print(f"✅ Table {table} exists")
                else:
                    issues.append(f"❌ Table {table} missing")
    except Exception as e:
        issues.append(f"❌ Table check failed: {e}")

    if issues:
        print(f"\n⚠️  Setup Issues Found ({len(issues)}):")
        for issue in issues:
            print(f"   {issue}")
        print("\n💡 Run 'python tools/setup.py --full-setup' to fix issues")
        return 1
    else:
        print("\n🎉 Setup verification passed! System ready to use.")
        return 0


def interactive_setup():
    """Interactive setup wizard."""
    print("🧙 Interactive Setup Wizard")
    print("=" * 30)

    print("This will guide you through setting up YouTube Analytics.")
    print("You can also use --full-setup for automated setup.")

    response = input("\nProceed with interactive setup? (y/N): ")
    if response.lower() != "y":
        print("Setup cancelled.")
        return 0

    # For now, just run full setup
    # In the future, this could ask questions about configuration
    print("\n🚀 Running automated setup...")
    return main_with_args(["--full-setup"])


def main_with_args(args_list):
    """Helper to run main with specific arguments."""
    original_argv = sys.argv
    try:
        sys.argv = ["setup.py"] + args_list
        return main()
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    sys.exit(main())
