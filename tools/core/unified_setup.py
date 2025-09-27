#!/usr / bin / env python3
"""
🔧 Unified YouTube Analytics Setup Tool

Consolidates all setup functionality into a single, robust tool using standardized patterns.
Handles environment configuration, database creation, validation, and system initialization.

Usage:
    python tools / core / unified_setup.py                    # Interactive setup
    python tools / core / unified_setup.py --create - tables    # Create database tables
    python tools / core / unified_setup.py --full - setup       # Complete automated setup
    python tools / core / unified_setup.py --check            # Verify setup
    python tools / core / unified_setup.py --env - only         # Environment setup only
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tools.shared.common import (
    ConfigurationError,
    ExecutionError,
    ToolBase,
    ToolConfig,
    ValidationError,
    register_tool,
)


class SystemSetup(ToolBase):
    """
    Unified system setup tool that consolidates all setup functionality.

    This tool handles:
    - Environment configuration (.env file setup)
    - Database schema creation and validation
    - System dependency verification
    - Configuration validation and testing
    - Initial data setup and validation
    """

    def __init__(self):
        super().__init__(name="unified - setup", version="1.0.0")

        # Register this tool in the global registry
        register_tool(self.get_tool_config())

        # Setup state tracking
        self.setup_state = {
            "env_configured": False,
            "database_connected": False,
            "tables_created": False,
            "validation_passed": False,
        }

    def get_required_environment_vars(self) -> List[str]:
        """Return list of required environment variables."""
        # These are required for database operations, but we'll create them during setup
        return []

    def get_tool_config(self) -> ToolConfig:
        """Return tool configuration metadata."""
        return ToolConfig(
            name="unified - setup",
            version="1.0.0",
            description="Unified YouTube Analytics system setup tool",
            dependencies=[
                "python>=3.8",
                "pymysql",
                "sqlalchemy",
                "python - dotenv",
            ],
            environment_vars=[
                "YOUTUBE_API_KEY",
                "DB_HOST",
                "DB_PORT",
                "DB_USER",
                "DB_PASS",
                "DB_NAME",
            ],
            usage_examples=[
                "python tools / core / unified_setup.py --full - setup",
                "python tools / core / unified_setup.py --check",
                "python tools / core / unified_setup.py --create - tables",
            ],
            category="core",
        )

    def run(self) -> None:
        """Main execution method - should not be called directly, use specific setup methods."""
        self.log_progress("Use specific setup methods like setup_environment() or create_tables()")

    def setup_environment(self, interactive: bool = True, force: bool = False) -> bool:
        """
        Set up the .env file with required configuration.

        Args:
            interactive: Whether to prompt user for input
            force: Whether to overwrite existing configuration

        Returns:
            True if setup successful, False otherwise
        """
        self.log_progress("Starting environment setup")

        try:
            env_file = Path(".env")

            if env_file.exists() and not force:
                if interactive:
                    self.log_progress("✅ .env file already exists")
                    self._display_current_config()

                    response = input("\n❓ Do you want to update the configuration? (y / N): ").lower().strip()
                    if response != "y":
                        self.log_progress("Keeping existing configuration")
                        self.setup_state["env_configured"] = True
                        return True
                else:
                    self.log_progress("✅ .env file exists, skipping environment setup")
                    self.setup_state["env_configured"] = True
                    return True

            if interactive:
                return self._interactive_env_setup()
            else:
                return self._automated_env_setup()

        except Exception as e:
            self.handle_error(e, "environment setup")
            return False

    def _display_current_config(self) -> None:
        """Display current .env configuration (safely)."""
        try:
            with open(".env", "r") as f:
                content = f.read()

            # Mask sensitive values
            lines = content.split("\n")
            safe_lines = []

            for line in lines:
                if "=" in line and not line.strip().startswith("#"):
                    key, value = line.split("=", 1)
                    if any(sensitive in key.upper() for sensitive in ["KEY", "PASS", "SECRET", "TOKEN"]):
                        safe_lines.append(f"{key}=***MASKED***")
                    else:
                        safe_lines.append(line)
                else:
                    safe_lines.append(line)

            self.log_progress("📝 Current configuration:")
            print("\n".join(safe_lines))

        except Exception as e:
            self.log_progress(f"Could not display config: {e}", level="WARNING")

    def _interactive_env_setup(self) -> bool:
        """Interactive environment setup with user prompts."""
        self.log_progress("🎬 YouTube ETL Pipeline - Environment Setup")
        self.log_progress("=" * 50)

        print("\n📝 Let's set up your .env file:")
        print("You'll need:")
        print("1. YouTube Data API v3 key (from Google Cloud Console)")
        print("2. MySQL database credentials")
        print("3. Artist channel information (pre - configured)")
        print()

        # Get YouTube API key
        api_key = input("🔑 Enter your YouTube Data API v3 key: ").strip()
        if not api_key:
            raise ConfigurationError("YouTube API key is required!")

        # Get database configuration
        print("\n🗄️  Database Configuration:")
        db_host = input("Host (default: 127.0.0.1): ").strip() or "127.0.0.1"
        db_port = input("Port (default: 3306): ").strip() or "3306"
        db_user = input("Username: ").strip()
        if not db_user:
            raise ConfigurationError("Database username is required!")

        db_pass = input("Password: ").strip()
        db_name = input("Database name (default: icatalog): ").strip() or "icatalog"

        # Create .env content
        env_content = self._generate_env_content(api_key, db_host, db_port, db_user, db_pass, db_name)

        # Write to .env file
        with open(".env", "w") as f:
            f.write(env_content)

        self.log_progress("✅ .env file created successfully!")
        self.setup_state["env_configured"] = True

        # Offer to test the configuration
        test_config = input("\n❓ Do you want to test the configuration now? (y / N): ").lower().strip()
        if test_config == "y":
            return self.validate_configuration()

        return True

    def _automated_env_setup(self) -> bool:
        """Automated environment setup using environment variables or defaults."""
        self.log_progress("Setting up environment automatically")

        # Check if required variables are available in environment
        required_vars = ["YOUTUBE_API_KEY", "DB_USER"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ConfigurationError(f"Automated setup requires these environment variables: {', '.join(missing_vars)}")

        # Get values from environment or use defaults
        api_key = os.getenv("YOUTUBE_API_KEY")
        db_host = os.getenv("DB_HOST", "127.0.0.1")
        db_port = os.getenv("DB_PORT", "3306")
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS", "")
        db_name = os.getenv("DB_NAME", "icatalog")

        # Create .env content
        env_content = self._generate_env_content(api_key, db_host, db_port, db_user, db_pass, db_name)

        # Write to .env file
        with open(".env", "w") as f:
            f.write(env_content)

        self.log_progress("✅ .env file created from environment variables")
        self.setup_state["env_configured"] = True
        return True

    def _generate_env_content(
        self, api_key: str, db_host: str, db_port: str, db_user: str, db_pass: str, db_name: str
    ) -> str:
        """Generate .env file content with all required configuration."""
        return f"""# YouTube Data API Configuration
# Get your API key from: https://console.developers.google.com/
YOUTUBE_API_KEY={api_key}

# Database Configuration
DB_HOST={db_host}
DB_PORT={db_port}
DB_USER={db_user}
DB_PASS={db_pass}
DB_NAME={db_name}
DB_NAME_PUBLIC={db_name}_public

# Artist YouTube Channels
# BicFizzle
BICDIZZLE_CHANNEL_ID=UCZcMK - f8loeOkk3GX3hsmtQ
BICDIZZLE_SPOTIFY_URL=https://open.spotify.com / artist / 55zZKMiLQNwu6unkKc8J4y

# Cobrah
COBRAH_CHANNEL_ID=@COBRAH
COBRAH_SPOTIFY_URL=https://open.spotify.com / artist / 1AHswQqsDNmu1xaE8KpBne

# Corook
COROOK_CHANNEL_ID=@hicorook
COROOK_SPOTIFY_URL=https://open.spotify.com / artist / 1rNVlQNJSIESCd5mixdqMt

# Enchanting
ENCHANTING_CHANNEL_ID=@Enchanting
ENCHANTING_SPOTIFY_URL=https://open.spotify.com / artist / 26XGM4cZDcTgrXo1nis5HT

# Flyana Boss
FLYANA_BOSS_CHANNEL_ID=@FlyanaBoss
FLYANA_BOSS_SPOTIFY_URL=https://open.spotify.com / artist / 0CLW5934vy2XusynS1px1S

# YouTube API Quota Settings
YOUTUBE_QUOTA_LIMIT=10000
YOUTUBE_MAX_RETRIES=3
YOUTUBE_BATCH_SIZE=50

# ETL Configuration
ETL_MAX_VIDEOS_PER_ARTIST=100
ETL_CACHE_THRESHOLD_HOURS=24
ETL_DEVELOPMENT_MODE=false

# Data Retention (YouTube ToS Compliance)
YOUTUBE_DATA_RETENTION_DAYS=30

# Logging
LOG_LEVEL=INFO
LOG_FILE=youtube_etl.log
"""

    def create_tables(self, force: bool = False) -> bool:
        """
        Create all required database tables.

        Args:
            force: Whether to drop existing tables first

        Returns:
            True if successful, False otherwise
        """
        self.log_progress("Creating database tables")

        try:
            # Ensure database exists first
            if not self._ensure_database_exists():
                return False

            # Import database creation functionality
            from tools.core.create_tables import create_youtube_tables

            # Create tables
            success = create_youtube_tables()

            if success:
                self.log_progress("✅ Database tables created successfully")
                self.setup_state["tables_created"] = True
                return True
            else:
                self.log_progress("❌ Failed to create database tables", level="ERROR")
                return False

        except Exception as e:
            self.handle_error(e, "table creation")
            return False

    def _ensure_database_exists(self) -> bool:
        """Ensure the target database exists."""
        try:
            from dotenv import load_dotenv
            import pymysql

            # Load environment variables
            load_dotenv()

            host = os.getenv("DB_HOST", "127.0.0.1")
            port = int(os.getenv("DB_PORT", "3306"))
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASS", "")
            db_name = os.getenv("DB_NAME", "icatalog")

            if not user:
                raise ConfigurationError("DB_USER environment variable is required")

            # Connect without specifying database
            conn = pymysql.connect(host=host, port=port, user=user, password=password)

            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )

            conn.close()
            self.log_progress(f"✅ Ensured database exists: {db_name}")
            self.setup_state["database_connected"] = True
            return True

        except Exception as e:
            self.handle_error(e, "database creation")
            return False

    def validate_configuration(self) -> bool:  # noqa: C901
        """
        Validate the current system configuration.

        Returns:
            True if validation passes, False otherwise
        """
        self.log_progress("🔍 Validating system configuration")

        issues = []

        try:
            # Check .env file exists
            if not Path(".env").exists():
                issues.append("❌ .env file missing")
            else:
                self.log_progress("✅ .env file exists")

            # Load environment variables
            from dotenv import load_dotenv

            load_dotenv()

            # Check required environment variables
            required_vars = ["YOUTUBE_API_KEY", "DB_HOST", "DB_USER", "DB_NAME"]

            for var in required_vars:
                if not os.getenv(var):
                    issues.append(f"❌ Environment variable {var} missing")
                else:
                    self.log_progress(f"✅ Environment variable {var} set")

            # Test database connection
            try:
                from web.etl_helpers import get_engine

                engine = get_engine()
                with engine.connect() as conn:
                    conn.execute("SELECT 1")
                self.log_progress("✅ Database connection working")
                self.setup_state["database_connected"] = True
            except Exception as e:
                issues.append(f"❌ Database connection failed: {e}")

            # Check required tables
            if self.setup_state["database_connected"]:
                try:
                    from sqlalchemy import text

                    from web.etl_helpers import get_engine

                    engine = get_engine()
                    required_tables = ["youtube_videos", "youtube_comments", "youtube_metrics", "youtube_videos_raw"]

                    with engine.connect() as conn:
                        for table in required_tables:
                            result = conn.execute(text(f"SHOW TABLES LIKE '{table}'"))
                            if result.fetchone():
                                self.log_progress(f"✅ Table {table} exists")
                            else:
                                issues.append(f"❌ Table {table} missing")

                    if not issues or not any("Table" in issue for issue in issues):
                        self.setup_state["tables_created"] = True

                except Exception as e:
                    issues.append(f"❌ Table validation failed: {e}")

            # Test YouTube API (optional)
            try:
                api_key = os.getenv("YOUTUBE_API_KEY")
                if api_key:
                    # Simple API test - just check if key format is valid
                    if len(api_key) > 30:  # Basic sanity check
                        self.log_progress("✅ YouTube API key format looks valid")
                    else:
                        issues.append("⚠️  YouTube API key format may be invalid")
            except Exception as e:
                issues.append(f"⚠️  YouTube API validation failed: {e}")

            # Report results
            if issues:
                self.log_progress(f"⚠️  Configuration Issues Found ({len(issues)}):", level="WARNING")
                for issue in issues:
                    print(f"   {issue}")
                self.log_progress("💡 Run setup commands to fix issues")
                return False
            else:
                self.log_progress("🎉 Configuration validation passed! System ready to use.")
                self.setup_state["validation_passed"] = True
                return True

        except Exception as e:
            self.handle_error(e, "configuration validation")
            return False

    def full_setup(self, interactive: bool = True, force: bool = False) -> bool:
        """
        Run complete system setup.

        Args:
            interactive: Whether to use interactive prompts
            force: Whether to force overwrite existing setup

        Returns:
            True if successful, False otherwise
        """
        self.log_progress("🚀 Running complete system setup")

        try:
            # Step 1: Environment setup
            self.log_progress("📋 Step 1: Environment Configuration")
            if not self.setup_environment(interactive=interactive, force=force):
                self.log_progress("❌ Environment setup failed", level="ERROR")
                return False

            # Step 2: Database tables
            self.log_progress("📊 Step 2: Database Tables")
            if not self.create_tables(force=force):
                self.log_progress("❌ Database setup failed", level="ERROR")
                return False

            # Step 3: Validation
            self.log_progress("🔍 Step 3: System Validation")
            if not self.validate_configuration():
                self.log_progress("❌ System validation failed", level="ERROR")
                return False

            self.log_progress("✅ Complete setup finished successfully!")
            self.log_progress("💡 Next steps:")
            print("   • Run 'python tools / core / etl.py --help' to see ETL options")
            print("   • Check 'notebooks/' directory for analysis examples")
            print("   • Review 'docs/' for detailed documentation")

            return True

        except Exception as e:
            self.handle_error(e, "full setup")
            return False

    def get_setup_status(self) -> Dict[str, Any]:
        """Get current setup status."""
        return {
            "setup_state": self.setup_state.copy(),
            "env_file_exists": Path(".env").exists(),
            "timestamp": self.get_config_value("timestamp", "unknown"),
        }

    def cleanup_resources(self) -> None:
        """Clean up any resources used during setup."""
        # No persistent resources to clean up
        pass


def main():
    """Main entry point for the unified setup tool."""
    parser = argparse.ArgumentParser(
        description="Unified YouTube Analytics Setup Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools / core / unified_setup.py                    # Interactive setup wizard
  python tools / core / unified_setup.py --create - tables    # Create database tables only
  python tools / core / unified_setup.py --full - setup       # Complete automated setup
  python tools / core / unified_setup.py --check            # Verify current setup
  python tools / core / unified_setup.py --env - only         # Environment setup only
        """,
    )

    # Setup options
    parser.add_argument("--create - tables", action="store_true", help="Create database tables")
    parser.add_argument("--full - setup", action="store_true", help="Complete automated setup (env + tables)")
    parser.add_argument("--env - only", action="store_true", help="Environment setup only")
    parser.add_argument("--check", action="store_true", help="Verify current setup")
    parser.add_argument("--status", action="store_true", help="Show setup status")

    # Control options
    parser.add_argument("--force", action="store_true", help="Force overwrite existing setup")
    parser.add_argument("--non - interactive", action="store_true", help="Run in non - interactive mode")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Create setup tool instance
    with SystemSetup() as setup_tool:
        try:
            if args.status:
                status = setup_tool.get_setup_status()
                print(json.dumps(status, indent=2))
                return 0
            elif args.check:
                success = setup_tool.validate_configuration()
                return 0 if success else 1
            elif args.create_tables:
                success = setup_tool.create_tables(force=args.force)
                return 0 if success else 1
            elif args.env_only:
                success = setup_tool.setup_environment(interactive=not args.non_interactive, force=args.force)
                return 0 if success else 1
            elif args.full_setup:
                success = setup_tool.full_setup(interactive=not args.non_interactive, force=args.force)
                return 0 if success else 1
            else:
                # Interactive setup by default
                success = setup_tool.full_setup(interactive=True, force=args.force)
                return 0 if success else 1

        except KeyboardInterrupt:
            setup_tool.log_progress("Setup cancelled by user")
            return 1
        except Exception as e:
            setup_tool.handle_error(e, "main execution")
            return 1


if __name__ == "__main__":
    sys.exit(main())
