"""
Tests for tools / core / unified_setup.py - unified system setup tool.

This test suite validates:
- SystemSetup class functionality
- Environment configuration setup
- Database table creation
- Configuration validation
- Full setup workflow
- Error handling and recovery
"""

import json
import os
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, mock_open, patch

import pytest

from tools.core.unified_setup import SystemSetup, main


class TestSystemSetup:
    """Test SystemSetup class functionality."""

    def setup_method(self):
        """Set up fresh setup tool for each test."""
        self.setup_tool = SystemSetup()

    def test_tool_initialization(self):
        """Test basic tool initialization."""
        assert self.setup_tool.name == "unified - setup"
        assert self.setup_tool.version == "1.0.0"
        assert self.setup_tool.logger is not None

        # Check initial setup state
        expected_state = {
            "env_configured": False,
            "database_connected": False,
            "tables_created": False,
            "validation_passed": False,
        }
        assert self.setup_tool.setup_state == expected_state

    def test_get_tool_config(self):
        """Test tool configuration metadata."""
        config = self.setup_tool.get_tool_config()

        assert config.name == "unified - setup"
        assert config.version == "1.0.0"
        assert config.category == "core"
        assert "python>=3.8" in config.dependencies
        assert "YOUTUBE_API_KEY" in config.environment_vars
        assert "DB_HOST" in config.environment_vars

    def test_get_required_environment_vars(self):
        """Test required environment variables."""
        # Setup tool doesn't require vars initially (creates them during setup)
        required_vars = self.setup_tool.get_required_environment_vars()
        assert required_vars == []


class TestEnvironmentSetup:
    """Test environment configuration setup."""

    def setup_method(self):
        """Set up fresh setup tool for each test."""
        self.setup_tool = SystemSetup()

    def test_generate_env_content(self):
        """Test .env file content generation."""
        content = self.setup_tool._generate_env_content(
            api_key="test_api_key",
            db_host="localhost",
            db_port="3306",
            db_user="testuser",
            db_pass="testpass",
            db_name="testdb",
        )

        assert "YOUTUBE_API_KEY=test_api_key" in content
        assert "DB_HOST=localhost" in content
        assert "DB_USER=testuser" in content
        assert "DB_PASS=testpass" in content
        assert "DB_NAME=testdb" in content
        assert "BICDIZZLE_CHANNEL_ID=" in content  # Pre - configured channels
        assert "YOUTUBE_QUOTA_LIMIT=10000" in content

    @patch("builtins.input")
    @patch("builtins.open", new_callable=mock_open)
    @patch("pathlib.Path.exists")
    def test_interactive_env_setup_new_file(self, mock_exists, mock_file, mock_input):
        """Test interactive environment setup for new .env file."""
        # Mock .env file doesn't exist
        mock_exists.return_value = False

        # Mock user inputs
        mock_input.side_effect = [
            "test_youtube_api_key",  # YouTube API key
            "127.0.0.1",  # DB host (default)
            "3306",  # DB port (default)
            "testuser",  # DB user
            "testpass",  # DB password
            "testdb",  # DB name
            "n",  # Don't test config
        ]

        result = self.setup_tool._interactive_env_setup()

        assert result is True
        assert self.setup_tool.setup_state["env_configured"] is True
        mock_file.assert_called_once_with(".env", "w")

    @patch("builtins.input")
    @patch("pathlib.Path.exists")
    def test_interactive_env_setup_missing_api_key(self, mock_exists, mock_input):
        """Test interactive setup fails with missing API key."""
        mock_exists.return_value = False
        mock_input.side_effect = [""]  # Empty API key

        with pytest.raises(Exception):  # Should raise ConfigurationError
            self.setup_tool._interactive_env_setup()

    @patch.dict(os.environ, {"YOUTUBE_API_KEY": "test_key", "DB_USER": "testuser", "DB_PASS": "testpass"})
    @patch("builtins.open", new_callable=mock_open)
    def test_automated_env_setup_success(self, mock_file):
        """Test automated environment setup with environment variables."""
        result = self.setup_tool._automated_env_setup()

        assert result is True
        assert self.setup_tool.setup_state["env_configured"] is True
        mock_file.assert_called_once_with(".env", "w")

    def test_automated_env_setup_missing_vars(self):
        """Test automated setup fails with missing required variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(Exception):  # Should raise ConfigurationError
                self.setup_tool._automated_env_setup()

    @patch("pathlib.Path.exists")
    @patch("builtins.input")
    def test_setup_environment_existing_file_keep(self, mock_input, mock_exists):
        """Test setup with existing .env file - user chooses to keep."""
        mock_exists.return_value = True
        mock_input.return_value = "n"  # Don't update

        with patch.object(self.setup_tool, "_display_current_config"):
            result = self.setup_tool.setup_environment(interactive=True)

        assert result is True
        assert self.setup_tool.setup_state["env_configured"] is True

    @patch("pathlib.Path.exists")
    def test_setup_environment_non_interactive_existing(self, mock_exists):
        """Test non - interactive setup with existing .env file."""
        mock_exists.return_value = True

        result = self.setup_tool.setup_environment(interactive=False)

        assert result is True
        assert self.setup_tool.setup_state["env_configured"] is True


class TestDatabaseSetup:
    """Test database setup functionality."""

    def setup_method(self):
        """Set up fresh setup tool for each test."""
        self.setup_tool = SystemSetup()

    @patch("pymysql.connect")
    @patch("dotenv.load_dotenv")
    @patch.dict(
        os.environ,
        {"DB_HOST": "localhost", "DB_PORT": "3306", "DB_USER": "testuser", "DB_PASS": "testpass", "DB_NAME": "testdb"},
    )
    def test_ensure_database_exists_success(self, mock_load_dotenv, mock_connect):
        """Test successful database creation."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        result = self.setup_tool._ensure_database_exists()

        assert result is True
        assert self.setup_tool.setup_state["database_connected"] is True
        mock_cursor.execute.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("pymysql.connect")
    @patch("dotenv.load_dotenv")
    def test_ensure_database_exists_missing_user(self, mock_load_dotenv, mock_connect):
        """Test database creation fails with missing DB_USER."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(Exception):  # Should raise ConfigurationError
                self.setup_tool._ensure_database_exists()

    @patch.object(SystemSetup, "_ensure_database_exists")
    @patch("tools.core.create_tables.create_youtube_tables")
    def test_create_tables_success(self, mock_create_tables, mock_ensure_db):
        """Test successful table creation."""
        mock_ensure_db.return_value = True
        mock_create_tables.return_value = True

        result = self.setup_tool.create_tables()

        assert result is True
        assert self.setup_tool.setup_state["tables_created"] is True
        mock_ensure_db.assert_called_once()
        mock_create_tables.assert_called_once()

    @patch.object(SystemSetup, "_ensure_database_exists")
    def test_create_tables_database_failure(self, mock_ensure_db):
        """Test table creation fails when database setup fails."""
        mock_ensure_db.return_value = False

        result = self.setup_tool.create_tables()

        assert result is False
        assert self.setup_tool.setup_state["tables_created"] is False


class TestConfigurationValidation:
    """Test configuration validation functionality."""

    def setup_method(self):
        """Set up fresh setup tool for each test."""
        self.setup_tool = SystemSetup()

    @patch("pathlib.Path.exists")
    def test_validate_configuration_missing_env_file(self, mock_exists):
        """Test validation fails when .env file is missing."""
        mock_exists.return_value = False

        result = self.setup_tool.validate_configuration()

        assert result is False

    @patch("pathlib.Path.exists")
    @patch("dotenv.load_dotenv")
    @patch.dict(
        os.environ,
        {
            "YOUTUBE_API_KEY": "AIzaSyDummyKeyForTestingPurposes123456789",  # Longer key for validation
            "DB_HOST": "localhost",
            "DB_USER": "testuser",
            "DB_NAME": "testdb",
        },
    )
    @patch("web.etl_helpers.get_engine")
    def test_validate_configuration_database_success(self, mock_get_engine, mock_load_dotenv, mock_exists):
        """Test validation with successful database connection."""
        mock_exists.return_value = True

        # Mock database connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock table checks
        mock_result = MagicMock()
        mock_result.fetchone.return_value = ("youtube_videos",)  # Table exists
        mock_conn.execute.return_value = mock_result

        result = self.setup_tool.validate_configuration()

        assert result is True
        assert self.setup_tool.setup_state["database_connected"] is True
        assert self.setup_tool.setup_state["tables_created"] is True
        assert self.setup_tool.setup_state["validation_passed"] is True

    @patch("pathlib.Path.exists")
    @patch("dotenv.load_dotenv")
    @patch.dict(os.environ, {"YOUTUBE_API_KEY": "test_key"})
    def test_validate_configuration_missing_db_vars(self, mock_load_dotenv, mock_exists):
        """Test validation fails with missing database variables."""
        mock_exists.return_value = True

        result = self.setup_tool.validate_configuration()

        assert result is False


class TestFullSetup:
    """Test full setup workflow."""

    def setup_method(self):
        """Set up fresh setup tool for each test."""
        self.setup_tool = SystemSetup()

    @patch.object(SystemSetup, "setup_environment")
    @patch.object(SystemSetup, "create_tables")
    @patch.object(SystemSetup, "validate_configuration")
    def test_full_setup_success(self, mock_validate, mock_create_tables, mock_setup_env):
        """Test successful full setup workflow."""
        # Mock all steps succeed
        mock_setup_env.return_value = True
        mock_create_tables.return_value = True
        mock_validate.return_value = True

        result = self.setup_tool.full_setup(interactive=False)

        assert result is True
        mock_setup_env.assert_called_once_with(interactive=False, force=False)
        mock_create_tables.assert_called_once_with(force=False)
        mock_validate.assert_called_once()

    @patch.object(SystemSetup, "setup_environment")
    def test_full_setup_env_failure(self, mock_setup_env):
        """Test full setup fails when environment setup fails."""
        mock_setup_env.return_value = False

        result = self.setup_tool.full_setup(interactive=False)

        assert result is False
        mock_setup_env.assert_called_once()

    @patch.object(SystemSetup, "setup_environment")
    @patch.object(SystemSetup, "create_tables")
    def test_full_setup_table_failure(self, mock_create_tables, mock_setup_env):
        """Test full setup fails when table creation fails."""
        mock_setup_env.return_value = True
        mock_create_tables.return_value = False

        result = self.setup_tool.full_setup(interactive=False)

        assert result is False
        mock_setup_env.assert_called_once()
        mock_create_tables.assert_called_once()


class TestUtilityMethods:
    """Test utility and helper methods."""

    def setup_method(self):
        """Set up fresh setup tool for each test."""
        self.setup_tool = SystemSetup()

    def test_get_setup_status(self):
        """Test setup status reporting."""
        status = self.setup_tool.get_setup_status()

        assert "setup_state" in status
        assert "env_file_exists" in status
        assert "timestamp" in status

        # Check setup state structure
        setup_state = status["setup_state"]
        expected_keys = ["env_configured", "database_connected", "tables_created", "validation_passed"]
        for key in expected_keys:
            assert key in setup_state
            assert isinstance(setup_state[key], bool)

    @patch("builtins.open", new_callable=mock_open, read_data="DB_HOST=localhost\nYOUTUBE_API_KEY=secret123\n")
    def test_display_current_config_masks_sensitive(self, mock_file):
        """Test that sensitive values are masked in config display."""
        with patch("pathlib.Path.exists", return_value=True):
            # Capture print output
            with patch("builtins.print") as mock_print:
                self.setup_tool._display_current_config()

                # Check that sensitive values were masked
                printed_content = str(mock_print.call_args_list)
                assert "***MASKED***" in printed_content
                assert "secret123" not in printed_content

    def test_cleanup_resources(self):
        """Test resource cleanup (should not raise errors)."""
        # Should not raise any exceptions
        self.setup_tool.cleanup_resources()


class TestMainFunction:
    """Test main function and CLI interface."""

    @patch("tools.core.unified_setup.SystemSetup")
    def test_main_check_option(self, mock_setup_class):
        """Test main function with --check option."""
        mock_setup = MagicMock()
        mock_setup.validate_configuration.return_value = True
        mock_setup_class.return_value.__enter__.return_value = mock_setup

        with patch("sys.argv", ["unified_setup.py", "--check"]):
            result = main()

        assert result == 0
        mock_setup.validate_configuration.assert_called_once()

    @patch("tools.core.unified_setup.SystemSetup")
    def test_main_create_tables_option(self, mock_setup_class):
        """Test main function with --create - tables option."""
        mock_setup = MagicMock()
        mock_setup.create_tables.return_value = True
        mock_setup_class.return_value.__enter__.return_value = mock_setup

        with patch("sys.argv", ["unified_setup.py", "--create - tables"]):
            result = main()

        assert result == 0
        mock_setup.create_tables.assert_called_once_with(force=False)

    @patch("tools.core.unified_setup.SystemSetup")
    def test_main_full_setup_option(self, mock_setup_class):
        """Test main function with --full - setup option."""
        mock_setup = MagicMock()
        mock_setup.full_setup.return_value = True
        mock_setup_class.return_value.__enter__.return_value = mock_setup

        with patch("sys.argv", ["unified_setup.py", "--full - setup"]):
            result = main()

        assert result == 0
        mock_setup.full_setup.assert_called_once_with(interactive=True, force=False)

    @patch("tools.core.unified_setup.SystemSetup")
    def test_main_status_option(self, mock_setup_class):
        """Test main function with --status option."""
        mock_setup = MagicMock()
        mock_setup.get_setup_status.return_value = {"test": "status"}
        mock_setup_class.return_value.__enter__.return_value = mock_setup

        with patch("sys.argv", ["unified_setup.py", "--status"]):
            with patch("builtins.print") as mock_print:
                result = main()

        assert result == 0
        mock_setup.get_setup_status.assert_called_once()
        # Should print JSON status
        mock_print.assert_called_once()

    @patch("tools.core.unified_setup.SystemSetup")
    def test_main_keyboard_interrupt(self, mock_setup_class):
        """Test main function handles keyboard interrupt gracefully."""
        mock_setup = MagicMock()
        mock_setup.full_setup.side_effect = KeyboardInterrupt()
        mock_setup_class.return_value.__enter__.return_value = mock_setup

        with patch("sys.argv", ["unified_setup.py", "--full - setup"]):
            result = main()

        assert result == 1

    @patch("tools.core.unified_setup.SystemSetup")
    def test_main_exception_handling(self, mock_setup_class):
        """Test main function handles exceptions gracefully."""
        mock_setup = MagicMock()
        mock_setup.full_setup.side_effect = Exception("Test error")
        mock_setup.handle_error = MagicMock()
        mock_setup_class.return_value.__enter__.return_value = mock_setup

        with patch("sys.argv", ["unified_setup.py", "--full - setup"]):
            result = main()

        assert result == 1
        mock_setup.handle_error.assert_called_once()


class TestIntegration:
    """Integration tests for the unified setup tool."""

    def test_tool_registration(self):
        """Test that the tool registers itself properly."""
        from tools.shared.common import find_tool

        # Create tool instance (should register itself)
        setup_tool = SystemSetup()

        # Find the registered tool
        found_tool = find_tool("unified - setup")

        assert found_tool is not None
        assert found_tool.name == "unified - setup"
        assert found_tool.version == "1.0.0"
        assert found_tool.category == "core"

    def test_context_manager_usage(self):
        """Test tool can be used as context manager."""
        cleanup_called = False

        class TestSetup(SystemSetup):
            def cleanup_resources(self):
                nonlocal cleanup_called
                cleanup_called = True

        with TestSetup() as setup_tool:
            assert setup_tool.name == "unified - setup"

        assert cleanup_called
