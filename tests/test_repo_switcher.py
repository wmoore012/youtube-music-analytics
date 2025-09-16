#!/usr/bin/env python3
"""
Test Suite for Repository Switcher - TDD Implementation
=====================================================

Comprehensive test coverage for repository management functionality.
Following TDD principles with test-first development.

Built by Grammy-nominated producer + M.S. Data Science student.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

sys.path.append("scripts")
from repo_switcher import RepositorySwitcher


class TestRepositorySwitcher(unittest.TestCase):
    """Test suite for RepositorySwitcher class."""

    def setUp(self):
        """Set up test environment with temporary directory."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Create .kiro/settings directory
        Path(".kiro/settings").mkdir(parents=True, exist_ok=True)

        self.switcher = RepositorySwitcher()

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.test_dir)

    def test_init_creates_default_config(self):
        """Test that initialization creates default configuration."""
        # Verify config file exists
        self.assertTrue(self.switcher.config_file.exists())

        # Verify default repositories are configured
        self.assertIn("public", self.switcher.config["repositories"])
        self.assertIn("staging", self.switcher.config["repositories"])

        # Verify default target is public
        self.assertEqual(self.switcher.config["current_target"], "public")

    def test_load_existing_config(self):
        """Test loading existing configuration file."""
        # Create custom config
        custom_config = {
            "repositories": {
                "test": {
                    "name": "test-remote",
                    "url": "https://github.com/test/repo.git",
                    "description": "Test repository",
                    "excluded_files": ["*.test"],
                    "required_files": ["README.md"],
                }
            },
            "current_target": "test",
            "auto_switch_rules": {},
        }

        with open(self.switcher.config_file, "w") as f:
            json.dump(custom_config, f)

        # Create new switcher instance
        new_switcher = RepositorySwitcher()

        # Verify custom config is loaded
        self.assertEqual(new_switcher.config["current_target"], "test")
        self.assertIn("test", new_switcher.config["repositories"])

    def test_save_config(self):
        """Test saving configuration to file."""
        # Modify config
        self.switcher.config["current_target"] = "staging"
        self.switcher.save_config()

        # Verify file is updated
        with open(self.switcher.config_file, "r") as f:
            saved_config = json.load(f)

        self.assertEqual(saved_config["current_target"], "staging")

    def test_switch_target_valid(self):
        """Test switching to a valid repository target."""
        result = self.switcher.switch_target("staging")

        self.assertTrue(result)
        self.assertEqual(self.switcher.config["current_target"], "staging")

    def test_switch_target_invalid(self):
        """Test switching to an invalid repository target."""
        result = self.switcher.switch_target("nonexistent")

        self.assertFalse(result)
        self.assertEqual(self.switcher.config["current_target"], "public")  # Should remain unchanged

    @patch("subprocess.run")
    def test_ensure_remote_configured_new_remote(self, mock_run):
        """Test adding a new git remote."""
        # Mock git commands: git rev-parse (is git repo), get-url fails, add succeeds
        mock_run.side_effect = [
            Mock(returncode=0),  # git rev-parse succeeds (is git repo)
            subprocess.CalledProcessError(1, "git"),  # get-url fails
            Mock(returncode=0),  # add succeeds
        ]

        self.switcher._ensure_remote_configured("staging")

        # Verify git remote add was called
        mock_run.assert_any_call(
            ["git", "remote", "add", "staging", "https://github.com/wmoore012/staging_yt_analytics.git"], check=True
        )

    @patch("subprocess.run")
    def test_ensure_remote_configured_update_url(self, mock_run):
        """Test updating existing git remote URL."""
        # Mock git commands: git rev-parse (is git repo), get-url returns old URL, set-url succeeds
        mock_run.side_effect = [
            Mock(returncode=0),  # git rev-parse succeeds (is git repo)
            Mock(stdout="https://github.com/old/repo.git\n", returncode=0),  # get-url returns old URL
            Mock(returncode=0),  # set-url succeeds
        ]

        self.switcher._ensure_remote_configured("staging")

        # Verify git remote set-url was called
        mock_run.assert_any_call(
            ["git", "remote", "set-url", "staging", "https://github.com/wmoore012/staging_yt_analytics.git"], check=True
        )

    @patch("subprocess.run")
    def test_show_file_impact_with_exclusions(self, mock_run):
        """Test showing file impact for repository with exclusions."""
        # Mock find command to return some files
        mock_run.return_value = Mock(stdout="./test.env\n./data/test.csv\n", returncode=0)

        # Capture output
        with patch("builtins.print") as mock_print:
            self.switcher._show_file_impact("public")

        # Verify exclusion patterns are shown
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        exclusion_output = "\n".join(print_calls)

        self.assertIn("FILE EXCLUSION RULES", exclusion_output)
        self.assertIn(".env*", exclusion_output)

    def test_show_file_impact_no_exclusions(self):
        """Test showing file impact for repository without exclusions."""
        with patch("builtins.print") as mock_print:
            self.switcher._show_file_impact("staging")

        # Verify "all files included" message
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("All files will be included", output)

    def test_create_deployment_script_valid_target(self):
        """Test creating deployment script for valid target."""
        result = self.switcher.create_deployment_script("staging")

        self.assertTrue(result)

        # Verify script file exists
        script_path = Path("deploy_to_staging.sh")
        self.assertTrue(script_path.exists())

        # Verify script content
        script_content = script_path.read_text()
        self.assertIn("DEPLOYING TO STAGING REPOSITORY", script_content)
        self.assertIn("python scripts/repo_switcher.py switch staging", script_content)
        self.assertIn("git push staging main", script_content)

        # Verify script is executable
        self.assertTrue(os.access(script_path, os.X_OK))

    def test_create_deployment_script_invalid_target(self):
        """Test creating deployment script for invalid target."""
        result = self.switcher.create_deployment_script("nonexistent")

        self.assertFalse(result)

        # Verify no script file is created
        script_path = Path("deploy_to_nonexistent.sh")
        self.assertFalse(script_path.exists())

    @patch("subprocess.run")
    def test_status_with_git_info(self, mock_run):
        """Test status method with git information."""
        # Mock git commands: git rev-parse (is git repo), git remote -v, git branch --show-current
        mock_run.side_effect = [
            Mock(returncode=0),  # git rev-parse succeeds (is git repo)
            Mock(stdout="origin\thttps://github.com/test/repo.git (fetch)\n", returncode=0),  # git remote -v
            Mock(returncode=0),  # git rev-parse succeeds again (for branch check)
            Mock(stdout="main\n", returncode=0),  # git branch --show-current
        ]

        with patch("builtins.print") as mock_print:
            self.switcher.status()

        # Verify status information is displayed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("REPOSITORY SWITCHER STATUS", output)
        self.assertIn("Current Target: public", output)
        self.assertIn("Current Branch: main", output)

    def test_list_repositories(self):
        """Test listing all configured repositories."""
        with patch("builtins.print") as mock_print:
            with patch("subprocess.run") as mock_run:
                # Mock git remote get-url to succeed for both remotes
                mock_run.return_value = Mock(stdout="https://github.com/test/repo.git\n", returncode=0)

                self.switcher.list_repositories()

        # Verify repository information is displayed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("CONFIGURED REPOSITORIES", output)
        self.assertIn("PUBLIC", output)
        self.assertIn("STAGING", output)
        self.assertIn("Remote configured", output)


class TestRepositorySwitcherIntegration(unittest.TestCase):
    """Integration tests for RepositorySwitcher with real git operations."""

    def setUp(self):
        """Set up test environment with git repository."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        # Initialize git repository
        subprocess.run(["git", "init"], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], check=True)
        subprocess.run(["git", "config", "user.name", "Test User"], check=True)

        # Create .kiro/settings directory
        Path(".kiro/settings").mkdir(parents=True, exist_ok=True)

        self.switcher = RepositorySwitcher()

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.test_dir)

    def test_real_git_remote_operations(self):
        """Test real git remote operations."""
        # Add a test remote
        subprocess.run(["git", "remote", "add", "test-remote", "https://github.com/test/repo.git"], check=True)

        # Test ensuring remote is configured
        self.switcher._ensure_remote_configured("staging")

        # Verify staging remote was added
        result = subprocess.run(["git", "remote", "get-url", "staging"], capture_output=True, text=True)

        self.assertEqual(result.returncode, 0)
        self.assertIn("staging_yt_analytics.git", result.stdout)

    def test_deployment_script_execution_dry_run(self):
        """Test deployment script creation and basic validation."""
        # Create deployment script
        result = self.switcher.create_deployment_script("staging")
        self.assertTrue(result)

        # Verify script exists and is executable
        script_path = Path("deploy_to_staging.sh")
        self.assertTrue(script_path.exists())
        self.assertTrue(os.access(script_path, os.X_OK))

        # Verify script has proper shebang and error handling
        script_content = script_path.read_text()
        self.assertIn("#!/bin/bash", script_content)
        self.assertIn("set -e", script_content)


class TestRepositorySwitcherEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        Path(".kiro/settings").mkdir(parents=True, exist_ok=True)
        self.switcher = RepositorySwitcher()

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.test_dir)

    def test_config_file_permission_error(self):
        """Test handling of config file permission errors."""
        # Make config directory read-only
        os.chmod(self.switcher.config_file.parent, 0o444)

        try:
            # This should handle the permission error gracefully
            with self.assertRaises(PermissionError):
                self.switcher.save_config()
        finally:
            # Restore permissions for cleanup
            os.chmod(self.switcher.config_file.parent, 0o755)

    def test_invalid_json_config(self):
        """Test handling of invalid JSON in config file."""
        # Write invalid JSON to config file
        with open(self.switcher.config_file, "w") as f:
            f.write("{ invalid json }")

        # Creating new switcher should fall back to default config
        with self.assertRaises(json.JSONDecodeError):
            RepositorySwitcher()

    @patch("subprocess.run")
    def test_git_command_failures(self, mock_run):
        """Test handling of git command failures."""
        # Mock git commands to fail
        mock_run.side_effect = subprocess.CalledProcessError(1, "git")

        # These should handle git failures gracefully
        with patch("builtins.print"):
            self.switcher.status()  # Should not raise exception

    def test_empty_repository_config(self):
        """Test behavior with empty repository configuration."""
        # Create config with no repositories
        empty_config = {"repositories": {}, "current_target": "nonexistent", "auto_switch_rules": {}}

        with open(self.switcher.config_file, "w") as f:
            json.dump(empty_config, f)

        # Should handle gracefully
        new_switcher = RepositorySwitcher()
        result = new_switcher.switch_target("anything")
        self.assertFalse(result)


class TestRepositorySwitcherCLI(unittest.TestCase):
    """Test command-line interface functionality."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)

        Path(".kiro/settings").mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        import shutil

        shutil.rmtree(self.test_dir)

    @patch("sys.argv", ["repo_switcher.py"])
    def test_cli_no_arguments(self):
        """Test CLI with no arguments shows usage."""
        with patch("builtins.print") as mock_print:
            from repo_switcher import main

            main()

        # Verify usage information is displayed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("REPOSITORY SWITCHER", output)
        self.assertIn("Usage:", output)

    @patch("sys.argv", ["repo_switcher.py", "list"])
    def test_cli_list_command(self):
        """Test CLI list command."""
        with patch("builtins.print") as mock_print:
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = Mock(stdout="test\n", returncode=0)

                from repo_switcher import main

                main()

        # Verify list output
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("CONFIGURED REPOSITORIES", output)

    @patch("sys.argv", ["repo_switcher.py", "switch", "staging"])
    def test_cli_switch_command(self):
        """Test CLI switch command."""
        with patch("builtins.print") as mock_print:
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = Mock(stdout="test\n", returncode=0)

                from repo_switcher import main

                main()

        # Verify switch output
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("SWITCHING REPOSITORY TARGET", output)

    @patch("sys.argv", ["repo_switcher.py", "invalid"])
    def test_cli_invalid_command(self):
        """Test CLI with invalid command."""
        with patch("builtins.print") as mock_print:
            from repo_switcher import main

            main()

        # Verify error message
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "\n".join(print_calls)

        self.assertIn("Unknown command: invalid", output)


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
