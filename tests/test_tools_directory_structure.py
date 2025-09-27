"""
Tests for the new tools directory structure.

This test suite validates:
- Directory structure exists as designed
- README files are present and informative
- Tools are properly organized by category
- Import paths work correctly
"""

import os
from pathlib import Path

import pytest


class TestToolsDirectoryStructure:
    """Test the new tools directory organization."""

    def test_core_directories_exist(self):
        """Test that all core directories exist."""
        tools_dir = Path("tools")

        expected_dirs = ["core", "specialized", "development", "shared", "legacy"]

        for dir_name in expected_dirs:
            dir_path = tools_dir / dir_name
            assert dir_path.exists(), f"Directory {dir_name} should exist"
            assert dir_path.is_dir(), f"{dir_name} should be a directory"

    def test_specialized_subdirectories_exist(self):
        """Test that specialized subdirectories exist."""
        specialized_dir = Path("tools / specialized")

        expected_subdirs = ["analytics", "migration", "benchmarking"]

        for subdir_name in expected_subdirs:
            subdir_path = specialized_dir / subdir_name
            assert subdir_path.exists(), f"Specialized subdirectory {subdir_name} should exist"
            assert subdir_path.is_dir(), f"{subdir_name} should be a directory"

    def test_development_subdirectories_exist(self):
        """Test that development subdirectories exist."""
        development_dir = Path("tools / development")

        expected_subdirs = ["code_quality", "testing", "ci_enforcement"]

        for subdir_name in expected_subdirs:
            subdir_path = development_dir / subdir_name
            assert subdir_path.exists(), f"Development subdirectory {subdir_name} should exist"
            assert subdir_path.is_dir(), f"{subdir_name} should be a directory"

    def test_readme_files_exist(self):
        """Test that README files exist in key directories."""
        readme_locations = [
            "tools / README.md",
            "tools / core / README.md",
            "tools / specialized / README.md",
            "tools / development / README.md",
            "tools / shared / README.md",
            "tools / legacy / README.md",
        ]

        for readme_path in readme_locations:
            path = Path(readme_path)
            assert path.exists(), f"README should exist at {readme_path}"
            assert path.is_file(), f"{readme_path} should be a file"

            # Check that README has content
            content = path.read_text()
            assert len(content) > 100, f"README at {readme_path} should have substantial content"

    def test_init_files_exist(self):
        """Test that __init__.py files exist in Python packages."""
        init_locations = [
            "tools / __init__.py",
            "tools / core / __init__.py",
            "tools / specialized / __init__.py",
            "tools / development / __init__.py",
            "tools / shared / __init__.py",
            "tools / legacy / __init__.py",
            "tools / specialized / analytics / __init__.py",
            "tools / specialized / migration / __init__.py",
            "tools / specialized / benchmarking / __init__.py",
            "tools / development / code_quality / __init__.py",
            "tools / development / testing / __init__.py",
            "tools / development / ci_enforcement / __init__.py",
        ]

        for init_path in init_locations:
            path = Path(init_path)
            assert path.exists(), f"__init__.py should exist at {init_path}"
            assert path.is_file(), f"{init_path} should be a file"

    def test_tools_moved_to_appropriate_locations(self):
        """Test that tools have been moved to appropriate locations."""
        # Check that core tools exist
        core_tools = [
            "tools / core / monitor.py",
            "tools / core / setup.py",
            "tools / core / run_focused_etl.py",
            "tools / core / cleanup_old_artists.py",
        ]

        for tool_path in core_tools:
            path = Path(tool_path)
            assert path.exists(), f"Core tool should exist at {tool_path}"

        # Check that specialized tools exist
        specialized_tools = [
            "tools / specialized / analytics / alias_manager.py",
            "tools / specialized / migration / migrate_data_files.py",
        ]

        for tool_path in specialized_tools:
            path = Path(tool_path)
            assert path.exists(), f"Specialized tool should exist at {tool_path}"

        # Check that development tools exist
        development_tools = ["tools / development / notebook_manager.py", "tools / development / run_notebooks.py"]

        for tool_path in development_tools:
            path = Path(tool_path)
            assert path.exists(), f"Development tool should exist at {tool_path}"

    def test_shared_utilities_importable(self):
        """Test that shared utilities can be imported."""
        try:
            from tools.shared.common import (
                ConfigurationError,
                ExecutionError,
                ToolBase,
                ToolConfig,
                ToolError,
                ToolRegistry,
                ValidationError,
            )

            # Test that classes can be instantiated
            registry = ToolRegistry()
            assert registry is not None

            config = ToolConfig(name="test", version="1.0.0", description="Test config")
            assert config.name == "test"

        except ImportError as e:
            pytest.fail(f"Failed to import shared utilities: {e}")

    def test_old_directories_removed(self):
        """Test that old directory structure has been cleaned up."""
        tools_dir = Path("tools")

        # These directories should no longer exist at the root level
        old_dirs = ["analytics", "monitoring", "sentiment", "utilities"]

        for old_dir in old_dirs:
            old_path = tools_dir / old_dir
            assert not old_path.exists(), f"Old directory {old_dir} should have been removed or moved"

    def test_directory_organization_follows_design(self):
        """Test that directory organization follows the design document."""
        # Core tools should contain essential daily - use tools
        core_dir = Path("tools / core")
        core_files = [f.name for f in core_dir.glob("*.py")]

        # Should contain ETL, setup, monitoring, and maintenance tools
        expected_core_patterns = ["etl", "setup", "monitor", "cleanup"]

        for pattern in expected_core_patterns:
            matching_files = [f for f in core_files if pattern in f.lower()]
            assert len(matching_files) > 0, f"Core directory should contain tools matching pattern '{pattern}'"

        # Specialized should contain domain - specific tools
        specialized_analytics = Path("tools / specialized / analytics")
        if specialized_analytics.exists():
            analytics_files = list(specialized_analytics.glob("*.py"))
            assert len(analytics_files) > 0, "Analytics directory should contain tools"

        # Development should contain dev utilities
        dev_code_quality = Path("tools / development / code_quality")
        if dev_code_quality.exists():
            quality_files = list(dev_code_quality.glob("*.py"))
            assert len(quality_files) > 0, "Code quality directory should contain tools"
