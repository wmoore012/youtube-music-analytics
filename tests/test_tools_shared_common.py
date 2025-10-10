"""
Tests for tools / shared / common.py-shared base classes and utilities.

This test suite validates:
- ToolBase functionality (logging, configuration, error handling)
- ToolConfig dataclass validation
- ToolRegistry system for tool discovery
- Standardized error classes
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tools.shared.common import (
    ConfigurationError,
    ExecutionError,
    ToolBase,
    ToolConfig,
    ToolError,
    ToolRegistry,
    ValidationError,
    find_tool,
    get_tool_registry,
    register_tool,
)


class TestToolConfig:
    """Test ToolConfig dataclass functionality."""

    def test_tool_config_creation(self):
        """Test basic ToolConfig creation."""
        config = ToolConfig(name="test-tool", version="1.0.0", description="Test tool for validation")

        assert config.name == "test-tool"
        assert config.version == "1.0.0"
        assert config.description == "Test tool for validation"
        assert config.dependencies == []
        assert config.environment_vars == []
        assert config.usage_examples == []
        assert config.category == "general"

    def test_tool_config_validation_success(self):
        """Test successful validation of complete config."""
        config = ToolConfig(
            name="valid-tool",
            version="2.1.0",
            description="A valid tool configuration",
            dependencies=["python>=3.8"],
            environment_vars=["TEST_VAR"],
            usage_examples=["python tool.py"],
            category="core",
        )

        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            issues = config.validate()
            assert issues == []

    def test_tool_config_validation_failures(self):
        """Test validation failures for incomplete config."""
        # Missing required fields
        config = ToolConfig(name="", version="", description="")
        issues = config.validate()

        assert "Tool name is required" in issues
        assert "Tool version is required" in issues
        assert "Tool description is required" in issues

    def test_tool_config_missing_env_vars(self):
        """Test validation failure for missing environment variables."""
        config = ToolConfig(
            name="test-tool", version="1.0.0", description="Test tool", environment_vars=["MISSING_VAR"]
        )

        issues = config.validate()
        assert any("MISSING_VAR" in issue for issue in issues)


class TestToolRegistry:
    """Test ToolRegistry functionality."""

    def setup_method(self):
        """Set up fresh registry for each test."""
        self.registry = ToolRegistry()

    def test_register_valid_tool(self):
        """Test registering a valid tool."""
        config = ToolConfig(name="test-tool", version="1.0.0", description="Test tool")

        self.registry.register_tool(config)
        found_tool = self.registry.find_tool("test-tool")

        assert found_tool is not None
        assert found_tool.name == "test-tool"
        assert found_tool.version == "1.0.0"

    def test_register_invalid_tool(self):
        """Test registering an invalid tool raises ValidationError."""
        config = ToolConfig(name="", version="", description="")

        with pytest.raises(ValidationError) as exc_info:
            self.registry.register_tool(config)

        assert "Tool configuration validation failed" in str(exc_info.value)

    def test_list_tools_all(self):
        """Test listing all registered tools."""
        config1 = ToolConfig(name="tool1", version="1.0.0", description="Tool 1")
        config2 = ToolConfig(name="tool2", version="2.0.0", description="Tool 2")

        self.registry.register_tool(config1)
        self.registry.register_tool(config2)

        tools = self.registry.list_tools()
        assert len(tools) == 2
        assert tools[0].name == "tool1"  # Should be sorted by name
        assert tools[1].name == "tool2"

    def test_list_tools_by_category(self):
        """Test listing tools filtered by category."""
        config1 = ToolConfig(name="core-tool", version="1.0.0", description="Core tool", category="core")
        config2 = ToolConfig(name="dev-tool", version="1.0.0", description="Dev tool", category="development")

        self.registry.register_tool(config1)
        self.registry.register_tool(config2)

        core_tools = self.registry.list_tools(category="core")
        assert len(core_tools) == 1
        assert core_tools[0].name == "core-tool"

    def test_validate_tools(self):
        """Test validation of all registered tools."""
        # Register a valid tool
        valid_config = ToolConfig(name="valid", version="1.0.0", description="Valid tool")
        self.registry.register_tool(valid_config)

        # Manually add an invalid tool to test validation
        invalid_config = ToolConfig(
            name="invalid", version="1.0.0", description="Invalid tool", environment_vars=["MISSING_VAR"]
        )
        self.registry._tools["invalid"] = invalid_config

        errors = self.registry.validate_tools()
        assert len(errors) == 1
        assert "invalid" in str(errors[0])


class MockTool(ToolBase):
    """Mock tool for testing ToolBase functionality."""

    def __init__(self, name="mock-tool", required_vars=None):
        self._required_vars = required_vars or []
        super().__init__(name=name, version="1.0.0")

    def get_required_environment_vars(self):
        return self._required_vars

    def get_tool_config(self):
        return ToolConfig(name=self.name, version=self.version, description="Mock tool for testing")

    def run(self):
        self.log_progress("Mock tool running")


class TestToolBase:
    """Test ToolBase functionality."""

    def test_tool_initialization(self):
        """Test basic tool initialization."""
        tool = MockTool()

        assert tool.name == "mock-tool"
        assert tool.version == "1.0.0"
        assert tool.logger is not None
        assert tool.config is not None

    def test_required_environment_vars_validation(self):
        """Test validation of required environment variables."""
        # Should fail with missing required var
        with pytest.raises(ConfigurationError) as exc_info:
            MockTool(required_vars=["REQUIRED_VAR"])

        assert "Missing required environment variables" in str(exc_info.value)
        assert "REQUIRED_VAR" in str(exc_info.value)

    def test_required_environment_vars_success(self):
        """Test successful validation when required vars are present."""
        with patch.dict(os.environ, {"REQUIRED_VAR": "test_value"}):
            tool = MockTool(required_vars=["REQUIRED_VAR"])
            assert tool.name == "mock-tool"

    def test_get_config_value(self):
        """Test configuration value retrieval."""
        with patch.dict(os.environ, {"TEST_CONFIG": "test_value"}):
            tool = MockTool()

            # Test getting existing value
            value = tool.get_config_value("TEST_CONFIG")
            assert value == "test_value"

            # Test getting with default
            value = tool.get_config_value("MISSING_CONFIG", default="default_value")
            assert value == "default_value"

            # Test required value that exists
            value = tool.get_config_value("TEST_CONFIG", required=True)
            assert value == "test_value"

    def test_get_config_value_required_missing(self):
        """Test ConfigurationError when required config value is missing."""
        tool = MockTool()

        with pytest.raises(ConfigurationError) as exc_info:
            tool.get_config_value("MISSING_REQUIRED", required=True)

        assert "Required configuration key 'MISSING_REQUIRED' not found" in str(exc_info.value)

    def test_validate_input_success(self):
        """Test successful input validation."""
        tool = MockTool()

        result = tool.validate_input(
            "valid_string", lambda x: isinstance(x, str) and len(x) > 0, "Must be non-empty string"
        )

        assert result == "valid_string"

    def test_validate_input_failure(self):
        """Test input validation failure."""
        tool = MockTool()

        with pytest.raises(ValidationError) as exc_info:
            tool.validate_input("", lambda x: len(x) > 0, "Must be non-empty string")

        assert "Must be non-empty string" in str(exc_info.value)

    def test_handle_error_tool_error(self):
        """Test error handling preserves ToolError types."""
        tool = MockTool()

        original_error = ConfigurationError("Original config error")

        with pytest.raises(ConfigurationError) as exc_info:
            tool.handle_error(original_error, "test context")

        assert exc_info.value is original_error

    def test_handle_error_generic_exception(self):
        """Test error handling converts generic exceptions to ExecutionError."""
        tool = MockTool()

        original_error = ValueError("Generic error")

        with pytest.raises(ExecutionError) as exc_info:
            tool.handle_error(original_error, "test context")

        assert "Error in mock-tool (test context)" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    def test_context_manager(self):
        """Test tool can be used as context manager."""
        cleanup_called = False

        class TestTool(MockTool):
            def cleanup_resources(self):
                nonlocal cleanup_called
                cleanup_called = True

        with TestTool() as tool:
            assert tool.name == "mock-tool"

        assert cleanup_called

    def test_env_file_loading(self):
        """Test loading configuration from .env file."""
        # Create temporary .env file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("TEST_ENV_VAR=test_value\n")
            f.write("# This is a comment\n")
            f.write("ANOTHER_VAR=another_value\n")
            env_file_path = f.name

        try:
            # Change to directory containing .env file
            original_cwd = os.getcwd()
            env_dir = os.path.dirname(env_file_path)
            env_filename = os.path.basename(env_file_path)

            # Rename to .env
            actual_env_path = os.path.join(env_dir, ".env")
            os.rename(env_file_path, actual_env_path)

            os.chdir(env_dir)

            # Clear environment variables to test loading
            with patch.dict(os.environ, {}, clear=True):
                tool = MockTool()

                # Check that .env values were loaded
                assert tool.get_config_value("TEST_ENV_VAR") == "test_value"
                assert tool.get_config_value("ANOTHER_VAR") == "another_value"

        finally:
            os.chdir(original_cwd)
            if os.path.exists(actual_env_path):
                os.unlink(actual_env_path)


class TestStandardizedErrors:
    """Test standardized error classes."""

    def test_tool_error_base(self):
        """Test ToolError base class."""
        error = ToolError("Test error", {"key": "value"})

        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.details == {"key": "value"}

    def test_configuration_error(self):
        """Test ConfigurationError inherits from ToolError."""
        error = ConfigurationError("Config error")

        assert isinstance(error, ToolError)
        assert str(error) == "Config error"

    def test_execution_error(self):
        """Test ExecutionError inherits from ToolError."""
        error = ExecutionError("Execution error")

        assert isinstance(error, ToolError)
        assert str(error) == "Execution error"

    def test_validation_error(self):
        """Test ValidationError inherits from ToolError."""
        error = ValidationError("Validation error")

        assert isinstance(error, ToolError)
        assert str(error) == "Validation error"


class TestGlobalRegistry:
    """Test global registry functions."""

    def test_get_tool_registry(self):
        """Test getting global registry instance."""
        registry1 = get_tool_registry()
        registry2 = get_tool_registry()

        # Should return same instance
        assert registry1 is registry2

    def test_register_and_find_tool(self):
        """Test global register and find functions."""
        config = ToolConfig(name="global-test-tool", version="1.0.0", description="Global test tool")

        register_tool(config)
        found_tool = find_tool("global-test-tool")

        assert found_tool is not None
        assert found_tool.name == "global-test-tool"
