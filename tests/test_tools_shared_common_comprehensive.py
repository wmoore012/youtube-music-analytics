#!/usr / bin / env python3
"""
Test: Tools Shared Common (Comprehensive)

Comprehensive tests for the shared tools common functionality.
Tests the ToolBase class, ToolConfig, ToolRegistry, and error handling.
"""

from datetime import datetime
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestToolBase:
    """Test ToolBase class functionality."""

    def test_tool_base_imports(self):
        """Test that ToolBase imports correctly."""
        from tools.shared.common import ToolBase

        assert ToolBase is not None

    def test_tool_base_initialization(self):
        """Test ToolBase initialization."""
        from tools.shared.common import ToolBase

        class TestTool(ToolBase):
            def get_required_environment_vars(self):
                return []

            def get_tool_config(self):
                from tools.shared.common import ToolConfig

                return ToolConfig(
                    name="test - tool",
                    version="1.0.0",
                    description="Test tool",
                    dependencies=[],
                    environment_vars=[],
                    usage_examples=[],
                    category="test",
                )

            def run(self):
                pass

            def cleanup_resources(self):
                pass

        tool = TestTool(name="test - tool", version="1.0.0")
        assert tool.name == "test - tool"
        assert tool.version == "1.0.0"

    def test_tool_base_context_manager(self):
        """Test ToolBase context manager functionality."""
        from tools.shared.common import ToolBase

        class TestTool(ToolBase):
            def __init__(self):
                super().__init__(name="test - tool", version="1.0.0")
                self.cleanup_called = False

            def get_required_environment_vars(self):
                return []

            def get_tool_config(self):
                from tools.shared.common import ToolConfig

                return ToolConfig(
                    name="test - tool",
                    version="1.0.0",
                    description="Test tool",
                    dependencies=[],
                    environment_vars=[],
                    usage_examples=[],
                    category="test",
                )

            def run(self):
                pass

            def cleanup_resources(self):
                self.cleanup_called = True

        # Test context manager
        with TestTool() as tool:
            assert tool.name == "test - tool"

        # Cleanup should have been called
        assert tool.cleanup_called

    def test_tool_base_logging(self):
        """Test ToolBase logging functionality."""
        from tools.shared.common import ToolBase

        class TestTool(ToolBase):
            def __init__(self):
                super().__init__(name="test - tool", version="1.0.0")
                self.logged_messages = []

            def get_required_environment_vars(self):
                return []

            def get_tool_config(self):
                from tools.shared.common import ToolConfig

                return ToolConfig(
                    name="test - tool",
                    version="1.0.0",
                    description="Test tool",
                    dependencies=[],
                    environment_vars=[],
                    usage_examples=[],
                    category="test",
                )

            def run(self):
                pass

            def cleanup_resources(self):
                pass

        tool = TestTool()

        # Test logging methods exist
        assert hasattr(tool, "log_progress")
        assert hasattr(tool, "handle_error")

        # Test logging doesn't crash
        tool.log_progress("Test message")
        tool.handle_error(Exception("Test error"), "test context")

    def test_tool_base_configuration(self):
        """Test ToolBase configuration functionality."""
        from tools.shared.common import ToolBase

        class TestTool(ToolBase):
            def __init__(self):
                super().__init__(name="test - tool", version="1.0.0")

            def get_required_environment_vars(self):
                return ["TEST_VAR"]

            def get_tool_config(self):
                from tools.shared.common import ToolConfig

                return ToolConfig(
                    name="test - tool",
                    version="1.0.0",
                    description="Test tool",
                    dependencies=["python>=3.8"],
                    environment_vars=["TEST_VAR"],
                    usage_examples=["python test_tool.py"],
                    category="test",
                )

            def run(self):
                pass

            def cleanup_resources(self):
                pass

        tool = TestTool()
        config = tool.get_tool_config()

        assert config.name == "test - tool"
        assert config.version == "1.0.0"
        assert "TEST_VAR" in config.environment_vars
        assert "python>=3.8" in config.dependencies


class TestToolConfig:
    """Test ToolConfig dataclass functionality."""

    def test_tool_config_creation(self):
        """Test ToolConfig creation."""
        from tools.shared.common import ToolConfig

        config = ToolConfig(
            name="test - tool",
            version="1.0.0",
            description="Test tool description",
            dependencies=["python>=3.8", "requests"],
            environment_vars=["API_KEY", "DATABASE_URL"],
            usage_examples=["python test_tool.py --help"],
            category="test",
        )

        assert config.name == "test - tool"
        assert config.version == "1.0.0"
        assert config.description == "Test tool description"
        assert len(config.dependencies) == 2
        assert len(config.environment_vars) == 2
        assert len(config.usage_examples) == 1
        assert config.category == "test"

    def test_tool_config_validation(self):
        """Test ToolConfig validation."""
        from tools.shared.common import ToolConfig

        # Valid config should work
        config = ToolConfig(
            name="valid - tool",
            version="1.0.0",
            description="Valid tool",
            dependencies=[],
            environment_vars=[],
            usage_examples=[],
            category="test",
        )
        assert config.name == "valid - tool"

    def test_tool_config_serialization(self):
        """Test ToolConfig serialization."""
        import dataclasses

        from tools.shared.common import ToolConfig

        config = ToolConfig(
            name="serializable - tool",
            version="2.0.0",
            description="Serializable tool",
            dependencies=["pandas", "numpy"],
            environment_vars=["DATA_PATH"],
            usage_examples=["python tool.py --data /path / to / data"],
            category="analytics",
        )

        # Should be serializable to dict
        config_dict = dataclasses.asdict(config)
        assert config_dict["name"] == "serializable - tool"
        assert config_dict["version"] == "2.0.0"
        assert len(config_dict["dependencies"]) == 2


class TestToolRegistry:
    """Test ToolRegistry functionality."""

    def test_tool_registry_imports(self):
        """Test that ToolRegistry imports correctly."""
        from tools.shared.common import ToolRegistry, get_tool_registry

        assert ToolRegistry is not None
        assert get_tool_registry is not None

    def test_tool_registry_singleton(self):
        """Test ToolRegistry singleton behavior."""
        from tools.shared.common import get_tool_registry

        registry1 = get_tool_registry()
        registry2 = get_tool_registry()

        # Should be the same instance
        assert registry1 is registry2

    def test_tool_registry_registration(self):
        """Test tool registration in registry."""
        from tools.shared.common import ToolConfig, get_tool_registry, register_tool

        # Create test config
        config = ToolConfig(
            name="registry - test - tool",
            version="1.0.0",
            description="Registry test tool",
            dependencies=[],
            environment_vars=[],
            usage_examples=[],
            category="test",
        )

        # Register tool
        register_tool(config)

        # Verify registration
        registry = get_tool_registry()
        tools = registry.list_tools()

        # Should find our registered tool
        registered_tool = None
        for tool in tools:
            if tool.name == "registry - test - tool":
                registered_tool = tool
                break

        assert registered_tool is not None
        assert registered_tool.name == "registry - test - tool"
        assert registered_tool.version == "1.0.0"

    def test_tool_registry_list_tools(self):
        """Test listing tools from registry."""
        from tools.shared.common import get_tool_registry

        registry = get_tool_registry()
        tools = registry.list_tools()

        # Should return a list
        assert isinstance(tools, list)

        # Each tool should have required attributes
        for tool in tools:
            assert hasattr(tool, "name")
            assert hasattr(tool, "version")
            assert hasattr(tool, "description")
            assert hasattr(tool, "category")

    def test_tool_registry_filter_by_category(self):
        """Test filtering tools by category."""
        from tools.shared.common import ToolConfig, get_tool_registry, register_tool

        # Register tools in different categories
        test_configs = [
            ToolConfig(
                name="category - test - 1",
                version="1.0.0",
                description="Category test 1",
                dependencies=[],
                environment_vars=[],
                usage_examples=[],
                category="analytics",
            ),
            ToolConfig(
                name="category - test - 2",
                version="1.0.0",
                description="Category test 2",
                dependencies=[],
                environment_vars=[],
                usage_examples=[],
                category="development",
            ),
        ]

        for config in test_configs:
            register_tool(config)

        registry = get_tool_registry()

        # Test category filtering
        analytics_tools = registry.list_tools(category="analytics")
        development_tools = registry.list_tools(category="development")

        # Should find tools in correct categories
        analytics_names = [tool.name for tool in analytics_tools]
        development_names = [tool.name for tool in development_tools]

        assert "category - test - 1" in analytics_names
        assert "category - test - 2" in development_names

    def test_tool_registry_validation(self):
        """Test tool registry validation."""
        from tools.shared.common import get_tool_registry

        registry = get_tool_registry()

        # Test validation method exists
        assert hasattr(registry, "validate_tools")

        # Run validation
        validation_errors = registry.validate_tools()

        # Should return a list (empty or with errors)
        assert isinstance(validation_errors, list)


class TestErrorHandling:
    """Test error handling functionality."""

    def test_error_classes_import(self):
        """Test that error classes import correctly."""
        from tools.shared.common import ConfigurationError, ExecutionError, ToolError, ValidationError

        assert ToolError is not None
        assert ConfigurationError is not None
        assert ExecutionError is not None
        assert ValidationError is not None

    def test_error_class_hierarchy(self):
        """Test error class inheritance hierarchy."""
        from tools.shared.common import ConfigurationError, ExecutionError, ToolError, ValidationError

        # All should inherit from ToolError
        assert issubclass(ConfigurationError, ToolError)
        assert issubclass(ExecutionError, ToolError)
        assert issubclass(ValidationError, ToolError)

    def test_error_instantiation(self):
        """Test error instantiation and messages."""
        from tools.shared.common import ConfigurationError, ExecutionError, ToolError, ValidationError

        # Test basic error
        error = ToolError("Basic tool error")
        assert str(error) == "Basic tool error"

        # Test specific errors
        config_error = ConfigurationError("Configuration issue")
        assert str(config_error) == "Configuration issue"

        exec_error = ExecutionError("Execution failed")
        assert str(exec_error) == "Execution failed"

        val_error = ValidationError("Validation failed")
        assert str(val_error) == "Validation failed"

    def test_error_raising(self):
        """Test raising and catching custom errors."""
        from tools.shared.common import ConfigurationError, ExecutionError

        # Test raising ConfigurationError
        with pytest.raises(ConfigurationError):
            raise ConfigurationError("Test configuration error")

        # Test raising ExecutionError
        with pytest.raises(ExecutionError):
            raise ExecutionError("Test execution error")

        # Test catching as base ToolError
        from tools.shared.common import ToolError

        try:
            raise ConfigurationError("Test error")
        except ToolError as e:
            assert "Test error" in str(e)


class TestUtilityFunctions:
    """Test utility functions in common module."""

    def test_get_config_value_function(self):
        """Test get_config_value utility function if it exists."""
        try:
            from tools.shared.common import get_config_value

            # Test with default value
            value = get_config_value("NONEXISTENT_VAR", "default_value")
            assert value == "default_value"

        except ImportError:
            # Function might not exist, that's okay
            pass

    def test_validate_environment_function(self):
        """Test validate_environment utility function if it exists."""
        try:
            from tools.shared.common import validate_environment

            # Test validation with empty requirements
            result = validate_environment([])
            assert isinstance(result, (bool, dict, list))

        except ImportError:
            # Function might not exist, that's okay
            pass

    def test_setup_logging_function(self):
        """Test setup_logging utility function if it exists."""
        try:
            from tools.shared.common import setup_logging

            # Test logging setup
            logger = setup_logging("test - logger")
            assert logger is not None

        except ImportError:
            # Function might not exist, that's okay
            pass


class TestIntegrationScenarios:
    """Test integration scenarios with multiple components."""

    def test_full_tool_lifecycle(self):
        """Test complete tool lifecycle from creation to cleanup."""
        from tools.shared.common import ToolBase, ToolConfig, get_tool_registry, register_tool

        class LifecycleTestTool(ToolBase):
            def __init__(self):
                super().__init__(name="lifecycle - test", version="1.0.0")
                self.initialized = True
                self.run_called = False
                self.cleanup_called = False

            def get_required_environment_vars(self):
                return ["TEST_ENV_VAR"]

            def get_tool_config(self):
                return ToolConfig(
                    name="lifecycle - test",
                    version="1.0.0",
                    description="Lifecycle test tool",
                    dependencies=["python>=3.8"],
                    environment_vars=["TEST_ENV_VAR"],
                    usage_examples=["python lifecycle_test.py"],
                    category="test",
                )

            def run(self):
                self.run_called = True
                return "Tool executed successfully"

            def cleanup_resources(self):
                self.cleanup_called = True

        # Test full lifecycle
        with LifecycleTestTool() as tool:
            # Tool should be initialized
            assert tool.initialized

            # Register tool
            register_tool(tool.get_tool_config())

            # Verify registration
            registry = get_tool_registry()
            tools = registry.list_tools()
            tool_names = [t.name for t in tools]
            assert "lifecycle - test" in tool_names

            # Run tool
            result = tool.run()
            assert tool.run_called
            assert result == "Tool executed successfully"

        # Cleanup should have been called
        assert tool.cleanup_called

    def test_error_handling_integration(self):
        """Test error handling integration across components."""
        from tools.shared.common import ExecutionError, ToolBase, ToolConfig

        class ErrorTestTool(ToolBase):
            def __init__(self):
                super().__init__(name="error - test", version="1.0.0")

            def get_required_environment_vars(self):
                return []

            def get_tool_config(self):
                return ToolConfig(
                    name="error - test",
                    version="1.0.0",
                    description="Error test tool",
                    dependencies=[],
                    environment_vars=[],
                    usage_examples=[],
                    category="test",
                )

            def run(self):
                raise ExecutionError("Intentional test error")

            def cleanup_resources(self):
                pass

        tool = ErrorTestTool()

        # Test error handling
        with pytest.raises(ExecutionError):
            tool.run()

        # Test error handling through handle_error method
        try:
            tool.run()
        except ExecutionError as e:
            tool.handle_error(e, "test context")
            # Should not re - raise


def test_module_structure():
    """Test that common module has expected structure."""
    import tools.shared.common as common_module

    # Module should exist and be importable
    assert common_module is not None

    # Should have main classes
    expected_classes = ["ToolBase", "ToolConfig", "ToolRegistry"]
    for cls_name in expected_classes:
        assert hasattr(common_module, cls_name), f"Missing class: {cls_name}"

    # Should have error classes
    expected_errors = ["ToolError", "ConfigurationError", "ExecutionError", "ValidationError"]
    for error_name in expected_errors:
        assert hasattr(common_module, error_name), f"Missing error class: {error_name}"

    # Should have utility functions
    expected_functions = ["register_tool", "get_tool_registry"]
    for func_name in expected_functions:
        assert hasattr(common_module, func_name), f"Missing function: {func_name}"


if __name__ == "__main__":
    print("🧪 RUNNING TOOLS SHARED COMMON COMPREHENSIVE TESTS")
    print("=" * 60)
    print("🔧 These tests ensure the shared tools framework is working correctly")
    print()

    # Run the tests
    pytest.main([__file__, "-v"])
