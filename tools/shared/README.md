# Tools Shared Utilities

This directory contains shared base classes and utilities that all tools should use for consistency, reliability, and maintainability.

## Core Components

### ToolBase Class

The `ToolBase` class provides standardized functionality for all tools:

- **Logging**: Consistent logging setup with proper formatting
- **Configuration**: Environment variable loading and validation
- **Error Handling**: Robust error handling with clear error types
- **Progress Reporting**: Standardized progress logging
- **Resource Management**: Context manager support for cleanup

### ToolConfig Dataclass

The `ToolConfig` dataclass standardizes tool metadata:

- Tool name, version, and description
- Dependencies and environment variables
- Usage examples and categorization
- Built-in validation

### ToolRegistry System

The `ToolRegistry` provides tool discovery and validation:

- Central registry for all tools
- Tool validation and health checks
- Category-based tool organization
- Global registry instance for easy access

### Standardized Error Classes

Four error types for consistent error handling:

- `ToolError`: Base exception for all tool errors
- `ConfigurationError`: Configuration-related errors
- `ExecutionError`: Runtime execution errors  
- `ValidationError`: Data validation errors

## Usage Examples

### Creating a New Tool

```python
from tools.shared.common import ToolBase, ToolConfig, register_tool

class MyTool(ToolBase):
    def __init__(self):
        super().__init__(name="my-tool", version="1.0.0")
        register_tool(self.get_tool_config())
    
    def get_required_environment_vars(self):
        return ["DATABASE_URL", "API_KEY"]
    
    def get_tool_config(self):
        return ToolConfig(
            name="my-tool",
            version="1.0.0", 
            description="My custom tool",
            dependencies=["requests", "sqlalchemy"],
            environment_vars=["DATABASE_URL", "API_KEY"],
            usage_examples=["python tools/my_tool.py --help"],
            category="core"
        )
    
    def run(self):
        self.log_progress("Starting tool execution")
        
        try:
            # Get configuration
            db_url = self.get_config_value("DATABASE_URL", required=True)
            
            # Validate input
            self.validate_input(
                db_url,
                lambda x: x.startswith("mysql://"),
                "Database URL must start with mysql://"
            )
            
            # Do work...
            self.log_progress("Tool completed successfully")
            
        except Exception as e:
            self.handle_error(e, "main execution")

# Usage
if __name__ == "__main__":
    with MyTool() as tool:
        tool.run()
```

### Using the Registry

```python
from tools.shared.common import get_tool_registry, find_tool

# Get registry instance
registry = get_tool_registry()

# List all tools
all_tools = registry.list_tools()

# List tools by category
core_tools = registry.list_tools(category="core")

# Find specific tool
my_tool = find_tool("my-tool")
if my_tool:
    print(f"Found {my_tool.name} v{my_tool.version}")
```

### Error Handling

```python
from tools.shared.common import ConfigurationError, ValidationError

# Configuration errors
if not os.getenv("REQUIRED_VAR"):
    raise ConfigurationError("REQUIRED_VAR environment variable is required")

# Validation errors  
if not data.get("id"):
    raise ValidationError("Data must contain an 'id' field")

# Using tool's error handler
try:
    risky_operation()
except Exception as e:
    self.handle_error(e, "risky operation context")
```

## Best Practices

### Tool Development

1. **Always inherit from ToolBase** for consistency
2. **Register your tool** in the global registry
3. **Use standardized error types** for clear error handling
4. **Validate environment variables** in `get_required_environment_vars()`
5. **Provide comprehensive tool config** with examples and dependencies

### Configuration Management

1. **Use .env files** for local development configuration
2. **Validate required variables** early in tool initialization
3. **Provide sensible defaults** where appropriate
4. **Document all configuration options** in tool config

### Error Handling

1. **Fail fast and loud** - don't silently ignore errors
2. **Use appropriate error types** for different failure modes
3. **Provide context** when handling errors
4. **Log errors with full traceback** for debugging

### Logging

1. **Use the built-in logger** from ToolBase
2. **Log progress** at appropriate intervals
3. **Use appropriate log levels** (DEBUG, INFO, WARNING, ERROR)
4. **Include tool name** in log messages (handled automatically)

## Testing

The shared utilities include comprehensive tests in `tests/test_tools_shared_common.py`. When creating new tools:

1. **Test tool initialization** and configuration
2. **Test error handling** for various failure modes
3. **Test environment variable validation**
4. **Test tool registration** and discovery
5. **Mock external dependencies** for reliable tests

## Migration Guide

When migrating existing tools to use the shared utilities:

1. **Inherit from ToolBase** instead of custom base classes
2. **Replace custom logging** with ToolBase logging
3. **Use standardized error types** instead of generic exceptions
4. **Register tools** in the global registry
5. **Update tests** to use the new patterns

This ensures all tools follow consistent patterns and benefit from shared improvements.