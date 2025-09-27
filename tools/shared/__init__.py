"""
Shared utilities and base classes for all tools.

This module provides common functionality that all tools should use:
- ToolBase: Base class with standardized logging, configuration, and error handling
- ToolConfig: Configuration dataclass for tool metadata
- ToolRegistry: System for tool discovery and validation
- Standardized error classes for consistent error handling
"""

from .common import (
    ConfigurationError,
    ExecutionError,
    ToolBase,
    ToolConfig,
    ToolError,
    ToolRegistry,
    ValidationError,
)

__all__ = [
    "ToolBase",
    "ToolConfig",
    "ToolRegistry",
    "ToolError",
    "ConfigurationError",
    "ExecutionError",
    "ValidationError",
]
