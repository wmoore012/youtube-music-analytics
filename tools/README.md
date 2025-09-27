# 🛠️ YouTube Analytics Tools

**Professional tooling ecosystem for the YouTube Analytics platform**

[![Tools](https://img.shields.io/badge/tools-22+-green.svg)](.)
[![Categories](https://img.shields.io/badge/categories-5-blue.svg)](.)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](../tests/)

## 🚀 Quick Start

```bash
# System setup and initialization
python tools/core/unified_setup.py --full-setup

# System health check
python tools/core/unified_monitor.py --full-check

# System maintenance
python tools/core/unified_maintenance.py --cleanup-old
```

## 📋 Documentation

- **[📖 Comprehensive Guide](../docs/TOOLS_COMPREHENSIVE_GUIDE.md)** - Complete documentation with examples
- **[🔧 Configuration Guide](../docs/TOOLS_COMPREHENSIVE_GUIDE.md#configuration-guide)** - Environment setup and configuration
- **[🚨 Troubleshooting](../docs/TOOLS_COMPREHENSIVE_GUIDE.md#troubleshooting)** - Common issues and solutions
- **[🔄 Migration Guide](../docs/TOOLS_COMPREHENSIVE_GUIDE.md#migration-guide)** - Upgrading from legacy tools

This directory contains utilities and scripts for managing the YouTube ETL system, organized by purpose and usage frequency.

## Directory Structure

### Core Tools (`core/`)
Essential daily-use tools for system operation:
- **ETL Pipeline**: Unified ETL tool with focused, comprehensive, and channel-specific modes
- **System Setup**: Complete system initialization and configuration
- **System Monitor**: Health monitoring and data quality validation
- **Maintenance**: Database cleanup and maintenance operations

### Specialized Tools (`specialized/`)
Specific-purpose tools for specialized workflows:
- **Analytics** (`analytics/`): Data analysis, reporting, and visualization utilities
- **Migration** (`migration/`): Data migration and transformation tools
- **Benchmarking** (`benchmarking/`): Performance testing and evaluation tools

### Development Tools (`development/`)
Developer utilities for code quality and testing:
- **Code Quality** (`code_quality/`): Linting, formatting, and validation tools
- **Testing** (`testing/`): Test execution and management utilities
- **CI/CD** (`ci_enforcement/`): Continuous integration and deployment tools

### Shared Utilities (`shared/`)
Common base classes and utilities used by all tools:
- **ToolBase**: Standardized base class with logging and error handling
- **ToolConfig**: Configuration management and validation
- **ToolRegistry**: Tool discovery and registration system

### Legacy Tools (`legacy/`)
Deprecated tools with migration guidance (temporary during transition)

## Quick Start

### Daily Operations
```bash
# System health check
python tools/core/monitor.py --check-all

# Run focused ETL
python tools/core/etl.py --mode focused

# Database maintenance
python tools/core/maintenance.py --cleanup-old-data
```

### First-Time Setup
```bash
# Complete system setup
python tools/core/setup.py --full-setup

# Validate configuration
python tools/core/setup.py --validate-config
```

### Development Workflow
```bash
# Format code
python tools/development/code_quality/format_code.py

# Run tests
python tools/development/testing/run_tests.py

# Pre-commit validation
python tools/development/ci_enforcement/pre_commit.py
```

## Tool Discovery

Use the tool registry to discover available tools:

```python
from tools.shared.common import get_tool_registry

registry = get_tool_registry()
core_tools = registry.list_tools(category="core")
```

## Migration from Legacy Tools

If you're using old tool paths, see the migration guide in `legacy/README.md` for updated commands and new tool locations.

## Design Principles

- **Consistent Interface**: All tools follow standardized patterns
- **Clear Organization**: Tools grouped by purpose and usage frequency  
- **Robust Error Handling**: Graceful failure with clear error messages
- **Progress Feedback**: Clear indication of what's happening during execution
- **Configuration-Driven**: Behavior controlled through environment variables