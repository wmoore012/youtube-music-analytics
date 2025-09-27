# Unified Setup Tool

The unified setup tool consolidates all system setup functionality into a single, robust tool that follows standardized patterns and provides comprehensive setup capabilities.

## Overview

The `unified_setup.py` tool replaces multiple scattered setup scripts with a single, well-tested tool that handles:

- Environment configuration (.env file setup)
- Database schema creation and validation
- System dependency verification
- Configuration validation and testing
- Initial data setup and validation

## Features

### Standardized Base Classes

Built on the shared `ToolBase` class, providing:
- Consistent logging with proper formatting
- Environment variable loading and validation
- Robust error handling with clear error types
- Progress reporting and status tracking
- Resource cleanup and context manager support

### Comprehensive Setup Options

- **Interactive Setup**: Guided setup with user prompts
- **Automated Setup**: Non-interactive setup using environment variables
- **Modular Operations**: Run individual setup steps as needed
- **Validation**: Comprehensive system validation and health checks
- **Status Reporting**: Detailed setup status and progress tracking

### Error Handling

- Clear error messages with context
- Standardized error types (`ConfigurationError`, `ExecutionError`, `ValidationError`)
- Graceful failure handling with cleanup
- Detailed logging for troubleshooting

## Usage

### Basic Usage

```bash
# Interactive setup (recommended for first-time setup)
python tools/core/unified_setup.py

# Complete automated setup
python tools/core/unified_setup.py --full-setup

# Check current setup status
python tools/core/unified_setup.py --check

# Show detailed status
python tools/core/unified_setup.py --status
```

### Individual Operations

```bash
# Environment setup only
python tools/core/unified_setup.py --env-only

# Database tables only
python tools/core/unified_setup.py --create-tables

# Non-interactive mode
python tools/core/unified_setup.py --full-setup --non-interactive

# Force overwrite existing setup
python tools/core/unified_setup.py --full-setup --force
```

### Help and Options

```bash
# Show all available options
python tools/core/unified_setup.py --help

# Verbose output for debugging
python tools/core/unified_setup.py --check --verbose
```

## Setup Process

### 1. Environment Configuration

The tool creates or updates a `.env` file with:

- **YouTube API Configuration**: API key and quota settings
- **Database Configuration**: Connection details and credentials
- **Artist Channel Configuration**: Pre-configured YouTube channels
- **ETL Configuration**: Pipeline settings and options
- **Logging Configuration**: Log levels and file settings

### 2. Database Setup

- Creates the target database if it doesn't exist
- Creates all required tables with proper schemas
- Sets up indexes and constraints
- Validates table creation success

### 3. System Validation

- Verifies .env file exists and is properly formatted
- Checks all required environment variables are set
- Tests database connectivity
- Validates table existence and structure
- Performs basic API key format validation

## Configuration

### Required Environment Variables

The tool requires these environment variables (created during setup):

- `YOUTUBE_API_KEY`: YouTube Data API v3 key
- `DB_HOST`: Database host (default: 127.0.0.1)
- `DB_PORT`: Database port (default: 3306)
- `DB_USER`: Database username
- `DB_PASS`: Database password
- `DB_NAME`: Database name (default: icatalog)

### Optional Configuration

- `YOUTUBE_QUOTA_LIMIT`: API quota limit (default: 10000)
- `YOUTUBE_MAX_RETRIES`: API retry attempts (default: 3)
- `ETL_MAX_VIDEOS_PER_ARTIST`: Video processing limit (default: 100)
- `LOG_LEVEL`: Logging level (default: INFO)

## Interactive vs Automated Setup

### Interactive Setup

Best for first-time setup or when you need to configure settings:

```bash
python tools/core/unified_setup.py
```

- Prompts for YouTube API key
- Asks for database credentials
- Offers to test configuration
- Provides helpful guidance and examples

### Automated Setup

Best for CI/CD, Docker, or scripted deployments:

```bash
# Set environment variables first
export YOUTUBE_API_KEY="your_api_key"
export DB_USER="your_db_user"
export DB_PASS="your_db_password"

# Run automated setup
python tools/core/unified_setup.py --full-setup --non-interactive
```

## Error Handling and Troubleshooting

### Common Issues

1. **Missing API Key**
   ```
   ConfigurationError: YouTube API key is required!
   ```
   Solution: Obtain API key from Google Cloud Console

2. **Database Connection Failed**
   ```
   ExecutionError: Database connection failed: Access denied
   ```
   Solution: Check database credentials and permissions

3. **Missing Environment Variables**
   ```
   ConfigurationError: Missing required environment variables: DB_USER
   ```
   Solution: Set required environment variables or run interactive setup

### Validation Failures

The tool performs comprehensive validation and reports specific issues:

```bash
python tools/core/unified_setup.py --check
```

Example output:
```
⚠️  Configuration Issues Found (2):
   ❌ Table youtube_videos missing
   ❌ YouTube API key format may be invalid
💡 Run setup commands to fix issues
```

### Debugging

Use verbose mode for detailed troubleshooting:

```bash
python tools/core/unified_setup.py --check --verbose
```

## Migration from Old Setup Tools

### Backward Compatibility

The old `setup.py` file has been converted to a wrapper that:
- Shows deprecation warnings
- Redirects to the unified setup tool
- Maintains compatibility with existing scripts

### Migration Steps

1. **Update Scripts**: Replace references to old setup tools
   ```bash
   # Old
   python tools/setup.py --create-tables
   
   # New
   python tools/core/unified_setup.py --create-tables
   ```

2. **Update Documentation**: Update any documentation or README files

3. **Update CI/CD**: Update deployment scripts and CI configurations

## Integration with Other Tools

### Tool Registry

The unified setup tool registers itself in the global tool registry:

```python
from tools.shared.common import find_tool

setup_tool_config = find_tool("unified-setup")
print(f"Found {setup_tool_config.name} v{setup_tool_config.version}")
```

### Programmatic Usage

Use the tool programmatically in other scripts:

```python
from tools.core.unified_setup import SystemSetup

with SystemSetup() as setup_tool:
    # Check if system is configured
    if setup_tool.validate_configuration():
        print("System is ready!")
    else:
        # Run setup if needed
        setup_tool.full_setup(interactive=False)
```

### Status Monitoring

Get detailed setup status for monitoring:

```python
from tools.core.unified_setup import SystemSetup

setup_tool = SystemSetup()
status = setup_tool.get_setup_status()
print(f"Database connected: {status['setup_state']['database_connected']}")
```

## Testing

The unified setup tool includes comprehensive tests:

```bash
# Run all setup tool tests
python -m pytest tests/test_unified_setup_tool.py -v

# Run specific test categories
python -m pytest tests/test_unified_setup_tool.py::TestEnvironmentSetup -v
python -m pytest tests/test_unified_setup_tool.py::TestDatabaseSetup -v
```

## Best Practices

### Development

1. **Use Interactive Setup**: For development environments, use interactive setup for better guidance
2. **Test Configuration**: Always run `--check` after setup to validate configuration
3. **Version Control**: Never commit `.env` files with real credentials

### Production

1. **Use Automated Setup**: Use non-interactive mode for production deployments
2. **Environment Variables**: Set credentials via environment variables, not .env files
3. **Validation**: Include setup validation in deployment health checks

### CI/CD

1. **Test Environment**: Use separate test database and API keys
2. **Validation**: Run setup validation as part of CI pipeline
3. **Cleanup**: Clean up test environments after CI runs

## Architecture

### Class Structure

```
SystemSetup (extends ToolBase)
├── Environment Setup
│   ├── Interactive configuration
│   ├── Automated configuration
│   └── .env file generation
├── Database Setup
│   ├── Database creation
│   ├── Table creation
│   └── Schema validation
├── Configuration Validation
│   ├── Environment variable checks
│   ├── Database connectivity tests
│   └── API key validation
└── Status Reporting
    ├── Setup state tracking
    ├── Progress monitoring
    └── Health status
```

### Error Hierarchy

```
ToolError (base)
├── ConfigurationError
│   ├── Missing environment variables
│   ├── Invalid configuration values
│   └── Missing required files
├── ExecutionError
│   ├── Database connection failures
│   ├── Table creation failures
│   └── API communication errors
└── ValidationError
    ├── Invalid input formats
    ├── Schema validation failures
    └── Data integrity issues
```

This unified setup tool provides a robust, well-tested foundation for system initialization that follows project standards and provides excellent user experience.