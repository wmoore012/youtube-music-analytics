# Core Tools

Essential daily-use tools for system operation and maintenance.

## Tools in this directory

### ETL Pipeline (`etl.py`)
Unified ETL tool consolidating multiple ETL scripts:
- Focused ETL for core data only
- Comprehensive ETL with all data sources  
- Channel-specific ETL operations
- Progress monitoring and error handling

**Usage:**
```bash
python tools/core/etl.py --mode focused
python tools/core/etl.py --mode comprehensive
python tools/core/etl.py --channels "artist1,artist2"
```

### System Setup (`setup.py`)
Complete system initialization and configuration:
- Database schema creation
- Environment configuration validation
- Dependency verification
- Initial data setup

**Usage:**
```bash
python tools/core/setup.py --init-database
python tools/core/setup.py --validate-config
python tools/core/setup.py --full-setup
```

### System Monitor (`monitor.py`)
Comprehensive health monitoring and diagnostics:
- Data quality validation
- ETL pipeline status monitoring
- System resource checks
- Performance metrics

**Usage:**
```bash
python tools/core/monitor.py --check-all
python tools/core/monitor.py --data-quality
python tools/core/monitor.py --etl-status
```

### Maintenance (`maintenance.py`)
Database cleanup and maintenance operations:
- Data retention policy enforcement
- Database optimization
- Cleanup of temporary data
- Archive management

**Usage:**
```bash
python tools/core/maintenance.py --cleanup-old-data
python tools/core/maintenance.py --optimize-database
python tools/core/maintenance.py --archive-data
```

## Design Principles

- **Single responsibility**: Each tool has a clear, focused purpose
- **Consistent interface**: All tools follow the same patterns and conventions
- **Robust error handling**: Graceful failure with clear error messages
- **Progress feedback**: Clear indication of what's happening during execution
- **Configuration-driven**: Behavior controlled through environment variables

## Migration from Legacy Tools

These core tools replace multiple scattered scripts:

| Legacy Tool | New Core Tool | Migration Notes |
|-------------|---------------|-----------------|
| `run_focused_etl.py` | `etl.py --mode focused` | Same functionality, cleaner interface |
| `run_comprehensive_etl.py` | `etl.py --mode comprehensive` | Enhanced error handling |
| `run_etl_and_notebooks.py` | `etl.py --with-notebooks` | Separated concerns |
| `tools/setup.py` | `setup.py` | Consolidated setup functions |
| `tools/monitor.py` | `monitor.py` | Enhanced monitoring capabilities |
| Various cleanup scripts | `maintenance.py` | Unified maintenance interface |

## Getting Started

1. **First-time setup**: `python tools/core/setup.py --full-setup`
2. **Daily ETL**: `python tools/core/etl.py --mode focused`
3. **Health check**: `python tools/core/monitor.py --check-all`
4. **Maintenance**: `python tools/core/maintenance.py --cleanup-old-data`