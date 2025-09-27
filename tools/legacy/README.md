# 🔄 Legacy Tools & Backward Compatibility

**Deprecated tools and compatibility wrappers for migration to consolidated tooling**

[![Status](https://img.shields.io/badge/status-deprecated-red.svg)](.)
[![Migration](https://img.shields.io/badge/migration-required-orange.svg)](.)

Deprecated tools with migration guidance to new consolidated tools.

## ⚠️ Deprecation Notice

All tools in this directory are deprecated and will be removed in a future version.
Please migrate to the new consolidated tools in the appropriate directories:
- `tools/core/` for essential daily-use tools
- `tools/specialized/` for specific-purpose tools  
- `tools/development/` for development utilities

## Migration Guide

### ETL Tools
| Legacy Tool | New Tool | Migration Command |
|-------------|----------|-------------------|
| `run_focused_etl.py` | `tools/core/etl.py` | `python tools/core/etl.py --mode focused` |
| `run_comprehensive_etl.py` | `tools/core/etl.py` | `python tools/core/etl.py --mode comprehensive` |
| `run_etl_and_notebooks.py` | `tools/core/etl.py` | `python tools/core/etl.py --with-notebooks` |

### Setup and Monitoring Tools
| Legacy Tool | New Tool | Migration Command |
|-------------|----------|-------------------|
| `tools/setup.py` | `tools/core/setup.py` | `python tools/core/setup.py --full-setup` |
| `tools/monitor.py` | `tools/core/monitor.py` | `python tools/core/monitor.py --check-all` |

### Analytics Tools
| Legacy Tool | New Tool | Migration Command |
|-------------|----------|-------------------|
| Analytics scripts | `tools/specialized/analytics/` | See analytics directory README |

### Development Tools
| Legacy Tool | New Tool | Migration Command |
|-------------|----------|-------------------|
| Code quality scripts | `tools/development/code_quality/` | See code quality directory README |
| Testing utilities | `tools/development/testing/` | See testing directory README |

## Deprecation Timeline

- **Phase 1** (Current): Legacy tools available with deprecation warnings
- **Phase 2** (Next month): Legacy tools moved to this directory
- **Phase 3** (Month 2): Legacy tools provide migration guidance only
- **Phase 4** (Month 3): Legacy tools removed completely

## Getting Help

If you need help migrating from a legacy tool:

1. Check the migration table above for the new equivalent
2. Read the README in the new tool's directory
3. Run the new tool with `--help` for usage information
4. Consult the main tools README for general guidance

## Wrapper Scripts

Some legacy tools are provided as wrapper scripts that:
1. Display a deprecation warning
2. Explain the migration path
3. Optionally delegate to the new tool

These wrappers help ensure existing scripts continue to work during the transition period.