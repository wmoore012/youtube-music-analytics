# 🔄 Tool Migration Guide

**Complete guide for migrating from legacy tools to the new consolidated tooling system**

[![Migration](https://img.shields.io/badge/migration-required-orange.svg)](.)
[![Support](https://img.shields.io/badge/support-available-green.svg)](.)

## 📋 Table of Contents

- [Overview](#overview)
- [Migration Benefits](#migration-benefits)
- [Pre-Migration Assessment](#pre-migration-assessment)
- [Step-by-Step Migration](#step-by-step-migration)
- [Tool-Specific Migrations](#tool-specific-migrations)
- [Workflow Updates](#workflow-updates)
- [Validation & Testing](#validation--testing)
- [Troubleshooting](#troubleshooting)
- [Support & Resources](#support--resources)

## 🎯 Overview

The YouTube Analytics platform has consolidated its tooling ecosystem to provide:
- **Better reliability** with comprehensive error handling
- **Standardized interfaces** across all tools
- **Enhanced functionality** with new features and capabilities
- **Improved documentation** and user experience
- **Future-proof architecture** for ongoing development

This guide helps you migrate from legacy tools to the new consolidated system.

## ✅ Migration Benefits

### Before (Legacy Tools)
- ❌ Scattered tools with inconsistent interfaces
- ❌ Limited error handling and logging
- ❌ Fragmented documentation
- ❌ Difficult maintenance and updates
- ❌ No standardized configuration

### After (Consolidated Tools)
- ✅ **Unified interfaces** with consistent command-line options
- ✅ **Comprehensive error handling** with detailed diagnostics
- ✅ **Standardized logging** across all tools
- ✅ **Enhanced functionality** with new features
- ✅ **Better performance** and resource management
- ✅ **Comprehensive documentation** with examples
- ✅ **Future-proof design** for ongoing development

## 🔍 Pre-Migration Assessment

### 1. Inventory Current Usage

```bash
# Find all legacy tool references in your scripts
find . -name "*.sh" -o -name "*.py" -o -name "*.md" | xargs grep -l "run_etl.py\|monitor.py\|cleanup.py\|setup_system.py"

# Check cron jobs
crontab -l | grep -E "run_etl|monitor|cleanup|setup_system"

# Check documentation
grep -r "run_etl.py\|monitor.py\|cleanup.py" docs/ README.md
```

### 2. Assess Current Workflows

Document your current workflows:
- **Daily operations**: What tools do you run daily?
- **Scheduled tasks**: What's in your cron jobs?
- **Emergency procedures**: What tools do you use for troubleshooting?
- **Development workflows**: What tools are part of your dev process?

### 3. Check Dependencies

```bash
# List all legacy tool mappings
python tools/legacy/compatibility_wrapper.py --list-mappings

# Get comprehensive migration guide
python tools/legacy/compatibility_wrapper.py --migration-guide
```

## 🚀 Step-by-Step Migration

### Phase 1: Preparation (Week 1)

#### 1.1 Environment Setup
```bash
# Ensure you have the latest tools
git pull origin main

# Verify new tools are available
ls -la tools/core/
ls -la tools/specialized/

# Check environment requirements
python tools/core/unified_setup.py --list-requirements
```

#### 1.2 Backup Current Setup
```bash
# Backup current scripts
cp -r scripts/ scripts_backup_$(date +%Y%m%d)

# Backup cron jobs
crontab -l > crontab_backup_$(date +%Y%m%d).txt

# Backup configuration
cp .env .env.backup_$(date +%Y%m%d)
```

#### 1.3 Test New Tools
```bash
# Test core tools
python tools/core/unified_setup.py --help
python tools/core/unified_monitor.py --help
python tools/core/unified_maintenance.py --help

# Run health check
python tools/core/unified_monitor.py --full-check
```

### Phase 2: Core Tool Migration (Week 2)

#### 2.1 ETL Tools Migration

**Legacy → New Mapping:**
```bash
# Old
python run_etl.py --focused
python run_etl.py --comprehensive
python run_etl.py --channels

# New
python tools/core/unified_setup.py --run-etl --mode focused
python tools/core/unified_setup.py --run-etl --mode comprehensive
python tools/core/unified_setup.py --run-etl --channels
```

**Migration Steps:**
```bash
# 1. Test new ETL tool
python tools/core/unified_setup.py --run-etl --mode focused --dry-run

# 2. Update scripts
sed -i 's/run_etl.py --focused/tools\/core\/unified_setup.py --run-etl --mode focused/g' scripts/*.sh

# 3. Validate changes
bash scripts/daily_etl.sh --dry-run
```

#### 2.2 Monitoring Tools Migration

**Legacy → New Mapping:**
```bash
# Old
python monitor.py --health
python monitor.py --performance
python system_health.py

# New
python tools/core/unified_monitor.py --health-check
python tools/core/unified_monitor.py --performance-check
python tools/core/unified_monitor.py --full-check
```

**Migration Steps:**
```bash
# 1. Test new monitoring
python tools/core/unified_monitor.py --full-check

# 2. Update monitoring scripts
sed -i 's/monitor.py --health/tools\/core\/unified_monitor.py --health-check/g' scripts/*.sh

# 3. Update cron jobs
# Edit crontab manually to use new tools
crontab -e
```

#### 2.3 Maintenance Tools Migration

**Legacy → New Mapping:**
```bash
# Old
python cleanup.py --old-data
python cleanup.py --optimize
python database_maintenance.py

# New
python tools/core/unified_maintenance.py --cleanup-old
python tools/core/unified_maintenance.py --optimize-database
python tools/core/unified_maintenance.py --system-maintenance
```

### Phase 3: Specialized Tools Migration (Week 3)

#### 3.1 Analytics Tools

```bash
# Old
python sentiment_analysis.py --analyze
python sentiment_analysis.py --batch

# New
python tools/specialized/analytics/sentiment_analysis_tool.py --run-analysis
python tools/specialized/analytics/sentiment_analysis_tool.py --batch-process
```

#### 3.2 Benchmarking Tools

```bash
# Old
python benchmark.py --models
python benchmark.py --system

# New
python tools/specialized/benchmarking/unified_benchmark_tool.py --model-benchmark
python tools/specialized/benchmarking/unified_benchmark_tool.py --system-benchmark
```

#### 3.3 Migration Tools

```bash
# Old
python migrate_data.py --to-file
python migrate_data.py --to-db

# New
python tools/specialized/migration/storage_migrator.py --db-to-file
python tools/specialized/migration/storage_migrator.py --file-to-db
```

### Phase 4: Validation & Cleanup (Week 4)

#### 4.1 Comprehensive Testing
```bash
# Test all migrated workflows
bash scripts/daily_etl.sh
bash scripts/weekly_maintenance.sh
bash scripts/monitoring_check.sh

# Run system health check
python tools/core/unified_monitor.py --full-check

# Validate all functionality
python tools/core/unified_setup.py --validate-system
```

#### 4.2 Documentation Updates
```bash
# Update README files
# Update internal documentation
# Update runbooks and procedures
# Update team training materials
```

#### 4.3 Legacy Cleanup
```bash
# Remove legacy tool references (after validation)
# Clean up old scripts
# Update cron jobs
# Archive old documentation
```

## 🔧 Tool-Specific Migrations

### ETL Pipeline Migration

#### Current State Assessment
```bash
# Check current ETL usage
grep -r "run_etl" scripts/ cron/
grep -r "youtube_channel_etl" scripts/ cron/
```

#### Migration Commands
```bash
# Focused ETL
# Old: python run_etl.py --focused
# New: python tools/core/unified_setup.py --run-etl --mode focused

# Comprehensive ETL  
# Old: python run_etl.py --comprehensive
# New: python tools/core/unified_setup.py --run-etl --mode comprehensive

# Channel-specific ETL
# Old: python run_etl.py --channels channel1,channel2
# New: python tools/core/unified_setup.py --run-etl --channels channel1,channel2
```

#### Configuration Changes
```bash
# New environment variables (add to .env)
ETL_MODE=focused
ETL_BATCH_SIZE=1000
ETL_TIMEOUT_SECONDS=3600

# Validate configuration
python tools/core/unified_setup.py --validate-environment
```

### Monitoring Migration

#### Current State Assessment
```bash
# Check current monitoring setup
crontab -l | grep monitor
grep -r "monitor.py" scripts/
```

#### Migration Commands
```bash
# System health check
# Old: python monitor.py --health
# New: python tools/core/unified_monitor.py --health-check

# Performance monitoring
# Old: python monitor.py --performance  
# New: python tools/core/unified_monitor.py --performance-check

# Comprehensive check
# Old: python monitor.py --all
# New: python tools/core/unified_monitor.py --full-check
```

#### Enhanced Features
```bash
# New monitoring capabilities
python tools/core/unified_monitor.py --database-health
python tools/core/unified_monitor.py --api-connectivity
python tools/core/unified_monitor.py --resource-usage
```

### Maintenance Migration

#### Current State Assessment
```bash
# Check current maintenance tasks
crontab -l | grep cleanup
grep -r "cleanup.py" scripts/
```

#### Migration Commands
```bash
# Data cleanup
# Old: python cleanup.py --old-data
# New: python tools/core/unified_maintenance.py --cleanup-old

# Database optimization
# Old: python cleanup.py --optimize
# New: python tools/core/unified_maintenance.py --optimize-database

# System maintenance
# Old: python database_maintenance.py
# New: python tools/core/unified_maintenance.py --system-maintenance
```

#### New Maintenance Features
```bash
# Enhanced maintenance capabilities
python tools/core/unified_maintenance.py --retention-cleanup
python tools/core/unified_maintenance.py --performance-optimization
python tools/core/unified_maintenance.py --health-check
```

## 🔄 Workflow Updates

### Daily Operations Workflow

#### Before (Legacy)
```bash
#!/bin/bash
# daily_operations.sh (legacy)

# Morning health check
python monitor.py --health

# Run ETL
python run_etl.py --focused

# Evening cleanup
python cleanup.py --old-data
```

#### After (Consolidated)
```bash
#!/bin/bash
# daily_operations.sh (new)

# Morning comprehensive check
python tools/core/unified_monitor.py --full-check

# Run ETL with better error handling
python tools/core/unified_setup.py --run-etl --mode focused

# Evening maintenance
python tools/core/unified_maintenance.py --cleanup-old
```

### Weekly Maintenance Workflow

#### Before (Legacy)
```bash
#!/bin/bash
# weekly_maintenance.sh (legacy)

python cleanup.py --optimize
python database_maintenance.py
python monitor.py --performance
```

#### After (Consolidated)
```bash
#!/bin/bash
# weekly_maintenance.sh (new)

# Comprehensive maintenance
python tools/core/unified_maintenance.py --optimize-database
python tools/core/unified_maintenance.py --system-maintenance

# Performance monitoring
python tools/core/unified_monitor.py --performance-check

# Run benchmarks
python tools/specialized/benchmarking/unified_benchmark_tool.py --system-benchmark
```

### Cron Job Updates

#### Before (Legacy)
```bash
# Legacy cron jobs
0 6 * * * cd /path/to/project && python run_etl.py --focused
0 12 * * * cd /path/to/project && python monitor.py --health
0 2 * * 0 cd /path/to/project && python cleanup.py --optimize
```

#### After (Consolidated)
```bash
# New cron jobs
0 6 * * * cd /path/to/project && python tools/core/unified_setup.py --run-etl --mode focused
0 12 * * * cd /path/to/project && python tools/core/unified_monitor.py --health-check
0 2 * * 0 cd /path/to/project && python tools/core/unified_maintenance.py --optimize-database
```

## ✅ Validation & Testing

### Pre-Migration Testing
```bash
# Test compatibility wrapper
python tools/legacy/compatibility_wrapper.py run_etl --focused

# Verify argument mapping
python tools/legacy/compatibility_wrapper.py monitor --health

# Check all mappings
python tools/legacy/compatibility_wrapper.py --list-mappings
```

### Post-Migration Validation
```bash
# Validate system health
python tools/core/unified_monitor.py --full-check

# Test all workflows
bash scripts/daily_operations.sh --dry-run
bash scripts/weekly_maintenance.sh --dry-run

# Verify functionality
python tools/core/unified_setup.py --validate-system
```

### Regression Testing
```bash
# Compare outputs (if applicable)
# Run old and new tools side by side
# Verify data consistency
# Check performance metrics
```

## 🚨 Troubleshooting

### Common Migration Issues

#### Environment Variable Issues
```bash
# Problem: New tools require different environment variables
# Solution: Check requirements and update .env
python tools/core/unified_setup.py --list-requirements
python tools/core/unified_setup.py --validate-environment
```

#### Path Issues
```bash
# Problem: Scripts can't find new tools
# Solution: Use absolute paths or update PATH
export PATH="$PATH:$(pwd)/tools/core:$(pwd)/tools/specialized"
```

#### Configuration Issues
```bash
# Problem: New tools use different configuration format
# Solution: Migrate configuration
python tools/core/unified_setup.py --migrate-config
```

#### Permission Issues
```bash
# Problem: New tools don't have execute permissions
# Solution: Fix permissions
chmod +x tools/core/*.py
chmod +x tools/specialized/*/*.py
```

### Rollback Procedures

If you need to rollback during migration:

```bash
# Restore scripts
cp -r scripts_backup_YYYYMMDD/* scripts/

# Restore cron jobs
crontab crontab_backup_YYYYMMDD.txt

# Restore configuration
cp .env.backup_YYYYMMDD .env

# Use compatibility wrapper temporarily
python tools/legacy/compatibility_wrapper.py run_etl --focused
```

### Getting Help

#### Diagnostic Commands
```bash
# Check system status
python tools/core/unified_monitor.py --full-check

# Validate environment
python tools/core/unified_setup.py --validate-environment

# Check tool status
python tools/core/unified_monitor.py --tool-status
```

#### Debug Mode
```bash
# Enable debug logging
export DEBUG_MODE=true
export LOG_LEVEL=DEBUG

# Run with verbose output
python tools/core/unified_setup.py --run-etl --verbose
```

## 📚 Support & Resources

### Documentation
- **[Comprehensive Tools Guide](TOOLS_COMPREHENSIVE_GUIDE.md)** - Complete tool documentation
- **[Legacy Tools README](../tools/legacy/README.md)** - Legacy tool information
- **[Development Standards](DEVELOPMENT_STANDARDS.md)** - Development guidelines

### Migration Tools
```bash
# Compatibility wrapper
python tools/legacy/compatibility_wrapper.py --help

# Migration guide
python tools/legacy/compatibility_wrapper.py --migration-guide

# Tool mappings
python tools/legacy/compatibility_wrapper.py --list-mappings
```

### Support Channels
1. **Documentation**: Check this guide and tool help (`--help`)
2. **Compatibility Wrapper**: Use for testing and guidance
3. **System Diagnostics**: Run health checks and validation
4. **Issue Tracking**: Create detailed issue reports with logs

### Best Practices
- **Test thoroughly** before deploying to production
- **Migrate incrementally** rather than all at once
- **Keep backups** of working configurations
- **Document changes** for your team
- **Validate functionality** after each migration step

---

**Migration Timeline**: Plan for 2-4 weeks depending on complexity  
**Support Available**: Use compatibility wrapper and comprehensive documentation  
**Rollback Ready**: Keep backups and use compatibility wrapper if needed