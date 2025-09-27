# Unified Monitoring Tool

The unified monitoring tool consolidates all system monitoring functionality into a single, comprehensive tool that provides enterprise-grade monitoring capabilities for the YouTube Analytics platform.

## Overview

The `unified_monitor.py` tool replaces multiple scattered monitoring scripts with a single, well-tested tool that handles:

- Data quality monitoring and validation
- ETL pipeline health checks and status tracking
- System resource monitoring and performance metrics
- Enterprise-grade SLA monitoring and alerting
- Sentiment analysis monitoring and bot detection
- Automated issue detection and recovery suggestions

## Features

### Standardized Base Classes

Built on the shared `ToolBase` class, providing:
- Consistent logging with proper formatting
- Environment variable loading and validation
- Robust error handling with clear error types
- Progress reporting and status tracking
- Resource cleanup and context manager support

### Comprehensive Monitoring Capabilities

- **Quick Health Check**: Fast system status overview
- **Data Quality Monitoring**: Comprehensive data validation and issue detection
- **ETL Health Monitoring**: Pipeline status, performance, and error tracking
- **Performance Monitoring**: System metrics, trends, and performance analysis
- **Enterprise Monitoring**: SLA compliance, executive dashboards, and alerting
- **Sentiment Monitoring**: Sentiment analysis accuracy and bot detection
- **Full System Check**: Complete monitoring across all components

### Advanced Features

- **Automated Issue Detection**: Intelligent issue categorization and severity assessment
- **Recovery Suggestions**: Actionable recommendations for issue resolution
- **Trend Analysis**: Historical data analysis and performance trending
- **Executive Reporting**: High-level dashboards for management visibility
- **Multi-format Output**: Human-readable reports and JSON for automation

## Usage

### Basic Usage

```bash
# Quick health check (default)
python tools/core/unified_monitor.py

# Complete system monitoring
python tools/core/unified_monitor.py --full-check

# Specific monitoring types
python tools/core/unified_monitor.py --data-quality
python tools/core/unified_monitor.py --etl-status
python tools/core/unified_monitor.py --performance
python tools/core/unified_monitor.py --enterprise
python tools/core/unified_monitor.py --sentiment
```

### Advanced Options

```bash
# Extended analysis period
python tools/core/unified_monitor.py --performance --days 30

# Automatic issue fixing
python tools/core/unified_monitor.py --data-quality --fix-issues

# JSON output for automation
python tools/core/unified_monitor.py --full-check --json

# Verbose output for debugging
python tools/core/unified_monitor.py --etl-status --verbose
```

### Help and Options

```bash
# Show all available options
python tools/core/unified_monitor.py --help

# Get monitoring status
python tools/core/unified_monitor.py --json | jq '.summary'
```

## Monitoring Types

### 1. Quick Health Check

Fast overview of system status covering:
- Database connectivity
- Basic data counts and availability
- ETL pipeline status
- Data consistency validation

**Usage:**
```bash
python tools/core/unified_monitor.py
```

**Output:**
- Overall system status (HEALTHY/WARNING/CRITICAL)
- Component-level status summary
- Key metrics and counts
- Quick recommendations

### 2. Data Quality Monitoring

Comprehensive data validation including:
- Completeness validation (missing data detection)
- Consistency validation (referential integrity)
- Duplicate detection and handling
- Anomaly detection for unusual patterns
- Data freshness and staleness checks

**Usage:**
```bash
python tools/core/unified_monitor.py --data-quality
python tools/core/unified_monitor.py --data-quality --fix-issues
```

**Output:**
- Overall data quality score (0-100)
- Detailed issue breakdown by category and severity
- Sample records for investigation
- Automated fix suggestions
- Data quality statistics and trends

### 3. ETL Health Monitoring

Pipeline health and performance tracking:
- Environment variable validation
- Database connectivity and schema validation
- YouTube API credentials and quota status
- Data freshness validation
- ETL component availability
- System dependencies verification

**Usage:**
```bash
python tools/core/unified_monitor.py --etl-status
python tools/core/unified_monitor.py --etl-status --verbose
```

**Output:**
- ETL system health status
- Component-by-component validation results
- Recovery instructions for failed components
- Performance metrics and timing
- API usage and quota information

### 4. Performance Monitoring

System performance metrics and trend analysis:
- Database performance metrics
- ETL processing performance
- Data growth trends and patterns
- API usage metrics and efficiency
- Resource utilization tracking

**Usage:**
```bash
python tools/core/unified_monitor.py --performance
python tools/core/unified_monitor.py --performance --days 30
```

**Output:**
- Performance metrics across all components
- Trend analysis and growth patterns
- Performance alerts and recommendations
- Resource utilization statistics
- Bottleneck identification

### 5. Enterprise Monitoring

Executive-level monitoring and SLA tracking:
- Service health monitoring
- SLA compliance tracking
- Executive summary generation
- Enterprise alerting and escalation
- Management dashboards

**Usage:**
```bash
python tools/core/unified_monitor.py --enterprise
```

**Output:**
- Executive summary with key metrics
- SLA compliance status and trends
- Service health scores
- Critical issue identification
- Management-level recommendations

### 6. Sentiment Monitoring

Sentiment analysis system monitoring:
- Sentiment analysis accuracy tracking
- Bot detection performance
- Model performance validation
- Data quality for sentiment analysis
- Sentiment system health scoring

**Usage:**
```bash
python tools/core/unified_monitor.py --sentiment
```

**Output:**
- Sentiment system health score
- Analysis accuracy metrics
- Bot detection performance
- Model validation results
- Sentiment-specific alerts

### 7. Full System Check

Comprehensive monitoring across all components:
- Runs all monitoring types in sequence
- Provides complete system overview
- Generates comprehensive recommendations
- Suitable for scheduled health checks

**Usage:**
```bash
python tools/core/unified_monitor.py --full-check
python tools/core/unified_monitor.py --full-check --days 14 --fix-issues
```

**Output:**
- Complete system status across all components
- Comprehensive issue summary
- System-wide recommendations
- Performance and health trends
- Executive summary

## Output Formats

### Human-Readable Reports

Default output provides structured, easy-to-read reports with:
- Clear status indicators (✅ ⚠️ ❌)
- Organized sections (Summary, Components, Issues, Recommendations)
- Color-coded severity levels
- Actionable recommendations

### JSON Output

Machine-readable output for automation and integration:
```bash
python tools/core/unified_monitor.py --full-check --json
```

JSON structure includes:
- Timestamp and metadata
- Status and severity information
- Detailed metrics and statistics
- Issue details with fix suggestions
- Recommendations and alerts

## Status Levels

### System Status

- **HEALTHY**: All systems operating normally
- **WARNING**: Minor issues detected, system functional
- **CRITICAL**: Major issues requiring immediate attention
- **ERROR**: System errors preventing monitoring

### Issue Severity

- **CRITICAL**: System-breaking issues requiring immediate action
- **HIGH**: Significant issues affecting functionality
- **MEDIUM**: Moderate issues that should be addressed
- **LOW**: Minor issues for future consideration

## Integration and Automation

### Scheduled Monitoring

Set up regular monitoring with cron:
```bash
# Daily full system check
0 6 * * * /path/to/python tools/core/unified_monitor.py --full-check --json > /var/log/system-health.json

# Hourly quick health check
0 * * * * /path/to/python tools/core/unified_monitor.py --json >> /var/log/health-checks.log
```

### CI/CD Integration

Include monitoring in deployment pipelines:
```yaml
- name: System Health Check
  run: |
    python tools/core/unified_monitor.py --full-check --json > health-report.json
    if [ $? -ne 0 ]; then
      echo "System health check failed"
      exit 1
    fi
```

### Alerting Integration

Use JSON output for alerting systems:
```bash
# Check for critical issues
python tools/core/unified_monitor.py --json | jq '.status' | grep -q "CRITICAL" && send_alert.sh
```

### Programmatic Usage

Use the tool programmatically in other scripts:
```python
from tools.core.unified_monitor import SystemMonitor

with SystemMonitor() as monitor:
    # Quick health check
    health = monitor.quick_health_check()
    if health["status"] != "HEALTHY":
        print(f"System issue detected: {health['status']}")
    
    # Data quality check
    quality = monitor.data_quality_check()
    print(f"Data quality score: {quality['overall_score']}")
    
    # Get monitoring status
    status = monitor.get_monitoring_status()
    print(f"Monitoring session: {status['monitoring_session']}")
```

## Configuration

### Environment Variables

The tool requires these environment variables:
- `DB_HOST`: Database host
- `DB_USER`: Database username
- `DB_NAME`: Database name
- `YOUTUBE_API_KEY`: YouTube API key (for API monitoring)

### Optional Configuration

- `MONITORING_EMAIL_ENABLED`: Enable email alerts
- `MONITORING_SLACK_ENABLED`: Enable Slack notifications
- `MONITORING_WEBHOOK_URL`: Custom webhook for alerts

## Error Handling and Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```
   CRITICAL: Database connection failed: Access denied
   ```
   Solution: Check database credentials and connectivity

2. **Missing Environment Variables**
   ```
   ConfigurationError: Missing required environment variables: DB_HOST
   ```
   Solution: Set required environment variables in .env file

3. **Data Quality Issues**
   ```
   WARNING: Data quality score below threshold: 65%
   ```
   Solution: Run data quality fixes or investigate data sources

### Debugging

Use verbose mode for detailed troubleshooting:
```bash
python tools/core/unified_monitor.py --full-check --verbose
```

Check monitoring logs:
```bash
# View recent monitoring activity
tail -f logs/monitoring.log

# Check for specific errors
grep "ERROR" logs/monitoring.log
```

## Migration from Old Monitoring Tools

### Backward Compatibility

The old monitoring tools have been converted to wrappers that:
- Show deprecation warnings
- Redirect to the unified monitoring tool
- Map old arguments to new functionality

### Migration Steps

1. **Update Scripts**: Replace references to old monitoring tools
   ```bash
   # Old
   python tools/monitor.py --quality
   
   # New
   python tools/core/unified_monitor.py --data-quality
   ```

2. **Update Automation**: Update cron jobs and CI/CD scripts

3. **Update Documentation**: Update any documentation or runbooks

### Argument Mapping

| Old Tool | Old Argument | New Tool | New Argument |
|----------|--------------|----------|--------------|
| `monitor.py` | `--quality` | `unified_monitor.py` | `--data-quality` |
| `monitor.py` | `--consistency` | `unified_monitor.py` | (included in quick check) |
| `monitor.py` | `--etl-status` | `unified_monitor.py` | `--etl-status` |
| `monitor.py` | `--full-check` | `unified_monitor.py` | `--full-check` |

## Best Practices

### Development

1. **Regular Monitoring**: Run quick health checks frequently during development
2. **Data Quality Focus**: Monitor data quality after ETL changes
3. **Performance Tracking**: Use performance monitoring to identify bottlenecks

### Production

1. **Scheduled Monitoring**: Set up regular automated monitoring
2. **Alert Thresholds**: Configure appropriate alert thresholds for your environment
3. **Trend Analysis**: Use historical data to identify patterns and predict issues

### Operations

1. **Incident Response**: Use monitoring output to guide incident response
2. **Capacity Planning**: Use performance trends for capacity planning
3. **SLA Tracking**: Monitor SLA compliance for service level agreements

## Architecture

### Class Structure

```
SystemMonitor (extends ToolBase)
├── Quick Health Check
│   ├── Database connectivity
│   ├── Basic data counts
│   ├── Consistency validation
│   └── ETL status
├── Data Quality Monitoring
│   ├── Completeness validation
│   ├── Consistency checks
│   ├── Duplicate detection
│   ├── Anomaly detection
│   └── Freshness validation
├── ETL Health Monitoring
│   ├── Environment validation
│   ├── Database schema checks
│   ├── API credential validation
│   ├── Component availability
│   └── Dependency verification
├── Performance Monitoring
│   ├── Database performance
│   ├── ETL performance
│   ├── Growth trends
│   └── Resource utilization
├── Enterprise Monitoring
│   ├── Service health
│   ├── SLA compliance
│   ├── Executive reporting
│   └── Alert management
└── Sentiment Monitoring
    ├── Accuracy tracking
    ├── Bot detection
    ├── Model validation
    └── Health scoring
```

### Integration Points

```
Unified Monitor
├── Data Quality Validator
├── ETL Health Checker
├── Sentiment Monitor
├── Enterprise Monitor
└── Performance Analyzer
```

This unified monitoring tool provides a comprehensive, enterprise-grade monitoring solution that consolidates all monitoring functionality while maintaining backward compatibility and providing excellent user experience.