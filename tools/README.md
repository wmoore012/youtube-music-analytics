# 🛠️ YouTube Analytics Tools

Essential tools for managing your YouTube analytics pipeline.

## 🚀 Quick Start

### 1. Initial Setup
```bash
# Set up database and environment
python tools/setup.py

# Create database tables
python tools/setup.py --create-tables
```

### 2. Run ETL Pipeline
```bash
# Full ETL pipeline (recommended)
python tools/etl.py

# Quick focused ETL (faster, core data only)
python tools/etl.py --focused

# ETL with specific channels
python tools/etl.py --channels "Artist1,Artist2"
```

### 3. Monitor System Health
```bash
# Check data quality and pipeline health
python tools/monitor.py

# Run consistency checks
python tools/monitor.py --consistency

# Check recent ETL runs
python tools/monitor.py --etl-status
```

## 📁 Tool Categories

### 🔧 **Core Tools** (Use These Regularly)
- **`etl.py`** - Main ETL pipeline (combines focused + comprehensive)
- **`setup.py`** - Database setup and environment configuration
- **`monitor.py`** - Data quality monitoring and health checks

### 🗃️ **Specialized Tools** (Use When Needed)
- **`maintenance/`** - Database cleanup and maintenance scripts
- **`archive/`** - Legacy tools and one-off scripts

## 💡 Usage Examples

```bash
# Daily workflow
python tools/etl.py                    # Run ETL
python tools/monitor.py                # Check quality

# Weekly maintenance
python tools/monitor.py --full-check   # Deep quality check
python tools/maintenance/cleanup.py    # Clean old data

# Setup new environment
python tools/setup.py --full-setup     # Complete setup
```

## 🔍 Tool Details

Each tool includes:
- ✅ Comprehensive error handling
- 📊 Progress reporting
- 🔧 Configurable options
- 📝 Detailed logging
- ⚡ Performance optimization
