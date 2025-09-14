# Folder Structure Audit Report

## Current State Analysis

### ✅ Well-Organized Sections
- **`src/icatalogviz/`** - Core library code (good separation)
- **`notebooks/`** - Analysis notebooks with executed versions
- **`tests/`** - Comprehensive test coverage
- **`web/`** - ETL and data processing modules
- **`tools/`** - Utility scripts and pipeline tools

### ⚠️ Areas for Improvement

#### Root Directory Cleanup Needed
- **Too many loose files in root** (19 files)
- **Temporary/development files** should be moved or removed:
  - `ETL.ipynb` → move to `notebooks/development/`
  - `cleanup_db.py` → move to `tools/maintenance/`
  - `create_tables.py` → move to `tools/setup/`
  - `docker_setup.py` → move to `tools/setup/`
  - `setup_env.py` → move to `tools/setup/`
  - `import subprocess.py` → remove (appears to be temp file)
  - `sentiment_plot_test.html` → remove or move to `reports/`
  - `.yt_local.db` → should be in `data/` or ignored

#### Inconsistent Naming
- **Mixed naming conventions**: Some files use underscores, others use hyphens
- **Inconsistent capitalization**: `DOCS/` vs `logs/`

#### Missing Structure
- **No `data/` directory** for local data files
- **No `config/` vs `configs/`** inconsistency
- **No `scripts/deployment/`** for production scripts

## Recommended Structure

```
youtube-etl-analysis/
├── README.md
├── pyproject.toml
├── requirements.txt
├── .env.example
├── .gitignore
├── Makefile
│
├── src/
│   └── icatalogviz/           # Core library
│       ├── __init__.py
│       ├── data.py            # Data loading functions
│       ├── charts.py          # Visualization functions
│       ├── bot_detection.py   # Bot detection system
│       └── utils.py           # Utility functions
│
├── web/                       # ETL and data processing
│   ├── __init__.py
│   ├── etl_helpers.py         # Core ETL functions
│   ├── youtube_integration.py # YouTube API integration
│   ├── sentiment_job.py       # Sentiment analysis jobs
│   └── data_retention.py      # Data management
│
├── tools/                     # Utility scripts
│   ├── etl/                   # ETL-specific tools
│   │   ├── run_comprehensive_etl.py
│   │   └── sentiment_analysis.py
│   ├── setup/                 # Setup and initialization
│   │   ├── create_tables.py
│   │   ├── setup_env.py
│   │   └── docker_setup.py
│   ├── maintenance/           # Maintenance scripts
│   │   └── cleanup_db.py
│   └── notebooks/             # Notebook utilities
│       └── run_notebooks.py
│
├── notebooks/                 # Analysis notebooks
│   ├── production/            # Production-ready notebooks
│   │   ├── 01_descriptive_overview.ipynb
│   │   ├── 02_artist_comparison.ipynb
│   │   └── 03_data_quality.ipynb
│   ├── development/           # Development notebooks
│   │   └── ETL.ipynb
│   ├── executed/              # Executed notebook outputs
│   └── templates/             # Notebook templates
│
├── tests/                     # Test suite
│   ├── unit/                  # Unit tests
│   ├── integration/           # Integration tests
│   └── fixtures/              # Test data
│
├── config/                    # Configuration files
│   ├── artist_colors.json
│   └── bot_detection.yaml
│
├── data/                      # Local data files (gitignored)
│   ├── raw/                   # Raw data files
│   ├── processed/             # Processed data
│   └── exports/               # Data exports
│
├── reports/                   # Generated reports
│   ├── data_quality/
│   ├── performance/
│   └── bot_analysis/
│
├── docs/                      # Documentation (lowercase)
│   ├── architecture/
│   ├── api/
│   └── guides/
│
├── scripts/                   # Deployment and automation
│   ├── deployment/
│   │   └── deploy.sh
│   └── automation/
│       └── nightly_etl.sh
│
└── .github/                   # GitHub workflows
    └── workflows/
```

## Immediate Actions Needed

### 1. Root Directory Cleanup (High Priority)
```bash
# Move development files
mkdir -p notebooks/development
mv ETL.ipynb notebooks/development/

# Move setup files
mkdir -p tools/setup
mv create_tables.py tools/setup/
mv docker_setup.py tools/setup/
mv setup_env.py tools/setup/

# Move maintenance files
mkdir -p tools/maintenance
mv cleanup_db.py tools/maintenance/

# Remove temporary files
rm -f "import subprocess.py"
rm -f sentiment_plot_test.html
```

### 2. Create Missing Directories
```bash
mkdir -p data/{raw,processed,exports}
mkdir -p reports/{data_quality,performance,bot_analysis}
mkdir -p config
mkdir -p scripts/{deployment,automation}
```

### 3. Rename for Consistency
```bash
mv DOCS docs
mv configs/* config/
rmdir configs
```

## Code Organization Guidelines

### File Naming Conventions
- **Python files**: `snake_case.py`
- **Notebooks**: `##_descriptive_name.ipynb`
- **Config files**: `snake_case.json/yaml`
- **Scripts**: `snake_case.sh`

### Module Structure
- **Maximum 500 lines per file**
- **Clear separation of concerns**
- **Comprehensive docstrings**
- **Type hints for all functions**

### Import Organization
```python
# Standard library
import os
import sys
from datetime import datetime

# Third-party
import pandas as pd
import numpy as np

# Local imports
from icatalogviz.data import load_data
from web.etl_helpers import get_engine
```

## Git Repository Preparation

### Files to Add to .gitignore
```
# Data files
data/
*.db
*.sqlite

# Temporary files
*.tmp
*.temp
sentiment_plot_test.html

# IDE files
.vscode/
.idea/
*.swp
*.swo

# OS files
.DS_Store
Thumbs.db
```

### Pre-commit Hooks Needed
- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking
- **pytest** for running tests

## Migration Plan

### Phase 1: Immediate Cleanup (This Session)
1. Move loose files to appropriate directories
2. Create missing directory structure
3. Update imports in affected files
4. Test that everything still works

### Phase 2: Code Quality (Next Session)
1. Add comprehensive type hints
2. Improve docstrings
3. Refactor large functions
4. Add missing tests

### Phase 3: Modularization (Future)
1. Split into separate Git repositories:
   - `youtube-etl-core` (ETL pipeline)
   - `music-analytics-notebooks` (Analysis notebooks)
   - `bot-detection-system` (Bot detection)
   - `data-visualization-tools` (Charts and viz)

## Benefits of This Structure

### Developer Experience
- **Clear separation of concerns**
- **Easy to find relevant code**
- **Consistent naming conventions**
- **Proper dependency management**

### Maintainability
- **Modular architecture**
- **Clear testing strategy**
- **Comprehensive documentation**
- **Version control friendly**

### Scalability
- **Easy to add new features**
- **Clear extension points**
- **Proper configuration management**
- **Ready for CI/CD integration**
