# Project Structure & Organization

## Directory Layout

### Core Application Code
```
web/                           # Main ETL engine and business logic
├── youtube_channel_etl.py     # Primary ETL pipeline
├── sentiment_job.py           # Sentiment analysis pipeline
├── bulletproof_runner.py      # Fault-tolerant execution framework
├── etl_helpers.py            # Database utilities and helpers
├── youtube_integration.py     # YouTube API integration
└── etl_entrypoints.py        # High-level ETL orchestration
```

### Analytics Package
```
src/youtubeviz/              # Installable helper package for notebooks
├── __init__.py               # Public API exports
├── utils.py                  # Data filtering and utilities
├── charts.py                 # Plotly/Altair visualization functions
├── data.py                   # Data loading and computation helpers
└── bot_detection.py          # Comment bot detection algorithms
```

### Tools & Scripts
```
tools/                        # Operational and maintenance scripts
├── etl/                      # ETL execution scripts
│   ├── run_focused_etl.py    # Core ETL pipeline (recommended)
│   ├── run_etl_and_notebooks.py  # ETL + notebook execution
│   └── run_channels_from_env.py  # Process channels from environment
├── setup/                    # Database and environment setup
│   ├── create_tables.py      # Database schema initialization
│   └── setup_env.py          # Environment configuration
├── maintenance/              # Database cleanup and maintenance
└── monitor.py               # Data quality monitoring
```

### Analysis & Reporting
```
notebooks/                    # Jupyter notebook analytics
├── analysis/                 # Primary analysis notebooks
│   ├── 01_descriptive_overview.ipynb    # KPI dashboard
│   └── 02_artist_deepdives.ipynb        # Per-artist analysis
├── quality/                  # Data quality notebooks
│   └── 03_appendix_data_quality.ipynb   # QA checks
├── editable/                 # Working notebook versions
├── executed/                 # Output notebook versions
└── operations/               # Operational dashboards
```

### Configuration & Data
```
config/                       # Application configuration
├── artist_aliases.json       # Artist name normalization mapping
└── artist_colors.json        # Visualization color schemes

data/                         # Data storage (gitignored)
├── raw/                      # Raw API responses
├── processed/                # Cleaned/transformed data
└── exports/                  # Analysis outputs
```

### Testing & Quality
```
tests/                        # Test suite
├── test_youtube_channel_etl.py    # ETL pipeline tests
├── test_sentiment_*.py            # Sentiment analysis tests
├── test_data_consistency.py       # Data quality tests
└── conftest.py                    # Pytest configuration
```

## File Naming Conventions

### Python Modules
- **snake_case** for all Python files and modules
- **Descriptive names** indicating purpose (e.g., `youtube_channel_etl.py`)
- **Test files** prefixed with `test_` matching module names

### Notebooks
- **Numbered prefixes** for execution order (`01_`, `02_`, etc.)
- **Descriptive names** indicating analysis type
- **Executed versions** suffixed with `-executed.ipynb`

### Configuration Files
- **Lowercase with underscores** for JSON configs
- **Environment files** use `.env` pattern with automatic parsing
- **Channel URLs** stored as `YT_ARTISTNAME_YT` variables for automatic extraction
- **Documentation** uses `UPPERCASE.md` for important files

## Import Patterns

### Internal Imports
```python
# ETL modules
from web.youtube_channel_etl import YouTubeChannelETL
from web.etl_helpers import get_engine, read_sql_safe

# Analytics package
from youtubeviz.utils import filter_artists, safe_head
from youtubeviz.charts import views_over_time_plotly
```

### External Dependencies
```python
# Standard library first
import os
from datetime import datetime
from typing import Optional, List

# Third-party packages
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
```

## Data Flow Architecture

### ETL Pipeline Flow
1. **Raw Extraction** → `youtube_videos_raw` table
2. **Processing** → `youtube_videos`, `youtube_metrics` tables
3. **Sentiment Analysis** → `youtube_comments`, `youtube_sentiment_summary`
4. **Analytics** → Notebooks consume processed data via `youtubeviz`

### Database Schema Patterns
- **Raw tables** suffixed with `_raw` (JSON storage)
- **Processed tables** use clean names (`youtube_videos`)
- **Summary tables** suffixed with `_summary`
- **Metadata tables** for ETL tracking (`youtube_etl_runs`)

## Development Workflow

### Code Organization Principles
- **Separation of concerns**: ETL logic in `web/`, analytics in `src/`
- **Minimal dependencies**: Core ETL uses only `requests` + `pymysql`
- **Testable design**: Business logic separated from I/O operations
- **Configuration-driven**: Environment variables for all settings

### Notebook Organization
- **Editable versions** for development work
- **Executed versions** for sharing results
- **Modular design** using `youtubeviz` package for reusable functions
- **Clear categorization** by analysis type (analysis, quality, operations)
- **Storytelling approach**: Notebooks should be FUN and tell a compelling story
- **Interactive charts**: All visualizations should be interactive (Plotly/Altair)
- **Consistent colors**: Use global color schemes for artist/category consistency
- **Data science education**: Explain complex topics for students new to music industry
