# 🎬 YouTube ETL & Sentiment Analysis Platform

**Professional-grade YouTube data pipeline with advanced sentiment analysis capabilities**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

## 🚀 Overview

A comprehensive ETL platform for YouTube data extraction, processing, and sentiment analysis. Features bulletproof execution, advanced analytics, and professional data visualizations.

### ✨ Key Features

- **🔄 YouTube Data ETL**: Complete pipeline for videos, metrics, and comments
- **😊 Sentiment Analysis**: VADER-based sentiment scoring with artist/channel grouping
- **📊 Interactive Visualizations**: Professional Plotly charts and dashboards
- **🛡️ Bulletproof Execution**: Robust error handling and daily rate limiting
- **🔍 Data Quality Monitoring**: Comprehensive diagnostics and health checks
- **🧹 Professional Text Processing**: Advanced cleaning and normalization

## 🏗️ Architecture

```
├── web/                    # Core ETL modules
│   ├── youtube_channel_etl.py      # Main ETL engine
│   ├── sentiment_job.py            # Sentiment analysis pipeline
│   ├── bulletproof_runner.py       # Fault-tolerant execution
│   └── etl_helpers.py              # Utility functions
├── tests/                  # Comprehensive test suite
├── tools/                  # Development utilities
├── create_tables.py        # Database schema setup
└── requirements.txt        # Python dependencies
```

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.8+
- MySQL 5.7+ or 8.0+
- YouTube Data API v3 key

### 2. Installation

```bash
# Clone the repository
git clone <repository-url>
cd youtube-etl-analysis

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your configuration
nano .env
```

Required environment variables:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_db_user
DB_PASS=your_db_password
DB_NAME=youtube_analytics

# YouTube API
YOUTUBE_API_KEY=your_youtube_api_key_here

# YouTube Data Retention Compliance (IMPORTANT!)
# Academic/Research use: 30 days recommended
# Educational use: 180 days maximum recommended
# Commercial use: Check current YouTube API ToS
YOUTUBE_DATA_RETENTION_DAYS=30
YOUTUBE_COMMENT_RETENTION_DAYS=30

# Channel URLs (add your channels)
YT_CHANNEL_1=https://www.youtube.com/@yourchannel1
YT_CHANNEL_2=https://www.youtube.com/@yourchannel2
```

### ⚖️ YouTube API Compliance

**IMPORTANT**: This platform includes built-in YouTube API Terms of Service compliance:

- **Data Retention Limits**: Automatically enforced based on your use case
- **User Content Protection**: Comments and user data have stricter retention policies
- **Configurable Policies**: Set retention periods via environment variables
- **Automatic Cleanup**: Built-in tools to maintain compliance

**Always review current [YouTube API Terms of Service](https://developers.google.com/youtube/terms/api-services-terms-of-service) for your specific use case.**

### 4. Database Setup

```bash
# Create database tables
python create_tables.py
```

### 5. Run ETL Pipeline

```bash
# Focused ETL (sentiment + quality + key notebooks)
python tools/etl/run_focused_etl.py

# ETL & Notebooks (full pipeline + notebook execution)
python tools/etl/run_etl_and_notebooks.py

# Ingest channels from .env (YT_* URLs)
python tools/etl/run_channels_from_env.py
```

## 📒 Analysis Notebooks

- Notebooks are organized for clarity:
  - `notebooks/analysis/01_descriptive_overview.ipynb` — KPI snapshot, trends, comparisons
  - `notebooks/analysis/02_artist_deepdives.ipynb` — one section per artist (parameterized list)
  - `notebooks/quality/03_appendix_data_quality.ipynb` — QA checks (dupes, nulls, outliers)

### Avoiding Artist Duplicates
- Set `ARTIST_ALIASES_JSON` in `.env` to unify common variants, e.g.:
  - `ARTIST_ALIASES_JSON={"LuvEnchantingINC":"Enchanting","enchanting":"Enchanting"}`
- The data loader applies DB `artist_aliases` plus your env mapping to normalize names.

## 👤 Artist Alias Manager (Important Step)

We made alias management simple and explicit — run it manually after ingestion (not in cron):

- Purpose: Merge duplicate names like `LuvEnchantingINC` → `Enchanting` in analytics.
- What it does:
  - Ensures a lowercase snake_case table `artist_aliases` exists
    - Table DDL (MySQL):
      - `artist_aliases(alias_id PK, artist_id FK, alias, is_preferred, created_at, updated_at)`
  - Guides you to pick a canonical artist and add aliases interactively
  - Verifies changes before applying
  - Upserts to DB and writes `config/artist_aliases.json`
- Run after ETL ingests videos/artists so you pick from real names:

```bash
python tools/alias_manager.py
```

Tips:
- Channels from `.env` (keys starting with `YT_`) are highlighted during selection.
- After finishing, copy `config/artist_aliases.json` into `.env` as `ARTIST_ALIASES_JSON` if you want env overrides.

Install the helper package and enable clean commits:

```bash
make dev   # pip install -e . && pre-commit install
```

Use helpers inside notebooks:

```python
from icatalogviz.utils import safe_head, filter_artists
from icatalogviz.charts import views_over_time_plotly, artist_compare_altair, linked_scatter_detail_altair
```

Pre-commit is configured to strip notebook outputs (`nbstripout`) and run formatters.

## 📊 Data Schema

### Core Tables

- **`youtube_videos_raw`**: Raw YouTube API responses
- **`youtube_videos`**: Processed video metadata
- **`youtube_metrics`**: Daily snapshots of view counts, likes, etc.
- **`youtube_comments`**: Comment data with sentiment scores
- **`youtube_sentiment_summary`**: Aggregated sentiment by video/channel

## 🎯 Usage Examples

### Basic ETL Execution

```python
from web.youtube_channel_etl import YouTubeChannelETL

etl = YouTubeChannelETL(
    api_key="your_api_key",
    db_host="localhost",
    db_port=3306,
    db_user="user",
    db_pass="password",
    db_name="youtube_db"
)

# Process channel data
summary = etl.run_for_channel("https://www.youtube.com/@channel")
```

### Sentiment Analysis

```python
from web.etl_entrypoints import run_sentiment_scoring

# Run sentiment analysis on all comments
stats = run_sentiment_scoring(
    batch_size=1000,
    loop=True,
    update_summary=True
)
print(f"Processed {stats['processed']} comments")
```

### Data Quality Monitoring

```python
from web.etl_helpers import analyze_data_quality

# Get comprehensive data quality report
quality_report = analyze_data_quality()
```

### YouTube API Compliance Management

```python
from web.youtube_data_retention import create_retention_job

# Check current data retention status
manager = create_retention_job()
status = manager.get_retention_status()

# Perform dry-run cleanup (see what would be deleted)
cleanup_stats = manager.cleanup_expired_data(dry_run=True)

# Actually cleanup expired data (USE WITH CAUTION)
# cleanup_stats = manager.cleanup_expired_data(dry_run=False)
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `YT_FETCH_COMMENTS` | Enable comment fetching | `1` |
| `YT_COMMENTS_PER_VIDEO` | Comments per video limit | `80` |
| `ETL_BATCH_SIZE` | Processing batch size | `50` |
| `SENTIMENT_VADER_WEIGHT` | VADER sentiment weight | `0.7` |

### Daily Rate Limiting

The platform includes built-in daily rate limiting to prevent API quota exhaustion:

- Each channel can only be processed once per day
- Configurable via `youtube_etl_runs` table
- Automatic reset at midnight

## 🧪 Testing

```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/test_youtube_channel_etl.py

# Run with coverage
python -m pytest --cov=web
```

## 📈 Data Analysis

The platform provides professional-grade analytics tools:

- **Artist/Channel Performance**: Comparative analysis across channels
- **Sentiment Trends**: Emotional response tracking over time
- **Engagement Metrics**: Views, likes, comments correlation analysis
- **Interactive Dashboards**: Plotly-based visualizations

## 🛡️ Production Considerations

### Security
- Never commit API keys or credentials
- Use environment variables for all secrets
- Implement proper database access controls

### Performance
- Built-in connection pooling
- Batch processing for large datasets
- Configurable rate limiting

### Monitoring
- Comprehensive logging throughout pipeline
- Data quality health checks
- ETL run status tracking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run pre-commit hooks
pre-commit install

# Run linting
flake8 web/ tests/
black web/ tests/
```

### Code Quality Standards

This project maintains professional code quality standards:

✅ **Automated Formatting**: Black with 120 character line length
✅ **Import Organization**: isort with black profile
✅ **Linting**: flake8 with custom rules for this codebase
✅ **Type Checking**: mypy static type analysis
✅ **Security Scanning**: bandit security analysis
✅ **Pre-commit Hooks**: Automated quality checks before each commit
✅ **CI/CD Pipeline**: GitHub Actions for continuous integration
✅ **YouTube API Compliance**: Built-in data retention policies

**Run quality checks locally:**
```bash
# Format code
black web/ --line-length=120
isort web/ --profile black

# Check quality
flake8 web/ --max-line-length=120
mypy web/ --ignore-missing-imports

# Security scan
bandit -r web/
```

### Testing Data Retention Policies

```bash
# Check current retention status
python -c "from web.youtube_data_retention import create_retention_job; print(create_retention_job().get_retention_status())"

# Dry-run cleanup (safe)
python -c "from web.youtube_data_retention import create_retention_job; create_retention_job().cleanup_expired_data(dry_run=True)"
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- YouTube Data API v3 for data access
- VADER Sentiment Analysis for emotion scoring
- Plotly for interactive visualizations
- MySQL for reliable data storage

## 📞 Support

For questions, issues, or contributions:

1. Check existing [Issues](../../issues)
2. Create a new issue with detailed description
3. Include logs and configuration (without secrets)

---

**Built with ❤️ for the YouTube analytics community**
