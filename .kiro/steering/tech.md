# Technology Stack & Build System

## Core Technologies

### Backend Stack
- **Python 3.8+**: Primary language (3.10+ recommended for development)
- **MySQL 5.7+/8.0+**: Primary database for analytics data
- **SQLAlchemy 2.0+**: Database ORM and connection management
- **PyMySQL**: MySQL database connector
- **Requests**: HTTP client for YouTube API calls

### Analytics & Visualization
- **Pandas 2.2+**: Data manipulation and analysis
- **Plotly 5.20+**: Interactive visualizations and dashboards
- **Altair 5.0+**: Statistical visualization grammar
- **VADER Sentiment**: Comment sentiment analysis
- **Jupyter**: Notebook-based analytics workflow

### Development Tools
- **Black**: Code formatting (120 character line length)
- **isort**: Import sorting (black profile)
- **flake8**: Linting with custom rules
- **mypy**: Static type checking
- **pre-commit**: Automated quality checks
- **pytest**: Testing framework with 30s timeout

## Build System

### Package Management
```bash
# Install core package in development mode
pip install -e .

# Install all dependencies
pip install -r requirements.txt

# Development setup with pre-commit hooks
make dev
```

### Common Commands

#### Development Workflow
```bash
# Format code
make format          # black + isort
black . --line-length=120
isort . --profile black

# Quality checks
make lint           # flake8
make typecheck      # mypy
flake8 --max-line-length=120

# Testing
make test           # pytest -q
pytest -q --timeout=30

# Notebook management
make nbstrip        # Strip notebook outputs
```

#### ETL Operations
```bash
# Main ETL pipelines
python tools/etl/run_focused_etl.py        # Core ETL (recommended)
python tools/etl/run_etl_and_notebooks.py  # ETL + notebook execution
python tools/etl/run_channels_from_env.py  # Process channels from .env

# Database setup
python tools/setup/create_tables.py        # Initialize database schema

# Monitoring
python tools/monitor.py                    # Data quality checks
```

#### Notebook Workflow
```bash
# Execute notebooks
python tools/run_notebooks.py

# Patch notebook outputs
python tools/patch_notebooks.py
```

## Configuration

### Environment Setup
- Copy `.env.example` to `.env`
- Required: `YOUTUBE_API_KEY`, database credentials, channel URLs
- YouTube compliance: `YOUTUBE_DATA_RETENTION_DAYS=30`

### Code Quality Standards
- **Line length**: 120 characters (keep LOC below recommended amounts)
- **Import style**: Black profile with isort
- **Type hints**: Required for public APIs
- **Docstrings**: Required for modules and classes
- **Testing**: TDD approach with comprehensive test coverage
- **Database columns**: Always use lowercase_snake_case
- **Booleans**: Avoid when possible, prefer descriptive strings/enums
- **Error handling**: Fail loudly with clear error messages
- **Code organization**: Human-readable structure, use helper functions
- **Comments**: Extensive commenting, especially for complex logic or potential mistakes
- **No bloated AI code**: Keep implementations clean and purposeful
- **No fallback code**: Be explicit about requirements and dependencies

### Pre-commit Hooks
- Trailing whitespace removal
- YAML validation
- Black formatting
- isort import sorting
- flake8 linting
- mypy type checking
- nbstripout (notebook output stripping)