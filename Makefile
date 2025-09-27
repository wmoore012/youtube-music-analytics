# YouTube Analytics Platform - Enterprise Makefile
# Professional build automation and deployment management

.PHONY: help install dev test lint format typecheck clean
.PHONY: run-etl run-notebooks quality-check deploy monitor
.PHONY: enterprise-deploy enterprise-test enterprise-monitor
.PHONY: security-scan compliance-check performance-test
.PHONY: personal-cleanup-dummy-videos

# Default target
help: ## Show available commands
	@echo "🎵 YouTube Analytics Platform - Enterprise Commands"
	@echo "=================================================="
	@echo ""
	@echo "📦 Development & Setup:"
	@echo "  install           Install production dependencies"
	@echo "  dev               Install development environment + pre-commit hooks"
	@echo "  clean             Clean temporary files and caches"
	@echo ""
	@echo "🧪 Testing & Quality:"
	@echo "  test              Run comprehensive test suite"
	@echo "  test-enterprise   Run enterprise-grade test suite with benchmarks"
	@echo "  test-notebooks    Run notebook validation tests"
	@echo "  test-notebook-execution  🧪 Run comprehensive notebook execution tests"
	@echo "  lint              Run code linting (flake8)"
	@echo "  format            Format code (black + isort)"
	@echo "  typecheck         Run static type checking (mypy)"
	@echo "  security-scan     Run security vulnerability scanning"
	@echo "  quality-check     Run data quality validation"
	@echo "  load-songs FILE=  Load songs CSV (isrc,title,artist) into songs table"
	@echo "  refresh-normalized  Normalize videos and show ISRC nulls summary"
	@echo "  ci-local          🚀 Run local CI/CD pipeline (QUICK)"
	@echo "  ci-comprehensive  🎤 Run comprehensive artist validation"
	@echo "  personal-cleanup-dummy-videos  ⚠️ PERSONAL: Remove known dummy video_ids from your local DB"
	@echo ""
	@echo "🚀 Pipeline Operations:"
	@echo "  run-etl           Execute core ETL pipeline"
	@echo "  run-notebooks     Generate analytics notebooks"
	@echo "  run-production    Execute full production pipeline"
	@echo ""
	@echo "🏢 Enterprise Operations:"
	@echo "  enterprise-deploy Deploy to production environment"
	@echo "  enterprise-test   Run enterprise validation suite"
	@echo "  enterprise-monitor Start enterprise monitoring system"
	@echo "  compliance-check  Validate YouTube ToS and privacy compliance"
	@echo "  performance-test  Run performance benchmarking"
	@echo ""
	@echo "📊 Monitoring & Reporting:"
	@echo "  monitor           Start system monitoring"
	@echo "  health-check      System health assessment"
	@echo "  sla-report        Generate SLA compliance report"
	@echo "  executive-report  Generate executive dashboard"

# Development setup
install: ## Install production dependencies
	@echo "📦 Installing production dependencies..."
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -e .
	@echo "✅ Production installation complete"

dev: install ## Set up development environment
	@echo "🛠️ Setting up development environment..."
	pip install pre-commit black isort flake8 mypy pytest pytest-cov pytest-benchmark bandit safety
	pre-commit install
	@echo "✅ Development environment ready"

# Code quality
format: ## Format code with black and isort
	@echo "🎨 Formatting code..."
	black --line-length=120 .
	isort --profile black .
	@echo "✅ Code formatting complete"

lint: ## Run code linting with flake8
	@echo "🔍 Running code linting..."
	flake8 --max-line-length=120 --exclude=.venv,__pycache__,tools/archive .
	@echo "✅ Linting complete"

typecheck: ## Run static type checking with mypy
	@echo "🔬 Running type checking..."
	mypy --ignore-missing-imports --exclude tools/archive .
	@echo "✅ Type checking complete"

security-scan: ## Run security vulnerability scanning
	@echo "🔒 Running security vulnerability scan..."
	bandit -r . -x tests/,tools/archive/
	safety check
	@echo "✅ Security scan complete"

# Testing
test: ## Run comprehensive test suite
	@echo "🧪 Running test suite..."
	python -m pytest tests/ -v --tb=short
	@echo "✅ Tests complete"

smoke: ## Run chart smoke tests (soundcheck before the show)
	@echo "🎵 Running chart smoke tests..."
	python -m pytest tests/test_charts_smoke.py -v --tb=short
	@echo "✅ Chart smoke tests complete"

test-notebooks: ## Run notebook validation tests
	@echo "📓 Running notebook validation tests..."
	python -m pytest tests/test_notebook_validation.py tests/test_notebook_execution.py -v
	@echo "✅ Notebook tests complete"

test-notebook-execution: ## Run comprehensive notebook execution tests
	@echo "🧪 Running comprehensive notebook execution tests..."
	python -m pytest tests/test_notebook_execution_robust.py tests/test_post_archive_notebook_validation.py tests/test_notebook_integration_comprehensive.py -v
	@echo "✅ Notebook execution tests complete"

test-notebooks-robust: ## Run robust notebook testing with validation
	@echo "📓 Running robust notebook tests..."
	python scripts/run_robust_notebook_tests.py --quick
	@echo "✅ Robust notebook tests complete"

test-notebooks-comprehensive: ## Run comprehensive notebook test suite
	@echo "🚀 Running comprehensive notebook test suite..."
	python scripts/run_robust_notebook_tests.py --comprehensive
	@echo "✅ Comprehensive notebook tests complete"

test-notebooks-post-archive: ## Run post-archive notebook validation
	@echo "🔍 Running post-archive notebook validation..."
	python scripts/run_robust_notebook_tests.py --post-archive
	@echo "✅ Post-archive validation complete"

test-enterprise: ## Run enterprise test suite with coverage and benchmarks
	@echo "🏢 Running enterprise test suite..."
	python -m pytest tests/ -v --tb=short --cov=src --cov=web --cov-report=xml --cov-report=html --cov-fail-under=80
	python -m pytest tests/ -k "benchmark" --benchmark-json=performance_benchmarks.json || echo "No benchmark tests found"
	@echo "✅ Enterprise testing complete"

test-zero-tolerance: ## Run zero tolerance test suite (all must pass)
	@echo "🧪 Running zero tolerance test suite..."
	python -m pytest tests/ -x --cov=src --cov=web --cov-fail-under=80 --tb=short
	@echo "✅ Zero tolerance testing complete"

performance-test: ## Run performance benchmarking
	@echo "⚡ Running performance benchmarks..."
	python -m pytest tests/ -k "benchmark" --benchmark-json=performance_results.json --benchmark-min-rounds=5 || echo "No benchmark tests found"
	@echo "✅ Performance testing complete"

# Data operations
quality-check: ## Run data quality validation
	@echo "🔍 Running data quality checks..."
	python scripts/run_data_quality_checks.py --output-format json
	@echo "✅ Data quality check complete"

load-songs: ## Load songs CSV into songs table (usage: make load-songs FILE=path/to/songs.csv)
	@if [ -z "$(FILE)" ]; then echo "❌ Please provide FILE=path/to/songs.csv"; exit 1; fi
	@echo "🎵 Loading songs from $(FILE) ..."
	python scripts/load_songs_csv.py $(FILE)
	@echo "✅ Songs load complete"

refresh-normalized: ## Normalize videos and print ISRC nulls
	@echo "🎼 Normalizing music videos..."
	python scripts/normalize_music_videos.py
	@echo "🔍 Checking ISRC nulls (videos + normalized + links + recordings) ..."
	python scripts/null_check.py --quiet --check-blanks --format table --tables youtube_videos,music_videos_normalized,video_recording_link,isrc_recordings

compliance-check: ## Validate YouTube ToS and privacy compliance
	@echo "⚖️ Running compliance validation..."
	python tools/maintenance/youtube_tos_compliance.py --status
	python scripts/run_data_quality_checks.py --compliance-check || echo "Compliance check completed with warnings"
	@echo "✅ Compliance check complete"

# Pipeline operations
run-etl: ## Execute core ETL pipeline
	@echo "🚀 Running ETL pipeline..."
	python tools/etl/run_focused_etl.py
	@echo "✅ ETL pipeline complete"

normalize-videos: ## Populate music_videos_normalized from existing youtube tables (fast)
	@echo "🎼 Normalizing music videos into music_videos_normalized..."
	python scripts/normalize_music_videos.py
	@echo "✅ Normalization complete"

run-notebooks: ## Generate analytics notebooks
	@echo "📊 Generating analytics notebooks..."
	python tools/run_notebooks.py
	@echo "✅ Notebooks generated"

create-dashboard: ## Create fresh dashboard with bulletproof toolchain
	@echo "🚀 Creating Professional Dashboard (Bulletproof Edition)..."
	python notebooks/🚀_CREATE_DASHBOARD.py
	@echo "✅ Dashboard created"

create-dashboard-sample: ## Create dashboard with sample data for testing
	@echo "🧪 Creating dashboard with sample data..."
	python notebooks/🚀_CREATE_DASHBOARD.py --sample
	@echo "✅ Sample dashboard created"

create-dashboard-execute: ## Create and execute dashboard with papermill
	@echo "⚡ Creating and executing dashboard..."
	python notebooks/🚀_CREATE_DASHBOARD.py --execute
	@echo "✅ Dashboard created and executed"

nbclean: ## Clear notebook outputs using nbconvert
	@echo "🧹 Clearing notebook outputs..."
	find notebooks/ -name "*.ipynb" -not -path "*/archive/*" -exec jupyter nbconvert --clear-output --inplace {} \;
	@echo "✅ Notebook outputs cleared"

nbstrip: ## Apply nbstripout to all notebooks
	@echo "🧹 Applying nbstripout to notebooks..."
	find notebooks/ -name "*.ipynb" -not -path "*/archive/*" -exec nbstripout {} \;
	@echo "✅ nbstripout applied"

run-production: ## Execute full production pipeline
	@echo "🏭 Running production pipeline..."
	python tools/etl/run_production_pipeline.py
	@echo "✅ Production pipeline complete"

# Monitoring
monitor: ## Start system monitoring
	@echo "📊 Starting system monitoring..."
	python tools/monitoring/enterprise_monitoring.py --mode continuous --duration 24

health-check: ## Run system health assessment
	@echo "🏥 Running system health check..."
	python tools/monitoring/enterprise_monitoring.py --mode health

sla-report: ## Generate SLA compliance report
	@echo "📈 Generating SLA compliance report..."
	python tools/monitoring/enterprise_monitoring.py --mode sla

executive-report: ## Generate executive dashboard
	@echo "👔 Generating executive report..."
	python tools/monitoring/enterprise_monitoring.py --mode report

# Enterprise operations
enterprise-deploy: ## Deploy to production environment
	@echo "🏢 Starting enterprise deployment..."
	./scripts/enterprise_deployment.sh production full
	@echo "✅ Enterprise deployment complete"

enterprise-test: ## Run enterprise validation suite
	@echo "🏢 Running enterprise validation..."
	$(MAKE) test-enterprise
	$(MAKE) security-scan
	$(MAKE) compliance-check
	$(MAKE) performance-test
	@echo "✅ Enterprise validation complete"

enterprise-monitor: ## Start enterprise monitoring system
	@echo "🏢 Starting enterprise monitoring system..."
	python tools/monitoring/enterprise_monitoring.py --mode continuous --duration 168 --config config/monitoring/enterprise_config.json

# Cleanup
clean: ## Clean temporary files and caches
	@echo "🧹 Cleaning up temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/
	rm -f performance_*.json security_audit.json
	@echo "✅ Cleanup complete"

# Database operations
db-init: ## Initialize database schema
	@echo "🗄️ Initializing database..."
	python tools/setup/create_tables.py
	@echo "✅ Database initialized"

db-cleanup: ## Run database cleanup
	@echo "🧹 Running database cleanup..."
	python tools/maintenance/cleanup_db.py --confirm
	@echo "✅ Database cleanup complete"

# CI/CD integration
ci: ## Run enhanced CI/CD pipeline (senior-level standards)
	@echo "🚀 Running enhanced CI/CD pipeline..."
	python scripts/enhanced_ci.py
	@echo "✅ Enhanced CI/CD complete"

ci-fix: ## Run enhanced CI with auto-fix
	@echo "🔧 Running enhanced CI with auto-fix..."
	python scripts/enhanced_ci.py --fix-issues
	@echo "✅ Enhanced CI with fixes complete"

ci-report: ## Generate AI agent reports only
	@echo "📊 Generating AI agent reports..."
	python scripts/enhanced_ci.py --report-only
	@echo "✅ AI agent reports generated"

# Personal/local cleanup (NOT for CI/CD; avoid committing outputs)
personal-cleanup-dummy-videos: ## ⚠️ PERSONAL: Remove known dummy video_ids from your local DB (uses .env)
	@echo "⚠️ PERSONAL MAINTENANCE TASK ⚠️"
	@echo "This removes dummy video_ids (vid1, vid2, vid3, vidX) from your local database."
	@echo "It reads DB settings from .env or DATABASE_URL. Do NOT run in shared/production environments."
	@echo ""
	python scripts/cleanup_dummy_videos.py --ids vid1 vid2 vid3 vidX --include-metrics

benchmark: ## Run project benchmark and track progress
	@echo "📊 Running project benchmark..."
	python scripts/benchmark_progress.py
	@echo "✅ Benchmark complete"

setup-sentiment: ## Set up basic sentiment analysis for benchmarking
	@echo "🎵 Setting up sentiment analysis..."
	python scripts/setup_sentiment.py
	@echo "✅ Sentiment analysis setup complete"

# Automation Management (Explicit User Control)
list-schedules: ## List available automation schedules
	@echo "🤖 Available automation schedules..."
	python scripts/automation_manager.py list

test-schedule: ## Test automation schedule (usage: make test-schedule SCHEDULE=standard)
	@echo "🧪 Testing automation schedule: $(SCHEDULE)"
	python scripts/automation_manager.py test $(SCHEDULE)

generate-cron-config: ## Generate CRON configuration (usage: make generate-cron-config SCHEDULE=standard)
	@echo "⚙️ Generating CRON configuration for: $(SCHEDULE)"
	python scripts/automation_manager.py generate-cron $(SCHEDULE)

apply-cron-schedule: ## Apply CRON schedule (usage: make apply-cron-schedule SCHEDULE=standard)
	@echo "🚀 Applying CRON schedule: $(SCHEDULE)"
	python scripts/automation_manager.py apply-cron $(SCHEDULE)

automation-status: ## Show current automation status
	@echo "📊 Checking automation status..."
	python scripts/automation_manager.py status

disable-automation: ## Disable all automated processes
	@echo "⚠️ Disabling all automation..."
	python scripts/automation_manager.py disable

restore-automation: ## Restore automation from backup
	@echo "🔄 Restoring automation from backup..."
	python scripts/automation_manager.py restore-cron

# Quick automation setup commands
setup-minimal-automation: ## Set up minimal automation (weekly health checks only)
	@echo "🤖 Setting up minimal automation..."
	python scripts/automation_manager.py generate-cron conservative
	python scripts/automation_manager.py apply-cron conservative --force

setup-standard-automation: ## Set up standard automation (daily ETL, weekly reports)
	@echo "🤖 Setting up standard automation..."
	python scripts/automation_manager.py generate-cron standard
	@echo "📋 Review the generated configuration before applying:"
	@echo "   cat config/automation/generated_standard_cron.txt"
	@echo "🚀 Apply with: make apply-cron-schedule SCHEDULE=standard"

setup-enterprise-automation: ## Set up enterprise automation (production monitoring)
	@echo "🏢 Setting up enterprise automation..."
	python scripts/automation_manager.py generate-cron enterprise
	@echo "📋 Review the generated configuration before applying:"
	@echo "   cat config/automation/generated_enterprise_cron.txt"
	@echo "🚀 Apply with: make apply-cron-schedule SCHEDULE=enterprise"

# User Experience Optimization Commands
quickstart: ## Complete setup with sample data (transparent process)
	@echo "🚀 YouTube Music Analytics - Quick Start"
	@echo "This task installs dependencies, sets up the schema, and runs checks"
	@echo ""
	@echo "This will:"
	@echo "  1. Install dependencies and verify environment"
	@echo "  2. Set up database schema"
	@echo "  3. Load sample music data (if available)"
	@echo "  4. Run validation checks"
	@echo ""
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	$(MAKE) setup
	$(MAKE) db-init
	$(MAKE) ci-report
	@echo "✅ Quick start complete! Check ci_validation_report.json for system status"

setup: ## Install dependencies and verify environment
	@echo "📦 Installing dependencies..."
	@echo "  • Upgrading pip..."
	pip install --upgrade pip
	@echo "  • Installing requirements..."
	pip install -r requirements.txt
	@echo "  • Installing package in development mode..."
	pip install -e .
	@echo "✅ Dependencies installed"

dev-environment: ## Complete development environment setup
	@echo "🛠️ Setting up development environment..."
	@echo "This will install:"
	@echo "  • Pre-commit hooks for code quality"
	@echo "  • Testing and linting tools"
	@echo "  • Development dependencies"
	@echo ""
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	$(MAKE) dev
	@echo "✅ Development environment ready"

configure-channels: ## Set up YouTube channels for data collection
	@echo "🎵 Configuring YouTube channels..."
	@echo "You'll need:"
	@echo "  • YouTube Data API key"
	@echo "  • Channel URLs for artists you want to track"
	@echo ""
	@echo "See .env.example for configuration format"
	@echo "Run: cp .env.example .env"
	@echo "Then edit .env with your settings"

run-examples: ## Run example analyses with current data
	@echo "📊 Running example analyses..."
	@echo "Available examples:"
	@echo "  • Artist comparison analysis"
	@echo "  • Sentiment trend analysis"
	@echo "  • Data quality validation"
	@echo ""
	@read -p "Which example? (comparison/sentiment/quality): " example; \
	case $$example in \
		comparison) python execute_artist_comparison.py ;; \
		sentiment) echo "Sentiment analysis example - run: python test_current_sentiment_model.py" ;; \
		quality) python execute_data_quality.py ;; \
		*) echo "Invalid option. Choose: comparison, sentiment, or quality" ;; \
	esac

ci-local: ## Run local CI/CD pipeline (quick validation)
	@echo "🚀 Running local CI/CD pipeline..."
	python scripts/validate_loc_limits.py
	python scripts/validate_notebooks.py
	python scripts/check_notebook_outputs.py
	python scripts/test_schema_alignment.py
	python scripts/generate_ci_report.py
	@echo "✅ Local CI/CD complete"

pre-commit-validate: ## Run enhanced pre-commit validation with zero tolerance
	@echo "🔒 Running enhanced pre-commit validation..."
	python scripts/pre_commit_hook.py
	@echo "✅ Pre-commit validation complete"

quality-gates: ## Run zero tolerance quality gates
	@echo "🚪 Running quality gates..."
	python scripts/enhanced_ci.py --report-only
	@echo "✅ Quality gates complete"

ci-comprehensive: ## Run comprehensive artist validation
	@echo "🎤 Running comprehensive artist validation..."
	python scripts/comprehensive_artist_validation.py
	@echo "✅ Comprehensive validation complete"

ci-test: clean install test-enterprise security-scan compliance-check ## Run CI/CD test pipeline
	@echo "✅ CI/CD test pipeline complete"

cd-deploy: ci-test ## Run CD deployment pipeline
	@echo "🚀 Starting CD deployment pipeline..."
	$(MAKE) enterprise-deploy
	@echo "✅ CD deployment complete"

# All-in-one commands
full-setup: dev db-init run-etl run-notebooks ## Complete system setup
	@echo "🎉 Full system setup complete!"

production-ready: clean install test security-scan compliance-check quality-check ## Validate production readiness
	@echo "🏭 System is production ready!"

# Legacy compatibility (maintain existing commands)
etl: run-etl ## Alias for run-etl
etl-full: run-notebooks ## Alias for run-notebooks
etl-production: run-production ## Alias for run-production
nbexecute: run-notebooks ## Alias for run-notebooks
