# 🧪 Local Testing Guide for CI Fixes

**Purpose:** Test CI workflow changes locally before pushing to GitHub to minimize failed workflow runs.

**Goal:** Make local development environment mirror CI environment as closely as possible.

---

## 📋 Pre-Push Verification Checklist

Before pushing CI fixes to GitHub, complete these steps:

- [ ] **Flake8 exclusions tested locally** - Archive files are skipped
- [ ] **Local MySQL configured with CI credentials** - `etl_enterprise_user` exists
- [ ] **Environment variables exported** - DB_* vars match CI
- [ ] **Full test suite passes locally** - Same results as expected in CI
- [ ] **Workflow syntax validated** - YAML is valid
- [ ] **(Optional) Tested with `act`** - GitHub Actions run locally

---

## 🔧 Part 1: Test Flake8 Exclusions Locally

### Current Issue
Archive files have intentional syntax errors. CI flake8 scans them and fails.

### Local Test Command

```bash
# Test with CURRENT exclusions (should fail with 52 errors)
flake8 --max-line-length=120 --exclude=.venv,__pycache__,tools/archive .

# Test with UPDATED exclusions (should pass or have fewer errors)
flake8 --max-line-length=120 --exclude=.venv,__pycache__,tools/archive,archive,notebooks/archive .
```

### Expected Results

**Before fix:**
```
./archive/dangerous_scripts/final_linting_cleanup.py:117:38: E999 SyntaxError: unterminated string literal
./archive/dangerous_scripts/final_zero_linting.py:20:145: E999 SyntaxError: unterminated string literal
... (52 total errors)
```

**After fix:**
```
# No errors from archive/ or notebooks/archive/ directories
# Only errors from active codebase (if any)
```

### Verification

```bash
# Count errors before fix
flake8 --max-line-length=120 --exclude=.venv,__pycache__,tools/archive . | wc -l

# Count errors after fix
flake8 --max-line-length=120 --exclude=.venv,__pycache__,tools/archive,archive,notebooks/archive . | wc -l

# Difference should be ~52 errors
```

---

## 🗄️ Part 2: Set Up Local MySQL with CI Credentials

### Current Issue
- Local tests read `DB_USER` from environment variables via `os.getenv("DB_USER")`
- CI creates MySQL with `etl_enterprise_user` credentials
- CI writes credentials to `.env.enterprise.test` file but doesn't export to `$GITHUB_ENV`
- Python scripts can't access credentials via `os.getenv()`, causing authentication failures

### Solution
Create local MySQL user matching CI environment so you can test with the same credentials CI uses.

### Step 1: Connect to MySQL as Root

```bash
# macOS (Homebrew MySQL)
mysql -u root -p

# Or if no root password set
mysql -u root
```

### Step 2: Create CI-Matching User and Database

```sql
-- Create database matching CI
CREATE DATABASE IF NOT EXISTS yt_proj_enterprise_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create user matching CI credentials
CREATE USER IF NOT EXISTS 'etl_enterprise_user'@'localhost' IDENTIFIED BY 'etl_enterprise_secure_password';

-- Grant all privileges
GRANT ALL PRIVILEGES ON yt_proj_enterprise_test.* TO 'etl_enterprise_user'@'localhost';

-- Also grant on your main database (if needed for other tests)
GRANT ALL PRIVILEGES ON yt_proj.* TO 'etl_enterprise_user'@'localhost';

-- Apply changes
FLUSH PRIVILEGES;

-- Verify user was created
SELECT User, Host FROM mysql.user WHERE User = 'etl_enterprise_user';

-- Exit MySQL
EXIT;
```

### Step 3: Test Connection

```bash
# Test connection with CI credentials
mysql -u etl_enterprise_user -p'etl_enterprise_secure_password' -h 127.0.0.1 -P 3306 yt_proj_enterprise_test

# If successful, you should see:
# mysql>

# Exit
EXIT;
```

---

## 🔐 Part 3: Configure Environment Variables

### Create Test Environment File

```bash
# Create .env.test.local (add to .gitignore if not already there)
cat > .env.test.local << 'EOF'
# Local test environment matching CI
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=etl_enterprise_user
DB_PASS=etl_enterprise_secure_password
DB_NAME=yt_proj_enterprise_test
YOUTUBE_API_KEY=test_api_key_placeholder
ENVIRONMENT=local_test
EOF
```

### Export Variables Before Running Tests

```bash
# Option 1: Export manually
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=etl_enterprise_user
export DB_PASS=etl_enterprise_secure_password
export DB_NAME=yt_proj_enterprise_test
export YOUTUBE_API_KEY=test_api_key_placeholder

# Option 2: Source from file
set -a  # Auto-export all variables
source .env.test.local
set +a  # Disable auto-export

# Verify variables are set
echo "DB_USER=$DB_USER"
echo "DB_NAME=$DB_NAME"
```

---

## 🧪 Part 4: Run Tests Locally

### Initialize Database Schema

```bash
# Make sure ENV_FILE points to test config
export ENV_FILE=.env.test.local

# Create tables
python tools/core/create_tables.py

# Verify tables were created
mysql -u etl_enterprise_user -p'etl_enterprise_secure_password' yt_proj_enterprise_test -e "SHOW TABLES;"
```

### Run Full Test Suite

```bash
# Run all tests with verbose output
pytest -v

# Run only the failing tests
pytest -v tests/test_youtube_channel_etl.py::test_batch_upsert_raw_and_metrics_smoke
pytest -v tests/test_youtube_channel_etl.py::test_daily_max_semantics

# Run with coverage
pytest -v --cov=. --cov-report=term-missing
```

### Expected Results

**Before fix (without DB_* env vars exported):**
```
FAILED tests/test_youtube_channel_etl.py::test_batch_upsert_raw_and_metrics_smoke
FAILED tests/test_youtube_channel_etl.py::test_daily_max_semantics
Error: Access denied for user 'test_user'@'localhost'
# Note: Shows 'test_user' because that's what's in local .env file
```

**After fix (with CI credentials exported to environment):**
```
PASSED tests/test_youtube_channel_etl.py::test_batch_upsert_raw_and_metrics_smoke
PASSED tests/test_youtube_channel_etl.py::test_daily_max_semantics
# Tests work with ANY valid credentials supplied via environment variables
```

---

## 🎬 Part 5: Test with `act` (GitHub Actions Local Runner)

### What is `act`?

`act` runs GitHub Actions workflows locally using Docker. This lets you test workflow changes before pushing.

### Check if `act` is Installed

```bash
# Check if act is available
which act

# If not installed, install via Homebrew (macOS)
brew install act

# Verify installation
act --version
```

### Test Workflow Locally

```bash
# List available jobs
act -l

# Run specific job (Code Quality check)
act -j "Code Quality & Standards Compliance"

# Run with specific event (schedule trigger)
act schedule

# Run with verbose output
act -j "Code Quality & Standards Compliance" -v
```

### Limitations of `act`

⚠️ **Note:** `act` may not perfectly replicate GitHub's environment:
- Some GitHub-specific features may not work
- Secrets need to be provided via `.secrets` file
- MySQL service containers may behave differently
- Useful for syntax validation and basic testing, not full CI replication

---

## ✅ Final Pre-Push Checklist

Before pushing CI fixes to GitHub:

1. **Flake8 Test**
   ```bash
   flake8 --max-line-length=120 --exclude=.venv,__pycache__,tools/archive,archive,notebooks/archive .
   ```
   - [ ] No errors from `archive/` directories
   - [ ] No errors from `notebooks/archive/` directories

2. **Database Connection Test**
   ```bash
   mysql -u etl_enterprise_user -p'etl_enterprise_secure_password' yt_proj_enterprise_test -e "SELECT 1;"
   ```
   - [ ] Connection succeeds

3. **Environment Variables Test**
   ```bash
   echo "DB_USER=$DB_USER DB_NAME=$DB_NAME"
   ```
   - [ ] Variables are set correctly

4. **Test Suite**
   ```bash
   pytest -v tests/test_youtube_channel_etl.py
   ```
   - [ ] Tests pass (or fail with expected errors)

5. **Workflow Syntax**
   ```bash
   # Validate YAML syntax
   python -c "import yaml; yaml.safe_load(open('.github/workflows/enterprise_ci_cd.yml'))"
   ```
   - [ ] No YAML syntax errors

6. **Git Status**
   ```bash
   git status
   git diff .github/workflows/enterprise_ci_cd.yml
   ```
   - [ ] Only intended files are modified
   - [ ] Changes match the fix plan

---

## 🚀 Push to GitHub

Once all checklist items pass:

```bash
# Stage changes
git add .github/workflows/enterprise_ci_cd.yml

# Commit with conventional commit format
git commit -m "fix(ci): exclude archive dirs from flake8 and export DB env vars

- Add archive/ and notebooks/archive/ to flake8 exclusions
- Export DB_* environment variables for Python test scripts
- Fixes Enterprise CI/CD Pipeline failures in scheduled runs
- Tested locally: flake8 passes, pytest passes with CI credentials"

# Push to GitHub (triggers CI)
git push origin main

# Monitor workflow run
/opt/homebrew/bin/gh run watch
```

---

*This guide ensures local environment mirrors CI environment, reducing trial-and-error commits on GitHub.*

