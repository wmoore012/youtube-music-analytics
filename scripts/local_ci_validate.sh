#!/usr/bin/env bash
# Lightweight local CI smoke runner to mirror key Actions steps.
# Note: This does not change your Python version; CI uses Python 3.10.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "🔎 Black --check"
black --check --line-length=120 .

echo "🧹 flake8 (CI-aligned)"
flake8 --max-line-length=120 --exclude=.git,__pycache__,notebooks,venv,.venv,archive

echo "🧪 Unit smoke tests (subset)"
PYTHONPATH=. pytest -v tests/test_utils_unit.py tests/test_storytelling.py tests/test_education.py --tb=short

echo "📓 Notebook Quality Check (file existence + execute stubs)"
PYTHONPATH=. pytest -v tests/test_notebook_execution.py::TestNotebookFiles --tb=short

echo "🔗 Integration tests"
PYTHONPATH=. pytest -v tests/test_integration.py --tb=short

echo "✅ Local CI smoke checks passed"

