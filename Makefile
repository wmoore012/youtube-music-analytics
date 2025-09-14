PYTHON := python
PIP := pip

.PHONY: help install dev precommit format lint typecheck test nbstrip clean

help:
	@echo "Targets: install, dev, precommit, format, lint, typecheck, test, nbstrip, clean"

install:
	$(PIP) install -e .

dev: install precommit

precommit:
	pre-commit install

format:
	black .
	isort .

lint:
	flake8

typecheck:
	mypy src

test:
	pytest -q

nbstrip:
	pre-commit run nbstripout --all-files || true

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
