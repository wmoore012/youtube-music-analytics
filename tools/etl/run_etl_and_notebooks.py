#!/usr/bin/env python3
"""
ETL & Notebooks Runner (Intuitive name)

Runs the full pipeline:
1. Sentiment analysis
2. Bot detection
3. Data quality validation
4. Performance metrics update
5. Executes key analysis notebooks

This is equivalent to the previous 'comprehensive' runner, just named clearly.
"""

import sys
from pathlib import Path

# Ensure repo root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tools.etl.run_comprehensive_etl import main as _legacy_main  # reuse implementation


def main():
    return _legacy_main()


if __name__ == "__main__":
    sys.exit(main())

