#!/usr / bin / env python3
"""
Legacy wrapper for run_etl.py
This script is deprecated. Please use the new consolidated tools.
"""
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from compatibility_wrapper import CompatibilityWrapper

if __name__ == "__main__":
    wrapper = CompatibilityWrapper()
    sys.exit(wrapper.handle_legacy_tool("run_etl", sys.argv[1:]))
