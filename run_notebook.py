#!/usr/bin/env python3
"""
Notebook execution script with proper environment setup
"""
import os
import subprocess
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv

load_dotenv()


def run_notebook(notebook_path, output_dir="notebooks/executed"):
    """Execute a notebook with proper environment setup"""

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Get notebook name for output
    notebook_name = Path(notebook_path).stem
    output_path = f"{output_dir}/{notebook_name}-executed.ipynb"

    # Set environment variables for subprocess
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    # Run nbconvert with execute
    cmd = [
        "jupyter",
        "nbconvert",
        "--to",
        "notebook",
        "--execute",
        "--output-dir",
        output_dir,
        "--output",
        f"{notebook_name}-executed.ipynb",
        notebook_path,
    ]

    print(f"🚀 Executing {notebook_path}...")
    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            print(f"✅ Successfully executed {notebook_path}")
            print(f"📄 Output saved to {output_path}")
            return True
        else:
            print(f"❌ Failed to execute {notebook_path}")
            print(f"Error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout executing {notebook_path}")
        return False
    except Exception as e:
        print(f"💥 Exception executing {notebook_path}: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_notebook.py <notebook_path>")
        sys.exit(1)

    notebook_path = sys.argv[1]
    success = run_notebook(notebook_path)
    sys.exit(0 if success else 1)
