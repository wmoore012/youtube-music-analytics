#!/usr/bin/env python3
"""
Check that notebook outputs are properly stripped.

This script ensures notebooks don't have execution outputs committed to git,
which keeps the repository clean and prevents merge conflicts.
"""

import json
import sys
from pathlib import Path
from typing import List


def check_notebook_outputs(notebook_path: Path) -> List[str]:
    """Check if notebook has any outputs that should be stripped."""
    violations = []

    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            notebook = json.load(f)

        for i, cell in enumerate(notebook.get("cells", [])):
            if cell.get("cell_type") == "code":
                # Check for outputs
                if cell.get("outputs"):
                    violations.append(f"Cell {i+1} has outputs")

                # Check for execution count
                if cell.get("execution_count") is not None:
                    violations.append(f"Cell {i+1} has execution_count: {cell['execution_count']}")

                # Check for metadata that indicates execution
                metadata = cell.get("metadata", {})
                if metadata.get("execution"):
                    violations.append(f"Cell {i+1} has execution metadata")

    except Exception as e:
        violations.append(f"Error reading notebook: {e}")

    return violations


def main():
    """Main function to check all notebooks."""
    print("🧹 Checking notebook outputs are stripped...")
    print("=" * 50)

    project_root = Path(__file__).parent.parent
    notebooks_dir = project_root / "notebooks"

    if not notebooks_dir.exists():
        print("📁 No notebooks directory found")
        return 0

    total_notebooks = 0
    notebooks_with_outputs = 0
    all_violations = []

    for notebook_path in notebooks_dir.rglob("*.ipynb"):
        # Skip checkpoint files
        if ".ipynb_checkpoints" in str(notebook_path):
            continue

        total_notebooks += 1
        violations = check_notebook_outputs(notebook_path)

        if violations:
            notebooks_with_outputs += 1
            print(f"\n❌ {notebook_path.relative_to(project_root)}:")
            for violation in violations:
                print(f"   • {violation}")
            all_violations.extend(violations)

    print(f"\n📊 Summary:")
    print(f"   • Total notebooks: {total_notebooks}")
    print(f"   • Notebooks with outputs: {notebooks_with_outputs}")
    print(f"   • Clean notebooks: {total_notebooks - notebooks_with_outputs}")

    if notebooks_with_outputs > 0:
        print(f"\n⚠️  Found {len(all_violations)} output violations in {notebooks_with_outputs} notebooks")
        print("\n💡 To fix this, run:")
        print("   nbstripout notebooks/**/*.ipynb")
        print("   # or install pre-commit hook:")
        print("   pip install nbstripout")
        print("   nbstripout --install")
        return 1
    else:
        print("✅ All notebooks have outputs properly stripped!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
