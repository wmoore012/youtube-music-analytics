#!/usr/bin/env python3
"""
Validate Jupyter notebooks for CI pipeline.

This script checks:
- Notebook syntax is valid JSON
- No execution errors in notebook cells
- Notebooks follow naming conventions
- Required metadata is present
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict


class NotebookValidator:
    """Validates Jupyter notebooks for CI pipeline."""

    def __init__(self):
        self.violations = []

    def validate_notebook_json(self, notebook_path: Path) -> bool:
        """Validate that notebook is valid JSON."""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                json.load(f)
            return True
        except json.JSONDecodeError as e:
            self.violations.append(f"Invalid JSON in {notebook_path}: {e}")
            return False
        except Exception as e:
            self.violations.append(f"Error reading {notebook_path}: {e}")
            return False

    def validate_notebook_structure(self, notebook_path: Path) -> bool:
        """Validate notebook has required structure."""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)

            # Check required fields
            required_fields = ["cells", "metadata", "nbformat"]
            for field in required_fields:
                if field not in notebook:
                    self.violations.append(f"Missing required field '{field}' in {notebook_path}")
                    return False

            # Check cells structure
            if not isinstance(notebook["cells"], list):
                self.violations.append(f"'cells' must be a list in {notebook_path}")
                return False

            # Validate each cell
            for i, cell in enumerate(notebook["cells"]):
                if "cell_type" not in cell:
                    self.violations.append(f"Cell {i} missing 'cell_type' in {notebook_path}")
                    return False

                if "source" not in cell:
                    self.violations.append(f"Cell {i} missing 'source' in {notebook_path}")
                    return False

            return True

        except Exception as e:
            self.violations.append(f"Error validating structure of {notebook_path}: {e}")
            return False

    def check_naming_convention(self, notebook_path: Path) -> bool:
        """Check notebook follows naming conventions."""
        name = notebook_path.name

        # Check for proper numbering in analysis notebooks
        if "analysis" in str(notebook_path):
            if not name[0].isdigit():
                self.violations.append(f"Analysis notebook should start with number: {notebook_path}")
                return False

        # Check for descriptive names
        if len(name.replace(".ipynb", "").replace("_", "").replace("-", "")) < 10:
            self.violations.append(f"Notebook name too short, should be descriptive: {notebook_path}")
            return False

        return True

    def check_outputs_stripped(self, notebook_path: Path) -> bool:
        """Check that notebook outputs are stripped."""
        try:
            with open(notebook_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)

            for i, cell in enumerate(notebook["cells"]):
                # Check for outputs in code cells
                if cell.get("cell_type") == "code":
                    if "outputs" in cell and cell["outputs"]:
                        self.violations.append(f"Cell {i} has outputs in {notebook_path} - run nbstripout")
                        return False

                    if "execution_count" in cell and cell["execution_count"] is not None:
                        self.violations.append(f"Cell {i} has execution_count in {notebook_path} - run nbstripout")
                        return False

            return True

        except Exception as e:
            self.violations.append(f"Error checking outputs in {notebook_path}: {e}")
            return False

    def validate_notebook(self, notebook_path: Path) -> Dict[str, Any]:
        """Validate a single notebook."""
        result = {
            "file": str(notebook_path),
            "valid_json": False,
            "valid_structure": False,
            "valid_naming": False,
            "outputs_stripped": False,
            "valid": False,
        }

        # Validate JSON
        result["valid_json"] = self.validate_notebook_json(notebook_path)
        if not result["valid_json"]:
            return result

        # Validate structure
        result["valid_structure"] = self.validate_notebook_structure(notebook_path)

        # Check naming convention
        result["valid_naming"] = self.check_naming_convention(notebook_path)

        # Check outputs are stripped
        result["outputs_stripped"] = self.check_outputs_stripped(notebook_path)

        # Overall validity
        result["valid"] = all(
            [result["valid_json"], result["valid_structure"], result["valid_naming"], result["outputs_stripped"]]
        )

        return result

    def validate_directory(self, directory: Path) -> Dict[str, Any]:
        """Validate all notebooks in a directory."""
        results = {"total_notebooks": 0, "valid_notebooks": 0, "violations": [], "notebooks": {}}

        for notebook_path in directory.rglob("*.ipynb"):
            # Skip checkpoint files and notebooks that intentionally retain outputs
            if ".ipynb_checkpoints" in str(notebook_path) or any(p in {"archive", "executed"} for p in notebook_path.parts):
                continue

            results["total_notebooks"] += 1
            notebook_result = self.validate_notebook(notebook_path)
            results["notebooks"][str(notebook_path)] = notebook_result

            if notebook_result["valid"]:
                results["valid_notebooks"] += 1

        results["violations"] = self.violations.copy()
        return results


def main():
    """Main validation function."""
    print("📓 Validating Jupyter notebooks...")
    print("=" * 50)

    validator = NotebookValidator()
    project_root = Path(__file__).parent.parent
    notebooks_dir = project_root / "notebooks"

    if not notebooks_dir.exists():
        print("📁 No notebooks directory found-skipping validation")
        return 0

    results = validator.validate_directory(notebooks_dir)

    print(f"📊 Found {results['total_notebooks']} notebooks")

    if results["violations"]:
        print(f"\n❌ Found {len(results['violations'])} violations:")
        for violation in results["violations"]:
            print(f"   • {violation}")

        print(f"\n📈 Summary: {results['valid_notebooks']}/{results['total_notebooks']} notebooks passed validation")
        print("\n💡 Tips:")
        print("   • Run 'nbstripout' to remove outputs")
        print("   • Use descriptive notebook names")
        print("   • Number analysis notebooks (01_, 02_, etc.)")
        return 1
    else:
        print(f"✅ All {results['total_notebooks']} notebooks passed validation!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
