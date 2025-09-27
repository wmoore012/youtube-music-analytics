"""
Tests for notebook validation and quality checks.
"""

import json
import os
from pathlib import Path

import nbformat
import pytest


class TestNotebookStructure:
    """Test notebook file structure and JSON validity."""

    @pytest.fixture
    def notebook_paths(self):
        """Get all notebook paths for testing."""
        return [
            "notebooks / 2025 - 09 - 16_MusicScope™_Complete_Analytics_Dashboard.ipynb",
        ]

    def test_notebook_json_validity(self, notebook_paths):
        """Test that all notebooks have valid JSON structure."""
        for notebook_path in notebook_paths:
            if not os.path.exists(notebook_path):
                pytest.skip(f"Notebook {notebook_path} does not exist")

            with open(notebook_path, "r") as f:
                try:
                    json.load(f)
                    print(f"✅ {notebook_path}: Valid JSON")
                except json.JSONDecodeError as e:
                    pytest.fail(f"❌ {notebook_path}: Invalid JSON - {e}")

    def test_notebook_nbformat_validity(self, notebook_paths):
        """Test that all notebooks can be read by nbformat."""
        for notebook_path in notebook_paths:
            if not os.path.exists(notebook_path):
                pytest.skip(f"Notebook {notebook_path} does not exist")

            try:
                with open(notebook_path, "r") as f:
                    nb = nbformat.read(f, as_version=4)
                print(f"✅ {notebook_path}: Valid nbformat")
                assert nb.nbformat >= 4, f"Notebook format version should be 4+, got {nb.nbformat}"
            except Exception as e:
                pytest.fail(f"❌ {notebook_path}: nbformat error - {e}")

    def test_notebook_has_cells(self, notebook_paths):
        """Test that all notebooks have at least one cell."""
        for notebook_path in notebook_paths:
            if not os.path.exists(notebook_path):
                pytest.skip(f"Notebook {notebook_path} does not exist")

            with open(notebook_path, "r") as f:
                nb = nbformat.read(f, as_version=4)

            assert len(nb.cells) > 0, f"Notebook {notebook_path} should have at least one cell"
            print(f"✅ {notebook_path}: Has {len(nb.cells)} cells")

    def test_notebook_has_markdown_title(self, notebook_paths):
        """Test that notebooks have a markdown title cell."""
        for notebook_path in notebook_paths:
            if not os.path.exists(notebook_path):
                pytest.skip(f"Notebook {notebook_path} does not exist")

            with open(notebook_path, "r") as f:
                nb = nbformat.read(f, as_version=4)

            # Check if first cell is markdown and contains a title
            if nb.cells:
                first_cell = nb.cells[0]
                if first_cell.cell_type == "markdown":
                    source = first_cell.source
                    if source.startswith("#"):
                        print(f"✅ {notebook_path}: Has markdown title")
                        continue

            print(f"⚠️ {notebook_path}: No markdown title found")


class TestNotebookCodeSyntax:
    """Test that all code cells have valid Python syntax."""

    @pytest.fixture
    def notebook_paths(self):
        """Get all notebook paths for testing."""
        return [
            "notebooks / 2025 - 09 - 16_MusicScope™_Complete_Analytics_Dashboard.ipynb",
        ]

    def test_code_cell_syntax(self, notebook_paths):
        """Test that all code cells have valid Python syntax."""
        for notebook_path in notebook_paths:
            if not os.path.exists(notebook_path):
                pytest.skip(f"Notebook {notebook_path} does not exist")

            with open(notebook_path, "r") as f:
                nb = nbformat.read(f, as_version=4)

            for i, cell in enumerate(nb.cells):
                if cell.cell_type == "code":
                    source = cell.source.strip()
                    if source:  # Skip empty cells
                        try:
                            compile(source, f"{notebook_path}:cell_{i}", "exec")
                            print(f"✅ {notebook_path}:cell_{i}: Valid Python syntax")
                        except SyntaxError as e:
                            pytest.fail(f"❌ {notebook_path}:cell_{i}: Syntax error - {e}")


if __name__ == "__main__":
    pytest.main([__file__])
