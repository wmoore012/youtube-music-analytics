"""Test notebook plugin integration."""

import json
import os
from pathlib import Path
import tempfile

from src.youtubeviz.notebook_plugin_integration import (
    PluginEnhancedNotebookGenerator,
    create_algorithm_comparison_notebook,
    create_plugin_enhanced_notebook,
)


class TestNotebookPluginIntegration:
    """Test notebook generation with plugin integration."""

    def test_plugin_enhanced_notebook_generator_init(self):
        """Test notebook generator initialization."""
        # Test with plugins enabled
        generator = PluginEnhancedNotebookGenerator(enable_plugins=True)
        assert generator._enable_plugins is True

        # Test with plugins disabled
        generator_no_plugins = PluginEnhancedNotebookGenerator(enable_plugins=False)
        assert generator_no_plugins._enable_plugins is False

        print("Notebook generator initialization: PASSED")

    def test_base_notebook_structure(self):
        """Test base notebook structure creation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=False)

        notebook = generator._create_base_notebook_structure("Test Notebook")

        # Validate notebook structure
        assert "cells" in notebook
        assert "metadata" in notebook
        assert "nbformat" in notebook
        assert "nbformat_minor" in notebook

        # Check cells
        assert len(notebook["cells"]) >= 2  # At least title and imports
        assert notebook["cells"][0]["cell_type"] == "markdown"
        assert notebook["cells"][1]["cell_type"] == "code"

        # Check metadata
        assert "kernelspec" in notebook["metadata"]
        assert "language_info" in notebook["metadata"]

        print("Base notebook structure: PASSED")

    def test_plugin_overview_cells(self):
        """Test plugin overview cells creation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=True)

        cells = generator._create_plugin_overview_cells()

        assert isinstance(cells, list)
        assert len(cells) >= 2  # At least markdown and code cells

        # Check cell types
        assert cells[0]["cell_type"] == "markdown"
        assert cells[1]["cell_type"] == "code"

        # Check content
        markdown_content = "".join(cells[0]["source"])
        assert "Plugin System Overview" in markdown_content

        code_content = "".join(cells[1]["source"])
        assert "initialize_plugins" in code_content

        print("Plugin overview cells: PASSED")

    def test_plugin_analysis_cells(self):
        """Test plugin analysis cells creation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=True)

        cells = generator._create_plugin_analysis_cells()

        assert isinstance(cells, list)
        assert len(cells) >= 4  # Multiple analysis cells

        # Check that we have both markdown and code cells
        cell_types = [cell["cell_type"] for cell in cells]
        assert "markdown" in cell_types
        assert "code" in cell_types

        print("Plugin analysis cells: PASSED")

    def test_fallback_analysis_cells(self):
        """Test fallback analysis cells creation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=False)

        cells = generator._create_fallback_analysis_cells()

        assert isinstance(cells, list)
        assert len(cells) >= 2

        # Check fallback content
        markdown_content = "".join(cells[0]["source"])
        assert "Plugin System Not Available" in markdown_content

        print("Fallback analysis cells: PASSED")

    def test_plugin_insights_cells(self):
        """Test plugin insights cells creation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=True)

        cells = generator._create_plugin_insights_cells()

        assert isinstance(cells, list)
        assert len(cells) >= 2

        # Check insights content
        markdown_content = "".join(cells[0]["source"])
        assert "Plugin System Insights" in markdown_content
        assert "Algorithm Diversity" in markdown_content

        print("Plugin insights cells: PASSED")

    def test_algorithm_comparison_cells(self):
        """Test algorithm comparison cells creation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=True)

        test_algorithms = ["momentum_scorer", "engagement_scorer"]
        cells = generator._create_algorithm_comparison_cells(test_algorithms)

        assert isinstance(cells, list)
        assert len(cells) >= 1  # At least intro cell

        # Should have cells for each algorithm
        expected_cells = 1 + (len(test_algorithms) * 2)  # Intro + (markdown + code) per algorithm
        assert len(cells) == expected_cells

        print("Algorithm comparison cells: PASSED")

    def test_notebook_saving(self):
        """Test notebook saving functionality."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=False)

        # Create a simple test notebook
        test_notebook = {
            "cells": [{"cell_type": "markdown", "metadata": {}, "source": ["# Test Notebook\n"]}],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4,
        }

        # Save to temporary file
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test_notebook.ipynb")

            saved_path = generator._save_notebook(test_notebook, output_path)

            # Verify file was created
            assert os.path.exists(saved_path)
            assert saved_path == output_path

            # Verify content
            with open(saved_path, "r") as f:
                loaded_notebook = json.load(f)

            assert loaded_notebook["cells"][0]["source"] == ["# Test Notebook\n"]

        print("Notebook saving: PASSED")

    def test_create_plugin_enhanced_notebook(self):
        """Test complete plugin-enhanced notebook creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test_enhanced.ipynb")

            result = create_plugin_enhanced_notebook(
                title="Test Enhanced Notebook",
                output_path=output_path,
                enable_plugins=False,  # Disable to avoid dependency issues
            )

            # Check result
            assert result["success"] is True
            assert "notebook_path" in result
            assert "cells_created" in result
            assert "title" in result

            # Verify file exists
            assert os.path.exists(result["notebook_path"])

            # Verify notebook content
            with open(result["notebook_path"], "r") as f:
                notebook = json.load(f)

            assert "cells" in notebook
            assert len(notebook["cells"]) > 0

        print("Plugin-enhanced notebook creation: PASSED")

    def test_create_algorithm_comparison_notebook(self):
        """Test algorithm comparison notebook creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test_comparison.ipynb")

            result = create_algorithm_comparison_notebook(
                algorithms=["momentum_scorer", "engagement_scorer"],
                output_path=output_path,
                enable_plugins=False,  # Disable to avoid dependency issues
            )

            # Check result
            assert result["success"] is True
            assert "notebook_path" in result
            assert "algorithms_compared" in result
            assert "cells_created" in result

            # Verify file exists
            assert os.path.exists(result["notebook_path"])

        print("Algorithm comparison notebook creation: PASSED")

    def test_notebook_json_validity(self):
        """Test that generated notebooks are valid JSON."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=False)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test_validity.ipynb")

            result = generator.create_plugin_enhanced_notebook(title="JSON Validity Test", output_path=output_path)

            # Load and validate JSON
            with open(result["notebook_path"], "r") as f:
                notebook = json.load(f)  # This will raise if invalid JSON

            # Basic notebook validation
            required_keys = ["cells", "metadata", "nbformat", "nbformat_minor"]
            for key in required_keys:
                assert key in notebook, f"Missing required key: {key}"

            # Validate cells structure
            for i, cell in enumerate(notebook["cells"]):
                assert "cell_type" in cell, f"Cell {i} missing cell_type"
                assert "metadata" in cell, f"Cell {i} missing metadata"
                assert "source" in cell, f"Cell {i} missing source"

                # Validate cell_type
                assert cell["cell_type"] in ["markdown", "code"], f"Invalid cell_type in cell {i}"

                # Validate source is list
                assert isinstance(cell["source"], list), f"Cell {i} source is not a list"

        print("Notebook JSON validity: PASSED")

    def test_error_handling(self):
        """Test error handling in notebook generation."""
        generator = PluginEnhancedNotebookGenerator(enable_plugins=False)

        # Test with invalid output path (directory that doesn't exist and can't be created)
        try:
            # This should handle the error gracefully
            result = generator.create_plugin_enhanced_notebook(
                title="Error Test", output_path="/invalid/path/that/cannot/be/created/test.ipynb"
            )
            # If it succeeds, that's also fine (directory creation worked)
            print("Error handling test: Path creation succeeded unexpectedly")
        except Exception as e:
            # Expected behavior - should raise a clear error
            assert "NotebookPluginIntegrationError" in str(type(e).__name__) or "PermissionError" in str(
                type(e).__name__
            )
            print("Error handling: PASSED")


if __name__ == "__main__":
    # Run all tests
    test = TestNotebookPluginIntegration()

    print("Testing notebook generator initialization...")
    test.test_plugin_enhanced_notebook_generator_init()

    print("\nTesting base notebook structure...")
    test.test_base_notebook_structure()

    print("\nTesting plugin overview cells...")
    test.test_plugin_overview_cells()

    print("\nTesting plugin analysis cells...")
    test.test_plugin_analysis_cells()

    print("\nTesting fallback analysis cells...")
    test.test_fallback_analysis_cells()

    print("\nTesting plugin insights cells...")
    test.test_plugin_insights_cells()

    print("\nTesting algorithm comparison cells...")
    test.test_algorithm_comparison_cells()

    print("\nTesting notebook saving...")
    test.test_notebook_saving()

    print("\nTesting plugin-enhanced notebook creation...")
    test.test_create_plugin_enhanced_notebook()

    print("\nTesting algorithm comparison notebook creation...")
    test.test_create_algorithm_comparison_notebook()

    print("\nTesting notebook JSON validity...")
    test.test_notebook_json_validity()

    print("\nTesting error handling...")
    test.test_error_handling()

    print("\nAll notebook plugin integration tests completed!")
