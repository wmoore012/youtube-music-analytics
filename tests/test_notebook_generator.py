"""
Tests for production-ready notebook template system.

This module tests the NotebookTemplateManager to ensure it generates
notebooks with correct chart counts and bulletproof CI / CD validation.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src.youtubeviz.notebook_generator import NotebookTemplateManager, create_production_notebook


class TestNotebookTemplateManager:
    """Test the notebook template manager."""

    def test_initialization_with_correct_chart_count(self):
        """Test that manager initializes with correct chart count."""
        manager = NotebookTemplateManager(total_charts=20)
        assert manager.total_charts == 20
        assert len(manager.chart_registry) == 20

        # Check that all chart IDs are present
        expected_ids = set(range(1, 21))
        actual_ids = set(manager.chart_registry.keys())
        assert actual_ids == expected_ids

    def test_chart_registry_completeness(self):
        """Test that chart registry contains all required charts."""
        manager = NotebookTemplateManager(total_charts=20)

        # Check original 15 advanced charts
        advanced_charts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        for chart_id in advanced_charts:
            assert chart_id in manager.chart_registry
            assert manager.chart_registry[chart_id]["module"] == "advanced_charts"

        # Check additional 5 charts from Complete Dashboard
        additional_charts = {
            16: {"module": "charts", "function": "views_over_time_plotly"},
            17: {"module": "content", "function": "create_artist_comparison_chart"},
            18: {"module": "sentiment", "function": "extract_top_positive_comments"},
            19: {"module": "storytelling", "function": "story_block"},
            20: {"module": "summary_generator", "function": "generate_executive_summary"},
        }

        for chart_id, expected in additional_charts.items():
            assert chart_id in manager.chart_registry
            assert manager.chart_registry[chart_id]["module"] == expected["module"]
            assert manager.chart_registry[chart_id]["function"] == expected["function"]

    def test_generate_notebook_template_structure(self):
        """Test that generated notebook has correct structure."""
        manager = NotebookTemplateManager(total_charts=20)
        notebook = manager.generate_notebook_template()

        # Check basic notebook structure
        assert "cells" in notebook
        assert "metadata" in notebook
        assert "nbformat" in notebook
        assert "nbformat_minor" in notebook

        # Check metadata
        assert notebook["nbformat"] == 4
        assert notebook["nbformat_minor"] == 4
        assert "kernelspec" in notebook["metadata"]
        assert "language_info" in notebook["metadata"]

    def test_generate_notebook_with_all_charts(self):
        """Test that notebook includes all 20 charts."""
        manager = NotebookTemplateManager(total_charts=20)
        notebook = manager.generate_notebook_template()

        cells = notebook["cells"]

        # Should have: header + imports + data loading + (2 cells per chart * 20) + validation
        # = 1 + 1 + 1 + 40 + 1 = 44 cells
        expected_cells = 1 + 1 + 1 + (2 * 20) + 1  # 44 cells
        assert len(cells) == expected_cells

        # Check that we have markdown cells for each chart
        chart_markdown_cells = [
            cell for cell in cells if cell["cell_type"] == "markdown" and "Chart " in "".join(cell["source"])
        ]
        assert len(chart_markdown_cells) == 20

        # Check that chart IDs are sequential 1-20
        chart_numbers = []
        for cell in chart_markdown_cells:
            source = "".join(cell["source"])
            for i in range(1, 21):
                if f"Chart {i}:" in source:
                    chart_numbers.append(i)
                    break

        assert sorted(chart_numbers) == list(range(1, 21))

    def test_validation_cell_counts_correct_charts(self):
        """Test that validation cell counts the correct number of charts."""
        manager = NotebookTemplateManager(total_charts=20)
        validation_cell = manager._create_validation_cell(20)

        source_code = "".join(validation_cell["source"])

        # Check that it loops through 1-21 (range(1, 21))
        assert "range(1, 21)" in source_code

        # Check that it reports correct totals
        assert "/20" in source_code

        # Check that it has CI / CD validation
        assert "CI / CD" in source_code
        assert "success_rate" in source_code

    def test_header_cell_shows_correct_chart_count(self):
        """Test that header cell shows correct chart count."""
        manager = NotebookTemplateManager(total_charts=20)
        header_cell = manager._create_header_cell("Test Notebook", 20)

        source = "".join(header_cell["source"])

        # Should mention 20 charts
        assert "20 Data-Science Grade Charts" in source
        assert "Total Charts**: 20" in source

    def test_imports_cell_includes_all_modules(self):
        """Test that imports cell includes all required modules."""
        manager = NotebookTemplateManager(total_charts=20)
        imports_cell = manager._create_imports_cell()

        source_code = "".join(imports_cell["source"])

        # Check for advanced charts import
        assert "from youtubeviz.advanced_charts import" in source_code

        # Check for additional chart imports
        assert "from youtubeviz.charts import views_over_time_plotly" in source_code
        assert "from youtubeviz.content import create_artist_comparison_chart" in source_code
        assert "from youtubeviz.sentiment import extract_top_positive_comments" in source_code
        assert "from youtubeviz.storytelling import story_block" in source_code
        assert "from youtubeviz.summary_generator import generate_executive_summary" in source_code

    def test_data_loading_cell_enforces_no_fake_data(self):
        """Test that data loading cell enforces no fake data policy."""
        manager = NotebookTemplateManager(total_charts=20)
        data_cell = manager._create_data_loading_cell()

        source_code = "".join(data_cell["source"])

        # Check for no fake data policy
        assert "NO FAKE DATA FALLBACK" in source_code
        assert "real data only" in source_code.lower()
        assert "load_recent_window_days" in source_code

        # Should not contain any fake data generation
        assert "pd.DataFrame({" not in source_code  # No hardcoded DataFrames
        assert "np.random" not in source_code  # No random data
        assert "fake" not in source_code.lower() or "NO FAKE" in source_code

    def test_chart_cells_handle_missing_data_gracefully(self):
        """Test that chart cells handle missing data gracefully."""
        manager = NotebookTemplateManager(total_charts=20)
        chart_info = {"name": "Test Chart", "function": "test_function", "module": "test_module"}
        cells = manager._create_chart_cells(1, chart_info)

        assert len(cells) == 2  # Markdown + Code

        code_cell = cells[1]
        source_code = "".join(code_cell["source"])

        # Should handle empty dataframe
        assert "if not df.empty:" in source_code
        assert "else:" in source_code

        # Should show data requirements when data is missing
        assert "Data Requirements" in source_code
        assert "Add real data to see analytics" in source_code

        # Should handle exceptions
        assert "except Exception as e:" in source_code
        assert "Chart 1 error" in source_code

    def test_save_and_load_notebook(self):
        """Test saving and loading notebook files."""
        manager = NotebookTemplateManager(total_charts=20)
        notebook = manager.generate_notebook_template()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            temp_path = f.name

        try:
            # Save notebook
            manager.save_notebook(notebook, temp_path)

            # Load and verify
            with open(temp_path, "r", encoding="utf-8") as f:
                loaded_notebook = json.load(f)

            assert loaded_notebook == notebook
            assert len(loaded_notebook["cells"]) == len(notebook["cells"])

        finally:
            os.unlink(temp_path)

    def test_create_production_notebook_function(self):
        """Test the create_production_notebook convenience function."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ipynb", delete=False) as f:
            temp_path = f.name

        try:
            # Create production notebook
            create_production_notebook(temp_path)

            # Verify file was created
            assert os.path.exists(temp_path)

            # Load and verify structure
            with open(temp_path, "r", encoding="utf-8") as f:
                notebook = json.load(f)

            # Should have all 20 charts
            cells = notebook["cells"]
            chart_markdown_cells = [
                cell for cell in cells if cell["cell_type"] == "markdown" and "Chart " in "".join(cell["source"])
            ]
            assert len(chart_markdown_cells) == 20

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


class TestNotebookValidation:
    """Test notebook validation and CI / CD integration."""

    def test_validation_cell_ci_cd_thresholds(self):
        """Test that validation cell implements correct CI / CD thresholds."""
        manager = NotebookTemplateManager(total_charts=20)
        validation_cell = manager._create_validation_cell(20)

        source_code = "".join(validation_cell["source"])

        # Check for success rate calculation
        assert "success_rate = len(real_charts)" in source_code

        # Check for CI / CD thresholds
        assert ">= 0.8" in source_code  # 80% threshold for PASS
        assert ">= 0.6" in source_code  # 60% threshold for WARNING
        assert "CI / CD: PASS" in source_code
        assert "CI / CD: WARNING" in source_code
        assert "CI / CD: FAIL" in source_code

    def test_notebook_includes_chart_counting_logic(self):
        """Test that notebook includes proper chart counting logic."""
        manager = NotebookTemplateManager(total_charts=20)
        notebook = manager.generate_notebook_template()

        # Find validation cell
        validation_cells = [
            cell
            for cell in notebook["cells"]
            if cell["cell_type"] == "code" and "REAL DATA ANALYTICS SUMMARY" in "".join(cell["source"])
        ]

        assert len(validation_cells) == 1

        validation_cell = validation_cells[0]
        source_code = "".join(validation_cell["source"])

        # Should count real charts, requirement charts, and error charts
        assert "real_charts = []" in source_code
        assert "requirement_charts = []" in source_code
        assert "error_charts = []" in source_code

        # Should check for real data vs annotations
        assert "has_real_data = any(" in source_code
        assert "hasattr(trace, 'x')" in source_code

    def test_notebook_enforces_no_fake_data_throughout(self):
        """Test that entire notebook enforces no fake data policy."""
        manager = NotebookTemplateManager(total_charts=20)
        notebook = manager.generate_notebook_template()

        # Check all code cells for fake data patterns
        code_cells = [cell for cell in notebook["cells"] if cell["cell_type"] == "code"]

        for cell in code_cells:
            source_code = "".join(cell["source"])

            # Should not contain fake data generation
            assert "pd.DataFrame({" not in source_code or "empty dataframe" in source_code.lower()
            assert "np.random" not in source_code or "Would calculate from real data" in source_code
            assert "fake_data" not in source_code
            assert "sample_data" not in source_code

            # If it mentions fake, it should be in context of avoiding it
            if "fake" in source_code.lower():
                assert any(phrase in source_code for phrase in ["NO FAKE", "no fake", "not fake"])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
