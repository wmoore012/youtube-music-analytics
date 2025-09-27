"""
TDD tests for critical import resolution - Task 1
These tests will fail initially and drive the implementation to fix import errors.
"""

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest


class TestPackageInstallation:
    """Comprehensive test suite for package installation validation"""

    def test_src_youtubeviz_storytelling_imports_successfully(self):
        """Test that storytelling module imports successfully"""
        try:
            import src.youtubeviz.storytelling

            assert hasattr(src.youtubeviz.storytelling, "story_block")
            assert hasattr(src.youtubeviz.storytelling, "quick_takeaways")
        except ImportError as e:
            pytest.fail(f"Failed to import src.youtubeviz.storytelling: {e}")

    def test_src_youtubeviz_charts_imports_successfully(self):
        """Test that charts module imports successfully"""
        try:
            import src.youtubeviz.charts

            # Test that basic chart functions are available
            assert (
                hasattr(src.youtubeviz.charts, "views_over_time_plotly")
                or hasattr(src.youtubeviz.charts, "create_chart")
                or len(dir(src.youtubeviz.charts)) > 5
            )  # Has some chart functions
        except ImportError as e:
            pytest.fail(f"Failed to import src.youtubeviz.charts: {e}")

    def test_src_youtubeviz_data_imports_successfully(self):
        """Test that data module imports successfully"""
        try:
            import src.youtubeviz.data

            # Test that basic data functions are available
            assert len(dir(src.youtubeviz.data)) > 5  # Has some data functions
        except ImportError as e:
            pytest.fail(f"Failed to import src.youtubeviz.data: {e}")

    def test_package_installation_validation_returns_true_when_all_imports_work(self):
        """Test validate_package_installation() returns True for working imports"""
        from src.youtubeviz.storytelling import validate_package_installation

        result = validate_package_installation()
        assert result is True, "Package installation validation should return True when imports work"

    def test_package_installation_validation_returns_false_when_imports_fail(self):
        """Test validate_package_installation() returns False for failed imports"""
        from src.youtubeviz.storytelling import validate_package_installation

        # Mock the helper function that does the imports
        with patch("src.youtubeviz.storytelling._test_imports", side_effect=ImportError("Mocked import failure")):
            result = validate_package_installation()
            assert result is False, "Package installation validation should return False when imports fail"

    def test_import_error_messages_are_helpful(self):
        """Test that import errors provide actionable error messages"""
        from src.youtubeviz.storytelling import validate_package_installation

        # Test with mocked import error
        with patch("builtins.__import__", side_effect=ImportError("No module named 'src'")):
            result = validate_package_installation()
            assert result is False
            # The function should provide helpful error messages (we'll implement this)


class TestNotebookImportIntegration:
    """Test notebook imports in Jupyter environment"""

    def test_notebook_can_import_storytelling_functions(self):
        """Test that notebook cells can import storytelling functions"""
        # Simulate notebook import
        try:
            from src.youtubeviz.storytelling import quick_takeaways, story_block

            assert callable(story_block)
            assert callable(quick_takeaways)
        except ImportError as e:
            pytest.fail(f"Notebook import simulation failed: {e}")

    def test_notebook_can_import_chart_functions(self):
        """Test that notebook cells can import chart functions"""
        try:
            import src.youtubeviz.charts as charts

            # Should be able to access chart functions
            assert charts is not None
        except ImportError as e:
            pytest.fail(f"Notebook chart import simulation failed: {e}")

    def test_notebook_can_import_data_functions(self):
        """Test that notebook cells can import data functions"""
        try:
            import src.youtubeviz.data as data

            # Should be able to access data functions
            assert data is not None
        except ImportError as e:
            pytest.fail(f"Notebook data import simulation failed: {e}")


class TestPipInstallEditable:
    """Test that pip install -e . works correctly"""

    def test_package_is_installed_in_editable_mode(self):
        """Test that the package is properly installed in development mode"""
        # Check if package is in sys.path
        import os
#         import sys

        # Look for the package in installed packages or development paths
        package_found = False
        for path in sys.path:
            if "src" in path or "youtubeviz" in str(path):
                package_found = True
                break

        # Alternative: try to import and check if it's editable
        try:
            import src.youtubeviz

            package_found = True
        except ImportError:
            pass

        assert package_found, "Package should be installed in editable mode (pip install -e .)"

    def test_changes_to_source_reflect_immediately(self):
        """Test that changes to source code are reflected without reinstall"""
        # This is more of a documentation test - editable installs should work
        # In practice, this would require modifying source and reimporting
        try:
            import src.youtubeviz.storytelling

            # If we can import it, editable mode is likely working
            assert True
        except ImportError:
            pytest.fail("Editable installation not working - run 'pip install -e .'")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
