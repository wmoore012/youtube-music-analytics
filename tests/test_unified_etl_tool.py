"""
Tests for the unified ETL tool.

This test suite validates:
- UnifiedETL class functionality
- Tool registration and configuration
- Command-line interface
- ETL mode selection and execution
"""

import argparse
from unittest.mock import MagicMock, patch

import pytest

from tools.core.etl import UnifiedETL, main
from tools.shared.common import ValidationError, find_tool


class TestUnifiedETL:
    """Test UnifiedETL class functionality."""

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    def test_etl_tool_initialization(self):
        """Test ETL tool can be initialized properly."""
        etl = UnifiedETL()

        assert etl.name == "unified-etl"
        assert etl.version == "1.0.0"
        assert etl.logger is not None

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    def test_tool_registration(self):
        """Test tool is registered in global registry."""
        etl = UnifiedETL()

        # Check tool is registered
        tool_config = find_tool("unified-etl")
        assert tool_config is not None
        assert tool_config.name == "unified-etl"
        assert tool_config.version == "1.0.0"
        assert tool_config.category == "core"

    def test_tool_config(self):
        """Test tool configuration metadata."""
        etl = UnifiedETL.__new__(UnifiedETL)  # Create without __init__
        config = etl.get_tool_config()

        assert config.name == "unified-etl"
        assert config.version == "1.0.0"
        assert "Unified ETL tool" in config.description
        assert config.category == "core"

        # Check dependencies
        expected_deps = ["sqlalchemy", "pandas", "python-dotenv", "requests"]
        for dep in expected_deps:
            assert dep in config.dependencies

        # Check usage examples
        assert len(config.usage_examples) > 0
        assert any("--mode focused" in example for example in config.usage_examples)

    def test_required_environment_vars(self):
        """Test required environment variables."""
        etl = UnifiedETL.__new__(UnifiedETL)  # Create without __init__
        required_vars = etl.get_required_environment_vars()

        assert "DATABASE_URL" in required_vars
        assert "YOUTUBE_API_KEY" in required_vars

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_focused_etl")
    def test_run_method_focused_mode(self, mock_focused):
        """Test run method with focused mode."""
        etl = UnifiedETL()

        etl.run(mode="focused")

        mock_focused.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_comprehensive_etl")
    def test_run_method_comprehensive_mode(self, mock_comprehensive):
        """Test run method with comprehensive mode."""
        etl = UnifiedETL()

        etl.run(mode="comprehensive")

        mock_comprehensive.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_channel_specific_etl")
    def test_run_method_with_channels(self, mock_channels):
        """Test run method with specific channels."""
        etl = UnifiedETL()

        channels = ["artist1", "artist2"]
        etl.run(channels=channels)

        mock_channels.assert_called_once_with(channels)

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_etl_with_notebooks")
    def test_run_method_with_notebooks(self, mock_notebooks):
        """Test run method with notebooks enabled."""
        etl = UnifiedETL()

        etl.run(mode="focused", with_notebooks=True)

        mock_notebooks.assert_called_once_with("focused")

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_production_pipeline")
    def test_run_method_production_mode(self, mock_production):
        """Test run method with production mode."""
        etl = UnifiedETL()

        etl.run(production=True)

        mock_production.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.validate_data_quality")
    def test_run_method_validate_only(self, mock_validate):
        """Test run method with validate only mode."""
        etl = UnifiedETL()

        etl.run(validate_only=True)

        mock_validate.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    def test_run_method_invalid_mode(self):
        """Test run method with invalid mode raises error."""
        etl = UnifiedETL()

        with pytest.raises(ValidationError) as exc_info:
            etl.run(mode="invalid")

        assert "Invalid ETL mode" in str(exc_info.value)


class TestUnifiedETLMethods:
    """Test individual ETL method implementations."""

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.run_focused_etl.main")
    def test_run_focused_etl(self, mock_focused_main):
        """Test focused ETL execution."""
        with patch("tools.core.etl.UnifiedETL.log_progress"):
            etl = UnifiedETL()
            etl.run_focused_etl()

        mock_focused_main.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.run_comprehensive_etl.main")
    def test_run_comprehensive_etl(self, mock_comprehensive_main):
        """Test comprehensive ETL execution."""
        with patch("tools.core.etl.UnifiedETL.log_progress"):
            etl = UnifiedETL()
            etl.run_comprehensive_etl()

        mock_comprehensive_main.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.run_channels_from_env.main")
    def test_run_channel_specific_etl(self, mock_channels_main):
        """Test channel-specific ETL execution."""
        etl = UnifiedETL()

        channels = ["artist1", "artist2"]

        with patch.dict("os.environ", {}, clear=False):
            etl.run_channel_specific_etl(channels)

        mock_channels_main.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_focused_etl")
    @patch("tools.development.run_notebooks.main")
    def test_run_etl_with_notebooks_focused(self, mock_notebooks, mock_focused):
        """Test ETL with notebooks in focused mode."""
        etl = UnifiedETL()

        etl.run_etl_with_notebooks(mode="focused")

        mock_focused.assert_called_once()
        mock_notebooks.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.etl.UnifiedETL.run_comprehensive_etl")
    @patch("tools.development.run_notebooks.main")
    def test_run_etl_with_notebooks_comprehensive(self, mock_notebooks, mock_comprehensive):
        """Test ETL with notebooks in comprehensive mode."""
        etl = UnifiedETL()

        etl.run_etl_with_notebooks(mode="comprehensive")

        mock_comprehensive.assert_called_once()
        mock_notebooks.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    def test_run_etl_with_notebooks_invalid_mode(self):
        """Test ETL with notebooks with invalid mode."""
        etl = UnifiedETL()

        with pytest.raises(ValidationError) as exc_info:
            etl.run_etl_with_notebooks(mode="invalid")

        assert "Invalid ETL mode" in str(exc_info.value)

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.run_production_pipeline.main")
    def test_run_production_pipeline(self, mock_production_main):
        """Test production pipeline execution."""
        etl = UnifiedETL()

        etl.run_production_pipeline()

        mock_production_main.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "test://db", "YOUTUBE_API_KEY": "test_key"})
    @patch("tools.core.data_quality_validator.main")
    def test_validate_data_quality(self, mock_validator_main):
        """Test data quality validation."""
        etl = UnifiedETL()

        etl.validate_data_quality()

        mock_validator_main.assert_called_once()


class TestCommandLineInterface:
    """Test command-line interface functionality."""

    @patch("tools.core.etl.UnifiedETL")
    def test_main_default_arguments(self, mock_etl_class):
        """Test main function with default arguments."""
        mock_etl = MagicMock()
        mock_etl_class.return_value.__enter__.return_value = mock_etl

        # Mock sys.argv
        with patch("sys.argv", ["etl.py"]):
            main()

        mock_etl.run.assert_called_once_with(
            mode="focused", channels=None, with_notebooks=False, production=False, validate_only=False
        )

    @patch("tools.core.etl.UnifiedETL")
    def test_main_comprehensive_mode(self, mock_etl_class):
        """Test main function with comprehensive mode."""
        mock_etl = MagicMock()
        mock_etl_class.return_value.__enter__.return_value = mock_etl

        with patch("sys.argv", ["etl.py", "--mode", "comprehensive"]):
            main()

        mock_etl.run.assert_called_once_with(
            mode="comprehensive", channels=None, with_notebooks=False, production=False, validate_only=False
        )

    @patch("tools.core.etl.UnifiedETL")
    def test_main_with_channels(self, mock_etl_class):
        """Test main function with specific channels."""
        mock_etl = MagicMock()
        mock_etl_class.return_value.__enter__.return_value = mock_etl

        with patch("sys.argv", ["etl.py", "--channels", "artist1,artist2"]):
            main()

        mock_etl.run.assert_called_once_with(
            mode="focused", channels=["artist1", "artist2"], with_notebooks=False, production=False, validate_only=False
        )

    @patch("tools.core.etl.UnifiedETL")
    def test_main_with_notebooks(self, mock_etl_class):
        """Test main function with notebooks enabled."""
        mock_etl = MagicMock()
        mock_etl_class.return_value.__enter__.return_value = mock_etl

        with patch("sys.argv", ["etl.py", "--with-notebooks"]):
            main()

        mock_etl.run.assert_called_once_with(
            mode="focused", channels=None, with_notebooks=True, production=False, validate_only=False
        )

    @patch("tools.core.etl.UnifiedETL")
    def test_main_production_mode(self, mock_etl_class):
        """Test main function with production mode."""
        mock_etl = MagicMock()
        mock_etl_class.return_value.__enter__.return_value = mock_etl

        with patch("sys.argv", ["etl.py", "--production"]):
            main()

        mock_etl.run.assert_called_once_with(
            mode="focused", channels=None, with_notebooks=False, production=True, validate_only=False
        )

    @patch("tools.core.etl.UnifiedETL")
    def test_main_validate_only(self, mock_etl_class):
        """Test main function with validate only mode."""
        mock_etl = MagicMock()
        mock_etl_class.return_value.__enter__.return_value = mock_etl

        with patch("sys.argv", ["etl.py", "--validate-only"]):
            main()

        mock_etl.run.assert_called_once_with(
            mode="focused", channels=None, with_notebooks=False, production=False, validate_only=True
        )
