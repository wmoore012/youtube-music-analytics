"""Tests for notebook template system."""

import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.youtubeviz.notebook_templates import NotebookConfig, StorytellingNotebook


class TestNotebookConfig:
    """Test NotebookConfig dataclass."""
    
    def test_basic_config_creation(self):
        """Test creating a basic configuration."""
        config = NotebookConfig(
            title="Test Analysis",
            analysis_type="artist_comparison",
            artists=["Artist A", "Artist B"]
        )
        
        assert config.title == "Test Analysis"
        assert config.analysis_type == "artist_comparison"
        assert config.artists == ["Artist A", "Artist B"]
        assert config.complexity_level == "beginner"  # default
        assert config.color_scheme == "vibrant"  # default
    
    def test_config_validation_complexity_level(self):
        """Test validation of complexity level."""
        with pytest.raises(ValueError, match="complexity_level must be one of"):
            NotebookConfig(
                title="Test",
                complexity_level="invalid"
            )
    
    def test_config_validation_narrative_style(self):
        """Test validation of narrative style."""
        with pytest.raises(ValueError, match="narrative_style must be one of"):
            NotebookConfig(
                title="Test",
                narrative_style="invalid"
            )
    
    def test_config_validation_color_scheme(self):
        """Test validation of color scheme."""
        with pytest.raises(ValueError, match="color_scheme must be one of"):
            NotebookConfig(
                title="Test",
                color_scheme="invalid"
            )
    
    def test_config_validation_date_range(self):
        """Test validation of date range."""
        with pytest.raises(ValueError, match="date_range_days must be positive"):
            NotebookConfig(
                title="Test",
                date_range_days=-5
            )
    
    def test_config_validation_chart_height(self):
        """Test validation of chart height."""
        with pytest.raises(ValueError, match="chart_height must be positive"):
            NotebookConfig(
                title="Test",
                chart_height=-100
            )
    
    @patch.dict(os.environ, {
        "ARTIST_COLOR_SCHEME": "pastel",
        "DEFAULT_CHART_HEIGHT": "800",
        "DEFAULT_CHART_WIDTH": "1200",
        "DEFAULT_ANALYSIS_DAYS": "60"
    })
    def test_load_env_defaults(self):
        """Test loading defaults from environment variables."""
        config = NotebookConfig(title="Test")
        
        assert config.color_scheme == "pastel"
        assert config.chart_height == 800
        assert config.chart_width == 1200
        assert config.date_range_days == 60
    
    @patch.dict(os.environ, {
        "YT_TAYLOR_SWIFT_YT": "https://youtube.com/channel/...",
        "YT_DRAKE_YT": "https://youtube.com/channel/...",
        "YT_THE_WEEKND_YT": "https://youtube.com/channel/..."
    })
    def test_from_env_artist_detection(self):
        """Test automatic artist detection from environment."""
        config = NotebookConfig.from_env("artist_comparison")
        
        assert "Taylor Swift" in config.artists
        assert "Drake" in config.artists
        assert "The Weeknd" in config.artists
        assert config.analysis_type == "artist_comparison"
        assert "Artist Comparison Analysis" in config.title
    
    def test_to_dict_serialization(self):
        """Test converting configuration to dictionary."""
        config = NotebookConfig(
            title="Test Analysis",
            artists=["Artist A"],
            complexity_level="intermediate"
        )
        
        config_dict = config.to_dict()
        
        assert config_dict["title"] == "Test Analysis"
        assert config_dict["artists"] == ["Artist A"]
        assert config_dict["complexity_level"] == "intermediate"
        assert isinstance(config_dict, dict)


class TestStorytellingNotebook:
    """Test StorytellingNotebook class."""
    
    def test_notebook_initialization(self):
        """Test notebook initialization with config."""
        config = NotebookConfig(title="Test Notebook")
        notebook = StorytellingNotebook(config)
        
        assert notebook.config == config
        assert notebook.sections == []
        assert notebook.current_section is None
    
    def test_add_section(self):
        """Test adding sections to notebook."""
        config = NotebookConfig(title="Test")
        notebook = StorytellingNotebook(config)
        
        notebook.add_section(
            "intro",
            "Introduction",
            "intro",
            "This is the introduction section"
        )
        
        assert len(notebook.sections) == 1
        assert notebook.sections[0]["id"] == "intro"
        assert notebook.sections[0]["title"] == "Introduction"
        assert notebook.sections[0]["type"] == "intro"
        assert notebook.sections[0]["description"] == "This is the introduction section"
        assert notebook.current_section == "intro"
    
    def test_add_markdown_cell(self):
        """Test adding markdown cells."""
        config = NotebookConfig(title="Test")
        notebook = StorytellingNotebook(config)
        
        notebook.add_section("intro", "Introduction")
        notebook.add_markdown_cell("# Hello World", "narrative")
        
        section = notebook.sections[0]
        assert len(section["cells"]) == 1
        
        cell = section["cells"][0]
        assert cell["cell_type"] == "markdown"
        assert cell["content_type"] == "narrative"
        assert cell["source"] == "# Hello World"
    
    def test_add_markdown_cell_without_section(self):
        """Test that adding markdown cell without section raises error."""
        config = NotebookConfig(title="Test")
        notebook = StorytellingNotebook(config)
        
        with pytest.raises(ValueError, match="No current section"):
            notebook.add_markdown_cell("Content")
    
    def test_add_code_cell(self):
        """Test adding code cells."""
        config = NotebookConfig(title="Test")
        notebook = StorytellingNotebook(config)
        
        notebook.add_section("analysis", "Analysis")
        notebook.add_code_cell(
            "print('Hello')",
            "Print hello message",
            {"chart_type": "bar_chart"}
        )
        
        section = notebook.sections[0]
        cell = section["cells"][0]
        
        assert cell["cell_type"] == "code"
        assert cell["source"] == "print('Hello')"
        assert cell["description"] == "Print hello message"
        assert cell["chart_context"]["chart_type"] == "bar_chart"
    
    def test_add_story_block_cell(self):
        """Test adding story block cells."""
        config = NotebookConfig(title="Test")
        notebook = StorytellingNotebook(config)
        
        notebook.add_section("analysis", "Analysis")
        notebook.add_story_block_cell(
            "fig = create_chart()",
            "Performance Chart",
            ["Point 1", "Point 2"],
            "This shows performance"
        )
        
        section = notebook.sections[0]
        cell = section["cells"][0]
        
        assert cell["cell_type"] == "code"
        assert "fig = create_chart()" in cell["source"]
        assert "story_block(" in cell["source"]
        assert "Performance Chart" in cell["source"]
    
    def test_method_chaining(self):
        """Test that methods support chaining."""
        config = NotebookConfig(title="Test")
        
        notebook = (StorytellingNotebook(config)
                   .add_section("intro", "Introduction")
                   .add_markdown_cell("# Intro")
                   .add_section("analysis", "Analysis")
                   .add_code_cell("print('test')"))
        
        assert len(notebook.sections) == 2
        assert notebook.sections[0]["id"] == "intro"
        assert notebook.sections[1]["id"] == "analysis"
        assert notebook.current_section == "analysis"
    
    def test_generate_notebook_json(self):
        """Test generating complete notebook JSON."""
        config = NotebookConfig(title="Test Notebook", artists=["Artist A"])
        notebook = StorytellingNotebook(config)
        
        notebook.add_section("intro", "Introduction", description="Intro section")
        notebook.add_markdown_cell("# Welcome")
        notebook.add_code_cell("print('hello')")
        
        notebook_json = notebook.generate_notebook_json()
        
        # Check basic structure
        assert notebook_json["nbformat"] == 4
        assert notebook_json["nbformat_minor"] == 4
        assert "cells" in notebook_json
        assert "metadata" in notebook_json
        
        # Check metadata
        assert notebook_json["metadata"]["storytelling_config"]["title"] == "Test Notebook"
        
        # Check cells
        cells = notebook_json["cells"]
        assert len(cells) >= 3  # Title + section header + content cells
        
        # First cell should be title
        assert cells[0]["cell_type"] == "markdown"
        assert "Test Notebook" in cells[0]["source"][0]
    
    def test_save_notebook(self):
        """Test saving notebook to file."""
        config = NotebookConfig(title="Test Notebook")
        notebook = StorytellingNotebook(config)
        
        notebook.add_section("intro", "Introduction")
        notebook.add_markdown_cell("# Test content")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            filepath = Path(temp_dir) / "test_notebook.ipynb"
            notebook.save_notebook(filepath)
            
            # Check file was created
            assert filepath.exists()
            
            # Check file content
            with open(filepath, 'r') as f:
                saved_notebook = json.load(f)
            
            assert saved_notebook["nbformat"] == 4
            assert "Test Notebook" in saved_notebook["cells"][0]["source"][0]
    
    def test_create_artist_comparison_template(self):
        """Test creating standard artist comparison template."""
        config = NotebookConfig(
            title="Artist Comparison",
            artists=["Artist A", "Artist B"],
            include_sentiment=True
        )
        
        notebook = StorytellingNotebook.create_artist_comparison_template(config)
        
        # Check that standard sections were created
        section_ids = [section["id"] for section in notebook.sections]
        
        expected_sections = [
            "introduction",
            "data_loading", 
            "performance_overview",
            "head_to_head",
            "engagement_analysis",
            "sentiment_analysis",  # Should be included since include_sentiment=True
            "recommendations"
        ]
        
        for expected in expected_sections:
            assert expected in section_ids
    
    def test_create_artist_comparison_template_no_sentiment(self):
        """Test creating template without sentiment analysis."""
        config = NotebookConfig(
            title="Artist Comparison",
            include_sentiment=False
        )
        
        notebook = StorytellingNotebook.create_artist_comparison_template(config)
        section_ids = [section["id"] for section in notebook.sections]
        
        assert "sentiment_analysis" not in section_ids
        assert "recommendations" in section_ids  # Should still have other sections


class TestIntegration:
    """Test integration between config and notebook classes."""
    
    @patch.dict(os.environ, {
        "YT_ARTIST_A_YT": "https://youtube.com/channel/1",
        "YT_ARTIST_B_YT": "https://youtube.com/channel/2",
        "ARTIST_COLOR_SCHEME": "pastel"
    })
    def test_end_to_end_notebook_creation(self):
        """Test complete notebook creation from environment."""
        # Create config from environment
        config = NotebookConfig.from_env("artist_comparison")
        
        # Create notebook template
        notebook = StorytellingNotebook.create_artist_comparison_template(config)
        
        # Add some content
        notebook.current_section = "introduction"
        notebook.add_markdown_cell("Welcome to our analysis!")
        
        # Generate notebook JSON
        notebook_json = notebook.generate_notebook_json()
        
        # Verify everything works together
        assert "Artist A" in config.artists
        assert "Artist B" in config.artists
        assert config.color_scheme == "pastel"
        assert len(notebook.sections) >= 6  # Standard sections
        assert notebook_json["nbformat"] == 4
        
        # Check that config is preserved in metadata
        saved_config = notebook_json["metadata"]["storytelling_config"]
        assert saved_config["artists"] == config.artists
        assert saved_config["color_scheme"] == "pastel"