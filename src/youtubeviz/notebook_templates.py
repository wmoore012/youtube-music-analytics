"""Notebook template system for consistent storytelling structure."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union
from pathlib import Path


@dataclass
class NotebookConfig:
    """Configuration for storytelling notebooks.
    
    Manages all the settings needed to generate consistent, engaging
    notebooks with proper theming, artist information, and analysis parameters.
    """
    
    # Basic notebook information
    title: str
    analysis_type: str = "artist_comparison"
    description: Optional[str] = None
    
    # Artist and data configuration
    artists: List[str] = field(default_factory=list)
    date_range_days: int = 30
    include_sentiment: bool = True
    
    # Storytelling configuration
    complexity_level: str = "beginner"  # beginner, intermediate, advanced
    narrative_style: str = "engaging"   # engaging, professional, academic
    include_educational_sidebars: bool = True
    
    # Visual configuration
    color_scheme: str = "vibrant"       # vibrant, pastel, monochrome
    chart_height: int = 600
    chart_width: Optional[int] = None
    
    # Business context
    target_audience: str = "music_industry"  # music_industry, academic, general
    business_focus: str = "investment"       # investment, marketing, research
    
    # Technical configuration
    database_config: Dict[str, Any] = field(default_factory=dict)
    output_format: str = "html"             # html, pdf, slides
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_config()
        self._load_env_defaults()
    
    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        valid_complexity = ["beginner", "intermediate", "advanced"]
        if self.complexity_level not in valid_complexity:
            raise ValueError(f"complexity_level must be one of {valid_complexity}")
        
        valid_styles = ["engaging", "professional", "academic"]
        if self.narrative_style not in valid_styles:
            raise ValueError(f"narrative_style must be one of {valid_styles}")
        
        valid_schemes = ["vibrant", "pastel", "monochrome"]
        if self.color_scheme not in valid_schemes:
            raise ValueError(f"color_scheme must be one of {valid_schemes}")
        
        if self.date_range_days <= 0:
            raise ValueError("date_range_days must be positive")
        
        if self.chart_height <= 0:
            raise ValueError("chart_height must be positive")
    
    def _load_env_defaults(self) -> None:
        """Load default values from environment variables."""
        # Load artist color scheme from .env if available
        env_color_scheme = os.getenv("ARTIST_COLOR_SCHEME")
        if env_color_scheme and env_color_scheme in ["vibrant", "pastel", "monochrome"]:
            self.color_scheme = env_color_scheme
        
        # Load chart dimensions from .env
        env_height = os.getenv("DEFAULT_CHART_HEIGHT")
        if env_height and env_height.isdigit():
            self.chart_height = int(env_height)
        
        env_width = os.getenv("DEFAULT_CHART_WIDTH")
        if env_width and env_width.isdigit():
            self.chart_width = int(env_width)
        
        # Load analysis parameters
        env_date_range = os.getenv("DEFAULT_ANALYSIS_DAYS")
        if env_date_range and env_date_range.isdigit():
            self.date_range_days = int(env_date_range)
    
    @classmethod
    def from_env(cls, analysis_type: str = "artist_comparison") -> NotebookConfig:
        """Create configuration from environment variables.
        
        Automatically detects artists from YT_*_YT environment variables
        and loads other configuration from .env file.
        """
        # Auto-detect artists from environment
        artists = []
        for key, value in os.environ.items():
            if key.startswith("YT_") and key.endswith("_YT") and value:
                # Extract artist name from YT_ARTISTNAME_YT format
                artist_name = key[3:-3].replace("_", " ").title()
                artists.append(artist_name)
        
        return cls(
            title=f"{analysis_type.replace('_', ' ').title()} Analysis",
            analysis_type=analysis_type,
            artists=artists,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for serialization."""
        return {
            "title": self.title,
            "analysis_type": self.analysis_type,
            "description": self.description,
            "artists": self.artists,
            "date_range_days": self.date_range_days,
            "include_sentiment": self.include_sentiment,
            "complexity_level": self.complexity_level,
            "narrative_style": self.narrative_style,
            "include_educational_sidebars": self.include_educational_sidebars,
            "color_scheme": self.color_scheme,
            "chart_height": self.chart_height,
            "chart_width": self.chart_width,
            "target_audience": self.target_audience,
            "business_focus": self.business_focus,
            "database_config": self.database_config,
            "output_format": self.output_format,
        }


class StorytellingNotebook:
    """Template system for creating consistent storytelling notebooks.
    
    Manages notebook structure, section organization, and content generation
    to ensure all notebooks follow the same engaging, educational format.
    """
    
    def __init__(self, config: NotebookConfig):
        """Initialize notebook with configuration.
        
        Args:
            config: NotebookConfig instance with all settings
        """
        self.config = config
        self.sections: List[Dict[str, Any]] = []
        self.current_section: Optional[str] = None
    
    def add_section(
        self,
        section_id: str,
        title: str,
        section_type: str = "analysis",
        description: Optional[str] = None,
    ) -> StorytellingNotebook:
        """Add a new section to the notebook.
        
        Args:
            section_id: Unique identifier for the section
            title: Display title for the section
            section_type: Type of section (intro, analysis, conclusion, etc.)
            description: Optional description of what this section covers
            
        Returns:
            Self for method chaining
        """
        section = {
            "id": section_id,
            "title": title,
            "type": section_type,
            "description": description,
            "cells": [],
        }
        
        self.sections.append(section)
        self.current_section = section_id
        return self
    
    def add_markdown_cell(
        self,
        content: str,
        cell_type: str = "narrative",
    ) -> StorytellingNotebook:
        """Add a markdown cell to the current section.
        
        Args:
            content: Markdown content for the cell
            cell_type: Type of cell (narrative, educational, transition, etc.)
            
        Returns:
            Self for method chaining
        """
        if not self.current_section:
            raise ValueError("No current section. Call add_section() first.")
        
        cell = {
            "cell_type": "markdown",
            "content_type": cell_type,
            "source": content,
        }
        
        # Find current section and add cell
        for section in self.sections:
            if section["id"] == self.current_section:
                section["cells"].append(cell)
                break
        
        return self
    
    def add_code_cell(
        self,
        code: str,
        description: Optional[str] = None,
        chart_context: Optional[Dict[str, Any]] = None,
    ) -> StorytellingNotebook:
        """Add a code cell to the current section.
        
        Args:
            code: Python code for the cell
            description: Optional description of what the code does
            chart_context: Optional context for chart interpretation
            
        Returns:
            Self for method chaining
        """
        if not self.current_section:
            raise ValueError("No current section. Call add_section() first.")
        
        cell = {
            "cell_type": "code",
            "source": code,
            "description": description,
            "chart_context": chart_context,
        }
        
        # Find current section and add cell
        for section in self.sections:
            if section["id"] == self.current_section:
                section["cells"].append(cell)
                break
        
        return self
    
    def add_story_block_cell(
        self,
        chart_code: str,
        title: str,
        bullets: List[str],
        caption: Optional[str] = None,
        chart_context: Optional[Dict[str, Any]] = None,
    ) -> StorytellingNotebook:
        """Add a story block (chart + narrative) cell.
        
        Args:
            chart_code: Code to generate the chart
            title: Title for the story block
            bullets: Key points explaining the chart
            caption: Optional caption
            chart_context: Optional context for chart interpretation
            
        Returns:
            Self for method chaining
        """
        # Create the complete code that generates chart and displays story block
        full_code = f"""# {title}
{chart_code}

# Display with story block
from src.youtubeviz.storytelling import story_block

story_block(
    fig=fig,
    title="{title}",
    bullets={bullets!r},
    caption={caption!r} if {caption!r} else None,
    theme="{self._get_theme()}"
)"""
        
        return self.add_code_cell(
            code=full_code,
            description=f"Generate and display: {title}",
            chart_context=chart_context,
        )
    
    def generate_notebook_json(self) -> Dict[str, Any]:
        """Generate complete Jupyter notebook JSON structure.
        
        Returns:
            Dictionary representing a complete Jupyter notebook
        """
        cells = []
        
        # Add title cell
        title_cell = {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"# {self.config.title}\n\n{self._generate_notebook_header()}"]
        }
        cells.append(title_cell)
        
        # Add sections
        for section in self.sections:
            # Add section header
            section_header = {
                "cell_type": "markdown", 
                "metadata": {},
                "source": [f"## {section['title']}\n\n{section.get('description', '')}"]
            }
            cells.append(section_header)
            
            # Add section cells
            for cell in section["cells"]:
                if cell["cell_type"] == "markdown":
                    nb_cell = {
                        "cell_type": "markdown",
                        "metadata": {},
                        "source": [cell["source"]]
                    }
                elif cell["cell_type"] == "code":
                    nb_cell = {
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": [cell["source"]]
                    }
                
                cells.append(nb_cell)
        
        # Create complete notebook structure
        notebook = {
            "cells": cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "name": "python",
                    "version": "3.8.0"
                },
                "storytelling_config": self.config.to_dict(),
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        
        return notebook
    
    def save_notebook(self, filepath: Union[str, Path]) -> None:
        """Save notebook to file.
        
        Args:
            filepath: Path where to save the notebook
        """
        import json
        
        notebook_json = self.generate_notebook_json()
        
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(notebook_json, f, indent=2, ensure_ascii=False)
    
    def _generate_notebook_header(self) -> str:
        """Generate the notebook header with configuration info."""
        header_parts = []
        
        if self.config.description:
            header_parts.append(self.config.description)
        
        if self.config.artists:
            artist_list = ", ".join(self.config.artists)
            header_parts.append(f"**Artists:** {artist_list}")
        
        header_parts.append(f"**Analysis Period:** Last {self.config.date_range_days} days")
        header_parts.append(f"**Target Audience:** {self.config.target_audience.replace('_', ' ').title()}")
        
        return "\n\n".join(header_parts)
    
    def _get_theme(self) -> str:
        """Get theme setting for story blocks."""
        return "light"  # Could be made configurable later
    
    @classmethod
    def create_artist_comparison_template(cls, config: NotebookConfig) -> StorytellingNotebook:
        """Create a standard artist comparison notebook template.
        
        Args:
            config: Configuration for the notebook
            
        Returns:
            StorytellingNotebook with standard artist comparison structure
        """
        notebook = cls(config)
        
        # Add standard sections for artist comparison
        notebook.add_section(
            "introduction",
            "Introduction & Overview", 
            "intro",
            "Setting the stage for our artist comparison analysis"
        )
        
        notebook.add_section(
            "data_loading",
            "Data Loading & Preparation",
            "setup", 
            "Loading and preparing YouTube data for analysis"
        )
        
        notebook.add_section(
            "performance_overview",
            "Performance Overview",
            "analysis",
            "High-level performance metrics across all artists"
        )
        
        notebook.add_section(
            "head_to_head",
            "Head-to-Head Comparison", 
            "analysis",
            "Direct comparison of key performance indicators"
        )
        
        notebook.add_section(
            "engagement_analysis",
            "Engagement Deep Dive",
            "analysis", 
            "Understanding audience engagement patterns"
        )
        
        if config.include_sentiment:
            notebook.add_section(
                "sentiment_analysis",
                "Sentiment Analysis",
                "analysis",
                "What fans are saying in the comments"
            )
        
        notebook.add_section(
            "recommendations",
            "Strategic Recommendations",
            "conclusion",
            "Data-driven insights for investment and marketing decisions"
        )
        
        return notebook