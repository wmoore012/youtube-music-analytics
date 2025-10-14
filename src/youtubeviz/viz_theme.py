"""
Visualization Theme and Color Palette Management

Provides centralized color palette and theme configuration for all charts
in the MusicScope™ dashboard to ensure visual consistency.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional
import plotly.express as px

logger = logging.getLogger(__name__)

# Default color palette (fallback if config file not found)
DEFAULT_ARTIST_COLORS = {
    "BiC Fizzle": "#8dd3c7",
    "COBRAH": "#fb8072",
    "Flyana Boss": "#bebada",
    "hicorook": "#fdb462",
    "Raiche": "#80b1d3",
    "re6ce": "#fccde5"
}

# Special color constants for specific chart types
BREAKOUT_COLOR = "#FF6B6B"  # Bright red for breakout periods (Chart 19a)
NORMAL_COLOR = "#CCCCCC"    # Grey for normal periods (Chart 19a)
GOOD_COLOR = "#2E7D32"      # Green for positive/increase (Charts 20, 21, 23)
BAD_COLOR = "#C62828"       # Red for negative/decrease (Charts 20, 21, 23)
NEUTRAL_COLOR = "#5f6b7a"   # Neutral grey


def get_artist_color_palette(config_path: Optional[str] = None) -> Dict[str, str]:
    """
    Load the global artist color palette from configuration file.
    
    This ensures all charts use consistent colors for each artist across
    the entire dashboard.
    
    Args:
        config_path: Optional path to artist_colors.json. If None, uses default location.
    
    Returns:
        Dictionary mapping artist names to hex color codes
    
    Example:
        >>> palette = get_artist_color_palette()
        >>> palette["BiC Fizzle"]
        '#8dd3c7'
    """
    if config_path is None:
        # Default location: config/artist_colors.json
        config_path = Path(__file__).parent.parent.parent / "config" / "artist_colors.json"
    else:
        config_path = Path(config_path)
    
    try:
        with open(config_path, 'r') as f:
            artist_colors = json.load(f)
        logger.info(f"Loaded artist color palette with {len(artist_colors)} artists from {config_path}")
        return artist_colors
    except FileNotFoundError:
        logger.warning(f"Artist color config not found at {config_path}, using default palette")
        return DEFAULT_ARTIST_COLORS.copy()
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in artist color config: {e}, using default palette")
        return DEFAULT_ARTIST_COLORS.copy()
    except Exception as e:
        logger.error(f"Error loading artist color config: {e}, using default palette")
        return DEFAULT_ARTIST_COLORS.copy()


def get_artist_color(artist_name: str, palette: Optional[Dict[str, str]] = None) -> str:
    """
    Get the color for a specific artist.
    
    Args:
        artist_name: Name of the artist
        palette: Optional pre-loaded palette. If None, loads from config.
    
    Returns:
        Hex color code for the artist
    """
    if palette is None:
        palette = get_artist_color_palette()
    
    return palette.get(artist_name, "#999999")  # Default grey if artist not found


def build_color_discrete_map(artists: list, palette: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Build a color_discrete_map for Plotly Express charts.
    
    This is used with px.scatter, px.line, etc. to ensure consistent artist colors.
    
    Args:
        artists: List of artist names in the data
        palette: Optional pre-loaded palette. If None, loads from config.
    
    Returns:
        Dictionary suitable for px.scatter(color_discrete_map=...)
    
    Example:
        >>> artists = ["BiC Fizzle", "COBRAH", "hicorook"]
        >>> color_map = build_color_discrete_map(artists)
        >>> fig = px.scatter(df, x="views", y="likes", color="artist_name",
        ...                  color_discrete_map=color_map)
    """
    if palette is None:
        palette = get_artist_color_palette()
    
    return {artist: palette.get(artist, "#999999") for artist in artists}


def get_color_sequence(artists: list, palette: Optional[Dict[str, str]] = None) -> list:
    """
    Get an ordered list of colors for a list of artists.
    
    This is used with go.Figure when adding traces manually.
    
    Args:
        artists: Ordered list of artist names
        palette: Optional pre-loaded palette. If None, loads from config.
    
    Returns:
        List of hex color codes in the same order as artists
    
    Example:
        >>> artists = ["BiC Fizzle", "COBRAH", "hicorook"]
        >>> colors = get_color_sequence(artists)
        >>> for artist, color in zip(artists, colors):
        ...     fig.add_trace(go.Scatter(name=artist, marker_color=color))
    """
    if palette is None:
        palette = get_artist_color_palette()
    
    return [palette.get(artist, "#999999") for artist in artists]


def should_use_global_palette(chart_name: str) -> bool:
    """
    Determine if a chart should use the global artist color palette.
    
    Some charts have special color logic (e.g., grey bars for low performers,
    red/green for increase/decrease) and should NOT use global artist colors.
    
    Args:
        chart_name: Name of the chart function
    
    Returns:
        True if chart should use global palette, False otherwise
    """
    # Charts that should NOT use global artist colors
    excluded_charts = {
        'create_momentum_bar_race',           # Chart 19a: Uses grey/red dynamic switching
        'create_budget_reallocation_chart',   # Chart 20: Uses red/green for increase/decrease
        'create_artist_momentum_tracker',     # Chart 21: Uses grey/orange/green thresholds (when time_window_weeks=None)
        'create_growth_signal_breakdown',     # Chart 23: Uses grey gradient for low performers
    }
    
    return chart_name not in excluded_charts


# Export commonly used colors
__all__ = [
    'get_artist_color_palette',
    'get_artist_color',
    'build_color_discrete_map',
    'get_color_sequence',
    'should_use_global_palette',
    'BREAKOUT_COLOR',
    'NORMAL_COLOR',
    'GOOD_COLOR',
    'BAD_COLOR',
    'NEUTRAL_COLOR',
]

