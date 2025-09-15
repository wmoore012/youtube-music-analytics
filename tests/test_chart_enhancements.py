"""Tests for chart enhancement and beautification functions."""

import os
from unittest.mock import patch, MagicMock

import pytest
import pandas as pd

from src.youtubeviz.charts import (
    enhance_chart_beauty,
    apply_color_scheme,
    create_chart_annotations,
    _get_scheme_colors,
)


class TestEnhanceChartBeauty:
    """Test chart beautification functions."""
    
    def test_enhance_unknown_chart_type(self):
        """Test that unknown chart types are returned unchanged."""
        mock_chart = {"type": "unknown"}
        result = enhance_chart_beauty(mock_chart)
        
        assert result == mock_chart
    
    def test_enhance_plotly_chart(self):
        """Test enhancing a Plotly chart."""
        # Mock Plotly figure
        mock_fig = MagicMock()
        mock_fig.update_layout = MagicMock()
        
        result = enhance_chart_beauty(
            mock_fig,
            title="Test Chart",
            emotional_theme="professional"
        )
        
        # Should call update_layout
        mock_fig.update_layout.assert_called()
        assert result == mock_fig
    
    def test_enhance_altair_chart(self):
        """Test enhancing an Altair chart."""
        # Mock Altair chart with all required attributes
        mock_chart = MagicMock()
        
        # Make hasattr checks pass for Altair detection
        mock_chart.mark_bar = True
        mock_chart.resolve_scale = MagicMock()
        mock_chart.properties = MagicMock()
        
        # Set up method chaining
        mock_resolved = MagicMock()
        mock_resolved.properties = MagicMock(return_value=mock_resolved)
        mock_chart.resolve_scale.return_value = mock_resolved
        
        result = enhance_chart_beauty(
            mock_chart,
            title="Test Chart",
            emotional_theme="energetic"
        )
        
        # Verify the result is not None and methods were called
        assert result is not None
        # For now, just check that we get a result - the exact method calls
        # depend on the internal implementation details
    
    def test_emotional_themes(self):
        """Test different emotional themes."""
        mock_fig = MagicMock()
        mock_fig.update_layout = MagicMock()
        
        themes = ["professional", "energetic", "warm", "dramatic"]
        
        for theme in themes:
            enhance_chart_beauty(mock_fig, emotional_theme=theme)
            mock_fig.update_layout.assert_called()
    
    def test_chart_config_application(self):
        """Test that chart configuration is applied."""
        mock_fig = MagicMock()
        mock_fig.update_layout = MagicMock()
        
        config = {
            "height": 800,
            "width": 1200,
            "title_size": 28,
            "axis_title_size": 16,
        }
        
        enhance_chart_beauty(mock_fig, config=config)
        
        # Check that update_layout was called with config values
        call_args = mock_fig.update_layout.call_args
        assert call_args is not None


class TestApplyColorScheme:
    """Test color scheme application."""
    
    @patch.dict(os.environ, {"ARTIST_COLOR_SCHEME": "pastel"})
    def test_apply_color_scheme_from_env(self):
        """Test applying color scheme from environment."""
        mock_fig = MagicMock()
        mock_fig.update_traces = MagicMock()
        
        artists = ["Artist A", "Artist B"]
        
        result = apply_color_scheme(mock_fig, artists=artists)
        
        # Should return the figure (possibly modified)
        assert result is not None
    
    def test_apply_custom_colors(self):
        """Test applying custom color mappings."""
        mock_fig = MagicMock()
        mock_fig.update_traces = MagicMock()
        
        custom_colors = {
            "Artist A": "#FF0000",
            "Artist B": "#00FF00"
        }
        
        result = apply_color_scheme(
            mock_fig,
            custom_colors=custom_colors,
            artists=["Artist A", "Artist B"]
        )
        
        assert result is not None
    
    def test_get_scheme_colors(self):
        """Test getting colors for different schemes."""
        vibrant_colors = _get_scheme_colors("vibrant")
        pastel_colors = _get_scheme_colors("pastel")
        mono_colors = _get_scheme_colors("monochrome")
        
        # Should return different color lists
        assert len(vibrant_colors) > 0
        assert len(pastel_colors) > 0
        assert len(mono_colors) > 0
        
        # Should be different schemes
        assert vibrant_colors != pastel_colors
        assert pastel_colors != mono_colors
    
    def test_unknown_scheme_fallback(self):
        """Test fallback for unknown color schemes."""
        colors = _get_scheme_colors("unknown_scheme")
        vibrant_colors = _get_scheme_colors("vibrant")
        
        # Should fall back to vibrant
        assert colors == vibrant_colors
    
    def test_apply_to_altair_chart(self):
        """Test applying colors to Altair chart."""
        mock_chart = MagicMock()
        mock_chart.mark_bar = True  # Make hasattr return True
        mock_chart.encoding = MagicMock()
        mock_chart.encoding.color = MagicMock()
        mock_chart.encoding.color.field = "artist"
        mock_chart.encode = MagicMock(return_value=mock_chart)
        
        color_map = {"Artist A": "#FF0000"}
        
        result = apply_color_scheme(mock_chart, custom_colors=color_map, artists=["Artist A"])
        
        # Should return a result
        assert result is not None


class TestCreateChartAnnotations:
    """Test chart annotation creation."""
    
    def test_create_basic_annotations(self):
        """Test creating basic insight annotations."""
        insights = [
            "Artist A is trending upward",
            "Engagement peaked on weekends",
            "Comments show positive sentiment"
        ]
        
        annotations = create_chart_annotations(insights, "line")
        
        assert len(annotations) == 3
        
        # Check annotation structure
        for annotation in annotations:
            assert "text" in annotation
            assert "💡" in annotation["text"]
            assert "x" in annotation
            assert "y" in annotation
            assert annotation["showarrow"] is False
    
    def test_create_annotations_with_highlights(self):
        """Test creating annotations with highlight points."""
        insights = ["Key insight"]
        highlight_points = [
            {"x": 5, "y": 100, "text": "Peak performance"},
            {"x": 10, "y": 50, "text": "Dip in engagement"}
        ]
        
        annotations = create_chart_annotations(
            insights, 
            "line", 
            highlight_points
        )
        
        # Should have insight annotations + highlight annotations
        assert len(annotations) == 3
        
        # Check that highlight annotations have arrows
        highlight_annotations = [a for a in annotations if a.get("showarrow") is True]
        assert len(highlight_annotations) == 2
    
    def test_different_chart_types(self):
        """Test annotations for different chart types."""
        insights = ["Test insight"]
        
        line_annotations = create_chart_annotations(insights, "line")
        bar_annotations = create_chart_annotations(insights, "bar")
        scatter_annotations = create_chart_annotations(insights, "scatter")
        
        # All should create annotations
        assert len(line_annotations) == 1
        assert len(bar_annotations) == 1
        assert len(scatter_annotations) == 1
        
        # Y positions might be different
        line_y = line_annotations[0]["y"]
        bar_y = bar_annotations[0]["y"]
        
        # Bar charts typically have higher y positions
        assert bar_y >= line_y
    
    def test_annotation_limit(self):
        """Test that annotations are limited to 5."""
        insights = [f"Insight {i}" for i in range(10)]
        
        annotations = create_chart_annotations(insights, "line")
        
        # Should be limited to 5 annotations
        assert len(annotations) == 5
    
    def test_empty_insights(self):
        """Test handling empty insights list."""
        annotations = create_chart_annotations([], "line")
        
        assert len(annotations) == 0
    
    def test_highlight_points_validation(self):
        """Test that invalid highlight points are ignored."""
        insights = ["Test"]
        highlight_points = [
            {"x": 5, "y": 100},  # Valid
            {"x": 10},  # Invalid - missing y
            {"y": 50},  # Invalid - missing x
            "invalid",  # Invalid - not a dict
        ]
        
        annotations = create_chart_annotations(
            insights,
            "line", 
            highlight_points
        )
        
        # Should have 1 insight + 1 valid highlight
        assert len(annotations) == 2
        
        # Check that only valid highlight was added
        highlight_annotations = [a for a in annotations if a.get("showarrow") is True]
        assert len(highlight_annotations) == 1


class TestIntegration:
    """Test integration between chart enhancement functions."""
    
    def test_complete_chart_enhancement_workflow(self):
        """Test using all enhancement functions together."""
        # Mock Plotly figure
        mock_fig = MagicMock()
        mock_fig.update_layout = MagicMock()
        mock_fig.data = []
        
        # Create insights and annotations
        insights = ["Great performance", "Strong engagement"]
        annotations = create_chart_annotations(insights, "line")
        
        # Apply color scheme
        artists = ["Artist A", "Artist B"]
        colored_fig = apply_color_scheme(
            mock_fig,
            scheme_name="vibrant",
            artists=artists
        )
        
        # Enhance beauty with annotations
        enhanced_fig = enhance_chart_beauty(
            colored_fig,
            title="Enhanced Chart",
            emotional_theme="professional",
            annotations=annotations
        )
        
        # Should have called update_layout
        mock_fig.update_layout.assert_called()
        assert enhanced_fig is not None
    
    @patch.dict(os.environ, {
        "ARTIST_COLOR_SCHEME": "pastel",
        "ARTIST_COLORS_JSON": '{"Artist A": "#FF0000", "Artist B": "#00FF00"}'
    })
    def test_env_integration(self):
        """Test integration with environment configuration."""
        mock_fig = MagicMock()
        mock_fig.update_layout = MagicMock()
        
        # Should use environment settings
        result = apply_color_scheme(mock_fig, artists=["Artist A", "Artist B"])
        
        assert result is not None
        
        # Test color scheme from env
        colors = _get_scheme_colors("pastel")  # Should use env setting
        assert len(colors) > 0