"""Tests for enhanced storytelling module functions."""

import pytest
from src.youtubeviz.storytelling import (
    narrative_intro,
    educational_sidebar,
    section_transition,
    chart_context,
)


class TestNarrativeIntro:
    """Test narrative introduction generation."""
    
    def test_artist_comparison_with_single_artist(self):
        """Test intro generation for single artist comparison."""
        result = narrative_intro(
            analysis_type="artist_comparison",
            context={"artists": ["Taylor Swift"]}
        )
        
        assert "Taylor Swift" in result
        assert "🎵" in result or "🚀" in result or "💡" in result
        assert len(result) > 100  # Should be substantial content
        
    def test_artist_comparison_with_multiple_artists(self):
        """Test intro generation for multiple artist comparison."""
        result = narrative_intro(
            analysis_type="artist_comparison", 
            context={"artists": ["Drake", "The Weeknd", "Justin Bieber"]}
        )
        
        assert "Drake" in result
        assert "The Weeknd" in result
        assert "Justin Bieber" in result
        assert " and " in result  # Should format list properly
        
    def test_sentiment_analysis_intro(self):
        """Test intro generation for sentiment analysis."""
        result = narrative_intro(analysis_type="sentiment_analysis")
        
        assert "sentiment" in result.lower()
        assert "💬" in result
        assert "comments" in result.lower()
        
    def test_generic_analysis_intro(self):
        """Test intro generation for unknown analysis types."""
        result = narrative_intro(analysis_type="custom_analysis")
        
        assert "custom_analysis" in result
        assert "📊" in result
        assert len(result) > 50
        
    def test_empty_context_handling(self):
        """Test that function handles empty or None context gracefully."""
        result1 = narrative_intro("artist_comparison", context=None)
        result2 = narrative_intro("artist_comparison", context={})
        
        assert "our featured artists" in result1
        assert "our featured artists" in result2
        assert len(result1) > 50
        assert len(result2) > 50


class TestEducationalSidebar:
    """Test educational sidebar generation."""
    
    def test_engagement_rate_beginner(self):
        """Test beginner-level engagement rate explanation."""
        result = educational_sidebar("engagement_rate", "beginner")
        
        assert "engagement rate" in result.lower()
        assert "📚" in result
        assert "applause at a concert" in result.lower()
        assert len(result) > 100
        
    def test_engagement_rate_intermediate(self):
        """Test intermediate-level engagement rate explanation."""
        result = educational_sidebar("engagement_rate", "intermediate")
        
        assert "engagement rate =" in result.lower()
        assert "📊" in result
        assert "benchmark" in result.lower()
        assert "%" in result
        
    def test_engagement_rate_advanced(self):
        """Test advanced-level engagement rate explanation."""
        result = educational_sidebar("engagement_rate", "advanced")
        
        assert "engagement velocity" in result.lower()
        assert "🔬" in result
        assert "24 hours" in result.lower()
        
    def test_momentum_explanations(self):
        """Test momentum explanations at all levels."""
        beginner = educational_sidebar("momentum", "beginner")
        intermediate = educational_sidebar("momentum", "intermediate")
        advanced = educational_sidebar("momentum", "advanced")
        
        assert "momentum" in beginner.lower()
        assert "🚀" in beginner
        
        assert "percentage change" in intermediate.lower()
        assert "📈" in intermediate
        
        assert "trend decomposition" in advanced.lower()
        assert "⚡" in advanced
        
    def test_youtube_algorithm_explanations(self):
        """Test YouTube algorithm explanations."""
        result = educational_sidebar("youtube_algorithm", "beginner")
        
        assert "algorithm" in result.lower()
        assert "🤖" in result
        assert "digital dj" in result.lower()
        
    def test_unknown_concept_fallback(self):
        """Test fallback for unknown concepts."""
        result = educational_sidebar("unknown_concept", "beginner")
        
        assert "unknown concept" in result.lower()
        assert "💡" in result
        assert "music industry analytics" in result.lower()
        
    def test_invalid_complexity_level(self):
        """Test handling of invalid complexity levels."""
        result = educational_sidebar("engagement_rate", "expert")
        
        # Should fall back to generic explanation
        assert "engagement rate" in result.lower()
        assert "💡" in result


class TestSectionTransition:
    """Test section transition generation."""
    
    def test_overview_to_comparison_transition(self):
        """Test transition from overview to comparison section."""
        result = section_transition("overview", "comparison")
        
        assert "---" in result  # Should include markdown separator
        assert "comparison" in result.lower()
        assert len(result) > 50
        
    def test_comparison_to_deep_dive_transition(self):
        """Test transition from comparison to deep dive section."""
        result = section_transition("comparison", "deep_dive")
        
        assert "deep" in result.lower() or "dig" in result.lower()
        assert "🔍" in result or "💎" in result or "🕵️‍♀️" in result
        
    def test_deep_dive_to_recommendations_transition(self):
        """Test transition from deep dive to recommendations."""
        result = section_transition("deep_dive", "recommendations")
        
        assert "action" in result.lower() or "strategic" in result.lower()
        assert "💡" in result or "💰" in result or "🎯" in result
        
    def test_analysis_to_sentiment_transition(self):
        """Test transition from analysis to sentiment section."""
        result = section_transition("analysis", "sentiment")
        
        assert "sentiment" in result.lower()
        assert "💬" in result or "🗣️" in result or "❤️" in result
        
    def test_unknown_section_transition(self):
        """Test transition between unknown sections."""
        result = section_transition("custom_section", "another_section")
        
        assert "custom_section" in result
        assert "another_section" in result
        assert "🔄" in result
        
    def test_transition_with_key_insight(self):
        """Test transition generation with key insight."""
        insight = "Engagement rates are 40% higher on weekends"
        result = section_transition(
            "overview", 
            "comparison", 
            key_insight=insight
        )
        
        assert insight in result
        assert "Key insight from overview" in result
        assert "*" in result  # Should be italicized
        
    def test_transition_formatting(self):
        """Test that transitions are properly formatted."""
        result = section_transition("overview", "comparison")
        
        assert result.startswith("\n---\n\n")
        assert result.endswith("\n")


class TestChartContext:
    """Test chart context generation."""
    
    def test_line_chart_context(self):
        """Test context generation for line charts."""
        what_to_look_for = ["Upward trends", "Seasonal patterns", "Sudden spikes"]
        result = chart_context("line_chart", what_to_look_for)
        
        assert "📈" in result
        assert "timeline" in result.lower()
        assert "Upward trends" in result
        assert "Seasonal patterns" in result
        assert "Sudden spikes" in result
        
    def test_bar_chart_context(self):
        """Test context generation for bar charts."""
        what_to_look_for = ["Highest performers", "Relative differences"]
        result = chart_context("bar_chart", what_to_look_for)
        
        assert "📊" in result
        assert "comparing" in result.lower()
        assert "• Highest performers" in result
        assert "• Relative differences" in result
        
    def test_scatter_plot_context(self):
        """Test context generation for scatter plots."""
        what_to_look_for = ["Correlation patterns", "Outliers"]
        result = chart_context("scatter_plot", what_to_look_for)
        
        assert "🎯" in result
        assert "relationships" in result.lower()
        assert "correlation" in result.lower()
        
    def test_chart_context_with_business_implications(self):
        """Test chart context with business implications."""
        what_to_look_for = ["Growth trends"]
        business_implications = ["Invest in growing artists", "Reduce spend on declining content"]
        
        result = chart_context(
            "line_chart", 
            what_to_look_for, 
            business_implications
        )
        
        assert "💼 Business Impact:" in result
        assert "Invest in growing artists" in result
        assert "Reduce spend on declining content" in result
        
    def test_unknown_chart_type(self):
        """Test handling of unknown chart types."""
        result = chart_context("custom_chart", ["Pattern 1", "Pattern 2"])
        
        assert "custom chart" in result.lower()
        assert "📊" in result
        assert "Pattern 1" in result
        assert "Pattern 2" in result
        
    def test_empty_what_to_look_for(self):
        """Test handling of empty what_to_look_for list."""
        result = chart_context("line_chart", [])
        
        assert "📈" in result
        assert "timeline" in result.lower()
        # Should still generate basic context even with empty list
        
    def test_chart_context_formatting(self):
        """Test proper formatting of chart context."""
        what_to_look_for = ["Item 1", "Item 2"]
        business_implications = ["Implication 1", "Implication 2"]
        
        result = chart_context("bar_chart", what_to_look_for, business_implications)
        
        # Check bullet point formatting
        assert "• Item 1" in result
        assert "• Item 2" in result
        assert "• Implication 1" in result
        assert "• Implication 2" in result


class TestIntegration:
    """Test integration between storytelling functions."""
    
    def test_complete_narrative_flow(self):
        """Test using multiple storytelling functions together."""
        # Generate intro
        intro = narrative_intro(
            "artist_comparison",
            context={"artists": ["Artist A", "Artist B"]}
        )
        
        # Generate educational content
        education = educational_sidebar("engagement_rate", "beginner")
        
        # Generate transition
        transition = section_transition("overview", "comparison")
        
        # Generate chart context
        chart_help = chart_context(
            "bar_chart",
            ["Compare engagement rates", "Look for clear winners"],
            ["Invest in higher-engagement artists"]
        )
        
        # All should be substantial content
        assert len(intro) > 100
        assert len(education) > 100
        assert len(transition) > 50
        assert len(chart_help) > 100
        
        # All should contain appropriate emojis and formatting
        assert any(emoji in intro for emoji in ["🎵", "🚀", "💡"])
        assert "📚" in education
        assert "---" in transition
        assert "📊" in chart_help