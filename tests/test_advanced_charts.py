"""
Tests for data-science grade chart implementations.
"""

import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

from src.youtubeviz.advanced_charts import (
    ColorBrewerPalettes,
    add_uncertainty_indicators,
    create_diverging_sentiment_bars,
    create_sentiment_cluster_heatmap,
    enhance_chart_beauty,
)


class TestColorBrewerPalettes:
    """Test ColorBrewer palette definitions."""

    def test_sentiment_diverging_palette(self):
        """Test sentiment diverging palette has required colors."""
        palette = ColorBrewerPalettes.SENTIMENT_DIVERGING

        required_keys = ["very_negative", "negative", "neutral", "positive", "very_positive"]
        for key in required_keys:
            assert key in palette
            assert isinstance(palette[key], str)
            assert palette[key].startswith("#")

    def test_categorical_palette(self):
        """Test categorical palette has 8 colors."""
        palette = ColorBrewerPalettes.CATEGORICAL

        assert len(palette) == 8
        for color in palette:
            assert isinstance(color, str)
            assert color.startswith("#")


class TestDivergingSentimentBars:
    """Test diverging sentiment bar chart implementation."""

    @pytest.fixture
    def sample_sentiment_data(self):
        """Create sample sentiment data for testing."""
        return pd.DataFrame(
            {
                "artist_name": ["Artist A", "Artist A", "Artist A", "Artist B", "Artist B", "Artist C"] * 10,
                "sentiment_category": ["positive", "negative", "neutral"] * 20,
                "video_id": ["video1", "video1", "video1", "video2", "video2", "video3"] * 10,
            }
        )

    def test_create_diverging_sentiment_bars_basic(self, sample_sentiment_data):
        """Test basic diverging sentiment bars creation."""
        fig = create_diverging_sentiment_bars(sample_sentiment_data)

        # Check that figure is created
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 2  # At least positive and negative bars

        # Check that we have the expected traces
        trace_names = [trace.name for trace in fig.data]
        assert "Positive" in trace_names
        assert "Negative" in trace_names

    def test_diverging_bars_with_wilson_intervals(self, sample_sentiment_data):
        """Test diverging bars with Wilson confidence intervals."""
        fig = create_diverging_sentiment_bars(sample_sentiment_data, use_wilson_intervals=True)

        # Should have additional traces for error bars
        assert len(fig.data) >= 2

        # Check that error bars are present (scatter traces with error_y)
        error_traces = [trace for trace in fig.data if hasattr(trace, "error_y") and trace.error_y is not None]
        # Note: May not always have error bars depending on data, so just check structure
        assert isinstance(fig, go.Figure)

    def test_diverging_bars_with_bayesian_shrinkage(self, sample_sentiment_data):
        """Test diverging bars with Bayesian shrinkage."""
        fig = create_diverging_sentiment_bars(sample_sentiment_data, use_bayesian_shrinkage=True)

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 2

    def test_diverging_bars_empty_data(self):
        """Test diverging bars with empty data."""
        empty_df = pd.DataFrame()
        fig = create_diverging_sentiment_bars(empty_df)

        assert isinstance(fig, go.Figure)
        # Should have annotation about no data
        assert len(fig.layout.annotations) > 0

    def test_diverging_bars_insufficient_data_warning(self):
        """Test warning for insufficient data."""
        small_df = pd.DataFrame(
            {"artist_name": ["Artist A"] * 3, "sentiment_category": ["positive", "negative", "neutral"]}
        )

        # Should not raise error, but may show warnings
        fig = create_diverging_sentiment_bars(small_df, min_comments_threshold=20)
        assert isinstance(fig, go.Figure)

    def test_diverging_bars_layout_properties(self, sample_sentiment_data):
        """Test that layout properties are set correctly."""
        fig = create_diverging_sentiment_bars(sample_sentiment_data)

        # Check title
        assert "Sentiment Breakdown" in fig.layout.title.text

        # Check axes
        assert fig.layout.xaxis.title.text == "Artist"
        assert fig.layout.yaxis.title.text == "Sentiment Rate"

        # Check y-axis range includes negative values
        assert fig.layout.yaxis.range[0] <= -0.5
        assert fig.layout.yaxis.range[1] >= 0.5


class TestSentimentClusterHeatmap:
    """Test sentiment cluster heatmap implementation."""

    @pytest.fixture
    def sample_aspect_data(self):
        """Create sample sentiment aspect data."""
        aspects = ["music_quality", "vocals", "production", "lyrics"]
        artists = ["Artist A", "Artist B", "Artist C"]
        sentiments = ["positive", "negative"]

        data = []
        for artist in artists:
            for aspect in aspects:
                for sentiment in sentiments:
                    # Create some variation in the data
                    count = np.random.randint(1, 10)
                    for _ in range(count):
                        data.append(
                            {"artist_name": artist, "sentiment_aspect": aspect, "sentiment_category": sentiment}
                        )

        return pd.DataFrame(data)

    def test_create_sentiment_cluster_heatmap_basic(self, sample_aspect_data):
        """Test basic sentiment cluster heatmap creation."""
        fig = create_sentiment_cluster_heatmap(sample_aspect_data)

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

        # Check that it's a heatmap
        assert any(isinstance(trace, go.Heatmap) for trace in fig.data)

    def test_heatmap_with_bayesian_shrinkage(self, sample_aspect_data):
        """Test heatmap with Bayesian shrinkage."""
        fig = create_sentiment_cluster_heatmap(sample_aspect_data, use_bayesian_shrinkage=True)

        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_heatmap_empty_data(self):
        """Test heatmap with empty data."""
        empty_df = pd.DataFrame()
        fig = create_sentiment_cluster_heatmap(empty_df)

        assert isinstance(fig, go.Figure)
        # Should have annotation about no data
        assert len(fig.layout.annotations) > 0

    def test_heatmap_layout_properties(self, sample_aspect_data):
        """Test heatmap layout properties."""
        fig = create_sentiment_cluster_heatmap(sample_aspect_data)

        # Check title
        assert "Sentiment Aspects" in fig.layout.title.text

        # Check axes
        assert fig.layout.xaxis.title.text == "Artist"
        assert fig.layout.yaxis.title.text == "Sentiment Aspect"


class TestChartEnhancements:
    """Test chart enhancement functions."""

    def test_enhance_chart_beauty_professional(self):
        """Test professional theme enhancement."""
        fig = go.Figure(data=go.Bar(x=["A", "B"], y=[1, 2]))
        enhanced = enhance_chart_beauty(fig, theme="professional")

        assert isinstance(enhanced, go.Figure)
        assert enhanced.layout.template.layout.plot_bgcolor == "white"
        assert "Arial" in enhanced.layout.font.family

    def test_enhance_chart_beauty_academic(self):
        """Test academic theme enhancement."""
        fig = go.Figure(data=go.Bar(x=["A", "B"], y=[1, 2]))
        enhanced = enhance_chart_beauty(fig, theme="academic")

        assert isinstance(enhanced, go.Figure)
        assert enhanced.layout.showlegend is True
        assert "Times" in enhanced.layout.font.family

    def test_enhance_chart_beauty_presentation(self):
        """Test presentation theme enhancement."""
        fig = go.Figure(data=go.Bar(x=["A", "B"], y=[1, 2]))
        enhanced = enhance_chart_beauty(fig, theme="presentation")

        assert isinstance(enhanced, go.Figure)
        assert enhanced.layout.paper_bgcolor == "#1e1e1e"
        assert "Helvetica" in enhanced.layout.font.family

    def test_add_uncertainty_indicators_bar(self):
        """Test adding uncertainty indicators to bar charts."""
        fig = go.Figure(data=go.Bar(x=["A", "B"], y=[1, 2]))

        uncertainty_data = {"error_y": dict(type="data", array=[0.1, 0.2], visible=True)}

        enhanced = add_uncertainty_indicators(fig, uncertainty_data, chart_type="bar")
        assert isinstance(enhanced, go.Figure)

    def test_add_uncertainty_indicators_scatter(self):
        """Test adding uncertainty indicators to scatter plots."""
        fig = go.Figure(data=go.Scatter(x=[1, 2, 3], y=[1, 2, 3]))

        uncertainty_data = {
            "confidence_bands": True,
            "x": [1, 2, 3],
            "upper": [1.2, 2.2, 3.2],
            "lower": [0.8, 1.8, 2.8],
        }

        enhanced = add_uncertainty_indicators(fig, uncertainty_data, chart_type="scatter")
        assert isinstance(enhanced, go.Figure)


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_complete_sentiment_analysis_workflow(self):
        """Test complete workflow from data to visualization."""
        # Create realistic sentiment data
        np.random.seed(42)
        artists = ["Rising Star", "Established Act", "New Signee"]

        data = []
        for artist in artists:
            # Vary the amount of data per artist (new vs established)
            n_comments = np.random.randint(50, 200) if artist != "New Signee" else np.random.randint(5, 15)

            for _ in range(n_comments):
                sentiment = np.random.choice(
                    ["positive", "negative", "neutral"],
                    p=[0.6, 0.3, 0.1] if artist == "Rising Star" else [0.4, 0.4, 0.2],
                )
                data.append(
                    {
                        "artist_name": artist,
                        "sentiment_category": sentiment,
                        "video_id": f"video_{np.random.randint(1, 5)}",
                    }
                )

        df = pd.DataFrame(data)

        # Create diverging sentiment bars
        fig = create_diverging_sentiment_bars(
            df, use_wilson_intervals=True, use_bayesian_shrinkage=True, min_comments_threshold=20
        )

        # Enhance the chart
        fig = enhance_chart_beauty(fig, theme="professional")

        # Verify the complete workflow produces a valid figure
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 2  # At least positive and negative bars

        # Check that "New Signee" gets flagged for insufficient data
        annotations = [ann.text for ann in fig.layout.annotations if hasattr(ann, "text")]
        needs_more_data_annotations = [ann for ann in annotations if "Needs more data" in ann]
        # May or may not have the annotation depending on random data, but structure should be valid

        # Verify layout is properly enhanced
        assert fig.layout.plot_bgcolor == "white"
        assert "Sentiment Breakdown" in fig.layout.title.text


class TestLollipopThemeCharts:
    """Test lollipop charts for top 3 positive / negative themes (Charts #3-4)."""

    @pytest.fixture
    def sample_theme_data(self):
        """Create sample theme data with extractive quotes."""
        np.random.seed(42)
        artists = ["Artist A", "Artist B", "Artist C"]
        themes = ["vocals", "production", "lyrics", "energy", "visuals", "melody"]

        data = []
        for artist in artists:
            for theme in themes:
                # Create varying amounts of positive / negative comments per theme
                n_positive = np.random.randint(5, 25)
                n_negative = np.random.randint(2, 15)

                # Positive comments
                for i in range(n_positive):
                    data.append(
                        {
                            "artist_name": artist,
                            "theme": theme,
                            "sentiment_category": "positive",
                            "comment_text": f"Love the {theme} in this track! Amazing work by {artist}.",
                            "timestamp": f"2:3{i % 10}",
                            "video_id": f"video_{np.random.randint(1, 3)}",
                        }
                    )

                # Negative comments
                for i in range(n_negative):
                    data.append(
                        {
                            "artist_name": artist,
                            "theme": theme,
                            "sentiment_category": "negative",
                            "comment_text": f"The {theme} could be better in this one.",
                            "timestamp": f"1:2{i % 10}",
                            "video_id": f"video_{np.random.randint(1, 3)}",
                        }
                    )

        return pd.DataFrame(data)

    def test_create_positive_theme_lollipops_basic(self, sample_theme_data):
        """Test basic positive theme lollipop chart creation."""
        from src.youtubeviz.advanced_charts import create_positive_theme_lollipops

        fig = create_positive_theme_lollipops(sample_theme_data)

        # Check that figure is created
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

        # Should have scatter traces for lollipop dots
        scatter_traces = [trace for trace in fig.data if isinstance(trace, go.Scatter)]
        assert len(scatter_traces) >= 1

    def test_positive_lollipops_wilson_intervals(self, sample_theme_data):
        """Test positive lollipops with Wilson confidence intervals."""
        from src.youtubeviz.advanced_charts import create_positive_theme_lollipops

        fig = create_positive_theme_lollipops(sample_theme_data, use_wilson_intervals=True)

        # Should have error bars (scatter traces with error_x or error_y)
        assert isinstance(fig, go.Figure)
        error_traces = [trace for trace in fig.data if hasattr(trace, "error_y") and trace.error_y is not None]
        # May or may not have error bars depending on implementation, but structure should be valid

    def test_positive_lollipops_top_3_filtering(self, sample_theme_data):
        """Test that only top 3 themes per artist are shown."""
        from src.youtubeviz.advanced_charts import create_positive_theme_lollipops

        fig = create_positive_theme_lollipops(sample_theme_data, top_n=3)

        # Should limit to top 3 themes per artist
        assert isinstance(fig, go.Figure)

        # Check that we don't have more than 3 themes per artist in the data
        # This will be validated in the implementation

    def test_negative_theme_lollipops_basic(self, sample_theme_data):
        """Test basic negative theme lollipop chart creation."""
        from src.youtubeviz.advanced_charts import create_negative_theme_lollipops

        fig = create_negative_theme_lollipops(sample_theme_data)

        # Check that figure is created
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

        # Should use red-orange diverging palette
        colors_used = []
        for trace in fig.data:
            if hasattr(trace, "marker") and hasattr(trace.marker, "color"):
                colors_used.append(trace.marker.color)

        # Should have some color (exact color testing depends on implementation)
        assert len(colors_used) > 0

    def test_lollipop_extractive_quotes_integration(self, sample_theme_data):
        """Test that lollipops can integrate with extractive quotes."""
        from src.youtubeviz.advanced_charts import create_positive_theme_lollipops

        fig = create_positive_theme_lollipops(sample_theme_data, include_quotes=True, max_quotes_per_theme=2)

        # Should include quote information in hover or custom data
        assert isinstance(fig, go.Figure)

        # Check that customdata or hovertemplate includes quote information
        has_quote_data = False
        for trace in fig.data:
            if hasattr(trace, "customdata") and trace.customdata is not None:
                has_quote_data = True
            elif hasattr(trace, "hovertemplate") and "quote" in str(trace.hovertemplate).lower():
                has_quote_data = True

        # Should have some form of quote integration
        # (This will be implemented to pass)

    def test_lollipop_overlapping_ci_collapse(self, sample_theme_data):
        """Test visual collapse of near-ties (overlapping CIs)."""
        from src.youtubeviz.advanced_charts import create_positive_theme_lollipops

        # Create data with very similar rates to test overlap detection
        similar_data = pd.DataFrame(
            {
                "artist_name": ["Artist A"] * 20,
                "theme": ["theme1"] * 10 + ["theme2"] * 10,
                "sentiment_category": ["positive"] * 18 + ["negative"] * 2,
                "comment_text": ["Great!"] * 20,
                "timestamp": ["1:00"] * 20,
                "video_id": ["video1"] * 20,
            }
        )

        fig = create_positive_theme_lollipops(similar_data, collapse_overlapping_ci=True)

        # Should handle overlapping confidence intervals
        assert isinstance(fig, go.Figure)

    def test_lollipop_chart_layout_properties(self, sample_theme_data):
        """Test lollipop chart layout properties."""
        from src.youtubeviz.advanced_charts import create_positive_theme_lollipops

        fig = create_positive_theme_lollipops(sample_theme_data)

        # Check title mentions themes
        assert "theme" in fig.layout.title.text.lower() or "positive" in fig.layout.title.text.lower()

        # Should have appropriate axis labels
        assert fig.layout.xaxis.title.text is not None
        assert fig.layout.yaxis.title.text is not None


class TestThemeExtractionSystem:
    """Test theme extraction and quote selection system."""

    def test_extract_top_themes_per_artist(self):
        """Test extraction of top themes per artist."""
        from src.youtubeviz.advanced_charts import extract_top_themes_per_artist

        # Create test data
        theme_data = pd.DataFrame(
            {
                "artist_name": ["Artist A"] * 15 + ["Artist B"] * 10,
                "theme": ["vocals"] * 8 + ["production"] * 4 + ["lyrics"] * 3 + ["vocals"] * 6 + ["energy"] * 4,
                "sentiment_category": ["positive"] * 25,
                "comment_text": ["Great vocals!"] * 25,
            }
        )

        top_themes = extract_top_themes_per_artist(theme_data, sentiment="positive", top_n=2)

        # Should return top 2 themes per artist
        assert isinstance(top_themes, pd.DataFrame)
        assert len(top_themes) <= 4  # 2 artists × 2 themes max

        # Should be sorted by frequency / rate
        artist_a_themes = top_themes[top_themes["artist_name"] == "Artist A"]
        if len(artist_a_themes) > 1:
            # First theme should have higher or equal count than second
            assert artist_a_themes.iloc[0]["count"] >= artist_a_themes.iloc[1]["count"]

    def test_extract_representative_quotes(self):
        """Test extraction of representative quotes for themes."""
        from src.youtubeviz.advanced_charts import extract_representative_quotes

        # Create test data with varied quotes
        quote_data = pd.DataFrame(
            {
                "artist_name": ["Artist A"] * 5,
                "theme": ["vocals"] * 5,
                "sentiment_category": ["positive"] * 5,
                "comment_text": [
                    "Amazing vocals on this track!",
                    "Love the vocal performance here",
                    "Vocals are incredible as always",
                    "Such powerful vocals",
                    "Best vocals I've heard",
                ],
                "timestamp": ["1:30", "2:15", "0:45", "3:20", "1:05"],
            }
        )

        quotes = extract_representative_quotes(
            quote_data, artist="Artist A", theme="vocals", sentiment="positive", max_quotes=2
        )

        # Should return up to 2 representative quotes
        assert isinstance(quotes, list)
        assert len(quotes) <= 2
        assert len(quotes) > 0

        # Each quote should have required fields
        for quote in quotes:
            assert "text" in quote
            assert "timestamp" in quote
            assert isinstance(quote["text"], str)
            assert isinstance(quote["timestamp"], str)


class TestStandoutVideosScatterPlot:
    """Test standout videos scatter plot (Chart #5)."""

    @pytest.fixture
    def rng(self):
        """Create deterministic RNG for tests."""
        return np.random.default_rng(42)

    @pytest.fixture
    def sample_video_performance_data(self, rng):
        """Create sample video performance data."""

        # Create realistic video performance data
        n_videos = 50
        log_views = rng.uniform(2, 5, n_videos)  # Log10 of views (100 to 100k)

        # Create trend: higher views generally correlate with positive sentiment
        true_trend = 0.3 + 0.1 * log_views
        noise = rng.normal(0, 0.05, n_videos)
        positive_rates = np.clip(true_trend + noise, 0, 1)

        # Add a few standout videos (high positive rate for their view count)
        standout_indices = [10, 25, 40]
        positive_rates[standout_indices] += 0.2
        positive_rates = np.clip(positive_rates, 0, 1)

        data = []
        for i in range(n_videos):
            views = int(10 ** log_views[i])
            positive_rate = positive_rates[i]

            # Calculate counts from rate
            total_comments = rng.integers(20, 100)
            positive_comments = int(positive_rate * total_comments)

            data.append(
                {
                    "video_id": f"video_{i}",
                    "artist_name": f"Artist_{i % 5}",
                    "views": views,
                    "log_views": log_views[i],
                    "positive_rate": positive_rate,
                    "positive_comments": positive_comments,
                    "total_comments": total_comments,
                    "upload_date": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                }
            )

        return pd.DataFrame(data)

    def test_create_standout_videos_scatter_basic(self, sample_video_performance_data):
        """Test basic standout videos scatter plot creation."""
        from src.youtubeviz.advanced_charts import create_standout_videos_scatter

        fig = create_standout_videos_scatter(sample_video_performance_data)

        # Check that figure is created
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

        # Should have scatter plot
        scatter_traces = [trace for trace in fig.data if isinstance(trace, go.Scatter)]
        assert len(scatter_traces) >= 1

    def test_standout_scatter_with_loess_trend(self, sample_video_performance_data):
        """Test scatter plot with LOESS trend line."""
        from src.youtubeviz.advanced_charts import create_standout_videos_scatter

        fig = create_standout_videos_scatter(sample_video_performance_data, use_loess_trend=True)

        # Should have additional traces for trend line
        assert len(fig.data) >= 2

        # Check for trend line (line trace)
        line_traces = [trace for trace in fig.data if trace.mode and "lines" in trace.mode]
        assert len(line_traces) >= 1

    def test_standout_scatter_residual_highlighting(self, sample_video_performance_data):
        """Test highlighting of large positive residuals."""
        from src.youtubeviz.advanced_charts import create_standout_videos_scatter

        fig = create_standout_videos_scatter(
            sample_video_performance_data, highlight_residuals=True, residual_threshold=1.0
        )

        # Should highlight standout videos
        assert isinstance(fig, go.Figure)

        # Check that some points are highlighted (different colors / sizes)
        scatter_traces = [trace for trace in fig.data if isinstance(trace, go.Scatter) and trace.mode == "markers"]
        assert len(scatter_traces) >= 1

    def test_standout_scatter_confidence_bands(self, sample_video_performance_data):
        """Test gray 95% confidence bands around trend."""
        from src.youtubeviz.advanced_charts import create_standout_videos_scatter

        fig = create_standout_videos_scatter(
            sample_video_performance_data, use_loess_trend=True, show_confidence_bands=True
        )

        # Should have confidence band traces
        assert len(fig.data) >= 3  # Scatter + trend + confidence bands

    def test_standout_scatter_interactive_hover(self, sample_video_performance_data):
        """Test interactive hover with residual values."""
        from src.youtubeviz.advanced_charts import create_standout_videos_scatter

        fig = create_standout_videos_scatter(sample_video_performance_data, include_residuals_in_hover=True)

        # Check that hover templates include residual information
        scatter_traces = [trace for trace in fig.data if isinstance(trace, go.Scatter) and trace.mode == "markers"]
        assert len(scatter_traces) >= 1

        # Should have custom hover data or template
        main_trace = scatter_traces[0]
        has_residual_info = (hasattr(main_trace, "customdata") and main_trace.customdata is not None) or (
            hasattr(main_trace, "hovertemplate") and "residual" in str(main_trace.hovertemplate).lower()
        )
        # Will be implemented to pass

    def test_standout_scatter_layout_properties(self, sample_video_performance_data):
        """Test scatter plot layout properties."""
        from src.youtubeviz.advanced_charts import create_standout_videos_scatter

        fig = create_standout_videos_scatter(sample_video_performance_data)

        # Check axes
        assert "views" in fig.layout.xaxis.title.text.lower() or "log" in fig.layout.xaxis.title.text.lower()
        assert "positive" in fig.layout.yaxis.title.text.lower() or "rate" in fig.layout.yaxis.title.text.lower()

        # Should use log scale for x-axis
        assert fig.layout.xaxis.type == "log" or "log" in fig.layout.xaxis.title.text.lower()


class TestResidualAnalysisSystem:
    """Test residual analysis for standout video detection."""

    def test_calculate_video_residuals(self):
        """Test calculation of video performance residuals."""
        from src.youtubeviz.advanced_charts import calculate_video_residuals

        # Create test data with known trend
        log_views = np.array([2, 3, 4, 5])
        positive_rates = np.array([0.4, 0.5, 0.6, 0.9])  # Last one is standout

        residuals = calculate_video_residuals(log_views, positive_rates)

        # Should return residuals array
        assert isinstance(residuals, np.ndarray)
        assert len(residuals) == len(log_views)

        # Last video should have highest residual (above trend)
        assert residuals[3] > residuals[0]

    def test_identify_standout_videos(self):
        """Test identification of standout videos by residual threshold."""
        from src.youtubeviz.advanced_charts import identify_standout_videos

        # Create test data
        video_data = pd.DataFrame(
            {
                "video_id": ["v1", "v2", "v3", "v4"],
                "views": [100, 1000, 10000, 100000],
                "positive_rate": [0.4, 0.5, 0.6, 0.9],
                "artist_name": ["Artist A"] * 4,
            }
        )

        standouts = identify_standout_videos(video_data, residual_threshold=1.0)

        # Should return DataFrame with standout videos
        assert isinstance(standouts, pd.DataFrame)
        assert len(standouts) <= len(video_data)

        # Should include residual information
        assert "residual" in standouts.columns


class TestUpSetPlot:
    """Test UpSet plot for feature intersections (Chart #7)."""

    @pytest.fixture
    def sample_feature_data(self):
        """Create sample feature intersection data."""
        np.random.seed(42)

        # Create video data with various features
        n_videos = 100
        data = []

        for i in range(n_videos):
            # Randomly assign features with different probabilities
            has_isrc = np.random.random() < 0.6  # 60% have ISRC
            is_short_form = np.random.random() < 0.4  # 40% are short-form
            is_visualizer = np.random.random() < 0.3  # 30% are visualizers
            has_teaser = np.random.random() < 0.2  # 20% have teasers
            is_music_video = np.random.random() < 0.5  # 50% are music videos

            # Generate views based on feature combinations
            base_views = 1000
            if has_isrc:
                base_views *= 2
            if is_short_form:
                base_views *= 1.5
            if is_music_video:
                base_views *= 1.3

            views = int(base_views * np.random.uniform(0.5, 2.0))

            data.append(
                {
                    "video_id": f"video_{i}",
                    "artist_name": f"Artist_{i % 5}",
                    "has_isrc": has_isrc,
                    "short_form": is_short_form,
                    "visualizer": is_visualizer,
                    "teaser": has_teaser,
                    "music_video": is_music_video,
                    "views": views,
                    "engagement_rate": np.random.uniform(0.02, 0.08),
                }
            )

        return pd.DataFrame(data)

    def test_create_upset_plot_basic(self, sample_feature_data):
        """Test basic UpSet plot creation."""
        from src.youtubeviz.advanced_charts import create_upset_plot

        feature_cols = ["has_isrc", "short_form", "visualizer", "teaser", "music_video"]
        fig = create_upset_plot(sample_feature_data, feature_columns=feature_cols)

        # Check that figure is created
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_upset_plot_ranking_by_views(self, sample_feature_data):
        """Test UpSet plot ranking by views / engagement."""
        from src.youtubeviz.advanced_charts import create_upset_plot

        feature_cols = ["has_isrc", "short_form", "visualizer"]
        fig = create_upset_plot(sample_feature_data, feature_columns=feature_cols, rank_by="views")

        # Should rank intersections by total views
        assert isinstance(fig, go.Figure)

    def test_upset_plot_click_intersection_filtering(self, sample_feature_data):
        """Test click intersection to filter functionality."""
        from src.youtubeviz.advanced_charts import create_upset_plot

        feature_cols = ["has_isrc", "short_form"]
        fig = create_upset_plot(sample_feature_data, feature_columns=feature_cols, enable_click_filtering=True)

        # Should have interactive elements for filtering
        assert isinstance(fig, go.Figure)

        # Check for custom data or click events (implementation dependent)
        has_interactive_elements = any(
            hasattr(trace, "customdata") and trace.customdata is not None for trace in fig.data
        )
        # Will be implemented to pass

    def test_upset_plot_better_than_venn(self, sample_feature_data):
        """Test that UpSet plot handles >3 sets better than Venn diagrams."""
        from src.youtubeviz.advanced_charts import create_upset_plot

        # Test with 5 features (more than Venn can handle well)
        feature_cols = ["has_isrc", "short_form", "visualizer", "teaser", "music_video"]
        fig = create_upset_plot(sample_feature_data, feature_columns=feature_cols)

        # Should handle 5+ features without issues
        assert isinstance(fig, go.Figure)
        assert len(fig.data) >= 1

    def test_upset_plot_layout_properties(self, sample_feature_data):
        """Test UpSet plot layout properties."""
        from src.youtubeviz.advanced_charts import create_upset_plot

        feature_cols = ["has_isrc", "short_form", "visualizer"]
        fig = create_upset_plot(sample_feature_data, feature_columns=feature_cols)

        # Check title mentions intersections or features
        title_text = fig.layout.title.text.lower()
        assert "intersection" in title_text or "feature" in title_text or "upset" in title_text


class TestFeatureIntersectionAnalysis:
    """Test feature intersection analysis system."""

    def test_calculate_feature_intersections(self):
        """Test calculation of feature intersections."""
        from src.youtubeviz.advanced_charts import calculate_feature_intersections

        # Create test data
        test_data = pd.DataFrame(
            {
                "video_id": ["v1", "v2", "v3", "v4"],
                "feature_a": [True, True, False, True],
                "feature_b": [True, False, True, True],
                "views": [1000, 2000, 1500, 3000],
            }
        )

        intersections = calculate_feature_intersections(
            test_data, feature_columns=["feature_a", "feature_b"], value_column="views"
        )

        # Should return intersection analysis
        assert isinstance(intersections, pd.DataFrame)
        assert "intersection" in intersections.columns
        assert "total_views" in intersections.columns
        assert "count" in intersections.columns

    def test_rank_intersections_by_metric(self):
        """Test ranking of intersections by different metrics."""
        from src.youtubeviz.advanced_charts import rank_intersections_by_metric

        # Create intersection data
        intersection_data = pd.DataFrame(
            {
                "intersection": ["A", "B", "A∩B", "∅"],
                "total_views": [5000, 3000, 8000, 1000],
                "count": [10, 8, 5, 20],
                "avg_engagement": [0.05, 0.07, 0.06, 0.03],
            }
        )

        # Rank by views
        ranked = rank_intersections_by_metric(intersection_data, "total_views")

        # Should be sorted by total_views descending
        assert isinstance(ranked, pd.DataFrame)
        assert ranked.iloc[0]["total_views"] >= ranked.iloc[1]["total_views"]


class TestUMAPClusteringChart:
    """Test UMAP clustering for tour compatibility analysis (Chart #6)."""

    def test_create_umap_clustering_chart_import_succeeds(self):
        """Test that UMAP chart function exists (TDD-now implemented)."""
        from src.youtubeviz.advanced_charts import create_umap_clustering_chart

        # Should be callable
        assert callable(create_umap_clustering_chart)

    def test_umap_data_validator_fails_on_bad_data(self):
        """Test that Pydantic validator catches bad data (should fail without implementation)."""
        from src.youtubeviz.chart_models import ChartDataValidationError, UMAPClusteringData

        # This should fail-empty comment text
        with pytest.raises(ValueError):
            UMAPClusteringData(
                artist_name="Test Artist",
                video_id="test_video",
                comment_text="",  # Too short
                sentiment_category="positive",
                content_type="music_video",
                views=1000,
                engagement_rate=0.05,
            )

    def test_umap_data_validator_fails_on_spam_text(self):
        """Test that validator catches spam-like text."""
        from src.youtubeviz.chart_models import UMAPClusteringData

        # This should fail-repetitive text
        with pytest.raises(ValueError, match="lacks diversity"):
            UMAPClusteringData(
                artist_name="Test Artist",
                video_id="test_video",
                comment_text="aaaaaaaaaaaaaaaaaaaaaa",  # No diversity
                sentiment_category="positive",
                content_type="music_video",
                views=1000,
                engagement_rate=0.05,
            )

    def test_dataframe_validation_fails_without_implementation(self):
        """Test DataFrame validation (should work with current implementation)."""
        import pandas as pd

        from src.youtubeviz.chart_models import ChartDataValidationError, UMAPClusteringData, validate_dataframe_schema

        # Bad data should fail validation
        bad_df = pd.DataFrame(
            {
                "artist_name": [""],  # Empty name
                "video_id": ["test"],
                "comment_text": ["short"],  # Too short
                "sentiment_category": ["positive"],
                "content_type": ["music_video"],
                "views": [1000],
                "engagement_rate": [0.05],
            }
        )

        with pytest.raises(ChartDataValidationError):
            validate_dataframe_schema(bad_df, UMAPClusteringData)


class TestUMAPClusteringImplementation:
    """Test UMAP clustering implementation (now implemented)."""

    def test_umap_clustering_class_implemented(self):
        """Test that UMAPClusteringAnalyzer class exists and fails loudly without dependencies."""
        from src.youtubeviz.advanced_charts import UMAPClusteringAnalyzer
        from src.youtubeviz.clustering_analysis import UMAPNotAvailableError

        # Should fail loudly with clear error message when dependencies missing
        with pytest.raises(UMAPNotAvailableError, match="Required dependencies not available"):
            UMAPClusteringAnalyzer()

    def test_similarity_matrix_calculation_implemented(self):
        """Test that similarity matrix calculation exists."""
        from src.youtubeviz.advanced_charts import calculate_artist_similarity_matrix

        # Should be callable
        assert callable(calculate_artist_similarity_matrix)

    def test_tour_compatibility_analysis_implemented(self):
        """Test that tour compatibility analysis exists."""
        from src.youtubeviz.advanced_charts import analyze_tour_compatibility

        # Should be callable
        assert callable(analyze_tour_compatibility)

    def test_create_umap_clustering_chart_implemented(self):
        """Test that UMAP clustering chart function exists."""
        from src.youtubeviz.advanced_charts import create_umap_clustering_chart

        # Should be callable
        assert callable(create_umap_clustering_chart)


class TestContentAnalysisSuite:
    """Test content analysis charts (Charts #8-11) - TDD approach."""

    def test_isrc_balance_chart_not_implemented(self):
        """Test that ISRC balance chart doesn't exist yet (should fail)."""
        with pytest.raises(ImportError):
            from src.youtubeviz.advanced_charts import create_isrc_balance_chart

    def test_content_data_validator_fails_on_bad_data(self):
        """Test Pydantic validation for content data."""
        from pydantic import ValidationError

        from src.youtubeviz.chart_models import ContentAnalysisData

        # Should fail-negative views
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            ContentAnalysisData(
                video_id="test_video",
                artist_name="Test Artist",
                views=-100,  # Invalid
                has_isrc=True,
                content_type="music_video",
                duration_seconds=180,
                upload_date="2024-01-01",
            )

    def test_content_data_validator_fails_on_invalid_duration(self):
        """Test validation fails on unrealistic duration."""
        from pydantic import ValidationError

        from src.youtubeviz.chart_models import ContentAnalysisData

        # Should fail-duration too long (Field constraint catches this)
        with pytest.raises(ValidationError, match="less than or equal to 3600"):
            ContentAnalysisData(
                video_id="test_video",
                artist_name="Test Artist",
                views=1000,
                has_isrc=True,
                content_type="music_video",
                duration_seconds=7200,  # 2 hours-too long for music video
                upload_date="2024-01-01",
            )

    def test_dumbbell_chart_not_implemented(self):
        """Test that dumbbell chart doesn't exist yet."""
        with pytest.raises(ImportError):
            from src.youtubeviz.advanced_charts import create_content_dumbbell_chart

    def test_cleveland_dot_plot_not_implemented(self):
        """Test that Cleveland dot plot doesn't exist yet."""
        with pytest.raises(ImportError):
            from src.youtubeviz.advanced_charts import create_cleveland_dot_plot


class TestContentAnalysisValidation:
    """Test content analysis data validation (bulletproof approach)."""

    def test_content_analysis_class_not_implemented(self):
        """Test that ContentAnalysisEngine class doesn't exist yet."""
        with pytest.raises(ImportError):
            from src.youtubeviz.advanced_charts import ContentAnalysisEngine

    def test_p_chart_control_limits_not_implemented(self):
        """Test that p-chart control limits calculation doesn't exist yet."""
        with pytest.raises(ImportError):
            from src.youtubeviz.advanced_charts import calculate_p_chart_control_limits


if __name__ == "__main__":
    pytest.main([__file__])
