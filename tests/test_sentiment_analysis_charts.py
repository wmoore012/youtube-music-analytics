"""
TDD tests for sentiment analysis chart functions-Task 2
These tests will fail initially and drive the implementation of sentiment analysis charts.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest


class TestSentimentAnalysisCharts:
    """Test suite for sentiment analysis chart functions"""

    def setup_method(self):
        """Set up test data for sentiment analysis"""
        # Sample sentiment data
        self.sentiment_data = pd.DataFrame(
            [
                {
                    "artist_name": "Flyana Boss",
                    "comment": "Love this track!",
                    "sentiment_score": 0.8,
                    "sentiment_category": "positive",
                },
                {
                    "artist_name": "Flyana Boss",
                    "comment": "Amazing vocals",
                    "sentiment_score": 0.9,
                    "sentiment_category": "positive",
                },
                {
                    "artist_name": "Flyana Boss",
                    "comment": "Not my style",
                    "sentiment_score": -0.3,
                    "sentiment_category": "negative",
                },
                {
                    "artist_name": "BiC Fizzle",
                    "comment": "Fire beat!",
                    "sentiment_score": 0.7,
                    "sentiment_category": "positive",
                },
                {
                    "artist_name": "BiC Fizzle",
                    "comment": "Could be better",
                    "sentiment_score": -0.2,
                    "sentiment_category": "negative",
                },
                {
                    "artist_name": "COBRAH",
                    "comment": "Incredible energy",
                    "sentiment_score": 0.85,
                    "sentiment_category": "positive",
                },
            ]
        )

        # Sample video data with sentiment
        self.video_sentiment_data = pd.DataFrame(
            [
                {
                    "artist_name": "Flyana Boss",
                    "video_title": "New Track",
                    "views": 50000,
                    "positive_sentiment_pct": 75,
                    "negative_sentiment_pct": 25,
                },
                {
                    "artist_name": "Flyana Boss",
                    "video_title": "Behind Scenes",
                    "views": 25000,
                    "positive_sentiment_pct": 85,
                    "negative_sentiment_pct": 15,
                },
                {
                    "artist_name": "BiC Fizzle",
                    "video_title": "Music Video",
                    "views": 100000,
                    "positive_sentiment_pct": 60,
                    "negative_sentiment_pct": 40,
                },
            ]
        )

    def test_divergent_stacked_bar_chart_creation(self):
        """Test divergent stacked bar chart explaining sentiment breakdown by artist"""
        from youtubeviz.charts import create_divergent_sentiment_chart

        # Test that function exists and returns a chart
        chart = create_divergent_sentiment_chart(
            df=self.sentiment_data, artist_col="artist_name", sentiment_col="sentiment_category"
        )

        # Should return a plotly figure
        assert chart is not None
        assert hasattr(chart, "data")  # Plotly figure has data attribute
        assert len(chart.data) > 0  # Should have traces

    def test_divergent_chart_shows_positive_and_negative_breakdown(self):
        """Test that divergent chart shows both positive and negative sentiment"""
        from youtubeviz.charts import create_divergent_sentiment_chart

        chart = create_divergent_sentiment_chart(
            df=self.sentiment_data, artist_col="artist_name", sentiment_col="sentiment_category"
        )

        # Should have traces for both positive and negative sentiment
        trace_names = [trace.name for trace in chart.data]
        assert any("positive" in name.lower() for name in trace_names)
        assert any("negative" in name.lower() for name in trace_names)

    def test_sentiment_cluster_analysis_chart(self):
        """Test sentiment cluster analysis chart showing sentiment model categories in action"""
        from youtubeviz.charts import create_sentiment_cluster_chart

        chart = create_sentiment_cluster_chart(
            df=self.sentiment_data,
            sentiment_score_col="sentiment_score",
            category_col="sentiment_category",
            artist_col="artist_name",
        )

        assert chart is not None
        assert hasattr(chart, "data")
        # Should show clustering of sentiment scores
        assert len(chart.data) > 0

    def test_top_positive_comments_extraction(self):
        """Test extraction of top 3 positive things fans say about each artist"""
        from youtubeviz.sentiment import extract_top_positive_comments

        result = extract_top_positive_comments(
            df=self.sentiment_data,
            artist_col="artist_name",
            comment_col="comment",
            sentiment_col="sentiment_category",
            top_n=3,
        )

        # Should return dictionary with artists as keys
        assert isinstance(result, dict)
        assert "Flyana Boss" in result
        assert "BiC Fizzle" in result

        # Each artist should have list of positive comments
        for artist, comments in result.items():
            assert isinstance(comments, list)
            assert len(comments) <= 3  # Top 3 max

    def test_top_negative_comments_with_percentages(self):
        """Test extraction of top 3 negative things with percentage breakdown"""
        from youtubeviz.sentiment import extract_top_negative_comments_with_percentages

        result = extract_top_negative_comments_with_percentages(
            df=self.sentiment_data,
            artist_col="artist_name",
            comment_col="comment",
            sentiment_col="sentiment_category",
            top_n=3,
        )

        # Should return dictionary with artist data
        assert isinstance(result, dict)

        for artist, data in result.items():
            assert "negative_comments" in data
            assert "positive_percentage" in data
            assert "negative_percentage" in data

            # Percentages should add up to 100
            pos_pct = data["positive_percentage"]
            neg_pct = data["negative_percentage"]
            assert abs((pos_pct + neg_pct) - 100) < 0.1  # Allow for rounding

    def test_standout_video_identification(self):
        """Test identification of standout videos with high positive sentiment but normal view counts"""
        from youtubeviz.sentiment import identify_standout_videos

        result = identify_standout_videos(
            df=self.video_sentiment_data,
            views_col="views",
            positive_sentiment_col="positive_sentiment_pct",
            min_positive_threshold=80,  # High positive sentiment
            max_views_threshold=30000,  # Normal view counts
        )

        # Should return DataFrame with standout videos
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

        # All results should meet criteria
        for _, row in result.iterrows():
            assert row["positive_sentiment_pct"] >= 80
            assert row["views"] <= 30000

    def test_roster_wide_sentiment_analysis(self):
        """Test roster-wide sentiment analysis grouping artists by fan types"""
        from youtubeviz.sentiment import analyze_roster_sentiment

        result = analyze_roster_sentiment(
            df=self.sentiment_data, artist_col="artist_name", sentiment_col="sentiment_category", comment_col="comment"
        )

        # Should return analysis for all artists
        assert isinstance(result, dict)
        assert len(result) > 0

        # Each artist should have sentiment analysis
        for artist, analysis in result.items():
            assert "fan_type" in analysis
            assert "sentiment_summary" in analysis
            assert "tour_compatibility" in analysis

    def test_tour_compatibility_grouping(self):
        """Test grouping artists by fan type compatibility for tours"""
        from youtubeviz.sentiment import group_artists_for_tours

        # Mock sentiment analysis results
        sentiment_analysis = {
            "Flyana Boss": {"fan_type": "energetic", "sentiment_summary": "positive"},
            "BiC Fizzle": {"fan_type": "energetic", "sentiment_summary": "mixed"},
            "COBRAH": {"fan_type": "intense", "sentiment_summary": "positive"},
        }

        result = group_artists_for_tours(sentiment_analysis)

        # Should return tour groupings
        assert isinstance(result, dict)
        assert "tour_groups" in result

        # Artists with similar fan types should be grouped together
        tour_groups = result["tour_groups"]
        assert len(tour_groups) > 0


class TestSentimentDataProcessing:
    """Test suite for sentiment data processing functions"""

    def test_sentiment_percentage_calculation(self):
        """Test calculation of sentiment percentages"""
        from youtubeviz.sentiment import calculate_sentiment_percentages

        # Sample data
        data = pd.DataFrame(
            [
                {"artist": "Artist1", "sentiment": "positive"},
                {"artist": "Artist1", "sentiment": "positive"},
                {"artist": "Artist1", "sentiment": "negative"},
                {"artist": "Artist2", "sentiment": "positive"},
            ]
        )

        result = calculate_sentiment_percentages(df=data, artist_col="artist", sentiment_col="sentiment")

        # Should return percentages for each artist
        assert isinstance(result, pd.DataFrame)
        assert "positive_pct" in result.columns
        assert "negative_pct" in result.columns

        # Artist1 should have 66.7% positive, 33.3% negative
        artist1_data = result[result["artist"] == "Artist1"].iloc[0]
        assert abs(artist1_data["positive_pct"] - 66.67) < 0.1

    def test_sentiment_model_validation(self):
        """Test validation of custom sentiment model performance"""
        from youtubeviz.sentiment import validate_sentiment_model_performance

        # Mock sentiment predictions vs actual
        predictions = [0.8, -0.3, 0.6, -0.1, 0.9]
        actual = ["positive", "negative", "positive", "negative", "positive"]

        result = validate_sentiment_model_performance(predictions, actual)

        # Should return performance metrics
        assert isinstance(result, dict)
        assert "accuracy" in result
        assert "precision" in result
        assert "recall" in result
        assert "explanation" in result  # Educational explanation

    def test_slang_detection_in_comments(self):
        """Test detection and handling of music industry slang in comments"""
        from youtubeviz.sentiment import detect_music_slang

        comments = ["This track is fire!", "Absolute banger", "Mid tbh", "No cap this slaps"]

        result = detect_music_slang(comments)

        # Should identify slang terms
        assert isinstance(result, dict)
        assert "slang_terms_found" in result
        assert "slang_sentiment_impact" in result

        # Should find music slang like "fire", "banger", "slaps"
        slang_found = result["slang_terms_found"]
        assert len(slang_found) > 0


class TestSentimentVisualization:
    """Test suite for sentiment visualization functions"""

    def test_sentiment_wordcloud_generation(self):
        """Test generation of sentiment-based word clouds"""
        from youtubeviz.charts import create_sentiment_wordcloud

        comments = ["Amazing track love it", "Great vocals incredible", "Not good disappointing"]

        result = create_sentiment_wordcloud(comments=comments, sentiment_type="positive")

        # Should return wordcloud data or image
        assert result is not None

    def test_sentiment_timeline_chart(self):
        """Test creation of sentiment timeline showing changes over time"""
        from youtubeviz.charts import create_sentiment_timeline

        # Sample time-series sentiment data
        timeline_data = pd.DataFrame(
            [
                {"date": "2024-01-01", "artist": "Artist1", "avg_sentiment": 0.6},
                {"date": "2024-01-02", "artist": "Artist1", "avg_sentiment": 0.7},
                {"date": "2024-01-01", "artist": "Artist2", "avg_sentiment": 0.4},
            ]
        )

        chart = create_sentiment_timeline(
            df=timeline_data, date_col="date", sentiment_col="avg_sentiment", artist_col="artist"
        )

        assert chart is not None
        assert hasattr(chart, "data")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
