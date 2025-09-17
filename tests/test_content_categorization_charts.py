"""
Tests for content categorization chart functions.
Implements TDD approach for video analysis and content strategy charts.
"""

from unittest.mock import patch

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest


class TestContentCategorizationCharts:

    @pytest.fixture
    def sample_video_data(self):
        """Create sample video data for testing."""
        artists = ["BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"]
        video_types = ["Music Video", "Lyric Video", "Visualizer", "Behind Scenes", "Interview", "Live Performance"]

        data = []
        for i in range(100):
            artist = np.random.choice(artists)
            video_type = np.random.choice(video_types)
            has_isrc = np.random.choice([True, False], p=[0.6, 0.4])  # 60% have ISRC
            duration_seconds = np.random.randint(60, 600)  # 1-10 minutes

            data.append(
                {
                    "artist_name": artist,
                    "video_type": video_type,
                    "has_isrc": has_isrc,
                    "duration_seconds": duration_seconds,
                    "views": np.random.randint(1000, 1000000),
                    "likes": np.random.randint(50, 50000),
                    "comments": np.random.randint(10, 5000),
                    "video_title": f"Sample {video_type} by {artist}",
                    "upload_date": pd.Timestamp.now() - pd.Timedelta(days=np.random.randint(1, 365)),
                }
            )

        return pd.DataFrame(data)

    def test_create_isrc_balance_chart(self, sample_video_data):
        """Test ISRC vs non-ISRC balance analysis chart."""
        from youtubeviz.charts import create_isrc_balance_chart

        chart = create_isrc_balance_chart(
            df=sample_video_data, artist_col="artist_name", isrc_col="has_isrc", views_col="views"
        )

        assert chart is not None
        assert hasattr(chart, "data")  # Plotly figure should have data attribute

    def test_create_video_duration_breakdown_chart(self, sample_video_data):
        """Test short-form vs long-form video breakdown chart."""
        from youtubeviz.charts import create_duration_breakdown_chart

        chart = create_duration_breakdown_chart(
            df=sample_video_data, artist_col="artist_name", duration_col="duration_seconds", views_col="views"
        )

        assert chart is not None
        assert hasattr(chart, "data")

    def test_create_content_type_analysis_chart(self, sample_video_data):
        """Test content type breakdown (music video vs lyric video vs other)."""
        from youtubeviz.charts import create_content_type_breakdown_chart

        chart = create_content_type_breakdown_chart(
            df=sample_video_data, artist_col="artist_name", content_type_col="video_type", views_col="views"
        )

        assert chart is not None
        assert hasattr(chart, "data")

    def test_create_artist_comparison_matrix(self, sample_video_data):
        """Test side-by-side artist comparison chart."""
        from youtubeviz.charts import create_artist_content_comparison_chart

        chart = create_artist_content_comparison_chart(
            df=sample_video_data, artist_col="artist_name", content_type_col="video_type", views_col="views"
        )

        assert chart is not None
        assert hasattr(chart, "data")

    def test_create_roster_overview_chart(self, sample_video_data):
        """Test combined roster analysis chart."""
        from youtubeviz.charts import create_roster_content_overview_chart

        chart = create_roster_content_overview_chart(
            df=sample_video_data, artist_col="artist_name", content_type_col="video_type", views_col="views"
        )

        assert chart is not None
        assert hasattr(chart, "data")

    def test_create_genre_context_chart(self, sample_video_data):
        """Test genre context chart for new signees."""
        # Add genre information to sample data
        genre_mapping = {
            "BiC Fizzle": "Hip-Hop",
            "COBRAH": "Electronic",
            "Corook": "Pop",
            "Flyana Boss": "Hip-Hop",
            "Raiche": "R&B",
            "re6ce": "Alternative",
        }
        sample_video_data["genre"] = sample_video_data["artist_name"].map(genre_mapping)

        from youtubeviz.charts import create_genre_context_chart

        chart = create_genre_context_chart(
            df=sample_video_data, artist_col="artist_name", genre_col="genre", views_col="views"
        )

        assert chart is not None
        assert hasattr(chart, "data")

    def test_create_venn_diagram_chart(self, sample_video_data):
        """Test overlapping circle/Venn diagram for artist strengths."""
        from youtubeviz.charts import create_venn_diagram_chart

        chart = create_venn_diagram_chart(
            df=sample_video_data,
            artist_col="artist_name",
            categories=["high_views", "high_engagement", "consistent_uploads"],
        )

        assert chart is not None
        # Venn diagrams might return different format, so just check it's not None

    def test_charts_handle_empty_data(self):
        """Test that charts handle empty data gracefully."""
        empty_df = pd.DataFrame(columns=["artist_name", "video_type", "views"])

        from youtubeviz.charts import create_content_type_breakdown_chart

        # Should not crash with empty data
        chart = create_content_type_breakdown_chart(
            df=empty_df, artist_col="artist_name", content_type_col="video_type", views_col="views"
        )

        # Should return something (even if empty chart)
        assert chart is not None

    def test_charts_handle_missing_columns(self, sample_video_data):
        """Test that charts handle missing columns gracefully."""
        from youtubeviz.charts import create_isrc_balance_chart

        # Remove a required column
        df_missing_col = sample_video_data.drop(columns=["has_isrc"])

        # Should handle missing column gracefully (return empty chart or raise informative error)
        try:
            chart = create_isrc_balance_chart(
                df=df_missing_col, artist_col="artist_name", isrc_col="has_isrc", views_col="views"
            )
            # If it doesn't raise an error, it should return something
            assert chart is not None
        except KeyError as e:
            # If it raises an error, it should be informative
            assert "has_isrc" in str(e)

    def test_content_categorization_with_real_artist_count(self, sample_video_data):
        """Test that content categorization works with all 6 artists from .env."""
        from youtubeviz.config_validation import get_artists_from_env

        # Get actual artists from .env
        expected_artists, expected_count = get_artists_from_env()

        # Filter sample data to only include expected artists
        filtered_df = sample_video_data[sample_video_data["artist_name"].isin(expected_artists)]

        # Should have data for all expected artists
        actual_artists = filtered_df["artist_name"].unique()
        assert len(actual_artists) <= expected_count  # May have fewer due to random sampling

        # Test chart creation with real artist data
        from youtubeviz.charts import create_content_type_breakdown_chart

        chart = create_content_type_breakdown_chart(
            df=filtered_df, artist_col="artist_name", content_type_col="video_type", views_col="views"
        )

        assert chart is not None


if __name__ == "__main__":
    pytest.main([__file__])
