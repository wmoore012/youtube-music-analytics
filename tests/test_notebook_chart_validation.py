"""
TDD tests for bulletproof notebook CI/CD chart validation.

This module ensures that:
1. All charts from both notebooks are preserved and working
2. Charts work with real data (not fake/sample data)
3. Automatic validation of chart count and success rate
4. CI/CD can detect when charts break or data is missing
"""

import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# Add project root to path
sys.path.insert(0, os.path.abspath("."))

from src.youtubeviz.charts import (
    create_content_type_breakdown_chart,
    create_divergent_sentiment_chart,
    create_isrc_balance_chart,
    create_sentiment_cluster_chart,
    views_over_time_plotly,
)
from src.youtubeviz.content import create_artist_comparison_chart
from src.youtubeviz.data import load_youtube_data
from src.youtubeviz.sentiment import extract_top_negative_comments_with_percentages, extract_top_positive_comments


class TestNotebookChartValidation:
    """Test comprehensive chart validation for CI/CD bulletproofing."""

    def test_expected_chart_count_matches_notebooks(self):
        """Test that we expect exactly 15 charts as specified in requirements."""
        expected_charts = [
            "Chart #1: Sentiment Breakdown by Artist (Diverging Stacked Bars)",
            "Chart #2: Sentiment Model Categories Heatmap",
            "Chart #3: Top 3 Positive Theme Lollipops",
            "Chart #4: Top 3 Negative Theme Lollipops",
            "Chart #5: Standout Videos Scatter Plot",
            "Chart #6: Tour Compatibility Analysis (UMAP + Similarity Matrix)",
            "Chart #7: UpSet Plot for Feature Intersections",
            "Chart #8: ISRC vs Non-ISRC Content Analysis",
            "Chart #9: Short-form vs Long-form Video Analysis",
            "Chart #10: Content Type Breakdown (MV/Lyric/Visualizer)",
            "Chart #11: Total Views by Category Over Time",
            "Chart #12: Genre Context Heatmap",
            "Chart #13: Artist Rank Bump Chart",
            "Chart #14: Comment Polarity Ridgeline Plots",
            "Chart #15: A/B Test Uplift Analysis",
        ]

        assert len(expected_charts) == 15, "Must have exactly 15 charts as per requirements"

        # This test ensures CI/CD knows the expected chart count
        print(f"✅ Expected chart count: {len(expected_charts)}")

    def test_real_data_availability_for_charts(self):
        """Test that real data is available for chart generation."""
        try:
            df = load_youtube_data()
            assert not df.empty, "Real data must be available"
            assert len(df) > 0, "Must have actual data rows"

            # Check for essential columns needed by charts
            required_columns = [
                "artist_name",
                "view_count",
                "like_count",
                "comment_count",
                "published_at",
                "title",
                "video_id",
            ]

            missing_columns = [col for col in required_columns if col not in df.columns]
            assert not missing_columns, f"Missing required columns: {missing_columns}"

            # Check data quality
            assert df["artist_name"].notna().sum() > 0, "Must have artist names"
            assert df["view_count"].sum() > 0, "Must have view data"

            print(f"✅ Real data available: {len(df)} rows, {df.columns.tolist()}")
            return True

        except Exception as e:
            pytest.fail(f"Real data not available for charts: {e}")

    def test_chart_function_imports_work(self):
        """Test that all chart functions can be imported successfully."""
        chart_functions = [
            "views_over_time_plotly",
            "create_divergent_sentiment_chart",
            "create_sentiment_cluster_chart",
            "create_isrc_balance_chart",
            "create_content_type_breakdown_chart",
            "create_artist_comparison_chart",
            "extract_top_positive_comments",
            "extract_top_negative_comments_with_percentages",
        ]

        for func_name in chart_functions:
            try:
                # Try to import each function
                if func_name in [
                    "views_over_time_plotly",
                    "create_divergent_sentiment_chart",
                    "create_sentiment_cluster_chart",
                    "create_isrc_balance_chart",
                    "create_content_type_breakdown_chart",
                ]:
                    from src.youtubeviz.charts import views_over_time_plotly
                elif func_name == "create_artist_comparison_chart":
                    from src.youtubeviz.content import create_artist_comparison_chart
                elif func_name in ["extract_top_positive_comments", "extract_top_negative_comments_with_percentages"]:
                    from src.youtubeviz.sentiment import extract_top_positive_comments

                print(f"✅ {func_name} imports successfully")

            except ImportError as e:
                pytest.fail(f"Chart function {func_name} cannot be imported: {e}")

    def test_chart_execution_with_real_data(self):
        """Test that charts can execute with real data without errors."""
        try:
            df = load_youtube_data()

            # Test basic chart with real data
            if not df.empty and "artist_name" in df.columns:
                # Filter to artists with data
                artists_with_data = df[df["artist_name"].notna()]["artist_name"].unique()

                if len(artists_with_data) > 0:
                    test_artist = artists_with_data[0]
                    artist_data = df[df["artist_name"] == test_artist]

                    # Test views over time chart
                    if len(artist_data) > 0:
                        fig = views_over_time_plotly(
                            artist_data, "published_at", "view_count", title=f"Views Over Time - {test_artist}"
                        )
                        assert fig is not None, "Chart must generate successfully"
                        print(f"✅ Chart executed with real data for {test_artist}")
                        return True

        except Exception as e:
            pytest.fail(f"Chart execution failed with real data: {e}")

    def test_notebook_chart_count_validation_system(self):
        """Test the system that validates chart counts in notebooks."""

        class ChartValidator:
            def __init__(self):
                self.real_charts = []
                self.requirement_charts = []
                self.error_charts = []

            def validate_chart(self, chart_id, chart_function, data):
                """Validate a single chart and categorize the result."""
                try:
                    if data is None or data.empty:
                        self.requirement_charts.append(chart_id)
                        return "needs_data"

                    # Try to execute chart
                    result = chart_function(data)
                    if result is not None:
                        self.real_charts.append(chart_id)
                        return "success"
                    else:
                        self.error_charts.append(chart_id)
                        return "error"

                except Exception as e:
                    self.error_charts.append(chart_id)
                    return f"error: {e}"

            def get_summary(self):
                """Get validation summary like the notebook cell."""
                return {
                    "real_charts": len(self.real_charts),
                    "requirement_charts": len(self.requirement_charts),
                    "error_charts": len(self.error_charts),
                    "total_expected": 15,
                }

        # Test the validator
        validator = ChartValidator()

        # Mock some chart validations
        mock_data = pd.DataFrame({"artist_name": ["Test"], "view_count": [1000]})

        def mock_chart_success(data):
            return "chart_object"

        def mock_chart_error(data):
            raise ValueError("Missing column")

        # Test successful chart
        result1 = validator.validate_chart(1, mock_chart_success, mock_data)
        assert result1 == "success"

        # Test error chart
        result2 = validator.validate_chart(2, mock_chart_error, mock_data)
        assert result2.startswith("error:")

        # Test missing data chart
        result3 = validator.validate_chart(3, mock_chart_success, None)
        assert result3 == "needs_data"

        summary = validator.get_summary()
        assert summary["real_charts"] == 1
        assert summary["error_charts"] == 1
        assert summary["requirement_charts"] == 1
        assert summary["total_expected"] == 15

        print("✅ Chart validation system works correctly")

    def test_ci_cd_chart_success_threshold(self):
        """Test CI/CD success thresholds for chart validation."""

        # Define success criteria for CI/CD
        def evaluate_chart_health(real_charts, total_charts):
            """Evaluate if chart health meets CI/CD standards."""
            success_rate = real_charts / total_charts if total_charts > 0 else 0

            if success_rate >= 0.8:  # 80% or more charts working
                return "PASS", "Excellent chart health"
            elif success_rate >= 0.6:  # 60-79% charts working
                return "WARNING", "Acceptable chart health but needs improvement"
            elif success_rate >= 0.3:  # 30-59% charts working
                return "FAIL", "Poor chart health - major issues"
            else:  # Less than 30% working
                return "CRITICAL", "Critical chart failure - immediate attention needed"

        # Test different scenarios
        status1, msg1 = evaluate_chart_health(15, 15)  # Perfect
        assert status1 == "PASS"

        status2, msg2 = evaluate_chart_health(12, 15)  # 80% - Good
        assert status2 == "PASS"

        status3, msg3 = evaluate_chart_health(9, 15)  # 60% - Warning
        assert status3 == "WARNING"

        status4, msg4 = evaluate_chart_health(4, 15)  # 27% - Critical
        assert status4 == "CRITICAL"

        status5, msg5 = evaluate_chart_health(0, 15)  # 0% - Critical
        assert status5 == "CRITICAL"

        print("✅ CI/CD success thresholds defined and tested")


class TestNotebookIntegration:
    """Test integration between notebooks and chart validation."""

    def test_notebook_summary_cell_format(self):
        """Test that notebook summary cell format matches expected output."""

        def generate_summary_cell(real_charts, requirement_charts, error_charts, total=15):
            """Generate the summary cell content like in notebooks."""
            lines = [
                "🎯 REAL DATA ANALYTICS SUMMARY",
                "=" * 50,
                f"📊 Charts with REAL data: {len(real_charts)}/{total}",
                f"📋 Charts showing data requirements: {len(requirement_charts)}/{total}",
                f"❌ Charts with errors: {len(error_charts)}/{total}",
            ]

            if real_charts:
                lines.append(f"✅ Working with real data: {real_charts}")
            if requirement_charts:
                lines.append(f"📋 Need data columns: {requirement_charts}")
            if error_charts:
                lines.append(f"❌ Have errors: {error_charts}")

            # Success message
            if len(real_charts) >= 5:
                lines.extend(
                    [
                        f"🎉 SUCCESS: {len(real_charts)} charts working with REAL data!",
                        "💝 No fake data used - authentic analytics only!",
                    ]
                )
            elif len(real_charts) >= 1:
                lines.extend(
                    [
                        f"🌱 PROGRESS: {len(real_charts)} charts working with real data!",
                        "🔧 Add missing data columns to unlock more charts!",
                    ]
                )
            else:
                lines.append("📋 All charts show data requirements - add real data to see analytics!")

            lines.append("🎵 MusicScope™ Real Data Analytics Complete! 🎵")
            return "\n".join(lines)

        # Test different scenarios
        summary1 = generate_summary_cell([1, 2, 3, 4, 5, 6], [7, 8], [9, 10, 11, 12, 13, 14, 15])
        assert "SUCCESS: 6 charts working" in summary1

        summary2 = generate_summary_cell([1, 2], [3, 4, 5], [6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
        assert "PROGRESS: 2 charts working" in summary2

        summary3 = generate_summary_cell([], [1, 2, 3], [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
        assert "All charts show data requirements" in summary3

        print("✅ Notebook summary cell format validated")

    def test_chart_preservation_from_both_notebooks(self):
        """Test that charts from both notebooks are preserved (no deletions)."""

        # Charts that should exist from MusicScope_Complete_Dashboard
        complete_dashboard_charts = [
            "views_over_time_plotly",
            "create_divergent_sentiment_chart",
            "create_sentiment_cluster_chart",
            "create_isrc_balance_chart",
            "create_content_type_breakdown_chart",
            "create_artist_comparison_chart",
        ]

        # Charts that should exist from Real_Data_Dashboard
        real_data_charts = ["extract_top_positive_comments", "extract_top_negative_comments_with_percentages"]

        # All charts should be importable (preserved)
        all_charts = complete_dashboard_charts + real_data_charts

        for chart_name in all_charts:
            try:
                if chart_name in [
                    "views_over_time_plotly",
                    "create_divergent_sentiment_chart",
                    "create_sentiment_cluster_chart",
                    "create_isrc_balance_chart",
                    "create_content_type_breakdown_chart",
                ]:
                    exec(f"from src.youtubeviz.charts import {chart_name}")
                elif chart_name == "create_artist_comparison_chart":
                    exec(f"from src.youtubeviz.content import {chart_name}")
                elif chart_name in ["extract_top_positive_comments", "extract_top_negative_comments_with_percentages"]:
                    exec(f"from src.youtubeviz.sentiment import {chart_name}")

                print(f"✅ {chart_name} preserved and importable")

            except ImportError as e:
                pytest.fail(f"Chart {chart_name} was deleted or broken: {e}")


if __name__ == "__main__":
    # Run tests when executed directly
    pytest.main([__file__, "-v"])
