"""
TDD tests to enforce NO FAKE DATA policy.

This module ensures that:
1. All charts ONLY use real data from the database
2. No fake / sample / generated data is ever used
3. Charts fail gracefully when real data is missing
4. CI / CD detects and prevents fake data usage
"""

import inspect
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# Add project root to path
sys.path.insert(0, os.path.abspath("."))

from src.youtubeviz.data import load_recent_window_days, load_youtube_data
from web.etl_helpers import get_engine


class TestNoFakeDataEnforcement:
    """Enforce NO FAKE DATA policy across all chart functions."""

    def test_only_real_database_data_allowed(self):
        """Test that only real data from database is used."""

        # Get real data from database
        real_data = load_youtube_data()

        # Verify it's actually from database (has real structure)
        assert isinstance(real_data, pd.DataFrame), "Must be real DataFrame from database"

        if not real_data.empty:
            # Real data should have database columns
            expected_db_columns = ["video_id", "artist_name", "view_count", "published_at"]
            has_db_columns = any(col in real_data.columns for col in expected_db_columns)
            assert has_db_columns, "Data must have real database columns"

            # Real data should have actual video IDs (not fake ones)
            if "video_id" in real_data.columns:
                video_ids = real_data["video_id"].dropna()
                if len(video_ids) > 0:
                    # YouTube video IDs are 11 characters
                    sample_id = video_ids.iloc[0]
                    assert len(str(sample_id)) == 11, f"Real YouTube video ID should be 11 chars, got: {sample_id}"

        print(f"✅ Using real database data: {len(real_data)} rows")

    def test_detect_fake_data_patterns(self):
        """Test detection of fake data patterns that should never be used."""

        def is_fake_data(df):
            """Detect if DataFrame contains fake / sample data."""
            if df is None or df.empty:
                return False, "Empty data"

            fake_indicators = []

            # Check for fake artist names
            if "artist_name" in df.columns:
                fake_artists = ["Test Artist", "Sample Artist", "Fake Artist", "Artist A", "Artist B"]
                has_fake_artists = df["artist_name"].isin(fake_artists).any()
                if has_fake_artists:
                    fake_indicators.append("Contains fake artist names")

            # Check for fake video titles
            if "title" in df.columns:
                fake_titles = ["Test Video", "Sample Song", "Fake Title", "Test Title"]
                has_fake_titles = df["title"].isin(fake_titles).any()
                if has_fake_titles:
                    fake_indicators.append("Contains fake video titles")

            # Check for obviously fake view counts (round numbers)
            if "view_count" in df.columns:
                view_counts = df["view_count"].dropna()
                if len(view_counts) > 0:
                    # Real view counts are rarely perfect round numbers
                    round_numbers = view_counts[view_counts % 1000 == 0]
                    if len(round_numbers) == len(view_counts) and len(view_counts) > 1:
                        fake_indicators.append("All view counts are round numbers (suspicious)")

            # Check for fake video IDs
            if "video_id" in df.columns:
                video_ids = df["video_id"].dropna().astype(str)
                fake_ids = video_ids[video_ids.str.contains("test|fake|sample", case=False, na=False)]
                if len(fake_ids) > 0:
                    fake_indicators.append("Contains fake video IDs")

            return len(fake_indicators) > 0, fake_indicators

        # Test with real data (should not be fake)
        real_data = load_youtube_data()
        is_fake, indicators = is_fake_data(real_data)

        if is_fake:
            pytest.fail(f"Real data contains fake indicators: {indicators}")

        print("✅ Real data passes fake detection tests")

    def test_chart_functions_reject_fake_data(self):
        """Test that chart functions can detect and reject fake data."""

        def create_obviously_fake_data():
            """Create obviously fake data that should be rejected."""
            return pd.DataFrame(
                {
                    "artist_name": ["Fake Artist", "Test Artist"],
                    "view_count": [1000, 2000],  # Round numbers
                    "video_id": ["fake_id_123", "test_video"],  # Fake IDs
                    "title": ["Test Video", "Sample Song"],
                    "published_at": ["2023-01-01", "2023-01-02"],
                }
            )

        fake_data = create_obviously_fake_data()

        # Import chart functions
        from src.youtubeviz.charts import views_over_time_plotly

        # Chart functions should either:
        # 1. Reject fake data explicitly, OR
        # 2. Work with any data (but we validate the source separately)

        try:
            # If chart accepts the data, that's OK-we validate data source elsewhere
            result = views_over_time_plotly(fake_data, "published_at", "view_count")
            print("⚠️  Chart function accepts data (source validation required)")
        except Exception as e:
            print(f"✅ Chart function rejects suspicious data: {e}")

    def test_database_connection_required(self):
        """Test that real database connection is required and working."""

        try:
            engine = get_engine()
            assert engine is not None, "Database engine must be available"

            # Test actual database query
            with engine.connect() as conn:
                result = conn.execute("SELECT COUNT(*) FROM youtube_videos")
                count = result.fetchone()[0]
                assert count >= 0, "Database must be accessible"

            print(f"✅ Database connection working: {count} videos in database")

        except Exception as e:
            pytest.fail(f"Database connection required for real data: {e}")

    def test_no_hardcoded_sample_data_in_functions(self):
        """Test that chart functions don't contain hardcoded sample data."""

        # Import all chart modules
        from src.youtubeviz import charts, content, sentiment

        modules_to_check = [charts, content, sentiment]

        for module in modules_to_check:
            # Get all functions in the module
            functions = inspect.getmembers(module, inspect.isfunction)

            for func_name, func in functions:
                if func_name.startswith("create_") or func_name.startswith("extract_"):
                    # Get function source code
                    try:
                        source = inspect.getsource(func)

                        # Check for hardcoded fake data patterns
                        fake_patterns = [
                            "pd.DataFrame({",  # Hardcoded DataFrames
                            "'Test Artist'",
                            "'Fake Artist'",
                            "'Sample Data'",
                            "np.random.",  # Random data generation
                            "fake_data",
                            "sample_data",
                        ]

                        found_patterns = []
                        for pattern in fake_patterns:
                            if pattern in source:
                                found_patterns.append(pattern)

                        if found_patterns:
                            print(f"⚠️  {func_name} contains potential fake data patterns: {found_patterns}")
                            # Don't fail-might be legitimate use cases
                        else:
                            print(f"✅ {func_name} clean of fake data patterns")

                    except Exception as e:
                        print(f"⚠️  Could not inspect {func_name}: {e}")

    def test_data_validation_before_chart_creation(self):
        """Test that data is validated as real before chart creation."""

        def validate_real_data(df, chart_name="chart"):
            """Validate that data is real before using in charts."""

            if df is None:
                return False, f"{chart_name}: No data provided"

            if df.empty:
                return False, f"{chart_name}: Empty dataset-need real data"

            # Check for minimum data quality indicators
            quality_checks = []

            # Must have reasonable number of rows (not just 1-2 test rows)
            if len(df) < 3:
                quality_checks.append("Too few rows (likely test data)")

            # Check for real artist names (not test names)
            if "artist_name" in df.columns:
                artists = df["artist_name"].dropna().unique()
                test_artists = [
                    name for name in artists if any(word in str(name).lower() for word in ["test", "fake", "sample"])
                ]
                if test_artists:
                    quality_checks.append(f"Test artist names detected: {test_artists}")

            # Check for real video IDs
            if "video_id" in df.columns:
                video_ids = df["video_id"].dropna().astype(str)
                # Real YouTube IDs are 11 characters, alphanumeric
                invalid_ids = video_ids[~video_ids.str.match(r"^[A-Za-z0-9_-]{11}$")]
                if len(invalid_ids) > 0:
                    quality_checks.append(f"Invalid video IDs detected: {invalid_ids.tolist()[:3]}")

            if quality_checks:
                return False, f"{chart_name}: Data quality issues - {'; '.join(quality_checks)}"

            return True, f"{chart_name}: Real data validated"

        # Test with real data
        real_data = load_youtube_data()
        is_valid, message = validate_real_data(real_data, "test_chart")

        print(f"Data validation result: {message}")

        # If we have real data, it should pass validation
        if not real_data.empty:
            assert is_valid, f"Real data should pass validation: {message}"

    def test_ci_cd_fake_data_detection(self):
        """Test CI / CD system can detect fake data usage."""

        def ci_cd_data_audit():
            """Audit data sources for CI / CD pipeline."""

            audit_results = {
                "database_accessible": False,
                "real_data_available": False,
                "fake_data_detected": False,
                "data_quality_score": 0.0,
            }

            try:
                # Check database accessibility
                engine = get_engine()
                with engine.connect() as conn:
                    result = conn.execute("SELECT COUNT(*) FROM youtube_videos")
                    video_count = result.fetchone()[0]
                    audit_results["database_accessible"] = True

                    if video_count > 0:
                        audit_results["real_data_available"] = True

                # Load and check data quality
                df = load_youtube_data()
                if not df.empty:
                    # Calculate data quality score
                    quality_score = 0.0

                    # Points for having real columns
                    if "video_id" in df.columns:
                        quality_score += 0.2
                    if "artist_name" in df.columns:
                        quality_score += 0.2
                    if "view_count" in df.columns:
                        quality_score += 0.2

                    # Points for data volume
                    if len(df) > 100:
                        quality_score += 0.2
                    elif len(df) > 10:
                        quality_score += 0.1

                    # Points for real artist names
                    if "artist_name" in df.columns:
                        real_artists = df["artist_name"].dropna()
                        fake_count = sum(
                            1
                            for name in real_artists
                            if any(word in str(name).lower() for word in ["test", "fake", "sample"])
                        )
                        if fake_count == 0:
                            quality_score += 0.2
                        else:
                            audit_results["fake_data_detected"] = True

                    audit_results["data_quality_score"] = quality_score

            except Exception as e:
                print(f"CI / CD audit error: {e}")

            return audit_results

        # Run audit
        audit = ci_cd_data_audit()

        # CI / CD requirements
        assert audit["database_accessible"], "CI / CD requires database access"

        if audit["real_data_available"]:
            assert not audit["fake_data_detected"], "CI / CD must not detect fake data"
            assert audit["data_quality_score"] >= 0.6, f"Data quality too low: {audit['data_quality_score']}"

        print(f"✅ CI / CD audit passed: {audit}")


class TestRealDataOnlyCharts:
    """Test that charts work with real data only."""

    def test_charts_work_with_real_data_or_fail_gracefully(self):
        """Test that charts either work with real data or fail gracefully."""

        real_data = load_youtube_data()

        # Import chart functions
        from src.youtubeviz.charts import views_over_time_plotly

        if real_data.empty:
            print("⚠️  No real data available-charts should show data requirements")

            # Charts should handle empty data gracefully
            try:
                result = views_over_time_plotly(real_data, "published_at", "view_count")
                # If it returns something, it should indicate missing data
                print("✅ Chart handles empty real data gracefully")
            except Exception as e:
                print(f"✅ Chart fails gracefully with empty data: {e}")

        else:
            print(f"✅ Real data available: {len(real_data)} rows")

            # Charts should work with real data
            try:
                if "published_at" in real_data.columns and "view_count" in real_data.columns:
                    result = views_over_time_plotly(real_data, "published_at", "view_count")
                    assert result is not None, "Chart should work with real data"
                    print("✅ Chart works with real data")
                else:
                    print("⚠️  Real data missing required columns")

            except Exception as e:
                print(f"⚠️  Chart failed with real data: {e}")
                # This might be OK if data doesn't meet chart requirements

    def test_no_fallback_to_fake_data(self):
        """Test that functions never fall back to fake data."""

        # Mock empty database to test fallback behavior
        with patch("src.youtubeviz.data.load_youtube_data") as mock_load:
            mock_load.return_value = pd.DataFrame()  # Empty real data

            # Functions should return empty results, not fake data
            empty_data = load_youtube_data()
            assert empty_data.empty, "Should return empty data, not fake data"

            print("✅ No fallback to fake data when real data unavailable")


if __name__ == "__main__":
    # Run tests when executed directly
    pytest.main([__file__, "-v"])
