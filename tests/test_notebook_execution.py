#!/usr/bin/env python3
"""
🧪 Comprehensive Notebook Execution Tests for CI/CD
==================================================

Tests that all notebooks execute successfully and contain the expected data.
This is the definitive test that ensures all notebooks work in CI/CD.
"""

import re
import subprocess
import sys
from pathlib import Path

import pytest


class TestNotebookExecution:
    """Test suite for notebook execution validation."""

    EXPECTED_ARTISTS = {"BiC Fizzle", "COBRAH", "Corook", "Flyana Boss", "Raiche", "re6ce"}
    EXPECTED_ARTIST_COUNT = 6

    def test_music_analytics_execution(self):
        """Test that music analytics notebook executes and shows all artists."""

        result = subprocess.run(
            [sys.executable, "execute_music_analytics.py"], capture_output=True, text=True, timeout=120
        )

        assert result.returncode == 0, f"Music analytics failed: {result.stderr}"

        output = result.stdout

        # Check that all artists are mentioned
        for artist in self.EXPECTED_ARTISTS:
            assert artist in output, f"Artist '{artist}' not found in music analytics output"

        # Check artist count
        artist_count_matches = re.findall(r"Artists?:\s*(\d+)", output)
        assert artist_count_matches, "No artist count found in output"

        found_count = int(artist_count_matches[0])
        assert (
            found_count == self.EXPECTED_ARTIST_COUNT
        ), f"Expected {self.EXPECTED_ARTIST_COUNT} artists, found {found_count}"

        # Check for key sections
        assert "MUSIC INDUSTRY PERFORMANCE DASHBOARD" in output
        assert "Market Share Analysis" in output
        assert "Revenue Analysis" in output
        assert "INVESTMENT RECOMMENDATIONS" in output

        # Check portfolio value is present
        assert "$674,457" in output or "674457" in output or "674,457" in output

        print("✅ Music analytics test passed")

    def test_data_quality_execution(self):
        """Test that data quality notebook executes and shows all artists."""

        result = subprocess.run(
            [sys.executable, "execute_data_quality.py"], capture_output=True, text=True, timeout=120
        )

        assert result.returncode == 0, f"Data quality failed: {result.stderr}"

        output = result.stdout

        # Check that all artists are mentioned
        for artist in self.EXPECTED_ARTISTS:
            assert artist in output, f"Artist '{artist}' not found in data quality output"

        # Check artist count
        artist_count_matches = re.findall(r"Artists?:\s*(\d+)", output)
        assert artist_count_matches, "No artist count found in output"

        found_count = int(artist_count_matches[0])
        assert (
            found_count == self.EXPECTED_ARTIST_COUNT
        ), f"Expected {self.EXPECTED_ARTIST_COUNT} artists, found {found_count}"

        # Check for key sections
        assert "DATA QUALITY ASSESSMENT RESULTS" in output
        assert "OVERALL DATA QUALITY SCORE" in output

        # Check data quality score is good
        quality_matches = re.findall(r"OVERALL DATA QUALITY SCORE:\s*([\d.]+)%", output)
        if quality_matches:
            quality_score = float(quality_matches[0])
            assert quality_score >= 95.0, f"Data quality score too low: {quality_score}%"

        print("✅ Data quality test passed")

    def test_artist_comparison_execution(self):
        """Test that artist comparison notebook executes and shows all artists."""

        result = subprocess.run(
            [sys.executable, "execute_artist_comparison.py"], capture_output=True, text=True, timeout=120
        )

        assert result.returncode == 0, f"Artist comparison failed: {result.stderr}"

        output = result.stdout

        # Check that all artists are mentioned
        for artist in self.EXPECTED_ARTISTS:
            assert artist in output, f"Artist '{artist}' not found in artist comparison output"

        # Check artist count
        artist_count_matches = re.findall(r"Artists?:\s*(\d+)", output)
        assert artist_count_matches, "No artist count found in output"

        found_count = int(artist_count_matches[0])
        assert (
            found_count == self.EXPECTED_ARTIST_COUNT
        ), f"Expected {self.EXPECTED_ARTIST_COUNT} artists, found {found_count}"

        # Check for key sections
        assert "Artist Comparison Metrics" in output
        assert "ARTIST RANKING SUMMARY" in output
        assert "Top Performing Videos by Artist" in output

        # Check that each artist has top videos listed
        for artist in self.EXPECTED_ARTISTS:
            assert f"🎤 {artist}:" in output, f"No top videos section found for {artist}"

        print("✅ Artist comparison test passed")

    def test_all_notebooks_artist_consistency(self):
        """Test that all notebooks show consistent artist data."""

        notebooks = [
            ("execute_music_analytics.py", "Music Analytics"),
            ("execute_data_quality.py", "Data Quality"),
            ("execute_artist_comparison.py", "Artist Comparison"),
        ]

        artist_counts = {}

        for script, name in notebooks:
            result = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=120)

            assert result.returncode == 0, f"{name} failed: {result.stderr}"

            # Extract artist count
            artist_count_matches = re.findall(r"Artists?:\s*(\d+)", result.stdout)
            assert artist_count_matches, f"No artist count found in {name}"

            artist_counts[name] = int(artist_count_matches[0])

        # All notebooks should show the same artist count
        unique_counts = set(artist_counts.values())
        assert len(unique_counts) == 1, f"Inconsistent artist counts: {artist_counts}"

        # Should be the expected count
        consistent_count = list(unique_counts)[0]
        assert (
            consistent_count == self.EXPECTED_ARTIST_COUNT
        ), f"All notebooks show {consistent_count} artists, expected {self.EXPECTED_ARTIST_COUNT}"

        print(f"✅ All notebooks consistently show {consistent_count} artists")

    def test_notebook_performance(self):
        """Test that notebooks execute within reasonable time limits."""

        notebooks = [
            ("execute_music_analytics.py", 60),  # 60 second limit
            ("execute_data_quality.py", 60),  # 60 second limit
            ("execute_artist_comparison.py", 60),  # 60 second limit
        ]

        for script, timeout in notebooks:
            try:
                result = subprocess.run([sys.executable, script], capture_output=True, text=True, timeout=timeout)

                assert result.returncode == 0, f"{script} failed: {result.stderr}"
                print(f"✅ {script} completed within {timeout}s")

            except subprocess.TimeoutExpired:
                pytest.fail(f"{script} exceeded {timeout}s timeout")

    def test_notebook_output_quality(self):
        """Test that notebook outputs contain expected quality indicators."""

        # Test music analytics output quality
        result = subprocess.run(
            [sys.executable, "execute_music_analytics.py"], capture_output=True, text=True, timeout=120
        )

        assert result.returncode == 0
        output = result.stdout

        # Should contain revenue information
        assert any(
            char in output for char in ["$", "USD", "revenue"]
        ), "No revenue information found in music analytics"

        # Should contain view counts
        assert re.search(r"\d{1,3}(,\d{3})*\s+views", output), "No properly formatted view counts found"

        # Should contain percentages
        assert re.search(r"\d+\.\d+%", output), "No percentage values found"

        # Test data quality output
        result = subprocess.run(
            [sys.executable, "execute_data_quality.py"], capture_output=True, text=True, timeout=120
        )

        assert result.returncode == 0
        output = result.stdout

        # Should contain quality score
        assert "OVERALL DATA QUALITY SCORE" in output, "No overall quality score found"

        # Should contain specific metrics
        quality_indicators = [
            "Missing ISRC codes",
            "Orphaned metrics",
            "Videos without metrics",
            "Statistical outliers",
        ]

        for indicator in quality_indicators:
            assert indicator in output, f"Quality indicator '{indicator}' not found"

        print("✅ Notebook output quality test passed")

    def test_comprehensive_artist_validation_integration(self):
        """Test that comprehensive artist validation passes."""

        result = subprocess.run(
            [sys.executable, "scripts/comprehensive_artist_validation.py"], capture_output=True, text=True, timeout=180
        )

        assert result.returncode == 0, f"Comprehensive validation failed: {result.stderr}"

        output = result.stdout

        # Should pass all validations
        assert "ALL VALIDATIONS PASSED" in output, "Comprehensive artist validation did not pass"

        # Should validate all expected components
        validation_components = ["Database Tables", "CSV Analysis Tables", "Notebook Outputs", "Chart Data"]

        for component in validation_components:
            assert f"✅ PASSED: {component}" in output, f"Component '{component}' did not pass validation"

        print("✅ Comprehensive artist validation integration test passed")


class TestNotebookFiles:
    """Test notebook file integrity and structure."""

    def test_notebook_files_exist(self):
        """Test that all expected notebook files exist."""

        expected_notebooks = [
            "notebooks/editable/02_artist_comparison.ipynb",
            "notebooks/editable/03_appendix_data_quality_clean.ipynb",
            "notebooks/editable/04_sentiment_deep_dive_fun.ipynb",
            "notebooks/analysis/01_music_focused_analytics.ipynb",
            "notebooks/analysis/02_artist_comparison.ipynb",
        ]

        for notebook in expected_notebooks:
            assert Path(notebook).exists(), f"Notebook file missing: {notebook}"

        print("✅ All expected notebook files exist")

    def test_execute_scripts_exist(self):
        """Test that all execute scripts exist and are executable."""

        execute_scripts = ["execute_music_analytics.py", "execute_data_quality.py", "execute_artist_comparison.py"]

        for script in execute_scripts:
            script_path = Path(script)
            assert script_path.exists(), f"Execute script missing: {script}"

            # Test that it's valid Python
            result = subprocess.run([sys.executable, "-m", "py_compile", script], capture_output=True)

            assert result.returncode == 0, f"Execute script has syntax errors: {script}"

        print("✅ All execute scripts exist and have valid syntax")


if __name__ == "__main__":
    # Run tests directly
    test_suite = TestNotebookExecution()

    print("🧪 RUNNING COMPREHENSIVE NOTEBOOK TESTS")
    print("=" * 50)

    try:
        test_suite.test_music_analytics_execution()
        test_suite.test_data_quality_execution()
        test_suite.test_artist_comparison_execution()
        test_suite.test_all_notebooks_artist_consistency()
        test_suite.test_notebook_performance()
        test_suite.test_notebook_output_quality()
        test_suite.test_comprehensive_artist_validation_integration()

        file_tests = TestNotebookFiles()
        file_tests.test_notebook_files_exist()
        file_tests.test_execute_scripts_exist()

        print("\n🎉 ALL NOTEBOOK TESTS PASSED!")
        print("✅ All notebooks execute successfully")
        print("✅ All artists appear in all outputs")
        print("✅ Performance within acceptable limits")
        print("✅ Output quality meets standards")

    except Exception as e:
        print(f"\n❌ NOTEBOOK TEST FAILED: {e}")
        sys.exit(1)
