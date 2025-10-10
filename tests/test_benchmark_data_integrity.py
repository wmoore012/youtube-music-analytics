#!/usr / bin / env python3
"""
Test: Benchmark Data Integrity

CRITICAL TEST: Ensures benchmark system never uses fake data.
This test MUST pass before any benchmarking can occur.

Run in CI / CD to guarantee data integrity.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "src")

from youtubeviz.model_benchmark_system import BenchmarkConfig, ModelBenchmarkSystem


class TestBenchmarkDataIntegrity:
    """Critical tests for benchmark data integrity."""

    def test_no_synthetic_data_methods_exist(self):
        """Test that no synthetic data generation methods exist."""

        system = ModelBenchmarkSystem()

        # These methods should NOT exist (removed for data integrity)
        forbidden_methods = [
            "_create_synthetic_dataset",
            "create_fake_data",
            "generate_dummy_data",
            "make_synthetic_comments",
        ]

        for method_name in forbidden_methods:
            assert not hasattr(
                system, method_name
            ), f"FORBIDDEN: Method '{method_name}' found-no synthetic data allowed"

    def test_validation_method_exists(self):
        """Test that data validation method exists and is strict."""

        system = ModelBenchmarkSystem()

        # Must have validation method
        assert hasattr(system, "_validate_real_data_only"), "Missing required data validation method"
        assert hasattr(system, "_run_pre_benchmark_tests"), "Missing required pre-benchmark tests"

    def test_synthetic_data_detection(self):
        """Test that synthetic data patterns are properly detected and rejected."""

        import pandas as pd

        system = ModelBenchmarkSystem()

        # Test data with synthetic patterns (should be rejected)
        fake_data = pd.DataFrame(
            {
                "comment_id": ["pos_1", "neg_2", "neu_3"],
                "comment_text": ["fake positive", "fake negative", "fake neutral"],
                "like_count": [10, 2, 5],
                "ground_truth": ["positive", "negative", "neutral"],
            }
        )

        # Should raise ValueError for synthetic data
        with pytest.raises(ValueError, match="synthetic comment IDs"):
            system._validate_real_data_only(fake_data)

    def test_real_data_validation_passes(self):
        """Test that real-looking data passes validation."""

        import pandas as pd

        system = ModelBenchmarkSystem()

        # Test data that looks real (should pass)
        real_data = pd.DataFrame(
            {
                "comment_id": ["UgxKj8vN2K1_abc123", "UgwQ9mP4R7s_def456", "UgzL3nM8T2x_ghi789"],
                "comment_text": [
                    "this song is amazing, love the beat",
                    "not my favorite but decent production",
                    "okay track, nothing special though",
                ],
                "like_count": [25, 3, 8],
                "ground_truth": ["positive", "negative", "neutral"],
            }
        )

        # Should pass validation
        result = system._validate_real_data_only(real_data)
        assert result is True

    def test_benchmark_fails_without_database(self):
        """Test that benchmark fails gracefully without database connection."""

        system = ModelBenchmarkSystem()
        config = BenchmarkConfig(experiment_name="test_no_db")

        # Mock database failure
        with patch("youtubeviz.model_benchmark_system.get_engine") as mock_engine:
            mock_engine.side_effect = Exception("Database connection failed")

            # Should raise ValueError, not fall back to synthetic data
            with pytest.raises(ValueError, match="Database connection failed"):
                system.run_benchmark(config)

    def test_pre_benchmark_tests_are_mandatory(self):
        """Test that pre-benchmark tests always run and are mandatory."""

        system = ModelBenchmarkSystem()
        config = BenchmarkConfig(experiment_name="test_mandatory")

        # Mock the pre-benchmark tests to fail
        with patch.object(system, "_run_pre_benchmark_tests") as mock_tests:
            mock_tests.side_effect = ValueError("Pre-benchmark test failed")

            # Should fail immediately when pre-tests fail
            with pytest.raises(ValueError, match="Pre-benchmark test failed"):
                system.run_benchmark(config)

            # Verify pre-tests were called
            mock_tests.assert_called_once()

    def test_no_fallback_to_synthetic_data(self):
        """Test that there's no fallback to synthetic data anywhere in the code."""

        # Read the source code and check for forbidden patterns
        import inspect

        system = ModelBenchmarkSystem()
        source = inspect.getsource(system.fetch_benchmark_dataset)

        # These patterns should NOT exist in the code (except in comments about rejecting them)
        forbidden_patterns = ["create_fake", "generate_dummy", "return.*synthetic", "fallback.*synthetic"]

        for pattern in forbidden_patterns:
            assert pattern.lower() not in source.lower(), f"FORBIDDEN: Found '{pattern}' in benchmark code"

        # The word "synthetic" is OK if it's in validation / rejection context
        if "synthetic" in source.lower():
            # Should only appear in validation context
            assert (
                "not synthetic" in source.lower() or "reject" in source.lower() or "invalid" in source.lower()
            ), "Word 'synthetic' should only appear in validation / rejection context"

    def test_error_messages_mention_real_data_only(self):
        """Test that error messages explicitly mention real data requirement."""

        import pandas as pd

        system = ModelBenchmarkSystem()

        # Mock successful pre-tests but insufficient data
        with patch.object(system, "_run_pre_benchmark_tests") as mock_pre_tests:
            mock_pre_tests.return_value = True

            with patch.object(system, "fetch_benchmark_dataset") as mock_fetch:
                mock_fetch.return_value = pd.DataFrame()  # Empty dataset

                config = BenchmarkConfig(experiment_name="test_insufficient")

                with pytest.raises(ValueError) as exc_info:
                    system.run_benchmark(config)

                error_message = str(exc_info.value).lower()

                # Error should mention real data requirement
                assert any(
                    phrase in error_message for phrase in ["real database data", "real data", "database data"]
                ), f"Error message should mention real data requirement: {error_message}"


def test_benchmark_system_initialization():
    """Test that benchmark system initializes without synthetic data capabilities."""

    system = ModelBenchmarkSystem()

    # Should initialize successfully
    assert system is not None
    assert hasattr(system, "models")
    assert len(system.models) > 0

    # Should not have synthetic data methods
    assert not hasattr(system, "_create_synthetic_dataset")


if __name__ == "__main__":
    print("🧪 RUNNING BENCHMARK DATA INTEGRITY TESTS")
    print("=" * 60)
    print("🔒 These tests ensure NO FAKE DATA is ever used in benchmarks")
    print()

    # Run the tests
    pytest.main([__file__, "-v"])
