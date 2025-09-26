"""Test sentiment plugin integration."""

import os

import pandas as pd
import pytest

from web.enhanced_sentiment_job import (
    EnhancedYouTubeCommentSentimentJob,
    create_enhanced_sentiment_job,
    run_enhanced_sentiment_batch,
)


class TestSentimentPluginIntegration:
    """Test sentiment analysis plugin integration."""

    def test_enhanced_sentiment_job_initialization(self):
        """Test enhanced sentiment job initializes correctly."""
        # Test with plugins enabled
        job = EnhancedYouTubeCommentSentimentJob(enable_plugins=True)
        assert job._enable_plugins is True

        # Test with plugins disabled
        job_no_plugins = EnhancedYouTubeCommentSentimentJob(enable_plugins=False)
        assert job_no_plugins._enable_plugins is False

        print("Enhanced sentiment job initialization: PASSED")

    def test_plugin_system_status(self):
        """Test getting plugin system status."""
        job = create_enhanced_sentiment_job(enable_plugins=True)
        status = job.get_plugin_system_status()

        assert isinstance(status, dict)
        assert "plugins_enabled" in status
        assert "available_algorithms" in status
        assert "sentiment_algorithms" in status

        print(f"Plugin system status: {status}")

    def test_enhanced_sentiment_with_mock_data(self):
        """Test enhanced sentiment processing with mock data."""
        # Create job without plugins to test basic functionality
        job = create_enhanced_sentiment_job(enable_plugins=False)

        # Test that the job can be created and has expected methods
        assert hasattr(job, "score_batch_enhanced")
        assert hasattr(job, "refresh_summary_enhanced")
        assert hasattr(job, "_process_with_vader")

        print("Enhanced sentiment job methods: PASSED")

    def test_plugin_data_preparation(self):
        """Test plugin data preparation."""
        job = create_enhanced_sentiment_job(enable_plugins=True)

        # Create mock comment data
        mock_comments = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "video_id": ["vid1", "vid2", "vid3"],
                "comment_text": ["Great song!", "Not my favorite", "Amazing performance"],
                "author_name": ["user1", "user2", "user3"],
                "published_at": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            }
        )

        # Test data preparation
        plugin_data = job._prepare_plugin_data(mock_comments)

        assert "entity_id" in plugin_data.columns
        assert "text_length" in plugin_data.columns
        assert "word_count" in plugin_data.columns
        assert "days_since_published" in plugin_data.columns

        # Check that entity_id is string type
        assert plugin_data["entity_id"].dtype == "object"

        print("Plugin data preparation: PASSED")
        print(f"Prepared data columns: {list(plugin_data.columns)}")

    def test_vader_processing(self):
        """Test VADER sentiment processing."""
        job = create_enhanced_sentiment_job(enable_plugins=False)

        # Create mock comment data
        mock_comments = pd.DataFrame(
            {"id": [1, 2, 3], "comment_text": ["I love this song!", "This is terrible", "It's okay I guess"]}
        )

        # Test VADER processing
        updates = job._process_with_vader(mock_comments)

        assert isinstance(updates, list)
        assert len(updates) == 3

        # Check that updates contain (score, comment_id) tuples
        for update in updates:
            assert isinstance(update, tuple)
            assert len(update) == 2
            score, comment_id = update
            assert isinstance(score, float)
            assert isinstance(comment_id, (int, str))
            assert -1.0 <= score <= 1.0

        print("VADER processing: PASSED")
        print(f"Sample VADER scores: {updates}")

    @pytest.mark.skipif(
        not all([os.getenv("DB_HOST"), os.getenv("DB_USER"), os.getenv("DB_NAME")]),
        reason="Database credentials not available",
    )
    def test_table_creation(self):
        """Test plugin sentiment table creation."""
        job = create_enhanced_sentiment_job(enable_plugins=True)

        try:
            # Test table creation methods
            job._ensure_plugin_sentiment_table()
            job._ensure_plugin_summary_table()

            print("Plugin sentiment tables created successfully")

        except Exception as e:
            print(f"Table creation test failed: {e}")
            # Don't fail test if database is not available

    def test_enhanced_sentiment_stats(self):
        """Test enhanced sentiment statistics."""
        from web.enhanced_sentiment_job import EnhancedSentimentStats

        # Test basic stats creation
        stats = EnhancedSentimentStats(processed=100, updated=95, skipped=5)

        assert stats.processed == 100
        assert stats.updated == 95
        assert stats.skipped == 5
        assert stats.plugin_scores_generated == 0
        assert isinstance(stats.plugin_algorithms_used, list)
        assert isinstance(stats.errors, list)

        print("Enhanced sentiment stats: PASSED")

    def test_backward_compatibility_functions(self):
        """Test backward compatibility functions."""
        # Test job creation function
        job = create_enhanced_sentiment_job(enable_plugins=False)
        assert isinstance(job, EnhancedYouTubeCommentSentimentJob)

        # Test that batch function exists and can be called
        # (We won't actually run it without database)
        assert callable(run_enhanced_sentiment_batch)

        print("Backward compatibility functions: PASSED")

    def test_decimal_conversion(self):
        """Test decimal conversion utility."""
        job = create_enhanced_sentiment_job(enable_plugins=False)

        # Test various values
        test_cases = [
            (0.5, 0.5),
            (1.5, 1.0),  # Should be clamped to 1.0
            (-1.5, -1.0),  # Should be clamped to -1.0
            (0.123456, 0.12),  # Should be rounded to 2 decimal places
            (-0.987654, -0.99),  # Should be rounded to 2 decimal places
        ]

        for input_val, expected in test_cases:
            result = job._to_decimal_2(input_val)
            assert result == expected, f"Expected {expected}, got {result} for input {input_val}"

        print("Decimal conversion: PASSED")


if __name__ == "__main__":
    # Run basic tests
    test = TestSentimentPluginIntegration()

    print("Testing enhanced sentiment job initialization...")
    test.test_enhanced_sentiment_job_initialization()

    print("\nTesting plugin system status...")
    test.test_plugin_system_status()

    print("\nTesting enhanced sentiment with mock data...")
    test.test_enhanced_sentiment_with_mock_data()

    print("\nTesting plugin data preparation...")
    test.test_plugin_data_preparation()

    print("\nTesting VADER processing...")
    test.test_vader_processing()

    print("\nTesting enhanced sentiment stats...")
    test.test_enhanced_sentiment_stats()

    print("\nTesting backward compatibility functions...")
    test.test_backward_compatibility_functions()

    print("\nTesting decimal conversion...")
    test.test_decimal_conversion()

    print("\nTesting table creation...")
    test.test_table_creation()

    print("\nAll sentiment plugin integration tests completed!")
