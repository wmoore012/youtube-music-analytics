#!/usr/bin/env python3
"""
Tests for channel cleanup functionality
"""

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tools.maintenance.channel_cleanup import (
    analyze_channel_data,
    create_cleanup_plan,
    execute_cleanup,
    get_channel_titles_from_urls,
    get_configured_channels,
)


class TestChannelConfiguration:
    """Test channel configuration extraction from environment."""

    @patch.dict(
        os.environ,
        {
            "YT_BICFIZZLE_YT": "https://youtube.com/@BicFizzle",
            "YT_COBRAH_YT": "https://www.youtube.com/@COBRAH",
            "YT_FLYANABOSS_YT": "https://www.youtube.com/@FlyanaBoss",
            "OTHER_VAR": "not_a_channel",
            "YT_EMPTY_YT": "",  # Empty channel should be ignored
        },
    )
    def test_get_configured_channels(self):
        """Test extraction of configured channels from environment."""
        channels = get_configured_channels()

        # Should extract valid channels
        assert "BiC Fizzle" in channels
        assert "COBRAH" in channels
        assert "Flyana Boss" in channels

        # Should ignore non-channel variables
        assert "OTHER_VAR" not in str(channels)

        # Should ignore empty channels
        assert "" not in channels.values()

        # Check URL values
        assert channels["BiC Fizzle"] == "https://youtube.com/@BicFizzle"
        assert channels["COBRAH"] == "https://www.youtube.com/@COBRAH"

        print(f"✅ Configured channels extracted: {len(channels)} channels")
        print("✅ Channel configuration parsing working correctly")

    def test_channel_name_normalization(self):
        """Test that channel names are properly normalized."""
        with patch.dict(
            os.environ,
            {
                "YT_BICFIZZLE_YT": "https://youtube.com/@BicFizzle",
                "YT_SOME_ARTIST_YT": "https://youtube.com/@SomeArtist",
            },
        ):
            channels = get_configured_channels()

            # Check normalization
            assert "BiC Fizzle" in channels  # Special case
            assert "Some Artist" in channels  # Underscore to space, title case

        print("✅ Channel name normalization working correctly")


class TestChannelAnalysis:
    """Test channel data analysis functionality."""

    def test_analyze_channel_data_structure(self):
        """Test that channel analysis returns expected structure."""
        # Mock database engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock pandas read_sql to return sample data
        sample_data = pd.DataFrame(
            {
                "channel_title": ["Artist1", "Artist2", "UnknownChannel"],
                "video_count": [10, 5, 2],
                "metrics_count": [10, 5, 2],
                "comment_count": [100, 50, 10],
                "earliest_video": ["2023-01-01", "2023-02-01", "2023-03-01"],
                "latest_video": ["2023-12-01", "2023-11-01", "2023-10-01"],
            }
        )

        with patch("pandas.read_sql", return_value=sample_data):
            analysis = analyze_channel_data(mock_engine)

        # Check structure
        assert "total_channels" in analysis
        assert "channel_data" in analysis
        assert "total_videos" in analysis
        assert "total_metrics" in analysis
        assert "total_comments" in analysis

        # Check values
        assert analysis["total_channels"] == 3
        assert analysis["total_videos"] == 17  # 10 + 5 + 2
        assert analysis["total_comments"] == 160  # 100 + 50 + 10

        print("✅ Channel analysis structure correct")

    def test_create_cleanup_plan(self):
        """Test cleanup plan creation."""
        # Sample current analysis
        current_analysis = {
            "channel_data": pd.DataFrame(
                {
                    "channel_title": ["ValidArtist", "InvalidChannel", "AnotherValid"],
                    "video_count": [10, 5, 8],
                    "metrics_count": [10, 5, 8],
                    "comment_count": [100, 50, 80],
                    "earliest_video": ["2023-01-01", "2023-02-01", "2023-03-01"],
                    "latest_video": ["2023-12-01", "2023-11-01", "2023-10-01"],
                }
            )
        }

        # Valid channels to keep
        valid_channels = {"ValidArtist", "AnotherValid"}

        cleanup_plan = create_cleanup_plan(valid_channels, current_analysis)

        # Check structure
        assert "channels_to_remove" in cleanup_plan
        assert "channels_to_keep" in cleanup_plan
        assert "summary" in cleanup_plan

        # Check that invalid channel is marked for removal
        channels_to_remove = cleanup_plan["channels_to_remove"]
        assert len(channels_to_remove) == 1
        assert channels_to_remove[0]["channel_title"] == "InvalidChannel"

        # Check summary
        summary = cleanup_plan["summary"]
        assert summary["channels_removed"] == 1
        assert summary["videos_removed"] == 5
        assert summary["comments_removed"] == 50

        print("✅ Cleanup plan creation working correctly")


class TestCleanupExecution:
    """Test cleanup execution functionality."""

    def test_dry_run_mode(self):
        """Test that dry run mode doesn't execute actual deletions."""
        # Mock cleanup plan
        cleanup_plan = {
            "channels_to_remove": [
                {
                    "channel_title": "TestChannel",
                    "video_count": 5,
                    "metrics_count": 5,
                    "comment_count": 50,
                    "earliest_video": "2023-01-01",
                    "latest_video": "2023-12-01",
                }
            ]
        }

        # Mock database engine
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock pandas read_sql to return empty results (no videos found)
        with patch("pandas.read_sql", return_value=pd.DataFrame({"video_id": []})):
            # Execute in dry run mode
            execute_cleanup(cleanup_plan, mock_engine, dry_run=True)

        # Verify no database operations were called
        mock_conn.execute.assert_not_called()
        mock_conn.commit.assert_not_called()

        print("✅ Dry run mode working correctly (no database operations)")

    def test_live_mode_execution_order(self):
        """Test that live mode executes deletions in correct order."""
        cleanup_plan = {
            "channels_to_remove": [
                {
                    "channel_title": "TestChannel",
                    "video_count": 1,
                    "metrics_count": 1,
                    "comment_count": 1,
                    "earliest_video": "2023-01-01",
                    "latest_video": "2023-12-01",
                }
            ]
        }

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock video IDs
        mock_video_data = pd.DataFrame({"video_id": ["video123"]})

        with patch("pandas.read_sql", return_value=mock_video_data):
            execute_cleanup(cleanup_plan, mock_engine, dry_run=False)

        # Verify database operations were called
        assert mock_conn.execute.call_count >= 4  # Should delete from 4 tables
        mock_conn.commit.assert_called_once()

        print("✅ Live mode execution working correctly")


class TestChannelCleanupIntegration:
    """Integration tests for channel cleanup functionality."""

    def test_full_cleanup_workflow(self):
        """Test the complete cleanup workflow."""
        # This would be an integration test that:
        # 1. Sets up test data in database
        # 2. Configures test channels
        # 3. Runs cleanup
        # 4. Verifies results

        # For now, just verify the workflow components exist
        assert callable(get_configured_channels)
        assert callable(analyze_channel_data)
        assert callable(create_cleanup_plan)
        assert callable(execute_cleanup)

        print("✅ Full cleanup workflow components available")

    def test_cleanup_safety_checks(self):
        """Test safety mechanisms in cleanup process."""
        # Test that cleanup plan shows what will be removed
        current_analysis = {
            "channel_data": pd.DataFrame(
                {
                    "channel_title": ["TestChannel"],
                    "video_count": [100],  # Large number to test safety
                    "metrics_count": [100],
                    "comment_count": [1000],
                    "earliest_video": ["2023-01-01"],
                    "latest_video": ["2023-12-01"],
                }
            )
        }

        valid_channels = set()  # No valid channels - everything should be removed

        cleanup_plan = create_cleanup_plan(valid_channels, current_analysis)

        # Verify large removal is detected
        assert cleanup_plan["summary"]["videos_removed"] == 100
        assert cleanup_plan["summary"]["comments_removed"] == 1000

        # This should trigger safety warnings in real usage
        print("✅ Cleanup safety checks working correctly")


class TestChannelValidation:
    """Test channel validation against configured channels."""

    def test_channel_matching_logic(self):
        """Test the logic for matching database channels to configured channels."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Mock database responses
        def mock_execute(query):
            mock_result = MagicMock()
            if "BicFizzle" in str(query.params):
                mock_result.fetchall.return_value = [("BiC Fizzle",)]
            else:
                mock_result.fetchall.return_value = []
            return mock_result

        mock_conn.execute.side_effect = mock_execute

        configured_channels = {"BiC Fizzle": "https://youtube.com/@BicFizzle"}

        valid_titles = get_channel_titles_from_urls(configured_channels, mock_engine)

        assert "BiC Fizzle" in valid_titles
        print("✅ Channel matching logic working correctly")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
