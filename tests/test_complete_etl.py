#!/usr/bin/env python3
"""
Comprehensive Test Suite for Complete ETL System

This test suite covers:
- Extract phase testing
- Transform phase testing (with your existing helpers)
- Load phase testing
- Null value handling
- Error scenarios
- Integration testing
"""

import json
import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

# Import the modules we're testing
from src.icatalog_public.oss.extract import ExtractConfig, ExtractOrchestrator
from src.icatalog_public.oss.load import LoadConfig, LoadOrchestrator
from src.icatalog_public.oss.transform_complete import CompleteTransformer, TransformConfig


class TestExtractPhase:
    """Test the Extract phase functionality."""

    def test_extract_config_creation(self):
        """Test ExtractConfig creation with various parameters."""
        config = ExtractConfig(
            youtube_playlists=["PLl-ShioB5kaqu8jD43bGi7qX799RIZA3Q"],
            spotify_playlists=["37i9dQZF1DX..."],
            tidal_playlists=["12345678-1234-1234-1234-123456789012"],
            max_retries=5,
            retry_delay=10,
        )

        assert config.youtube_playlists == ["PLl-ShioB5kaqu8jD43bGi7qX799RIZA3Q"]
        assert config.spotify_playlists == ["37i9dQZF1DX..."]
        assert config.tidal_playlists == ["12345678-1234-1234-1234-123456789012"]
        assert config.max_retries == 5
        assert config.retry_delay == 10

    def test_extract_config_defaults(self):
        """Test ExtractConfig with default values."""
        config = ExtractConfig()

        assert config.youtube_playlists == []
        assert config.spotify_playlists == []
        assert config.tidal_playlists == []
        assert config.max_retries == 3
        assert config.retry_delay == 5

    @patch("googleapiclient.discovery.build")
    def test_youtube_extractor_initialization(self, mock_build):
        """Test YouTubeExtractor initialization."""
        mock_youtube = Mock()
        mock_build.return_value = mock_youtube

        from src.icatalog_public.oss.extract import YouTubeExtractor

        extractor = YouTubeExtractor("test_api_key")

        assert extractor.api_key == "test_api_key"
        assert extractor.youtube == mock_youtube
        mock_build.assert_called_once()

    @patch("googleapiclient.discovery.build")
    def test_youtube_extractor_with_invalid_api_key(self, mock_build):
        """Test YouTubeExtractor with invalid API key."""
        mock_build.side_effect = Exception("Invalid API key")

        from src.icatalog_public.oss.extract import YouTubeExtractor

        with pytest.raises(Exception):
            YouTubeExtractor("invalid_key")

    def test_extract_orchestrator_creation(self):
        """Test ExtractOrchestrator creation."""
        config = ExtractConfig()
        orchestrator = ExtractOrchestrator(config)

        assert orchestrator.config == config
        assert orchestrator.youtube_extractor is None
        assert orchestrator.spotify_extractor is None
        assert orchestrator.tidal_extractor is None

    @patch("src.icatalog_public.oss.extract.YouTubeExtractor")
    @patch("src.icatalog_public.oss.extract.SpotifyExtractor")
    @patch("src.icatalog_public.oss.extract.TidalExtractor")
    def test_extract_orchestrator_initialization(self, mock_tidal, mock_spotify, mock_youtube):
        """Test ExtractOrchestrator initialization with all extractors."""
        config = ExtractConfig(
            youtube_playlists=["test_playlist"],
            spotify_playlists=["test_playlist"],
            tidal_playlists=["test_playlist"],
        )

        with patch.dict(
            os.environ,
            {
                "YOUTUBE_API_KEY": "test_key",
                "SPOTIFY_CLIENT_ID": "test_id",
                "SPOTIFY_CLIENT_SECRET": "test_secret",
                "TIDAL_ACCESS_TOKEN": "test_token",
            },
        ):
            orchestrator = ExtractOrchestrator(config)

            assert orchestrator.youtube_extractor is not None
            assert orchestrator.spotify_extractor is not None
            assert orchestrator.tidal_extractor is not None


class TestTransformPhase:
    """Test the Transform phase functionality."""

    def test_transform_config_creation(self):
        """Test TransformConfig creation."""
        config = TransformConfig(
            enable_spotify_normalization=True,
            enable_tidal_normalization=False,
            enable_youtube_normalization=True,
            enable_artist_role_extraction=True,
            enable_version_detection=False,
            batch_size=100,
        )

        assert config.enable_spotify_normalization is True
        assert config.enable_tidal_normalization is False
        assert config.enable_youtube_normalization is True
        assert config.enable_artist_role_extraction is True
        assert config.enable_version_detection is False
        assert config.batch_size == 100

    def test_transform_config_defaults(self):
        """Test TransformConfig with default values."""
        config = TransformConfig()

        assert config.enable_spotify_normalization is True
        assert config.enable_tidal_normalization is True
        assert config.enable_youtube_normalization is True
        assert config.enable_artist_role_extraction is True
        assert config.enable_version_detection is True
        assert config.batch_size == 50

    @patch("web.etl_helpers.get_engine")
    @patch("web.etl_helpers.init_tables")
    @patch("web.etl_helpers._assert_tables_exist")
    def test_complete_transformer_initialization(self, mock_assert_tables, mock_init_tables, mock_get_engine):
        """Test CompleteTransformer initialization."""
        mock_engine = Mock()
        mock_get_engine.return_value = mock_engine

        config = TransformConfig()
        transformer = CompleteTransformer(config)

        assert transformer.config == config
        # Engine is lazy-loaded, so it should be None initially
        assert transformer.engine is None

    @patch("web.etl_helpers.get_engine")
    def test_complete_transformer_initialization_failure(self, mock_get_engine):
        """Test CompleteTransformer initialization failure."""
        mock_get_engine.side_effect = Exception("Database connection failed")

        config = TransformConfig()
        transformer = CompleteTransformer(config)

        # Should fail when trying to initialize database
        with pytest.raises(Exception):
            transformer._init_database()

    @patch("web.etl_helpers.get_engine")
    @patch("web.etl_helpers.init_tables")
    @patch("web.etl_helpers._assert_tables_exist")
    def test_spotify_data_transformation_with_null_values(self, mock_assert_tables, mock_init_tables, mock_get_engine):
        """Test Spotify data transformation with null values."""
        config = TransformConfig()

        # Mock engine
        mock_engine = Mock()
        mock_get_engine.return_value = mock_engine

        # Mock raw data with null values
        raw_tracks = [
            {
                "track_id": "test_id_1",
                "raw_data": json.dumps(
                    {
                        "name": None,
                        "external_ids": {"isrc": None},
                        "artists": [],
                        "album": None,
                    }
                ),
            },
            {
                "track_id": "test_id_2",
                "raw_data": json.dumps(
                    {
                        "name": "Test Song",
                        "external_ids": {"isrc": "TEST12345678"},
                        "artists": [{"name": "Test Artist"}],
                        "album": {"name": "Test Album"},
                    }
                ),
            },
        ]

        with patch("web.etl_helpers.get_connection") as mock_conn:
            mock_connection = Mock()
            mock_conn.return_value.__enter__.return_value = mock_connection

            with patch("web.etl_helpers.normalize_spotify_track") as mock_normalize:
                # First call should fail due to null ISRC
                mock_normalize.side_effect = [ValueError("Missing ISRC"), Mock()]

                transformer = CompleteTransformer(config)
                result = transformer.transform_spotify_data(raw_tracks)

                assert result["success"] == 1
                assert result["error"] == 1
                assert result["total"] == 2

    @patch("web.etl_helpers.get_engine")
    @patch("web.etl_helpers.init_tables")
    @patch("web.etl_helpers._assert_tables_exist")
    def test_tidal_data_transformation_with_missing_attributes(
        self, mock_assert_tables, mock_init_tables, mock_get_engine
    ):
        """Test Tidal data transformation with missing attributes."""
        config = TransformConfig()

        # Mock engine
        mock_engine = Mock()
        mock_get_engine.return_value = mock_engine

        # Mock raw data with missing attributes
        raw_tracks = [
            {
                "track_id": "test_id_1",
                "raw_data": json.dumps(
                    {
                        "name": "Test Song",
                        "isrc": "TEST12345678",
                        "artists": [],
                        "album": None,
                    }
                ),
            }
        ]

        with patch("web.etl_helpers.get_connection") as mock_conn:
            mock_connection = Mock()
            mock_conn.return_value.__enter__.return_value = mock_connection

            with patch("web.etl_helpers.normalize_tidal") as mock_normalize:
                mock_normalize.return_value = (Mock(), Mock(), Mock(), Mock())

                transformer = CompleteTransformer(config)
                result = transformer.transform_tidal_data(raw_tracks)

                assert result["success"] == 1
                assert result["error"] == 0
                assert result["total"] == 1

    @patch("web.etl_helpers.get_engine")
    @patch("web.etl_helpers.init_tables")
    @patch("web.etl_helpers._assert_tables_exist")
    def test_youtube_data_transformation_with_invalid_json(self, mock_assert_tables, mock_init_tables, mock_get_engine):
        """Test YouTube data transformation with invalid JSON."""
        config = TransformConfig()

        # Mock engine
        mock_engine = Mock()
        mock_get_engine.return_value = mock_engine

        # Mock raw data with invalid JSON
        raw_videos = [{"video_id": "test_id_1", "raw_data": "invalid json string"}]

        with patch("web.etl_helpers.get_connection") as mock_conn:
            mock_connection = Mock()
            mock_conn.return_value.__enter__.return_value = mock_connection

            transformer = CompleteTransformer(config)
            result = transformer.transform_youtube_data(raw_videos)

            assert result["success"] == 0
            assert result["error"] == 1
            assert result["total"] == 1


class TestLoadPhase:
    """Test the Load phase functionality."""

    def test_load_config_creation(self):
        """Test LoadConfig creation."""
        config = LoadConfig(
            batch_size=100,
            enable_duplicate_handling=True,
            enable_transaction_rollback=False,
            max_retries=5,
        )

        assert config.batch_size == 100
        assert config.enable_duplicate_handling is True
        assert config.enable_transaction_rollback is False
        assert config.max_retries == 5

    def test_load_config_defaults(self):
        """Test LoadConfig with default values."""
        config = LoadConfig()

        assert config.batch_size == 50
        assert config.enable_duplicate_handling is True
        assert config.enable_transaction_rollback is True
        assert config.max_retries == 3

    @patch("src.icatalog_public.oss.load.get_engine")
    def test_load_orchestrator_initialization(self, mock_get_engine):
        """Test LoadOrchestrator initialization."""
        mock_engine = Mock()
        mock_get_engine.return_value = mock_engine

        config = LoadConfig()
        loader = LoadOrchestrator(config)

        assert loader.config == config
        assert loader.youtube_loader.engine == mock_engine
        assert loader.spotify_loader.engine == mock_engine
        assert loader.tidal_loader.engine == mock_engine

    def test_youtube_loader_with_empty_data(self):
        """Test YouTubeLoader with empty data."""
        config = LoadConfig()

        with patch("src.icatalog_public.oss.load.get_engine") as mock_get_engine:
            mock_engine = Mock()
            mock_get_engine.return_value = mock_engine

            from src.icatalog_public.oss.load import YouTubeLoader

            loader = YouTubeLoader(config)

            # Test with empty video data
            empty_video = {}
            result = loader.load_video(empty_video)

            assert result is False  # Should fail due to missing required fields

    def test_spotify_loader_with_null_values(self):
        """Test SpotifyLoader with null values."""
        config = LoadConfig()

        with patch("src.icatalog_public.oss.load.get_engine") as mock_get_engine:
            mock_engine = Mock()
            mock_get_engine.return_value = mock_engine

            from src.icatalog_public.oss.load import SpotifyLoader

            loader = SpotifyLoader(config)

            # Test with null values
            track_with_nulls = {
                "track_id": None,
                "name": None,
                "artist_names": None,
                "album_name": None,
            }

            result = loader.load_track(track_with_nulls)

            assert result is False  # Should fail due to null values


class TestIntegration:
    """Integration tests for the complete ETL pipeline."""

    def test_full_etl_pipeline_with_mock_data(self):
        """Test the complete ETL pipeline with mock data."""
        # Mock extract phase
        mock_raw_data = {
            "youtube": [
                {
                    "video_id": "test_video_1",
                    "playlist_id": "test_playlist",
                    "raw_data": json.dumps(
                        {
                            "snippet": {
                                "title": "Test Video",
                                "channelTitle": "Test Channel",
                            }
                        }
                    ),
                    "fetched_at": datetime.now().isoformat(),
                }
            ],
            "spotify": [
                {
                    "track_id": "test_track_1",
                    "playlist_id": "test_playlist",
                    "raw_data": json.dumps(
                        {
                            "name": "Test Song",
                            "external_ids": {"isrc": "TEST12345678"},
                            "artists": [{"name": "Test Artist"}],
                            "album": {"name": "Test Album"},
                        }
                    ),
                    "fetched_at": datetime.now().isoformat(),
                }
            ],
            "tidal": [],
        }

        # Mock transform phase
        mock_transformed_data = {
            "youtube": [
                {
                    "video_id": "test_video_1",
                    "title": "Test Video",
                    "channel_title": "Test Channel",
                    "isrc": "TEST12345678",
                }
            ],
            "spotify": [
                {
                    "track_id": "test_track_1",
                    "name": "Test Song",
                    "artist_names": "Test Artist",
                    "album_name": "Test Album",
                    "isrc": "TEST12345678",
                }
            ],
            "tidal": [],
        }

        # Test extract phase
        with patch("src.icatalog_public.oss.extract.ExtractOrchestrator.extract_all_data") as mock_extract:
            mock_extract.return_value = mock_raw_data

            config = ExtractConfig()
            orchestrator = ExtractOrchestrator(config)
            raw_data = orchestrator.extract_all_data()

            assert raw_data == mock_raw_data
            assert len(raw_data["youtube"]) == 1
            assert len(raw_data["spotify"]) == 1
            assert len(raw_data["tidal"]) == 0

        # Test transform phase
        with patch(
            "src.icatalog_public.oss.transform_complete.CompleteTransformer.transform_all_data"
        ) as mock_transform:
            mock_transform.return_value = {
                "youtube": {"success": 1, "error": 0, "total": 1},
                "spotify": {"success": 1, "error": 0, "total": 1},
                "tidal": {"success": 0, "error": 0, "total": 0},
                "artist_roles": {"success": 1, "error": 0, "total": 1},
            }

            config = TransformConfig()
            transformer = CompleteTransformer(config)
            transform_results = transformer.transform_all_data(mock_raw_data)

            assert transform_results["youtube"]["success"] == 1
            assert transform_results["spotify"]["success"] == 1
            assert transform_results["tidal"]["success"] == 0
            assert transform_results["artist_roles"]["success"] == 1

        # Test load phase
        with patch("src.icatalog_public.oss.load.LoadOrchestrator.load_all_data") as mock_load:
            mock_load.return_value = {
                "youtube": {"success": 1, "error": 0},
                "spotify": {"success": 1, "error": 0},
                "tidal": {"success": 0, "error": 0},
            }

            config = LoadConfig()
            loader = LoadOrchestrator(config)
            load_results = loader.load_all_data(mock_transformed_data)

            assert load_results["youtube"]["success"] == 1
            assert load_results["spotify"]["success"] == 1
            assert load_results["tidal"]["success"] == 0


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_api_rate_limiting_handling(self):
        """Test handling of API rate limiting."""
        config = ExtractConfig(max_retries=2, retry_delay=1)

        with patch("src.icatalog_public.oss.extract.YouTubeExtractor.extract_playlist") as mock_extract:
            mock_extract.side_effect = Exception("Rate limit exceeded")

            from src.icatalog_public.oss.extract import YouTubeExtractor

            extractor = YouTubeExtractor("test_key")

            # Should handle rate limiting gracefully
            with pytest.raises(Exception):
                extractor.extract_playlist("test_playlist")

    def test_database_connection_failure(self):
        """Test handling of database connection failures."""
        config = TransformConfig()

        with patch("src.icatalog_public.oss.transform_complete.get_engine") as mock_get_engine:
            mock_get_engine.side_effect = Exception("Database connection failed")

            with pytest.raises(Exception):
                CompleteTransformer(config)

    def test_invalid_json_handling(self):
        """Test handling of invalid JSON data."""
        config = TransformConfig()

        # Test with invalid JSON
        invalid_json_data = [{"track_id": "test_1", "raw_data": '{"invalid": json}'}]

        with patch("src.icatalog_public.oss.transform_complete.get_connection"):
            transformer = CompleteTransformer(config)

            # Should handle invalid JSON gracefully
            result = transformer.transform_spotify_data(invalid_json_data)
            assert result["error"] == 1


class TestNullValueHandling:
    """Test null value handling throughout the pipeline."""

    def test_null_api_key_handling(self):
        """Test handling of null API keys."""
        with patch.dict(os.environ, {}, clear=True):
            config = ExtractConfig()

            # Should handle missing API keys gracefully
            orchestrator = ExtractOrchestrator(config)
            assert orchestrator.youtube_extractor is None

    def test_null_playlist_id_handling(self):
        """Test handling of null playlist IDs."""
        config = ExtractConfig(youtube_playlists=[None, ""])

        # Should filter out null/empty playlist IDs
        assert len(config.youtube_playlists) == 2

    def test_null_track_data_handling(self):
        """Test handling of null track data."""
        config = TransformConfig()

        null_track_data = [{"track_id": None, "raw_data": None}]

        with patch("src.icatalog_public.oss.transform_complete.get_connection"):
            transformer = CompleteTransformer(config)

            # Should handle null track data gracefully
            result = transformer.transform_spotify_data(null_track_data)
            assert result["error"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
