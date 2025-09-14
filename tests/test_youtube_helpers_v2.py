# tests/icatalog_public/oss/test_youtube_helpers_v2.py
"""
TDD Tests for YouTube Helpers v2

This module tests the bulletproof YouTube data processing functions.
Following the TDD pattern established by title_credit_parser_v2.
"""

import pytest
from src.icatalog_public.oss.youtube_helpers_v2 import (
    classify_video_version,
    clean_video_title,
    extract_artist_from_channel,
    extract_video_id,
    handle_api_error,
    is_quota_exceeded_error,
    normalize_string,
    parse_duration_iso8601,
    validate_playlist_data,
    validate_video_data,
)


class TestExtractVideoId:
    """Test video ID extraction function."""

    def test_extract_video_id_dict_format(self):
        """Test extracting video ID from dict format."""
        item = {"id": {"videoId": "dQw4w9WgXcQ"}}
        result = extract_video_id(item)
        assert result == "dQw4w9WgXcQ"

    def test_extract_video_id_direct_string(self):
        """Test extracting video ID when ID is direct string."""
        item = {"id": "dQw4w9WgXcQ"}
        result = extract_video_id(item)
        assert result == "dQw4w9WgXcQ"

    def test_extract_video_id_object_format(self):
        """Test extracting video ID from object format."""

        class MockItem:
            def __init__(self, video_id):
                self.id = video_id

        item = MockItem("dQw4w9WgXcQ")
        result = extract_video_id(item)
        assert result == "dQw4w9WgXcQ"

    def test_extract_video_id_null_item(self):
        """Test handling null item."""
        with pytest.raises(ValueError, match="Video item cannot be null"):
            extract_video_id(None)

    def test_extract_video_id_missing_id_field(self):
        """Test handling item missing 'id' field."""
        item = {"title": "Test Video"}
        with pytest.raises(KeyError, match="Video item missing 'id' field"):
            extract_video_id(item)

    def test_extract_video_id_missing_videoid_field(self):
        """Test handling item missing 'videoId' field."""
        item = {"id": {"title": "Test"}}
        with pytest.raises(KeyError, match="Video item missing 'videoId' field"):
            extract_video_id(item)

    def test_extract_video_id_null_video_id(self):
        """Test handling null video ID."""
        item = {"id": {"videoId": None}}
        with pytest.raises(ValueError, match="Video ID cannot be null"):
            extract_video_id(item)

    def test_extract_video_id_empty_video_id(self):
        """Test handling empty video ID."""
        item = {"id": {"videoId": ""}}
        with pytest.raises(ValueError, match="Video ID cannot be empty"):
            extract_video_id(item)

    def test_extract_video_id_whitespace_video_id(self):
        """Test handling whitespace-only video ID."""
        item = {"id": {"videoId": "   "}}
        with pytest.raises(ValueError, match="Video ID cannot be empty"):
            extract_video_id(item)

    def test_extract_video_id_non_string_video_id(self):
        """Test handling non-string video ID."""
        item = {"id": {"videoId": 123}}
        with pytest.raises(ValueError, match="Video ID must be string"):
            extract_video_id(item)


class TestNormalizeString:
    """Test string normalization function."""

    def test_normalize_string_basic(self):
        """Test basic string normalization."""
        result = normalize_string("Test Artist Name")
        assert result == "test artist name"

    def test_normalize_string_with_punctuation(self):
        """Test normalization with punctuation."""
        result = normalize_string("Artist & Band (Official)")
        assert result == "artist band official"

    def test_normalize_string_with_extra_whitespace(self):
        """Test normalization with extra whitespace."""
        result = normalize_string("  Artist   Name  ")
        assert result == "artist name"

    def test_normalize_string_with_numbers(self):
        """Test normalization with numbers."""
        result = normalize_string("Artist 123")
        assert result == "artist 123"

    def test_normalize_string_null_input(self):
        """Test handling null input."""
        with pytest.raises(ValueError, match="Text cannot be null"):
            normalize_string(None)

    def test_normalize_string_non_string_input(self):
        """Test handling non-string input."""
        with pytest.raises(ValueError, match="Text must be string"):
            normalize_string(123)

    def test_normalize_string_empty_input(self):
        """Test handling empty input."""
        with pytest.raises(ValueError, match="Text cannot be empty"):
            normalize_string("")

    def test_normalize_string_whitespace_only(self):
        """Test handling whitespace-only input."""
        with pytest.raises(ValueError, match="Text cannot be empty"):
            normalize_string("   ")


class TestClassifyVideoVersion:
    """Test video version classification function."""

    def test_classify_video_version_official_music_video(self):
        """Test classifying official music video."""
        result = classify_video_version("Song Title (Official Music Video)", "Artist Channel")
        assert result == "Official Music Video"

    def test_classify_video_version_official_audio(self):
        """Test classifying official audio."""
        result = classify_video_version("Song Title", "Artist - Topic")
        assert result == "Official Audio"

    def test_classify_video_version_live(self):
        """Test classifying live performance."""
        result = classify_video_version("Song Title (Live)", "Artist Channel")
        assert result == "Live Performance"

    def test_classify_video_version_remix(self):
        """Test classifying remix."""
        result = classify_video_version("Song Title (Remix)", "Artist Channel")
        assert result == "Remix"

    def test_classify_video_version_acoustic(self):
        """Test classifying acoustic."""
        result = classify_video_version("Song Title (Acoustic)", "Artist Channel")
        assert result == "Acoustic"

    def test_classify_video_version_original(self):
        """Test classifying original when no keywords found."""
        result = classify_video_version("Song Title", "Artist Channel")
        assert result == "Original"

    def test_classify_video_version_with_description(self):
        """Test classification using description."""
        result = classify_video_version("Song Title", "Artist Channel", "This is a lyric video")
        assert result == "Lyric Video"

    def test_classify_video_version_null_title(self):
        """Test handling null video title."""
        with pytest.raises(ValueError, match="Video title cannot be null"):
            classify_video_version(None, "Artist Channel")

    def test_classify_video_version_empty_title(self):
        """Test handling empty video title."""
        with pytest.raises(ValueError, match="Video title cannot be empty"):
            classify_video_version("", "Artist Channel")

    def test_classify_video_version_null_channel(self):
        """Test handling null channel title."""
        with pytest.raises(ValueError, match="Channel title cannot be null"):
            classify_video_version("Song Title", None)

    def test_classify_video_version_empty_channel(self):
        """Test handling empty channel title."""
        with pytest.raises(ValueError, match="Channel title cannot be empty"):
            classify_video_version("Song Title", "")


class TestExtractArtistFromChannel:
    """Test artist extraction from channel title."""

    def test_extract_artist_from_channel_basic(self):
        """Test basic artist extraction."""
        result = extract_artist_from_channel("Artist Name")
        assert result == "Artist Name"

    def test_extract_artist_from_channel_vevo(self):
        """Test extraction removing VEVO suffix."""
        result = extract_artist_from_channel("Artist Name VEVO")
        assert result == "Artist Name"

    def test_extract_artist_from_channel_topic(self):
        """Test extraction removing Topic suffix."""
        result = extract_artist_from_channel("Artist Name - Topic")
        assert result == "Artist Name"

    def test_extract_artist_from_channel_official(self):
        """Test extraction removing Official suffix."""
        result = extract_artist_from_channel("Artist Name Official")
        assert result == "Artist Name"

    def test_extract_artist_from_channel_multiple_suffixes(self):
        """Test extraction with multiple suffixes."""
        result = extract_artist_from_channel("Artist Name VEVO Official")
        assert result == "Artist Name"

    def test_extract_artist_from_channel_null_input(self):
        """Test handling null input."""
        with pytest.raises(ValueError, match="Channel title cannot be null"):
            extract_artist_from_channel(None)

    def test_extract_artist_from_channel_empty_input(self):
        """Test handling empty input."""
        with pytest.raises(ValueError, match="Channel title cannot be empty"):
            extract_artist_from_channel("")


class TestValidateVideoData:
    """Test video data validation function."""

    def test_validate_video_data_valid(self):
        """Test validating valid video data."""
        video_data = {
            "id": "dQw4w9WgXcQ",
            "snippet": {"title": "Test Video", "channelTitle": "Test Channel"},
        }
        result = validate_video_data(video_data)
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_video_data_missing_id(self):
        """Test validating video data missing ID."""
        video_data = {"snippet": {"title": "Test Video", "channelTitle": "Test Channel"}}
        result = validate_video_data(video_data)
        assert result["valid"] is False
        assert "Missing video ID" in result["errors"]

    def test_validate_video_data_missing_snippet(self):
        """Test validating video data missing snippet."""
        video_data = {"id": "dQw4w9WgXcQ"}
        result = validate_video_data(video_data)
        assert result["valid"] is False
        assert "Missing snippet data" in result["errors"]

    def test_validate_video_data_null_title(self):
        """Test validating video data with null title."""
        video_data = {
            "id": "dQw4w9WgXcQ",
            "snippet": {"title": None, "channelTitle": "Test Channel"},
        }
        result = validate_video_data(video_data)
        assert result["valid"] is False
        assert "Video title cannot be null" in result["errors"]

    def test_validate_video_data_null_input(self):
        """Test handling null input."""
        with pytest.raises(ValueError, match="Video data cannot be null"):
            validate_video_data(None)

    def test_validate_video_data_non_dict_input(self):
        """Test handling non-dict input."""
        with pytest.raises(ValueError, match="Video data must be dict"):
            validate_video_data("not a dict")


class TestValidatePlaylistData:
    """Test playlist data validation function."""

    def test_validate_playlist_data_valid(self):
        """Test validating valid playlist data."""
        playlist_data = {
            "id": "PLl-ShioB5kaqu8jD43bGi7qX799RIZA3Q",
            "snippet": {"title": "Test Playlist"},
        }
        result = validate_playlist_data(playlist_data)
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_playlist_data_missing_id(self):
        """Test validating playlist data missing ID."""
        playlist_data = {"snippet": {"title": "Test Playlist"}}
        result = validate_playlist_data(playlist_data)
        assert result["valid"] is False
        assert "Missing playlist ID" in result["errors"]

    def test_validate_playlist_data_null_id(self):
        """Test validating playlist data with null ID."""
        playlist_data = {"id": None, "snippet": {"title": "Test Playlist"}}
        result = validate_playlist_data(playlist_data)
        assert result["valid"] is False
        assert "Playlist ID cannot be null" in result["errors"]

    def test_validate_playlist_data_null_input(self):
        """Test handling null input."""
        with pytest.raises(ValueError, match="Playlist data cannot be null"):
            validate_playlist_data(None)


class TestCleanVideoTitle:
    """Test video title cleaning function."""

    def test_clean_video_title_basic(self):
        """Test basic title cleaning."""
        result = clean_video_title("Artist - Song Title")
        assert result == "Artist - Song Title"

    def test_clean_video_title_remove_official_music_video(self):
        """Test removing Official Music Video."""
        result = clean_video_title("Artist - Song Title (Official Music Video)")
        assert result == "Artist - Song Title"

    def test_clean_video_title_remove_official_audio(self):
        """Test removing Official Audio."""
        result = clean_video_title("Artist - Song Title (Official Audio)")
        assert result == "Artist - Song Title"

    def test_clean_video_title_remove_multiple_patterns(self):
        """Test removing multiple patterns."""
        result = clean_video_title("Artist - Song Title (Official Music Video) (HD)")
        assert result == "Artist - Song Title"

    def test_clean_video_title_preserve_features(self):
        """Test preserving featured artists."""
        result = clean_video_title("Artist - Song Title (feat. Guest) (Official Music Video)")
        assert result == "Artist - Song Title (feat. Guest)"

    def test_clean_video_title_no_cleaning(self):
        """Test title cleaning with noise removal disabled."""
        result = clean_video_title("Artist - Song Title (Official Music Video)", remove_youtube_noise=False)
        assert result == "Artist - Song Title (Official Music Video)"

    def test_clean_video_title_null_input(self):
        """Test handling null input."""
        with pytest.raises(ValueError, match="Title cannot be null"):
            clean_video_title(None)

    def test_clean_video_title_empty_input(self):
        """Test handling empty input."""
        with pytest.raises(ValueError, match="Title cannot be empty"):
            clean_video_title("")


class TestParseDurationIso8601:
    """Test ISO 8601 duration parsing function."""

    def test_parse_duration_minutes_seconds(self):
        """Test parsing minutes and seconds."""
        result = parse_duration_iso8601("PT3M33S")
        assert result == 213  # 3*60 + 33

    def test_parse_duration_hours_minutes_seconds(self):
        """Test parsing hours, minutes, and seconds."""
        result = parse_duration_iso8601("PT1H2M30S")
        assert result == 3750  # 1*3600 + 2*60 + 30

    def test_parse_duration_seconds_only(self):
        """Test parsing seconds only."""
        result = parse_duration_iso8601("PT45S")
        assert result == 45

    def test_parse_duration_minutes_only(self):
        """Test parsing minutes only."""
        result = parse_duration_iso8601("PT5M")
        assert result == 300  # 5*60

    def test_parse_duration_hours_only(self):
        """Test parsing hours only."""
        result = parse_duration_iso8601("PT2H")
        assert result == 7200  # 2*3600

    def test_parse_duration_null_input(self):
        """Test handling null input."""
        with pytest.raises(ValueError, match="Duration cannot be null"):
            parse_duration_iso8601(None)

    def test_parse_duration_empty_input(self):
        """Test handling empty input."""
        with pytest.raises(ValueError, match="Duration cannot be empty"):
            parse_duration_iso8601("")

    def test_parse_duration_invalid_format(self):
        """Test handling invalid format."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration_iso8601("invalid")


class TestHandleApiError:
    """Test API error handling function."""

    def test_handle_api_error_quota_exceeded(self):
        """Test handling quota exceeded error."""

        class MockError:
            def __init__(self):
                self.resp = {"status": 403}

            def __str__(self):
                return "quotaExceeded"

        error = MockError()
        result = handle_api_error(error)

        assert result["is_quota_error"] is True
        assert result["should_retry"] is False
        assert result["status_code"] == 403

    def test_handle_api_error_rate_limit(self):
        """Test handling rate limit error."""

        class MockError:
            def __init__(self):
                self.resp = {"status": 429}

            def __str__(self):
                return "rate limit"

        error = MockError()
        result = handle_api_error(error)

        assert result["is_quota_error"] is False
        assert result["should_retry"] is True
        assert result["status_code"] == 429

    def test_handle_api_error_server_error(self):
        """Test handling server error."""

        class MockError:
            def __init__(self):
                self.resp = {"status": 500}

            def __str__(self):
                return "internal server error"

        error = MockError()
        result = handle_api_error(error)

        assert result["is_quota_error"] is False
        assert result["should_retry"] is True
        assert result["status_code"] == 500

    def test_handle_api_error_null_input(self):
        """Test handling null input."""
        result = handle_api_error(None)

        assert result["is_quota_error"] is False
        assert result["should_retry"] is False
        assert "Unknown error" in result["message"]


class TestIsQuotaExceededError:
    """Test quota exceeded error detection function."""

    def test_is_quota_exceeded_error_true(self):
        """Test detecting quota exceeded error."""

        class MockError:
            def __init__(self):
                self.resp = {"status": 403}

            def __str__(self):
                return "quotaExceeded"

        error = MockError()
        result = is_quota_exceeded_error(error)
        assert result is True

    def test_is_quota_exceeded_error_false(self):
        """Test detecting non-quota error."""

        class MockError:
            def __init__(self):
                self.resp = {"status": 404}

            def __str__(self):
                return "not found"

        error = MockError()
        result = is_quota_exceeded_error(error)
        assert result is False


# Integration tests
class TestIntegration:
    """Test integration scenarios."""

    def test_full_video_processing_flow(self):
        """Test full video processing flow."""
        # Simulate processing a video from API response
        video_data = {
            "id": "dQw4w9WgXcQ",
            "snippet": {
                "title": "Rick Astley - Never Gonna Give You Up (Official Music Video)",
                "channelTitle": "Rick Astley",
                "description": "Official music video",
            },
        }

        # Validate the data
        validation = validate_video_data(video_data)
        assert validation["valid"] is True

        # Extract video ID
        video_id = extract_video_id(video_data)
        assert video_id == "dQw4w9WgXcQ"

        # Clean the title
        cleaned_title = clean_video_title(video_data["snippet"]["title"])
        assert cleaned_title == "Rick Astley - Never Gonna Give You Up"

        # Classify the version
        version = classify_video_version(
            video_data["snippet"]["title"],
            video_data["snippet"]["channelTitle"],
            video_data["snippet"]["description"],
        )
        assert version == "Official Music Video"

        # Extract artist from channel
        artist = extract_artist_from_channel(video_data["snippet"]["channelTitle"])
        assert artist == "Rick Astley"

    def test_error_handling_flow(self):
        """Test error handling flow."""
        # Test with invalid data
        invalid_data = {
            "id": None,  # This should cause validation to fail
            "snippet": {"title": "Test", "channelTitle": "Test"},
        }

        validation = validate_video_data(invalid_data)
        assert validation["valid"] is False
        assert len(validation["errors"]) > 0

        # Test with null data
        with pytest.raises(ValueError):
            validate_video_data(None)
