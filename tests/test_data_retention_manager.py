"""
Tests for DataRetentionManager

This test suite validates the safe data retention functionality including
dependency checking, deletion operations, and orphaned data cleanup.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from web.data_retention_manager import DataRetentionManager, DeletionReport, DeletionResult, DependencyInfo, VideoInfo


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine for testing."""
    engine = Mock()
    engine.connect.return_value.__enter__ = Mock()
    engine.connect.return_value.__exit__ = Mock()
    return engine


@pytest.fixture
def mock_connection():
    """Create a mock database connection for testing."""
    conn = Mock(spec=Connection)
    return conn


@pytest.fixture
def retention_manager(mock_engine):
    """Create a DataRetentionManager instance for testing."""
    return DataRetentionManager(mock_engine, retention_days=30)


class TestDataRetentionManagerInit:
    """Test DataRetentionManager initialization."""

    def test_init_with_explicit_retention_days(self, mock_engine):
        """Test initialization with explicit retention days."""
        manager = DataRetentionManager(mock_engine, retention_days=45)
        assert manager.retention_days == 45
        assert manager.engine == mock_engine

        # Check cutoff date is approximately correct
        expected_cutoff = datetime.now(timezone.utc) - timedelta(days=45)
        assert abs((manager.cutoff_date - expected_cutoff).total_seconds()) < 60

    def test_init_with_env_var_retention_days(self, mock_engine):
        """Test initialization using environment variable."""
        with patch.dict("os.environ", {"YOUTUBE_DATA_RETENTION_DAYS": "60"}):
            manager = DataRetentionManager(mock_engine)
            assert manager.retention_days == 60

    def test_init_with_default_retention_days(self, mock_engine):
        """Test initialization with default retention days."""
        with patch.dict("os.environ", {}, clear=True):
            manager = DataRetentionManager(mock_engine)
            assert manager.retention_days == 30


class TestDeletionSafetyCheck:
    """Test deletion safety checking functionality."""

    def test_check_deletion_safety_no_videos(self, retention_manager, mock_connection):
        """Test safety check when no videos are found."""
        mock_connection.execute.return_value = []

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            report = retention_manager.check_deletion_safety()

            assert report.total_videos_checked == 0
            assert len(report.eligible_videos) == 0
            assert len(report.blocked_videos) == 0

    def test_check_deletion_safety_with_expired_videos(self, retention_manager, mock_connection):
        """Test safety check with expired videos that have no dependencies."""
        # Mock expired videos
        old_date = datetime.now(timezone.utc) - timedelta(days=45)
        mock_videos = [
            {
                "video_id": "video1",
                "title": "Test Video 1",
                "published_at": old_date,
                "fetched_at": datetime.now(timezone.utc),
            }
        ]

        # Mock no dependencies
        mock_connection.execute.side_effect = [
            mock_videos,  # _get_expired_videos
            0,  # metrics count
            0,  # comments count
            0,  # isrc links count
        ]

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            report = retention_manager.check_deletion_safety()

            assert report.total_videos_checked == 1
            assert len(report.eligible_videos) == 1
            assert len(report.videos_safe_to_delete) == 1
            assert report.eligible_videos[0].video_id == "video1"
            assert not report.eligible_videos[0].has_dependencies

    def test_check_deletion_safety_with_dependencies(self, retention_manager, mock_connection):
        """Test safety check with videos that have dependencies."""
        old_date = datetime.now(timezone.utc) - timedelta(days=45)
        mock_videos = [
            {
                "video_id": "video1",
                "title": "Test Video 1",
                "published_at": old_date,
                "fetched_at": datetime.now(timezone.utc),
            }
        ]

        # Mock dependencies exist
        mock_connection.execute.side_effect = [
            mock_videos,  # _get_expired_videos
            5,  # metrics count
            [("2023-01-01",), ("2023-01-02",)],  # sample metrics
            10,  # comments count
            [("comment1",), ("comment2",)],  # sample comments
            2,  # isrc links count
            [("ISRC123",), ("ISRC456",)],  # sample isrcs
        ]

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            report = retention_manager.check_deletion_safety()

            assert report.total_videos_checked == 1
            assert len(report.eligible_videos) == 1
            assert len(report.videos_safe_to_delete) == 0
            assert len(report.videos_with_dependencies) == 1

            video = report.eligible_videos[0]
            assert video.has_dependencies
            assert len(video.dependencies) == 3

            # Check dependency details
            metrics_dep = next(d for d in video.dependencies if d.table_name == "youtube_metrics")
            assert metrics_dep.count == 5
            assert len(metrics_dep.sample_ids) == 2

    def test_check_deletion_safety_specific_videos(self, retention_manager, mock_connection):
        """Test safety check for specific video IDs."""
        video_ids = ["video1", "video2"]
        mock_videos = [
            {
                "video_id": "video1",
                "title": "Test Video 1",
                "published_at": datetime.now(timezone.utc) - timedelta(days=45),
                "fetched_at": datetime.now(timezone.utc),
            }
        ]

        mock_connection.execute.side_effect = [mock_videos, 0, 0, 0]  # _get_specific_videos  # no dependencies

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            report = retention_manager.check_deletion_safety(video_ids)

            # Verify specific video query was used
            calls = mock_connection.execute.call_args_list
            assert "video_id_0" in str(calls[0])  # Check for parameterized query


class TestDeleteExpiredVideos:
    """Test video deletion functionality."""

    def test_delete_expired_videos_dry_run(self, retention_manager):
        """Test dry run deletion (no actual deletion)."""
        mock_videos = [
            VideoInfo(
                video_id="video1",
                title="Test Video",
                published_at=datetime.now(timezone.utc) - timedelta(days=45),
                age_days=45,
                dependencies=[],
            )
        ]

        with patch.object(retention_manager, "check_deletion_safety") as mock_check:
            mock_check.return_value = Mock(videos_safe_to_delete=mock_videos)

            result = retention_manager.delete_expired_videos(dry_run=True)

            assert result.videos_deleted == 1
            assert len(result.errors) == 0
            assert result.related_data_deleted == {}

    def test_delete_expired_videos_no_confirmation(self, retention_manager):
        """Test that deletion requires explicit confirmation."""
        with pytest.raises(ValueError, match="confirm_deletion must be True"):
            retention_manager.delete_expired_videos(dry_run=False, confirm_deletion=False)

    def test_delete_expired_videos_no_eligible_videos(self, retention_manager):
        """Test deletion when no videos are eligible."""
        with patch.object(retention_manager, "check_deletion_safety") as mock_check:
            mock_check.return_value = Mock(videos_safe_to_delete=[])

            result = retention_manager.delete_expired_videos(dry_run=False, confirm_deletion=True)

            assert result.videos_deleted == 0
            assert len(result.errors) == 0

    def test_delete_expired_videos_actual_deletion(self, retention_manager, mock_connection):
        """Test actual video deletion."""
        mock_videos = [
            VideoInfo(
                video_id="video1",
                title="Test Video",
                published_at=datetime.now(timezone.utc) - timedelta(days=45),
                age_days=45,
                dependencies=[],
            )
        ]

        # Mock successful deletion
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        mock_connection.commit.return_value = None

        with patch.object(retention_manager, "check_deletion_safety") as mock_check:
            mock_check.return_value = Mock(videos_safe_to_delete=mock_videos)

            with patch.object(retention_manager.engine, "connect") as mock_connect:
                mock_connect.return_value.__enter__.return_value = mock_connection

                result = retention_manager.delete_expired_videos(dry_run=False, confirm_deletion=True)

                assert result.videos_deleted == 1
                assert len(result.errors) == 0
                mock_connection.commit.assert_called_once()

    def test_delete_expired_videos_with_error(self, retention_manager, mock_connection):
        """Test deletion with database error."""
        mock_videos = [
            VideoInfo(
                video_id="video1",
                title="Test Video",
                published_at=datetime.now(timezone.utc) - timedelta(days=45),
                age_days=45,
                dependencies=[],
            )
        ]

        # Mock database error
        mock_connection.execute.side_effect = Exception("Database error")

        with patch.object(retention_manager, "check_deletion_safety") as mock_check:
            mock_check.return_value = Mock(videos_safe_to_delete=mock_videos)

            with patch.object(retention_manager.engine, "connect") as mock_connect:
                mock_connect.return_value.__enter__.return_value = mock_connection

                result = retention_manager.delete_expired_videos(dry_run=False, confirm_deletion=True)

                assert result.videos_deleted == 0
                assert len(result.errors) == 1
                assert "Database error" in result.errors[0]
                mock_connection.rollback.assert_called_once()


class TestOrphanedDataCleanup:
    """Test orphaned data cleanup functionality."""

    def test_cleanup_orphaned_data_dry_run(self, retention_manager, mock_connection):
        """Test orphaned data cleanup in dry run mode."""
        # Mock orphaned data
        mock_connection.execute.side_effect = [
            ["orphaned_video1", "orphaned_video2"],  # orphaned metrics
            ["orphaned_video3"],  # orphaned comments
            ["orphaned_video4"],  # orphaned isrc links
        ]

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            result = retention_manager.cleanup_orphaned_data(dry_run=True)

            assert result.videos_deleted == 0
            assert result.related_data_deleted["youtube_metrics"] == 2
            assert result.related_data_deleted["youtube_comments"] == 1
            assert result.related_data_deleted["video_recording_link"] == 1
            assert len(result.errors) == 0

    def test_cleanup_orphaned_data_actual_cleanup(self, retention_manager, mock_connection):
        """Test actual orphaned data cleanup."""
        # Mock orphaned data and successful deletion
        mock_connection.execute.side_effect = [
            ["orphaned_video1"],  # orphaned metrics
            ["orphaned_video2"],  # orphaned comments
            [],  # orphaned isrc links
            None,  # delete metrics
            None,  # delete comments
        ]

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            result = retention_manager.cleanup_orphaned_data(dry_run=False)

            assert result.related_data_deleted["youtube_metrics"] == 1
            assert result.related_data_deleted["youtube_comments"] == 1
            assert result.related_data_deleted["video_recording_link"] == 0
            assert len(result.errors) == 0
            mock_connection.commit.assert_called_once()

    def test_cleanup_orphaned_data_with_error(self, retention_manager, mock_connection):
        """Test orphaned data cleanup with database error."""
        # Mock orphaned data and database error
        mock_connection.execute.side_effect = [
            ["orphaned_video1"],  # orphaned metrics
            [],  # orphaned comments
            [],  # orphaned isrc links
            Exception("Cleanup error"),  # delete error
        ]

        with patch.object(retention_manager.engine, "connect") as mock_connect:
            mock_connect.return_value.__enter__.return_value = mock_connection

            result = retention_manager.cleanup_orphaned_data(dry_run=False)

            assert len(result.errors) == 1
            assert "Cleanup error" in result.errors[0]
            mock_connection.rollback.assert_called_once()


class TestDataClasses:
    """Test data class functionality."""

    def test_video_info_has_dependencies(self):
        """Test VideoInfo dependency checking."""
        # Video with no dependencies
        video_no_deps = VideoInfo(
            video_id="video1", title="Test Video", published_at=datetime.now(timezone.utc), age_days=30, dependencies=[]
        )
        assert not video_no_deps.has_dependencies

        # Video with dependencies
        video_with_deps = VideoInfo(
            video_id="video2",
            title="Test Video 2",
            published_at=datetime.now(timezone.utc),
            age_days=30,
            dependencies=[DependencyInfo(table_name="youtube_metrics", count=5, sample_ids=["metric1", "metric2"])],
        )
        assert video_with_deps.has_dependencies

    def test_deletion_report_properties(self):
        """Test DeletionReport property methods."""
        safe_video = VideoInfo(
            video_id="safe_video",
            title="Safe Video",
            published_at=datetime.now(timezone.utc),
            age_days=30,
            dependencies=[],
        )

        unsafe_video = VideoInfo(
            video_id="unsafe_video",
            title="Unsafe Video",
            published_at=datetime.now(timezone.utc),
            age_days=30,
            dependencies=[DependencyInfo("youtube_metrics", 5, ["m1", "m2"])],
        )

        blocked_video = VideoInfo(
            video_id="blocked_video",
            title="Blocked Video",
            published_at=datetime.now(timezone.utc),
            age_days=15,  # Too recent
            dependencies=[],
        )

        report = DeletionReport(
            eligible_videos=[safe_video, unsafe_video],
            blocked_videos=[blocked_video],
            total_videos_checked=3,
            retention_cutoff_date=datetime.now(timezone.utc) - timedelta(days=30),
        )

        assert len(report.videos_safe_to_delete) == 1
        assert report.videos_safe_to_delete[0].video_id == "safe_video"

        assert len(report.videos_with_dependencies) == 2
        video_ids = [v.video_id for v in report.videos_with_dependencies]
        assert "unsafe_video" in video_ids
        assert "blocked_video" in video_ids


class TestPrivateMethods:
    """Test private helper methods."""

    def test_get_expired_videos(self, retention_manager, mock_connection):
        """Test _get_expired_videos method."""
        mock_result = [
            Mock(
                _mapping={
                    "video_id": "v1",
                    "title": "Video 1",
                    "published_at": datetime.now(),
                    "fetched_at": datetime.now(),
                }
            ),
            Mock(
                _mapping={
                    "video_id": "v2",
                    "title": "Video 2",
                    "published_at": datetime.now(),
                    "fetched_at": datetime.now(),
                }
            ),
        ]
        mock_connection.execute.return_value = mock_result

        videos = retention_manager._get_expired_videos(mock_connection)

        assert len(videos) == 2
        assert videos[0]["video_id"] == "v1"
        assert videos[1]["video_id"] == "v2"

    def test_check_video_dependencies(self, retention_manager, mock_connection):
        """Test _check_video_dependencies method."""
        # Mock dependency counts and samples
        mock_connection.execute.side_effect = [
            5,  # metrics count
            [("2023-01-01",), ("2023-01-02",)],  # sample metrics
            3,  # comments count
            [("comment1",), ("comment2",)],  # sample comments
            1,  # isrc links count
            [("ISRC123",)],  # sample isrcs
        ]

        dependencies = retention_manager._check_video_dependencies(mock_connection, "test_video")

        assert len(dependencies) == 3

        metrics_dep = next(d for d in dependencies if d.table_name == "youtube_metrics")
        assert metrics_dep.count == 5
        assert len(metrics_dep.sample_ids) == 2

        comments_dep = next(d for d in dependencies if d.table_name == "youtube_comments")
        assert comments_dep.count == 3

        isrc_dep = next(d for d in dependencies if d.table_name == "video_recording_link")
        assert isrc_dep.count == 1
