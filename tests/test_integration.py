#!/usr / bin / env python3
"""
Integration Tests for ETL Pipeline

This module provides integration tests that verify:
- End - to - end ETL pipeline functionality
- Database operations and transactions
- Component integration and data flow
- Real - world scenarios and edge cases
"""

from datetime import datetime, timedelta
import os
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import text

from tests.conftest import (
    assert_comment_in_database,
    assert_video_in_database,
    get_table_count,
    insert_test_comment,
    insert_test_video,
)
from web.models import YouTubeComment, YouTubeVideo
from web.validation import get_data_validator
from web.video_filter import filter_videos_at_api_level


class TestETLPipelineIntegration:
    """Test complete ETL pipeline integration."""

    def test_complete_video_processing_pipeline(self, test_engine, test_data_factory):
        """Test complete video processing from API to database."""
        # Step 1: Simulate API response
        api_videos_data = [
            {
                "video_id": "dQw4w9WgXcQ",
                "title": "Valid Video 1",
                "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
                "channel_title": "Test Channel 1",
                "published_at": datetime.now() - timedelta(days=1),
                "duration": "PT3M33S",
                "view_count": 1000,
                "like_count": 100,
                "comment_count": 50,
            },
            {
                "video_id": "oHg5SJYRHA0",
                "title": "Valid Video 2",
                "channel_id": "UC - 9-kyTW8ZkZNDHQJ6FgpwQ",
                "channel_title": "Test Channel 2",
                "published_at": datetime.now() - timedelta(days=2),
                "duration": "PT4M15S",
                "view_count": 2000,
                "like_count": 200,
                "comment_count": 100,
            },
            {
                "video_id": "jNQXAC9IVRw",
                "title": "Short Video - Will be filtered",
                "channel_id": "UCsT0YIqwnpJCM - mx7 - gSA4Q",
                "channel_title": "Test Channel 3",
                "published_at": datetime.now() - timedelta(days=3),
                "duration": "PT15S",  # Too short - will be filtered
                "view_count": 500,
                "like_count": 50,
                "comment_count": 25,
            },
        ]

        # Step 2: Validate video data
        validator = get_data_validator()
        validated_videos = []

        for video_data in api_videos_data:
            try:
                video = validator.validate_youtube_video(video_data)
                validated_videos.append(video)
            except Exception as e:
                print(f"Validation failed for video {video_data.get('video_id')}: {e}")

        assert len(validated_videos) == 3  # All should validate successfully

        # Step 3: Apply video filtering
        passed_videos, filter_results = filter_videos_at_api_level(validated_videos)

        # Should filter out the short video
        assert len(passed_videos) == 2
        assert len([r for r in filter_results if r.is_filtered]) == 1

        # Step 4: Insert passed videos into database
        for video in passed_videos:
            insert_test_video(test_engine, video)

        # Step 5: Verify database state
        assert get_table_count(test_engine, "youtube_videos") == 2
        assert assert_video_in_database(test_engine, "dQw4w9WgXcQ")
        assert assert_video_in_database(test_engine, "oHg5SJYRHA0")

        # Filtered video should not be in database
        with test_engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM youtube_videos WHERE video_id = :video_id"), {"video_id": "jNQXAC9IVRw"}
            )
            assert result.scalar() == 0

    def test_comment_processing_pipeline(self, test_engine, test_data_factory):
        """Test complete comment processing pipeline."""
        # Step 1: Insert a test video first
        video = test_data_factory.create_youtube_video()
        insert_test_video(test_engine, video)

        # Step 2: Create test comments
        comments_data = [
            {
                "comment_id": "comment_1",
                "video_id": video.video_id,
                "author_name": "Happy User",
                "comment_text": "This is amazing! Love it! 🔥",
                "like_count": 10,
                "published_at": datetime.now() - timedelta(hours=1),
            },
            {
                "comment_id": "comment_2",
                "video_id": video.video_id,
                "author_name": "Critical User",
                "comment_text": "Not my favorite, but okay",
                "like_count": 2,
                "published_at": datetime.now() - timedelta(hours=2),
            },
            {
                "comment_id": "comment_3",
                "video_id": video.video_id,
                "author_name": "Enthusiastic Fan",
                "comment_text": "Best song ever! Playing on repeat!",
                "like_count": 25,
                "published_at": datetime.now() - timedelta(hours=3),
            },
        ]

        # Step 3: Validate and insert comments
        validator = get_data_validator()

        for comment_data in comments_data:
            comment = validator.validate_youtube_comment(comment_data)
            insert_test_comment(test_engine, comment)

        # Step 4: Verify comments in database
        assert get_table_count(test_engine, "youtube_comments") == 3

        for comment_data in comments_data:
            assert assert_comment_in_database(test_engine, comment_data["comment_id"])

        # Step 5: Verify comment - video relationship
        with test_engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM youtube_comments
                    WHERE video_id = :video_id
                """
                ),
                {"video_id": video.video_id},
            )
            assert result.scalar() == 3

    def test_sentiment_analysis_integration(self, test_engine, test_data_factory):
        """Test sentiment analysis integration with database."""
        # Step 1: Set up test data
        video = test_data_factory.create_youtube_video()
        insert_test_video(test_engine, video)

        comments = test_data_factory.create_test_comments_batch(video.video_id, 5)
        for comment in comments:
            insert_test_comment(test_engine, comment)

        # Step 2: Simulate sentiment analysis processing
        sentiment_results = []

        with test_engine.connect() as conn:
            # Get comments for processing
            result = conn.execute(text("SELECT comment_id, comment_text FROM youtube_comments"))

            for row in result:
                # Simulate sentiment analysis (simple rule - based for testing)
                text_lower = row.comment_text.lower()
                if any(word in text_lower for word in ["great", "love", "amazing", "good"]):
                    sentiment_score = 0.8
                    confidence = 0.9
                elif any(word in text_lower for word in ["bad", "hate", "terrible"]):
                    sentiment_score = -0.6
                    confidence = 0.8
                else:
                    sentiment_score = 0.1
                    confidence = 0.6

                # Insert sentiment result
                conn.execute(
                    text(
                        """
                        INSERT INTO comment_sentiment
                        (comment_id, video_id, comment_text, sentiment_score, confidence_score)
                        VALUES (:comment_id, :video_id, :text, :sentiment, :confidence)
                    """
                    ),
                    {
                        "comment_id": row.comment_id,
                        "video_id": video.video_id,
                        "text": row.comment_text,
                        "sentiment": sentiment_score,
                        "confidence": confidence,
                    },
                )

            conn.commit()

        # Step 3: Verify sentiment analysis results
        assert get_table_count(test_engine, "comment_sentiment") == 5

        # Verify sentiment scores are within valid range
        with test_engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT sentiment_score, confidence_score
                    FROM comment_sentiment
                """
                )
            )

            for row in result:
                assert -1.0 <= row.sentiment_score <= 1.0
                assert 0.0 <= row.confidence_score <= 1.0


class TestDatabaseTransactionIntegration:
    """Test database transaction handling and rollback scenarios."""

    def test_transaction_rollback_on_error(self, test_engine, test_data_factory):
        """Test transaction rollback when errors occur."""
        video = test_data_factory.create_youtube_video()

        try:
            with test_engine.connect() as conn:
                # Start transaction
                trans = conn.begin()

                try:
                    # Insert valid video
                    conn.execute(
                        text(
                            """
                        INSERT INTO youtube_videos
                        (video_id,
                            title,
                            channel_id,
                            channel_title,
                            published_at,
                            view_count,
                            like_count,
                            comment_count)
                        VALUES (:video_id,
                            :title,
                            :channel_id,
                            :channel_title,
                            :published_at,
                            :view_count,
                            :like_count,
                            :comment_count)
                    """
                        ),
                        {
                            "video_id": video.video_id,
                            "title": video.title,
                            "channel_id": video.channel_id,
                            "channel_title": video.channel_title,
                            "published_at": video.published_at,
                            "view_count": video.view_count,
                            "like_count": video.like_count,
                            "comment_count": video.comment_count,
                        },
                    )

                    # Attempt to insert duplicate (should fail)
                    conn.execute(
                        text(
                            """
                        INSERT INTO youtube_videos
                        (video_id, title, channel_id, channel_title, published_at, view_count, like_count, comment_count)
                        VALUES (:video_id, :title, :channel_id, :channel_title, :published_at, :view_count, :like_count, :comment_count)
                    """
                        ),
                        {
                            "video_id": video.video_id,  # Same video_id - should cause constraint violation
                            "title": "Duplicate Video",
                            "channel_id": video.channel_id,
                            "channel_title": video.channel_title,
                            "published_at": video.published_at,
                            "view_count": 500,
                            "like_count": 50,
                            "comment_count": 25,
                        },
                    )

                    trans.commit()

                except Exception:
                    trans.rollback()
                    raise

        except Exception:
            # Expected to fail due to duplicate key
            pass

        # Verify no videos were inserted due to rollback
        assert get_table_count(test_engine, "youtube_videos") == 0

    def test_partial_batch_processing_with_errors(self, test_engine, test_data_factory):
        """Test handling of partial batch processing when some items fail."""
        videos = test_data_factory.create_test_videos_batch(3)

        successful_inserts = 0
        failed_inserts = 0

        for video in videos:
            try:
                with test_engine.connect() as conn:
                    conn.execute(
                        text(
                            """
                        INSERT INTO youtube_videos
                        (video_id, title, channel_id, channel_title, published_at, view_count, like_count, comment_count)
                        VALUES (:video_id, :title, :channel_id, :channel_title, :published_at, :view_count, :like_count, :comment_count)
                    """
                        ),
                        {
                            "video_id": video.video_id,
                            "title": video.title,
                            "channel_id": video.channel_id,
                            "channel_title": video.channel_title,
                            "published_at": video.published_at,
                            "view_count": video.view_count,
                            "like_count": video.like_count,
                            "comment_count": video.comment_count,
                        },
                    )
                    conn.commit()
                    successful_inserts += 1

            except Exception as e:
                failed_inserts += 1
                print(f"Failed to insert video {video.video_id}: {e}")

        # All should succeed in this case
        assert successful_inserts == 3
        assert failed_inserts == 0
        assert get_table_count(test_engine, "youtube_videos") == 3


class TestDataConsistencyIntegration:
    """Test data consistency across related tables."""

    def test_video_comment_relationship_consistency(self, test_engine, test_data_factory):
        """Test consistency between videos and comments tables."""
        # Step 1: Insert videos
        videos = test_data_factory.create_test_videos_batch(2)
        for video in videos:
            insert_test_video(test_engine, video)

        # Step 2: Insert comments for videos
        all_comments = []
        for video in videos:
            comments = test_data_factory.create_test_comments_batch(video.video_id, 3)
            all_comments.extend(comments)
            for comment in comments:
                insert_test_comment(test_engine, comment)

        # Step 3: Verify relationship consistency
        with test_engine.connect() as conn:
            # Check that all comments reference existing videos
            result = conn.execute(
                text(
                    """
                SELECT COUNT(*) FROM youtube_comments c
                LEFT JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE v.video_id IS NULL
            """
                )
            )
            orphaned_comments = result.scalar()
            assert orphaned_comments == 0

            # Check comment counts match
            for video in videos:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM youtube_comments WHERE video_id = :video_id"),
                    {"video_id": video.video_id},
                )
                comment_count = result.scalar()
                assert comment_count == 3  # We inserted 3 comments per video

    def test_sentiment_comment_relationship_consistency(self, test_engine, test_data_factory):
        """Test consistency between comments and sentiment tables."""
        # Step 1: Set up video and comments
        video = test_data_factory.create_youtube_video()
        insert_test_video(test_engine, video)

        comments = test_data_factory.create_test_comments_batch(video.video_id, 3)
        for comment in comments:
            insert_test_comment(test_engine, comment)

        # Step 2: Insert sentiment data for comments
        with test_engine.connect() as conn:
            for comment in comments:
                conn.execute(
                    text(
                        """
                    INSERT INTO comment_sentiment
                    (comment_id, video_id, comment_text, sentiment_score, confidence_score)
                    VALUES (:comment_id, :video_id, :text, :sentiment, :confidence)
                """
                    ),
                    {
                        "comment_id": comment.comment_id,
                        "video_id": comment.video_id,
                        "text": comment.comment_text,
                        "sentiment": 0.5,
                        "confidence": 0.8,
                    },
                )
            conn.commit()

        # Step 3: Verify relationship consistency
        with test_engine.connect() as conn:
            # Check that all sentiment records reference existing comments
            result = conn.execute(
                text(
                    """
                SELECT COUNT(*) FROM comment_sentiment cs
                LEFT JOIN youtube_comments c ON cs.comment_id = c.comment_id
                WHERE c.comment_id IS NULL
            """
                )
            )
            orphaned_sentiment = result.scalar()
            assert orphaned_sentiment == 0

            # Check sentiment coverage
            result = conn.execute(
                text(
                    """
                SELECT
                    (SELECT COUNT(*) FROM youtube_comments) as total_comments,
                    (SELECT COUNT(*) FROM comment_sentiment) as total_sentiment
            """
                )
            )
            row = result.fetchone()
            assert row.total_comments == row.total_sentiment


class TestPerformanceIntegration:
    """Test performance aspects of integrated operations."""

    def test_bulk_insert_performance(self, test_engine, test_data_factory):
        """Test performance of bulk insert operations."""
        # Create larger dataset
        videos = test_data_factory.create_test_videos_batch(10)

        start_time = datetime.now()

        # Bulk insert using transaction
        with test_engine.connect() as conn:
            trans = conn.begin()
            try:
                for video in videos:
                    conn.execute(
                        text(
                            """
                        INSERT INTO youtube_videos
                        (video_id, title, channel_id, channel_title, published_at, view_count, like_count, comment_count)
                        VALUES (:video_id, :title, :channel_id, :channel_title, :published_at, :view_count, :like_count, :comment_count)
                    """
                        ),
                        {
                            "video_id": video.video_id,
                            "title": video.title,
                            "channel_id": video.channel_id,
                            "channel_title": video.channel_title,
                            "published_at": video.published_at,
                            "view_count": video.view_count,
                            "like_count": video.like_count,
                            "comment_count": video.comment_count,
                        },
                    )
                trans.commit()
            except Exception:
                trans.rollback()
                raise

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        # Verify all videos were inserted
        assert get_table_count(test_engine, "youtube_videos") == 10

        # Performance should be reasonable (less than 2 seconds for 10 videos)
        assert processing_time < 2.0

        print(f"Bulk insert of {len(videos)} videos took {processing_time:.3f} seconds")

    def test_query_performance_with_joins(self, test_engine, test_data_factory):
        """Test query performance with table joins."""
        # Set up test data
        videos = test_data_factory.create_test_videos_batch(5)
        for video in videos:
            insert_test_video(test_engine, video)

            # Add comments for each video
            comments = test_data_factory.create_test_comments_batch(video.video_id, 5)
            for comment in comments:
                insert_test_comment(test_engine, comment)

        start_time = datetime.now()

        # Execute complex query with joins
        with test_engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT
                    v.video_id,
                    v.title,
                    v.view_count,
                    COUNT(c.comment_id) as comment_count,
                    AVG(CAST(c.like_count AS FLOAT)) as avg_comment_likes
                FROM youtube_videos v
                LEFT JOIN youtube_comments c ON v.video_id = c.video_id
                GROUP BY v.video_id, v.title, v.view_count
                ORDER BY v.view_count DESC
            """
                )
            )

            results = result.fetchall()

        end_time = datetime.now()
        query_time = (end_time - start_time).total_seconds()

        # Verify query results
        assert len(results) == 5  # Should return all 5 videos

        for row in results:
            assert row.comment_count == 5  # Each video has 5 comments
            assert row.avg_comment_likes > 0  # Should have positive average

        # Query should be fast (less than 1 second)
        assert query_time < 1.0

        print(f"Complex join query took {query_time:.3f} seconds")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
