# Implementation Plan

- [x] 1. Fix youtube_metrics schema alignment
  - Update Python ETL code to use actual youtube_metrics columns from yt_proj.sql
  - Remove references to non-existent columns like `isrc` and `favorite_count`
  - Fix upsert operations to use correct primary key (video_id, metrics_date)
  - Update all metrics processing to use `metrics_date` and `fetched_at` columns
  - Write unit tests to validate metrics operations against actual schema
  - _Requirements: 1.1, 1.2, 1.3, 1.5_

- [x] 2. Fix analytics queries to use existing tables
  - Replace all references to non-existent `songs` table with `isrc_recordings`
  - Update queries to use `video_recording_link` for video-to-ISRC relationships
  - Fix artist performance queries to use `artist_primary` from `isrc_recordings`
  - Update all analytics functions to reference actual database tables
  - Add query validation to prevent future table reference errors
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [ ] 3. Implement SQLAlchemy-based sentiment analyzer
  - Create `web/sentiment_analyzer.py` with SentimentAnalyzer class using SQLAlchemy engine
  - Replace pymysql direct connections in `web/sentiment_job.py` with SQLAlchemy engine
  - Implement batch processing with configurable batch sizes (default 500 records)
  - Add connection pooling with `pool_pre_ping=True` and `pool_recycle=180`
  - Ensure sentiment_score values are clamped to [-1.0, 1.0] for DECIMAL(3,2) compatibility
  - Add progress tracking and timeout handling for large dataset processing
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 4. Create safe data retention manager
  - Create `web/data_retention_manager.py` with DataRetentionManager class
  - Implement dependency checking to prevent deletion of videos with associated metrics/comments/ISRC links
  - Add `check_deletion_safety()` method that returns detailed dependency reports
  - Implement `delete_expired_videos()` with dry-run mode and explicit confirmation
  - Use consistent UTC timestamps and respect `YOUTUBE_DATA_RETENTION_DAYS` environment variable
  - Add comprehensive logging of all deletion operations with counts and reasoning
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 5. Improve code quality and maintainability
  - Refactor `web/sentiment_analyzer.py` and `web/data_retention_manager.py` to stay under 200 lines
  - Extract constants like `DEFAULT_BATCH_SIZE = 500`, `CONNECTION_TIMEOUT = 180` to module top
  - Add comprehensive error handling with specific error types (DatabaseOperationError, DataQualityError)
  - Create `tests/test_sentiment_analyzer.py` and `tests/test_data_retention_manager.py` with 10-15 tests each
  - Ensure all functions stay under 25 lines with clear, descriptive names
  - Add concise logging that shows progress without excessive stack traces
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 6. Implement robust validation and monitoring
  - Create `web/schema_validator.py` with SchemaValidator class for runtime schema validation
  - Implement `validate_table_columns()` method using SQLAlchemy reflection
  - Add `detect_schema_drift()` method that compares expected vs actual database schema
  - Create data validation decorators that validate input types before database operations
  - Implement referential integrity checks for video_id foreign key relationships
  - Add schema validation to ETL startup process with clear error messages for mismatches
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 7. Optimize database queries and operations
  - Update analytics queries in `src/youtubeviz/data.py` to use window functions for ranking and aggregation
  - Implement configurable batch sizes in sentiment analyzer (environment variable `SENTIMENT_BATCH_SIZE`)
  - Add query performance monitoring to log slow queries (>5 seconds) with execution plans
  - Create indexes on frequently queried columns: `youtube_comments(sentiment_score)`, `youtube_metrics(metrics_date)`
  - Add timeout handling to all database operations with configurable timeouts
  - Implement progress tracking for batch operations that process >1000 records
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

- [ ] 8. Implement compliance and data handling
  - Integrate DataRetentionManager with existing ETL pipeline for automatic cleanup
  - Add data anonymization for comment text in retention reports (show only first 50 characters)
  - Implement cascading deletion across related tables (youtube_metrics, youtube_comments, video_recording_link)
  - Create `tools/migration/apply_retention_policy.py` script for updating existing data retention policies
  - Add compliance reporting with data age statistics and cleanup audit logs
  - Ensure all retention operations respect `YOUTUBE_DATA_RETENTION_DAYS` from environment
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

- [ ] 9. Remove deprecated schema elements
  - Create migration script to safely drop the deprecated `songs` table after verifying no dependencies
  - Update schema validation to ensure no code references the deprecated table
  - Add warning logs if deprecated table references are detected during runtime
  - Document the migration from `songs` table to ISRC-based schema in migration notes
  - _Requirements: 2.4, 6.3_
