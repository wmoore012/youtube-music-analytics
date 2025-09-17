# Implementation Plan

- [ ] 1. Fix youtube_metrics schema alignment
  - Update Python ETL code to use actual youtube_metrics columns from yt_proj.sql
  - Remove references to non-existent columns like `isrc` and `favorite_count`
  - Fix upsert operations to use correct primary key (video_id, metrics_date)
  - Update all metrics processing to use `metrics_date` and `fetched_at` columns
  - Write unit tests to validate metrics operations against actual schema
  - _Requirements: 1.1, 1.2, 1.3, 1.5_

- [ ] 2. Fix analytics queries to use existing tables
  - Replace all references to non-existent `songs` table with `isrc_recordings`
  - Update queries to use `video_recording_link` for video-to-ISRC relationships
  - Fix artist performance queries to use `artist_primary` from `isrc_recordings`
  - Update all analytics functions to reference actual database tables
  - Add query validation to prevent future table reference errors
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [ ] 3. Implement SQLAlchemy-based sentiment analyzer
  - Create SentimentAnalyzer class with proper SQLAlchemy engine integration
  - Implement batch processing with connection pooling and timeouts
  - Add explicit UTC timestamp handling for all sentiment operations
  - Ensure sentiment_score respects DECIMAL(3,2) precision limits
  - Add comprehensive error handling and progress tracking
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 4. Create safe data retention manager
  - Implement DataRetentionManager with dependency checking before deletion
  - Add safety checks to only delete videos with no associated data
  - Use consistent UTC timestamps across all retention operations
  - Provide detailed deletion reports with dependency information
  - Require explicit confirmation for all destructive operations
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 5. Improve code quality and maintainability
  - Refactor database modules to stay under 200 lines with functions under 25 lines
  - Add clear, descriptive function names and comprehensive error handling
  - Extract magic numbers to named constants at module top
  - Create concise, informative logging without excessive stack traces
  - Add 10-15 unit tests for each new database functionality
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 6. Implement robust validation and monitoring
  - Add proper constraints, indexes, and foreign key relationships to database operations
  - Validate data types and constraints before database operations
  - Create schema drift detection with administrator alerts
  - Implement data validation that fails loudly with specific error details
  - Add referential integrity verification across all linked tables
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 7. Optimize database queries and operations
  - Use proper window functions and CTEs for efficient analytics queries
  - Implement appropriate batch sizes for memory usage and performance balance
  - Leverage database indexes for optimal performance on frequently queried data
  - Add query execution plans and optimization suggestions for slow queries
  - Include progress tracking and timeout handling for large dataset processing
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

- [ ] 8. Implement compliance and data handling
  - Respect configured retention periods and automatic cleanup schedules
  - Add proper privacy protections and data anonymization for user content
  - Ensure complete removal across all related tables when deleting expired data
  - Create migration tools for applying new retention policies to existing data
  - Generate clear reports on data age and cleanup status for auditing
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_
