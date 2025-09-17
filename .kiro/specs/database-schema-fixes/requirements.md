# Requirements Document

## Introduction

This feature addresses critical database schema mismatches and ETL alignment issues that are causing bugs in the YouTube analytics platform. The current system has schema inconsistencies between the database DDL and Python code, queries referencing non-existent tables, and inefficient data processing patterns. These foundational issues must be resolved to ensure data integrity and system reliability.

## Requirements

### Requirement 1

**User Story:** As a data engineer, I want the Python ETL code to match the actual database schema, so that data insertion and updates work correctly without runtime errors.

#### Acceptance Criteria

1. WHEN the ETL runs youtube_metrics upsert THEN it SHALL use columns that exist in the actual database table (video_id, metrics_date, fetched_at, view_count, like_count, dislike_count, comment_count, subscriber_count)
2. WHEN inserting metrics data THEN the system SHALL NOT reference non-existent columns like isrc or favorite_count
3. WHEN performing upserts THEN the system SHALL use the correct primary key (video_id, metrics_date) for conflict resolution
4. IF the schema changes THEN the Python models SHALL be automatically validated against the actual database schema
5. WHEN running ETL operations THEN all database operations SHALL succeed without column mismatch errors

### Requirement 2

**User Story:** As a data analyst, I want SQL queries to reference tables that actually exist in the database, so that analytics queries execute successfully.

#### Acceptance Criteria

1. WHEN running viewcount analysis queries THEN they SHALL use isrc_recordings and isrc_artists tables instead of non-existent songs table
2. WHEN joining video data with recording metadata THEN queries SHALL use the video_recording_link table for proper relationships
3. WHEN analyzing artist performance THEN queries SHALL reference artist_primary from isrc_recordings table
4. IF queries need song metadata THEN they SHALL use the proper ISRC-based schema with foreign key relationships
5. WHEN executing analytics functions THEN all table references SHALL be validated against the actual database schema

### Requirement 3

**User Story:** As a system administrator, I want efficient and safe sentiment analysis processing, so that comment sentiment is processed reliably without performance issues.

#### Acceptance Criteria

1. WHEN processing sentiment analysis THEN the system SHALL use SQLAlchemy engine with proper connection pooling and timeouts
2. WHEN updating sentiment scores THEN the system SHALL use batch operations with explicit UTC timestamps
3. WHEN storing sentiment data THEN the system SHALL respect the DECIMAL(3,2) precision limits for sentiment_score
4. IF sentiment processing fails THEN the system SHALL log clear error messages and continue with remaining batches
5. WHEN generating sentiment summaries THEN the system SHALL use efficient aggregation queries with proper indexing

### Requirement 4

**User Story:** As a database administrator, I want safe data retention and cleanup operations, so that data deletion is controlled and doesn't accidentally remove valid records.

#### Acceptance Criteria

1. WHEN deleting old youtube_videos records THEN the system SHALL only delete videos with no associated metrics, comments, or ISRC links
2. WHEN applying data retention policies THEN the system SHALL use consistent UTC timestamps across all tables
3. WHEN performing cleanup operations THEN the system SHALL provide clear warnings and require explicit confirmation
4. IF cleanup would affect linked data THEN the system SHALL prevent deletion and report the dependencies
5. WHEN retention policies run THEN they SHALL log all deletion operations with counts and reasoning

### Requirement 5

**User Story:** As a developer, I want improved code quality and maintainability in database operations, so that the codebase is easier to understand and modify.

#### Acceptance Criteria

1. WHEN writing database modules THEN they SHALL be limited to 200 lines with functions under 25 lines 8x out of 10 each (maxium 35 LOC in any .py files, notebooks)
2. WHEN implementing database operations THEN they SHALL use clear, descriptive function names and comprehensive error handling
3. WHEN adding new database functionality THEN it SHALL include 10-15 unit tests covering normal and error cases
4. IF database operations use magic numbers THEN they SHALL be extracted to named constants at module top
5. WHEN logging database operations THEN logs SHALL be concise and informative without excessive stack traces

### Requirement 6

**User Story:** As a data quality engineer, I want robust validation and monitoring for database operations, so that data integrity issues are caught early.

#### Acceptance Criteria

1. WHEN database tables are created THEN they SHALL include proper constraints, indexes, and foreign key relationships
2. WHEN inserting data THEN the system SHALL validate data types and constraints before database operations
3. WHEN detecting schema drift THEN the system SHALL alert administrators and provide migration recommendations
4. IF data validation fails THEN the system SHALL fail loudly with specific error details and suggested fixes
5. WHEN running data quality checks THEN they SHALL verify referential integrity across all linked tables

### Requirement 7

**User Story:** As a performance engineer, I want optimized database queries and operations, so that the system scales efficiently with large datasets.

#### Acceptance Criteria

1. WHEN executing analytics queries THEN they SHALL use proper window functions and CTEs for efficient data processing
2. WHEN performing batch operations THEN they SHALL use appropriate batch sizes to balance memory usage and performance
3. WHEN accessing frequently queried data THEN the system SHALL leverage database indexes for optimal performance
4. IF queries are slow THEN the system SHALL provide query execution plans and optimization suggestions
5. WHEN processing large datasets THEN operations SHALL include progress tracking and timeout handling

### Requirement 8

**User Story:** As a compliance officer, I want proper data handling and retention policies, so that the system meets YouTube API terms of service and data protection requirements.

#### Acceptance Criteria

1. WHEN storing YouTube data THEN the system SHALL respect configured retention periods and automatic cleanup schedules
2. WHEN handling user-generated content THEN the system SHALL implement proper privacy protections and data anonymization
3. WHEN deleting expired data THEN the system SHALL ensure complete removal across all related tables
4. IF retention policies change THEN the system SHALL provide migration tools to apply new policies to existing data
5. WHEN auditing data retention THEN the system SHALL provide clear reports on data age and cleanup status
