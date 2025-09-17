# Requirements Document

## Introduction

This consolidated feature combines critical elements from multiple specs to create a working YouTube analytics platform as quickly as possible. The focus is on getting the database populated, notebooks executing properly, and the system operational for git commit. This consolidates bulletproof ETL, database schema fixes, CI setup, and storytelling notebooks into one streamlined implementation plan.

## Requirements

### Requirement 1

**User Story:** As a developer, I want the database tables populated with real data from the ETL pipeline, so that notebooks can execute successfully with actual results.

#### Acceptance Criteria

1. WHEN the ETL pipeline runs THEN it SHALL successfully populate youtube_videos, youtube_metrics, and youtube_comments tables with real data
2. WHEN inserting data THEN the system SHALL use the correct schema from yt_proj.sql without column mismatch errors
3. WHEN processing sentiment analysis THEN it SHALL populate comment_sentiment and youtube_sentiment_summary tables
4. WHEN linking ISRC data THEN it SHALL use the video_recording_link table with proper foreign key relationships
5. WHEN ETL completes THEN the database SHALL contain sufficient data for meaningful notebook analysis

### Requirement 2

**User Story:** As a data analyst, I want notebooks that execute successfully and produce compelling storytelling analysis, so that I can demonstrate the platform's capabilities.

#### Acceptance Criteria

1. WHEN executing notebooks THEN they SHALL load real data from the database and produce actual analysis results
2. WHEN generating visualizations THEN they SHALL create beautiful, interactive Plotly charts with consistent artist colors
3. WHEN presenting analysis THEN notebooks SHALL tell compelling stories about artist performance and music industry insights
4. WHEN explaining concepts THEN they SHALL include educational content suitable for data science students
5. WHEN notebooks complete THEN they SHALL be ready for portfolio presentation with professional quality outputs

### Requirement 3

**User Story:** As a system operator, I want bulletproof error handling and data quality validation, so that the system runs reliably without silent failures.

#### Acceptance Criteria

1. WHEN any component fails THEN the system SHALL log detailed error information and fail loudly with clear messages
2. WHEN processing data THEN the system SHALL validate data quality and integrity at each stage
3. WHEN database operations occur THEN they SHALL use proper connection pooling, timeouts, and retry logic
4. WHEN sentiment analysis runs THEN it SHALL handle batches efficiently with progress tracking
5. WHEN data quality issues are detected THEN the system SHALL provide actionable recommendations for resolution

### Requirement 4

**User Story:** As a developer preparing for git commit, I want a working CI system that validates code quality and system functionality, so that I can confidently push to GitHub.

#### Acceptance Criteria

1. WHEN running `make ci` THEN the system SHALL execute comprehensive quality checks including formatting, linting, and type checking
2. WHEN CI runs THEN it SHALL validate that notebooks execute without errors and produce expected outputs
3. WHEN testing the system THEN it SHALL verify database connectivity and data integrity
4. WHEN code quality checks run THEN they SHALL enforce 120-character line limits and coding standards
5. WHEN CI passes THEN the system SHALL be ready for git commit with confidence

### Requirement 5

**User Story:** As a music industry analyst, I want artist comparison and performance analysis, so that I can make informed investment and marketing decisions.

#### Acceptance Criteria

1. WHEN analyzing artists THEN the system SHALL provide comprehensive performance metrics across views, engagement, and sentiment
2. WHEN comparing artists THEN it SHALL identify momentum trends and growth opportunities
3. WHEN showing recommendations THEN it SHALL provide specific, actionable insights for budget allocation
4. WHEN displaying results THEN visualizations SHALL be emotionally engaging and professionally presented
5. WHEN explaining analysis THEN it SHALL connect data insights to real music business implications

### Requirement 6

**User Story:** As a system maintainer, I want proper database schema alignment and efficient operations, so that all components work together seamlessly.

#### Acceptance Criteria

1. WHEN ETL operations run THEN they SHALL use the exact column names and data types from yt_proj.sql
2. WHEN performing upserts THEN the system SHALL use correct primary keys (video_id, metrics_date) for conflict resolution
3. WHEN querying data THEN all SQL SHALL reference tables that actually exist in the database schema
4. WHEN processing large datasets THEN operations SHALL use appropriate batch sizes and indexing for performance
5. WHEN handling data retention THEN the system SHALL respect YouTube API compliance and configured retention policies

### Requirement 7

**User Story:** As a portfolio developer, I want the repository to demonstrate professional development practices with strict code quality standards, so that it showcases senior-level engineering capabilities.

#### Acceptance Criteria

1. WHEN writing database modules THEN they SHALL be limited to 200 lines with functions under 25 lines 8x out of 10 each (maximum 35 LOC in any .py files, notebooks)
2. WHEN examining database operations THEN all SQL queries SHALL be human-readable with proper formatting, line breaks, and clear variable naming
3. WHEN reviewing any code file THEN it SHALL be extensively commented explaining complex logic, business context, and potential pitfalls
4. WHEN running tests THEN they SHALL achieve comprehensive coverage with 10-15 unit tests per new database functionality
5. WHEN preparing for public release THEN there SHALL be NO bulky AI-generated code that appears amateurish or unnecessarily verbose

### Requirement 8

**User Story:** As a user working with AI agents, I want the system to provide clear status information and validation reports, so that AI can effectively help with development and troubleshooting.

#### Acceptance Criteria

1. WHEN the system runs THEN it SHALL generate comprehensive status reports with metrics and health indicators
2. WHEN errors occur THEN they SHALL be logged with sufficient context for AI agents to provide helpful suggestions
3. WHEN data quality checks run THEN they SHALL produce detailed reports on data integrity and completeness
4. WHEN notebooks execute THEN they SHALL validate outputs and provide feedback on analysis quality
5. WHEN CI completes THEN it SHALL generate validation reports that AI agents can analyze for system health assessment
