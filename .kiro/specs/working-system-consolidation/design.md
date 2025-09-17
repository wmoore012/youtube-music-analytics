# Design Document

## Overview

This design focuses on the essential tasks to get a working YouTube analytics system operational quickly. The priority is: populate database tables with real data → get notebooks executing with actual results → validate everything works → commit to git. This consolidates only the most critical elements needed for a functional system.

## Architecture

### Immediate Priority Flow
```
1. Fix Schema Mismatches → 2. Run ETL to Populate Tables → 3. Execute Notebooks with Real Data → 4. Basic CI Validation → 5. Git Commit
```

The design uses the existing `yt_proj.sql` schema as the source of truth and fixes Python code to match it. Focus is on getting real data flowing through the system rather than extensive refactoring.

### Core Components (Minimal Viable System)
- **Schema Alignment**: Fix Python ETL code to match actual database columns from `yt_proj.sql`
- **Data Population**: Run existing ETL pipeline to fill tables with real YouTube data
- **Working Notebooks**: Get storytelling notebooks executing with actual database results
- **Basic CI**: Simple validation that everything works before git operations
- **LOC Compliance**: Keep all code under the specified line limits (200 lines per module, 25 per function)

## Components and Interfaces

### 1. Schema Alignment Fixes (Critical)
**Purpose**: Fix immediate column mismatch errors between Python code and `yt_proj.sql`

**Specific Fixes Needed**:
- `youtube_metrics` table: Use actual columns (`video_id`, `metrics_date`, `view_count`, `like_count`, `dislike_count`, `comment_count`, `subscriber_count`, `fetched_at`)
- Remove references to non-existent columns like `isrc` or `favorite_count` in metrics operations
- Fix analytics queries to use `isrc_recordings` and `isrc_artists` instead of missing `songs` table
- Use `video_recording_link` for proper ISRC-video relationships

### 2. ETL Data Population (Essential)
**Purpose**: Get real data into the database tables using existing ETL pipeline

**Key Operations**:
- Run `tools/etl/run_focused_etl.py` to populate `youtube_videos`, `youtube_metrics`, `youtube_comments`
- Execute sentiment analysis to fill `comment_sentiment` and `youtube_sentiment_summary`
- Process ISRC linking to populate `video_recording_link` table
- Ensure sufficient data volume for meaningful notebook analysis

### 3. Working Notebooks (Priority)
**Purpose**: Get existing notebooks executing successfully with real data

**Focus Areas**:
- Fix `02_artist_comparison_storytelling.ipynb` to load actual data from database
- Ensure charts render with real data points and proper colors
- Maintain existing storytelling structure while fixing data loading issues
- Preserve all existing visualizations and analysis logic

### 4. Basic CI Validation (Minimal)
**Purpose**: Simple checks to ensure system works before git commit

**Essential Checks**:
- Code formatting (black, isort)
- Basic linting (flake8)
- Database connectivity test
- Notebook execution validation
- LOC limit enforcement (200 lines per module, 25 per function max)

## Data Models

### Database Schema (Use Existing `yt_proj.sql`)
The system will use the current schema exactly as defined:

**Primary Tables for Data Population**:
- `youtube_videos`: Video metadata (`video_id`, `title`, `channel_title`, `published_at`, `view_count`, `like_count`, `comment_count`)
- `youtube_metrics`: Time-series data (`video_id`, `metrics_date`, `view_count`, `like_count`, `dislike_count`, `comment_count`, `subscriber_count`, `fetched_at`)
- `youtube_comments`: Comment data (`video_id`, `comment_id`, `comment_text`, `author_name`, `sentiment_score`)
- `isrc_recordings`: Music metadata (`isrc`, `title`, `artist_primary`)
- `video_recording_link`: ISRC-video relationships (`video_id`, `isrc`, `match_method`, `confidence`)

### Python Code Alignment
Fix existing Python code to match actual database columns:

```python
# Fix youtube_metrics operations to use actual columns
def upsert_metrics(video_id, metrics_date, view_count, like_count,
                  dislike_count, comment_count, subscriber_count, fetched_at):
    # Use actual column names from yt_proj.sql
    pass

# Fix analytics queries to use existing tables
def get_artist_performance():
    # Use isrc_recordings instead of songs table
    # Use video_recording_link for relationships
    pass
```

## Error Handling

### Simple Error Strategy (Get It Working First)
1. **Fix Schema Errors**: Update Python code to match database schema exactly
2. **Log Clear Errors**: When something fails, show exactly what went wrong
3. **Fail Fast**: Don't continue with broken data - fix the root cause
4. **Keep It Simple**: Avoid complex error handling until basic functionality works

### Common Issues to Fix
- **Column Name Mismatches**: Python code expecting columns that don't exist in database
- **Table Name Errors**: Queries referencing non-existent tables like `songs`
- **Data Type Issues**: Ensure Python data types match database column types
- **Connection Problems**: Basic database connectivity and timeout handling

## Testing Strategy

### Minimal Testing (Get It Working First)
- **Basic Functionality**: Test that ETL populates tables without errors
- **Notebook Execution**: Verify notebooks run to completion with real data
- **Schema Validation**: Ensure Python code matches database schema
- **LOC Compliance**: Automated checks for line count limits

### Simple Test Approach
1. **Database Connection Test**: Verify we can connect and query tables
2. **ETL Smoke Test**: Run ETL and check that data appears in tables
3. **Notebook Execution Test**: Execute notebooks and verify they complete
4. **Code Quality Test**: Run black, isort, flake8 to ensure code standards

## Implementation Priority

### Phase 1: Critical Fixes (Tasks 1-5)
1. Fix schema mismatches between Python code and `yt_proj.sql`
2. Run ETL to populate database tables with real data
3. Fix notebook data loading to use actual database tables
4. Ensure notebooks execute successfully with real data
5. Basic CI validation for code quality and functionality

This focused approach prioritizes getting a working system over comprehensive features. Once the system works, additional enhancements can be added incrementally.
