# Design Document

## Overview

This design addresses critical database schema mismatches and ETL alignment issues in the YouTube analytics platform. The solution focuses on fixing concrete bugs where Python code expects different database columns than what exists, queries reference non-existent tables, and inefficient processing patterns cause performance and reliability issues.

The design prioritizes minimal, surgical fixes that resolve immediate bugs while establishing patterns for maintainable database operations going forward.

## Architecture

### Database Schema Alignment Strategy

The core issue is a mismatch between the actual database schema (defined in `yt_proj.sql`) and the Python code expectations. The solution uses a "fix Python to match database" approach for immediate stability, with validation mechanisms to prevent future drift.

**Schema Validation Layer:**
- Automated schema introspection to validate Python models against actual database tables
- Runtime validation of column existence before executing queries
- Clear error messages when schema mismatches are detected

**Migration Strategy:**
- Fix Python code to match existing database schema (minimal disruption)
- Add validation to prevent future schema drift
- Establish patterns for schema evolution going forward

### Component Architecture

```
Database Layer (MySQL)
├── Core Tables (youtube_videos, youtube_metrics, youtube_comments)
├── ISRC Schema (isrc_recordings, isrc_artists, video_recording_link)
├── Analytics Tables (youtube_sentiment_summary, artist_performance_summary)
└── Operational Tables (youtube_etl_runs, operational_health_log)

ETL Processing Layer
├── Schema Validator (validates Python models against DB)
├── Metrics Processor (fixed to match actual youtube_metrics schema)
├── Sentiment Analyzer (SQLAlchemy-based with proper batching)
├── Query Builder (generates queries using actual table schema)
└── Data Retention Manager (safe deletion with dependency checking)

Validation & Monitoring
├── Schema Drift Detection
├── Data Quality Validation
├── Performance Monitoring
└── Compliance Tracking
```

## Components and Interfaces

### 1. Schema Validation Component

**Purpose:** Prevent runtime errors from schema mismatches

**Interface:**
```python
class SchemaValidator:
    def validate_table_columns(self, table_name: str, expected_columns: List[str]) -> ValidationResult
    def get_table_schema(self, table_name: str) -> TableSchema
    def validate_query_references(self, query: str) -> List[ValidationError]
```

**Implementation:**
- Uses SQLAlchemy reflection to inspect actual database schema
- Validates Python model fields against actual table columns
- Provides clear error messages for mismatches

### 2. Fixed Metrics Processor

**Purpose:** Align youtube_metrics operations with actual database schema

**Current Schema (from yt_proj.sql):**
```sql
youtube_metrics (
    video_id VARCHAR(50),
    view_count BIGINT,
    like_count BIGINT,
    dislike_count BIGINT,
    comment_count BIGINT,
    subscriber_count BIGINT,
    metrics_date DATE,
    fetched_at DATETIME,
    PRIMARY KEY (video_id, metrics_date)
)
```

**Fixed Interface:**
```python
class MetricsProcessor:
    def upsert_metrics(self, video_id: str, views: int, likes: int, comments: int) -> None
    def get_metrics_history(self, video_id: str, days: int) -> List[MetricsRecord]
```

**Key Changes:**
- Remove references to non-existent `isrc` and `favorite_count` columns
- Use `metrics_date` and `fetched_at` instead of single `fetch_datetime`
- Proper upsert logic using composite primary key (video_id, metrics_date)

### 3. Enhanced Sentiment Analyzer

**Purpose:** Efficient, reliable sentiment processing with proper database integration

**Interface:**
```python
class SentimentAnalyzer:
    def __init__(self, engine: Engine)
    def score_batch(self, limit: int = 500) -> SentimentStats
    def refresh_summary(self) -> int
    def snapshot_daily_sentiment(self) -> int
```

**Key Improvements:**
- SQLAlchemy engine with connection pooling and timeouts
- Batch processing with proper transaction management
- Explicit UTC timestamp handling
- Respect DECIMAL(3,2) precision limits for sentiment_score
- Clear error handling and progress tracking

### 4. Query Builder for Analytics

**Purpose:** Generate queries using actual database schema

**Interface:**
```python
class AnalyticsQueryBuilder:
    def build_viewcount_analysis(self) -> str
    def build_artist_performance_query(self) -> str
    def validate_query_tables(self, query: str) -> bool
```

**Schema Mapping:**
- Replace `songs` table references with `isrc_recordings`
- Use `video_recording_link` for video-to-ISRC relationships
- Leverage `isrc_artists` for artist role information
- Use proper window functions and CTEs for efficient processing

### 5. Safe Data Retention Manager

**Purpose:** Controlled data deletion with dependency checking

**Interface:**
```python
class DataRetentionManager:
    def __init__(self, engine: Engine, retention_days: int)
    def check_deletion_safety(self, video_ids: List[str]) -> DeletionReport
    def delete_expired_videos(self, dry_run: bool = True) -> DeletionResult
    def cleanup_orphaned_data(self) -> CleanupResult
```

**Safety Rules:**
- Only delete youtube_videos with no associated metrics, comments, or ISRC links
- Use consistent UTC timestamps across all retention operations
- Provide detailed deletion reports with dependency information
- Require explicit confirmation for destructive operations

## Data Models

### Fixed Metrics Model

```python
@dataclass
class MetricsRecord:
    video_id: str
    view_count: int
    like_count: int
    dislike_count: int = 0
    comment_count: int
    subscriber_count: int = 0
    metrics_date: date
    fetched_at: datetime
```

### Sentiment Processing Model

```python
@dataclass
class SentimentResult:
    comment_id: int
    sentiment_score: Decimal  # DECIMAL(3,2) compatible
    confidence: float
    processed_at: datetime
```

### Analytics Query Model

```python
@dataclass
class ViewcountAnalysis:
    video_id: str
    video_title: str
    isrc: Optional[str]
    recording_title: Optional[str]
    artist_primary: Optional[str]
    first_count: int
    last_count: int
    increase: int
    pct_increase: Decimal
```

## Error Handling

### Schema Validation Errors

**Strategy:** Fail fast with clear, actionable error messages

```python
class SchemaValidationError(Exception):
    def __init__(self, table: str, missing_columns: List[str], available_columns: List[str]):
        self.table = table
        self.missing_columns = missing_columns
        self.available_columns = available_columns
        super().__init__(f"Schema mismatch in {table}: missing {missing_columns}, available: {available_columns}")
```

### Database Operation Errors

**Strategy:** Comprehensive error context with recovery suggestions

```python
class DatabaseOperationError(Exception):
    def __init__(self, operation: str, table: str, error: Exception, context: Dict[str, Any]):
        self.operation = operation
        self.table = table
        self.original_error = error
        self.context = context
        super().__init__(f"Database {operation} failed on {table}: {error}")
```

### Data Quality Errors

**Strategy:** Categorized errors with severity levels and remediation steps

```python
class DataQualityError(Exception):
    def __init__(self, severity: str, category: str, message: str, remediation: str):
        self.severity = severity  # CRITICAL, WARNING, INFO
        self.category = category  # SCHEMA, INTEGRITY, PERFORMANCE
        self.remediation = remediation
        super().__init__(f"{severity} {category}: {message}. Remediation: {remediation}")
```

## Testing Strategy

### Schema Validation Testing

**Approach:** Test against actual database schema using reflection

```python
def test_schema_validation():
    # Test with actual database connection
    validator = SchemaValidator(engine)

    # Test valid schema
    result = validator.validate_table_columns("youtube_metrics",
        ["video_id", "view_count", "metrics_date"])
    assert result.is_valid

    # Test invalid schema
    result = validator.validate_table_columns("youtube_metrics",
        ["video_id", "nonexistent_column"])
    assert not result.is_valid
    assert "nonexistent_column" in result.missing_columns
```

### Database Operation Testing

**Approach:** Use test database with actual schema for integration testing

```python
def test_metrics_upsert():
    processor = MetricsProcessor(test_engine)

    # Test successful upsert
    processor.upsert_metrics("test_video", 1000, 50, 25)

    # Verify data was inserted correctly
    metrics = processor.get_metrics_history("test_video", 1)
    assert len(metrics) == 1
    assert metrics[0].view_count == 1000
```

### Query Validation Testing

**Approach:** Validate generated queries against actual database schema

```python
def test_analytics_queries():
    builder = AnalyticsQueryBuilder(test_engine)

    # Test query generation
    query = builder.build_viewcount_analysis()

    # Validate all referenced tables exist
    assert builder.validate_query_tables(query)

    # Test query execution
    result = test_engine.execute(text(query))
    assert result is not None
```

## Performance Considerations

### Database Connection Management
- Use SQLAlchemy connection pooling with `pool_pre_ping=True`
- Set appropriate `pool_recycle` timeout (180 seconds)
- Implement connection retry logic with exponential backoff

### Batch Processing Optimization
- Process sentiment analysis in batches of 500 records
- Use `executemany()` for bulk operations
- Commit transactions at batch boundaries, not per record

### Query Optimization
- Use window functions and CTEs for efficient analytics queries
- Leverage existing indexes on `video_id`, `metrics_date`, `published_at`
- Add new indexes for frequently queried columns (sentiment_score, isrc)

### Memory Management
- Stream large result sets instead of loading all into memory
- Use generators for processing large datasets
- Implement progress tracking for long-running operations

## Migration Plan

### Phase 1: Critical Bug Fixes (Immediate)
1. Fix youtube_metrics upsert to use correct columns
2. Update analytics queries to use existing tables
3. Add schema validation to prevent future mismatches

### Phase 2: Enhanced Processing (Week 1)
1. Implement SQLAlchemy-based sentiment analyzer
2. Add safe data retention manager
3. Create comprehensive error handling

### Phase 3: Quality & Performance (Week 2)
1. Add performance monitoring and optimization
2. Implement comprehensive testing suite
3. Add data quality validation framework

### Phase 4: Operational Excellence (Week 3)
1. Add operational monitoring and alerting
2. Implement automated schema drift detection
3. Create maintenance and cleanup automation
