# Development Standards - YouTube Analytics Platform

## 🎯 Overview

This document outlines the development standards and best practices for the YouTube Analytics Platform. These standards ensure code quality, maintainability, and consistency across the entire codebase.

## 📋 Table of Contents

1. [Naming Conventions](#naming-conventions)
2. [Helper Functions](#helper-functions)
3. [Error Handling](#error-handling)
4. [Data Management](#data-management)
5. [Code Organization](#code-organization)
6. [Examples and Templates](#examples-and-templates)

---

## 🏷️ Naming Conventions

### Variables and Functions: `snake_case`

```python
# ✅ CORRECT
user_count = 100
video_data = get_video_metrics()
sentiment_score = calculate_sentiment(comment_text)

def process_youtube_data(channel_id):
    return processed_data

# ❌ INCORRECT
userCount = 100  # camelCase
videoData = getVideoMetrics()  # camelCase
sentimentScore = calculateSentiment(commentText)  # camelCase

def processYouTubeData(channelId):  # camelCase
    return processedData
```

### Classes: `PascalCase`

```python
# ✅ CORRECT
class YouTubeChannelETL:
    pass

class SentimentAnalyzer:
    pass

class DataQualityValidator:
    pass

# ❌ INCORRECT
class youtube_channel_etl:  # snake_case
    pass

class sentimentAnalyzer:  # camelCase
    pass
```

### Constants: `UPPER_CASE`

```python
# ✅ CORRECT
MAX_RETRIES = 3
DEFAULT_TIMEOUT = 30
API_BASE_URL = "https://api.youtube.com"

# Private constants (acceptable)
_DEFAULT_CONFIG = {...}
_RETRY_DELAYS = [1, 2, 4, 8]

# ❌ INCORRECT
maxRetries = 3  # camelCase
default_timeout = 30  # snake_case for constants
```

### Database Columns: `lowercase_snake_case`

```sql
-- ✅ CORRECT
CREATE TABLE youtube_videos (
    video_id VARCHAR(50),
    channel_title VARCHAR(255),
    published_at DATETIME,
    view_count BIGINT
);

-- ❌ INCORRECT
CREATE TABLE YouTubeVideos (  -- PascalCase
    videoId VARCHAR(50),      -- camelCase
    channelTitle VARCHAR(255), -- camelCase
    publishedAt DATETIME      -- camelCase
);
```

---

## 🔧 Helper Functions

### Using Common Helpers

The `src/youtubeviz/common_helpers.py` module provides reusable functions to reduce code duplication:

```python
from src.youtubeviz.common_helpers import (
    execute_query_safely,
    validate_required_fields,
    format_number,
    clean_text_field,
    retry_operation
)

# ✅ CORRECT - Using helper functions
def get_video_stats(conn, video_id):
    query = "SELECT view_count, like_count FROM youtube_videos WHERE video_id = :video_id"
    result = execute_query_safely(conn, query, {"video_id": video_id})
    return result.fetchone()

def process_comment(comment_data):
    # Validate required fields
    missing = validate_required_fields(comment_data, ["comment_id", "comment_text"])
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    
    # Clean text
    clean_text = clean_text_field(comment_data["comment_text"], max_length=1000)
    
    return {
        "comment_id": comment_data["comment_id"],
        "clean_text": clean_text,
        "formatted_likes": format_number(comment_data.get("like_count", 0))
    }

# ❌ INCORRECT - Duplicating logic
def get_video_stats_bad(conn, video_id):
    try:
        result = conn.execute(text("SELECT view_count, like_count FROM youtube_videos WHERE video_id = :video_id"), {"video_id": video_id})
        return result.fetchone()
    except Exception as e:
        logging.error(f"Database query failed: {e}")
        raise

def process_comment_bad(comment_data):
    # Duplicated validation logic
    if "comment_id" not in comment_data or not comment_data["comment_id"]:
        raise ValueError("Missing comment_id")
    if "comment_text" not in comment_data or not comment_data["comment_text"]:
        raise ValueError("Missing comment_text")
    
    # Duplicated text cleaning logic
    text = comment_data["comment_text"].strip()
    if len(text) > 1000:
        text = text[:1000]
    
    # Duplicated number formatting
    likes = comment_data.get("like_count", 0)
    if likes >= 1000000:
        formatted_likes = f"{likes/1000000:.1f}M"
    elif likes >= 1000:
        formatted_likes = f"{likes/1000:.1f}K"
    else:
        formatted_likes = str(likes)
```

### Available Helper Categories

#### Database Operations
```python
# Safe query execution with error handling
result = execute_query_safely(conn, query, params)

# Get table row count
count = get_table_row_count(conn, "youtube_videos")

# Check if table exists
exists = check_table_exists(conn, "youtube_comments")

# Batch insert for performance
inserted = batch_insert_records(conn, "youtube_metrics", records, batch_size=1000)
```

#### Data Validation
```python
# Validate required fields
missing = validate_required_fields(data, ["video_id", "title"])

# Validate data types
errors = validate_data_types(data, {"view_count": int, "title": str})

# Clean text fields
clean_text = clean_text_field(raw_text, max_length=500)

# Validate YouTube IDs
is_valid = validate_youtube_id("dQw4w9WgXcQ", "video")
```

#### Error Handling
```python
# Retry operations with backoff
result = retry_operation(lambda: api_call(), max_retries=3)

# Safe division
percentage = safe_divide(views, total_views, default=0)

# Log errors with context
log_error_with_context(exception, {"video_id": video_id, "operation": "processing"})
```

#### Formatting
```python
# Format large numbers
formatted = format_number(1234567)  # "1.2M"

# Format duration
duration = format_duration(3661)  # "1h 1m 1s"

# Format percentage
percent = format_percentage(25, 100)  # "25.0%"

# Create progress bar
progress = create_progress_bar(75, 100)  # "[████████████████████████████████████████░░░░░░░░░░] 75.0%"
```

---

## 🛡️ Error Handling

### Fail-Loud Principles

Always handle errors explicitly and provide clear messages:

```python
# ✅ CORRECT - Fail-loud with specific exceptions
def process_video_data(video_id):
    try:
        video_data = fetch_video_from_api(video_id)
    except requests.HTTPError as e:
        logging.error(f"Failed to fetch video {video_id} from API: {e}")
        raise APIError(f"Video {video_id} could not be retrieved: {e}") from e
    except requests.Timeout as e:
        logging.error(f"Timeout fetching video {video_id}: {e}")
        raise TimeoutError(f"API timeout for video {video_id}") from e
    
    try:
        processed_data = transform_video_data(video_data)
    except ValueError as e:
        logging.error(f"Invalid video data for {video_id}: {e}")
        raise DataValidationError(f"Video {video_id} has invalid data: {e}") from e
    
    return processed_data

# ❌ INCORRECT - Silent failures
def process_video_data_bad(video_id):
    try:
        video_data = fetch_video_from_api(video_id)
        processed_data = transform_video_data(video_data)
        return processed_data
    except:
        pass  # Silent failure - never do this!
        return None
```

### Exception Handling Patterns

```python
# Database operations
try:
    conn.execute(query)
except pymysql.Error as e:
    logging.error(f"Database query failed: {query[:100]}... Error: {e}")
    raise DatabaseError(f"Query execution failed: {e}") from e

# API calls
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
except requests.RequestException as e:
    logging.error(f"API request failed: {url} - {e}")
    raise APIError(f"Request to {url} failed: {e}") from e

# File operations
try:
    with open(file_path, 'r') as f:
        data = f.read()
except FileNotFoundError:
    logging.error(f"File not found: {file_path}")
    raise FileNotFoundError(f"Required file missing: {file_path}")
except PermissionError:
    logging.error(f"Permission denied: {file_path}")
    raise PermissionError(f"Cannot access file: {file_path}")
```

---

## 📊 Data Management

### Boolean Fields vs Descriptive Values

Use descriptive values instead of booleans when the meaning could be unclear:

```python
# ✅ CORRECT - Descriptive values
video_status = "published"  # vs "draft", "private", "unlisted"
sentiment_category = "positive"  # vs "negative", "neutral"
processing_state = "completed"  # vs "pending", "failed", "in_progress"

# Database schema
CREATE TABLE youtube_videos (
    video_id VARCHAR(50),
    status ENUM('published', 'draft', 'private', 'unlisted'),
    processing_state ENUM('pending', 'in_progress', 'completed', 'failed')
);

# ❌ INCORRECT - Unclear booleans
is_published = True  # What about draft? private? unlisted?
is_positive = False  # Could be negative OR neutral
is_done = True  # Done with what? Success or failure?

# Database schema
CREATE TABLE youtube_videos (
    video_id VARCHAR(50),
    is_published BOOLEAN,  -- Unclear what false means
    is_positive BOOLEAN    -- Doesn't handle neutral sentiment
);
```

### Real Data Access Patterns

Always use real data from the database or API, not fake/mock data:

```python
# ✅ CORRECT - Real data access
def get_channel_analytics(channel_id):
    with get_connection() as conn:
        query = """
            SELECT 
                COUNT(*) as video_count,
                SUM(view_count) as total_views,
                AVG(sentiment_score) as avg_sentiment
            FROM youtube_videos v
            LEFT JOIN comment_sentiment cs ON v.video_id = cs.video_id
            WHERE v.channel_id = %s
        """
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, (channel_id,))
        return cursor.fetchone()

# ❌ INCORRECT - Fake data
def get_channel_analytics_fake(channel_id):
    return {
        "video_count": random.randint(10, 100),
        "total_views": random.randint(1000, 1000000),
        "avg_sentiment": random.uniform(-1, 1)
    }
```

---

## 🏗️ Code Organization

### Function Design Principles

1. **Single Responsibility**: Each function does one thing well
2. **Under 31 Lines**: Keep functions concise and focused
3. **Meaningful Names**: Function names should describe what they do
4. **Comprehensive Comments**: Explain the why, not just the what

```python
# ✅ CORRECT - Well-designed function
def calculate_engagement_rate(video_data: Dict[str, Any]) -> float:
    """
    Calculate engagement rate for a YouTube video.
    
    Engagement rate = (likes + comments) / views * 100
    
    Args:
        video_data: Dictionary containing video metrics
        
    Returns:
        Engagement rate as percentage (0-100)
        
    Raises:
        ValueError: If required metrics are missing or invalid
    """
    required_fields = ["view_count", "like_count", "comment_count"]
    missing = validate_required_fields(video_data, required_fields)
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    
    views = video_data["view_count"]
    likes = video_data["like_count"]
    comments = video_data["comment_count"]
    
    if views == 0:
        return 0.0
    
    engagement = (likes + comments) / views * 100
    return round(engagement, 2)

# ❌ INCORRECT - Poor function design
def calc(d):  # Unclear name, no documentation
    # No validation, no error handling
    return (d["like_count"] + d["comment_count"]) / d["view_count"] * 100
```

### Variable Naming

Use descriptive names that explain the purpose:

```python
# ✅ CORRECT - Meaningful names
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
max_comments_per_video = 100
sentiment_analysis_results = []
video_processing_start_time = datetime.now()

# Processing loop with clear variable names
for video_metadata in channel_video_list:
    video_id = video_metadata["id"]
    video_title = video_metadata["snippet"]["title"]
    published_date = parse_youtube_timestamp(video_metadata["snippet"]["publishedAt"])
    
    # Process each video...

# ❌ INCORRECT - Unclear names
key = os.getenv("YOUTUBE_API_KEY")  # What kind of key?
max_c = 100  # Max what?
results = []  # Results of what?
start = datetime.now()  # Start of what?

# Processing loop with unclear names
for item in data:
    id = item["id"]  # ID of what?
    title = item["snippet"]["title"]  # Title of what?
    date = parse_youtube_timestamp(item["snippet"]["publishedAt"])  # What date?
```

---

## 📚 Examples and Templates

### Complete Function Template

```python
def process_video_comments(video_id: str, max_comments: int = 100) -> Dict[str, Any]:
    """
    Process comments for a YouTube video and calculate sentiment metrics.
    
    This function fetches comments from the database, analyzes sentiment,
    and returns aggregated metrics for the video.
    
    Args:
        video_id: YouTube video ID (11 characters)
        max_comments: Maximum number of comments to process
        
    Returns:
        Dictionary containing:
        - comment_count: Total comments processed
        - avg_sentiment: Average sentiment score (-1 to 1)
        - sentiment_distribution: Count by sentiment category
        
    Raises:
        ValueError: If video_id is invalid
        DatabaseError: If database query fails
        
    Example:
        >>> metrics = process_video_comments("dQw4w9WgXcQ", max_comments=50)
        >>> print(metrics["avg_sentiment"])
        0.65
    """
    # Validate input
    if not validate_youtube_id(video_id, "video"):
        raise ValueError(f"Invalid YouTube video ID: {video_id}")
    
    # Fetch comments from database
    try:
        with get_connection() as conn:
            query = """
                SELECT comment_text, sentiment_score 
                FROM youtube_comments 
                WHERE video_id = %s 
                ORDER BY published_at DESC 
                LIMIT %s
            """
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, (video_id, max_comments))
            comments = cursor.fetchall()
            
    except Exception as e:
        log_error_with_context(e, {"video_id": video_id, "operation": "fetch_comments"})
        raise DatabaseError(f"Failed to fetch comments for video {video_id}") from e
    
    if not comments:
        return {
            "comment_count": 0,
            "avg_sentiment": 0.0,
            "sentiment_distribution": {"positive": 0, "neutral": 0, "negative": 0}
        }
    
    # Calculate metrics
    sentiment_scores = [c["sentiment_score"] for c in comments if c["sentiment_score"] is not None]
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
    
    # Categorize sentiments
    distribution = {"positive": 0, "neutral": 0, "negative": 0}
    for score in sentiment_scores:
        if score > 0.1:
            distribution["positive"] += 1
        elif score < -0.1:
            distribution["negative"] += 1
        else:
            distribution["neutral"] += 1
    
    return {
        "comment_count": len(comments),
        "avg_sentiment": round(avg_sentiment, 3),
        "sentiment_distribution": distribution
    }
```

### Database Operation Template

```python
def update_video_metrics(video_id: str, metrics: Dict[str, Any]) -> bool:
    """
    Update video metrics in the database.
    
    Args:
        video_id: YouTube video ID
        metrics: Dictionary of metrics to update
        
    Returns:
        True if update successful, False otherwise
        
    Raises:
        ValueError: If video_id or metrics are invalid
        DatabaseError: If database update fails
    """
    # Validate inputs
    if not validate_youtube_id(video_id, "video"):
        raise ValueError(f"Invalid video ID: {video_id}")
    
    required_metrics = ["view_count", "like_count", "comment_count"]
    missing = validate_required_fields(metrics, required_metrics)
    if missing:
        raise ValueError(f"Missing required metrics: {missing}")
    
    # Prepare update query
    update_fields = []
    params = {"video_id": video_id}
    
    for field in required_metrics:
        if field in metrics:
            update_fields.append(f"{field} = :{field}")
            params[field] = metrics[field]
    
    if not update_fields:
        return False
    
    query = f"""
        UPDATE youtube_videos 
        SET {', '.join(update_fields)}, fetched_at = NOW()
        WHERE video_id = :video_id
    """
    
    # Execute update
    try:
        with get_connection() as conn:
            result = execute_query_safely(conn, query, params)
            return result.rowcount > 0
            
    except Exception as e:
        log_error_with_context(e, {"video_id": video_id, "metrics": metrics})
        raise DatabaseError(f"Failed to update metrics for video {video_id}") from e
```

---

## 🎓 Onboarding Checklist

### For New Developers

- [ ] Read this development standards document
- [ ] Review `src/youtubeviz/common_helpers.py` for available utilities
- [ ] Read `docs/error_handling_guidelines.md`
- [ ] Set up development environment with linting tools
- [ ] Practice writing functions following the templates above
- [ ] Submit first PR following these standards for review

### Code Review Checklist

- [ ] Naming conventions followed (snake_case, PascalCase, etc.)
- [ ] Helper functions used instead of duplicating code
- [ ] Proper error handling with specific exceptions
- [ ] Meaningful variable names and comprehensive comments
- [ ] Functions under 31 lines with single responsibility
- [ ] Real data access patterns (no fake/mock data in production)
- [ ] Descriptive values used instead of unclear booleans

### Tools and Automation

```bash
# Install development tools
pip install black isort flake8 mypy

# Format code
black . --line-length=120
isort . --profile black

# Check code quality
flake8 --max-line-length=120
mypy src/

# Run naming convention auditor
python tools/code_quality/naming_convention_auditor.py --scan

# Check for code duplication
python tools/code_quality/duplicate_code_analyzer.py --analyze
```

---

## 📞 Getting Help

- **Standards Questions**: Review this document and examples
- **Helper Functions**: Check `src/youtubeviz/common_helpers.py`
- **Error Handling**: See `docs/error_handling_guidelines.md`
- **Code Review**: Use the checklist above before submitting PRs

Remember: These standards exist to make our code more maintainable, readable, and reliable. When in doubt, prioritize clarity and consistency!