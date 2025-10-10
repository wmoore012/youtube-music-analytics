# Development Standards Onboarding Workshop

## 🎯 Workshop Overview

This hands-on workshop teaches the new development standards through practical examples and exercises. You'll learn to write code that follows our established patterns and best practices.

## 📋 Workshop Agenda

1. [Before & After Examples](#before--after-examples)
2. [Hands-On Exercises](#hands-on-exercises)
3. [Common Mistakes](#common-mistakes)
4. [Best Practices Checklist](#best-practices-checklist)

---

## 🔄 Before & After Examples

### Example 1: Data Processing Function

#### ❌ BEFORE (Old Way)
```python
def processData(d):
    try:
        results = []
        for item in d:
            if item['views'] > 1000:
                # Calculate engagement
                eng = (item['likes'] + item['comments']) / item['views'] * 100
                if eng > 5:
                    results.append({
                        'id': item['id'],
                        'engagement': eng,
                        'category': 'high' if eng > 10 else 'medium'
                    })
        return results
    except:
        return []
```

**Problems:**
- camelCase function name
- Single letter variable names
- No documentation
- Silent error handling
- Hardcoded magic numbers
- No input validation

#### ✅ AFTER (New Way)
```python
from src.youtubeviz.common_helpers import validate_required_fields, safe_divide

def filter_high_engagement_videos(video_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter videos with high engagement rates from a list of video data.

    Engagement rate = (likes + comments) / views * 100
    Only includes videos with >1000 views and >5% engagement rate.

    Args:
        video_data: List of video dictionaries with metrics

    Returns:
        List of high-engagement videos with calculated metrics

    Raises:
        ValueError: If required fields are missing from video data
    """
    MIN_VIEWS_THRESHOLD = 1000
    MIN_ENGAGEMENT_THRESHOLD = 5.0
    HIGH_ENGAGEMENT_THRESHOLD = 10.0

    if not video_data:
        return []

    high_engagement_videos = []

    for video in video_data:
        # Validate required fields
        required_fields = ["id", "views", "likes", "comments"]
        missing_fields = validate_required_fields(video, required_fields)
        if missing_fields:
            logging.warning(f"Skipping video {video.get('id', 'unknown')}: missing {missing_fields}")
            continue

        # Filter by minimum views
        if video["views"] < MIN_VIEWS_THRESHOLD:
            continue

        # Calculate engagement rate safely
        total_interactions = video["likes"] + video["comments"]
        engagement_rate = safe_divide(total_interactions, video["views"]) * 100

        # Filter by minimum engagement
        if engagement_rate < MIN_ENGAGEMENT_THRESHOLD:
            continue

        # Categorize engagement level
        engagement_category = "high" if engagement_rate > HIGH_ENGAGEMENT_THRESHOLD else "medium"

        high_engagement_videos.append({
            "video_id": video["id"],
            "engagement_rate": round(engagement_rate, 2),
            "engagement_category": engagement_category,
            "view_count": video["views"],
            "total_interactions": total_interactions
        })

    return high_engagement_videos
```

**Improvements:**
- ✅ snake_case function name
- ✅ Descriptive variable names
- ✅ Comprehensive documentation
- ✅ Proper error handling with logging
- ✅ Named constants instead of magic numbers
- ✅ Input validation using helper functions
- ✅ Type hints for clarity

### Example 2: Database Operation

#### ❌ BEFORE (Old Way)
```python
def getVideoStats(videoId):
    try:
        conn = mysql.connector.connect(host='localhost', user='user', password='pass', database='db')
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM videos WHERE id = '{videoId}'")
        result = cursor.fetchone()
        conn.close()
        return result
    except:
        return None
```

**Problems:**
- camelCase naming
- SQL injection vulnerability
- Hardcoded connection details
- Silent error handling
- No resource cleanup guarantee
- No input validation

#### ✅ AFTER (New Way)
```python
from src.youtubeviz.common_helpers import execute_query_safely, validate_youtube_id

def get_video_statistics(video_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve comprehensive statistics for a YouTube video.

    Args:
        video_id: YouTube video ID (11 characters)

    Returns:
        Dictionary with video statistics or None if not found

    Raises:
        ValueError: If video_id format is invalid
        DatabaseError: If database query fails
    """
    # Validate input
    if not validate_youtube_id(video_id, "video"):
        raise ValueError(f"Invalid YouTube video ID format: {video_id}")

    query = """
        SELECT
            video_id,
            title,
            channel_title,
            view_count,
            like_count,
            comment_count,
            published_at,
            fetched_at
        FROM youtube_videos
        WHERE video_id = :video_id
    """

    try:
        with get_connection() as conn:
            result = execute_query_safely(conn, query, {"video_id": video_id})
            row = result.fetchone()

            if not row:
                logging.info(f"Video not found in database: {video_id}")
                return None

            # Convert to dictionary with meaningful keys
            return {
                "video_id": row["video_id"],
                "title": row["title"],
                "channel_title": row["channel_title"],
                "metrics": {
                    "view_count": row["view_count"],
                    "like_count": row["like_count"],
                    "comment_count": row["comment_count"]
                },
                "published_at": row["published_at"],
                "last_updated": row["fetched_at"]
            }

    except Exception as e:
        log_error_with_context(e, {"video_id": video_id, "operation": "get_video_statistics"})
        raise DatabaseError(f"Failed to retrieve statistics for video {video_id}") from e
```

**Improvements:**
- ✅ snake_case naming
- ✅ Parameterized queries (no SQL injection)
- ✅ Connection management with context manager
- ✅ Proper error handling with context
- ✅ Input validation
- ✅ Structured return data
- ✅ Comprehensive logging

---

## 🏋️ Hands-On Exercises

### Exercise 1: Refactor This Function

**Your Task:** Refactor this function to follow our standards:

```python
def calcSentiment(comments):
    try:
        total = 0
        count = 0
        for c in comments:
            if c['sentiment']:
                total += c['sentiment']
                count += 1
        return total / count if count > 0 else 0
    except:
        return 0
```

**Solution:**
```python
from src.youtubeviz.common_helpers import safe_divide, validate_required_fields

def calculate_average_sentiment(comment_data: List[Dict[str, Any]]) -> float:
    """
    Calculate the average sentiment score from a list of comments.

    Args:
        comment_data: List of comment dictionaries with sentiment scores

    Returns:
        Average sentiment score (-1.0 to 1.0), or 0.0 if no valid scores

    Raises:
        ValueError: If comment_data is not a list
    """
    if not isinstance(comment_data, list):
        raise ValueError("comment_data must be a list")

    if not comment_data:
        return 0.0

    valid_sentiment_scores = []

    for comment in comment_data:
        # Validate comment structure
        if not isinstance(comment, dict):
            logging.warning("Skipping non-dictionary comment entry")
            continue

        sentiment_score = comment.get("sentiment_score")

        # Only include valid sentiment scores
        if sentiment_score is not None and isinstance(sentiment_score, (int, float)):
            # Ensure sentiment score is in valid range
            if -1.0 <= sentiment_score <= 1.0:
                valid_sentiment_scores.append(sentiment_score)
            else:
                logging.warning(f"Sentiment score out of range: {sentiment_score}")

    if not valid_sentiment_scores:
        logging.info("No valid sentiment scores found in comment data")
        return 0.0

    total_sentiment = sum(valid_sentiment_scores)
    average_sentiment = safe_divide(total_sentiment, len(valid_sentiment_scores), default=0.0)

    return round(average_sentiment, 3)
```

### Exercise 2: Create a New Function

**Your Task:** Write a function that processes YouTube channel data following our standards.

**Requirements:**
- Function name: `analyze_channel_performance`
- Input: channel_id (string)
- Output: Dictionary with channel metrics
- Must use helper functions
- Must have proper error handling
- Must validate inputs
- Must include comprehensive documentation

**Template to Complete:**
```python
def analyze_channel_performance(channel_id: str) -> Dict[str, Any]:
    """
    [YOUR DOCUMENTATION HERE]
    """
    # [YOUR IMPLEMENTATION HERE]
    pass
```

**Solution:**
```python
from src.youtubeviz.common_helpers import (
    execute_query_safely, validate_required_fields,
    format_number, get_current_timestamp
)

def analyze_channel_performance(channel_id: str) -> Dict[str, Any]:
    """
    Analyze performance metrics for a YouTube channel.

    Calculates comprehensive metrics including video count, total views,
    engagement rates, and growth trends for the specified channel.

    Args:
        channel_id: YouTube channel ID (starts with 'UC')

    Returns:
        Dictionary containing:
        - basic_metrics: Video count, total views, subscribers
        - engagement_metrics: Average likes, comments, engagement rate
        - content_metrics: Upload frequency, popular video types
        - analysis_timestamp: When analysis was performed

    Raises:
        ValueError: If channel_id format is invalid
        DatabaseError: If database queries fail

    Example:
        >>> metrics = analyze_channel_performance("UCuAXFkgsw1L7xaCfnd5JJOw")
        >>> print(metrics["basic_metrics"]["total_views"])
        "1.2M"
    """
    # Validate channel ID format
    if not channel_id or not isinstance(channel_id, str):
        raise ValueError("channel_id must be a non-empty string")

    if not channel_id.startswith("UC") or len(channel_id) != 24:
        raise ValueError(f"Invalid YouTube channel ID format: {channel_id}")

    try:
        with get_connection() as conn:
            # Get basic channel metrics
            basic_metrics_query = """
                SELECT
                    COUNT(*) as video_count,
                    SUM(view_count) as total_views,
                    AVG(view_count) as avg_views_per_video,
                    MAX(published_at) as latest_video_date,
                    MIN(published_at) as first_video_date
                FROM youtube_videos
                WHERE channel_id = :channel_id
            """

            basic_result = execute_query_safely(conn, basic_metrics_query, {"channel_id": channel_id})
            basic_data = basic_result.fetchone()

            if not basic_data or basic_data["video_count"] == 0:
                logging.warning(f"No videos found for channel: {channel_id}")
                return {
                    "channel_id": channel_id,
                    "basic_metrics": {"video_count": 0, "total_views": 0},
                    "engagement_metrics": {},
                    "content_metrics": {},
                    "analysis_timestamp": get_current_timestamp().isoformat()
                }

            # Get engagement metrics
            engagement_query = """
                SELECT
                    AVG(like_count) as avg_likes,
                    AVG(comment_count) as avg_comments,
                    AVG((like_count + comment_count) / NULLIF(view_count, 0) * 100) as avg_engagement_rate
                FROM youtube_videos
                WHERE channel_id = :channel_id AND view_count > 0
            """

            engagement_result = execute_query_safely(conn, engagement_query, {"channel_id": channel_id})
            engagement_data = engagement_result.fetchone()

            # Format results
            basic_metrics = {
                "video_count": basic_data["video_count"],
                "total_views": format_number(basic_data["total_views"] or 0),
                "avg_views_per_video": format_number(basic_data["avg_views_per_video"] or 0),
                "channel_age_days": (
                    (basic_data["latest_video_date"] - basic_data["first_video_date"]).days
                    if basic_data["latest_video_date"] and basic_data["first_video_date"]
                    else 0
                )
            }

            engagement_metrics = {
                "avg_likes": round(engagement_data["avg_likes"] or 0, 1),
                "avg_comments": round(engagement_data["avg_comments"] or 0, 1),
                "avg_engagement_rate": round(engagement_data["avg_engagement_rate"] or 0, 2)
            }

            # Calculate upload frequency
            upload_frequency = safe_divide(
                basic_data["video_count"],
                basic_metrics["channel_age_days"],
                default=0
            ) * 30  # Videos per month

            content_metrics = {
                "upload_frequency_per_month": round(upload_frequency, 1),
                "content_consistency": "high" if upload_frequency > 4 else "medium" if upload_frequency > 1 else "low"
            }

            return {
                "channel_id": channel_id,
                "basic_metrics": basic_metrics,
                "engagement_metrics": engagement_metrics,
                "content_metrics": content_metrics,
                "analysis_timestamp": get_current_timestamp().isoformat()
            }

    except Exception as e:
        log_error_with_context(e, {"channel_id": channel_id, "operation": "analyze_channel_performance"})
        raise DatabaseError(f"Failed to analyze performance for channel {channel_id}") from e
```

---

## ⚠️ Common Mistakes

### 1. Naming Convention Violations

```python
# ❌ WRONG
def getUserData(userId):
    userName = "John"
    isActive = True
    return userName, isActive

# ✅ CORRECT
def get_user_data(user_id):
    user_name = "John"
    account_status = "active"  # More descriptive than boolean
    return user_name, account_status
```

### 2. Not Using Helper Functions

```python
# ❌ WRONG-Duplicating validation logic
def process_video_a(video_data):
    if "video_id" not in video_data:
        raise ValueError("Missing video_id")
    if "title" not in video_data:
        raise ValueError("Missing title")
    # ... processing logic

def process_video_b(video_data):
    if "video_id" not in video_data:
        raise ValueError("Missing video_id")
    if "title" not in video_data:
        raise ValueError("Missing title")
    # ... different processing logic

# ✅ CORRECT-Using helper function
def process_video_a(video_data):
    missing = validate_required_fields(video_data, ["video_id", "title"])
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    # ... processing logic

def process_video_b(video_data):
    missing = validate_required_fields(video_data, ["video_id", "title"])
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    # ... different processing logic
```

### 3. Silent Error Handling

```python
# ❌ WRONG
try:
    result = risky_operation()
except:
    pass  # Silent failure

# ✅ CORRECT
try:
    result = risky_operation()
except SpecificException as e:
    logging.error(f"Operation failed: {e}")
    raise ProcessingError(f"Failed to complete operation: {e}") from e
```

### 4. Unclear Boolean Usage

```python
# ❌ WRONG
video_status = True  # What does True mean?
processing_done = False  # Done with what? Success or failure?

# ✅ CORRECT
video_status = "published"  # Clear meaning
processing_state = "in_progress"  # Specific state
```

---

## ✅ Best Practices Checklist

### Before Writing Code
- [ ] Plan function purpose and single responsibility
- [ ] Choose descriptive function and variable names
- [ ] Identify reusable patterns that could use helper functions
- [ ] Consider error scenarios and how to handle them

### While Writing Code
- [ ] Use snake_case for functions and variables
- [ ] Use PascalCase for classes
- [ ] Import and use helper functions from `common_helpers.py`
- [ ] Add comprehensive docstrings with Args, Returns, Raises
- [ ] Validate inputs using helper functions
- [ ] Handle errors with specific exceptions and logging
- [ ] Use descriptive values instead of unclear booleans

### After Writing Code
- [ ] Review function length (keep under 31 lines)
- [ ] Check for code duplication that could be extracted
- [ ] Verify error handling provides clear messages
- [ ] Test with invalid inputs to ensure proper error handling
- [ ] Run naming convention auditor
- [ ] Format code with black and isort

### Code Review
- [ ] All naming conventions followed
- [ ] Helper functions used appropriately
- [ ] No silent error handling
- [ ] Clear, meaningful variable names
- [ ] Comprehensive documentation
- [ ] Real data access patterns (no fake data)
- [ ] Appropriate use of descriptive values vs booleans

---

## 🎓 Graduation Exercise

**Task:** Create a complete module following all standards

Create a new file `src/youtubeviz/video_analyzer.py` with the following functions:

1. `validate_video_data(video_data: Dict) -> List[str]`
   - Validate video data structure
   - Return list of validation errors

2. `calculate_video_metrics(video_data: Dict) -> Dict[str, Any]`
   - Calculate engagement rate, view velocity, etc.
   - Use helper functions appropriately

3. `categorize_video_performance(metrics: Dict) -> str`
   - Return "excellent", "good", "average", or "poor"
   - Use descriptive values, not booleans

**Requirements:**
- Follow all naming conventions
- Use helper functions from `common_helpers.py`
- Include comprehensive documentation
- Implement proper error handling
- Keep functions under 31 lines
- Add meaningful comments

**Evaluation Criteria:**
- Code follows all established standards
- Helper functions used appropriately
- Error handling is fail-loud with clear messages
- Documentation is comprehensive
- Variable names are meaningful
- No code duplication

---

## 📞 Getting Help

- **Questions about standards**: Review the examples in this workshop
- **Helper function usage**: Check `src/youtubeviz/common_helpers.py`
- **Error handling patterns**: See the before/after examples above
- **Code review**: Use the checklist before submitting

Remember: These standards make our code more maintainable and reliable. Practice makes perfect!
