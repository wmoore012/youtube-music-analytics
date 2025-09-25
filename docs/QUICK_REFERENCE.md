# Quick Reference Guide - Development Standards

## 🚀 Daily Development Cheat Sheet

### Naming Conventions
```python
# Variables & Functions: snake_case
user_count = 100
def get_video_data():

# Classes: PascalCase
class VideoProcessor:

# Constants: UPPER_CASE
MAX_RETRIES = 3
```

### Helper Functions Import
```python
from src.youtubeviz.common_helpers import (
    # Database
    execute_query_safely, get_table_row_count,

    # Validation
    validate_required_fields, validate_youtube_id, clean_text_field,

    # Error Handling
    retry_operation, safe_divide, log_error_with_context,

    # Formatting
    format_number, format_duration, format_percentage,

    # Files
    read_json_file, write_json_file, ensure_directory_exists
)
```

### Function Template
```python
def function_name(param: type) -> return_type:
    """
    Brief description of what the function does.

    Args:
        param: Description of parameter

    Returns:
        Description of return value

    Raises:
        ExceptionType: When this exception occurs
    """
    # Validate inputs
    if not param:
        raise ValueError("param cannot be empty")

    # Main logic here
    result = process_data(param)

    return result
```

### Error Handling Pattern
```python
try:
    result = risky_operation()
except SpecificException as e:
    logging.error(f"Operation failed: {e}")
    raise CustomError(f"Failed to complete operation: {e}") from e
```

### Database Query Pattern
```python
def get_data(video_id: str) -> Optional[Dict]:
    query = "SELECT * FROM table WHERE id = :id"

    try:
        with get_connection() as conn:
            result = execute_query_safely(conn, query, {"id": video_id})
            return result.fetchone()
    except Exception as e:
        log_error_with_context(e, {"video_id": video_id})
        raise DatabaseError(f"Query failed: {e}") from e
```

## 🔧 Common Helper Functions

### Database Operations
```python
# Safe query execution
result = execute_query_safely(conn, query, params)

# Check table exists
if check_table_exists(conn, "youtube_videos"):
    # Table exists

# Get row count
count = get_table_row_count(conn, "youtube_comments")
```

### Data Validation
```python
# Validate required fields
missing = validate_required_fields(data, ["id", "title"])
if missing:
    raise ValueError(f"Missing: {missing}")

# Validate YouTube ID
if not validate_youtube_id(video_id, "video"):
    raise ValueError("Invalid video ID")

# Clean text
clean_text = clean_text_field(raw_text, max_length=500)
```

### Number Formatting
```python
# Format large numbers
views_formatted = format_number(1234567)  # "1.2M"

# Format duration
duration_text = format_duration(3661)  # "1h 1m 1s"

# Format percentage
percent_text = format_percentage(75, 100)  # "75.0%"

# Safe division
rate = safe_divide(likes, views, default=0)
```

### Error Handling
```python
# Retry with backoff
result = retry_operation(lambda: api_call(), max_retries=3)

# Log with context
log_error_with_context(exception, {"video_id": video_id})
```

## ❌ Common Anti-Patterns to Avoid

```python
# DON'T: camelCase
def getUserData(): pass

# DON'T: Silent errors
try:
    risky_operation()
except:
    pass

# DON'T: Unclear booleans
is_good = True  # Good at what?

# DON'T: Magic numbers
if views > 1000:  # What's special about 1000?

# DON'T: Duplicate validation
if "id" not in data:
    raise ValueError("Missing id")
```

## ✅ Correct Patterns

```python
# DO: snake_case
def get_user_data(): pass

# DO: Specific error handling
try:
    risky_operation()
except SpecificError as e:
    logging.error(f"Operation failed: {e}")
    raise

# DO: Descriptive values
performance_level = "excellent"  # Clear meaning

# DO: Named constants
MIN_VIEWS_THRESHOLD = 1000
if views > MIN_VIEWS_THRESHOLD:

# DO: Use helper functions
missing = validate_required_fields(data, ["id"])
if missing:
    raise ValueError(f"Missing: {missing}")
```

## 🛠️ Development Tools

```bash
# Format code
black . --line-length=120
isort . --profile black

# Check naming conventions
python tools/code_quality/naming_convention_auditor.py --scan

# Check for duplicates
python tools/code_quality/duplicate_code_analyzer.py --analyze

# Verify standards compliance
python tools/code_quality/verify_helper_functions.py
```

## 📋 Pre-Commit Checklist

- [ ] Function names are snake_case
- [ ] Class names are PascalCase
- [ ] Used helper functions instead of duplicating code
- [ ] Added comprehensive docstring
- [ ] Validated inputs properly
- [ ] Used specific exception handling
- [ ] Added meaningful variable names
- [ ] Kept functions under 31 lines
- [ ] No magic numbers or unclear booleans
- [ ] Formatted with black and isort

## 🎯 Quick Wins

1. **Import helpers first**: Always check `common_helpers.py` before writing utility code
2. **Validate early**: Use `validate_required_fields()` at function start
3. **Name clearly**: If you can't explain the variable name, choose a better one
4. **Handle errors**: Never use bare `except:` clauses
5. **Document thoroughly**: Future you will thank present you

## 📞 Need Help?

- **Standards**: `docs/DEVELOPMENT_STANDARDS.md`
- **Workshop**: `docs/ONBOARDING_WORKSHOP.md`
- **Error Handling**: `docs/error_handling_guidelines.md`
- **Helper Functions**: `src/youtubeviz/common_helpers.py`
