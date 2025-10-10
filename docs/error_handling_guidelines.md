# Error Handling Guidelines

## Principles

1. **Fail Loud**: Never silently ignore errors
2. **Specific Exceptions**: Always catch specific exception types
3. **Proper Logging**: Log all errors with context
4. **Recovery Instructions**: Provide clear error messages

## Best Practices

### ✅ Good Error Handling

```python
try:
    result = risky_operation()
except ValueError as e:
    logging.error(f"Invalid value in operation: {e}")
    raise
except ConnectionError as e:
    logging.error(f"Database connection failed: {e}")
    return None
```

### ❌ Bad Error Handling

```python
try:
    result = risky_operation()
except:
    pass  # Silent failure-never do this!
```

### ✅ Error Handling with Context

```python
try:
    process_video(video_id)
except Exception as e:
    logging.error(f"Failed to process video {video_id}: {e}")
    raise ProcessingError(f"Video processing failed: {e}") from e
```

## Common Patterns

### Database Operations
```python
try:
    conn.execute(query)
except pymysql.Error as e:
    logging.error(f"Database query failed: {query[:100]}... Error: {e}")
    raise DatabaseError(f"Query execution failed: {e}") from e
```

### API Calls
```python
try:
    response = requests.get(url)
    response.raise_for_status()
except requests.RequestException as e:
    logging.error(f"API request failed: {url} - {e}")
    raise APIError(f"Request to {url} failed: {e}") from e
```

### File Operations
```python
try:
    with open(file_path, 'r') as f:
        data = f.read()
except FileNotFoundError:
    logging.error(f"File not found: {file_path}")
    raise
except PermissionError:
    logging.error(f"Permission denied: {file_path}")
    raise
```
