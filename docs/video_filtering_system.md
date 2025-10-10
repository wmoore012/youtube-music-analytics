# Video Filtering System Documentation

## Overview

The Video Filtering System provides comprehensive video filtering at the API level to prevent problematic videos from entering the database. This system addresses the requirement to handle "4 videos that need to be deleted every time we run ETL" by filtering them before they reach the database instead.

## Key Features

### 1. API-Level Filtering
- Filters videos before database insertion (fail-fast approach)
- Prevents problematic videos from ever entering the system
- Reduces database operations and improves performance

### 2. Configuration-Driven Rules
- Environment variable configuration
- JSON configuration file support
- Multiple filtering criteria support

### 3. Personal Issue Video Handling
- Dedicated handling for the problematic videos mentioned in requirements
- Configurable via `config/personal_issue_videos.json`
- Clear logging of filtering decisions

### 4. Comprehensive Filtering Rules
- **Video ID blocking**: Block specific video IDs
- **Channel ID blocking**: Block entire channels
- **Title pattern matching**: Block videos with specific title patterns
- **Duration constraints**: Filter videos that are too short or too long
- **ISRC requirements**: Require ISRC codes for inclusion

### 5. Robust Error Handling
- Pydantic model validation
- Comprehensive error logging
- Graceful handling of invalid data

### 6. Statistics and Monitoring
- Detailed filtering statistics
- Filter reason breakdown
- Performance monitoring

## Architecture

### Core Components

1. **VideoFilterEngine**: Main filtering engine that applies rules
2. **FilterResult**: Structured result of filtering operations
3. **FilterStats**: Statistics tracking for monitoring
4. **VideoFilter**: Pydantic model for configuration validation

### Data Flow

```
YouTube API Response
        ↓
Video Data Validation (Pydantic)
        ↓
Video Filtering Engine
        ↓
Filter Results + Passed Videos
        ↓
Database Insertion (only passed videos)
```

## Configuration

### Environment Variables

```bash
# Personal issue videos (comma-separated)
PERSONAL_ISSUE_VIDEO_IDS="video1,video2,video3,video4"

# Blocked video IDs (comma-separated)
BLOCKED_VIDEO_IDS="spam123,fake456"

# Blocked channel IDs (comma-separated)
BLOCKED_CHANNEL_IDS="UCspam123,UCfake456"

# Blocked title patterns (pipe-separated regex patterns)
BLOCKED_TITLE_PATTERNS="spam.*content|clickbait|free.*money"

# Duration constraints (seconds)
MIN_VIDEO_DURATION_SECONDS=30
MAX_VIDEO_DURATION_SECONDS=3600

# ISRC requirement
REQUIRE_ISRC_FOR_VIDEOS=false
```

### Configuration File

Create `config/personal_issue_videos.json`:

```json
{
  "description": "Personal issue video IDs that need to be filtered at API level",
  "video_ids": [
    "ACTUAL_VIDEO_ID_1",
    "ACTUAL_VIDEO_ID_2",
    "ACTUAL_VIDEO_ID_3",
    "ACTUAL_VIDEO_ID_4"
  ],
  "last_updated": "2024-01-01",
  "updated_by": "ETL System"
}
```

## Usage

### Basic Integration

```python
from web.video_filter import filter_videos_at_api_level
from web.models import YouTubeVideo

# Get videos from YouTube API (validated as YouTubeVideo objects)
videos = get_videos_from_youtube_api()

# Apply filtering
passed_videos, filter_results = filter_videos_at_api_level(videos)

# Process only the videos that passed filtering
for video in passed_videos:
    insert_video_to_database(video)

# Log filtering results
filtered_count = len([r for r in filter_results if r.is_filtered])
print(f"Filtered {filtered_count} problematic videos")
```

### Advanced Usage

```python
from web.video_filter import VideoFilterEngine, VideoFilter

# Create custom filter configuration
config = VideoFilter(
    blocked_video_ids=["spam123", "fake456"],
    blocked_title_patterns=["spam.*content"],
    min_duration_seconds=30,
    max_duration_seconds=3600
)

# Create filter engine
filter_engine = VideoFilterEngine(config)

# Filter videos
passed_videos, filter_results = filter_engine.filter_videos(videos)

# Get statistics
stats = filter_engine.get_stats()
print(f"Filter rate: {stats.filter_rate:.1f}%")
```

## Filter Reasons

The system provides detailed reasons for why videos are filtered:

- `BLOCKED_VIDEO_ID`: Video ID is in blocked list
- `BLOCKED_CHANNEL_ID`: Channel ID is in blocked list
- `BLOCKED_TITLE_PATTERN`: Title matches blocked pattern
- `DURATION_TOO_SHORT`: Video is shorter than minimum duration
- `DURATION_TOO_LONG`: Video exceeds maximum duration
- `MISSING_ISRC_REQUIRED`: ISRC is required but not present
- `PERSONAL_ISSUE`: Video is in personal issue list
- `QUALITY_ISSUE`: General quality issues
- `INVALID_DATA`: Data validation errors

## Testing

The system includes comprehensive tests covering:

- Individual filter rule testing
- Multiple video filtering
- Configuration loading
- Statistics tracking
- Error handling
- Integration scenarios

Run tests with:
```bash
python -m pytest tests/test_video_filter.py -v
```

## Example Output

```
📊 Video Filtering Summary:
   Total videos processed: 5
   Videos passed: 3
   Videos filtered: 2
   Filter rate: 40.0%
   Filter reasons:
     blocked_title_pattern: 1
     duration_too_short: 1

🚫 Filtered Videos Details:
   Video jNQXAC9IVRw: duration_too_short-Duration 15s is less than minimum 30s
   Video ScMzIvxBSi4: blocked_title_pattern-Title matches blocked pattern: spam.*content
```

## Benefits

### Before (Database Deletion Approach)
- Problematic videos entered database
- Required deletion operations every ETL run
- Database bloat and performance impact
- Reactive approach

### After (API-Level Filtering)
- Problematic videos never enter database
- No deletion operations needed
- Clean database with only valid data
- Proactive approach
- Better performance
- Clear audit trail

## Monitoring

The system provides comprehensive monitoring capabilities:

1. **Filter Statistics**: Track filtering rates and reasons
2. **Performance Metrics**: Monitor filtering performance
3. **Configuration Validation**: Ensure filter rules are valid
4. **Error Tracking**: Log and track filtering errors

## Maintenance

### Adding New Filter Rules

1. Update environment variables or configuration files
2. Restart the ETL pipeline to pick up new configuration
3. Monitor filtering statistics to verify rules are working

### Updating Personal Issue Videos

1. Edit `config/personal_issue_videos.json`
2. Add/remove video IDs as needed
3. Update the `last_updated` field
4. Restart ETL pipeline

### Performance Tuning

- Monitor filter statistics to identify bottlenecks
- Optimize regex patterns for better performance
- Adjust batch sizes if needed
- Use caching for frequently accessed filter rules

## Integration Points

The video filtering system integrates with:

1. **YouTube API Integration**: Filters videos from API responses
2. **ETL Pipeline**: Main filtering point before database insertion
3. **Error Handling System**: Uses centralized error handling
4. **Validation System**: Uses Pydantic models for validation
5. **Logging System**: Comprehensive logging of all decisions

## Future Enhancements

Potential future improvements:

1. **Machine Learning Filters**: AI-based content quality assessment
2. **Dynamic Rule Updates**: Hot-reload configuration changes
3. **Advanced Pattern Matching**: More sophisticated text analysis
4. **Performance Optimization**: Caching and batch processing improvements
5. **Dashboard Integration**: Web-based filter management interface
