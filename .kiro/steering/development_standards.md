# Development Standards & Best Practices

## Code Quality Philosophy

### Core Principles
- **TDD (Test-Driven Development)**: Write tests first, then implement
- **Bulletproof execution**: Robust error handling with clear failure messages
- **Human-readable code**: Organize for clarity, not cleverness
- **Fail loudly**: Never silently ignore errors or use fallback code
- **Helper-driven design**: Extract reusable functions, avoid code duplication

### Database Standards
- **Column naming**: Always use `lowercase_snake_case` (never camelCase)
- **Avoid booleans**: Use descriptive strings/enums instead of true/false
- **Explicit schemas**: Define clear data types and constraints
- **Consistent naming**: Follow established patterns across all tables

## User Experience Standards

### Frontend Safety (When Users Involved)
- **Input validation**: Always sanitize and validate user inputs
- **Error boundaries**: Graceful error handling with user-friendly messages
- **Security first**: Assume malicious input, protect against injection attacks
- **Clear feedback**: Users should always know what's happening

### Interactive Design (Steve Krug's "Don't Make Me Think")
- **Simple language**: Avoid complex technical terms in user interfaces
- **Clear choices**: Present options in plain language
- **Immediate feedback**: Show results of user actions instantly
- **Minimal cognitive load**: Reduce decision fatigue with smart defaults

## Configuration Management

### Environment Variables (.env)
- **Global control**: Any variable users might want to change goes in .env
- **Clear naming**: Use descriptive, consistent variable names
- **Channel URLs**: Store as `YT_ARTISTNAME_YT` for automatic parsing by cleanup tools
- **Analysis type**: Use `CHANNEL_ANALYSIS_TYPE` to configure cleanup behavior
- **Documentation**: Comment all environment variables with examples
- **No hardcoded values**: Extract magic numbers and strings to configuration

### Examples of .env Variables
```bash
# Artist visualization colors
ARTIST_COLOR_SCHEME=vibrant  # Options: vibrant, pastel, monochrome
DEFAULT_CHART_HEIGHT=600
MAX_ARTISTS_PER_CHART=10

# Analysis parameters  
MOMENTUM_THRESHOLD_DAYS=30
GROWTH_RATE_MINIMUM=0.05
SENTIMENT_CONFIDENCE_THRESHOLD=0.7
```

## Notebook Standards

### Storytelling Approach
- **Narrative flow**: Each notebook tells a complete story
- **Educational focus**: Explain concepts for data science students
- **Music industry context**: Bridge data science and music business
- **Human connection**: Use artist names, show compassion for their journey

### Visualization Requirements
- **Interactive charts**: All visualizations must be interactive (Plotly/Altair)
- **Consistent colors**: Use global color mapping for artists/categories
- **Clear legends**: Always label axes, provide context
- **Mobile-friendly**: Charts should work on different screen sizes

### Analysis Structure
- **Descriptive**: What happened? (current state)
- **Diagnostic**: Why did it happen? (root causes)
- **Predictive**: What will happen? (forecasting)
- **Prescriptive**: What should we do? (recommendations)

## Code Organization

### File Structure Principles
- **Logical grouping**: Related functionality stays together
- **Clear naming**: File names should indicate purpose immediately
- **Minimal nesting**: Avoid deep directory structures
- **Separation of concerns**: ETL, analysis, and utilities in separate modules

### Function Design
- **Single responsibility**: Each function does one thing well
- **Clear signatures**: Use type hints and descriptive parameter names
- **Helper functions**: Extract common patterns into reusable helpers
- **Error handling**: Every function should handle its failure modes

## Comment Standards

### When to Comment
- **Complex algorithms**: Explain the "why" not just the "what"
- **Business logic**: Connect code to real-world music industry concepts
- **Potential mistakes**: Document tricky areas to prevent future errors
- **API integrations**: Explain external service requirements and limitations

### Comment Style
```python
# IMPORTANT: YouTube API has strict rate limits (10,000 requests/day)
# This function implements exponential backoff to handle quota errors
def fetch_video_data(video_id: str) -> dict:
    """
    Fetch video metadata from YouTube API with bulletproof error handling.
    
    Args:
        video_id: YouTube video ID (11 characters, alphanumeric)
        
    Returns:
        Video metadata dict with standardized keys
        
    Raises:
        YouTubeAPIError: When API returns error or quota exceeded
        ValidationError: When video_id format is invalid
    """
```

## Database Cleanup & Maintenance Standards

### Channel Configuration Management
- **Automatic parsing**: Channel URLs in .env are automatically parsed by cleanup tools
- **Configuration validation**: Always validate .env channels against database before cleanup
- **Backup warnings**: Show serious warnings about irreversible data deletion operations
- **Step-by-step confirmation**: Get explicit user confirmation with reasoning for each deletion
- **Human-readable SQL**: Format all SQL queries with proper spacing and newlines

### Data Quality Integration
- **Cleanup in ETL**: Run cleanup scripts as part of standard ETL pipeline
- **Test integration**: Database cleanup runs in tests to ensure clean data before notebooks
- **Quality-first notebooks**: Run quality notebook before analysis notebooks
- **Unexpected data detection**: Alert users when unknown channels appear in database

## Anti-Patterns to Avoid

### Code Smells
- **Bloated AI code**: Overly complex solutions that look generated
- **Silent failures**: Using try/except without proper error handling
- **Magic numbers**: Hardcoded values without explanation
- **Boolean parameters**: Use enums or descriptive strings instead
- **Fallback code**: If something is required, make it explicit
- **Vague comments**: Replace "Delete from table" with specific reasoning and impact

### Bad Practices
- **Complex menu language**: Use simple, clear terms in interfaces
- **Assumption-driven design**: Ask questions when requirements are unclear
- **Copy-paste programming**: Extract common patterns into helpers
- **Inconsistent naming**: Stick to established conventions throughout
- **Hardcoded channel lists**: Always use .env configuration for channel management

## Music Industry Considerations

### Artist Representation
- **Humanize data**: Remember these are real people's careers
- **Respectful language**: Show compassion for artists' journeys
- **Privacy awareness**: Protect sensitive performance data
- **Cultural sensitivity**: Consider diverse musical backgrounds

### Business Context
- **Momentum focus**: Identify artists gaining traction for marketing investment
- **Budget justification**: Provide clear ROI metrics for label executives
- **Competitive analysis**: Compare artists within appropriate peer groups
- **Growth potential**: Highlight emerging opportunities, not just current success