# Notebooks: Production-Ready Analytics

Clean, presentation-ready notebooks that connect directly to your database. No fallback code, no fake data.

## How to Run the Notebooks

### 🚀 Quick Start
1. **Open any notebook** in Jupyter/VS Code/your preferred environment
2. **Run all cells** - they connect directly to your database
3. **Customize the ARTISTS list** in the second cell if you want specific artists

### 📊 Available Notebooks
- **01_descriptive_overview.ipynb** - Executive summary and momentum analysis
- **02_artist_comparison.ipynb** - Advanced competitive intelligence
- **03_appendix_data_quality.ipynb** - Data validation and cleaning

### 🎨 Artist Selection
```python
ARTISTS = [
    # Leave empty to analyze all available artists, or specify:
    # "Artist Name 1", "Artist Name 2", etc.
]
```

**Note**: Artist names should match exactly as they appear in your database (`songs.artist` or `youtube_videos.channel_title`).

## 🎯 What Each Notebook Does

### 📊 01_descriptive_overview.ipynb
**The Executive Summary** - Perfect for presentations to stakeholders
- Artist performance KPIs and market share
- Momentum analysis with investment recommendations
- Interactive time series with monthly animation
- Strategic insights and next steps

### 🏆 02_artist_comparison.ipynb
**Advanced Competitive Intelligence** - Deep dive for strategic planning
- Performance distribution analysis (consistency vs volatility)
- Momentum scoring with budget allocation framework
- Sentiment analysis from comment NLP
- Revenue modeling and ROI projections

### 🔍 03_appendix_data_quality.ipynb
**Data Validation & Trust** - Ensure your analysis is reliable
- Comprehensive quality assessment with scoring
- Outlier detection and data cleaning
- Best practices for ongoing monitoring
- Quality gates for decision-making

## 💡 Pro Tips

### For Presentations
- All notebooks are fully commented and presentation-ready
- Charts are interactive - great for live demos
- Each section has business context for non-technical stakeholders

### For Regular Reporting
- Notebooks connect directly to your live database
- Customize the `ARTISTS` list for specific analysis
- Run `python tools/2_run_notebooks.py` to batch execute all notebooks

### Configuration via .env
```bash
# Revenue modeling
REVENUE_RPM_DEFAULT=3.0
REVENUE_RPM_MAP_JSON={"Artist A": 4.0, "Artist B": 2.5}

# Visualization colors
ARTIST_COLORS_JSON={"Artist A": "#1f77b4", "Artist B": "#ff7f0e"}

# ETL run controls
YT_RAW_ONCE_PER_DAY=1
YT_METRICS_LOCK_ONCE_PER_DAY=1
ETL_RUN_TYPE=manual  # Set to 'cron' for automated runs
```

## 🚀 ETL Pipeline & Monitoring

### Running ETL
- **Manual**: Open and run `ETL.ipynb`
- **Automated**: Set `ETL_RUN_TYPE=cron` in .env for scheduled runs
- **Daily Protection**: ETL won't run twice in the same day (controlled by .env)

### ETL Monitoring
- **Dashboard**: `notebooks/etl_dashboard.ipynb` - Monitor run history and performance
- **Database Table**: `youtube_etl_runs` tracks all attempts with:
  - Success/failure status and reasons
  - Manual vs CRON run detection
  - Performance metrics (videos processed, duration)
  - Error messages for troubleshooting

### ETL Logging Functions
```python
from web.etl_helpers import start_etl_run, finish_etl_run, log_etl_attempt

# Method 1: Manual logging
run_info = start_etl_run('CHANNEL_ID', 'Reason for run')
# ... do ETL work ...
finish_etl_run(run_info, 'success', None, videos_processed=10, metrics_collected=50)

# Method 2: Convenience function
log_etl_attempt('CHANNEL_ID', success=True, reason='Daily update',
                videos_processed=10, metrics_collected=50)
```
