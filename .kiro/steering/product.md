# Product Overview

## YouTube ETL & Sentiment Analysis Platform

A professional-grade YouTube data pipeline with advanced sentiment analysis capabilities. The platform extracts, processes, and analyzes YouTube video data, comments, and metrics to provide comprehensive analytics and insights.

### Core Capabilities

- **YouTube Data ETL**: Complete pipeline for videos, metrics, and comments extraction
- **Sentiment Analysis**: VADER-based sentiment scoring with artist/channel grouping
- **Interactive Visualizations**: Professional Plotly charts and dashboards
- **Data Quality Monitoring**: Comprehensive diagnostics and health checks
- **YouTube API Compliance**: Built-in data retention policies and ToS compliance

### Key Components

- **ETL Engine**: Bulletproof execution with daily rate limiting (`web/youtube_channel_etl.py`)
- **Sentiment Pipeline**: Automated comment sentiment analysis (`web/sentiment_job.py`)
- **Analytics Package**: Helper library for notebooks and visualizations (`src/youtubeviz/`)
- **Monitoring Tools**: Data quality and pipeline health monitoring (`tools/monitor.py`)

### Target Users

- Music industry analysts tracking artist performance
- Researchers studying YouTube engagement patterns
- Content creators analyzing audience sentiment
- Data scientists building on YouTube analytics

### Compliance Focus

The platform emphasizes YouTube API Terms of Service compliance with configurable data retention policies, automatic cleanup tools, and built-in safeguards for user content protection.
