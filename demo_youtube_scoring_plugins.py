#!/usr/bin/env python3
"""
Demo script showing YouTube scoring plugins working with real database data.

This demonstrates the scoring plugins using existing YouTube analytics data
from the database tables.
"""

import pandas as pd
from datetime import datetime, timedelta

from src.data_organization.scoring_engine import ScoringEngine
from src.data_organization.youtube_scoring_plugins import (
    ArtistMomentumScoringPlugin,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
)
from youtubeviz.data import load_artist_daily_metrics, _get_engine
from web.etl_helpers import get_engine


def load_sample_data_for_momentum(engine, limit_artists=5):
    """Load sample data for momentum scoring from database."""
    print("Loading data for momentum scoring...")
    
    # Load recent data (last 60 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=60)
    
    # Load artist daily metrics
    data = load_artist_daily_metrics(start=start_date, end=end_date, engine=engine)
    
    if data.empty:
        print("No data found in database. Make sure ETL has been run.")
        return pd.DataFrame()
    
    # Limit to top artists by total views for demo
    top_artists = (data.groupby("artist_name")["views"]
                   .sum()
                   .sort_values(ascending=False)
                   .head(limit_artists)
                   .index.tolist())
    
    data = data[data["artist_name"].isin(top_artists)]
    
    # Rename columns to match plugin requirements
    data = data.rename(columns={
        "date": "metrics_date",
        "views": "view_count",
        "likes": "like_count", 
        "comments": "comment_count",
        "video_title": "title"
    })
    
    # Add published_at from video data if not present
    if "published_at" not in data.columns:
        data["published_at"] = data["metrics_date"]  # Fallback
    
    print(f"Loaded {len(data)} records for {len(top_artists)} artists")
    return data


def load_sample_data_for_engagement(engine, limit_videos=20):
    """Load sample data for engagement scoring from database."""
    print("Loading data for engagement scoring...")
    
    # Load video metrics with sentiment data
    query = """
    SELECT 
        v.video_id,
        v.title,
        v.channel_title as artist_name,
        m.view_count,
        m.like_count,
        m.comment_count,
        s.avg_sentiment,
        COALESCE(s.comment_count, 0) as sentiment_magnitude
    FROM youtube_videos v
    JOIN youtube_metrics m ON v.video_id = m.video_id
    LEFT JOIN youtube_sentiment_summary s ON v.video_id = s.video_id
    WHERE m.view_count > 1000  -- Filter for videos with meaningful engagement
    ORDER BY m.view_count DESC
    LIMIT :limit
    """
    
    from sqlalchemy import text
    data = pd.read_sql(text(query), engine, params={"limit": limit_videos})
    
    if data.empty:
        print("No engagement data found in database.")
        return pd.DataFrame()
    
    # Fill missing sentiment data
    data["avg_sentiment"] = data["avg_sentiment"].fillna(0.0)
    data["sentiment_magnitude"] = data["sentiment_magnitude"].fillna(0.0)
    
    print(f"Loaded {len(data)} videos for engagement scoring")
    return data


def load_sample_data_for_growth_potential(engine, limit_artists=3):
    """Load sample data for growth potential scoring from database."""
    print("Loading data for growth potential scoring...")
    
    # Load time series data (last 90 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)
    
    query = """
    SELECT 
        v.channel_title as artist_name,
        v.video_id,
        m.metrics_date,
        m.view_count,
        m.like_count,
        m.comment_count
    FROM youtube_videos v
    JOIN youtube_metrics m ON v.video_id = m.video_id
    WHERE m.metrics_date >= :start_date 
    AND m.metrics_date <= :end_date
    ORDER BY v.channel_title, m.metrics_date
    """
    
    from sqlalchemy import text
    data = pd.read_sql(text(query), engine, params={
        "start_date": start_date,
        "end_date": end_date
    })
    
    if data.empty:
        print("No growth potential data found in database.")
        return pd.DataFrame()
    
    # Limit to top artists by data volume for demo
    artist_counts = data["artist_name"].value_counts()
    top_artists = artist_counts.head(limit_artists).index.tolist()
    data = data[data["artist_name"].isin(top_artists)]
    
    print(f"Loaded {len(data)} records for {len(top_artists)} artists")
    return data


def demo_momentum_scoring():
    """Demonstrate artist momentum scoring."""
    print("\n" + "="*60)
    print("ARTIST MOMENTUM SCORING DEMO")
    print("="*60)
    
    engine = get_engine()
    data = load_sample_data_for_momentum(engine)
    
    if data.empty:
        print("Skipping momentum scoring - no data available")
        return
    
    # Create and register plugin
    scoring_engine = ScoringEngine()
    momentum_plugin = ArtistMomentumScoringPlugin()
    scoring_engine.register_plugin(momentum_plugin)
    
    # Execute scoring
    try:
        result = scoring_engine.execute_scoring("artist_momentum_scorer", data)
        
        print(f"\nMomentum Scoring Results:")
        print(f"Algorithm: {result.algorithm_name} v{result.algorithm_version}")
        print(f"Processed: {len(data)} input records")
        print(f"Generated: {len(result.entity_scores)} artist scores")
        
        # Display results
        scores_df = result.entity_scores.sort_values("score_value", ascending=False)
        print(f"\nTop Artists by Momentum Score:")
        print(scores_df[["entity_id", "score_value", "confidence", "momentum_category", 
                        "total_videos", "recent_videos"]].head(10).to_string(index=False))
        
        # Show momentum categories
        print(f"\nMomentum Categories:")
        category_counts = scores_df["momentum_category"].value_counts()
        for category, count in category_counts.items():
            print(f"  {category}: {count} artists")
            
    except Exception as e:
        print(f"Error in momentum scoring: {e}")


def demo_engagement_scoring():
    """Demonstrate engagement scoring."""
    print("\n" + "="*60)
    print("ENGAGEMENT SCORING DEMO")
    print("="*60)
    
    engine = get_engine()
    data = load_sample_data_for_engagement(engine)
    
    if data.empty:
        print("Skipping engagement scoring - no data available")
        return
    
    # Create and register plugin
    scoring_engine = ScoringEngine()
    engagement_plugin = EngagementScoringPlugin()
    scoring_engine.register_plugin(engagement_plugin)
    
    # Execute scoring
    try:
        result = scoring_engine.execute_scoring("engagement_scorer", data)
        
        print(f"\nEngagement Scoring Results:")
        print(f"Algorithm: {result.algorithm_name} v{result.algorithm_version}")
        print(f"Processed: {len(data)} input records")
        print(f"Generated: {len(result.entity_scores)} video scores")
        
        # Display results
        scores_df = result.entity_scores.sort_values("score_value", ascending=False)
        print(f"\nTop Videos by Engagement Score:")
        display_cols = ["entity_id", "score_value", "confidence", "engagement_rate", 
                       "sentiment_boost", "total_engagement"]
        print(scores_df[display_cols].head(10).to_string(index=False))
        
        # Show engagement statistics
        print(f"\nEngagement Statistics:")
        print(f"  Average engagement score: {scores_df['score_value'].mean():.4f}")
        print(f"  Median engagement score: {scores_df['score_value'].median():.4f}")
        print(f"  Videos with positive sentiment boost: {(scores_df['sentiment_boost'] > 0).sum()}")
        
    except Exception as e:
        print(f"Error in engagement scoring: {e}")


def demo_growth_potential_scoring():
    """Demonstrate growth potential scoring."""
    print("\n" + "="*60)
    print("GROWTH POTENTIAL SCORING DEMO")
    print("="*60)
    
    engine = get_engine()
    data = load_sample_data_for_growth_potential(engine)
    
    if data.empty:
        print("Skipping growth potential scoring - no data available")
        return
    
    # Create and register plugin
    scoring_engine = ScoringEngine()
    growth_plugin = GrowthPotentialScoringPlugin()
    scoring_engine.register_plugin(growth_plugin)
    
    # Execute scoring
    try:
        result = scoring_engine.execute_scoring("growth_potential_scorer", data)
        
        print(f"\nGrowth Potential Scoring Results:")
        print(f"Algorithm: {result.algorithm_name} v{result.algorithm_version}")
        print(f"Processed: {len(data)} input records")
        print(f"Generated: {len(result.entity_scores)} artist scores")
        
        # Display results
        scores_df = result.entity_scores.sort_values("score_value", ascending=False)
        print(f"\nArtists by Growth Potential:")
        display_cols = ["entity_id", "score_value", "confidence", "trend_direction", 
                       "growth_velocity", "data_points"]
        print(scores_df[display_cols].head(10).to_string(index=False))
        
        # Show trend directions
        print(f"\nTrend Directions:")
        trend_counts = scores_df["trend_direction"].value_counts()
        for trend, count in trend_counts.items():
            print(f"  {trend}: {count} artists")
            
    except Exception as e:
        print(f"Error in growth potential scoring: {e}")


def demo_plugin_system_status():
    """Show scoring system status and capabilities."""
    print("\n" + "="*60)
    print("SCORING SYSTEM STATUS")
    print("="*60)
    
    # Create scoring engine and register all plugins
    scoring_engine = ScoringEngine()
    
    plugins = [
        ArtistMomentumScoringPlugin(),
        EngagementScoringPlugin(),
        GrowthPotentialScoringPlugin()
    ]
    
    for plugin in plugins:
        scoring_engine.register_plugin(plugin)
    
    # Show system status
    status = scoring_engine.get_system_status()
    print(f"Loaded plugins: {status['loaded_plugins']}")
    print(f"Available algorithms: {', '.join(status['available_algorithms'])}")
    print(f"Plugin isolation enabled: {status['isolation_enabled']}")
    print(f"Max execution time: {status['max_execution_time']}s")
    
    # Show plugin metadata
    print(f"\nPlugin Details:")
    for algorithm in status['available_algorithms']:
        metadata = scoring_engine.get_plugin_metadata(algorithm)
        print(f"\n  {metadata['name']} v{metadata['version']}")
        print(f"    Description: {metadata['description']}")
        print(f"    Parameters: {len(metadata['parameters'])} configurable")
        print(f"    Input requirements: {len(metadata['input_requirements'])} columns")


def main():
    """Run all scoring plugin demos."""
    print("YouTube Analytics Scoring Plugins Demo")
    print("This demo shows the scoring plugins working with real database data.")
    
    try:
        # Check database connection
        engine = get_engine()
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT COUNT(*) as count FROM youtube_videos"))
            video_count = result.fetchone()[0]
            print(f"Database connected: {video_count} videos available")
        
        # Run demos
        demo_plugin_system_status()
        demo_momentum_scoring()
        demo_engagement_scoring()
        demo_growth_potential_scoring()
        
        print("\n" + "="*60)
        print("DEMO COMPLETE")
        print("="*60)
        print("All scoring plugins demonstrated successfully!")
        print("These plugins can now be used in notebooks and analytics workflows.")
        
    except Exception as e:
        print(f"Demo failed: {e}")
        print("Make sure the database is set up and ETL has been run.")


if __name__ == "__main__":
    main()