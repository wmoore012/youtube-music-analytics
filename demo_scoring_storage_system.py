#!/usr/bin/env python3
"""
Demo script for the scoring results storage system.

This demonstrates the complete workflow of:
1. Creating scoring tables
2. Storing scoring results
3. Querying and analyzing stored results
4. Historical trend analysis
"""

import pandas as pd
from datetime import datetime, timedelta

from src.data_organization.scoring_engine import ScoringEngine
from src.data_organization.scoring_storage import ScoringStorage
from src.data_organization.youtube_scoring_plugins import (
    ArtistMomentumScoringPlugin,
    EngagementScoringPlugin,
    GrowthPotentialScoringPlugin,
)
from youtubeviz.data import load_artist_daily_metrics
from web.etl_helpers import get_engine


def check_scoring_schema():
    """Check if scoring schema exists and is valid."""
    print("🔍 Checking scoring schema...")
    
    storage = ScoringStorage()
    validation_result = storage.validate_schema()
    
    if validation_result.is_valid:
        print("✅ Scoring schema is valid and ready!")
        return True
    else:
        print("❌ Scoring schema validation failed:")
        for error in validation_result.errors:
            print(f"  - {error}")
        
        if validation_result.warnings:
            print("⚠️  Warnings:")
            for warning in validation_result.warnings:
                print(f"  - {warning}")
        
        print("\n💡 Run 'python tools/setup/create_scoring_tables.py' to create the schema")
        return False


def demo_scoring_with_storage():
    """Demonstrate scoring with automatic storage."""
    print("\n" + "="*60)
    print("SCORING WITH AUTOMATIC STORAGE DEMO")
    print("="*60)
    
    # Create scoring engine with storage enabled
    engine = ScoringEngine(enable_storage=True)
    
    # Register plugins
    momentum_plugin = ArtistMomentumScoringPlugin()
    engagement_plugin = EngagementScoringPlugin()
    
    engine.register_plugin(momentum_plugin)
    engine.register_plugin(engagement_plugin)
    
    # Load sample data
    try:
        db_engine = get_engine()
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        data = load_artist_daily_metrics(start=start_date, end=end_date, engine=db_engine)
        
        if data.empty:
            print("No data available for demo. Make sure ETL has been run.")
            return
        
        # Limit to top 3 artists for demo
        top_artists = data.groupby("artist_name")["views"].sum().nlargest(3).index.tolist()
        data = data[data["artist_name"].isin(top_artists)]
        
        # Prepare data for momentum scoring
        momentum_data = data.rename(columns={
            "date": "metrics_date",
            "views": "view_count",
            "likes": "like_count",
            "comments": "comment_count"
        })
        momentum_data["published_at"] = momentum_data["metrics_date"]
        momentum_data["channel_title"] = momentum_data["artist_name"]
        
        print(f"📊 Running momentum scoring on {len(momentum_data)} records for {len(top_artists)} artists...")
        
        # Execute scoring with automatic storage
        result = engine.execute_scoring(
            "artist_momentum_scorer", 
            momentum_data,
            store_results=True,
            entity_type="artist"
        )
        
        print(f"✅ Scoring completed and stored!")
        print(f"   Run ID: {result.metadata.get('run_id', 'N/A')}")
        print(f"   Results: {len(result.entity_scores)} artist scores")
        
        # Show results
        scores_df = result.entity_scores.sort_values("score_value", ascending=False)
        print(f"\n📈 Top Artists by Momentum Score:")
        display_cols = ["entity_id", "score_value", "confidence", "momentum_category"]
        print(scores_df[display_cols].to_string(index=False))
        
        return result.metadata.get('run_id')
        
    except Exception as e:
        print(f"❌ Error in scoring demo: {e}")
        return None


def demo_storage_queries(run_id=None):
    """Demonstrate querying stored scoring results."""
    print("\n" + "="*60)
    print("SCORING STORAGE QUERIES DEMO")
    print("="*60)
    
    storage = ScoringStorage()
    
    try:
        # Get latest scores
        print("📊 Latest Scoring Results:")
        latest_scores = storage.get_latest_scores(
            algorithm_name="artist_momentum_scorer",
            entity_type="artist",
            limit=10
        )
        
        if not latest_scores.empty:
            print(latest_scores[["entity_id", "score_value", "confidence_level", "calculation_timestamp"]].to_string(index=False))
        else:
            print("No scoring results found.")
        
        # Get algorithm performance
        print(f"\n🔧 Algorithm Performance:")
        performance = storage.get_algorithm_performance("artist_momentum_scorer")
        
        if not performance.empty:
            print(performance[["algorithm_name", "total_runs", "total_results", "overall_avg_score"]].to_string(index=False))
        else:
            print("No performance data found.")
        
        # Get entity rankings
        print(f"\n🏆 Artist Rankings:")
        rankings = storage.get_entity_rankings(
            algorithm_name="artist_momentum_scorer",
            entity_type="artist",
            limit=5
        )
        
        if not rankings.empty:
            print(rankings[["entity_id", "score_value", "ranking", "confidence_level"]].to_string(index=False))
        else:
            print("No ranking data found.")
        
        # Get scoring history for top artist
        if not latest_scores.empty:
            top_artist = latest_scores.iloc[0]["entity_id"]
            print(f"\n📈 Scoring History for {top_artist}:")
            
            history = storage.get_scoring_history(
                entity_id=top_artist,
                entity_type="artist",
                algorithm_name="artist_momentum_scorer",
                days_back=30
            )
            
            if not history.empty:
                print(history[["calculation_timestamp", "score_value", "confidence_level"]].head().to_string(index=False))
            else:
                print("No historical data found.")
        
    except Exception as e:
        print(f"❌ Error in storage queries: {e}")


def demo_multiple_algorithms():
    """Demonstrate storing results from multiple algorithms."""
    print("\n" + "="*60)
    print("MULTIPLE ALGORITHMS STORAGE DEMO")
    print("="*60)
    
    engine = ScoringEngine(enable_storage=True)
    storage = ScoringStorage()
    
    # Register multiple plugins
    plugins = [
        ArtistMomentumScoringPlugin(),
        EngagementScoringPlugin(),
        GrowthPotentialScoringPlugin()
    ]
    
    for plugin in plugins:
        engine.register_plugin(plugin)
    
    # Create sample data for each algorithm
    sample_artists = ["Artist A", "Artist B", "Artist C"]
    
    try:
        # Momentum scoring data
        momentum_data = pd.DataFrame({
            "artist_name": sample_artists * 3,
            "video_id": [f"vid_{i}" for i in range(9)],
            "published_at": [datetime.now() - timedelta(days=i*5) for i in range(9)],
            "view_count": [1000 + i*500 for i in range(9)],
            "like_count": [10 + i*5 for i in range(9)],
            "comment_count": [5 + i*2 for i in range(9)],
            "channel_title": sample_artists * 3,
            "metrics_date": [datetime.now() - timedelta(days=i*5) for i in range(9)]
        })
        
        # Engagement scoring data
        engagement_data = pd.DataFrame({
            "video_id": [f"vid_{i}" for i in range(6)],
            "view_count": [2000 + i*300 for i in range(6)],
            "like_count": [20 + i*10 for i in range(6)],
            "comment_count": [10 + i*5 for i in range(6)],
            "avg_sentiment": [0.1 + i*0.1 for i in range(6)],
            "sentiment_magnitude": [0.5 + i*0.05 for i in range(6)]
        })
        
        # Growth potential data
        growth_data = pd.DataFrame({
            "artist_name": ["Artist A"] * 10,
            "video_id": ["vid_growth"] * 10,
            "metrics_date": [datetime.now().date() - timedelta(days=i) for i in range(10)],
            "view_count": [1000 + i*100 for i in range(10)],
            "like_count": [10 + i*2 for i in range(10)],
            "comment_count": [5 + i*1 for i in range(10)]
        })
        
        # Execute each algorithm
        algorithms_data = [
            ("artist_momentum_scorer", momentum_data, "artist"),
            ("engagement_scorer", engagement_data, "video"),
            ("growth_potential_scorer", growth_data, "artist")
        ]
        
        run_ids = []
        
        for algorithm_name, data, entity_type in algorithms_data:
            print(f"🔄 Running {algorithm_name}...")
            
            result = engine.execute_scoring(
                algorithm_name,
                data,
                store_results=True,
                entity_type=entity_type
            )
            
            run_id = result.metadata.get('run_id')
            run_ids.append(run_id)
            
            print(f"   ✅ Stored {len(result.entity_scores)} results with run_id: {run_id}")
        
        # Show algorithm performance summary
        print(f"\n📊 Algorithm Performance Summary:")
        performance = storage.get_algorithm_performance()
        
        if not performance.empty:
            print(performance[["algorithm_name", "total_runs", "total_results", "overall_avg_score"]].to_string(index=False))
        
        return run_ids
        
    except Exception as e:
        print(f"❌ Error in multiple algorithms demo: {e}")
        return []


def demo_storage_maintenance():
    """Demonstrate storage maintenance operations."""
    print("\n" + "="*60)
    print("STORAGE MAINTENANCE DEMO")
    print("="*60)
    
    storage = ScoringStorage()
    
    try:
        # Show current storage status
        print("📊 Current Storage Status:")
        
        # Get all algorithms
        performance = storage.get_algorithm_performance()
        if not performance.empty:
            total_runs = performance["total_runs"].sum()
            total_results = performance["total_results"].sum()
            print(f"   Total algorithms: {len(performance)}")
            print(f"   Total runs: {total_runs}")
            print(f"   Total results: {total_results}")
        else:
            print("   No data in storage")
        
        # Demonstrate cleanup (but don't actually delete recent data)
        print(f"\n🧹 Storage Cleanup Simulation:")
        print("   (This would clean up results older than 90 days)")
        
        # In a real scenario, you might run:
        # deleted_count = storage.cleanup_old_results(days_to_keep=90)
        # print(f"   Deleted {deleted_count} old records")
        
        print("   Cleanup simulation complete")
        
    except Exception as e:
        print(f"❌ Error in maintenance demo: {e}")


def main():
    """Run all scoring storage system demos."""
    print("🎯 Scoring Results Storage System Demo")
    print("This demo shows the complete scoring storage workflow.")
    
    # Check schema first
    if not check_scoring_schema():
        return
    
    try:
        # Demo 1: Basic scoring with storage
        run_id = demo_scoring_with_storage()
        
        # Demo 2: Storage queries
        demo_storage_queries(run_id)
        
        # Demo 3: Multiple algorithms
        demo_multiple_algorithms()
        
        # Demo 4: Storage maintenance
        demo_storage_maintenance()
        
        print("\n" + "="*60)
        print("🎉 ALL DEMOS COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("The scoring storage system is working correctly!")
        print("You can now:")
        print("  - Store scoring results automatically")
        print("  - Query historical scoring data")
        print("  - Analyze algorithm performance")
        print("  - Track scoring trends over time")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        print("Make sure the database is set up and scoring tables are created.")


if __name__ == "__main__":
    main()