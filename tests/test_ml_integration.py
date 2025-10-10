"""
Integration tests for ML Analytics with existing YouTube data pipeline.

Tests the integration between ML analytics, content categorization, and existing
YouTube helpers to ensure seamless workflow for music industry applications.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


class TestMLIntegration:
    """Test ML analytics integration with existing systems"""

    @pytest.fixture
    def integrated_youtube_data(self):
        """Sample YouTube data that integrates multiple data sources"""
        np.random.seed(42)

        # Simulate realistic YouTube data with multiple dimensions
        artists = ["Lil Wayne", "Drake", "Kendrick Lamar", "J. Cole", "Future"]
        content_types = ["Music Video", "Lyric Video", "Behind Scenes", "Live Performance"]

        data = []
        base_date = datetime(2024, 1, 1)

        for i in range(200):  # 200 data points
            artist = np.random.choice(artists)
            content_type = np.random.choice(content_types)
            date = base_date + timedelta(days=i % 90)

            # Simulate realistic performance metrics
            base_views = np.random.normal(75000, 25000)
            engagement_multiplier = {
                "Music Video": 1.2,
                "Lyric Video": 0.9,
                "Behind Scenes": 0.7,
                "Live Performance": 1.1,
            }[content_type]

            views = max(1000, int(base_views * engagement_multiplier))
            likes = max(10, int(views * np.random.normal(0.05, 0.02)))
            comments = max(5, int(views * np.random.normal(0.01, 0.005)))

            data.append(
                {
                    "artist_name": artist,
                    "title": f"{artist} - Song Title {i}",
                    "channel_title": f"{artist} Official",
                    "video_type": content_type,
                    "content_type": content_type,
                    "date": date,
                    "daily_views": views,
                    "views": views,
                    "daily_likes": likes,
                    "likes": likes,
                    "daily_comments": comments,
                    "comments": comments,
                    "engagement_rate": (likes + comments) / views,
                    "subscriber_growth": np.random.normal(50, 20),
                    "has_isrc": content_type in ["Music Video", "Lyric Video"],
                    "genre": np.random.choice(["Hip-Hop", "R&B", "Pop"]),
                    "duration_seconds": np.random.randint(120, 300),
                }
            )

        return pd.DataFrame(data)

    def test_end_to_end_ml_pipeline(self, integrated_youtube_data):
        """Test complete ML pipeline from data ingestion to insights"""
        from youtubeviz.content import analyze_isrc_vs_content_balance, categorize_video_content
        from youtubeviz.ml_analytics import (
            calculate_viral_potential,
            create_artist_performance_model,
            generate_content_optimization_recommendations,
            predict_artist_momentum,
        )

        df = integrated_youtube_data

        # Step 1: Content categorization
        categorized_df = categorize_video_content(df, title_col="title", description_col=None)

        assert "content_category" in categorized_df.columns

        # Step 2: ISRC analysis
        isrc_analysis = analyze_isrc_vs_content_balance(
            df, artist_col="artist_name", isrc_col="has_isrc", views_col="views"
        )

        assert "isrc_analysis" in isrc_analysis
        assert "content_analysis" in isrc_analysis

        # Step 3: Momentum prediction
        momentum_predictions = predict_artist_momentum(
            df, artist_col="artist_name", metrics_cols=["daily_views", "engagement_rate"], prediction_horizon_days=30
        )

        assert len(momentum_predictions) == df["artist_name"].nunique()
        assert "momentum_score" in momentum_predictions.columns

        # Step 4: Content optimization
        content_recommendations = generate_content_optimization_recommendations(
            df,
            artist_col="artist_name",
            content_type_col="content_type",
            performance_metrics=["daily_views", "engagement_rate"],
        )

        assert isinstance(content_recommendations, dict)
        assert len(content_recommendations) == df["artist_name"].nunique()

        # Step 5: Viral potential
        viral_scores = calculate_viral_potential(
            df, artist_col="artist_name", engagement_metrics=["daily_likes", "daily_comments"], velocity_window_days=7
        )

        assert len(viral_scores) == df["artist_name"].nunique()
        assert "viral_score" in viral_scores.columns

        # Step 6: Performance model
        performance_model = create_artist_performance_model(
            df,
            artist_col="artist_name",
            target_metric="daily_views",
            feature_cols=["engagement_rate", "subscriber_growth"],
            model_type="random_forest",
        )

        assert "model" in performance_model
        assert "performance" in performance_model
        assert "predictions" in performance_model

    def test_ml_with_youtube_helpers_integration(self, integrated_youtube_data):
        """Test ML analytics integration with YouTube helper functions"""
        from youtubeviz.ml_analytics import integrate_with_youtube_helpers

        df = integrated_youtube_data

        # Integrate with YouTube helpers
        enhanced_df = integrate_with_youtube_helpers(df, title_col="title", channel_col="channel_title")

        # Should have additional ML-derived features
        expected_features = ["title_length", "title_word_count", "has_version_info"]

        for feature in expected_features:
            assert feature in enhanced_df.columns

        # Features should have reasonable values
        assert enhanced_df["title_length"].min() >= 0
        assert enhanced_df["title_word_count"].min() >= 0
        assert enhanced_df["has_version_info"].dtype == bool

    def test_business_intelligence_workflow(self, integrated_youtube_data):
        """Test business intelligence and ROI optimization workflow"""
        from youtubeviz.ml_analytics import (
            benchmark_performance,
            calculate_investment_priorities,
            optimize_marketing_roi,
        )

        df = integrated_youtube_data

        # Create business metrics
        business_df = df.groupby("artist_name").agg({"daily_views": "sum", "engagement_rate": "mean"}).reset_index()

        # Add financial metrics
        business_df["monthly_revenue"] = business_df["daily_views"] * 0.001  # Simplified
        business_df["marketing_spend"] = business_df["monthly_revenue"] * 0.2
        business_df["roi"] = business_df["monthly_revenue"] / business_df["marketing_spend"]

        # ROI optimization
        roi_optimization = optimize_marketing_roi(
            business_df,
            artist_col="artist_name",
            revenue_col="monthly_revenue",
            spend_col="marketing_spend",
            target_total_budget=50000,
        )

        assert "optimal_allocation" in roi_optimization
        assert "expected_roi" in roi_optimization

        # Performance benchmarking
        benchmarks = benchmark_performance(
            business_df, artist_col="artist_name", metrics_cols=["monthly_revenue", "roi"]
        )

        assert "percentile_rank" in benchmarks.columns
        assert "performance_tier" in benchmarks.columns

        # Investment priorities
        priorities = calculate_investment_priorities(
            business_df, artist_col="artist_name", performance_metrics=["monthly_revenue", "roi"]
        )

        assert "priority_score" in priorities.columns
        assert "investment_recommendation" in priorities.columns

    def test_advanced_analytics_pipeline(self, integrated_youtube_data):
        """Test advanced analytics including clustering and anomaly detection"""
        from youtubeviz.ml_analytics import (
            detect_performance_anomalies,
            perform_advanced_clustering,
            perform_statistical_tests,
        )

        df = integrated_youtube_data

        # Advanced clustering
        clustering_results = perform_advanced_clustering(
            df,
            artist_col="artist_name",
            feature_cols=["daily_views", "engagement_rate", "subscriber_growth"],
            clustering_method="kmeans",
        )

        assert "cluster_labels" in clustering_results
        assert "cluster_analysis" in clustering_results
        assert "silhouette_score" in clustering_results

        # Anomaly detection
        anomalies = detect_performance_anomalies(
            df, artist_col="artist_name", metrics_cols=["daily_views", "engagement_rate"], sensitivity=0.05
        )

        assert isinstance(anomalies, pd.DataFrame)
        if len(anomalies) > 0:
            assert "anomaly_type" in anomalies.columns
            assert "anomaly_score" in anomalies.columns

        # Statistical testing
        stat_tests = perform_statistical_tests(
            df, group_col="artist_name", metric_cols=["daily_views", "engagement_rate"], test_type="anova"
        )

        assert isinstance(stat_tests, dict)
        for metric in ["daily_views", "engagement_rate"]:
            if metric in stat_tests:
                assert "p_value" in stat_tests[metric]
                assert "significance" in stat_tests[metric]

    def test_music_industry_specific_features(self, integrated_youtube_data):
        """Test music industry specific ML features and insights"""
        from youtubeviz.content import (
            analyze_genre_context,
            check_isrc_compliance,
            generate_content_strategy_recommendations,
        )
        from youtubeviz.ml_analytics import detect_viral_content_patterns

        df = integrated_youtube_data

        # Viral content pattern detection
        viral_patterns = detect_viral_content_patterns(
            df,
            artist_col="artist_name",
            content_type_col="content_type",
            engagement_metrics=["daily_likes", "daily_comments"],
            time_col="date",
        )

        assert "content_type_patterns" in viral_patterns
        assert "temporal_patterns" in viral_patterns
        assert "engagement_patterns" in viral_patterns

        # Genre context analysis
        genre_analysis = analyze_genre_context(
            df, artist_col="artist_name", genre_col="genre", performance_col="daily_views"
        )

        assert "genre_analysis" in genre_analysis
        assert "new_signee_context" in genre_analysis

        # ISRC compliance
        compliance_check = check_isrc_compliance(
            df, artist_col="artist_name", isrc_col="has_isrc", content_type_col="content_type"
        )

        assert "artist_compliance" in compliance_check
        assert "overall_compliance_rate" in compliance_check

        # Content strategy recommendations
        strategy_recs = generate_content_strategy_recommendations(
            df, artist_col="artist_name", content_type_col="content_type", performance_col="daily_views"
        )

        assert isinstance(strategy_recs, dict)
        for artist in df["artist_name"].unique():
            if artist in strategy_recs:
                assert isinstance(strategy_recs[artist], list)

    def test_scalability_and_performance(self, integrated_youtube_data):
        """Test ML analytics scalability with larger datasets"""
        from youtubeviz.ml_analytics import predict_artist_momentum

        # Create larger dataset
        large_df = pd.concat([integrated_youtube_data] * 5, ignore_index=True)

        # Test that ML functions can handle larger datasets efficiently
        start_time = pd.Timestamp.now()

        momentum_predictions = predict_artist_momentum(
            large_df,
            artist_col="artist_name",
            metrics_cols=["daily_views", "engagement_rate"],
            prediction_horizon_days=30,
        )

        end_time = pd.Timestamp.now()
        processing_time = (end_time-start_time).total_seconds()

        # Should complete within reasonable time (adjust threshold as needed)
        assert processing_time < 30  # 30 seconds max
        assert len(momentum_predictions) == large_df["artist_name"].nunique()

        # Results should be consistent regardless of dataset size
        assert all(0 <= score <= 1 for score in momentum_predictions["momentum_score"])
        assert all(0 <= prob <= 1 for prob in momentum_predictions["breakthrough_probability"])
