"""
Test suite for ML Analytics and Advanced Statistical Analysis

This module tests machine learning integration examples with real music industry
applications, sophisticated statistical analysis, and business intelligence insights.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


class TestMusicIndustryMLAnalytics:
    """Test suite for music industry ML analytics"""

    @pytest.fixture
    def artist_performance_data(self):
        """Sample artist performance data for ML testing"""
        np.random.seed(42)  # For reproducible tests

        artists = ["Artist A", "Artist B", "Artist C", "Artist D", "Artist E"]
        dates = pd.date_range("2024-01-01", periods=90, freq="D")

        data = []
        for artist in artists:
            for date in dates:
                # Simulate realistic music industry metrics
                base_views = np.random.normal(50000, 15000)
                seasonal_factor = 1 + 0.3 * np.sin(2 * np.pi * date.dayofyear / 365)
                trend_factor = 1 + 0.001 * (date-dates[0]).days

                data.append(
                    {
                        "artist_name": artist,
                        "date": date,
                        "daily_views": max(0, int(base_views * seasonal_factor * trend_factor)),
                        "daily_likes": max(0, int(base_views * 0.05 * seasonal_factor)),
                        "daily_comments": max(0, int(base_views * 0.01 * seasonal_factor)),
                        "daily_shares": max(0, int(base_views * 0.02 * seasonal_factor)),
                        "subscriber_growth": np.random.normal(100, 50),
                        "engagement_rate": np.random.normal(0.05, 0.02),
                        "genre": np.random.choice(["Hip-Hop", "R&B", "Pop", "Alternative"]),
                        "content_type": np.random.choice(["Music Video", "Lyric Video", "Behind Scenes"]),
                    }
                )

        return pd.DataFrame(data)

    @pytest.fixture
    def market_data(self):
        """Sample market and industry data"""
        return pd.DataFrame(
            [
                {"metric": "industry_avg_engagement", "value": 0.045, "category": "benchmark"},
                {"metric": "top_10_percent_threshold", "value": 0.08, "category": "benchmark"},
                {"metric": "viral_threshold_views", "value": 1000000, "category": "benchmark"},
                {"metric": "playlist_placement_boost", "value": 1.5, "category": "multiplier"},
                {"metric": "algorithm_boost_factor", "value": 2.0, "category": "multiplier"},
            ]
        )

    def test_momentum_prediction_model(self, artist_performance_data):
        """Test ML model for predicting artist momentum and breakthrough potential"""
        from youtubeviz.ml_analytics import predict_artist_momentum

        # Should predict momentum scores for each artist
        momentum_predictions = predict_artist_momentum(
            df=artist_performance_data,
            artist_col="artist_name",
            metrics_cols=["daily_views", "daily_likes", "engagement_rate"],
            prediction_horizon_days=30,
        )

        # Should return predictions for each artist
        assert isinstance(momentum_predictions, pd.DataFrame)
        assert "artist_name" in momentum_predictions.columns
        assert "momentum_score" in momentum_predictions.columns
        assert "breakthrough_probability" in momentum_predictions.columns
        assert "predicted_growth_rate" in momentum_predictions.columns

        # Momentum scores should be between 0 and 1
        assert all(0 <= score <= 1 for score in momentum_predictions["momentum_score"])

        # Should have predictions for all artists
        unique_artists = artist_performance_data["artist_name"].unique()
        assert len(momentum_predictions) == len(unique_artists)

    def test_content_optimization_recommendations(self, artist_performance_data):
        """Test ML-powered content optimization recommendations"""
        from youtubeviz.ml_analytics import generate_content_optimization_recommendations

        recommendations = generate_content_optimization_recommendations(
            df=artist_performance_data,
            artist_col="artist_name",
            content_type_col="content_type",
            performance_metrics=["daily_views", "engagement_rate"],
        )

        # Should return actionable recommendations
        assert isinstance(recommendations, dict)

        # Should have recommendations for each artist
        for artist in artist_performance_data["artist_name"].unique():
            assert artist in recommendations
            artist_recs = recommendations[artist]

            # Should include optimization strategies
            assert "optimal_content_mix" in artist_recs
            assert "posting_schedule" in artist_recs
            assert "engagement_strategies" in artist_recs
            assert "growth_potential" in artist_recs

    def test_market_positioning_analysis(self, artist_performance_data, market_data):
        """Test market positioning and competitive analysis"""
        from youtubeviz.ml_analytics import analyze_market_positioning

        positioning = analyze_market_positioning(
            artist_data=artist_performance_data, market_data=market_data, artist_col="artist_name", genre_col="genre"
        )

        # Should return positioning analysis
        assert isinstance(positioning, dict)
        assert "market_segments" in positioning
        assert "competitive_landscape" in positioning
        assert "positioning_recommendations" in positioning

        # Should analyze each artist's market position
        for artist in artist_performance_data["artist_name"].unique():
            assert artist in positioning["competitive_landscape"]

    def test_viral_potential_scoring(self, artist_performance_data):
        """Test viral potential scoring algorithm"""
        from youtubeviz.ml_analytics import calculate_viral_potential

        viral_scores = calculate_viral_potential(
            df=artist_performance_data,
            artist_col="artist_name",
            engagement_metrics=["daily_likes", "daily_comments", "daily_shares"],
            velocity_window_days=7,
        )

        # Should return viral potential scores
        assert isinstance(viral_scores, pd.DataFrame)
        assert "artist_name" in viral_scores.columns
        assert "viral_score" in viral_scores.columns
        assert "velocity_score" in viral_scores.columns
        assert "engagement_quality" in viral_scores.columns

        # Scores should be normalized
        assert all(0 <= score <= 1 for score in viral_scores["viral_score"])

    def test_audience_segmentation_clustering(self, artist_performance_data):
        """Test ML-based audience segmentation and clustering"""
        from youtubeviz.ml_analytics import perform_audience_segmentation

        segments = perform_audience_segmentation(
            df=artist_performance_data,
            artist_col="artist_name",
            behavioral_features=["engagement_rate", "daily_views", "subscriber_growth"],
            n_segments=3,
        )

        # Should return segmentation results
        assert isinstance(segments, dict)
        assert "segment_profiles" in segments
        assert "artist_segments" in segments
        assert "segment_characteristics" in segments

        # Should have the requested number of segments
        assert len(segments["segment_profiles"]) == 3

    def test_trend_forecasting_model(self, artist_performance_data):
        """Test time series forecasting for trend prediction"""
        from youtubeviz.ml_analytics import forecast_performance_trends

        forecasts = forecast_performance_trends(
            df=artist_performance_data,
            artist_col="artist_name",
            date_col="date",
            target_metrics=["daily_views", "engagement_rate"],
            forecast_days=30,
        )

        # Should return forecasts for each artist and metric
        assert isinstance(forecasts, dict)

        for artist in artist_performance_data["artist_name"].unique():
            assert artist in forecasts
            artist_forecast = forecasts[artist]

            # Should have forecasts for each target metric
            assert "daily_views" in artist_forecast
            assert "engagement_rate" in artist_forecast

            # Should include confidence intervals
            for metric in ["daily_views", "engagement_rate"]:
                metric_forecast = artist_forecast[metric]
                assert "forecast" in metric_forecast
                assert "confidence_lower" in metric_forecast
                assert "confidence_upper" in metric_forecast

    def test_anomaly_detection_system(self, artist_performance_data):
        """Test anomaly detection for unusual performance patterns"""
        from youtubeviz.ml_analytics import detect_performance_anomalies

        anomalies = detect_performance_anomalies(
            df=artist_performance_data,
            artist_col="artist_name",
            metrics_cols=["daily_views", "engagement_rate"],
            sensitivity=0.05,
        )

        # Should return anomaly detection results
        assert isinstance(anomalies, pd.DataFrame)
        assert "artist_name" in anomalies.columns
        assert "date" in anomalies.columns
        assert "anomaly_type" in anomalies.columns
        assert "anomaly_score" in anomalies.columns
        assert "affected_metric" in anomalies.columns

        # Anomaly scores should be meaningful
        if len(anomalies) > 0:
            assert all(score > 0 for score in anomalies["anomaly_score"])


class TestStatisticalAnalysis:
    """Test suite for sophisticated statistical analysis"""

    @pytest.fixture
    def performance_metrics_data(self):
        """Sample performance metrics for statistical analysis"""
        np.random.seed(42)

        return pd.DataFrame(
            {
                "artist_name": ["Artist A"] * 100 + ["Artist B"] * 100 + ["Artist C"] * 100,
                "views": np.concatenate(
                    [
                        np.random.normal(100000, 20000, 100),  # Artist A
                        np.random.normal(150000, 30000, 100),  # Artist B
                        np.random.normal(80000, 15000, 100),  # Artist C
                    ]
                ),
                "engagement_rate": np.concatenate(
                    [
                        np.random.normal(0.05, 0.01, 100),  # Artist A
                        np.random.normal(0.07, 0.015, 100),  # Artist B
                        np.random.normal(0.04, 0.008, 100),  # Artist C
                    ]
                ),
                "genre": ["Hip-Hop"] * 100 + ["R&B"] * 100 + ["Pop"] * 100,
                "release_week": np.tile(range(1, 101), 3),
            }
        )

    def test_statistical_significance_testing(self, performance_metrics_data):
        """Test statistical significance testing for A / B comparisons"""
        from youtubeviz.ml_analytics import perform_statistical_tests

        test_results = perform_statistical_tests(
            df=performance_metrics_data,
            group_col="artist_name",
            metric_cols=["views", "engagement_rate"],
            test_type="anova",
        )

        # Should return statistical test results
        assert isinstance(test_results, dict)

        for metric in ["views", "engagement_rate"]:
            assert metric in test_results
            metric_results = test_results[metric]

            # Should include test statistics
            assert "test_statistic" in metric_results
            assert "p_value" in metric_results
            assert "effect_size" in metric_results
            assert "significance" in metric_results

            # P-values should be between 0 and 1
            assert 0 <= metric_results["p_value"] <= 1

    def test_correlation_analysis(self, performance_metrics_data):
        """Test correlation analysis between performance metrics"""
        from youtubeviz.ml_analytics import analyze_metric_correlations

        correlations = analyze_metric_correlations(
            df=performance_metrics_data, metric_cols=["views", "engagement_rate", "release_week"], method="pearson"
        )

        # Should return correlation analysis
        assert isinstance(correlations, dict)
        assert "correlation_matrix" in correlations
        assert "significant_correlations" in correlations
        assert "correlation_insights" in correlations

        # Correlation matrix should be square
        corr_matrix = correlations["correlation_matrix"]
        assert corr_matrix.shape[0] == corr_matrix.shape[1]

    def test_distribution_analysis(self, performance_metrics_data):
        """Test distribution analysis and normality testing"""
        from youtubeviz.ml_analytics import analyze_metric_distributions

        distributions = analyze_metric_distributions(
            df=performance_metrics_data, metric_cols=["views", "engagement_rate"], group_col="artist_name"
        )

        # Should return distribution analysis
        assert isinstance(distributions, dict)

        for metric in ["views", "engagement_rate"]:
            assert metric in distributions
            metric_dist = distributions[metric]

            # Should include distribution statistics
            assert "normality_test" in metric_dist
            assert "descriptive_stats" in metric_dist
            assert "outlier_analysis" in metric_dist

    def test_confidence_intervals(self, performance_metrics_data):
        """Test confidence interval calculations"""
        from youtubeviz.ml_analytics import calculate_confidence_intervals

        intervals = calculate_confidence_intervals(
            df=performance_metrics_data,
            metric_cols=["views", "engagement_rate"],
            group_col="artist_name",
            confidence_level=0.95,
        )

        # Should return confidence intervals
        assert isinstance(intervals, pd.DataFrame)
        assert "artist_name" in intervals.columns
        assert "metric" in intervals.columns
        assert "mean" in intervals.columns
        assert "ci_lower" in intervals.columns
        assert "ci_upper" in intervals.columns

        # Confidence intervals should be properly ordered
        assert all(intervals["ci_lower"] <= intervals["mean"])
        assert all(intervals["mean"] <= intervals["ci_upper"])


class TestBusinessIntelligenceInsights:
    """Test suite for business intelligence and actionable insights"""

    @pytest.fixture
    def business_metrics_data(self):
        """Sample business metrics data"""
        return pd.DataFrame(
            [
                {"artist_name": "Artist A", "monthly_revenue": 50000, "marketing_spend": 10000, "roi": 5.0},
                {"artist_name": "Artist B", "monthly_revenue": 75000, "marketing_spend": 15000, "roi": 5.0},
                {"artist_name": "Artist C", "monthly_revenue": 30000, "marketing_spend": 8000, "roi": 3.75},
                {"artist_name": "Artist D", "monthly_revenue": 90000, "marketing_spend": 12000, "roi": 7.5},
                {"artist_name": "Artist E", "monthly_revenue": 40000, "marketing_spend": 6000, "roi": 6.67},
            ]
        )

    def test_roi_optimization_analysis(self, business_metrics_data):
        """Test ROI optimization and budget allocation recommendations"""
        from youtubeviz.ml_analytics import optimize_marketing_roi

        optimization = optimize_marketing_roi(
            df=business_metrics_data,
            artist_col="artist_name",
            revenue_col="monthly_revenue",
            spend_col="marketing_spend",
            target_total_budget=60000,
        )

        # Should return optimization recommendations
        assert isinstance(optimization, dict)
        assert "optimal_allocation" in optimization
        assert "expected_roi" in optimization
        assert "budget_recommendations" in optimization

        # Budget allocation should sum to target
        total_allocated = sum(optimization["optimal_allocation"].values())
        assert abs(total_allocated-60000) < 1000  # Allow small rounding differences

    def test_performance_benchmarking(self, business_metrics_data):
        """Test performance benchmarking against industry standards"""
        from youtubeviz.ml_analytics import benchmark_performance

        benchmarks = benchmark_performance(
            df=business_metrics_data,
            artist_col="artist_name",
            metrics_cols=["monthly_revenue", "roi"],
            benchmark_type="industry_percentiles",
        )

        # Should return benchmarking results
        assert isinstance(benchmarks, pd.DataFrame)
        assert "artist_name" in benchmarks.columns
        assert "performance_tier" in benchmarks.columns
        assert "percentile_rank" in benchmarks.columns

        # Percentile ranks should be between 0 and 100
        assert all(0 <= rank <= 100 for rank in benchmarks["percentile_rank"])

    def test_investment_priority_scoring(self, business_metrics_data):
        """Test investment priority scoring for artist development"""
        from youtubeviz.ml_analytics import calculate_investment_priorities

        priorities = calculate_investment_priorities(
            df=business_metrics_data,
            artist_col="artist_name",
            performance_metrics=["monthly_revenue", "roi"],
            growth_potential_weight=0.4,
            current_performance_weight=0.6,
        )

        # Should return priority scores
        assert isinstance(priorities, pd.DataFrame)
        assert "artist_name" in priorities.columns
        assert "priority_score" in priorities.columns
        assert "investment_recommendation" in priorities.columns
        assert "reasoning" in priorities.columns

        # Priority scores should be normalized
        assert all(0 <= score <= 1 for score in priorities["priority_score"])

    def test_market_opportunity_identification(self, business_metrics_data):
        """Test market opportunity identification and gap analysis"""
        from youtubeviz.ml_analytics import identify_market_opportunities

        opportunities = identify_market_opportunities(
            df=business_metrics_data,
            artist_col="artist_name",
            performance_metrics=["monthly_revenue", "roi"],
            market_size_estimate=1000000,
        )

        # Should return market opportunities
        assert isinstance(opportunities, dict)
        assert "opportunity_gaps" in opportunities
        assert "market_potential" in opportunities
        assert "strategic_recommendations" in opportunities

        # Should identify specific opportunities
        assert len(opportunities["opportunity_gaps"]) > 0
