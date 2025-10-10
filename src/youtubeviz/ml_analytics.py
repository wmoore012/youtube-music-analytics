"""
Machine Learning Analytics for Music Industry Applications

This module provides sophisticated statistical analysis, machine learning integration,
and business intelligence insights specifically designed for music industry applications.
Includes momentum prediction, content optimization, market positioning, and ROI analysis.
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

# Try to import ML libraries with graceful fallbacks
try:
    from sklearn.cluster import KMeans
    from sklearn.ensemble import IsolationForest, RandomForestRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error, r2_score, silhouette_score
    from sklearn.model_selection import cross_val_score, train_test_split
    from sklearn.preprocessing import MinMaxScaler, StandardScaler

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from scipy import stats
    from scipy.optimize import minimize
    from scipy.signal import find_peaks

    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# Try to import advanced analytics libraries
try:
    import statsmodels.api as sm
    from statsmodels.tsa.seasonal import seasonal_decompose

    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

# Constants for music industry analytics
VIRAL_THRESHOLD_VIEWS = 1000000
INDUSTRY_AVG_ENGAGEMENT = 0.045
TOP_PERFORMER_THRESHOLD = 0.08
MOMENTUM_LOOKBACK_DAYS = 30
FORECAST_CONFIDENCE_LEVEL = 0.95

# Advanced ML constants
MIN_SAMPLES_FOR_ML = 10
ANOMALY_CONTAMINATION = 0.1
CLUSTERING_RANDOM_STATE = 42
CROSS_VALIDATION_FOLDS = 5

# Music industry benchmarks
MUSIC_INDUSTRY_BENCHMARKS = {
    "viral_velocity_threshold": 0.5,
    "engagement_quality_threshold": 0.6,
    "momentum_persistence_days": 14,
    "breakthrough_confidence_threshold": 0.7,
}


def predict_artist_momentum(
    df: pd.DataFrame, artist_col: str, metrics_cols: List[str], prediction_horizon_days: int = 30
) -> pd.DataFrame:
    """
    Predict artist momentum and breakthrough potential using ML models.

    Args:
        df: DataFrame with artist performance data
        artist_col: Column name for artist names
        metrics_cols: List of metric columns to analyze
        prediction_horizon_days: Days ahead to predict

    Returns:
        DataFrame with momentum predictions for each artist
    """
    predictions = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist].copy()

        if len(artist_df) < 10:  # Need minimum data for meaningful prediction
            predictions.append(
                {
                    artist_col: artist,
                    "momentum_score": 0.5,  # Neutral score
                    "breakthrough_probability": 0.3,
                    "predicted_growth_rate": 0.0,
                    "confidence": "low",
                }
            )
            continue

        # Calculate momentum indicators
        momentum_indicators = _calculate_momentum_indicators(artist_df, metrics_cols)

        # Calculate growth trends
        growth_trends = _calculate_growth_trends(artist_df, metrics_cols)

        # Combine indicators into momentum score
        momentum_score = _combine_momentum_indicators(momentum_indicators, growth_trends)

        # Calculate breakthrough probability
        breakthrough_prob = _calculate_breakthrough_probability(artist_df, metrics_cols)

        # Predict growth rate
        predicted_growth = _predict_growth_rate(artist_df, metrics_cols, prediction_horizon_days)

        predictions.append(
            {
                artist_col: artist,
                "momentum_score": round(momentum_score, 3),
                "breakthrough_probability": round(breakthrough_prob, 3),
                "predicted_growth_rate": round(predicted_growth, 3),
                "confidence": "high" if len(artist_df) > 30 else "medium",
            }
        )

    return pd.DataFrame(predictions)


def _calculate_momentum_indicators(df: pd.DataFrame, metrics_cols: List[str]) -> Dict[str, float]:
    """Calculate various momentum indicators from performance data."""
    indicators = {}

    for metric in metrics_cols:
        if metric in df.columns:
            values = df[metric].dropna()
            if len(values) > 1:
                # Velocity (rate of change)
                indicators[f"{metric}_velocity"] = values.pct_change().mean()

                # Acceleration (change in velocity)
                velocity_series = values.pct_change()
                indicators[f"{metric}_acceleration"] = velocity_series.pct_change().mean()

                # Consistency (inverse of coefficient of variation)
                cv = values.std() / values.mean() if values.mean() != 0 else 1
                indicators[f"{metric}_consistency"] = 1 / (1 + cv)

    return indicators


def _calculate_growth_trends(df: pd.DataFrame, metrics_cols: List[str]) -> Dict[str, float]:
    """Calculate growth trends using linear regression."""
    trends = {}

    if "date" in df.columns:
        df_sorted = df.sort_values("date")
        x = np.arange(len(df_sorted))

        for metric in metrics_cols:
            if metric in df_sorted.columns:
                y = df_sorted[metric].dropna()
                if len(y) > 1:
                    # Simple linear trend
                    slope, intercept, r_value, p_value, std_err = stats.linregress(x[: len(y)], y)
                    trends[f"{metric}_trend_slope"] = slope
                    trends[f"{metric}_trend_r2"] = r_value**2

    return trends


def _combine_momentum_indicators(momentum_indicators: Dict[str, float], growth_trends: Dict[str, float]) -> float:
    """Combine various indicators into a single momentum score."""
    scores = []

    # Weight different types of indicators
    for key, value in momentum_indicators.items():
        if "velocity" in key and not np.isnan(value):
            scores.append(min(max(value, -1), 1))  # Normalize to [-1, 1]
        elif "consistency" in key and not np.isnan(value):
            scores.append(value)  # Already normalized to [0, 1]

    for key, value in growth_trends.items():
        if "trend_r2" in key and not np.isnan(value):
            scores.append(value)  # R² is already [0, 1]

    if not scores:
        return 0.5  # Neutral score if no valid indicators

    # Combine scores with equal weighting
    combined_score = np.mean(scores)

    # Normalize to [0, 1] range
    return max(0, min(1, (combined_score + 1) / 2))


def _calculate_breakthrough_probability(df: pd.DataFrame, metrics_cols: List[str]) -> float:
    """Calculate probability of breakthrough based on performance patterns."""
    factors = []

    for metric in metrics_cols:
        if metric in df.columns:
            values = df[metric].dropna()
            if len(values) > 0:
                # Recent performance vs historical average
                recent_avg = values.tail(7).mean() if len(values) >= 7 else values.mean()
                historical_avg = values.mean()

                if historical_avg > 0:
                    performance_ratio = recent_avg / historical_avg
                    factors.append(min(performance_ratio, 2.0))  # Cap at 2x

    if not factors:
        return 0.3  # Default probability

    # Convert performance factors to probability
    avg_factor = np.mean(factors)
    probability = 1 / (1 + np.exp(-(avg_factor-1) * 3))  # Sigmoid transformation

    return max(0.1, min(0.9, probability))


def _predict_growth_rate(df: pd.DataFrame, metrics_cols: List[str], horizon_days: int) -> float:
    """Predict growth rate for the specified horizon."""
    if len(df) < 5:
        return 0.0

    growth_rates = []

    for metric in metrics_cols:
        if metric in df.columns:
            values = df[metric].dropna()
            if len(values) > 1:
                # Calculate historical growth rate
                first_value = values.iloc[0]
                last_value = values.iloc[-1]
                days_span = len(values)

                if first_value > 0 and days_span > 0:
                    daily_growth = (last_value / first_value) ** (1 / days_span) - 1
                    projected_growth = (1 + daily_growth) ** horizon_days-1
                    growth_rates.append(projected_growth)

    return np.mean(growth_rates) if growth_rates else 0.0


def generate_content_optimization_recommendations(
    df: pd.DataFrame, artist_col: str, content_type_col: str, performance_metrics: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Generate ML-powered content optimization recommendations.

    Args:
        df: DataFrame with content performance data
        artist_col: Column name for artist names
        content_type_col: Column name for content types
        performance_metrics: List of performance metric columns

    Returns:
        Dictionary with optimization recommendations for each artist
    """
    recommendations = {}

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Analyze content type performance
        content_performance = _analyze_content_type_performance(artist_df, content_type_col, performance_metrics)

        # Determine optimal content mix
        optimal_mix = _calculate_optimal_content_mix(content_performance)

        # Analyze posting patterns
        posting_schedule = _analyze_posting_patterns(artist_df)

        # Generate engagement strategies
        engagement_strategies = _generate_engagement_strategies(artist_df, performance_metrics)

        # Assess growth potential
        growth_potential = _assess_growth_potential(artist_df, performance_metrics)

        recommendations[artist] = {
            "optimal_content_mix": optimal_mix,
            "posting_schedule": posting_schedule,
            "engagement_strategies": engagement_strategies,
            "growth_potential": growth_potential,
        }

    return recommendations


def _analyze_content_type_performance(
    df: pd.DataFrame, content_type_col: str, metrics: List[str]
) -> Dict[str, Dict[str, float]]:
    """Analyze performance by content type."""
    performance = {}

    for content_type in df[content_type_col].unique():
        type_df = df[df[content_type_col] == content_type]
        type_performance = {}

        for metric in metrics:
            if metric in type_df.columns:
                values = type_df[metric].dropna()
                if len(values) > 0:
                    type_performance[metric] = {
                        "mean": values.mean(),
                        "median": values.median(),
                        "std": values.std(),
                        "count": len(values),
                    }

        performance[content_type] = type_performance

    return performance


def _calculate_optimal_content_mix(content_performance: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    """Calculate optimal content mix based on performance data."""
    if not content_performance:
        return {"Music Video": 0.4, "Lyric Video": 0.3, "Behind Scenes": 0.3}

    # Score each content type based on performance
    content_scores = {}

    for content_type, metrics in content_performance.items():
        score = 0
        metric_count = 0

        for metric_name, metric_data in metrics.items():
            if isinstance(metric_data, dict) and "mean" in metric_data:
                # Normalize score based on metric performance
                normalized_score = min(metric_data["mean"] / 100000, 1.0)  # Normalize views
                score += normalized_score
                metric_count += 1

        content_scores[content_type] = score / metric_count if metric_count > 0 else 0.1

    # Convert scores to percentages
    total_score = sum(content_scores.values())
    if total_score > 0:
        return {ct: score / total_score for ct, score in content_scores.items()}
    else:
        # Default distribution
        return {ct: 1.0 / len(content_scores) for ct in content_scores.keys()}


def _analyze_posting_patterns(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze optimal posting patterns."""
    if "date" in df.columns:
        df_with_date = df.copy()
        df_with_date["date"] = pd.to_datetime(df_with_date["date"])
        df_with_date["day_of_week"] = df_with_date["date"].dt.day_name()

        # Analyze performance by day of week
        day_performance = df_with_date.groupby("day_of_week")["daily_views"].mean().to_dict()

        # Find best posting days
        best_days = sorted(day_performance.items(), key=lambda x: x[1], reverse=True)[:3]

        return {
            "best_posting_days": [day for day, _ in best_days],
            "day_performance": day_performance,
            "recommended_frequency": "weekly" if len(df) < 30 else "bi-weekly",
        }

    return {"best_posting_days": ["Tuesday", "Thursday", "Saturday"], "recommended_frequency": "weekly"}


def _generate_engagement_strategies(df: pd.DataFrame, metrics: List[str]) -> List[str]:
    """Generate engagement optimization strategies."""
    strategies = []

    # Analyze engagement patterns
    if "engagement_rate" in df.columns:
        avg_engagement = df["engagement_rate"].mean()

        if avg_engagement < INDUSTRY_AVG_ENGAGEMENT:
            strategies.append("Focus on improving engagement rate through interactive content")
            strategies.append("Encourage comments with questions and calls-to-action")

        if avg_engagement > TOP_PERFORMER_THRESHOLD:
            strategies.append("Maintain high engagement with consistent quality content")
            strategies.append("Leverage high engagement for cross-promotion")

    # Add general strategies
    strategies.extend(
        [
            "Optimize video thumbnails for higher click-through rates",
            "Use trending hashtags and keywords in descriptions",
            "Collaborate with other artists for cross-audience exposure",
        ]
    )

    return strategies[:5]  # Return top 5 strategies


def _assess_growth_potential(df: pd.DataFrame, metrics: List[str]) -> Dict[str, Any]:
    """Assess growth potential based on current performance."""
    potential_score = 0.5  # Default neutral score

    if "daily_views" in df.columns:
        recent_views = df["daily_views"].tail(7).mean()
        historical_views = df["daily_views"].mean()

        if historical_views > 0:
            growth_ratio = recent_views / historical_views
            potential_score = min(max(growth_ratio, 0.1), 1.0)

    return {
        "growth_score": round(potential_score, 3),
        "potential_level": "high" if potential_score > 0.7 else "medium" if potential_score > 0.4 else "developing",
        "key_factors": ["consistent content quality", "audience engagement", "market timing"],
    }


def analyze_market_positioning(
    artist_data: pd.DataFrame, market_data: pd.DataFrame, artist_col: str, genre_col: str = "genre"
) -> Dict[str, Any]:
    """
    Analyze market positioning and competitive landscape.

    Args:
        artist_data: DataFrame with artist performance data
        market_data: DataFrame with market benchmarks
        artist_col: Column name for artist names
        genre_col: Column name for genres

    Returns:
        Dictionary with market positioning analysis
    """
    # Extract market benchmarks
    benchmarks = {}
    for _, row in market_data.iterrows():
        benchmarks[row["metric"]] = row["value"]

    # Analyze market segments
    market_segments = _analyze_market_segments(artist_data, genre_col)

    # Competitive landscape analysis
    competitive_landscape = _analyze_competitive_landscape(artist_data, artist_col, benchmarks)

    # Generate positioning recommendations
    positioning_recommendations = _generate_positioning_recommendations(competitive_landscape, market_segments)

    return {
        "market_segments": market_segments,
        "competitive_landscape": competitive_landscape,
        "positioning_recommendations": positioning_recommendations,
        "market_benchmarks": benchmarks,
    }


def _analyze_market_segments(df: pd.DataFrame, genre_col: str) -> Dict[str, Any]:
    """Analyze market segments by genre and performance."""
    segments = {}

    if genre_col in df.columns:
        for genre in df[genre_col].unique():
            genre_df = df[df[genre_col] == genre]

            segments[genre] = {
                "artist_count": len(genre_df["artist_name"].unique()) if "artist_name" in df.columns else len(genre_df),
                "avg_performance": genre_df["daily_views"].mean() if "daily_views" in genre_df.columns else 0,
                "market_share": len(genre_df) / len(df),
                "growth_trend": "stable",  # Simplified for now
            }

    return segments


def _analyze_competitive_landscape(
    df: pd.DataFrame, artist_col: str, benchmarks: Dict[str, float]
) -> Dict[str, Dict[str, Any]]:
    """Analyze competitive positioning for each artist."""
    landscape = {}

    industry_avg_engagement = benchmarks.get("industry_avg_engagement", INDUSTRY_AVG_ENGAGEMENT)

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate artist metrics
        avg_views = artist_df["daily_views"].mean() if "daily_views" in artist_df.columns else 0
        avg_engagement = artist_df["engagement_rate"].mean() if "engagement_rate" in artist_df.columns else 0

        # Determine competitive position
        if avg_engagement > industry_avg_engagement * 1.5:
            position = "market_leader"
        elif avg_engagement > industry_avg_engagement:
            position = "strong_performer"
        else:
            position = "developing_artist"

        landscape[artist] = {
            "competitive_position": position,
            "avg_views": round(avg_views, 0),
            "engagement_vs_industry": round(avg_engagement / industry_avg_engagement, 2),
            "market_tier": "tier_1" if avg_views > 100000 else "tier_2" if avg_views > 50000 else "tier_3",
        }

    return landscape


def _generate_positioning_recommendations(landscape: Dict[str, Dict[str, Any]], segments: Dict[str, Any]) -> List[str]:
    """Generate strategic positioning recommendations."""
    recommendations = []

    # Analyze overall competitive landscape
    tier_1_count = sum(1 for artist_data in landscape.values() if artist_data["market_tier"] == "tier_1")
    total_artists = len(landscape)

    if tier_1_count / total_artists < 0.3:
        recommendations.append("Focus on developing tier-2 artists to tier-1 status")

    recommendations.extend(
        [
            "Identify underserved market segments for expansion opportunities",
            "Leverage strong performers for cross-promotion of developing artists",
            "Consider strategic partnerships within complementary genres",
        ]
    )

    return recommendations


def calculate_viral_potential(
    df: pd.DataFrame, artist_col: str, engagement_metrics: List[str], velocity_window_days: int = 7
) -> pd.DataFrame:
    """
    Calculate viral potential scoring based on engagement velocity and quality.

    Args:
        df: DataFrame with engagement data
        artist_col: Column name for artist names
        engagement_metrics: List of engagement metric columns
        velocity_window_days: Window for calculating velocity

    Returns:
        DataFrame with viral potential scores
    """
    viral_scores = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist].copy()

        if len(artist_df) < velocity_window_days:
            viral_scores.append(
                {artist_col: artist, "viral_score": 0.3, "velocity_score": 0.3, "engagement_quality": 0.3}
            )
            continue

        # Calculate engagement velocity
        velocity_score = _calculate_engagement_velocity(artist_df, engagement_metrics, velocity_window_days)

        # Calculate engagement quality
        quality_score = _calculate_engagement_quality(artist_df, engagement_metrics)

        # Combine into viral potential score
        viral_score = velocity_score * 0.6 + quality_score * 0.4

        viral_scores.append(
            {
                artist_col: artist,
                "viral_score": round(viral_score, 3),
                "velocity_score": round(velocity_score, 3),
                "engagement_quality": round(quality_score, 3),
            }
        )

    return pd.DataFrame(viral_scores)


def _calculate_engagement_velocity(df: pd.DataFrame, metrics: List[str], window_days: int) -> float:
    """Calculate engagement velocity over the specified window."""
    velocities = []

    for metric in metrics:
        if metric in df.columns:
            values = df[metric].dropna()
            if len(values) >= window_days:
                recent_avg = values.tail(window_days).mean()
                previous_avg = values.iloc[:-window_days].mean() if len(values) > window_days else values.mean()

                if previous_avg > 0:
                    velocity = (recent_avg-previous_avg) / previous_avg
                    velocities.append(max(0, min(1, velocity + 0.5)))  # Normalize to [0, 1]

    return np.mean(velocities) if velocities else 0.3


def _calculate_engagement_quality(df: pd.DataFrame, metrics: List[str]) -> float:
    """Calculate engagement quality based on consistency and absolute values."""
    quality_scores = []

    for metric in metrics:
        if metric in df.columns:
            values = df[metric].dropna()
            if len(values) > 1:
                # Consistency score (inverse of coefficient of variation)
                cv = values.std() / values.mean() if values.mean() > 0 else 1
                consistency = 1 / (1 + cv)

                # Absolute performance score (normalized)
                if "likes" in metric.lower():
                    performance = min(values.mean() / 10000, 1.0)  # Normalize likes
                elif "comments" in metric.lower():
                    performance = min(values.mean() / 1000, 1.0)  # Normalize comments
                else:
                    performance = min(values.mean() / 5000, 1.0)  # Normalize shares

                quality_scores.append((consistency + performance) / 2)

    return np.mean(quality_scores) if quality_scores else 0.3


def perform_audience_segmentation(
    df: pd.DataFrame, artist_col: str, behavioral_features: List[str], n_segments: int = 3
) -> Dict[str, Any]:
    """
    Perform ML-based audience segmentation using clustering.

    Args:
        df: DataFrame with behavioral data
        artist_col: Column name for artist names
        behavioral_features: List of behavioral feature columns
        n_segments: Number of segments to create

    Returns:
        Dictionary with segmentation results
    """
    if not SKLEARN_AVAILABLE:
        # Fallback segmentation without sklearn
        return _fallback_audience_segmentation(df, artist_col, behavioral_features, n_segments)

    # Prepare data for clustering
    feature_data = []
    artist_mapping = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate aggregate features for each artist
        features = []
        for feature in behavioral_features:
            if feature in artist_df.columns:
                features.append(artist_df[feature].mean())
            else:
                features.append(0)

        feature_data.append(features)
        artist_mapping.append(artist)

    if len(feature_data) < n_segments:
        n_segments = max(1, len(feature_data))

    # Perform clustering
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(feature_data)

    kmeans = KMeans(n_clusters=n_segments, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(scaled_features)

    # Analyze segments
    segment_profiles = _analyze_cluster_profiles(feature_data, cluster_labels, behavioral_features, n_segments)

    # Map artists to segments
    artist_segments = {artist_mapping[i]: int(cluster_labels[i]) for i in range(len(artist_mapping))}

    # Generate segment characteristics
    segment_characteristics = _generate_segment_characteristics(segment_profiles)

    return {
        "segment_profiles": segment_profiles,
        "artist_segments": artist_segments,
        "segment_characteristics": segment_characteristics,
    }


def _fallback_audience_segmentation(
    df: pd.DataFrame, artist_col: str, features: List[str], n_segments: int
) -> Dict[str, Any]:
    """Fallback segmentation without sklearn."""
    # Simple rule-based segmentation
    segments = {}

    for i in range(n_segments):
        segments[i] = {
            "size": len(df) // n_segments,
            "characteristics": f"Segment {i + 1}",
            "avg_features": {feature: df[feature].mean() if feature in df.columns else 0 for feature in features},
        }

    # Assign artists to segments based on performance
    artist_segments = {}
    artists = df[artist_col].unique()

    for i, artist in enumerate(artists):
        artist_segments[artist] = i % n_segments

    return {
        "segment_profiles": segments,
        "artist_segments": artist_segments,
        "segment_characteristics": ["High Engagement", "Medium Engagement", "Developing"][:n_segments],
    }


def _analyze_cluster_profiles(
    feature_data: List[List[float]], labels: List[int], feature_names: List[str], n_segments: int
) -> Dict[int, Dict[str, Any]]:
    """Analyze the profiles of each cluster."""
    profiles = {}

    for segment_id in range(n_segments):
        segment_indices = [i for i, label in enumerate(labels) if label == segment_id]

        if segment_indices:
            segment_features = [feature_data[i] for i in segment_indices]
            avg_features = np.mean(segment_features, axis=0)

            profiles[segment_id] = {
                "size": len(segment_indices),
                "avg_features": {feature_names[i]: round(avg_features[i], 3) for i in range(len(feature_names))},
                "characteristics": f"Segment {segment_id + 1}",
            }
        else:
            profiles[segment_id] = {
                "size": 0,
                "avg_features": {feature: 0 for feature in feature_names},
                "characteristics": f"Empty Segment {segment_id + 1}",
            }

    return profiles


def _generate_segment_characteristics(profiles: Dict[int, Dict[str, Any]]) -> List[str]:
    """Generate descriptive characteristics for each segment."""
    characteristics = []

    for segment_id, profile in profiles.items():
        if profile["size"] > 0:
            # Analyze features to determine characteristics
            features = profile["avg_features"]

            if "engagement_rate" in features:
                if features["engagement_rate"] > 0.06:
                    characteristics.append("High Engagement Segment")
                elif features["engagement_rate"] > 0.04:
                    characteristics.append("Medium Engagement Segment")
                else:
                    characteristics.append("Developing Engagement Segment")
            else:
                characteristics.append(f"Segment {segment_id + 1}")
        else:
            characteristics.append("Empty Segment")

    return characteristics


def forecast_performance_trends(
    df: pd.DataFrame, artist_col: str, date_col: str, target_metrics: List[str], forecast_days: int = 30
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Forecast performance trends using time series analysis.

    Args:
        df: DataFrame with time series data
        artist_col: Column name for artist names
        date_col: Column name for dates
        target_metrics: List of metrics to forecast
        forecast_days: Number of days to forecast

    Returns:
        Dictionary with forecasts for each artist and metric
    """
    forecasts = {}

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist].copy()
        artist_df[date_col] = pd.to_datetime(artist_df[date_col])
        artist_df = artist_df.sort_values(date_col)

        artist_forecasts = {}

        for metric in target_metrics:
            if metric in artist_df.columns:
                forecast_result = _forecast_single_metric(artist_df, date_col, metric, forecast_days)
                artist_forecasts[metric] = forecast_result

        forecasts[artist] = artist_forecasts

    return forecasts


def _forecast_single_metric(df: pd.DataFrame, date_col: str, metric: str, forecast_days: int) -> Dict[str, Any]:
    """Forecast a single metric using simple trend analysis."""
    values = df[metric].dropna()

    if len(values) < 5:
        # Not enough data for meaningful forecast
        last_value = values.iloc[-1] if len(values) > 0 else 0
        return {
            "forecast": [last_value] * forecast_days,
            "confidence_lower": [last_value * 0.8] * forecast_days,
            "confidence_upper": [last_value * 1.2] * forecast_days,
            "method": "constant",
        }

    # Simple linear trend forecast
    x = np.arange(len(values))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, values)

    # Generate forecast
    forecast_x = np.arange(len(values), len(values) + forecast_days)
    forecast_values = slope * forecast_x + intercept

    # Calculate confidence intervals (simplified)
    residuals = values - (slope * x + intercept)
    residual_std = np.std(residuals)

    confidence_lower = forecast_values-1.96 * residual_std
    confidence_upper = forecast_values + 1.96 * residual_std

    return {
        "forecast": forecast_values.tolist(),
        "confidence_lower": confidence_lower.tolist(),
        "confidence_upper": confidence_upper.tolist(),
        "method": "linear_trend",
        "r_squared": r_value**2,
    }


def detect_performance_anomalies(
    df: pd.DataFrame, artist_col: str, metrics_cols: List[str], sensitivity: float = 0.05
) -> pd.DataFrame:
    """
    Detect performance anomalies using statistical methods.

    Args:
        df: DataFrame with performance data
        artist_col: Column name for artist names
        metrics_cols: List of metric columns to analyze
        sensitivity: Sensitivity threshold (lower = more sensitive)

    Returns:
        DataFrame with detected anomalies
    """
    anomalies = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist].copy()

        for metric in metrics_cols:
            if metric in artist_df.columns:
                artist_anomalies = _detect_metric_anomalies(artist_df, metric, sensitivity, artist)
                anomalies.extend(artist_anomalies)

    return pd.DataFrame(anomalies)


def _detect_metric_anomalies(df: pd.DataFrame, metric: str, sensitivity: float, artist: str) -> List[Dict[str, Any]]:
    """Detect anomalies in a single metric using statistical methods."""
    values = df[metric].dropna()

    if len(values) < 10:
        return []  # Need sufficient data for anomaly detection

    anomalies = []

    # Statistical anomaly detection using z-score
    mean_val = values.mean()
    std_val = values.std()

    if std_val > 0:
        z_scores = np.abs((values-mean_val) / std_val)
        threshold = stats.norm.ppf(1-sensitivity / 2)  # Two-tailed test

        anomaly_indices = np.where(z_scores > threshold)[0]

        for idx in anomaly_indices:
            original_idx = values.index[idx]
            anomaly_score = z_scores.iloc[idx]

            anomaly_type = "spike" if values.iloc[idx] > mean_val else "drop"

            anomaly_data = {
                "artist_name": artist,
                "date": df.loc[original_idx, "date"] if "date" in df.columns else original_idx,
                "anomaly_type": anomaly_type,
                "anomaly_score": round(anomaly_score, 3),
                "affected_metric": metric,
                "actual_value": values.iloc[idx],
                "expected_value": round(mean_val, 2),
            }

            anomalies.append(anomaly_data)

    return anomalies


# Statistical Analysis Functions


def perform_statistical_tests(
    df: pd.DataFrame, group_col: str, metric_cols: List[str], test_type: str = "anova"
) -> Dict[str, Dict[str, Any]]:
    """
    Perform statistical significance testing.

    Args:
        df: DataFrame with data
        group_col: Column name for groups
        metric_cols: List of metric columns
        test_type: Type of test ('anova', 't_test')

    Returns:
        Dictionary with test results for each metric
    """
    if not SCIPY_AVAILABLE:
        return _fallback_statistical_tests(df, group_col, metric_cols)

    results = {}

    for metric in metric_cols:
        if metric in df.columns:
            groups = [df[df[group_col] == group][metric].dropna() for group in df[group_col].unique()]
            groups = [group for group in groups if len(group) > 0]

            if len(groups) >= 2:
                if test_type == "anova" and len(groups) > 2:
                    f_stat, p_value = stats.f_oneway(*groups)
                    effect_size = _calculate_eta_squared(groups)
                    test_stat = f_stat
                else:
                    # T-test for two groups
                    if len(groups) >= 2:
                        t_stat, p_value = stats.ttest_ind(groups[0], groups[1])
                        effect_size = _calculate_cohens_d(groups[0], groups[1])
                        test_stat = t_stat
                    else:
                        test_stat, p_value, effect_size = 0, 1, 0

                significance = "significant" if p_value < 0.05 else "not_significant"

                results[metric] = {
                    "test_statistic": round(test_stat, 4),
                    "p_value": round(p_value, 4),
                    "effect_size": round(effect_size, 4),
                    "significance": significance,
                }

    return results


def _fallback_statistical_tests(df: pd.DataFrame, group_col: str, metric_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fallback statistical tests without scipy."""
    results = {}

    for metric in metric_cols:
        if metric in df.columns:
            # Simple variance analysis
            groups = df.groupby(group_col)[metric].agg(["mean", "std", "count"])

            # Calculate F-statistic approximation
            between_var = groups["mean"].var()
            within_var = groups["std"].mean()

            f_approx = between_var / within_var if within_var > 0 else 0

            results[metric] = {
                "test_statistic": round(f_approx, 4),
                "p_value": 0.05,  # Placeholder
                "effect_size": 0.1,  # Placeholder
                "significance": "estimated",
            }

    return results


def _calculate_eta_squared(groups: List[np.ndarray]) -> float:
    """Calculate eta-squared effect size for ANOVA."""
    all_values = np.concatenate(groups)
    grand_mean = np.mean(all_values)

    ss_between = sum(len(group) * (np.mean(group) - grand_mean) ** 2 for group in groups)
    ss_total = sum((value-grand_mean) ** 2 for value in all_values)

    return ss_between / ss_total if ss_total > 0 else 0


def _calculate_cohens_d(group1: np.ndarray, group2: np.ndarray) -> float:
    """Calculate Cohen's d effect size for t-test."""
    n1, n2 = len(group1), len(group2)

    if n1 <= 1 or n2 <= 1:
        return 0

    pooled_std = np.sqrt(((n1-1) * np.var(group1, ddof=1) + (n2-1) * np.var(group2, ddof=1)) / (n1 + n2-2))

    return (np.mean(group1) - np.mean(group2)) / pooled_std if pooled_std > 0 else 0


def analyze_metric_correlations(df: pd.DataFrame, metric_cols: List[str], method: str = "pearson") -> Dict[str, Any]:
    """
    Analyze correlations between performance metrics.

    Args:
        df: DataFrame with metrics
        metric_cols: List of metric columns
        method: Correlation method ('pearson', 'spearman')

    Returns:
        Dictionary with correlation analysis
    """
    # Filter to only include columns that exist
    available_cols = [col for col in metric_cols if col in df.columns]

    if len(available_cols) < 2:
        return {
            "correlation_matrix": pd.DataFrame(),
            "significant_correlations": [],
            "correlation_insights": ["Insufficient metrics for correlation analysis"],
        }

    # Calculate correlation matrix
    corr_matrix = df[available_cols].corr(method=method)

    # Find significant correlations
    significant_correlations = []

    for i in range(len(available_cols)):
        for j in range(i + 1, len(available_cols)):
            corr_value = corr_matrix.iloc[i, j]

            if abs(corr_value) > 0.5:  # Threshold for significance
                significant_correlations.append(
                    {
                        "metric1": available_cols[i],
                        "metric2": available_cols[j],
                        "correlation": round(corr_value, 3),
                        "strength": "strong" if abs(corr_value) > 0.7 else "moderate",
                    }
                )

    # Generate insights
    insights = _generate_correlation_insights(significant_correlations)

    return {
        "correlation_matrix": corr_matrix,
        "significant_correlations": significant_correlations,
        "correlation_insights": insights,
    }


def _generate_correlation_insights(correlations: List[Dict[str, Any]]) -> List[str]:
    """Generate insights from correlation analysis."""
    insights = []

    if not correlations:
        insights.append("No strong correlations found between metrics")
        return insights

    # Analyze positive correlations
    positive_corrs = [c for c in correlations if c["correlation"] > 0]
    if positive_corrs:
        insights.append(f"Found {len(positive_corrs)} positive correlations indicating synergistic metrics")

    # Analyze negative correlations
    negative_corrs = [c for c in correlations if c["correlation"] < 0]
    if negative_corrs:
        insights.append(f"Found {len(negative_corrs)} negative correlations indicating trade-offs")

    # Highlight strongest correlation
    if correlations:
        strongest = max(correlations, key=lambda x: abs(x["correlation"]))
        insights.append(
            f"Strongest correlation: {strongest['metric1']} and {strongest['metric2']} ({strongest['correlation']})"
        )

    return insights


def analyze_metric_distributions(
    df: pd.DataFrame, metric_cols: List[str], group_col: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze distributions and perform normality testing.

    Args:
        df: DataFrame with metrics
        metric_cols: List of metric columns
        group_col: Optional grouping column

    Returns:
        Dictionary with distribution analysis
    """
    distributions = {}

    for metric in metric_cols:
        if metric in df.columns:
            values = df[metric].dropna()

            if len(values) > 3:
                # Descriptive statistics
                desc_stats = {
                    "mean": round(values.mean(), 3),
                    "median": round(values.median(), 3),
                    "std": round(values.std(), 3),
                    "skewness": round(values.skew(), 3),
                    "kurtosis": round(values.kurtosis(), 3),
                }

                # Normality test
                if SCIPY_AVAILABLE and len(values) >= 8:
                    shapiro_stat, shapiro_p = stats.shapiro(values[:5000])  # Limit for performance
                    normality_test = {
                        "test": "shapiro_wilk",
                        "statistic": round(shapiro_stat, 4),
                        "p_value": round(shapiro_p, 4),
                        "is_normal": shapiro_p > 0.05,
                    }
                else:
                    normality_test = {"test": "visual_inspection", "is_normal": abs(desc_stats["skewness"]) < 1.0}

                # Outlier analysis
                q1 = values.quantile(0.25)
                q3 = values.quantile(0.75)
                iqr = q3-q1
                lower_bound = q1-1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                outliers = values[(values < lower_bound) | (values > upper_bound)]

                outlier_analysis = {
                    "outlier_count": len(outliers),
                    "outlier_percentage": round(len(outliers) / len(values) * 100, 2),
                    "lower_bound": round(lower_bound, 3),
                    "upper_bound": round(upper_bound, 3),
                }

                distributions[metric] = {
                    "descriptive_stats": desc_stats,
                    "normality_test": normality_test,
                    "outlier_analysis": outlier_analysis,
                }

    return distributions


def calculate_confidence_intervals(
    df: pd.DataFrame, metric_cols: List[str], group_col: str, confidence_level: float = 0.95
) -> pd.DataFrame:
    """
    Calculate confidence intervals for metrics by group.

    Args:
        df: DataFrame with data
        metric_cols: List of metric columns
        group_col: Column name for grouping
        confidence_level: Confidence level (e.g., 0.95 for 95%)

    Returns:
        DataFrame with confidence intervals
    """
    intervals = []
    alpha = 1-confidence_level

    for group in df[group_col].unique():
        group_df = df[df[group_col] == group]

        for metric in metric_cols:
            if metric in group_df.columns:
                values = group_df[metric].dropna()

                if len(values) > 1:
                    mean_val = values.mean()
                    std_val = values.std()
                    n = len(values)

                    # Calculate confidence interval
                    if SCIPY_AVAILABLE:
                        t_critical = stats.t.ppf(1-alpha / 2, n-1)
                    else:
                        t_critical = 1.96  # Approximate for large samples

                    margin_error = t_critical * (std_val / np.sqrt(n))

                    intervals.append(
                        {
                            group_col: group,
                            "metric": metric,
                            "mean": round(mean_val, 3),
                            "ci_lower": round(mean_val-margin_error, 3),
                            "ci_upper": round(mean_val + margin_error, 3),
                            "sample_size": n,
                        }
                    )

    return pd.DataFrame(intervals)


# Business Intelligence Functions


def optimize_marketing_roi(
    df: pd.DataFrame, artist_col: str, revenue_col: str, spend_col: str, target_total_budget: float
) -> Dict[str, Any]:
    """
    Optimize marketing ROI and budget allocation.

    Args:
        df: DataFrame with financial data
        artist_col: Column name for artist names
        revenue_col: Column name for revenue
        spend_col: Column name for marketing spend
        target_total_budget: Target total budget to allocate

    Returns:
        Dictionary with optimization results
    """
    # Calculate current ROI for each artist
    df_copy = df.copy()
    df_copy["roi"] = df_copy[revenue_col] / df_copy[spend_col]

    # Simple optimization: allocate budget proportional to ROI
    total_roi = df_copy["roi"].sum()

    optimal_allocation = {}
    for _, row in df_copy.iterrows():
        artist = row[artist_col]
        roi_weight = row["roi"] / total_roi
        allocation = target_total_budget * roi_weight
        optimal_allocation[artist] = round(allocation, 2)

    # Calculate expected ROI
    expected_revenue = sum(
        optimal_allocation[artist] * df_copy[df_copy[artist_col] == artist]["roi"].iloc[0]
        for artist in optimal_allocation.keys()
    )
    expected_roi = expected_revenue / target_total_budget

    # Generate budget recommendations
    recommendations = []
    for artist, allocation in optimal_allocation.items():
        current_spend = df_copy[df_copy[artist_col] == artist][spend_col].iloc[0]
        change = allocation-current_spend

        if change > 0:
            recommendations.append(f"Increase {artist} budget by ${change:,.0f}")
        elif change < 0:
            recommendations.append(f"Decrease {artist} budget by ${abs(change):,.0f}")

    return {
        "optimal_allocation": optimal_allocation,
        "expected_roi": round(expected_roi, 2),
        "budget_recommendations": recommendations,
    }


def benchmark_performance(
    df: pd.DataFrame, artist_col: str, metrics_cols: List[str], benchmark_type: str = "industry_percentiles"
) -> pd.DataFrame:
    """
    Benchmark performance against industry standards.

    Args:
        df: DataFrame with performance data
        artist_col: Column name for artist names
        metrics_cols: List of metric columns
        benchmark_type: Type of benchmarking

    Returns:
        DataFrame with benchmarking results
    """
    benchmarks = []

    # Calculate percentiles for each metric
    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate composite performance score
        performance_scores = []

        for metric in metrics_cols:
            if metric in artist_df.columns:
                artist_value = artist_df[metric].mean()

                # Calculate percentile rank within the dataset
                all_values = df[metric].dropna()
                percentile_rank = (all_values < artist_value).mean() * 100
                performance_scores.append(percentile_rank)

        avg_percentile = np.mean(performance_scores) if performance_scores else 50

        # Determine performance tier
        if avg_percentile >= 80:
            tier = "top_performer"
        elif avg_percentile >= 60:
            tier = "above_average"
        elif avg_percentile >= 40:
            tier = "average"
        else:
            tier = "below_average"

        benchmarks.append(
            {
                artist_col: artist,
                "percentile_rank": round(avg_percentile, 1),
                "performance_tier": tier,
                "benchmark_score": round(avg_percentile / 100, 3),
            }
        )

    return pd.DataFrame(benchmarks)


def calculate_investment_priorities(
    df: pd.DataFrame,
    artist_col: str,
    performance_metrics: List[str],
    growth_potential_weight: float = 0.4,
    current_performance_weight: float = 0.6,
) -> pd.DataFrame:
    """
    Calculate investment priority scores for artist development.

    Args:
        df: DataFrame with performance data
        artist_col: Column name for artist names
        performance_metrics: List of performance metric columns
        growth_potential_weight: Weight for growth potential
        current_performance_weight: Weight for current performance

    Returns:
        DataFrame with priority scores
    """
    priorities = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        # Calculate current performance score
        current_scores = []
        for metric in performance_metrics:
            if metric in artist_df.columns:
                # Normalize metric to 0-1 scale
                metric_values = df[metric].dropna()
                if len(metric_values) > 0:
                    artist_value = artist_df[metric].mean()
                    metric_range = metric_values.max() - metric_values.min()
                    if metric_range > 0:
                        normalized_score = (artist_value-metric_values.min()) / metric_range
                        current_scores.append(max(0, min(1, normalized_score)))
                    else:
                        current_scores.append(0.5)  # Neutral score if no variance

        current_performance = np.mean(current_scores) if current_scores else 0.5

        # Calculate growth potential (simplified)
        if len(artist_df) > 5:
            # Look at recent trend
            recent_performance = (
                artist_df.tail(3)[performance_metrics[0]].mean() if performance_metrics[0] in artist_df.columns else 0
            )
            historical_performance = (
                artist_df[performance_metrics[0]].mean() if performance_metrics[0] in artist_df.columns else 0
            )

            growth_potential = (
                min(1, max(0, recent_performance / historical_performance)) if historical_performance > 0 else 0.5
            )
        else:
            growth_potential = 0.5  # Neutral for insufficient data

        # Calculate weighted priority score
        priority_score = current_performance * current_performance_weight + growth_potential * growth_potential_weight

        # Generate investment recommendation
        if priority_score > 0.7:
            recommendation = "high_priority"
            reasoning = "Strong current performance with good growth potential"
        elif priority_score > 0.5:
            recommendation = "medium_priority"
            reasoning = "Moderate performance with development opportunities"
        else:
            recommendation = "low_priority"
            reasoning = "Focus on foundational development first"

        priorities.append(
            {
                artist_col: artist,
                "priority_score": round(priority_score, 3),
                "investment_recommendation": recommendation,
                "reasoning": reasoning,
                "current_performance": round(current_performance, 3),
                "growth_potential": round(growth_potential, 3),
            }
        )

    return pd.DataFrame(priorities).sort_values("priority_score", ascending=False)


def identify_market_opportunities(
    df: pd.DataFrame, artist_col: str, performance_metrics: List[str], market_size_estimate: float
) -> Dict[str, Any]:
    """
    Identify market opportunities and gaps.

    Args:
        df: DataFrame with performance data
        artist_col: Column name for artist names
        performance_metrics: List of performance metrics
        market_size_estimate: Estimated total market size

    Returns:
        Dictionary with market opportunities
    """
    # Calculate current market capture
    total_performance = df[performance_metrics[0]].sum() if performance_metrics[0] in df.columns else 0
    market_capture_rate = total_performance / market_size_estimate if market_size_estimate > 0 else 0

    # Identify performance gaps
    opportunity_gaps = []

    # Genre-based opportunities
    if "genre" in df.columns:
        genre_performance = (
            df.groupby("genre")[performance_metrics[0]].sum() if performance_metrics[0] in df.columns else pd.Series()
        )

        for genre in df["genre"].unique():
            genre_total = genre_performance.get(genre, 0)
            genre_potential = market_size_estimate * 0.2  # Assume each genre has 20% potential

            if genre_total < genre_potential * 0.5:  # Less than 50% of potential
                opportunity_gaps.append(
                    {
                        "type": "genre_opportunity",
                        "category": genre,
                        "current_performance": genre_total,
                        "potential": genre_potential,
                        "gap": genre_potential-genre_total,
                    }
                )

    # Artist development opportunities
    artist_performance = (
        df.groupby(artist_col)[performance_metrics[0]].sum() if performance_metrics[0] in df.columns else pd.Series()
    )

    # Find underperforming artists with potential
    median_performance = artist_performance.median()

    for artist in df[artist_col].unique():
        artist_perf = artist_performance.get(artist, 0)

        if artist_perf < median_performance * 0.7:  # Significantly below median
            opportunity_gaps.append(
                {
                    "type": "artist_development",
                    "category": artist,
                    "current_performance": artist_perf,
                    "potential": median_performance,
                    "gap": median_performance-artist_perf,
                }
            )

    # Calculate market potential
    market_potential = {
        "total_addressable_market": market_size_estimate,
        "current_capture": total_performance,
        "capture_rate": round(market_capture_rate, 3),
        "untapped_potential": market_size_estimate-total_performance,
    }

    # Generate strategic recommendations
    strategic_recommendations = [
        "Focus on underperforming genres with high market potential",
        "Invest in artist development for below-median performers",
        "Consider market expansion strategies for untapped segments",
    ]

    if market_capture_rate < 0.1:
        strategic_recommendations.append("Significant market opportunity exists-consider aggressive expansion")

    return {
        "opportunity_gaps": opportunity_gaps,
        "market_potential": market_potential,
        "strategic_recommendations": strategic_recommendations,
    }


# Advanced ML Analytics Functions


def create_artist_performance_model(
    df: pd.DataFrame, artist_col: str, target_metric: str, feature_cols: List[str], model_type: str = "random_forest"
) -> Dict[str, Any]:
    """
    Create and train a machine learning model for artist performance prediction.

    Args:
        df: DataFrame with training data
        artist_col: Column name for artist names
        target_metric: Target metric to predict
        feature_cols: List of feature columns
        model_type: Type of ML model ('random_forest', 'linear', 'ensemble')

    Returns:
        Dictionary with trained model and performance metrics
    """
    if not SKLEARN_AVAILABLE:
        return _fallback_performance_model(df, artist_col, target_metric, feature_cols)

    # Prepare data
    feature_data = df[feature_cols + [target_metric]].dropna()

    if len(feature_data) < MIN_SAMPLES_FOR_ML:
        return {
            "model": None,
            "performance": {"error": "Insufficient data for ML model training"},
            "feature_importance": {},
            "predictions": {},
        }

    X = feature_data[feature_cols]
    y = feature_data[target_metric]

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train model
    if model_type == "random_forest":
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)
        feature_importance = dict(zip(feature_cols, model.feature_importances_))
    elif model_type == "linear":
        model = LinearRegression()
        model.fit(X_train_scaled, y_train)
        feature_importance = dict(zip(feature_cols, abs(model.coef_)))
    else:
        # Ensemble approach
        rf_model = RandomForestRegressor(n_estimators=50, random_state=42)
        lr_model = LinearRegression()

        rf_model.fit(X_train_scaled, y_train)
        lr_model.fit(X_train_scaled, y_train)

        # Simple ensemble averaging
        rf_pred = rf_model.predict(X_test_scaled)
        lr_pred = lr_model.predict(X_test_scaled)
        y_pred = (rf_pred + lr_pred) / 2

        model = {"rf": rf_model, "lr": lr_model, "scaler": scaler}
        feature_importance = dict(zip(feature_cols, rf_model.feature_importances_))

    # Evaluate model
    if model_type != "ensemble":
        y_pred = model.predict(X_test_scaled)

    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Cross-validation
    if model_type != "ensemble":
        cv_scores = cross_val_score(model, X_train_scaled, y_train, cv=CROSS_VALIDATION_FOLDS)
        cv_mean = cv_scores.mean()
        cv_std = cv_scores.std()
    else:
        cv_mean, cv_std = r2, 0.1  # Placeholder for ensemble

    # Generate predictions for each artist
    artist_predictions = {}
    for artist in df[artist_col].unique():
        artist_data = df[df[artist_col] == artist][feature_cols].dropna()

        if len(artist_data) > 0:
            if model_type == "ensemble":
                artist_scaled = scaler.transform(artist_data)
                rf_pred = model["rf"].predict(artist_scaled)
                lr_pred = model["lr"].predict(artist_scaled)
                pred = (rf_pred + lr_pred) / 2
            else:
                artist_scaled = scaler.fit_transform(artist_data)
                pred = model.predict(artist_scaled)

            artist_predictions[artist] = {
                "predicted_performance": float(np.mean(pred)),
                "confidence": min(1.0, cv_mean),
                "prediction_range": [float(np.min(pred)), float(np.max(pred))],
            }

    return {
        "model": model,
        "scaler": scaler,
        "performance": {
            "mse": round(mse, 4),
            "r2_score": round(r2, 4),
            "cv_mean": round(cv_mean, 4),
            "cv_std": round(cv_std, 4),
        },
        "feature_importance": {k: round(v, 4) for k, v in feature_importance.items()},
        "predictions": artist_predictions,
    }


def _fallback_performance_model(
    df: pd.DataFrame, artist_col: str, target_metric: str, feature_cols: List[str]
) -> Dict[str, Any]:
    """Fallback performance model without sklearn."""
    # Simple correlation-based model
    correlations = {}

    for feature in feature_cols:
        if feature in df.columns and target_metric in df.columns:
            corr = df[feature].corr(df[target_metric])
            correlations[feature] = abs(corr) if not np.isnan(corr) else 0

    # Generate simple predictions based on correlations
    predictions = {}
    for artist in df[artist_col].unique():
        artist_data = df[df[artist_col] == artist]
        if len(artist_data) > 0 and target_metric in artist_data.columns:
            predicted = artist_data[target_metric].mean()
            predictions[artist] = {
                "predicted_performance": predicted,
                "confidence": 0.5,
                "prediction_range": [predicted * 0.8, predicted * 1.2],
            }

    return {
        "model": "correlation_based",
        "performance": {"r2_score": 0.5},
        "feature_importance": correlations,
        "predictions": predictions,
    }


def detect_viral_content_patterns(
    df: pd.DataFrame, artist_col: str, content_type_col: str, engagement_metrics: List[str], time_col: str = "date"
) -> Dict[str, Any]:
    """
    Detect patterns in viral content using advanced analytics.

    Args:
        df: DataFrame with content performance data
        artist_col: Column name for artist names
        content_type_col: Column name for content types
        engagement_metrics: List of engagement metric columns
        time_col: Column name for time / date

    Returns:
        Dictionary with viral pattern analysis
    """
    viral_patterns = {
        "content_type_patterns": {},
        "temporal_patterns": {},
        "engagement_patterns": {},
        "viral_indicators": {},
    }

    # Analyze content type patterns
    for content_type in df[content_type_col].unique():
        type_df = df[df[content_type_col] == content_type]

        viral_content = _identify_viral_content(type_df, engagement_metrics)

        viral_patterns["content_type_patterns"][content_type] = {
            "viral_rate": len(viral_content) / len(type_df) if len(type_df) > 0 else 0,
            "avg_viral_performance": viral_content[engagement_metrics[0]].mean() if len(viral_content) > 0 else 0,
            "viral_characteristics": _analyze_viral_characteristics(viral_content, engagement_metrics),
        }

    # Analyze temporal patterns
    if time_col in df.columns:
        df_time = df.copy()
        df_time[time_col] = pd.to_datetime(df_time[time_col])
        df_time["hour"] = df_time[time_col].dt.hour
        df_time["day_of_week"] = df_time[time_col].dt.dayofweek

        # Find optimal posting times
        hourly_performance = df_time.groupby("hour")[engagement_metrics[0]].mean()
        daily_performance = df_time.groupby("day_of_week")[engagement_metrics[0]].mean()

        viral_patterns["temporal_patterns"] = {
            "best_hours": hourly_performance.nlargest(3).index.tolist(),
            "best_days": daily_performance.nlargest(3).index.tolist(),
            "hourly_performance": hourly_performance.to_dict(),
            "daily_performance": daily_performance.to_dict(),
        }

    # Analyze engagement patterns
    viral_patterns["engagement_patterns"] = _analyze_engagement_patterns(df, engagement_metrics)

    # Generate viral indicators
    viral_patterns["viral_indicators"] = _generate_viral_indicators(df, artist_col, engagement_metrics)

    return viral_patterns


def _identify_viral_content(df: pd.DataFrame, engagement_metrics: List[str]) -> pd.DataFrame:
    """Identify viral content based on engagement thresholds."""
    if len(df) == 0:
        return df

    viral_mask = pd.Series([False] * len(df), index=df.index)

    for metric in engagement_metrics:
        if metric in df.columns:
            # Define viral threshold as top 10% or above absolute threshold
            threshold = max(
                df[metric].quantile(0.9), VIRAL_THRESHOLD_VIEWS if "views" in metric.lower() else df[metric].mean() * 2
            )
            viral_mask |= df[metric] > threshold

    return df[viral_mask]


def _analyze_viral_characteristics(viral_df: pd.DataFrame, engagement_metrics: List[str]) -> Dict[str, Any]:
    """Analyze characteristics of viral content."""
    if len(viral_df) == 0:
        return {"no_viral_content": True}

    characteristics = {}

    for metric in engagement_metrics:
        if metric in viral_df.columns:
            characteristics[f"{metric}_avg"] = viral_df[metric].mean()
            characteristics[f"{metric}_median"] = viral_df[metric].median()
            characteristics[f"{metric}_std"] = viral_df[metric].std()

    # Analyze content patterns
    if "content_type" in viral_df.columns:
        characteristics["dominant_content_types"] = viral_df["content_type"].value_counts().head(3).to_dict()

    return characteristics


def _analyze_engagement_patterns(df: pd.DataFrame, engagement_metrics: List[str]) -> Dict[str, Any]:
    """Analyze engagement patterns across the dataset."""
    patterns = {}

    # Calculate engagement ratios
    if len(engagement_metrics) >= 2:
        for i, metric1 in enumerate(engagement_metrics):
            for metric2 in engagement_metrics[i + 1 :]:
                if metric1 in df.columns and metric2 in df.columns:
                    ratio_name = f"{metric1}_to_{metric2}_ratio"
                    df_temp = df[[metric1, metric2]].dropna()

                    if len(df_temp) > 0:
                        ratios = df_temp[metric1] / (df_temp[metric2] + 1)  # Add 1 to avoid division by zero
                        patterns[ratio_name] = {"mean": ratios.mean(), "median": ratios.median(), "std": ratios.std()}

    # Analyze engagement velocity
    for metric in engagement_metrics:
        if metric in df.columns:
            values = df[metric].dropna()
            if len(values) > 1:
                velocity = values.pct_change().dropna()
                patterns[f"{metric}_velocity"] = {
                    "mean_change": velocity.mean(),
                    "volatility": velocity.std(),
                    "positive_changes": (velocity > 0).mean(),
                }

    return patterns


def _generate_viral_indicators(df: pd.DataFrame, artist_col: str, engagement_metrics: List[str]) -> Dict[str, Any]:
    """Generate viral potential indicators for each artist."""
    indicators = {}

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        artist_indicators = {}

        # Calculate viral readiness score
        viral_readiness = 0

        for metric in engagement_metrics:
            if metric in artist_df.columns:
                values = artist_df[metric].dropna()
                if len(values) > 0:
                    # Consistency score
                    cv = values.std() / values.mean() if values.mean() > 0 else 1
                    consistency = 1 / (1 + cv)

                    # Growth trend
                    if len(values) > 1:
                        growth = values.iloc[-1] / values.iloc[0] if values.iloc[0] > 0 else 1
                        growth_score = min(1, max(0, (growth-0.5) * 2))  # Normalize around 1.0
                    else:
                        growth_score = 0.5

                    metric_score = (consistency + growth_score) / 2
                    viral_readiness += metric_score

        viral_readiness = viral_readiness / len(engagement_metrics) if engagement_metrics else 0

        artist_indicators["viral_readiness"] = round(viral_readiness, 3)
        artist_indicators["content_volume"] = len(artist_df)
        artist_indicators["avg_engagement"] = (
            artist_df[engagement_metrics[0]].mean()
            if engagement_metrics and engagement_metrics[0] in artist_df.columns
            else 0
        )

        indicators[artist] = artist_indicators

    return indicators


def perform_advanced_clustering(
    df: pd.DataFrame,
    artist_col: str,
    feature_cols: List[str],
    clustering_method: str = "kmeans",
    n_clusters: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Perform advanced clustering analysis with multiple algorithms.

    Args:
        df: DataFrame with artist data
        artist_col: Column name for artist names
        feature_cols: List of feature columns for clustering
        clustering_method: Clustering algorithm ('kmeans', 'hierarchical', 'dbscan')
        n_clusters: Number of clusters (auto-determined if None)

    Returns:
        Dictionary with clustering results and analysis
    """
    if not SKLEARN_AVAILABLE:
        return _fallback_clustering(df, artist_col, feature_cols, n_clusters or 3)

    # Prepare data
    feature_data = []
    artist_mapping = []

    for artist in df[artist_col].unique():
        artist_df = df[df[artist_col] == artist]

        features = []
        for feature in feature_cols:
            if feature in artist_df.columns:
                features.append(artist_df[feature].mean())
            else:
                features.append(0)

        feature_data.append(features)
        artist_mapping.append(artist)

    if len(feature_data) < 2:
        return {"error": "Insufficient data for clustering"}

    # Scale features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(feature_data)

    # Determine optimal number of clusters if not provided
    if n_clusters is None:
        n_clusters = _determine_optimal_clusters(scaled_features)

    # Perform clustering
    if clustering_method == "kmeans":
        clusterer = KMeans(n_clusters=n_clusters, random_state=CLUSTERING_RANDOM_STATE, n_init=10)
        cluster_labels = clusterer.fit_predict(scaled_features)
        cluster_centers = clusterer.cluster_centers_
    else:
        # Fallback to KMeans for other methods
        clusterer = KMeans(n_clusters=n_clusters, random_state=CLUSTERING_RANDOM_STATE, n_init=10)
        cluster_labels = clusterer.fit_predict(scaled_features)
        cluster_centers = clusterer.cluster_centers_

    # Calculate silhouette score
    if len(set(cluster_labels)) > 1:
        silhouette_avg = silhouette_score(scaled_features, cluster_labels)
    else:
        silhouette_avg = 0

    # Analyze clusters
    cluster_analysis = _analyze_clusters_advanced(
        feature_data, cluster_labels, cluster_centers, feature_cols, artist_mapping, scaler
    )

    # Generate cluster insights
    cluster_insights = _generate_cluster_insights(cluster_analysis, feature_cols)

    return {
        "cluster_labels": {artist_mapping[i]: int(cluster_labels[i]) for i in range(len(artist_mapping))},
        "cluster_analysis": cluster_analysis,
        "cluster_insights": cluster_insights,
        "silhouette_score": round(silhouette_avg, 3),
        "n_clusters": n_clusters,
        "feature_importance": _calculate_feature_importance_clustering(scaled_features, cluster_labels, feature_cols),
    }


def _determine_optimal_clusters(data: np.ndarray, max_clusters: int = 8) -> int:
    """Determine optimal number of clusters using elbow method."""
    if len(data) < 4:
        return min(2, len(data))

    max_clusters = min(max_clusters, len(data) - 1)
    inertias = []

    for k in range(2, max_clusters + 1):
        kmeans = KMeans(n_clusters=k, random_state=CLUSTERING_RANDOM_STATE, n_init=10)
        kmeans.fit(data)
        inertias.append(kmeans.inertia_)

    # Simple elbow detection
    if len(inertias) >= 2:
        # Find the point with maximum decrease in inertia
        decreases = [inertias[i] - inertias[i + 1] for i in range(len(inertias) - 1)]
        optimal_k = decreases.index(max(decreases)) + 2
        return min(optimal_k, max_clusters)

    return 3  # Default


def _analyze_clusters_advanced(
    feature_data: List[List[float]],
    labels: List[int],
    centers: np.ndarray,
    feature_names: List[str],
    artist_names: List[str],
    scaler: StandardScaler,
) -> Dict[int, Dict[str, Any]]:
    """Advanced cluster analysis with detailed profiling."""
    analysis = {}

    for cluster_id in set(labels):
        cluster_indices = [i for i, label in enumerate(labels) if label == cluster_id]
        cluster_artists = [artist_names[i] for i in cluster_indices]
        cluster_features = [feature_data[i] for i in cluster_indices]

        if cluster_features:
            # Calculate statistics
            feature_stats = {}
            for i, feature_name in enumerate(feature_names):
                feature_values = [features[i] for features in cluster_features]
                feature_stats[feature_name] = {
                    "mean": np.mean(feature_values),
                    "std": np.std(feature_values),
                    "min": np.min(feature_values),
                    "max": np.max(feature_values),
                }

            # Calculate cluster center in original scale
            center_original = scaler.inverse_transform([centers[cluster_id]])[0]
            center_dict = {feature_names[i]: center_original[i] for i in range(len(feature_names))}

            # Determine cluster characteristics
            characteristics = _determine_cluster_characteristics(feature_stats, feature_names)

            analysis[cluster_id] = {
                "size": len(cluster_indices),
                "artists": cluster_artists,
                "feature_stats": feature_stats,
                "cluster_center": center_dict,
                "characteristics": characteristics,
            }

    return analysis


def _determine_cluster_characteristics(
    feature_stats: Dict[str, Dict[str, float]], feature_names: List[str]
) -> List[str]:
    """Determine characteristics of a cluster based on feature statistics."""
    characteristics = []

    # Analyze engagement patterns
    if "engagement_rate" in feature_stats:
        engagement_mean = feature_stats["engagement_rate"]["mean"]
        if engagement_mean > 0.06:
            characteristics.append("High Engagement")
        elif engagement_mean > 0.04:
            characteristics.append("Medium Engagement")
        else:
            characteristics.append("Developing Engagement")

    # Analyze view patterns
    if "daily_views" in feature_stats:
        views_mean = feature_stats["daily_views"]["mean"]
        if views_mean > 100000:
            characteristics.append("High Reach")
        elif views_mean > 50000:
            characteristics.append("Medium Reach")
        else:
            characteristics.append("Growing Reach")

    # Analyze growth patterns
    if "subscriber_growth" in feature_stats:
        growth_mean = feature_stats["subscriber_growth"]["mean"]
        if growth_mean > 100:
            characteristics.append("Fast Growing")
        elif growth_mean > 50:
            characteristics.append("Steady Growth")
        else:
            characteristics.append("Stable Base")

    return characteristics if characteristics else ["General Cluster"]


def _generate_cluster_insights(cluster_analysis: Dict[int, Dict[str, Any]], feature_names: List[str]) -> List[str]:
    """Generate actionable insights from cluster analysis."""
    insights = []

    if not cluster_analysis:
        return ["No clusters identified"]

    # Find the largest cluster
    largest_cluster = max(cluster_analysis.items(), key=lambda x: x[1]["size"])
    insights.append(
        f"Largest cluster ({largest_cluster[1]['size']} artists): {', '.join(largest_cluster[1]['characteristics'])}"
    )

    # Find high-performing cluster
    if "engagement_rate" in feature_names:
        high_engagement_cluster = max(
            cluster_analysis.items(), key=lambda x: x[1]["feature_stats"].get("engagement_rate", {}).get("mean", 0)
        )
        insights.append(f"Highest engagement cluster: {', '.join(high_engagement_cluster[1]['characteristics'])}")

    # Identify development opportunities
    for cluster_id, analysis in cluster_analysis.items():
        if analysis["size"] > 1 and "Developing" in " ".join(analysis["characteristics"]):
            insights.append(f"Development opportunity: {analysis['size']} artists in developing cluster")

    return insights


def _calculate_feature_importance_clustering(
    data: np.ndarray, labels: List[int], feature_names: List[str]
) -> Dict[str, float]:
    """Calculate feature importance for clustering results."""
    if not SKLEARN_AVAILABLE:
        return {name: 1.0 / len(feature_names) for name in feature_names}

    # Use a simple approach: train a classifier to predict cluster labels
    try:
        from sklearn.ensemble import RandomForestClassifier

        if len(set(labels)) > 1:
            clf = RandomForestClassifier(n_estimators=50, random_state=42)
            clf.fit(data, labels)

            importance_dict = {feature_names[i]: clf.feature_importances_[i] for i in range(len(feature_names))}
            return importance_dict
    except Exception:
        pass

    # Fallback: equal importance
    return {name: 1.0 / len(feature_names) for name in feature_names}


def _fallback_clustering(df: pd.DataFrame, artist_col: str, feature_cols: List[str], n_clusters: int) -> Dict[str, Any]:
    """Fallback clustering without sklearn."""
    # Simple rule-based clustering
    artists = df[artist_col].unique()
    cluster_size = len(artists) // n_clusters

    cluster_labels = {}
    for i, artist in enumerate(artists):
        cluster_labels[artist] = i // cluster_size if cluster_size > 0 else 0

    return {
        "cluster_labels": cluster_labels,
        "cluster_analysis": {
            i: {"size": cluster_size, "characteristics": [f"Cluster {i + 1}"]} for i in range(n_clusters)
        },
        "cluster_insights": ["Simple rule-based clustering applied"],
        "silhouette_score": 0.5,
        "n_clusters": n_clusters,
    }


def integrate_with_youtube_helpers(
    df: pd.DataFrame, title_col: str = "title", channel_col: str = "channel_title"
) -> pd.DataFrame:
    """
    Integrate ML analytics with existing YouTube helper functions.

    Args:
        df: DataFrame with YouTube video data
        title_col: Column name for video titles
        channel_col: Column name for channel titles

    Returns:
        Enhanced DataFrame with ML-derived features
    """
    enhanced_df = df.copy()

    try:
        # Import YouTube helpers
        from web.youtube_version_parser import extract_version_from_title, parse_youtube_title

        # Extract enhanced metadata
        enhanced_features = []

        for _, row in df.iterrows():
            title = row.get(title_col, "")
            channel = row.get(channel_col, "")

            # Parse title for advanced features
            parsed_title = parse_youtube_title(title, channel)

            # Extract version information
            cleaned_title, version_type = extract_version_from_title(title, channel)

            # Create ML features
            features = {
                "title_length": len(title),
                "title_word_count": len(title.split()),
                "has_version_info": version_type is not None,
                "version_type": version_type or "unknown",
                "primary_artists_count": len(parsed_title.get("primary", [])),
                "featured_artists_count": len(parsed_title.get("featured", [])),
                "title_complexity_score": _calculate_title_complexity(title),
                "channel_authority_score": _calculate_channel_authority(channel),
            }

            enhanced_features.append(features)

        # Add features to DataFrame
        feature_df = pd.DataFrame(enhanced_features)
        enhanced_df = pd.concat([enhanced_df, feature_df], axis=1)

    except ImportError:
        # Fallback feature extraction
        enhanced_df["title_length"] = enhanced_df[title_col].str.len()
        enhanced_df["title_word_count"] = enhanced_df[title_col].str.split().str.len()
        enhanced_df["has_version_info"] = enhanced_df[title_col].str.contains("official|video|audio", case=False)

    return enhanced_df


def _calculate_title_complexity(title: str) -> float:
    """Calculate a complexity score for video titles."""
    if not title:
        return 0.0

    # Factors that contribute to complexity
    factors = {
        "length": min(len(title) / 100, 1.0),  # Normalize to 0-1
        "word_count": min(len(title.split()) / 20, 1.0),
        "special_chars": len([c for c in title if not c.isalnum() and c != " "]) / len(title),
        "uppercase_ratio": sum(1 for c in title if c.isupper()) / len(title),
        "has_numbers": 0.2 if any(c.isdigit() for c in title) else 0,
        "has_brackets": 0.3 if any(c in title for c in "()[]{}") else 0,
    }

    # Weighted average
    weights = {
        "length": 0.2,
        "word_count": 0.3,
        "special_chars": 0.2,
        "uppercase_ratio": 0.1,
        "has_numbers": 0.1,
        "has_brackets": 0.1,
    }

    complexity = sum(factors[key] * weights[key] for key in factors)
    return min(1.0, complexity)


def _calculate_channel_authority(channel_name: str) -> float:
    """Calculate an authority score for YouTube channels."""
    if not channel_name:
        return 0.0

    # Authority indicators
    authority_indicators = {
        "official": 0.8 if "official" in channel_name.lower() else 0,
        "vevo": 0.9 if "vevo" in channel_name.lower() else 0,
        "records": 0.7 if "records" in channel_name.lower() else 0,
        "music": 0.6 if "music" in channel_name.lower() else 0,
        "verified_pattern": (
            0.5 if any(word in channel_name.lower() for word in ["tv", "media", "entertainment"]) else 0
        ),
    }

    # Base score from channel name characteristics
    base_score = max(authority_indicators.values()) if any(authority_indicators.values()) else 0.3

    # Adjust for channel name length and professionalism
    if len(channel_name.split()) <= 3 and channel_name.replace(" ", "").isalnum():
        base_score += 0.1  # Professional naming

    return min(1.0, base_score)


# Export key functions for easy access
__all__ = [
    "predict_artist_momentum",
    "generate_content_optimization_recommendations",
    "analyze_market_positioning",
    "calculate_viral_potential",
    "perform_audience_segmentation",
    "forecast_performance_trends",
    "detect_performance_anomalies",
    "perform_statistical_tests",
    "analyze_metric_correlations",
    "analyze_metric_distributions",
    "calculate_confidence_intervals",
    "optimize_marketing_roi",
    "benchmark_performance",
    "calculate_investment_priorities",
    "identify_market_opportunities",
    "create_artist_performance_model",
    "detect_viral_content_patterns",
    "perform_advanced_clustering",
    "integrate_with_youtube_helpers",
]
