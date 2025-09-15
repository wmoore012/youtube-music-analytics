"""
Artist Momentum Analysis Module

This module calculates momentum scores for artists based on:
- View velocity (growth rate)
- Engagement acceleration
- Performance consistency
- Viral coefficient

Momentum helps identify rising artists and investment opportunities.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class MomentumConfig:
    """Configuration for momentum analysis."""

    # Weights for momentum components (must sum to 1.0)
    view_velocity_weight: float = 0.4
    engagement_growth_weight: float = 0.3
    consistency_weight: float = 0.3

    # Thresholds for momentum classification
    high_momentum_threshold: float = 50.0
    medium_momentum_threshold: float = 0.0

    # Minimum data points required for reliable calculation
    min_data_points: int = 3

    # Smoothing factor for velocity calculations (0-1)
    smoothing_factor: float = 0.3


class ArtistMomentumAnalyzer:
    """Analyzes artist momentum based on multiple performance factors."""

    def __init__(self, config: MomentumConfig = None):
        self.config = config or MomentumConfig()

    def calculate_view_velocity(self, daily_data: pd.DataFrame) -> float:
        """
        Calculate view velocity (daily growth rate).

        Args:
            daily_data: DataFrame with columns ['date', 'views']

        Returns:
            Average daily growth rate as percentage
        """
        if len(daily_data) < 2:
            return 0.0

        # Sort by date
        data = daily_data.sort_values("date").copy()

        # Calculate daily growth rates
        data["prev_views"] = data["views"].shift(1)
        data["growth_rate"] = ((data["views"] - data["prev_views"]) / data["prev_views"].replace(0, np.nan)) * 100

        # Remove infinite and NaN values
        growth_rates = data["growth_rate"].replace([np.inf, -np.inf], np.nan).dropna()

        if len(growth_rates) == 0:
            return 0.0

        # Apply exponential smoothing (recent data weighted more heavily)
        weights = np.exp(np.linspace(0, 1, len(growth_rates)))
        weighted_average = np.average(growth_rates, weights=weights)

        return float(weighted_average)

    def calculate_engagement_growth(self, daily_data: pd.DataFrame) -> float:
        """
        Calculate engagement growth rate.

        Args:
            daily_data: DataFrame with columns ['date', 'views', 'likes', 'comments']

        Returns:
            Engagement growth rate as percentage
        """
        if len(daily_data) < 2:
            return 0.0

        # Sort by date
        data = daily_data.sort_values("date").copy()

        # Calculate engagement rates
        data["engagement_rate"] = ((data["likes"] + data["comments"]) / data["views"].replace(0, np.nan)) * 100

        # Calculate growth in engagement rate
        data["prev_engagement"] = data["engagement_rate"].shift(1)
        data["engagement_growth"] = (
            (data["engagement_rate"] - data["prev_engagement"]) / data["prev_engagement"].replace(0, np.nan)
        ) * 100

        # Remove infinite and NaN values
        growth_rates = data["engagement_growth"].replace([np.inf, -np.inf], np.nan).dropna()

        if len(growth_rates) == 0:
            return 0.0

        # Apply exponential smoothing
        weights = np.exp(np.linspace(0, 1, len(growth_rates)))
        weighted_average = np.average(growth_rates, weights=weights)

        return float(weighted_average)

    def calculate_consistency_score(self, daily_data: pd.DataFrame) -> float:
        """
        Calculate performance consistency score.

        Args:
            daily_data: DataFrame with columns ['date', 'views']

        Returns:
            Consistency score (0-1, higher = more consistent)
        """
        if len(daily_data) < self.config.min_data_points:
            return 0.0

        views = daily_data["views"]

        if views.mean() == 0:
            return 0.0

        # Calculate coefficient of variation (lower = more consistent)
        cv = views.std() / views.mean()

        # Convert to consistency score (0-1 scale)
        # Use exponential decay to map CV to consistency
        consistency = np.exp(-cv)

        return float(np.clip(consistency, 0, 1))

    def calculate_viral_coefficient(self, video_data: pd.DataFrame) -> float:
        """
        Calculate viral coefficient based on view distribution.

        Args:
            video_data: DataFrame with video-level data

        Returns:
            Viral coefficient (0-100, higher = more viral potential)
        """
        if len(video_data) == 0:
            return 0.0

        views = video_data["views"]

        if len(views) < 2:
            return 0.0

        # Calculate what percentage of videos are in top 20% by views
        top_20_threshold = views.quantile(0.8)
        top_20_count = (views >= top_20_threshold).sum()
        viral_ratio = top_20_count / len(views)

        # Calculate view concentration (how much top videos dominate)
        sorted_views = views.sort_values(ascending=False)
        top_10_percent = int(max(1, len(sorted_views) * 0.1))
        top_views_share = sorted_views.head(top_10_percent).sum() / sorted_views.sum()

        # Combine viral ratio and concentration
        viral_coefficient = (viral_ratio * 50) + (top_views_share * 50)

        return float(np.clip(viral_coefficient, 0, 100))

    def calculate_momentum_score(self, artist_data: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate overall momentum score for an artist.

        Args:
            artist_data: DataFrame with artist's performance data

        Returns:
            Dict with momentum components and overall score
        """
        # Prepare daily aggregated data
        daily_data = (
            artist_data.groupby("date")
            .agg({"views": "sum", "likes": "sum", "comments": "sum"})
            .reset_index()
            .sort_values("date")
        )

        # Calculate components
        view_velocity = self.calculate_view_velocity(daily_data)
        engagement_growth = self.calculate_engagement_growth(daily_data)
        consistency = self.calculate_consistency_score(daily_data)
        viral_coefficient = self.calculate_viral_coefficient(artist_data)

        # Calculate weighted momentum score
        momentum_score = (
            (view_velocity * self.config.view_velocity_weight)
            + (engagement_growth * self.config.engagement_growth_weight)
            + (consistency * 100 * self.config.consistency_weight)  # Scale consistency to 0-100
        )

        # Classify momentum level
        if momentum_score >= self.config.high_momentum_threshold:
            momentum_level = "HIGH"
            momentum_emoji = "🚀"
        elif momentum_score >= self.config.medium_momentum_threshold:
            momentum_level = "MEDIUM"
            momentum_emoji = "📈"
        else:
            momentum_level = "LOW"
            momentum_emoji = "📉"

        return {
            "momentum_score": round(momentum_score, 2),
            "momentum_level": momentum_level,
            "momentum_emoji": momentum_emoji,
            "view_velocity": round(view_velocity, 2),
            "engagement_growth": round(engagement_growth, 2),
            "consistency_score": round(consistency, 3),
            "viral_coefficient": round(viral_coefficient, 2),
            "data_points": len(daily_data),
        }

    def analyze_portfolio_momentum(self, portfolio_data: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze momentum for all artists in a portfolio.

        Args:
            portfolio_data: DataFrame with multi-artist performance data

        Returns:
            DataFrame with momentum analysis for each artist
        """
        results = []

        for artist in portfolio_data["artist_name"].unique():
            artist_data = portfolio_data[portfolio_data["artist_name"] == artist]

            momentum_analysis = self.calculate_momentum_score(artist_data)
            momentum_analysis["artist_name"] = artist

            results.append(momentum_analysis)

        # Convert to DataFrame and sort by momentum score
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values("momentum_score", ascending=False)

        return results_df

    def get_investment_recommendations(self, momentum_df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Generate investment recommendations based on momentum analysis.

        Args:
            momentum_df: DataFrame from analyze_portfolio_momentum

        Returns:
            Dict with investment recommendations by category
        """
        recommendations = {"high_priority": [], "medium_priority": [], "monitor": [], "reassess": []}

        for _, row in momentum_df.iterrows():
            artist = row["artist_name"]
            momentum_level = row["momentum_level"]
            momentum_score = row["momentum_score"]

            if momentum_level == "HIGH":
                recommendations["high_priority"].append(artist)
            elif momentum_level == "MEDIUM" and momentum_score > 25:
                recommendations["medium_priority"].append(artist)
            elif momentum_level == "MEDIUM":
                recommendations["monitor"].append(artist)
            else:
                recommendations["reassess"].append(artist)

        return recommendations


def create_momentum_report(portfolio_data: pd.DataFrame, config: MomentumConfig = None) -> Dict:
    """
    Create a comprehensive momentum analysis report.

    Args:
        portfolio_data: DataFrame with artist performance data
        config: Optional momentum configuration

    Returns:
        Dict with complete momentum analysis and recommendations
    """
    analyzer = ArtistMomentumAnalyzer(config)

    # Analyze momentum for all artists
    momentum_df = analyzer.analyze_portfolio_momentum(portfolio_data)

    # Get investment recommendations
    recommendations = analyzer.get_investment_recommendations(momentum_df)

    # Calculate portfolio-level metrics
    portfolio_metrics = {
        "total_artists": len(momentum_df),
        "high_momentum_artists": len(momentum_df[momentum_df["momentum_level"] == "HIGH"]),
        "average_momentum": momentum_df["momentum_score"].mean(),
        "momentum_range": {"min": momentum_df["momentum_score"].min(), "max": momentum_df["momentum_score"].max()},
    }

    return {
        "momentum_analysis": momentum_df,
        "recommendations": recommendations,
        "portfolio_metrics": portfolio_metrics,
        "analysis_date": datetime.now().isoformat(),
        "config_used": config or MomentumConfig(),
    }
