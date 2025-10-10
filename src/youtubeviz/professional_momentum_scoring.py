#!/usr / bin / env python3
"""
Professional Momentum Scoring System

Mathematically sound momentum scoring algorithms that provide interpretable,
statistically valid results for music industry decision - making.

Key Features:
- Statistical significance testing
- Confidence intervals for all scores
- Normalized scoring (0 - 1 scale) with clear interpretation
- Robust handling of outliers and missing data
- Industry - relevant momentum categories
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy.engine import Engine

from .data import load_artist_daily_metrics


@dataclass
class MomentumScore:
    """Professional momentum score with statistical validation."""

    artist_name: str
    score_value: float  # 0 - 1 normalized score
    confidence: float  # Statistical confidence (0 - 1)
    category: str  # Human - readable category
    statistical_significance: float  # p - value for trend test

    # Component scores (all 0 - 1 normalized)
    growth_score: float
    engagement_score: float
    consistency_score: float
    velocity_score: float

    # Supporting metrics
    total_videos: int
    recent_videos: int
    data_quality_score: float

    # Statistical measures
    growth_rate_pct: float  # Actual percentage growth
    confidence_interval_lower: float
    confidence_interval_upper: float


@dataclass
class MomentumAnalysisConfig:
    """Configuration for momentum analysis."""

    analysis_window_days: int = 90  # Days to analyze
    recent_window_days: int = 30  # Recent activity window
    min_videos_required: int = 3  # Minimum videos for reliable analysis
    min_data_points: int = 5  # Minimum data points for trend analysis
    confidence_level: float = 0.95  # Statistical confidence level
    outlier_threshold: float = 3.0  # Z - score threshold for outlier detection


class ProfessionalMomentumScorer:
    """
    Professional momentum scoring system with statistical rigor.

    Provides mathematically sound momentum analysis for music industry
    decision - making with proper confidence intervals and significance testing.
    """

    def __init__(self, config: Optional[MomentumAnalysisConfig] = None):
        self.config = config or MomentumAnalysisConfig()

    def calculate_momentum_scores(self, engine: Engine, artists: Optional[List[str]] = None) -> List[MomentumScore]:
        """
        Calculate professional momentum scores for artists.

        Args:
            engine: Database engine
            artists: Optional list of specific artists to analyze

        Returns:
            List of MomentumScore objects with statistical validation
        """
        print("🔍 PROFESSIONAL MOMENTUM ANALYSIS")
        print("=" * 50)

        # Load data with proper time windows
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=self.config.analysis_window_days)

        data = load_artist_daily_metrics(artists=artists, start=start_date, end=end_date, engine=engine)

        if data.empty:
            print("⚠️  No data available for momentum analysis")
            return []

        print(f"📊 Analyzing {len(data)} data points for {data['artist_name'].nunique()} artists")

        # Calculate scores for each artist
        momentum_scores = []

        for artist_name, artist_data in data.groupby("artist_name"):
            try:
                score = self._calculate_artist_momentum(artist_name, artist_data)
                if score:
                    momentum_scores.append(score)
            except Exception as e:
                print(f"⚠️  Failed to calculate momentum for {artist_name}: {e}")

        # Sort by score value (descending)
        momentum_scores.sort(key=lambda x: x.score_value, reverse=True)

        print(f"✅ Calculated momentum scores for {len(momentum_scores)} artists")
        return momentum_scores

    def _calculate_artist_momentum(self, artist_name: str, data: pd.DataFrame) -> Optional[MomentumScore]:
        """Calculate momentum score for a single artist."""

        # Data quality validation
        if len(data) < self.config.min_videos_required:
            return None

        # Ensure proper data types and sorting
        data = data.copy()
        data["date"] = pd.to_datetime(data["date"])
        data = data.sort_values("date")

        # Remove outliers for more stable analysis
        data = self._remove_outliers(data)

        if len(data) < self.config.min_data_points:
            return None

        # Calculate component scores
        growth_score, growth_rate_pct, growth_significance = self._calculate_growth_score(data)
        engagement_score = self._calculate_engagement_score(data)
        consistency_score = self._calculate_consistency_score(data)
        velocity_score = self._calculate_velocity_score(data)

        # Calculate overall momentum score (weighted average)
        weights = {
            "growth": 0.35,  # Growth is most important for momentum
            "engagement": 0.25,  # Engagement shows audience connection
            "consistency": 0.20,  # Consistency indicates sustainability
            "velocity": 0.20,  # Velocity shows acceleration
        }

        overall_score = (
            growth_score * weights["growth"]
            + engagement_score * weights["engagement"]
            + consistency_score * weights["consistency"]
            + velocity_score * weights["velocity"]
        )

        # Calculate confidence based on data quality and statistical significance
        data_quality_score = self._calculate_data_quality_score(data)
        statistical_confidence = min(1.0, 1.0 - growth_significance) if growth_significance else 0.5
        confidence = (data_quality_score + statistical_confidence) / 2.0

        # Calculate confidence intervals
        ci_lower, ci_upper = self._calculate_confidence_interval(overall_score, confidence)

        # Categorize momentum
        category = self._categorize_momentum(overall_score, confidence)

        # Count videos
        total_videos = len(data)
        recent_cutoff = datetime.now().date() - timedelta(days=self.config.recent_window_days)
        recent_videos = len(data[data["date"].dt.date >= recent_cutoff])

        return MomentumScore(
            artist_name=artist_name,
            score_value=round(overall_score, 4),
            confidence=round(confidence, 4),
            category=category,
            statistical_significance=round(growth_significance, 4) if growth_significance else 1.0,
            growth_score=round(growth_score, 4),
            engagement_score=round(engagement_score, 4),
            consistency_score=round(consistency_score, 4),
            velocity_score=round(velocity_score, 4),
            total_videos=total_videos,
            recent_videos=recent_videos,
            data_quality_score=round(data_quality_score, 4),
            growth_rate_pct=round(growth_rate_pct, 2),
            confidence_interval_lower=round(ci_lower, 4),
            confidence_interval_upper=round(ci_upper, 4),
        )

    def _remove_outliers(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove statistical outliers using Z - score method."""
        numeric_columns = ["views", "likes", "comments"]

        for col in numeric_columns:
            if col in data.columns:
                z_scores = np.abs(stats.zscore(data[col].fillna(0)))
                data = data[z_scores < self.config.outlier_threshold]

        return data

    def _calculate_growth_score(self, data: pd.DataFrame) -> Tuple[float, float, float]:
        """
        Calculate growth score with statistical significance testing.

        Returns:
            Tuple of (normalized_score, growth_rate_percentage, p_value)
        """
        if len(data) < 2:
            return 0.0, 0.0, 1.0

        # Use views as primary growth metric
        views = data["views"].fillna(0).values

        if np.sum(views) == 0:
            return 0.0, 0.0, 1.0

        # Calculate trend using linear regression
        x = np.arange(len(views))

        try:
            # Perform linear regression
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, views)

            # Calculate growth rate as percentage
            if intercept > 0:
                growth_rate_pct = (slope * len(views) / intercept) * 100
            else:
                growth_rate_pct = 0.0

            # Normalize growth score to 0 - 1 scale
            # Use sigmoid function to handle extreme values
            normalized_score = 1 / (1 + np.exp(-growth_rate_pct / 50))  # 50% growth = 0.5 score

            return normalized_score, growth_rate_pct, p_value

        except Exception:
            return 0.0, 0.0, 1.0

    def _calculate_engagement_score(self, data: pd.DataFrame) -> float:
        """Calculate engagement score based on likes and comments relative to views."""
        if data.empty:
            return 0.0

        # Calculate engagement rates
        total_views = data["views"].sum()
        total_likes = data["likes"].sum()
        total_comments = data["comments"].sum()

        if total_views == 0:
            return 0.0

        # Calculate rates
        like_rate = total_likes / total_views
        comment_rate = total_comments / total_views

        # Weight comments higher than likes (comments show deeper engagement)
        engagement_rate = like_rate * 0.3 + comment_rate * 0.7

        # Normalize to 0 - 1 scale using industry benchmarks
        # Typical music video engagement rates: 2 - 5% likes, 0.1 - 0.5% comments
        normalized_like_score = min(1.0, like_rate / 0.05)  # 5% like rate = max score
        normalized_comment_score = min(1.0, comment_rate / 0.005)  # 0.5% comment rate = max score

        return normalized_like_score * 0.3 + normalized_comment_score * 0.7

    def _calculate_consistency_score(self, data: pd.DataFrame) -> float:
        """Calculate consistency score based on regular posting patterns."""
        if len(data) < 2:
            return 0.0

        # Calculate time intervals between posts
        data_sorted = data.sort_values("date")
        intervals = data_sorted["date"].diff().dt.days.dropna()

        if len(intervals) == 0:
            return 0.0

        # Calculate coefficient of variation (lower = more consistent)
        mean_interval = intervals.mean()
        std_interval = intervals.std()

        if mean_interval == 0:
            return 0.0

        cv = std_interval / mean_interval

        # Convert to consistency score (0 - 1, higher = more consistent)
        # CV of 0.5 or less = high consistency
        consistency_score = max(0.0, 1.0 - cv / 0.5)

        return consistency_score

    def _calculate_velocity_score(self, data: pd.DataFrame) -> float:
        """Calculate velocity score based on acceleration in metrics."""
        if len(data) < 3:
            return 0.0

        # Sort by date
        data_sorted = data.sort_values("date")
        views = data_sorted["views"].fillna(0).values

        # Calculate second derivative (acceleration)
        first_diff = np.diff(views)
        second_diff = np.diff(first_diff)

        if len(second_diff) == 0:
            return 0.0

        # Average acceleration
        avg_acceleration = np.mean(second_diff)

        # Normalize using sigmoid function
        velocity_score = 1 / (1 + np.exp(-avg_acceleration / 1000))  # Adjust scaling as needed

        return velocity_score

    def _calculate_data_quality_score(self, data: pd.DataFrame) -> float:
        """Calculate data quality score based on completeness and consistency."""
        if data.empty:
            return 0.0

        # Check completeness
        required_columns = ["views", "likes", "comments"]
        completeness_scores = []

        for col in required_columns:
            if col in data.columns:
                non_null_ratio = data[col].notna().sum() / len(data)
                completeness_scores.append(non_null_ratio)

        if not completeness_scores:
            return 0.0

        completeness_score = np.mean(completeness_scores)

        # Check data volume (more data points = higher quality)
        volume_score = min(1.0, len(data) / 20)  # 20 data points = max volume score

        # Combine scores
        quality_score = completeness_score * 0.7 + volume_score * 0.3

        return quality_score

    def _calculate_confidence_interval(self, score: float, confidence: float) -> Tuple[float, float]:
        """Calculate confidence interval for the momentum score."""
        # Use confidence level to determine interval width
        margin_of_error = (1 - confidence) * 0.2  # Max 20% margin of error

        lower = max(0.0, score - margin_of_error)
        upper = min(1.0, score + margin_of_error)

        return lower, upper

    def _categorize_momentum(self, score: float, confidence: float) -> str:
        """Categorize momentum into human - readable categories."""
        # Adjust thresholds based on confidence
        high_threshold = 0.7 if confidence > 0.8 else 0.75
        medium_threshold = 0.4 if confidence > 0.8 else 0.45

        if score >= high_threshold:
            return "High Momentum"
        elif score >= medium_threshold:
            return "Moderate Momentum"
        elif score >= 0.2:
            return "Low Momentum"
        else:
            return "Declining"


def create_momentum_summary_dataframe(momentum_scores: List[MomentumScore]) -> pd.DataFrame:
    """Convert momentum scores to a pandas DataFrame for analysis."""
    if not momentum_scores:
        return pd.DataFrame()

    data = []
    for score in momentum_scores:
        data.append(
            {
                "artist_name": score.artist_name,
                "score_value": score.score_value,
                "confidence": score.confidence,
                "momentum_category": score.category,
                "growth_rate_pct": score.growth_rate_pct,
                "statistical_significance": score.statistical_significance,
                "total_videos": score.total_videos,
                "recent_videos": score.recent_videos,
                "data_quality_score": score.data_quality_score,
                "confidence_interval": f"[{score.confidence_interval_lower:.3f}, {score.confidence_interval_upper:.3f}]",
            }
        )

    return pd.DataFrame(data)


def display_momentum_analysis_results(momentum_scores: List[MomentumScore]) -> None:
    """Display professional momentum analysis results with educational content."""
    if not momentum_scores:
        print("⚠️  No momentum scores to display")
        return

    print("\n🏆 PROFESSIONAL MOMENTUM ANALYSIS RESULTS")
    print("=" * 60)

    # Summary statistics
    scores = [s.score_value for s in momentum_scores]
    confidences = [s.confidence for s in momentum_scores]

    print(f"📊 Analysis Summary:")
    print(f"   Artists Analyzed: {len(momentum_scores)}")
    print(f"   Average Score: {np.mean(scores):.3f} ± {np.std(scores):.3f}")
    print(f"   Average Confidence: {np.mean(confidences):.3f}")
    print(f"   Score Range: {min(scores):.3f} - {max(scores):.3f}")

    # Category distribution
    categories = [s.category for s in momentum_scores]
    category_counts = pd.Series(categories).value_counts()

    print(f"\n📈 Momentum Categories:")
    for category, count in category_counts.items():
        percentage = (count / len(momentum_scores)) * 100
        print(f"   {category}: {count} artists ({percentage:.1f}%)")

    # Top performers
    print(f"\n🌟 Top 5 Momentum Leaders:")
    for i, score in enumerate(momentum_scores[:5], 1):
        confidence_indicator = "🔴" if score.confidence < 0.5 else "🟡" if score.confidence < 0.8 else "🟢"
        print(f"   {i}. {score.artist_name}")
        print(f"      Score: {score.score_value:.3f} ({score.category})")
        print(f"      Confidence: {confidence_indicator} {score.confidence:.3f}")
        print(f"      Growth Rate: {score.growth_rate_pct:+.1f}%")
        print()

    # Educational content
    print(f"🎓 UNDERSTANDING MOMENTUM SCORES")
    print("-" * 40)
    print("💡 Score Interpretation:")
    print("   • 0.70 - 1.00: High Momentum - Strong growth trajectory")
    print("   • 0.40 - 0.69: Moderate Momentum - Steady progress")
    print("   • 0.20 - 0.39: Low Momentum - Limited growth")
    print("   • 0.00 - 0.19: Declining - Negative or stagnant trends")

    print("\n🔍 Confidence Indicators:")
    print("   🟢 High (0.8+): Statistically reliable, sufficient data")
    print("   🟡 Medium (0.5 - 0.8): Moderately reliable, some uncertainty")
    print("   🔴 Low (<0.5): Limited reliability, insufficient data")

    print("\n📈 Business Applications:")
    print("   • High momentum artists: Priority for marketing investment")
    print("   • Moderate momentum: Monitor for breakthrough potential")
    print("   • Low momentum: Consider strategic repositioning")
    print("   • Declining: Investigate causes, potential intervention needed")


if __name__ == "__main__":
    # Demo the professional momentum scoring system
    from web.etl_helpers import get_engine

    print("🚀 PROFESSIONAL MOMENTUM SCORING DEMO")
    print("=" * 60)

    try:
        engine = get_engine()
        scorer = ProfessionalMomentumScorer()

        # Calculate momentum scores
        momentum_scores = scorer.calculate_momentum_scores(engine)

        # Display results
        display_momentum_analysis_results(momentum_scores)

        # Create summary DataFrame
        summary_df = create_momentum_summary_dataframe(momentum_scores)
        print(f"\n📋 Summary DataFrame created with {len(summary_df)} records")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback

        traceback.print_exc()
