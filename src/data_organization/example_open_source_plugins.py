"""
Example open-source plugins for music analytics scoring algorithms.

These plugins demonstrate common patterns in music data analysis that researchers
can use as starting points for their own algorithms. All examples work with
real YouTube analytics data structures.

Designed for music data researchers to extend and customize for their needs.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

from src.data_organization.open_source_plugin_framework import (
    OpenSourceScoringPlugin,
    PluginMetadata
)
from src.data_organization.notebook_validator import ValidationResult

logger = logging.getLogger(__name__)


class ViewVelocityPlugin(OpenSourceScoringPlugin):
    """
    Calculates view velocity scores based on recent view growth patterns.
    
    This plugin analyzes how quickly videos are gaining views over time,
    which is useful for identifying trending content and viral potential.
    
    Works with real youtube_videos and youtube_metrics data.
    """

    def get_name(self) -> str:
        return "view_velocity"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="view_velocity",
            version="1.0.0",
            author="Music Analytics Community",
            description="Calculates view velocity scores based on recent view growth patterns",
            parameters={
                "time_window_days": 7,
                "min_views_threshold": 1000,
                "velocity_weight": 0.7,
                "acceleration_weight": 0.3
            },
            input_requirements=[
                "video_id", "view_count", "published_date", "analytics_date"
            ],
            output_schema={
                "video_id": "object",
                "view_velocity_score": "float64",
                "daily_view_rate": "float64",
                "velocity_category": "object"
            },
            license="MIT",
            repository_url="https://github.com/music-analytics/youtube-plugins",
            tags=["velocity", "trending", "growth", "views"]
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for view velocity calculation."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            checked_items=0,
            passed_items=0
        )

        # Check required columns
        required_cols = self.get_metadata().input_requirements
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Check data types
        if 'view_count' in data.columns:
            result.checked_items += 1
            if not pd.api.types.is_numeric_dtype(data['view_count']):
                result.add_error("view_count must be numeric")
            else:
                result.passed_items += 1

        # Check for minimum data
        result.checked_items += 1
        if len(data) < 2:
            result.add_error("Need at least 2 data points to calculate velocity")
        else:
            result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate view velocity scores."""
        self._record_execution_start()
        
        try:
            # Configuration
            time_window = self.config.get('time_window_days', 7)
            min_views = self.config.get('min_views_threshold', 1000)
            velocity_weight = self.config.get('velocity_weight', 0.7)
            acceleration_weight = self.config.get('acceleration_weight', 0.3)

            # Ensure datetime columns
            data = data.copy()
            if 'published_date' in data.columns:
                data['published_date'] = pd.to_datetime(data['published_date'])
            if 'analytics_date' in data.columns:
                data['analytics_date'] = pd.to_datetime(data['analytics_date'])

            results = []

            # Group by video to calculate velocity for each
            for video_id, video_data in data.groupby('video_id'):
                if len(video_data) < 2:
                    # For single data points, create a basic score
                    current_views = video_data['view_count'].iloc[0]
                    if current_views >= min_views:
                        # Simple score based on view count relative to threshold
                        velocity_score = min(current_views / 100000, 1.0) * 0.5  # Max 0.5 for single point
                        category = "insufficient_data"
                        
                        results.append({
                            'video_id': video_id,
                            'view_velocity_score': velocity_score,
                            'daily_view_rate': 0.0,
                            'velocity_category': category
                        })
                    continue

                # Sort by analytics date
                video_data = video_data.sort_values('analytics_date')
                
                # Calculate daily view changes
                video_data['view_diff'] = video_data['view_count'].diff()
                video_data['days_diff'] = (video_data['analytics_date'] - 
                                         video_data['analytics_date'].shift(1)).dt.days
                
                # Calculate daily view rate
                video_data['daily_view_rate'] = video_data['view_diff'] / video_data['days_diff']
                
                # Get recent data within time window
                recent_cutoff = video_data['analytics_date'].max() - timedelta(days=time_window)
                recent_data = video_data[video_data['analytics_date'] >= recent_cutoff]
                
                if len(recent_data) < 2:
                    continue

                # Calculate velocity metrics
                avg_daily_rate = recent_data['daily_view_rate'].mean()
                velocity_trend = recent_data['daily_view_rate'].diff().mean()  # Acceleration
                
                # Normalize velocity score (0-1 scale)
                current_views = video_data['view_count'].iloc[-1]
                if current_views < min_views:
                    velocity_score = 0.0
                else:
                    # Combine velocity and acceleration
                    velocity_component = min(avg_daily_rate / 10000, 1.0)  # Normalize to 10k views/day
                    acceleration_component = max(min(velocity_trend / 1000, 1.0), -1.0)  # Normalize acceleration
                    
                    velocity_score = (velocity_weight * velocity_component + 
                                    acceleration_weight * max(acceleration_component, 0))

                # Categorize velocity
                if velocity_score >= 0.8:
                    category = "viral"
                elif velocity_score >= 0.6:
                    category = "high_growth"
                elif velocity_score >= 0.3:
                    category = "moderate_growth"
                else:
                    category = "slow_growth"

                results.append({
                    'video_id': video_id,
                    'view_velocity_score': velocity_score,
                    'daily_view_rate': avg_daily_rate,
                    'velocity_category': category
                })

            result_df = pd.DataFrame(results)
            
            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise


class EngagementQualityPlugin(OpenSourceScoringPlugin):
    """
    Analyzes engagement quality beyond simple ratios.
    
    This plugin looks at engagement patterns, comment sentiment,
    and engagement sustainability to provide a more nuanced
    view of audience connection quality.
    
    Works with youtube_videos, youtube_comments, and sentiment data.
    """

    def get_name(self) -> str:
        return "engagement_quality"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="engagement_quality",
            version="1.0.0",
            author="Music Analytics Community",
            description="Analyzes engagement quality beyond simple ratios using sentiment and patterns",
            parameters={
                "sentiment_weight": 0.4,
                "consistency_weight": 0.3,
                "diversity_weight": 0.3,
                "min_comments": 10
            },
            input_requirements=[
                "video_id", "view_count", "like_count", "comment_count"
            ],
            output_schema={
                "video_id": "object",
                "engagement_quality_score": "float64",
                "sentiment_factor": "float64",
                "engagement_consistency": "float64",
                "quality_category": "object"
            },
            license="MIT",
            repository_url="https://github.com/music-analytics/youtube-plugins",
            tags=["engagement", "quality", "sentiment", "audience"]
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for engagement quality calculation."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            checked_items=0,
            passed_items=0
        )

        # Check required columns
        required_cols = ["video_id", "view_count", "like_count", "comment_count"]
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Check for numeric columns
        numeric_cols = ["view_count", "like_count", "comment_count"]
        for col in numeric_cols:
            if col in data.columns:
                result.checked_items += 1
                if not pd.api.types.is_numeric_dtype(data[col]):
                    result.add_error(f"Column '{col}' must be numeric")
                else:
                    result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate engagement quality scores."""
        self._record_execution_start()
        
        try:
            # Configuration
            sentiment_weight = self.config.get('sentiment_weight', 0.4)
            consistency_weight = self.config.get('consistency_weight', 0.3)
            diversity_weight = self.config.get('diversity_weight', 0.3)
            min_comments = self.config.get('min_comments', 10)

            data = data.copy()
            results = []

            for _, row in data.iterrows():
                video_id = row['video_id']
                view_count = row['view_count']
                like_count = row['like_count']
                comment_count = row['comment_count']

                # Skip videos with insufficient engagement
                if comment_count < min_comments or view_count == 0:
                    results.append({
                        'video_id': video_id,
                        'engagement_quality_score': 0.0,
                        'sentiment_factor': 0.0,
                        'engagement_consistency': 0.0,
                        'quality_category': 'insufficient_data'
                    })
                    continue

                # Calculate basic engagement ratios
                like_ratio = like_count / view_count
                comment_ratio = comment_count / view_count

                # Engagement consistency (how balanced likes vs comments are)
                expected_comment_ratio = like_ratio * 0.1  # Typical comment:like ratio
                consistency_score = 1.0 - abs(comment_ratio - expected_comment_ratio) / expected_comment_ratio

                # Sentiment factor (placeholder - would integrate with real sentiment data)
                # In real implementation, this would query youtube_comments with sentiment scores
                sentiment_factor = self._estimate_sentiment_factor(like_ratio, comment_ratio)

                # Engagement diversity (how well-distributed engagement is)
                diversity_score = min(like_ratio * 1000, 1.0) * min(comment_ratio * 10000, 1.0)

                # Combine factors
                quality_score = (
                    sentiment_weight * sentiment_factor +
                    consistency_weight * max(consistency_score, 0) +
                    diversity_weight * diversity_score
                )

                # Categorize quality
                if quality_score >= 0.8:
                    category = "exceptional"
                elif quality_score >= 0.6:
                    category = "high_quality"
                elif quality_score >= 0.4:
                    category = "moderate_quality"
                else:
                    category = "low_quality"

                results.append({
                    'video_id': video_id,
                    'engagement_quality_score': quality_score,
                    'sentiment_factor': sentiment_factor,
                    'engagement_consistency': max(consistency_score, 0),
                    'quality_category': category
                })

            result_df = pd.DataFrame(results)
            
            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise

    def _estimate_sentiment_factor(self, like_ratio: float, comment_ratio: float) -> float:
        """
        Estimate sentiment factor based on engagement patterns.
        
        In a real implementation, this would query actual sentiment data
        from youtube_comments table with sentiment_score column.
        """
        # Higher like ratio generally indicates positive sentiment
        # Higher comment ratio could indicate controversy or strong engagement
        
        if like_ratio > 0.05:  # Very high like ratio
            return 0.9
        elif like_ratio > 0.02:  # Good like ratio
            return 0.7
        elif like_ratio > 0.01:  # Average like ratio
            return 0.5
        else:
            return 0.3


class CrossPlatformMomentumPlugin(OpenSourceScoringPlugin):
    """
    Calculates momentum scores that can work across different platforms.
    
    This plugin provides a standardized momentum calculation that can be
    applied to YouTube, Spotify, or other platform data by normalizing
    different engagement metrics to a common scale.
    
    Useful for music researchers comparing artists across platforms.
    """

    def get_name(self) -> str:
        return "cross_platform_momentum"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="cross_platform_momentum",
            version="1.0.0",
            author="Music Analytics Community",
            description="Platform-agnostic momentum calculation for cross-platform music analysis",
            parameters={
                "growth_window_days": 30,
                "velocity_weight": 0.4,
                "acceleration_weight": 0.3,
                "consistency_weight": 0.3,
                "platform_normalization": True
            },
            input_requirements=[
                "entity_id", "metric_value", "metric_date", "platform"
            ],
            output_schema={
                "entity_id": "object",
                "momentum_score": "float64",
                "growth_velocity": "float64",
                "growth_acceleration": "float64",
                "momentum_category": "object",
                "platform": "object"
            },
            license="MIT",
            repository_url="https://github.com/music-analytics/youtube-plugins",
            tags=["momentum", "cross-platform", "growth", "standardized"]
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for cross-platform momentum calculation."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            checked_items=0,
            passed_items=0
        )

        # Check required columns
        required_cols = self.get_metadata().input_requirements
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Check data types
        if 'metric_value' in data.columns:
            result.checked_items += 1
            if not pd.api.types.is_numeric_dtype(data['metric_value']):
                result.add_error("metric_value must be numeric")
            else:
                result.passed_items += 1

        # Check for minimum data per entity
        if 'entity_id' in data.columns:
            result.checked_items += 1
            entity_counts = data['entity_id'].value_counts()
            if entity_counts.min() < 3:
                result.add_warning("Some entities have fewer than 3 data points - momentum calculation may be less accurate")
            result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate cross-platform momentum scores."""
        self._record_execution_start()
        
        try:
            # Configuration
            window_days = self.config.get('growth_window_days', 30)
            velocity_weight = self.config.get('velocity_weight', 0.4)
            acceleration_weight = self.config.get('acceleration_weight', 0.3)
            consistency_weight = self.config.get('consistency_weight', 0.3)
            normalize_platforms = self.config.get('platform_normalization', True)

            data = data.copy()
            data['metric_date'] = pd.to_datetime(data['metric_date'])
            
            results = []

            # Process each entity separately
            for entity_id, entity_data in data.groupby('entity_id'):
                if len(entity_data) < 3:
                    # For insufficient data, create a basic score
                    if len(entity_data) >= 1:
                        platform = entity_data['platform'].iloc[0]
                        # Basic score based on single data point
                        momentum_score = 0.3  # Default moderate score for insufficient data
                        category = "insufficient_data"
                        
                        results.append({
                            'entity_id': entity_id,
                            'momentum_score': momentum_score,
                            'growth_velocity': 0.0,
                            'growth_acceleration': 0.0,
                            'momentum_category': category,
                            'platform': platform
                        })
                    continue

                # Sort by date
                entity_data = entity_data.sort_values('metric_date')
                
                # Get platform for normalization
                platform = entity_data['platform'].iloc[0]
                
                # Calculate time-based metrics
                entity_data['days_since_start'] = (
                    entity_data['metric_date'] - entity_data['metric_date'].min()
                ).dt.days
                
                # Apply platform normalization if enabled
                if normalize_platforms:
                    normalized_values = self._normalize_by_platform(
                        entity_data['metric_value'], platform
                    )
                else:
                    normalized_values = entity_data['metric_value']

                # Calculate growth metrics
                growth_rates = normalized_values.pct_change().fillna(0)
                
                # Focus on recent window
                recent_cutoff = entity_data['metric_date'].max() - timedelta(days=window_days)
                recent_mask = entity_data['metric_date'] >= recent_cutoff
                recent_growth = growth_rates[recent_mask]
                
                if len(recent_growth) < 2:
                    continue

                # Calculate momentum components
                velocity = recent_growth.mean()  # Average growth rate
                acceleration = recent_growth.diff().mean()  # Change in growth rate
                consistency = 1.0 - (recent_growth.std() / (abs(recent_growth.mean()) + 0.001))  # Growth consistency

                # Combine into momentum score
                momentum_score = (
                    velocity_weight * max(velocity, 0) +
                    acceleration_weight * max(acceleration, 0) +
                    consistency_weight * max(consistency, 0)
                )

                # Normalize to 0-1 scale
                momentum_score = min(max(momentum_score, 0), 1)

                # Categorize momentum
                if momentum_score >= 0.8:
                    category = "explosive"
                elif momentum_score >= 0.6:
                    category = "strong"
                elif momentum_score >= 0.4:
                    category = "moderate"
                elif momentum_score >= 0.2:
                    category = "weak"
                else:
                    category = "declining"

                results.append({
                    'entity_id': entity_id,
                    'momentum_score': momentum_score,
                    'growth_velocity': velocity,
                    'growth_acceleration': acceleration,
                    'momentum_category': category,
                    'platform': platform
                })

            result_df = pd.DataFrame(results)
            
            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise

    def _normalize_by_platform(self, values: pd.Series, platform: str) -> pd.Series:
        """
        Normalize metric values by platform to enable cross-platform comparison.
        
        This applies platform-specific scaling factors based on typical
        engagement patterns for each platform.
        """
        # Platform normalization factors (based on typical engagement scales)
        normalization_factors = {
            'youtube': 1.0,      # Base platform
            'spotify': 0.1,      # Spotify streams are typically 10x higher volume
            'tiktok': 2.0,       # TikTok views are typically lower per video
            'instagram': 1.5,    # Instagram engagement is typically higher ratio
            'twitter': 5.0       # Twitter engagement is typically much lower volume
        }
        
        factor = normalization_factors.get(platform.lower(), 1.0)
        return values * factor


class GenreSpecificScoringPlugin(OpenSourceScoringPlugin):
    """
    Provides genre-specific scoring that accounts for different engagement
    patterns across music genres.
    
    This plugin recognizes that different genres have different typical
    engagement patterns and adjusts scoring accordingly.
    
    Useful for fair comparison of artists within and across genres.
    """

    def get_name(self) -> str:
        return "genre_specific_scoring"

    def get_version(self) -> str:
        return "1.0.0"

    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="genre_specific_scoring",
            version="1.0.0",
            author="Music Analytics Community",
            description="Genre-aware scoring that accounts for different engagement patterns by music genre",
            parameters={
                "genre_weights": {
                    "pop": {"viral_factor": 1.2, "engagement_threshold": 0.03},
                    "hip_hop": {"viral_factor": 1.5, "engagement_threshold": 0.025},
                    "rock": {"viral_factor": 0.8, "engagement_threshold": 0.02},
                    "electronic": {"viral_factor": 1.0, "engagement_threshold": 0.015},
                    "country": {"viral_factor": 0.9, "engagement_threshold": 0.035},
                    "default": {"viral_factor": 1.0, "engagement_threshold": 0.025}
                },
                "cross_genre_normalization": True
            },
            input_requirements=[
                "entity_id", "genre", "view_count", "like_count", "comment_count"
            ],
            output_schema={
                "entity_id": "object",
                "genre_adjusted_score": "float64",
                "raw_engagement_score": "float64",
                "genre_factor": "float64",
                "genre": "object",
                "performance_vs_genre": "object"
            },
            license="MIT",
            repository_url="https://github.com/music-analytics/youtube-plugins",
            tags=["genre", "music", "normalization", "fair-comparison"]
        )

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for genre-specific scoring."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            checked_items=0,
            passed_items=0
        )

        # Check required columns
        required_cols = self.get_metadata().input_requirements
        for col in required_cols:
            result.checked_items += 1
            if col not in data.columns:
                result.add_error(f"Required column '{col}' not found")
            else:
                result.passed_items += 1

        # Check for valid genres
        if 'genre' in data.columns:
            result.checked_items += 1
            null_genres = data['genre'].isnull().sum()
            if null_genres > 0:
                result.add_warning(f"{null_genres} rows have missing genre information")
            result.passed_items += 1

        return result

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate genre-specific scores."""
        self._record_execution_start()
        
        try:
            # Configuration
            genre_weights = self.config.get('genre_weights', {})
            normalize_cross_genre = self.config.get('cross_genre_normalization', True)

            data = data.copy()
            results = []

            for _, row in data.iterrows():
                entity_id = row['entity_id']
                genre = row.get('genre', 'unknown').lower()
                view_count = row['view_count']
                like_count = row['like_count']
                comment_count = row['comment_count']

                # Get genre-specific parameters
                genre_params = genre_weights.get(genre, genre_weights.get('default', {
                    'viral_factor': 1.0,
                    'engagement_threshold': 0.025
                }))

                # Calculate raw engagement score
                if view_count > 0:
                    like_ratio = like_count / view_count
                    comment_ratio = comment_count / view_count
                    raw_engagement = (like_ratio + comment_ratio * 10) / 2  # Weight comments more
                else:
                    raw_engagement = 0.0

                # Apply genre-specific adjustments
                viral_factor = genre_params.get('viral_factor', 1.0)
                engagement_threshold = genre_params.get('engagement_threshold', 0.025)

                # Adjust score based on genre expectations
                genre_factor = viral_factor
                if raw_engagement > engagement_threshold:
                    # Bonus for exceeding genre threshold
                    genre_factor *= (1 + (raw_engagement - engagement_threshold) / engagement_threshold)

                genre_adjusted_score = raw_engagement * genre_factor

                # Normalize to 0-1 scale
                genre_adjusted_score = min(genre_adjusted_score, 1.0)

                # Determine performance vs genre average
                if raw_engagement > engagement_threshold * 1.5:
                    performance = "above_genre_average"
                elif raw_engagement > engagement_threshold:
                    performance = "at_genre_average"
                else:
                    performance = "below_genre_average"

                results.append({
                    'entity_id': entity_id,
                    'genre_adjusted_score': genre_adjusted_score,
                    'raw_engagement_score': raw_engagement,
                    'genre_factor': genre_factor,
                    'genre': genre,
                    'performance_vs_genre': performance
                })

            result_df = pd.DataFrame(results)
            
            # Apply cross-genre normalization if enabled
            if normalize_cross_genre and len(result_df) > 0:
                result_df = self._apply_cross_genre_normalization(result_df)
            
            self._record_execution_end(True)
            return result_df

        except Exception as e:
            self._record_execution_end(False, str(e))
            raise

    def _apply_cross_genre_normalization(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply cross-genre normalization to make scores comparable across genres.
        
        This ensures that artists from different genres can be fairly compared
        while still accounting for genre-specific engagement patterns.
        """
        data = data.copy()
        
        # Calculate genre-specific percentiles
        genre_percentiles = data.groupby('genre')['genre_adjusted_score'].rank(pct=True)
        
        # Blend genre-specific ranking with raw score
        data['cross_genre_normalized_score'] = (
            0.7 * genre_percentiles +  # 70% based on genre ranking
            0.3 * data['genre_adjusted_score']  # 30% based on raw adjusted score
        )
        
        return data