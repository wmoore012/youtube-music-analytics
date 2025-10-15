#!/usr/bin/env python3
"""
Enhanced Sentiment Analysis Integration

Integrates the enhanced sentiment system with existing infrastructure,
leveraging database helpers, statistical utilities, and configuration management.
"""

import logging
import sys
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text

# Add paths for imports
sys.path.insert(0, ".")
sys.path.insert(0, "src")

# Use existing infrastructure
try:
    from web.etl_helpers import get_engine
except ImportError:
    # Fallback for testing
    def get_engine():
        from sqlalchemy import create_engine

        return create_engine("sqlite:///:memory:")


from youtubeviz.proprietary_sentiment_formula import ProprietarySentimentEnhancer, parse_proprietary_formula
from youtubeviz.sentiment_config import get_sentiment_config
from youtubeviz.sentiment_evaluation import SentimentEvaluationFramework
from youtubeviz.statistical_utils import calculate_wilson_intervals
from youtubeviz.vader_variants import VADERVariantManager, VariantType

logger = logging.getLogger(__name__)


class EnhancedSentimentPipeline:
    """
    Production-ready sentiment analysis pipeline that integrates with existing infrastructure.

    Uses existing database helpers, statistical utilities, and configuration management
    while adding enhanced VADER capabilities.
    """

    def __init__(self):
        self.config = get_sentiment_config()
        self.variant_manager = VADERVariantManager()
        self.evaluation_framework = SentimentEvaluationFramework()
        self._engine = None

        # Initialize proprietary enhancer if enabled
        self.proprietary_enhancer = None
        if self.config.proprietary_lexicon_enabled and self.config.secret_enhancement_formula:
            self.proprietary_enhancer = ProprietarySentimentEnhancer()
            self.formula_config = parse_proprietary_formula(self.config.secret_enhancement_formula)

    @property
    def engine(self):
        """Lazy database connection using existing helper."""
        if self._engine is None:
            self._engine = get_engine()
        return self._engine

    def get_enhanced_analyzer(self):
        """Get the configured enhanced VADER analyzer."""
        variant_type = VariantType(self.config.variant_type)
        return self.variant_manager.create_variant(variant_type)

    def fetch_evaluation_comments(
        self, limit: int = 1000, min_engagement: int = 5, artist_filter: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch UNIQUE comments for evaluation-ensures no data leakage.

        Args:
            limit: Maximum number of comments to fetch
            min_engagement: Minimum like count for comment inclusion
            artist_filter: Optional list of artist names to filter by

        Returns:
            DataFrame with comment_id, comment_text, video_id, channel_title, like_count
        """

        try:
            from youtubeviz.unique_comment_manager import get_unique_comments_for_evaluation

            comments_data = get_unique_comments_for_evaluation("enhanced_sentiment_integration", limit)

            if not comments_data:
                raise ValueError("No unique comments available for evaluation")

            # Convert to DataFrame and apply filters
            df_data = []
            for comment_data in comments_data:
                like_count = comment_data.get("like_count", 0)
                channel_title = comment_data.get("channel_title", "")

                # Apply engagement filter
                if like_count < min_engagement:
                    continue

                # Apply artist filter if specified
                if artist_filter and channel_title not in artist_filter:
                    continue

                # Determine engagement level
                if like_count >= 50:
                    engagement_level = "high_engagement"
                elif like_count >= 10:
                    engagement_level = "medium_engagement"
                else:
                    engagement_level = "low_engagement"

                df_data.append(
                    {
                        "comment_id": f"unique_{hash(comment_data['comment_text']) % 1000000}",
                        "comment_text": comment_data["comment_text"],
                        "video_id": comment_data.get("video_id", "unknown"),
                        "like_count": like_count,
                        "published_at": comment_data.get("published_at"),
                        "channel_title": channel_title,
                        "video_title": "unknown",
                        "engagement_level": engagement_level,
                    }
                )

            df = pd.DataFrame(df_data)

            if df.empty:
                print("⚠️  No comments match the specified filters. This is normal if:")
                print("   - All suitable comments are already allocated to other systems")
                print("   - Filters are too restrictive")
                print("   - Database has limited comment data")
                # Return empty DataFrame instead of raising error
                return pd.DataFrame(
                    columns=[
                        "comment_id",
                        "comment_text",
                        "video_id",
                        "like_count",
                        "published_at",
                        "channel_title",
                        "video_title",
                        "engagement_level",
                    ]
                )

            print(f"✅ Allocated {len(df)} UNIQUE comments for evaluation")
            return df

        except Exception as e:
            print(f"❌ Error fetching unique comments for evaluation: {e}")
            raise ValueError(f"Could not fetch unique comments: {e}")

    def evaluate_against_current_system(self, sample_size: int = 500) -> Dict[str, float]:
        """
        Evaluate enhanced system against current production system.

        Uses existing advanced_music_sentiment as baseline and compares
        with enhanced VADER variants.
        """

        # Fetch evaluation data
        comments_df = self.fetch_evaluation_comments(limit=sample_size)

        if comments_df.empty:
            raise ValueError("No comments available for evaluation")

        logger.info(f"Evaluating on {len(comments_df)} comments")

        # Get current system scores
        try:
            from youtubeviz.advanced_music_sentiment import AdvancedMusicSentimentAnalyzer

            current_analyzer = AdvancedMusicSentimentAnalyzer()

            current_scores = []
            for text_item in comments_df["comment_text"]:
                try:
                    score = current_analyzer.analyze_sentiment(text)
                    current_scores.append(score.get("compound", 0.0))
                except Exception as e:
                    logger.warning(f"Current analyzer failed on text: {e}")
                    current_scores.append(0.0)

            comments_df["current_sentiment"] = current_scores

        except ImportError:
            logger.warning("Current advanced sentiment analyzer not available")
            comments_df["current_sentiment"] = 0.0

        # Get enhanced system scores
        enhanced_analyzer = self.get_enhanced_analyzer()

        enhanced_scores = []
        for text_item in comments_df["comment_text"]:
            try:
                scores = enhanced_analyzer.polarity_scores(text)
                base_score = scores["compound"]

                # Apply proprietary enhancement if available
                if self.proprietary_enhancer:
                    try:
                        # Get TextBlob score for fusion
                        from textblob import TextBlob

                        textblob_score = TextBlob(text).sentiment.polarity

                        enhanced_score, _ = self.proprietary_enhancer.enhance_sentiment_score(
                            base_score, textblob_score, text
                        )
                        enhanced_scores.append(enhanced_score)
                    except Exception as e:
                        logger.warning(f"Proprietary enhancement failed: {e}")
                        enhanced_scores.append(base_score)
                else:
                    enhanced_scores.append(base_score)

            except Exception as e:
                logger.warning(f"Enhanced analyzer failed on text: {e}")
                enhanced_scores.append(0.0)

        comments_df["enhanced_sentiment"] = enhanced_scores

        # Calculate comparison metrics
        correlation = comments_df["current_sentiment"].corr(comments_df["enhanced_sentiment"])

        # Calculate agreement on sentiment direction
        current_direction = comments_df["current_sentiment"].apply(
            lambda x: "positive" if x > 0.05 else "negative" if x < -0.05 else "neutral"
        )
        enhanced_direction = comments_df["enhanced_sentiment"].apply(
            lambda x: "positive" if x > 0.05 else "negative" if x < -0.05 else "neutral"
        )

        agreement = (current_direction == enhanced_direction).mean()

        # Calculate variance in scores (higher variance might indicate better sensitivity)
        current_variance = comments_df["current_sentiment"].var()
        enhanced_variance = comments_df["enhanced_sentiment"].var()

        return {
            "sample_size": len(comments_df),
            "correlation": correlation,
            "directional_agreement": agreement,
            "current_score_variance": current_variance,
            "enhanced_score_variance": enhanced_variance,
            "variance_improvement": (
                (enhanced_variance - current_variance) / current_variance if current_variance > 0 else 0
            ),
        }

    def run_comprehensive_evaluation(self) -> Dict[str, any]:
        """
        Run comprehensive evaluation using existing statistical utilities.

        Returns detailed evaluation results with confidence intervals.
        """

        # Fetch evaluation data stratified by engagement
        high_engagement = self.fetch_evaluation_comments(limit=200, min_engagement=50)
        medium_engagement = self.fetch_evaluation_comments(limit=200, min_engagement=10)
        low_engagement = self.fetch_evaluation_comments(limit=200, min_engagement=1)

        results = {
            "config": {
                "variant_type": self.config.variant_type,
                "confidence_threshold": self.config.confidence_threshold,
                "privacy_features": self.config.get_privacy_summary(),
            },
            "data_summary": {
                "high_engagement_comments": len(high_engagement),
                "medium_engagement_comments": len(medium_engagement),
                "low_engagement_comments": len(low_engagement),
                "total_comments": len(high_engagement) + len(medium_engagement) + len(low_engagement),
            },
            "evaluations": {},
        }

        # Evaluate each engagement tier
        for tier_name, tier_data in [
            ("high_engagement", high_engagement),
            ("medium_engagement", medium_engagement),
            ("low_engagement", low_engagement),
        ]:
            if not tier_data.empty:
                tier_results = self.evaluate_against_current_system(len(tier_data))

                # Add confidence intervals using existing statistical utilities
                if tier_results["sample_size"] > 30:
                    # Calculate Wilson confidence intervals for agreement rate
                    successes = int(tier_results["directional_agreement"] * tier_results["sample_size"])
                    n = tier_results["sample_size"]

                    try:
                        lower, upper = calculate_wilson_intervals(successes, n, confidence=0.95)
                        tier_results["agreement_confidence_interval"] = (lower, upper)
                    except Exception as e:
                        logger.warning(f"Could not calculate confidence interval: {e}")
                        tier_results["agreement_confidence_interval"] = None

                results["evaluations"][tier_name] = tier_results

        return results

    def generate_deployment_recommendation(self) -> Dict[str, any]:
        """
        Generate deployment recommendation based on evaluation results.

        Returns recommendation with rationale and risk assessment.
        """

        evaluation = self.run_comprehensive_evaluation()

        # Analyze results
        total_comments = evaluation["data_summary"]["total_comments"]

        if total_comments < 100:
            return {
                "recommendation": "insufficient_data",
                "rationale": f"Only {total_comments} comments available for evaluation. Need at least 100.",
                "risk_level": "high",
                "next_steps": ["Collect more evaluation data", "Run evaluation on larger dataset"],
            }

        # Check agreement across tiers
        agreements = []
        correlations = []

        for tier_name, tier_results in evaluation["evaluations"].items():
            if tier_results["sample_size"] > 0:
                agreements.append(tier_results["directional_agreement"])
                correlations.append(tier_results["correlation"])

        if not agreements:
            return {
                "recommendation": "evaluation_failed",
                "rationale": "No successful evaluations completed",
                "risk_level": "high",
                "next_steps": ["Debug evaluation pipeline", "Check data availability"],
            }

        avg_agreement = sum(agreements) / len(agreements)
        avg_correlation = sum(correlations) / len(correlations)

        # Make recommendation
        if avg_agreement >= 0.85 and avg_correlation >= 0.7:
            recommendation = "deploy"
            risk_level = "low"
            rationale = (
                f"High agreement ({avg_agreement:.2f}) and correlation ({avg_correlation:.2f}) with current system"
            )
        elif avg_agreement >= 0.75 and avg_correlation >= 0.5:
            recommendation = "deploy_with_monitoring"
            risk_level = "medium"
            rationale = f"Good agreement ({avg_agreement:.2f}) and correlation ({
                avg_correlation:.2f}), deploy with close monitoring"
        else:
            recommendation = "do_not_deploy"
            risk_level = "high"
            rationale = (
                f"Low agreement ({avg_agreement:.2f}) or correlation ({avg_correlation:.2f}) with current system"
            )

        return {
            "recommendation": recommendation,
            "rationale": rationale,
            "risk_level": risk_level,
            "metrics": {
                "average_agreement": avg_agreement,
                "average_correlation": avg_correlation,
                "evaluation_sample_size": total_comments,
            },
            "evaluation_details": evaluation,
        }


def quick_evaluation() -> None:
    """Quick evaluation for testing the integration."""

    print("🧪 Enhanced Sentiment Analysis Integration Test")
    print("=" * 60)

    try:
        pipeline = EnhancedSentimentPipeline()

        print(f"✅ Pipeline initialized")
        print(f"   Variant: {pipeline.config.variant_type}")
        print(f"   Privacy features: {pipeline.config.get_privacy_summary()}")

        # Test comment fetching
        comments = pipeline.fetch_evaluation_comments(limit=10)
        print(f"\n📊 Sample data: {len(comments)} comments fetched")

        if not comments.empty:
            print(f"   Engagement levels: {comments['engagement_level'].value_counts().to_dict()}")

            # Test evaluation
            if len(comments) >= 5:
                results = pipeline.evaluate_against_current_system(sample_size=len(comments))
                print(f"\n🔍 Evaluation results:")
                print(f"   Sample size: {results['sample_size']}")
                print(f"   Correlation: {results['correlation']:.3f}")
                print(f"   Agreement: {results['directional_agreement']:.3f}")

        print(f"\n✅ Integration test completed successfully")

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    quick_evaluation()
