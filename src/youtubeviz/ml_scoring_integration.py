#!/usr / bin / env python3
"""
ML Scoring Integration

Integrates ML-ready data collection with existing scoring plugins
for seamless data pipeline integration.
"""

import sys
from typing import Any, Dict, List, Optional

import pandas as pd

sys.path.insert(0, "src")

try:
    from data_organization.plugin_manager import PluginManager
    from data_organization.scoring_plugin import ScoringPlugin, ScoringResult, ValidationResult
    from youtubeviz.ml_data_models import CommentMetadata, MLComment, MLDataset, MusicDomain
    from youtubeviz.unique_comment_manager import UniqueCommentManager

    SCORING_AVAILABLE = True
except ImportError:
    SCORING_AVAILABLE = False


class MLDataScoringPlugin(ScoringPlugin):
    """Scoring plugin that provides ML-ready data collection capabilities."""

    def __init__(self):
        super().__init__()
        self.comment_manager = UniqueCommentManager() if SCORING_AVAILABLE else None

    def get_name(self) -> str:
        return "ml_data_collector"

    def get_version(self) -> str:
        return "1.0.0"

    def get_parameters(self) -> Dict[str, Any]:
        return {
            "music_domain_filter": True,
            "min_engagement": 2,
            "max_comments": 1000,
            "include_metadata": True,
            "export_format": "jsonl",
        }

    def get_input_requirements(self) -> List[str]:
        return ["comment_text", "video_id", "channel_title"]

    def get_output_schema(self) -> Dict[str, Any]:
        return {
            "comment_id": "string",
            "ml_readiness_score": "float",
            "music_domain": "string",
            "contains_slang": "boolean",
            "data_quality_score": "float",
        }

    def validate_input(self, data: pd.DataFrame) -> ValidationResult:
        """Validate input data for ML processing."""
        errors = []
        warnings = []

        required_cols = ["comment_text", "video_id"]
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        if len(data) == 0:
            errors.append("Input data is empty")

        # Check for empty comments
        if "comment_text" in data.columns:
            empty_comments = data["comment_text"].isnull().sum()
            if empty_comments > 0:
                warnings.append(f"Found {empty_comments} empty comments")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            checked_items=len(data),
            passed_items=len(data) - len(errors),
        )

    def calculate_scores(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate ML readiness scores for comments."""
        if not SCORING_AVAILABLE or not self.comment_manager:
            raise RuntimeError("ML scoring components not available")

        results = []

        for _, row in data.iterrows():
            comment_text = row.get("comment_text", "")
            video_id = row.get("video_id", "")
            channel_title = row.get("channel_title", "")

            # Calculate ML readiness score
            ml_score = self._calculate_ml_readiness(comment_text)

            # Classify music domain
            music_domain = self._classify_music_domain(comment_text, channel_title, row.get("video_title", ""))

            # Check for music slang
            contains_slang = self._contains_music_slang(comment_text)

            # Calculate data quality score
            quality_score = self._calculate_data_quality(comment_text, row)

            # Create unique hash for comment
            import hashlib

            comment_hash = hashlib.sha256(comment_text.encode("utf-8")).hexdigest()[:16]

            results.append(
                {
                    "entity_id": comment_hash,
                    "comment_id": comment_hash,
                    "score_value": ml_score,
                    "ml_readiness_score": ml_score,
                    "music_domain": music_domain,
                    "contains_slang": contains_slang,
                    "data_quality_score": quality_score,
                    "video_id": video_id,
                    "channel_title": channel_title,
                }
            )

        return pd.DataFrame(results)

    def _calculate_ml_readiness(self, comment_text: str) -> float:
        """Calculate how ready a comment is for ML training."""
        score = 0.0

        # Length check (10-500 characters is ideal)
        length = len(comment_text)
        if 10 <= length <= 500:
            score += 0.3
        elif length < 10:
            score += 0.1  # Too short
        else:
            score += 0.2  # Too long but usable

        # Contains meaningful content (not just emoji / punctuation)
        import re

        word_count = len(re.findall(r"\b\w+\b", comment_text))
        if word_count >= 3:
            score += 0.3
        elif word_count >= 1:
            score += 0.2

        # Music relevance
        if self._contains_music_terms(comment_text):
            score += 0.2

        # Language quality (basic check)
        if not self._is_likely_spam(comment_text):
            score += 0.2

        return min(score, 1.0)

    def _classify_music_domain(self, comment_text: str, channel_title: str, video_title: str) -> str:
        """Classify the music domain of content."""
        text_lower = comment_text.lower()
        channel_lower = (channel_title or "").lower()
        video_lower = (video_title or "").lower()

        # Check for live performance
        if any(term in text_lower or term in video_lower for term in ["live", "concert", "performance", "tour"]):
            return "live_performance"

        # Check for music video
        if any(term in video_lower for term in ["official", "music video", "mv"]):
            return "music_video"

        # Check for artist content
        if any(term in channel_lower for term in ["official", "records", "music"]):
            return "artist_content"

        # Check for music discussion
        if any(term in text_lower for term in ["song", "album", "track", "artist"]):
            return "music_discussion"

        return "general"

    def _contains_music_slang(self, comment_text: str) -> bool:
        """Check if comment contains music slang."""
        text_lower = comment_text.lower()
        slang_terms = [
            "slaps",
            "banger",
            "fire",
            "goated",
            "hits different",
            "goes hard",
            "periodt",
            "no cap",
            "mid",
            "trash",
        ]
        return any(term in text_lower for term in slang_terms)

    def _contains_music_terms(self, comment_text: str) -> bool:
        """Check if comment contains music-related terms."""
        text_lower = comment_text.lower()
        music_terms = [
            "song",
            "track",
            "album",
            "music",
            "beat",
            "vocals",
            "lyrics",
            "artist",
            "singer",
            "rapper",
            "producer",
        ]
        return any(term in text_lower for term in music_terms)

    def _calculate_data_quality(self, comment_text: str, row: pd.Series) -> float:
        """Calculate overall data quality score."""
        score = 0.0

        # Text quality
        if comment_text and len(comment_text.strip()) > 0:
            score += 0.3

        # Has engagement (likes)
        like_count = row.get("like_count", 0)
        if like_count > 0:
            score += 0.2

        # Not spam
        if not self._is_likely_spam(comment_text):
            score += 0.3

        # Has metadata
        if row.get("video_id") and row.get("channel_title"):
            score += 0.2

        return min(score, 1.0)

    def _is_likely_spam(self, comment_text: str) -> bool:
        """Simple spam detection."""
        text_lower = comment_text.lower()
        spam_indicators = ["subscribe", "like and subscribe", "check out my", "follow me", "click here", "link in bio"]
        return any(indicator in text_lower for indicator in spam_indicators)


class MLDataPipelineIntegration:
    """Integration layer between ML data collection and scoring plugins."""

    def __init__(self):
        self.comment_manager = UniqueCommentManager() if SCORING_AVAILABLE else None
        self.plugin_manager = PluginManager() if SCORING_AVAILABLE else None

        if self.plugin_manager:
            # Register ML data scoring plugin
            ml_plugin = MLDataScoringPlugin()
            # Try different registration method names
            if hasattr(self.plugin_manager, "register_plugin"):
                self.plugin_manager.register_plugin(ml_plugin)
            elif hasattr(self.plugin_manager, "add_plugin"):
                self.plugin_manager.add_plugin(ml_plugin)
            elif hasattr(self.plugin_manager, "register"):
                self.plugin_manager.register(ml_plugin)

    def collect_ml_training_data_with_scoring(
        self, count: int = 1000, music_domain_filter: bool = True, min_quality_score: float = 0.7
    ) -> Optional[pd.DataFrame]:
        """
        Collect ML training data using scoring plugins for quality assessment.

        Args:
            count: Number of comments to collect
            music_domain_filter: Whether to filter for music domain
            min_quality_score: Minimum quality score threshold

        Returns:
            DataFrame with scored ML-ready comments or None if failed
        """
        if not SCORING_AVAILABLE or not self.comment_manager:
            print("❌ Scoring integration not available")
            return None

        try:
            # Get raw comments
            print(f"📊 Collecting {count} comments for ML training...")
            raw_comments = self.comment_manager.get_ml_ready_comments(
                system_name="ml_pipeline_integration",
                usage_type="training",
                count=count * 2,  # Get extra to filter
                music_domain_filter=music_domain_filter,
            )

            if not raw_comments:
                print("❌ No comments collected")
                return None

            # Convert to DataFrame for scoring
            df = pd.DataFrame(raw_comments)

            # Apply ML data scoring plugin
            ml_plugin = MLDataScoringPlugin()
            scoring_result = ml_plugin.execute(df)

            # Filter by quality score
            scored_df = scoring_result.entity_scores
            high_quality = scored_df[scored_df["data_quality_score"] >= min_quality_score]

            # Limit to requested count
            final_df = high_quality.head(count)

            print(f"✅ Collected {len(final_df)} high-quality ML training comments")
            print(f"📊 Quality stats:")
            print(f"   Avg ML readiness: {final_df['ml_readiness_score'].mean():.3f}")
            print(f"   Avg data quality: {final_df['data_quality_score'].mean():.3f}")
            print(f"   Music domain distribution:")
            print(final_df["music_domain"].value_counts().to_dict())

            return final_df

        except Exception as e:
            print(f"❌ Error in ML data pipeline integration: {e}")
            return None

    def export_scored_ml_dataset(
        self,
        output_path: str,
        train_count: int = 1000,
        val_count: int = 200,
        test_count: int = 200,
        min_quality_score: float = 0.7,
    ) -> bool:
        """
        Export a complete scored ML dataset with train / val / test splits.

        Args:
            output_path: Path for output file
            train_count: Training samples
            val_count: Validation samples
            test_count: Test samples
            min_quality_score: Minimum quality threshold

        Returns:
            True if successful, False otherwise
        """
        try:
            all_data = []

            # Collect training data
            train_data = self.collect_ml_training_data_with_scoring(
                count=train_count, min_quality_score=min_quality_score
            )
            if train_data is not None:
                train_data["split"] = "train"
                all_data.append(train_data)

            # Collect validation data
            val_data = self.collect_ml_training_data_with_scoring(count=val_count, min_quality_score=min_quality_score)
            if val_data is not None:
                val_data["split"] = "validation"
                all_data.append(val_data)

            # Collect test data
            test_data = self.collect_ml_training_data_with_scoring(
                count=test_count, min_quality_score=min_quality_score
            )
            if test_data is not None:
                test_data["split"] = "test"
                all_data.append(test_data)

            if not all_data:
                print("❌ No data collected for any split")
                return False

            # Combine and export
            combined_df = pd.concat(all_data, ignore_index=True)

            if output_path.endswith(".jsonl"):
                import json

                with open(output_path, "w", encoding="utf-8") as f:
                    for _, row in combined_df.iterrows():
                        f.write(json.dumps(row.to_dict(), ensure_ascii=False) + "\n")
            else:
                combined_df.to_csv(output_path, index=False)

            print(f"✅ Exported scored ML dataset: {output_path}")
            print(f"📊 Total samples: {len(combined_df)}")
            print(f"   Train: {len(combined_df[combined_df['split'] == 'train'])}")
            print(f"   Validation: {len(combined_df[combined_df['split'] == 'validation'])}")
            print(f"   Test: {len(combined_df[combined_df['split'] == 'test'])}")

            return True

        except Exception as e:
            print(f"❌ Error exporting scored ML dataset: {e}")
            return False


# Convenience functions for easy integration
def get_ml_pipeline_integration() -> Optional[MLDataPipelineIntegration]:
    """Get ML pipeline integration instance."""
    if not SCORING_AVAILABLE:
        print("❌ Scoring integration not available. Install required dependencies.")
        return None
    return MLDataPipelineIntegration()


def quick_ml_data_collection(count: int = 500) -> Optional[pd.DataFrame]:
    """Quick collection of ML-ready data with scoring."""
    integration = get_ml_pipeline_integration()
    if integration:
        return integration.collect_ml_training_data_with_scoring(count=count)
    return None


if __name__ == "__main__":
    # Demo the ML scoring integration
    print("🤖 ML SCORING INTEGRATION DEMO")
    print("=" * 50)

    integration = get_ml_pipeline_integration()
    if integration:
        # Collect sample data
        sample_data = integration.collect_ml_training_data_with_scoring(count=50)

        if sample_data is not None:
            print(f"\n📊 Sample data collected: {len(sample_data)} comments")
            print("\nTop 5 highest quality comments:")
            top_quality = sample_data.nlargest(5, "data_quality_score")
            for _, row in top_quality.iterrows():
                print(f"  Score: {row['data_quality_score']:.3f} | {row['comment_text'][:60]}...")
        else:
            print("❌ Failed to collect sample data")
    else:
        print("❌ Integration not available")
