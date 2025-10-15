#!/usr/bin/env python3
"""
Enhanced Sentiment Analysis Configuration Management

Professional configuration system for sentiment analysis parameters with
privacy controls and environment-based secret management.
"""

from dataclasses import dataclass, field
import os
from typing import Dict, Optional

from data_organization.configuration_manager import ValidationResult


@dataclass
class SentimentEnhancementConfig:
    """Configuration for sentiment analysis enhancements with privacy controls."""

    # Public configuration (can be in git)
    variant_type: str = "comprehensive"  # minimal, moderate, comprehensive, aggressive, hybrid
    confidence_threshold: float = 0.5
    enable_music_domain_lexicon: bool = True
    enable_multi_word_idioms: bool = True
    enable_emoji_processing: bool = True

    # Private configuration (from .env only)
    proprietary_lexicon_enabled: bool = field(default=False)
    custom_booster_weights: Optional[Dict[str, float]] = field(default=None)
    secret_enhancement_formula: Optional[str] = field(default=None)

    # Evaluation settings
    cross_validation_folds: int = 5
    bootstrap_iterations: int = 1000
    statistical_significance_alpha: float = 0.05

    @classmethod
    def from_environment(cls) -> "SentimentEnhancementConfig":
        """Load configuration from environment variables with privacy controls."""

        config = cls()

        # Public settings (can have defaults)
        config.variant_type = os.getenv("SENTIMENT_VARIANT_TYPE", "comprehensive")
        config.confidence_threshold = float(os.getenv("SENTIMENT_CONFIDENCE_THRESHOLD", "0.5"))
        config.enable_music_domain_lexicon = os.getenv("SENTIMENT_ENABLE_MUSIC_LEXICON", "true").lower() == "true"
        config.enable_multi_word_idioms = os.getenv("SENTIMENT_ENABLE_IDIOMS", "true").lower() == "true"
        config.enable_emoji_processing = os.getenv("SENTIMENT_ENABLE_EMOJI", "true").lower() == "true"

        # Private settings (only if explicitly set in .env)
        if os.getenv("SENTIMENT_PROPRIETARY_ENABLED"):
            config.proprietary_lexicon_enabled = os.getenv("SENTIMENT_PROPRIETARY_ENABLED", "false").lower() == "true"

        if os.getenv("SENTIMENT_CUSTOM_BOOSTERS"):
            try:
                import json

                config.custom_booster_weights = json.loads(os.getenv("SENTIMENT_CUSTOM_BOOSTERS", "{}"))
            except (json.JSONDecodeError, TypeError):
                config.custom_booster_weights = None

        if os.getenv("SENTIMENT_SECRET_FORMULA"):
            config.secret_enhancement_formula = os.getenv("SENTIMENT_SECRET_FORMULA")

        # Evaluation settings
        config.cross_validation_folds = int(os.getenv("SENTIMENT_CV_FOLDS", "5"))
        config.bootstrap_iterations = int(os.getenv("SENTIMENT_BOOTSTRAP_ITERATIONS", "1000"))
        config.statistical_significance_alpha = float(os.getenv("SENTIMENT_ALPHA", "0.05"))

        return config

    def validate(self) -> ValidationResult:
        """Validate configuration parameters."""
        result = ValidationResult()

        # Validate variant type
        valid_variants = ["minimal", "moderate", "comprehensive", "aggressive", "hybrid"]
        if self.variant_type not in valid_variants:
            result.add_error(f"Invalid variant_type: {self.variant_type}. Must be one of {valid_variants}")

        # Validate confidence threshold
        if not (0.0 <= self.confidence_threshold <= 1.0):
            result.add_error(f"confidence_threshold must be between 0.0 and 1.0, got {self.confidence_threshold}")

        # Validate CV folds
        if self.cross_validation_folds < 2:
            result.add_error(f"cross_validation_folds must be >= 2, got {self.cross_validation_folds}")

        # Validate bootstrap iterations
        if self.bootstrap_iterations < 100:
            result.add_warning(f"bootstrap_iterations is low ({self.bootstrap_iterations}), recommend >= 1000")

        # Validate alpha
        if not (0.0 < self.statistical_significance_alpha < 1.0):
            result.add_error(
                f"statistical_significance_alpha must be between 0.0 and 1.0, got {self.statistical_significance_alpha}"
            )

        result.checked_items = 6
        result.passed_items = result.checked_items - len(result.errors)

        return result

    def get_privacy_summary(self) -> Dict[str, bool]:
        """Get summary of what private features are enabled."""
        return {
            "proprietary_lexicon": self.proprietary_lexicon_enabled,
            "custom_boosters": self.custom_booster_weights is not None,
            "secret_formula": self.secret_enhancement_formula is not None,
        }


def get_sentiment_config() -> SentimentEnhancementConfig:
    """Get validated sentiment configuration from environment."""
    config = SentimentEnhancementConfig.from_environment()

    validation = config.validate()
    if not validation.is_valid:
        raise ValueError(f"Invalid sentiment configuration: {validation.errors}")

    if validation.warnings:
        import warnings

        for warning in validation.warnings:
            warnings.warn(f"Sentiment config warning: {warning}")

    return config


# Example .env configuration for reference
ENV_EXAMPLE = """
# Enhanced Sentiment Analysis Configuration

# Public settings (safe to commit defaults)
SENTIMENT_VARIANT_TYPE=comprehensive
SENTIMENT_CONFIDENCE_THRESHOLD=0.5
SENTIMENT_ENABLE_MUSIC_LEXICON=true
SENTIMENT_ENABLE_IDIOMS=true
SENTIMENT_ENABLE_EMOJI=true

# Evaluation settings
SENTIMENT_CV_FOLDS=5
SENTIMENT_BOOTSTRAP_ITERATIONS=1000
SENTIMENT_ALPHA=0.05

# Private settings (only set if you have proprietary enhancements)
# SENTIMENT_PROPRIETARY_ENABLED=true
# SENTIMENT_CUSTOM_BOOSTERS={"no_cap": 0.35, "fr": 0.30, "deadass": 0.40}
# SENTIMENT_SECRET_FORMULA=CSA:1.34|DERW:1.28,0.76|MMSF:0.75,0.45|TSDM:1.25,1.15,1.20|SIGMOID:2.5,1.2
"""

if __name__ == "__main__":
    print("🔧 Sentiment Configuration Example")
    print("=" * 50)
    print(ENV_EXAMPLE)

    print("\n🧪 Testing Configuration Loading")
    print("=" * 50)

    try:
        config = get_sentiment_config()
        print(f"✅ Configuration loaded successfully")
        print(f"   Variant: {config.variant_type}")
        print(f"   Confidence threshold: {config.confidence_threshold}")
        print(f"   Privacy features: {config.get_privacy_summary()}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
