#!/usr/bin/env python3
"""
Comprehensive Sentiment Analysis Evaluation Framework

Provides rigorous statistical evaluation with multiple testing approaches including:
- Paired testing on identical comment sets
- GroupKFold cross-validation by video_id to prevent data leakage
- McNemar's test for paired classifier comparison
- Bootstrap confidence intervals for performance deltas
- Multiple comparison correction with Benjamini-Hochberg FDR control
"""

from __future__ import annotations

import json
import random
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import chi2_contingency
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import GroupKFold

# --------------------------- Data Models ---------------------------


@dataclass
class ClassMetrics:
    """Per-class evaluation metrics."""

    precision: float
    recall: float
    f1_score: float
    support: int


@dataclass
class McNemarResult:
    """Results from McNemar's test for paired classifier comparison."""

    statistic: float
    p_value: float
    significant: bool
    alpha: float = 0.05

    def __post_init__(self):
        self.significant = self.p_value < self.alpha


@dataclass
class SliceMetrics:
    """Metrics for a specific data slice."""

    slice_name: str
    sample_size: int
    accuracy: float
    f1_score: float
    precision: float
    recall: float
    confidence_interval: Optional[Tuple[float, float]] = None


@dataclass
class ExperimentConfig:
    """Configuration for experiment reproducibility."""

    experiment_id: str
    timestamp: datetime
    random_seed: int
    data_fingerprint: str
    model_configs: Dict[str, Any]
    cv_folds: int = 5
    confidence_level: float = 0.95

    def to_dict(self) -> Dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "timestamp": self.timestamp.isoformat(),
            "random_seed": self.random_seed,
            "data_fingerprint": self.data_fingerprint,
            "model_configs": self.model_configs,
            "cv_folds": self.cv_folds,
            "confidence_level": self.confidence_level,
        }


@dataclass
class EvaluationResults:
    """Comprehensive evaluation results."""

    # Model identification
    model_name: str
    variant_config: Dict[str, Any]
    evaluation_timestamp: datetime

    # Overall metrics
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    macro_f1: float

    # Per-class metrics
    class_metrics: Dict[str, ClassMetrics]

    # Confidence intervals
    confidence_intervals: Dict[str, Tuple[float, float]]

    # Statistical tests
    mcnemar_results: Optional[McNemarResult] = None

    # Slice analysis
    slice_results: Dict[str, SliceMetrics] = field(default_factory=dict)

    # Experiment metadata
    experiment_config: Optional[ExperimentConfig] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "model_name": self.model_name,
            "variant_config": self.variant_config,
            "evaluation_timestamp": self.evaluation_timestamp.isoformat(),
            "accuracy": self.accuracy,
            "precision": self.precision,
            "recall": self.recall,
            "f1_score": self.f1_score,
            "macro_f1": self.macro_f1,
            "class_metrics": {
                k: {"precision": v.precision, "recall": v.recall, "f1_score": v.f1_score, "support": v.support}
                for k, v in self.class_metrics.items()
            },
            "confidence_intervals": self.confidence_intervals,
            "slice_results": {
                k: {
                    "slice_name": v.slice_name,
                    "sample_size": v.sample_size,
                    "accuracy": v.accuracy,
                    "f1_score": v.f1_score,
                    "precision": v.precision,
                    "recall": v.recall,
                    "confidence_interval": v.confidence_interval,
                }
                for k, v in self.slice_results.items()
            },
        }

        if self.mcnemar_results:
            result["mcnemar_results"] = {
                "statistic": self.mcnemar_results.statistic,
                "p_value": self.mcnemar_results.p_value,
                "significant": self.mcnemar_results.significant,
                "alpha": self.mcnemar_results.alpha,
            }

        if self.experiment_config:
            result["experiment_config"] = self.experiment_config.to_dict()

        return result


@dataclass
class CVResults:
    """Cross-validation evaluation results."""

    model_name: str
    cv_scores: List[float]
    mean_score: float
    std_score: float
    confidence_interval: Tuple[float, float]
    fold_results: List[EvaluationResults]


# --------------------------- Exception Classes ---------------------------


class EvaluationError(Exception):
    """Base class for evaluation errors."""

    pass


class InsufficientDataError(EvaluationError):
    """Raised when insufficient data for evaluation."""

    pass


class ModelComparisonError(EvaluationError):
    """Raised when model comparison fails."""

    pass


class StatisticalTestError(EvaluationError):
    """Raised when statistical tests fail."""

    pass


# --------------------------- Main Evaluation Framework ---------------------------


class SentimentEvaluationFramework:
    """
    Multi-model evaluation framework with statistical rigor.

    Supports:
    - Paired testing on identical comment sets
    - GroupKFold cross-validation by video_id
    - McNemar's test for paired classifier comparison
    - Bootstrap confidence intervals
    - Multiple comparison correction
    """

    def __init__(self, random_seed: int = 42, confidence_level: float = 0.95):
        self.random_seed = random_seed
        self.confidence_level = confidence_level
        self._set_random_seeds()

    def _set_random_seeds(self) -> None:
        """Set random seeds for reproducibility."""
        random.seed(self.random_seed)
        np.random.seed(self.random_seed)

    def run_paired_evaluation(
        self, models: Dict[str, Any], comments: List[str], true_labels: List[str], experiment_id: Optional[str] = None
    ) -> Dict[str, EvaluationResults]:
        """
        Run paired evaluation on identical comment sets.

        Args:
            models: Dictionary of model_name -> model_instance
            comments: List of comment texts
            true_labels: List of true sentiment labels
            experiment_id: Optional experiment identifier

        Returns:
            Dictionary of model_name -> EvaluationResults
        """
        if len(comments) != len(true_labels):
            raise InsufficientDataError("Comments and labels must have same length")

        if len(comments) < 10:
            raise InsufficientDataError("Need at least 10 samples for evaluation")

        # Create experiment config
        experiment_config = ExperimentConfig(
            experiment_id=experiment_id or f"paired_eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            timestamp=datetime.now(),
            random_seed=self.random_seed,
            data_fingerprint=self._compute_data_fingerprint(comments, true_labels),
            model_configs={name: self._get_model_config(model) for name, model in models.items()},
        )

        results = {}
        predictions = {}

        # Get predictions from all models
        for model_name, model in models.items():
            try:
                model_predictions = self._get_model_predictions(model, comments)
                predictions[model_name] = model_predictions

                # Compute metrics
                results[model_name] = self._compute_evaluation_metrics(
                    model_name=model_name,
                    true_labels=true_labels,
                    predictions=model_predictions,
                    experiment_config=experiment_config,
                )

            except Exception as e:
                raise ModelComparisonError(f"Failed to evaluate model {model_name}: {e}")

        # Add pairwise McNemar tests
        model_names = list(models.keys())
        for i, model_a in enumerate(model_names):
            for model_b in model_names[i + 1 :]:
                try:
                    mcnemar_result = self.compute_mcnemar_test(
                        true_labels=true_labels, predictions_a=predictions[model_a], predictions_b=predictions[model_b]
                    )

                    # Add to both models' results
                    if not hasattr(results[model_a], "pairwise_comparisons"):
                        results[model_a].pairwise_comparisons = {}
                    if not hasattr(results[model_b], "pairwise_comparisons"):
                        results[model_b].pairwise_comparisons = {}

                    results[model_a].pairwise_comparisons = getattr(results[model_a], "pairwise_comparisons", {})
                    results[model_b].pairwise_comparisons = getattr(results[model_b], "pairwise_comparisons", {})

                    results[model_a].pairwise_comparisons[model_b] = mcnemar_result
                    results[model_b].pairwise_comparisons[model_a] = mcnemar_result

                except Exception as e:
                    warnings.warn(f"McNemar test failed for {model_a} vs {model_b}: {e}")

        return results

    def run_grouped_cv_evaluation(
        self,
        models: Dict[str, Any],
        data: pd.DataFrame,
        text_col: str = "comment_text",
        label_col: str = "sentiment",
        group_col: str = "video_id",
        cv_folds: int = 5,
        experiment_id: Optional[str] = None,
    ) -> Dict[str, CVResults]:
        """
        Run GroupKFold cross-validation by video_id to prevent data leakage.

        Args:
            models: Dictionary of model_name -> model_instance
            data: DataFrame with comments, labels, and video_ids
            text_col: Column name for comment text
            label_col: Column name for sentiment labels
            group_col: Column name for grouping (video_id)
            cv_folds: Number of CV folds
            experiment_id: Optional experiment identifier

        Returns:
            Dictionary of model_name -> CVResults
        """
        if len(data) < cv_folds * 10:
            raise InsufficientDataError(f"Need at least {cv_folds * 10} samples for {cv_folds}-fold CV")

        required_cols = [text_col, label_col, group_col]
        missing_cols = [col for col in required_cols if col not in data.columns]
        if missing_cols:
            raise InsufficientDataError(f"Missing required columns: {missing_cols}")

        # Create experiment config
        experiment_config = ExperimentConfig(
            experiment_id=experiment_id or f"grouped_cv_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            timestamp=datetime.now(),
            random_seed=self.random_seed,
            data_fingerprint=self._compute_data_fingerprint(data[text_col].tolist(), data[label_col].tolist()),
            model_configs={name: self._get_model_config(model) for name, model in models.items()},
            cv_folds=cv_folds,
        )

        # Set up GroupKFold
        gkf = GroupKFold(n_splits=cv_folds)
        X = data[text_col].values
        y = data[label_col].values
        groups = data[group_col].values

        results = {}

        for model_name, model in models.items():
            fold_scores = []
            fold_results = []

            for fold_idx, (train_idx, test_idx) in enumerate(gkf.split(X, y, groups)):
                X_test = X[test_idx]
                y_test = y[test_idx]

                try:
                    # Get predictions for test fold
                    predictions = self._get_model_predictions(model, X_test.tolist())

                    # Compute fold metrics
                    fold_result = self._compute_evaluation_metrics(
                        model_name=f"{model_name}_fold_{fold_idx}",
                        true_labels=y_test.tolist(),
                        predictions=predictions,
                        experiment_config=experiment_config,
                    )

                    fold_results.append(fold_result)
                    fold_scores.append(fold_result.f1_score)

                except Exception as e:
                    warnings.warn(f"Fold {fold_idx} failed for model {model_name}: {e}")
                    continue

            if not fold_scores:
                raise ModelComparisonError(f"All folds failed for model {model_name}")

            # Compute CV statistics
            mean_score = np.mean(fold_scores)
            std_score = np.std(fold_scores)

            # Bootstrap confidence interval for mean
            ci = self.bootstrap_confidence_intervals(fold_scores, self.confidence_level)

            results[model_name] = CVResults(
                model_name=model_name,
                cv_scores=fold_scores,
                mean_score=mean_score,
                std_score=std_score,
                confidence_interval=ci,
                fold_results=fold_results,
            )

        return results

    def compute_mcnemar_test(
        self, true_labels: List[str], predictions_a: List[str], predictions_b: List[str], alpha: float = 0.05
    ) -> McNemarResult:
        """
        Compute McNemar's test for paired classifier comparison.

        Args:
            true_labels: True sentiment labels
            predictions_a: Predictions from model A
            predictions_b: Predictions from model B
            alpha: Significance level

        Returns:
            McNemarResult with test statistics
        """
        if len(true_labels) != len(predictions_a) or len(true_labels) != len(predictions_b):
            raise StatisticalTestError("All input lists must have same length")

        # Create contingency table for McNemar's test
        # Rows: Model A correct/incorrect, Cols: Model B correct/incorrect
        correct_a = [pred == true for pred, true in zip(predictions_a, true_labels)]
        correct_b = [pred == true for pred, true in zip(predictions_b, true_labels)]

        # McNemar contingency table
        both_correct = sum(a and b for a, b in zip(correct_a, correct_b))
        a_correct_b_wrong = sum(a and not b for a, b in zip(correct_a, correct_b))
        a_wrong_b_correct = sum(not a and b for a, b in zip(correct_a, correct_b))
        both_wrong = sum(not a and not b for a, b in zip(correct_a, correct_b))

        # McNemar's test focuses on discordant pairs
        discordant_pairs = a_correct_b_wrong + a_wrong_b_correct

        if discordant_pairs == 0:
            # No difference between models
            return McNemarResult(statistic=0.0, p_value=1.0, significant=False, alpha=alpha)

        # McNemar's test statistic with continuity correction
        statistic = ((abs(a_correct_b_wrong - a_wrong_b_correct) - 1) ** 2) / discordant_pairs

        # Chi-square test with 1 degree of freedom
        p_value = 1 - stats.chi2.cdf(statistic, df=1)

        return McNemarResult(statistic=statistic, p_value=p_value, significant=p_value < alpha, alpha=alpha)

    def bootstrap_confidence_intervals(
        self, metric_values: List[float], confidence: float = 0.95, n_bootstrap: int = 1000
    ) -> Tuple[float, float]:
        """
        Compute bootstrap confidence intervals for performance metrics.

        Args:
            metric_values: List of metric values (e.g., F1 scores from CV folds)
            confidence: Confidence level (default 0.95 for 95% CI)
            n_bootstrap: Number of bootstrap samples

        Returns:
            Tuple of (lower_bound, upper_bound)
        """
        if len(metric_values) < 2:
            raise StatisticalTestError("Need at least 2 values for bootstrap CI")

        # Bootstrap resampling
        bootstrap_means = []
        for _ in range(n_bootstrap):
            bootstrap_sample = np.random.choice(metric_values, size=len(metric_values), replace=True)
            bootstrap_means.append(np.mean(bootstrap_sample))

        # Compute percentiles for confidence interval
        alpha = 1 - confidence
        lower_percentile = (alpha / 2) * 100
        upper_percentile = (1 - alpha / 2) * 100

        lower_bound = np.percentile(bootstrap_means, lower_percentile)
        upper_bound = np.percentile(bootstrap_means, upper_percentile)

        return (lower_bound, upper_bound)

    def evaluate_data_slices(
        self,
        models: Dict[str, Any],
        data: pd.DataFrame,
        text_col: str = "comment_text",
        label_col: str = "sentiment",
        slice_definitions: Optional[Dict[str, callable]] = None,
    ) -> Dict[str, Dict[str, SliceMetrics]]:
        """
        Evaluate models on specific data slices (emoji-heavy, booster-present, etc.).

        Args:
            models: Dictionary of model_name -> model_instance
            data: DataFrame with comments and labels
            text_col: Column name for comment text
            label_col: Column name for sentiment labels
            slice_definitions: Dictionary of slice_name -> filter_function

        Returns:
            Dictionary of model_name -> slice_name -> SliceMetrics
        """
        if slice_definitions is None:
            slice_definitions = self._get_default_slice_definitions()

        results = {}

        for model_name, model in models.items():
            results[model_name] = {}

            for slice_name, slice_filter in slice_definitions.items():
                try:
                    # Apply slice filter
                    slice_data = data[slice_filter(data[text_col])]

                    if len(slice_data) < 5:
                        warnings.warn(f"Slice '{slice_name}' has only {len(slice_data)} samples, skipping")
                        continue

                    # Get predictions for slice
                    slice_comments = slice_data[text_col].tolist()
                    slice_labels = slice_data[label_col].tolist()
                    predictions = self._get_model_predictions(model, slice_comments)

                    # Compute metrics
                    accuracy = accuracy_score(slice_labels, predictions)
                    f1 = f1_score(slice_labels, predictions, average="macro")
                    precision = precision_score(slice_labels, predictions, average="macro", zero_division=0)
                    recall = recall_score(slice_labels, predictions, average="macro", zero_division=0)

                    # Bootstrap CI for accuracy
                    if len(slice_data) >= 10:
                        accuracies = []
                        for _ in range(100):  # Smaller bootstrap for slices
                            indices = np.random.choice(len(slice_labels), size=len(slice_labels), replace=True)
                            boot_labels = [slice_labels[i] for i in indices]
                            boot_preds = [predictions[i] for i in indices]
                            accuracies.append(accuracy_score(boot_labels, boot_preds))
                        ci = self.bootstrap_confidence_intervals(accuracies, self.confidence_level)
                    else:
                        ci = None

                    results[model_name][slice_name] = SliceMetrics(
                        slice_name=slice_name,
                        sample_size=len(slice_data),
                        accuracy=accuracy,
                        f1_score=f1,
                        precision=precision,
                        recall=recall,
                        confidence_interval=ci,
                    )

                except Exception as e:
                    warnings.warn(f"Slice evaluation failed for {model_name}/{slice_name}: {e}")
                    continue

        return results

    def apply_multiple_comparison_correction(
        self, p_values: List[float], method: str = "benjamini_hochberg", alpha: float = 0.05
    ) -> List[bool]:
        """
        Apply multiple comparison correction to control false discovery rate.

        Args:
            p_values: List of p-values from multiple tests
            method: Correction method ("benjamini_hochberg" or "bonferroni")
            alpha: Family-wise error rate

        Returns:
            List of boolean values indicating significance after correction
        """
        if not p_values:
            return []

        if method == "benjamini_hochberg":
            return self._benjamini_hochberg_correction(p_values, alpha)
        elif method == "bonferroni":
            corrected_alpha = alpha / len(p_values)
            return [p < corrected_alpha for p in p_values]
        else:
            raise ValueError(f"Unknown correction method: {method}")

    def _benjamini_hochberg_correction(self, p_values: List[float], alpha: float) -> List[bool]:
        """Apply Benjamini-Hochberg FDR correction."""
        n = len(p_values)
        if n == 0:
            return []

        # Sort p-values with original indices
        indexed_p_values = [(p, i) for i, p in enumerate(p_values)]
        indexed_p_values.sort()

        # Apply BH procedure
        significant = [False] * n
        for k, (p_value, original_index) in enumerate(indexed_p_values):
            bh_threshold = (k + 1) / n * alpha
            if p_value <= bh_threshold:
                significant[original_index] = True
            else:
                # BH procedure: once we fail, all subsequent tests fail
                break

        return significant

    # --------------------------- Helper Methods ---------------------------

    def _get_model_predictions(self, model: Any, comments: List[str]) -> List[str]:
        """Get predictions from a model, handling different model interfaces."""
        predictions = []

        for comment in comments:
            try:
                if hasattr(model, "predict"):
                    # Sklearn-style interface
                    pred = model.predict([comment])[0]
                elif hasattr(model, "polarity_scores"):
                    # VADER-style interface
                    scores = model.polarity_scores(comment)
                    compound = scores["compound"]
                    if compound >= 0.05:
                        pred = "positive"
                    elif compound <= -0.05:
                        pred = "negative"
                    else:
                        pred = "neutral"
                elif callable(model):
                    # Function interface
                    result = model(comment)
                    if isinstance(result, dict) and "sentiment" in result:
                        pred = result["sentiment"]
                    elif isinstance(result, str):
                        pred = result
                    else:
                        pred = str(result)
                else:
                    raise ModelComparisonError(f"Unknown model interface: {type(model)}")

                predictions.append(pred)

            except Exception as e:
                warnings.warn(f"Prediction failed for comment '{comment[:50]}...': {e}")
                predictions.append("neutral")  # Default fallback

        return predictions

    def _compute_evaluation_metrics(
        self,
        model_name: str,
        true_labels: List[str],
        predictions: List[str],
        experiment_config: Optional[ExperimentConfig] = None,
    ) -> EvaluationResults:
        """Compute comprehensive evaluation metrics."""

        # Overall metrics
        accuracy = accuracy_score(true_labels, predictions)
        precision = precision_score(true_labels, predictions, average="macro", zero_division=0)
        recall = recall_score(true_labels, predictions, average="macro", zero_division=0)
        f1 = f1_score(true_labels, predictions, average="macro", zero_division=0)
        macro_f1 = f1  # Same as f1 with macro averaging

        # Per-class metrics
        class_report = classification_report(true_labels, predictions, output_dict=True, zero_division=0)
        class_metrics = {}

        for label, metrics in class_report.items():
            if label not in ["accuracy", "macro avg", "weighted avg"]:
                class_metrics[label] = ClassMetrics(
                    precision=metrics["precision"],
                    recall=metrics["recall"],
                    f1_score=metrics["f1-score"],
                    support=int(metrics["support"]),
                )

        # Bootstrap confidence intervals for key metrics
        if len(true_labels) >= 10:
            accuracy_values = []
            f1_values = []

            for _ in range(100):  # Bootstrap samples
                indices = np.random.choice(len(true_labels), size=len(true_labels), replace=True)
                boot_true = [true_labels[i] for i in indices]
                boot_pred = [predictions[i] for i in indices]

                accuracy_values.append(accuracy_score(boot_true, boot_pred))
                f1_values.append(f1_score(boot_true, boot_pred, average="macro", zero_division=0))

            confidence_intervals = {
                "accuracy": self.bootstrap_confidence_intervals(accuracy_values, self.confidence_level),
                "f1_score": self.bootstrap_confidence_intervals(f1_values, self.confidence_level),
            }
        else:
            confidence_intervals = {}

        return EvaluationResults(
            model_name=model_name,
            variant_config={},  # To be filled by caller if needed
            evaluation_timestamp=datetime.now(),
            accuracy=accuracy,
            precision=precision,
            recall=recall,
            f1_score=f1,
            macro_f1=macro_f1,
            class_metrics=class_metrics,
            confidence_intervals=confidence_intervals,
            experiment_config=experiment_config,
        )

    def _get_model_config(self, model: Any) -> Dict[str, Any]:
        """Extract model configuration for reproducibility."""
        config = {"model_type": type(model).__name__, "model_module": type(model).__module__}

        # Try to extract additional config
        if hasattr(model, "get_params"):
            try:
                config.update(model.get_params())
            except:
                pass

        if hasattr(model, "__dict__"):
            try:
                # Extract simple attributes
                for key, value in model.__dict__.items():
                    if isinstance(value, (str, int, float, bool, type(None))):
                        config[key] = value
            except:
                pass

        return config

    def _compute_data_fingerprint(self, comments: List[str], labels: List[str]) -> str:
        """Compute fingerprint of data for reproducibility tracking."""
        import hashlib

        # Create deterministic hash of data
        data_str = json.dumps(
            {"comments": sorted(comments), "labels": sorted(labels), "size": len(comments)}, sort_keys=True
        )

        return hashlib.sha256(data_str.encode()).hexdigest()[:16]

    def _get_default_slice_definitions(self) -> Dict[str, callable]:
        """Get default data slice definitions for evaluation."""
        import re

        def has_emoji(text_series):
            emoji_pattern = re.compile(
                r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002700-\U000027BF]+"
            )
            return text_series.str.contains(emoji_pattern, regex=True, na=False)

        def has_boosters(text_series):
            booster_pattern = r"\b(no cap|fr|frfr|deadass|af|asf|lowkey|highkey)\b"
            return text_series.str.contains(booster_pattern, case=False, regex=True, na=False)

        def has_idioms(text_series):
            idiom_pattern = r"\b(this slaps|ate and left no crumbs|hits different|goes hard|chef\'s kiss)\b"
            return text_series.str.contains(idiom_pattern, case=False, regex=True, na=False)

        def is_long_comment(text_series):
            return text_series.str.len() > 100

        def is_short_comment(text_series):
            return text_series.str.len() <= 20

        return {
            "emoji_heavy": has_emoji,
            "booster_present": has_boosters,
            "idiom_present": has_idioms,
            "long_comments": is_long_comment,
            "short_comments": is_short_comment,
        }


# --------------------------- Convenience Functions ---------------------------


def create_evaluation_framework(random_seed: int = 42, confidence_level: float = 0.95) -> SentimentEvaluationFramework:
    """Create a configured evaluation framework instance."""
    return SentimentEvaluationFramework(random_seed=random_seed, confidence_level=confidence_level)


def quick_model_comparison(
    models: Dict[str, Any], comments: List[str], true_labels: List[str], random_seed: int = 42
) -> Dict[str, EvaluationResults]:
    """Quick comparison of multiple models on the same dataset."""
    framework = create_evaluation_framework(random_seed=random_seed)
    return framework.run_paired_evaluation(models, comments, true_labels)


if __name__ == "__main__":
    # Example usage
    print("🧪 Sentiment Evaluation Framework")
    print("=" * 50)

    # Mock data for testing
    mock_comments = [
        "this song slaps fr",
        "not feeling this one",
        "absolute banger no cap",
        "meh it's okay",
        "obsessed with this track",
    ]

    mock_labels = ["positive", "negative", "positive", "neutral", "positive"]

    # Mock models for testing
    class MockModel:
        def __init__(self, name):
            self.name = name

        def predict(self, comments):
            # Simple mock predictions
            return [
                (
                    "positive"
                    if "slaps" in c or "banger" in c or "obsessed" in c
                    else "negative" if "not feeling" in c else "neutral"
                )
                for c in comments
            ]

    models = {"mock_model_a": MockModel("A"), "mock_model_b": MockModel("B")}

    try:
        framework = create_evaluation_framework()
        results = framework.run_paired_evaluation(models, mock_comments, mock_labels)

        print(f"✅ Evaluation completed for {len(results)} models")
        for model_name, result in results.items():
            print(f"  {model_name}: F1={result.f1_score:.3f}, Accuracy={result.accuracy:.3f}")

    except Exception as e:
        print(f"❌ Evaluation failed: {e}")
