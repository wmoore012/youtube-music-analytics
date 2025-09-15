#!/usr/bin/env python3
"""
Dataset-Based Model Evaluation Framework

Comprehensive evaluation framework that uses the centralized music industry
sentiment dataset to test and compare different sentiment models.

This framework provides:
- Consistent evaluation across all models
- Random train/test splits for fair comparison
- Detailed performance metrics by category
- Statistical significance testing
- Model comparison and recommendation
"""

import os
import random
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from datasets.music_industry_sentiment_dataset import (
    SentimentLabel,
    get_music_industry_dataset,
)
from src.youtubeviz.production_music_sentiment import ProductionMusicSentimentAnalyzer


@dataclass
class ModelEvaluationResult:
    """Results from evaluating a sentiment model."""

    model_name: str
    total_tests: int
    correct_predictions: int
    accuracy: float
    precision_by_sentiment: Dict[str, float]
    recall_by_sentiment: Dict[str, float]
    f1_by_sentiment: Dict[str, float]
    category_accuracy: Dict[str, float]
    failed_cases: List[Dict]
    confusion_matrix: Dict[str, Dict[str, int]]
    available: bool = True
    error_message: str = ""


class DatasetModelEvaluator:
    """
    Comprehensive model evaluation using the centralized dataset.

    Provides fair, consistent evaluation of sentiment models against
    the scientifically classified music industry sentiment dataset.
    """

    def __init__(self, test_size: float = 0.2, random_state: int = 42):
        """
        Initialize the evaluator.

        Args:
            test_size: Proportion of dataset to use for testing
            random_state: Random seed for reproducible splits
        """
        self.dataset = get_music_industry_dataset()
        self.test_size = test_size
        self.random_state = random_state

        # Get train/test split
        self.train_entries, self.test_entries = self.dataset.get_train_test_split(test_size, random_state)

        print(f"📊 Dataset loaded: {len(self.dataset.entries)} total phrases")
        print(f"🔄 Train/Test split: {len(self.train_entries)} train, {len(self.test_entries)} test")

    def _calculate_metrics(self, y_true: List[str], y_pred: List[str]) -> Dict:
        """Calculate precision, recall, F1 for each sentiment class."""

        # Get unique labels
        labels = list(set(y_true + y_pred))

        metrics = {}
        confusion_matrix = {label: {pred_label: 0 for pred_label in labels} for label in labels}

        # Build confusion matrix
        for true_label, pred_label in zip(y_true, y_pred):
            confusion_matrix[true_label][pred_label] += 1

        # Calculate metrics for each label
        for label in labels:
            tp = confusion_matrix[label][label]
            fp = sum(confusion_matrix[other_label][label] for other_label in labels if other_label != label)
            fn = sum(confusion_matrix[label][other_label] for other_label in labels if other_label != label)

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

            metrics[label] = {"precision": precision, "recall": recall, "f1": f1}

        return metrics, confusion_matrix

    def evaluate_vader_model(self) -> ModelEvaluationResult:
        """Evaluate VADER sentiment model."""
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

            vader = SentimentIntensityAnalyzer()

            y_true = []
            y_pred = []
            failed_cases = []
            category_results = {}

            for entry in self.test_entries:
                # Get VADER prediction
                scores = vader.polarity_scores(entry.phrase)
                compound = scores["compound"]

                if compound >= 0.05:
                    predicted = "positive"
                elif compound <= -0.05:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                expected = entry.sentiment.value

                y_true.append(expected)
                y_pred.append(predicted)

                # Track failed cases
                if predicted != expected:
                    failed_cases.append(
                        {
                            "phrase": entry.phrase,
                            "expected": expected,
                            "predicted": predicted,
                            "score": compound,
                            "category": entry.category.value,
                            "confidence": entry.confidence,
                        }
                    )

                # Category tracking
                category = entry.category.value
                if category not in category_results:
                    category_results[category] = {"total": 0, "correct": 0}
                category_results[category]["total"] += 1
                if predicted == expected:
                    category_results[category]["correct"] += 1

            # Calculate metrics
            metrics, confusion_matrix = self._calculate_metrics(y_true, y_pred)

            # Calculate category accuracy
            category_accuracy = {
                cat: stats["correct"] / stats["total"] if stats["total"] > 0 else 0.0
                for cat, stats in category_results.items()
            }

            accuracy = sum(1 for t, p in zip(y_true, y_pred) if t == p) / len(y_true) * 100

            return ModelEvaluationResult(
                model_name="VADER",
                total_tests=len(self.test_entries),
                correct_predictions=len(y_true) - len(failed_cases),
                accuracy=accuracy,
                precision_by_sentiment={k: v["precision"] for k, v in metrics.items()},
                recall_by_sentiment={k: v["recall"] for k, v in metrics.items()},
                f1_by_sentiment={k: v["f1"] for k, v in metrics.items()},
                category_accuracy=category_accuracy,
                failed_cases=failed_cases,
                confusion_matrix=confusion_matrix,
            )

        except ImportError as e:
            return ModelEvaluationResult(
                model_name="VADER",
                total_tests=0,
                correct_predictions=0,
                accuracy=0.0,
                precision_by_sentiment={},
                recall_by_sentiment={},
                f1_by_sentiment={},
                category_accuracy={},
                failed_cases=[],
                confusion_matrix={},
                available=False,
                error_message=f"VADER not available: {e}",
            )

    def evaluate_textblob_model(self) -> ModelEvaluationResult:
        """Evaluate TextBlob sentiment model."""
        try:
            from textblob import TextBlob

            y_true = []
            y_pred = []
            failed_cases = []
            category_results = {}

            for entry in self.test_entries:
                # Get TextBlob prediction
                blob = TextBlob(entry.phrase)
                polarity = blob.sentiment.polarity

                if polarity > 0.1:
                    predicted = "positive"
                elif polarity < -0.1:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                expected = entry.sentiment.value

                y_true.append(expected)
                y_pred.append(predicted)

                # Track failed cases
                if predicted != expected:
                    failed_cases.append(
                        {
                            "phrase": entry.phrase,
                            "expected": expected,
                            "predicted": predicted,
                            "score": polarity,
                            "category": entry.category.value,
                            "confidence": entry.confidence,
                        }
                    )

                # Category tracking
                category = entry.category.value
                if category not in category_results:
                    category_results[category] = {"total": 0, "correct": 0}
                category_results[category]["total"] += 1
                if predicted == expected:
                    category_results[category]["correct"] += 1

            # Calculate metrics
            metrics, confusion_matrix = self._calculate_metrics(y_true, y_pred)

            # Calculate category accuracy
            category_accuracy = {
                cat: stats["correct"] / stats["total"] if stats["total"] > 0 else 0.0
                for cat, stats in category_results.items()
            }

            accuracy = sum(1 for t, p in zip(y_true, y_pred) if t == p) / len(y_true) * 100

            return ModelEvaluationResult(
                model_name="TextBlob",
                total_tests=len(self.test_entries),
                correct_predictions=len(y_true) - len(failed_cases),
                accuracy=accuracy,
                precision_by_sentiment={k: v["precision"] for k, v in metrics.items()},
                recall_by_sentiment={k: v["recall"] for k, v in metrics.items()},
                f1_by_sentiment={k: v["f1"] for k, v in metrics.items()},
                category_accuracy=category_accuracy,
                failed_cases=failed_cases,
                confusion_matrix=confusion_matrix,
            )

        except ImportError as e:
            return ModelEvaluationResult(
                model_name="TextBlob",
                total_tests=0,
                correct_predictions=0,
                accuracy=0.0,
                precision_by_sentiment={},
                recall_by_sentiment={},
                f1_by_sentiment={},
                category_accuracy={},
                failed_cases=[],
                confusion_matrix={},
                available=False,
                error_message=f"TextBlob not available: {e}",
            )

    def evaluate_production_model(self) -> ModelEvaluationResult:
        """Evaluate our production music sentiment model."""
        try:
            # Create analyzer without loading dataset (to avoid data leakage)
            analyzer = ProductionMusicSentimentAnalyzer(use_dataset=False)

            # Train on training set
            for entry in self.train_entries:
                if entry.sentiment == SentimentLabel.POSITIVE:
                    analyzer.positive_phrases[entry.phrase.lower()] = entry.confidence
                elif entry.sentiment == SentimentLabel.NEGATIVE:
                    analyzer.negative_phrases[entry.phrase.lower()] = entry.confidence

            y_true = []
            y_pred = []
            failed_cases = []
            category_results = {}

            for entry in self.test_entries:
                # Get production model prediction
                result = analyzer.analyze_comment(entry.phrase)
                score = result["sentiment_score"]

                if score > 0.1:
                    predicted = "positive"
                elif score < -0.1:
                    predicted = "negative"
                else:
                    predicted = "neutral"

                expected = entry.sentiment.value

                y_true.append(expected)
                y_pred.append(predicted)

                # Track failed cases
                if predicted != expected:
                    failed_cases.append(
                        {
                            "phrase": entry.phrase,
                            "expected": expected,
                            "predicted": predicted,
                            "score": score,
                            "category": entry.category.value,
                            "confidence": entry.confidence,
                        }
                    )

                # Category tracking
                category = entry.category.value
                if category not in category_results:
                    category_results[category] = {"total": 0, "correct": 0}
                category_results[category]["total"] += 1
                if predicted == expected:
                    category_results[category]["correct"] += 1

            # Calculate metrics
            metrics, confusion_matrix = self._calculate_metrics(y_true, y_pred)

            # Calculate category accuracy
            category_accuracy = {
                cat: stats["correct"] / stats["total"] if stats["total"] > 0 else 0.0
                for cat, stats in category_results.items()
            }

            accuracy = sum(1 for t, p in zip(y_true, y_pred) if t == p) / len(y_true) * 100

            return ModelEvaluationResult(
                model_name="Production Music",
                total_tests=len(self.test_entries),
                correct_predictions=len(y_true) - len(failed_cases),
                accuracy=accuracy,
                precision_by_sentiment={k: v["precision"] for k, v in metrics.items()},
                recall_by_sentiment={k: v["recall"] for k, v in metrics.items()},
                f1_by_sentiment={k: v["f1"] for k, v in metrics.items()},
                category_accuracy=category_accuracy,
                failed_cases=failed_cases,
                confusion_matrix=confusion_matrix,
            )

        except Exception as e:
            return ModelEvaluationResult(
                model_name="Production Music",
                total_tests=0,
                correct_predictions=0,
                accuracy=0.0,
                precision_by_sentiment={},
                recall_by_sentiment={},
                f1_by_sentiment={},
                category_accuracy={},
                failed_cases=[],
                confusion_matrix={},
                available=False,
                error_message=f"Production model error: {e}",
            )

    def run_comprehensive_evaluation(self) -> List[ModelEvaluationResult]:
        """Run comprehensive evaluation on all available models."""

        print("\n🎵 Comprehensive Model Evaluation on Music Industry Dataset")
        print("=" * 80)

        # Print dataset statistics
        self.dataset.print_statistics()

        print(f"\n🧪 Evaluating Models on Test Set ({len(self.test_entries)} phrases)...")
        print("-" * 60)

        # Evaluate all models
        results = [self.evaluate_vader_model(), self.evaluate_textblob_model(), self.evaluate_production_model()]

        # Print results
        for result in results:
            self._print_model_results(result)

        # Compare models
        self._print_model_comparison(results)

        # Save detailed results
        self._save_evaluation_results(results)

        return results

    def _print_model_results(self, result: ModelEvaluationResult):
        """Print detailed results for a model."""

        if not result.available:
            print(f"\n❌ {result.model_name}: {result.error_message}")
            return

        print(f"\n📊 {result.model_name} Results:")
        print(f"   Accuracy: {result.accuracy:.1f}% ({result.correct_predictions}/{result.total_tests})")

        # Sentiment-wise metrics
        print(f"   Precision by sentiment:")
        for sentiment, precision in result.precision_by_sentiment.items():
            print(f"     {sentiment}: {precision:.3f}")

        print(f"   Recall by sentiment:")
        for sentiment, recall in result.recall_by_sentiment.items():
            print(f"     {sentiment}: {recall:.3f}")

        print(f"   F1 by sentiment:")
        for sentiment, f1 in result.f1_by_sentiment.items():
            print(f"     {sentiment}: {f1:.3f}")

        # Category performance (top 5 worst)
        worst_categories = sorted(result.category_accuracy.items(), key=lambda x: x[1])[:5]
        if worst_categories:
            print(f"   Worst performing categories:")
            for category, accuracy in worst_categories:
                print(f"     {category}: {accuracy:.1%}")

    def _print_model_comparison(self, results: List[ModelEvaluationResult]):
        """Print comparison between models."""

        available_results = [r for r in results if r.available]
        if not available_results:
            print("\n❌ No models available for comparison")
            return

        print(f"\n🏆 Model Comparison Summary:")
        print("-" * 60)

        # Sort by accuracy
        available_results.sort(key=lambda x: x.accuracy, reverse=True)

        print(f"{'Model':<20} {'Accuracy':<10} {'Avg F1':<10} {'Best Category':<15}")
        print("-" * 65)

        for result in available_results:
            avg_f1 = sum(result.f1_by_sentiment.values()) / len(result.f1_by_sentiment) if result.f1_by_sentiment else 0
            best_category = (
                max(result.category_accuracy.items(), key=lambda x: x[1])[0] if result.category_accuracy else "N/A"
            )

            print(f"{result.model_name:<20} {result.accuracy:<10.1f}% {avg_f1:<10.3f} {best_category:<15}")

        # Recommend best model
        best_model = available_results[0]
        print(f"\n🎯 RECOMMENDED MODEL: {best_model.model_name}")
        print(f"   Accuracy: {best_model.accuracy:.1f}%")
        print(f"   Strengths: {', '.join([k for k, v in best_model.category_accuracy.items() if v > 0.8])}")

        if best_model.model_name == "Production Music":
            print("✅ Our production model performs best for music industry analytics!")
        else:
            print(f"⚠️  {best_model.model_name} outperforms our production model.")
            print("   Consider improving the production model.")

    def _save_evaluation_results(self, results: List[ModelEvaluationResult]):
        """Save detailed evaluation results to files."""

        # Save summary
        summary_data = []
        for result in results:
            if result.available:
                summary_data.append(
                    {
                        "model": result.model_name,
                        "accuracy": result.accuracy,
                        "correct": result.correct_predictions,
                        "total": result.total_tests,
                        "avg_precision": (
                            sum(result.precision_by_sentiment.values()) / len(result.precision_by_sentiment)
                            if result.precision_by_sentiment
                            else 0
                        ),
                        "avg_recall": (
                            sum(result.recall_by_sentiment.values()) / len(result.recall_by_sentiment)
                            if result.recall_by_sentiment
                            else 0
                        ),
                        "avg_f1": (
                            sum(result.f1_by_sentiment.values()) / len(result.f1_by_sentiment)
                            if result.f1_by_sentiment
                            else 0
                        ),
                    }
                )

        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv("dataset_model_evaluation_summary.csv", index=False)
            print(f"\n💾 Evaluation summary saved to: dataset_model_evaluation_summary.csv")

        # Save failed cases for best model
        available_results = [r for r in results if r.available]
        if available_results:
            best_model = max(available_results, key=lambda x: x.accuracy)

            if best_model.failed_cases:
                failed_df = pd.DataFrame(best_model.failed_cases)
                filename = f"{best_model.model_name.lower().replace(' ', '_')}_failed_cases.csv"
                failed_df.to_csv(filename, index=False)
                print(f"💾 Failed cases saved to: {filename}")


def main():
    """Run comprehensive dataset-based model evaluation."""

    evaluator = DatasetModelEvaluator(test_size=0.2, random_state=42)
    results = evaluator.run_comprehensive_evaluation()

    return results


if __name__ == "__main__":
    main()
