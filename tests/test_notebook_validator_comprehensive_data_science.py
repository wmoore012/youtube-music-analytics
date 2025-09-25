"""
Comprehensive data science validation tests for notebook validation system.

This test suite covers every data science scenario imaginable without actually
installing heavy dependencies. It uses mocks and simulated data to test
validation of common data science workflows, libraries, and patterns.
"""

from datetime import datetime, timedelta
import json
import os
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.data_organization.notebook_validator import (
    MetricExplainer,
    NotebookValidator,
    OutputValidationError,
    OutputValidator,
    SchemaValidationError,
    ValidationError,
    ValidationResult,
)


class TestDataScienceWorkflowValidation:
    """Test validation of common data science workflows and patterns."""

    def setup_method(self):
        """Set up test fixtures."""
        self.notebook_validator = NotebookValidator()
        self.output_validator = OutputValidator()
        self.metric_explainer = MetricExplainer()

    def test_machine_learning_pipeline_validation(self):
        """Test validation of ML pipeline outputs."""
        # Simulate ML model training results
        ml_results = pd.DataFrame(
            {
                "model_name": ["RandomForest", "XGBoost", "LinearRegression", "SVM"],
                "accuracy": [0.92, 0.94, 0.78, 0.89],
                "precision": [0.91, 0.93, 0.76, 0.87],
                "recall": [0.90, 0.95, 0.80, 0.88],
                "f1_score": [0.905, 0.94, 0.78, 0.875],
                "training_time_seconds": [45.2, 120.8, 5.1, 78.3],
                "cross_val_score": [0.89, 0.92, 0.75, 0.86],
            }
        )

        # Validate ML metrics schema
        ml_schema = {
            "type": "dataframe",
            "columns": {
                "model_name": "object",
                "accuracy": "float64",
                "precision": "float64",
                "recall": "float64",
                "f1_score": "float64",
                "training_time_seconds": "float64",
                "cross_val_score": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(ml_results, ml_schema)
        assert result.is_valid is True

        # Validate metric ranges (ML scores should be 0-1)
        for metric in ["accuracy", "precision", "recall", "f1_score", "cross_val_score"]:
            range_result = self.output_validator.validate_score_range(ml_results[metric], 0.0, 1.0)
            assert range_result.is_valid is True, f"{metric} validation failed"

        # Validate training times are positive
        time_result = self.output_validator.validate_score_range(ml_results["training_time_seconds"], 0.0, float("inf"))
        assert time_result.is_valid is True

    def test_time_series_analysis_validation(self):
        """Test validation of time series analysis outputs."""
        # Simulate time series data
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        ts_data = pd.DataFrame(
            {
                "date": dates,
                "value": np.random.randn(100).cumsum() + 100,
                "trend": np.linspace(100, 120, 100),
                "seasonal": np.sin(np.arange(100) * 2 * np.pi / 7) * 5,
                "residual": np.random.randn(100) * 2,
                "forecast": np.random.randn(100) * 3 + 110,
                "confidence_lower": np.random.randn(100) * 2 + 105,
                "confidence_upper": np.random.randn(100) * 2 + 115,
            }
        )

        # Validate time series schema
        ts_schema = {
            "type": "dataframe",
            "columns": {
                "date": "datetime64[ns]",
                "value": "float64",
                "trend": "float64",
                "seasonal": "float64",
                "residual": "float64",
                "forecast": "float64",
                "confidence_lower": "float64",
                "confidence_upper": "float64",
            },
            "min_rows": 30,  # Minimum for meaningful time series
        }

        result = self.notebook_validator.validate_cell_output(ts_data, ts_schema)
        assert result.is_valid is True

        # Validate confidence intervals are ordered correctly
        confidence_valid = (ts_data["confidence_lower"] <= ts_data["confidence_upper"]).all()
        assert confidence_valid, "Confidence intervals should be properly ordered"

    def test_statistical_analysis_validation(self):
        """Test validation of statistical analysis outputs."""
        # Simulate statistical test results
        stats_results = pd.DataFrame(
            {
                "test_name": ["t_test", "chi_square", "anova", "correlation", "regression"],
                "statistic": [2.45, 15.67, 8.92, 0.73, 12.34],
                "p_value": [0.014, 0.001, 0.003, 0.000, 0.045],
                "effect_size": [0.42, 0.31, 0.58, 0.73, 0.29],
                "confidence_interval_lower": [0.12, 0.18, 0.34, 0.65, 0.08],
                "confidence_interval_upper": [0.72, 0.44, 0.82, 0.81, 0.50],
                "sample_size": [150, 200, 180, 120, 175],
                "degrees_of_freedom": [148, 3, 2, 118, 173],
            }
        )

        # Validate statistical results
        stats_schema = {
            "type": "dataframe",
            "columns": {
                "test_name": "object",
                "statistic": "float64",
                "p_value": "float64",
                "effect_size": "float64",
                "confidence_interval_lower": "float64",
                "confidence_interval_upper": "float64",
                "sample_size": "int64",
                "degrees_of_freedom": "int64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(stats_results, stats_schema)
        assert result.is_valid is True

        # Validate p-values are between 0 and 1
        p_result = self.output_validator.validate_score_range(stats_results["p_value"], 0.0, 1.0)
        assert p_result.is_valid is True

        # Validate sample sizes are positive
        sample_result = self.output_validator.validate_score_range(stats_results["sample_size"], 1, float("inf"))
        assert sample_result.is_valid is True

    def test_deep_learning_metrics_validation(self):
        """Test validation of deep learning training metrics."""
        # Simulate deep learning training history
        epochs = 50
        dl_history = pd.DataFrame(
            {
                "epoch": range(1, epochs + 1),
                "train_loss": np.exp(-np.linspace(0, 3, epochs)) + np.abs(np.random.normal(0, 0.01, epochs)),
                "val_loss": np.exp(-np.linspace(0, 2.5, epochs)) + np.abs(np.random.normal(0, 0.02, epochs)),
                "train_accuracy": np.clip(
                    1 - np.exp(-np.linspace(0, 3, epochs)) + np.random.normal(0, 0.01, epochs), 0, 1
                ),
                "val_accuracy": np.clip(
                    1 - np.exp(-np.linspace(0, 2.5, epochs)) + np.random.normal(0, 0.02, epochs), 0, 1
                ),
                "learning_rate": [0.001 * (0.95 ** (i // 10)) for i in range(epochs)],
                "batch_size": [32] * epochs,
                "gradient_norm": np.random.exponential(1.0, epochs),
            }
        )

        # Validate deep learning schema
        dl_schema = {
            "type": "dataframe",
            "columns": {
                "epoch": "int64",
                "train_loss": "float64",
                "val_loss": "float64",
                "train_accuracy": "float64",
                "val_accuracy": "float64",
                "learning_rate": "float64",
                "batch_size": "int64",
                "gradient_norm": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(dl_history, dl_schema)
        assert result.is_valid is True

        # Validate accuracy metrics are between 0 and 1
        for acc_col in ["train_accuracy", "val_accuracy"]:
            acc_result = self.output_validator.validate_score_range(dl_history[acc_col], 0.0, 1.0)
            assert acc_result.is_valid is True

        # Validate loss values are non-negative
        for loss_col in ["train_loss", "val_loss"]:
            loss_result = self.output_validator.validate_score_range(dl_history[loss_col], 0.0, float("inf"))
            assert loss_result.is_valid is True

    def test_clustering_analysis_validation(self):
        """Test validation of clustering analysis results."""
        # Simulate clustering results
        n_samples = 1000
        clustering_results = pd.DataFrame(
            {
                "sample_id": range(n_samples),
                "cluster_id": np.random.randint(0, 5, n_samples),
                "distance_to_centroid": np.random.exponential(2.0, n_samples),
                "silhouette_score": np.random.uniform(-1, 1, n_samples),
                "feature_1": np.random.randn(n_samples),
                "feature_2": np.random.randn(n_samples),
                "feature_3": np.random.randn(n_samples),
                "is_outlier": np.random.choice([True, False], n_samples, p=[0.05, 0.95]),
            }
        )

        # Validate clustering schema
        clustering_schema = {
            "type": "dataframe",
            "columns": {
                "sample_id": "int64",
                "cluster_id": "int64",
                "distance_to_centroid": "float64",
                "silhouette_score": "float64",
                "feature_1": "float64",
                "feature_2": "float64",
                "feature_3": "float64",
                "is_outlier": "bool",
            },
            "min_rows": 10,
        }

        result = self.notebook_validator.validate_cell_output(clustering_results, clustering_schema)
        assert result.is_valid is True

        # Validate silhouette scores are between -1 and 1
        silhouette_result = self.output_validator.validate_score_range(
            clustering_results["silhouette_score"], -1.0, 1.0
        )
        assert silhouette_result.is_valid is True

        # Validate distances are non-negative
        distance_result = self.output_validator.validate_score_range(
            clustering_results["distance_to_centroid"], 0.0, float("inf")
        )
        assert distance_result.is_valid is True

    def test_natural_language_processing_validation(self):
        """Test validation of NLP analysis outputs."""
        # Simulate NLP results
        nlp_results = pd.DataFrame(
            {
                "text_id": range(100),
                "text_length": np.random.randint(10, 500, 100),
                "sentiment_score": np.random.uniform(-1, 1, 100),
                "sentiment_confidence": np.random.uniform(0.5, 1.0, 100),
                "toxicity_score": np.random.uniform(0, 1, 100),
                "readability_score": np.random.uniform(0, 100, 100),
                "named_entities_count": np.random.randint(0, 20, 100),
                "topic_id": np.random.randint(0, 10, 100),
                "topic_probability": np.random.uniform(0, 1, 100),
                "language_detected": np.random.choice(["en", "es", "fr", "de"], 100),
                "language_confidence": np.random.uniform(0.8, 1.0, 100),
            }
        )

        # Validate NLP schema
        nlp_schema = {
            "type": "dataframe",
            "columns": {
                "text_id": "int64",
                "text_length": "int64",
                "sentiment_score": "float64",
                "sentiment_confidence": "float64",
                "toxicity_score": "float64",
                "readability_score": "float64",
                "named_entities_count": "int64",
                "topic_id": "int64",
                "topic_probability": "float64",
                "language_detected": "object",
                "language_confidence": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(nlp_results, nlp_schema)
        assert result.is_valid is True

        # Validate sentiment scores are between -1 and 1
        sentiment_result = self.output_validator.validate_score_range(nlp_results["sentiment_score"], -1.0, 1.0)
        assert sentiment_result.is_valid is True

        # Validate probability scores are between 0 and 1
        for prob_col in ["sentiment_confidence", "toxicity_score", "topic_probability", "language_confidence"]:
            prob_result = self.output_validator.validate_score_range(nlp_results[prob_col], 0.0, 1.0)
            assert prob_result.is_valid is True

    def test_computer_vision_metrics_validation(self):
        """Test validation of computer vision model outputs."""
        # Simulate computer vision results
        cv_results = pd.DataFrame(
            {
                "image_id": range(200),
                "prediction_class": np.random.randint(0, 1000, 200),  # ImageNet classes
                "confidence_score": np.random.uniform(0, 1, 200),
                "bounding_box_x": np.random.uniform(0, 224, 200),
                "bounding_box_y": np.random.uniform(0, 224, 200),
                "bounding_box_width": np.random.uniform(10, 100, 200),
                "bounding_box_height": np.random.uniform(10, 100, 200),
                "iou_score": np.random.uniform(0, 1, 200),
                "precision_at_k": np.random.uniform(0, 1, 200),
                "mean_average_precision": np.random.uniform(0, 1, 200),
                "inference_time_ms": np.random.uniform(1, 100, 200),
            }
        )

        # Validate computer vision schema
        cv_schema = {
            "type": "dataframe",
            "columns": {
                "image_id": "int64",
                "prediction_class": "int64",
                "confidence_score": "float64",
                "bounding_box_x": "float64",
                "bounding_box_y": "float64",
                "bounding_box_width": "float64",
                "bounding_box_height": "float64",
                "iou_score": "float64",
                "precision_at_k": "float64",
                "mean_average_precision": "float64",
                "inference_time_ms": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(cv_results, cv_schema)
        assert result.is_valid is True

        # Validate metric scores are between 0 and 1
        for metric_col in ["confidence_score", "iou_score", "precision_at_k", "mean_average_precision"]:
            metric_result = self.output_validator.validate_score_range(cv_results[metric_col], 0.0, 1.0)
            assert metric_result.is_valid is True

        # Validate bounding box coordinates are non-negative
        for bbox_col in ["bounding_box_x", "bounding_box_y", "bounding_box_width", "bounding_box_height"]:
            bbox_result = self.output_validator.validate_score_range(cv_results[bbox_col], 0.0, float("inf"))
            assert bbox_result.is_valid is True

    def test_recommendation_system_validation(self):
        """Test validation of recommendation system outputs."""
        # Simulate recommendation results
        rec_results = pd.DataFrame(
            {
                "user_id": np.repeat(range(50), 10),  # 50 users, 10 recommendations each
                "item_id": np.random.randint(1, 10000, 500),
                "predicted_rating": np.random.uniform(1, 5, 500),
                "confidence_score": np.random.uniform(0, 1, 500),
                "rank": np.tile(range(1, 11), 50),  # Rank 1-10 for each user
                "diversity_score": np.random.uniform(0, 1, 500),
                "novelty_score": np.random.uniform(0, 1, 500),
                "serendipity_score": np.random.uniform(0, 1, 500),
                "explanation_strength": np.random.uniform(0, 1, 500),
            }
        )

        # Validate recommendation schema
        rec_schema = {
            "type": "dataframe",
            "columns": {
                "user_id": "int64",
                "item_id": "int64",
                "predicted_rating": "float64",
                "confidence_score": "float64",
                "rank": "int64",
                "diversity_score": "float64",
                "novelty_score": "float64",
                "serendipity_score": "float64",
                "explanation_strength": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(rec_results, rec_schema)
        assert result.is_valid is True

        # Validate rating range (1-5 scale)
        rating_result = self.output_validator.validate_score_range(rec_results["predicted_rating"], 1.0, 5.0)
        assert rating_result.is_valid is True

        # Validate score ranges (0-1)
        for score_col in [
            "confidence_score",
            "diversity_score",
            "novelty_score",
            "serendipity_score",
            "explanation_strength",
        ]:
            score_result = self.output_validator.validate_score_range(rec_results[score_col], 0.0, 1.0)
            assert score_result.is_valid is True

    def test_anomaly_detection_validation(self):
        """Test validation of anomaly detection outputs."""
        # Simulate anomaly detection results
        anomaly_results = pd.DataFrame(
            {
                "sample_id": range(1000),
                "anomaly_score": np.random.exponential(0.1, 1000),  # Most values near 0, few high
                "is_anomaly": np.random.choice([True, False], 1000, p=[0.05, 0.95]),
                "isolation_score": np.random.uniform(-1, 1, 1000),
                "local_outlier_factor": np.random.uniform(0.5, 3.0, 1000),
                "reconstruction_error": np.random.exponential(0.5, 1000),
                "mahalanobis_distance": np.random.gamma(2, 2, 1000),
                "confidence_level": np.random.uniform(0, 1, 1000),
            }
        )

        # Validate anomaly detection schema
        anomaly_schema = {
            "type": "dataframe",
            "columns": {
                "sample_id": "int64",
                "anomaly_score": "float64",
                "is_anomaly": "bool",
                "isolation_score": "float64",
                "local_outlier_factor": "float64",
                "reconstruction_error": "float64",
                "mahalanobis_distance": "float64",
                "confidence_level": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(anomaly_results, anomaly_schema)
        assert result.is_valid is True

        # Validate anomaly scores are non-negative
        anomaly_result = self.output_validator.validate_score_range(anomaly_results["anomaly_score"], 0.0, float("inf"))
        assert anomaly_result.is_valid is True

        # Validate confidence levels are between 0 and 1
        confidence_result = self.output_validator.validate_score_range(anomaly_results["confidence_level"], 0.0, 1.0)
        assert confidence_result.is_valid is True

    def test_feature_engineering_validation(self):
        """Test validation of feature engineering outputs."""
        # Simulate feature engineering results
        n_samples = 500
        feature_data = pd.DataFrame(
            {
                "original_feature_1": np.random.randn(n_samples),
                "original_feature_2": np.random.randn(n_samples),
                "scaled_feature_1": np.random.randn(n_samples),  # Standardized
                "scaled_feature_2": np.random.randn(n_samples),
                "normalized_feature_1": np.random.uniform(0, 1, n_samples),  # Min-max normalized
                "normalized_feature_2": np.random.uniform(0, 1, n_samples),
                "log_transformed": np.random.lognormal(0, 1, n_samples),
                "polynomial_feature": np.random.randn(n_samples) ** 2,
                "interaction_feature": np.random.randn(n_samples) * np.random.randn(n_samples),
                "binned_feature": np.random.randint(0, 5, n_samples),
                "encoded_categorical": np.random.randint(0, 10, n_samples),
                "feature_importance": np.random.uniform(0, 1, n_samples),
            }
        )

        # Validate feature engineering schema
        feature_schema = {
            "type": "dataframe",
            "columns": {
                "original_feature_1": "float64",
                "original_feature_2": "float64",
                "scaled_feature_1": "float64",
                "scaled_feature_2": "float64",
                "normalized_feature_1": "float64",
                "normalized_feature_2": "float64",
                "log_transformed": "float64",
                "polynomial_feature": "float64",
                "interaction_feature": "float64",
                "binned_feature": "int64",
                "encoded_categorical": "int64",
                "feature_importance": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(feature_data, feature_schema)
        assert result.is_valid is True

        # Validate normalized features are between 0 and 1
        for norm_col in ["normalized_feature_1", "normalized_feature_2"]:
            norm_result = self.output_validator.validate_score_range(feature_data[norm_col], 0.0, 1.0)
            assert norm_result.is_valid is True

        # Validate feature importance scores
        importance_result = self.output_validator.validate_score_range(feature_data["feature_importance"], 0.0, 1.0)
        assert importance_result.is_valid is True

    def test_ab_testing_validation(self):
        """Test validation of A/B testing analysis outputs."""
        # Simulate A/B test results
        ab_results = pd.DataFrame(
            {
                "experiment_id": ["exp_001", "exp_002", "exp_003", "exp_004"],
                "variant": ["A", "B", "A", "B"],
                "sample_size": [1000, 1050, 800, 820],
                "conversion_rate": [0.12, 0.15, 0.08, 0.11],
                "confidence_interval_lower": [0.10, 0.13, 0.06, 0.09],
                "confidence_interval_upper": [0.14, 0.17, 0.10, 0.13],
                "p_value": [0.023, 0.045, 0.156, 0.089],
                "effect_size": [0.25, 0.38, 0.15, 0.28],
                "statistical_power": [0.80, 0.85, 0.65, 0.75],
                "lift_percentage": [25.0, 37.5, 37.5, 37.5],
                "revenue_impact": [2500.0, 3750.0, 1200.0, 1680.0],
            }
        )

        # Validate A/B testing schema
        ab_schema = {
            "type": "dataframe",
            "columns": {
                "experiment_id": "object",
                "variant": "object",
                "sample_size": "int64",
                "conversion_rate": "float64",
                "confidence_interval_lower": "float64",
                "confidence_interval_upper": "float64",
                "p_value": "float64",
                "effect_size": "float64",
                "statistical_power": "float64",
                "lift_percentage": "float64",
                "revenue_impact": "float64",
            },
            "min_rows": 2,  # Need at least A and B variants
        }

        result = self.notebook_validator.validate_cell_output(ab_results, ab_schema)
        assert result.is_valid is True

        # Validate conversion rates are between 0 and 1
        conversion_result = self.output_validator.validate_score_range(ab_results["conversion_rate"], 0.0, 1.0)
        assert conversion_result.is_valid is True

        # Validate p-values are between 0 and 1
        p_result = self.output_validator.validate_score_range(ab_results["p_value"], 0.0, 1.0)
        assert p_result.is_valid is True

        # Validate statistical power is between 0 and 1
        power_result = self.output_validator.validate_score_range(ab_results["statistical_power"], 0.0, 1.0)
        assert power_result.is_valid is True

    @patch("importlib.import_module")
    def test_mock_heavy_library_validation(self, mock_import):
        """Test validation with mocked heavy data science libraries."""
        # Mock heavy libraries that we don't want to actually import
        mock_sklearn = Mock()
        mock_tensorflow = Mock()
        mock_pytorch = Mock()
        mock_xgboost = Mock()
        mock_lightgbm = Mock()
        mock_catboost = Mock()

        def mock_import_side_effect(module_name):
            if module_name == "sklearn":
                return mock_sklearn
            elif module_name == "tensorflow":
                return mock_tensorflow
            elif module_name == "torch":
                return mock_pytorch
            elif module_name == "xgboost":
                return mock_xgboost
            elif module_name == "lightgbm":
                return mock_lightgbm
            elif module_name == "catboost":
                return mock_catboost
            else:
                raise ImportError(f"No module named '{module_name}'")

        mock_import.side_effect = mock_import_side_effect

        # Simulate model comparison results from multiple libraries
        model_comparison = pd.DataFrame(
            {
                "library": ["sklearn", "xgboost", "lightgbm", "catboost", "tensorflow"],
                "model_type": [
                    "RandomForest",
                    "XGBClassifier",
                    "LGBMClassifier",
                    "CatBoostClassifier",
                    "Neural Network",
                ],
                "accuracy": [0.89, 0.92, 0.91, 0.90, 0.93],
                "training_time": [45.2, 120.8, 89.3, 156.7, 450.2],
                "memory_usage_mb": [128.0, 256.0, 180.0, 220.0, 512.0],
                "model_size_mb": [15.2, 8.9, 12.1, 18.5, 45.8],
            }
        )

        # Validate the comparison results
        comparison_schema = {
            "type": "dataframe",
            "columns": {
                "library": "object",
                "model_type": "object",
                "accuracy": "float64",
                "training_time": "float64",
                "memory_usage_mb": "float64",
                "model_size_mb": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(model_comparison, comparison_schema)
        assert result.is_valid is True

        # Validate accuracy scores
        acc_result = self.output_validator.validate_score_range(model_comparison["accuracy"], 0.0, 1.0)
        assert acc_result.is_valid is True

        # Validate resource usage is positive
        for resource_col in ["training_time", "memory_usage_mb", "model_size_mb"]:
            resource_result = self.output_validator.validate_score_range(
                model_comparison[resource_col], 0.0, float("inf")
            )
            assert resource_result.is_valid is True

    def test_hyperparameter_tuning_validation(self):
        """Test validation of hyperparameter tuning results."""
        # Simulate hyperparameter tuning results
        hp_results = pd.DataFrame(
            {
                "trial_id": range(100),
                "learning_rate": np.random.uniform(0.001, 0.1, 100),
                "max_depth": np.random.randint(3, 15, 100),
                "n_estimators": np.random.randint(50, 500, 100),
                "subsample": np.random.uniform(0.5, 1.0, 100),
                "colsample_bytree": np.random.uniform(0.5, 1.0, 100),
                "reg_alpha": np.random.uniform(0, 1, 100),
                "reg_lambda": np.random.uniform(0, 1, 100),
                "cv_score": np.random.uniform(0.7, 0.95, 100),
                "std_score": np.random.uniform(0.01, 0.05, 100),
                "fit_time": np.random.uniform(10, 300, 100),
                "score_time": np.random.uniform(1, 10, 100),
            }
        )

        # Validate hyperparameter tuning schema
        hp_schema = {
            "type": "dataframe",
            "columns": {
                "trial_id": "int64",
                "learning_rate": "float64",
                "max_depth": "int64",
                "n_estimators": "int64",
                "subsample": "float64",
                "colsample_bytree": "float64",
                "reg_alpha": "float64",
                "reg_lambda": "float64",
                "cv_score": "float64",
                "std_score": "float64",
                "fit_time": "float64",
                "score_time": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(hp_results, hp_schema)
        assert result.is_valid is True

        # Validate hyperparameter ranges
        lr_result = self.output_validator.validate_score_range(hp_results["learning_rate"], 0.0, 1.0)
        assert lr_result.is_valid is True

        subsample_result = self.output_validator.validate_score_range(hp_results["subsample"], 0.0, 1.0)
        assert subsample_result.is_valid is True

        colsample_result = self.output_validator.validate_score_range(hp_results["colsample_bytree"], 0.0, 1.0)
        assert colsample_result.is_valid is True

    def test_data_quality_assessment_validation(self):
        """Test validation of data quality assessment outputs."""
        # Simulate data quality assessment
        dq_results = pd.DataFrame(
            {
                "column_name": ["feature_1", "feature_2", "feature_3", "target", "id"],
                "data_type": ["float64", "int64", "object", "float64", "int64"],
                "missing_percentage": [0.05, 0.12, 0.00, 0.02, 0.00],
                "unique_values": [450, 25, 8, 2, 1000],
                "cardinality_ratio": [0.45, 0.025, 0.008, 0.002, 1.0],
                "outlier_percentage": [0.08, 0.15, 0.00, 0.03, 0.00],
                "skewness": [0.23, -1.45, 0.00, 0.67, 0.12],
                "kurtosis": [2.89, 5.67, 0.00, 3.21, 2.95],
                "data_quality_score": [0.87, 0.73, 0.95, 0.91, 0.98],
            }
        )

        # Validate data quality schema
        dq_schema = {
            "type": "dataframe",
            "columns": {
                "column_name": "object",
                "data_type": "object",
                "missing_percentage": "float64",
                "unique_values": "int64",
                "cardinality_ratio": "float64",
                "outlier_percentage": "float64",
                "skewness": "float64",
                "kurtosis": "float64",
                "data_quality_score": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(dq_results, dq_schema)
        assert result.is_valid is True

        # Validate percentage values are between 0 and 1
        for pct_col in ["missing_percentage", "cardinality_ratio", "outlier_percentage", "data_quality_score"]:
            pct_result = self.output_validator.validate_score_range(dq_results[pct_col], 0.0, 1.0)
            assert pct_result.is_valid is True

    def test_comprehensive_data_science_explanations(self):
        """Test metric explanations for various data science metrics."""
        # Test explanations for different types of data science metrics
        ds_metrics = {
            "accuracy": 0.92,
            "precision": 0.89,
            "recall": 0.94,
            "f1_score": 0.915,
            "auc_roc": 0.87,
            "mean_squared_error": 0.15,
            "r_squared": 0.78,
            "silhouette_score": 0.65,
            "adjusted_rand_index": 0.72,
            "mutual_information": 0.83,
        }

        # Generate explanations for each metric
        explanations = {}
        for metric, value in ds_metrics.items():
            explanation = self.metric_explainer.generate_tooltip_text(metric, value)
            explanations[metric] = explanation

            # Verify explanation is generated
            assert len(explanation) > 10
            assert str(value) in explanation or f"{value:.3f}" in explanation

        # Test legend generation for data science metrics
        legends = self.metric_explainer.create_legend_definitions(list(ds_metrics.keys()))
        assert len(legends) == len(ds_metrics)

        for metric in ds_metrics.keys():
            assert metric in legends
            assert len(legends[metric]) > 10

    def test_edge_cases_and_error_scenarios(self):
        """Test validation of edge cases and error scenarios in data science."""
        # Test with extreme values
        extreme_data = pd.DataFrame(
            {
                "very_small_values": [1e-10, 1e-15, 1e-20],
                "very_large_values": [1e10, 1e15, 1e20],
                "infinite_values": [float("inf"), float("-inf"), float("nan")],
                "zero_values": [0.0, 0.0, 0.0],
                "negative_values": [-1.0, -100.0, -0.001],
            }
        )

        # Test validation with infinite values
        inf_result = self.output_validator.validate_chart_requirements(extreme_data, "scatter")
        # Should still be valid for chart requirements, but warnings should be generated
        assert inf_result.is_valid is True

        # Test with empty DataFrame
        empty_data = pd.DataFrame()
        empty_result = self.output_validator.validate_chart_requirements(empty_data, "scatter")
        assert empty_result.is_valid is False

        # Test with single row
        single_row = pd.DataFrame({"x": [1], "y": [2]})
        single_result = self.output_validator.validate_chart_requirements(single_row, "scatter")
        assert single_result.is_valid is False  # Scatter needs at least 2 points

        # Test with mismatched schema
        mismatched_data = pd.DataFrame(
            {"expected_float": ["string", "values", "here"], "expected_int": [1.5, 2.7, 3.9]}
        )

        mismatched_schema = {"type": "dataframe", "columns": {"expected_float": "float64", "expected_int": "int64"}}

        mismatch_result = self.notebook_validator.validate_cell_output(mismatched_data, mismatched_schema)
        assert mismatch_result.is_valid is False

    def test_performance_with_large_datasets(self):
        """Test validation performance with large datasets."""
        # Create a large dataset
        n_rows = 50000
        large_data = pd.DataFrame(
            {
                "id": range(n_rows),
                "feature_1": np.random.randn(n_rows),
                "feature_2": np.random.randn(n_rows),
                "feature_3": np.random.randn(n_rows),
                "target": np.random.randint(0, 2, n_rows),
                "prediction": np.random.uniform(0, 1, n_rows),
                "confidence": np.random.uniform(0, 1, n_rows),
            }
        )

        # Time the validation
        import time

        start_time = time.time()

        # Validate large dataset
        large_schema = {
            "type": "dataframe",
            "columns": {
                "id": "int64",
                "feature_1": "float64",
                "feature_2": "float64",
                "feature_3": "float64",
                "target": "int64",
                "prediction": "float64",
                "confidence": "float64",
            },
            "min_rows": 1000,
        }

        result = self.notebook_validator.validate_cell_output(large_data, large_schema)

        # Validate prediction scores
        pred_result = self.output_validator.validate_score_range(large_data["prediction"], 0.0, 1.0)

        # Validate confidence scores
        conf_result = self.output_validator.validate_score_range(large_data["confidence"], 0.0, 1.0)

        end_time = time.time()
        validation_time = end_time - start_time

        # Assertions
        assert result.is_valid is True
        assert pred_result.is_valid is True
        assert conf_result.is_valid is True
        assert validation_time < 5.0  # Should complete within 5 seconds

        print(f"Validated {n_rows} rows in {validation_time:.2f} seconds")

    def test_multi_modal_data_validation(self):
        """Test validation of multi-modal data science outputs."""
        # Simulate multi-modal analysis results (text + image + audio)
        multimodal_results = pd.DataFrame(
            {
                "sample_id": range(100),
                "text_embedding_dim_1": np.random.randn(100),
                "text_embedding_dim_2": np.random.randn(100),
                "image_embedding_dim_1": np.random.randn(100),
                "image_embedding_dim_2": np.random.randn(100),
                "audio_embedding_dim_1": np.random.randn(100),
                "audio_embedding_dim_2": np.random.randn(100),
                "fusion_score": np.random.uniform(0, 1, 100),
                "text_confidence": np.random.uniform(0, 1, 100),
                "image_confidence": np.random.uniform(0, 1, 100),
                "audio_confidence": np.random.uniform(0, 1, 100),
                "overall_prediction": np.random.randint(0, 5, 100),
                "cross_modal_similarity": np.random.uniform(0, 1, 100),
            }
        )

        # Validate multi-modal schema
        multimodal_schema = {
            "type": "dataframe",
            "columns": {
                "sample_id": "int64",
                "text_embedding_dim_1": "float64",
                "text_embedding_dim_2": "float64",
                "image_embedding_dim_1": "float64",
                "image_embedding_dim_2": "float64",
                "audio_embedding_dim_1": "float64",
                "audio_embedding_dim_2": "float64",
                "fusion_score": "float64",
                "text_confidence": "float64",
                "image_confidence": "float64",
                "audio_confidence": "float64",
                "overall_prediction": "int64",
                "cross_modal_similarity": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(multimodal_results, multimodal_schema)
        assert result.is_valid is True

        # Validate confidence and similarity scores
        for conf_col in [
            "fusion_score",
            "text_confidence",
            "image_confidence",
            "audio_confidence",
            "cross_modal_similarity",
        ]:
            conf_result = self.output_validator.validate_score_range(multimodal_results[conf_col], 0.0, 1.0)
            assert conf_result.is_valid is True

    def test_reinforcement_learning_validation(self):
        """Test validation of reinforcement learning outputs."""
        # Simulate RL training results
        rl_results = pd.DataFrame(
            {
                "episode": range(1, 1001),
                "total_reward": np.random.normal(100, 50, 1000).cumsum(),
                "episode_length": np.random.randint(50, 500, 1000),
                "average_reward": np.random.normal(1, 0.5, 1000),
                "epsilon": np.exp(-np.linspace(0, 5, 1000)),  # Decaying exploration
                "learning_rate": [0.001 * (0.99 ** (i // 100)) for i in range(1000)],
                "q_value_mean": np.random.normal(10, 5, 1000),
                "q_value_std": np.random.uniform(1, 10, 1000),
                "policy_entropy": np.random.uniform(0, 2, 1000),
                "value_loss": np.random.exponential(1, 1000),
                "policy_loss": np.random.exponential(0.5, 1000),
            }
        )

        # Validate RL schema
        rl_schema = {
            "type": "dataframe",
            "columns": {
                "episode": "int64",
                "total_reward": "float64",
                "episode_length": "int64",
                "average_reward": "float64",
                "epsilon": "float64",
                "learning_rate": "float64",
                "q_value_mean": "float64",
                "q_value_std": "float64",
                "policy_entropy": "float64",
                "value_loss": "float64",
                "policy_loss": "float64",
            },
            "min_rows": 1,
        }

        result = self.notebook_validator.validate_cell_output(rl_results, rl_schema)
        assert result.is_valid is True

        # Validate epsilon values (exploration rate should be 0-1)
        epsilon_result = self.output_validator.validate_score_range(rl_results["epsilon"], 0.0, 1.0)
        assert epsilon_result.is_valid is True

        # Validate learning rates are positive
        lr_result = self.output_validator.validate_score_range(rl_results["learning_rate"], 0.0, 1.0)
        assert lr_result.is_valid is True

        # Validate episode lengths are positive
        length_result = self.output_validator.validate_score_range(rl_results["episode_length"], 1, float("inf"))
        assert length_result.is_valid is True
