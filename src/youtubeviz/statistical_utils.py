"""
Statistical utilities for data - science grade charts with uncertainty handling.
Implements Wilson confidence intervals, Bayesian shrinkage, and LOESS smoothing.
"""

import os
from typing import Any, Dict, Optional, Tuple
import warnings

import numpy as np
import pandas as pd

# Auto - install scipy if needed
try:
    from .auto_install import ensure

    scipy_module = ensure("scipy")

    if scipy_module:
        from scipy import stats

        HAS_SCIPY = True
    else:
        raise ImportError("scipy not available")

except ImportError:
    HAS_SCIPY = False
    warnings.warn(
        "scipy not available. Statistical functions will use simplified approximations. "
        "Install scipy for full statistical functionality: pip install scipy",
        UserWarning,
    )

    # Create a mock stats module for fallback
    class MockStats:
        @staticmethod
        def norm():
            class MockNorm:
                @staticmethod
                def ppf(x):
                    # Simple approximation for normal distribution percentile point function
                    if x <= 0.5:
                        return -1.96 if x <= 0.025 else (-0.67 if x <= 0.16 else 0)
                    else:
                        return 1.96 if x >= 0.975 else (0.67 if x >= 0.84 else 0)

            return MockNorm()

    stats = MockStats()


def calculate_wilson_intervals(
    successes: np.ndarray, totals: np.ndarray, confidence: float = 0.95
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate Wilson confidence intervals for proportions.

    Wilson intervals are more accurate than normal approximation for small samples
    and proportions near 0 or 1. Essential for new artists with limited data.

    Args:
        successes: Array of success counts (e.g., positive comments)
        totals: Array of total counts (e.g., total comments)
        confidence: Confidence level (default 0.95 for 95% CI)

    Returns:
        Tuple of (lower_bounds, upper_bounds) arrays

    Example:
        >>> successes = np.array([8, 15, 3])
        >>> totals = np.array([10, 20, 5])
        >>> lower, upper = calculate_wilson_intervals(successes, totals)
        >>> # Returns confidence intervals for proportions 0.8, 0.75, 0.6
    """
    # Convert to numpy arrays
    successes = np.asarray(successes, dtype=float)
    totals = np.asarray(totals, dtype=float)

    # Handle edge cases
    if successes.shape != totals.shape:
        raise ValueError("successes and totals must have same shape")

    # Avoid division by zero
    totals = np.where(totals == 0, 1, totals)

    # Calculate proportions
    p = successes / totals

    if not HAS_SCIPY:
        # Fallback to simple normal approximation when scipy not available
        z_score = 1.96  # Approximate 95% confidence
        std_error = np.sqrt(p * (1 - p) / totals)
        margin = z_score * std_error

        lower = np.maximum(0, p - margin)
        upper = np.minimum(1, p + margin)

        return lower, upper

    # Z - score for confidence level
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    z_squared = z**2

    # Wilson interval formula
    denominator = 1 + z_squared / totals
    center = (p + z_squared / (2 * totals)) / denominator
    margin = z * np.sqrt((p * (1 - p) + z_squared / (4 * totals)) / totals) / denominator

    lower = center - margin
    upper = center + margin

    # Ensure bounds are in [0, 1]
    lower = np.clip(lower, 0, 1)
    upper = np.clip(upper, 0, 1)

    return lower, upper


def apply_bayesian_shrinkage(
    observed_rates: pd.Series,
    totals: pd.Series,
    prior_alpha: Optional[float] = None,
    prior_beta: Optional[float] = None,
    min_observations: int = 5,
) -> pd.Series:
    """
    Apply Bayesian shrinkage (empirical Bayes) to stabilize rates for small samples.

    Uses beta - binomial conjugate prior to shrink individual artist rates toward
    the overall roster mean. Critical for new artists with few comments / videos.

    Args:
        observed_rates: Series of observed rates (e.g., positive sentiment rate per artist)
        totals: Series of total observations per artist
        prior_alpha: Prior alpha parameter (if None, estimated from data)
        prior_beta: Prior beta parameter (if None, estimated from data)
        min_observations: Minimum observations to apply shrinkage

    Returns:
        Series of shrunken rates

    Example:
        >>> rates = pd.Series([0.9, 0.1, 0.8], index=['Artist A', 'Artist B', 'Artist C'])
        >>> totals = pd.Series([5, 3, 20], index=['Artist A', 'Artist B', 'Artist C'])
        >>> shrunken = apply_bayesian_shrinkage(rates, totals)
        >>> # Artist B's extreme 0.1 rate gets pulled toward roster mean
    """
    if len(observed_rates) != len(totals):
        raise ValueError("observed_rates and totals must have same length")

    # Calculate overall success counts
    successes = observed_rates * totals
    total_successes = successes.sum()
    total_observations = totals.sum()

    if total_observations == 0:
        return observed_rates.copy()

    # Estimate prior parameters if not provided
    if prior_alpha is None or prior_beta is None:
        overall_rate = total_successes / total_observations

        # Method of moments estimation for beta distribution
        # Assume prior equivalent to 10 observations at overall rate
        prior_strength = 10
        prior_alpha = overall_rate * prior_strength
        prior_beta = (1 - overall_rate) * prior_strength

    # Apply Bayesian updating: posterior = prior + observed
    posterior_alpha = prior_alpha + successes
    posterior_beta = prior_beta + (totals - successes)

    # Posterior mean (shrunken rate)
    shrunken_rates = posterior_alpha / (posterior_alpha + posterior_beta)

    # Only apply shrinkage to artists with few observations
    mask = totals >= min_observations
    result = observed_rates.copy()
    result[~mask] = shrunken_rates[~mask]

    return result


def apply_loess_smoothing(
    x: np.ndarray,
    y: np.ndarray,
    frac: float = 0.3,
    it: int = 3,
    return_confidence_bands: bool = True,
    confidence: float = 0.95,
) -> Dict[str, np.ndarray]:
    """
    Apply LOWESS (locally weighted scatterplot smoothing) with confidence bands.

    LOWESS is robust to outliers and provides smooth trend lines for time series
    and scatter plots. Essential for standout video analysis and trend detection.

    Args:
        x: Independent variable (e.g., log views, time)
        y: Dependent variable (e.g., positive rate, engagement)
        frac: Fraction of data used for each local regression (0.2 - 0.8)
        it: Number of robustifying iterations
        return_confidence_bands: Whether to calculate confidence bands
        confidence: Confidence level for bands

    Returns:
        Dictionary with 'x_smooth', 'y_smooth', and optionally 'lower', 'upper'

    Example:
        >>> x = np.log10(np.array([100, 500, 1000, 5000, 10000]))
        >>> y = np.array([0.6, 0.7, 0.65, 0.8, 0.75])
        >>> result = apply_loess_smoothing(x, y)
        >>> # Returns smooth trend line with confidence bands
    """
    try:
        from statsmodels.nonparametric.smoothers_lowess import lowess
    except ImportError:
        warnings.warn("statsmodels not available, using simple moving average")
        return _simple_smoothing(x, y, return_confidence_bands, confidence)

    # Sort by x values
    sort_idx = np.argsort(x)
    x_sorted = x[sort_idx]
    y_sorted = y[sort_idx]

    # Apply LOWESS
    smoothed = lowess(y_sorted, x_sorted, frac=frac, it=it, return_sorted=True)
    x_smooth = smoothed[:, 0]
    y_smooth = smoothed[:, 1]

    result = {"x_smooth": x_smooth, "y_smooth": y_smooth}

    if return_confidence_bands:
        # Calculate residuals for confidence bands
        y_interp = np.interp(x_sorted, x_smooth, y_smooth)
        residuals = y_sorted - y_interp

        # Estimate standard error (simplified approach)
        residual_std = np.std(residuals)
        z_score = stats.norm.ppf(1 - (1 - confidence) / 2)
        margin = z_score * residual_std

        result["lower"] = y_smooth - margin
        result["upper"] = y_smooth + margin

    return result


def _simple_smoothing(
    x: np.ndarray, y: np.ndarray, return_confidence_bands: bool, confidence: float
) -> Dict[str, np.ndarray]:
    """Fallback smoothing when statsmodels is not available."""
    # Simple moving average as fallback
    window_size = max(3, len(x) // 5)

    # Sort by x
    sort_idx = np.argsort(x)
    x_sorted = x[sort_idx]
    y_sorted = y[sort_idx]

    # Apply moving average
    y_smooth = pd.Series(y_sorted).rolling(window=window_size, center=True, min_periods=1).mean().values

    result = {"x_smooth": x_sorted, "y_smooth": y_smooth}

    if return_confidence_bands:
        # Simple confidence bands based on standard deviation
        std_dev = np.std(y_sorted)
        z_score = stats.norm.ppf(1 - (1 - confidence) / 2)
        margin = z_score * std_dev

        result["lower"] = y_smooth - margin
        result["upper"] = y_smooth + margin

    return result


def detect_needs_more_data(totals: np.ndarray, min_threshold: int = 20) -> np.ndarray:
    """
    Detect which data points need more observations for reliable analysis.

    Args:
        totals: Array of total observation counts
        min_threshold: Minimum observations for reliable analysis

    Returns:
        Boolean array indicating which points need more data
    """
    return totals < min_threshold


def calculate_residuals(observed: np.ndarray, predicted: np.ndarray) -> np.ndarray:
    """
    Calculate residuals for identifying standout performers.

    Args:
        observed: Observed values (e.g., actual positive rates)
        predicted: Predicted values (e.g., from LOESS trend)

    Returns:
        Array of residuals (observed - predicted)
    """
    return np.asarray(observed) - np.asarray(predicted)


def standardize_residuals(residuals: np.ndarray) -> np.ndarray:
    """
    Standardize residuals to identify outliers.

    Args:
        residuals: Array of residuals

    Returns:
        Standardized residuals (z - scores)
    """
    residuals = np.asarray(residuals)
    return (residuals - np.mean(residuals)) / np.std(residuals)
