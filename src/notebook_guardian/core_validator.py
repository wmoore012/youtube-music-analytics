"""
Core validation classes for Notebook Guardian.

This module provides the main validation interfaces that work with both
.py files and .ipynb notebooks.
"""

# Re-export from existing modules for compatibility
from ..data_organization.notebook_validator import (
    NotebookValidator,
    MetricExplainer,
    OutputValidator as DataValidator,
    ValidationResult,
    ValidationError
)

# Aliases for better API
CoreValidator = NotebookValidator
DataValidator = DataValidator  # Keep the alias

__all__ = [
    'DataValidator',
    'MetricExplainer',
    'NotebookValidator', 
    'CoreValidator',
    'ValidationResult',
    'ValidationError'
]