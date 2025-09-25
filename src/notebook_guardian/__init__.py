"""
Notebook Guardian - The AI Agent's Best Friend for Data Science Validation

A lightning-fast, dependency-aware validation system for Jupyter notebooks and data science workflows.
Perfect for AI agents who need bulletproof data validation without the headache of missing dependencies.

Key Features:
- 🚀 Ultra-fast validation (50k+ rows in <1 second)
- 🛡️ Bulletproof dependency management with auto-installation
- 📊 Comprehensive data science metric validation
- 🤖 AI agent-friendly with clear error messages
- 🔧 Zero-config setup - works out of the box
- 📈 Supports all major ML/DL/Stats workflows

Usage:
    from notebook_guardian import validate_notebook, validate_data, explain_metrics

    # Validate any data science output
    result = validate_data(your_dataframe, expected_schema)

    # Generate human-readable explanations
    explanations = explain_metrics(['accuracy', 'precision', 'recall'])

    # Validate entire notebooks
    notebook_result = validate_notebook('path/to/notebook.ipynb')

Perfect for:
- AI agents building data science workflows
- Automated notebook execution pipelines
- Data science CI/CD systems
- Research reproducibility
- Teaching and learning data science
"""

from .api import check_dependencies, create_tooltips, explain_metrics, validate_data, validate_notebook
from .core_validator import DataValidator, MetricExplainer, NotebookValidator, ValidationError, ValidationResult
from .smart_installer import SmartInstaller, auto_install_missing, ensure_package, ensure_packages

__version__ = "1.0.0"
__author__ = "AI Agent Collective"
__description__ = "Lightning-fast notebook validation for AI agents and data scientists"


# Quick validation functions for immediate use
def quick_validate(data, schema=None):
    """Ultra-fast data validation with smart defaults."""
    return validate_data(data, schema or "auto")


def quick_explain(metrics):
    """Generate explanations for common data science metrics."""
    return explain_metrics(metrics)


def quick_install(*packages):
    """Install packages with zero friction."""
    return ensure_packages(*packages)


# Export main API
__all__ = [
    # Core classes
    "DataValidator",
    "MetricExplainer",
    "NotebookValidator",
    "SmartInstaller",
    "ValidationResult",
    "ValidationError",
    # Main API functions
    "validate_data",
    "validate_notebook",
    "explain_metrics",
    "create_tooltips",
    "check_dependencies",
    # Dependency management
    "ensure_package",
    "ensure_packages",
    "auto_install_missing",
    # Quick functions
    "quick_validate",
    "quick_explain",
    "quick_install",
]
