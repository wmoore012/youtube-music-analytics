"""
Main API for Notebook Guardian - The AI Agent's Best Friend.

Simple, fast functions for validating data science workflows.
"""

from typing import Union, Dict, Any, List, Optional
from pathlib import Path
import json

from .python_validator import PythonFileValidator, PythonValidationResult
from .core_validator import DataValidator, MetricExplainer, NotebookValidator
from .smart_installer import SmartInstaller, check_dependencies as _check_deps


def validate_data(data, schema=None):
    """
    Validate data with smart defaults.
    
    Args:
        data: DataFrame or other data to validate
        schema: Validation schema or 'auto' for smart detection
        
    Returns:
        ValidationResult
    """
    validator = DataValidator()
    
    if schema == 'auto' or schema is None:
        # Smart schema detection based on data
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            schema = {
                'type': 'dataframe',
                'columns': {col: str(data[col].dtype) for col in data.columns},
                'min_rows': 1
            }
    
    return validator.validate_cell_output(data, schema)


def validate_notebook(notebook_path: str):
    """
    Validate a Jupyter notebook file.
    
    Args:
        notebook_path: Path to .ipynb file
        
    Returns:
        ValidationResult
    """
    validator = NotebookValidator()
    return validator.create_validation_report(notebook_path)


def validate_python_file(file_path: str):
    """
    Validate a Python file.
    
    Args:
        file_path: Path to .py file
        
    Returns:
        PythonValidationResult
    """
    validator = PythonFileValidator()
    return validator.validate_file(file_path)


def explain_metrics(metrics: List[str]) -> Dict[str, str]:
    """
    Generate explanations for data science metrics.
    
    Args:
        metrics: List of metric names
        
    Returns:
        Dictionary mapping metrics to explanations
    """
    explainer = MetricExplainer()
    return explainer.create_legend_definitions(metrics)


def create_tooltips(metrics_and_values: Dict[str, float]) -> Dict[str, str]:
    """
    Create tooltips for chart elements.
    
    Args:
        metrics_and_values: Dictionary mapping metric names to values
        
    Returns:
        Dictionary mapping metrics to tooltip text
    """
    explainer = MetricExplainer()
    tooltips = {}
    
    for metric, value in metrics_and_values.items():
        tooltips[metric] = explainer.generate_tooltip_text(metric, value)
    
    return tooltips


def check_dependencies(file_or_code: str) -> List[str]:
    """
    Check what dependencies are required.
    
    Args:
        file_or_code: File path or source code string
        
    Returns:
        List of required package names
    """
    deps = _check_deps(file_or_code)
    return list(deps)


def auto_install_file(file_path: str):
    """
    Automatically install missing dependencies from a file.
    
    ⚠️ SECURITY WARNING: This function will automatically install packages
    without user confirmation. Only use with trusted code. Review dependencies
    with check_dependencies() first in production environments.
    
    Args:
        file_path: Path to .py or .ipynb file
        
    Returns:
        InstallationResult
        
    Security Notes:
        - Installs packages without confirmation
        - Can modify your Python environment
        - May install malicious packages from untrusted code
        - Use check_dependencies() first to review what will be installed
    """
    installer = SmartInstaller(auto_install=True)
    return installer.process_file(file_path)