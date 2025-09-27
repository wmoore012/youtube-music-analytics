"""
Smart dependency installer for AI agents and data scientists.

Ultra - fast, intelligent package installation with zero friction.
Perfect for AI agents who forget to install dependencies.

Key Features:
- 🚀 Lightning - fast dependency detection (regex + AST)
- 🧠 Smart package mapping (pd -> pandas, sklearn -> scikit - learn)
- 🔧 Parallel installation for maximum speed
- 🛡️ Bulletproof error handling and recovery
- 📦 Works with .py files, .ipynb notebooks, and code strings
- 🎯 Zero - config setup - just works out of the box
"""

import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import importlib
import json
import logging
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class InstallationResult:
    """
    Result of package installation with detailed tracking.

    Optimized for speed and comprehensive error reporting.
    """

    is_valid: bool
    installed_packages: List[str] = field(default_factory=list)
    failed_packages: List[str] = field(default_factory=list)
    already_installed: List[str] = field(default_factory=list)

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    installation_time: float = 0.0
    total_packages: int = 0

    def add_success(self, package: str) -> None:
        """Mark package as successfully installed."""
        self.installed_packages.append(package)

    def add_failure(self, package: str, error: str) -> None:
        """Mark package as failed to install."""
        self.failed_packages.append(package)
        self.errors.append(f"Failed to install {package}: {error}")
        self.is_valid = False

    def add_already_installed(self, package: str) -> None:
        """Mark package as already installed."""
        self.already_installed.append(package)

    def merge(self, other: "InstallationResult") -> "InstallationResult":
        """Merge two installation results."""
        return InstallationResult(
            is_valid=self.is_valid and other.is_valid,
            installed_packages=self.installed_packages + other.installed_packages,
            failed_packages=self.failed_packages + other.failed_packages,
            already_installed=self.already_installed + other.already_installed,
            errors=self.errors + other.errors,
            warnings=self.warnings + other.warnings,
            installation_time=self.installation_time + other.installation_time,
            total_packages=self.total_packages + other.total_packages,
        )


class FastDependencyDetector:
    """
    Ultra - fast dependency detection using optimized algorithms.

    Combines regex, AST analysis, and string matching for maximum speed.
    """

    def __init__(self):
        # Pre - compiled regex patterns for lightning - fast detection
        self._import_patterns = {
            "pandas": [
                re.compile(r"\bpd\.", re.MULTILINE),
                re.compile(r"import pandas", re.MULTILINE),
                re.compile(r"from pandas", re.MULTILINE),
            ],
            "numpy": [
                re.compile(r"\bnp\.", re.MULTILINE),
                re.compile(r"import numpy", re.MULTILINE),
                re.compile(r"from numpy", re.MULTILINE),
            ],
            "matplotlib": [
                re.compile(r"\bplt\.", re.MULTILINE),
                re.compile(r"import matplotlib", re.MULTILINE),
                re.compile(r"from matplotlib", re.MULTILINE),
            ],
            "seaborn": [
                re.compile(r"\bsns\.", re.MULTILINE),
                re.compile(r"import seaborn", re.MULTILINE),
                re.compile(r"from seaborn", re.MULTILINE),
            ],
            "scikit - learn": [
                re.compile(r"from sklearn", re.MULTILINE),
                re.compile(r"import sklearn", re.MULTILINE),
                re.compile(r"RandomForestClassifier|LogisticRegression|SVC|LinearRegression", re.MULTILINE),
            ],
            "tensorflow": [
                re.compile(r"\btf\.", re.MULTILINE),
                re.compile(r"import tensorflow", re.MULTILINE),
                re.compile(r"from tensorflow", re.MULTILINE),
            ],
            "torch": [
                re.compile(r"import torch", re.MULTILINE),
                re.compile(r"from torch", re.MULTILINE),
                re.compile(r"torch\.", re.MULTILINE),
            ],
            "plotly": [
                re.compile(r"import plotly", re.MULTILINE),
                re.compile(r"from plotly", re.MULTILINE),
                re.compile(r"plotly\.", re.MULTILINE),
            ],
            "scipy": [
                re.compile(r"import scipy", re.MULTILINE),
                re.compile(r"from scipy", re.MULTILINE),
                re.compile(r"scipy\.", re.MULTILINE),
            ],
            "xgboost": [
                re.compile(r"import xgboost", re.MULTILINE),
                re.compile(r"from xgboost", re.MULTILINE),
                re.compile(r"XGBClassifier|XGBRegressor", re.MULTILINE),
            ],
            "lightgbm": [
                re.compile(r"import lightgbm", re.MULTILINE),
                re.compile(r"from lightgbm", re.MULTILINE),
                re.compile(r"LGBMClassifier|LGBMRegressor", re.MULTILINE),
            ],
            "catboost": [
                re.compile(r"import catboost", re.MULTILINE),
                re.compile(r"from catboost", re.MULTILINE),
                re.compile(r"CatBoostClassifier|CatBoostRegressor", re.MULTILINE),
            ],
            "joblib": [re.compile(r"import joblib", re.MULTILINE), re.compile(r"joblib\.", re.MULTILINE)],
            "pickle": [re.compile(r"import pickle", re.MULTILINE), re.compile(r"pickle\.", re.MULTILINE)],
            "statsmodels": [
                re.compile(r"import statsmodels", re.MULTILINE),
                re.compile(r"from statsmodels", re.MULTILINE),
            ],
        }

        # Common alias mappings for ultra - fast lookup
        self._alias_mapping = {
            "pd": "pandas",
            "np": "numpy",
            "plt": "matplotlib",
            "sns": "seaborn",
            "tf": "tensorflow",
            "sk": "scikit - learn",
            "sklearn": "scikit - learn",
        }

        # Cache for performance
        self._detection_cache = {}
        self._max_cache_size = 1000

    def detect_dependencies(self, code: str, file_type: str = "python") -> Set[str]:
        """
        Ultra - fast dependency detection using multiple strategies.

        Args:
            code: Source code to analyze
            file_type: 'python' or 'notebook'

        Returns:
            Set of required package names
        """
        # Check cache first
        code_hash = hash(code)
        if code_hash in self._detection_cache:
            return self._detection_cache[code_hash]

        dependencies = set()

        # Strategy 1: Fast regex - based detection (fastest)
        dependencies.update(self._regex_detection(code))

        # Strategy 2: AST - based detection (more accurate)
        try:
            dependencies.update(self._ast_detection(code))
        except SyntaxError:
            # Fallback to regex - only for syntax errors
            pass

        # Strategy 3: Notebook - specific detection
        if file_type == "notebook":
            dependencies.update(self._notebook_detection(code))

        # Cache result if cache not full
        if len(self._detection_cache) < self._max_cache_size:
            self._detection_cache[code_hash] = dependencies

        return dependencies

    def _regex_detection(self, code: str) -> Set[str]:
        """Lightning - fast regex - based dependency detection."""
        found_packages = set()

        for package, patterns in self._import_patterns.items():
            for pattern in patterns:
                if pattern.search(code):
                    found_packages.add(package)
                    break  # Found one pattern, no need to check others

        return found_packages

    def _ast_detection(self, code: str) -> Set[str]:
        """AST - based detection for more accurate results."""
        found_packages = set()

        try:
            tree = ast.parse(code)

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        package = self._normalize_package_name(alias.name)
                        if package:
                            found_packages.add(package)

                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        package = self._normalize_package_name(node.module)
                        if package:
                            found_packages.add(package)

        except SyntaxError:
            # Return empty set, regex detection will handle it
            pass

        return found_packages

    def _notebook_detection(self, notebook_content: str) -> Set[str]:
        """Detect dependencies in Jupyter notebook JSON."""
        found_packages = set()

        try:
            if notebook_content.strip().startswith("{"):
                # Parse as JSON notebook
                notebook = json.loads(notebook_content)

                for cell in notebook.get("cells", []):
                    if cell.get("cell_type") == "code":
                        source = cell.get("source", [])
                        if isinstance(source, list):
                            code = "".join(source)
                        else:
                            code = source

                        # Detect dependencies in each code cell
                        found_packages.update(self._regex_detection(code))

                        try:
                            found_packages.update(self._ast_detection(code))
                        except Exception as e:
                            logging.warning(f"Operation failed: {e}")
                            pass

        except (json.JSONDecodeError, KeyError):
            # Not a valid notebook, treat as regular code
            pass

        return found_packages

    def _normalize_package_name(self, import_name: str) -> Optional[str]:
        """Normalize import names to actual package names."""
        # Handle submodules (e.g., sklearn.ensemble -> scikit - learn)
        if import_name.startswith("sklearn"):
            return "scikit - learn"
        elif import_name.startswith("tensorflow"):
            return "tensorflow"
        elif import_name.startswith("torch"):
            return "torch"
        elif import_name in self._alias_mapping:
            return self._alias_mapping[import_name]
        elif import_name in self._import_patterns:
            return import_name

        return None


class ParallelInstaller:
    """
    High - speed parallel package installer with intelligent error handling.

    Uses thread pools and optimized subprocess calls for maximum speed.
    """

    def __init__(self, max_workers: int = 3, timeout: int = 120):
        self.max_workers = max_workers
        self.timeout = timeout
        self._installation_cache = {}

    def install_packages(self, packages: List[str]) -> InstallationResult:
        """
        Install packages in parallel with comprehensive error handling.

        Args:
            packages: List of package names to install

        Returns:
            InstallationResult with detailed installation information
        """
        start_time = time.time()

        result = InstallationResult(is_valid=True, total_packages=len(packages))

        if not packages:
            result.installation_time = time.time() - start_time
            return result

        # Check which packages are already installed
        already_installed = self._check_installed_packages(packages)
        remaining_packages = [pkg for pkg in packages if pkg not in already_installed]

        for pkg in already_installed:
            result.add_already_installed(pkg)

        if not remaining_packages:
            result.installation_time = time.time() - start_time
            return result

        # Install remaining packages in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._install_single_package, pkg): pkg for pkg in remaining_packages}

            for future in as_completed(futures):
                package = futures[future]
                try:
                    success, error_msg = future.result()
                    if success:
                        result.add_success(package)
                    else:
                        result.add_failure(package, error_msg)
                except Exception as e:
                    result.add_failure(package, str(e))

        result.installation_time = time.time() - start_time
        return result

    def _check_installed_packages(self, packages: List[str]) -> List[str]:
        """Quickly check which packages are already installed."""
        installed = []

        for package in packages:
            try:
                # Try to import the package
                if package == "scikit - learn":
                    importlib.import_module("sklearn")
                else:
                    importlib.import_module(package.replace("-", "_"))
                installed.append(package)
            except ImportError:
                pass

        return installed

    def _install_single_package(self, package: str) -> Tuple[bool, Optional[str]]:
        """
        Install a single package with optimized subprocess call.

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Use optimized pip command
            cmd = [sys.executable, "-m", "pip", "install", package, "--quiet", "--no - warn - script - location"]

            _result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout, check=True)

            return True, None

        except subprocess.TimeoutExpired:
            return False, f"Installation timeout after {self.timeout}s"

        except subprocess.CalledProcessError as e:
            return False, f"Installation failed: {e.stderr}"

        except Exception as e:
            return False, f"Unexpected error: {str(e)}"


class SmartInstaller:
    """
    Main smart installer class combining detection and installation.

    ⚠️ SECURITY WARNING: This class can automatically install packages
    without user confirmation when auto_install=True. Use with caution,
    especially with AI - generated or untrusted code.

    Security Best Practices:
    - Set auto_install=False in production environments
    - Always review dependencies before installation
    - Use virtual environments to isolate installations
    - Audit AI - generated code for malicious dependencies
    """

    def __init__(self, auto_install: bool = True, max_workers: int = 3):
        self.auto_install = auto_install
        self.detector = FastDependencyDetector()
        self.installer = ParallelInstaller(max_workers=max_workers)

        # Performance tracking
        self._stats = {"files_processed": 0, "packages_installed": 0, "total_time": 0.0}

    def process_file(self, file_path: str) -> InstallationResult:
        """
        Process a single file (.py or .ipynb) and install missing dependencies.

        Args:
            file_path: Path to Python file or Jupyter notebook

        Returns:
            InstallationResult with installation details
        """
        start_time = time.time()

        try:
            path_obj = Path(file_path)

            if not path_obj.exists():
                result = InstallationResult(is_valid=False)
                result.errors.append(f"File not found: {file_path}")
                return result

            # Read file content
            with open(file_path, "r", encoding="utf - 8", errors="ignore") as f:
                content = f.read()

            # Detect file type
            file_type = "notebook" if file_path.endswith(".ipynb") else "python"

            # Detect dependencies
            dependencies = self.detector.detect_dependencies(content, file_type)

            # Install if auto_install is enabled
            if self.auto_install and dependencies:
                result = self.installer.install_packages(list(dependencies))
            else:
                result = InstallationResult(is_valid=True)
                result.warnings.append(f"Found {len(dependencies)} dependencies but auto_install is disabled")

            # Update stats
            self._stats["files_processed"] += 1
            self._stats["packages_installed"] += len(result.installed_packages)
            self._stats["total_time"] += time.time() - start_time

            return result

        except Exception as e:
            result = InstallationResult(is_valid=False)
            result.errors.append(f"Error processing file: {str(e)}")
            return result

    def process_code_string(self, code: str, file_type: str = "python") -> InstallationResult:
        """
        Process code string and install missing dependencies.

        Args:
            code: Source code string
            file_type: 'python' or 'notebook'

        Returns:
            InstallationResult with installation details
        """
        start_time = time.time()

        try:
            # Detect dependencies
            dependencies = self.detector.detect_dependencies(code, file_type)

            # Install if auto_install is enabled
            if self.auto_install and dependencies:
                result = self.installer.install_packages(list(dependencies))
            else:
                result = InstallationResult(is_valid=True)
                if dependencies:
                    result.warnings.append(f"Found {len(dependencies)} dependencies but auto_install is disabled")

            # Update stats
            self._stats["packages_installed"] += len(result.installed_packages)
            self._stats["total_time"] += time.time() - start_time

            return result

        except Exception as e:
            result = InstallationResult(is_valid=False)
            result.errors.append(f"Error processing code: {str(e)}")
            return result

    def process_multiple_files(self, file_paths: List[str]) -> List[InstallationResult]:
        """
        Process multiple files in parallel for maximum speed.

        Args:
            file_paths: List of file paths to process

        Returns:
            List of InstallationResult objects
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.installer.max_workers) as executor:
            futures = {executor.submit(self.process_file, path): path for path in file_paths}

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    error_result = InstallationResult(is_valid=False)
                    error_result.errors.append(f"Failed to process {futures[future]}: {str(e)}")
                    results.append(error_result)

        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset performance statistics."""
        self._stats = {"files_processed": 0, "packages_installed": 0, "total_time": 0.0}


# Convenience functions for easy use
def ensure_package(package_name: str) -> bool:
    """
    Ensure a single package is installed.

    ⚠️ SECURITY WARNING: This function will install packages automatically.
    Only use with trusted package names. Verify package legitimacy first.

    Args:
        package_name: Name of package to install

    Returns:
        True if package is available after installation

    Security Notes:
        - Automatically installs without confirmation
        - Verify package name is not typosquatting
        - Use in isolated environments for untrusted packages
    """
    installer = SmartInstaller()
    result = installer.installer.install_packages([package_name])
    return result.is_valid and (package_name in result.installed_packages or package_name in result.already_installed)


def ensure_packages(*package_names: str) -> Dict[str, bool]:
    """
    Ensure multiple packages are installed.

    Args:
        package_names: Names of packages to install

    Returns:
        Dictionary mapping package names to installation success
    """
    installer = SmartInstaller()
    result = installer.installer.install_packages(list(package_names))

    status = {}
    for pkg in package_names:
        status[pkg] = pkg in result.installed_packages or pkg in result.already_installed

    return status


def auto_install_missing(file_path: str) -> InstallationResult:
    """
    Automatically detect and install missing dependencies from a file.

    Args:
        file_path: Path to Python file or Jupyter notebook

    Returns:
        InstallationResult with installation details
    """
    installer = SmartInstaller(auto_install=True)
    return installer.process_file(file_path)


def check_dependencies(code_or_file: str) -> Set[str]:
    """
    Check what dependencies are required without installing.

    Args:
        code_or_file: Either source code string or file path

    Returns:
        Set of required package names
    """
    detector = FastDependencyDetector()

    if Path(code_or_file).exists():
        # It's a file path
        with open(code_or_file, "r", encoding="utf - 8", errors="ignore") as f:
            content = f.read()
        file_type = "notebook" if code_or_file.endswith(".ipynb") else "python"
    else:
        # It's source code
        content = code_or_file
        file_type = "python"

    return detector.detect_dependencies(content, file_type)
