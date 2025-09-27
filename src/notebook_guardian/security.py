"""
Security configuration and warnings for Notebook Guardian.

This module provides security controls and warnings for safe usage
of automatic package installation features.
"""

from dataclasses import dataclass
import os
from typing import List, Optional, Set
import warnings


@dataclass
class SecurityConfig:
    """
    Security configuration for Notebook Guardian.

    Controls automatic installation behavior and security warnings.
    """

    # Installation controls
    auto_install_enabled: bool = False  # Default to safe mode
    require_confirmation: bool = True
    allow_untrusted_packages: bool = False

    # Package filtering
    blocked_packages: Set[str] = None
    allowed_packages: Set[str] = None

    # Warning controls
    show_security_warnings: bool = True
    strict_mode: bool = False  # Fail on any security concern

    def __post_init__(self):
        if self.blocked_packages is None:
            self.blocked_packages = set()
        if self.allowed_packages is None:
            self.allowed_packages = set()


# Global security configuration
_security_config = SecurityConfig()


def get_security_config() -> SecurityConfig:
    """Get current security configuration."""
    return _security_config


def set_security_config(config: SecurityConfig) -> None:
    """Set security configuration."""
    global _security_config
    _security_config = config


def enable_safe_mode() -> None:
    """
    Enable safe mode - disables automatic installation.

    Recommended for production environments and untrusted code.
    """
    global _security_config
    _security_config.auto_install_enabled = False
    _security_config.require_confirmation = True
    _security_config.strict_mode = True
    print("🔒 Safe mode enabled - automatic installation disabled")


def enable_auto_install_mode() -> None:
    """
    Enable automatic installation mode.

    ⚠️ WARNING: Only use with trusted code and in isolated environments.
    """
    global _security_config
    if _security_config.show_security_warnings:
        warnings.warn(
            "⚠️ SECURITY WARNING: Automatic installation enabled. "
            "Only use with trusted code. Packages will be installed without confirmation.",
            UserWarning,
            stacklevel=2,
        )

    _security_config.auto_install_enabled = True
    _security_config.require_confirmation = False


def check_package_safety(package_name: str) -> bool:
    """
    Check if a package is safe to install based on security configuration.

    Args:
        package_name: Name of package to check

    Returns:
        True if package is considered safe to install

    Raises:
        SecurityError: If package is blocked or unsafe in strict mode
    """
    config = get_security_config()

    # Check blocked packages
    if package_name in config.blocked_packages:
        if config.strict_mode:
            raise SecurityError(f"Package '{package_name}' is blocked by security policy")
        return False

    # Check allowed packages (if allowlist is configured)
    if config.allowed_packages and package_name not in config.allowed_packages:
        if config.strict_mode:
            raise SecurityError(f"Package '{package_name}' not in allowed packages list")
        return False

    # Check for common typosquatting patterns
    if _is_potential_typosquat(package_name):
        if config.show_security_warnings:
            warnings.warn(
                f"⚠️ SECURITY WARNING: Package '{package_name}' may be typosquatting. "
                "Verify package name is correct.",
                UserWarning,
            )
        if config.strict_mode:
            raise SecurityError(f"Package '{package_name}' flagged as potential typosquat")
        return False

    return True


def _is_potential_typosquat(package_name: str) -> bool:
    """
    Check if package name might be typosquatting a popular package.

    This is a basic heuristic check - not comprehensive security.
    """
    # Common legitimate packages
    legitimate_packages = {
        "pandas",
        "numpy",
        "matplotlib",
        "seaborn",
        "plotly",
        "scikit - learn",
        "sklearn",
        "tensorflow",
        "torch",
        "pytorch",
        "scipy",
        "statsmodels",
        "xgboost",
        "lightgbm",
        "catboost",
        "jupyter",
        "ipython",
        "notebook",
        "jupyterlab",
    }

    # Check for suspicious variations
    package_lower = package_name.lower()

    for legit in legitimate_packages:
        # Check for common typosquatting patterns
        if package_lower != legit and (
            package_lower.replace("-", "") == legit.replace("-", "")
            or package_lower.replace("_", "") == legit.replace("_", "")
            or _levenshtein_distance(package_lower, legit) == 1
        ):
            return True

    return False


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


class SecurityError(Exception):
    """Raised when security policy is violated."""

    pass


def warn_about_ai_generated_code() -> None:
    """Show warning about AI - generated code risks."""
    if get_security_config().show_security_warnings:
        warnings.warn(
            "⚠️ SECURITY WARNING: Be cautious with AI - generated code. "
            "AI models may suggest malicious or incorrect package names. "
            "Always verify dependencies before installation.",
            UserWarning,
        )


def create_security_report(packages: List[str]) -> dict:
    """
    Create a security report for a list of packages.

    Args:
        packages: List of package names to analyze

    Returns:
        Dictionary with security analysis results
    """
    report = {
        "total_packages": len(packages),
        "safe_packages": [],
        "suspicious_packages": [],
        "blocked_packages": [],
        "warnings": [],
    }

    config = get_security_config()

    for package in packages:
        try:
            if check_package_safety(package):
                report["safe_packages"].append(package)
            else:
                report["suspicious_packages"].append(package)
        except SecurityError:
            report["blocked_packages"].append(package)

    # Add general warnings
    if report["suspicious_packages"]:
        report["warnings"].append(f"{len(report['suspicious_packages'])} packages flagged as suspicious")

    if report["blocked_packages"]:
        report["warnings"].append(f"{len(report['blocked_packages'])} packages blocked by security policy")

    return report


# Environment - based security defaults
def _load_security_from_environment():
    """Load security configuration from environment variables."""
    global _security_config

    # Check for safe mode environment variable
    if os.getenv("NOTEBOOK_GUARDIAN_SAFE_MODE", "").lower() in ("true", "1", "yes"):
        enable_safe_mode()

    # Check for disabled warnings
    if os.getenv("NOTEBOOK_GUARDIAN_NO_WARNINGS", "").lower() in ("true", "1", "yes"):
        _security_config.show_security_warnings = False


# Load configuration on import
_load_security_from_environment()
