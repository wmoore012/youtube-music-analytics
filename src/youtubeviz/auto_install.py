"""
Automatic package installation utilities for YouTube analytics.

This module provides a general-purpose AutoInstaller class that can
automatically install any missing Python package on demand.
"""

import importlib
import subprocess
import sys
from typing import Dict, List, Optional, Union
import warnings


class AutoInstaller:
    """
    General-purpose automatic package installer.

    This class can automatically install any Python package when it's needed,
    with caching to avoid repeated installation attempts.

    Example:
        installer = AutoInstaller()

        # Install and import scipy
        scipy = installer.ensure_package('scipy')

        # Install with different import name
        sklearn = installer.ensure_package('scikit-learn', import_name='sklearn')

        # Install multiple packages
        installer.ensure_packages(['numpy', 'pandas', 'matplotlib'])
    """

    def __init__(self, timeout: int = 120, verbose: bool = True):
        """
        Initialize the AutoInstaller.

        Args:
            timeout: Installation timeout in seconds
            verbose: Whether to print installation messages
        """
        self.timeout = timeout
        self.verbose = verbose
        self._installation_cache = {}  # Cache successful / failed installations
        self._import_cache = {}  # Cache imported modules

    def ensure_package(
        self, package_name: str, import_name: str = None, version: str = None, upgrade: bool = False
    ) -> Optional[object]:
        """
        Ensure a package is available, installing if necessary.

        Args:
            package_name: Name of package to install (e.g., 'scipy')
            import_name: Name to use for import (defaults to package_name)
            version: Specific version to install (e.g., '>=1.0.0')
            upgrade: Whether to upgrade if already installed

        Returns:
            Imported module or None if installation failed
        """
        if import_name is None:
            import_name = package_name

        # Check cache first
        cache_key = f"{package_name}:{import_name}"
        if cache_key in self._import_cache and not upgrade:
            return self._import_cache[cache_key]

        # Try to import first
        try:
            module = importlib.import_module(import_name)
            self._import_cache[cache_key] = module
            return module
        except ImportError:
            pass

        # Check if we've already tried installing this package
        if cache_key in self._installation_cache and not upgrade:
            if not self._installation_cache[cache_key]:
                return None  # Previous installation failed

        # Install the package
        if self._install_package(package_name, version, upgrade):
            try:
                module = importlib.import_module(import_name)
                self._import_cache[cache_key] = module
                self._installation_cache[cache_key] = True
                return module
            except ImportError:
                if self.verbose:
                    print(f"❌ {package_name} installed but import failed")
                self._installation_cache[cache_key] = False
                return None
        else:
            self._installation_cache[cache_key] = False
            return None

    def _install_package(self, package_name: str, version: str = None, upgrade: bool = False) -> bool:
        """Install a single package."""
        if self.verbose:
            print(f"📦 Installing {package_name}...")

        try:
            # Build installation command
            install_cmd = [sys.executable, "-m", "pip", "install"]

            if upgrade:
                install_cmd.append("--upgrade")

            # Add version specification if provided
            if version:
                install_cmd.append(f"{package_name}{version}")
            else:
                install_cmd.append(package_name)

            # Run installation
            result = subprocess.run(install_cmd, capture_output=True, text=True, timeout=self.timeout)

            if result.returncode == 0:
                if self.verbose:
                    print(f"✅ {package_name} installed successfully!")
                return True
            else:
                if self.verbose:
                    print(f"❌ Failed to install {package_name}: {result.stderr}")
                return False

        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            if self.verbose:
                print(f"⚠️  Could not install {package_name}: {str(e)}")
            return False

    def ensure_packages(
        self, packages: Union[List[str], Dict[str, str]], upgrade: bool = False
    ) -> Dict[str, Optional[object]]:
        """
        Ensure multiple packages are available.

        Args:
            packages: List of package names or dict mapping package_name -> import_name
            upgrade: Whether to upgrade existing packages

        Returns:
            Dictionary mapping package names to imported modules (or None if failed)
        """
        if isinstance(packages, list):
            packages = {pkg: pkg for pkg in packages}

        results = {}

        for package_name, import_name in packages.items():
            results[package_name] = self.ensure_package(package_name, import_name, upgrade=upgrade)

        return results

    def install_from_requirements(self, requirements_text: str) -> bool:
        """
        Install packages from requirements.txt format text.

        Args:
            requirements_text: Text in requirements.txt format

        Returns:
            True if all packages installed successfully
        """
        lines = requirements_text.strip().split("\n")
        packages = []

        for line in lines:
            line = line.strip()
            if line and not line.startswith("#"):
                # Handle package==version format
                if "==" in line:
                    _pkg_name = line.split("==")[0]
                    packages.append(line)  # Keep version specification
                elif ">=" in line:
                    _pkg_name = line.split(">=")[0]
                    packages.append(line)
                else:
                    packages.append(line)

        if self.verbose:
            print(f"📋 Installing {len(packages)} packages from requirements...")

        success_count = 0
        for package_spec in packages:
            # Extract package name for version specs
            _pkg_name = package_spec.split("==")[0].split(">=")[0].split("<=")[0]  # noqa: F841

            if self._install_package(package_spec):
                success_count += 1

        success = success_count == len(packages)
        if self.verbose:
            if success:
                print("🎉 All packages installed successfully!")
            else:
                print(f"⚠️  Installed {success_count}/{len(packages)} packages")

        return success

    def check_package(self, package_name: str, import_name: str = None) -> bool:
        """
        Check if a package is available without installing.

        Args:
            package_name: Name of package to check
            import_name: Name to use for import (defaults to package_name)

        Returns:
            True if package is available
        """
        if import_name is None:
            import_name = package_name

        try:
            importlib.import_module(import_name)
            return True
        except ImportError:
            return False

    def get_installation_status(self) -> Dict[str, bool]:
        """
        Get status of all installation attempts.

        Returns:
            Dictionary mapping package names to installation success status
        """
        return self._installation_cache.copy()

    def clear_cache(self):
        """Clear installation and import caches."""
        self._installation_cache.clear()
        self._import_cache.clear()


# Global installer instance for convenience
_global_installer = AutoInstaller()


def auto_install_package(package_name: str, import_name: str = None, timeout: int = 120) -> bool:
    """
    Automatically install a package if it's not available (convenience function).

    Args:
        package_name: Name of package to install (e.g., 'scipy')
        import_name: Name to use for import (defaults to package_name)
        timeout: Installation timeout in seconds

    Returns:
        True if package is available (either was installed or already present)
    """
    global _global_installer  # noqa: F824
    _global_installer.timeout = timeout
    module = _global_installer.ensure_package(package_name, import_name)
    return module is not None


# Convenience functions using the global installer
def ensure(package_name: str, import_name: str = None, version: str = None) -> Optional[object]:
    """
    Ensure a package is available, installing if necessary.

    Args:
        package_name: Name of package to install
        import_name: Name to use for import (defaults to package_name)
        version: Version specification (e.g., '>=1.0.0')

    Returns:
        Imported module or None if failed

    Example:
        scipy = ensure('scipy')
        sklearn = ensure('scikit-learn', 'sklearn')
        pandas = ensure('pandas', version='>=1.0.0')
    """
    return _global_installer.ensure_package(package_name, import_name, version)


def ensure_packages(*packages) -> Dict[str, Optional[object]]:
    """
    Ensure multiple packages are available.

    Args:
        packages: Package names or (package_name, import_name) tuples

    Returns:
        Dictionary mapping package names to modules

    Example:
        modules = ensure_packages('numpy', 'pandas', ('scikit-learn', 'sklearn'))
        np = modules['numpy']
        pd = modules['pandas']
        sklearn = modules['scikit-learn']
    """
    package_dict = {}

    for pkg in packages:
        if isinstance(pkg, tuple):
            package_name, import_name = pkg
        else:
            package_name = import_name = pkg

        package_dict[package_name] = import_name

    return _global_installer.ensure_packages(package_dict)


def check_available(package_name: str, import_name: str = None) -> bool:
    """
    Check if a package is available without installing.

    Args:
        package_name: Name of package to check
        import_name: Name to use for import

    Returns:
        True if available
    """
    return _global_installer.check_package(package_name, import_name)


def ensure_analytics_packages() -> Dict[str, bool]:
    """
    Ensure all common analytics packages are available.

    Returns:
        Dictionary mapping package names to availability status
    """
    # Core analytics packages
    packages = {
        # Statistical analysis
        "scipy": "scipy",
        "statsmodels": "statsmodels",
        "scikit-learn": "sklearn",
        # Visualization
        "seaborn": "seaborn",
        "plotly": "plotly",
        "matplotlib": "matplotlib",
        "altair": "altair",
        # Data processing
        "openpyxl": "openpyxl",  # Excel files
        "xlsxwriter": "xlsxwriter",  # Excel writing
        "pyarrow": "pyarrow",  # Parquet files
        "fastparquet": "fastparquet",  # Alternative parquet
        # Performance monitoring
        "psutil": "psutil",
        "memory-profiler": "memory_profiler",
        # Jupyter notebook enhancements
        "ipywidgets": "ipywidgets",
        "tqdm": "tqdm",  # Progress bars
        "rich": "rich",  # Rich text and progress
        # Music / audio analysis (optional)
        "librosa": "librosa",
        "spotipy": "spotipy",  # Spotify API
        # Network analysis
        "networkx": "networkx",
        # Time series analysis
        "prophet": "prophet",
        "seasonal-decompose": "seasonal",
        # Text analysis
        "textblob": "textblob",
        "wordcloud": "wordcloud",
        "nltk": "nltk",
        # Database connectors
        "sqlalchemy": "sqlalchemy",
        "pymysql": "pymysql",
        "psycopg2-binary": "psycopg2",
        # API clients
        "requests": "requests",
        "httpx": "httpx",
        # Configuration
        "python-dotenv": "dotenv",
        "pyyaml": "yaml",
        "toml": "toml",
        # Caching
        "joblib": "joblib",
        "diskcache": "diskcache",
    }

    results = {}

    for package_name, import_name in packages.items():
        try:
            results[package_name] = auto_install_package(package_name, import_name)
        except Exception as e:
            print(f"Error with {package_name}: {str(e)}")
            results[package_name] = False

    return results


def check_optional_dependencies() -> Dict[str, List[str]]:
    """
    Check which optional dependencies are missing, organized by category.

    Returns:
        Dictionary mapping categories to lists of missing package names
    """
    package_categories = {
        "essential": [
            ("scipy", "scipy"),
            ("statsmodels", "statsmodels"),
            ("scikit-learn", "sklearn"),
            ("plotly", "plotly"),
            ("seaborn", "seaborn"),
            ("tqdm", "tqdm"),
            ("psutil", "psutil"),
        ],
        "visualization": [
            ("matplotlib", "matplotlib"),
            ("altair", "altair"),
            ("wordcloud", "wordcloud"),
            ("rich", "rich"),
        ],
        "data_formats": [
            ("openpyxl", "openpyxl"),
            ("xlsxwriter", "xlsxwriter"),
            ("pyarrow", "pyarrow"),
            ("fastparquet", "fastparquet"),
        ],
        "jupyter": [("ipywidgets", "ipywidgets"), ("memory-profiler", "memory_profiler")],
        "music_analysis": [("librosa", "librosa"), ("spotipy", "spotipy")],
        "advanced_analytics": [
            ("networkx", "networkx"),
            ("prophet", "prophet"),
            ("textblob", "textblob"),
            ("nltk", "nltk"),
        ],
        "database": [("sqlalchemy", "sqlalchemy"), ("pymysql", "pymysql"), ("psycopg2-binary", "psycopg2")],
        "utilities": [
            ("requests", "requests"),
            ("httpx", "httpx"),
            ("python-dotenv", "dotenv"),
            ("pyyaml", "yaml"),
            ("joblib", "joblib"),
            ("diskcache", "diskcache"),
        ],
    }

    missing_by_category = {}

    for category, packages in package_categories.items():
        missing = []
        for package_name, import_name in packages:
            try:
                importlib.import_module(import_name)
            except ImportError:
                missing.append(package_name)

        if missing:
            missing_by_category[category] = missing

    return missing_by_category


def install_missing_dependencies(categories: List[str] = None, auto_install: bool = True) -> bool:
    """
    Install missing optional dependencies by category.

    Args:
        categories: List of categories to install (None for all)
        auto_install: Whether to automatically install missing packages

    Returns:
        True if all requested packages are now available
    """
    missing_by_category = check_optional_dependencies()

    if not missing_by_category:
        print("✅ All optional dependencies are available!")
        return True

    # Filter by requested categories
    if categories:
        missing_by_category = {cat: packages for cat, packages in missing_by_category.items() if cat in categories}

    # Flatten to get all missing packages
    all_missing = []
    for packages in missing_by_category.values():
        all_missing.extend(packages)

    if not all_missing:
        print("✅ All requested dependencies are available!")
        return True

    print("📋 Missing packages by category:")
    for category, packages in missing_by_category.items():
        print(f"  {category}: {', '.join(packages)}")

    if not auto_install:
        print("\nAuto-install disabled. Install manually with:")
        print(f"pip install {' '.join(all_missing)}")
        return False

    success_count = 0
    total_count = len(all_missing)

    print(f"\n🚀 Installing {total_count} missing packages...")

    for category, packages in missing_by_category.items():
        print(f"\n📦 Installing {category} packages...")
        for package in packages:
            if auto_install_package(package):
                success_count += 1

    if success_count == total_count:
        print("🎉 All missing packages installed successfully!")
        return True
    else:
        print(f"⚠️  Installed {success_count}/{total_count} packages")
        remaining = total_count - success_count
        print(f"   {remaining} packages failed to install")
        return False


def safe_import_with_fallback(package_name: str, fallback_message: str = None):
    """
    Safely import a package with automatic installation attempt.

    Args:
        package_name: Name of package to import
        fallback_message: Message to show if import fails

    Returns:
        Imported module or None if failed
    """
    try:
        return importlib.import_module(package_name)
    except ImportError:
        # Try auto-installation
        if auto_install_package(package_name):
            try:
                return importlib.import_module(package_name)
            except ImportError:
                pass

        # Show fallback message
        if fallback_message:
            warnings.warn(fallback_message, UserWarning)

        return None


# Convenience functions for common packages
def ensure_scipy():
    """Ensure scipy is available."""
    return auto_install_package("scipy")


def ensure_statsmodels():
    """Ensure statsmodels is available."""
    return auto_install_package("statsmodels")


def ensure_sklearn():
    """Ensure scikit-learn is available."""
    return auto_install_package("scikit-learn", "sklearn")


def ensure_seaborn():
    """Ensure seaborn is available."""
    return auto_install_package("seaborn")


# Predefined package collections for common use cases
ANALYTICS_ESSENTIALS = {
    "numpy": "numpy",
    "pandas": "pandas",
    "scipy": "scipy",
    "matplotlib": "matplotlib",
    "seaborn": "seaborn",
    "plotly": "plotly",
    "pydantic": "pydantic",  # Required for data validation models
}

JUPYTER_ESSENTIALS = {
    "ipywidgets": "ipywidgets",
    "tqdm": "tqdm",
    "rich": "rich",
    "memory-profiler": "memory_profiler",
}  # noqa: E128

MUSIC_ANALYTICS = {"librosa": "librosa", "spotipy": "spotipy", "textblob": "textblob", "wordcloud": "wordcloud"}

DATABASE_CONNECTORS = {"sqlalchemy": "sqlalchemy", "pymysql": "pymysql", "psycopg2-binary": "psycopg2"}

DATA_FORMATS = {"openpyxl": "openpyxl", "xlsxwriter": "xlsxwriter", "pyarrow": "pyarrow", "fastparquet": "fastparquet"}


def install_analytics_stack() -> Dict[str, Optional[object]]:
    """Install essential analytics packages."""
    return _global_installer.ensure_packages(ANALYTICS_ESSENTIALS)


def install_jupyter_stack() -> Dict[str, Optional[object]]:
    """Install Jupyter notebook enhancements."""
    return _global_installer.ensure_packages(JUPYTER_ESSENTIALS)


def install_music_stack() -> Dict[str, Optional[object]]:
    """Install music-specific analysis packages."""
    return _global_installer.ensure_packages(MUSIC_ANALYTICS)


def install_database_stack() -> Dict[str, Optional[object]]:
    """Install database connector packages."""
    return _global_installer.ensure_packages(DATABASE_CONNECTORS)


def install_data_formats_stack() -> Dict[str, Optional[object]]:
    """Install data format support packages."""
    return _global_installer.ensure_packages(DATA_FORMATS)


def install_full_stack() -> Dict[str, Optional[object]]:
    """Install all predefined package stacks."""
    all_packages = {}
    all_packages.update(ANALYTICS_ESSENTIALS)
    all_packages.update(JUPYTER_ESSENTIALS)
    all_packages.update(MUSIC_ANALYTICS)
    all_packages.update(DATABASE_CONNECTORS)
    all_packages.update(DATA_FORMATS)

    return _global_installer.ensure_packages(all_packages)


if __name__ == "__main__":
    """Run interactive package installer when called directly."""
    print("🔍 YouTube Analytics Auto-Installer")
    print("=" * 50)

    print("Available package stacks:")
    print("1. Analytics Essentials (numpy, pandas, scipy, matplotlib, seaborn, plotly)")
    print("2. Jupyter Enhancements (ipywidgets, tqdm, rich, memory-profiler)")
    print("3. Music Analytics (librosa, spotipy, textblob, wordcloud)")
    print("4. Database Connectors (sqlalchemy, pymysql, psycopg2)")
    print("5. Data Formats (openpyxl, xlsxwriter, pyarrow, fastparquet)")
    print("6. Full Stack (all of the above)")
    print("7. Custom package")
    print("8. Check what's missing")

    choice = input("\nSelect option (1-8): ").strip()

    if choice == "1":
        print("Installing Analytics Essentials...")
        install_analytics_stack()
    elif choice == "2":
        print("Installing Jupyter Enhancements...")
        install_jupyter_stack()
    elif choice == "3":
        print("Installing Music Analytics...")
        install_music_stack()
    elif choice == "4":
        print("Installing Database Connectors...")
        install_database_stack()
    elif choice == "5":
        print("Installing Data Formats...")
        install_data_formats_stack()
    elif choice == "6":
        print("Installing Full Stack...")
        install_full_stack()
    elif choice == "7":
        package = input("Enter package name: ").strip()
        import_name = input("Import name (press enter if same): ").strip()
        if not import_name:
            import_name = package

        result = ensure(package, import_name)
        if result:
            print(f"✅ {package} is now available!")
        else:
            print(f"❌ Failed to install {package}")
    elif choice == "8":
        missing = check_optional_dependencies()
        if missing:
            print("Missing packages by category:")
            for category, packages in missing.items():
                print(f"  {category}: {', '.join(packages)}")
        else:
            print("✅ All predefined packages are available!")
    else:
        print("Invalid choice")
