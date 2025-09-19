"""
Setup script for Notebook Guardian - The AI Agent's Best Friend.

Ultra-fast, dependency-aware validation for data science workflows.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "NOTEBOOK_GUARDIAN_README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read version from __init__.py
version = "1.0.0"

setup(
    name="notebook-guardian",
    version=version,
    author="AI Agent Collective",
    author_email="support@notebook-guardian.dev",
    description="The AI Agent's Best Friend for Data Science Validation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ai-agent-collective/notebook-guardian",
    project_urls={
        "Bug Reports": "https://github.com/ai-agent-collective/notebook-guardian/issues",
        "Source": "https://github.com/ai-agent-collective/notebook-guardian",
        "Documentation": "https://notebook-guardian.readthedocs.io",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        # Development Status
        "Development Status :: 5 - Production/Stable",
        
        # Intended Audience
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Education",
        
        # Topic
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Software Development :: Testing",
        
        # License
        "License :: OSI Approved :: MIT License",
        
        # Python versions
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        
        # Operating Systems
        "Operating System :: OS Independent",
        
        # Environment
        "Environment :: Console",
        "Environment :: Web Environment",
        
        # Natural Language
        "Natural Language :: English",
    ],
    keywords=[
        "data-science", "validation", "jupyter", "notebook", "ai-agent",
        "machine-learning", "deep-learning", "dependencies", "automation",
        "testing", "quality-assurance", "pandas", "scikit-learn", "tensorflow"
    ],
    python_requires=">=3.8",
    install_requires=[
        # Core dependencies (minimal for speed)
        "pandas>=1.3.0",
        "numpy>=1.20.0",
    ],
    extras_require={
        # Full installation with all optional dependencies
        "full": [
            "matplotlib>=3.3.0",
            "seaborn>=0.11.0",
            "plotly>=5.0.0",
            "scikit-learn>=1.0.0",
            "scipy>=1.7.0",
            "statsmodels>=0.12.0",
            "jupyter>=1.0.0",
            "nbconvert>=6.0.0",
            "ipywidgets>=7.6.0",
        ],
        
        # Development dependencies
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.10.0",
            "pytest-mock>=3.6.0",
            "pytest-timeout>=1.4.0",
            "black>=21.0.0",
            "isort>=5.9.0",
            "flake8>=3.9.0",
            "mypy>=0.910",
            "pre-commit>=2.15.0",
        ],
        
        # Documentation dependencies
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=0.5.0",
            "myst-parser>=0.15.0",
        ],
        
        # Machine Learning extras
        "ml": [
            "scikit-learn>=1.0.0",
            "xgboost>=1.5.0",
            "lightgbm>=3.2.0",
            "catboost>=1.0.0",
        ],
        
        # Deep Learning extras
        "dl": [
            "tensorflow>=2.6.0",
            "torch>=1.9.0",
            "torchvision>=0.10.0",
        ],
        
        # Visualization extras
        "viz": [
            "matplotlib>=3.3.0",
            "seaborn>=0.11.0",
            "plotly>=5.0.0",
            "altair>=4.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "notebook-guardian=notebook_guardian.cli:main",
            "ng-validate=notebook_guardian.cli:validate_command",
            "ng-install=notebook_guardian.cli:install_command",
        ],
    },
    include_package_data=True,
    package_data={
        "notebook_guardian": [
            "data/*.json",
            "templates/*.md",
        ],
    },
    zip_safe=False,
    
    # Performance optimizations
    options={
        "bdist_wheel": {
            "universal": False,  # Not universal (uses f-strings, etc.)
        }
    },
)

# Additional metadata for PyPI
setup_kwargs = {
    "name": "notebook-guardian",
    "version": version,
    "description": "The AI Agent's Best Friend for Data Science Validation",
    
    # Marketing keywords for discoverability
    "keywords": [
        # AI/ML Keywords
        "artificial-intelligence", "machine-learning", "deep-learning", 
        "data-science", "mlops", "ai-agent", "automation",
        
        # Validation Keywords  
        "validation", "testing", "quality-assurance", "data-validation",
        "schema-validation", "type-checking", "error-detection",
        
        # Jupyter/Python Keywords
        "jupyter", "notebook", "python", "pandas", "numpy", "scikit-learn",
        "tensorflow", "pytorch", "matplotlib", "seaborn", "plotly",
        
        # Workflow Keywords
        "ci-cd", "pipeline", "reproducibility", "research", "education",
        "dependency-management", "package-installation", "auto-install",
        
        # Performance Keywords
        "fast", "parallel", "efficient", "optimized", "lightning-fast",
        "high-performance", "scalable", "production-ready"
    ],
    
    # Detailed project information
    "project_urls": {
        "Homepage": "https://github.com/ai-agent-collective/notebook-guardian",
        "Documentation": "https://notebook-guardian.readthedocs.io",
        "Repository": "https://github.com/ai-agent-collective/notebook-guardian",
        "Bug Tracker": "https://github.com/ai-agent-collective/notebook-guardian/issues",
        "Changelog": "https://github.com/ai-agent-collective/notebook-guardian/blob/main/CHANGELOG.md",
        "Discussions": "https://github.com/ai-agent-collective/notebook-guardian/discussions",
        "Funding": "https://github.com/sponsors/ai-agent-collective",
    },
}

if __name__ == "__main__":
    print("🛡️ Setting up Notebook Guardian - The AI Agent's Best Friend")
    print("⚡ Ultra-fast validation for data science workflows")
    print("🚀 Perfect for AI agents who forget dependencies!")
    print()
    print("📦 Installing with optimized dependencies...")
    print("🔧 Zero-config setup - just works out of the box!")
    print()
    print("✅ Ready to guard your notebooks and Python files!")