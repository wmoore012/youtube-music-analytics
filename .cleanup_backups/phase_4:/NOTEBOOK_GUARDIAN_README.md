# 🛡️ Notebook Guardian

**The AI Agent's Best Friend for Data Science Validation**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-passing-green.svg)](tests/)

Lightning-fast, dependency-aware validation system for Jupyter notebooks and Python data science workflows. Perfect for AI agents who need bulletproof data validation without the headache of missing dependencies.

## ⚠️ **IMPORTANT SECURITY DISCLAIMER**

**USE AT YOUR OWN RISK**

Notebook Guardian includes automatic package installation features that can execute `pip install` commands on your system. This functionality comes with inherent security risks:

### 🚨 **Critical Warnings**

- **Automatic Installation Risk**: The auto-install feature can install packages without explicit user confirmation
- **AI-Generated Code Risk**: This tool may be used with AI-generated code that could contain malicious or unintended dependencies
- **Supply Chain Attacks**: Automatically installed packages could be compromised or malicious
- **System Modification**: Package installation modifies your Python environment and system
- **Network Security**: Installation requires network access and downloads code from external sources

### 🛡️ **Security Best Practices**

1. **Disable Auto-Install in Production**:
   ```python
   # Safe usage-manual control
   installer = SmartInstaller(auto_install=False)
   deps = check_dependencies('script.py')
   # Review dependencies before installing
   ```

2. **Review Dependencies Before Installation**:
   ```python
   # Always check what will be installed
   deps = check_dependencies('ai_generated_script.py')
   print(f"Will install: {deps}")
   # Manually verify each package is legitimate
   ```

3. **Use Virtual Environments**:
   ```bash
   # Isolate installations
   python -m venv guardian_env
   source guardian_env/bin/activate
   pip install notebook-guardian
   ```

4. **Audit AI-Generated Code**:
   - Never blindly trust AI-generated import statements
   - Verify package names are legitimate (not typosquatting)
   - Check for suspicious or unnecessary dependencies

### 🔒 **Recommended Safe Usage**

```python
# SAFE: Manual dependency checking
from notebook_guardian import check_dependencies
from notebook_guardian.security import enable_safe_mode, create_security_report

# Enable safe mode (disables auto-install)
enable_safe_mode()

# Check dependencies without installing
deps = check_dependencies('ai_script.py')
print(f"Required packages: {deps}")

# Get security analysis
security_report = create_security_report(deps)
print(f"Safe packages: {security_report['safe_packages']}")
print(f"Suspicious packages: {security_report['suspicious_packages']}")

# Review each package manually before installing
for package in deps:
    print(f"Install {package}? (verify legitimacy first)")

# UNSAFE: Automatic installation without review
# auto_install_file('untrusted_ai_script.py')  # DON'T DO THIS
```

### 🛡️ **Security Controls**

```python
from notebook_guardian.security import SecurityConfig, set_security_config

# Configure strict security
config = SecurityConfig(
    auto_install_enabled=False,
    require_confirmation=True,
    strict_mode=True,
    blocked_packages={'suspicious-package', 'malware-pkg'},
    allowed_packages={'pandas', 'numpy', 'matplotlib'}  # Allowlist
)
set_security_config(config)
```

### 📋 **Your Responsibility**

By using Notebook Guardian, you acknowledge that:
- You understand the security risks of automatic package installation
- You will review all dependencies before installation in production environments
- You accept full responsibility for any packages installed on your system
- You will use appropriate security measures (virtual environments, code review, etc.)

**The authors of Notebook Guardian are not responsible for any security issues, system damage, or malicious packages installed through this tool.**

## 🚀 **Why Notebook Guardian?**

**For AI Agents:**
- 🤖 Never forget to install dependencies again
- ⚡ Ultra-fast validation (50k+ rows in <1 second)
- 🛡️ Bulletproof error handling with clear messages
- 🔧 Zero-config setup-works out of the box

**For Data Scientists:**
- 📊 Comprehensive validation for all ML/DL workflows
- 📝 Human-readable explanations for complex metrics
- 🎯 Supports both .py files and .ipynb notebooks
- 🚀 Parallel processing for maximum speed

**For Teams:**
- 🔄 Perfect for CI/CD pipelines
- 📈 Ensures reproducible research
- 🎓 Great for teaching and learning
- 🌐 Works with any data science stack

## ⚡ **Quick Start**

### Installation
```bash
pip install notebook-guardian
```

### Basic Usage
```python
from notebook_guardian import validate_data, auto_install_file, explain_metrics

# Validate any DataFrame
import pandas as pd
df = pd.DataFrame({'accuracy': [0.95, 0.87, 0.92]})
result = validate_data(df)
print(f"✅ Data valid: {result.is_valid}")

# Auto-install missing dependencies
auto_install_file('my_ml_script.py')

# Get human-readable explanations
explanations = explain_metrics(['accuracy', 'precision', 'recall'])
print(explanations['accuracy'])
# Output: "Accuracy: Measures the proportion of correct predictions..."
```

## 🎯 **Key Features**

### **🔍 Smart Dependency Detection**
```python
from notebook_guardian import check_dependencies

# Works with files
deps = check_dependencies('ml_pipeline.py')
print(f"Required: {deps}")
# Output: ['pandas', 'scikit-learn', 'matplotlib']

# Works with code strings
code = "df = pd.DataFrame(); model = RandomForestClassifier()"
deps = check_dependencies(code)
# Automatically detects: pandas, scikit-learn
```

### **⚡ Lightning-Fast Validation**
```python
from notebook_guardian import validate_python_file, validate_notebook

# Validate Python files
result = validate_python_file('data_analysis.py')
print(f"Functions found: {result.functions_found}")
print(f"Data science patterns: {result.patterns_detected}")

# Validate Jupyter notebooks
result = validate_notebook('analysis.ipynb')
print(f"Notebook valid: {result.is_valid}")
```

### **📊 Data Science Pattern Recognition**
Automatically detects:
- 🔄 **Data Loading**: `pd.read_csv()`, `np.load()`
- 🧹 **Data Preprocessing**: `dropna()`, `fillna()`, `get_dummies()`
- 🔧 **Feature Engineering**: `transform()`, `fit_transform()`
- 🤖 **Model Training**: `fit()`, `RandomForestClassifier()`
- 📈 **Model Evaluation**: `accuracy_score()`, `classification_report()`
- 📊 **Visualization**: `plt.plot()`, `sns.heatmap()`
- 🧠 **Deep Learning**: `Sequential()`, `Dense()`, `compile()`

### **🎨 Beautiful Explanations**
```python
from notebook_guardian import create_tooltips

metrics = {
    'accuracy': 0.95,
    'precision': 0.87,
    'recall': 0.92
}

tooltips = create_tooltips(metrics)
print(tooltips['accuracy'])
# Output: "Accuracy: 0.950<br>Exceptional performance-model is highly accurate"
```

## 🧪 **Comprehensive Testing**

Notebook Guardian has been tested against **every major data science workflow**:

### **Machine Learning**
- ✅ Scikit-learn pipelines
- ✅ XGBoost, LightGBM, CatBoost
- ✅ Hyperparameter tuning
- ✅ Cross-validation workflows

### **Deep Learning**
- ✅ TensorFlow/Keras models
- ✅ PyTorch workflows
- ✅ Training history validation
- ✅ Callback systems

### **Statistics & Analysis**
- ✅ Statistical tests (t-test, ANOVA, chi-square)
- ✅ Time series analysis
- ✅ A/B testing workflows
- ✅ Confidence intervals

### **Specialized Workflows**
- ✅ Natural Language Processing
- ✅ Computer Vision
- ✅ Recommendation Systems
- ✅ Anomaly Detection
- ✅ Clustering Analysis
- ✅ Multi-modal Analysis

## 🚀 **Performance Benchmarks**

| Operation | Speed | Scale |
|-----------|-------|-------|
| **Dependency Detection** | <10ms | Any file size |
| **Python File Validation** | <100ms | 10k+ lines |
| **Notebook Validation** | <200ms | 100+ cells |
| **Parallel Installation** | 3x faster | Multiple packages |
| **Large Dataset Validation** | <1s | 50k+ rows |

## 🔧 **Advanced Usage**

### **Batch Processing**
```python
from notebook_guardian import PythonFileValidator

validator = PythonFileValidator()

# Process multiple files in parallel
files = ['script1.py', 'script2.py', 'script3.py']
results = validator.validate_multiple_files(files)

for result in results:
    print(f"{result.file_path}: {result.is_valid}")
```

### **Custom Validation**
```python
from notebook_guardian import DataValidator

validator = DataValidator()

# Define custom schema
schema = {
    'type': 'dataframe',
    'columns': {
        'model_name': 'object',
        'accuracy': 'float64',
        'training_time': 'float64'
    },
    'min_rows': 1
}

# Validate with custom schema
result = validator.validate_cell_output(your_data, schema)
```

### **Smart Installation**
```python
from notebook_guardian import SmartInstaller

installer = SmartInstaller(auto_install=True, max_workers=4)

# Process entire project
results = installer.process_multiple_files([
    'data_loader.py',
    'model_trainer.py',
    'evaluator.py'
])

# Get installation stats
stats = installer.get_stats()
print(f"Installed {stats['packages_installed']} packages in {stats['total_time']:.2f}s")
```

## 🎓 **Perfect for Teaching**

Notebook Guardian is excellent for educational environments:

```python
# Students can focus on learning, not debugging imports
from notebook_guardian import quick_validate, quick_install

# Validate student submissions
result = quick_validate(student_dataframe)
if not result.is_valid:
    print("❌ Data validation failed:")
    for error in result.errors:
        print(f"  - {error}")

# Auto-install missing packages
quick_install('pandas', 'matplotlib', 'seaborn')
```

## 🔄 **CI/CD Integration**

Perfect for automated workflows:

```python
# In your CI pipeline
from notebook_guardian import validate_notebook, auto_install_file

def validate_research_notebook(notebook_path):
    # Auto-install dependencies
    install_result = auto_install_file(notebook_path)

    # Validate notebook
    validation_result = validate_notebook(notebook_path)

    if not validation_result.is_valid:
        raise ValueError(f"Notebook validation failed: {validation_result.errors}")

    return True
```

## 🌟 **Why Choose Notebook Guardian?**

### **vs Manual Dependency Management**
- ❌ Manual: Forget imports → Runtime errors → Frustration
- ✅ Guardian: Auto-detect → Auto-install → Just works

### **vs Other Validation Tools**
- ❌ Others: Generic validation, no data science focus
- ✅ Guardian: Built specifically for ML/DL workflows

### **vs No Validation**
- ❌ No validation: Silent failures, bad data, unreliable results
- ✅ Guardian: Catch issues early, ensure data quality, reliable science

## 📦 **Installation Options**

### **Basic Installation**
```bash
pip install notebook-guardian
```

### **Secure Installation (Recommended)**
```bash
# Create isolated environment
python -m venv notebook-guardian-env
source notebook-guardian-env/bin/activate  # On Windows: notebook-guardian-env\Scripts\activate

# Install in isolated environment
pip install notebook-guardian

# Set safe mode by default
export NOTEBOOK_GUARDIAN_SAFE_MODE=true
```

### **With All Optional Dependencies**
```bash
pip install notebook-guardian[full]
```

### **Development Installation**
```bash
git clone https://github.com/your-org/notebook-guardian
cd notebook-guardian
pip install -e .[dev]
```

### **Security-First Setup**
```python
# In your code, always start with safe mode
from notebook_guardian.security import enable_safe_mode
enable_safe_mode()  # Disables automatic installation

# Then use manual dependency checking
from notebook_guardian import check_dependencies
deps = check_dependencies('your_script.py')
# Review and manually install each dependency
```

## 🤝 **Contributing**

We love contributions! Notebook Guardian is designed to be:

- 🚀 **Fast**: Every feature must be optimized for speed
- 🧠 **Smart**: Intelligent defaults and auto-detection
- 🛡️ **Reliable**: Comprehensive testing and error handling
- 📚 **Educational**: Clear documentation and examples

### **Development Setup**
```bash
git clone https://github.com/your-org/notebook-guardian
cd notebook-guardian
pip install -e .[dev]
pytest tests/ -v
```

### **Adding New Patterns**
```python
# Add to DataSciencePatternDetector
new_pattern = re.compile(r'your_pattern_here', re.I)
self._ml_patterns['new_pattern_name'] = new_pattern
```

## 📄 **License**

MIT License-see [LICENSE](LICENSE) file for details.

## 🙏 **Acknowledgments**

Built with ❤️ for the data science community. Special thanks to:
- The pandas, scikit-learn, and Jupyter communities
- AI researchers pushing the boundaries of automated development
- Data scientists who inspired the need for better validation tools

## 📞 **Support**

- 📖 **Documentation**: [Full docs](https://notebook-guardian.readthedocs.io)
- 🐛 **Issues**: [GitHub Issues](https://github.com/your-org/notebook-guardian/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/your-org/notebook-guardian/discussions)
- 📧 **Email**: support@notebook-guardian.dev

---

**Made with 🤖 for AI agents and 🧠 for data scientists**

*"Never let your AI agent forget dependencies again!"*
